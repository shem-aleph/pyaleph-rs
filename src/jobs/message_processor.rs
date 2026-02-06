//! Message processor job
//!
//! Processes pending messages from the queue with proper validation,
//! content fetching, handler dispatch, and retry logic.
//!
//! Reference: aleph/jobs/process_pending_messages.py

use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};
use sqlx::PgPool;
use chrono::Utc;

use crate::config::Config;
use crate::types::{Message, ItemType, ErrorCode};
use crate::services::crypto::CryptoService;
use crate::services::cost::CostService;
use crate::services::ipfs::IpfsService;
use crate::network::rabbitmq::RabbitMQService;
use crate::handlers::{self, HandlerContext};
use crate::db::models::PendingMessageDb;
use futures::stream::{self, StreamExt};

/// Process interval in milliseconds
const PROCESS_INTERVAL_MS: u64 = 50;

/// Maximum messages to process per batch
const BATCH_SIZE: i64 = 2000;

/// Maximum number of retries before rejecting a message
const MAX_RETRIES: i32 = 10;

/// Base retry delay in seconds (exponential backoff)
const BASE_RETRY_DELAY: f64 = 60.0;

/// Maximum retry delay in seconds
const MAX_RETRY_DELAY: f64 = 3600.0;

/// Processing context containing all services
#[derive(Clone)]
pub struct ProcessorContext {
    pub db: PgPool,
    pub crypto: Arc<CryptoService>,
    pub ipfs: Arc<IpfsService>,
    pub config: Arc<Config>,
    /// RabbitMQ for publishing processed messages to the network
    pub rabbitmq: Option<Arc<RabbitMQService>>,
    /// Cost service for balance checking
    pub cost: Option<Arc<CostService>>,
}

impl ProcessorContext {
    pub fn new(db: PgPool, crypto: Arc<CryptoService>, ipfs: Arc<IpfsService>, config: Arc<Config>) -> Self {
        Self { db, crypto, ipfs, config, rabbitmq: None, cost: None }
    }

    pub fn with_rabbitmq(mut self, rabbitmq: Arc<RabbitMQService>) -> Self {
        self.rabbitmq = Some(rabbitmq);
        self
    }

    pub fn with_cost(mut self, cost: Arc<CostService>) -> Self {
        self.cost = Some(cost);
        self
    }
}

/// Run the message processor job
pub async fn run(ctx: Arc<ProcessorContext>) {
    let mut ticker = interval(Duration::from_millis(PROCESS_INTERVAL_MS));
    
    info!("Message processor started");
    
    loop {
        ticker.tick().await;
        
        match process_batch(&ctx).await {
            Ok(processed) => {
                if processed > 0 {
                    debug!("Processed {} messages", processed);
                }
            }
            Err(e) => {
                error!("Message processing error: {}", e);
            }
        }
    }
}

/// Process a batch of pending messages
pub async fn process_batch(ctx: &ProcessorContext) -> Result<u32, ProcessorError> {
    let now = Utc::now().timestamp() as f64;
    
    // Fetch messages that are due for processing
    let pending_messages = sqlx::query_as::<_, PendingMessageDb>(
        r#"
        SELECT * FROM pending_messages 
        WHERE next_attempt <= $1 AND retries < $2
        ORDER BY
            reception_time ASC
        LIMIT $3
        FOR UPDATE SKIP LOCKED
        "#
    )
    .bind(now)
    .bind(MAX_RETRIES)
    .bind(BATCH_SIZE)
    .fetch_all(&ctx.db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    if pending_messages.is_empty() {
        return Ok(0);
    }
    
    // Group messages by sender address for safe parallel processing
    // Messages from the same address must be processed sequentially
    const MAX_PER_ADDRESS: usize = 1000;  // Higher cap
    // Blacklisted addresses (skip processing entirely)
    const BLACKLISTED_ADDRESSES: &[&str] = &[
        "0x51A58800b26AA1451aaA803d1746687cB88E0501", // UNSLASHED - 3.2M spam messages
    ];
    use std::collections::HashMap;
    
    // Group by address — all message types together, processed sequentially per address.
    // Within each group, messages are in reception_time order (from SQL ORDER BY).
    // This means aggregates created before posts get processed first for the same address.
    // Cross-address dependencies resolve on retry.
    let mut by_address: HashMap<String, Vec<PendingMessageDb>> = HashMap::new();
    let mut blacklisted_hashes: Vec<String> = Vec::new();
    for msg in pending_messages {
        let addr = msg.message.get("sender").and_then(|s| s.as_str()).unwrap_or("").to_string();
        if BLACKLISTED_ADDRESSES.contains(&addr.as_str()) {
            blacklisted_hashes.push(msg.item_hash.clone());
            continue;
        }
        let entry = by_address.entry(addr).or_default();
        if entry.len() < MAX_PER_ADDRESS {
            entry.push(msg);
        }
    }
    // Delete blacklisted messages from pending
    if !blacklisted_hashes.is_empty() {
        tracing::info!("Skipping {} blacklisted messages", blacklisted_hashes.len());
        let _ = batch_delete_pending(&ctx.db, &blacklisted_hashes).await;
    }
    
    tracing::info!("Processing {} address groups", by_address.len());
    
    // Process address groups in parallel — within each group, messages are sequential
    let results: Vec<_> = stream::iter(by_address.into_values())
        .map(|address_msgs| {
            let ctx = ctx.clone();
            async move {
                let mut group_results = Vec::new();
                for pending in address_msgs {
                    let hash = pending.item_hash.clone();
                    let result = match timeout(std::time::Duration::from_secs(30), process_single_message(&ctx, &pending)).await {
                        Ok(r) => r,
                        Err(_) => Err(ProcessorError::InvalidMessage("Timeout".to_string()))
                    };
                    group_results.push((hash, pending, result));
                }
                group_results
            }
        })
        .buffer_unordered(200)
        .collect::<Vec<_>>()
        .await;
    
    // Flatten results
    let results: Vec<_> = results.into_iter().flatten().collect();
    tracing::info!("Processed {} messages", results.len());
    // Batch collect successful deletions
    let mut to_delete: Vec<String> = Vec::new();
    let mut processed_count = 0u32;
    tracing::info!("Results to process: {}", results.len());
    
    for (hash, pending, result) in results {
        match result {
            Ok(status) => {
                processed_count += 1;
                match status {
                    ProcessResult::Processed => {
                         to_delete.push(hash);
                    }
                    ProcessResult::Rejected(code, msg) => {
                        tracing::info!("Message {} rejected: {:?} - {}", hash, code, msg);
                        let _ = move_to_rejected(&ctx.db, &pending, code, &msg).await;
                    }
                    ProcessResult::Retry(reason) => {
                        let _ = update_retry(&ctx.db, &pending, &reason).await;
                    }
                    ProcessResult::Deferred => {
                        // Left in pending_messages — next_attempt already updated
                    }
                }
            }
            Err(e) => {
                error!("Error processing message {}: {}", hash, e);
                let _ = update_retry(&ctx.db, &pending, &e.to_string()).await;
            }
        }
    }
    
    // Batch delete processed messages
    tracing::info!("End of batch: to_delete has {} items", to_delete.len());
    if !to_delete.is_empty() {
        batch_delete_pending(&ctx.db, &to_delete).await?;
    }
    
    Ok(processed_count)
}

/// Result of processing a single message
enum ProcessResult {
    /// Message processed successfully
    Processed,
    /// Message rejected with error
    Rejected(ErrorCode, String),
    /// Message needs retry
    Retry(String),
    /// Message deferred — waiting for external action (e.g. content fetch)
    /// Left in pending_messages with updated next_attempt, no retry count increment
    Deferred,
}

/// Process a single pending message
async fn process_single_message(
    ctx: &ProcessorContext,
    pending: &PendingMessageDb,
) -> Result<ProcessResult, ProcessorError> {
    debug!("Processing message: {}", pending.item_hash);
    
    // Step 1: Parse the message
    let message: Message = serde_json::from_value(pending.message.clone())
        .map_err(|e| ProcessorError::InvalidMessage(format!("Failed to parse: {}", e)))?;
    
    // Step 2: Check for duplicate (already processed)
    if is_duplicate(&ctx.db, &message.item_hash).await? {
        debug!("Message {} is a duplicate, skipping", message.item_hash);

        // Record chain confirmation if the duplicate came from chain sync
        // The pending message JSON may contain chain tx metadata from store_chain_message()
        record_chain_confirmation_if_present(&ctx.db, &message.item_hash, &pending.message).await;

        return Ok(ProcessResult::Processed); // Just remove from pending
    }

    // Step 2b: Check if already forgotten (race condition: FORGET processed before target)
    // Reference: aleph/handlers/message_handler.py:443-457
    if is_forgotten(&ctx.db, &message.item_hash).await? {
        debug!("Message {} is already forgotten, rejecting", message.item_hash);
        return Ok(ProcessResult::Rejected(
            ErrorCode::InvalidFormat,
            "Message already forgotten".to_string(),
        ));
    }

    // Step 3: Fetch content if needed
    // For storage/IPFS messages without content, defer to content_fetch service
    // rather than trying to fetch here (avoids race conditions and wasted retries)
    let message = if needs_content_fetch(&message) {
        if pending.fetched {
            // content_fetch has filled the content — reload from messages table
            match reload_message_content(&ctx.db, &pending.item_hash).await {
                Ok(Some(content)) => {
                    let mut msg = message.clone();
                    msg.item_content = Some(content);
                    msg
                }
                Ok(None) => {
                    // Content still not in messages table despite fetched=true, defer
                    return Ok(ProcessResult::Retry("Content marked fetched but not found in messages table".to_string()));
                }
                Err(e) => {
                    return Ok(ProcessResult::Retry(format!("Failed to reload content: {}", e)));
                }
            }
        } else {
            // Content not yet fetched by content_fetch — defer without wasting retries
            defer_pending(&ctx.db, &pending.item_hash, 30.0).await?;
            debug!("Deferring {} — waiting for content_fetch to provide content", pending.item_hash);
            return Ok(ProcessResult::Deferred);
        }
    } else {
        message
    };
    
    // Step 4: Verify signature
    // Only verify if message is NOT from a trusted source (indexer)
    // Messages from indexer are pre-verified by the network
    if !pending.trusted_source {
        match message.verify_signature(&ctx.crypto) {
            Ok(true) => {}
            Ok(false) => {
                return Ok(ProcessResult::Rejected(
                    ErrorCode::InvalidSignature,
                    "Signature verification failed".to_string(),
                ));
            }
            Err(e) => {
                return Ok(ProcessResult::Rejected(
                    ErrorCode::InvalidSignature,
                    format!("Signature verification error: {}", e),
                ));
            }
        }
    } else {
        tracing::debug!("Skipping signature verification for trusted source message: {}", pending.item_hash);
    }
    
    // Step 5: Verify item hash matches content (for inline messages)
    if message.item_type == ItemType::Inline {
        match message.verify_item_hash() {
            Ok(true) => {}
            Ok(false) => {
                return Ok(ProcessResult::Rejected(
                    ErrorCode::InvalidFormat,
                    "Item hash does not match content".to_string(),
                ));
            }
            Err(e) => {
                return Ok(ProcessResult::Rejected(
                    ErrorCode::InvalidFormat,
                    format!("Hash verification error: {}", e),
                ));
            }
        }
    }
    
    // Step 6: Create handler context
    let handler_ctx = create_handler_context(ctx, pending.trusted_source);
    
    // Step 7: Process with appropriate handler
    let status = handlers::process_message(&message, &handler_ctx).await;
    
    // Step 8: Store result based on status
    tracing::debug!("process_single_message status: {} for {}", status.status.as_str(), message.item_hash);
    match status.status.as_str() {
        "processed" => {
            // Store in messages table
            store_processed_message(&ctx.db, &message).await?;

            // Publish to RabbitMQ (fire-and-forget: log warnings but don't block processing)
            if let Some(ref rmq) = ctx.rabbitmq {
                // Announce to local consumers (aleph-messages exchange)
                if let Err(e) = rmq.publish_processed(&message).await {
                    warn!("Failed to publish processed message {} to aleph-messages: {}", message.item_hash, e);
                }
                // Relay to p2p-service for network propagation (p2p-publish exchange)
                if let Err(e) = rmq.publish_to_network(&message).await {
                    warn!("Failed to publish message {} to p2p network: {}", message.item_hash, e);
                }
            }

            Ok(ProcessResult::Processed)
        }
        "rejected" => {
            Ok(ProcessResult::Rejected(
                status.error_code.unwrap_or(ErrorCode::InternalError),
                status.error_message.unwrap_or_else(|| "Unknown error".to_string()),
            ))
        }
        _ => {
            Ok(ProcessResult::Retry("Processing returned unknown status".to_string()))
        }
    }
}

/// Check if message needs content to be fetched from IPFS/storage
fn needs_content_fetch(message: &Message) -> bool {
    match message.item_type {
        ItemType::Inline => false,
        ItemType::Ipfs | ItemType::Storage => message.item_content.is_none(),
    }
}

/// Fetch content for a message from IPFS or storage
async fn fetch_message_content(
    ctx: &ProcessorContext,
    message: &Message,
) -> Result<Message, ProcessorError> {
    let content = match message.item_type {
        ItemType::Ipfs => {
            ctx.ipfs.get(&message.item_hash).await
                .map_err(|e| ProcessorError::ContentFetch(e.to_string()))?
        }
        ItemType::Storage => {
            // Try IPFS gateway for storage items too
            ctx.ipfs.get(&message.item_hash).await
                .map_err(|e| ProcessorError::ContentFetch(e.to_string()))?
        }
        ItemType::Inline => {
            return Ok(message.clone()); // No fetch needed
        }
    };
    
    let content_str = String::from_utf8(content)
        .map_err(|e| ProcessorError::ContentFetch(format!("Invalid UTF-8: {}", e)))?;
    
    // Create new message with fetched content
    let mut msg = message.clone();
    msg.item_content = Some(content_str);
    
    Ok(msg)
}


/// Reload item_content from messages table (after content_fetch has stored it)
async fn reload_message_content(db: &PgPool, item_hash: &str) -> Result<Option<String>, ProcessorError> {
    let result: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT item_content FROM messages WHERE item_hash = $1"
    )
    .bind(item_hash)
    .fetch_optional(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;

    Ok(result.and_then(|(content,)| content))
}

/// Defer a pending message by pushing its next_attempt forward without incrementing retries
async fn defer_pending(db: &PgPool, item_hash: &str, delay_secs: f64) -> Result<(), ProcessorError> {
    let next_attempt = chrono::Utc::now().timestamp() as f64 + delay_secs;
    sqlx::query("UPDATE pending_messages SET next_attempt = $1 WHERE item_hash = $2")
        .bind(next_attempt)
        .bind(item_hash)
        .execute(db)
        .await
        .map_err(|e| ProcessorError::Database(e.to_string()))?;
    Ok(())
}

/// Check if a message has already been processed (duplicate detection)
/// 
/// NOTE: We check if derived data exists, not just if the message exists in messages table.
/// This is because chain_sync inserts to messages table first, then queues for processing.
async fn is_duplicate(db: &PgPool, item_hash: &str) -> Result<bool, ProcessorError> {
    // Check if this message already has derived data (posts, aggregates, stores)
    let has_derived = sqlx::query_scalar::<_, bool>(
        r#"SELECT EXISTS(
            SELECT 1 FROM posts WHERE item_hash = $1
            UNION ALL
            SELECT 1 FROM aggregate_elements WHERE item_hash = $1
            UNION ALL
            SELECT 1 FROM file_pins WHERE item_hash = $1
        )"#
    )
    .bind(item_hash)
    .fetch_one(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    Ok(has_derived)
}

/// Record a chain confirmation for a duplicate message if chain tx metadata is present.
///
/// When chain_sync inserts a message that's already processed, we still want to record
/// the chain transaction as a confirmation. The pending message JSON may contain
/// `tx_hash`, `chain`, and `height` fields from the chain sync path.
///
/// Reference: aleph/jobs/process_pending_messages.py — record_chain_confirmation
async fn record_chain_confirmation_if_present(
    db: &PgPool,
    item_hash: &str,
    pending_json: &serde_json::Value,
) {
    // Extract chain metadata from the pending message envelope
    let tx_hash = pending_json.get("tx_hash").and_then(|v| v.as_str());
    let chain = pending_json.get("chain").and_then(|v| v.as_str());
    let height = pending_json.get("height").and_then(|v| v.as_i64());

    if let (Some(tx_hash), Some(chain), Some(height)) = (tx_hash, chain, height) {
        let result = sqlx::query(
            r#"
            INSERT INTO chain_txs (item_hash, tx_hash, chain, height, confirmed_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (item_hash, tx_hash) DO NOTHING
            "#,
        )
        .bind(item_hash)
        .bind(tx_hash)
        .bind(chain)
        .bind(height)
        .execute(db)
        .await;

        match result {
            Ok(r) if r.rows_affected() > 0 => {
                debug!(
                    "Recorded chain confirmation for duplicate {}: tx={} chain={} height={}",
                    item_hash, tx_hash, chain, height
                );
            }
            Ok(_) => {} // Already had this confirmation
            Err(e) => {
                warn!("Failed to record chain confirmation for {}: {}", item_hash, e);
            }
        }
    }
}

/// Check if a message has already been forgotten
async fn is_forgotten(db: &PgPool, item_hash: &str) -> Result<bool, ProcessorError> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM forgotten_messages WHERE item_hash = $1)"
    )
    .bind(item_hash)
    .fetch_one(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    Ok(exists)
}

/// Mark a pending message as fetched
async fn mark_fetched(db: &PgPool, item_hash: &str) -> Result<(), ProcessorError> {
    sqlx::query("UPDATE pending_messages SET fetched = true WHERE item_hash = $1")
        .bind(item_hash)
        .execute(db)
        .await
        .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    Ok(())
}

/// Delete a pending message after successful processing
async fn delete_pending(db: &PgPool, item_hash: &str) -> Result<(), ProcessorError> {
    let result = sqlx::query("DELETE FROM pending_messages WHERE item_hash = $1")
        .bind(item_hash)
        .execute(db)
        .await;
    
    match &result {
        Ok(r) => tracing::info!("delete_pending: {} rows affected for {}", r.rows_affected(), item_hash),
        Err(e) => tracing::error!("delete_pending FAILED for {}: {}", item_hash, e),
    }
    
    result.map(|_| ()).map_err(|e| ProcessorError::Database(e.to_string()))
}

/// Batch delete pending messages
async fn batch_delete_pending(pool: &PgPool, hashes: &[String]) -> Result<(), ProcessorError> {
    if hashes.is_empty() {
        tracing::debug!("batch_delete_pending: empty list, skipping");
        return Ok(());
    }
    
    tracing::info!("batch_delete_pending: deleting {} hashes", hashes.len());
    
    let result = sqlx::query("DELETE FROM pending_messages WHERE item_hash = ANY($1)")
        .bind(hashes)
        .execute(pool)
        .await;
    
    match &result {
        Ok(r) => tracing::info!("batch_delete_pending: {} rows affected", r.rows_affected()),
        Err(e) => tracing::error!("batch_delete_pending FAILED: {}", e),
    }
    
    result.map(|_| ()).map_err(|e| ProcessorError::Database(e.to_string()))
}

/// Move a message to the rejected table
async fn move_to_rejected(
    db: &PgPool,
    pending: &PendingMessageDb,
    error_code: ErrorCode,
    error_message: &str,
) -> Result<(), ProcessorError> {
    // Insert into rejected_messages
    sqlx::query(
        r#"
        INSERT INTO rejected_messages (item_hash, message, error_code, error_message, rejected_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (item_hash) DO UPDATE SET
            error_code = EXCLUDED.error_code,
            error_message = EXCLUDED.error_message,
            rejected_at = EXCLUDED.rejected_at
        "#
    )
    .bind(&pending.item_hash)
    .bind(&pending.message)
    .bind(error_code.as_i32())
    .bind(error_message)
    .execute(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    // Delete from pending_messages
    delete_pending(db, &pending.item_hash).await?;
    
    Ok(())
}

/// Update retry count and next attempt time with exponential backoff
async fn update_retry(
    db: &PgPool,
    pending: &PendingMessageDb,
    reason: &str,
) -> Result<(), ProcessorError> {
    let new_retries = pending.retries + 1;
    
    // Check if max retries exceeded
    if new_retries >= MAX_RETRIES {
        return move_to_rejected(
            db, 
            pending, 
            ErrorCode::InternalError,
            &format!("Max retries ({}) exceeded: {}", MAX_RETRIES, reason),
        ).await;
    }
    
    // Calculate next attempt with exponential backoff
    let delay = (BASE_RETRY_DELAY * 2.0_f64.powi(pending.retries)).min(MAX_RETRY_DELAY);
    let next_attempt = Utc::now().timestamp() as f64 + delay;
    
    sqlx::query(
        "UPDATE pending_messages SET retries = $1, next_attempt = $2 WHERE item_hash = $3"
    )
    .bind(new_retries)
    .bind(next_attempt)
    .bind(&pending.item_hash)
    .execute(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    debug!(
        "Message {} scheduled for retry {} in {} seconds",
        pending.item_hash, new_retries, delay
    );
    
    Ok(())
}

/// Store a processed message in the main messages table
async fn store_processed_message(db: &PgPool, message: &Message) -> Result<(), ProcessorError> {
    sqlx::query(
        r#"
        INSERT INTO messages (
            item_hash, message_type, chain, sender, signature, 
            item_type, item_content, channel, time, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (item_hash) DO UPDATE SET created_at = NOW()
        "#
    )
    .bind(&message.item_hash)
    .bind(message.message_type.to_string())
    .bind(message.chain.to_string())
    .bind(&message.sender)
    .bind(&message.signature)
    .bind(format!("{:?}", message.item_type).to_lowercase())
    .bind(&message.item_content)
    .bind(&message.channel)
    .bind(message.time)
    .execute(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    Ok(())
}

/// Create a handler context from the processor context
fn create_handler_context(ctx: &ProcessorContext, trusted_source: bool) -> HandlerContext {
    let mut handler_ctx = HandlerContext::new();
    handler_ctx.crypto = Some(ctx.crypto.clone());
    handler_ctx.pool = Some(ctx.db.clone());
    handler_ctx.db = Some(Arc::new(crate::db::PgDatabase::new(ctx.db.clone())));
    handler_ctx.trusted_source = trusted_source;
    handler_ctx.cost = ctx.cost.clone();
    handler_ctx
}

/// Processor errors
#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Invalid message: {0}")]
    InvalidMessage(String),
    
    #[error("Content fetch failed: {0}")]
    ContentFetch(String),
    
    #[error("Handler error: {0}")]
    Handler(String),
}

/// Job statistics
#[derive(Debug, Clone)]
pub struct ProcessorStats {
    pub messages_processed: u64,
    pub messages_rejected: u64,
    pub messages_pending: u64,
    pub avg_process_time_ms: f64,
}

/// Get current processor statistics
pub async fn get_stats(db: &PgPool) -> Result<ProcessorStats, ProcessorError> {
    let pending_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pending_messages"
    )
    .fetch_one(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    let processed_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages"
    )
    .fetch_one(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    let rejected_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rejected_messages"
    )
    .fetch_one(db)
    .await
    .map_err(|e| ProcessorError::Database(e.to_string()))?;
    
    Ok(ProcessorStats {
        messages_processed: processed_count.0 as u64,
        messages_rejected: rejected_count.0 as u64,
        messages_pending: pending_count.0 as u64,
        avg_process_time_ms: 0.0, // Would need timing instrumentation
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_exponential_backoff() {
        // Retry 0: 60 seconds
        let delay_0 = (BASE_RETRY_DELAY * 2.0_f64.powi(0)).min(MAX_RETRY_DELAY);
        assert_eq!(delay_0, 60.0);
        
        // Retry 1: 120 seconds
        let delay_1 = (BASE_RETRY_DELAY * 2.0_f64.powi(1)).min(MAX_RETRY_DELAY);
        assert_eq!(delay_1, 120.0);
        
        // Retry 5: 1920 seconds (~32 min)
        let delay_5 = (BASE_RETRY_DELAY * 2.0_f64.powi(5)).min(MAX_RETRY_DELAY);
        assert_eq!(delay_5, 1920.0);
        
        // Retry 10: capped at MAX_RETRY_DELAY
        let delay_10 = (BASE_RETRY_DELAY * 2.0_f64.powi(10)).min(MAX_RETRY_DELAY);
        assert_eq!(delay_10, MAX_RETRY_DELAY);
    }
}
