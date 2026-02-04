//! Chain synchronization job - OPTIMIZED FOR SPEED
//!
//! Key optimizations:
//! 1. Parallel IPFS fetches (up to 20 concurrent)
//! 2. Batch inserts (multi-value INSERT)
//! 3. Higher pagination limit (5000)
//! 4. Fetches individual message content for storage-type messages
//! 5. Queues messages to pending_messages for handler processing

use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use tokio::time::{interval, Duration};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use sqlx::PgPool;
use futures::future::join_all;
use chrono::Utc;

use crate::config::Config;
use crate::chains::{start_indexers, ChainIndexer};
use crate::chains::indexer::{IndexerClient, IndexerBlockchain, IndexerSyncEvent};
use crate::db::SyncStateAccessor;
use crate::types::{Chain, Message, ItemType};

/// Sync interval in seconds
const SYNC_INTERVAL_SECS: u64 = 12;

/// Maximum blocks to sync per batch
const MAX_BLOCKS_PER_BATCH: u64 = 100;

/// Maximum concurrent IPFS fetches
const MAX_CONCURRENT_IPFS: usize = 100;

/// Pagination limit for indexer queries
/// Each event contains a batch file with ~80-100 messages, so 200 events ≈ ~17K messages
const INDEXER_LIMIT: usize = 200;

/// Batch size for database inserts
const DB_BATCH_SIZE: usize = 500;

/// Blacklisted sender addresses — skip these during sync to prevent OOM from spam
const BLACKLISTED_SENDERS: &[&str] = &[
    "0x51A58800b26AA1451aaA803d1746687cB88E0501", // UNSLASHED - 3.5M spam messages
];

/// Run the chain sync job
pub async fn run(config: Arc<Config>) {
    let indexers = start_indexers(&config.chains).await;
    
    if indexers.is_empty() {
        info!("No chain indexers configured, chain sync job idle");
        return;
    }
    
    info!("Started {} chain indexers", indexers.len());
    
    let mut interval = interval(Duration::from_secs(SYNC_INTERVAL_SECS));
    
    loop {
        interval.tick().await;
        
        for indexer in &indexers {
            if let Err(e) = sync_chain(indexer.as_ref(), None).await {
                error!("Chain sync error for {:?}: {}", indexer.chain(), e);
            }
        }
    }
}

/// Run chain sync with database persistence
pub async fn run_with_db(config: Arc<Config>, pool: PgPool) {
    let indexers = start_indexers(&config.chains).await;
    
    if indexers.is_empty() {
        info!("No chain indexers configured, chain sync job idle");
        return;
    }
    
    info!("Started {} chain indexers with database persistence", indexers.len());
    
    for indexer in &indexers {
        let chain = indexer.chain();
        let start_block = match chain {
            Chain::ETH => config.chains.ethereum.as_ref().map(|c| c.start_block).unwrap_or(0),
            _ => 0,
        };
        
        if let Err(e) = SyncStateAccessor::init_chain(&pool, chain, start_block).await {
            error!("Failed to init sync state for {:?}: {}", chain, e);
        }
    }
    
    let mut interval = interval(Duration::from_secs(SYNC_INTERVAL_SECS));
    
    loop {
        interval.tick().await;
        
        for indexer in &indexers {
            if let Err(e) = sync_chain(indexer.as_ref(), Some(&pool)).await {
                error!("Chain sync error for {:?}: {}", indexer.chain(), e);
            }
        }
    }
}

/// Sync a single chain
async fn sync_chain(
    indexer: &dyn ChainIndexer,
    pool: Option<&PgPool>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let chain = indexer.chain();
    let current_block = indexer.get_block_height().await?;
    
    let last_synced = if let Some(pool) = pool {
        SyncStateAccessor::get_last_block(pool, chain).await?.unwrap_or(0)
    } else {
        0
    };
    
    if current_block <= last_synced {
        return Ok(0);
    }
    
    let blocks_behind = current_block - last_synced;
    let start_block = last_synced + 1;
    let end_block = std::cmp::min(start_block + MAX_BLOCKS_PER_BATCH - 1, current_block);
    
    if blocks_behind > MAX_BLOCKS_PER_BATCH {
        debug!(
            "{:?}: {} blocks behind, syncing {} to {} ({} blocks)",
            chain, blocks_behind, start_block, end_block, end_block - start_block + 1
        );
    }
    
    let result = indexer.index_blocks(start_block, end_block).await?;
    
    if let Some(pool) = pool {
        SyncStateAccessor::update_last_block(pool, chain, result.last_block).await?;
        
        for indexed_msg in &result.messages {
            debug!(
                "Found message: {} ({})",
                indexed_msg.message.item_hash,
                indexed_msg.message.message_type
            );
        }
    }
    
    if !result.messages.is_empty() {
        info!(
            "{:?}: Synced {} messages from blocks {} to {}",
            chain, result.messages.len(), start_block, end_block
        );
    }
    
    Ok(result.messages.len())
}

/// Get sync status for all chains
pub async fn get_sync_status(pool: &PgPool) -> Result<Vec<ChainSyncStatus>, sqlx::Error> {
    let states = SyncStateAccessor::get_all(pool).await?;
    
    Ok(states.into_iter().map(|s| ChainSyncStatus {
        chain: s.chain,
        last_block: s.last_block,
        last_sync: s.last_sync.to_rfc3339(),
        synced: true,
    }).collect())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ChainSyncStatus {
    pub chain: String,
    pub last_block: u64,
    pub last_sync: String,
    pub synced: bool,
}

#[derive(Debug, serde::Deserialize)]
pub struct IpfsBatchContent {
    pub protocol: String,
    pub version: u32,
    pub content: IpfsBatchMessages,
}

#[derive(Debug, serde::Deserialize)]
pub struct IpfsBatchMessages {
    pub messages: Vec<Message>,
}

/// OPTIMIZED: Run indexer-based chain sync with parallel processing
pub async fn run_indexer_sync(config: Arc<Config>, pool: PgPool, ipfs_url: &str) {
    let indexer_client = IndexerClient::new(None);
    let ipfs_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(MAX_CONCURRENT_IPFS)
        .build()
        .expect("Failed to build HTTP client");
    
    // Use gateway for reading content (may not be pinned locally)
    let gateway_url = config.ipfs.gateway_url.as_str();
    
    info!("Starting OPTIMIZED indexer-based chain sync (max {} concurrent IPFS fetches, with content resolution via gateway)", MAX_CONCURRENT_IPFS);
    
    let mut interval = interval(Duration::from_secs(15));
    
    loop {
        interval.tick().await;
        
        if let Err(e) = sync_chain_from_indexer_optimized(
            &indexer_client,
            &ipfs_client,
            ipfs_url,
            gateway_url,
            &pool,
            Chain::ETH,
        ).await {
            error!("Ethereum indexer sync error: {}", e);
        }
    }
}

/// OPTIMIZED: Sync with parallel IPFS fetches, content resolution, and batch inserts
async fn sync_chain_from_indexer_optimized(
    indexer: &IndexerClient,
    ipfs_client: &reqwest::Client,
    ipfs_url: &str,
    gateway_url: &str,
    pool: &PgPool,
    chain: Chain,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let blockchain = IndexerBlockchain::from(chain);
    
    let mut start_ts = SyncStateAccessor::get_last_sync_timestamp(pool, chain)
        .await?
        .unwrap_or(0);
    
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let mut total_messages = 0;
    let limit = INDEXER_LIMIT;
    
    // Semaphore for limiting concurrent IPFS fetches
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_IPFS));
    
    loop {
        let events = indexer.fetch_sync_events(
            blockchain,
            start_ts,
            now_ms,
            limit,
        ).await?;
        
        let events_count = events.len();
        
        if events.is_empty() {
            break;
        }
        
        info!("{}: Processing {} sync events in parallel (from ts {})", chain, events_count, start_ts);
        
        // Collect unique IPFS CIDs to fetch (batch files)
        let mut cids_to_fetch: Vec<(String, u64)> = Vec::new();
        for event in &events {
            if let Ok(sync_content) = IndexerClient::parse_sync_message(&event.message) {
                cids_to_fetch.push((sync_content.content, event.timestamp));
            } else {
                if cids_to_fetch.is_empty() && events.len() > 0 {
                    tracing::warn!("Failed to parse sync event. Sample: {:.100}", &event.message);
                }
            }
        }
        
        // Parallel IPFS fetches for batch files
        let fetch_futures: Vec<_> = cids_to_fetch.iter().map(|(cid, _ts)| {
            let sem = Arc::clone(&semaphore);
            let client = ipfs_client.clone();
            let url = ipfs_url.to_string();
            let cid = cid.clone();
            
            async move {
                let _permit = sem.acquire().await.ok()?;
                fetch_ipfs_with_retry(&client, &url, &cid, 3).await.ok()
            }
        }).collect();
        
        let ipfs_results: Vec<Option<String>> = join_all(fetch_futures).await;
        
        // Collect all messages from IPFS batches
        let mut all_messages: Vec<Message> = Vec::new();
        let mut successful_fetches = 0;
        
        for result in ipfs_results.into_iter() {
            if let Some(content) = result {
                if let Ok(batch) = serde_json::from_str::<IpfsBatchContent>(&content) {
                    all_messages.extend(batch.content.messages);
                    successful_fetches += 1;
                }
            }
        }
        
        info!("{}: Fetched {} IPFS batches, {} total messages", 
              chain, successful_fetches, all_messages.len());
        
        // Deduplicate messages by item_hash (prevent ON CONFLICT duplicate row issue)
        let mut seen: HashMap<String, Message> = HashMap::new();
        for msg in all_messages {
            seen.insert(msg.item_hash.clone(), msg);
        }
        let all_messages: Vec<Message> = seen.into_values().collect();
        info!("{}: {} unique messages after deduplication", chain, all_messages.len());

        // Filter out blacklisted senders (prevents OOM from spam addresses)
        let pre_blacklist = all_messages.len();
        let all_messages: Vec<Message> = all_messages
            .into_iter()
            .filter(|m| !BLACKLISTED_SENDERS.contains(&m.sender.as_str()))
            .collect();
        if all_messages.len() < pre_blacklist {
            info!("{}: Filtered {} blacklisted messages", chain, pre_blacklist - all_messages.len());
        }

        // Filter out messages that already exist in the database (critical optimization)
        let all_messages = filter_new_messages(pool, all_messages).await;
        info!("{}: {} truly new messages after DB dedup", chain, all_messages.len());

        if !all_messages.is_empty() {
            // Split: inline messages can be inserted immediately, storage-type need content
            let (have_content, need_content): (Vec<_>, Vec<_>) = all_messages
                .into_iter()
                .partition(|m| m.item_content.is_some());

            // Insert inline messages immediately (fast path)
            if !have_content.is_empty() {
                let inline_count = have_content.len();
                let inserted = batch_insert_messages_and_queue(pool, &have_content).await?;
                total_messages += inserted;
                info!("{}: Inserted {} inline messages immediately", chain, inserted);
            }

            // Queue storage-type messages for background content resolution
            // Insert them WITHOUT content first (so cursor can advance), then resolve async
            if !need_content.is_empty() {
                let storage_count = need_content.len();
                info!("{}: {} storage-type messages need content — inserting shells and queuing", chain, storage_count);

                // Insert message shells (without content) so they exist in DB
                let inserted = batch_insert_messages_and_queue(pool, &need_content).await?;
                total_messages += inserted;

                // Resolve content in background (don't block cursor advancement)
                let pool_clone = pool.clone();
                let client_clone = ipfs_client.clone();
                let gateway = gateway_url.to_string();
                let sem = Arc::clone(&semaphore);
                tokio::spawn(async move {
                    resolve_and_update_content(
                        &client_clone,
                        &gateway,
                        &pool_clone,
                        need_content,
                        &sem,
                    ).await;
                });
                info!("{}: Spawned background content resolution for {} messages", chain, storage_count);
            }
        }
        
        // Update cursor — store timestamp + 1 to advance past the last processed event
        // (indexer uses inclusive startDate, so we must skip past the last seen timestamp)
        if let Some(last_event) = events.last() {
            let next_ts = last_event.timestamp + 1;
            start_ts = next_ts;
            SyncStateAccessor::update_last_sync_timestamp(pool, chain, next_ts).await?;
        }
        
        if events_count < limit {
            break;
        }
        
        // Minimal delay between batches
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    
    if total_messages > 0 {
        info!("{}: Synced {} total messages", chain, total_messages);
    }
    
    Ok(total_messages)
}

/// Filter out messages that already exist in the database
/// This is critical for performance: avoids re-downloading IPFS content for known messages
async fn filter_new_messages(pool: &PgPool, messages: Vec<Message>) -> Vec<Message> {
    if messages.is_empty() {
        return messages;
    }

    // Check in batches of 1000 hashes
    let mut existing_hashes: HashSet<String> = HashSet::new();

    for chunk in messages.chunks(1000) {
        let hashes: Vec<&str> = chunk.iter().map(|m| m.item_hash.as_str()).collect();

        // Build a query with ANY($1)
        let result: Vec<(String,)> = match sqlx::query_as(
            "SELECT item_hash FROM messages WHERE item_hash = ANY($1)"
        )
        .bind(&hashes)
        .fetch_all(pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!("Failed to check existing messages: {}, skipping dedup", e);
                return messages;
            }
        };

        for (hash,) in result {
            existing_hashes.insert(hash);
        }
    }

    let before = messages.len();
    let filtered: Vec<Message> = messages
        .into_iter()
        .filter(|m| !existing_hashes.contains(&m.item_hash))
        .collect();

    if before != filtered.len() {
        info!("Filtered {} existing messages, {} new", before - filtered.len(), filtered.len());
    }

    filtered
}

/// Background task: resolve content for storage-type messages and update them in DB
async fn resolve_and_update_content(
    client: &reqwest::Client,
    gateway_url: &str,
    pool: &PgPool,
    messages: Vec<Message>,
    semaphore: &Arc<Semaphore>,
) {
    let total = messages.len();
    let mut resolved = 0;
    let mut failed = 0;

    // Process in chunks to avoid holding too much in memory
    for (chunk_idx, chunk) in messages.chunks(1000).enumerate() {
        let fetch_futures: Vec<_> = chunk.iter().map(|msg| {
            let sem = Arc::clone(semaphore);
            let client = client.clone();
            let gateway = gateway_url.to_string();
            let item_hash = msg.item_hash.clone();

            async move {
                let _permit = sem.acquire().await.ok();
                match fetch_from_gateway(&client, &gateway, &item_hash, 3).await {
                    Ok(content) => Some((item_hash, content)),
                    Err(_) => None,
                }
            }
        }).collect();

        let results: Vec<Option<(String, String)>> = join_all(fetch_futures).await;

        for result in results {
            if let Some((hash, content)) = result {
                // Update the message in the DB with the fetched content
                match sqlx::query(
                    "UPDATE messages SET item_content = $1 WHERE item_hash = $2 AND item_content IS NULL"
                )
                .bind(&content)
                .bind(&hash)
                .execute(pool)
                .await
                {
                    Ok(_) => resolved += 1,
                    Err(e) => {
                        debug!("Failed to update content for {}: {}", hash, e);
                        failed += 1;
                    }
                }
            } else {
                failed += 1;
            }
        }

        if (chunk_idx + 1) % 10 == 0 || chunk_idx == 0 {
            info!("Content resolution progress: {}/{} resolved, {} failed, {}/{} total",
                  resolved, resolved + failed, failed, (chunk_idx + 1) * 1000, total);
        }
    }

    info!("Content resolution complete: {}/{} resolved, {} failed", resolved, total, failed);
}

/// Resolve content for messages with item_type storage/ipfs
async fn resolve_message_contents(
    client: &reqwest::Client,
    gateway_url: &str,
    messages: Vec<Message>,
    semaphore: &Arc<Semaphore>,
) -> Vec<Message> {
    // Separate messages that need content fetch
    let (need_fetch, have_content): (Vec<_>, Vec<_>) = messages
        .into_iter()
        .partition(|m| {
            (m.item_type == ItemType::Storage || m.item_type == ItemType::Ipfs) 
                && m.item_content.is_none()
        });
    
    if need_fetch.is_empty() {
        return have_content;
    }
    
    info!("Fetching content for {} storage-type messages via gateway", need_fetch.len());
    
    // Fetch content in parallel using gateway
    let fetch_futures: Vec<_> = need_fetch.into_iter().map(|msg| {
        let sem = Arc::clone(semaphore);
        let client = client.clone();
        let gateway = gateway_url.to_string();
        let item_hash = msg.item_hash.clone();
        
        async move {
            let _permit = sem.acquire().await.ok();
            match fetch_from_gateway(&client, &gateway, &item_hash, 3).await {
                Ok(content) => {
                    // Create new message with content
                    Message {
                        item_content: Some(content),
                        ..msg
                    }
                }
                Err(e) => {
                    debug!("Failed to fetch content for {}: {}", item_hash, e);
                    msg // Return original without content
                }
            }
        }
    }).collect();
    
    let mut resolved: Vec<Message> = join_all(fetch_futures).await;
    
    // Count how many got content
    let with_content = resolved.iter().filter(|m| m.item_content.is_some()).count();
    info!("Resolved content for {}/{} messages", with_content, resolved.len());
    
    // Combine with messages that already had content
    resolved.extend(have_content);
    resolved
}

/// Fetch content from IPFS gateway with retries
async fn fetch_from_gateway(
    client: &reqwest::Client,
    gateway_url: &str,
    cid: &str,
    max_retries: usize,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Gateway URL format: https://ipfs.aleph.im/ipfs/{cid}
    let url = format!("{}/{}", gateway_url.trim_end_matches('/'), cid);
    
    for attempt in 0..max_retries {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                return Ok(response.text().await?);
            }
            Ok(response) => {
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_millis(200 * (attempt as u64 + 1))).await;
                } else {
                    return Err(format!("Gateway fetch failed: {}", response.status()).into());
                }
            }
            Err(e) => {
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_millis(200 * (attempt as u64 + 1))).await;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    
    Err("Max retries exceeded".into())
}

/// Fetch IPFS content with retries (local API)
async fn fetch_ipfs_with_retry(
    client: &reqwest::Client,
    ipfs_url: &str,
    cid: &str,
    max_retries: usize,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/api/v0/cat?arg={}", ipfs_url, cid);
    
    for attempt in 0..max_retries {
        match client.post(&url).send().await {
            Ok(response) if response.status().is_success() => {
                return Ok(response.text().await?);
            }
            Ok(response) => {
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                } else {
                    return Err(format!("IPFS fetch failed: {}", response.status()).into());
                }
            }
            Err(e) => {
                if attempt < max_retries - 1 {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    
    Err("Max retries exceeded".into())
}

/// OPTIMIZED: Batch insert messages AND queue them to pending_messages for processing
async fn batch_insert_messages_and_queue(
    pool: &PgPool,
    messages: &[Message],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if messages.is_empty() {
        return Ok(0);
    }
    
    let mut total_inserted = 0;
    let now = Utc::now().timestamp() as f64;
    
    // Process in batches
    for chunk in messages.chunks(DB_BATCH_SIZE) {
        // Build multi-value INSERT for messages table
        let mut query = String::from(
            "INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time, created_at) VALUES "
        );
        
        let mut param_idx = 1;
        
        for (i, _) in chunk.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push_str(&format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, NOW())",
                param_idx, param_idx + 1, param_idx + 2, param_idx + 3, param_idx + 4,
                param_idx + 5, param_idx + 6, param_idx + 7, param_idx + 8
            ));
            param_idx += 9;
        }
        
        query.push_str(" ON CONFLICT (item_hash) DO UPDATE SET item_content = EXCLUDED.item_content WHERE messages.item_content IS NULL RETURNING item_hash");
        
        // Build and execute query
        let mut sqlx_query = sqlx::query_scalar::<_, String>(&query);
        
        for msg in chunk {
            sqlx_query = sqlx_query
                .bind(&msg.item_hash)
                .bind(msg.message_type.to_string())
                .bind(msg.chain.to_string())
                .bind(&msg.sender)
                .bind(&msg.signature)
                .bind(msg.item_type.to_string())
                .bind(msg.item_content.as_ref().map(|s| s.as_str()))
                .bind(&msg.channel)
                .bind(msg.time);
        }
        
        // Get list of inserted/updated item_hashes
        let affected_hashes: Vec<String> = sqlx_query.fetch_all(pool).await?;
        let affected_count = affected_hashes.len();
        
        if affected_count > 0 {
            // Queue messages that were inserted or had content updated
            let affected_set: HashSet<&str> = affected_hashes.iter().map(|s| s.as_str()).collect();
            let messages_to_queue: Vec<&Message> = chunk.iter()
                .filter(|m| affected_set.contains(m.item_hash.as_str()))
                .collect();
            
            if !messages_to_queue.is_empty() {
                queue_messages_for_processing(pool, &messages_to_queue, now).await?;
            }
        }
        
        total_inserted += affected_count;
    }
    
    Ok(total_inserted)
}

/// Queue messages for processing by inserting into pending_messages
async fn queue_messages_for_processing(
    pool: &PgPool,
    messages: &[&Message],
    now: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if messages.is_empty() {
        return Ok(());
    }
    
    // Build multi-value INSERT for pending_messages
    let mut query = String::from(
        "INSERT INTO pending_messages (item_hash, message, reception_time, fetched, check_message, retries, next_attempt, trusted_source) VALUES "
    );
    
    let mut param_idx = 1;
    
    for (i, _) in messages.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, true)",
            param_idx, param_idx + 1, param_idx + 2, param_idx + 3, 
            param_idx + 4, param_idx + 5, param_idx + 6
        ));
        param_idx += 7;
    }
    
    query.push_str(" ON CONFLICT (item_hash) DO NOTHING");
    
    let mut sqlx_query = sqlx::query(&query);
    
    for msg in messages {
        let message_json = serde_json::to_value(msg)?;
        
        sqlx_query = sqlx_query
            .bind(&msg.item_hash)
            .bind(message_json)
            .bind(now)
            .bind(msg.item_content.is_some())  // fetched = true only if we have content
            .bind(true)  // check_message
            .bind(0i32)  // retries
            .bind(now);  // next_attempt (process immediately)
    }
    
    sqlx_query.execute(pool).await?;
    
    debug!("Queued {} messages for processing", messages.len());
    Ok(())
}

/// Legacy function for backwards compatibility
async fn batch_insert_messages(
    pool: &PgPool,
    messages: &[Message],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    batch_insert_messages_and_queue(pool, messages).await
}

// Keep original functions for backwards compatibility
async fn fetch_ipfs_content(
    client: &reqwest::Client,
    ipfs_url: &str,
    cid: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    fetch_ipfs_with_retry(client, ipfs_url, cid, 1).await
}

async fn parse_and_store_messages(
    pool: &PgPool,
    content: &str,
    _chain: Chain,
    _event: &IndexerSyncEvent,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let batch: IpfsBatchContent = serde_json::from_str(content)?;
    batch_insert_messages_and_queue(pool, &batch.content.messages).await
}

/// Insert chain TX and link to messages
async fn insert_chain_tx_confirmations(
    pool: &PgPool,
    tx_hash: &str,
    chain: &str,
    height: i64,
    datetime_ms: u64,
    publisher: &str,
    content: &str,
    item_hashes: &[String],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    use chrono::{TimeZone, Utc};
    
    let datetime = Utc.timestamp_millis_opt(datetime_ms as i64).single();
    
    // Insert chain_txs entry
    sqlx::query(
        "INSERT INTO chain_txs (hash, chain, height, datetime, publisher, protocol, content)
         VALUES ($1, $2, $3, $4, $5, 'aleph-offchain', $6::jsonb)
         ON CONFLICT (hash) DO NOTHING"
    )
    .bind(tx_hash)
    .bind(chain)
    .bind(height)
    .bind(datetime)
    .bind(publisher)
    .bind(serde_json::json!({"content": content}))
    .execute(pool)
    .await?;
    
    // Insert message_confirmations links
    let mut count = 0;
    for item_hash in item_hashes {
        let result = sqlx::query(
            "INSERT INTO message_confirmations (item_hash, tx_hash)
             VALUES ($1, $2)
             ON CONFLICT (item_hash, tx_hash) DO NOTHING"
        )
        .bind(item_hash)
        .bind(tx_hash)
        .execute(pool)
        .await;
        
        if result.is_ok() {
            count += 1;
        }
    }
    
    Ok(count)
}

/// Run RPC-based chain sync (direct eth_getLogs, bypasses multichain indexer)
///
/// This is an alternative to `run_indexer_sync` that reads SyncEvent logs directly
/// from Ethereum RPC endpoints, avoiding the rate-limited multichain indexer API.
pub async fn run_rpc_sync(config: Arc<Config>, pool: PgPool, ipfs_url: &str) {
    use crate::chains::rpc_sync::RpcSyncClient;

    let eth_config = match &config.chains.ethereum {
        Some(c) if c.enabled => c,
        _ => {
            info!("RPC sync: Ethereum not configured or disabled");
            return;
        }
    };

    let rpc_client = match RpcSyncClient::new(eth_config) {
        Ok(c) => c,
        Err(e) => {
            error!("RPC sync: failed to create client: {}", e);
            return;
        }
    };

    let ipfs_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(MAX_CONCURRENT_IPFS)
        .build()
        .expect("Failed to build HTTP client");

    let gateway_url = config.ipfs.gateway_url.as_str();

    info!(
        "Starting RPC-based chain sync (direct eth_getLogs, max {} concurrent IPFS fetches)",
        MAX_CONCURRENT_IPFS
    );

    // Ensure chain_sync_state row exists for ETH rpc_sync
    if let Err(e) = ensure_rpc_sync_state(&pool, &rpc_client).await {
        error!("RPC sync: failed to init sync state: {}", e);
    }

    let mut interval = interval(Duration::from_secs(15));

    loop {
        interval.tick().await;

        if let Err(e) = rpc_sync_cycle(
            &rpc_client,
            &ipfs_client,
            ipfs_url,
            gateway_url,
            &pool,
        ).await {
            error!("RPC sync cycle error: {}", e);
            // Wait a bit longer on error
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Ensure the chain_sync_state row exists for RPC sync tracking
async fn ensure_rpc_sync_state(
    pool: &PgPool,
    client: &crate::chains::rpc_sync::RpcSyncClient,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Use sync_type 'rpc_sync' to track separately from indexer
    // But since the actual table PK is just (chain), we need to use
    // the last_block column for RPC block tracking
    let result: Option<(i64,)> = sqlx::query_as(
        "SELECT last_block FROM chain_sync_state WHERE chain = $1"
    )
    .bind("ETH")
    .fetch_optional(pool)
    .await?;

    if result.is_none() {
        let start = client.start_block() as i64;
        sqlx::query(
            "INSERT INTO chain_sync_state (chain, last_block, last_sync, last_sync_timestamp) \
             VALUES ($1, $2, NOW(), 0) ON CONFLICT (chain) DO NOTHING"
        )
        .bind("ETH")
        .bind(start)
        .execute(pool)
        .await?;
        info!("RPC sync: initialized sync state at block {}", start);
    }

    Ok(())
}

/// Get the last RPC-synced block from DB
async fn get_last_rpc_block(pool: &PgPool) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let result: Option<(i64,)> = sqlx::query_as(
        "SELECT last_block FROM chain_sync_state WHERE chain = 'ETH'"
    )
    .fetch_optional(pool)
    .await?;

    Ok(result.map(|(b,)| b as u64).unwrap_or(0))
}

/// Update the last RPC-synced block
async fn update_last_rpc_block(pool: &PgPool, block: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sqlx::query(
        "INSERT INTO chain_sync_state (chain, last_block, last_sync) \
         VALUES ('ETH', $1, NOW()) \
         ON CONFLICT (chain) DO UPDATE SET last_block = $1, last_sync = NOW()"
    )
    .bind(block as i64)
    .execute(pool)
    .await?;

    Ok(())
}

/// One cycle of RPC sync: fetch events, get IPFS content, insert messages
async fn rpc_sync_cycle(
    rpc_client: &crate::chains::rpc_sync::RpcSyncClient,
    ipfs_client: &reqwest::Client,
    ipfs_url: &str,
    gateway_url: &str,
    pool: &PgPool,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    use crate::chains::rpc_sync::RpcSyncClient;

    // Get where we left off
    let last_block = get_last_rpc_block(pool).await?;
    let start_block = last_block + 1;

    // Fetch sync events from RPC
    let (events, end_block) = rpc_client.fetch_sync_events(start_block).await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    if events.is_empty() {
        return Ok(0);
    }

    info!("RPC sync: {} sync events from blocks {}..{}", events.len(), start_block, end_block);

    // Extract IPFS CIDs from sync events
    let cids_to_fetch = RpcSyncClient::extract_ipfs_cids(&events);
    
    if cids_to_fetch.is_empty() {
        // No valid CIDs but still advance the cursor
        update_last_rpc_block(pool, end_block).await?;
        return Ok(0);
    }

    info!("RPC sync: {} IPFS CIDs to fetch", cids_to_fetch.len());

    // Semaphore for limiting concurrent IPFS fetches
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_IPFS));

    // Parallel IPFS fetches for batch files
    let fetch_futures: Vec<_> = cids_to_fetch.iter().map(|(cid, _block)| {
        let sem = Arc::clone(&semaphore);
        let client = ipfs_client.clone();
        let url = ipfs_url.to_string();
        let gw = gateway_url.to_string();
        let cid = cid.clone();

        async move {
            let _permit = sem.acquire().await.ok()?;
            // Try local IPFS first, then gateway
            match fetch_ipfs_with_retry(&client, &url, &cid, 2).await {
                Ok(content) => Some(content),
                Err(_) => {
                    // Fallback to gateway
                    fetch_from_gateway(&client, &gw, &cid, 2).await.ok()
                }
            }
        }
    }).collect();

    let ipfs_results: Vec<Option<String>> = futures::future::join_all(fetch_futures).await;

    // Collect all messages from IPFS batches
    let mut all_messages: Vec<crate::types::Message> = Vec::new();
    let mut successful_fetches = 0;

    for result in ipfs_results.into_iter() {
        if let Some(content) = result {
            if let Ok(batch) = serde_json::from_str::<IpfsBatchContent>(&content) {
                all_messages.extend(batch.content.messages);
                successful_fetches += 1;
            }
        }
    }

    info!(
        "RPC sync: fetched {}/{} IPFS batches, {} total messages",
        successful_fetches, cids_to_fetch.len(), all_messages.len()
    );

    // Deduplicate messages by item_hash
    let mut seen: HashMap<String, crate::types::Message> = HashMap::new();
    for msg in all_messages {
        seen.insert(msg.item_hash.clone(), msg);
    }
    let all_messages: Vec<crate::types::Message> = seen.into_values().collect();
    info!("RPC sync: {} unique messages after deduplication", all_messages.len());

    // Filter out blacklisted senders
    let pre_blacklist = all_messages.len();
    let all_messages: Vec<crate::types::Message> = all_messages
        .into_iter()
        .filter(|m| !BLACKLISTED_SENDERS.contains(&m.sender.as_str()))
        .collect();
    if all_messages.len() < pre_blacklist {
        info!("RPC sync: filtered {} blacklisted messages", pre_blacklist - all_messages.len());
    }

    // Filter out existing messages
    let all_messages = filter_new_messages(pool, all_messages).await;
    info!("RPC sync: {} truly new messages after DB dedup", all_messages.len());

    let mut total_messages = 0;

    if !all_messages.is_empty() {
        // Split: inline messages vs storage-type that need content
        let (have_content, need_content): (Vec<_>, Vec<_>) = all_messages
            .into_iter()
            .partition(|m| m.item_content.is_some());

        // Insert inline messages immediately
        if !have_content.is_empty() {
            let inserted = batch_insert_messages_and_queue(pool, &have_content).await?;
            total_messages += inserted;
            info!("RPC sync: inserted {} inline messages", inserted);
        }

        // Insert storage-type message shells and resolve content in background
        if !need_content.is_empty() {
            let storage_count = need_content.len();
            let inserted = batch_insert_messages_and_queue(pool, &need_content).await?;
            total_messages += inserted;

            // Resolve content in background
            let pool_clone = pool.clone();
            let client_clone = ipfs_client.clone();
            let gateway = gateway_url.to_string();
            let sem = Arc::clone(&semaphore);
            tokio::spawn(async move {
                resolve_and_update_content(
                    &client_clone,
                    &gateway,
                    &pool_clone,
                    need_content,
                    &sem,
                ).await;
            });
            info!("RPC sync: spawned background content resolution for {} messages", storage_count);
        }
    }

    // Update cursor
    update_last_rpc_block(pool, end_block).await?;

    if total_messages > 0 {
        info!("RPC sync: synced {} messages, now at block {}", total_messages, end_block);
    }

    Ok(total_messages)
}
