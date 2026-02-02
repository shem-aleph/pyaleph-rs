//! Chain synchronization job
//!
//! Syncs messages from supported blockchains using:
//! - Aleph multichain indexer (https://multichain.api.aleph.cloud)
//! - Direct RPC for supplementary sync
//! - IPFS for fetching message batches

use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use sqlx::PgPool;

use crate::config::Config;
use crate::chains::{start_indexers, ChainIndexer};
use crate::chains::indexer::{IndexerClient, IndexerBlockchain, IndexerSyncEvent};
use crate::db::SyncStateAccessor;
use crate::types::{Chain, Message};

/// Sync interval in seconds
const SYNC_INTERVAL_SECS: u64 = 12; // Ethereum block time

/// Maximum blocks to sync per batch
const MAX_BLOCKS_PER_BATCH: u64 = 100;

/// Run the chain sync job
pub async fn run(config: Arc<Config>) {
    // Initialize chain indexers
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
    // Initialize chain indexers
    let indexers = start_indexers(&config.chains).await;
    
    if indexers.is_empty() {
        info!("No chain indexers configured, chain sync job idle");
        return;
    }
    
    info!("Started {} chain indexers with database persistence", indexers.len());
    
    // Initialize sync state for each chain
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
    
    // Get current block height
    let current_block = indexer.get_block_height().await?;
    
    // Get last synced block
    let last_synced = if let Some(pool) = pool {
        SyncStateAccessor::get_last_block(pool, chain).await?.unwrap_or(0)
    } else {
        0
    };
    
    if current_block <= last_synced {
        return Ok(0);
    }
    
    // Calculate blocks to sync
    let blocks_behind = current_block - last_synced;
    let start_block = last_synced + 1;
    let end_block = std::cmp::min(start_block + MAX_BLOCKS_PER_BATCH - 1, current_block);
    
    if blocks_behind > MAX_BLOCKS_PER_BATCH {
        debug!(
            "{:?}: {} blocks behind, syncing {} to {} ({} blocks)",
            chain, blocks_behind, start_block, end_block, end_block - start_block + 1
        );
    }
    
    // Index blocks
    let result = indexer.index_blocks(start_block, end_block).await?;
    
    // Update sync state
    if let Some(pool) = pool {
        SyncStateAccessor::update_last_block(pool, chain, result.last_block).await?;
        
        // Store messages
        for indexed_msg in &result.messages {
            // TODO: Use MessageService to process and store
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
        synced: true, // TODO: Compare with current block
    }).collect())
}

/// Chain sync status
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChainSyncStatus {
    pub chain: String,
    pub last_block: u64,
    pub last_sync: String,
    pub synced: bool,
}

/// IPFS batch content containing messages
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

/// Run indexer-based chain sync (recommended)
/// 
/// Uses the Aleph multichain indexer to fetch sync events,
/// then retrieves message batches from IPFS.
pub async fn run_indexer_sync(config: Arc<Config>, pool: PgPool, ipfs_url: &str) {
    let indexer_client = IndexerClient::new(None);
    let ipfs_client = reqwest::Client::new();
    
    info!("Starting indexer-based chain sync");
    
    let mut interval = interval(Duration::from_secs(30)); // Check every 30s
    
    loop {
        interval.tick().await;
        
        // Sync Ethereum
        if let Err(e) = sync_chain_from_indexer(
            &indexer_client,
            &ipfs_client,
            ipfs_url,
            &pool,
            Chain::ETH,
        ).await {
            error!("Ethereum indexer sync error: {}", e);
        }
    }
}

/// Sync a chain using the indexer - FIXED with pagination loop
/// 
/// Key fixes applied:
/// 1. Increased limit from 100 to 1000
/// 2. Added pagination loop - keeps fetching while events.len() >= limit
/// 3. Uses last event timestamp + 1ms as next start to avoid re-fetching same events
async fn sync_chain_from_indexer(
    indexer: &IndexerClient,
    ipfs_client: &reqwest::Client,
    ipfs_url: &str,
    pool: &PgPool,
    chain: Chain,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let blockchain = IndexerBlockchain::from(chain);
    
    // Get last sync timestamp
    let mut start_ts = SyncStateAccessor::get_last_sync_timestamp(pool, chain)
        .await?
        .unwrap_or(0);
    
    // Current time in milliseconds
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    
    let mut total_messages = 0;
    let limit = 1000; // Increased from 100
    
    // Pagination loop - keep fetching until we get less than limit events
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
        
        info!("{}: Processing {} sync events (from ts {})", chain, events_count, start_ts);
        
        for event in &events {
            // Parse the sync message to get IPFS hash
            let sync_content = match IndexerClient::parse_sync_message(&event.message) {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to parse sync message: {}", e);
                    continue;
                }
            };
            
            // Fetch IPFS content
            let ipfs_content = match fetch_ipfs_content(
                ipfs_client,
                ipfs_url,
                &sync_content.content,
            ).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to fetch IPFS content {}: {}", sync_content.content, e);
                    continue;
                }
            };
            
            // Parse and store messages
            match parse_and_store_messages(pool, &ipfs_content, chain, event).await {
                Ok(count) => {
                    total_messages += count;
                    if count > 0 {
                        debug!(
                            "{}: Stored {} messages from IPFS {}",
                            chain, count, sync_content.content
                        );
                    }
                }
                Err(e) => {
                    warn!("Failed to store messages from {}: {}", sync_content.content, e);
                }
            }
        }
        
        // Update cursor to last event timestamp + 1ms to avoid re-fetching
        if let Some(last_event) = events.last() {
            start_ts = last_event.timestamp + 1;
            SyncStateAccessor::update_last_sync_timestamp(pool, chain, last_event.timestamp).await?;
        }
        
        // If we got less than limit events, we have reached the end
        if events_count < limit {
            break;
        }
        
        // Small delay to avoid hammering the indexer
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    if total_messages > 0 {
        info!("{}: Synced {} total messages", chain, total_messages);
    }
    
    Ok(total_messages)
}

/// Fetch content from IPFS
async fn fetch_ipfs_content(
    client: &reqwest::Client,
    ipfs_url: &str,
    cid: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/api/v0/cat?arg={}", ipfs_url, cid);
    
    let response = client
        .post(&url)
        .timeout(Duration::from_secs(30))
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(format!("IPFS fetch failed: {}", response.status()).into());
    }
    
    Ok(response.text().await?)
}

/// Parse IPFS content and store messages
async fn parse_and_store_messages(
    pool: &PgPool,
    content: &str,
    chain: Chain,
    event: &IndexerSyncEvent,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let batch: IpfsBatchContent = serde_json::from_str(content)?;
    
    let mut count = 0;
    
    for message in &batch.content.messages {
        // Store message
        let result = sqlx::query(
            r#"
            INSERT INTO messages (
                item_hash, message_type, chain, sender, signature,
                item_type, item_content, channel, time, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&message.item_hash)
        .bind(message.message_type.to_string())
        .bind(message.chain.to_string())
        .bind(&message.sender)
        .bind(&message.signature)
        .bind(message.item_type.to_string())
        .bind(message.item_content.as_ref().map(|s| s.as_str()))
        .bind(&message.channel)
        .bind(message.time)
        .execute(pool)
        .await?;
        
        if result.rows_affected() > 0 {
            count += 1;
        }
    }
    
    Ok(count)
}
