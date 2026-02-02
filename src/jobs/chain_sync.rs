//! Chain synchronization job - OPTIMIZED FOR SPEED
//!
//! Key optimizations:
//! 1. Parallel IPFS fetches (up to 20 concurrent)
//! 2. Batch inserts (multi-value INSERT)
//! 3. Higher pagination limit (5000)
//! 4. Reduced sleeps
//! 5. Better error handling with retries

use std::sync::Arc;
use std::collections::HashSet;
use tokio::time::{interval, Duration};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use sqlx::PgPool;
use futures::future::join_all;

use crate::config::Config;
use crate::chains::{start_indexers, ChainIndexer};
use crate::chains::indexer::{IndexerClient, IndexerBlockchain, IndexerSyncEvent};
use crate::db::SyncStateAccessor;
use crate::types::{Chain, Message};

/// Sync interval in seconds
const SYNC_INTERVAL_SECS: u64 = 12;

/// Maximum blocks to sync per batch
const MAX_BLOCKS_PER_BATCH: u64 = 100;

/// Maximum concurrent IPFS fetches
const MAX_CONCURRENT_IPFS: usize = 20;

/// Pagination limit for indexer queries
const INDEXER_LIMIT: usize = 5000;

/// Batch size for database inserts
const DB_BATCH_SIZE: usize = 500;

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
    
    info!("Starting OPTIMIZED indexer-based chain sync (max {} concurrent IPFS fetches)", MAX_CONCURRENT_IPFS);
    
    let mut interval = interval(Duration::from_secs(15)); // Reduced from 30s
    
    loop {
        interval.tick().await;
        
        if let Err(e) = sync_chain_from_indexer_optimized(
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

/// OPTIMIZED: Sync with parallel IPFS fetches and batch inserts
async fn sync_chain_from_indexer_optimized(
    indexer: &IndexerClient,
    ipfs_client: &reqwest::Client,
    ipfs_url: &str,
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
        
        // Collect unique IPFS CIDs to fetch
        let mut cids_to_fetch: Vec<(String, u64)> = Vec::new();
        for event in &events {
            if let Ok(sync_content) = IndexerClient::parse_sync_message(&event.message) {
                cids_to_fetch.push((sync_content.content, event.timestamp));
            }
        }
        
        // Parallel IPFS fetches
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
        
        for (i, result) in ipfs_results.into_iter().enumerate() {
            if let Some(content) = result {
                if let Ok(batch) = serde_json::from_str::<IpfsBatchContent>(&content) {
                    all_messages.extend(batch.content.messages);
                    successful_fetches += 1;
                }
            }
        }
        
        info!("{}: Fetched {} IPFS batches, {} total messages to insert", 
              chain, successful_fetches, all_messages.len());
        
        // Batch insert messages
        if !all_messages.is_empty() {
            let inserted = batch_insert_messages(pool, &all_messages).await?;
            total_messages += inserted;
            info!("{}: Inserted {} new messages", chain, inserted);
        }
        
        // Update cursor
        if let Some(last_event) = events.last() {
            start_ts = last_event.timestamp + 1;
            SyncStateAccessor::update_last_sync_timestamp(pool, chain, last_event.timestamp).await?;
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

/// Fetch IPFS content with retries
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

/// OPTIMIZED: Batch insert messages using multi-value INSERT
async fn batch_insert_messages(
    pool: &PgPool,
    messages: &[Message],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    if messages.is_empty() {
        return Ok(0);
    }
    
    let mut total_inserted = 0;
    
    // Process in batches
    for chunk in messages.chunks(DB_BATCH_SIZE) {
        // Build multi-value INSERT
        let mut query = String::from(
            "INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time, created_at) VALUES "
        );
        
        let mut params: Vec<String> = Vec::new();
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
        
        query.push_str(" ON CONFLICT (item_hash) DO NOTHING");
        
        // Build and execute query
        let mut sqlx_query = sqlx::query(&query);
        
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
        
        let result = sqlx_query.execute(pool).await?;
        total_inserted += result.rows_affected() as usize;
    }
    
    Ok(total_inserted)
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
    chain: Chain,
    event: &IndexerSyncEvent,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let batch: IpfsBatchContent = serde_json::from_str(content)?;
    batch_insert_messages(pool, &batch.content.messages).await
}
