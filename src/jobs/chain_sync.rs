//! Chain synchronization job
//!
//! Syncs messages from supported blockchains.

use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use sqlx::PgPool;

use crate::config::Config;
use crate::chains::{start_indexers, ChainIndexer};
use crate::db::SyncStateAccessor;
use crate::types::Chain;

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
