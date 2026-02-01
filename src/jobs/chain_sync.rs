//! Chain synchronization job
//!
//! Syncs messages from supported blockchains.

use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::chains::{start_indexers, ChainIndexer};

/// Sync interval in seconds
const SYNC_INTERVAL_SECS: u64 = 12; // Ethereum block time

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
            match sync_chain(indexer.as_ref()).await {
                Ok(messages) => {
                    if messages > 0 {
                        info!(
                            "Synced {} messages from {:?}",
                            messages,
                            indexer.chain()
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "Chain sync error for {:?}: {}",
                        indexer.chain(),
                        e
                    );
                }
            }
        }
    }
}

/// Sync a single chain
async fn sync_chain(indexer: &dyn ChainIndexer) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let chain = indexer.chain();
    
    // Get current block height
    let current_block = indexer.get_block_height().await?;
    
    // TODO: Get last synced block from database
    let last_synced = 0u64; // Placeholder
    
    if current_block <= last_synced {
        return Ok(0);
    }
    
    // Index blocks in batches
    let batch_size = 100u64;
    let start_block = last_synced + 1;
    let end_block = std::cmp::min(start_block + batch_size - 1, current_block);
    
    debug!(
        "Syncing {:?} blocks {} to {}",
        chain, start_block, end_block
    );
    
    let result = indexer.index_blocks(start_block, end_block).await?;
    
    // TODO: Store messages in database
    // TODO: Update last synced block
    
    Ok(result.messages.len())
}
