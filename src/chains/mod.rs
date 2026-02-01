//! Chain indexing module
//!
//! Handles synchronization with various blockchains including:
//! - Ethereum (mainnet, BSC, Avalanche, Polygon)
//! - Solana
//! - Tezos
//! - NULS
//! - Cosmos SDK chains

pub mod abi;
pub mod ethereum;
pub mod avalanche;
pub mod bsc;
pub mod solana;
pub mod tezos;
pub mod tx_packer;

use async_trait::async_trait;
use thiserror::Error;
use std::sync::Arc;

use crate::config::ChainsConfig;
use crate::types::{Chain, ChainRef, Message};

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("RPC error: {0}")]
    Rpc(String),
    
    #[error("Contract error: {0}")]
    Contract(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Chain not supported: {0}")]
    NotSupported(String),
    
    #[error("Connection failed: {0}")]
    Connection(String),
    
    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u64),
    
    #[error("Invalid block range: {0}")]
    InvalidRange(String),
    
    #[error("Signature error: {0}")]
    Signature(String),
    
    #[error("Timeout")]
    Timeout,
}

/// Result of indexing blocks
#[derive(Debug)]
pub struct IndexResult {
    pub messages: Vec<IndexedMessage>,
    pub sync_hashes: Vec<String>,
    pub last_block: u64,
    pub blocks_processed: u64,
}

impl Default for IndexResult {
    fn default() -> Self {
        Self {
            messages: Vec::new(),
            sync_hashes: Vec::new(),
            last_block: 0,
            blocks_processed: 0,
        }
    }
}

/// A message found on-chain
#[derive(Debug, Clone)]
pub struct IndexedMessage {
    pub message: Message,
    pub chain_ref: ChainRef,
    pub tx_hash: String,
    pub block_time: Option<u64>,
}

/// Sync event from blockchain
/// 
/// Sync events contain IPFS hashes to fetch messages from,
/// used for bulk message synchronization.
#[derive(Debug, Clone)]
pub struct SyncEvent {
    /// IPFS hash containing messages
    pub content_hash: String,
    /// Block height where event was found
    pub block_height: u64,
    /// Transaction hash
    pub tx_hash: String,
    /// Emitter address
    pub emitter: String,
    /// Timestamp
    pub timestamp: Option<u64>,
}

/// Chain sync state for tracking progress
#[derive(Debug, Clone)]
pub struct ChainSyncState {
    pub chain: Chain,
    pub last_message_height: u64,
    pub last_sync_height: u64,
    pub last_sync_time: chrono::DateTime<chrono::Utc>,
}

/// Trait for chain indexers
#[async_trait]
pub trait ChainIndexer: Send + Sync {
    /// Get the chain this indexer handles
    fn chain(&self) -> Chain;
    
    /// Get the current block height from the chain
    async fn get_block_height(&self) -> Result<u64, ChainError>;
    
    /// Index blocks from start to end (inclusive)
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError>;
    
    /// Index sync events from blocks
    async fn index_sync_events(&self, start: u64, end: u64) -> Result<Vec<SyncEvent>, ChainError>;
    
    /// Watch for new blocks (streaming)
    async fn watch(&self) -> Result<(), ChainError>;
    
    /// Check if the chain is synced (within threshold)
    async fn is_synced(&self, threshold: u64) -> Result<bool, ChainError> {
        let height = self.get_block_height().await?;
        // Default implementation - override in specific chains
        Ok(height > 0)
    }
    
    /// Get chain-specific configuration
    fn confirmations_required(&self) -> u64 {
        10 // Default
    }
    
    /// Get recommended batch size for indexing
    fn batch_size(&self) -> u64 {
        1000 // Default
    }
}

/// Start all configured chain indexers
pub async fn start_indexers(config: &ChainsConfig) -> Vec<Arc<dyn ChainIndexer>> {
    let mut indexers: Vec<Arc<dyn ChainIndexer>> = Vec::new();
    
    // Ethereum mainnet
    if let Some(eth_config) = &config.ethereum {
        if eth_config.enabled {
            match ethereum::EthereumIndexer::new(eth_config).await {
                Ok(indexer) => {
                    indexers.push(Arc::new(indexer));
                    tracing::info!("Ethereum indexer initialized");
                }
                Err(e) => tracing::error!("Failed to create Ethereum indexer: {}", e),
            }
        }
    }
    
    // Avalanche C-Chain
    if let Some(avax_config) = &config.avalanche {
        if avax_config.enabled {
            match avalanche::AvalancheIndexer::new(avax_config).await {
                Ok(indexer) => {
                    indexers.push(Arc::new(indexer));
                    tracing::info!("Avalanche indexer initialized");
                }
                Err(e) => tracing::error!("Failed to create Avalanche indexer: {}", e),
            }
        }
    }
    
    // BSC (Binance Smart Chain)
    if let Some(bsc_config) = &config.bsc {
        if bsc_config.enabled {
            match bsc::BscIndexer::new(bsc_config).await {
                Ok(indexer) => {
                    indexers.push(Arc::new(indexer));
                    tracing::info!("BSC indexer initialized");
                }
                Err(e) => tracing::error!("Failed to create BSC indexer: {}", e),
            }
        }
    }
    
    // Solana
    if let Some(sol_config) = &config.solana {
        if sol_config.enabled {
            match solana::SolanaIndexer::new(sol_config).await {
                Ok(indexer) => {
                    indexers.push(Arc::new(indexer));
                    tracing::info!("Solana indexer initialized");
                }
                Err(e) => tracing::error!("Failed to create Solana indexer: {}", e),
            }
        }
    }
    
    // Tezos
    if let Some(tez_config) = &config.tezos {
        if tez_config.enabled {
            match tezos::TezosIndexer::new(tez_config).await {
                Ok(indexer) => {
                    indexers.push(Arc::new(indexer));
                    tracing::info!("Tezos indexer initialized");
                }
                Err(e) => tracing::error!("Failed to create Tezos indexer: {}", e),
            }
        }
    }
    
    indexers
}

/// Run chain sync job for all indexers
pub async fn run_chain_sync(
    indexers: Vec<Arc<dyn ChainIndexer>>,
    db: sqlx::PgPool,
    metrics: Arc<crate::services::Metrics>,
) {
    use tokio::time::{interval, Duration};
    
    let mut ticker = interval(Duration::from_secs(12)); // ~1 Ethereum block
    
    loop {
        ticker.tick().await;
        
        for indexer in &indexers {
            let chain = indexer.chain();
            
            // Get last indexed height from DB
            let last_height = get_last_indexed_height(&db, &chain).await.unwrap_or(0);
            
            // Get current chain height
            let current_height = match indexer.get_block_height().await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!("Failed to get {} block height: {}", chain, e);
                    metrics.inc_chain_sync_errors();
                    continue;
                }
            };
            
            // Calculate safe height (with confirmations)
            let safe_height = current_height.saturating_sub(indexer.confirmations_required());
            
            if last_height >= safe_height {
                continue; // Already synced
            }
            
            // Index in batches
            let batch_size = indexer.batch_size();
            let mut from = last_height + 1;
            
            while from <= safe_height {
                let to = (from + batch_size - 1).min(safe_height);
                
                match indexer.index_blocks(from, to).await {
                    Ok(result) => {
                        // Process indexed messages
                        for msg in &result.messages {
                            if let Err(e) = store_chain_message(&db, msg).await {
                                tracing::error!("Failed to store message: {}", e);
                            }
                        }
                        
                        // Update sync state
                        if let Err(e) = update_sync_state(&db, &chain, to).await {
                            tracing::error!("Failed to update sync state: {}", e);
                        }
                        
                        // Update metrics
                        metrics.inc_blocks_indexed(result.blocks_processed);
                        match chain {
                            Chain::ETH => metrics.set_last_block_eth(to),
                            Chain::SOL => metrics.set_last_block_sol(to),
                            Chain::AVAX => metrics.set_last_block_avax(to),
                            _ => {}
                        }
                        
                        tracing::debug!(
                            "{}: indexed blocks {}-{}, {} messages",
                            chain, from, to, result.messages.len()
                        );
                    }
                    Err(ChainError::RateLimited(secs)) => {
                        tracing::warn!("{}: rate limited, waiting {} seconds", chain, secs);
                        tokio::time::sleep(Duration::from_secs(secs)).await;
                    }
                    Err(e) => {
                        tracing::error!("{}: indexing error: {}", chain, e);
                        metrics.inc_chain_sync_errors();
                        break;
                    }
                }
                
                from = to + 1;
            }
        }
    }
}

/// Get last indexed height from database
async fn get_last_indexed_height(db: &sqlx::PgPool, chain: &Chain) -> Result<u64, sqlx::Error> {
    let result: Option<(i64,)> = sqlx::query_as(
        "SELECT last_height FROM chain_sync_state WHERE chain = $1 AND sync_type = 'messages'"
    )
    .bind(chain.to_string())
    .fetch_optional(db)
    .await?;
    
    Ok(result.map(|r| r.0 as u64).unwrap_or(0))
}

/// Update sync state in database
async fn update_sync_state(db: &sqlx::PgPool, chain: &Chain, height: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO chain_sync_state (chain, sync_type, last_height, last_sync)
        VALUES ($1, 'messages', $2, NOW())
        ON CONFLICT (chain, sync_type) DO UPDATE SET
            last_height = EXCLUDED.last_height,
            last_sync = EXCLUDED.last_sync
        "#
    )
    .bind(chain.to_string())
    .bind(height as i64)
    .execute(db)
    .await?;
    
    Ok(())
}

/// Store a chain-indexed message
async fn store_chain_message(db: &sqlx::PgPool, msg: &IndexedMessage) -> Result<(), sqlx::Error> {
    // Insert into pending_messages for processing
    let now = chrono::Utc::now().timestamp() as f64;
    
    sqlx::query(
        r#"
        INSERT INTO pending_messages (item_hash, message, reception_time, fetched, check_message, retries, next_attempt, created_at)
        VALUES ($1, $2, $3, true, true, 0, $3, NOW())
        ON CONFLICT (item_hash) DO NOTHING
        "#
    )
    .bind(&msg.message.item_hash)
    .bind(serde_json::to_value(&msg.message).unwrap())
    .bind(now)
    .execute(db)
    .await?;
    
    // Record chain transaction
    sqlx::query(
        r#"
        INSERT INTO chain_txs (hash, chain, height, item_hash, protocol, created_at)
        VALUES ($1, $2, $3, $4, 'aleph', NOW())
        ON CONFLICT (hash) DO NOTHING
        "#
    )
    .bind(&msg.tx_hash)
    .bind(msg.chain_ref.chain.to_string())
    .bind(msg.chain_ref.height as i64)
    .bind(&msg.message.item_hash)
    .execute(db)
    .await?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_result_default() {
        let result = IndexResult::default();
        assert!(result.messages.is_empty());
        assert!(result.sync_hashes.is_empty());
        assert_eq!(result.last_block, 0);
    }
}
