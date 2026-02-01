//! Chain indexing module
//!
//! Handles synchronization with various blockchains.

pub mod abi;
pub mod ethereum;

use async_trait::async_trait;
use thiserror::Error;

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
}

/// Result of indexing a block
#[derive(Debug)]
pub struct IndexResult {
    pub messages: Vec<IndexedMessage>,
    pub last_block: u64,
}

/// A message found on-chain
#[derive(Debug)]
pub struct IndexedMessage {
    pub message: Message,
    pub chain_ref: ChainRef,
    pub tx_hash: String,
}

/// Trait for chain indexers
#[async_trait]
pub trait ChainIndexer: Send + Sync {
    /// Get the chain this indexer handles
    fn chain(&self) -> Chain;
    
    /// Get the current block height
    async fn get_block_height(&self) -> Result<u64, ChainError>;
    
    /// Index blocks from start to end (inclusive)
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError>;
    
    /// Watch for new blocks (streaming)
    async fn watch(&self) -> Result<(), ChainError>;
}

/// Start all configured chain indexers
pub async fn start_indexers(config: &crate::config::ChainsConfig) -> Vec<Box<dyn ChainIndexer>> {
    let mut indexers: Vec<Box<dyn ChainIndexer>> = Vec::new();
    
    if let Some(eth_config) = &config.ethereum {
        if eth_config.enabled {
            match ethereum::EthereumIndexer::new(eth_config).await {
                Ok(indexer) => indexers.push(Box::new(indexer)),
                Err(e) => tracing::error!("Failed to create Ethereum indexer: {}", e),
            }
        }
    }
    
    // TODO: Add other chains (Solana, Tezos, etc.)
    
    indexers
}
