//! Ethereum chain indexer
//!
//! Indexes Aleph messages from the Ethereum blockchain.

use async_trait::async_trait;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    types::{Address as EthAddress, Filter, Log, H256, U64},
};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::EthereumConfig;
use crate::types::{Chain, ChainRef, ItemType, Message, MessageType};

use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage};

/// Aleph.im message event signature
/// event Message(address indexed sender, string msgType, bytes content)
const MESSAGE_EVENT_SIGNATURE: &str = "Message(address,string,bytes)";

/// Sync event signature
/// event SyncMessage(bytes content)
const SYNC_EVENT_SIGNATURE: &str = "SyncMessage(bytes)";

/// Ethereum chain indexer
pub struct EthereumIndexer {
    provider: Arc<Provider<Http>>,
    contract_address: EthAddress,
    chain_id: u64,
    start_block: u64,
}

impl EthereumIndexer {
    /// Create a new Ethereum indexer
    pub async fn new(config: &EthereumConfig) -> Result<Self, ChainError> {
        let provider = Provider::<Http>::try_from(&config.rpc_url)
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        let contract_address = config.contract_address
            .parse::<EthAddress>()
            .map_err(|e| ChainError::Parse(e.to_string()))?;
        
        info!(
            "Ethereum indexer initialized: contract={}, chain_id={}, start_block={}",
            contract_address, config.chain_id, config.start_block
        );
        
        Ok(Self {
            provider: Arc::new(provider),
            contract_address,
            chain_id: config.chain_id,
            start_block: config.start_block,
        })
    }
    
    /// Parse a log into an indexed message
    fn parse_log(&self, log: &Log) -> Result<Option<IndexedMessage>, ChainError> {
        // Get transaction hash
        let tx_hash = log.transaction_hash
            .map(|h| format!("{:?}", h))
            .unwrap_or_default();
        
        let block_number = log.block_number
            .map(|n| n.as_u64())
            .unwrap_or(0);
        
        // Check which event type this is
        if log.topics.is_empty() {
            return Ok(None);
        }
        
        let event_sig = log.topics[0];
        
        // Parse based on event type
        // For now, we'll implement basic parsing
        // Full implementation would decode the ABI-encoded data
        
        debug!("Found log at block {}: tx={}", block_number, tx_hash);
        
        // TODO: Implement full ABI decoding
        // For now, return None (would need proper ABI parsing)
        Ok(None)
    }
    
    /// Decode message data from log
    fn decode_message_data(&self, _data: &[u8]) -> Result<Message, ChainError> {
        // TODO: Implement proper ABI decoding
        Err(ChainError::Parse("ABI decoding not yet implemented".to_string()))
    }
}

#[async_trait]
impl ChainIndexer for EthereumIndexer {
    fn chain(&self) -> Chain {
        Chain::ETH
    }
    
    async fn get_block_height(&self) -> Result<u64, ChainError> {
        let block = self.provider
            .get_block_number()
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        Ok(block.as_u64())
    }
    
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        info!("Indexing Ethereum blocks {} to {}", start, end);
        
        // Create filter for Aleph contract events
        let filter = Filter::new()
            .address(self.contract_address)
            .from_block(start)
            .to_block(end);
        
        // Get logs
        let logs = self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        debug!("Found {} logs in blocks {} to {}", logs.len(), start, end);
        
        // Parse logs into messages
        let mut messages = Vec::new();
        for log in &logs {
            match self.parse_log(log) {
                Ok(Some(msg)) => messages.push(msg),
                Ok(None) => {}
                Err(e) => warn!("Failed to parse log: {}", e),
            }
        }
        
        Ok(IndexResult {
            messages,
            last_block: end,
        })
    }
    
    async fn watch(&self) -> Result<(), ChainError> {
        info!("Starting Ethereum block watcher");
        
        let mut last_block = self.get_block_height().await?;
        
        loop {
            // Sleep for ~12 seconds (Ethereum block time)
            tokio::time::sleep(tokio::time::Duration::from_secs(12)).await;
            
            let current_block = self.get_block_height().await?;
            
            if current_block > last_block {
                // Index new blocks
                let result = self.index_blocks(last_block + 1, current_block).await?;
                
                if !result.messages.is_empty() {
                    info!(
                        "Found {} messages in blocks {} to {}",
                        result.messages.len(),
                        last_block + 1,
                        current_block
                    );
                }
                
                last_block = current_block;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_indexer_creation() {
        let config = EthereumConfig {
            rpc_url: "https://eth-mainnet.g.alchemy.com/v2/demo".to_string(),
            contract_address: "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59".to_string(),
            chain_id: 1,
            enabled: true,
            start_block: 10000000,
        };
        
        // This will fail without a valid RPC endpoint, but tests the structure
        let result = EthereumIndexer::new(&config).await;
        // We expect this to succeed since we're not making any calls yet
        assert!(result.is_ok());
    }
}
