//! Ethereum chain indexer
//!
//! Indexes Aleph messages from the Ethereum blockchain.

use async_trait::async_trait;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    types::{Address as EthAddress, Filter, Log},
};
use std::sync::Arc;
use tracing::{debug, info, warn, error};

use crate::config::EthereumConfig;
use crate::types::{Chain, ChainRef, Message};

use super::abi::{self, signatures};
use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage, SyncEvent};

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
        
        // Try to decode as Message event
        if event_sig == *signatures::MESSAGE {
            match abi::decode_message_event(log) {
                Ok(decoded) => {
                    debug!(
                        "Decoded Message event: sender={}, type={}, content_len={}",
                        decoded.sender, decoded.message_type, decoded.content.len()
                    );
                    
                    // Parse the message content
                    match abi::parse_message_content(
                        &decoded.content,
                        Chain::ETH,
                        &tx_hash,
                        block_number,
                    ) {
                        Ok(message) => {
                            return Ok(Some(IndexedMessage {
                                message,
                                chain_ref: ChainRef {
                                    chain: Chain::ETH,
                                    height: block_number,
                                    hash: tx_hash.clone(),
                                },
                                tx_hash: log.transaction_hash
                                    .map(|h| format!("{:?}", h))
                                    .unwrap_or_default(),
                                block_time: None, // Block time would need to be fetched separately
                            }));
                        }
                        Err(e) => {
                            warn!("Failed to parse message content: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to decode Message event: {}", e);
                }
            }
        }
        
        // Try to decode as SyncMessage event
        if event_sig == *signatures::SYNC_MESSAGE {
            match abi::decode_sync_event(log) {
                Ok(decoded) => {
                    debug!(
                        "Decoded SyncMessage event: content_len={}",
                        decoded.content.len()
                    );
                    
                    // Sync messages contain IPFS hashes to fetch
                    match abi::parse_sync_content(&decoded.content) {
                        Ok(hashes) => {
                            debug!("Sync message contains {} hashes to fetch", hashes.len());
                            // TODO: Queue these hashes for fetching from IPFS
                        }
                        Err(e) => {
                            warn!("Failed to parse sync content: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to decode SyncMessage event: {}", e);
                }
            }
        }
        
        Ok(None)
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
        
        info!("Indexed {} messages from blocks {} to {}", messages.len(), start, end);
        
        Ok(IndexResult {
            messages,
            sync_hashes: Vec::new(),
            last_block: end,
            blocks_processed: end - start + 1,
        })
    }
    
    async fn index_sync_events(&self, start: u64, end: u64) -> Result<Vec<SyncEvent>, ChainError> {
        info!("Indexing sync events from Ethereum blocks {} to {}", start, end);
        
        // Create filter for SyncMessage events only
        let filter = Filter::new()
            .address(self.contract_address)
            .from_block(start)
            .to_block(end)
            .topic0(*signatures::SYNC_MESSAGE);
        
        // Get logs
        let logs = self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        debug!("Found {} sync event logs in blocks {} to {}", logs.len(), start, end);
        
        // Parse logs into sync events
        let mut events = Vec::new();
        for log in &logs {
            let tx_hash = log.transaction_hash
                .map(|h| format!("{:?}", h))
                .unwrap_or_default();
            
            let block_number = log.block_number
                .map(|n| n.as_u64())
                .unwrap_or(0);
            
            match abi::decode_sync_event(log) {
                Ok(decoded) => {
                    match abi::parse_sync_content(&decoded.content) {
                        Ok(hashes) => {
                            for hash in hashes {
                                events.push(SyncEvent {
                                    content_hash: hash,
                                    block_height: block_number,
                                    tx_hash: tx_hash.clone(),
                                    emitter: format!("{:?}", self.contract_address),
                                    timestamp: None, // Would need to fetch block timestamp
                                });
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse sync content: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to decode SyncMessage event: {}", e);
                }
            }
        }
        
        info!("Indexed {} sync events from blocks {} to {}", events.len(), start, end);
        Ok(events)
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
        
        // This will succeed since we're not making any calls yet
        let result = EthereumIndexer::new(&config).await;
        assert!(result.is_ok());
    }
}
