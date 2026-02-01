//! Avalanche C-Chain indexer
//!
//! Uses the same contract ABI as Ethereum since Avalanche is EVM-compatible.

use async_trait::async_trait;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    types::{Address as EthAddress, Filter, Log},
};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::config::AvalancheConfig;
use crate::types::{Chain, ChainRef, Message};

use super::abi::{self, signatures};
use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage, SyncEvent};

/// Avalanche C-Chain indexer
pub struct AvalancheIndexer {
    provider: Arc<Provider<Http>>,
    contract_address: EthAddress,
    chain_id: u64,
    start_block: u64,
    confirmations: u64,
    batch_size: u64,
}

impl AvalancheIndexer {
    /// Create a new Avalanche indexer
    pub async fn new(config: &AvalancheConfig) -> Result<Self, ChainError> {
        let provider = Provider::<Http>::try_from(&config.rpc_url)
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        let contract_address = config.contract_address
            .parse::<EthAddress>()
            .map_err(|e| ChainError::Parse(e.to_string()))?;
        
        info!(
            "Avalanche indexer initialized: contract={}, chain_id={}",
            contract_address, config.chain_id
        );
        
        Ok(Self {
            provider: Arc::new(provider),
            contract_address,
            chain_id: config.chain_id,
            start_block: config.start_block,
            confirmations: config.confirmations,
            batch_size: 2000, // Avalanche has higher throughput
        })
    }
    
    fn parse_log(&self, log: &Log) -> Result<Option<IndexedMessage>, ChainError> {
        let tx_hash = log.transaction_hash
            .map(|h| format!("{:?}", h))
            .unwrap_or_default();
        
        let block_number = log.block_number
            .map(|n| n.as_u64())
            .unwrap_or(0);
        
        if log.topics.is_empty() {
            return Ok(None);
        }
        
        let event_sig = log.topics[0];
        
        if event_sig == *signatures::MESSAGE {
            match abi::decode_message_event(log) {
                Ok(decoded) => {
                    match abi::parse_message_content(
                        &decoded.content,
                        Chain::AVAX,
                        &tx_hash,
                        block_number,
                    ) {
                        Ok(message) => {
                            return Ok(Some(IndexedMessage {
                                message,
                                chain_ref: ChainRef {
                                    chain: Chain::AVAX,
                                    height: block_number,
                                    hash: tx_hash,
                                },
                                tx_hash: log.transaction_hash
                                    .map(|h| format!("{:?}", h))
                                    .unwrap_or_default(),
                                block_time: None,
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
        
        Ok(None)
    }
}

#[async_trait]
impl ChainIndexer for AvalancheIndexer {
    fn chain(&self) -> Chain {
        Chain::AVAX
    }
    
    async fn get_block_height(&self) -> Result<u64, ChainError> {
        let block = self.provider
            .get_block_number()
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        Ok(block.as_u64())
    }
    
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        let filter = Filter::new()
            .address(self.contract_address)
            .from_block(start)
            .to_block(end);
        
        let logs = self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        let mut messages = Vec::new();
        for log in &logs {
            if let Ok(Some(msg)) = self.parse_log(log) {
                messages.push(msg);
            }
        }
        
        Ok(IndexResult {
            messages,
            sync_hashes: Vec::new(),
            last_block: end,
            blocks_processed: end - start + 1,
        })
    }
    
    async fn index_sync_events(&self, start: u64, end: u64) -> Result<Vec<SyncEvent>, ChainError> {
        let filter = Filter::new()
            .address(self.contract_address)
            .from_block(start)
            .to_block(end)
            .topic0(*signatures::SYNC_MESSAGE);
        
        let logs = self.provider
            .get_logs(&filter)
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        let mut events = Vec::new();
        for log in logs {
            if let Ok(decoded) = abi::decode_sync_event(&log) {
                if let Ok(hashes) = abi::parse_sync_content(&decoded.content) {
                    for hash in hashes {
                        events.push(SyncEvent {
                            content_hash: hash,
                            block_height: log.block_number.map(|n| n.as_u64()).unwrap_or(0),
                            tx_hash: log.transaction_hash.map(|h| format!("{:?}", h)).unwrap_or_default(),
                            emitter: format!("0x{}", hex::encode(&log.topics.get(1).map(|t| &t.as_bytes()[12..]).unwrap_or(&[]))),
                            timestamp: None,
                        });
                    }
                }
            }
        }
        
        Ok(events)
    }
    
    async fn watch(&self) -> Result<(), ChainError> {
        info!("Starting Avalanche block watcher");
        
        let mut last_block = self.get_block_height().await?;
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // 2s block time
            
            let current_block = self.get_block_height().await?;
            
            if current_block > last_block {
                let result = self.index_blocks(last_block + 1, current_block).await?;
                
                if !result.messages.is_empty() {
                    info!(
                        "AVAX: Found {} messages in blocks {} to {}",
                        result.messages.len(),
                        last_block + 1,
                        current_block
                    );
                }
                
                last_block = current_block;
            }
        }
    }
    
    fn confirmations_required(&self) -> u64 {
        self.confirmations
    }
    
    fn batch_size(&self) -> u64 {
        self.batch_size
    }
}
