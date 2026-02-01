//! Tezos chain indexer
//!
//! Indexes Aleph messages from Tezos smart contract operations.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn, error};

use crate::config::TezosConfig;
use crate::types::{Chain, ChainRef, Message};

use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage, SyncEvent};

/// Tezos block header
#[derive(Debug, Deserialize)]
struct BlockHeader {
    level: u64,
    timestamp: String,
    hash: String,
}

/// Tezos operation
#[derive(Debug, Deserialize)]
struct Operation {
    hash: String,
    contents: Vec<OperationContent>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
#[serde(rename_all = "snake_case")]
enum OperationContent {
    Transaction {
        destination: String,
        parameters: Option<Parameters>,
        metadata: Option<OperationMetadata>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct Parameters {
    entrypoint: String,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct OperationMetadata {
    operation_result: Option<OperationResult>,
}

#[derive(Debug, Deserialize)]
struct OperationResult {
    status: String,
}

/// Tezos indexer
pub struct TezosIndexer {
    client: Client,
    rpc_url: String,
    contract_address: String,
    confirmations: u64,
}

impl TezosIndexer {
    /// Create a new Tezos indexer
    pub async fn new(config: &TezosConfig) -> Result<Self, ChainError> {
        if config.contract_address.is_empty() {
            return Err(ChainError::Parse("Tezos contract_address is required".to_string()));
        }
        
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| ChainError::Connection(e.to_string()))?;
        
        info!(
            "Tezos indexer initialized: contract={}, rpc={}",
            config.contract_address, config.rpc_url
        );
        
        Ok(Self {
            client,
            rpc_url: config.rpc_url.clone(),
            contract_address: config.contract_address.clone(),
            confirmations: config.confirmations,
        })
    }
    
    /// Get block header
    async fn get_block_header(&self, level: &str) -> Result<BlockHeader, ChainError> {
        let url = format!("{}/chains/main/blocks/{}/header", self.rpc_url, level);
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        if !response.status().is_success() {
            return Err(ChainError::Rpc(format!("HTTP {}", response.status())));
        }
        
        response.json().await.map_err(|e| ChainError::Parse(e.to_string()))
    }
    
    /// Get operations for a block
    async fn get_block_operations(&self, level: u64) -> Result<Vec<Operation>, ChainError> {
        let url = format!(
            "{}/chains/main/blocks/{}/operations/3", // 3 = manager operations
            self.rpc_url, level
        );
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        if !response.status().is_success() {
            return Err(ChainError::Rpc(format!("HTTP {}", response.status())));
        }
        
        response.json().await.map_err(|e| ChainError::Parse(e.to_string()))
    }
    
    /// Parse Aleph message from Tezos operation
    fn parse_operation(&self, op: &Operation, level: u64) -> Option<IndexedMessage> {
        for content in &op.contents {
            if let OperationContent::Transaction { destination, parameters, metadata } = content {
                // Check if this is our contract
                if destination != &self.contract_address {
                    continue;
                }
                
                // Check operation was successful
                if let Some(meta) = metadata {
                    if let Some(result) = &meta.operation_result {
                        if result.status != "applied" {
                            continue;
                        }
                    }
                }
                
                // Parse parameters
                if let Some(params) = parameters {
                    if params.entrypoint == "post_message" || params.entrypoint == "sync_message" {
                        // Try to extract message from value
                        if let Some(message) = self.parse_message_value(&params.value) {
                            return Some(IndexedMessage {
                                message,
                                chain_ref: ChainRef {
                                    chain: Chain::TEZOS,
                                    height: level,
                                    hash: op.hash.clone(),
                                },
                                tx_hash: op.hash.clone(),
                                block_time: None,
                            });
                        }
                    }
                }
            }
        }
        
        None
    }
    
    /// Parse Aleph message from Michelson value
    fn parse_message_value(&self, value: &serde_json::Value) -> Option<Message> {
        // Tezos contract stores messages in a specific format
        // This is a simplified parser - real implementation would need full Michelson parsing
        
        // Try to find string fields that might contain JSON message
        if let Some(bytes) = value.get("bytes").and_then(|v| v.as_str()) {
            // Decode hex bytes
            if let Ok(decoded) = hex::decode(bytes) {
                if let Ok(json_str) = String::from_utf8(decoded) {
                    if let Ok(message) = serde_json::from_str::<Message>(&json_str) {
                        return Some(message);
                    }
                }
            }
        }
        
        // Try string field
        if let Some(s) = value.get("string").and_then(|v| v.as_str()) {
            if let Ok(message) = serde_json::from_str::<Message>(s) {
                return Some(message);
            }
        }
        
        None
    }
}

#[async_trait]
impl ChainIndexer for TezosIndexer {
    fn chain(&self) -> Chain {
        Chain::TEZOS
    }
    
    async fn get_block_height(&self) -> Result<u64, ChainError> {
        let header = self.get_block_header("head").await?;
        Ok(header.level)
    }
    
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        let mut messages = Vec::new();
        
        for level in start..=end {
            let operations = match self.get_block_operations(level).await {
                Ok(ops) => ops,
                Err(e) => {
                    warn!("Failed to get operations for level {}: {}", level, e);
                    continue;
                }
            };
            
            for op in operations {
                if let Some(msg) = self.parse_operation(&op, level) {
                    messages.push(msg);
                }
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
        // Tezos sync events would be in contract operations
        let mut events = Vec::new();
        
        for level in start..=end {
            let operations = match self.get_block_operations(level).await {
                Ok(ops) => ops,
                Err(_) => continue,
            };
            
            for op in operations {
                for content in &op.contents {
                    if let OperationContent::Transaction { destination, parameters, .. } = content {
                        if destination != &self.contract_address {
                            continue;
                        }
                        
                        if let Some(params) = parameters {
                            if params.entrypoint == "sync_message" {
                                // Extract IPFS hash from sync message
                                if let Some(hash) = params.value.get("string").and_then(|v| v.as_str()) {
                                    events.push(SyncEvent {
                                        content_hash: hash.to_string(),
                                        block_height: level,
                                        tx_hash: op.hash.clone(),
                                        emitter: destination.clone(),
                                        timestamp: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(events)
    }
    
    async fn watch(&self) -> Result<(), ChainError> {
        info!("Starting Tezos block watcher");
        
        let mut last_level = self.get_block_height().await?;
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await; // ~30s block time
            
            let current_level = self.get_block_height().await?;
            
            if current_level > last_level {
                let result = self.index_blocks(last_level + 1, current_level).await?;
                
                if !result.messages.is_empty() {
                    info!(
                        "TEZOS: Found {} messages in levels {} to {}",
                        result.messages.len(),
                        last_level + 1,
                        current_level
                    );
                }
                
                last_level = current_level;
            }
        }
    }
    
    fn confirmations_required(&self) -> u64 {
        self.confirmations
    }
    
    fn batch_size(&self) -> u64 {
        100 // Tezos blocks are slower, smaller batches
    }
}
