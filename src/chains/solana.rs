//! Solana chain indexer
//!
//! Indexes Aleph messages from Solana transaction logs.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn, error};

use crate::config::SolanaConfig;
use crate::types::{Chain, ChainRef, Message};

use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage, SyncEvent};

/// Solana RPC request
#[derive(Debug, Serialize)]
struct RpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: serde_json::Value,
}

/// Solana RPC response
#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// Slot info response
#[derive(Debug, Deserialize)]
struct SlotInfo {
    slot: u64,
}

/// Transaction response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionResponse {
    slot: u64,
    block_time: Option<i64>,
    transaction: TransactionData,
    meta: Option<TransactionMeta>,
}

#[derive(Debug, Deserialize)]
struct TransactionData {
    message: TransactionMessage,
    signatures: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionMessage {
    account_keys: Vec<String>,
    instructions: Vec<Instruction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Instruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TransactionMeta {
    log_messages: Option<Vec<String>>,
}

/// Signatures for address response
#[derive(Debug, Deserialize)]
struct SignatureInfo {
    signature: String,
    slot: u64,
    #[serde(rename = "blockTime")]
    block_time: Option<i64>,
}

/// Solana indexer
pub struct SolanaIndexer {
    client: Client,
    rpc_url: String,
    program_id: String,
    confirmations: u64,
}

impl SolanaIndexer {
    /// Create a new Solana indexer
    pub async fn new(config: &SolanaConfig) -> Result<Self, ChainError> {
        if config.program_id.is_empty() {
            return Err(ChainError::Parse("Solana program_id is required".to_string()));
        }
        
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| ChainError::Connection(e.to_string()))?;
        
        info!(
            "Solana indexer initialized: program={}, rpc={}",
            config.program_id, config.rpc_url
        );
        
        Ok(Self {
            client,
            rpc_url: config.rpc_url.clone(),
            program_id: config.program_id.clone(),
            confirmations: config.confirmations,
        })
    }
    
    /// Make an RPC call
    async fn rpc_call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ChainError> {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: method.to_string(),
            params,
        };
        
        let response = self.client
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| ChainError::Rpc(e.to_string()))?;
        
        let rpc_response: RpcResponse<T> = response
            .json()
            .await
            .map_err(|e| ChainError::Parse(e.to_string()))?;
        
        if let Some(error) = rpc_response.error {
            return Err(ChainError::Rpc(format!("{}: {}", error.code, error.message)));
        }
        
        rpc_response.result.ok_or_else(|| ChainError::Rpc("Empty response".to_string()))
    }
    
    /// Get signatures for the program
    async fn get_signatures(&self, before: Option<&str>, limit: usize) -> Result<Vec<SignatureInfo>, ChainError> {
        let mut config = serde_json::json!({
            "limit": limit,
            "commitment": "confirmed"
        });
        
        if let Some(sig) = before {
            config["before"] = serde_json::json!(sig);
        }
        
        self.rpc_call(
            "getSignaturesForAddress",
            serde_json::json!([self.program_id, config]),
        ).await
    }
    
    /// Get transaction details
    async fn get_transaction(&self, signature: &str) -> Result<Option<TransactionResponse>, ChainError> {
        let result: Result<TransactionResponse, _> = self.rpc_call(
            "getTransaction",
            serde_json::json!([signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}]),
        ).await;
        
        match result {
            Ok(tx) => Ok(Some(tx)),
            Err(ChainError::Rpc(msg)) if msg.contains("not found") => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    /// Parse Aleph message from transaction logs
    fn parse_transaction(&self, tx: &TransactionResponse) -> Option<IndexedMessage> {
        let logs = tx.meta.as_ref()?.log_messages.as_ref()?;
        
        // Look for Aleph message in logs
        for log in logs {
            // Aleph messages are logged as JSON
            if log.contains("aleph_message:") {
                let json_start = log.find("aleph_message:")? + "aleph_message:".len();
                let json_str = &log[json_start..].trim();
                
                if let Ok(message) = serde_json::from_str::<Message>(json_str) {
                    let signature = tx.transaction.signatures.first()?.clone();
                    
                    return Some(IndexedMessage {
                        message,
                        chain_ref: ChainRef {
                            chain: Chain::SOL,
                            height: tx.slot,
                            hash: signature.clone(),
                        },
                        tx_hash: signature,
                        block_time: tx.block_time.map(|t| t as u64),
                    });
                }
            }
        }
        
        None
    }
}

#[async_trait]
impl ChainIndexer for SolanaIndexer {
    fn chain(&self) -> Chain {
        Chain::SOL
    }
    
    async fn get_block_height(&self) -> Result<u64, ChainError> {
        let slot: u64 = self.rpc_call("getSlot", serde_json::json!([])).await?;
        Ok(slot)
    }
    
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        // Solana uses slot-based indexing
        // We need to get signatures and filter by slot range
        
        let signatures = self.get_signatures(None, 1000).await?;
        
        let mut messages = Vec::new();
        let mut processed_slots = 0u64;
        
        for sig_info in signatures {
            // Filter by slot range
            if sig_info.slot < start || sig_info.slot > end {
                continue;
            }
            
            processed_slots += 1;
            
            // Get full transaction
            if let Ok(Some(tx)) = self.get_transaction(&sig_info.signature).await {
                if let Some(msg) = self.parse_transaction(&tx) {
                    messages.push(msg);
                }
            }
        }
        
        Ok(IndexResult {
            messages,
            sync_hashes: Vec::new(),
            last_block: end,
            blocks_processed: processed_slots,
        })
    }
    
    async fn index_sync_events(&self, _start: u64, _end: u64) -> Result<Vec<SyncEvent>, ChainError> {
        // Solana sync events would be in transaction logs
        // This is a simplified implementation
        Ok(Vec::new())
    }
    
    async fn watch(&self) -> Result<(), ChainError> {
        info!("Starting Solana slot watcher");
        
        let mut last_slot = self.get_block_height().await?;
        let mut last_signature: Option<String> = None;
        
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(400)).await; // ~400ms slot time
            
            // Get new signatures
            let signatures = self.get_signatures(last_signature.as_deref(), 100).await?;
            
            for sig_info in signatures {
                if sig_info.slot <= last_slot {
                    continue;
                }
                
                if let Ok(Some(tx)) = self.get_transaction(&sig_info.signature).await {
                    if let Some(msg) = self.parse_transaction(&tx) {
                        info!(
                            "SOL: Found message {} in slot {}",
                            msg.message.item_hash, sig_info.slot
                        );
                    }
                }
                
                last_signature = Some(sig_info.signature);
                last_slot = sig_info.slot;
            }
        }
    }
    
    fn confirmations_required(&self) -> u64 {
        self.confirmations
    }
    
    fn batch_size(&self) -> u64 {
        100 // Solana has different performance characteristics
    }
}
