//! Direct Ethereum RPC sync via eth_getLogs
//!
//! This module implements direct blockchain sync by querying SyncEvent logs
//! from the Aleph smart contract using Ethereum JSON-RPC (eth_getLogs).
//! This bypasses the multichain indexer API which rate-limits with HTTP 429.
//!
//! Flow:
//! 1. Call eth_getLogs with the contract address + SyncEvent topic
//! 2. ABI-decode each log: SyncEvent(uint256 timestamp, address addr, string message)
//! 3. Parse message JSON → get IPFS CID (content hash)
//! 4. Feed IPFS CIDs into the same fetch + insert pipeline as indexer sync
//!
//! Implements:
//! - Adaptive block range (halves on "too many results" errors)
//! - Multi-RPC failover via the existing MultiRpcProvider
//! - Block-based progress tracking (persisted to chain_sync_state)

use ethers::{
    abi::{decode, ParamType, Token},
    providers::{Http, Middleware, Provider},
    types::{Address as EthAddress, Filter, Log, H256, U64},
};
use sha3::{Digest, Keccak256};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::chains::ethereum::MultiRpcProvider;
use crate::chains::indexer::SyncMessageContent;
use crate::config::EthereumConfig;

/// SyncEvent(uint256,address,string) topic hash
fn sync_event_topic() -> H256 {
    let hash = Keccak256::digest(b"SyncEvent(uint256,address,string)");
    H256::from_slice(&hash)
}

/// Decoded SyncEvent from the Aleph contract
#[derive(Debug, Clone)]
pub struct DecodedAlephSyncEvent {
    /// Timestamp from the event
    pub timestamp: u64,
    /// Emitter address
    pub addr: String,
    /// Message JSON string (contains {protocol, version, content})
    pub message: String,
    /// Block number where this event was found
    pub block_number: u64,
    /// Transaction hash
    pub tx_hash: String,
}

/// RPC sync client for direct blockchain sync
pub struct RpcSyncClient {
    provider: Arc<MultiRpcProvider>,
    contract_address: EthAddress,
    /// Maximum block range per query (adaptive)
    max_block_range: u64,
    /// Starting block if no state exists
    start_block: u64,
    /// Authorized emitters (empty = accept all)
    authorized_emitters: Vec<EthAddress>,
}

/// Errors during RPC sync
#[derive(Debug, thiserror::Error)]
pub enum RpcSyncError {
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Too many logs in range {start}..{end}")]
    TooManyLogs { start: u64, end: u64 },
    #[error("ABI decode error: {0}")]
    AbiDecode(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("All RPCs failed: {0}")]
    AllFailed(String),
}

impl RpcSyncClient {
    /// Create a new RPC sync client from config
    pub fn new(config: &EthereumConfig) -> Result<Self, RpcSyncError> {
        let provider = MultiRpcProvider::new(
            &config.rpc_url,
            &config.backup_rpc_urls,
        ).map_err(|e| RpcSyncError::Rpc(e.to_string()))?;

        let contract_address = config.contract_address
            .parse::<EthAddress>()
            .map_err(|e| RpcSyncError::Parse(e.to_string()))?;

        let authorized_emitters: Vec<EthAddress> = config.authorized_emitters
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .collect();

        // Default max range: 10000 blocks (~1.4 days of Ethereum)
        let max_block_range = config.batch_size.max(1000).min(50000);

        info!(
            "RPC sync client initialized: contract={:?}, max_range={}, rpcs={}",
            contract_address, max_block_range, config.backup_rpc_urls.len() + 1
        );

        Ok(Self {
            provider: Arc::new(provider),
            contract_address,
            max_block_range,
            start_block: config.start_block,
            authorized_emitters,
        })
    }

    /// Get current block number from the chain
    pub async fn get_block_number(&self) -> Result<u64, RpcSyncError> {
        self.provider.execute_with_retry(|provider| {
            async move {
                provider.get_block_number().await.map(|n| n.as_u64())
            }
        }).await.map_err(|e| RpcSyncError::Rpc(e.to_string()))
    }

    /// Get configured start block
    pub fn start_block(&self) -> u64 {
        self.start_block
    }

    /// Fetch logs in a specific block range with the SyncEvent topic
    async fn get_logs_in_range(&self, from: u64, to: u64) -> Result<Vec<Log>, RpcSyncError> {
        let contract = self.contract_address;
        let topic = sync_event_topic();

        self.provider.execute_with_retry(|provider| {
            let filter = Filter::new()
                .address(contract)
                .from_block(from)
                .to_block(to)
                .topic0(topic);

            async move {
                provider.get_logs(&filter).await
            }
        }).await.map_err(|e| {
            let err_str = e.to_string();
            // Detect "too many results" / "query returned more than X results" errors
            if err_str.contains("too many") 
                || err_str.contains("query returned more than")
                || err_str.contains("-32005")
                || err_str.contains("Log response size exceeded")
            {
                RpcSyncError::TooManyLogs { start: from, end: to }
            } else {
                RpcSyncError::Rpc(err_str)
            }
        })
    }

    /// Fetch all SyncEvent logs from start_block to latest, using adaptive batch sizing.
    /// Returns (events, last_processed_block).
    pub async fn fetch_sync_events(
        &self,
        start_block: u64,
    ) -> Result<(Vec<DecodedAlephSyncEvent>, u64), RpcSyncError> {
        let latest_block = self.get_block_number().await?;

        if start_block > latest_block {
            debug!("RPC sync: already at latest block {}", latest_block);
            return Ok((Vec::new(), latest_block));
        }

        let mut block_range = self.max_block_range;
        let mut current_start = start_block;
        let mut all_events = Vec::new();
        let mut last_block = start_block;

        info!(
            "RPC sync: fetching SyncEvents from block {} to {} (range: {})",
            start_block, latest_block, block_range
        );

        while current_start <= latest_block {
            let current_end = (current_start + block_range - 1).min(latest_block);

            match self.get_logs_in_range(current_start, current_end).await {
                Ok(logs) => {
                    let log_count = logs.len();

                    // Decode logs into SyncEvents
                    for log in &logs {
                        match self.decode_sync_event(log) {
                            Ok(Some(event)) => {
                                all_events.push(event);
                            }
                            Ok(None) => {
                                // Filtered out (unauthorized emitter)
                            }
                            Err(e) => {
                                debug!("Failed to decode log: {}", e);
                            }
                        }
                    }

                    if log_count > 0 {
                        debug!(
                            "RPC sync: blocks {}..{}: {} logs, {} sync events",
                            current_start, current_end, log_count, all_events.len()
                        );
                    }

                    last_block = current_end;
                    current_start = current_end + 1;

                    // On success, try to increase range back to max
                    if block_range < self.max_block_range {
                        block_range = (block_range * 2).min(self.max_block_range);
                    }
                }
                Err(RpcSyncError::TooManyLogs { start, end }) => {
                    // Halve the range
                    let old_range = block_range;
                    block_range = (block_range / 2).max(100);
                    warn!(
                        "RPC sync: too many logs in {}..{}, reducing range {} → {}",
                        start, end, old_range, block_range
                    );

                    if block_range < 100 {
                        error!("RPC sync: block range too small, skipping to next chunk");
                        current_start = current_end + 1;
                        block_range = 1000; // Reset
                    }
                    // Don't advance current_start, retry with smaller range
                }
                Err(e) => {
                    error!("RPC sync: error fetching blocks {}..{}: {}", current_start, current_end, e);
                    // Wait and retry
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    // Try with a smaller range
                    block_range = (block_range / 2).max(100);
                }
            }

            // Yield control periodically  
            if all_events.len() > 10000 {
                info!(
                    "RPC sync: batch limit reached with {} events at block {}",
                    all_events.len(), last_block
                );
                break;
            }
        }

        info!(
            "RPC sync: fetched {} sync events up to block {}",
            all_events.len(), last_block
        );

        Ok((all_events, last_block))
    }

    /// Decode a raw log into a DecodedAlephSyncEvent
    fn decode_sync_event(&self, log: &Log) -> Result<Option<DecodedAlephSyncEvent>, RpcSyncError> {
        // Verify the topic matches SyncEvent
        if log.topics.is_empty() || log.topics[0] != sync_event_topic() {
            return Ok(None);
        }

        let block_number = log.block_number
            .map(|n| n.as_u64())
            .unwrap_or(0);

        let tx_hash = log.transaction_hash
            .map(|h| format!("{:?}", h))
            .unwrap_or_default();

        // SyncEvent has NO indexed parameters, all data is in log.data
        // ABI: (uint256 timestamp, address addr, string message)
        let params = vec![
            ParamType::Uint(256),   // timestamp
            ParamType::Address,     // addr
            ParamType::String,      // message
        ];

        let tokens = decode(&params, &log.data)
            .map_err(|e| RpcSyncError::AbiDecode(format!(
                "Failed to decode SyncEvent data at block {}: {}", block_number, e
            )))?;

        let timestamp = match &tokens[0] {
            Token::Uint(v) => v.as_u64(),
            _ => return Err(RpcSyncError::AbiDecode("Expected uint256 for timestamp".into())),
        };

        let addr = match &tokens[1] {
            Token::Address(a) => format!("{:?}", a),
            _ => return Err(RpcSyncError::AbiDecode("Expected address for addr".into())),
        };

        let message = match &tokens[2] {
            Token::String(s) => s.clone(),
            _ => return Err(RpcSyncError::AbiDecode("Expected string for message".into())),
        };

        // Check authorized emitters
        if !self.authorized_emitters.is_empty() {
            if let Ok(emitter_addr) = addr.parse::<EthAddress>() {
                if !self.authorized_emitters.contains(&emitter_addr) {
                    debug!("RPC sync: skipping event from unauthorized emitter {}", addr);
                    return Ok(None);
                }
            }
        }

        Ok(Some(DecodedAlephSyncEvent {
            timestamp,
            addr,
            message,
            block_number,
            tx_hash,
        }))
    }

    /// Extract IPFS CIDs from decoded sync events
    pub fn extract_ipfs_cids(events: &[DecodedAlephSyncEvent]) -> Vec<(String, u64)> {
        let mut cids = Vec::new();

        for event in events {
            match serde_json::from_str::<SyncMessageContent>(&event.message) {
                Ok(content) => {
                    cids.push((content.content, event.block_number));
                }
                Err(e) => {
                    debug!(
                        "RPC sync: failed to parse sync message at block {}: {} (message: {:.100})",
                        event.block_number, e, event.message
                    );
                }
            }
        }

        cids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_event_topic() {
        let topic = sync_event_topic();
        // Should be a valid 32-byte hash
        assert_eq!(topic.as_bytes().len(), 32);
        // Verify it's the keccak256 of "SyncEvent(uint256,address,string)"
        let expected = Keccak256::digest(b"SyncEvent(uint256,address,string)");
        assert_eq!(topic.as_bytes(), expected.as_slice());
    }

    #[test]
    fn test_extract_ipfs_cids() {
        let events = vec![
            DecodedAlephSyncEvent {
                timestamp: 1234567890,
                addr: "0x1234".to_string(),
                message: r#"{"protocol":"aleph-offchain","version":1,"content":"QmU1YbveJuJvgHFB35qBuaMQfhpY19zpBv8hzxyZEj3s41"}"#.to_string(),
                block_number: 100,
                tx_hash: "0xabc".to_string(),
            },
        ];

        let cids = RpcSyncClient::extract_ipfs_cids(&events);
        assert_eq!(cids.len(), 1);
        assert_eq!(cids[0].0, "QmU1YbveJuJvgHFB35qBuaMQfhpY19zpBv8hzxyZEj3s41");
        assert_eq!(cids[0].1, 100);
    }
}
