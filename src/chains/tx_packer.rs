//! Chain TX Packer
//!
//! Packages pending messages into blockchain transactions for on-chain confirmation.
//! This implements the sync message packing used by pyaleph to batch messages.
//!
//! Reference: aleph/chains/chain_data_service.py

use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn, error};
use sqlx::PgPool;
use ethers::{
    prelude::*,
    providers::{Http, Provider},
    types::Address as EthAddress,
    signers::{LocalWallet, Signer},
};

use crate::config::Config;
use crate::types::{Chain, Message};
use crate::services::ipfs::IpfsService;

/// Maximum messages per sync batch
const MAX_MESSAGES_PER_BATCH: usize = 100;

/// Minimum time between sync publishes (seconds)
const MIN_SYNC_INTERVAL: u64 = 300; // 5 minutes

/// Maximum pending messages before forcing sync
const FORCE_SYNC_THRESHOLD: usize = 1000;

/// Pending TX record
#[derive(Debug, Clone)]
pub struct PendingTx {
    pub item_hashes: Vec<String>,
    pub ipfs_hash: String,
    pub chain: Chain,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub tx_hash: Option<String>,
    pub status: TxStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TxStatus {
    Pending,
    Submitted,
    Confirmed,
    Failed,
}

/// TX Packer for publishing sync messages to chains
pub struct TxPacker {
    db: PgPool,
    ipfs: Arc<IpfsService>,
    config: Arc<Config>,
    ethereum_wallet: Option<LocalWallet>,
    ethereum_provider: Option<Arc<Provider<Http>>>,
}

impl TxPacker {
    /// Create a new TX packer
    pub async fn new(
        db: PgPool,
        ipfs: Arc<IpfsService>,
        config: Arc<Config>,
    ) -> Result<Self, TxPackerError> {
        let mut packer = Self {
            db,
            ipfs,
            config: config.clone(),
            ethereum_wallet: None,
            ethereum_provider: None,
        };
        
        // Initialize Ethereum signer if configured
        if let Some(eth_config) = &config.chains.ethereum {
            if eth_config.enabled {
                let provider = Provider::<Http>::try_from(&eth_config.rpc_url)
                    .map_err(|e| TxPackerError::Connection(e.to_string()))?;
                packer.ethereum_provider = Some(Arc::new(provider));
                
                // Load wallet from environment or config
                if let Ok(key) = std::env::var("ALEPH_SYNC_PRIVATE_KEY") {
                    let wallet = key.parse::<LocalWallet>()
                        .map_err(|e| TxPackerError::Config(e.to_string()))?
                        .with_chain_id(eth_config.chain_id);
                    packer.ethereum_wallet = Some(wallet);
                    info!("Ethereum sync wallet loaded");
                }
            }
        }
        
        Ok(packer)
    }
    
    /// Run the TX packer job
    pub async fn run(&self) {
        let mut ticker = interval(Duration::from_secs(60)); // Check every minute
        let mut last_sync: HashMap<Chain, chrono::DateTime<chrono::Utc>> = HashMap::new();
        
        info!("TX Packer started");
        
        loop {
            ticker.tick().await;
            
            // Get pending messages count
            let pending_count = self.get_pending_count().await.unwrap_or(0);
            
            if pending_count == 0 {
                continue;
            }
            
            // Check if we should sync
            let now = chrono::Utc::now();
            let chains = vec![Chain::ETH]; // Add more chains as configured
            
            for chain in chains {
                let should_sync = match last_sync.get(&chain) {
                    Some(last) => {
                        let elapsed = (now - *last).num_seconds() as u64;
                        elapsed >= MIN_SYNC_INTERVAL || pending_count >= FORCE_SYNC_THRESHOLD
                    }
                    None => pending_count >= 10, // Initial threshold
                };
                
                if should_sync {
                    match self.sync_chain(chain).await {
                        Ok(tx_hash) => {
                            info!("{}: Sync TX submitted: {}", chain, tx_hash.as_deref().unwrap_or("batched"));
                            last_sync.insert(chain, now);
                        }
                        Err(e) => {
                            error!("{}: Sync failed: {}", chain, e);
                        }
                    }
                }
            }
        }
    }
    
    /// Get count of pending confirmed messages
    async fn get_pending_count(&self) -> Result<usize, TxPackerError> {
        let result: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM messages m
            WHERE NOT EXISTS (
                SELECT 1 FROM chain_txs ct WHERE ct.item_hash = m.item_hash
            )
            "#
        )
        .fetch_one(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        Ok(result.0 as usize)
    }
    
    /// Sync messages to a specific chain
    async fn sync_chain(&self, chain: Chain) -> Result<Option<String>, TxPackerError> {
        // Get unconfirmed messages
        let messages: Vec<(String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT m.item_hash, row_to_json(m.*) 
            FROM messages m
            WHERE NOT EXISTS (
                SELECT 1 FROM chain_txs ct 
                WHERE ct.item_hash = m.item_hash AND ct.chain = $1
            )
            ORDER BY m.time ASC
            LIMIT $2
            "#
        )
        .bind(chain.to_string())
        .bind(MAX_MESSAGES_PER_BATCH as i64)
        .fetch_all(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        if messages.is_empty() {
            return Ok(None);
        }
        
        let item_hashes: Vec<String> = messages.iter().map(|(h, _)| h.clone()).collect();
        
        info!("{}: Syncing {} messages", chain, item_hashes.len());
        
        // Create sync content (list of IPFS hashes)
        let sync_content = serde_json::json!(item_hashes);
        let content_bytes = sync_content.to_string().into_bytes();
        
        // Upload to IPFS
        let ipfs_hash = self.ipfs.add(content_bytes).await
            .map_err(|e| TxPackerError::Ipfs(e.to_string()))?;
        
        info!("{}: Sync content uploaded to IPFS: {}", chain, ipfs_hash);
        
        // Record pending TX
        let pending = PendingTx {
            item_hashes: item_hashes.clone(),
            ipfs_hash: ipfs_hash.clone(),
            chain,
            created_at: chrono::Utc::now(),
            tx_hash: None,
            status: TxStatus::Pending,
        };
        
        self.store_pending_tx(&pending).await?;
        
        // Submit to chain based on type
        let tx_hash = match chain {
            Chain::ETH => self.submit_ethereum_sync(&ipfs_hash).await?,
            Chain::AVAX => self.submit_evm_sync(chain, &ipfs_hash).await?,
            Chain::BSC => self.submit_evm_sync(chain, &ipfs_hash).await?,
            _ => {
                warn!("{}: Chain sync not implemented", chain);
                None
            }
        };
        
        // Update pending TX with hash
        if let Some(ref hash) = tx_hash {
            self.update_pending_tx_hash(&ipfs_hash, hash).await?;
        }
        
        Ok(tx_hash)
    }
    
    /// Submit sync message to Ethereum
    async fn submit_ethereum_sync(&self, ipfs_hash: &str) -> Result<Option<String>, TxPackerError> {
        let wallet = self.ethereum_wallet.as_ref()
            .ok_or_else(|| TxPackerError::Config("No Ethereum wallet configured".to_string()))?;
        
        let provider = self.ethereum_provider.as_ref()
            .ok_or_else(|| TxPackerError::Config("No Ethereum provider".to_string()))?;
        
        let eth_config = self.config.chains.ethereum.as_ref()
            .ok_or_else(|| TxPackerError::Config("No Ethereum config".to_string()))?;
        
        let contract_address: EthAddress = eth_config.contract_address.parse()
            .map_err(|e| TxPackerError::Config(format!("Invalid contract address: {}", e)))?;
        
        // Build sync message call data
        // syncMessage(bytes content) function selector: 0x...
        let content_bytes = ipfs_hash.as_bytes();
        let call_data = encode_sync_message(content_bytes);
        
        // Build transaction
        let client = SignerMiddleware::new(provider.clone(), wallet.clone());
        
        let tx = TransactionRequest::new()
            .to(contract_address)
            .data(call_data)
            .gas(100000u64);
        
        // Send transaction - extract tx hash immediately to avoid lifetime issues
        let result = {
            let pending = client.send_transaction(tx, None).await;
            match pending {
                Ok(p) => Ok(format!("{:?}", p.tx_hash())),
                Err(e) => Err(e.to_string()),
            }
        };
        
        match result {
            Ok(tx_hash) => {
                info!("ETH sync TX submitted: {}", tx_hash);
                Ok(Some(tx_hash))
            }
            Err(e) => {
                error!("ETH sync TX failed: {}", e);
                Err(TxPackerError::Chain(e))
            }
        }
    }
    
    /// Submit sync message to EVM-compatible chain
    async fn submit_evm_sync(&self, chain: Chain, ipfs_hash: &str) -> Result<Option<String>, TxPackerError> {
        // Similar to Ethereum but with chain-specific config
        // For now, return None as these need separate wallet config
        warn!("{}: EVM sync not fully implemented", chain);
        Ok(None)
    }
    
    /// Store pending TX in database
    async fn store_pending_tx(&self, pending: &PendingTx) -> Result<(), TxPackerError> {
        sqlx::query(
            r#"
            INSERT INTO pending_txs (ipfs_hash, chain, item_hashes, status, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (ipfs_hash) DO NOTHING
            "#
        )
        .bind(&pending.ipfs_hash)
        .bind(pending.chain.to_string())
        .bind(serde_json::to_value(&pending.item_hashes).unwrap())
        .bind("pending")
        .execute(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    /// Update pending TX with hash
    async fn update_pending_tx_hash(&self, ipfs_hash: &str, tx_hash: &str) -> Result<(), TxPackerError> {
        sqlx::query(
            "UPDATE pending_txs SET tx_hash = $1, status = 'submitted' WHERE ipfs_hash = $2"
        )
        .bind(tx_hash)
        .bind(ipfs_hash)
        .execute(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        Ok(())
    }
    
    /// Check and confirm pending TXs
    pub async fn check_confirmations(&self) -> Result<u32, TxPackerError> {
        // Get submitted TXs
        let pending: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT ipfs_hash, chain, tx_hash FROM pending_txs WHERE status = 'submitted' AND tx_hash IS NOT NULL"
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        let mut confirmed_count = 0u32;
        
        for (ipfs_hash, chain_str, tx_hash) in pending {
            let chain: Chain = chain_str.parse().unwrap_or(Chain::ETH);
            
            // Check if TX is confirmed
            let confirmed = match chain {
                Chain::ETH | Chain::AVAX | Chain::BSC => {
                    self.check_evm_confirmation(&tx_hash).await.unwrap_or(false)
                }
                _ => false,
            };
            
            if confirmed {
                // Update status and record chain_txs
                self.confirm_pending_tx(&ipfs_hash, &chain, &tx_hash).await?;
                confirmed_count += 1;
            }
        }
        
        Ok(confirmed_count)
    }
    
    /// Check if EVM transaction is confirmed
    async fn check_evm_confirmation(&self, tx_hash: &str) -> Result<bool, TxPackerError> {
        let provider = self.ethereum_provider.as_ref()
            .ok_or_else(|| TxPackerError::Config("No provider".to_string()))?;
        
        let hash: H256 = tx_hash.parse()
            .map_err(|e| TxPackerError::Parse(format!("Invalid tx hash: {}", e)))?;
        
        match provider.get_transaction_receipt(hash).await {
            Ok(Some(receipt)) => {
                Ok(receipt.status.map(|s| s.as_u64() == 1).unwrap_or(false))
            }
            Ok(None) => Ok(false),
            Err(e) => Err(TxPackerError::Chain(e.to_string())),
        }
    }
    
    /// Confirm a pending TX and record chain_txs
    async fn confirm_pending_tx(&self, ipfs_hash: &str, chain: &Chain, tx_hash: &str) -> Result<(), TxPackerError> {
        // Get item hashes from pending TX
        let result: (serde_json::Value,) = sqlx::query_as(
            "SELECT item_hashes FROM pending_txs WHERE ipfs_hash = $1"
        )
        .bind(ipfs_hash)
        .fetch_one(&self.db)
        .await
        .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        let item_hashes: Vec<String> = serde_json::from_value(result.0)
            .map_err(|e| TxPackerError::Parse(e.to_string()))?;
        
        let message_count = item_hashes.len();
        
        // Get TX block height
        let height = if let Some(provider) = &self.ethereum_provider {
            let hash: H256 = tx_hash.parse().unwrap_or_default();
            provider.get_transaction_receipt(hash)
                .await
                .ok()
                .flatten()
                .and_then(|r| r.block_number)
                .map(|n| n.as_u64())
                .unwrap_or(0)
        } else {
            0
        };
        
        // Insert chain_txs for each message
        for item_hash in item_hashes {
            sqlx::query(
                r#"
                INSERT INTO chain_txs (hash, chain, height, item_hash, protocol, created_at)
                VALUES ($1, $2, $3, $4, 'aleph', NOW())
                ON CONFLICT (hash) DO NOTHING
                "#
            )
            .bind(tx_hash)
            .bind(chain.to_string())
            .bind(height as i64)
            .bind(&item_hash)
            .execute(&self.db)
            .await
            .map_err(|e| TxPackerError::Database(e.to_string()))?;
        }
        
        // Update pending TX status
        sqlx::query("UPDATE pending_txs SET status = 'confirmed' WHERE ipfs_hash = $1")
            .bind(ipfs_hash)
            .execute(&self.db)
            .await
            .map_err(|e| TxPackerError::Database(e.to_string()))?;
        
        info!("{}: TX {} confirmed with {} messages", chain, tx_hash, message_count);
        
        Ok(())
    }
}

/// Encode syncMessage function call
fn encode_sync_message(content: &[u8]) -> Vec<u8> {
    use ethers::abi::{encode, Token};
    use sha3::{Digest, Keccak256};
    
    // Function selector: keccak256("syncMessage(bytes)")[:4]
    let selector = &Keccak256::digest(b"syncMessage(bytes)")[..4];
    
    // Encode parameters
    let encoded_params = encode(&[Token::Bytes(content.to_vec())]);
    
    // Combine selector + params
    let mut call_data = selector.to_vec();
    call_data.extend(encoded_params);
    
    call_data
}

/// TX Packer errors
#[derive(Debug, thiserror::Error)]
pub enum TxPackerError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("IPFS error: {0}")]
    Ipfs(String),
    
    #[error("Chain error: {0}")]
    Chain(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Parse error: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encode_sync_message() {
        let content = b"QmTest123";
        let encoded = encode_sync_message(content);
        
        // Should have 4-byte selector + encoded bytes
        assert!(encoded.len() > 4);
        
        // First 4 bytes are the selector
        let selector = &encoded[..4];
        assert_eq!(selector.len(), 4);
    }
}
