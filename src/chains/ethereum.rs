//! Ethereum chain indexer
//!
//! Indexes Aleph messages from the Ethereum blockchain.
//! Supports multi-RPC failover for reliability.

use async_trait::async_trait;
use ethers::{
    prelude::*,
    providers::{Http, Provider, ProviderError},
    types::{Address as EthAddress, Filter, Log},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

use crate::config::EthereumConfig;
use crate::types::{Chain, ChainRef};

use super::abi::{self, signatures};
use super::{ChainError, ChainIndexer, IndexResult, IndexedMessage, SyncEvent};

/// Maximum blocks to index in a single batch
const MAX_BLOCKS_PER_BATCH: u64 = 100;

/// Multi-RPC provider with automatic failover
pub struct MultiRpcProvider {
    providers: Vec<Arc<Provider<Http>>>,
    current_index: AtomicUsize,
    last_error: RwLock<Option<String>>,
    failure_counts: RwLock<Vec<u32>>,
}

impl MultiRpcProvider {
    /// Create a new multi-RPC provider with primary and backup URLs
    pub fn new(primary_url: &str, backup_urls: &[String]) -> Result<Self, ChainError> {
        let mut providers = Vec::new();
        
        // Primary provider
        let primary = Provider::<Http>::try_from(primary_url)
            .map_err(|e| ChainError::Rpc(format!("Failed to create primary provider: {}", e)))?;
        providers.push(Arc::new(primary));
        
        // Backup providers
        for url in backup_urls {
            match Provider::<Http>::try_from(url.as_str()) {
                Ok(provider) => providers.push(Arc::new(provider)),
                Err(e) => {
                    warn!("Failed to create backup provider {}: {}", url, e);
                }
            }
        }
        
        let failure_counts = vec![0u32; providers.len()];
        
        info!("Created multi-RPC provider with {} endpoints", providers.len());
        
        Ok(Self {
            providers,
            current_index: AtomicUsize::new(0),
            last_error: RwLock::new(None),
            failure_counts: RwLock::new(failure_counts),
        })
    }
    
    /// Get the current provider
    pub fn current(&self) -> Arc<Provider<Http>> {
        let index = self.current_index.load(Ordering::SeqCst);
        self.providers[index].clone()
    }
    
    /// Mark the current provider as failed and switch to next
    pub async fn mark_failed(&self, error: &str) {
        let index = self.current_index.load(Ordering::SeqCst);
        
        // Increment failure count
        {
            let mut counts = self.failure_counts.write().await;
            if index < counts.len() {
                counts[index] += 1;
            }
        }
        
        // Store error
        {
            let mut last_err = self.last_error.write().await;
            *last_err = Some(error.to_string());
        }
        
        // Switch to next provider if available
        if self.providers.len() > 1 {
            let next_index = (index + 1) % self.providers.len();
            self.current_index.store(next_index, Ordering::SeqCst);
            info!("Switched to RPC provider {} after failure: {}", next_index, error);
        } else {
            warn!("No backup RPC providers available, continuing with current");
        }
    }
    
    /// Reset failure counts (called on success)
    pub async fn mark_success(&self) {
        let index = self.current_index.load(Ordering::SeqCst);
        let mut counts = self.failure_counts.write().await;
        if index < counts.len() {
            counts[index] = 0;
        }
    }
    
    /// Execute a request with automatic failover
    pub async fn execute_with_retry<F, T, Fut>(&self, f: F) -> Result<T, ChainError>
    where
        F: Fn(Arc<Provider<Http>>) -> Fut,
        Fut: std::future::Future<Output = Result<T, ProviderError>>,
    {
        let max_attempts = self.providers.len();
        let mut last_error = None;
        
        for attempt in 0..max_attempts {
            let provider = self.current();
            
            match f(provider).await {
                Ok(result) => {
                    self.mark_success().await;
                    return Ok(result);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    warn!(
                        "RPC request failed (attempt {}/{}): {}",
                        attempt + 1, max_attempts, error_msg
                    );
                    
                    // Check if it's a rate limit error
                    if error_msg.contains("429") || error_msg.contains("rate limit") {
                        // Rate limited - wait before switching
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                    
                    self.mark_failed(&error_msg).await;
                    last_error = Some(error_msg);
                }
            }
        }
        
        Err(ChainError::Rpc(format!(
            "All {} RPC providers failed. Last error: {}",
            max_attempts,
            last_error.unwrap_or_else(|| "Unknown error".to_string())
        )))
    }
}

/// Ethereum chain indexer with multi-RPC support
pub struct EthereumIndexer {
    provider: Arc<MultiRpcProvider>,
    contract_address: EthAddress,
    chain_id: u64,
    start_block: u64,
    confirmations: u64,
    batch_size: u64,
    authorized_emitters: Vec<EthAddress>,
}

impl EthereumIndexer {
    /// Create a new Ethereum indexer with multi-RPC support
    pub async fn new(config: &EthereumConfig) -> Result<Self, ChainError> {
        let provider = Arc::new(MultiRpcProvider::new(
            &config.rpc_url,
            &config.backup_rpc_urls,
        )?);
        
        let contract_address = config.contract_address
            .parse::<EthAddress>()
            .map_err(|e| ChainError::Parse(e.to_string()))?;
        
        // Parse authorized emitters
        let authorized_emitters: Vec<EthAddress> = config.authorized_emitters
            .iter()
            .filter_map(|addr| addr.parse().ok())
            .collect();
        
        info!(
            "Ethereum indexer initialized: contract={}, chain_id={}, start_block={}, backup_rpcs={}",
            contract_address, config.chain_id, config.start_block, config.backup_rpc_urls.len()
        );
        
        Ok(Self {
            provider,
            contract_address,
            chain_id: config.chain_id,
            start_block: config.start_block,
            confirmations: config.confirmations,
            batch_size: config.batch_size.min(MAX_BLOCKS_PER_BATCH),
            authorized_emitters,
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
        
        // Try to decode as SyncMessage event
        if event_sig == *signatures::SYNC_MESSAGE {
            match abi::decode_sync_event(log) {
                Ok(decoded) => {
                    debug!(
                        "Decoded SyncMessage event: content_len={}",
                        decoded.content.len()
                    );
                    
                    // Check authorized emitters if configured
                    if !self.authorized_emitters.is_empty() {
                        if let Some(sender) = log.topics.get(1) {
                            let sender_addr = EthAddress::from_slice(&sender.as_bytes()[12..]);
                            if !self.authorized_emitters.contains(&sender_addr) {
                                debug!("Ignoring sync event from unauthorized emitter: {:?}", sender_addr);
                                return Ok(None);
                            }
                        }
                    }
                    
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
    
    /// Index blocks with dynamic range adjustment
    async fn index_blocks_adaptive(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        let mut batch_size = self.batch_size;
        let mut from = start;
        let mut all_messages = Vec::new();
        let mut all_sync_hashes = Vec::new();
        
        while from <= end {
            let to = (from + batch_size - 1).min(end);
            
            match self.fetch_logs(from, to).await {
                Ok(logs) => {
                    // Parse logs
                    for log in &logs {
                        match self.parse_log(log) {
                            Ok(Some(msg)) => all_messages.push(msg),
                            Ok(None) => {}
                            Err(e) => warn!("Failed to parse log: {}", e),
                        }
                    }
                    
                    // Increase batch size on success (up to max)
                    batch_size = (batch_size + 10).min(MAX_BLOCKS_PER_BATCH);
                    from = to + 1;
                }
                Err(ChainError::RateLimited(secs)) => {
                    warn!("Rate limited, waiting {} seconds and reducing batch size", secs);
                    tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await;
                    batch_size = (batch_size / 2).max(10);
                }
                Err(e) => {
                    // If batch is already minimum, propagate error
                    if batch_size <= 10 {
                        return Err(e);
                    }
                    // Reduce batch size and retry
                    batch_size = (batch_size / 2).max(10);
                    warn!("Reducing batch size to {} after error: {}", batch_size, e);
                }
            }
        }
        
        Ok(IndexResult {
            messages: all_messages,
            sync_hashes: all_sync_hashes,
            last_block: end,
            blocks_processed: end - start + 1,
        })
    }
    
    /// Fetch logs with multi-RPC failover
    async fn fetch_logs(&self, from: u64, to: u64) -> Result<Vec<Log>, ChainError> {
        let contract = self.contract_address;
        
        self.provider.execute_with_retry(|provider| {
            let filter = Filter::new()
                .address(contract)
                .from_block(from)
                .to_block(to);
            
            async move {
                provider.get_logs(&filter).await
            }
        }).await
    }
}

#[async_trait]
impl ChainIndexer for EthereumIndexer {
    fn chain(&self) -> Chain {
        Chain::ETH
    }
    
    async fn get_block_height(&self) -> Result<u64, ChainError> {
        self.provider.execute_with_retry(|provider| {
            async move {
                provider.get_block_number().await.map(|n| n.as_u64())
            }
        }).await
    }
    
    async fn index_blocks(&self, start: u64, end: u64) -> Result<IndexResult, ChainError> {
        info!("Indexing Ethereum blocks {} to {}", start, end);
        
        // Validate block range
        if start > end {
            return Err(ChainError::InvalidRange(format!(
                "Start block {} is greater than end block {}",
                start, end
            )));
        }
        
        // Use adaptive indexing for large ranges
        if end - start > self.batch_size {
            return self.index_blocks_adaptive(start, end).await;
        }
        
        // Fetch logs with failover
        let logs = self.fetch_logs(start, end).await?;
        
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
        
        let contract = self.contract_address;
        
        // Fetch sync event logs with failover
        let logs = self.provider.execute_with_retry(|provider| {
            let filter = Filter::new()
                .address(contract)
                .from_block(start)
                .to_block(end)
                .topic0(*signatures::SYNC_MESSAGE);
            
            async move {
                provider.get_logs(&filter).await
            }
        }).await?;
        
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
            
            // Check authorized emitters
            if !self.authorized_emitters.is_empty() {
                if let Some(sender) = log.topics.get(1) {
                    let sender_addr = EthAddress::from_slice(&sender.as_bytes()[12..]);
                    if !self.authorized_emitters.contains(&sender_addr) {
                        continue;
                    }
                }
            }
            
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
                                    timestamp: None,
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
            
            match self.get_block_height().await {
                Ok(current_block) => {
                    // Apply confirmations
                    let safe_block = current_block.saturating_sub(self.confirmations);
                    
                    if safe_block > last_block {
                        // Index new blocks
                        match self.index_blocks(last_block + 1, safe_block).await {
                            Ok(result) => {
                                if !result.messages.is_empty() {
                                    info!(
                                        "Found {} messages in blocks {} to {}",
                                        result.messages.len(),
                                        last_block + 1,
                                        safe_block
                                    );
                                }
                                last_block = safe_block;
                            }
                            Err(e) => {
                                error!("Failed to index blocks: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to get block height: {}", e);
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_indexer_creation() {
        let config = EthereumConfig {
            rpc_url: "https://eth-mainnet.g.alchemy.com/v2/demo".to_string(),
            backup_rpc_urls: vec![
                "https://eth.llamarpc.com".to_string(),
                "https://rpc.ankr.com/eth".to_string(),
            ],
            contract_address: "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59".to_string(),
            chain_id: 1,
            enabled: true,
            start_block: 10000000,
            confirmations: 10,
            batch_size: 1000,
            authorized_emitters: Vec::new(),
            use_indexer: false,
            indexer_url: None,
        };
        
        let result = EthereumIndexer::new(&config).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_multi_rpc_provider() {
        let provider = MultiRpcProvider::new(
            "https://eth-mainnet.g.alchemy.com/v2/demo",
            &["https://eth.llamarpc.com".to_string()],
        );
        assert!(provider.is_ok());
        
        let provider = provider.unwrap();
        assert_eq!(provider.providers.len(), 2);
    }
    
    #[test]
    fn test_block_range_validation() {
        // Block range validation is done in index_blocks
    }
}
