//! Metrics service
//!
//! Tracks node metrics for monitoring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use serde::Serialize;

/// Node metrics
pub struct Metrics {
    // Message counts
    pub messages_received: AtomicU64,
    pub messages_processed: AtomicU64,
    pub messages_rejected: AtomicU64,
    
    // API metrics
    pub api_requests: AtomicU64,
    pub api_errors: AtomicU64,
    
    // Chain sync
    pub blocks_indexed: AtomicU64,
    pub last_block_eth: AtomicU64,
    
    // P2P
    pub peers_connected: AtomicU64,
    pub p2p_messages_sent: AtomicU64,
    pub p2p_messages_received: AtomicU64,
    
    // Storage
    pub files_stored: AtomicU64,
    pub storage_bytes: AtomicU64,
    
    // Timing
    start_time: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_processed: AtomicU64::new(0),
            messages_rejected: AtomicU64::new(0),
            api_requests: AtomicU64::new(0),
            api_errors: AtomicU64::new(0),
            blocks_indexed: AtomicU64::new(0),
            last_block_eth: AtomicU64::new(0),
            peers_connected: AtomicU64::new(0),
            p2p_messages_sent: AtomicU64::new(0),
            p2p_messages_received: AtomicU64::new(0),
            files_stored: AtomicU64::new(0),
            storage_bytes: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
    
    /// Increment messages received
    pub fn inc_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment messages processed
    pub fn inc_messages_processed(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment messages rejected
    pub fn inc_messages_rejected(&self) {
        self.messages_rejected.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment API requests
    pub fn inc_api_requests(&self) {
        self.api_requests.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Increment API errors
    pub fn inc_api_errors(&self) {
        self.api_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Update last Ethereum block
    pub fn set_last_block_eth(&self, block: u64) {
        self.last_block_eth.store(block, Ordering::Relaxed);
    }
    
    /// Increment blocks indexed
    pub fn inc_blocks_indexed(&self, count: u64) {
        self.blocks_indexed.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Set peer count
    pub fn set_peers(&self, count: u64) {
        self.peers_connected.store(count, Ordering::Relaxed);
    }
    
    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
    
    /// Get snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: self.uptime_secs(),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            messages_rejected: self.messages_rejected.load(Ordering::Relaxed),
            api_requests: self.api_requests.load(Ordering::Relaxed),
            api_errors: self.api_errors.load(Ordering::Relaxed),
            blocks_indexed: self.blocks_indexed.load(Ordering::Relaxed),
            last_block_eth: self.last_block_eth.load(Ordering::Relaxed),
            peers_connected: self.peers_connected.load(Ordering::Relaxed),
            p2p_messages_sent: self.p2p_messages_sent.load(Ordering::Relaxed),
            p2p_messages_received: self.p2p_messages_received.load(Ordering::Relaxed),
            files_stored: self.files_stored.load(Ordering::Relaxed),
            storage_bytes: self.storage_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Serializable metrics snapshot
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub messages_received: u64,
    pub messages_processed: u64,
    pub messages_rejected: u64,
    pub api_requests: u64,
    pub api_errors: u64,
    pub blocks_indexed: u64,
    pub last_block_eth: u64,
    pub peers_connected: u64,
    pub p2p_messages_sent: u64,
    pub p2p_messages_received: u64,
    pub files_stored: u64,
    pub storage_bytes: u64,
}

/// Health check result
#[derive(Debug, Clone, Serialize)]
pub struct HealthCheck {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub database: bool,
    pub ipfs: bool,
    pub p2p: bool,
    pub chains: ChainsHealth,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainsHealth {
    pub ethereum: ChainHealth,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainHealth {
    pub enabled: bool,
    pub synced: bool,
    pub last_block: u64,
}
