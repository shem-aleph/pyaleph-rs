//! Metrics service
//!
//! Tracks node metrics for monitoring with Prometheus-compatible output.
//! Reference: aleph/web/controllers/metrics.py

use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use serde::Serialize;
use tokio::sync::RwLock;

/// Node metrics with thread-safe counters
#[derive(Debug)]
pub struct Metrics {
    // Message counts
    pub messages_received: AtomicU64,
    pub messages_processed: AtomicU64,
    pub messages_rejected: AtomicU64,
    pub messages_pending: AtomicU64,
    pub messages_forgotten: AtomicU64,
    
    // Message type counts
    message_type_counts: RwLock<HashMap<String, u64>>,
    
    // API metrics
    pub api_requests_total: AtomicU64,
    pub api_errors_total: AtomicU64,
    pub api_request_duration_sum: AtomicU64, // microseconds
    pub api_request_duration_count: AtomicU64,
    
    // Endpoint-specific metrics
    endpoint_counts: RwLock<HashMap<String, u64>>,
    
    // Chain sync metrics
    pub blocks_indexed: AtomicU64,
    pub last_block_eth: AtomicU64,
    pub last_block_sol: AtomicU64,
    pub last_block_avax: AtomicU64,
    pub chain_sync_errors: AtomicU64,
    
    // P2P metrics
    pub peers_connected: AtomicU64,
    pub p2p_messages_sent: AtomicU64,
    pub p2p_messages_received: AtomicU64,
    pub p2p_bytes_sent: AtomicU64,
    pub p2p_bytes_received: AtomicU64,
    
    // Storage metrics
    pub files_stored: AtomicU64,
    pub storage_bytes_total: AtomicU64,
    pub ipfs_pins: AtomicU64,
    pub ipfs_fetch_success: AtomicU64,
    pub ipfs_fetch_failure: AtomicU64,
    
    // Database metrics
    pub db_connections_active: AtomicU64,
    pub db_connections_idle: AtomicU64,
    pub db_queries_total: AtomicU64,
    pub db_query_duration_sum: AtomicU64, // microseconds
    
    // Cache metrics
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_size: AtomicU64,
    
    // Balance metrics
    pub balance_updates: AtomicU64,
    pub credit_updates: AtomicU64,
    
    // Handler metrics
    handler_durations: RwLock<HashMap<String, (u64, u64)>>, // (sum_us, count)
    
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
            messages_pending: AtomicU64::new(0),
            messages_forgotten: AtomicU64::new(0),
            message_type_counts: RwLock::new(HashMap::new()),
            
            api_requests_total: AtomicU64::new(0),
            api_errors_total: AtomicU64::new(0),
            api_request_duration_sum: AtomicU64::new(0),
            api_request_duration_count: AtomicU64::new(0),
            endpoint_counts: RwLock::new(HashMap::new()),
            
            blocks_indexed: AtomicU64::new(0),
            last_block_eth: AtomicU64::new(0),
            last_block_sol: AtomicU64::new(0),
            last_block_avax: AtomicU64::new(0),
            chain_sync_errors: AtomicU64::new(0),
            
            peers_connected: AtomicU64::new(0),
            p2p_messages_sent: AtomicU64::new(0),
            p2p_messages_received: AtomicU64::new(0),
            p2p_bytes_sent: AtomicU64::new(0),
            p2p_bytes_received: AtomicU64::new(0),
            
            files_stored: AtomicU64::new(0),
            storage_bytes_total: AtomicU64::new(0),
            ipfs_pins: AtomicU64::new(0),
            ipfs_fetch_success: AtomicU64::new(0),
            ipfs_fetch_failure: AtomicU64::new(0),
            
            db_connections_active: AtomicU64::new(0),
            db_connections_idle: AtomicU64::new(0),
            db_queries_total: AtomicU64::new(0),
            db_query_duration_sum: AtomicU64::new(0),
            
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_size: AtomicU64::new(0),
            
            balance_updates: AtomicU64::new(0),
            credit_updates: AtomicU64::new(0),
            
            handler_durations: RwLock::new(HashMap::new()),
            
            start_time: Instant::now(),
        }
    }
    
    // ===== Message Metrics =====
    
    pub fn inc_messages_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_messages_processed(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_messages_rejected(&self) {
        self.messages_rejected.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn set_messages_pending(&self, count: u64) {
        self.messages_pending.store(count, Ordering::Relaxed);
    }
    
    pub fn inc_messages_forgotten(&self) {
        self.messages_forgotten.fetch_add(1, Ordering::Relaxed);
    }
    
    pub async fn inc_message_type(&self, message_type: &str) {
        let mut counts = self.message_type_counts.write().await;
        *counts.entry(message_type.to_string()).or_insert(0) += 1;
    }
    
    // ===== API Metrics =====
    
    pub fn inc_api_requests(&self) {
        self.api_requests_total.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_api_errors(&self) {
        self.api_errors_total.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_api_duration(&self, duration_us: u64) {
        self.api_request_duration_sum.fetch_add(duration_us, Ordering::Relaxed);
        self.api_request_duration_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub async fn inc_endpoint(&self, endpoint: &str) {
        let mut counts = self.endpoint_counts.write().await;
        *counts.entry(endpoint.to_string()).or_insert(0) += 1;
    }
    
    // ===== Chain Metrics =====
    
    pub fn set_last_block_eth(&self, block: u64) {
        self.last_block_eth.store(block, Ordering::Relaxed);
    }
    
    pub fn set_last_block_sol(&self, block: u64) {
        self.last_block_sol.store(block, Ordering::Relaxed);
    }
    
    pub fn set_last_block_avax(&self, block: u64) {
        self.last_block_avax.store(block, Ordering::Relaxed);
    }
    
    pub fn inc_blocks_indexed(&self, count: u64) {
        self.blocks_indexed.fetch_add(count, Ordering::Relaxed);
    }
    
    pub fn inc_chain_sync_errors(&self) {
        self.chain_sync_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    // ===== P2P Metrics =====
    
    pub fn set_peers(&self, count: u64) {
        self.peers_connected.store(count, Ordering::Relaxed);
    }
    
    pub fn inc_p2p_sent(&self, bytes: u64) {
        self.p2p_messages_sent.fetch_add(1, Ordering::Relaxed);
        self.p2p_bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }
    
    pub fn inc_p2p_received(&self, bytes: u64) {
        self.p2p_messages_received.fetch_add(1, Ordering::Relaxed);
        self.p2p_bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }
    
    // ===== Storage Metrics =====
    
    pub fn inc_files_stored(&self, size: u64) {
        self.files_stored.fetch_add(1, Ordering::Relaxed);
        self.storage_bytes_total.fetch_add(size, Ordering::Relaxed);
    }
    
    pub fn inc_ipfs_pins(&self) {
        self.ipfs_pins.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_ipfs_fetch(&self, success: bool) {
        if success {
            self.ipfs_fetch_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ipfs_fetch_failure.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    // ===== Database Metrics =====
    
    pub fn set_db_connections(&self, active: u64, idle: u64) {
        self.db_connections_active.store(active, Ordering::Relaxed);
        self.db_connections_idle.store(idle, Ordering::Relaxed);
    }
    
    pub fn record_db_query(&self, duration_us: u64) {
        self.db_queries_total.fetch_add(1, Ordering::Relaxed);
        self.db_query_duration_sum.fetch_add(duration_us, Ordering::Relaxed);
    }
    
    // ===== Cache Metrics =====
    
    pub fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn set_cache_size(&self, size: u64) {
        self.cache_size.store(size, Ordering::Relaxed);
    }
    
    // ===== Balance Metrics =====
    
    pub fn inc_balance_update(&self) {
        self.balance_updates.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_credit_update(&self) {
        self.credit_updates.fetch_add(1, Ordering::Relaxed);
    }
    
    // ===== Handler Metrics =====
    
    pub async fn record_handler_duration(&self, handler: &str, duration_us: u64) {
        let mut durations = self.handler_durations.write().await;
        let entry = durations.entry(handler.to_string()).or_insert((0, 0));
        entry.0 += duration_us;
        entry.1 += 1;
    }
    
    // ===== Info =====
    
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
            messages_pending: self.messages_pending.load(Ordering::Relaxed),
            api_requests: self.api_requests_total.load(Ordering::Relaxed),
            api_errors: self.api_errors_total.load(Ordering::Relaxed),
            blocks_indexed: self.blocks_indexed.load(Ordering::Relaxed),
            last_block_eth: self.last_block_eth.load(Ordering::Relaxed),
            peers_connected: self.peers_connected.load(Ordering::Relaxed),
            p2p_messages_sent: self.p2p_messages_sent.load(Ordering::Relaxed),
            p2p_messages_received: self.p2p_messages_received.load(Ordering::Relaxed),
            files_stored: self.files_stored.load(Ordering::Relaxed),
            storage_bytes: self.storage_bytes_total.load(Ordering::Relaxed),
        }
    }
    
    /// Generate Prometheus-format metrics output
    pub async fn prometheus_format(&self) -> String {
        let mut output = String::new();
        
        // Header
        output.push_str("# HELP aleph_info Node information\n");
        output.push_str("# TYPE aleph_info gauge\n");
        output.push_str(&format!(
            "aleph_info{{version=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        ));
        
        // Uptime
        output.push_str("# HELP aleph_uptime_seconds Node uptime in seconds\n");
        output.push_str("# TYPE aleph_uptime_seconds counter\n");
        output.push_str(&format!("aleph_uptime_seconds {}\n", self.uptime_secs()));
        
        // Messages
        output.push_str("\n# HELP aleph_messages_total Total messages by status\n");
        output.push_str("# TYPE aleph_messages_total counter\n");
        output.push_str(&format!(
            "aleph_messages_total{{status=\"received\"}} {}\n",
            self.messages_received.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_messages_total{{status=\"processed\"}} {}\n",
            self.messages_processed.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_messages_total{{status=\"rejected\"}} {}\n",
            self.messages_rejected.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_messages_total{{status=\"forgotten\"}} {}\n",
            self.messages_forgotten.load(Ordering::Relaxed)
        ));
        
        // Pending messages gauge
        output.push_str("\n# HELP aleph_messages_pending Current pending messages\n");
        output.push_str("# TYPE aleph_messages_pending gauge\n");
        output.push_str(&format!(
            "aleph_messages_pending {}\n",
            self.messages_pending.load(Ordering::Relaxed)
        ));
        
        // Message types
        let type_counts = self.message_type_counts.read().await;
        if !type_counts.is_empty() {
            output.push_str("\n# HELP aleph_messages_by_type Messages by type\n");
            output.push_str("# TYPE aleph_messages_by_type counter\n");
            for (msg_type, count) in type_counts.iter() {
                output.push_str(&format!(
                    "aleph_messages_by_type{{type=\"{}\"}} {}\n",
                    msg_type.to_lowercase(), count
                ));
            }
        }
        
        // API metrics
        output.push_str("\n# HELP aleph_api_requests_total Total API requests\n");
        output.push_str("# TYPE aleph_api_requests_total counter\n");
        output.push_str(&format!(
            "aleph_api_requests_total {}\n",
            self.api_requests_total.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_api_errors_total Total API errors\n");
        output.push_str("# TYPE aleph_api_errors_total counter\n");
        output.push_str(&format!(
            "aleph_api_errors_total {}\n",
            self.api_errors_total.load(Ordering::Relaxed)
        ));
        
        let req_count = self.api_request_duration_count.load(Ordering::Relaxed);
        if req_count > 0 {
            let sum_us = self.api_request_duration_sum.load(Ordering::Relaxed);
            output.push_str("\n# HELP aleph_api_request_duration_seconds API request duration\n");
            output.push_str("# TYPE aleph_api_request_duration_seconds summary\n");
            output.push_str(&format!(
                "aleph_api_request_duration_seconds_sum {:.6}\n",
                sum_us as f64 / 1_000_000.0
            ));
            output.push_str(&format!(
                "aleph_api_request_duration_seconds_count {}\n",
                req_count
            ));
        }
        
        // Chain sync
        output.push_str("\n# HELP aleph_chain_height Latest indexed block height\n");
        output.push_str("# TYPE aleph_chain_height gauge\n");
        let eth_block = self.last_block_eth.load(Ordering::Relaxed);
        if eth_block > 0 {
            output.push_str(&format!("aleph_chain_height{{chain=\"ETH\"}} {}\n", eth_block));
        }
        let sol_block = self.last_block_sol.load(Ordering::Relaxed);
        if sol_block > 0 {
            output.push_str(&format!("aleph_chain_height{{chain=\"SOL\"}} {}\n", sol_block));
        }
        let avax_block = self.last_block_avax.load(Ordering::Relaxed);
        if avax_block > 0 {
            output.push_str(&format!("aleph_chain_height{{chain=\"AVAX\"}} {}\n", avax_block));
        }
        
        output.push_str("\n# HELP aleph_blocks_indexed_total Total blocks indexed\n");
        output.push_str("# TYPE aleph_blocks_indexed_total counter\n");
        output.push_str(&format!(
            "aleph_blocks_indexed_total {}\n",
            self.blocks_indexed.load(Ordering::Relaxed)
        ));
        
        // P2P
        output.push_str("\n# HELP aleph_peers_connected Current peer count\n");
        output.push_str("# TYPE aleph_peers_connected gauge\n");
        output.push_str(&format!(
            "aleph_peers_connected {}\n",
            self.peers_connected.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_p2p_messages_total P2P messages sent/received\n");
        output.push_str("# TYPE aleph_p2p_messages_total counter\n");
        output.push_str(&format!(
            "aleph_p2p_messages_total{{direction=\"sent\"}} {}\n",
            self.p2p_messages_sent.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_p2p_messages_total{{direction=\"received\"}} {}\n",
            self.p2p_messages_received.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_p2p_bytes_total P2P bytes transferred\n");
        output.push_str("# TYPE aleph_p2p_bytes_total counter\n");
        output.push_str(&format!(
            "aleph_p2p_bytes_total{{direction=\"sent\"}} {}\n",
            self.p2p_bytes_sent.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_p2p_bytes_total{{direction=\"received\"}} {}\n",
            self.p2p_bytes_received.load(Ordering::Relaxed)
        ));
        
        // Storage
        output.push_str("\n# HELP aleph_files_stored_total Total files stored\n");
        output.push_str("# TYPE aleph_files_stored_total counter\n");
        output.push_str(&format!(
            "aleph_files_stored_total {}\n",
            self.files_stored.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_storage_bytes_total Total storage bytes\n");
        output.push_str("# TYPE aleph_storage_bytes_total counter\n");
        output.push_str(&format!(
            "aleph_storage_bytes_total {}\n",
            self.storage_bytes_total.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_ipfs_fetches_total IPFS fetch operations\n");
        output.push_str("# TYPE aleph_ipfs_fetches_total counter\n");
        output.push_str(&format!(
            "aleph_ipfs_fetches_total{{status=\"success\"}} {}\n",
            self.ipfs_fetch_success.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_ipfs_fetches_total{{status=\"failure\"}} {}\n",
            self.ipfs_fetch_failure.load(Ordering::Relaxed)
        ));
        
        // Database
        output.push_str("\n# HELP aleph_db_connections Database connections\n");
        output.push_str("# TYPE aleph_db_connections gauge\n");
        output.push_str(&format!(
            "aleph_db_connections{{state=\"active\"}} {}\n",
            self.db_connections_active.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "aleph_db_connections{{state=\"idle\"}} {}\n",
            self.db_connections_idle.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_db_queries_total Total database queries\n");
        output.push_str("# TYPE aleph_db_queries_total counter\n");
        output.push_str(&format!(
            "aleph_db_queries_total {}\n",
            self.db_queries_total.load(Ordering::Relaxed)
        ));
        
        // Cache
        output.push_str("\n# HELP aleph_cache_hits_total Cache hits\n");
        output.push_str("# TYPE aleph_cache_hits_total counter\n");
        output.push_str(&format!(
            "aleph_cache_hits_total {}\n",
            self.cache_hits.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_cache_misses_total Cache misses\n");
        output.push_str("# TYPE aleph_cache_misses_total counter\n");
        output.push_str(&format!(
            "aleph_cache_misses_total {}\n",
            self.cache_misses.load(Ordering::Relaxed)
        ));
        
        output.push_str("\n# HELP aleph_cache_size Current cache size\n");
        output.push_str("# TYPE aleph_cache_size gauge\n");
        output.push_str(&format!(
            "aleph_cache_size {}\n",
            self.cache_size.load(Ordering::Relaxed)
        ));
        
        // Handler durations
        let handler_durations = self.handler_durations.read().await;
        if !handler_durations.is_empty() {
            output.push_str("\n# HELP aleph_handler_duration_seconds Handler processing time\n");
            output.push_str("# TYPE aleph_handler_duration_seconds summary\n");
            for (handler, (sum_us, count)) in handler_durations.iter() {
                output.push_str(&format!(
                    "aleph_handler_duration_seconds_sum{{handler=\"{}\"}} {:.6}\n",
                    handler, *sum_us as f64 / 1_000_000.0
                ));
                output.push_str(&format!(
                    "aleph_handler_duration_seconds_count{{handler=\"{}\"}} {}\n",
                    handler, count
                ));
            }
        }
        
        output
    }
}

/// Serializable metrics snapshot
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub messages_received: u64,
    pub messages_processed: u64,
    pub messages_rejected: u64,
    pub messages_pending: u64,
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
    pub database: ServiceHealth,
    pub ipfs: ServiceHealth,
    pub p2p: ServiceHealth,
    pub rabbitmq: ServiceHealth,
    pub redis: ServiceHealth,
    pub chains: ChainsHealth,
}

#[derive(Debug, Clone, Serialize)]
pub struct ServiceHealth {
    pub connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ServiceHealth {
    pub fn connected() -> Self {
        Self { connected: true, latency_ms: None, error: None }
    }
    
    pub fn disconnected() -> Self {
        Self { connected: false, latency_ms: None, error: None }
    }
    
    pub fn with_latency(mut self, ms: u64) -> Self {
        self.latency_ms = Some(ms);
        self
    }
    
    pub fn with_error(mut self, err: &str) -> Self {
        self.error = Some(err.to_string());
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainsHealth {
    pub ethereum: ChainHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub solana: Option<ChainHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avalanche: Option<ChainHealth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bsc: Option<ChainHealth>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChainHealth {
    pub enabled: bool,
    pub synced: bool,
    pub last_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_block: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag: Option<u64>,
}

impl ChainHealth {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            synced: false,
            last_block: 0,
            head_block: None,
            lag: None,
        }
    }
    
    pub fn enabled_with_block(last_block: u64, head_block: Option<u64>) -> Self {
        let lag = head_block.map(|h| h.saturating_sub(last_block));
        let synced = lag.map(|l| l < 100).unwrap_or(false);
        
        Self {
            enabled: true,
            synced,
            last_block,
            head_block,
            lag,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::new();
        
        metrics.inc_messages_received();
        metrics.inc_messages_received();
        metrics.inc_messages_processed();
        
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 2);
        assert_eq!(snapshot.messages_processed, 1);
    }
    
    #[tokio::test]
    async fn test_prometheus_format() {
        let metrics = Metrics::new();
        
        metrics.inc_messages_received();
        metrics.inc_messages_processed();
        metrics.set_last_block_eth(12345);
        
        let output = metrics.prometheus_format().await;
        
        assert!(output.contains("aleph_info"));
        assert!(output.contains("aleph_uptime_seconds"));
        assert!(output.contains("aleph_messages_total"));
        assert!(output.contains("aleph_chain_height{chain=\"ETH\"} 12345"));
    }
}
