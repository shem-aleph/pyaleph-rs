//! Tiered storage system
//!
//! Wraps local storage, IPFS, and the sharding service to provide
//! content-aware storage decisions:
//!
//! - **Owned content** (we're a responsible node): stored locally or pinned in IPFS
//! - **Non-owned content**: kept in a warm cache with TTL-based eviction
//!
//! The warm cache allows nodes to serve recently-seen content without
//! permanently storing everything, reducing per-node storage requirements
//! when sharding is enabled.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::LocalStorage;
use crate::services::ipfs::IpfsService;
use crate::services::sharding::{ContentDecision, ShardingService};

/// Tiered storage combining owned storage, warm cache, and IPFS.
///
/// Debug is manually implemented because inner types don't all derive it.
pub struct TieredStorage {
    /// Primary storage for content we're responsible for
    owned_storage: LocalStorage,
    /// Short-lived cache for non-owned content
    warm_cache: WarmCache,
    /// IPFS service for pinning large files
    ipfs: Option<Arc<IpfsService>>,
    /// Sharding service for routing decisions
    sharding: Option<Arc<ShardingService>>,
}

impl std::fmt::Debug for TieredStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredStorage")
            .field("has_ipfs", &self.ipfs.is_some())
            .field("has_sharding", &self.sharding.is_some())
            .finish_non_exhaustive()
    }
}

impl TieredStorage {
    pub fn new(
        data_dir: &Path,
        warm_cache_dir: &Path,
        warm_cache_max_bytes: u64,
        warm_cache_ttl: Duration,
    ) -> Self {
        Self {
            owned_storage: LocalStorage::new(data_dir),
            warm_cache: WarmCache::new(warm_cache_dir, warm_cache_max_bytes, warm_cache_ttl),
            ipfs: None,
            sharding: None,
        }
    }

    pub fn with_ipfs(mut self, ipfs: Arc<IpfsService>) -> Self {
        self.ipfs = Some(ipfs);
        self
    }

    pub fn with_sharding(mut self, sharding: Arc<ShardingService>) -> Self {
        self.sharding = Some(sharding);
        self
    }

    /// Initialize storage directories.
    pub async fn init(&self) -> std::io::Result<()> {
        self.owned_storage.init().await?;
        self.warm_cache.storage.init().await?;
        Ok(())
    }

    /// Store content with sharding-aware placement.
    ///
    /// - If sharding is disabled or we're responsible: store in owned storage
    ///   (large files >1 MiB go to IPFS pin, small files to local filesystem)
    /// - If we're NOT responsible: store in warm cache with TTL
    pub async fn store(&self, hash: &str, content: &[u8]) -> std::io::Result<()> {
        let is_owned = match &self.sharding {
            Some(svc) => {
                matches!(svc.get_routing_decision(hash).await, ContentDecision::Owned { .. })
            }
            None => true, // No sharding = store everything
        };

        if is_owned {
            // Owned content: store locally, pin in IPFS for large files
            self.owned_storage.put(hash, content).await?;

            if content.len() > 1024 * 1024 {
                // Large file: also pin in IPFS
                if let Some(ref ipfs) = self.ipfs {
                    if let Err(e) = ipfs.pin(hash).await {
                        warn!("Failed to IPFS pin owned content {}: {}", hash, e);
                    }
                }
            }
        } else {
            // Not owned: warm cache only
            self.warm_cache.put(hash, content).await?;
        }

        Ok(())
    }

    /// Get content, trying owned storage first, then warm cache, then IPFS.
    pub async fn get(&self, hash: &str) -> Option<Vec<u8>> {
        // Try owned storage
        if let Some(content) = self.owned_storage.get(hash).await {
            return Some(content);
        }

        // Try warm cache
        if let Some(content) = self.warm_cache.get(hash).await {
            return Some(content);
        }

        // Try IPFS as last resort
        if let Some(ref ipfs) = self.ipfs {
            match ipfs.get(hash).await {
                Ok(content) => return Some(content),
                Err(e) => {
                    debug!("IPFS get failed for {}: {}", hash, e);
                }
            }
        }

        None
    }

    /// Check if content exists in any tier.
    pub async fn exists(&self, hash: &str) -> bool {
        self.owned_storage.exists(hash).await || self.warm_cache.exists(hash).await
    }

    /// Remove content from all tiers.
    pub async fn remove(&self, hash: &str) -> std::io::Result<()> {
        let _ = self.owned_storage.remove(hash).await;
        let _ = self.warm_cache.remove(hash).await;

        if let Some(ref ipfs) = self.ipfs {
            let _ = ipfs.unpin(hash).await;
        }

        Ok(())
    }

    /// Get the responsible nodes for a content hash (for routing hints in API responses).
    pub async fn get_responsible_nodes(&self, hash: &str) -> Option<Vec<String>> {
        if let Some(ref svc) = self.sharding {
            let nodes = svc.get_responsible_nodes(hash).await;
            if !nodes.is_empty() {
                return Some(nodes.iter().map(|n| n.http_address.clone()).collect());
            }
        }
        None
    }

    /// Evict expired warm cache entries. Returns bytes freed.
    pub async fn evict_expired_cache(&self) -> u64 {
        self.warm_cache.evict_expired().await
    }

    /// Get warm cache stats.
    pub async fn warm_cache_stats(&self) -> WarmCacheStats {
        self.warm_cache.stats().await
    }
}

// ── Warm Cache ─────────────────────────────────────────────────────────

/// Warm cache for non-owned content with TTL-based eviction.
///
/// Uses a separate storage directory from owned content, with an in-memory
/// index tracking entry timestamps for TTL and LRU eviction.
pub struct WarmCache {
    storage: LocalStorage,
    /// Maximum cache size in bytes
    max_bytes: u64,
    /// TTL for cache entries
    ttl: Duration,
    /// In-memory index: hash → (size, inserted_at)
    index: RwLock<HashMap<String, CacheEntry>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    size: u64,
    inserted_at: Instant,
}

/// Statistics for the warm cache
#[derive(Debug, Clone)]
pub struct WarmCacheStats {
    pub entry_count: usize,
    pub total_bytes: u64,
    pub max_bytes: u64,
}

impl WarmCache {
    pub fn new(cache_dir: &Path, max_bytes: u64, ttl: Duration) -> Self {
        Self {
            storage: LocalStorage::new(cache_dir),
            max_bytes,
            ttl,
            index: RwLock::new(HashMap::new()),
        }
    }

    /// Store content in the warm cache.
    pub async fn put(&self, hash: &str, content: &[u8]) -> std::io::Result<()> {
        let size = content.len() as u64;

        // Check if we need to evict to make room
        self.ensure_capacity(size).await;

        self.storage.put(hash, content).await?;

        let mut index = self.index.write().await;
        index.insert(hash.to_string(), CacheEntry {
            size,
            inserted_at: Instant::now(),
        });

        Ok(())
    }

    /// Get content from the warm cache (returns None if expired or missing).
    pub async fn get(&self, hash: &str) -> Option<Vec<u8>> {
        // Check if entry exists and is not expired
        {
            let index = self.index.read().await;
            if let Some(entry) = index.get(hash) {
                if entry.inserted_at.elapsed() > self.ttl {
                    // Expired — will be cleaned up by evict_expired
                    return None;
                }
            } else {
                return None;
            }
        }

        self.storage.get(hash).await
    }

    /// Check if content exists in the warm cache (and is not expired).
    pub async fn exists(&self, hash: &str) -> bool {
        let index = self.index.read().await;
        if let Some(entry) = index.get(hash) {
            entry.inserted_at.elapsed() <= self.ttl
        } else {
            false
        }
    }

    /// Remove content from the warm cache.
    pub async fn remove(&self, hash: &str) -> std::io::Result<()> {
        let mut index = self.index.write().await;
        index.remove(hash);
        self.storage.remove(hash).await
    }

    /// Evict all expired entries. Returns total bytes freed.
    pub async fn evict_expired(&self) -> u64 {
        let mut index = self.index.write().await;
        let mut freed = 0u64;

        let expired: Vec<String> = index
            .iter()
            .filter(|(_, entry)| entry.inserted_at.elapsed() > self.ttl)
            .map(|(hash, _)| hash.clone())
            .collect();

        for hash in expired {
            if let Some(entry) = index.remove(&hash) {
                freed += entry.size;
                let _ = self.storage.remove(&hash).await;
            }
        }

        if freed > 0 {
            debug!("Warm cache: evicted expired entries, freed {} bytes", freed);
        }

        freed
    }

    /// Ensure we have capacity for `needed_bytes` by evicting oldest entries.
    async fn ensure_capacity(&self, needed_bytes: u64) {
        let mut index = self.index.write().await;

        let current_size: u64 = index.values().map(|e| e.size).sum();
        if current_size + needed_bytes <= self.max_bytes {
            return;
        }

        // Sort entries by insertion time (oldest first) for LRU eviction
        let mut entries: Vec<(String, CacheEntry)> = index
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_by_key(|(_, e)| e.inserted_at);

        let mut freed = 0u64;
        let target = needed_bytes.saturating_sub(self.max_bytes.saturating_sub(current_size));

        for (hash, entry) in entries {
            if freed >= target {
                break;
            }
            index.remove(&hash);
            freed += entry.size;
            let _ = self.storage.remove(&hash).await;
        }

        if freed > 0 {
            debug!("Warm cache: LRU evicted {} bytes to make room", freed);
        }
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> WarmCacheStats {
        let index = self.index.read().await;
        WarmCacheStats {
            entry_count: index.len(),
            total_bytes: index.values().map(|e| e.size).sum(),
            max_bytes: self.max_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_warm_cache_put_get() {
        let tmp = TempDir::new().unwrap();
        let cache = WarmCache::new(tmp.path(), 1024 * 1024, Duration::from_secs(60));
        cache.storage.init().await.unwrap();

        cache.put("hash1", b"hello").await.unwrap();
        let content = cache.get("hash1").await.unwrap();
        assert_eq!(content, b"hello");
    }

    #[tokio::test]
    async fn test_warm_cache_expiry() {
        let tmp = TempDir::new().unwrap();
        // 0-second TTL means everything expires immediately
        let cache = WarmCache::new(tmp.path(), 1024 * 1024, Duration::from_millis(1));
        cache.storage.init().await.unwrap();

        cache.put("hash1", b"hello").await.unwrap();

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Should be expired now
        assert!(cache.get("hash1").await.is_none());
        assert!(!cache.exists("hash1").await);
    }

    #[tokio::test]
    async fn test_warm_cache_lru_eviction() {
        let tmp = TempDir::new().unwrap();
        // Max 10 bytes
        let cache = WarmCache::new(tmp.path(), 10, Duration::from_secs(60));
        cache.storage.init().await.unwrap();

        // Put 5 bytes
        cache.put("hash1", b"aaaaa").await.unwrap();
        // Put another 5 bytes — still fits
        cache.put("hash2", b"bbbbb").await.unwrap();
        // Put 5 more — needs to evict hash1 (oldest)
        cache.put("hash3", b"ccccc").await.unwrap();

        // hash1 should have been evicted
        assert!(cache.get("hash1").await.is_none());
        // hash3 should exist
        assert!(cache.get("hash3").await.is_some());
    }

    #[tokio::test]
    async fn test_warm_cache_evict_expired() {
        let tmp = TempDir::new().unwrap();
        let cache = WarmCache::new(tmp.path(), 1024 * 1024, Duration::from_millis(1));
        cache.storage.init().await.unwrap();

        cache.put("h1", b"aaa").await.unwrap();
        cache.put("h2", b"bbb").await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        let freed = cache.evict_expired().await;
        assert_eq!(freed, 6); // 3 + 3 bytes

        let stats = cache.stats().await;
        assert_eq!(stats.entry_count, 0);
    }

    #[tokio::test]
    async fn test_tiered_storage_no_sharding() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        let cache_dir = tmp.path().join("cache");

        let ts = TieredStorage::new(
            &data_dir,
            &cache_dir,
            1024 * 1024,
            Duration::from_secs(60),
        );
        ts.init().await.unwrap();

        // No sharding → everything goes to owned storage
        ts.store("hash1", b"content").await.unwrap();
        let content = ts.get("hash1").await.unwrap();
        assert_eq!(content, b"content");
        assert!(ts.exists("hash1").await);
    }

    #[tokio::test]
    async fn test_tiered_storage_remove() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        let cache_dir = tmp.path().join("cache");

        let ts = TieredStorage::new(
            &data_dir,
            &cache_dir,
            1024 * 1024,
            Duration::from_secs(60),
        );
        ts.init().await.unwrap();

        ts.store("hash1", b"content").await.unwrap();
        assert!(ts.exists("hash1").await);
        ts.remove("hash1").await.unwrap();
        assert!(!ts.exists("hash1").await);
    }
}
