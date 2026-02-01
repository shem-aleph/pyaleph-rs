//! Redis cache layer
//!
//! Provides caching for frequently accessed data to reduce database load.
//! This implements the caching layer that pyaleph uses for performance.
//!
//! Reference: aleph/services/cache.py

use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use std::collections::HashMap;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Key not found: {0}")]
    NotFound(String),
    
    #[error("Operation timeout")]
    Timeout,
    
    #[error("Client not connected")]
    NotConnected,
}

/// Redis configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis server URL
    pub url: String,
    /// Connection pool size
    pub pool_size: u32,
    /// Default TTL for cached items (seconds)
    pub default_ttl: u64,
    /// Prefix for all keys (namespace)
    pub key_prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            pool_size: 10,
            default_ttl: 300, // 5 minutes
            key_prefix: "aleph:".to_string(),
        }
    }
}

/// Cache entry with expiration
#[derive(Debug, Clone)]
struct CacheEntry {
    value: String,
    expires_at: std::time::Instant,
}

impl CacheEntry {
    fn new(value: String, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: std::time::Instant::now() + ttl,
        }
    }
    
    fn is_expired(&self) -> bool {
        std::time::Instant::now() > self.expires_at
    }
}

/// Redis service for caching
/// 
/// In production, this would use the redis crate.
/// For now, we implement an in-memory fallback that can be replaced with real Redis.
#[derive(Debug)]
pub struct RedisService {
    config: RedisConfig,
    /// In-memory fallback cache (replace with actual Redis client in production)
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
    connected: Arc<RwLock<bool>>,
}

impl RedisService {
    /// Create a new Redis service
    pub fn new(config: RedisConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
            connected: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Connect to Redis
    pub async fn connect(&self) -> Result<(), RedisError> {
        // In production, establish Redis connection here
        // For now, mark as connected (in-memory fallback is always available)
        tracing::info!("Initializing cache layer (in-memory fallback)");
        tracing::info!("Redis URL configured: {} (connection pending)", self.config.url);
        
        let mut connected = self.connected.write().await;
        *connected = true;
        
        Ok(())
    }
    
    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }
    
    /// Get prefixed key
    fn prefixed_key(&self, key: &str) -> String {
        format!("{}{}", self.config.key_prefix, key)
    }
    
    /// Get a value from cache
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<T, RedisError> {
        let full_key = self.prefixed_key(key);
        let cache = self.cache.read().await;
        
        match cache.get(&full_key) {
            Some(entry) if !entry.is_expired() => {
                serde_json::from_str(&entry.value)
                    .map_err(|e| RedisError::Serialization(e.to_string()))
            }
            _ => Err(RedisError::NotFound(key.to_string())),
        }
    }
    
    /// Get a raw string value
    pub async fn get_string(&self, key: &str) -> Result<String, RedisError> {
        let full_key = self.prefixed_key(key);
        let cache = self.cache.read().await;
        
        match cache.get(&full_key) {
            Some(entry) if !entry.is_expired() => Ok(entry.value.clone()),
            _ => Err(RedisError::NotFound(key.to_string())),
        }
    }
    
    /// Set a value in cache with default TTL
    pub async fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<(), RedisError> {
        self.set_with_ttl(key, value, Duration::from_secs(self.config.default_ttl)).await
    }
    
    /// Set a value with custom TTL
    pub async fn set_with_ttl<T: Serialize>(
        &self, 
        key: &str, 
        value: &T, 
        ttl: Duration
    ) -> Result<(), RedisError> {
        let full_key = self.prefixed_key(key);
        let serialized = serde_json::to_string(value)
            .map_err(|e| RedisError::Serialization(e.to_string()))?;
        
        let entry = CacheEntry::new(serialized, ttl);
        
        let mut cache = self.cache.write().await;
        cache.insert(full_key, entry);
        
        Ok(())
    }
    
    /// Set a raw string value
    pub async fn set_string(&self, key: &str, value: &str) -> Result<(), RedisError> {
        self.set_string_with_ttl(key, value, Duration::from_secs(self.config.default_ttl)).await
    }
    
    /// Set a raw string with TTL
    pub async fn set_string_with_ttl(
        &self, 
        key: &str, 
        value: &str, 
        ttl: Duration
    ) -> Result<(), RedisError> {
        let full_key = self.prefixed_key(key);
        let entry = CacheEntry::new(value.to_string(), ttl);
        
        let mut cache = self.cache.write().await;
        cache.insert(full_key, entry);
        
        Ok(())
    }
    
    /// Delete a key from cache
    pub async fn delete(&self, key: &str) -> Result<bool, RedisError> {
        let full_key = self.prefixed_key(key);
        let mut cache = self.cache.write().await;
        Ok(cache.remove(&full_key).is_some())
    }
    
    /// Check if a key exists (and is not expired)
    pub async fn exists(&self, key: &str) -> Result<bool, RedisError> {
        let full_key = self.prefixed_key(key);
        let cache = self.cache.read().await;
        
        Ok(cache.get(&full_key).map(|e| !e.is_expired()).unwrap_or(false))
    }
    
    /// Set multiple keys at once
    pub async fn mset<T: Serialize>(&self, items: &[(String, T)]) -> Result<(), RedisError> {
        let ttl = Duration::from_secs(self.config.default_ttl);
        let mut cache = self.cache.write().await;
        
        for (key, value) in items {
            let full_key = self.prefixed_key(key);
            let serialized = serde_json::to_string(value)
                .map_err(|e| RedisError::Serialization(e.to_string()))?;
            cache.insert(full_key, CacheEntry::new(serialized, ttl));
        }
        
        Ok(())
    }
    
    /// Get multiple keys at once
    pub async fn mget<T: DeserializeOwned>(&self, keys: &[&str]) -> Vec<Option<T>> {
        let cache = self.cache.read().await;
        
        keys.iter()
            .map(|key| {
                let full_key = self.prefixed_key(key);
                cache.get(&full_key)
                    .filter(|e| !e.is_expired())
                    .and_then(|e| serde_json::from_str(&e.value).ok())
            })
            .collect()
    }
    
    /// Increment a counter
    pub async fn incr(&self, key: &str) -> Result<i64, RedisError> {
        let full_key = self.prefixed_key(key);
        let mut cache = self.cache.write().await;
        
        let current: i64 = cache.get(&full_key)
            .filter(|e| !e.is_expired())
            .and_then(|e| e.value.parse().ok())
            .unwrap_or(0);
        
        let new_value = current + 1;
        let ttl = Duration::from_secs(self.config.default_ttl);
        cache.insert(full_key, CacheEntry::new(new_value.to_string(), ttl));
        
        Ok(new_value)
    }
    
    /// Decrement a counter
    pub async fn decr(&self, key: &str) -> Result<i64, RedisError> {
        let full_key = self.prefixed_key(key);
        let mut cache = self.cache.write().await;
        
        let current: i64 = cache.get(&full_key)
            .filter(|e| !e.is_expired())
            .and_then(|e| e.value.parse().ok())
            .unwrap_or(0);
        
        let new_value = current - 1;
        let ttl = Duration::from_secs(self.config.default_ttl);
        cache.insert(full_key, CacheEntry::new(new_value.to_string(), ttl));
        
        Ok(new_value)
    }
    
    /// Add to a set
    pub async fn sadd(&self, key: &str, member: &str) -> Result<bool, RedisError> {
        let full_key = self.prefixed_key(key);
        let mut cache = self.cache.write().await;
        
        let mut set: Vec<String> = cache.get(&full_key)
            .filter(|e| !e.is_expired())
            .and_then(|e| serde_json::from_str(&e.value).ok())
            .unwrap_or_default();
        
        let is_new = !set.contains(&member.to_string());
        if is_new {
            set.push(member.to_string());
        }
        
        let serialized = serde_json::to_string(&set)
            .map_err(|e| RedisError::Serialization(e.to_string()))?;
        
        let ttl = Duration::from_secs(self.config.default_ttl);
        cache.insert(full_key, CacheEntry::new(serialized, ttl));
        
        Ok(is_new)
    }
    
    /// Check if member exists in set
    pub async fn sismember(&self, key: &str, member: &str) -> Result<bool, RedisError> {
        let full_key = self.prefixed_key(key);
        let cache = self.cache.read().await;
        
        let set: Vec<String> = cache.get(&full_key)
            .filter(|e| !e.is_expired())
            .and_then(|e| serde_json::from_str(&e.value).ok())
            .unwrap_or_default();
        
        Ok(set.contains(&member.to_string()))
    }
    
    /// Get all members of a set
    pub async fn smembers(&self, key: &str) -> Result<Vec<String>, RedisError> {
        let full_key = self.prefixed_key(key);
        let cache = self.cache.read().await;
        
        let set: Vec<String> = cache.get(&full_key)
            .filter(|e| !e.is_expired())
            .and_then(|e| serde_json::from_str(&e.value).ok())
            .unwrap_or_default();
        
        Ok(set)
    }
    
    /// Clean up expired entries (should be called periodically)
    pub async fn cleanup_expired(&self) {
        let mut cache = self.cache.write().await;
        let now = std::time::Instant::now();
        
        cache.retain(|_, entry| entry.expires_at > now);
    }
    
    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        let now = std::time::Instant::now();
        
        let total = cache.len();
        let expired = cache.values().filter(|e| e.expires_at <= now).count();
        let active = total - expired;
        
        CacheStats {
            total_keys: total,
            active_keys: active,
            expired_keys: expired,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_keys: usize,
    pub active_keys: usize,
    pub expired_keys: usize,
}

/// Cache key builders for common patterns
pub mod keys {
    /// Message cache key
    pub fn message(item_hash: &str) -> String {
        format!("msg:{}", item_hash)
    }
    
    /// Message status cache key
    pub fn message_status(item_hash: &str) -> String {
        format!("msg_status:{}", item_hash)
    }
    
    /// Aggregate cache key
    pub fn aggregate(address: &str, key: &str) -> String {
        format!("agg:{}:{}", address, key)
    }
    
    /// Balance cache key
    pub fn balance(address: &str, chain: &str) -> String {
        format!("bal:{}:{}", address, chain)
    }
    
    /// Credit balance cache key
    pub fn credit_balance(address: &str) -> String {
        format!("credit:{}", address)
    }
    
    /// File info cache key
    pub fn file_info(hash: &str) -> String {
        format!("file:{}", hash)
    }
    
    /// Pending messages count
    pub fn pending_count() -> String {
        "pending_count".to_string()
    }
    
    /// Chain sync height
    pub fn chain_height(chain: &str) -> String {
        format!("chain_height:{}", chain)
    }
    
    /// Rate limit key
    pub fn rate_limit(address: &str, endpoint: &str) -> String {
        format!("rl:{}:{}", address, endpoint)
    }
    
    /// Seen message (deduplication)
    pub fn seen_message(item_hash: &str) -> String {
        format!("seen:{}", item_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_basic_operations() {
        let config = RedisConfig::default();
        let redis = RedisService::new(config);
        redis.connect().await.unwrap();
        
        // Set and get
        redis.set("test_key", &"test_value").await.unwrap();
        let value: String = redis.get("test_key").await.unwrap();
        assert_eq!(value, "test_value");
        
        // Delete
        let deleted = redis.delete("test_key").await.unwrap();
        assert!(deleted);
        
        // Should not exist
        assert!(!redis.exists("test_key").await.unwrap());
    }
    
    #[tokio::test]
    async fn test_counter_operations() {
        let config = RedisConfig::default();
        let redis = RedisService::new(config);
        redis.connect().await.unwrap();
        
        // Increment
        assert_eq!(redis.incr("counter").await.unwrap(), 1);
        assert_eq!(redis.incr("counter").await.unwrap(), 2);
        
        // Decrement
        assert_eq!(redis.decr("counter").await.unwrap(), 1);
    }
    
    #[tokio::test]
    async fn test_set_operations() {
        let config = RedisConfig::default();
        let redis = RedisService::new(config);
        redis.connect().await.unwrap();
        
        // Add to set
        assert!(redis.sadd("myset", "a").await.unwrap());
        assert!(!redis.sadd("myset", "a").await.unwrap()); // Duplicate
        assert!(redis.sadd("myset", "b").await.unwrap());
        
        // Check membership
        assert!(redis.sismember("myset", "a").await.unwrap());
        assert!(!redis.sismember("myset", "c").await.unwrap());
        
        // Get all members
        let members = redis.smembers("myset").await.unwrap();
        assert_eq!(members.len(), 2);
    }
    
    #[tokio::test]
    async fn test_ttl_expiration() {
        let config = RedisConfig::default();
        let redis = RedisService::new(config);
        redis.connect().await.unwrap();
        
        // Set with short TTL
        redis.set_string_with_ttl("short_ttl", "value", Duration::from_millis(50)).await.unwrap();
        
        // Should exist immediately
        assert!(redis.exists("short_ttl").await.unwrap());
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Should be expired
        assert!(!redis.exists("short_ttl").await.unwrap());
    }
}
