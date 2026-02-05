//! Cleanup job
//!
//! Handles periodic cleanup tasks like removing stale data.

use std::sync::Arc;
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

use crate::config::Config;

/// Cleanup interval in seconds (run every hour)
const CLEANUP_INTERVAL_SECS: u64 = 3600;

/// Maximum cache size in bytes (10 GB)
const CACHE_MAX_SIZE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Run the cleanup job
pub async fn run(pool: PgPool, config: Arc<Config>) {
    let mut interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));

    // Skip first tick (don't run immediately on startup)
    interval.tick().await;

    loop {
        interval.tick().await;

        info!("Running periodic cleanup");

        // Clean up old pending messages
        match cleanup_pending_messages(&pool).await {
            Ok(count) => {
                if count > 0 {
                    info!("Cleaned up {} stale pending messages", count);
                }
            }
            Err(e) => {
                error!("Pending message cleanup error: {}", e);
            }
        }

        // Clean up expired credits
        match cleanup_expired_credits(&pool).await {
            Ok(count) => {
                if count > 0 {
                    info!("Cleaned up {} expired credit entries", count);
                }
            }
            Err(e) => {
                error!("Credit cleanup error: {}", e);
            }
        }

        // Clean up cache
        match cleanup_cache(&config).await {
            Ok(bytes) => {
                if bytes > 0 {
                    info!("Freed {} bytes from cache", bytes);
                }
            }
            Err(e) => {
                error!("Cache cleanup error: {}", e);
            }
        }
    }
}

/// Clean up old pending messages that have exceeded retry limits
async fn cleanup_pending_messages(pool: &PgPool) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let result = sqlx::query(
        "DELETE FROM pending_messages WHERE retries > 10 AND next_attempt < NOW() - INTERVAL '1 day'"
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() as u32)
}

/// Clean up expired credit balances
async fn cleanup_expired_credits(pool: &PgPool) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let result = sqlx::query(
        "DELETE FROM credit_balances WHERE expiration IS NOT NULL AND expiration < NOW() AND balance <= 0"
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected() as u32)
}

/// Clean up old cache files using LRU eviction when cache exceeds size limit
async fn cleanup_cache(config: &Config) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::fs;

    if !config.storage.enable_cache {
        return Ok(0);
    }

    let cache_dir = &config.storage.cache_dir;
    if !cache_dir.exists() {
        return Ok(0);
    }

    // Collect all files with their sizes and modification times
    let mut entries: Vec<(std::path::PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total_size: u64 = 0;

    let mut read_dir = fs::read_dir(cache_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_file() {
            continue;
        }

        let size = metadata.len();
        let modified = metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        total_size += size;
        entries.push((entry.path(), size, modified));
    }

    if total_size <= CACHE_MAX_SIZE_BYTES {
        debug!("Cache size {} bytes is within limit", total_size);
        return Ok(0);
    }

    // Sort by modification time ascending (oldest first) for LRU eviction
    entries.sort_by_key(|(_, _, modified)| *modified);

    let mut freed: u64 = 0;
    for (path, size, _) in &entries {
        if total_size - freed <= CACHE_MAX_SIZE_BYTES {
            break;
        }

        if let Err(e) = fs::remove_file(&path).await {
            debug!("Failed to remove cache file {}: {}", path.display(), e);
            continue;
        }

        freed += size;
    }

    Ok(freed)
}
