//! Garbage Collector Job
//!
//! Cleans up files that are no longer referenced by any message,
//! respecting the grace period for recently deleted items.
//!
//! Reference: aleph/jobs/garbage_collector.py

use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn, error};
use sqlx::PgPool;
use chrono::{Utc, Duration as ChronoDuration};

use crate::config::Config;
use crate::services::ipfs::IpfsService;
use crate::services::Metrics;

/// Run the garbage collector job
pub async fn run(
    db: PgPool,
    ipfs: Arc<IpfsService>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) {
    let gc_period = config.storage.gc_period;
    let grace_period_secs = config.storage.grace_period as i64;
    
    let mut ticker = interval(Duration::from_secs(gc_period));
    
    info!("Garbage collector started (period: {}s, grace: {}s)", gc_period, grace_period_secs);
    
    loop {
        ticker.tick().await;
        
        match run_gc_cycle(&db, &ipfs, grace_period_secs, &metrics).await {
            Ok(stats) => {
                if stats.files_removed > 0 || stats.orphaned_pins > 0 {
                    info!(
                        "GC cycle complete: {} files removed, {} orphaned pins cleaned, {} bytes freed",
                        stats.files_removed, stats.orphaned_pins, stats.bytes_freed
                    );
                } else {
                    debug!("GC cycle complete: nothing to clean");
                }
            }
            Err(e) => {
                error!("Garbage collection error: {}", e);
            }
        }
    }
}

/// Statistics from a GC cycle
#[derive(Debug, Default)]
pub struct GcStats {
    pub files_checked: u64,
    pub files_removed: u64,
    pub orphaned_pins: u64,
    pub bytes_freed: u64,
    pub errors: u64,
}

/// Run a single GC cycle
async fn run_gc_cycle(
    db: &PgPool,
    ipfs: &IpfsService,
    grace_period_secs: i64,
    metrics: &Metrics,
) -> Result<GcStats, GcError> {
    let mut stats = GcStats::default();
    let grace_cutoff = Utc::now() - ChronoDuration::seconds(grace_period_secs);
    
    // Step 1: Find file pins with no active references
    // A file is orphaned if:
    // - The message that stored it was forgotten (and grace period passed)
    // - No other messages reference the file
    let orphaned_files: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT fp.item_hash, fp.size
        FROM file_pins fp
        WHERE NOT EXISTS (
            -- No STORE message references this file
            SELECT 1 FROM messages m 
            WHERE m.message_type = 'STORE' 
            AND m.item_content::json->>'item_hash' = fp.item_hash
        )
        AND NOT EXISTS (
            -- No active programs or instances use this file
            SELECT 1 FROM programs p WHERE p.code_ref = fp.item_hash OR p.runtime_ref = fp.item_hash
            UNION
            SELECT 1 FROM instances i WHERE i.rootfs_ref = fp.item_hash
        )
        AND NOT EXISTS (
            -- File is not tagged for retention
            SELECT 1 FROM file_tags ft WHERE ft.item_hash = fp.item_hash AND ft.tag = 'keep'
        )
        AND fp.created_at < $1
        LIMIT 100
        "#
    )
    .bind(grace_cutoff)
    .fetch_all(db)
    .await
    .map_err(|e| GcError::Database(e.to_string()))?;
    
    stats.files_checked = orphaned_files.len() as u64;
    
    // Step 2: Remove orphaned files
    for (hash, size) in orphaned_files {
        match remove_file(db, ipfs, &hash).await {
            Ok(true) => {
                stats.files_removed += 1;
                stats.bytes_freed += size as u64;
            }
            Ok(false) => {
                // File was referenced again, skip
            }
            Err(e) => {
                warn!("Failed to remove file {}: {}", hash, e);
                stats.errors += 1;
            }
        }
    }
    
    // Step 3: Clean up orphaned IPFS pins
    let orphaned_pins = clean_orphaned_pins(db, ipfs).await?;
    stats.orphaned_pins = orphaned_pins;
    
    // Step 4: Clean up expired pending messages
    let expired_pending = clean_expired_pending(db, grace_period_secs).await?;
    debug!("Cleaned {} expired pending messages", expired_pending);
    
    // Step 5: Clean up old rejected messages (keep for 7 days)
    let old_rejected = clean_old_rejected(db).await?;
    debug!("Cleaned {} old rejected messages", old_rejected);
    
    Ok(stats)
}

/// Remove a file from storage and database
async fn remove_file(db: &PgPool, ipfs: &IpfsService, hash: &str) -> Result<bool, GcError> {
    // Double-check file is still orphaned (race condition protection)
    let still_orphaned: bool = sqlx::query_scalar(
        r#"
        SELECT NOT EXISTS (
            SELECT 1 FROM messages m 
            WHERE m.message_type = 'STORE' 
            AND m.item_content::json->>'item_hash' = $1
        )
        "#
    )
    .bind(hash)
    .fetch_one(db)
    .await
    .map_err(|e| GcError::Database(e.to_string()))?;
    
    if !still_orphaned {
        return Ok(false);
    }
    
    // Unpin from IPFS
    if let Err(e) = ipfs.unpin(hash).await {
        debug!("IPFS unpin failed for {}: {} (may not be pinned)", hash, e);
    }
    
    // Remove from file_pins
    sqlx::query("DELETE FROM file_pins WHERE item_hash = $1")
        .bind(hash)
        .execute(db)
        .await
        .map_err(|e| GcError::Database(e.to_string()))?;
    
    // Remove file tags
    sqlx::query("DELETE FROM file_tags WHERE item_hash = $1")
        .bind(hash)
        .execute(db)
        .await
        .map_err(|e| GcError::Database(e.to_string()))?;
    
    debug!("Removed orphaned file: {}", hash);
    
    Ok(true)
}

/// Clean up orphaned IPFS pins (pins without corresponding file_pins entry)
async fn clean_orphaned_pins(db: &PgPool, ipfs: &IpfsService) -> Result<u64, GcError> {
    // This would require querying IPFS for all pins and comparing with database
    // For now, we skip this as it's expensive
    // In production, this should be done periodically with rate limiting
    
    Ok(0)
}

/// Clean up expired pending messages
async fn clean_expired_pending(db: &PgPool, grace_period_secs: i64) -> Result<u64, GcError> {
    let cutoff = Utc::now() - ChronoDuration::seconds(grace_period_secs);
    let cutoff_timestamp = cutoff.timestamp() as f64;
    
    // Move to rejected if max retries exceeded
    let result = sqlx::query(
        r#"
        WITH expired AS (
            DELETE FROM pending_messages
            WHERE next_attempt < $1 AND retries >= 10
            RETURNING item_hash, message
        )
        INSERT INTO rejected_messages (item_hash, message, error_code, error_message, rejected_at)
        SELECT item_hash, message, -1, 'Max retries exceeded', NOW()
        FROM expired
        ON CONFLICT (item_hash) DO NOTHING
        "#
    )
    .bind(cutoff_timestamp)
    .execute(db)
    .await
    .map_err(|e| GcError::Database(e.to_string()))?;
    
    Ok(result.rows_affected())
}

/// Clean up old rejected messages (older than 7 days)
async fn clean_old_rejected(db: &PgPool) -> Result<u64, GcError> {
    let cutoff = Utc::now() - ChronoDuration::days(7);
    
    let result = sqlx::query(
        "DELETE FROM rejected_messages WHERE rejected_at < $1"
    )
    .bind(cutoff)
    .execute(db)
    .await
    .map_err(|e| GcError::Database(e.to_string()))?;
    
    Ok(result.rows_affected())
}

/// Garbage collector errors
#[derive(Debug, thiserror::Error)]
pub enum GcError {
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("IPFS error: {0}")]
    Ipfs(String),
}

/// Run GC on demand (for testing or manual cleanup)
pub async fn run_gc_now(
    db: &PgPool,
    ipfs: &IpfsService,
    grace_period_secs: i64,
) -> Result<GcStats, GcError> {
    let metrics = Metrics::new();
    run_gc_cycle(db, ipfs, grace_period_secs, &metrics).await
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_gc_stats_default() {
        let stats = GcStats::default();
        assert_eq!(stats.files_removed, 0);
        assert_eq!(stats.bytes_freed, 0);
    }
}
