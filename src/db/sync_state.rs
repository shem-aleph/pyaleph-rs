//! Chain sync state persistence
//!
//! Tracks the last synced block for each chain.

use sqlx::PgPool;
use tracing::{debug, info};

use crate::types::Chain;

/// Chain sync state accessor
pub struct SyncStateAccessor;

impl SyncStateAccessor {
    /// Get last synced block for a chain
    pub async fn get_last_block(pool: &PgPool, chain: Chain) -> Result<Option<u64>, sqlx::Error> {
        let result: Option<(i64,)> = sqlx::query_as(
            "SELECT last_block FROM chain_sync_state WHERE chain = $1"
        )
        .bind(chain.to_string())
        .fetch_optional(pool)
        .await?;
        
        Ok(result.map(|(b,)| b as u64))
    }
    
    /// Update last synced block for a chain
    pub async fn update_last_block(
        pool: &PgPool,
        chain: Chain,
        block: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO chain_sync_state (chain, last_block, last_sync)
            VALUES ($1, $2, NOW())
            ON CONFLICT (chain) DO UPDATE SET last_block = $2, last_sync = NOW()
            "#
        )
        .bind(chain.to_string())
        .bind(block as i64)
        .execute(pool)
        .await?;
        
        debug!("Updated sync state for {}: block {}", chain, block);
        Ok(())
    }
    
    /// Get all sync states
    pub async fn get_all(pool: &PgPool) -> Result<Vec<ChainSyncState>, sqlx::Error> {
        let results = sqlx::query_as::<_, ChainSyncStateDb>(
            "SELECT chain, last_block, last_sync FROM chain_sync_state"
        )
        .fetch_all(pool)
        .await?;
        
        Ok(results.into_iter().map(|r| ChainSyncState {
            chain: r.chain,
            last_block: r.last_block as u64,
            last_sync: r.last_sync,
        }).collect())
    }
    
    /// Initialize sync state for a chain (if not exists)
    pub async fn init_chain(
        pool: &PgPool,
        chain: Chain,
        start_block: u64,
    ) -> Result<u64, sqlx::Error> {
        // Try to get existing state
        if let Some(block) = Self::get_last_block(pool, chain).await? {
            return Ok(block);
        }
        
        // Initialize with start block
        sqlx::query(
            r#"
            INSERT INTO chain_sync_state (chain, last_block, last_sync)
            VALUES ($1, $2, NOW())
            ON CONFLICT (chain) DO NOTHING
            "#
        )
        .bind(chain.to_string())
        .bind(start_block as i64)
        .execute(pool)
        .await?;
        
        info!("Initialized sync state for {}: starting at block {}", chain, start_block);
        Ok(start_block)
    }
}

/// Chain sync state from database
#[derive(Debug, Clone, sqlx::FromRow)]
struct ChainSyncStateDb {
    chain: String,
    last_block: i64,
    last_sync: chrono::DateTime<chrono::Utc>,
}

/// Chain sync state
#[derive(Debug, Clone)]
pub struct ChainSyncState {
    pub chain: String,
    pub last_block: u64,
    pub last_sync: chrono::DateTime<chrono::Utc>,
}
