//! Aggregate message handler - optimized version
//!
//! Handles out-of-order aggregate messages by detecting timestamp inversions
//! and rebuilding the aggregate from all stored elements in chronological order.
//!
//! Reference: aleph/handlers/content/aggregate.py

use async_trait::async_trait;
use serde_json::Value;

use crate::types::{Message, MessageType, AggregateContent};
use super::{HandlerContext, HandlerError, MessageHandler};

pub struct AggregateHandler;

impl AggregateHandler {
    fn deep_merge(base: &mut Value, update: &Value) {
        match (base, update) {
            (Value::Object(base_map), Value::Object(update_map)) => {
                for (key, value) in update_map {
                    let entry = base_map.entry(key.clone()).or_insert(Value::Null);
                    Self::deep_merge(entry, value);
                }
            }
            (base, update) => {
                *base = update.clone();
            }
        }
    }

    /// Rebuild an aggregate from all stored elements in chronological order.
    ///
    /// This is called when out-of-order messages are detected. It fetches all
    /// aggregate_elements for (address, key), sorts by time ASC, and deep merges
    /// them chronologically to produce the correct final state.
    ///
    /// Reference: aleph/handlers/content/aggregate.py rebuild_aggregate()
    pub(crate) async fn rebuild_aggregate_from_elements(
        pool: &sqlx::PgPool,
        address: &str,
        key: &str,
    ) -> Result<(), HandlerError> {
        // Fetch all elements ordered chronologically
        let elements: Vec<(String, Value, f64)> = sqlx::query_as(
            "SELECT item_hash, content, time FROM aggregate_elements
             WHERE address = $1 AND key = $2
             ORDER BY time ASC, item_hash ASC"
        )
        .bind(address)
        .bind(key)
        .fetch_all(pool)
        .await
        .map_err(|e| HandlerError::Database(e.to_string()))?;

        if elements.is_empty() {
            return Ok(());
        }

        // Deep merge all elements in order
        let mut merged = Value::Object(serde_json::Map::new());
        let mut latest_hash = String::new();
        let mut latest_time = 0.0_f64;

        for (hash, content, time) in &elements {
            Self::deep_merge(&mut merged, content);
            latest_hash = hash.clone();
            latest_time = *time;
        }

        // Update the aggregate with the rebuilt content
        sqlx::query(
            "UPDATE aggregates SET content = $1, time = $2, last_revision_hash = $3, dirty = false
             WHERE address = $4 AND key = $5"
        )
        .bind(&merged)
        .bind(latest_time)
        .bind(&latest_hash)
        .bind(address)
        .bind(key)
        .execute(pool)
        .await
        .map_err(|e| HandlerError::Database(e.to_string()))?;

        tracing::info!(
            "Rebuilt aggregate {}/{} from {} elements",
            address, key, elements.len()
        );

        Ok(())
    }
}

#[async_trait]
impl MessageHandler for AggregateHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Aggregate
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid aggregate content: {}", e)))?;
        
        // Note: sender != content.address is allowed for delegated authorization.
        // The permission check happens in check_permissions() via the security aggregate system.
        
        if content.key.is_empty() {
            return Err(HandlerError::InvalidContent("Aggregate key cannot be empty".to_string()));
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;

        let content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;

        if let Some(ref pool) = ctx.pool {
            // Insert into aggregate_elements first (always, for tracking and rebuild)
            sqlx::query(
                "INSERT INTO aggregate_elements (address, key, item_hash, content, time)
                 SELECT $1, $2, $3, $4, $5
                 WHERE NOT EXISTS (SELECT 1 FROM aggregate_elements WHERE item_hash = $3)"
            )
            .bind(&content.address)
            .bind(&content.key)
            .bind(&message.item_hash)
            .bind(&content.content)
            .bind(content.time)
            .execute(pool)
            .await
            .map_err(|e| HandlerError::Database(e.to_string()))?;

            // Optimistic insert - try INSERT first, skip SELECT for new keys
            let insert_result = sqlx::query(
                "INSERT INTO aggregates (address, key, content, time, last_revision_hash, dirty)
                 VALUES ($1, $2, $3, $4, $5, false)
                 ON CONFLICT (address, key) DO NOTHING"
            )
            .bind(&content.address)
            .bind(&content.key)
            .bind(&content.content)
            .bind(content.time)
            .bind(&message.item_hash)
            .execute(pool)
            .await
            .map_err(|e| HandlerError::Database(e.to_string()))?;

            // If no rows affected, key exists - need to merge or rebuild
            if insert_result.rows_affected() == 0 {
                // Fetch existing state including dirty flag
                let existing_row: Option<(Value, f64, bool)> = sqlx::query_as(
                    "SELECT content, time, dirty FROM aggregates WHERE address = $1 AND key = $2"
                )
                .bind(&content.address)
                .bind(&content.key)
                .fetch_optional(pool)
                .await
                .map_err(|e| HandlerError::Database(e.to_string()))?;

                if let Some((existing_content, existing_time, is_dirty)) = existing_row {
                    if is_dirty {
                        // Already dirty — element is stored, skip aggregate update entirely.
                        // Will be rebuilt lazily on next API read.
                        tracing::debug!(
                            "Aggregate {}/{} already dirty, skipping update",
                            content.address, content.key
                        );
                    } else if content.time >= existing_time {
                        // Normal case: new message is newer, deep merge on top
                        let mut merged = existing_content;
                        Self::deep_merge(&mut merged, &content.content);

                        sqlx::query(
                            "UPDATE aggregates SET content = $1, time = $2, last_revision_hash = $3, dirty = false
                             WHERE address = $4 AND key = $5"
                        )
                        .bind(&merged)
                        .bind(content.time)
                        .bind(&message.item_hash)
                        .bind(&content.address)
                        .bind(&content.key)
                        .execute(pool)
                        .await
                        .map_err(|e| HandlerError::Database(e.to_string()))?;
                    } else {
                        // Out-of-order: new message is older than existing aggregate.
                        // Reference: aleph/handlers/content/aggregate.py — dirty threshold
                        // Check element count: if > 1000, mark dirty instead of rebuilding
                        let count: (i64,) = sqlx::query_as(
                            "SELECT COUNT(*) FROM aggregate_elements WHERE address = $1 AND key = $2"
                        )
                        .bind(&content.address)
                        .bind(&content.key)
                        .fetch_one(pool)
                        .await
                        .map_err(|e| HandlerError::Database(e.to_string()))?;

                        if count.0 > 1000 {
                            // Large aggregate — mark dirty for lazy refresh
                            sqlx::query(
                                "UPDATE aggregates SET dirty = true WHERE address = $1 AND key = $2"
                            )
                            .bind(&content.address)
                            .bind(&content.key)
                            .execute(pool)
                            .await
                            .map_err(|e| HandlerError::Database(e.to_string()))?;

                            tracing::info!(
                                "Out-of-order aggregate {}/{}: {} elements > threshold, marked dirty",
                                content.address, content.key, count.0
                            );
                        } else {
                            // Small aggregate — rebuild immediately
                            tracing::debug!(
                                "Out-of-order aggregate {}/{}: {} elements, rebuilding",
                                content.address, content.key, count.0
                            );
                            Self::rebuild_aggregate_from_elements(pool, &content.address, &content.key).await?;
                        }
                    }
                }
            }

            tracing::debug!("Stored aggregate: {}/{}", content.address, content.key);
        }

        Ok(())
    }
}
