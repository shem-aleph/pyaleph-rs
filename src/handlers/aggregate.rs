//! Aggregate message handler - optimized version

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
        
        if message.sender.to_lowercase() != content.address.to_lowercase() {
            return Err(HandlerError::Unauthorized);
        }
        
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
            
            // If no rows affected, key exists - need to merge
            if insert_result.rows_affected() == 0 {
                // Fetch existing and merge
                let existing_row: Option<(Value, f64)> = sqlx::query_as(
                    "SELECT content, time FROM aggregates WHERE address = $1 AND key = $2"
                )
                .bind(&content.address)
                .bind(&content.key)
                .fetch_optional(pool)
                .await
                .map_err(|e| HandlerError::Database(e.to_string()))?;
                
                if let Some((existing_content, existing_time)) = existing_row {
                    let mut merged = existing_content;
                    Self::deep_merge(&mut merged, &content.content);
                    let final_time = if content.time >= existing_time { content.time } else { existing_time };
                    
                    sqlx::query(
                        "UPDATE aggregates SET content = $1, time = $2, last_revision_hash = $3, dirty = false
                         WHERE address = $4 AND key = $5"
                    )
                    .bind(&merged)
                    .bind(final_time)
                    .bind(&message.item_hash)
                    .bind(&content.address)
                    .bind(&content.key)
                    .execute(pool)
                    .await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;
                }
            }
            
            tracing::debug!("Stored aggregate: {}/{}", content.address, content.key);
        }
        
        Ok(())
    }
}
