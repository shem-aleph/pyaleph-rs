//! Aggregate message handler
//!
//! Aggregates are key-value updates for an address. They support:
//! - Creating new key-value pairs
//! - Updating existing keys (merging content)
//! - Out-of-order message handling via timestamps
//!
//! Reference: aleph/handlers/content/aggregate.py

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use crate::types::{Message, MessageType, AggregateContent, ErrorCode};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Maximum number of aggregate elements before triggering a refresh
const DIRTY_AGGREGATE_THRESHOLD: usize = 1000;

/// Handler for aggregate messages
pub struct AggregateHandler;

impl AggregateHandler {
    /// Merge two JSON values deeply
    /// 
    /// Objects are merged recursively, other types are replaced.
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
    
    /// Build aggregate from individual elements
    /// 
    /// This handles out-of-order messages by sorting by timestamp.
    /// Uses iterators for efficient processing.
    fn build_aggregate_from_elements(elements: &[AggregateElement]) -> Value {
        // Early return for empty input
        if elements.is_empty() {
            return Value::Object(serde_json::Map::new());
        }
        
        // Sort elements by timestamp using a sorted reference slice
        let mut sorted_indices: Vec<usize> = (0..elements.len()).collect();
        sorted_indices.sort_by(|&a, &b| {
            elements[a].time
                .partial_cmp(&elements[b].time)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Apply each element in order using fold for cleaner iteration
        sorted_indices
            .into_iter()
            .map(|i| &elements[i])
            .fold(Value::Object(serde_json::Map::new()), |mut acc, element| {
                Self::deep_merge(&mut acc, &element.content);
                acc
            })
    }
    
    /// Check if an aggregate needs a full refresh
    fn needs_refresh(element_count: usize, new_key_conflicts: bool) -> bool {
        element_count >= DIRTY_AGGREGATE_THRESHOLD || new_key_conflicts
    }
}

/// Represents a single aggregate element (one message's contribution)
#[derive(Debug, Clone)]
pub struct AggregateElement {
    pub item_hash: String,
    pub key: String,
    pub content: Value,
    pub time: f64,
}

#[async_trait]
impl MessageHandler for AggregateHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Aggregate
    }
    
    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        // Parse content (use as_deref to avoid clone)
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid aggregate content: {}", e)))?;
        
        // Verify sender matches content address
        // The aggregate is stored under content.address, and the message sender must be authorized
        if message.sender.to_lowercase() != content.address.to_lowercase() {
            return Err(HandlerError::Unauthorized);
        }
        
        // Validate key is not empty
        if content.key.is_empty() {
            return Err(HandlerError::InvalidContent("Aggregate key cannot be empty".to_string()));
        }
        
        // Validate content is an object
        if !content.content.is_object() {
            return Err(HandlerError::InvalidContent("Aggregate content must be a JSON object".to_string()));
        }
        
        // Verify signature
        if let Some(ref crypto) = ctx.crypto {
            if !message.verify_signature(crypto).map_err(|e| HandlerError::InvalidContent(e))? {
                return Err(HandlerError::InvalidContent("Invalid signature".to_string()));
            }
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing aggregate: address={}, key={}",
            content.address,
            content.key
        );
        
        // Create the aggregate element from this message
        let element = AggregateElement {
            item_hash: message.item_hash.clone(),
            key: content.key.clone(),
            content: content.content.clone(),
            time: content.time,
        };
        
        // Store the element
        if let Some(ref db) = ctx.db {
            // Check if aggregate exists
            let existing = db.get_aggregate(&content.address, &content.key).await
                .map_err(|e| HandlerError::Database(e.to_string()))?;
            
            // Store the new element
            db.store_aggregate_element(&content.address, &element).await
                .map_err(|e| HandlerError::Database(e.to_string()))?;
            
            // Get all elements for this aggregate
            let elements = db.get_aggregate_elements(&content.address, &content.key).await
                .map_err(|e| HandlerError::Database(e.to_string()))?;
            
            // Check if we need to handle out-of-order messages or conflicts
            let has_conflicts = existing.is_some() && {
                // Check if the new content has keys that conflict with existing
                if let (Some(existing_obj), Value::Object(new_obj)) = (&existing, &content.content) {
                    if let Value::Object(existing_map) = existing_obj {
                        new_obj.keys().any(|k| existing_map.contains_key(k))
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            
            // Determine if we need a full refresh
            if Self::needs_refresh(elements.len(), has_conflicts) {
                tracing::info!(
                    "Aggregate {}/{} marked dirty, performing full refresh",
                    content.address,
                    content.key
                );
                
                // Rebuild aggregate from all elements
                let rebuilt = Self::build_aggregate_from_elements(&elements);
                
                // Store the rebuilt aggregate
                db.store_aggregate(&content.address, &content.key, &rebuilt, message.time).await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;
                
                // Mark as clean
                db.mark_aggregate_clean(&content.address, &content.key).await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;
            } else {
                // Simple append/merge
                let mut current = existing.unwrap_or(Value::Object(serde_json::Map::new()));
                
                // Only apply if this message is newer than what we have
                let current_time = db.get_aggregate_time(&content.address, &content.key).await
                    .map_err(|e| HandlerError::Database(e.to_string()))?
                    .unwrap_or(0.0);
                
                if content.time >= current_time {
                    Self::deep_merge(&mut current, &content.content);
                    
                    db.store_aggregate(&content.address, &content.key, &current, content.time).await
                        .map_err(|e| HandlerError::Database(e.to_string()))?;
                } else {
                    // Out-of-order message - mark as dirty for later refresh
                    tracing::debug!(
                        "Out-of-order aggregate message for {}/{}, marking dirty",
                        content.address,
                        content.key
                    );
                    db.mark_aggregate_dirty(&content.address, &content.key).await
                        .map_err(|e| HandlerError::Database(e.to_string()))?;
                }
            }
        } else {
            tracing::warn!("No database configured, aggregate not persisted");
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_deep_merge_simple() {
        let mut base = serde_json::json!({"a": 1, "b": 2});
        let update = serde_json::json!({"b": 3, "c": 4});
        
        AggregateHandler::deep_merge(&mut base, &update);
        
        assert_eq!(base, serde_json::json!({"a": 1, "b": 3, "c": 4}));
    }
    
    #[test]
    fn test_deep_merge_nested() {
        let mut base = serde_json::json!({
            "settings": {"theme": "dark", "lang": "en"},
            "count": 5
        });
        let update = serde_json::json!({
            "settings": {"theme": "light"},
            "count": 10
        });
        
        AggregateHandler::deep_merge(&mut base, &update);
        
        assert_eq!(base, serde_json::json!({
            "settings": {"theme": "light", "lang": "en"},
            "count": 10
        }));
    }
    
    #[test]
    fn test_build_aggregate_from_elements() {
        let elements = vec![
            AggregateElement {
                item_hash: "hash1".to_string(),
                key: "test".to_string(),
                content: serde_json::json!({"a": 1}),
                time: 1000.0,
            },
            AggregateElement {
                item_hash: "hash2".to_string(),
                key: "test".to_string(),
                content: serde_json::json!({"a": 2, "b": 1}),
                time: 2000.0,
            },
            AggregateElement {
                item_hash: "hash3".to_string(),
                key: "test".to_string(),
                content: serde_json::json!({"c": 3}),
                time: 1500.0, // Out of order!
            },
        ];
        
        let result = AggregateHandler::build_aggregate_from_elements(&elements);
        
        // Should be: {a: 1} -> {a: 2, c: 3} -> {a: 2, b: 1, c: 3} (sorted by time)
        assert_eq!(result, serde_json::json!({"a": 2, "b": 1, "c": 3}));
    }
}
