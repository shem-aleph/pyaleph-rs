//! Store message handler - optimized version

use async_trait::async_trait;

use crate::types::{Message, MessageType, StoreContent, ItemType};
use super::{HandlerContext, HandlerError, MessageHandler};

pub struct StoreHandler;

impl StoreHandler {
    fn validate_ipfs_hash(hash: &str) -> bool {
        if hash.starts_with("Qm") && hash.len() == 46 {
            return true;
        }
        if hash.starts_with("b") && hash.len() >= 50 {
            return true;
        }
        if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return true;
        }
        false
    }
}

#[async_trait]
impl MessageHandler for StoreHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Store
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid store content: {}", e)))?;
        
        // Note: sender != content.address is allowed for delegated authorization.
        // The permission check happens in check_permissions() via the security aggregate system.
        
        if content.item_hash.is_empty() {
            return Err(HandlerError::InvalidContent("Empty item_hash".to_string()));
        }
        
        if matches!(content.item_type, ItemType::Ipfs) {
            if !Self::validate_ipfs_hash(&content.item_hash) {
                return Err(HandlerError::InvalidContent(format!(
                    "Invalid IPFS hash format: {}",
                    content.item_hash
                )));
            }
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        let file_size = content.size.unwrap_or(0) as i64;
        
        if let Some(ref pool) = ctx.pool {
            sqlx::query(
                "INSERT INTO file_pins (item_hash, owner, size, content_type)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (item_hash, owner) DO UPDATE SET
                   size = EXCLUDED.size,
                   content_type = COALESCE(EXCLUDED.content_type, file_pins.content_type)"
            )
            .bind(&content.item_hash)
            .bind(&content.address)
            .bind(file_size)
            .bind(&content.content_type)
            .execute(pool)
            .await
            .map_err(|e| HandlerError::Database(e.to_string()))?;
            
            tracing::debug!("Stored file_pin: {}", content.item_hash);
        }
        
        Ok(())
    }
}
