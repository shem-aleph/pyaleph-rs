//! Store message handler
//!
//! Store messages reference files stored on IPFS/Aleph storage.

use async_trait::async_trait;
use crate::types::{Message, MessageType, StoreContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for store messages
pub struct StoreHandler;

#[async_trait]
impl MessageHandler for StoreHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Store
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        // Validate hash format
        if content.item_hash.is_empty() {
            return Err(HandlerError::InvalidContent("Empty item_hash".to_string()));
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing store: address={}, hash={}",
            content.address,
            content.item_hash
        );
        
        // TODO: Pin content to IPFS
        // TODO: Store reference in database
        // TODO: Calculate storage costs
        
        Ok(())
    }
}
