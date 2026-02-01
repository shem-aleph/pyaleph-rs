//! Aggregate message handler
//!
//! Aggregates are key-value updates for an address.

use async_trait::async_trait;
use crate::types::{Message, MessageType, AggregateContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for aggregate messages
pub struct AggregateHandler;

#[async_trait]
impl MessageHandler for AggregateHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Aggregate
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        // Parse content
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let _content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        // Verify sender matches content address
        // TODO: Add more validation
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: AggregateContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing aggregate: address={}, key={}",
            content.address,
            content.key
        );
        
        // TODO: Store in database
        // TODO: Handle amend if key already exists
        
        Ok(())
    }
}
