//! Post message handler
//!
//! Posts are immutable content entries.

use async_trait::async_trait;
use crate::types::{Message, MessageType, PostContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for post messages
pub struct PostHandler;

#[async_trait]
impl MessageHandler for PostHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Post
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let _content: PostContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: PostContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing post: address={}, type={}",
            content.address,
            content.post_type
        );
        
        // TODO: Store in database
        
        Ok(())
    }
}
