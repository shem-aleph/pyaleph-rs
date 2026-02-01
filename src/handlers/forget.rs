//! Forget message handler
//!
//! Forget messages request deletion of previously stored content.

use async_trait::async_trait;
use crate::types::{Message, MessageType, ForgetContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for forget messages
pub struct ForgetHandler;

#[async_trait]
impl MessageHandler for ForgetHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Forget
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: ForgetContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        // Must have at least one hash to forget
        if content.hashes.is_empty() {
            return Err(HandlerError::InvalidContent("No hashes to forget".to_string()));
        }
        
        // Verify sender owns the content
        // TODO: Check ownership in database
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: ForgetContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing forget: address={}, hashes={}, reason={:?}",
            content.address,
            content.hashes.len(),
            content.reason
        );
        
        for hash in &content.hashes {
            tracing::debug!("Forgetting hash: {}", hash);
            // TODO: Mark content as forgotten in database
            // TODO: Unpin from IPFS
            // TODO: Delete from local storage (if applicable)
        }
        
        Ok(())
    }
}
