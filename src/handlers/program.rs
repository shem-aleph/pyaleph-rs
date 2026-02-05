//! Program message handler
//!
//! Programs are serverless functions that run on Aleph compute nodes.

use async_trait::async_trait;
use crate::types::{Message, MessageType, ProgramContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for program messages
pub struct ProgramHandler;

#[async_trait]
impl MessageHandler for ProgramHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Program
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: ProgramContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        // Validate resource requirements
        if content.resources.memory == 0 {
            return Err(HandlerError::InvalidContent("Memory must be > 0".to_string()));
        }

        if content.resources.vcpus == 0 {
            return Err(HandlerError::InvalidContent("vCPUs must be > 0".to_string()));
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: ProgramContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing program: address={}, memory={}MB, vcpus={}",
            content.address,
            content.resources.memory,
            content.resources.vcpus
        );
        
        // TODO: Verify code hash exists
        // TODO: Verify runtime hash exists
        // TODO: Calculate compute costs
        // TODO: Store program in database
        // TODO: Signal to compute nodes
        
        Ok(())
    }
}
