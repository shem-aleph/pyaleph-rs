//! Instance message handler
//!
//! Instances are persistent VMs running on Aleph compute nodes.

use async_trait::async_trait;
use crate::types::{Message, MessageType, InstanceContent};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for instance messages
pub struct InstanceHandler;

#[async_trait]
impl MessageHandler for InstanceHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Instance
    }
    
    async fn validate(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: InstanceContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        // Validate resource requirements
        if content.memory == 0 {
            return Err(HandlerError::InvalidContent("Memory must be > 0".to_string()));
        }
        
        if content.vcpus == 0 {
            return Err(HandlerError::InvalidContent("vCPUs must be > 0".to_string()));
        }
        
        // Validate payment for paid instances
        if let Some(payment) = &content.payment {
            // TODO: Verify payment based on type
            tracing::debug!("Payment type: {:?}", payment.payment_type);
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, _ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: InstanceContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing instance: address={}, memory={}MB, vcpus={}, ssh_keys={}",
            content.address,
            content.memory,
            content.vcpus,
            content.ssh_keys.len()
        );
        
        // TODO: Verify rootfs hash exists
        // TODO: Calculate compute costs
        // TODO: Verify payment/balance
        // TODO: Store instance in database
        // TODO: Signal to compute nodes for scheduling
        
        Ok(())
    }
}
