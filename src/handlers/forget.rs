//! Forget message handler
//!
//! Forget messages request deletion of previously stored content.
//! Critical protections:
//! - Cannot forget content owned by others (without permission)
//! - Cannot forget files used as VM volumes
//! - Cannot forget a forget message
//! - Cannot forget already-forgotten content (duplicate check)
//!
//! Reference: aleph/handlers/content/forget.py

use async_trait::async_trait;

use crate::types::{Message, MessageType, ForgetContent, ErrorCode};
use super::{HandlerContext, HandlerError, MessageHandler};

/// Handler for forget messages
pub struct ForgetHandler;

impl ForgetHandler {
    /// Check if a hash is for a forget message type
    async fn is_forget_message(
        &self,
        item_hash: &str,
        ctx: &HandlerContext,
    ) -> Result<bool, HandlerError> {
        if let Some(ref db) = ctx.db {
            if let Some(message) = db.get_message(item_hash).await
                .map_err(|e| HandlerError::Database(e))?
            {
                return Ok(message.message_type == MessageType::Forget);
            }
        }
        Ok(false)
    }
    
    /// Check if a file hash is used by any VMs
    async fn check_vm_dependencies(
        &self,
        file_hash: &str,
        ctx: &HandlerContext,
    ) -> Result<Option<String>, HandlerError> {
        if let Some(ref db) = ctx.db {
            let dependent_vms = db.get_dependent_vms(file_hash).await
                .map_err(|e| HandlerError::Database(e))?;
            
            if !dependent_vms.is_empty() {
                return Ok(Some(dependent_vms[0].clone()));
            }
        }
        Ok(None)
    }
    
    /// Check ownership of content
    async fn check_ownership(
        &self,
        item_hash: &str,
        sender: &str,
        ctx: &HandlerContext,
    ) -> Result<bool, HandlerError> {
        if let Some(ref db) = ctx.db {
            // Check if it's a message
            if let Some(message) = db.get_message(item_hash).await
                .map_err(|e| HandlerError::Database(e))?
            {
                return Ok(message.sender.to_lowercase() == sender.to_lowercase());
            }
            
            // Check if it's a file pin
            if let Some(pin) = db.get_file_pin(item_hash).await
                .map_err(|e| HandlerError::Database(e))?
            {
                return Ok(pin.owner.to_lowercase() == sender.to_lowercase());
            }
            
            // Content not found - could be aggregate, post, etc.
            // Try post
            if let Some(post) = db.get_post(item_hash).await
                .map_err(|e| HandlerError::Database(e))?
            {
                return Ok(post.address.to_lowercase() == sender.to_lowercase());
            }
        }
        
        // If we can't verify ownership, deny by default
        Ok(false)
    }
}

#[async_trait]
impl MessageHandler for ForgetHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Forget
    }
    
    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: ForgetContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid forget content: {}", e)))?;
        
        // Must have at least one hash to forget
        if content.hashes.is_empty() {
            return Err(HandlerError::InvalidContent(
                "Forget message must specify at least one hash to forget".to_string()
            ));
        }
        
        // Note: sender != content.address is allowed for delegated authorization.
        // The permission check happens in check_permissions() via the security aggregate system.
        
        // Verify signature
        if let Some(ref crypto) = ctx.crypto {
            if !message.verify_signature(crypto).map_err(|e| HandlerError::InvalidSignature(e))? {
                return Err(HandlerError::InvalidSignature("Signature verification failed".to_string()));
            }
        }
        
        // Validate each hash
        for hash in &content.hashes {
            // Check if trying to forget a forget message
            if self.is_forget_message(hash, ctx).await? {
                return Err(HandlerError::NotAllowed(format!(
                    "Cannot forget a forget message: {}",
                    hash
                )));
            }
            
            // Check if already forgotten
            if let Some(ref db) = ctx.db {
                let already_forgotten = db.get_forgotten_hashes(&[hash.clone()]).await
                    .map_err(|e| HandlerError::Database(e))?;
                
                if !already_forgotten.is_empty() {
                    return Err(HandlerError::Duplicate(format!(
                        "Content already forgotten: {}",
                        hash
                    )));
                }
            }
            
            // Check ownership
            if !self.check_ownership(hash, &message.sender, ctx).await? {
                return Err(HandlerError::PermissionDenied(format!(
                    "Not authorized to forget content: {}",
                    hash
                )));
            }
            
            // Check VM dependencies
            if let Some(vm_hash) = self.check_vm_dependencies(hash, ctx).await? {
                return Err(HandlerError::NotAllowed(format!(
                    "Cannot forget file {} - it is used by VM {}",
                    hash, vm_hash
                )));
            }
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
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

            if let Some(ref db) = ctx.db {
                // Look up the message to get its type for derived table cleanup
                let message_type = db.get_message(hash).await
                    .map_err(|e| HandlerError::Database(e))?
                    .map(|m| m.message_type.to_string());

                // Mark as forgotten (insert into forgotten_messages)
                db.mark_forgotten(
                    hash,
                    &message.item_hash,
                    content.reason.as_deref(),
                ).await.map_err(|e| HandlerError::Database(e))?;

                // Remove file pin if it exists
                db.remove_file_pin(hash, &content.address).await
                    .map_err(|e| HandlerError::Database(e))?;

                // Delete from derived tables based on message type
                if let Some(ref msg_type) = message_type {
                    db.delete_derived_data(hash, msg_type).await
                        .map_err(|e| HandlerError::Database(e))?;
                }

                // Delete from messages table (pyaleph removes forgotten messages from messages)
                db.delete_message(hash).await
                    .map_err(|e| HandlerError::Database(e))?;
            }

            // Unpin from IPFS
            if let Some(ref ipfs) = ctx.ipfs {
                if let Err(e) = ipfs.unpin(hash).await {
                    // Log but don't fail - the content might not be pinned locally
                    tracing::warn!("Failed to unpin {} from IPFS: {}", hash, e);
                }
            }
        }
        
        // Update message status to show it's been processed
        if let Some(ref db) = ctx.db {
            db.update_message_status(&message.item_hash, &crate::types::ProcessingStatus::processed())
                .await
                .map_err(|e| HandlerError::Database(e))?;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Add tests for forget validation logic
}
