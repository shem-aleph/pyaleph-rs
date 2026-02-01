//! Post message handler
//!
//! Posts are content entries that can be:
//! - Original posts: New content with a type and data
//! - Amend posts: Updates to existing posts (type = "amend")
//!
//! Reference: aleph/handlers/content/post.py

use async_trait::async_trait;

use crate::types::{Message, MessageType, PostContent, ErrorCode};
use super::{HandlerContext, HandlerError, MessageHandler, PostRecord};

/// Handler for post messages
pub struct PostHandler;

impl PostHandler {
    /// Check if this is an amend post
    fn is_amend(post_type: &str) -> bool {
        post_type.to_lowercase() == "amend"
    }
    
    /// Validate an amend post against its target
    async fn validate_amend(
        &self,
        message: &Message,
        content: &PostContent,
        ctx: &HandlerContext,
    ) -> Result<(), HandlerError> {
        // Amend posts MUST have a ref field
        let target_ref = content.ref_.as_ref()
            .ok_or_else(|| HandlerError::InvalidContent(
                "Amend post requires a 'ref' field pointing to the original post".to_string()
            ))?;
        
        // Look up the target post
        let db = ctx.db.as_ref()
            .ok_or_else(|| HandlerError::Database("No database configured".to_string()))?;
        
        let target_post = db.get_post(target_ref).await
            .map_err(|e| HandlerError::Database(e))?
            .ok_or_else(|| HandlerError::TargetNotFound(format!(
                "Target post {} not found",
                target_ref
            )))?;
        
        // Verify ownership - only the original poster can amend
        if target_post.address.to_lowercase() != message.sender.to_lowercase() {
            return Err(HandlerError::PermissionDenied(
                "Only the original poster can amend a post".to_string()
            ));
        }
        
        // Cannot amend an amend (must amend the original)
        if Self::is_amend(&target_post.post_type) {
            // Get the original to provide a helpful error
            let original_hash = target_post.original_item_hash.as_ref()
                .unwrap_or(&target_post.item_hash);
            return Err(HandlerError::NotAllowed(format!(
                "Cannot amend an amend post. Amend the original post {} instead",
                original_hash
            )));
        }
        
        Ok(())
    }
}

#[async_trait]
impl MessageHandler for PostHandler {
    fn message_type(&self) -> MessageType {
        MessageType::Post
    }
    
    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: PostContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid post content: {}", e)))?;
        
        // Verify sender matches content address
        if message.sender.to_lowercase() != content.address.to_lowercase() {
            return Err(HandlerError::Unauthorized);
        }
        
        // Validate post type is not empty
        if content.post_type.is_empty() {
            return Err(HandlerError::InvalidContent("Post type cannot be empty".to_string()));
        }
        
        // Verify signature
        if let Some(ref crypto) = ctx.crypto {
            if !message.verify_signature(crypto).map_err(|e| HandlerError::InvalidSignature(e))? {
                return Err(HandlerError::InvalidSignature("Signature verification failed".to_string()));
            }
        }
        
        // If this is an amend, validate against the target
        if Self::is_amend(&content.post_type) {
            self.validate_amend(message, &content, ctx).await?;
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: PostContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        let is_amend = Self::is_amend(&content.post_type);
        
        tracing::info!(
            "Processing post: address={}, type={}, amend={}",
            content.address,
            content.post_type,
            is_amend
        );
        
        // Get the original hash for amends
        let original_item_hash = if is_amend {
            // Safe to use ok_or - validate() already verified ref exists for amends
            let target_ref = content.ref_.as_deref()
                .ok_or_else(|| HandlerError::InvalidContent(
                    "Amend post missing ref field".to_string()
                ))?;
            
            // Look up target to determine the original hash
            match &ctx.db {
                Some(db) => {
                    db.get_post(target_ref).await
                        .map_err(HandlerError::Database)?
                        .map(|target| {
                            // If target has an original, use that; otherwise use target's hash
                            target.original_item_hash.unwrap_or(target.item_hash)
                        })
                        .or_else(|| Some(target_ref.to_string()))
                }
                None => Some(target_ref.to_string()),
            }
        } else {
            None
        };
        
        // Create post record
        let record = PostRecord {
            item_hash: message.item_hash.clone(),
            address: content.address.clone(),
            post_type: content.post_type.clone(),
            ref_: content.ref_.clone(),
            content: content.content.clone(),
            channel: message.channel.clone(),
            time: content.time,
            original_item_hash: original_item_hash.clone(),
            latest_amend: None, // Will be set on the original
        };
        
        // Store in database
        if let Some(ref db) = ctx.db {
            db.store_post(&record).await
                .map_err(|e| HandlerError::Database(e))?;
            
            // If this is an amend, update the original's latest_amend
            if is_amend {
                if let Some(ref original_hash) = original_item_hash {
                    db.update_post_latest_amend(original_hash, &message.item_hash).await
                        .map_err(|e| HandlerError::Database(e))?;
                    
                    tracing::debug!(
                        "Updated latest_amend for {} to {}",
                        original_hash,
                        message.item_hash
                    );
                }
            }
        } else {
            tracing::warn!("No database configured, post not persisted");
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_is_amend() {
        assert!(PostHandler::is_amend("amend"));
        assert!(PostHandler::is_amend("AMEND"));
        assert!(PostHandler::is_amend("Amend"));
        assert!(!PostHandler::is_amend("post"));
        assert!(!PostHandler::is_amend("blog"));
    }
}
