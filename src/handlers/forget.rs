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

use crate::types::{Message, MessageType, ForgetContent, StoreContent};
use super::{HandlerContext, HandlerError, MessageHandler};
use super::store::make_file_tag;
use super::aggregate::AggregateHandler;

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
    
    /// Handle forgetting a STORE message with grace period logic.
    ///
    /// Instead of immediately unpinning from IPFS, we:
    /// 1. Delete the message-type pin for this file
    /// 2. Refresh the file tag to point to the next newest pin
    /// 3. If no active pins remain, insert a grace period pin (24h TTL)
    ///    so the garbage collector can clean up later
    async fn process_store_forget(
        &self,
        hash: &str,
        address: &str,
        target_message: &Option<Message>,
        db: &dyn super::Database,
    ) -> Result<(), HandlerError> {
        // Parse the STORE content to get the file hash and ref for tag computation
        let (file_hash, ref_) = if let Some(msg) = target_message {
            if let Some(ref content_str) = msg.item_content {
                if let Ok(store_content) = serde_json::from_str::<StoreContent>(content_str) {
                    (store_content.item_hash.clone(), store_content.ref_.clone())
                } else {
                    (hash.to_string(), None)
                }
            } else {
                (hash.to_string(), None)
            }
        } else {
            (hash.to_string(), None)
        };

        // Delete the message-type pin for this forgotten message
        db.delete_file_pin_by_message(hash).await
            .map_err(|e| HandlerError::Database(e))?;

        // Refresh the file tag to point to the next newest pin (or delete if none remain)
        let tag = make_file_tag(address, ref_.as_deref(), &file_hash);
        db.refresh_file_tag(&tag, address).await
            .map_err(|e| HandlerError::Database(e))?;

        // Check if any active (non-grace-period) pins remain for this file
        let active_pins = db.count_active_pins(&file_hash).await
            .map_err(|e| HandlerError::Database(e))?;

        if active_pins == 0 {
            // No active pins — schedule for garbage collection with 24h grace period
            let delete_by = chrono::Utc::now() + chrono::Duration::hours(24);
            db.insert_grace_period_pin(&file_hash, address, delete_by).await
                .map_err(|e| HandlerError::Database(e))?;

            tracing::info!(
                "File {} has no active pins, grace period until {}",
                file_hash, delete_by
            );
        } else {
            tracing::debug!(
                "File {} still has {} active pin(s), no grace period needed",
                file_hash, active_pins
            );
        }

        Ok(())
    }

    /// Cascade forget to related data based on message type.
    ///
    /// - Post: forget all amends pointing to this post
    /// - Aggregate: delete from aggregate_elements, rebuild or delete parent aggregate
    /// - Program/Instance: delete VM versions and cost records
    ///
    /// Reference: aleph/handlers/content/forget.py _forget_by_type()
    async fn cascade_forget(
        &self,
        hash: &str,
        message_type: &MessageType,
        pool: &sqlx::PgPool,
        db: &dyn super::Database,
    ) -> Result<(), HandlerError> {
        match message_type {
            MessageType::Post => {
                // Forget all amends pointing to this original post
                let amend_hashes: Vec<(String,)> = sqlx::query_as(
                    "SELECT item_hash FROM posts WHERE original_item_hash = $1"
                )
                .bind(hash)
                .fetch_all(pool)
                .await
                .map_err(|e| HandlerError::Database(e.to_string()))?;

                for (amend_hash,) in &amend_hashes {
                    db.mark_forgotten(amend_hash, hash, Some("parent post forgotten")).await
                        .map_err(|e| HandlerError::Database(e))?;
                    db.delete_derived_data(amend_hash, "POST").await
                        .map_err(|e| HandlerError::Database(e))?;
                    db.delete_message(amend_hash).await
                        .map_err(|e| HandlerError::Database(e))?;
                    tracing::debug!("Cascade-forgot amend post: {}", amend_hash);
                }
            }
            MessageType::Aggregate => {
                // Find which aggregate this element belongs to
                let agg_info: Option<(String, String)> = sqlx::query_as(
                    "SELECT address, key FROM aggregate_elements WHERE item_hash = $1"
                )
                .bind(hash)
                .fetch_optional(pool)
                .await
                .map_err(|e| HandlerError::Database(e.to_string()))?;

                // Delete the aggregate element
                sqlx::query("DELETE FROM aggregate_elements WHERE item_hash = $1")
                    .bind(hash)
                    .execute(pool)
                    .await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;

                // Rebuild or delete the parent aggregate
                if let Some((address, key)) = agg_info {
                    let remaining: (i64,) = sqlx::query_as(
                        "SELECT COUNT(*) FROM aggregate_elements WHERE address = $1 AND key = $2"
                    )
                    .bind(&address)
                    .bind(&key)
                    .fetch_one(pool)
                    .await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;

                    if remaining.0 == 0 {
                        // No elements left — delete the aggregate entirely
                        sqlx::query("DELETE FROM aggregates WHERE address = $1 AND key = $2")
                            .bind(&address)
                            .bind(&key)
                            .execute(pool)
                            .await
                            .map_err(|e| HandlerError::Database(e.to_string()))?;
                        tracing::info!("Deleted empty aggregate {}/{}", address, key);
                    } else {
                        // Rebuild from remaining elements
                        AggregateHandler::rebuild_aggregate_from_elements(pool, &address, &key).await?;
                    }
                }
            }
            MessageType::Program | MessageType::Instance => {
                // Delete VM versions for this item
                db.delete_vm_updates(hash).await
                    .map_err(|e| HandlerError::Database(e))?;

                // Delete cost records for this item
                sqlx::query("DELETE FROM account_costs WHERE item_hash = $1")
                    .bind(hash)
                    .execute(pool)
                    .await
                    .map_err(|e| HandlerError::Database(e.to_string()))?;

                tracing::debug!("Deleted VM versions and costs for {}", hash);
            }
            _ => {
                // Store, Forget — no additional cascade needed
            }
        }

        Ok(())
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
                let target_message = db.get_message(hash).await
                    .map_err(|e| HandlerError::Database(e))?;
                let message_type = target_message.as_ref().map(|m| m.message_type.clone());
                let message_type_str = message_type.as_ref().map(|t| t.to_string());

                // Mark as forgotten (insert into forgotten_messages)
                db.mark_forgotten(
                    hash,
                    &message.item_hash,
                    content.reason.as_deref(),
                ).await.map_err(|e| HandlerError::Database(e))?;

                // Handle STORE messages with grace period logic
                if message_type == Some(MessageType::Store) {
                    self.process_store_forget(hash, &content.address, &target_message, db.as_ref()).await?;
                } else {
                    // Non-STORE: remove file pin immediately
                    db.remove_file_pin(hash, &content.address).await
                        .map_err(|e| HandlerError::Database(e))?;

                    // Unpin from IPFS immediately for non-STORE messages
                    if let Some(ref ipfs) = ctx.ipfs {
                        if let Err(e) = ipfs.unpin(hash).await {
                            tracing::warn!("Failed to unpin {} from IPFS: {}", hash, e);
                        }
                    }
                }

                // Message-type-specific cascade (forget amends, rebuild aggregates, delete costs)
                if let (Some(ref msg_type), Some(ref pool)) = (&message_type, &ctx.pool) {
                    self.cascade_forget(hash, msg_type, pool, db.as_ref()).await?;
                }

                // Delete from derived tables based on message type
                if let Some(ref msg_type) = message_type_str {
                    db.delete_derived_data(hash, msg_type).await
                        .map_err(|e| HandlerError::Database(e))?;
                }

                // Delete from messages table (pyaleph removes forgotten messages from messages)
                db.delete_message(hash).await
                    .map_err(|e| HandlerError::Database(e))?;
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
