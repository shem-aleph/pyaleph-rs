//! Store message handler
//!
//! Store messages reference files stored on IPFS/Aleph storage.
//! Key features:
//! - Balance pre-check before processing
//! - File size validation
//! - IPFS pinning
//! - File tagging for garbage collection
//!
//! Reference: aleph/handlers/content/store.py

use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

use crate::types::{Message, MessageType, StoreContent, ItemType, ErrorCode};
use super::{HandlerContext, HandlerError, MessageHandler, FilePinRecord};

/// Maximum file size for unauthenticated uploads (25 MB)
const MAX_UNAUTHENTICATED_FILE_SIZE: u64 = 25 * 1024 * 1024;

/// Handler for store messages
pub struct StoreHandler;

impl StoreHandler {
    /// Calculate storage cost for a file
    fn calculate_storage_cost(size_bytes: u64) -> Decimal {
        use rust_decimal_macros::dec;
        
        // Cost per MiB per month (simplified)
        // In production, this should read from the pricing aggregate
        const PRICE_PER_MIB_PER_MONTH: Decimal = dec!(0.0033);
        let size_mib = Decimal::from(size_bytes) / Decimal::from(1024 * 1024);
        
        // Minimum 1 month of storage
        size_mib * PRICE_PER_MIB_PER_MONTH
    }
    
    /// Check if the file type is allowed
    fn is_allowed_content_type(content_type: Option<&str>) -> bool {
        // For now, allow all types. In production, might want to block certain types.
        true
    }
    
    /// Validate IPFS hash format
    fn validate_ipfs_hash(hash: &str) -> bool {
        // IPFS CIDv0 starts with "Qm" and is 46 characters
        // IPFS CIDv1 varies but typically starts with "b" for base32
        if hash.starts_with("Qm") && hash.len() == 46 {
            return true;
        }
        if hash.starts_with("b") && hash.len() >= 50 {
            return true;
        }
        // Also allow raw SHA256 hashes (64 hex chars)
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
    
    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(format!("Invalid store content: {}", e)))?;
        
        // Verify sender matches content address
        if message.sender.to_lowercase() != content.address.to_lowercase() {
            return Err(HandlerError::Unauthorized);
        }
        
        // Validate hash format
        if content.item_hash.is_empty() {
            return Err(HandlerError::InvalidContent("Empty item_hash".to_string()));
        }
        
        // Validate hash format for IPFS content
        if matches!(content.item_type, ItemType::Ipfs) {
            if !Self::validate_ipfs_hash(&content.item_hash) {
                return Err(HandlerError::InvalidContent(format!(
                    "Invalid IPFS hash format: {}",
                    content.item_hash
                )));
            }
        }
        
        // Check content type
        if !Self::is_allowed_content_type(content.content_type.as_deref()) {
            return Err(HandlerError::InvalidContent(
                "Content type not allowed".to_string()
            ));
        }
        
        // Verify signature
        if let Some(ref crypto) = ctx.crypto {
            if !message.verify_signature(crypto).map_err(|e| HandlerError::InvalidSignature(e))? {
                return Err(HandlerError::InvalidSignature("Signature verification failed".to_string()));
            }
        }
        
        // Get file size (from message or by fetching)
        let file_size = if let Some(size) = content.size {
            size
        } else if let Some(ref ipfs) = ctx.ipfs {
            // Fetch size from IPFS
            ipfs.get_size(&content.item_hash).await
                .map_err(|e| HandlerError::Storage(format!("Cannot determine file size: {}", e)))?
        } else {
            // Size unknown and no IPFS service - check later
            0
        };
        
        // Check balance before processing
        if file_size > 0 {
            let required_balance = Self::calculate_storage_cost(file_size);
            
            if let Some(ref db) = ctx.db {
                // Check holding balance first
                let balance = db.get_balance(&content.address, &message.chain.to_string()).await
                    .map_err(|e| HandlerError::Database(e))?
                    .unwrap_or(Decimal::ZERO);
                
                // If not enough holding balance, check credit balance
                if balance < required_balance {
                    let credit_balance = db.get_credit_balance(&content.address).await
                        .map_err(|e| HandlerError::Database(e))?
                        .unwrap_or(Decimal::ZERO);
                    
                    if credit_balance < required_balance {
                        return Err(HandlerError::InsufficientBalance);
                    }
                }
            }
        }
        
        // Size limit for unauthenticated (no balance) uploads
        if file_size > MAX_UNAUTHENTICATED_FILE_SIZE {
            // Verify user has some balance
            if let Some(ref db) = ctx.db {
                let balance = db.get_balance(&content.address, &message.chain.to_string()).await
                    .map_err(|e| HandlerError::Database(e))?
                    .unwrap_or(Decimal::ZERO);
                
                let credit_balance = db.get_credit_balance(&content.address).await
                    .map_err(|e| HandlerError::Database(e))?
                    .unwrap_or(Decimal::ZERO);
                
                if balance.is_zero() && credit_balance.is_zero() {
                    return Err(HandlerError::InvalidContent(format!(
                        "File size {} exceeds limit for accounts without balance ({})",
                        file_size, MAX_UNAUTHENTICATED_FILE_SIZE
                    )));
                }
            }
        }
        
        Ok(())
    }
    
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;
        
        tracing::info!(
            "Processing store: address={}, hash={}, type={:?}",
            content.address,
            content.item_hash,
            content.item_type
        );
        
        // Get file size
        let file_size = if let Some(size) = content.size {
            size
        } else if let Some(ref ipfs) = ctx.ipfs {
            ipfs.get_size(&content.item_hash).await
                .unwrap_or(0)
        } else {
            0
        };
        
        // Pin content to IPFS
        if let Some(ref ipfs) = ctx.ipfs {
            ipfs.pin(&content.item_hash).await
                .map_err(|e| HandlerError::Storage(format!("Failed to pin to IPFS: {}", e)))?;
            
            tracing::debug!("Pinned {} to IPFS", content.item_hash);
        }
        
        // Create file pin record
        let pin_record = FilePinRecord {
            item_hash: content.item_hash.clone(),
            owner: content.address.clone(),
            size: file_size,
            content_type: content.content_type.clone(),
            created_at: chrono::Utc::now(),
        };
        
        // Store in database
        if let Some(ref db) = ctx.db {
            // Check if pin already exists (update scenario)
            let existing = db.get_file_pin(&content.item_hash).await
                .map_err(|e| HandlerError::Database(e))?;
            
            if existing.is_some() {
                // Update existing pin (add another reference)
                db.update_file_pin(&content.item_hash, &content.address).await
                    .map_err(|e| HandlerError::Database(e))?;
                
                tracing::debug!("Updated existing pin for {}", content.item_hash);
            } else {
                // Create new pin
                db.store_file_pin(&pin_record).await
                    .map_err(|e| HandlerError::Database(e))?;
                
                tracing::debug!("Created new pin for {}", content.item_hash);
            }
            
            // Store the message itself
            db.store_message(message).await
                .map_err(|e| HandlerError::Database(e))?;
        } else {
            tracing::warn!("No database configured, store not persisted");
        }
        
        // Calculate and log cost
        if file_size > 0 {
            let cost = Self::calculate_storage_cost(file_size);
            tracing::info!(
                "Storage cost for {} ({} bytes): {} ALEPH/month",
                content.item_hash,
                file_size,
                cost
            );
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ipfs_hash_validation() {
        // Valid CIDv0
        assert!(StoreHandler::validate_ipfs_hash(
            "QmYwAPJzv5CZsnAzt8auVZRn6TRBMnPk9FbGUPXvPmQNzK"
        ));
        
        // Valid SHA256
        assert!(StoreHandler::validate_ipfs_hash(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        
        // Invalid
        assert!(!StoreHandler::validate_ipfs_hash("invalid"));
        assert!(!StoreHandler::validate_ipfs_hash(""));
    }
    
    #[test]
    fn test_storage_cost_calculation() {
        // 1 MiB should cost 0.0033 ALEPH/month
        let cost = StoreHandler::calculate_storage_cost(1024 * 1024);
        assert!(cost > Decimal::ZERO);
        
        // Larger files cost more
        let cost_10mb = StoreHandler::calculate_storage_cost(10 * 1024 * 1024);
        assert!(cost_10mb > cost);
    }
}
