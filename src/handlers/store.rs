//! Store message handler
//!
//! Handles STORE messages which represent file storage on the Aleph network.
//! Supports file versioning via `ref` field, file tags for latest-version
//! tracking, and cost validation for large files.
//!
//! Reference: aleph/handlers/content/store.py

use async_trait::async_trait;

use crate::types::{Message, MessageType, StoreContent, ItemType};
use crate::services::cost::defaults;
use super::{HandlerContext, HandlerError, MessageHandler, FilePinRecord, PinType, FileType, FileTagRecord, AccountCostRecord};

pub struct StoreHandler;

/// Maximum unauthenticated upload file size (25 MiB) — files under this are free
const MAX_FREE_FILE_SIZE: u64 = 25 * 1024 * 1024;

impl StoreHandler {
    fn validate_ipfs_hash(hash: &str) -> bool {
        // CIDv0 (Qm prefix, 46 chars base58)
        if hash.starts_with("Qm") && hash.len() == 46 {
            return true;
        }
        // CIDv1 (b prefix, base32/base36)
        if hash.starts_with("b") && hash.len() >= 50 {
            return true;
        }
        // SHA256 hex hash (64 hex chars)
        if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return true;
        }
        false
    }

    /// Validate that item_type matches hash format
    fn validate_item_type_hash(item_type: &ItemType, hash: &str) -> Result<(), HandlerError> {
        match item_type {
            ItemType::Ipfs => {
                if !Self::validate_ipfs_hash(hash) {
                    return Err(HandlerError::InvalidContent(format!(
                        "Invalid IPFS hash format: {}", hash
                    )));
                }
            }
            ItemType::Storage => {
                // Storage type uses SHA256 hex hashes
                if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(HandlerError::InvalidContent(format!(
                        "Invalid storage hash format (expected 64 hex chars): {}", hash
                    )));
                }
            }
            ItemType::Inline => {
                // Inline doesn't use external hashes
            }
        }
        Ok(())
    }

    /// Validate ref_ field if present
    fn validate_ref(ref_: &str) -> Result<(), HandlerError> {
        if ref_.is_empty() {
            return Err(HandlerError::InvalidContent(
                "ref field cannot be empty string".to_string()
            ));
        }
        // ref_ can be a message hash (64 hex chars) or a user-defined string
        // We don't restrict the format — Python allows arbitrary strings
        Ok(())
    }
}

/// Compute the file tag key for versioning
///
/// Tags are formatted as `{owner}:{ref_or_item_hash}` and used to track
/// the latest version of a file when ref-based versioning is used.
///
/// Reference: aleph/handlers/content/store.py make_file_tag
pub fn make_file_tag(owner: &str, ref_: Option<&str>, item_hash: &str) -> String {
    match ref_ {
        Some(r) if !r.is_empty() => format!("{}:{}", owner.to_lowercase(), r),
        _ => format!("{}:{}", owner.to_lowercase(), item_hash),
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

        if content.item_hash.is_empty() {
            return Err(HandlerError::InvalidContent("Empty item_hash".to_string()));
        }

        // Validate item_type matches hash format
        Self::validate_item_type_hash(&content.item_type, &content.item_hash)?;

        // Validate ref if present
        if let Some(ref ref_) = content.ref_ {
            Self::validate_ref(ref_)?;

            // If ref looks like a message hash, verify the target message exists
            // and the target isn't itself a ref (no revision chains)
            let is_message_hash = ref_.len() == 64 && ref_.chars().all(|c| c.is_ascii_hexdigit());
            if is_message_hash {
                if let Some(ref db) = ctx.db {
                    if let Some(target_msg) = db.get_message(ref_).await
                        .map_err(|e| HandlerError::Database(e))?
                    {
                        // Verify target is a STORE message
                        if target_msg.message_type != MessageType::Store {
                            return Err(HandlerError::InvalidContent(format!(
                                "ref target {} is not a STORE message", ref_
                            )));
                        }
                        // Check that target doesn't have its own ref (no chains)
                        if let Some(ref target_content_str) = target_msg.item_content {
                            if let Ok(target_content) = serde_json::from_str::<StoreContent>(target_content_str) {
                                if target_content.ref_.is_some() {
                                    return Err(HandlerError::InvalidContent(
                                        "Cannot create revision chain: ref target has its own ref".to_string()
                                    ));
                                }
                            }
                        }
                    }
                    // If message not found, it's a user-defined ref string that happens
                    // to look like a hash — we allow it
                }
            }
        }

        Ok(())
    }

    async fn check_permissions(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        // Default security aggregate check for delegated authorization
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;

        // Check sender != address → needs security aggregate authorization
        if message.sender.to_lowercase() != content.address.to_lowercase() {
            let db = ctx.db.as_ref()
                .ok_or_else(|| HandlerError::Database("No database configured".to_string()))?;
            let authorized = crate::permissions::check_sender_authorization(db.as_ref(), message).await
                .map_err(|e| HandlerError::Database(e.to_string()))?;
            if !authorized {
                return Err(HandlerError::PermissionDenied(
                    format!("Sender {} is not authorized to post on behalf of {}", message.sender, content.address)
                ));
            }
        }

        // If ref_ is set, check ownership of the tag
        if let Some(ref ref_) = content.ref_ {
            if let Some(ref db) = ctx.db {
                let tag = make_file_tag(&content.address, Some(ref_), &content.item_hash);
                if let Some(existing_tag) = db.get_file_tag(&tag).await
                    .map_err(|e| HandlerError::Database(e))?
                {
                    if existing_tag.owner.to_lowercase() != content.address.to_lowercase() {
                        return Err(HandlerError::PermissionDenied(
                            format!("Address {} does not own file tag {}", content.address, tag)
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    async fn check_balance(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;
        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;

        let file_size = content.size.unwrap_or(0);

        // Files under the free threshold don't need balance checking
        if file_size <= MAX_FREE_FILE_SIZE {
            return Ok(());
        }

        // Check if payment info is provided
        let payment = match content.payment {
            Some(ref p) => p,
            None => return Ok(()), // No payment info = no balance check (legacy behavior)
        };

        let db = match ctx.db.as_ref() {
            Some(db) => db,
            None => return Ok(()),
        };

        let payment_type = match payment.payment_type {
            crate::types::PaymentType::Hold => "hold",
            crate::types::PaymentType::Credit => "credit",
            crate::types::PaymentType::Superfluid => return Ok(()), // On-chain validation
        };

        let size_mib = rust_decimal::Decimal::from(file_size / (1024 * 1024));
        let cost_hold = defaults::storage_holding() * size_mib;
        let cost_credit = defaults::storage_credit() * size_mib;

        let chain = payment.chain.to_string();
        match payment_type {
            "hold" => {
                if let Some(balance) = db.get_balance(&content.address, &chain).await
                    .map_err(|e| HandlerError::Database(e))?
                {
                    let total_existing = db.get_total_cost_for_address(&content.address, "hold").await
                        .map_err(|e| HandlerError::Database(e))?;
                    if balance < total_existing + cost_hold {
                        return Err(HandlerError::InsufficientBalance);
                    }
                }
            }
            "credit" => {
                if let Some(balance) = db.get_credit_balance(&content.address).await
                    .map_err(|e| HandlerError::Database(e))?
                {
                    let total_existing = db.get_total_cost_for_address(&content.address, "credit").await
                        .map_err(|e| HandlerError::Database(e))?;
                    if balance < total_existing + cost_credit {
                        return Err(HandlerError::InsufficientCredit);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
        let content_str = message.item_content.as_deref()
            .ok_or_else(|| HandlerError::InvalidContent("Missing item_content".to_string()))?;

        let content: StoreContent = serde_json::from_str(content_str)
            .map_err(|e| HandlerError::InvalidContent(e.to_string()))?;

        let file_size = content.size.unwrap_or(0);

        if let Some(ref db) = ctx.db {
            // 1. Upsert files table
            db.upsert_file(&content.item_hash, file_size, &FileType::File).await
                .map_err(|e| HandlerError::Database(e))?;

            // 2. Insert typed file pin
            let pin = FilePinRecord {
                item_hash: content.item_hash.clone(),
                owner: content.address.clone(),
                size: file_size,
                content_type: content.content_type.clone(),
                pin_type: PinType::Message,
                ref_: content.ref_.clone(),
                delete_by: None,
                message_hash: Some(message.item_hash.clone()),
                created_at: chrono::Utc::now(),
            };
            db.insert_file_pin_typed(&pin).await
                .map_err(|e| HandlerError::Database(e))?;

            // 3. Compute and upsert file tag
            let tag_key = make_file_tag(&content.address, content.ref_.as_deref(), &content.item_hash);
            let file_tag = FileTagRecord {
                tag: tag_key,
                owner: content.address.clone(),
                file_hash: content.item_hash.clone(),
                last_updated: message.time,
            };
            db.upsert_file_tag(&file_tag).await
                .map_err(|e| HandlerError::Database(e))?;

            // 4. Store cost records for files with payment info
            // Balance validation already happened in check_balance()
            if let Some(ref payment) = content.payment {
                let payment_type = match payment.payment_type {
                    crate::types::PaymentType::Hold => "hold",
                    crate::types::PaymentType::Credit => "credit",
                    crate::types::PaymentType::Superfluid => "superfluid",
                };

                let size_mib = rust_decimal::Decimal::from(file_size / (1024 * 1024));
                let cost_hold = defaults::storage_holding() * size_mib;
                let cost_credit = defaults::storage_credit() * size_mib;

                let cost_record = AccountCostRecord {
                    owner: content.address.clone(),
                    item_hash: message.item_hash.clone(),
                    cost_type: "STORAGE".to_string(),
                    name: "storage".to_string(),
                    ref_hash: Some(content.item_hash.clone()),
                    payment_type: payment_type.to_string(),
                    cost_hold,
                    cost_stream: rust_decimal::Decimal::ZERO,
                    cost_credit,
                };

                db.store_account_costs(&[cost_record]).await
                    .map_err(|e| HandlerError::Database(e))?;
            }

            tracing::debug!(
                "Stored file_pin: {} (ref={:?}, tag={}, size={})",
                content.item_hash,
                content.ref_,
                make_file_tag(&content.address, content.ref_.as_deref(), &content.item_hash),
                file_size,
            );
        }

        // Also write via raw pool for backward compatibility with existing queries
        if let Some(ref pool) = ctx.pool {
            sqlx::query(
                "INSERT INTO file_pins (item_hash, owner, size, content_type, pin_type, message_hash)
                 VALUES ($1, $2, $3, $4, 'message', $5)
                 ON CONFLICT (item_hash, owner) DO UPDATE SET
                   size = EXCLUDED.size,
                   content_type = COALESCE(EXCLUDED.content_type, file_pins.content_type),
                   message_hash = EXCLUDED.message_hash"
            )
            .bind(&content.item_hash)
            .bind(&content.address)
            .bind(file_size as i64)
            .bind(&content.content_type)
            .bind(&message.item_hash)
            .execute(pool)
            .await
            .map_err(|e| HandlerError::Database(e.to_string()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_ipfs_hash() {
        // CIDv0
        assert!(StoreHandler::validate_ipfs_hash("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"));
        // SHA256
        assert!(StoreHandler::validate_ipfs_hash(
            "a" .repeat(64).as_str()
        ));
        // Invalid
        assert!(!StoreHandler::validate_ipfs_hash("short"));
    }

    #[test]
    fn test_make_file_tag() {
        // With ref
        assert_eq!(
            make_file_tag("0xABC", Some("my-file"), "hash123"),
            "0xabc:my-file"
        );
        // Without ref — uses item_hash
        assert_eq!(
            make_file_tag("0xABC", None, "hash123"),
            "0xabc:hash123"
        );
        // Empty ref — uses item_hash
        assert_eq!(
            make_file_tag("0xABC", Some(""), "hash123"),
            "0xabc:hash123"
        );
    }

    #[test]
    fn test_validate_item_type_hash() {
        // Valid IPFS hash
        assert!(StoreHandler::validate_item_type_hash(
            &ItemType::Ipfs,
            "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
        ).is_ok());

        // Valid storage hash
        let hex_hash = "a".repeat(64);
        assert!(StoreHandler::validate_item_type_hash(
            &ItemType::Storage,
            &hex_hash,
        ).is_ok());

        // Invalid storage hash
        assert!(StoreHandler::validate_item_type_hash(
            &ItemType::Storage,
            "short",
        ).is_err());
    }

    #[test]
    fn test_validate_ref() {
        assert!(StoreHandler::validate_ref("my-file").is_ok());
        assert!(StoreHandler::validate_ref("").is_err());
    }
}
