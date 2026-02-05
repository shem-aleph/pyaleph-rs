//! Security aggregate permission checking
//!
//! Implements delegated authorization via the "security" aggregate key.
//! An address can publish an AGGREGATE message with key "security" containing
//! an `authorizations` array to grant other addresses permission to post
//! messages on its behalf.
//!
//! Reference: aleph/permissions.py

use serde::Deserialize;
use serde_json::Value;

use crate::handlers::Database;
use crate::types::{Message, MessageType};

/// A single authorization entry within the security aggregate
#[derive(Debug, Clone, Deserialize)]
pub struct SecurityAuthorization {
    /// The delegate address being granted permissions
    pub address: String,

    /// Optional: restrict to a specific chain (e.g. "ETH")
    #[serde(default)]
    pub chain: Option<String>,

    /// Optional: restrict to specific channels. Empty = allow all.
    #[serde(default)]
    pub channels: Vec<String>,

    /// Optional: restrict to specific message types (e.g. ["POST", "AGGREGATE"]).
    /// Empty = allow all.
    #[serde(default, rename = "types")]
    pub message_types: Vec<String>,

    /// Optional: restrict to specific post types (e.g. ["blog", "comment"]).
    /// Only checked when the message type is POST. Empty = allow all.
    #[serde(default)]
    pub post_types: Vec<String>,

    /// Optional: restrict to specific aggregate keys (e.g. ["settings"]).
    /// Only checked when the message type is AGGREGATE. Empty = allow all.
    #[serde(default)]
    pub aggregate_keys: Vec<String>,
}

/// The content of a security aggregate
#[derive(Debug, Clone, Deserialize)]
pub struct SecurityAggregateContent {
    #[serde(default)]
    pub authorizations: Vec<SecurityAuthorization>,
}

/// Check if `sender` has delegated authorization from `owner_address`.
///
/// This queries the security aggregate for `owner_address` and checks whether
/// `sender` appears in the `authorizations` list with matching filters.
///
/// Filter matching rules (from Python reference):
/// - `address`: must match sender (case-insensitive)
/// - `chain`: if set, must match message chain
/// - `channels`: if non-empty, message channel must be in the list
/// - `types`: if non-empty, message type must be in the list
/// - `post_types`: if non-empty AND message is POST, content.type must be in the list
/// - `aggregate_keys`: if non-empty AND message is AGGREGATE, content.key must be in the list
///
/// Empty arrays mean "allow all" for that filter.
pub async fn check_delegated_authorization(
    db: &dyn Database,
    sender: &str,
    owner_address: &str,
    message: &Message,
) -> Result<bool, String> {
    // Self-authorization
    if sender.to_lowercase() == owner_address.to_lowercase() {
        return Ok(true);
    }

    // Query the security aggregate for the owner
    let aggregate_content = db
        .get_aggregate(owner_address, "security")
        .await
        .map_err(|e| format!("Failed to query security aggregate: {}", e))?;

    let aggregate_value = match aggregate_content {
        Some(v) => v,
        None => return Ok(false),
    };

    // Parse the authorizations
    let security: SecurityAggregateContent =
        serde_json::from_value(aggregate_value).map_err(|e| {
            format!("Failed to parse security aggregate content: {}", e)
        })?;

    // Extract message-type-specific fields for filter checking
    let message_type_str = message.message_type.to_string();
    let message_chain_str = message.chain.to_string();

    // For POST messages, extract content.type (post_type)
    // For AGGREGATE messages, extract content.key
    let (post_type, aggregate_key) = extract_content_fields(message);

    for auth in &security.authorizations {
        // Address must match sender (case-insensitive)
        if auth.address.to_lowercase() != sender.to_lowercase() {
            continue;
        }

        // Chain filter: if set, must match
        if let Some(ref auth_chain) = auth.chain {
            if message_chain_str != *auth_chain {
                continue;
            }
        }

        // Channels filter: if non-empty, message channel must be in list
        if !auth.channels.is_empty() {
            let msg_channel = message.channel.as_deref().unwrap_or("");
            if !auth.channels.iter().any(|c| c == msg_channel) {
                continue;
            }
        }

        // Types filter: if non-empty, message type must be in list
        if !auth.message_types.is_empty() {
            if !auth.message_types.iter().any(|t| t == &message_type_str) {
                continue;
            }
        }

        // Post types filter: only for POST messages
        if message.message_type == MessageType::Post && !auth.post_types.is_empty() {
            if let Some(ref pt) = post_type {
                if !auth.post_types.iter().any(|t| t == pt) {
                    continue;
                }
            }
        }

        // Aggregate keys filter: only for AGGREGATE messages
        if message.message_type == MessageType::Aggregate && !auth.aggregate_keys.is_empty() {
            if let Some(ref ak) = aggregate_key {
                if !auth.aggregate_keys.iter().any(|k| k == ak) {
                    continue;
                }
            }
        }

        // All filters passed
        return Ok(true);
    }

    Ok(false)
}

/// Main entry point: check if the message sender is authorized.
///
/// For POST amend messages, this checks permissions against the original post
/// instead of the amend message itself. This ensures that delegated accounts
/// can only amend posts they originally had permission for.
///
/// Special behavior for amend messages:
/// - If the original post is found, check authorization against original's address
/// - Verify the amend's content.address matches the original's address (prevents hijacking)
/// - If original not found, fall through to standard check
pub async fn check_sender_authorization(
    db: &dyn Database,
    message: &Message,
) -> Result<bool, String> {
    // Parse content to get address
    let content_str = match message.item_content.as_deref() {
        Some(s) => s,
        None => return Ok(false),
    };

    let content: Value = serde_json::from_str(content_str)
        .map_err(|e| format!("Failed to parse message content: {}", e))?;

    let address = match content.get("address").and_then(|a| a.as_str()) {
        Some(a) => a.to_string(),
        None => return Ok(false),
    };

    let sender = &message.sender;

    // If sender is the content address, all good
    if sender.to_lowercase() == address.to_lowercase() {
        return Ok(true);
    }

    // Special handling for POST amend messages
    if message.message_type == MessageType::Post {
        let post_type = content
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("");

        if post_type == "amend" {
            if let Some(ref_value) = content.get("ref").or_else(|| content.get("ref_")) {
                let ref_hash = ref_value.as_str().unwrap_or("").to_string();

                if !ref_hash.is_empty() {
                    // Look up the original message
                    let original = db
                        .get_message(&ref_hash)
                        .await
                        .map_err(|e| format!("Failed to look up original message: {}", e))?;

                    if let Some(original_msg) = original {
                        // Parse original content to get its address
                        if let Some(original_content_str) = original_msg.item_content.as_deref() {
                            if let Ok(original_content) =
                                serde_json::from_str::<Value>(original_content_str)
                            {
                                if let Some(original_address) =
                                    original_content.get("address").and_then(|a| a.as_str())
                                {
                                    // Check new owner matches original owner (prevent hijacking)
                                    if address.to_lowercase()
                                        != original_address.to_lowercase()
                                    {
                                        return Ok(false);
                                    }

                                    // Check delegated permissions against original message
                                    return check_delegated_authorization(
                                        db,
                                        sender,
                                        original_address,
                                        &original_msg,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                    // If original not found, fall through to standard check
                }
            }
        }
    }

    // Standard delegation check
    check_delegated_authorization(db, sender, &address, message).await
}

/// Extract content-type-specific fields from a message for filter matching.
///
/// Returns (post_type, aggregate_key) parsed from item_content.
fn extract_content_fields(message: &Message) -> (Option<String>, Option<String>) {
    let content_str = match message.item_content.as_deref() {
        Some(s) => s,
        None => return (None, None),
    };

    let content: Value = match serde_json::from_str(content_str) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };

    let post_type = content
        .get("type")
        .and_then(|t| t.as_str())
        .map(|s| s.to_string());

    let aggregate_key = content
        .get("key")
        .and_then(|k| k.as_str())
        .map(|s| s.to_string());

    (post_type, aggregate_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::{Database, FilePinRecord, HandlerContext, PostRecord};
    use crate::types::{Chain, ItemType, Message, MessageType, ProcessingStatus};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock database for testing permissions
    struct MockDb {
        aggregates: Mutex<HashMap<(String, String), serde_json::Value>>,
        messages: Mutex<HashMap<String, Message>>,
    }

    impl MockDb {
        fn new() -> Self {
            Self {
                aggregates: Mutex::new(HashMap::new()),
                messages: Mutex::new(HashMap::new()),
            }
        }

        fn with_aggregate(self, owner: &str, key: &str, content: serde_json::Value) -> Self {
            self.aggregates
                .lock()
                .unwrap()
                .insert((owner.to_string(), key.to_string()), content);
            self
        }

        fn with_message(self, hash: &str, message: Message) -> Self {
            self.messages
                .lock()
                .unwrap()
                .insert(hash.to_string(), message);
            self
        }
    }

    #[async_trait]
    impl Database for MockDb {
        async fn get_message(&self, item_hash: &str) -> Result<Option<Message>, String> {
            Ok(self
                .messages
                .lock()
                .unwrap()
                .get(item_hash)
                .cloned())
        }

        async fn store_message(&self, _message: &Message) -> Result<(), String> {
            Ok(())
        }

        async fn update_message_status(
            &self,
            _item_hash: &str,
            _status: &ProcessingStatus,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn get_aggregate(
            &self,
            address: &str,
            key: &str,
        ) -> Result<Option<serde_json::Value>, String> {
            Ok(self
                .aggregates
                .lock()
                .unwrap()
                .get(&(address.to_string(), key.to_string()))
                .cloned())
        }

        async fn store_aggregate(
            &self,
            _address: &str,
            _key: &str,
            _content: &serde_json::Value,
            _time: f64,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn get_aggregate_time(
            &self,
            _address: &str,
            _key: &str,
        ) -> Result<Option<f64>, String> {
            Ok(None)
        }

        async fn mark_aggregate_dirty(
            &self,
            _address: &str,
            _key: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn mark_aggregate_clean(
            &self,
            _address: &str,
            _key: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn get_post(&self, _item_hash: &str) -> Result<Option<PostRecord>, String> {
            Ok(None)
        }

        async fn store_post(&self, _post: &PostRecord) -> Result<(), String> {
            Ok(())
        }

        async fn update_post_latest_amend(
            &self,
            _original_hash: &str,
            _amend_hash: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn get_file_pin(
            &self,
            _item_hash: &str,
        ) -> Result<Option<FilePinRecord>, String> {
            Ok(None)
        }

        async fn store_file_pin(&self, _pin: &FilePinRecord) -> Result<(), String> {
            Ok(())
        }

        async fn update_file_pin(
            &self,
            _item_hash: &str,
            _owner: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn remove_file_pin(
            &self,
            _item_hash: &str,
            _owner: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn get_forgotten_hashes(
            &self,
            _hashes: &[String],
        ) -> Result<Vec<String>, String> {
            Ok(vec![])
        }

        async fn mark_forgotten(
            &self,
            _item_hash: &str,
            _forget_hash: &str,
            _reason: Option<&str>,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn delete_message(&self, _item_hash: &str) -> Result<(), String> {
            Ok(())
        }

        async fn delete_derived_data(&self, _item_hash: &str, _message_type: &str) -> Result<(), String> {
            Ok(())
        }

        async fn get_dependent_vms(
            &self,
            _file_hash: &str,
        ) -> Result<Vec<String>, String> {
            Ok(vec![])
        }

        async fn get_balance(
            &self,
            _address: &str,
            _chain: &str,
        ) -> Result<Option<rust_decimal::Decimal>, String> {
            Ok(None)
        }

        async fn get_credit_balance(
            &self,
            _address: &str,
        ) -> Result<Option<rust_decimal::Decimal>, String> {
            Ok(None)
        }
    }

    /// Helper to create a test message
    fn make_message(
        sender: &str,
        msg_type: MessageType,
        chain: Chain,
        channel: Option<&str>,
        item_content: &str,
    ) -> Message {
        Message {
            message_type: msg_type,
            chain,
            sender: sender.to_string(),
            signature: "test_signature".to_string(),
            item_type: ItemType::Inline,
            item_hash: "testhash".to_string(),
            item_content: Some(item_content.to_string()),
            channel: channel.map(|s| s.to_string()),
            time: 1651050219.0,
        }
    }

    // ==========================================
    // 7.1 Basic Authorization Tests
    // ==========================================

    #[tokio::test]
    async fn test_self_authorized() {
        let db = MockDb::new();
        let message = make_message(
            "0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23",
            MessageType::Store,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xdeF61fAadE93a8aaE303D083Ead5BF7a25E55a23","item_type":"storage","item_hash":"abc123","time":1652085236.777}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Sender == content.address should always be authorized");
    }

    #[tokio::test]
    async fn test_no_security_aggregate() {
        let db = MockDb::new();
        let message = make_message(
            "0x8b5C865d6ff6Dd5C5c402C8D918F7edd189C74D4",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"VM on executor","time":1651050219.348,"content":{"test":true},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "No security aggregate should deny authorization");
    }

    #[tokio::test]
    async fn test_basic_delegation() {
        let db = MockDb::new().with_aggregate(
            "0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E"}
                ]
            }),
        );

        let message = make_message(
            "0x86F39e17910E3E6d9F38412EB7F24Bf0Ba31eb2E",
            MessageType::Post,
            Chain::ETH,
            None,
            r#"{"address":"0xA3c613b12e862EB6e0C9897E03F1deEb207b5B58","time":1651050219.348,"content":{"test":true},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Basic delegation with no filters should authorize");
    }

    // ==========================================
    // 7.2 Filter Tests
    // ==========================================

    #[tokio::test]
    async fn test_chain_filter_match() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "chain": "ETH"}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Chain filter matching should authorize");
    }

    #[tokio::test]
    async fn test_chain_filter_mismatch() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "chain": "SOL"}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Chain filter mismatch should deny");
    }

    #[tokio::test]
    async fn test_channel_filter_match() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "channels": ["MY_APP", "OTHER"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("MY_APP"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Channel in list should authorize");
    }

    #[tokio::test]
    async fn test_channel_filter_mismatch() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "channels": ["MY_APP"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("WRONG_CHANNEL"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Channel not in list should deny");
    }

    #[tokio::test]
    async fn test_type_filter_match() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["POST", "AGGREGATE"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Message type in list should authorize");
    }

    #[tokio::test]
    async fn test_type_filter_mismatch() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["AGGREGATE"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Message type not in list should deny");
    }

    #[tokio::test]
    async fn test_post_type_filter_match() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["POST"], "post_types": ["blog", "comment"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"blog"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Post type in list should authorize");
    }

    #[tokio::test]
    async fn test_post_type_filter_mismatch() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["POST"], "post_types": ["blog"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"comment"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Post type not in list should deny");
    }

    #[tokio::test]
    async fn test_aggregate_key_filter_match() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["AGGREGATE"], "aggregate_keys": ["settings", "profile"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Aggregate,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","key":"settings","content":{"theme":"dark"},"time":1.0}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Aggregate key in list should authorize");
    }

    #[tokio::test]
    async fn test_aggregate_key_filter_mismatch() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDelegate", "types": ["AGGREGATE"], "aggregate_keys": ["settings"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Aggregate,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","key":"security","content":{},"time":1.0}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Aggregate key not in list should deny");
    }

    #[tokio::test]
    async fn test_empty_filters_allow_all() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {
                        "address": "0xDelegate",
                        "channels": [],
                        "types": [],
                        "post_types": [],
                        "aggregate_keys": []
                    }
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("ANY_CHANNEL"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"anything"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Empty filter arrays should allow all");
    }

    // ==========================================
    // 7.3 Amend Tests
    // ==========================================

    #[tokio::test]
    async fn test_delegated_amend_authorized() {
        let original_hash = "1111111111111111111111111111111111111111111111111111111111111111";

        let original_message = make_message(
            "0xOriginalSender",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xContentOwner","time":1.0,"content":{"title":"Original"},"type":"post"}"#,
        );

        let db = MockDb::new()
            .with_message(original_hash, {
                let mut m = original_message;
                m.item_hash = original_hash.to_string();
                m
            })
            .with_aggregate(
                "0xContentOwner",
                "security",
                serde_json::json!({
                    "authorizations": [
                        {
                            "address": "0xDelegatedAccount",
                            "types": ["POST"],
                            "post_types": ["post"]
                        }
                    ]
                }),
            );

        let amend_content = format!(
            r#"{{"address":"0xContentOwner","time":2.0,"content":{{"title":"Amended"}},"type":"amend","ref":"{}"}}"#,
            original_hash
        );

        let message = make_message(
            "0xDelegatedAccount",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            &amend_content,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Delegated account should be able to amend posts from authorized owner");
    }

    #[tokio::test]
    async fn test_delegated_amend_wrong_owner() {
        let original_hash = "1111111111111111111111111111111111111111111111111111111111111111";

        let original_message = Message {
            message_type: MessageType::Post,
            chain: Chain::ETH,
            sender: "0xOriginalSender".to_string(),
            signature: "sig".to_string(),
            item_type: ItemType::Inline,
            item_hash: original_hash.to_string(),
            item_content: Some(
                r#"{"address":"0xOriginalOwner","time":1.0,"content":{},"type":"post"}"#
                    .to_string(),
            ),
            channel: Some("TEST".to_string()),
            time: 1.0,
        };

        let db = MockDb::new()
            .with_message(original_hash, original_message)
            .with_aggregate(
                "0xOriginalOwner",
                "security",
                serde_json::json!({
                    "authorizations": [
                        {"address": "0xMalicious", "types": ["POST"]}
                    ]
                }),
            );

        // Amend tries to change address to a different owner
        let amend_content = format!(
            r#"{{"address":"0xDifferentOwner","time":2.0,"content":{{}},"type":"amend","ref":"{}"}}"#,
            original_hash
        );

        let message = make_message(
            "0xMalicious",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            &amend_content,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(
            !result,
            "Amend with different content.address than original should be denied"
        );
    }

    #[tokio::test]
    async fn test_amend_missing_original() {
        // No original message in DB — should fall through to standard check
        let db = MockDb::new().with_aggregate(
            "0xContentOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {
                        "address": "0xDelegatedAccount",
                        "types": ["POST"],
                        "post_types": ["amend"]
                    }
                ]
            }),
        );

        let amend_content = format!(
            r#"{{"address":"0xContentOwner","time":2.0,"content":{{}},"type":"amend","ref":"{}"}}"#,
            "0000000000000000000000000000000000000000000000000000000000000000"
        );

        let message = make_message(
            "0xDelegatedAccount",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            &amend_content,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(
            result,
            "Missing original should fall back to standard check against content.address"
        );
    }

    #[tokio::test]
    async fn test_delegated_amend_denied_no_permission() {
        let original_hash = "1111111111111111111111111111111111111111111111111111111111111111";

        let original_message = Message {
            message_type: MessageType::Post,
            chain: Chain::ETH,
            sender: "0xOriginalSender".to_string(),
            signature: "sig".to_string(),
            item_type: ItemType::Inline,
            item_hash: original_hash.to_string(),
            item_content: Some(
                r#"{"address":"0xContentOwner","time":1.0,"content":{},"type":"post"}"#
                    .to_string(),
            ),
            channel: Some("TEST".to_string()),
            time: 1.0,
        };

        let db = MockDb::new()
            .with_message(original_hash, original_message)
            .with_aggregate(
                "0xContentOwner",
                "security",
                serde_json::json!({
                    "authorizations": [
                        {
                            "address": "0xOtherDelegate",
                            "types": ["POST"]
                        }
                    ]
                }),
            );

        let amend_content = format!(
            r#"{{"address":"0xContentOwner","time":2.0,"content":{{}},"type":"amend","ref":"{}"}}"#,
            original_hash
        );

        let message = make_message(
            "0xUnauthorized",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            &amend_content,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "Unauthorized account should be denied amend");
    }

    // ==========================================
    // 7.4 Edge Cases
    // ==========================================

    #[tokio::test]
    async fn test_multiple_authorizations() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xOther", "types": ["AGGREGATE"]},
                    {"address": "0xDelegate", "types": ["POST"]}
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(
            result,
            "Second auth entry should match even if first doesn't"
        );
    }

    #[tokio::test]
    async fn test_authorization_with_all_filters() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {
                        "address": "0xDelegate",
                        "chain": "ETH",
                        "channels": ["MY_APP"],
                        "types": ["POST"],
                        "post_types": ["blog"]
                    }
                ]
            }),
        );

        // All filters match
        let message = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("MY_APP"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"blog"}"#,
        );
        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "All filters matching should authorize");

        // Wrong channel
        let message2 = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("WRONG"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"blog"}"#,
        );
        let result2 = check_sender_authorization(&db, &message2).await.unwrap();
        assert!(!result2, "Wrong channel should deny");

        // Wrong post type
        let message3 = make_message(
            "0xDelegate",
            MessageType::Post,
            Chain::ETH,
            Some("MY_APP"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"comment"}"#,
        );
        let result3 = check_sender_authorization(&db, &message3).await.unwrap();
        assert!(!result3, "Wrong post type should deny");
    }

    #[tokio::test]
    async fn test_case_insensitive_address() {
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {"address": "0xDELEGATE"}
                ]
            }),
        );

        let message = make_message(
            "0xdelegate",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Address comparison should be case-insensitive");
    }

    #[tokio::test]
    async fn test_case_insensitive_self_authorization() {
        let db = MockDb::new();
        let message = make_message(
            "0xAABBCC",
            MessageType::Post,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xaabbcc","time":1.0,"content":{},"type":"test"}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Self-authorization should be case-insensitive");
    }

    #[tokio::test]
    async fn test_no_item_content() {
        let db = MockDb::new();
        let message = Message {
            message_type: MessageType::Post,
            chain: Chain::ETH,
            sender: "0xSender".to_string(),
            signature: "sig".to_string(),
            item_type: ItemType::Ipfs,
            item_hash: "hash".to_string(),
            item_content: None,
            channel: Some("TEST".to_string()),
            time: 1.0,
        };

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(!result, "No item_content should deny");
    }

    #[tokio::test]
    async fn test_aggregate_delegation() {
        // Test that delegated aggregate messages work after removing sender==address check
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {
                        "address": "0xDelegate",
                        "types": ["AGGREGATE"],
                        "aggregate_keys": ["settings"]
                    }
                ]
            }),
        );

        let message = make_message(
            "0xDelegate",
            MessageType::Aggregate,
            Chain::ETH,
            Some("TEST"),
            r#"{"address":"0xOwner","key":"settings","content":{"theme":"dark"},"time":1.0}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(result, "Delegated aggregate should be authorized");
    }

    #[tokio::test]
    async fn test_security_aggregate_delegation() {
        // A delegate with aggregate_keys: ["security"] can modify the security aggregate itself
        let db = MockDb::new().with_aggregate(
            "0xOwner",
            "security",
            serde_json::json!({
                "authorizations": [
                    {
                        "address": "0xSecurityAdmin",
                        "types": ["AGGREGATE"],
                        "aggregate_keys": ["security"]
                    }
                ]
            }),
        );

        let message = make_message(
            "0xSecurityAdmin",
            MessageType::Aggregate,
            Chain::ETH,
            None,
            r#"{"address":"0xOwner","key":"security","content":{"authorizations":[]},"time":1.0}"#,
        );

        let result = check_sender_authorization(&db, &message).await.unwrap();
        assert!(
            result,
            "Security admin should be able to modify security aggregate"
        );
    }
}
