//! Message content schema validation
//!
//! Validates the structure and required fields of message content
//! before it reaches the handlers. This catches malformed messages early.

use serde_json::Value;

/// Validation errors
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid field type for {field}: expected {expected}")]
    InvalidType { field: String, expected: String },

    #[error("Invalid value for {field}: {reason}")]
    InvalidValue { field: String, reason: String },
}

/// Validate message content based on message type
pub fn validate_content(message_type: &str, content: &Value) -> Result<(), Vec<SchemaError>> {
    let mut errors = Vec::new();

    match message_type {
        "AGGREGATE" => validate_aggregate(content, &mut errors),
        "POST" => validate_post(content, &mut errors),
        "STORE" => validate_store(content, &mut errors),
        "PROGRAM" => validate_program(content, &mut errors),
        "INSTANCE" => validate_instance(content, &mut errors),
        "FORGET" => validate_forget(content, &mut errors),
        _ => errors.push(SchemaError::InvalidValue {
            field: "type".to_string(),
            reason: format!("Unknown message type: {}", message_type),
        }),
    }

    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn require_field<'a>(content: &'a Value, field: &str, errors: &mut Vec<SchemaError>) -> Option<&'a Value> {
    match content.get(field) {
        Some(v) if !v.is_null() => Some(v),
        _ => {
            errors.push(SchemaError::MissingField(field.to_string()));
            None
        }
    }
}

fn require_string(content: &Value, field: &str, errors: &mut Vec<SchemaError>) {
    if let Some(v) = require_field(content, field, errors) {
        if !v.is_string() {
            errors.push(SchemaError::InvalidType {
                field: field.to_string(),
                expected: "string".to_string(),
            });
        }
    }
}

fn validate_aggregate(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_string(content, "key", errors);
    require_field(content, "content", errors);
}

fn validate_post(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_string(content, "type", errors);
    require_field(content, "content", errors);
    // "ref" is required for amend type
    if let Some(t) = content.get("type").and_then(|v| v.as_str()) {
        if t.to_lowercase() == "amend" {
            require_string(content, "ref", errors);
        }
    }
}

fn validate_store(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_string(content, "item_hash", errors);
    require_string(content, "item_type", errors);
}

fn validate_program(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_field(content, "code", errors);
    require_field(content, "runtime", errors);
}

fn validate_instance(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_field(content, "rootfs", errors);
    require_field(content, "resources", errors);
}

fn validate_forget(content: &Value, errors: &mut Vec<SchemaError>) {
    require_string(content, "address", errors);
    require_field(content, "hashes", errors);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_aggregate() {
        let content = json!({
            "address": "0x1234",
            "key": "profile",
            "content": {"name": "test"}
        });
        assert!(validate_content("AGGREGATE", &content).is_ok());
    }

    #[test]
    fn test_missing_field() {
        let content = json!({"address": "0x1234"});
        let result = validate_content("AGGREGATE", &content);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| matches!(e, SchemaError::MissingField(f) if f == "key")));
    }

    #[test]
    fn test_valid_post() {
        let content = json!({
            "address": "0x1234",
            "type": "note",
            "content": {"body": "hello"}
        });
        assert!(validate_content("POST", &content).is_ok());
    }

    #[test]
    fn test_amend_requires_ref() {
        let content = json!({
            "address": "0x1234",
            "type": "amend",
            "content": {"body": "updated"}
        });
        let result = validate_content("POST", &content);
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_forget() {
        let content = json!({
            "address": "0x1234",
            "hashes": ["abc123"]
        });
        assert!(validate_content("FORGET", &content).is_ok());
    }

    #[test]
    fn test_valid_store() {
        let content = json!({
            "address": "0x1234",
            "item_hash": "Qm...",
            "item_type": "ipfs"
        });
        assert!(validate_content("STORE", &content).is_ok());
    }

    #[test]
    fn test_valid_program() {
        let content = json!({
            "address": "0x1234",
            "code": {"encoding": "zip", "entrypoint": "main:app"},
            "runtime": {"ref": "abc123", "use_latest": true}
        });
        assert!(validate_content("PROGRAM", &content).is_ok());
    }

    #[test]
    fn test_valid_instance() {
        let content = json!({
            "address": "0x1234",
            "rootfs": {"parent": {"ref": "abc"}},
            "resources": {"vcpus": 1, "memory": 256}
        });
        assert!(validate_content("INSTANCE", &content).is_ok());
    }

    #[test]
    fn test_unknown_type() {
        let content = json!({"address": "0x1234"});
        let result = validate_content("UNKNOWN", &content);
        assert!(result.is_err());
    }
}
