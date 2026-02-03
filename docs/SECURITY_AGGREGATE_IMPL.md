# Security Aggregate System — Implementation Specification for pyaleph-rs

## Overview

The **Security Aggregate** is Aleph Cloud's delegation/authorization system. It allows an address (the "owner") to grant other addresses ("delegates") permission to post messages on its behalf. This is implemented as a special aggregate with key `"security"` containing an `authorizations` array.

This document specifies the full implementation needed in pyaleph-rs to match the Python pyaleph reference implementation.

---

## 1. How It Works (Conceptual)

### The Security Aggregate

Any Aleph address can publish an AGGREGATE message with `key: "security"` to define who can act on their behalf. The aggregate content looks like:

```json
{
  "authorizations": [
    {
      "address": "0xDelegateAccount...",
      "chain": "ETH",                    // optional: restrict to specific chain
      "channels": ["MY_APP"],             // optional: restrict to specific channels
      "types": ["POST", "AGGREGATE"],     // optional: restrict to message types
      "post_types": ["blog", "comment"],  // optional: restrict to specific post types
      "aggregate_keys": ["settings"]      // optional: restrict to specific aggregate keys
    }
  ]
}
```

### Authorization Flow

When a message arrives where `sender != content.address`:
1. Look up the `"security"` aggregate for `content.address` (the owner)
2. Search the `authorizations` array for an entry matching the sender
3. If found, check all optional filters (chain, channels, types, post_types, aggregate_keys)
4. If all filters pass, the message is authorized

### Special Case: POST Amend Messages

When a delegated account amends a post:
1. Look up the **original post** via the amend's `ref` field
2. Check authorization against the **original post's owner/address**, not the amend's address
3. Verify the amend's `content.address` matches the original post's `owner` (prevents hijacking)
4. If the original post can't be found, fall back to standard permission checking

---

## 2. Files to Create/Modify

### 2.1 NEW: `src/permissions.rs` — Core Authorization Module

This is the heart of the system. Create this as a new top-level module.

```rust
// src/permissions.rs
//! Security aggregate permission checking
//!
//! Implements delegated authorization via the "security" aggregate key.
//! Reference: aleph/permissions.py

use serde_json::Value;
use sqlx::PgPool;

use crate::types::{Message, MessageType};
```

#### Required Functions

**`check_sender_authorization(pool, message) -> Result<bool, Error>`**

Main entry point. Called for every message where `sender != content.address`.

Logic:
1. Parse message content to extract `address` field
2. If `sender == address`, return `true` (self-authorized)
3. If message is a POST with `type == "amend"` and has a `ref`:
   a. Look up the original message by `ref` item_hash
   b. If found, extract original's `content.address`
   c. Verify amend's `content.address == original.content.address` (owner match)
   d. If mismatch, return `false`
   e. Call `check_delegated_authorization(pool, sender, original_address, original_message)`
   f. If original not found, fall through to standard check
4. Call `check_delegated_authorization(pool, sender, address, message)`

**`check_delegated_authorization(pool, sender, owner_address, message) -> Result<bool, Error>`**

Checks if `sender` has delegated authorization from `owner_address`.

Logic:
1. If `sender == owner_address`, return `true`
2. Query: `SELECT content FROM aggregates WHERE owner = $1 AND key = 'security'` for `owner_address`
3. If no aggregate found, return `false`
4. Extract `authorizations` array from aggregate content
5. For each authorization entry:
   a. If `auth.address != sender`, skip
   b. If `auth.chain` is set AND `message.chain != auth.chain`, skip
   c. If `auth.channels` is non-empty AND `message.channel` not in `auth.channels`, skip
   d. If `auth.types` is non-empty AND `message.message_type` not in `auth.types`, skip
   e. If message type is POST and `auth.post_types` is non-empty AND `content.type` not in `auth.post_types`, skip
   f. If message type is AGGREGATE and `auth.aggregate_keys` is non-empty AND `content.key` not in `auth.aggregate_keys`, skip
   g. All checks passed → return `true`
6. No matching authorization found → return `false`

#### Authorization Entry Struct

```rust
#[derive(Debug, Deserialize)]
pub struct SecurityAuthorization {
    pub address: String,
    #[serde(default)]
    pub chain: Option<String>,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default, rename = "types")]
    pub message_types: Vec<String>,
    #[serde(default)]
    pub post_types: Vec<String>,
    #[serde(default)]
    pub aggregate_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SecurityAggregateContent {
    #[serde(default)]
    pub authorizations: Vec<SecurityAuthorization>,
}
```

### 2.2 MODIFY: `src/handlers/mod.rs` — Add Permission Checking to Pipeline

The `process_message` function currently only validates and processes. It needs a permission check step between validation and processing.

**Current flow:**
```
validate → process
```

**New flow:**
```
validate → check_permissions → process
```

Add to the `MessageHandler` trait:
```rust
/// Check if the message sender is authorized
/// Default implementation checks the security aggregate
async fn check_permissions(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
    // Default: use security aggregate check
    // Parse content to get address field
    if let Some(ref content_str) = message.item_content {
        if let Ok(content) = serde_json::from_str::<serde_json::Value>(content_str) {
            if let Some(address) = content.get("address").and_then(|a| a.as_str()) {
                if message.sender.to_lowercase() != address.to_lowercase() {
                    // Need to check security aggregate
                    let pool = ctx.db.as_ref()
                        .ok_or(HandlerError::Database("No database".into()))?;
                    let authorized = crate::permissions::check_sender_authorization(pool, message).await
                        .map_err(|e| HandlerError::Database(e.to_string()))?;
                    if !authorized {
                        return Err(HandlerError::PermissionDenied(
                            format!("Sender {} is not authorized to post on behalf of {}", message.sender, address)
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
```

Update `process_message`:
```rust
pub async fn process_message(message: &Message, ctx: &HandlerContext) -> ProcessingStatus {
    let handler = get_handler(message);
    
    if let Err(e) = handler.validate(message, ctx).await { return e.into(); }
    if let Err(e) = handler.check_permissions(message, ctx).await { return e.into(); }
    
    match handler.process(message, ctx).await {
        Ok(()) => ProcessingStatus::processed(),
        Err(e) => e.into(),
    }
}
```

### 2.3 MODIFY: `src/handlers/aggregate.rs` — Fix Validation

The current `validate` method incorrectly rejects all messages where `sender != content.address`. This MUST be removed because delegated authorization allows exactly this case. The permission check should happen in `check_permissions`, not in `validate`.

**Remove this from `validate()`:**
```rust
// REMOVE THIS - it blocks delegated authorization
if message.sender.to_lowercase() != content.address.to_lowercase() {
    return Err(HandlerError::Unauthorized);
}
```

The aggregate handler should keep validation of structure (key not empty, content is object) but NOT check sender == address. That's the job of `check_permissions`.

### 2.4 MODIFY: `src/handlers/post.rs` — Add Amend Permission Check

The PostHandler needs a custom `check_permissions` override that:

1. Calls the default security aggregate check (via super/base)
2. For amend messages, adds an additional check: the amend's `content.address` must match the original post's `owner`

```rust
async fn check_permissions(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
    // Standard security aggregate check first
    // (call default implementation)
    
    // Additional check for amends
    if let Some(content) = parse_post_content(message) {
        if content.type_ == "amend" {
            if let Some(ref target_ref) = content.ref_ {
                if let Some(db) = ctx.db.as_ref() {
                    if let Ok(Some(original)) = db.get_post(target_ref).await {
                        if original.address.to_lowercase() != content.address.to_lowercase() {
                            return Err(HandlerError::PermissionDenied(format!(
                                "Cannot amend post {}: amend address {} does not match original owner {}",
                                target_ref, content.address, original.address
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
```

### 2.5 MODIFY: `src/db/accessors.rs` — Add Aggregate Query

Add a method to `AggregateAccessor` to get a single aggregate by owner + key:

```rust
impl AggregateAccessor {
    /// Get a specific aggregate by owner and key
    /// Used by the security permission system
    pub async fn get_by_key(
        pool: &PgPool,
        owner: &str,
        key: &str,
    ) -> Result<Option<AggregateDb>, sqlx::Error> {
        sqlx::query_as::<_, AggregateDb>(
            "SELECT * FROM aggregates WHERE owner = $1 AND key = $2"
        )
        .bind(owner)
        .bind(key)
        .fetch_optional(pool)
        .await
    }
}
```

Note: The column name in the Python schema uses `owner` for the aggregates table. The Rust models currently use `address`. Verify column names match the actual PostgreSQL schema:
- Python: `AggregateDb.owner`, `AggregateDb.key`
- Current Rust: `AggregateDb.address`, `AggregateDb.key`

If the actual table uses `owner`, update the Rust model to use `owner` (or add `#[sqlx(rename = "owner")]`).

### 2.6 MODIFY: `src/handlers/forget.rs` — Permission Check for Forget

The forget handler also needs security aggregate checking. A sender can forget a message if:
- They are the message sender, OR
- They have delegated authorization from the message's content address

This should work automatically via the default `check_permissions` implementation if the forget content includes an `address` field. Verify this works correctly.

### 2.7 MODIFY: `src/lib.rs` or `src/main.rs` — Register the Module

Add `pub mod permissions;` to the module tree.

---

## 3. Database Dependencies

### Required Query: Get Security Aggregate

```sql
SELECT content FROM aggregates 
WHERE owner = $1 AND key = 'security'
```

This must work with the existing `aggregates` table. The `content` column is JSONB and contains the security configuration.

### Required Query: Get Message by Item Hash

```sql
SELECT * FROM messages WHERE item_hash = $1
```

Already implemented in `MessageAccessor::get_by_hash`. Used for the amend→original lookup in permission checking.

### Dirty Aggregate Handling

When querying the security aggregate, if `dirty = true`, it should ideally be refreshed first (rebuild from aggregate_elements). However, for the initial implementation, reading the current content is acceptable since dirty aggregates are rare for security keys.

---

## 4. Integration with `HandlerContext`

The current `HandlerContext` uses trait objects (`Arc<dyn Database>`). The permissions module needs direct access to the PostgreSQL pool for aggregate queries.

**Option A (recommended):** Add `pool: Option<Arc<PgPool>>` to `HandlerContext` alongside the existing `db` field. The permissions module uses `pool` directly.

**Option B:** Add a `get_aggregate_by_key` method to the `Database` trait:
```rust
async fn get_aggregate_by_key(&self, owner: &str, key: &str) -> Result<Option<serde_json::Value>, String>;
```

Option B is cleaner for testing (can mock), but Option A is simpler. Either works.

---

## 5. Filter Matching Rules (Exact Semantics)

These rules come directly from the Python implementation and MUST be followed exactly:

| Filter | Condition to SKIP (continue to next auth) |
|--------|------------------------------------------|
| `address` | `auth.address != sender` |
| `chain` | `auth.chain` is set AND `message.chain != auth.chain` |
| `channels` | `channels` is non-empty AND `message.channel` not in `channels` |
| `types` | `types` is non-empty AND `message.type` not in `types` |
| `post_types` | Message is POST AND `post_types` is non-empty AND `content.type` not in `post_types` |
| `aggregate_keys` | Message is AGGREGATE AND `aggregate_keys` is non-empty AND `content.key` not in `aggregate_keys` |

**Important:** Empty arrays mean "allow all" for that filter. Only non-empty arrays restrict.

**Case sensitivity:** Address comparison should be case-insensitive (`.to_lowercase()`). Chain, channel, types, and post_types should match as-is (the Python code doesn't lowercase them).

---

## 6. Message Type String Mapping

The Python code uses `MessageType` enum values. In the security aggregate's `types` filter, these are stored as strings:
- `"POST"` 
- `"AGGREGATE"`
- `"STORE"`
- `"PROGRAM"`
- `"INSTANCE"`
- `"FORGET"`

Ensure the Rust implementation compares against the same string representations.

---

## 7. Test Cases

### 7.1 Basic Authorization

```rust
#[test]
fn test_self_authorized() {
    // sender == content.address → always authorized
}

#[test]
fn test_no_security_aggregate() {
    // sender != address, no security aggregate exists → denied
}

#[test]
fn test_basic_delegation() {
    // Security aggregate has sender in authorizations with no filters → authorized
}
```

### 7.2 Filter Tests

```rust
#[test]
fn test_chain_filter_match() {
    // auth.chain == message.chain → authorized
}

#[test]
fn test_chain_filter_mismatch() {
    // auth.chain != message.chain → denied
}

#[test]
fn test_channel_filter() {
    // message.channel in auth.channels → authorized
    // message.channel not in auth.channels → denied
}

#[test]
fn test_type_filter() {
    // message.type in auth.types → authorized
    // message.type not in auth.types → denied
}

#[test]
fn test_post_type_filter() {
    // POST message with content.type in auth.post_types → authorized
    // POST message with content.type not in auth.post_types → denied
}

#[test]
fn test_aggregate_key_filter() {
    // AGGREGATE message with content.key in auth.aggregate_keys → authorized
    // AGGREGATE message with content.key not in auth.aggregate_keys → denied
}

#[test]
fn test_empty_filters_allow_all() {
    // auth with empty channels, types, etc. → authorized (no restrictions)
}
```

### 7.3 Amend Tests

```rust
#[test]
fn test_delegated_amend_authorized() {
    // Delegate can amend a post if they have POST permission for the original owner
}

#[test]
fn test_delegated_amend_wrong_owner() {
    // Amend with different content.address than original post → denied
}

#[test]
fn test_amend_missing_original() {
    // Original post not found → falls back to standard check
}
```

### 7.4 Edge Cases

```rust
#[test]
fn test_multiple_authorizations() {
    // Multiple auth entries, first doesn't match, second does → authorized
}

#[test]
fn test_authorization_with_all_filters() {
    // Auth entry with chain + channels + types + post_types → must match ALL
}

#[test]
fn test_case_insensitive_address() {
    // Mixed case addresses should still match
}
```

---

## 8. Implementation Order

1. **Create `src/permissions.rs`** with structs and core logic
2. **Add `get_by_key`** to `AggregateAccessor` in `src/db/accessors.rs`
3. **Add `pool` field** to `HandlerContext` (or add method to Database trait)
4. **Add `check_permissions`** to `MessageHandler` trait with default implementation
5. **Update `process_message`** to call `check_permissions` between validate and process
6. **Remove sender==address check** from `AggregateHandler::validate`
7. **Override `check_permissions`** in `PostHandler` for amend-specific logic
8. **Register `mod permissions`** in the module tree
9. **Write tests**
10. **Verify integration** — run existing tests, ensure nothing breaks

---

## 9. Reference Files

| Concept | Python Source | Rust Target |
|---------|-------------|-------------|
| Core permissions | `src/aleph/permissions.py` | `src/permissions.rs` |
| Permission check call | `src/aleph/handlers/content/content_handler.py:check_permissions()` | `src/handlers/mod.rs:MessageHandler::check_permissions()` |
| Post amend check | `src/aleph/handlers/content/post.py:check_permissions()` | `src/handlers/post.rs:PostHandler::check_permissions()` |
| Aggregate model | `src/aleph/db/models/aggregates.py` | `src/db/models.rs:AggregateDb` |
| Aggregate queries | `src/aleph/db/accessors/aggregates.py:get_aggregate_by_key()` | `src/db/accessors.rs:AggregateAccessor::get_by_key()` |
| Message handler pipeline | `src/aleph/handlers/message_handler.py:MessageHandler.process()` | `src/handlers/mod.rs:process_message()` |
| Test cases | `tests/permissions/test_check_sender_authorization.py` | `src/permissions.rs` (inline tests) |

---

## 10. Security Considerations

1. **Never skip the permission check** — every message where sender != address MUST go through the security aggregate system
2. **Address comparison must be case-insensitive** — Ethereum addresses can vary in checksumming
3. **Empty filter arrays = allow all** — this is by design, not a bug
4. **The amend owner-match check is critical** — without it, anyone with POST delegation could hijack posts by amending with a different address
5. **Security aggregates themselves need protection** — a delegate with AGGREGATE authorization and `aggregate_keys: ["security"]` could modify the security aggregate. This is intentional (allows security key management delegation)
