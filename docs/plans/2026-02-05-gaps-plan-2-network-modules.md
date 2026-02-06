# Gaps Plan 2: P2P Network Layer & Placeholder Modules

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement real P2P networking (peer connections, message sending, event handling) and flesh out the empty storage/schemas placeholder modules.

**Architecture:** The P2P layer currently has the structure (PeerManager, event loop, protocol definitions) but all actual network I/O is stubbed. pyaleph uses libp2p GossipSub via a separate `p2p-service` sidecar that bridges to RabbitMQ. Our node already consumes from RabbitMQ via `p2p_consumer.rs`, so the P2P gap is specifically about direct peer-to-peer communication for nodes that don't run the p2p-service sidecar. The storage module should provide a local file storage abstraction for cached content. The schemas module should provide message content validation.

**Tech Stack:** Rust, tokio, TCP/QUIC networking, serde, sqlx

**Priority:** LOW — The node functions correctly without these. RabbitMQ + p2p-service handles P2P, IPFS handles storage, and message handlers do their own validation. These are "nice to have" for standalone operation.

---

## Context

The current node works in production because:
- P2P messages come through RabbitMQ (via the p2p-service sidecar) → `p2p_consumer.rs`
- Content storage is handled by IPFS (Kubo) → `services/ipfs.rs`
- Message validation happens in each handler's `validate()` method

These tasks add direct P2P capability (no sidecar needed), local content caching, and centralized schema validation.

---

### Task 1: Implement P2P peer connection

**Files:**
- Modify: `src/network/mod.rs:87-109`
- Reference: `src/network/protocol.rs` for message format

**Step 1: Read the full network module**

Read `src/network/mod.rs`, `src/network/peer.rs`, `src/network/protocol.rs` to understand the existing structures: `PeerManager`, `PeerInfo`, `PeerId`, protocol message types.

**Step 2: Decide on transport**

Options:
- **TCP + length-prefixed frames** — simplest, matches pyaleph's direct TCP approach
- **QUIC via quinn** — modern, built-in multiplexing, but adds dependency
- **libp2p** — full compatibility with p2p-service, but very heavy dependency

Recommended: **TCP + tokio + length-prefixed framing** for simplicity. The protocol.rs already defines message types.

**Step 3: Implement `connect_peer`**

```rust
pub async fn connect_peer(&self, addr: &str) -> anyhow::Result<PeerId> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    let (reader, writer) = stream.into_split();

    let peer_id = PeerId::from_address(addr);

    // Store connection handles
    let peer_info = PeerInfo {
        id: peer_id.clone(),
        address: addr.to_string(),
        connected_at: chrono::Utc::now(),
        writer: Some(Arc::new(tokio::sync::Mutex::new(writer))),
        reader_handle: Some(tokio::spawn(Self::read_loop(peer_id.clone(), reader, self.event_tx.clone()))),
        messages_sent: 0,
        messages_received: 0,
    };

    self.peers.write().await.insert(peer_id.clone(), peer_info);
    info!("Connected to peer: {} at {}", peer_id, addr);
    Ok(peer_id)
}

async fn read_loop(
    peer_id: PeerId,
    mut reader: tokio::net::tcp::OwnedReadHalf,
    event_tx: tokio::sync::mpsc::Sender<NetworkEvent>,
) {
    let mut len_buf = [0u8; 4];
    loop {
        // Read 4-byte length prefix
        if tokio::io::AsyncReadExt::read_exact(&mut reader, &mut len_buf).await.is_err() {
            let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
            return;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 10 * 1024 * 1024 {
            // Message too large (>10MB), disconnect
            let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
            return;
        }

        let mut msg_buf = vec![0u8; len];
        if tokio::io::AsyncReadExt::read_exact(&mut reader, &mut msg_buf).await.is_err() {
            let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
            return;
        }

        match serde_json::from_slice(&msg_buf) {
            Ok(msg) => {
                let _ = event_tx.send(NetworkEvent::MessageReceived(peer_id.clone(), msg)).await;
            }
            Err(e) => {
                tracing::warn!("Failed to deserialize message from {}: {}", peer_id, e);
            }
        }
    }
}
```

**Step 4: Verify compilation**
```bash
cargo check
```

**Step 5: Commit**
```bash
git add src/network/mod.rs
git commit -m "feat: implement TCP peer connection with length-prefixed framing"
```

---

### Task 2: Implement P2P message sending

**Files:**
- Modify: `src/network/mod.rs:136-148`

**Step 1: Implement `send_to_peer`**

```rust
pub async fn send_to_peer(&self, peer_id: &PeerId, message: &Message) -> anyhow::Result<()> {
    let peers = self.peers.read().await;
    let peer = peers.get(peer_id)
        .ok_or_else(|| anyhow::anyhow!("Peer not found: {}", peer_id))?;

    let writer = peer.writer.as_ref()
        .ok_or_else(|| anyhow::anyhow!("No connection to peer: {}", peer_id))?;

    let payload = serde_json::to_vec(message)?;
    let len = (payload.len() as u32).to_be_bytes();

    let mut writer = writer.lock().await;
    tokio::io::AsyncWriteExt::write_all(&mut *writer, &len).await?;
    tokio::io::AsyncWriteExt::write_all(&mut *writer, &payload).await?;
    tokio::io::AsyncWriteExt::flush(&mut *writer).await?;

    drop(writer);
    drop(peers);

    // Update stats
    let mut peers = self.peers.write().await;
    if let Some(peer) = peers.get_mut(peer_id) {
        peer.messages_sent += 1;
    }

    Ok(())
}
```

**Step 2: Add broadcast method**

```rust
pub async fn broadcast(&self, message: &Message) -> Vec<(PeerId, anyhow::Error)> {
    let peer_ids: Vec<PeerId> = self.peers.read().await.keys().cloned().collect();
    let mut errors = Vec::new();

    for peer_id in &peer_ids {
        if let Err(e) = self.send_to_peer(peer_id, message).await {
            errors.push((peer_id.clone(), e));
        }
    }

    errors
}
```

**Step 3: Verify and commit**
```bash
cargo check
git add src/network/mod.rs
git commit -m "feat: implement P2P message sending and broadcast"
```

---

### Task 3: Implement P2P event processing

**Files:**
- Modify: `src/network/mod.rs:189-196`

**Step 1: Read how the event loop dispatches events**

The event loop receives `NetworkEvent` variants. Currently `MessageReceived` and `SyncRequest` are TODO.

**Step 2: Implement event handlers**

```rust
NetworkEvent::MessageReceived(peer_id, message) => {
    tracing::debug!("Received message from {}: {}", peer_id, message.item_hash);

    // Update peer stats
    if let Some(peer) = self.peers.write().await.get_mut(&peer_id) {
        peer.messages_received += 1;
    }

    // Insert into pending_messages for processing
    if let Some(ref pool) = self.pool {
        let msg_json = serde_json::to_string(&message).unwrap_or_default();
        let _ = sqlx::query(
            r#"
            INSERT INTO pending_messages (item_hash, message_type, item_type, item_content, sender, chain, channel, signature, time, source, retries, next_attempt, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'p2p_direct', 0, NOW(), NOW())
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&message.item_hash)
        .bind(message.message_type.as_str())
        .bind(message.item_type.as_deref())
        .bind(&message.item_content)
        .bind(&message.sender)
        .bind(message.chain.as_str())
        .bind(&message.channel)
        .bind(&message.signature)
        .bind(message.time)
        .execute(pool)
        .await;
    }
}

NetworkEvent::SyncRequest(peer_id, request) => {
    tracing::info!("Sync request from {}: {:?}", peer_id, request);
    // Respond with messages matching the sync criteria
    if let Some(ref pool) = self.pool {
        let messages: Vec<Message> = sqlx::query_as(
            "SELECT * FROM messages WHERE time > $1 ORDER BY time ASC LIMIT 1000"
        )
        .bind(request.since_time)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        for msg in &messages {
            let _ = self.send_to_peer(&peer_id, msg).await;
        }
    }
}
```

**Step 3: Verify and commit**
```bash
cargo check
git add src/network/mod.rs
git commit -m "feat: implement P2P event processing for messages and sync requests"
```

---

### Task 4: Implement storage module

**Files:**
- Modify: `src/storage/mod.rs`

**Step 1: Design the storage abstraction**

The storage module provides local file caching. Content is stored by hash on disk, with IPFS as the primary backend and local disk as a read-through cache.

**Step 2: Implement the module**

```rust
//! Local content storage (file-based cache)
//!
//! Provides a disk-backed cache for message content. Files are stored
//! by their content hash under the configured data directory.
//! This acts as a read-through cache in front of IPFS.

use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Local file storage for content caching
pub struct LocalStorage {
    base_dir: PathBuf,
}

impl LocalStorage {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            base_dir: data_dir.join("storage"),
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.base_dir).await
    }

    /// Get content by hash from local storage
    pub async fn get(&self, hash: &str) -> Option<Vec<u8>> {
        let path = self.path_for(hash);
        fs::read(&path).await.ok()
    }

    /// Store content by hash
    pub async fn put(&self, hash: &str, content: &[u8]) -> std::io::Result<()> {
        let path = self.path_for(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = fs::File::create(&path).await?;
        file.write_all(content).await?;
        file.flush().await?;
        Ok(())
    }

    /// Check if content exists locally
    pub async fn exists(&self, hash: &str) -> bool {
        self.path_for(hash).exists()
    }

    /// Remove content by hash
    pub async fn remove(&self, hash: &str) -> std::io::Result<()> {
        let path = self.path_for(hash);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    /// Get the total size of cached content
    pub async fn cache_size(&self) -> std::io::Result<u64> {
        let mut total = 0u64;
        let mut entries = fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(metadata) = entry.metadata().await {
                total += metadata.len();
            }
        }
        Ok(total)
    }

    /// File path for a given content hash
    /// Uses two-level directory sharding: ab/cd/abcdef...
    fn path_for(&self, hash: &str) -> PathBuf {
        if hash.len() >= 4 {
            self.base_dir
                .join(&hash[0..2])
                .join(&hash[2..4])
                .join(hash)
        } else {
            self.base_dir.join(hash)
        }
    }
}
```

**Step 3: Verify and commit**
```bash
cargo check
git add src/storage/mod.rs
git commit -m "feat: implement local file storage module with disk-backed cache"
```

---

### Task 5: Implement schemas module

**Files:**
- Modify: `src/schemas/mod.rs`

**Step 1: Design schema validation**

The schemas module validates message content structure before processing. Each message type has required fields. This centralizes validation that's currently spread across handlers.

**Step 2: Implement the module**

```rust
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

fn require_string(content: &Value, field: &str, errors: &mut Vec<SchemaError>) -> Option<String> {
    require_field(content, field, errors)
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .or_else(|| {
            errors.push(SchemaError::InvalidType {
                field: field.to_string(),
                expected: "string".to_string(),
            });
            None
        })
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
}
```

**Step 3: Verify and commit**
```bash
cargo check && cargo test schemas
git add src/schemas/mod.rs
git commit -m "feat: implement message content schema validation module"
```

---

### Task 6: Build, test, deploy

**Step 1: Run full test suite**
```bash
cargo test
```

**Step 2: Build release**
```bash
cargo build --release
```

**Step 3: Deploy to dev server**
```bash
ssh root@2a01:240:ad00:2503:3:d785:ba28:c781 "systemctl stop pyaleph-rs"
scp target/release/aleph-core root@[2a01:240:ad00:2503:3:d785:ba28:c781]:/root/aleph-core
ssh root@2a01:240:ad00:2503:3:d785:ba28:c781 "systemctl start pyaleph-rs"
```

**Step 4: Verify**
```bash
ssh root@2a01:240:ad00:2503:3:d785:ba28:c781 "tail -50 /tmp/aleph-core.log"
```
