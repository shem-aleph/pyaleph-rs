# Gaps Plan 1: Maintenance Jobs & Data Correctness

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement the stubbed cleanup/maintenance jobs and fix database accessor filtering so the node doesn't leak resources and returns correct filtered results.

**Architecture:** Fill in existing stub functions with real SQL queries. The structure already exists — cleanup.rs has the job loop, accessors.rs has the function signatures — we just need to add the actual logic.

**Tech Stack:** Rust, sqlx (raw SQL), PostgreSQL, tokio

---

## Context

These gaps cause operational problems over time:
- Pending messages with too many retries accumulate forever
- Expired credits never get zeroed out
- Cache directory grows unbounded
- IPFS pins for deleted messages never get cleaned
- `db/accessors.rs` methods accept filter params but ignore them (returning unfiltered data)
- Ethereum chain sync finds message hashes but never queues them for IPFS fetching
- Tezos messages can't have their signatures verified

---

### Task 1: Implement pending message cleanup

**Files:**
- Modify: `src/jobs/cleanup.rs:65-69`
- Test: manual verification via `cargo test cleanup` (if tests exist) or SQL inspection

**Step 1: Read the current stub**

The function at line 65-69 currently returns `Ok(0)`. It needs a `PgPool` parameter instead of `Config`.

**Step 2: Implement the cleanup function**

Replace the stub with:

```rust
async fn cleanup_pending_messages(pool: &PgPool) -> Result<u32, CleanupError> {
    let result = sqlx::query(
        r#"
        DELETE FROM pending_messages
        WHERE retries > 10
        AND next_attempt < NOW() - INTERVAL '1 day'
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| CleanupError::Database(e.to_string()))?;

    Ok(result.rows_affected() as u32)
}
```

**Step 3: Update the caller**

The `run_cleanup` function needs to pass `pool` to this function. Check how it's called and ensure a `PgPool` is available in the cleanup job context. If the cleanup job only has `Config`, add `pool: &PgPool` to the `run_cleanup` signature and thread it through from `jobs/mod.rs` or `main.rs`.

**Step 4: Verify compilation**

Run: `cargo check`

**Step 5: Commit**

```bash
git add src/jobs/cleanup.rs
git commit -m "feat: implement pending message cleanup job"
```

---

### Task 2: Implement expired credit cleanup

**Files:**
- Modify: `src/jobs/cleanup.rs:72-76`

**Step 1: Implement the cleanup function**

```rust
async fn cleanup_expired_credits(pool: &PgPool) -> Result<u32, CleanupError> {
    // Zero out expired credit balances
    let result = sqlx::query(
        r#"
        UPDATE credit_balances
        SET balance = 0
        WHERE expiration IS NOT NULL
        AND expiration < NOW()
        AND balance > 0
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| CleanupError::Database(e.to_string()))?;

    Ok(result.rows_affected() as u32)
}
```

Note: Check the actual `credit_balances` table schema first — the column might be named differently. Run:
```sql
SELECT column_name FROM information_schema.columns WHERE table_name = 'credit_balances';
```

**Step 2: Verify and commit**

```bash
cargo check
git add src/jobs/cleanup.rs
git commit -m "feat: implement expired credit cleanup job"
```

---

### Task 3: Implement cache cleanup

**Files:**
- Modify: `src/jobs/cleanup.rs:79-101`

**Step 1: Implement LRU cache cleanup**

```rust
async fn cleanup_cache(config: &Config) -> Result<u64, CleanupError> {
    let cache_dir = config.node.data_dir.join("cache");
    if !cache_dir.exists() {
        return Ok(0);
    }

    let max_cache_bytes: u64 = 10 * 1024 * 1024 * 1024; // 10 GB default
    let mut entries: Vec<(std::path::PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total_size: u64 = 0;

    let mut read_dir = tokio::fs::read_dir(&cache_dir).await
        .map_err(|e| CleanupError::Io(e.to_string()))?;

    while let Some(entry) = read_dir.next_entry().await
        .map_err(|e| CleanupError::Io(e.to_string()))? {
        if let Ok(metadata) = entry.metadata().await {
            if metadata.is_file() {
                let size = metadata.len();
                let accessed = metadata.accessed().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                total_size += size;
                entries.push((entry.path(), size, accessed));
            }
        }
    }

    if total_size <= max_cache_bytes {
        return Ok(0);
    }

    // Sort by access time ascending (oldest first)
    entries.sort_by_key(|e| e.2);

    let mut freed: u64 = 0;
    for (path, size, _) in &entries {
        if total_size - freed <= max_cache_bytes {
            break;
        }
        if tokio::fs::remove_file(&path).await.is_ok() {
            freed += size;
        }
    }

    Ok(freed)
}
```

**Step 2: Verify and commit**

```bash
cargo check
git add src/jobs/cleanup.rs
git commit -m "feat: implement LRU cache cleanup job"
```

---

### Task 4: Implement garbage collector orphaned pin cleanup

**Files:**
- Modify: `src/jobs/garbage_collector.rs:186-192`

**Step 1: Read the current garbage_collector.rs to understand the full context**

Check what data is available — `PgPool` and `IpfsService` references.

**Step 2: Implement orphaned pin cleanup**

The approach: query `file_pins` table for all pinned hashes, then check if any reference a deleted/forgotten message. If so, unpin from IPFS and remove from `file_pins`.

```rust
async fn clean_orphaned_pins(db: &PgPool, ipfs: &IpfsService) -> Result<u64, GcError> {
    // Find file_pins that reference forgotten or deleted messages
    let orphaned: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT fp.item_hash
        FROM file_pins fp
        LEFT JOIN messages m ON m.item_hash = fp.item_hash
        WHERE m.item_hash IS NULL
        LIMIT 100
        "#
    )
    .fetch_all(db)
    .await
    .map_err(|e| GcError::Database(e.to_string()))?;

    let mut cleaned = 0u64;
    for (hash,) in &orphaned {
        // Try to unpin from IPFS (ignore errors — pin may already be gone)
        let _ = ipfs.unpin(hash).await;
        // Remove from file_pins table
        if sqlx::query("DELETE FROM file_pins WHERE item_hash = $1")
            .bind(hash)
            .execute(db)
            .await
            .is_ok()
        {
            cleaned += 1;
        }
    }

    Ok(cleaned)
}
```

**Step 3: Verify and commit**

```bash
cargo check
git add src/jobs/garbage_collector.rs
git commit -m "feat: implement orphaned IPFS pin cleanup in garbage collector"
```

---

### Task 5: Fix message accessor filtering

**Files:**
- Modify: `src/db/accessors.rs:19-35`

**Step 1: Read the full accessors.rs to understand the structs and query patterns**

**Step 2: Implement proper filtering in `MessageAccessor::list`**

Replace the stub that ignores filters with a dynamic query:

```rust
pub async fn list(
    pool: &PgPool,
    addresses: Option<&[String]>,
    message_type: Option<&str>,
    channel: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<MessageDb>, sqlx::Error> {
    let mut query = String::from("SELECT * FROM messages WHERE 1=1");
    let mut param_idx = 1;

    if let Some(addrs) = addresses {
        if !addrs.is_empty() {
            let placeholders: Vec<String> = addrs.iter().enumerate()
                .map(|(i, _)| format!("${}", param_idx + i))
                .collect();
            query.push_str(&format!(" AND sender IN ({})", placeholders.join(", ")));
            param_idx += addrs.len();
        }
    }

    if message_type.is_some() {
        query.push_str(&format!(" AND message_type = ${}", param_idx));
        param_idx += 1;
    }

    if channel.is_some() {
        query.push_str(&format!(" AND channel = ${}", param_idx));
        param_idx += 1;
    }

    query.push_str(&format!(" ORDER BY time DESC LIMIT ${} OFFSET ${}", param_idx, param_idx + 1));

    // Build and bind dynamically using sqlx::query_as with raw SQL
    // Note: sqlx doesn't support dynamic bind count easily.
    // Use QueryBuilder from db/query_builder.rs instead.
    // ... (adapt to use existing QueryBuilder pattern)
}
```

**Important:** Check if `src/db/query_builder.rs` has a `QueryBuilder` that supports dynamic WHERE clauses — use that pattern instead of raw string building for safety.

**Step 3: Verify and commit**

```bash
cargo check
git add src/db/accessors.rs
git commit -m "feat: implement message list filtering in accessor"
```

---

### Task 6: Fix aggregate accessor key filtering

**Files:**
- Modify: `src/db/accessors.rs:63-75`

**Step 1: Add key filtering to aggregate accessor**

```rust
pub async fn get(
    pool: &PgPool,
    address: &str,
    keys: Option<&[String]>,
) -> Result<Vec<AggregateDb>, sqlx::Error> {
    if let Some(keys) = keys {
        if !keys.is_empty() {
            let placeholders: Vec<String> = keys.iter().enumerate()
                .map(|(i, _)| format!("${}", i + 2))
                .collect();
            let query = format!(
                "SELECT * FROM aggregates WHERE address = $1 AND key IN ({})",
                placeholders.join(", ")
            );
            // Use query_as with dynamic bind
            let mut q = sqlx::query_as::<_, AggregateDb>(&query).bind(address);
            for key in keys {
                q = q.bind(key);
            }
            return q.fetch_all(pool).await;
        }
    }

    sqlx::query_as::<_, AggregateDb>(
        "SELECT * FROM aggregates WHERE address = $1"
    )
    .bind(address)
    .fetch_all(pool)
    .await
}
```

**Step 2: Verify and commit**

```bash
cargo check
git add src/db/accessors.rs
git commit -m "feat: implement aggregate key filtering in accessor"
```

---

### Task 7: Implement Ethereum sync hash queuing

**Files:**
- Modify: `src/chains/ethereum.rs:278-282`

**Step 1: Read the surrounding context to understand how sync events are processed**

The function parses sync content and extracts message hashes. Currently the hashes are found but never fetched.

**Step 2: Queue hashes into pending_messages**

After extracting hashes, insert them into `pending_messages` so the message processor will pick them up and fetch content from IPFS/peers:

```rust
// After parsing the sync content hashes...
for hash in &sync_hashes {
    sqlx::query(
        r#"
        INSERT INTO pending_messages (item_hash, item_type, source, retries, next_attempt, created_at)
        VALUES ($1, 'ipfs', 'chain_sync', 0, NOW(), NOW())
        ON CONFLICT (item_hash) DO NOTHING
        "#
    )
    .bind(hash)
    .execute(pool)
    .await
    .ok(); // Don't fail the whole sync on individual insert errors
}
```

**Step 3: Verify and commit**

```bash
cargo check
git add src/chains/ethereum.rs
git commit -m "feat: queue ethereum sync hashes for IPFS fetching"
```

---

### Task 8: Add Tezos signature verification

**Files:**
- Modify: `src/services/crypto.rs:293-308`
- Add dependency: `tezos-sig` or manual Ed25519/secp256k1/P256 verification

**Step 1: Research Tezos signature format**

Tezos uses three signature schemes based on address prefix:
- `tz1` → Ed25519
- `tz2` → secp256k1
- `tz3` → P256 (NIST)

The challenge: Tezos addresses are the hash of the public key, so you can't recover the pubkey from just the address. However, Aleph messages include the signature which is over the message bytes.

**Step 2: Implement basic Tezos verification**

For the Aleph use case, messages signed with Tezos typically include a Micheline-packed payload. The signature can be verified if we have the public key (which may be derivable from the signature for secp256k1, or may need to be stored).

If full verification is too complex, implement a "trust but log" approach for indexed messages (trusted_source=true) and reject direct submissions:

```rust
Chain::TEZOS => {
    if trusted_source {
        // Indexed messages are pre-verified by the network
        tracing::debug!("Tezos signature accepted from trusted source: {}", expected_address);
        Ok(())
    } else {
        Err(CryptoError::UnsupportedChain(
            "Direct Tezos signature verification not yet supported".to_string()
        ))
    }
}
```

**Step 3: Verify and commit**

```bash
cargo check
git add src/services/crypto.rs
git commit -m "feat: accept Tezos signatures from trusted sources"
```

---

### Task 9: Build, deploy, and verify

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

**Step 4: Verify cleanup jobs are running**
```bash
ssh root@2a01:240:ad00:2503:3:d785:ba28:c781 "grep -i 'cleanup\|garbage' /tmp/aleph-core.log | tail -20"
```

**Step 5: Commit any final fixes**
