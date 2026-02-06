# STORE Messages: Handler Completion, Network Sharding, and Storage Tiering

## Context

The STORE message handler is currently an 87-line stub that just inserts `(item_hash, owner, size, content_type)` into a flat `file_pins` table. The Python implementation has full lifecycle management across ~1200 lines: ref-based file versioning, polymorphic pin types, grace period garbage collection, cost validation, and content fetching decisions.

Additionally, all nodes currently store ALL content. There's no network-level sharding — every node accumulates copies of everything. With 100+ nodes, this is wasteful. We need consistent hashing to assign content responsibility so each node stores ~3% of content (replication factor 3).

This plan covers three interconnected features:
- **A**: Complete the STORE handler to Python feature parity
- **B**: Network-level content sharding via consistent hashing
- **C**: Storage tiering (IPFS pin vs local cache vs warm cache)

---

## Phase 1: Database Schema & Types (foundation for everything)

### 1.1 New migration: `files` table
**File**: `src/db/migrations.rs`

```sql
CREATE TABLE IF NOT EXISTS files (
    hash VARCHAR(128) PRIMARY KEY,
    size BIGINT NOT NULL DEFAULT 0,
    file_type VARCHAR(20) NOT NULL DEFAULT 'file',  -- 'file' or 'directory'
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 1.2 Alter `file_pins` table
**File**: `src/db/migrations.rs` — new function `alter_file_pins_v2()`

Add columns via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`:
- `pin_type VARCHAR(20) NOT NULL DEFAULT 'message'` — message/tx/content/grace_period
- `ref_ TEXT` — for file versioning
- `delete_by TIMESTAMPTZ` — for grace period expiry
- `message_hash VARCHAR(128)` — the message that created this pin (distinct from the file's item_hash)

The PK stays `(item_hash, owner)` for now — we add a unique index on `(item_hash, owner, pin_type)` separately to allow multiple pin types per file+owner.

New indexes:
```sql
CREATE INDEX IF NOT EXISTS idx_file_pins_pin_type ON file_pins(pin_type);
CREATE INDEX IF NOT EXISTS idx_file_pins_delete_by ON file_pins(delete_by) WHERE delete_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_file_pins_message_hash ON file_pins(message_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_pins_unique_typed ON file_pins(item_hash, owner, pin_type);
```

### 1.3 Rebuild `file_tags` table
**File**: `src/db/migrations.rs`

The current schema `(item_hash, tag)` doesn't match Python's `(tag PK, owner, file_hash, last_updated)`. Add a new `file_tags_v2` table:

```sql
CREATE TABLE IF NOT EXISTS file_tags_v2 (
    tag VARCHAR(512) PRIMARY KEY,
    owner VARCHAR(256) NOT NULL,
    file_hash VARCHAR(128) NOT NULL,
    last_updated DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_file_tags_v2_owner ON file_tags_v2(owner);
CREATE INDEX IF NOT EXISTS idx_file_tags_v2_file_hash ON file_tags_v2(file_hash);
```

### 1.4 Add `ref_` to `StoreContent`
**File**: `src/types/message.rs` (line ~171)

```rust
#[serde(skip_serializing_if = "Option::is_none", rename = "ref")]
pub ref_: Option<String>,
```

### 1.5 New types
**File**: `src/handlers/mod.rs`

- `PinType` enum: `Message`, `Tx`, `Content`, `GracePeriod`
- Enhanced `FilePinRecord`: add `pin_type`, `ref_`, `delete_by`, `message_hash`
- `FileRecord`: `hash`, `size`, `file_type` (File/Directory)
- `FileTagRecord`: `tag`, `owner`, `file_hash`, `last_updated`

### 1.6 Expand `Database` trait
**File**: `src/handlers/mod.rs`

New methods:
- `upsert_file(hash, size, file_type)`
- `get_file(hash) -> Option<FileRecord>`
- `insert_file_pin_typed(pin: &FilePinRecord)`
- `get_pins_for_file(file_hash) -> Vec<FilePinRecord>`
- `get_message_file_pin(item_hash) -> Option<FilePinRecord>` — find pin by originating message hash
- `delete_file_pin_by_message(message_hash)`
- `count_active_pins(file_hash) -> i64` — count non-grace-period pins
- `insert_grace_period_pin(file_hash, delete_by)`
- `get_expired_grace_pins(limit) -> Vec<FilePinRecord>`
- `upsert_file_tag(tag: &FileTagRecord)`
- `get_file_tag(tag: &str) -> Option<FileTagRecord>`
- `refresh_file_tag(tag: &str)` — update to point to newest remaining pin

Implement all in `src/db/pg_database.rs`.

---

## Phase 2: Complete STORE Handler

### 2.1 Store handler rewrite
**File**: `src/handlers/store.rs` (currently 87 lines → ~350 lines)

**validate()** — add:
- Validate `item_type` matches hash format (IPFS CID vs SHA256 hex) — port `item_type_from_hash()`
- If `ref_` is set and looks like a message hash (64 hex chars): verify target message exists and target isn't itself a ref (no revision chains). User-defined string refs always pass.

**New method: check_permissions()** — override from trait:
- Call default security aggregate check (existing)
- If `ref_` is set: compute `make_file_tag(owner, ref, item_hash)`, check if tag exists, verify owner matches

**process()** — rewrite to:
1. Parse `StoreContent` including `ref_`
2. Upsert `files` table with hash, size, file_type (default to File; IPFS directory detection comes in Phase 4)
3. Insert `file_pins` with `pin_type = Message`, `message_hash = message.item_hash`, `ref_`
4. Compute and upsert file tag via `make_file_tag(content.address, ref_, message.item_hash)`

**Helper**: `make_file_tag(owner, ref_, item_hash) -> String`
```rust
fn make_file_tag(owner: &str, ref_: Option<&str>, item_hash: &str) -> String {
    match ref_ {
        Some(r) if !r.is_empty() => format!("{}:{}", owner.to_lowercase(), r),
        _ => format!("{}:{}", owner.to_lowercase(), item_hash),
    }
}
```

### 2.2 Add cost validation to STORE handler
**File**: `src/handlers/store.rs` + `src/services/cost.rs`

Add `calculate_store_message_costs()` method to `CostService`:
- Get file size from `files` table (or from `StoreContent.size`)
- Convert to MiB
- If size <= 25 MiB (MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE / MiB): free, return empty costs
- Otherwise: calculate using `ProductPriceType::Storage` pricing with existing `calculate_storage_cost()`
- Determine payment type from `StoreContent` (hold/payg/credit)
- Validate balance against cost using existing `get_balance()`/`get_credit_balance()`/`get_total_cost_for_address()`

The existing `CostService` already has `calculate_storage_cost()`, `AccountCostRecord`, and the `Database` trait already has `get_balance()`, `get_credit_balance()`, `get_total_cost_for_address()`, `store_account_costs()`. We reuse all of these.

**Note**: Python has `are_store_and_program_free(message)` which checks a cutoff height/timestamp. We replicate this check.

### 2.3 Update forget handler for grace periods
**File**: `src/handlers/forget.rs`

Current behavior (lines 177-215): removes file pin, immediately unpins from IPFS.

New behavior for STORE-related forgets:
1. Look up the forgotten message to get its type
2. If it's a STORE message:
   - Delete the MESSAGE-type pin (`delete_file_pin_by_message(hash)`)
   - Refresh file tag (`refresh_file_tag(make_file_tag(...))`)
   - Check remaining active pins (`count_active_pins(file_hash)`)
   - If 0 remaining: insert grace period pin with `delete_by = now + 24h`
   - Do NOT unpin from IPFS yet
3. Other message types: existing behavior

### 2.4 Update garbage collector
**File**: `src/jobs/garbage_collector.rs`

Add new GC step: process expired grace period pins:
1. Query `file_pins WHERE pin_type = 'grace_period' AND delete_by < NOW()`
2. For each: check if any non-grace pins appeared in the meantime
3. If still no active pins: unpin from IPFS, remove from local storage, delete from `file_pins` and `files`
4. If active pins appeared: just delete the grace period pin

### 2.5 Update schema validation
**File**: `src/schemas/mod.rs` — allow optional `ref` field in STORE validation

---

## Phase 3: Network-Level Content Sharding

### 3.1 Sharding service
**New file**: `src/services/sharding.rs`

**ContentRing** — consistent hash ring:
- 64-bit ring (0 to 2^64-1) using SipHash of `"{node_id}:{vnode_index}"`
- 64 virtual nodes per physical node (100+ node network)
- Replication factor K=3 (configurable)
- `BTreeMap<u64, String>` for ring positions → node_id
- `HashMap<String, String>` for node_id → http_address

Key methods:
- `add_node(node_id, http_address)` / `remove_node(node_id)`
- `get_responsible_nodes(content_hash) -> Vec<(node_id, http_address)>` — walk clockwise, collect K distinct physical nodes
- `is_responsible(content_hash) -> bool` — checks if our node_id is in the responsible set

**ShardingService** — wraps the ring:
- Holds `Arc<RwLock<ContentRing>>`
- `rebuild_from_peers(peers: &[(node_id, http_address)])` — called when peer set changes
- `get_routing_decision(content_hash) -> ContentDecision` (Owned/NotOwned)

**ContentDecision enum**:
```rust
enum ContentDecision {
    Owned { replicas: Vec<(String, String)> },   // (node_id, url) pairs
    NotOwned { responsible: Vec<(String, String)> },
}
```

### 3.2 Node identity from corechannel
**File**: `src/services/peers.rs`

The corechannel aggregate provides multiaddresses like `/ip4/X.X.X.X/tcp/4025/p2p/QmPeerIdHere`. Currently (line 186) we use the HTTP URL as peer_id. Change to:
- Extract the `/p2p/{peer_id}` component from the multiaddress
- Store as the canonical node identity
- Our own node's peer_id comes from config (`node.peer_id` or derived from keypair)
- After updating the peer set, call `sharding_service.rebuild_from_peers()`

### 3.3 Configuration
**File**: `src/config/mod.rs`

Add to `StorageConfig`:
```rust
pub sharding_enabled: bool,         // default: false
pub replication_factor: usize,      // default: 3
pub virtual_nodes: usize,           // default: 64
pub warm_cache_ttl_secs: u64,       // default: 3600 (1 hour)
pub warm_cache_max_bytes: u64,      // default: 1GB
```

Add to `NodeConfig` (or P2P config):
```rust
pub peer_id: Option<String>,        // Our libp2p peer ID for the hash ring
```

### 3.4 Integrate with content fetch
**File**: `src/services/content_fetch.rs`

Currently shuffles peers randomly (line ~204). Change to:
1. Call `sharding.get_responsible_nodes(item_hash)`
2. Try those nodes first (in order)
3. Fall back to random peers (existing behavior)

### 3.5 Integrate with peer discovery
**File**: `src/services/peers.rs`

After `check_all_http_peers` completes:
- Build list of `(peer_id, http_address)` from alive peers
- Call `sharding_service.rebuild_from_peers()`
- Log ring changes (nodes added/removed)

---

## Phase 4: Storage Tiering

### 4.1 Tiered storage
**New file**: `src/storage/tiered.rs`

**TieredStorage** wraps LocalStorage + IpfsService + ShardingService:

```rust
pub struct TieredStorage {
    owned_storage: LocalStorage,     // For content we're responsible for
    warm_cache: WarmCache,           // For non-owned content (separate dir)
    ipfs: Option<Arc<IpfsService>>,
    sharding: Option<Arc<ShardingService>>,
}
```

**store()** logic:
- If sharding disabled OR we're responsible:
  - Directory → IPFS pin (always)
  - File > 1 MiB → IPFS pin
  - File <= 1 MiB → local filesystem
- If we're NOT responsible:
  - Store in warm cache with TTL

**get()** — try in order: local storage → warm cache → IPFS

### 4.2 Warm cache
In `src/storage/tiered.rs`:

```rust
pub struct WarmCache {
    storage: LocalStorage,      // Separate base_dir (e.g., data/warm_cache/)
    max_bytes: u64,
    ttl: Duration,
}
```
- LRU eviction when over max_bytes
- TTL-based expiry for entries
- `evict_expired() -> u64` — called by GC

### 4.3 STORE handler integration
**File**: `src/handlers/store.rs`

In `process()`, after database writes:
- Get routing decision from sharding service
- If Owned: store via TieredStorage (IPFS or local based on size/type)
- If NotOwned: store in warm cache only

For IPFS items: call `ipfs.get_file_stats(hash)` to determine File vs Directory, update `files` table.

### 4.4 Web API — storage endpoint updates
**File**: `src/web/handlers.rs` (around line 1574, `get_storage` function)

When serving `GET /api/v0/storage/{hash}`:
- Try TieredStorage.get(hash) for local content
- If not found and sharding enabled:
  - Get responsible nodes from sharding service
  - Return response with `X-Aleph-Responsible-Nodes` header listing responsible node URLs
  - Optionally proxy-fetch from first responsible node, cache in warm cache, serve

### 4.5 AppState updates
**File**: `src/web/state.rs`

Add:
```rust
pub sharding: Option<Arc<ShardingService>>,
pub tiered_storage: Option<Arc<TieredStorage>>,
```

### 4.6 GC integration
**File**: `src/jobs/garbage_collector.rs`

Add steps:
- Evict expired warm cache entries
- If sharding enabled: check content responsibility — content we're no longer responsible for (ring change) moves to warm cache or gets evicted

### 4.7 Job manager wiring
**File**: `src/jobs/mod.rs` and `src/main.rs`

- Construct ShardingService and TieredStorage at startup
- Pass to JobManager, content fetch, web AppState
- Ring rebuild is triggered by peer discovery (not a separate job)

---

## Implementation Order

```
Phase 1 (foundation):        Schema + types + Database trait
    |
    +-- Phase 2 (handler):   STORE handler + cost + forget + GC
    |
    +-- Phase 3 (sharding):  ContentRing + peer integration + content fetch
         |
         Phase 4 (tiering):  TieredStorage + warm cache + API + wiring
```

Phases 2 and 3 can be developed in parallel (both depend only on Phase 1). Phase 4 depends on both.

---

## Files to Create/Modify

**New files:**
- `src/services/sharding.rs` — consistent hash ring + sharding service
- `src/storage/tiered.rs` — tiered storage + warm cache

**Modified files:**
- `src/db/migrations.rs` — new tables, altered file_pins, file_tags_v2
- `src/db/pg_database.rs` — implement new Database trait methods
- `src/types/message.rs` — add `ref_` to StoreContent
- `src/handlers/mod.rs` — PinType enum, expanded types, Database trait methods
- `src/handlers/store.rs` — full rewrite with ref/tags/costs
- `src/handlers/forget.rs` — grace period logic
- `src/services/cost.rs` — add `calculate_store_message_costs()`
- `src/services/peers.rs` — extract p2p peer_id, trigger ring rebuilds
- `src/services/content_fetch.rs` — sharding-aware peer selection
- `src/services/mod.rs` — add sharding module
- `src/storage/mod.rs` — add tiered module
- `src/schemas/mod.rs` — allow `ref` in STORE validation
- `src/config/mod.rs` — sharding config fields
- `src/jobs/garbage_collector.rs` — grace period + warm cache eviction
- `src/jobs/mod.rs` — wire up sharding/tiered storage
- `src/web/state.rs` — add sharding + tiered storage to AppState
- `src/web/handlers.rs` — storage endpoint routing hints
- `src/main.rs` — construct and pass new services

## Verification

1. **Unit tests**: Test `ContentRing` with known inputs — verify deterministic node assignment, verify ring rebalance on node add/remove only affects ~1/N keys
2. **Unit tests**: Test `make_file_tag`, ref validation, grace period pin lifecycle
3. **Cargo test**: `cargo test` — all existing tests must pass
4. **Cargo clippy**: `cargo clippy -- -D warnings`
5. **Integration test**: Submit STORE message with `ref` field via API, verify file_pins and file_tags_v2 populated correctly
6. **Integration test**: Submit FORGET for a STORE message, verify grace period pin created (not immediate deletion)
7. **Integration test**: With sharding enabled, verify `get_storage` returns responsible node hints for non-local content
8. **Build**: `cargo build --release` must succeed
