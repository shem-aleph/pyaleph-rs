# pyaleph-rs API Gap Analysis

**Comparison:** pyaleph (Python) vs pyaleph-rs (Rust)  
**Generated:** 2026-02-02  

---

## 1. MISSING QUERY PARAMETERS

### `/api/v0/messages.json` - Message Listing

| Parameter | Python | Rust | Status |
|-----------|--------|------|--------|
| `sortBy` | ✅ `time`, `tx-time` | ❌ Not implemented | **MISSING** |
| `sortOrder` | ✅ `-1` (desc), `1` (asc) | ⚠️ `order` param (different name) | **NAME MISMATCH** |
| `msgType` (singular) | ✅ Supported | ✅ Supported | OK |
| `msgTypes` (plural) | ✅ Supported | ✅ Supported | OK |
| `msgStatuses` | ✅ `PROCESSED`, `REMOVING`, etc. | ❌ Not implemented | **MISSING** |
| `addresses` | ✅ Supported | ✅ Supported | OK |
| `owners` | ✅ `content.address` filter | ❌ Not implemented | **MISSING** |
| `refs` | ✅ `content.ref` filter | ❌ Not implemented | **MISSING** |
| `contentHashes` | ✅ `content.item_hash` filter | ❌ Not implemented | **MISSING** |
| `contentKeys` | ✅ `content.keys` filter | ❌ Not implemented | **MISSING** |
| `contentTypes` | ✅ `content.type` filter | ❌ Not implemented | **MISSING** |
| `chains` | ✅ Chain filter | ❌ Not implemented | **MISSING** |
| `channels` | ✅ Supported | ✅ Supported | OK |
| `tags` | ✅ `content.content.tag` filter | ⚠️ Listed but not implemented | **MISSING** |
| `hashes` | ✅ Item hash filter | ⚠️ Listed but not implemented | **MISSING** |
| `startDate` | ✅ Supported | ✅ `start_date` | OK |
| `endDate` | ✅ Supported | ✅ `end_date` | OK |
| `startBlock` | ✅ Block number filter | ❌ Not implemented | **MISSING** |
| `endBlock` | ✅ Block number filter | ❌ Not implemented | **MISSING** |
| `pagination` | ✅ Supported | ✅ Supported | OK |
| `page` | ✅ Supported | ✅ Supported | OK |
| `limit` | ❌ Not in Python | ✅ Added as alias | OK (enhancement) |

### `/api/v0/posts.json` - Posts Listing

| Parameter | Python | Rust | Status |
|-----------|--------|------|--------|
| `sortBy` | ✅ `time`, `tx-time` | ❌ Not implemented | **MISSING** |
| `sortOrder` | ✅ `-1`, `1` | ❌ Not implemented | **MISSING** |
| `addresses` | ✅ Sender filter | ❌ Listed but not applied | **MISSING** |
| `hashes` | ✅ Item hash filter | ❌ Listed but not applied | **MISSING** |
| `refs` | ✅ `content.ref` filter | ❌ Listed but not applied | **MISSING** |
| `types` | ✅ `content.type` filter | ❌ Listed but not applied | **MISSING** |
| `tags` | ✅ Tag filter | ❌ Listed but not applied | **MISSING** |
| `channels` | ✅ Channel filter | ❌ Listed but not applied | **MISSING** |
| `startDate` | ✅ Start time filter | ❌ Not implemented | **MISSING** |
| `endDate` | ✅ End time filter | ❌ Not implemented | **MISSING** |

**Note:** Rust `get_posts` does a basic `SELECT * FROM posts ORDER BY time DESC LIMIT $1 OFFSET $2` without applying ANY filters!

### `/api/v0/aggregates/{address}.json` - Aggregates

| Parameter | Python | Rust | Status |
|-----------|--------|------|--------|
| `keys` | ✅ Key filter | ✅ Supported | OK |
| `limit` | ✅ Default 1000 | ❌ Not implemented | **MISSING** |
| `with_info` | ✅ Include metadata | ❌ Not implemented | **MISSING** |
| `value_only` | ✅ Return only value | ❌ Not implemented | **MISSING** |

### `/api/v0/aggregates` (List all aggregates)

| Parameter | Python | Rust | Status |
|-----------|--------|------|--------|
| `keys` | ✅ Key filter | ❌ Not implemented | **ENDPOINT MISSING** |
| `addresses` | ✅ Address filter | ❌ Not implemented | **ENDPOINT MISSING** |
| `sortBy` | ✅ `creation_time`, `last_modified` | ❌ Not implemented | **ENDPOINT MISSING** |
| `sortOrder` | ✅ `-1`, `1` | ❌ Not implemented | **ENDPOINT MISSING** |
| `pagination` | ✅ Supported | ❌ Not implemented | **ENDPOINT MISSING** |
| `page` | ✅ Supported | ❌ Not implemented | **ENDPOINT MISSING** |

### `/api/v0/messages/hashes` - Message Hashes

| Parameter | Python | Rust | Status |
|-----------|--------|------|--------|
| `status` | ✅ Message status filter | ❌ Not implemented | **ENDPOINT MISSING** |
| `startDate` | ✅ Start time | ❌ Not implemented | **ENDPOINT MISSING** |
| `endDate` | ✅ End time | ❌ Not implemented | **ENDPOINT MISSING** |
| `sortOrder` | ✅ Sort order | ❌ Not implemented | **ENDPOINT MISSING** |
| `hash_only` | ✅ Return only hashes | ❌ Not implemented | **ENDPOINT MISSING** |
| `pagination` | ✅ Supported | ❌ Not implemented | **ENDPOINT MISSING** |

---

## 2. MISSING ENDPOINTS

### Completely Missing Endpoints

| Endpoint | Python | Rust | Priority |
|----------|--------|------|----------|
| `/api/v0/aggregates` | ✅ List aggregates | ❌ Missing | **HIGH** |
| `/api/v0/aggregates.json` | ✅ List aggregates | ❌ Missing | **HIGH** |
| `/api/v0/messages/hashes` | ✅ List hashes | ❌ Missing | **MEDIUM** |
| `/api/v0/messages/page/{page}.json` | ✅ Paginated messages | ❌ Missing | **LOW** |
| `/api/ws0/messages` | ✅ WebSocket messages | ⚠️ `/ws` exists | **MEDIUM** |
| `/api/ws0/status` | ✅ Status WebSocket | ❌ Missing | **LOW** |
| `/api/v0/channels/list.json` | ✅ List channels | ❌ Missing | **LOW** |
| `/api/v0/info/public.json` | ✅ Public multiaddress | ❌ Missing | **LOW** |
| `/api/v1/posts.json` | ✅ V1 posts format | ❌ Missing | **LOW** |
| `/api/v1/posts/page/{page}.json` | ✅ V1 paginated posts | ❌ Missing | **LOW** |
| `/api/v0/posts/page/{page}.json` | ✅ Paginated posts | ❌ Missing | **LOW** |
| `/api/v0/addresses/stats.json` | ✅ Address stats | ❌ Missing | **MEDIUM** |
| `/api/v1/addresses/stats.json` | ✅ V1 address stats | ❌ Missing | **MEDIUM** |
| `/api/v0/addresses/{address}/files` | ✅ Account files | ❌ Missing | **MEDIUM** |
| `/api/v0/addresses/{address}/post_types` | ✅ Post types | ❌ Missing | **LOW** |
| `/api/v0/addresses/{address}/channels` | ✅ Account channels | ❌ Missing | **LOW** |
| `/api/v0/addresses/{address}/credit_history` | ✅ Credit history | ❌ Missing | **MEDIUM** |
| `/api/v0/messages/{item_hash}/consumed_credits` | ✅ Resource credits | ❌ Missing | **MEDIUM** |
| `/api/v0/balances` | ✅ Chain balances | ❌ Missing | **HIGH** |
| `/api/v0/credit_balances` | ✅ Credit balances | ❌ Missing | **HIGH** |
| `/api/v0/ipfs/add_file` | ✅ IPFS add | ❌ Missing | **MEDIUM** |
| `/api/v0/ipfs/add_json` | ✅ IPFS JSON add | ❌ Missing | **MEDIUM** |
| `/api/v0/storage/add_file` | ✅ Storage add | ⚠️ `/storage/upload` | **NAMING** |
| `/api/v0/storage/add_json` | ✅ Storage JSON add | ❌ Missing | **MEDIUM** |
| `/api/v0/storage/by-message-hash/{hash}` | ✅ File by message | ❌ Missing | **MEDIUM** |
| `/api/v0/storage/by-ref/{ref}` | ✅ File by ref | ❌ Missing | **MEDIUM** |
| `/api/v0/storage/by-ref/{address}/{ref}` | ✅ File by ref | ❌ Missing | **MEDIUM** |
| `/api/v0/storage/count/{hash}` | ✅ File pin count | ❌ Missing | **LOW** |
| `/api/v0/price/{item_hash}` | ✅ Message price | ❌ Missing | **HIGH** |
| `/api/v0/price/estimate` | ✅ Price estimate | ⚠️ `/cost/estimate` | **NAMING** |
| `/api/v0/price/recalculate` | ✅ Recalculate costs | ❌ Missing | **LOW** |
| `/api/v0/programs/on/message` | ✅ Programs on message | ❌ Missing | **LOW** |
| `/api/v0/ipfs/pubsub/pub` | ✅ P2P publish | ❌ Missing | **LOW** |
| `/api/v0/p2p/pubsub/pub` | ✅ P2P publish | ❌ Missing | **LOW** |
| `/api/v0/core/{node_id}/metrics` | ✅ CCN metrics | ❌ Missing | **LOW** |
| `/api/v0/compute/{node_id}/metrics` | ✅ CRN metrics | ❌ Missing | **LOW** |
| `/version` | ✅ Version | ❌ Missing | **LOW** |
| `/api/v0/version` | ✅ Version | ❌ Missing | **LOW** |
| `/metrics` | ✅ Prometheus | ⚠️ `/_internal/metrics` | **PATH** |
| `/metrics.json` | ✅ JSON metrics | ❌ Missing | **LOW** |

---

## 3. BEHAVIOR DIFFERENCES

### Sort Order Parameter Names
- **Python:** Uses `sortOrder` (camelCase) with values `-1` (desc) and `1` (asc)
- **Rust:** Uses `order` with same values
- **Fix:** Add `sortOrder` as alias for `order` in Rust

### Response Field Differences

#### Messages Response
```json
// Python includes:
{
  "content": { /* parsed JSON content */ },
  "size": 1234,
  // ... more fields
}

// Rust missing:
// - size field
// - content parsing (returns item_content string)
```

#### Aggregates Response
```json
// Python with with_info=true includes:
{
  "address": "...",
  "data": { ... },
  "info": {
    "key": {
      "created": "...",
      "last_updated": "...",
      "original_item_hash": "...",
      "last_update_item_hash": "..."
    }
  }
}

// Rust missing:
// - info field entirely
```

### Pagination Behavior

#### Messages Pagination
- **Python:** `pagination=0` removes the limit entirely
- **Rust:** Uses `min(1000)` cap regardless

### WebSocket Differences
- **Python:** `/api/ws0/messages` with full message filtering
- **Rust:** `/ws` basic WebSocket, different query params

### Confirmations Fetching
- **Python:** Always includes confirmations from chain_txs
- **Rust:** `// TODO: Fetch actual confirmations from chain_txs table` - confirmations array is empty!

---

## 4. RECOMMENDED FIXES (Prioritized)

### P0 - Critical (Breaking API Compatibility)

1. **Add `sortBy` and fix `sortOrder` naming**
   - File: `src/web/handlers.rs` `MessageQuery`
   - Add `sortBy` param with `time` and `tx-time` support
   - Add `sortOrder` as alias for `order`

2. **Implement message confirmations**
   - Remove TODO, actually query chain_txs table
   - Critical for clients checking on-chain status

3. **Fix posts endpoint filters**
   - The `get_posts` handler ignores ALL query params
   - Need to apply: addresses, types, refs, tags, channels, startDate, endDate

4. **Add `/api/v0/balances` endpoint**
   - Required for SDK balance lookups

5. **Add `/api/v0/credit_balances` endpoint**
   - Required for pay-as-you-go credit checking

### P1 - High (Important for SDK Compatibility)

6. **Add aggregates list endpoint**
   - `/api/v0/aggregates` and `/api/v0/aggregates.json`
   - With full filtering: keys, addresses, sortBy, sortOrder, pagination

7. **Add aggregates metadata support**
   - `with_info` parameter
   - `limit` parameter
   - `value_only` parameter

8. **Add content filtering to messages**
   - `refs`, `contentHashes`, `contentTypes`, `contentKeys`
   - `owners` (content.address)
   - `chains`

9. **Add message status filtering**
   - `msgStatuses` parameter
   - Support: PROCESSED, PENDING, REMOVING, REMOVED, etc.

10. **Add `/api/v0/price/{item_hash}` endpoint**
    - Get price for existing message

### P2 - Medium (Nice to Have)

11. **Add storage endpoints**
    - `/api/v0/storage/add_json`
    - `/api/v0/storage/by-message-hash/{hash}`
    - `/api/v0/storage/by-ref/{ref}`

12. **Add account stats endpoints**
    - `/api/v0/addresses/stats.json`
    - `/api/v0/addresses/{address}/files`
    - `/api/v0/addresses/{address}/credit_history`

13. **Add message hashes endpoint**
    - `/api/v0/messages/hashes` with all filters

14. **Add block number filtering**
    - `startBlock`, `endBlock` parameters

15. **WebSocket compatibility**
    - Match `/api/ws0/messages` path
    - Support Python's `history` parameter

### P3 - Low (Can Defer)

16. Add channels list endpoint
17. Add version endpoints
18. Add paginated URL endpoints (`/page/{page}.json`)
19. Add V1 posts endpoints
20. Move metrics to `/metrics` path

---

## 5. QUICK WINS (Easy Fixes)

### 1. Add sortOrder alias (5 min)
```rust
#[derive(Debug, Deserialize)]
pub struct MessageQuery {
    // ... existing fields ...
    #[serde(alias = "sortOrder")]
    pub order: Option<i8>,
}
```

### 2. Fix confirmations fetch (10 min)
```rust
// In list_messages, replace the TODO:
let confirmations: Vec<ConfirmationResponse> = sqlx::query_as::<_, (String, String, i64)>(
    "SELECT c.chain, c.hash, c.height FROM message_confirmations mc 
     JOIN chain_txs c ON mc.tx_hash = c.hash 
     WHERE mc.item_hash = $1"
)
.bind(&msg.item_hash)
.fetch_all(state.db())
.await
.unwrap_or_default()
.into_iter()
.map(|(chain, hash, height)| ConfirmationResponse { chain, hash, height: height as u64 })
.collect();
```

### 3. Add sortBy support (15 min)
```rust
#[derive(Debug, Deserialize)]
pub enum SortBy {
    #[serde(rename = "time")]
    Time,
    #[serde(rename = "tx-time")]
    TxTime,
}

// In MessageQuery:
#[serde(alias = "sortBy")]
pub sort_by: Option<SortBy>,

// In query building:
let order_column = match params.sort_by {
    Some(SortBy::TxTime) => "first_confirmation_time",
    _ => "time",
};
```

---

## Summary

| Category | Count |
|----------|-------|
| Missing Query Parameters | ~25 |
| Missing Endpoints | ~30 |
| Behavior Differences | ~8 |
| Critical Fixes (P0) | 5 |
| High Priority (P1) | 5 |
| Medium Priority (P2) | 5 |
| Low Priority (P3) | 5 |

The Rust implementation has a solid foundation but is missing many filtering capabilities that the Python SDK expects. Priority should be:

1. **Sort parameters** - Most common client issue
2. **Confirmations** - Critical for on-chain verification  
3. **Posts filters** - Currently completely broken
4. **Balance endpoints** - Required for payment checks
5. **Content filtering** - Important for real-world queries
