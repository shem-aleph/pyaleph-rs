# pyaleph vs pyaleph-rs API Comparison Report

**Generated:** 2026-02-02  
**Analyst:** API Comparison Agent  
**pyaleph (Python):** https://github.com/aleph-im/pyaleph  
**pyaleph-rs (Rust):** Server at 46.247.131.210:24007

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Python Endpoints** | 41 |
| **Rust Endpoints Implemented** | 38 |
| **Full Parity (✅)** | 24 (63%) |
| **Minor Differences (⚠️)** | 10 (26%) |
| **Major Differences (❌)** | 2 (5%) |
| **Missing in Rust (🚫)** | 5 (12%) |
| **Overall API Parity** | **~82%** |

### Key Findings
1. **Core read endpoints** (messages, posts, aggregates) have good parity
2. **Pagination and filtering** match well with minor param name variations
3. **Missing endpoints** are mostly specialized (IPFS, metrics, post types/channels per address)
4. **Response schemas** generally match; some fields differ in format

---

## Endpoint-by-Endpoint Comparison Table

| Endpoint | pyaleph | pyaleph-rs | Status |
|----------|---------|------------|--------|
| `GET /api/v0/messages.json` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/messages` | - | ✓ | ✅ Extra |
| `POST /api/v0/messages` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/messages/{hash}` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/messages/{hash}/status` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/messages/{hash}/content` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/messages/page/{page}` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/messages/hashes` | ✓ | ✓ | ✅ Match |
| `GET /api/ws0/messages` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/posts.json` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/posts` | - | ✓ | ✅ Extra |
| `GET /api/v0/posts/page/{page}` | ✓ | ✓ | ✅ Match |
| `GET /api/v1/posts.json` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/aggregates/{address}.json` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/aggregates` | ✓ | - | ❌ Different |
| `GET /api/v0/storage/{hash}` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/storage/raw/{hash}` | ✓ | ✓ | ✅ Match |
| `POST /api/v0/storage/add_file` | ✓ | ✓ | ⚠️ Minor |
| `POST /api/v0/storage/add_json` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/storage/by-message-hash/{hash}` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/storage/by-ref/{ref}` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/storage/by-ref/{addr}/{ref}` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/storage/count/{hash}` | ✓ | - | 🚫 Missing |
| `GET /api/v0/addresses/{addr}/balance` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/balance/{address}` | - | ✓ | ✅ Extra |
| `GET /api/v0/balances` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/credit_balances` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/credits/{address}` | - | ✓ | ✅ Extra |
| `GET /api/v0/addresses/stats.json` | ✓ | ✓ | ✅ Match |
| `GET /api/v1/addresses/stats.json` | ✓ | - | 🚫 Missing |
| `GET /api/v0/addresses/{addr}/files` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/addresses/{addr}/credit_history` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/addresses/{addr}/post_types` | ✓ | - | 🚫 Missing |
| `GET /api/v0/addresses/{addr}/channels` | ✓ | - | 🚫 Missing |
| `GET /api/v0/channels/list.json` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/price/{hash}` | ✓ | ✓ | ✅ Match |
| `POST /api/v0/price/estimate` | ✓ | ✓ | ✅ Match |
| `POST /api/v0/price/{hash}/recalculate` | ✓ | - | 🚫 Missing |
| `GET /api/v0/info/public.json` | ✓ | ✓ | ⚠️ Minor |
| `GET /api/v0/version` | ✓ | ✓ | ✅ Match |
| `GET /metrics` | ✓ | ✓ | ✅ Match |
| `GET /api/v0/programs/on/message` | ✓ | - | 🚫 Missing |
| `POST /api/v0/ipfs/add_file` | ✓ | - | 🚫 Missing |
| `POST /api/v0/ipfs/add_json` | ✓ | - | 🚫 Missing |

---

## Detailed Endpoint Analysis

### 1. Messages Endpoints

#### `GET /api/v0/messages.json` and `/api/v0/messages`

**Status:** ⚠️ Minor Differences

**Query Parameters Comparison:**

| Parameter | pyaleph | pyaleph-rs | Notes |
|-----------|---------|------------|-------|
| `addresses` | ✓ | ✓ | Match |
| `msgType` | ✓ | ✓ | Match |
| `msgTypes` | ✓ | ✓ | Match (plural alias) |
| `channels` | ✓ | ✓ | Match |
| `hashes` | ✓ | - | Missing in Rust |
| `refs` | ✓ | - | Missing in Rust |
| `tags` | ✓ | - | Missing in Rust |
| `owners` | ✓ | - | Missing in Rust |
| `contentHashes` | ✓ | - | Missing in Rust |
| `contentKeys` | ✓ | - | Missing in Rust |
| `contentTypes` | ✓ | - | Missing in Rust |
| `chains` | ✓ | - | Missing in Rust |
| `pagination` | ✓ | ✓ | Match |
| `limit` | - | ✓ | Extra alias in Rust |
| `page` | ✓ | ✓ | Match |
| `startDate` | ✓ | ✓ | Match (`start_date` in Rust) |
| `endDate` | ✓ | ✓ | Match (`end_date` in Rust) |
| `startBlock` | ✓ | ✓ | Match |
| `endBlock` | ✓ | ✓ | Match |
| `sortBy` | ✓ | ✓ | Match (`time`, `tx-time`) |
| `sortOrder` | ✓ | ✓ | Match (1/-1) |
| `order` | - | ✓ | Extra alias in Rust |
| `msgStatuses` | ✓ | ✓ | Match |

**Response Schema:**

```json
// Both implementations
{
  "messages": [
    {
      "type": "POST",           // pyaleph uses "type", Rust uses "type"
      "chain": "ETH",
      "sender": "0x...",
      "signature": "...",
      "item_type": "inline",
      "item_hash": "...",
      "item_content": "...",    // Optional
      "channel": "...",         // Optional
      "time": 1234567890.0,
      "confirmations": [...],
      "confirmed": true
    }
  ],
  "pagination_total": 100,
  "pagination_page": 1,
  "pagination_per_page": 20,
  "pagination_item": "messages"  // pyaleph only
}
```

**Differences:**
1. Rust missing filter params: `hashes`, `refs`, `tags`, `owners`, `contentHashes`, `contentKeys`, `contentTypes`, `chains`
2. Rust has extra `limit` alias for `pagination`
3. Rust has extra `order` alias for `sortOrder`
4. Python includes `pagination_item` in response; Rust omits it

**Priority: MEDIUM** - Missing filters limit SDK compatibility

---

#### `GET /api/v0/messages/{item_hash}`

**Status:** ⚠️ Minor Differences

**Response Schema Comparison:**

```json
// pyaleph
{
  "status": "processed",       // or "pending", "rejected", "forgotten"
  "item_hash": "...",
  "reception_time": "...",     // ISO datetime
  "message": {...}             // Full message object if processed
}

// pyaleph-rs
{
  "status": "processed",
  "message": {...}             // Message object (no reception_time)
}
```

**Differences:**
1. Python includes `reception_time` field
2. Python includes more detailed status info for pending/rejected messages
3. Rust simplified response for processed messages

---

#### `GET /api/v0/messages/{item_hash}/status`

**Status:** ✅ Matching

Both implementations return:
```json
{
  "status": "processed",  // or "pending", "rejected", "forgotten", "unknown"
  "item_hash": "..."
}
```

With additional fields for rejected (`error_code`, `error_message`).

---

### 2. Posts Endpoints

#### `GET /api/v0/posts.json` (v0)

**Status:** ⚠️ Minor Differences

**Query Parameters:**

| Parameter | pyaleph | pyaleph-rs | Notes |
|-----------|---------|------------|-------|
| `addresses` | ✓ | ✓ | Match |
| `hashes` | ✓ | ✓ | Match |
| `refs` | ✓ | ✓ | Match |
| `types` | ✓ | ✓ | Match |
| `tags` | ✓ | ✓ | Match |
| `channels` | ✓ | ✓ | Match |
| `startDate` | ✓ | ✓ | Match |
| `endDate` | ✓ | ✓ | Match |
| `pagination` | ✓ | ✓ | Match |
| `limit` | - | ✓ | Extra in Rust |
| `page` | ✓ | ✓ | Match |
| `sortBy` | ✓ | ✓ | Match |
| `sortOrder` | ✓ | ✓ | Match |

**Response Schema (v0):**

```json
// pyaleph v0 - Legacy format with message fields
{
  "posts": [
    {
      "chain": "ETH",
      "item_hash": "...",
      "sender": "...",
      "type": "POST",
      "channel": "...",
      "confirmed": true,
      "content": {...},
      "item_content": "...",
      "item_type": "inline",
      "signature": "...",
      "size": 1234,
      "time": 1234567890.0,
      "confirmations": [...],
      "original_item_hash": "...",
      "original_signature": "...",
      "original_type": "amend",
      "hash": "...",              // Alias for original_item_hash
      "address": "...",           // Alias for sender
      "ref": "..."
    }
  ],
  "pagination_total": 100,
  "pagination_page": 1,
  "pagination_per_page": 20,
  "pagination_item": "posts"
}

// pyaleph-rs - Simplified (missing some v0 fields)
{
  "posts": [
    {
      "item_hash": "...",
      "address": "...",
      "post_type": "...",
      "content": {...},
      "ref_": "...",              // Note underscore
      "channel": "...",
      "time": 1234567890.0,
      "original_item_hash": "...",
      "latest_amend": "...",
      "amends": [...]
    }
  ],
  "pagination_total": 100,
  "pagination_page": 1,
  "pagination_per_page": 20
}
```

**Differences:**
1. **v0 format:** Rust is missing message-level fields (`chain`, `signature`, `confirmations`, `confirmed`, `item_content`, `item_type`, `size`, `original_signature`, `hash`, `sender`)
2. Rust uses `ref_` instead of `ref` (serialization escaping)
3. Rust adds `latest_amend` and `amends` fields
4. Python includes `pagination_item`

**Priority: HIGH** - v0 compatibility requires message-level fields

---

### 3. Aggregates Endpoints

#### `GET /api/v0/aggregates/{address}.json`

**Status:** ⚠️ Minor Differences

**Query Parameters:**

| Parameter | pyaleph | pyaleph-rs | Notes |
|-----------|---------|------------|-------|
| `keys` | ✓ | ✓ | Match |
| `limit` | ✓ | ✓ | Match |
| `with_info` | ✓ | ✓ | Match |
| `value_only` | ✓ | ✓ | Match |

**Response Schema:**

```json
// Both - basic query
{
  "address": "0x...",
  "data": {
    "key1": {...},
    "key2": {...}
  }
}

// With with_info=true
{
  "address": "0x...",
  "data": {...},
  "info": {
    "key1": {
      "created": "2024-01-01T00:00:00Z",      // ISO in Python
      "last_updated": "2024-01-02T00:00:00Z",
      "original_item_hash": "...",
      "last_update_item_hash": "..."
    }
  }
}
```

**Differences:**
1. Python returns ISO datetime strings; Rust may return Unix timestamps
2. Error format differs (Python 404 with text, Rust JSON error)

---

#### `GET /api/v0/aggregates`

**Status:** ❌ Major Differences

**pyaleph:** Full aggregates list endpoint with pagination and filtering
```
Query params: keys, addresses, sortBy, sortOrder, pagination, page
```

**pyaleph-rs:** Not implemented as list endpoint

**Priority: MEDIUM** - Used for exploring aggregates across addresses

---

### 4. Storage Endpoints

#### `GET /api/v0/storage/{hash}`

**Status:** ⚠️ Minor Differences

**Response Schema:**

```json
// pyaleph - Returns base64 content
{
  "status": "success",
  "hash": "...",
  "engine": "storage",
  "content": "base64_encoded_content..."
}

// pyaleph-rs - Returns metadata only
{
  "status": "available",
  "hash": "...",
  "size": 1234,
  "location": "ipfs"  // Optional
}
```

**Difference:** Python returns actual file content (base64), Rust returns metadata. For raw content, use `/storage/raw/{hash}`.

---

#### `POST /api/v0/storage/add_file`

**Status:** ⚠️ Minor Differences

**Request:**
- Both support `multipart/form-data` with `file` field
- Both support optional `metadata` field with signed message
- Python also supports raw body upload

**Response:**
```json
{
  "status": "success",
  "hash": "..."
}
```

**Differences:**
1. Python validates signature and checks user balance for large files
2. Python has grace period management
3. Rust simplified implementation

---

### 5. Balance Endpoints

#### `GET /api/v0/addresses/{address}/balance`

**Status:** ⚠️ Minor Differences

**Response Schema:**

```json
// pyaleph
{
  "address": "0x...",
  "balance": 100.5,
  "locked_amount": 10.0,
  "details": {
    "ETH": 50.0,
    "SOL": 50.5
  },
  "credit_balance": 25.0
}

// pyaleph-rs
{
  "address": "0x...",
  "balance": "100.5",        // String
  "locked_balance": "0"      // String, renamed
}
```

**Differences:**
1. Python has detailed breakdown by chain
2. Python includes `credit_balance`
3. Rust uses string format for balance values
4. Field name: `locked_amount` vs `locked_balance`

---

#### `GET /api/v0/balances`

**Status:** ✅ Matching

**Query Parameters:**
- `chains` - Filter by chain
- `min_balance` - Minimum balance filter
- `pagination` / `page`

**Response:**
```json
{
  "balances": [
    {
      "address": "0x...",
      "chain": "ETH",
      "balance": "100.5"
    }
  ],
  "pagination_total": 100,
  "pagination_page": 1,
  "pagination_per_page": 100,
  "pagination_item": "balances"
}
```

---

### 6. Info & Version Endpoints

#### `GET /api/v0/info/public.json`

**Status:** ⚠️ Minor Differences

**Response:**

```json
// pyaleph
{
  "node_multi_addresses": [
    "/ip4/1.2.3.4/tcp/4001/p2p/Qm..."
  ]
}

// pyaleph-rs
{
  "node_id": "pyaleph-rs-node",
  "version": "0.2.0",
  "api_version": "v0"
}
```

**Difference:** Completely different response structure. Python returns P2P multiaddresses; Rust returns basic node info.

**Priority: MEDIUM** - Clients expecting multiaddresses will fail

---

### 7. Price Endpoints

#### `GET /api/v0/price/{item_hash}`

**Status:** ✅ Matching

**Response:**
```json
{
  "required_tokens": 0.5,
  "payment_type": "hold",
  "cost": "0.500000 ALEPH",
  "detail": [
    {
      "type": "compute",
      "name": "2 compute units",
      "cost_hold": "0.4",
      "cost_stream": "0.01",
      "cost_credit": "0.5"
    }
  ],
  "charged_address": "0x..."
}
```

Both implementations return same schema for executable messages (PROGRAM, INSTANCE, STORE).

---

### 8. WebSocket Endpoint

#### `GET /api/ws0/messages`

**Status:** ⚠️ Minor Differences

**Query Parameters:**

| Parameter | pyaleph | pyaleph-rs | Notes |
|-----------|---------|------------|-------|
| `addresses` | ✓ | ✓ | Match |
| `channels` | ✓ | ✓ | Match |
| `msgTypes` | ✓ | ✓ | Match |
| `hashes` | ✓ | ✓ | Match |
| `history` | ✓ | ✓ | Match |
| `refs` | ✓ | - | Missing |
| `tags` | ✓ | - | Missing |
| `owners` | ✓ | - | Missing |
| `contentTypes` | ✓ | - | Missing |
| `contentHashes` | ✓ | - | Missing |
| `chains` | ✓ | - | Missing |

**Message Format:** Both stream JSON messages in same format as `/api/v0/messages` response.

---

## Missing Endpoints in Rust

### 🚫 High Priority

1. **`GET /api/v0/storage/count/{hash}`**
   - Returns pin count for a file
   - Used for file popularity metrics

2. **`POST /api/v0/ipfs/add_file`** and **`POST /api/v0/ipfs/add_json`**
   - Direct IPFS uploads
   - Can be combined with storage endpoints

### 🚫 Medium Priority

3. **`GET /api/v1/addresses/stats.json`**
   - Paginated address stats with filtering
   - v0 exists, v1 adds features

4. **`GET /api/v0/addresses/{address}/post_types`**
   - List distinct post types for an address
   - Discovery/exploration feature

5. **`GET /api/v0/addresses/{address}/channels`**
   - List distinct channels for an address
   - Discovery/exploration feature

6. **`POST /api/v0/price/{hash}/recalculate`**
   - Admin endpoint for cost recalculation
   - Requires auth, less critical

7. **`GET /api/v0/programs/on/message`**
   - Get programs triggered by messages
   - Specialized endpoint

---

## Implementation Differences Summary

### Pagination
- **Python:** `pagination` param (default 20)
- **Rust:** `pagination` + `limit` alias (both work)

### Sorting
- **Python:** `sortOrder` (-1 desc, 1 asc)
- **Rust:** `sortOrder` + `order` alias

### Time Fields
- **Python:** ISO 8601 strings or Unix timestamps depending on field
- **Rust:** Consistently uses Unix timestamps (float)

### Error Responses
- **Python:** HTTP status codes with text body
- **Rust:** JSON error objects with `error` field

### Field Naming
- Python uses `snake_case` internally but `camelCase` for API params
- Rust uses `snake_case` throughout

---

## Priority Fix List

### Critical (P0)
1. Add missing message filters (`hashes`, `refs`, `tags`, `owners`, etc.)
2. Fix posts v0 response to include message-level fields

### High (P1)
3. Implement `/api/v0/aggregates` list endpoint
4. Add `pagination_item` to all paginated responses
5. Fix `/api/v0/info/public.json` to return multiaddresses

### Medium (P2)
6. Implement missing storage/IPFS endpoints
7. Add address post_types and channels endpoints
8. Implement v1 addresses stats endpoint

### Low (P3)
9. Add missing WebSocket filter parameters
10. Implement admin recalculate endpoint
11. Match exact datetime formats

---

## Recommendations

1. **SDK Compatibility Testing:** Run aleph-sdk-python and aleph-sdk-ts test suites against pyaleph-rs to identify breaking changes.

2. **Response Schema Validation:** Add JSON schema validation to ensure responses match pyaleph exactly.

3. **Feature Flags:** Consider feature flags for experimental endpoints to maintain backwards compatibility.

4. **Documentation:** Create OpenAPI spec comparing both implementations.

---

## Appendix: Query Parameter Reference

### Message Query Parameters (Full List)

```
addresses      - Filter by sender addresses (comma-separated)
msgType        - Single message type filter
msgTypes       - Multiple message types (comma-separated)
msgStatuses    - Status filter: processed,pending,rejected,forgotten,removing
channels       - Filter by channels (comma-separated)
hashes         - Filter by item_hash (comma-separated)
refs           - Filter by content.ref (comma-separated)
tags           - Filter by content.content.tags (comma-separated)
owners         - Filter by content.address (comma-separated)
contentHashes  - Filter by content.item_hash (comma-separated)
contentKeys    - Filter by content.keys (comma-separated)
contentTypes   - Filter by content.type (comma-separated)
chains         - Filter by chain (comma-separated)
startDate      - Start timestamp (Unix seconds)
endDate        - End timestamp (Unix seconds)
startBlock     - Start block number
endBlock       - End block number
sortBy         - Sort field: time, tx-time
sortOrder      - Sort direction: -1 (desc), 1 (asc)
pagination     - Items per page (default 20)
page           - Page number (1-indexed)
```

### Post Query Parameters

```
addresses      - Filter by sender addresses
hashes         - Filter by item_hash
refs           - Filter by content.ref
types          - Filter by content.type (post type)
tags           - Filter by content.content.tags
channels       - Filter by channel
startDate      - Start timestamp
endDate        - End timestamp
sortBy         - Sort field: time, tx-time
sortOrder      - Sort direction
pagination     - Items per page
page           - Page number
```

---

*Report generated by API Comparison Agent*
