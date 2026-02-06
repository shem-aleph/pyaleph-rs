# API Compatibility Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all API incompatibilities between the Rust pyaleph-rs and the Python pyaleph reference, prioritized by impact.

**Architecture:** Each task is a targeted fix to `src/web/handlers.rs` or `src/web/routes.rs`. Tasks are ordered P0 (breaking) -> P2 (response format) -> P3 (missing params). No new files needed.

**Tech Stack:** Rust, Axum, sqlx, serde, serde_json, base64, chrono

---

### Task 1: Fix `startDate`/`endDate` aliases on MessageQuery

**Files:**
- Modify: `src/web/handlers.rs:162-164`

**Step 1: Write the fix**

In the `MessageQuery` struct, add `alias` attributes so both camelCase and snake_case work:

```rust
    /// Start time filter (Unix timestamp)
    #[serde(alias = "startDate")]
    pub start_date: Option<f64>,
    /// End time filter (Unix timestamp)
    #[serde(alias = "endDate")]
    pub end_date: Option<f64>,
```

**Step 2: Run tests**

Run: `cargo test message_ -v`
Expected: PASS

**Step 3: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: add startDate/endDate aliases to MessageQuery for pyaleph compat"
```

---

### Task 2: Handle `pagination=0` as "no limit" for messages and posts

**Files:**
- Modify: `src/web/handlers.rs:194` (messages)
- Modify: `src/web/handlers.rs:1020` (posts)

**Step 1: Write the fix**

In `list_messages` (line 194), change:
```rust
    let per_page = params.limit.or(params.pagination).unwrap_or(20).min(1000); // Max 1000 per page
```
to:
```rust
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
```

In `get_posts` (line 1020), same change:
```rust
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: treat pagination=0 as unlimited (matching pyaleph behavior)"
```

---

### Task 3: Fix `/storage/raw` route path and add `/storage/add_file` alias

**Files:**
- Modify: `src/web/routes.rs:76`

**Step 1: Write the fix**

Change line 76 from:
```rust
        .route("/storage/:hash/raw", get(handlers::get_storage_raw))
```
to:
```rust
        .route("/storage/raw/:hash", get(handlers::get_storage_raw))
```

Also add the `/storage/add_file` route alias after line 67:
```rust
        .route("/storage/add_file", post(handlers::upload_file))
```

And add `/ipfs/add_json` alias after the storage routes:
```rust
        .route("/ipfs/add_json", post(handlers::add_json_storage))
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/routes.rs
git commit -m "fix: correct /storage/raw route path, add /storage/add_file alias"
```

---

### Task 4: Fix `get_storage` to return base64 content (matching pyaleph)

**Files:**
- Modify: `src/web/handlers.rs:1443-1475`

**Step 1: Add base64 import**

At the top of handlers.rs, ensure `use base64::Engine;` is present (check if base64 is already a dependency, if not add to Cargo.toml).

**Step 2: Rewrite `get_storage`**

Replace the `get_storage` function:

```rust
/// Get storage content - matches pyaleph format (returns base64-encoded content)
pub async fn get_storage(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Try to get content from local storage first
    if let Some(ref storage) = state.storage {
        if let Ok(bytes) = storage.get(&hash).await {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "engine": "storage",
                "content": encoded,
            })));
        }
    }

    // Try IPFS
    match state.ipfs.get(&hash).await {
        Ok(bytes) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
            (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "engine": "ipfs",
                "content": encoded,
            })))
        }
        Err(_) => {
            (StatusCode::NOT_FOUND, Json(json!({
                "status": "not_found",
                "hash": hash,
            })))
        }
    }
}
```

NOTE: If the storage service doesn't have a `get()` method that returns bytes, check what methods are available (`exists`, `get_size`, etc.) and adapt accordingly. The key change is: fetch content -> base64 encode -> return with `"status": "success"` and `"engine"` field.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return base64-encoded content from /storage/{hash} (matching pyaleph)"
```

---

### Task 5: Fix `get_message` to include `reception_time`

**Files:**
- Modify: `src/web/handlers.rs:499-570`

**Step 1: Write the fix**

In the `get_message` function, add `reception_time` to all response branches.

For the processed case (~line 538), look up `created_at` from the messages table (this is the reception time). Change:
```rust
            (StatusCode::OK, Json(json!({
                "status": "processed",
                "message": response
            })))
```
to:
```rust
            (StatusCode::OK, Json(json!({
                "status": "processed",
                "item_hash": hash,
                "reception_time": msg.created_at.map(|t| t.timestamp_millis() as f64 / 1000.0).unwrap_or(msg.time),
                "message": response
            })))
```

For the pending case (~line 554), fetch reception_time from pending_messages:
```rust
            // Fetch pending message details
            let pending_msg = sqlx::query_as::<_, (f64,)>(
                "SELECT reception_time FROM pending_messages WHERE item_hash = $1 LIMIT 1"
            )
            .bind(&hash)
            .fetch_optional(state.db())
            .await
            .ok()
            .flatten();

            if let Some((reception_time,)) = pending_msg {
                (StatusCode::OK, Json(json!({
                    "status": "pending",
                    "item_hash": hash,
                    "reception_time": reception_time,
                })))
            } else {
```

NOTE: Check the actual columns available on `MessageDb` — it may have `created_at` as a `chrono::DateTime` or as `f64`. Adapt the reception_time extraction accordingly.

**Step 2: Fix `get_message_status` to return 404 instead of `"unknown"`**

In `get_message_status` (~line 651), change:
```rust
    (StatusCode::NOT_FOUND, Json(json!({
        "status": "unknown",
        "item_hash": hash,
    })))
```
to just return 404 with no body, or with Python's format:
```rust
    (StatusCode::NOT_FOUND, Json(json!({
        "error": "Message not found"
    })))
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: add reception_time to message responses, return 404 for unknown"
```

---

### Task 6: Fix `get_message_content` to return inner `content.content` for POST only

**Files:**
- Modify: `src/web/handlers.rs:1634-1693`

**Step 1: Write the fix**

In `get_message_content`, after fetching the message, add a type check and extract the inner content:

```rust
        Ok(Some(msg)) => {
            // Only POST messages supported (matching pyaleph)
            if msg.message_type.to_uppercase() != "POST" {
                return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
                    "error": "Only POST messages have content"
                })));
            }

            match msg.item_content {
                Some(content) => {
                    match serde_json::from_str::<serde_json::Value>(&content) {
                        Ok(json) => {
                            // Return the inner "content" field, not the full item_content
                            if let Some(inner) = json.get("content") {
                                (StatusCode::OK, Json(inner.clone()))
                            } else {
                                (StatusCode::OK, Json(json))
                            }
                        }
                        Err(_) => (StatusCode::OK, Json(json!({ "content": content }))),
                    }
                }
                // ... rest stays the same for IPFS fetch case,
                // but also extract "content" field from fetched JSON
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return content.content for POST messages only (matching pyaleph)"
```

---

### Task 7: Implement v1 posts response format

**Files:**
- Modify: `src/web/handlers.rs:3534-3540`

**Step 1: Define v1 post response struct**

Add near the PostResponseV0 struct:

```rust
/// Post response format for v1 API - simplified format
/// Reference: aleph/web/controllers/posts.py:merged_post_to_dict
#[derive(Debug, Clone, Serialize)]
pub struct PostResponseV1 {
    pub item_hash: String,
    pub content: serde_json::Value,
    pub original_item_hash: String,
    pub original_type: Option<String>,
    pub address: String,
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub channel: Option<String>,
    /// ISO 8601 timestamp
    pub created: String,
    /// ISO 8601 timestamp
    pub last_updated: String,
}
```

**Step 2: Rewrite `get_posts_v1`**

Replace the stub implementation with a real v1 handler that reuses the same query logic from `get_posts` but maps to `PostResponseV1`:

```rust
pub async fn get_posts_v1(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PostsQuery>,
) -> impl IntoResponse {
    // Same query logic as get_posts... (extract into shared function or duplicate)
    // But map to PostResponseV1 instead of PostResponseV0
    // Use chrono to format time as ISO 8601
    // created = original post time, last_updated = coalesced amend time
}
```

The key difference: v1 returns `created`/`last_updated` as ISO strings, no chain/signature/confirmations fields.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: implement proper v1 posts response format"
```

---

### Task 8: Fix `get_aggregates` to return 404 when not found

**Files:**
- Modify: `src/web/handlers.rs:836-840,902-906`

**Step 1: Write the fix**

Change both empty-aggregates branches. Around line 836:
```rust
        if aggregates.is_empty() {
            return Json(json!({
                "error": "No aggregate found for this address"
            }));
        }
```

The problem is the return type. The function returns `Json<Value>` currently, but we need to return a status code too. Change the function signature to return `impl IntoResponse` and wrap in tuple:

Actually, looking at the code more carefully — the function already returns `Json<Value>`. To return a 404, we need to change the return type to `(StatusCode, Json<Value>)`. This requires changing ALL return points.

A simpler fix that preserves the current architecture: use `axum::response::Response` builder. But the simplest approach for now:

Change the function signature to return `impl IntoResponse` (it already does). Then change the empty cases to:

```rust
        if aggregates.is_empty() {
            // axum will serialize this but we need to set 404 status
            // Since we can't easily change return type mid-function,
            // return a custom response
        }
```

Actually the cleanest fix: change the function to return `Result<Json<Value>, (StatusCode, Json<Value>)>`:

This needs careful refactoring. A practical approach is to change the function return type to `(StatusCode, Json<Value>)` and update all return points. Do this for BOTH the with_info and without_info branches.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return 404 from /aggregates/{address} when no aggregates found"
```

---

### Task 9: Fix `get_balance` response format

**Files:**
- Modify: `src/web/handlers.rs:1259-1293`

**Step 1: Write the fix**

Rewrite `get_balance` to match Python's `GetAccountBalanceResponse`:

```rust
pub async fn get_balance(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "balance": 0.0,
            "locked_amount": 0.0,
            "details": {},
            "credit_balance": 0
        }));
    }

    // Get per-chain balances
    let balances: Vec<(String, rust_decimal::Decimal)> = sqlx::query_as(
        "SELECT chain, balance FROM balances WHERE address = $1"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();

    // Sum total and build details map
    let mut total = rust_decimal::Decimal::ZERO;
    let mut details = serde_json::Map::new();
    for (chain, balance) in &balances {
        total += balance;
        details.insert(chain.clone(), json!(balance.to_f64().unwrap_or(0.0)));
    }

    // Get locked amount (total cost for address)
    let locked: (Option<rust_decimal::Decimal>,) = sqlx::query_as(
        "SELECT SUM(total_cost) FROM costs WHERE address = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((None,));

    // Get credit balance
    let credits: (Option<rust_decimal::Decimal>,) = sqlx::query_as(
        "SELECT balance FROM credit_balances WHERE address = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((None,));

    Json(json!({
        "address": address,
        "balance": total.to_f64().unwrap_or(0.0),
        "locked_amount": locked.0.map(|d| d.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
        "details": details,
        "credit_balance": credits.0.map(|d| d.to_string().parse::<i64>().unwrap_or(0)).unwrap_or(0)
    }))
}
```

NOTE: Check if `costs` table exists with a `total_cost` column. If the table structure is different, adapt the locked_amount query accordingly. Also `use rust_decimal::prelude::ToPrimitive;` may be needed for `.to_f64()`.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: match pyaleph balance response format (details, locked_amount, credit_balance)"
```

---

### Task 10: Fix `credit_balances` to return credits as integer

**Files:**
- Modify: `src/web/handlers.rs:1340-1343,1428-1430`

**Step 1: Write the fix**

Change the `CreditBalanceItem` struct:
```rust
pub struct CreditBalanceItem {
    pub address: String,
    pub credits: i64,  // Integer, not string
}
```

Change the mapping (~line 1428):
```rust
        .map(|(address, balance)| CreditBalanceItem {
            address,
            credits: balance.to_string().parse::<i64>().unwrap_or(0),
        })
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return credits as integer in /credit_balances (matching pyaleph)"
```

---

### Task 11: Fix `get_version` to use CARGO_PKG_VERSION

**Files:**
- Modify: `src/web/handlers.rs:3486-3492`

**Step 1: Write the fix**

```rust
pub async fn get_version() -> impl IntoResponse {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION")
    }))
}
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: use CARGO_PKG_VERSION for /version endpoint"
```

---

### Task 12: Fix `get_storage_count` to return JSON integer

**Files:**
- Modify: `src/web/handlers.rs:3706-3744`

**Step 1: Write the fix**

Replace the `get_storage_count` function to query actual count and return JSON:

```rust
pub async fn get_storage_count(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    let mut count = 0i64;

    if state.has_db() {
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM file_pins WHERE item_hash = $1"
        )
        .bind(&hash)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
        count = result.0;
    }

    Json(json!(count))
}
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return JSON integer from /storage/count (matching pyaleph)"
```

---

### Task 13: Fix address files default pagination (20 -> 100)

**Files:**
- Modify: `src/web/handlers.rs:2894`

**Step 1: Write the fix**

Change:
```rust
    let per_page = params.pagination.unwrap_or(20).min(1000);
```
to:
```rust
    let per_page = params.pagination.unwrap_or(100).min(1000);
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: default pagination for /addresses/{addr}/files to 100 (matching pyaleph)"
```

---

### Task 14: Fix credit history default pagination and add filters

**Files:**
- Modify: `src/web/handlers.rs:3018,3036-3050`

**Step 1: Fix default pagination**

Change line 3018 from:
```rust
    let per_page = params.pagination.unwrap_or(20).min(1000);
```
to:
```rust
    let raw_pagination = params.pagination.unwrap_or(0);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
```

**Step 2: Add filter params to the SQL query**

Replace the static SQL query with a dynamic one using QueryBuilder:

```rust
    let mut builder = crate::db::QueryBuilder::new(
        "SELECT amount, price, bonus_amount, tx_hash, token, chain, provider, origin, origin_ref, \
         payment_method, credit_ref, credit_index, expiration_date, message_timestamp \
         FROM credit_history WHERE address = $1"
    );
    // The address is already bound as $1, so we need to add filters manually
    // Actually, use a QueryBuilder starting after the address bind
```

NOTE: The QueryBuilder may need careful handling since `address` is already bound. An alternative approach is to build the WHERE clause manually:

```rust
    let mut conditions = vec!["address = $1".to_string()];
    let mut param_idx = 2;
    // ... add conditions for each filter param
```

Add conditions for: `tx_hash`, `token`, `chain`, `provider`, `origin`, `origin_ref`, `payment_method` — each as `= $N` equality checks.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: credit history pagination default to 0 (all), apply filter params"
```

---

### Task 15: Add `addressContains` and `sortBy` to v1 address stats

**Files:**
- Modify: `src/web/handlers.rs:3811-3820,3855-3864`

**Step 1: Update the query struct**

```rust
pub struct AddressStatsV1Query {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    #[serde(rename = "addressContains")]
    pub address_contains: Option<String>,
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
}
```

**Step 2: Update the handler**

Add addressContains filter to the SQL query:
```rust
    // Build WHERE clause
    let mut where_clause = String::new();
    if let Some(ref contains) = params.address_contains {
        // Limit to 66 chars (matching pyaleph validation)
        let search = &contains[..contains.len().min(66)];
        where_clause = format!(" WHERE sender ILIKE '%{}%'", search.replace('\'', "''"));
    }
```

Add sortBy support:
```rust
    let sort_column = match params.sort_by.as_deref() {
        Some("post") => "post",
        Some("aggregate") => "aggregate",
        Some("store") => "store",
        Some("program") => "program",
        Some("instance") => "instance",
        Some("forget") => "forget",
        _ => "total_messages",  // "total" or default
    };
```

NOTE: This requires restructuring the SQL to use a subquery with per-type counts that can be sorted. Consider using a CTE or subquery approach.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: add addressContains and sortBy params to v1 address stats"
```

---

### Task 16: Add `contentKeys` filter to messages query

**Files:**
- Modify: `src/web/handlers.rs` (after contentHashes filter block ~line 300, and in count_builder ~line 407)

**Step 1: Write the fix**

After the contentHashes filter block (around line 300), add:

```rust
    // Parse contentKeys filter (content.keys field - matches pyaleph)
    if let Some(ref content_keys) = params.content_keys {
        let key_list = crate::db::parse_csv_param(content_keys);
        if !key_list.is_empty() {
            // content keys are stored as an object with key names
            // Python checks: MessageDb.content["keys"].has_any(ARRAY[keys])
            for key in &key_list {
                builder.and_jsonb_has_key("item_content", "keys", key);
            }
        }
    }
```

Add the same to the count_builder block (~line 407).

NOTE: Check if `and_jsonb_has_key` exists on QueryBuilder. If not, use `and_raw` with a `item_content->'keys' ? $N` pattern, or implement the method.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: implement contentKeys filter for messages endpoint"
```

---

### Task 17: Add `sortBy` to aggregates list

**Files:**
- Modify: `src/web/handlers.rs:2371-2386,2453-2458`

**Step 1: Update query struct**

Add to `ListAggregatesQuery`:
```rust
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
```

**Step 2: Update sort logic**

Change the ORDER BY clause (around line 2455):
```rust
    let sort_column = match params.sort_by.as_deref() {
        Some("last_modified") | None => "COALESCE(ae.time, a.time)",
        _ => "COALESCE(ae.time, a.time)",  // Only last_modified supported in pyaleph
    };
```

Also change max pagination from 1000 to 500 (line 2412):
```rust
    let per_page = params.limit.or(params.pagination).unwrap_or(20).min(500);
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: add sortBy param to aggregates list, cap pagination at 500"
```

---

### Task 18: Fix `POST /messages` response format

**Files:**
- Modify: `src/web/handlers.rs:733-737`

**Step 1: Write the fix**

Change the success response to match Python's `BroadcastStatus`:

```rust
            (StatusCode::ACCEPTED, Json(json!({
                "publication_status": {
                    "status": "success",
                    "failed": []
                },
                "message_status": "pending"
            })))
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: match pyaleph BroadcastStatus response format for POST /messages"
```

---

### Task 19: Add `"tx-time"` sort to posts

**Files:**
- Modify: `src/web/handlers.rs:1133-1141`

**Step 1: Write the fix**

Add `"tx-time"` to the sort column match:

```rust
    let sort_column = match params.sort_by.as_deref() {
        Some("time") | None => "COALESCE(a.time, p.time)".to_string(),
        Some("tx-time") => "om.created_at".to_string(),
        Some("address") => "p.address".to_string(),
        Some("post_type") => "p.post_type".to_string(),
        Some("channel") => "p.channel".to_string(),
        _ => "COALESCE(a.time, p.time)".to_string(),
    };
```

NOTE: This assumes `om.created_at` (from the messages table JOIN) holds the chain confirmation time. Verify this is correct by checking the messages table schema.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: add tx-time sort support for posts endpoint"
```

---

### Task 20: Final build, clippy, and format check

**Files:** None (validation only)

**Step 1: Run full build**

Run: `cargo build --release`
Expected: PASS

**Step 2: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings

**Step 3: Run format check**

Run: `cargo fmt --check`
Expected: No formatting issues

**Step 4: Run tests**

Run: `cargo test`
Expected: All tests pass

**Step 5: Commit any formatting fixes**

```bash
cargo fmt
git add -A
git commit -m "chore: format and clippy cleanup after API compat fixes"
```

---

### Task 21: Implement `GET /api/v0/messages/hashes` endpoint

**Files:**
- Modify: `src/web/routes.rs` (add route)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add route**

In `api_v0()`, add BEFORE the `/messages/:hash` route (so it matches first):
```rust
        .route("/messages/hashes", get(handlers::get_message_hashes))
```

**Step 2: Add query struct and handler**

```rust
#[derive(Debug, Deserialize)]
pub struct MessageHashesQuery {
    pub status: Option<String>,
    pub page: Option<u32>,
    pub pagination: Option<u32>,
    #[serde(alias = "startDate")]
    pub start_date: Option<f64>,
    #[serde(alias = "endDate")]
    pub end_date: Option<f64>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    pub hash_only: Option<bool>,
}

pub async fn get_message_hashes(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageHashesQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    let hash_only = params.hash_only.unwrap_or(true);
    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);

    // Query message_statuses table (or messages table) for hashes
    // If hash_only, SELECT item_hash; else SELECT item_hash, status, reception_time
    let order = if ascending { "ASC" } else { "DESC" };

    // Build query based on status filter
    let mut builder = crate::db::QueryBuilder::new(
        "SELECT item_hash FROM messages WHERE 1=1"
    );
    if let Some(start) = params.start_date {
        if start > 0.0 { builder.and_gte("time", start); }
    }
    if let Some(end) = params.end_date {
        if end > 0.0 { builder.and_lte("time", end); }
    }
    builder.order_by("time", ascending);
    builder.limit(per_page as i64);
    builder.offset(offset);

    // Count query
    let mut count_builder = crate::db::QueryBuilder::new(
        "SELECT COUNT(*) FROM messages WHERE 1=1"
    );
    if let Some(start) = params.start_date {
        if start > 0.0 { count_builder.and_gte("time", start); }
    }
    if let Some(end) = params.end_date {
        if end > 0.0 { count_builder.and_lte("time", end); }
    }

    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db()).await.unwrap_or((0,));

    let (query, args) = builder.build();
    let hashes: Vec<(String,)> = sqlx::query_as_with(&query, args)
        .fetch_all(state.db()).await.unwrap_or_default();

    let hash_list: Vec<String> = hashes.into_iter().map(|(h,)| h).collect();

    Json(json!({
        "hashes": hash_list,
        "pagination_per_page": per_page,
        "pagination_page": page,
        "pagination_total": total.0,
        "pagination_item": "hashes",
    }))
}
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement GET /messages/hashes endpoint"
```

---

### Task 22: Implement `POST /api/v0/ipfs/pubsub/pub` and `POST /api/v0/p2p/pubsub/pub`

**Files:**
- Modify: `src/web/routes.rs` (add routes)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add routes**

In `api_v0()`:
```rust
        .route("/ipfs/pubsub/pub", post(handlers::pub_json))
        .route("/p2p/pubsub/pub", post(handlers::pub_json))
```

**Step 2: Add handler**

Python behavior: validates topic matches configured message topic, validates data is serialized JSON, publishes to IPFS and P2P topics. Returns `PublicationStatus` with `status` and `failed` list.

```rust
#[derive(Debug, Deserialize)]
pub struct PubSubRequest {
    pub topic: String,
    pub data: String,
}

pub async fn pub_json(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PubSubRequest>,
) -> impl IntoResponse {
    // Validate topic matches configured message topic
    let expected_topic = &state.config.aleph.queue_topic;
    if payload.topic != *expected_topic {
        return (StatusCode::FORBIDDEN, Json(json!({
            "error": format!("Unauthorized P2P topic: {}. Use {}.", payload.topic, expected_topic)
        })));
    }

    // Validate data is valid JSON
    if serde_json::from_str::<serde_json::Value>(&payload.data).is_err() {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "error": "'data': must be deserializable as JSON."
        })));
    }

    let mut failed: Vec<String> = vec![];

    // Publish to RabbitMQ / P2P
    if let Some(ref rabbitmq) = state.rabbitmq {
        let service = rabbitmq.read().await;
        if let Err(e) = service.publish_raw(&payload.topic, payload.data.as_bytes()).await {
            tracing::warn!("Failed to publish to P2P: {}", e);
            failed.push("p2p".to_string());
        }
    } else {
        failed.push("p2p".to_string());
    }

    let status = if failed.is_empty() { "success" } else { "error" };
    let http_status = if failed.is_empty() { StatusCode::OK } else { StatusCode::INTERNAL_SERVER_ERROR };

    (http_status, Json(json!({
        "status": status,
        "failed": failed
    })))
}
```

NOTE: Adapt the actual publish method to whatever RabbitMQ/P2P service methods are available. The key is to match the `PublicationStatus` response shape.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement /ipfs/pubsub/pub and /p2p/pubsub/pub endpoints"
```

---

### Task 23: Implement `POST /api/v0/ipfs/add_file`

**Files:**
- Modify: `src/web/routes.rs` (add route)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add route**

In `api_v0()`:
```rust
        .route("/ipfs/add_file", post(handlers::ipfs_add_file))
```

**Step 2: Add handler**

Python behavior: accepts multipart form with `file` field, uploads to IPFS, returns `{status, hash, name, size}`.

```rust
pub async fn ipfs_add_file(
    State(state): State<Arc<AppState>>,
    mut multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    let mut file_content: Option<Vec<u8>> = None;
    let mut filename = "file".to_string();

    while let Ok(Some(field)) = multipart.next_field().await {
        if field.name() == Some("file") {
            filename = field.file_name().unwrap_or("file").to_string();
            match field.bytes().await {
                Ok(bytes) => file_content = Some(bytes.to_vec()),
                Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({
                    "error": format!("Failed to read file: {}", e)
                }))),
            }
        }
    }

    let content = match file_content {
        Some(c) => c,
        None => return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "error": "Missing 'file' in multipart form"
        }))),
    };

    match state.ipfs.add(content.clone()).await {
        Ok(hash) => {
            let size = content.len();
            // Store file pin
            if state.has_db() {
                let _ = sqlx::query(
                    "INSERT INTO file_pins (item_hash, owner, size, created_at) \
                     VALUES ($1, 'anonymous', $2, NOW()) ON CONFLICT DO NOTHING"
                )
                .bind(&hash)
                .bind(size as i64)
                .execute(state.db())
                .await;
            }

            (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "name": filename,
                "size": size,
            })))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "error": format!("IPFS upload failed: {}", e)
        }))),
    }
}
```

NOTE: May need `axum::extract::Multipart` import. Check if axum multipart feature is enabled in Cargo.toml (`features = ["multipart"]`).

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs Cargo.toml
git commit -m "feat: implement POST /ipfs/add_file endpoint with multipart support"
```

---

### Task 24: Implement `GET /api/v0/programs/on/message`

**Files:**
- Modify: `src/web/routes.rs` (add route BEFORE `/programs/:address`)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add route**

In `api_v0()`, add BEFORE the `/programs/:address` route:
```rust
        .route("/programs/on/message", get(handlers::get_programs_on_message))
```

**Step 2: Add handler**

Python behavior: queries messages table for PROGRAM type messages where `item_content->'on'->'message'` exists, returns list of `{item_hash, content: {on: {message: [...]}}}`.

```rust
#[derive(Debug, Deserialize)]
pub struct ProgramsOnMessageQuery {
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
}

pub async fn get_programs_on_message(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ProgramsOnMessageQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!([]));
    }

    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);
    let order = if ascending { "ASC" } else { "DESC" };

    let query = format!(
        "SELECT item_hash, item_content FROM messages \
         WHERE message_type = 'PROGRAM' \
         AND item_content::jsonb->'on'->'message' IS NOT NULL \
         ORDER BY time {}",
        order
    );

    let rows: Vec<(String, Option<String>)> = sqlx::query_as(&query)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();

    let programs: Vec<serde_json::Value> = rows.into_iter().filter_map(|(item_hash, item_content)| {
        let content: serde_json::Value = item_content
            .and_then(|c| serde_json::from_str(&c).ok())?;
        let message_subs = content.get("on")?.get("message")?;
        Some(json!({
            "item_hash": item_hash,
            "content": {
                "on": {
                    "message": message_subs
                }
            }
        }))
    }).collect();

    Json(json!(programs))
}
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement GET /programs/on/message endpoint"
```

---

### Task 25: Implement `GET /api/v0/messages/{hash}/consumed_credits`

**Files:**
- Modify: `src/web/routes.rs` (add route)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add route**

In `api_v0()`, after the `/messages/:hash/content` route:
```rust
        .route("/messages/:hash/consumed_credits", get(handlers::get_consumed_credits))
```

**Step 2: Add handler**

Python behavior: queries `credit_usage` or similar table for total credits consumed by a resource (item_hash). Returns `{item_hash, consumed_credits}`.

```rust
pub async fn get_consumed_credits(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Query consumed credits for this resource
    // Python uses get_resource_consumed_credits() which sums from credit_usage table
    let consumed: (Option<i64>,) = sqlx::query_as(
        "SELECT SUM(amount) FROM credit_usage WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_one(state.db())
    .await
    .unwrap_or((None,));

    (StatusCode::OK, Json(json!({
        "item_hash": hash,
        "consumed_credits": consumed.0.unwrap_or(0)
    })))
}
```

NOTE: Verify the actual table name — it might be `credit_usage`, `credit_flows`, or similar. Check the Python accessor `get_resource_consumed_credits` for the exact query.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement GET /messages/{hash}/consumed_credits endpoint"
```

---

### Task 26: Implement `GET /metrics.json`

**Files:**
- Modify: `src/web/routes.rs` (add route at root level in legacy_routes or create_router)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add route**

In `legacy_routes()` or at the top level of `create_router`:
```rust
        .route("/metrics.json", get(handlers::metrics_json))
```

**Step 2: Add handler**

Python returns the same Metrics dataclass serialized as JSON. Match the `pyaleph_*` field names.

```rust
pub async fn metrics_json(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({}));
    }

    let db = state.db();

    let messages_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(db).await.unwrap_or(0);
    let pending_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_messages")
        .fetch_one(db).await.unwrap_or(0);
    let files_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'file_pins'"
    ).fetch_one(db).await.unwrap_or(0);

    Json(json!({
        "pyaleph_build_info": {
            "version": env!("CARGO_PKG_VERSION"),
        },
        "pyaleph_status_peers_total": 0,
        "pyaleph_status_sync_messages_total": messages_count,
        "pyaleph_status_sync_permanent_files_total": files_count,
        "pyaleph_status_sync_pending_messages_total": pending_count,
        "pyaleph_status_sync_pending_txs_total": 0,
    }))
}
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement GET /metrics.json endpoint"
```

---

### Task 27: Implement `GET /api/v0/core/{node_id}/metrics` and `GET /api/v0/compute/{node_id}/metrics`

**Files:**
- Modify: `src/web/routes.rs` (add routes)
- Modify: `src/web/handlers.rs` (add handlers)

**Step 1: Add routes**

In `api_v0()`:
```rust
        .route("/core/:node_id/metrics", get(handlers::get_ccn_metrics))
        .route("/compute/:node_id/metrics", get(handlers::get_crn_metrics))
```

**Step 2: Add handlers**

Python behavior: queries a `ccn_metrics` / `crn_metrics` table (aggregates from aleph.db.accessors.metrics), returns `{metrics: {...}}`.

```rust
#[derive(Debug, Deserialize)]
pub struct NodeMetricsQuery {
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub sort: Option<String>,
}

pub async fn get_ccn_metrics(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(params): Query<NodeMetricsQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Database not available"})));
    }

    // Query ccn_metrics aggregate (stored as AGGREGATE messages with key matching node_id)
    // The metrics are stored in the aggregates table with key = node_id
    let result = sqlx::query_as::<_, (serde_json::Value,)>(
        "SELECT content FROM aggregates WHERE key = $1 LIMIT 1"
    )
    .bind(&node_id)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    match result {
        Some((content,)) => {
            (StatusCode::OK, Json(json!({ "metrics": content })))
        }
        None => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "Node metrics not found"})))
        }
    }
}

pub async fn get_crn_metrics(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(params): Query<NodeMetricsQuery>,
) -> impl IntoResponse {
    // Same logic as ccn_metrics but queries CRN-specific data
    get_ccn_metrics(State(state), Path(node_id), Query(params)).await
}
```

NOTE: The actual query depends on how metrics are stored. Python uses `query_metric_ccn` / `query_metric_crn` from `aleph.db.accessors.metrics`. Check if there's a dedicated metrics table or if metrics come from aggregates. Adapt accordingly.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement CCN/CRN node metrics endpoints"
```

---

### Task 28: Fix `POST /api/v0/price/estimate` to accept message dict input

**Files:**
- Modify: `src/web/handlers.rs:1580-1630`
- Modify: `src/web/routes.rs` (change route from `/cost/estimate` to also serve at `/price/estimate`)

**Step 1: Add route alias**

In `api_v0()`, add:
```rust
        .route("/price/estimate", post(handlers::price_estimate))
```

**Step 2: Add handler accepting Python's message dict format**

Python expects `{"message": {...}}` where the message is a full Aleph message dict. It then extracts the content to determine resource requirements.

```rust
pub async fn price_estimate(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Accept Python format: {"message": {...}}
    let message_dict = match payload.get("message") {
        Some(msg) => msg,
        None => return (StatusCode::BAD_REQUEST, Json(json!({
            "error": "Missing 'message' field"
        }))),
    };

    // Extract content from message
    let item_content = message_dict.get("item_content")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .or_else(|| message_dict.get("content").cloned());

    let content = match item_content {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({
            "error": "Cannot parse message content"
        }))),
    };

    // Extract resource requirements from content
    let resources = content.get("resources").unwrap_or(&json!({}));
    let memory = resources.get("memory").and_then(|v| v.as_u64()).unwrap_or(2048) as u32;
    let vcpus = resources.get("vcpus").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let storage_mib = content.get("volumes").and_then(|v| {
        v.as_array().map(|vols| {
            vols.iter().filter_map(|vol| vol.get("size_mib").and_then(|s| s.as_u64())).sum::<u64>()
        })
    }).unwrap_or(0);

    let msg_type = message_dict.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let product_type = match msg_type.to_uppercase().as_str() {
        "INSTANCE" => ProductPriceType::Instance,
        "PROGRAM" => ProductPriceType::Program,
        _ => ProductPriceType::Program,
    };

    let internet = content.get("internet").and_then(|v| v.as_bool()).unwrap_or(true);

    // Calculate cost (same as existing estimate_cost logic)
    let cost = state.cost.calculate_instance_cost(
        memory, vcpus, storage_mib, 24 * 30, product_type, internet
    ).await;

    match cost {
        Some(result) => {
            let required_tokens = result.holding.to_f64().unwrap_or(0.0);
            (StatusCode::OK, Json(json!({
                "required_tokens": required_tokens,
                "payment_type": "hold",
                "cost": format!("{:.6}", required_tokens),
                "detail": [],
                "charged_address": content.get("address").and_then(|v| v.as_str()).unwrap_or("")
            })))
        }
        None => (StatusCode::BAD_REQUEST, Json(json!({
            "error": "Unable to calculate cost"
        }))),
    }
}
```

NOTE: The Python implementation uses a full cost calculation engine with detailed cost breakdown. This is a best-effort implementation. Need `use rust_decimal::prelude::ToPrimitive;` for `.to_f64()`.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: implement POST /price/estimate accepting message dict format"
```

---

### Task 29: Fix `GET /api/v0/price/{hash}` to read from costs table

**Files:**
- Modify: `src/web/handlers.rs` (rewrite `get_message_price`)

**Step 1: Rewrite handler**

Python reads from the pre-calculated `costs` table and returns `EstimatedCostsResponse`. Fix the Rust handler to do the same:

```rust
pub async fn get_message_price(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "Database not available"})));
    }

    // Check message status first
    let msg = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    if msg.is_none() {
        // Check if pending
        let pending = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pending_messages WHERE item_hash = $1)"
        )
        .bind(&hash)
        .fetch_one(state.db())
        .await
        .unwrap_or(false);

        if pending {
            return (StatusCode::from_u16(102).unwrap_or(StatusCode::PROCESSING), Json(json!({
                "error": "Message still pending"
            })));
        }
        return (StatusCode::NOT_FOUND, Json(json!({"error": "Message not found"})));
    }

    let msg = msg.unwrap();

    // Verify it's an executable message type
    let msg_type = msg.message_type.to_uppercase();
    if !["PROGRAM", "INSTANCE", "STORE"].contains(&msg_type.as_str()) {
        return (StatusCode::BAD_REQUEST, Json(json!({
            "error": format!("Message is not an executable or store message: {}", hash)
        })));
    }

    // Read costs from costs table
    let costs: Vec<(String, String, rust_decimal::Decimal, rust_decimal::Decimal, rust_decimal::Decimal)> = sqlx::query_as(
        "SELECT cost_type, name, cost_hold, cost_stream, cost_credit FROM costs WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();

    let total_hold: rust_decimal::Decimal = costs.iter().map(|c| c.2).sum();
    let required_tokens = total_hold.to_f64().unwrap_or(0.0);

    let detail: Vec<serde_json::Value> = costs.iter().map(|c| json!({
        "type": c.0,
        "name": c.1,
        "cost_hold": format!("{:.6}", c.2),
        "cost_stream": format!("{:.6}", c.3),
        "cost_credit": format!("{:.6}", c.4),
    })).collect();

    // Get charged address from message content
    let charged_address = msg.item_content.as_ref()
        .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok())
        .and_then(|v| v.get("address").and_then(|a| a.as_str().map(String::from)))
        .unwrap_or_else(|| msg.sender.clone());

    (StatusCode::OK, Json(json!({
        "required_tokens": required_tokens,
        "payment_type": "hold",
        "cost": format!("{:.6}", required_tokens),
        "detail": detail,
        "charged_address": charged_address,
    })))
}
```

NOTE: The `costs` table columns may differ — check migrations for exact schema. The Python `format_cost_str()` formats without " ALEPH" suffix. Need `use rust_decimal::prelude::ToPrimitive;`.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: read message price from costs table (matching pyaleph)"
```

---

### Task 30: Implement `POST /api/v0/price/recalculate`

**Files:**
- Modify: `src/web/routes.rs` (add routes)
- Modify: `src/web/handlers.rs` (add handler)

**Step 1: Add routes**

In `api_v0()`:
```rust
        .route("/price/recalculate", post(handlers::recalculate_costs))
        .route("/price/:hash/recalculate", post(handlers::recalculate_costs))
```

**Step 2: Add handler**

Python requires `X-Auth-Token` authentication. Recalculates costs for all (or one) executable messages.

```rust
pub async fn recalculate_costs(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<Option<String>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // Check auth token
    let auth_token = headers.get("x-auth-token").and_then(|v| v.to_str().ok());
    if auth_token.is_none() {
        return (StatusCode::UNAUTHORIZED, Json(json!({
            "error": "X-Auth-Token header required"
        })));
    }

    // For now, return a stub response (full implementation requires pricing timeline)
    (StatusCode::OK, Json(json!({
        "message": "Cost recalculation not yet fully implemented",
        "recalculated_count": 0,
        "total_messages": 0,
    })))
}
```

NOTE: The path parameter needs special handling — when called at `/price/recalculate` there's no hash, when called at `/price/:hash/recalculate` there is. Use separate route handlers or `Option<Path<String>>`. Full implementation requires the pricing timeline system.

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/handlers.rs
git commit -m "feat: add /price/recalculate endpoint stub with auth check"
```

---

### Task 31: Fix `/balances` endpoint to return balance as float

**Files:**
- Modify: `src/web/handlers.rs:2349-2357`

**Step 1: Write the fix**

Change `BalanceItem` and its mapping:

```rust
#[derive(Debug, Serialize)]
pub struct BalanceItem {
    pub address: String,
    pub chain: String,
    pub balance: f64,  // Float, not string
}
```

Change the mapping (~line 2352):
```rust
    let balance_items: Vec<BalanceItem> = balances
        .into_iter()
        .map(|b| BalanceItem {
            address: b.address,
            chain: b.chain,
            balance: b.balance.to_f64().unwrap_or(0.0),
        })
        .collect();
```

NOTE: Need `use rust_decimal::prelude::ToPrimitive;`.

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "fix: return balance as float in /balances endpoint (matching pyaleph)"
```

---

### Task 32: Implement WebSocket status streaming at `/api/ws0/status`

**Files:**
- Modify: `src/web/routes.rs:144`
- Modify: `src/web/websocket.rs` (or `src/web/handlers.rs`)

**Step 1: Fix route**

Change line 144 from:
```rust
        .route("/status", get(handlers::health_check))
```
to:
```rust
        .route("/status", get(websocket::status_ws_handler))
```

**Step 2: Implement status WebSocket handler**

In `src/web/websocket.rs`, add:

```rust
pub async fn status_ws_handler(
    State(state): State<Arc<AppState>>,
    ws: axum::extract::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| status_ws(socket, state))
}

async fn status_ws(mut socket: axum::extract::ws::WebSocket, state: Arc<AppState>) {
    use axum::extract::ws::Message;

    let mut previous_json: Option<String> = None;

    loop {
        // Build status metrics
        let status = if state.has_db() {
            let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(state.db()).await.unwrap_or(0);
            let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_messages")
                .fetch_one(state.db()).await.unwrap_or(0);
            let files: i64 = sqlx::query_scalar(
                "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'file_pins'"
            ).fetch_one(state.db()).await.unwrap_or(0);

            serde_json::json!({
                "pyaleph_status_sync_messages_total": messages,
                "pyaleph_status_sync_pending_messages_total": pending,
                "pyaleph_status_sync_permanent_files_total": files,
            })
        } else {
            serde_json::json!({})
        };

        let json_str = status.to_string();

        // Only send if changed
        if previous_json.as_ref() != Some(&json_str) {
            if socket.send(Message::Text(json_str.clone())).await.is_err() {
                break;
            }
            previous_json = Some(json_str);
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
```

**Step 3: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 4: Commit**

```bash
git add src/web/routes.rs src/web/websocket.rs
git commit -m "feat: implement /api/ws0/status as streaming WebSocket"
```

---

### Task 33: Implement `POST /messages` sync mode

**Files:**
- Modify: `src/web/handlers.rs:665-747`

**Step 1: Write the fix**

When `sync=true`, after inserting into pending_messages, poll for the message to appear in `messages` or `rejected_messages` tables (with timeout).

After the successful pending insert (~line 723), add sync logic:

```rust
            if payload.sync {
                // Wait up to 30 seconds for message to be processed
                let start = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(30);

                loop {
                    if start.elapsed() > timeout {
                        return (StatusCode::ACCEPTED, Json(json!({
                            "publication_status": {"status": "success", "failed": []},
                            "message_status": "pending"
                        })));
                    }

                    // Check if processed
                    if state.has_db() {
                        let processed = sqlx::query_scalar::<_, bool>(
                            "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)"
                        )
                        .bind(&msg.item_hash)
                        .fetch_one(state.db())
                        .await
                        .unwrap_or(false);

                        if processed {
                            return (StatusCode::OK, Json(json!({
                                "publication_status": {"status": "success", "failed": []},
                                "message_status": "processed"
                            })));
                        }

                        // Check if rejected
                        let rejected = sqlx::query_as::<_, (i32, Option<String>)>(
                            "SELECT error_code, error_message FROM rejected_messages WHERE item_hash = $1"
                        )
                        .bind(&msg.item_hash)
                        .fetch_optional(state.db())
                        .await
                        .ok()
                        .flatten();

                        if let Some((code, message)) = rejected {
                            return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
                                "publication_status": {"status": "success", "failed": []},
                                "message_status": "rejected",
                                "error_code": code,
                                "details": message,
                            })));
                        }
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
```

**Step 2: Build and test**

Run: `cargo build && cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add src/web/handlers.rs
git commit -m "feat: implement sync mode for POST /messages (polls for processing result)"
```

---

### Task 34: Final comprehensive build, lint, and test

**Files:** None (validation only)

**Step 1: Format**

Run: `cargo fmt`

**Step 2: Clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings (fix any issues)

**Step 3: Build release**

Run: `cargo build --release`
Expected: PASS

**Step 4: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 5: Commit**

```bash
cargo fmt
git add -A
git commit -m "chore: final format and cleanup after full API compatibility fixes"
```
