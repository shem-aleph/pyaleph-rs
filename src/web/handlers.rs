//! API request handlers
//!
//! These handlers match the pyaleph API format for client compatibility.
//! Reference: aleph/web/controllers/

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::collections::HashMap;

use super::state::AppState;
use crate::types::*;

/// Health check response - matches pyaleph format
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub database: DatabaseStatus,
    pub ipfs: ServiceStatus,
    pub p2p: ServiceStatus,
}

#[derive(Serialize)]
pub struct DatabaseStatus {
    pub connected: bool,
    pub message_count: Option<i64>,
}

#[derive(Serialize)]
pub struct ServiceStatus {
    pub connected: bool,
}

/// Health check endpoint
pub async fn health_check(
    State(state): State<Arc<AppState>>,
) -> Json<HealthResponse> {
    let db_connected = state.has_db();
    let message_count = if db_connected {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
            .fetch_one(state.db())
            .await
            .ok()
    } else {
        None
    };
    
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        database: DatabaseStatus {
            connected: db_connected,
            message_count,
        },
        ipfs: ServiceStatus {
            connected: state.ipfs.is_connected().await,
        },
        p2p: ServiceStatus {
            connected: state.p2p_connected,
        },
    })
}

/// Message response format matching pyaleph
/// Reference: aleph/web/controllers/messages.py:52-66
#[derive(Debug, Clone, Serialize)]
pub struct MessageResponse {
    #[serde(rename = "type")]
    pub message_type: String,
    pub chain: String,
    pub sender: String,
    pub signature: String,
    pub item_type: String,
    pub item_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    /// Parsed item_content as JSON (pyaleph compat)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Timestamp as Unix timestamp (seconds)
    pub time: f64,
    /// Size of item_content in bytes
    pub size: i64,
    /// Chain confirmations
    pub confirmations: Vec<ConfirmationResponse>,
    /// Whether message has any confirmations
    pub confirmed: bool,
}

/// Chain confirmation format
#[derive(Debug, Clone, Serialize)]
pub struct ConfirmationResponse {
    pub chain: String,
    pub hash: String,
    pub height: u64,
}

impl MessageResponse {
    fn from_db(msg: &crate::db::models::MessageDb, confirmations: Vec<ConfirmationResponse>) -> Self {
        let confirmed = !confirmations.is_empty();
        let size = msg.item_content.as_ref().map(|c| c.len() as i64).unwrap_or(0);
        let content = msg.item_content.as_ref()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok());
        Self {
            message_type: msg.message_type.clone(),
            chain: msg.chain.clone(),
            sender: msg.sender.clone(),
            signature: msg.signature.clone(),
            item_type: msg.item_type.clone(),
            item_hash: msg.item_hash.clone(),
            item_content: msg.item_content.clone(),
            content,
            channel: msg.channel.clone(),
            time: msg.time,
            size,
            confirmations,
            confirmed,
        }
    }
}

/// Sort by field options for messages
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortBy {
    /// Sort by message time (default)
    Time,
    /// Sort by transaction/confirmation time (uses created_at)
    TxTime,
}

/// Query parameters for message list
#[derive(Debug, Deserialize)]
pub struct MessageQuery {
    pub addresses: Option<String>,
    pub channels: Option<String>,
    #[serde(rename = "msgType")]
    pub message_type: Option<String>,
    /// Also support msgTypes (plural) as alias
    #[serde(rename = "msgTypes")]
    pub message_types: Option<String>,
    pub hashes: Option<String>,
    pub refs: Option<String>,
    pub tags: Option<String>,
    /// Filter by content.type field (comma-separated)
    #[serde(rename = "contentTypes")]
    pub content_types: Option<String>,
    /// Filter by content.item_hash (comma-separated)
    #[serde(rename = "contentHashes")]
    pub content_hashes: Option<String>,
    /// Filter by content key names (comma-separated)  
    #[serde(rename = "contentKeys")]
    pub content_keys: Option<String>,
    /// Filter by chain (comma-separated)
    pub chains: Option<String>,
    /// Filter by content.address for programs/instances (comma-separated)
    pub owners: Option<String>,
    pub pagination: Option<u32>,
    /// Alias for pagination (pyaleph compatibility)
    pub limit: Option<u32>,
    pub page: Option<u32>,
    /// Start time filter (Unix timestamp)
    #[serde(alias = "startDate")]
    pub start_date: Option<f64>,
    /// End time filter (Unix timestamp)
    #[serde(alias = "endDate")]
    pub end_date: Option<f64>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    pub order: Option<i8>,
    /// Alias for order (pyaleph compatibility)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    /// Sort field: "time" (default) or "tx-time"
    #[serde(rename = "sortBy")]
    pub sort_by: Option<SortBy>,
    /// Message statuses filter (comma-separated: processed,pending,rejected,forgotten)
    /// Default: processed,removing (matches pyaleph behavior)
    #[serde(rename = "msgStatuses")]
    pub msg_statuses: Option<String>,
    /// Start block number filter (inclusive) - filters via chain_txs.height
    #[serde(rename = "startBlock")]
    pub start_block: Option<i64>,
    /// End block number filter (inclusive) - filters via chain_txs.height
    #[serde(rename = "endBlock")]
    pub end_block: Option<i64>,
    /// Filter by payment type (comma-separated: hold, superfluid, credit)
    #[serde(rename = "paymentTypes")]
    pub payment_types: Option<String>,
    /// Cursor for keyset pagination (format: `{time}_{item_hash}`)
    /// When provided, skips count query and uses keyset filtering instead of OFFSET.
    pub cursor: Option<String>,
}

/// List messages - matches pyaleph /messages.json response format
///
/// Uses parameterized queries to prevent SQL injection.
/// Supports cursor-based pagination: when `cursor` param is provided, uses keyset
/// pagination (no COUNT query, returns `has_more` + `next_cursor` instead of total).
pub async fn list_messages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    // Support both 'limit' and 'pagination' parameters (limit takes precedence)
    // pagination=0 means "no limit" (pyaleph compat)
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let unlimited = raw_pagination == 0;
    let per_page = if unlimited { 0 } else { raw_pagination.min(1000) };
    let offset = if unlimited { 0 } else { ((page - 1) * per_page) as i64 };

    // Parse cursor for keyset pagination (format: {time}_{item_hash})
    let cursor_parts: Option<(f64, String)> = params.cursor.as_ref().and_then(|c| {
        let underscore_pos = c.find('_')?;
        let time_str = &c[..underscore_pos];
        let hash = &c[underscore_pos + 1..];
        if hash.is_empty() {
            return None;
        }
        let time = time_str.parse::<f64>().ok()?;
        Some((time, hash.to_string()))
    });
    let use_cursor = cursor_parts.is_some();

    // Merge msgType and msgTypes (msgType takes precedence)
    let message_type_filter = params.message_type.or(params.message_types);
    
    if !state.has_db() {
        return Json(json!({
            "messages": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "error": "Database not available"
        }));
    }
    
    // Parse msgStatuses filter - messages table only contains PROCESSED messages
    // Default: ["processed", "removing"] to match pyaleph behavior
    let status_list: Vec<String> = params.msg_statuses
        .as_ref()
        .map(|s| crate::db::parse_csv_param(s).iter().map(|x| x.to_lowercase()).collect())
        .unwrap_or_else(|| vec!["processed".to_string(), "removing".to_string()]);
    
    // The messages table only contains processed messages
    // If the filter does not include "processed" or "removing", return empty
    let include_processed = status_list.iter().any(|s| s == "processed" || s == "removing");
    if !include_processed {
        return Json(json!({
            "messages": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "messages",
        }));
    }
    
    // Determine if we need a LEFT JOIN for tx-time sorting
    let needs_tx_time_join = matches!(params.sort_by, Some(SortBy::TxTime))
        || params.start_block.is_some()
        || params.end_block.is_some();

    // Build query with safe parameterized filters
    // When sort_by=tx-time or block filters are present, LEFT JOIN chain_txs
    // to get the earliest confirmation time (matching pyaleph behavior).
    // Note: our chain_txs.created_at is the insertion time, not the blockchain
    // datetime (which pyaleph stores as chain_txs.datetime). This is the best
    // approximation we have.
    let base_query = if needs_tx_time_join {
        "SELECT messages.* FROM messages \
         LEFT JOIN (SELECT item_hash, MIN(created_at) as earliest_confirmation \
         FROM chain_txs GROUP BY item_hash) ct \
         ON ct.item_hash = messages.item_hash WHERE 1=1"
    } else {
        "SELECT * FROM messages WHERE 1=1"
    };
    let mut builder = crate::db::QueryBuilder::new(base_query);

    // Exclude forgotten messages unless explicitly requested via msgStatuses=forgotten
    let include_forgotten = status_list.iter().any(|s| s == "forgotten");
    if !include_forgotten {
        builder.and_raw("NOT EXISTS (SELECT 1 FROM forgotten_messages WHERE forgotten_messages.item_hash = messages.item_hash)");
    }

    // Parse addresses filter (parameterized)
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            builder.and_in("sender", &addr_list);
        }
    }
    
    // Parse message type filter (parameterized) - supports comma-separated list
    if let Some(ref msg_type) = message_type_filter {
        let type_list: Vec<String> = crate::db::parse_csv_param(msg_type).iter().map(|t| t.to_uppercase()).collect();
        if type_list.len() == 1 {
            builder.and_eq("message_type", type_list[0].clone());
        } else if !type_list.is_empty() {
            builder.and_in("message_type", &type_list);
        }
    }
    
    // Parse channels filter (parameterized)
    if let Some(ref channels) = params.channels {
        let channel_list = crate::db::parse_csv_param(channels);
        if !channel_list.is_empty() {
            builder.and_in("channel", &channel_list);
        }
    }
    
    // Parse hashes filter (filter by item_hash)
    if let Some(ref hashes) = params.hashes {
        let hash_list = crate::db::parse_csv_param(hashes);
        if !hash_list.is_empty() {
            builder.and_in("item_hash", &hash_list);
        }
    }
    
    // Parse chains filter
    if let Some(ref chains) = params.chains {
        let chain_list = crate::db::parse_csv_param(chains);
        if !chain_list.is_empty() {
            builder.and_in("chain", &chain_list);
        }
    }
    
    // Parse refs filter (content.ref field - uses denormalized content_ref column)
    if let Some(ref refs) = params.refs {
        let ref_list = crate::db::parse_csv_param(refs);
        if !ref_list.is_empty() {
            builder.and_in("content_ref", &ref_list);
        }
    }
    
    // Parse tags filter (content.content.tags array - OR logic matching pyaleph ?| operator)
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        if !tag_list.is_empty() {
            builder.and_jsonb_has_any("item_content", "content.tags", &tag_list);
        }
    }
    
    // Parse contentTypes filter (uses denormalized content_type column)
    if let Some(ref content_types) = params.content_types {
        let type_list = crate::db::parse_csv_param(content_types);
        if !type_list.is_empty() {
            builder.and_in("content_type", &type_list);
        }
    }
    
    // Parse contentHashes filter (uses denormalized content_item_hash column)
    if let Some(ref content_hashes) = params.content_hashes {
        let hash_list = crate::db::parse_csv_param(content_hashes);
        if !hash_list.is_empty() {
            builder.and_in("content_item_hash", &hash_list);
        }
    }
    
    // Parse owners filter (uses denormalized owner column)
    if let Some(ref owners) = params.owners {
        let owner_list = crate::db::parse_csv_param(owners);
        if !owner_list.is_empty() {
            builder.and_in("owner", &owner_list);
        }
    }

    // Parse contentKeys filter (check if content.content has any of these keys)
    // Python: MessageDb.content["content"].has_any(ARRAY[keys])
    if let Some(ref content_keys) = params.content_keys {
        let key_list = crate::db::parse_csv_param(content_keys);
        if !key_list.is_empty() {
            // Use ?| operator to check if any of the keys exist in content.content
            let keys_array: Vec<String> = key_list.iter().map(|k| format!("'{}'", k.replace('\'', "''"))).collect();
            let clause = format!(
                "(item_content::jsonb->'content') ?| ARRAY[{}]",
                keys_array.join(", ")
            );
            builder.and_raw(&clause);
        }
    }

    // Parse paymentTypes filter (uses denormalized payment_type column)
    if let Some(ref payment_types) = params.payment_types {
        let pt_list: Vec<String> = crate::db::parse_csv_param(payment_types)
            .iter()
            .map(|s| s.to_lowercase())
            .collect();
        if !pt_list.is_empty() {
            builder.and_in("payment_type", &pt_list);
        }
    }

    // Time filters (parameterized) - endDate uses strict less-than to match pyaleph
    if let Some(start) = params.start_date {
        builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lt("time", end);
    }
    
    // Block number filters (via chain_txs JOIN)
    // If startBlock or endBlock specified, filter by chain_txs.height
    if params.start_block.is_some() || params.end_block.is_some() {
        // Build subquery to get item_hashes matching block range
        let mut block_conditions = Vec::new();
        if let Some(start_block) = params.start_block {
            block_conditions.push(format!("height >= {}", start_block));
        }
        if let Some(end_block) = params.end_block {
            block_conditions.push(format!("height <= {}", end_block));
        }
        
        let block_filter = format!(
            "item_hash IN (SELECT item_hash FROM chain_txs WHERE {})",
            block_conditions.join(" AND ")
        );
        builder.and_raw(&block_filter);
    }
    
    // Order and pagination
    // Support both order and sortOrder params (sortOrder takes precedence)
    let order_value = params.sort_order.or(params.order);
    let ascending = order_value.map(|o| o == 1).unwrap_or(false);

    // Cursor-based keyset filter (only for non-tx-time sorting)
    // For tx-time sorting, cursor is not supported — fall back to offset pagination.
    //
    // The sort order is `time DESC, item_hash ASC` (or ASC, ASC for ascending).
    // Because the secondary sort direction differs from the primary in DESC mode,
    // we cannot use a simple tuple comparison. Instead we use an explicit condition:
    //   DESC: (time < cursor_time) OR (time = cursor_time AND item_hash > cursor_hash)
    //   ASC:  (time > cursor_time) OR (time = cursor_time AND item_hash > cursor_hash)
    if let Some((cursor_time, ref cursor_hash)) = cursor_parts {
        if !needs_tx_time_join {
            builder.and_cursor_keyset("time", cursor_time, "item_hash", cursor_hash.clone(), ascending);
        }
    }

    if needs_tx_time_join {
        // Sort by earliest chain confirmation time, with NULLS handling
        // and secondary sorts for deterministic ordering (matches pyaleph)
        if ascending {
            builder.order_by_raw("ct.earliest_confirmation ASC NULLS LAST, messages.time ASC, messages.item_hash ASC");
        } else {
            builder.order_by_raw("ct.earliest_confirmation DESC NULLS FIRST, messages.time DESC, messages.item_hash ASC");
        }
    } else {
        // Sort by message time with secondary sort on item_hash for determinism
        if ascending {
            builder.order_by_raw("time ASC, item_hash ASC");
        } else {
            builder.order_by_raw("time DESC, item_hash ASC");
        }
    }

    // When using cursor, fetch one extra row to detect has_more
    let fetch_limit = if use_cursor && !needs_tx_time_join && !unlimited {
        per_page as i64 + 1
    } else if !unlimited {
        per_page as i64
    } else {
        0
    };

    if !unlimited {
        builder.limit(if use_cursor && !needs_tx_time_join { fetch_limit } else { per_page as i64 });
        if !use_cursor || needs_tx_time_join {
            builder.offset(offset);
        }
    }

    // Cursor mode: active when cursor param provided and not using tx-time sorting
    let cursor_active = use_cursor && !needs_tx_time_join;

    // Fetch messages with the main query
    let (query, args) = builder.build();
    let mut messages = sqlx::query_as_with::<_, crate::db::models::MessageDb, _>(&query, args)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();

    // Detect has_more for cursor mode by checking if we got the extra row
    let has_more = if cursor_active && !unlimited {
        if messages.len() > per_page as usize {
            messages.truncate(per_page as usize);
            true
        } else {
            false
        }
    } else {
        false
    };

    // Build next_cursor from the last returned row
    let next_cursor = if cursor_active && has_more {
        messages.last().map(|msg| format!("{}_{}", msg.time, msg.item_hash))
    } else {
        None
    };

    // Get total count (skip entirely in cursor mode)
    let total: i64 = if cursor_active {
        // Cursor mode: no count query needed
        0
    } else if !unlimited && (messages.len() as u32) < per_page {
        // Skip-count optimization: if we got fewer rows than per_page,
        // we know the total without running COUNT(*)
        (page as i64 - 1) * per_page as i64 + messages.len() as i64
    } else {
        // Full count query needed
        let mut count_builder = crate::db::QueryBuilder::new("SELECT COUNT(*) FROM messages WHERE 1=1");

        if !include_forgotten {
            count_builder.and_raw("NOT EXISTS (SELECT 1 FROM forgotten_messages WHERE forgotten_messages.item_hash = messages.item_hash)");
        }
        if let Some(ref addresses) = params.addresses {
            let addr_list = crate::db::parse_csv_param(addresses);
            if !addr_list.is_empty() {
                count_builder.and_in("sender", &addr_list);
            }
        }
        if let Some(ref msg_type) = message_type_filter {
            let type_list: Vec<String> = crate::db::parse_csv_param(msg_type).iter().map(|t| t.to_uppercase()).collect();
            if type_list.len() == 1 {
                count_builder.and_eq("message_type", type_list[0].clone());
            } else if !type_list.is_empty() {
                count_builder.and_in("message_type", &type_list);
            }
        }
        if let Some(ref channels) = params.channels {
            let channel_list = crate::db::parse_csv_param(channels);
            if !channel_list.is_empty() {
                count_builder.and_in("channel", &channel_list);
            }
        }
        if let Some(ref hashes) = params.hashes {
            let hash_list = crate::db::parse_csv_param(hashes);
            if !hash_list.is_empty() {
                count_builder.and_in("item_hash", &hash_list);
            }
        }
        if let Some(ref chains) = params.chains {
            let chain_list = crate::db::parse_csv_param(chains);
            if !chain_list.is_empty() {
                count_builder.and_in("chain", &chain_list);
            }
        }
        if let Some(ref refs) = params.refs {
            let ref_list = crate::db::parse_csv_param(refs);
            if !ref_list.is_empty() {
                count_builder.and_in("content_ref", &ref_list);
            }
        }
        if let Some(ref tags) = params.tags {
            let tag_list = crate::db::parse_csv_param(tags);
            if !tag_list.is_empty() {
                count_builder.and_jsonb_has_any("item_content", "content.tags", &tag_list);
            }
        }
        if let Some(ref content_types) = params.content_types {
            let type_list = crate::db::parse_csv_param(content_types);
            if !type_list.is_empty() {
                count_builder.and_in("content_type", &type_list);
            }
        }
        if let Some(ref content_hashes) = params.content_hashes {
            let hash_list = crate::db::parse_csv_param(content_hashes);
            if !hash_list.is_empty() {
                count_builder.and_in("content_item_hash", &hash_list);
            }
        }
        if let Some(ref owners) = params.owners {
            let owner_list = crate::db::parse_csv_param(owners);
            if !owner_list.is_empty() {
                count_builder.and_in("owner", &owner_list);
            }
        }
        if let Some(ref content_keys) = params.content_keys {
            let key_list = crate::db::parse_csv_param(content_keys);
            if !key_list.is_empty() {
                let keys_array: Vec<String> = key_list.iter().map(|k| format!("'{}'", k.replace('\'', "''"))).collect();
                let clause = format!(
                    "(item_content::jsonb->'content') ?| ARRAY[{}]",
                    keys_array.join(", ")
                );
                count_builder.and_raw(&clause);
            }
        }
        if let Some(ref payment_types) = params.payment_types {
            let pt_list: Vec<String> = crate::db::parse_csv_param(payment_types)
                .iter()
                .map(|s| s.to_lowercase())
                .collect();
            if !pt_list.is_empty() {
                count_builder.and_in("payment_type", &pt_list);
            }
        }
        if let Some(start) = params.start_date {
            count_builder.and_gte("time", start);
        }
        if let Some(end) = params.end_date {
            count_builder.and_lt("time", end);
        }
        if params.start_block.is_some() || params.end_block.is_some() {
            let mut block_conditions = Vec::new();
            if let Some(start_block) = params.start_block {
                block_conditions.push(format!("height >= {}", start_block));
            }
            if let Some(end_block) = params.end_block {
                block_conditions.push(format!("height <= {}", end_block));
            }
            let block_filter = format!(
                "item_hash IN (SELECT item_hash FROM chain_txs WHERE {})",
                block_conditions.join(" AND ")
            );
            count_builder.and_raw(&block_filter);
        }

        let (count_query, count_args) = count_builder.build();
        let row: (i64,) = sqlx::query_as_with(&count_query, count_args)
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
        row.0
    };

    // Batch fetch confirmations for all messages
    let item_hashes: Vec<String> = messages.iter().map(|m| m.item_hash.clone()).collect();
    let mut confirmations_map: HashMap<String, Vec<ConfirmationResponse>> = HashMap::new();

    if !item_hashes.is_empty() {
        // Batch fetch confirmations in chunks to avoid exceeding PostgreSQL's u16::MAX param limit
        for chunk in item_hashes.chunks(10_000) {
            let placeholders: Vec<String> = (1..=chunk.len())
                .map(|i| format!("${}", i))
                .collect();
            let query = format!(
                "SELECT item_hash, chain, hash, height FROM chain_txs WHERE item_hash IN ({})",
                placeholders.join(", ")
            );

            let mut q = sqlx::query_as::<_, (String, String, String, i64)>(&query);
            for hash in chunk {
                q = q.bind(hash);
            }

            let confirmations = q.fetch_all(state.db()).await.unwrap_or_default();

            for (item_hash, chain, hash, height) in confirmations {
                confirmations_map
                    .entry(item_hash)
                    .or_insert_with(Vec::new)
                    .push(ConfirmationResponse { chain, hash, height: height as u64 });
            }
        }
    }

    // Convert to response format with confirmations
    let message_responses: Vec<MessageResponse> = messages.iter()
        .map(|msg| {
            let confirmations = confirmations_map
                .get(&msg.item_hash)
                .cloned()
                .unwrap_or_default();
            MessageResponse::from_db(msg, confirmations)
        })
        .collect();

    // Build response: cursor mode returns has_more/next_cursor, legacy returns total/page
    if cursor_active {
        let mut resp = json!({
            "messages": message_responses,
            "pagination_per_page": if unlimited { raw_pagination } else { per_page },
            "has_more": has_more,
        });
        if let Some(cursor) = next_cursor {
            resp["next_cursor"] = serde_json::Value::String(cursor);
        }
        Json(resp)
    } else {
        Json(json!({
            "messages": message_responses,
            "pagination_total": total,
            "pagination_page": page,
            "pagination_per_page": if unlimited { raw_pagination } else { per_page },
            "pagination_item": "messages",
        }))
    }
}

/// Get a single message by hash - matches pyaleph format
/// Reference: aleph/web/controllers/messages.py:view_message
pub async fn get_message(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "status": "error",
            "message": "Database not available"
        })));
    }

    let message = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await;

    match message {
        Ok(Some(msg)) => {
            // Fetch confirmations
            let confirmations: Vec<ConfirmationResponse> = sqlx::query_as::<_, (String, String, i64)>(
                "SELECT chain, hash, height FROM chain_txs WHERE item_hash = $1"
            )
            .bind(&hash)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(chain, hash, height)| ConfirmationResponse {
                chain,
                hash,
                height: height as u64,
            })
            .collect();

            // Use created_at as reception_time (when message was inserted into DB)
            let reception_time = msg.created_at.timestamp() as f64;
            let response = MessageResponse::from_db(&msg, confirmations);

            (StatusCode::OK, Json(json!({
                "status": "processed",
                "item_hash": hash,
                "reception_time": reception_time,
                "message": response
            })))
        }
        Ok(None) => {
            // Check if it's pending - include message content in response
            let pending = sqlx::query_as::<_, (String, f64, serde_json::Value)>(
                "SELECT item_hash, reception_time, message FROM pending_messages WHERE item_hash = $1 LIMIT 1"
            )
            .bind(&hash)
            .fetch_optional(state.db())
            .await
            .unwrap_or(None);

            if let Some((_item_hash, reception_time, message)) = pending {
                return (StatusCode::OK, Json(json!({
                    "status": "pending",
                    "item_hash": hash,
                    "reception_time": reception_time,
                    "messages": [message],
                })));
            }

            // Check forgotten messages
            let forgotten = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM forgotten_messages WHERE item_hash = $1)"
            )
            .bind(&hash)
            .fetch_one(state.db())
            .await
            .unwrap_or(false);

            if forgotten {
                return (StatusCode::OK, Json(json!({
                    "status": "forgotten",
                    "item_hash": hash,
                })));
            }

            // Check rejected messages
            let rejected = sqlx::query_as::<_, (i32, Option<String>)>(
                "SELECT error_code, error_message FROM rejected_messages WHERE item_hash = $1"
            )
            .bind(&hash)
            .fetch_optional(state.db())
            .await
            .ok()
            .flatten();

            if let Some((code, message)) = rejected {
                return (StatusCode::OK, Json(json!({
                    "status": "rejected",
                    "item_hash": hash,
                    "error_code": code,
                    "error_message": message,
                })));
            }

            (StatusCode::NOT_FOUND, Json(json!({
                "error": "Message not found"
            })))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "status": "error",
            "message": e.to_string()
        }))),
    }
}

/// Get message status - /messages/{hash}/status
pub async fn get_message_status(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "status": "error",
            "message": "Database not available"
        })));
    }
    
    // Check processed messages - include reception_time (created_at)
    let processed = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>,)>(
        "SELECT created_at FROM messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    if let Some((created_at,)) = processed {
        let reception_time = created_at.timestamp() as f64;
        return (StatusCode::OK, Json(json!({
            "status": "processed",
            "item_hash": hash,
            "reception_time": reception_time,
        })));
    }

    // Check pending messages - include reception_time
    let pending = sqlx::query_as::<_, (f64,)>(
        "SELECT reception_time FROM pending_messages WHERE item_hash = $1 LIMIT 1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    if let Some((reception_time,)) = pending {
        return (StatusCode::OK, Json(json!({
            "status": "pending",
            "item_hash": hash,
            "reception_time": reception_time,
        })));
    }
    
    // Check rejected messages
    let rejected = sqlx::query_as::<_, (i32, Option<String>)>(
        "SELECT error_code, error_message FROM rejected_messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();
    
    if let Some((code, message)) = rejected {
        return (StatusCode::OK, Json(json!({
            "status": "rejected",
            "item_hash": hash,
            "error_code": code,
            "error_message": message,
        })));
    }
    
    // Check forgotten messages
    let forgotten = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM forgotten_messages WHERE item_hash = $1)"
    )
    .bind(&hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(false);
    
    if forgotten {
        return (StatusCode::OK, Json(json!({
            "status": "forgotten",
            "item_hash": hash,
        })));
    }
    
    (StatusCode::NOT_FOUND, Json(json!({
        "status": "unknown",
        "item_hash": hash,
    })))
}

/// Post content request
#[derive(Debug, Deserialize)]
pub struct PostContentRequest {
    pub message: serde_json::Value,
    #[serde(default)]
    pub sync: bool,
}

/// Post a new message
pub async fn post_message(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PostContentRequest>,
) -> impl IntoResponse {
    // Parse the message
    let message: Result<Message, _> = serde_json::from_value(payload.message.clone());
    
    match message {
        Ok(msg) => {
            // Verify signature
            let sig_valid = msg.verify_signature(&state.crypto);
            
            match sig_valid {
                Ok(false) => {
                    return (StatusCode::BAD_REQUEST, Json(json!({
                        "status": "error",
                        "error_code": ErrorCode::InvalidSignature.as_i32(),
                        "message": "Signature verification failed"
                    })));
                }
                Err(e) => {
                    return (StatusCode::BAD_REQUEST, Json(json!({
                        "status": "error",
                        "error_code": ErrorCode::InvalidSignature.as_i32(),
                        "message": format!("Signature verification error: {}", e)
                    })));
                }
                Ok(true) => {}
            }
            
            // Verify item hash
            if let Some(ref content) = msg.item_content {
                if !msg.verify_item_hash().unwrap_or(false) {
                    return (StatusCode::BAD_REQUEST, Json(json!({
                        "status": "error",
                        "error_code": ErrorCode::InvalidFormat.as_i32(),
                        "message": "Item hash does not match content"
                    })));
                }
            }
            
            // Store in pending_messages for processing
            if state.has_db() {
                let now = chrono::Utc::now().timestamp() as f64;
                let result = sqlx::query(
                    "INSERT INTO pending_messages (item_hash, message, reception_time, retries, next_attempt) \
                     VALUES ($1, $2, $3, 0, $3) ON CONFLICT (item_hash) DO NOTHING"
                )
                .bind(&msg.item_hash)
                .bind(&payload.message)
                .bind(now)
                .execute(state.db())
                .await;
                
                if let Err(e) = result {
                    tracing::error!("Failed to store pending message: {}", e);
                }
            }
            
            // Publish to P2P network
            if let Some(ref rabbitmq) = state.rabbitmq {
                let service = rabbitmq.read().await;
                if let Err(e) = service.publish_to_network(&msg).await {
                    tracing::warn!("Failed to publish to P2P: {}", e);
                }
            }

            // Sync mode: poll until message is processed or rejected (up to 30s timeout)
            if payload.sync && state.has_db() {
                let item_hash = msg.item_hash.clone();
                let start = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(30);

                loop {
                    if start.elapsed() > timeout {
                        // Timeout - return pending status
                        return (StatusCode::ACCEPTED, Json(json!({
                            "publication_status": {"status": "success", "failed": []},
                            "message_status": "pending"
                        })));
                    }

                    // Check if processed
                    let processed = sqlx::query_scalar::<_, bool>(
                        "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)"
                    )
                    .bind(&item_hash)
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
                    .bind(&item_hash)
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

                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }

            (StatusCode::ACCEPTED, Json(json!({
                "publication_status": {
                    "status": "success",
                    "failed": []
                },
                "message_status": "pending"
            })))
        }
        Err(e) => {
            (StatusCode::BAD_REQUEST, Json(json!({
                "status": "error",
                "error_code": ErrorCode::InvalidFormat.as_i32(),
                "message": format!("Invalid message format: {}", e)
            })))
        }
    }
}

/// Query parameters for aggregate
#[derive(Debug, Deserialize)]
pub struct AggregateQuery {
    pub keys: Option<String>,
    pub limit: Option<u32>,         // NEW: limit number of aggregates
    pub with_info: Option<bool>,    // NEW: include metadata (created, last_updated, item_hashes)
    pub value_only: Option<bool>,   // NEW: return only the value, not the wrapper
}

/// Get aggregates for an address - matches pyaleph format
/// 
/// Uses parameterized queries to prevent SQL injection.
/// Supports:
/// - keys: comma-separated list of keys to filter
/// - limit: limit number of aggregates returned
/// - with_info: include metadata (created, last_updated, item_hashes)
/// - value_only: return just the aggregate values (only if single key requested)
pub async fn get_aggregates(
    State(state): State<Arc<AppState>>,
    Path(raw_address): Path<String>,
    Query(params): Query<AggregateQuery>,
) -> impl IntoResponse {
    // Strip .json suffix if present (pyaleph compat: /aggregates/0xaddr.json)
    let address = raw_address.strip_suffix(".json").unwrap_or(&raw_address).to_string();

    if !state.has_db() {
        return (StatusCode::OK, Json(json!({
            "address": address,
            "data": {},
            "error": "Database not available"
        })));
    }
    
    // Parse keys filter safely
    let key_list: Option<Vec<String>> = params.keys.as_ref().map(|keys| {
        keys.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    
    let with_info = params.with_info.unwrap_or(false);
    let value_only = params.value_only.unwrap_or(false);
    let limit = params.limit.unwrap_or(1000); // pyaleph default: 1000
    
    // Build base query - different for with_info vs regular
    if with_info {
        // Query with join to get metadata — include dirty flag for lazy refresh
        let aggregates: Vec<(String, serde_json::Value, f64, f64, Option<String>, Option<String>, bool)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                let mut query_str = String::from(
                    "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash, \
                     a.dirty \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1 AND a.key = ANY($2) LIMIT $3"
                );
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .bind(keys)
                    .bind(limit as i64)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                let query_str = "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash, \
                     a.dirty \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1 LIMIT $2";
                sqlx::query_as(query_str)
                    .bind(&address)
                    .bind(limit as i64)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
        };

        if aggregates.is_empty() {
            return (StatusCode::NOT_FOUND, Json(json!({
                "error": "No aggregate found for this address"
            })));
        }

        // Lazy refresh: rebuild any dirty aggregates before returning
        let dirty_keys: Vec<String> = aggregates.iter()
            .filter(|(_, _, _, _, _, _, dirty)| *dirty)
            .map(|(key, _, _, _, _, _, _)| key.clone())
            .collect();

        if !dirty_keys.is_empty() {
            for key in &dirty_keys {
                if let Err(e) = crate::handlers::aggregate::AggregateHandler::rebuild_aggregate_from_elements(
                    state.db(), &address, key,
                ).await {
                    tracing::warn!("Failed to refresh dirty aggregate {}/{}: {}", address, key, e);
                }
            }

            // Re-fetch after refresh
            let refreshed: Vec<(String, serde_json::Value, f64, f64, Option<String>, Option<String>)> = match &key_list {
                Some(keys) if !keys.is_empty() => {
                    let query_str = "SELECT a.key, a.content, a.time as created, \
                         COALESCE(ae.time, a.time) as last_updated, \
                         a.last_revision_hash as last_update_item_hash, \
                         ae.item_hash as original_item_hash \
                         FROM aggregates a \
                         LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                         WHERE a.address = $1 AND a.key = ANY($2) LIMIT $3";
                    sqlx::query_as(query_str)
                        .bind(&address)
                        .bind(keys)
                        .bind(limit as i64)
                        .fetch_all(state.db())
                        .await
                        .unwrap_or_default()
                }
                _ => {
                    let query_str = "SELECT a.key, a.content, a.time as created, \
                         COALESCE(ae.time, a.time) as last_updated, \
                         a.last_revision_hash as last_update_item_hash, \
                         ae.item_hash as original_item_hash \
                         FROM aggregates a \
                         LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                         WHERE a.address = $1 LIMIT $2";
                    sqlx::query_as(query_str)
                        .bind(&address)
                        .bind(limit as i64)
                        .fetch_all(state.db())
                        .await
                        .unwrap_or_default()
                }
            };

            let mut data = serde_json::Map::new();
            let mut info = serde_json::Map::new();

            for (key, content, created, last_updated, last_update_hash, original_hash) in refreshed {
                data.insert(key.clone(), content);
                let created_dt = chrono::DateTime::from_timestamp(created as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| created.to_string());
                let last_updated_dt = chrono::DateTime::from_timestamp(last_updated as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| last_updated.to_string());
                info.insert(key, json!({
                    "created": created_dt,
                    "last_updated": last_updated_dt,
                    "original_item_hash": original_hash.unwrap_or_default(),
                    "last_update_item_hash": last_update_hash.unwrap_or_default(),
                }));
            }

            return (StatusCode::OK, Json(json!({
                "address": address,
                "data": data,
                "info": info,
            })));
        }

        // Build data and info maps
        let mut data = serde_json::Map::new();
        let mut info = serde_json::Map::new();

        for (key, content, created, last_updated, last_update_hash, original_hash, _dirty) in aggregates {
            data.insert(key.clone(), content);

            // Convert timestamps to ISO format
            let created_dt = chrono::DateTime::from_timestamp(created as i64, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| created.to_string());
            let last_updated_dt = chrono::DateTime::from_timestamp(last_updated as i64, 0)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| last_updated.to_string());

            info.insert(key, json!({
                "created": created_dt,
                "last_updated": last_updated_dt,
                "original_item_hash": original_hash.unwrap_or_default(),
                "last_update_item_hash": last_update_hash.unwrap_or_default(),
            }));
        }

        (StatusCode::OK, Json(json!({
            "address": address,
            "data": data,
            "info": info,
        })))
    } else {
        // Regular query without metadata — include dirty flag for lazy refresh
        let aggregates: Vec<(String, serde_json::Value, bool)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                sqlx::query_as(
                    "SELECT key, content, dirty FROM aggregates WHERE address = $1 AND key = ANY($2) LIMIT $3"
                )
                    .bind(&address)
                    .bind(keys)
                    .bind(limit as i64)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                sqlx::query_as(
                    "SELECT key, content, dirty FROM aggregates WHERE address = $1 LIMIT $2"
                )
                    .bind(&address)
                    .bind(limit as i64)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
        };

        if aggregates.is_empty() {
            return (StatusCode::NOT_FOUND, Json(json!({
                "error": "No aggregate found for this address"
            })));
        }

        // Lazy refresh: rebuild any dirty aggregates before returning
        // Reference: aleph/web/controllers/aggregates.py — refreshes dirty on read
        let dirty_keys: Vec<String> = aggregates.iter()
            .filter(|(_, _, dirty)| *dirty)
            .map(|(key, _, _)| key.clone())
            .collect();

        if !dirty_keys.is_empty() {
            for key in &dirty_keys {
                if let Err(e) = crate::handlers::aggregate::AggregateHandler::rebuild_aggregate_from_elements(
                    state.db(), &address, key,
                ).await {
                    tracing::warn!("Failed to refresh dirty aggregate {}/{}: {}", address, key, e);
                }
            }

            // Re-fetch the refreshed aggregates
            let refreshed: Vec<(String, serde_json::Value)> = match &key_list {
                Some(keys) if !keys.is_empty() => {
                    sqlx::query_as(
                        "SELECT key, content FROM aggregates WHERE address = $1 AND key = ANY($2) LIMIT $3"
                    )
                        .bind(&address)
                        .bind(keys)
                        .bind(limit as i64)
                        .fetch_all(state.db())
                        .await
                        .unwrap_or_default()
                }
                _ => {
                    sqlx::query_as(
                        "SELECT key, content FROM aggregates WHERE address = $1 LIMIT $2"
                    )
                        .bind(&address)
                        .bind(limit as i64)
                        .fetch_all(state.db())
                        .await
                        .unwrap_or_default()
                }
            };

            // Handle value_only
            if value_only {
                if let Some(ref keys) = key_list {
                    if keys.len() == 1 {
                        for (key, content) in &refreshed {
                            if key == &keys[0] {
                                return (StatusCode::OK, Json(content.clone()));
                            }
                        }
                    }
                }
            }

            let mut data = serde_json::Map::new();
            for (key, content) in refreshed {
                data.insert(key, content);
            }

            return (StatusCode::OK, Json(json!({
                "address": address,
                "data": data,
                "info": {},
            })));
        }

        // Handle value_only - works regardless of with_info (3.1.4)
        if value_only {
            if let Some(ref keys) = key_list {
                if keys.len() == 1 {
                    for (key, content, _) in &aggregates {
                        if key == &keys[0] {
                            return (StatusCode::OK, Json(content.clone()));
                        }
                    }
                }
            }
        }

        // Build data map
        let mut data = serde_json::Map::new();
        for (key, content, _) in aggregates {
            data.insert(key, content);
        }

        // Always include info key (empty object when with_info=false) to match pyaleph
        (StatusCode::OK, Json(json!({
            "address": address,
            "data": data,
            "info": {},
        })))
    }
}

/// Query parameters for posts
/// Reference: aleph/web/controllers/posts.py
#[derive(Debug, Deserialize)]
pub struct PostsQuery {
    /// Filter by sender addresses (comma-separated)
    pub addresses: Option<String>,
    /// Filter by channel (comma-separated)
    pub channels: Option<String>,
    /// Filter by post type (comma-separated, e.g. "amend", "chat")
    pub types: Option<String>,
    /// Filter by content.ref (comma-separated)
    pub refs: Option<String>,
    /// Filter by tags (comma-separated) - searches in content.tags array
    pub tags: Option<String>,
    /// Filter by item_hash (comma-separated)
    pub hashes: Option<String>,
    /// Items per page (alias: limit)
    pub pagination: Option<u32>,
    /// Alias for pagination (pyaleph compatibility)
    pub limit: Option<u32>,
    /// Page number (1-indexed)
    pub page: Option<u32>,
    /// Start time filter (Unix timestamp)
    #[serde(rename = "startDate")]
    pub start_date: Option<f64>,
    /// End time filter (Unix timestamp)
    #[serde(rename = "endDate")]
    pub end_date: Option<f64>,
    /// Sort field (default: time). Allowed: time, address, post_type, channel
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    pub order: Option<i8>,
    /// Alias for order (pyaleph compatibility)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
}

/// Post response format for v0 API - includes message-level fields
/// Reference: aleph/web/controllers/posts.py
///
/// The v0 API merges original posts with their latest amends:
/// - Only returns original posts (amends IS NULL)
/// - Content comes from latest amend if one exists (COALESCE)
/// - type field = post_type (e.g. "staking-rewards-distribution"), NOT "POST"
/// - original_type = original post's type
#[derive(Debug, Clone, Serialize)]
pub struct PostResponseV0 {
    // From posts table (merged original + amend)
    pub item_hash: String,
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub address: String,
    #[serde(rename = "type")]
    pub post_type: String,  // The post type (e.g. "staking-rewards-distribution")
    pub content: serde_json::Value,
    pub channel: Option<String>,
    pub time: f64,
    pub original_item_hash: String,
    #[serde(rename = "hash")]
    pub hash: String,
    // From messages table (joined)
    pub chain: String,
    pub sender: String,
    pub signature: String,
    pub item_type: String,
    pub item_content: Option<String>,
    pub size: i64,
    pub confirmed: bool,
    pub confirmations: Vec<ConfirmationResponse>,
    pub original_signature: Option<String>,
    pub original_type: Option<String>,
}

/// Get posts - matches pyaleph format
/// Reference: aleph/web/controllers/posts.py
pub async fn get_posts(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PostsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    // Support both limit and pagination parameters (limit takes precedence)
    // pagination=0 means "no limit" (pyaleph compat)
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let posts_unlimited = raw_pagination == 0;
    let per_page = if posts_unlimited { 0 } else { raw_pagination.min(1000) };
    let offset = if posts_unlimited { 0 } else { ((page - 1) * per_page) as i64 };

    // Merge order and sort_order (order takes precedence)
    let order_param = params.order.or(params.sort_order);

    if !state.has_db() {
        return Json(json!({
            "posts": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": if posts_unlimited { raw_pagination } else { per_page },
        }));
    }

    // Determine if we need tx-time join for posts
    let posts_needs_tx_time = matches!(params.sort_by.as_deref(), Some("tx-time"));

    // Build merged post query matching Python pyaleph's make_select_merged_post_with_message_info_stmt()
    // - Only return originals (p.amends IS NULL)
    // - LEFT JOIN latest amend for content/type coalescing
    // - LEFT JOIN messages for both original and amend message-level fields
    let base_posts_query = if posts_needs_tx_time {
        "SELECT p.item_hash AS original_item_hash, \
         COALESCE(a.item_hash, p.item_hash) AS item_hash, \
         p.address, \
         COALESCE(a.post_type, p.post_type) AS post_type, \
         COALESCE(a.content, p.content) AS content, \
         p.ref_, p.channel, \
         COALESCE(a.time, p.time) AS time, \
         om.chain, om.sender, \
         COALESCE(am.signature, om.signature) AS signature, \
         COALESCE(am.item_type, om.item_type) AS item_type, \
         CASE WHEN am.item_content IS NOT NULL THEN am.item_content ELSE om.item_content END AS item_content, \
         om.signature AS original_signature, \
         p.post_type AS original_type \
         FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         LEFT JOIN messages om ON p.item_hash = om.item_hash \
         LEFT JOIN messages am ON a.item_hash = am.item_hash \
         LEFT JOIN (SELECT item_hash, MIN(created_at) as earliest_confirmation \
         FROM chain_txs GROUP BY item_hash) ct \
         ON ct.item_hash = COALESCE(a.item_hash, p.item_hash) \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    } else {
        "SELECT p.item_hash AS original_item_hash, \
         COALESCE(a.item_hash, p.item_hash) AS item_hash, \
         p.address, \
         COALESCE(a.post_type, p.post_type) AS post_type, \
         COALESCE(a.content, p.content) AS content, \
         p.ref_, p.channel, \
         COALESCE(a.time, p.time) AS time, \
         om.chain, om.sender, \
         COALESCE(am.signature, om.signature) AS signature, \
         COALESCE(am.item_type, om.item_type) AS item_type, \
         CASE WHEN am.item_content IS NOT NULL THEN am.item_content ELSE om.item_content END AS item_content, \
         om.signature AS original_signature, \
         p.post_type AS original_type \
         FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         LEFT JOIN messages om ON p.item_hash = om.item_hash \
         LEFT JOIN messages am ON a.item_hash = am.item_hash \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    };
    let mut builder = crate::db::QueryBuilder::new(base_posts_query);
    let mut count_builder = crate::db::QueryBuilder::new(
        "SELECT COUNT(*) FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    );
    
    // Filter by addresses (sender)
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            builder.and_in("p.address", &addr_list);
            count_builder.and_in("p.address", &addr_list);
        }
    }
    
    // Filter by channels
    if let Some(ref channels) = params.channels {
        let channel_list = crate::db::parse_csv_param(channels);
        if !channel_list.is_empty() {
            builder.and_in("p.channel", &channel_list);
            count_builder.and_in("p.channel", &channel_list);
        }
    }
    
    // Filter by post types (filter on original's post_type, matching Python's original_type filter)
    if let Some(ref types) = params.types {
        let type_list = crate::db::parse_csv_param(types);
        if !type_list.is_empty() {
            builder.and_in("p.post_type", &type_list);
            count_builder.and_in("p.post_type", &type_list);
        }
    }
    
    // Filter by refs
    if let Some(ref refs) = params.refs {
        let ref_list = crate::db::parse_csv_param(refs);
        if !ref_list.is_empty() {
            builder.and_in("p.ref_", &ref_list);
            count_builder.and_in("p.ref_", &ref_list);
        }
    }
    
    // Filter by item hashes
    if let Some(ref hashes) = params.hashes {
        let hash_list = crate::db::parse_csv_param(hashes);
        if !hash_list.is_empty() {
            builder.and_in("p.item_hash", &hash_list);
            count_builder.and_in("p.item_hash", &hash_list);
        }
    }
    
    // Time filters - use COALESCE(a.created_at, p.created_at) for date filtering (matches pyaleph)
    if let Some(start) = params.start_date {
        builder.and_raw(&format!("COALESCE(a.time, p.time) >= {}", start));
        count_builder.and_raw(&format!("COALESCE(a.time, p.time) >= {}", start));
    }
    if let Some(end) = params.end_date {
        builder.and_raw(&format!("COALESCE(a.time, p.time) < {}", end));
        count_builder.and_raw(&format!("COALESCE(a.time, p.time) < {}", end));
    }

    // Filter by tags - OR logic matching pyaleph's ?| operator
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        if !tag_list.is_empty() {
            let keys_array: Vec<String> = tag_list.iter().map(|k| format!("'{}'", k.replace('\'', "''"))).collect();
            let clause = format!(
                "(COALESCE(a.content, p.content)->'tags') ?| ARRAY[{}]",
                keys_array.join(", ")
            );
            builder.and_raw(&clause);
            count_builder.and_raw(&clause);
        }
    }
    
    // Determine sort column (validate against allowed columns)
    let needs_tx_time = matches!(params.sort_by.as_deref(), Some("tx-time"));
    let sort_column = match params.sort_by.as_deref() {
        Some("time") | None => "COALESCE(a.time, p.time)".to_string(),
        Some("address") => "p.address".to_string(),
        Some("post_type") => "p.post_type".to_string(),
        Some("channel") => "p.channel".to_string(),
        Some("tx-time") => "ct.earliest_confirmation".to_string(),
        _ => "COALESCE(a.created_at, p.created_at)".to_string(),
    };

    // Order: 1 = ascending, -1 = descending (default)
    let ascending = order_param.map(|o| o == 1).unwrap_or(false);

    // Add raw ORDER BY with secondary sort on original_item_hash for determinism
    let order_dir = if ascending { "ASC" } else { "DESC" };
    let nulls = if ascending { "NULLS LAST" } else { "NULLS FIRST" };
    let limit_clause = if posts_unlimited { String::new() } else { format!(" LIMIT {} OFFSET {}", per_page, offset) };
    let order_clause = if needs_tx_time {
        format!("1=1 ORDER BY {} {} {}, p.item_hash ASC{}", sort_column, order_dir, nulls, limit_clause)
    } else {
        format!("1=1 ORDER BY {} {}, p.item_hash ASC{}", sort_column, order_dir, limit_clause)
    };
    builder.and_raw(&order_clause);
    
    // Get total count first
    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    // Define row type for the merged post query
    type PostJoinRow = (
        String,                    // 0: original_item_hash
        String,                    // 1: item_hash (coalesced)
        String,                    // 2: address
        String,                    // 3: post_type (coalesced)
        serde_json::Value,         // 4: content (coalesced)
        Option<String>,            // 5: ref_
        Option<String>,            // 6: channel
        Option<f64>,               // 7: time (epoch)
        Option<String>,            // 8: chain
        Option<String>,            // 9: sender
        Option<String>,            // 10: signature (coalesced)
        Option<String>,            // 11: item_type (coalesced)
        Option<String>,            // 12: item_content
        Option<String>,            // 13: original_signature
        Option<String>,            // 14: original_type
    );
    
    // Get the posts with joined message data
    let (query, args) = builder.build();
    let rows: Vec<PostJoinRow> = match sqlx::query_as_with(&query, args)
        .fetch_all(state.db())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Posts query failed: {} | SQL: {}", e, query);
            vec![]
        }
    };
    
    // Get coalesced item_hashes for confirmation lookup (uses amend hash when available)
    let coalesced_hashes: Vec<String> = rows.iter().map(|r| r.1.clone()).collect();
    let mut confirmations_map: HashMap<String, Vec<ConfirmationResponse>> = HashMap::new();

    if !coalesced_hashes.is_empty() {
        // Batch in chunks to avoid exceeding PostgreSQL's u16::MAX param limit
        for chunk in coalesced_hashes.chunks(10_000) {
            let placeholders: Vec<String> = (1..=chunk.len())
                .map(|i| format!("${}", i))
                .collect();
            let conf_query = format!(
                "SELECT item_hash, chain, hash, height FROM chain_txs WHERE item_hash IN ({})",
                placeholders.join(", ")
            );

            let mut q = sqlx::query_as::<_, (String, String, String, i64)>(&conf_query);
            for hash in chunk {
                q = q.bind(hash);
            }

            let confirmations = q.fetch_all(state.db()).await.unwrap_or_default();

            for (item_hash, chain, hash, height) in confirmations {
                confirmations_map
                    .entry(item_hash)
                    .or_insert_with(Vec::new)
                    .push(ConfirmationResponse { chain, hash, height: height as u64 });
            }
        }
    }
    
    // Build response — merged post format matching Python pyaleph
    let posts: Vec<PostResponseV0> = rows.iter().map(|row| {
        let confirmations = confirmations_map
            .get(&row.1)
            .cloned()
            .unwrap_or_default();
        let confirmed = !confirmations.is_empty();
        
        let size = row.12.as_ref().map(|c| c.len() as i64).unwrap_or(0);
        
        PostResponseV0 {
            item_hash: row.1.clone(),          // coalesced (amend or original)
            ref_: row.5.clone(),
            address: row.2.clone(),
            post_type: row.3.clone(),          // coalesced post_type (NOT "POST")
            content: row.4.clone(),            // coalesced content
            channel: row.6.clone(),
            time: row.7.unwrap_or(0.0),
            original_item_hash: row.0.clone(), // always the original
            hash: row.0.clone(),               // pyaleph returns original_item_hash as hash
            chain: row.8.clone().unwrap_or_else(|| "ETH".to_string()),
            sender: row.9.clone().unwrap_or_else(|| row.2.clone()),
            signature: row.10.clone().unwrap_or_default(),
            item_type: row.11.clone().unwrap_or_else(|| "inline".to_string()),
            item_content: row.12.clone(),
            size,
            confirmed,
            confirmations,
            original_signature: row.13.clone(),
            original_type: row.14.clone(),
        }
    }).collect();
    
    Json(json!({
        "posts": posts,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": if posts_unlimited { raw_pagination } else { per_page },
        "pagination_item": "posts",
    }))
}

/// Query parameters for single address balance
#[derive(Debug, Deserialize)]
pub struct BalanceQuery {
    /// Filter by chain (e.g. "ETH", "AVAX")
    pub chain: Option<String>,
}

/// Get balance for an address - matches pyaleph GetAccountBalanceResponse format
/// Reference: aleph/web/controllers/accounts.py:get_account_balance
/// Returns balance as float, with per-chain details map and locked_amount from costs
pub async fn get_balance(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<BalanceQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "balance": 0.0,
            "locked_amount": 0.0,
            "details": {},
            "credit_balance": 0,
        }));
    }

    // Query per-chain balances for this address (optionally filtered by chain)
    let chain_balances: Vec<(String, rust_decimal::Decimal)> = if let Some(ref chain) = params.chain {
        sqlx::query_as(
            "SELECT chain, balance FROM balances WHERE address = $1 AND chain = $2"
        )
        .bind(&address)
        .bind(chain)
        .fetch_all(state.db())
        .await
        .unwrap_or_default()
    } else {
        sqlx::query_as(
            "SELECT chain, balance FROM balances WHERE address = $1"
        )
        .bind(&address)
        .fetch_all(state.db())
        .await
        .unwrap_or_default()
    };

    // Build details map and compute total
    let mut details = serde_json::Map::new();
    let mut total = rust_decimal::Decimal::ZERO;
    for (chain, balance) in &chain_balances {
        let bal_f64: f64 = balance.to_string().parse().unwrap_or(0.0);
        details.insert(chain.clone(), json!(bal_f64));
        total += balance;
    }
    let total_f64: f64 = total.to_string().parse().unwrap_or(0.0);

    // Query locked_amount from account_costs (cost_hold for the address, matching pyaleph)
    let locked: rust_decimal::Decimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(cost_hold), 0) FROM account_costs WHERE address = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or(rust_decimal::Decimal::ZERO);
    let locked_f64: f64 = locked.to_string().parse().unwrap_or(0.0);

    // Query credit_balance
    let credit_balance: i64 = sqlx::query_scalar(
        "SELECT COALESCE(balance, 0)::bigint FROM credit_balances WHERE address = $1"
    )
    .bind(&address)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    Json(json!({
        "address": address,
        "balance": total_f64,
        "locked_amount": locked_f64,
        "details": details,
        "credit_balance": credit_balance,
    }))
}

/// Get credit balance for an address
pub async fn get_credit_balance(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "balance": "0",
        }));
    }
    
    let balance = sqlx::query_as::<_, crate::db::models::CreditBalanceDb>(
        "SELECT * FROM credit_balances WHERE address = $1"
    )
    .bind(&address)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();
    
    match balance {
        Some(b) => Json(json!({
            "address": address,
            "balance": b.balance.to_string(),
            "expiration": b.expiration,
        })),
        None => Json(json!({
            "address": address,
            "balance": "0",
        })),
    }
}

/// Query parameters for credit balances list
#[derive(Debug, Deserialize)]
pub struct CreditBalancesQuery {
    pub addresses: Option<String>,       // Comma-separated addresses filter
    pub min_balance: Option<i64>,        // Minimum balance filter
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

/// Credit balance response item — matches Python AddressCreditBalanceResponse
#[derive(Debug, Serialize)]
pub struct CreditBalanceItem {
    pub address: String,
    pub credits: i64,  // Integer to match Python pyaleph
}

/// Get all credit balances - matches pyaleph /credit_balances format
pub async fn get_credit_balances(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CreditBalancesQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(100).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    let min_balance = params.min_balance.unwrap_or(0);
    
    if !state.has_db() {
        return Json(json!({
            "credit_balances": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "credit_balances",
            "error": "Database not available"
        }));
    }
    
    // Parse addresses filter
    let address_list: Option<Vec<String>> = params.addresses.as_ref().map(|addrs| {
        addrs.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    
    // Build query based on filters
    let (credit_balances, total) = if let Some(ref addresses) = address_list {
        if addresses.is_empty() {
            (vec![], 0i64)
        } else {
            // Query with address filter
            let balances: Vec<(String, rust_decimal::Decimal)> = sqlx::query_as(
                "SELECT address, balance FROM credit_balances WHERE address = ANY($1) AND balance >= $2 LIMIT $3 OFFSET $4"
            )
            .bind(addresses)
            .bind(rust_decimal::Decimal::from(min_balance))
            .bind(per_page as i64)
            .bind(offset)
            .fetch_all(state.db())
            .await
            .unwrap_or_default();
            
            let count: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM credit_balances WHERE address = ANY($1) AND balance >= $2"
            )
            .bind(addresses)
            .bind(rust_decimal::Decimal::from(min_balance))
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
            
            (balances, count.0)
        }
    } else {
        // Query without address filter
        let balances: Vec<(String, rust_decimal::Decimal)> = sqlx::query_as(
            "SELECT address, balance FROM credit_balances WHERE balance >= $1 LIMIT $2 OFFSET $3"
        )
        .bind(rust_decimal::Decimal::from(min_balance))
        .bind(per_page as i64)
        .bind(offset)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();
        
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM credit_balances WHERE balance >= $1"
        )
        .bind(rust_decimal::Decimal::from(min_balance))
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
        
        (balances, count.0)
    };
    
    // Format response
    let formatted_balances: Vec<CreditBalanceItem> = credit_balances
        .into_iter()
        .map(|(address, balance)| CreditBalanceItem {
            address,
            credits: balance.to_string().parse::<i64>().unwrap_or(0),
        })
        .collect();
    
    Json(json!({
        "credit_balances": formatted_balances,
        "pagination_per_page": per_page,
        "pagination_page": page,
        "pagination_total": total,
        "pagination_item": "credit_balances",
    }))
}

/// Get storage content as base64 — matches pyaleph format
/// Reference: aleph/web/controllers/storage.py:get_hash
pub async fn get_storage(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Determine engine from hash format (IPFS CID vs storage/inline hash)
    let engine = if hash.starts_with("Qm") && (44..=46).contains(&hash.len()) {
        "ipfs"
    } else if hash.starts_with("bafy") && hash.len() == 59 {
        "ipfs"
    } else {
        "storage"
    };

    // Try tiered storage first (if available)
    if let Some(ref tiered) = state.tiered_storage {
        if let Some(bytes) = tiered.get(&hash).await {
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "engine": engine,
                "content": content,
            })));
        }
    }

    // Try to get content from local storage
    if let Some(ref storage) = state.storage {
        if let Ok(bytes) = storage.get(&hash).await {
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "engine": engine,
                "content": content,
            })));
        }
    }

    // Try to get content from IPFS
    match state.ipfs.get(&hash).await {
        Ok(bytes) => {
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "engine": engine,
                "content": content,
            })))
        }
        Err(_) => {
            // If sharding is enabled, include responsible node hints
            let mut resp = json!({
                "status": "not_found",
                "hash": hash,
            });
            if let Some(ref tiered) = state.tiered_storage {
                if let Some(nodes) = tiered.get_responsible_nodes(&hash).await {
                    resp["responsible_nodes"] = json!(nodes);
                }
            }
            (StatusCode::NOT_FOUND, Json(resp))
        }
    }
}

/// Get programs for an address
pub async fn get_programs(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "programs": [],
        }));
    }
    
    let programs = sqlx::query_as::<_, crate::db::models::ProgramDb>(
        "SELECT * FROM programs WHERE owner = $1"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    Json(json!({
        "address": address,
        "programs": programs,
    }))
}

/// Get instances for an address
pub async fn get_instances(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "instances": [],
        }));
    }
    
    let instances = sqlx::query_as::<_, crate::db::models::InstanceDb>(
        "SELECT * FROM instances WHERE owner = $1"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    Json(json!({
        "address": address,
        "instances": instances,
    }))
}

/// Get pricing info - matches pyaleph format
pub async fn get_pricing(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let storage_price = state.cost.get_price(&ProductPriceType::Storage).await;
    let program_price = state.cost.get_price(&ProductPriceType::Program).await;
    let instance_price = state.cost.get_price(&ProductPriceType::Instance).await;
    let instance_gpu_premium = state.cost.get_price(&ProductPriceType::InstanceGpuPremium).await;
    let instance_gpu_standard = state.cost.get_price(&ProductPriceType::InstanceGpuStandard).await;
    let instance_confidential = state.cost.get_price(&ProductPriceType::InstanceConfidential).await;
    
    Json(json!({
        "pricing": {
            "storage": storage_price,
            "program": program_price,
            "instance": instance_price,
            "instance_gpu_premium": instance_gpu_premium,
            "instance_gpu_standard": instance_gpu_standard,
            "instance_confidential": instance_confidential,
        },
        "compute_unit": {
            "vcpus": 1,
            "memory_mib": 2048,
            "disk_mib": 20480,
        }
    }))
}

/// Get node info - matches pyaleph format
pub async fn get_info(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    Json(json!({
        "name": state.config.node.name,
        "version": env!("CARGO_PKG_VERSION"),
        "implementation": "aleph-core-rs",
        "api_version": "1.0",
        "status": {
            "database": state.has_db(),
            "ipfs": state.ipfs.is_connected().await,
            "p2p": state.p2p_connected,
        },
        "chains": {
            "ethereum": {
                "enabled": state.config.chains.ethereum.as_ref().map(|c| c.enabled).unwrap_or(false),
            }
        }
    }))
}

/// Estimate cost for a program or instance
#[derive(Debug, Deserialize)]
pub struct CostEstimateRequest {
    pub memory_mib: u32,
    pub vcpus: u32,
    pub storage_mib: u64,
    pub hours: u64,
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub product_type: Option<String>,
}

pub async fn estimate_cost(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CostEstimateRequest>,
) -> impl IntoResponse {
    let product_type = match request.product_type.as_deref() {
        Some("instance") => ProductPriceType::Instance,
        Some("instance_confidential") => ProductPriceType::InstanceConfidential,
        Some("instance_gpu_premium") => ProductPriceType::InstanceGpuPremium,
        Some("instance_gpu_standard") => ProductPriceType::InstanceGpuStandard,
        Some("program") | None => ProductPriceType::Program,
        _ => ProductPriceType::Instance,
    };
    
    let cost = state.cost.calculate_instance_cost(
        request.memory_mib,
        request.vcpus,
        request.storage_mib,
        request.hours,
        product_type,
        request.internet,
    ).await;
    
    match cost {
        Some(result) => Json(json!({
            "cost": {
                "holding": result.holding.to_string(),
                "payg": result.payg.to_string(),
                "credit": result.credit.to_string(),
            },
            "compute_units": state.cost.calculate_compute_units(request.memory_mib, request.vcpus),
            "storage_mib": request.storage_mib,
            "hours": request.hours,
        })),
        None => Json(json!({
            "error": "Unable to calculate cost",
            "message": "Unknown product type"
        })),
    }
}

/// POST /price/estimate — Estimate cost for a message dict
/// Accepts {"message": {...}} where message is a full Aleph message dict.
/// Parses item_content to extract resources (memory, vcpus, storage) and calculates cost.
/// Reference: aleph/web/controllers/prices.py:message_price_estimate
#[derive(Debug, Deserialize)]
pub struct PriceEstimateRequest {
    pub message: serde_json::Value,
}

pub async fn price_estimate(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PriceEstimateRequest>,
) -> impl IntoResponse {
    let msg = &request.message;

    // Determine message type
    let msg_type = msg.get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_uppercase();

    // Extract content: parse item_content string or use content field
    let content: serde_json::Value = if let Some(ic) = msg.get("item_content").and_then(|v| v.as_str()) {
        match serde_json::from_str(ic) {
            Ok(v) => v,
            Err(_) => {
                return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
                    "error": "Failed to parse item_content as JSON"
                })));
            }
        }
    } else if let Some(content_val) = msg.get("content") {
        content_val.clone()
    } else {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
            "error": "Message must contain item_content or content"
        })));
    };

    // Extract address for charged_address
    let charged_address = content.get("address")
        .and_then(|v| v.as_str())
        .or_else(|| msg.get("sender").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    // Extract payment type from content
    let payment_type_str = content.get("payment")
        .and_then(|p| p.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("hold");

    match msg_type.as_str() {
        "PROGRAM" | "INSTANCE" => {
            // Extract resources
            let resources = content.get("resources").unwrap_or(&serde_json::Value::Null);
            let memory = resources.get("memory")
                .and_then(|v| v.as_u64())
                .unwrap_or(2048) as u32;
            let vcpus = resources.get("vcpus")
                .and_then(|v| v.as_u64())
                .unwrap_or(1) as u32;

            // Calculate storage from volumes
            let mut total_storage_mib: u64 = if msg_type == "PROGRAM" { 20480 } else { 40960 };

            // Add persistent/ephemeral volumes
            if let Some(volumes) = content.get("volumes").and_then(|v| v.as_array()) {
                for vol in volumes {
                    if let Some(size) = vol.get("size_mib").and_then(|v| v.as_u64()) {
                        total_storage_mib += size;
                    } else if let Some(size) = vol.get("estimated_size_mib").and_then(|v| v.as_u64()) {
                        total_storage_mib += size;
                    }
                }
            }

            // Determine product type
            let is_confidential = content.get("trusted_execution").is_some()
                && !content.get("trusted_execution").unwrap().is_null();

            let product_type = if is_confidential {
                ProductPriceType::InstanceConfidential
            } else if msg_type == "PROGRAM" {
                ProductPriceType::Program
            } else {
                ProductPriceType::Instance
            };

            let internet_enabled = content.get("environment")
                .and_then(|e| e.get("internet"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let hours = 24 * 30; // Monthly cost estimate

            if let Some(cost) = state.cost.calculate_instance_cost(
                memory,
                vcpus,
                total_storage_mib,
                hours,
                product_type,
                internet_enabled,
            ).await {
                let required_tokens = match payment_type_str {
                    "credit" => cost.credit.to_string().parse::<f64>().unwrap_or(0.0),
                    "superfluid" | "stream" => cost.payg.to_string().parse::<f64>().unwrap_or(0.0),
                    _ => cost.holding.to_string().parse::<f64>().unwrap_or(0.0),
                };
                let compute_units = state.cost.calculate_compute_units(memory, vcpus);

                (StatusCode::OK, Json(json!({
                    "required_tokens": required_tokens,
                    "payment_type": payment_type_str,
                    "cost": format!("{:.6}", required_tokens),
                    "detail": [
                        {
                            "type": "compute",
                            "name": format!("{} compute units", compute_units),
                            "cost_hold": cost.holding.to_string(),
                            "cost_stream": cost.payg.to_string(),
                            "cost_credit": cost.credit.to_string()
                        }
                    ],
                    "charged_address": charged_address,
                })))
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "error": "Unable to calculate cost"
                })))
            }
        }
        "STORE" => {
            // For STORE messages, calculate storage cost
            let size_mib = content.get("size")
                .and_then(|v| v.as_u64())
                .or_else(|| content.get("estimated_size_mib").and_then(|v| v.as_u64()))
                .unwrap_or(1);

            let hours = 24 * 30;

            if let Some(cost) = state.cost.calculate_storage_cost(size_mib.max(1), hours, ProductPriceType::Storage).await {
                let required_tokens = cost.holding.to_string().parse::<f64>().unwrap_or(0.0);

                (StatusCode::OK, Json(json!({
                    "required_tokens": required_tokens,
                    "payment_type": "hold",
                    "cost": format!("{:.6}", required_tokens),
                    "detail": [
                        {
                            "type": "storage",
                            "name": format!("{} MiB", size_mib),
                            "cost_hold": cost.holding.to_string(),
                            "cost_stream": cost.payg.to_string(),
                            "cost_credit": cost.credit.to_string()
                        }
                    ],
                    "charged_address": charged_address,
                })))
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "error": "Unable to calculate storage cost"
                })))
            }
        }
        _ => {
            (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
                "error": format!("Invalid or unsupported message type: '{}'. Only PROGRAM, INSTANCE, and STORE are supported.", msg_type)
            })))
        }
    }
}

// ===== Additional Endpoints =====

/// Get message content — returns inner content.content for POST messages only
/// Reference: aleph/web/controllers/messages.py:view_message_content
pub async fn get_message_content(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    let message = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await;

    match message {
        Ok(Some(msg)) => {
            // Only POST messages have content via this endpoint
            if msg.message_type.to_uppercase() != "POST" {
                return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({
                    "error": format!("Invalid message hash type {} for hash {}", msg.message_type, hash)
                })));
            }

            // Get the item_content JSON string (inline or from IPFS)
            let content_str = match msg.item_content {
                Some(content) => content,
                None => {
                    // Content not inline, try to fetch from IPFS
                    match state.ipfs.get(&hash).await {
                        Ok(bytes) => match String::from_utf8(bytes) {
                            Ok(s) => s,
                            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                                "error": "Content is not valid UTF-8"
                            }))),
                        },
                        Err(e) => return (StatusCode::NOT_FOUND, Json(json!({
                            "error": "Content not found",
                            "message": e.to_string()
                        }))),
                    }
                }
            };

            // Parse the item_content and extract the inner "content" field
            match serde_json::from_str::<serde_json::Value>(&content_str) {
                Ok(parsed) => {
                    if let Some(inner_content) = parsed.get("content") {
                        (StatusCode::OK, Json(inner_content.clone()))
                    } else {
                        // Fallback: return the full parsed content if no inner "content" field
                        (StatusCode::OK, Json(parsed))
                    }
                }
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "error": "Failed to parse message content as JSON"
                }))),
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({
            "error": "Message not found"
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "error": e.to_string()
        }))),
    }
}

/// Get storage content raw
pub async fn get_storage_raw(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Try IPFS first
    match state.ipfs.get(&hash).await {
        Ok(bytes) => {
            (StatusCode::OK, bytes)
        }
        Err(e) => {
            (StatusCode::NOT_FOUND, format!("Content not found: {}", e).into_bytes())
        }
    }
}

/// Upload file to storage
#[derive(Debug, Deserialize)]
pub struct UploadQuery {
    #[serde(default)]
    pub sync: bool,
}

pub async fn upload_file(
    State(state): State<Arc<AppState>>,
    Query(_params): Query<UploadQuery>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Check file size
    let max_size = state.config.storage.max_unauthenticated_file_size;
    if body.len() as u64 > max_size {
        return (StatusCode::PAYLOAD_TOO_LARGE, Json(json!({
            "error": "File too large",
            "max_size": max_size,
        })));
    }
    
    // Upload to IPFS
    match state.ipfs.add(body.to_vec()).await {
        Ok(hash) => {
            let size = body.len();
            
            // Store file pin if we have DB
            if state.has_db() {
                let _ = sqlx::query(
                    "INSERT INTO file_pins (item_hash, owner, size, created_at) VALUES ($1, 'anonymous', $2, NOW()) ON CONFLICT DO NOTHING"
                )
                .bind(&hash)
                .bind(size as i64)
                .execute(state.db())
                .await;
            }
            
            (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
                "size": size,
            })))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "error": "Upload failed",
            "message": e.to_string(),
        }))),
    }
}

/// Get hashes endpoint
#[derive(Debug, Deserialize)]
pub struct HashesQuery {
    pub hashes: String,
}

/// Check which hashes exist in messages table
/// 
/// Uses parameterized queries to prevent SQL injection.
pub async fn get_hashes(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashesQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "hashes": {},
        }));
    }
    
    // Parse hashes safely
    let hash_list: Vec<String> = params.hashes
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && crate::utils::is_valid_hex(s))
        .collect();
    
    if hash_list.is_empty() {
        return Json(json!({ "hashes": {} }));
    }
    
    // Use ANY for safe parameterized IN clause
    let found: Vec<(String,)> = sqlx::query_as(
        "SELECT item_hash FROM messages WHERE item_hash = ANY($1)"
    )
    .bind(&hash_list)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let found_set: std::collections::HashSet<String> = found.into_iter().map(|r| r.0).collect();
    
    let mut result = serde_json::Map::new();
    for hash in &hash_list {
        result.insert(hash.clone(), json!(found_set.contains(hash)));
    }
    
    Json(json!({ "hashes": result }))
}

/// List all programs
pub async fn list_programs(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "programs": [],
            "pagination_total": 0,
        }));
    }
    
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20).min(100);
    let offset = ((page - 1) * per_page) as i64;
    
    let programs = sqlx::query_as::<_, crate::db::models::ProgramDb>(
        "SELECT * FROM programs ORDER BY created_at DESC LIMIT $1 OFFSET $2"
    )
    .bind(per_page as i64)
    .bind(offset)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM programs")
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    Json(json!({
        "programs": programs,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
    }))
}

/// List all instances
pub async fn list_instances(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "instances": [],
            "pagination_total": 0,
        }));
    }
    
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20).min(100);
    let offset = ((page - 1) * per_page) as i64;
    
    let instances = sqlx::query_as::<_, crate::db::models::InstanceDb>(
        "SELECT * FROM instances ORDER BY created_at DESC LIMIT $1 OFFSET $2"
    )
    .bind(per_page as i64)
    .bind(offset)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM instances")
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    Json(json!({
        "instances": instances,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
    }))
}

/// Get VM allocation status
pub async fn get_allocation(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "error": "Database not available"
        }));
    }
    
    // Check if it's a program
    let program = sqlx::query_as::<_, crate::db::models::ProgramDb>(
        "SELECT * FROM programs WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await;
    
    if let Ok(Some(prog)) = program {
        return Json(json!({
            "type": "program",
            "hash": hash,
            "owner": prog.owner,
            "allocated": true,
            "resources": {
                "memory": prog.memory,
                "vcpus": prog.vcpus,
            }
        }));
    }
    
    // Check if it's an instance
    let instance = sqlx::query_as::<_, crate::db::models::InstanceDb>(
        "SELECT * FROM instances WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await;
    
    if let Ok(Some(inst)) = instance {
        return Json(json!({
            "type": "instance",
            "hash": hash,
            "owner": inst.owner,
            "allocated": true,
            "resources": {
                "memory": inst.memory,
                "vcpus": inst.vcpus,
            }
        }));
    }
    
    Json(json!({
        "hash": hash,
        "allocated": false,
    }))
}

/// Get resource cost for a specific item
pub async fn get_resource_cost(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "error": "Database not available"
        }));
    }

    // Look up the message sender to find their account costs
    let msg = sqlx::query_as::<_, (String,)>(
        "SELECT sender FROM messages WHERE item_hash = $1"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    let address = match msg {
        Some((sender,)) => sender,
        None => {
            return Json(json!({
                "hash": hash,
                "storage_cost": "0",
                "compute_cost": "0",
                "total_cost": "0",
            }));
        }
    };

    // Query account-level costs
    let costs = sqlx::query_as::<_, crate::db::models::AccountCostDb>(
        "SELECT * FROM account_costs WHERE address = $1"
    )
    .bind(&address)
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten();

    match costs {
        Some(c) => Json(json!({
            "hash": hash,
            "storage_cost": c.storage_cost.to_string(),
            "compute_cost": c.compute_cost.to_string(),
            "total_cost": c.total_cost.to_string(),
        })),
        None => Json(json!({
            "hash": hash,
            "storage_cost": "0",
            "compute_cost": "0",
            "total_cost": "0",
        })),
    }
}

/// Get node statistics
pub async fn get_stats(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let mut stats = serde_json::Map::new();
    
    if state.has_db() {
        // Message counts by type
        let type_counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT message_type, COUNT(*) FROM messages GROUP BY message_type"
        )
        .fetch_all(state.db())
        .await
        .unwrap_or_default();
        
        let mut by_type = serde_json::Map::new();
        for (t, count) in type_counts {
            by_type.insert(t, json!(count));
        }
        stats.insert("messages_by_type".to_string(), json!(by_type));
        
        // Total counts
        let total_messages: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
        
        let pending_messages: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_messages")
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
        
        let file_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM file_pins")
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
        
        let storage_size: (i64,) = sqlx::query_as("SELECT COALESCE(SUM(size), 0) FROM file_pins")
            .fetch_one(state.db())
            .await
            .unwrap_or((0,));
        
        stats.insert("total_messages".to_string(), json!(total_messages.0));
        stats.insert("pending_messages".to_string(), json!(pending_messages.0));
        stats.insert("file_count".to_string(), json!(file_count.0));
        stats.insert("storage_bytes".to_string(), json!(storage_size.0));
    }
    
    stats.insert("uptime_secs".to_string(), json!(state.metrics.uptime_secs()));
    
    Json(json!(stats))
}

/// Get statistics for a specific address
pub async fn get_address_stats(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "error": "Database not available"
        }));
    }
    
    let message_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE sender = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((0,));
    
    let program_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM programs WHERE owner = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((0,));
    
    let instance_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM instances WHERE owner = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((0,));
    
    let file_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM file_pins WHERE owner = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((0,));
    
    Json(json!({
        "address": address,
        "messages": message_count.0,
        "programs": program_count.0,
        "instances": instance_count.0,
        "files": file_count.0,
    }))
}

/// Prometheus metrics endpoint
pub async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let metrics = state.metrics.prometheus_format().await;
    
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        metrics,
    )
}

/// Get detailed node status
pub async fn get_detailed_status(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db_status = if state.has_db() {
        let pool_status = state.db().size();
        json!({
            "connected": true,
            "pool_size": pool_status,
        })
    } else {
        json!({ "connected": false })
    };
    
    let ipfs_connected = state.ipfs.is_connected().await;
    
    let chain_status = if let Some(eth_config) = &state.config.chains.ethereum {
        json!({
            "ethereum": {
                "enabled": eth_config.enabled,
                "rpc_url": eth_config.rpc_url,
            }
        })
    } else {
        json!({})
    };
    
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.metrics.uptime_secs(),
        "database": db_status,
        "ipfs": {
            "connected": ipfs_connected,
            "api_url": state.config.ipfs.api_url,
        },
        "chains": chain_status,
        "p2p": {
            "connected": state.p2p_connected,
        },
        "config": {
            "node_name": state.config.node.name,
        }
    }))
}

/// Get chain sync status
pub async fn get_sync_status(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "error": "Database not available"
        }));
    }
    
    let sync_states: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT chain, sync_type, last_height FROM chain_sync_state"
    )
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let mut by_chain = serde_json::Map::new();
    for (chain, sync_type, height) in sync_states {
        let entry = by_chain.entry(chain).or_insert_with(|| json!({}));
        if let Some(obj) = entry.as_object_mut() {
            obj.insert(sync_type, json!(height));
        }
    }
    
    Json(json!({
        "chains": by_chain,
    }))
}

/// Get pending messages
pub async fn get_pending_messages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "pending": [],
            "total": 0,
        }));
    }
    
    let limit = params.pagination.unwrap_or(20).min(100) as i64;
    
    let pending = sqlx::query_as::<_, crate::db::models::PendingMessageDb>(
        "SELECT * FROM pending_messages ORDER BY reception_time DESC LIMIT $1"
    )
    .bind(limit)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_messages")
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    Json(json!({
        "pending": pending,
        "total": total.0,
    }))
}

/// Get cache statistics
pub async fn get_cache_stats(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Cache stats from metrics
    let snapshot = state.metrics.snapshot();
    
    Json(json!({
        "hits": snapshot.messages_processed, // Placeholder
        "misses": 0,
        "size": 0,
    }))
}

/// Debug config endpoint (should be protected)
pub async fn get_config_debug(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Only return non-sensitive config
    Json(json!({
        "node": {
            "name": state.config.node.name,
            "is_ccn": state.config.node.is_ccn,
        },
        "api": {
            "host": state.config.api.host,
            "port": state.config.api.port,
            "cors_enabled": state.config.api.cors_enabled,
        },
        "chains": {
            "ethereum_enabled": state.config.chains.ethereum.as_ref().map(|c| c.enabled).unwrap_or(false),
            "solana_enabled": state.config.chains.solana.as_ref().map(|c| c.enabled).unwrap_or(false),
        },
    }))
}

// ===== Balances List Endpoint =====

/// Query parameters for balances list
/// Reference: aleph/schemas/api/accounts.py:GetBalancesChainsQueryParams
#[derive(Debug, Deserialize)]
pub struct BalancesQuery {
    /// Comma-separated list of chains to filter by
    pub chains: Option<String>,
    /// Minimum balance required (as integer, will be compared to balance)
    pub min_balance: Option<i64>,
    /// Page size (default: 100)
    pub pagination: Option<u32>,
    /// Page number (default: 1)
    pub page: Option<u32>,
}

/// Balance item in response
/// Reference: aleph/schemas/api/accounts.py:AddressBalanceResponse
/// Python pyaleph uses FloatDecimal = Annotated[Decimal, PlainSerializer(lambda x: float(x))]
#[derive(Debug, Clone, Serialize)]
pub struct BalanceItem {
    pub address: String,
    pub chain: String,
    /// Balance as float (matching pyaleph FloatDecimal serialization)
    pub balance: f64,
}

/// Get list of balances - matches pyaleph /api/v0/balances format
///
/// Returns paginated list of balances with optional chain and min_balance filters.
/// Reference: aleph/web/controllers/accounts.py:get_chain_balances
pub async fn get_balances(
    State(state): State<Arc<AppState>>,
    Query(params): Query<BalancesQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.pagination.unwrap_or(100).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    
    if !state.has_db() {
        return Json(json!({
            "balances": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "balances",
            "error": "Database not available"
        }));
    }
    
    // Build the query with filters
    let mut builder = crate::db::QueryBuilder::new(
        "SELECT address, chain, balance FROM balances WHERE 1=1"
    );
    
    // Parse chains filter
    if let Some(ref chains) = params.chains {
        let chain_list = crate::db::parse_csv_param(chains);
        if !chain_list.is_empty() {
            builder.and_in("chain", &chain_list);
        }
    }
    
    // Add min_balance filter if specified
    if let Some(min_bal) = params.min_balance {
        if min_bal > 0 {
            builder.and_gte("balance", min_bal as f64);
        }
    }
    
    // Order by balance descending, then address for consistency
    builder.order_by("balance", false);
    builder.limit(per_page as i64);
    builder.offset(offset);
    
    // Build count query with same filters
    let mut count_builder = crate::db::QueryBuilder::new(
        "SELECT COUNT(*) FROM balances WHERE 1=1"
    );
    
    if let Some(ref chains) = params.chains {
        let chain_list = crate::db::parse_csv_param(chains);
        if !chain_list.is_empty() {
            count_builder.and_in("chain", &chain_list);
        }
    }
    
    if let Some(min_bal) = params.min_balance {
        if min_bal > 0 {
            count_builder.and_gte("balance", min_bal as f64);
        }
    }
    
    // Execute count query
    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    // Execute main query
    let (query, args) = builder.build();
    let balances: Vec<crate::db::models::BalanceDb> = 
        sqlx::query_as_with(&query, args)
            .fetch_all(state.db())
            .await
            .unwrap_or_default();
    
    // Convert to response format
    let balance_items: Vec<BalanceItem> = balances
        .into_iter()
        .map(|b| BalanceItem {
            address: b.address,
            chain: b.chain,
            balance: b.balance.to_string().parse::<f64>().unwrap_or(0.0),
        })
        .collect();
    
    Json(json!({
        "balances": balance_items,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
        "pagination_item": "balances",
    }))
}


/// Query parameters for listing aggregates
/// Reference: aleph/schemas/api/base.py:ListAggregateQueryParams
#[derive(Debug, Deserialize)]
pub struct ListAggregatesQuery {
    /// Filter by addresses (comma-separated)
    pub addresses: Option<String>,
    /// Filter by keys (comma-separated)
    pub keys: Option<String>,
    /// Items per page (default: 20, max: 500)
    pub limit: Option<u32>,
    /// Alias for limit (pyaleph compatibility)
    pub pagination: Option<u32>,
    /// Page number (1-indexed)
    pub page: Option<u32>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1, by last_updated)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    /// Sort by field (default: last_modified)
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
}

/// Aggregate item in list response
/// Reference: aleph/schemas/api/aggregates.py:AggregateListItemResponse  
#[derive(Debug, Clone, Serialize)]
pub struct AggregateListItem {
    pub address: String,
    pub key: String,
    pub content: serde_json::Value,
    /// ISO 8601 timestamp when aggregate was created
    pub created: String,
    /// ISO 8601 timestamp when aggregate was last updated
    pub last_updated: String,
}

/// List aggregates with filtering - matches pyaleph /api/v0/aggregates.json format
/// Reference: aleph/web/controllers/aggregates.py:list_aggregates_view
/// 
/// Supports filtering by addresses and keys, with pagination.
/// Returns aggregates with metadata (created, last_updated).
pub async fn list_aggregates(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListAggregatesQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1).max(1);
    // Support both limit and pagination params (limit takes precedence), max 500
    let per_page = params.limit.or(params.pagination).unwrap_or(20).min(500);
    let offset = ((page - 1) * per_page) as i64;
    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);

    if !state.has_db() {
        return Json(json!({
            "aggregates": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "aggregates",
            "error": "Database not available"
        }));
    }

    // Build query with filters — include dirty flag for lazy refresh
    let mut builder = crate::db::QueryBuilder::new(
        "SELECT a.address, a.key, a.content, a.time as created, \
         COALESCE(ae.time, a.time) as last_updated, a.dirty \
         FROM aggregates a \
         LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
         WHERE 1=1"
    );
    let mut count_builder = crate::db::QueryBuilder::new(
        "SELECT COUNT(*) FROM aggregates WHERE 1=1"
    );

    // Filter by addresses
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            builder.and_in("a.address", &addr_list);
            count_builder.and_in("address", &addr_list);
        }
    }

    // Filter by keys
    if let Some(ref keys) = params.keys {
        let key_list = crate::db::parse_csv_param(keys);
        if !key_list.is_empty() {
            builder.and_in("a.key", &key_list);
            count_builder.and_in("key", &key_list);
        }
    }

    // Determine sort column
    let sort_column = match params.sort_by.as_deref() {
        Some("creation_time") => "a.time",
        Some("last_modified") | _ => "COALESCE(ae.time, a.time)",
    };

    // Order and pagination
    let order_dir = if ascending { "ASC" } else { "DESC" };
    builder.and_raw(&format!(
        "1=1 ORDER BY {} {} LIMIT {} OFFSET {}",
        sort_column, order_dir, per_page, offset
    ));

    // Get total count
    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));

    // Get aggregates
    let (query, args) = builder.build();
    let aggregates: Vec<(String, String, serde_json::Value, f64, f64, bool)> =
        sqlx::query_as_with(&query, args)
            .fetch_all(state.db())
            .await
            .unwrap_or_default();

    // Lazy refresh: rebuild any dirty aggregates before returning
    let dirty_aggs: Vec<(String, String)> = aggregates.iter()
        .filter(|(_, _, _, _, _, dirty)| *dirty)
        .map(|(addr, key, _, _, _, _)| (addr.clone(), key.clone()))
        .collect();

    if !dirty_aggs.is_empty() {
        for (addr, key) in &dirty_aggs {
            if let Err(e) = crate::handlers::aggregate::AggregateHandler::rebuild_aggregate_from_elements(
                state.db(), addr, key,
            ).await {
                tracing::warn!("Failed to refresh dirty aggregate {}/{}: {}", addr, key, e);
            }
        }

        // Re-fetch after refresh (reuse same query shape without dirty column)
        let mut builder2 = crate::db::QueryBuilder::new(
            "SELECT a.address, a.key, a.content, a.time as created, \
             COALESCE(ae.time, a.time) as last_updated \
             FROM aggregates a \
             LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
             WHERE 1=1"
        );
        if let Some(ref addresses) = params.addresses {
            let addr_list = crate::db::parse_csv_param(addresses);
            if !addr_list.is_empty() { builder2.and_in("a.address", &addr_list); }
        }
        if let Some(ref keys) = params.keys {
            let key_list = crate::db::parse_csv_param(keys);
            if !key_list.is_empty() { builder2.and_in("a.key", &key_list); }
        }
        builder2.and_raw(&format!(
            "1=1 ORDER BY {} {} LIMIT {} OFFSET {}",
            sort_column, order_dir, per_page, offset
        ));
        let (q2, a2) = builder2.build();
        let refreshed: Vec<(String, String, serde_json::Value, f64, f64)> =
            sqlx::query_as_with(&q2, a2)
                .fetch_all(state.db())
                .await
                .unwrap_or_default();

        let aggregate_items: Vec<AggregateListItem> = refreshed
            .into_iter()
            .map(|(address, key, content, created, last_updated)| {
                let created_dt = chrono::DateTime::from_timestamp(created as i64, ((created.fract() * 1_000_000.0) as u32) * 1000)
                    .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                    .unwrap_or_else(|| format!("{}", created));
                let last_updated_dt = chrono::DateTime::from_timestamp(last_updated as i64, ((last_updated.fract() * 1_000_000.0) as u32) * 1000)
                    .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                    .unwrap_or_else(|| format!("{}", last_updated));
                AggregateListItem { address, key, content, created: created_dt, last_updated: last_updated_dt }
            })
            .collect();

        return Json(json!({
            "aggregates": aggregate_items,
            "pagination_per_page": per_page,
            "pagination_page": page,
            "pagination_total": total.0,
            "pagination_item": "aggregates",
        }));
    }

    // Format response with ISO timestamps
    let aggregate_items: Vec<AggregateListItem> = aggregates
        .into_iter()
        .map(|(address, key, content, created, last_updated, _dirty)| {
            // Convert Unix timestamps to ISO 8601 format
            let created_dt = chrono::DateTime::from_timestamp(created as i64, ((created.fract() * 1_000_000.0) as u32) * 1000)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                .unwrap_or_else(|| format!("{}", created));
            let last_updated_dt = chrono::DateTime::from_timestamp(last_updated as i64, ((last_updated.fract() * 1_000_000.0) as u32) * 1000)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                .unwrap_or_else(|| format!("{}", last_updated));

            AggregateListItem {
                address,
                key,
                content,
                created: created_dt,
                last_updated: last_updated_dt,
            }
        })
        .collect();

    Json(json!({
        "aggregates": aggregate_items,
        "pagination_per_page": per_page,
        "pagination_page": page,
        "pagination_total": total.0,
        "pagination_item": "aggregates",
    }))
}


// ===== Message Price Endpoint =====

/// Cost detail item in price response
/// Reference: aleph/schemas/api/costs.py:EstimatedCostDetailResponse
#[derive(Debug, Clone, Serialize)]
pub struct CostDetailItem {
    #[serde(rename = "type")]
    pub cost_type: String,
    pub name: String,
    pub cost_hold: String,
    pub cost_stream: String,
    pub cost_credit: String,
}

/// Message price response
/// Reference: aleph/schemas/api/costs.py:EstimatedCostsResponse
#[derive(Debug, Clone, Serialize)]
pub struct MessagePriceResponse {
    pub required_tokens: f64,
    pub payment_type: String,
    pub cost: String,
    pub detail: Vec<CostDetailItem>,
    pub charged_address: String,
}

/// Get price for an executable message (program or instance)
/// Reads from account_costs table first (matching pyaleph behavior), falls back to on-the-fly calculation.
/// Matches pyaleph /api/v0/price/{item_hash} format
/// Reference: aleph/web/controllers/prices.py:message_price
pub async fn get_message_price(
    State(state): State<Arc<AppState>>,
    Path(item_hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Check if message is pending first
    let pending = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM pending_messages WHERE item_hash = $1)"
    )
    .bind(&item_hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(false);

    if pending {
        // Return 102 Processing status (pyaleph compat: HTTPProcessing)
        return (StatusCode::PROCESSING, Json(json!({
            "error": "Message still pending",
            "item_hash": item_hash,
            "status": "pending"
        })));
    }

    // Check if message exists and get its type
    let message = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages WHERE item_hash = $1"
    )
    .bind(&item_hash)
    .fetch_optional(state.db())
    .await;

    match message {
        Ok(Some(msg)) => {
            let message_type = msg.message_type.to_uppercase();

            // Only executable messages (PROGRAM, INSTANCE) and STORE have prices
            if !matches!(message_type.as_str(), "PROGRAM" | "INSTANCE" | "STORE") {
                return (StatusCode::BAD_REQUEST, Json(json!({
                    "error": format!("Message is not an executable or store message: {}", item_hash)
                })));
            }

            // Calculate cost on-the-fly from message content
            match message_type.as_str() {
                "PROGRAM" => {
                    let program = sqlx::query_as::<_, crate::db::models::ProgramDb>(
                        "SELECT * FROM programs WHERE item_hash = $1"
                    )
                    .bind(&item_hash)
                    .fetch_optional(state.db())
                    .await;

                    match program {
                        Ok(Some(prog)) => {
                            let hours = 24 * 30;
                            let storage_mib = 20480u64;

                            if let Some(cost) = state.cost.calculate_instance_cost(
                                prog.memory as u32, prog.vcpus as u32,
                                storage_mib, hours, ProductPriceType::Program, false,
                            ).await {
                                let required_tokens = cost.holding.to_string().parse::<f64>().unwrap_or(0.0);
                                let compute_units = state.cost.calculate_compute_units(prog.memory as u32, prog.vcpus as u32);

                                (StatusCode::OK, Json(json!({
                                    "required_tokens": required_tokens,
                                    "payment_type": "hold",
                                    "cost": format!("{:.6}", required_tokens),
                                    "detail": [{
                                        "type": "compute",
                                        "name": format!("{} compute units", compute_units),
                                        "cost_hold": cost.holding.to_string(),
                                        "cost_stream": cost.payg.to_string(),
                                        "cost_credit": cost.credit.to_string()
                                    }],
                                    "charged_address": prog.owner,
                                })))
                            } else {
                                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                                    "error": "Unable to calculate cost"
                                })))
                            }
                        }
                        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({
                            "error": "Program not found in programs table",
                            "item_hash": item_hash
                        }))),
                        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                            "error": e.to_string()
                        }))),
                    }
                }
                "INSTANCE" => {
                    let instance = sqlx::query_as::<_, crate::db::models::InstanceDb>(
                        "SELECT * FROM instances WHERE item_hash = $1"
                    )
                    .bind(&item_hash)
                    .fetch_optional(state.db())
                    .await;

                    match instance {
                        Ok(Some(inst)) => {
                            let product_type = if inst.trusted_execution.is_some() {
                                ProductPriceType::InstanceConfidential
                            } else {
                                ProductPriceType::Instance
                            };
                            let payment_type = inst.payment_type.as_deref().unwrap_or("hold");
                            let hours = 24 * 30;
                            let storage_mib = 40960u64;

                            if let Some(cost) = state.cost.calculate_instance_cost(
                                inst.memory as u32, inst.vcpus as u32,
                                storage_mib, hours, product_type, false,
                            ).await {
                                let required_tokens = match payment_type {
                                    "credit" => cost.credit.to_string().parse::<f64>().unwrap_or(0.0),
                                    "superfluid" | "stream" => cost.payg.to_string().parse::<f64>().unwrap_or(0.0),
                                    _ => cost.holding.to_string().parse::<f64>().unwrap_or(0.0),
                                };
                                let compute_units = state.cost.calculate_compute_units(inst.memory as u32, inst.vcpus as u32);

                                (StatusCode::OK, Json(json!({
                                    "required_tokens": required_tokens,
                                    "payment_type": payment_type,
                                    "cost": format!("{:.6}", required_tokens),
                                    "detail": [{
                                        "type": "compute",
                                        "name": format!("{} compute units", compute_units),
                                        "cost_hold": cost.holding.to_string(),
                                        "cost_stream": cost.payg.to_string(),
                                        "cost_credit": cost.credit.to_string()
                                    }],
                                    "charged_address": inst.owner,
                                })))
                            } else {
                                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                                    "error": "Unable to calculate cost"
                                })))
                            }
                        }
                        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({
                            "error": "Instance not found in instances table",
                            "item_hash": item_hash
                        }))),
                        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                            "error": e.to_string()
                        }))),
                    }
                }
                "STORE" => {
                    let file_pin = sqlx::query_as::<_, (i64,)>(
                        "SELECT size FROM file_pins WHERE item_hash = $1"
                    )
                    .bind(&item_hash)
                    .fetch_optional(state.db())
                    .await;

                    let size_bytes = match file_pin {
                        Ok(Some((size,))) => size as u64,
                        _ => 0u64,
                    };
                    let size_mib = size_bytes / (1024 * 1024);
                    let hours = 24 * 30;

                    if let Some(cost) = state.cost.calculate_storage_cost(size_mib.max(1), hours, ProductPriceType::Storage).await {
                        let required_tokens = cost.holding.to_string().parse::<f64>().unwrap_or(0.0);

                        (StatusCode::OK, Json(json!({
                            "required_tokens": required_tokens,
                            "payment_type": "hold",
                            "cost": format!("{:.6}", required_tokens),
                            "detail": [{
                                "type": "storage",
                                "name": format!("{} MiB", size_mib),
                                "cost_hold": cost.holding.to_string(),
                                "cost_stream": cost.payg.to_string(),
                                "cost_credit": cost.credit.to_string()
                            }],
                            "charged_address": msg.sender,
                        })))
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                            "error": "Unable to calculate storage cost"
                        })))
                    }
                }
                _ => unreachable!(),
            }
        }
        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(json!({
                "error": format!("Message not found with hash: {}", item_hash)
            })))
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "error": e.to_string()
        }))),
    }
}

/// POST /price/recalculate — Force recalculation of message costs
/// Requires X-Auth-Token header for authentication.
/// Reference: aleph/web/controllers/prices.py:recalculate_message_costs
pub async fn recalculate_costs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Check auth token
    let auth_token = headers.get("x-auth-token")
        .and_then(|v| v.to_str().ok());

    if auth_token.is_none() {
        return (StatusCode::UNAUTHORIZED, Json(json!({
            "error": "Authentication required. Provide X-Auth-Token header."
        })));
    }

    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Stub implementation — full recalculation requires pricing timeline system
    (StatusCode::OK, Json(json!({
        "message": "Cost recalculation not yet fully implemented",
        "recalculated_count": 0,
        "total_messages": 0,
        "pricing_changes_found": 0
    })))
}

/// POST /price/:hash/recalculate — Force recalculation for a specific message
/// Requires X-Auth-Token header for authentication.
pub async fn recalculate_costs_for_hash(
    State(state): State<Arc<AppState>>,
    Path(item_hash): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Check auth token
    let auth_token = headers.get("x-auth-token")
        .and_then(|v| v.to_str().ok());

    if auth_token.is_none() {
        return (StatusCode::UNAUTHORIZED, Json(json!({
            "error": "Authentication required. Provide X-Auth-Token header."
        })));
    }

    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Verify message exists
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)"
    )
    .bind(&item_hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(false);

    if !exists {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": format!("Message not found with hash: {}", item_hash)
        })));
    }

    // Stub implementation — full recalculation requires pricing timeline system
    (StatusCode::OK, Json(json!({
        "message": "Cost recalculation not yet fully implemented",
        "recalculated_count": 0,
        "total_messages": 1,
        "item_hash": item_hash,
        "pricing_changes_found": 0
    })))
}

// ===== Address Stats Endpoint =====

/// Address stats item
#[derive(Debug, Clone, Serialize)]
pub struct AddressStatsItem {
    pub messages: i64,
    pub aggregate: i64,
    pub forget: i64,
    pub instance: i64,
    pub post: i64,
    pub program: i64,
    pub store: i64,
}

/// Get address statistics (v0) - matches pyaleph format
/// Returns message counts by type for specified addresses
/// Reference: aleph/web/controllers/accounts.py:addresses_stats_view_v0
pub async fn get_addresses_stats_v0(
    State(state): State<Arc<AppState>>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "data": {},
            "error": "Database not available"
        }));
    }
    
    // Parse addresses from query string (handles addresses[] format)
    let addresses: Vec<String> = raw_query
        .unwrap_or_default()
        .split('&')
        .filter_map(|param| {
            let mut parts = param.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            // Handle both encoded and non-encoded forms
            if key == "addresses%5B%5D" || key == "addresses[]" || key == "addresses" {
                Some(value.to_string())
            } else {
                None
            }
        })
        .collect();
    
    if addresses.is_empty() {
        return Json(json!({
            "data": {}
        }));
    }
    
    // Query message counts by type for each address
    let stats: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT sender, message_type, COUNT(*) as count FROM messages WHERE sender = ANY($1) GROUP BY sender, message_type"
    )
    .bind(&addresses)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    // Aggregate stats by address
    let mut data: HashMap<String, AddressStatsItem> = HashMap::new();
    
    for (sender, msg_type, count) in stats {
        let entry = data.entry(sender).or_insert_with(|| AddressStatsItem {
            messages: 0,
            aggregate: 0,
            forget: 0,
            instance: 0,
            post: 0,
            program: 0,
            store: 0,
        });
        
        match msg_type.to_uppercase().as_str() {
            "AGGREGATE" => entry.aggregate = count,
            "FORGET" => entry.forget = count,
            "INSTANCE" => entry.instance = count,
            "POST" => entry.post = count,
            "PROGRAM" => entry.program = count,
            "STORE" => entry.store = count,
            _ => {}
        }
        entry.messages += count;
    }
    
    Json(json!({
        "data": data
    }))
}

// ===== Address Files Endpoint =====

/// Query parameters for address files
/// Reference: aleph/web/controllers/accounts.py:get_account_files
#[derive(Debug, Deserialize)]
pub struct AddressFilesQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    #[serde(alias = "sortOrder")]
    pub sort_order: Option<i8>,
}

/// File item in response
#[derive(Debug, Clone, Serialize)]
pub struct FileItem {
    pub file_hash: String,
    pub created: String,
    pub item_hash: String,
    pub size: i64,
    #[serde(rename = "type")]
    pub file_type: Option<String>,
}

/// Get files for an address - matches pyaleph format
/// Reference: aleph/web/controllers/accounts.py:get_account_files
pub async fn get_address_files(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<AddressFilesQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.pagination.unwrap_or(100).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);
    
    // Get files from file_pins table
    let order_clause = if ascending { "ASC" } else { "DESC" };
    let query = format!(
        "SELECT item_hash, owner, size, content_type, created_at \
         FROM file_pins \
         WHERE owner = $1 \
         ORDER BY created_at {} \
         LIMIT $2 OFFSET $3",
        order_clause
    );
    
    let files: Vec<crate::db::models::FilePinDb> = sqlx::query_as(&query)
        .bind(&address)
        .bind(per_page as i64)
        .bind(offset)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();
    
    if files.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "No files found for this address"
        })));
    }
    
    // Get total count and size
    let stats: (i64, Option<i64>) = sqlx::query_as(
        "SELECT COUNT(*), SUM(size) FROM file_pins WHERE owner = $1"
    )
    .bind(&address)
    .fetch_one(state.db())
    .await
    .unwrap_or((0, None));
    
    let total_files = stats.0;
    let total_size = stats.1.unwrap_or(0);
    
    // Format response
    let file_items: Vec<FileItem> = files.into_iter().map(|f| FileItem {
        file_hash: f.item_hash.clone(),
        created: f.created_at.to_rfc3339(),
        item_hash: f.item_hash,
        size: f.size,
        file_type: f.content_type,
    }).collect();
    
    (StatusCode::OK, Json(json!({
        "address": address,
        "total_size": total_size,
        "files": file_items,
        "pagination_page": page,
        "pagination_total": total_files,
        "pagination_per_page": per_page,
    })))
}

// ===== Address Credit History Endpoint =====

/// Query parameters for credit history
/// Reference: aleph/web/controllers/accounts.py:get_account_credit_history
#[derive(Debug, Deserialize)]
pub struct CreditHistoryQuery {
    pub pagination: Option<u32>,
    pub page: Option<u32>,
    pub tx_hash: Option<String>,
    pub token: Option<String>,
    pub chain: Option<String>,
    pub provider: Option<String>,
    pub origin: Option<String>,
    pub origin_ref: Option<String>,
    pub payment_method: Option<String>,
}

/// Credit history item
#[derive(Debug, Clone, Serialize)]
pub struct CreditHistoryItem {
    pub amount: i64,
    pub price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bonus_amount: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment_method: Option<String>,
    pub credit_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<String>,
    pub message_timestamp: Option<String>,
}

/// Get credit history for an address - matches pyaleph format
/// Reference: aleph/web/controllers/accounts.py:get_account_credit_history
/// Note: Returns 404 if credit_history table does not exist
pub async fn get_address_credit_history(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<CreditHistoryQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }
    
    let page = params.page.unwrap_or(1).max(1);
    let raw_pagination = params.pagination.unwrap_or(0);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
    let offset = ((page - 1) * per_page) as i64;

    // Check if credit_history table exists
    let table_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'credit_history')"
    )
    .fetch_one(state.db())
    .await
    .unwrap_or((false,));

    if !table_exists.0 {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "Credit history not available (table does not exist)",
            "address": address
        })));
    }

    // Build dynamic WHERE clause with filters
    let mut where_clauses = vec!["address = $1".to_string()];
    let mut param_index = 1u32;

    // Collect filter values for binding
    let mut filter_values: Vec<String> = Vec::new();

    if let Some(ref v) = params.tx_hash {
        param_index += 1;
        where_clauses.push(format!("tx_hash = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.token {
        param_index += 1;
        where_clauses.push(format!("token = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.chain {
        param_index += 1;
        where_clauses.push(format!("chain = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.provider {
        param_index += 1;
        where_clauses.push(format!("provider = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.origin {
        param_index += 1;
        where_clauses.push(format!("origin = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.origin_ref {
        param_index += 1;
        where_clauses.push(format!("origin_ref = ${}", param_index));
        filter_values.push(v.clone());
    }
    if let Some(ref v) = params.payment_method {
        param_index += 1;
        where_clauses.push(format!("payment_method = ${}", param_index));
        filter_values.push(v.clone());
    }

    let where_sql = where_clauses.join(" AND ");
    let limit_param = param_index + 1;
    let offset_param = param_index + 2;

    let query_sql = format!(
        "SELECT amount, price, bonus_amount, tx_hash, token, chain, provider, origin, origin_ref, \
         payment_method, credit_ref, credit_index, expiration_date, message_timestamp \
         FROM credit_history WHERE {} ORDER BY message_timestamp DESC LIMIT ${} OFFSET ${}",
        where_sql, limit_param, offset_param
    );

    // Build and execute the query with dynamic bindings
    let mut query = sqlx::query_as::<_, (i64, Option<rust_decimal::Decimal>, Option<i64>, Option<String>, Option<String>,
                      Option<String>, Option<String>, Option<String>, Option<String>, Option<String>,
                      Option<String>, Option<i32>, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>)>(&query_sql)
        .bind(&address);

    for val in &filter_values {
        query = query.bind(val);
    }
    query = query.bind(per_page as i64).bind(offset);

    let history = query.fetch_all(state.db()).await.unwrap_or_default();

    if history.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "No credit history found for this address",
            "address": address
        })));
    }

    // Get total count with same filters
    let count_sql = format!(
        "SELECT COUNT(*) FROM credit_history WHERE {}",
        where_sql
    );

    let mut count_query = sqlx::query_as::<_, (i64,)>(&count_sql)
        .bind(&address);

    for val in &filter_values {
        count_query = count_query.bind(val);
    }

    let total: (i64,) = count_query.fetch_one(state.db()).await.unwrap_or((0,));

    // Format response
    let history_items: Vec<CreditHistoryItem> = history.into_iter().map(|h| CreditHistoryItem {
        amount: h.0,
        price: h.1.map(|p: rust_decimal::Decimal| p.to_string().parse::<f64>().unwrap_or(0.0)),
        bonus_amount: h.2,
        tx_hash: h.3,
        token: h.4,
        chain: h.5,
        provider: h.6,
        origin: h.7,
        origin_ref: h.8,
        payment_method: h.9,
        credit_ref: h.10,
        credit_index: h.11,
        expiration_date: h.12.map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339()),
        message_timestamp: h.13.map(|d: chrono::DateTime<chrono::Utc>| d.to_rfc3339()),
    }).collect();

    (StatusCode::OK, Json(json!({
        "address": address,
        "credit_history": history_items,
        "pagination_page": page,
        "pagination_total": total.0,
        "pagination_per_page": raw_pagination,
    })))
}

// ===== Storage Endpoints (File metadata by message hash and ref) =====

/// Store content from STORE message
#[derive(Debug, Deserialize)]
struct StoreContent {
    address: String,
    item_type: String,
    item_hash: String,
    time: f64,
    #[serde(default)]
    ref_: Option<String>,
    #[serde(rename = "ref")]
    #[serde(default)]
    ref_field: Option<String>,
}

impl StoreContent {
    fn get_ref(&self) -> Option<&str> {
        self.ref_field.as_deref().or(self.ref_.as_deref())
    }
}

/// File metadata response - matches pyaleph format
/// Reference: aleph/web/controllers/storage.py:FileMetadataResponse
#[derive(Debug, Serialize)]
pub struct FileMetadataResponse {
    #[serde(rename = "ref")]
    pub ref_: String,
    pub owner: String,
    pub file_hash: String,
    pub download_url: String,
    pub size: i64,
}

/// Add JSON to storage - matches pyaleph /api/v0/storage/add_json
/// Reference: aleph/web/controllers/storage.py:add_storage_json_controller
///
/// Accepts JSON body, hashes it with SHA256, stores to IPFS, returns hash.
pub async fn add_json_storage(
    State(state): State<Arc<AppState>>,
    Json(data): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Serialize JSON canonically
    let json_bytes = match serde_json::to_vec(&data) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(json!({
                "status": "error",
                "message": format!("Invalid JSON: {}", e)
            })));
        }
    };
    
    // Upload to IPFS
    match state.ipfs.add(json_bytes.clone()).await {
        Ok(hash) => {
            let size = json_bytes.len();
            
            // Store file pin if we have DB
            if state.has_db() {
                let _ = sqlx::query(
                    "INSERT INTO file_pins (item_hash, owner, size, content_type, created_at) \
                     VALUES ($1, 'anonymous', $2, 'application/json', NOW()) \
                     ON CONFLICT DO NOTHING"
                )
                .bind(&hash)
                .bind(size as i64)
                .execute(state.db())
                .await;
            }
            
            (StatusCode::OK, Json(json!({
                "status": "success",
                "hash": hash,
            })))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                "status": "error",
                "message": format!("Failed to store content: {}", e)
            })))
        }
    }
}

/// Get storage metadata by message hash - matches pyaleph /api/v0/storage/by-message-hash/{hash}
/// Reference: aleph/web/controllers/storage.py:get_file_metadata_by_message_hash
///
/// Returns the file metadata for a specific STORE message.
/// Avoids fetching the full message from the client to determine the file hash.
pub async fn get_storage_by_message_hash(
    State(state): State<Arc<AppState>>,
    Path(message_hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }
    
    // Get the STORE message
    let message = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages WHERE item_hash = $1 AND message_type = 'STORE'"
    )
    .bind(&message_hash)
    .fetch_optional(state.db())
    .await;
    
    match message {
        Ok(Some(msg)) => {
            // Parse the item_content to get file hash
            let content: StoreContent = match msg.item_content {
                Some(ref content_str) => {
                    match serde_json::from_str(content_str) {
                        Ok(c) => c,
                        Err(e) => {
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                                "error": format!("Failed to parse message content: {}", e)
                            })));
                        }
                    }
                }
                None => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                        "error": "Message has no inline content"
                    })));
                }
            };
            
            let file_hash = content.item_hash.clone();
            let owner = content.address.clone();
            let ref_ = content.get_ref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| message_hash.clone());
            
            // Try to get file size from file_pins or IPFS
            let size = sqlx::query_scalar::<_, i64>(
                "SELECT size FROM file_pins WHERE item_hash = $1 LIMIT 1"
            )
            .bind(&file_hash)
            .fetch_optional(state.db())
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
            
            (StatusCode::OK, Json(json!(FileMetadataResponse {
                ref_: ref_,
                owner,
                file_hash: file_hash.clone(),
                download_url: format!("/api/v0/storage/raw/{}", file_hash),
                size,
            })))
        }
        Ok(None) => {
            // Check if it exists but isn't a STORE message
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)"
            )
            .bind(&message_hash)
            .fetch_one(state.db())
            .await
            .unwrap_or(false);
            
            if exists {
                (StatusCode::BAD_REQUEST, Json(json!({
                    "error": "Message exists but is not a STORE message"
                })))
            } else {
                (StatusCode::NOT_FOUND, Json(json!({
                    "error": format!("No file found for message {}", message_hash)
                })))
            }
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                "error": e.to_string()
            })))
        }
    }
}

/// Get storage metadata by ref - matches pyaleph /api/v0/storage/by-ref/{ref}
/// Reference: aleph/web/controllers/storage.py:get_file_metadata_by_ref
///
/// Returns the latest version of a file using its ref.
/// Handles both /storage/by-ref/{address}/{ref} and /storage/by-ref/{item_hash}
pub async fn get_storage_by_ref(
    State(state): State<Arc<AppState>>,
    Path(ref_): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }
    
    // First, try to interpret ref as an item_hash (direct lookup)
    // Check if it looks like a hash (64 hex chars or starts with Qm for IPFS)
    let is_hash = (ref_.len() == 64 && ref_.chars().all(|c| c.is_ascii_hexdigit()))
        || ref_.starts_with("Qm");
    
    if is_hash {
        // Try direct file_tags lookup
        let file_tag = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT item_hash, tag FROM file_tags WHERE tag = $1 OR item_hash = $1 LIMIT 1"
        )
        .bind(&ref_)
        .fetch_optional(state.db())
        .await;
        
        match file_tag {
            Ok(Some((item_hash, _tag))) => {
                // Get file info
                let file_pin = sqlx::query_as::<_, (String, i64)>(
                    "SELECT owner, size FROM file_pins WHERE item_hash = $1 LIMIT 1"
                )
                .bind(&item_hash)
                .fetch_optional(state.db())
                .await
                .ok()
                .flatten();
                
                let (owner, size) = file_pin.unwrap_or(("unknown".to_string(), 0));
                
                return (StatusCode::OK, Json(json!(FileMetadataResponse {
                    ref_: ref_.clone(),
                    owner,
                    file_hash: item_hash.clone(),
                    download_url: format!("/api/v0/storage/raw/{}", item_hash),
                    size,
                })));
            }
            Ok(None) => {
                // Not found in file_tags, try finding a STORE message with this ref
                let store_msg = sqlx::query_as::<_, crate::db::models::MessageDb>(
                    "SELECT * FROM messages WHERE message_type = 'STORE' AND item_hash = $1"
                )
                .bind(&ref_)
                .fetch_optional(state.db())
                .await;
                
                if let Ok(Some(msg)) = store_msg {
                    if let Some(ref content_str) = msg.item_content {
                        if let Ok(content) = serde_json::from_str::<StoreContent>(content_str) {
                            let file_hash = content.item_hash;
                            let size = sqlx::query_scalar::<_, i64>(
                                "SELECT size FROM file_pins WHERE item_hash = $1"
                            )
                            .bind(&file_hash)
                            .fetch_optional(state.db())
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or(0);
                            
                            return (StatusCode::OK, Json(json!(FileMetadataResponse {
                                ref_: ref_.clone(),
                                owner: content.address,
                                file_hash: file_hash.clone(),
                                download_url: format!("/api/v0/storage/raw/{}", file_hash),
                                size,
                            })));
                        }
                    }
                }
            }
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "error": e.to_string()
                })));
            }
        }
    }
    
    // ref doesn't look like a hash - might be a custom ref, need address
    // For now, search in file_tags by tag
    let file_tag = sqlx::query_as::<_, (String,)>(
        "SELECT item_hash FROM file_tags WHERE tag = $1 LIMIT 1"
    )
    .bind(&ref_)
    .fetch_optional(state.db())
    .await;
    
    match file_tag {
        Ok(Some((item_hash,))) => {
            let file_pin = sqlx::query_as::<_, (String, i64)>(
                "SELECT owner, size FROM file_pins WHERE item_hash = $1 LIMIT 1"
            )
            .bind(&item_hash)
            .fetch_optional(state.db())
            .await
            .ok()
            .flatten();
            
            let (owner, size) = file_pin.unwrap_or(("unknown".to_string(), 0));
            
            (StatusCode::OK, Json(json!(FileMetadataResponse {
                ref_: ref_.clone(),
                owner,
                file_hash: item_hash.clone(),
                download_url: format!("/api/v0/storage/raw/{}", item_hash),
                size,
            })))
        }
        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(json!({
                "error": format!("No file found for tag {}", ref_)
            })))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                "error": e.to_string()
            })))
        }
    }
}

/// Get storage metadata by ref with address - matches pyaleph /api/v0/storage/by-ref/{address}/{ref}
pub async fn get_storage_by_address_ref(
    State(state): State<Arc<AppState>>,
    Path((address, ref_)): Path<(String, String)>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }
    
    // Build the tag as {address}/{ref}
    let tag = format!("{}/{}", address, ref_);
    
    let file_tag = sqlx::query_as::<_, (String,)>(
        "SELECT item_hash FROM file_tags WHERE tag = $1 LIMIT 1"
    )
    .bind(&tag)
    .fetch_optional(state.db())
    .await;
    
    match file_tag {
        Ok(Some((item_hash,))) => {
            let size = sqlx::query_scalar::<_, i64>(
                "SELECT size FROM file_pins WHERE item_hash = $1"
            )
            .bind(&item_hash)
            .fetch_optional(state.db())
            .await
            .ok()
            .flatten()
            .unwrap_or(0);
            
            (StatusCode::OK, Json(json!(FileMetadataResponse {
                ref_: ref_.clone(),
                owner: address,
                file_hash: item_hash.clone(),
                download_url: format!("/api/v0/storage/raw/{}", item_hash),
                size,
            })))
        }
        Ok(None) => {
            (StatusCode::NOT_FOUND, Json(json!({
                "error": format!("No file found for tag {}", tag)
            })))
        }
        Err(e) => {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                "error": e.to_string()
            })))
        }
    }
}

// ===== P3 HANDLERS =====

/// GET /api/v0/channels/list.json - List all channels
pub async fn list_channels(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let channels: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT channel FROM messages WHERE channel IS NOT NULL AND channel != '' ORDER BY channel"
    )
    .fetch_all(state.db())
    .await
    .unwrap_or_default();

    Json(json!({
        "channels": channels
    }))
}

/// GET /api/v0/version - API version info
pub async fn get_version() -> impl IntoResponse {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// GET /api/v0/info/public.json - Public node info (pyaleph compatible)
/// Reference: aleph/web/controllers/info.py:get_public_node_info
/// Returns P2P multiaddresses for the node (matches Python pyaleph format)
pub async fn get_public_info(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Build multiaddresses list from config
    // In production, this would come from the P2P node's actual multiaddresses
    let multiaddresses: Vec<String> = if state.config.p2p.enabled {
        // Return configured listen addresses (these should include full multiaddrs)
        state.config.p2p.listen_addrs.clone()
    } else {
        vec![]
    };
    
    Json(json!({
        "node_multi_addresses": multiaddresses
    }))
}

/// GET /api/v0/messages/page/{page}.json - Paginated messages
pub async fn list_messages_page(
    State(state): State<Arc<AppState>>,
    Path(page): Path<u32>,
    Query(mut params): Query<MessageQuery>,
) -> impl IntoResponse {
    params.page = Some(page);
    list_messages(State(state), Query(params)).await
}

/// GET /api/v1/posts/page/{page} - Paginated v1 posts
pub async fn list_posts_v1_page(
    State(state): State<Arc<AppState>>,
    Path(page): Path<u32>,
    Query(mut params): Query<PostsQuery>,
) -> impl IntoResponse {
    params.page = Some(page);
    get_posts_v1(State(state), Query(params)).await
}

/// GET /api/v0/posts/page/{page}.json - Paginated posts
pub async fn list_posts_page(
    State(state): State<Arc<AppState>>,
    Path(page): Path<u32>,
    Query(mut params): Query<PostsQuery>,
) -> impl IntoResponse {
    params.page = Some(page);
    get_posts(State(state), Query(params)).await
}

/// V1 post response — simpler format with ISO timestamps, no chain/signature/confirmations
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

/// GET /api/v1/posts - V1 posts endpoint with ISO timestamps and simplified fields
/// Reference: aleph/web/controllers/posts.py:view_posts_list_v1 + merged_post_to_dict
pub async fn get_posts_v1(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PostsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
    let offset = ((page - 1) * per_page) as i64;

    let order_param = params.order.or(params.sort_order);

    if !state.has_db() {
        return Json(json!({
            "posts": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "posts",
        }));
    }

    // V1 query: simpler than v0, no messages join needed
    // When tx-time sort is requested, add chain_txs LEFT JOIN
    let v1_base = if matches!(params.sort_by.as_deref(), Some("tx-time")) {
        "SELECT p.item_hash AS original_item_hash, \
         COALESCE(a.item_hash, p.item_hash) AS item_hash, \
         COALESCE(a.content, p.content) AS content, \
         p.address, \
         p.ref_, p.channel, \
         p.created_at AS created, \
         COALESCE(a.created_at, p.created_at) AS last_updated, \
         p.post_type AS original_type \
         FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         LEFT JOIN (SELECT item_hash, MIN(created_at) as earliest_confirmation \
         FROM chain_txs GROUP BY item_hash) ct \
         ON ct.item_hash = COALESCE(a.item_hash, p.item_hash) \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    } else {
        "SELECT p.item_hash AS original_item_hash, \
         COALESCE(a.item_hash, p.item_hash) AS item_hash, \
         COALESCE(a.content, p.content) AS content, \
         p.address, \
         p.ref_, p.channel, \
         p.created_at AS created, \
         COALESCE(a.created_at, p.created_at) AS last_updated, \
         p.post_type AS original_type \
         FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    };
    let mut builder = crate::db::QueryBuilder::new(v1_base);
    let mut count_builder = crate::db::QueryBuilder::new(
        "SELECT COUNT(*) FROM posts p \
         LEFT JOIN posts a ON p.latest_amend = a.item_hash \
         WHERE (p.amends IS NULL OR p.amends = '[]'::jsonb)"
    );

    // Filter by addresses
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            builder.and_in("p.address", &addr_list);
            count_builder.and_in("p.address", &addr_list);
        }
    }

    // Filter by channels
    if let Some(ref channels) = params.channels {
        let channel_list = crate::db::parse_csv_param(channels);
        if !channel_list.is_empty() {
            builder.and_in("p.channel", &channel_list);
            count_builder.and_in("p.channel", &channel_list);
        }
    }

    // Filter by post types
    if let Some(ref types) = params.types {
        let type_list = crate::db::parse_csv_param(types);
        if !type_list.is_empty() {
            builder.and_in("p.post_type", &type_list);
            count_builder.and_in("p.post_type", &type_list);
        }
    }

    // Filter by refs
    if let Some(ref refs) = params.refs {
        let ref_list = crate::db::parse_csv_param(refs);
        if !ref_list.is_empty() {
            builder.and_in("p.ref_", &ref_list);
            count_builder.and_in("p.ref_", &ref_list);
        }
    }

    // Filter by item hashes
    if let Some(ref hashes) = params.hashes {
        let hash_list = crate::db::parse_csv_param(hashes);
        if !hash_list.is_empty() {
            builder.and_in("p.item_hash", &hash_list);
            count_builder.and_in("p.item_hash", &hash_list);
        }
    }

    // Time filters - use COALESCE for date filtering
    if let Some(start) = params.start_date {
        builder.and_raw(&format!("COALESCE(a.time, p.time) >= {}", start));
        count_builder.and_raw(&format!("COALESCE(a.time, p.time) >= {}", start));
    }
    if let Some(end) = params.end_date {
        builder.and_raw(&format!("COALESCE(a.time, p.time) < {}", end));
        count_builder.and_raw(&format!("COALESCE(a.time, p.time) < {}", end));
    }

    // Filter by tags - OR logic matching pyaleph
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        if !tag_list.is_empty() {
            let keys_array: Vec<String> = tag_list.iter().map(|k| format!("'{}'", k.replace('\'', "''"))).collect();
            let clause = format!(
                "(COALESCE(a.content, p.content)->'tags') ?| ARRAY[{}]",
                keys_array.join(", ")
            );
            builder.and_raw(&clause);
            count_builder.and_raw(&clause);
        }
    }

    // Determine if v1 also needs tx-time
    let v1_needs_tx_time = matches!(params.sort_by.as_deref(), Some("tx-time"));

    // Sort column
    let sort_column = match params.sort_by.as_deref() {
        Some("time") | None => "COALESCE(a.created_at, p.created_at)".to_string(),
        Some("address") => "p.address".to_string(),
        Some("post_type") => "p.post_type".to_string(),
        Some("channel") => "p.channel".to_string(),
        Some("tx-time") => "ct.earliest_confirmation".to_string(),
        _ => "COALESCE(a.created_at, p.created_at)".to_string(),
    };

    let ascending = order_param.map(|o| o == 1).unwrap_or(false);
    let order_dir = if ascending { "ASC" } else { "DESC" };
    if v1_needs_tx_time {
        let nulls = if ascending { "NULLS LAST" } else { "NULLS FIRST" };
        builder.and_raw(&format!("1=1 ORDER BY {} {} {}, p.item_hash ASC LIMIT {} OFFSET {}", sort_column, order_dir, nulls, per_page, offset));
    } else {
        builder.and_raw(&format!("1=1 ORDER BY {} {}, p.item_hash ASC LIMIT {} OFFSET {}", sort_column, order_dir, per_page, offset));
    }

    // Get total count
    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));

    // V1 row type: no messages table fields
    type PostV1Row = (
        String,                                       // 0: original_item_hash
        String,                                       // 1: item_hash (coalesced)
        serde_json::Value,                            // 2: content (coalesced)
        String,                                       // 3: address
        Option<String>,                               // 4: ref_
        Option<String>,                               // 5: channel
        chrono::DateTime<chrono::Utc>,                // 6: created (original created_at)
        chrono::DateTime<chrono::Utc>,                // 7: last_updated (coalesced created_at)
        Option<String>,                               // 8: original_type
    );

    let (query, args) = builder.build();
    let rows: Vec<PostV1Row> = match sqlx::query_as_with(&query, args)
        .fetch_all(state.db())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("V1 posts query failed: {} | SQL: {}", e, query);
            vec![]
        }
    };

    let posts: Vec<PostResponseV1> = rows.iter().map(|row| {
        PostResponseV1 {
            item_hash: row.1.clone(),
            content: row.2.clone(),
            original_item_hash: row.0.clone(),
            original_type: row.8.clone(),
            address: row.3.clone(),
            ref_: row.4.clone(),
            channel: row.5.clone(),
            created: row.6.to_rfc3339(),
            last_updated: row.7.to_rfc3339(),
        }
    }).collect();

    Json(json!({
        "posts": posts,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
        "pagination_item": "posts",
    }))
}

/// GET /metrics - Prometheus metrics
pub async fn get_metrics(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Use pg_class for fast approximate counts (avoid full table scans)
    let messages_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'messages'"
    ).fetch_one(state.db()).await.unwrap_or(0);

    let posts_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'posts'"
    ).fetch_one(state.db()).await.unwrap_or(0);

    let aggregates_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'aggregates'"
    ).fetch_one(state.db()).await.unwrap_or(0);

    let metrics = format!(
        "# HELP aleph_messages_total Total messages in database\n# TYPE aleph_messages_total gauge\naleph_messages_total {}\n# HELP aleph_posts_total Total posts in database\n# TYPE aleph_posts_total gauge\naleph_posts_total {}\n# HELP aleph_aggregates_total Total aggregates in database\n# TYPE aleph_aggregates_total gauge\naleph_aggregates_total {}\n",
        messages_count, posts_count, aggregates_count
    );

    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics
    )
}

/// Get detailed monitoring stats for dashboard
pub async fn get_monitor_stats(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "error": "Database not available",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }));
    }
    
    let db = state.db();
    
    // Core counts
    let pending: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_messages")
        .fetch_one(db).await.unwrap_or((0,));
    
    let rejected: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'rejected_messages'"
    ).fetch_one(db).await.unwrap_or((0,));
    
    // Use pg_class for fast approximate counts on large tables
    let posts: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'posts'"
    ).fetch_one(db).await.unwrap_or((0,));

    let aggregates: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'aggregates'"
    ).fetch_one(db).await.unwrap_or((0,));

    let programs: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'programs'"
    ).fetch_one(db).await.unwrap_or((0,));

    let instances: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'instances'"
    ).fetch_one(db).await.unwrap_or((0,));

    let files: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'file_pins'"
    ).fetch_one(db).await.unwrap_or((0,));

    // Messages by type - use the index on message_type for a faster breakdown
    // (still not instant on huge tables, but idx_messages_type helps)
    let by_type: Vec<(String, i64)> = sqlx::query_as(
        "SELECT message_type, COUNT(*) FROM messages GROUP BY message_type ORDER BY COUNT(*) DESC"
    ).fetch_all(db).await.unwrap_or_default();
    
    // Recent processing rate - messages added in last 5 minutes
    let recent_rate: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE created_at > now() - interval '5 minutes'"
    ).fetch_one(db).await.unwrap_or((0,));
    
    let msgs_per_sec = recent_rate.0 as f64 / 300.0;
    
    // Estimate time to drain pending queue (in seconds)
    let eta_seconds = if msgs_per_sec > 0.0 {
        Some((pending.0 as f64 / msgs_per_sec) as i64)
    } else {
        None
    };
    
    // Format ETA as human readable
    let eta_human = eta_seconds.map(|s| {
        if s < 60 { format!("{}s", s) }
        else if s < 3600 { format!("{}m {}s", s / 60, s % 60) }
        else if s < 86400 { format!("{}h {}m", s / 3600, (s % 3600) / 60) }
        else { format!("{}d {}h", s / 86400, (s % 86400) / 3600) }
    });
    
    let mut types_map = serde_json::Map::new();
    let mut messages_total: i64 = 0;
    for (t, count) in by_type {
        messages_total += count;
        types_map.insert(t, json!(count));
    }

    // Content fetch stats — single query with conditional counts (uses idx_messages_item_type)
    let content_stats: (i64, i64, i64) = sqlx::query_as(
        r#"SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE item_content IS NULL) AS missing,
            COUNT(*) FILTER (WHERE item_content = '') AS unfetchable
        FROM messages WHERE item_type IN ('storage', 'ipfs')"#
    ).fetch_one(db).await.unwrap_or((0, 0, 0));

    let total_non_inline = content_stats.0;
    let missing_content = content_stats.1;
    let marked_unfetchable = content_stats.2;
    let fetched_content = total_non_inline - missing_content - marked_unfetchable;
    let fetch_pct = if total_non_inline > 0 {
        (fetched_content as f64 / total_non_inline as f64 * 100.0)
    } else { 0.0 };

    // Peer stats
    let online_peers: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM peers WHERE peer_type = 'HTTP' AND last_seen > now() - interval '5 minutes'"
    ).fetch_one(db).await.unwrap_or((0,));
    
    Json(json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "uptime_secs": state.metrics.uptime_secs(),
        "counts": {
            "pending_messages": pending.0,
            "messages": messages_total,
            "rejected_messages": rejected.0,
            "posts": posts.0,
            "aggregates": aggregates.0,
            "programs": programs.0,
            "instances": instances.0,
            "files": files.0
        },
        "messages_by_type": types_map,
        "processing": {
            "rate_per_sec": msgs_per_sec,
            "rate_per_min": msgs_per_sec * 60.0,
            "recent_5min": recent_rate.0,
            "eta_seconds": eta_seconds,
            "eta_human": eta_human
        },
        "content_fetch": {
            "total_non_inline": total_non_inline,
            "fetched": fetched_content,
            "missing": missing_content,
            "unfetchable": marked_unfetchable,
            "percent_complete": (fetch_pct * 10.0).round() / 10.0,
            "online_peers": online_peers.0
        }
    }))
}

/// Serve the monitor dashboard
pub async fn monitor_html() -> impl axum::response::IntoResponse {
    let html = include_str!("../../static/monitor.html");
    axum::response::Html(html)
}

// ===== P2 Missing Endpoints =====

/// GET /api/v0/storage/count/{hash} - Get count of nodes storing a file
/// Reference: aleph/web/controllers/storage.py:get_file_pins_count
///
/// Returns the number of nodes that have pinned/stored a file as a plain JSON integer.
pub async fn get_storage_count(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    let mut count = 0i64;

    // Query count from file_pins table
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

    // Also check IPFS if not found in DB
    if count == 0 {
        if state.ipfs.exists(&hash).await {
            count = 1;
        }
    }

    // Return plain JSON integer to match pyaleph format
    Json(json!(count))
}

/// GET /api/v0/addresses/{address}/post_types - Get post types used by an address
/// Reference: aleph/web/controllers/accounts.py:get_address_post_types
/// 
/// Returns list of distinct post types that an address has used.
pub async fn get_address_post_types(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "post_types": []
        }));
    }
    
    // Query distinct post types from messages table (matching pyaleph: content->'type')
    let post_types: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT item_content::jsonb->>'type' AS post_type \
         FROM messages \
         WHERE sender = $1 AND message_type = 'POST' \
         AND item_content IS NOT NULL AND item_content != '' \
         AND (item_content::jsonb->>'type') IS NOT NULL \
         ORDER BY post_type"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();

    let types: Vec<String> = post_types.into_iter().map(|(t,)| t).collect();
    
    Json(json!({
        "address": address,
        "post_types": types
    }))
}

/// GET /api/v0/addresses/{address}/channels - Get channels used by an address
/// Reference: aleph/web/controllers/accounts.py:get_address_channels
/// 
/// Returns list of distinct channels that an address has posted to.
pub async fn get_address_channels(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "channels": []
        }));
    }
    
    // Query distinct channels from messages table for this sender
    let channels: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT channel FROM messages WHERE sender = $1 AND channel IS NOT NULL AND channel != '' ORDER BY channel"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let channel_list: Vec<String> = channels.into_iter().map(|(c,)| c).collect();
    
    Json(json!({
        "address": address,
        "channels": channel_list
    }))
}

/// Query parameters for v1 address stats
#[derive(Debug, Deserialize)]
pub struct AddressStatsV1Query {
    /// Items per page (default: 20, max: 1000)
    pub pagination: Option<u32>,
    /// Page number (1-indexed)
    pub page: Option<u32>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    /// Filter addresses containing this substring (case-insensitive, max 66 chars)
    #[serde(rename = "addressContains")]
    pub address_contains: Option<String>,
    /// Sort by message type count: "post", "aggregate", "store", "program", "instance", "forget"
    /// Default: total messages
    #[serde(rename = "sortBy")]
    pub sort_by: Option<String>,
}

/// GET /api/v1/addresses/stats.json - Get paginated address statistics
/// Reference: aleph/web/controllers/accounts.py:addresses_stats_view_v1
/// 
/// Returns paginated list of addresses with message counts by type.
/// v1 adds pagination support compared to v0.
pub async fn get_addresses_stats_v1(
    State(state): State<Arc<AppState>>,
    Query(params): Query<AddressStatsV1Query>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "data": {},
            "pagination_total": 0,
            "pagination_page": 1,
            "pagination_per_page": 20,
            "pagination_item": "addresses",
            "error": "Database not available"
        }));
    }
    
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.pagination.unwrap_or(20).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);

    // Build WHERE clause for optional address filter (parameterized to prevent SQL injection)
    let address_search = params.address_contains.as_ref().map(|search| {
        // Limit search string to 66 chars (max address length)
        let truncated = &search[..search.len().min(66)];
        // Escape LIKE wildcards in user input
        format!("%{}%", truncated.replace('%', "\\%").replace('_', "\\_"))
    });

    // Get total unique senders count (with filter)
    let total: (i64,) = if let Some(ref pattern) = address_search {
        sqlx::query_as("SELECT COUNT(DISTINCT sender) FROM messages WHERE sender ILIKE $1")
            .bind(pattern)
            .fetch_one(state.db())
            .await
            .unwrap_or((0,))
    } else {
        sqlx::query_as("SELECT COUNT(DISTINCT sender) FROM messages")
            .fetch_one(state.db())
            .await
            .unwrap_or((0,))
    };

    // Determine sort column based on sortBy parameter
    let sort_column = match params.sort_by.as_deref() {
        Some("post") => "SUM(CASE WHEN message_type = 'POST' THEN 1 ELSE 0 END)".to_string(),
        Some("aggregate") => "SUM(CASE WHEN message_type = 'AGGREGATE' THEN 1 ELSE 0 END)".to_string(),
        Some("store") => "SUM(CASE WHEN message_type = 'STORE' THEN 1 ELSE 0 END)".to_string(),
        Some("program") => "SUM(CASE WHEN message_type = 'PROGRAM' THEN 1 ELSE 0 END)".to_string(),
        Some("instance") => "SUM(CASE WHEN message_type = 'INSTANCE' THEN 1 ELSE 0 END)".to_string(),
        Some("forget") => "SUM(CASE WHEN message_type = 'FORGET' THEN 1 ELSE 0 END)".to_string(),
        _ => "COUNT(*)".to_string(),
    };

    // Query addresses with their message counts (parameterized)
    let order_clause = if ascending { "ASC" } else { "DESC" };

    let addresses: Vec<(String, i64)> = if let Some(ref pattern) = address_search {
        let query = format!(
            "SELECT sender, COUNT(*) as total_messages \
             FROM messages WHERE sender ILIKE $1 \
             GROUP BY sender \
             ORDER BY {} {} \
             LIMIT $2 OFFSET $3",
            sort_column, order_clause
        );
        sqlx::query_as(&query)
            .bind(pattern)
            .bind(per_page as i64)
            .bind(offset)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    } else {
        let query = format!(
            "SELECT sender, COUNT(*) as total_messages \
             FROM messages \
             GROUP BY sender \
             ORDER BY {} {} \
             LIMIT $1 OFFSET $2",
            sort_column, order_clause
        );
        sqlx::query_as(&query)
            .bind(per_page as i64)
            .bind(offset)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    };
    
    if addresses.is_empty() {
        return Json(json!({
            "data": {},
            "pagination_total": total.0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "pagination_item": "addresses"
        }));
    }
    
    // Get address list for detailed stats query
    let addr_list: Vec<String> = addresses.iter().map(|(a, _)| a.clone()).collect();
    
    // Query message counts by type for these addresses
    let stats: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT sender, message_type, COUNT(*) as count \
         FROM messages \
         WHERE sender = ANY($1) \
         GROUP BY sender, message_type"
    )
    .bind(&addr_list)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    // Aggregate stats by address
    let mut data: HashMap<String, AddressStatsItem> = HashMap::new();
    
    // Initialize all addresses with zero counts
    for (sender, total) in &addresses {
        data.insert(sender.clone(), AddressStatsItem {
            messages: *total,
            aggregate: 0,
            forget: 0,
            instance: 0,
            post: 0,
            program: 0,
            store: 0,
        });
    }
    
    // Fill in the type-specific counts
    for (sender, msg_type, count) in stats {
        if let Some(entry) = data.get_mut(&sender) {
            match msg_type.to_uppercase().as_str() {
                "AGGREGATE" => entry.aggregate = count,
                "FORGET" => entry.forget = count,
                "INSTANCE" => entry.instance = count,
                "POST" => entry.post = count,
                "PROGRAM" => entry.program = count,
                "STORE" => entry.store = count,
                _ => {}
            }
        }
    }
    
    Json(json!({
        "data": data,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
        "pagination_item": "addresses"
    }))
}

// ============================================================================
// Task 21: GET /api/v0/messages/hashes
// ============================================================================

/// Query parameters for message hashes endpoint
#[derive(Debug, Deserialize)]
pub struct MessageHashesQuery {
    /// Message status filter (not used for messages table which is always processed)
    pub status: Option<String>,
    /// Page number (1-based)
    pub page: Option<u32>,
    /// Items per page
    pub pagination: Option<u32>,
    /// Start time filter (Unix timestamp)
    #[serde(alias = "startDate")]
    pub start_date: Option<f64>,
    /// End time filter (Unix timestamp)
    #[serde(alias = "endDate")]
    pub end_date: Option<f64>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
    /// If true, return only hashes without pagination info
    pub hash_only: Option<bool>,
}

/// Get message hashes with optional date filters and pagination
pub async fn get_message_hashes(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageHashesQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "hashes": [],
            "pagination_per_page": 0,
            "pagination_page": 1,
            "pagination_total": 0,
            "pagination_item": "hashes"
        }));
    }

    let page = params.page.unwrap_or(1).max(1);
    let raw_hashes_pagination = params.pagination.unwrap_or(20);
    let hashes_unlimited = raw_hashes_pagination == 0;
    let per_page = if hashes_unlimited { 0 } else { raw_hashes_pagination.min(1000) };
    let offset = if hashes_unlimited { 0 } else { ((page - 1) * per_page) as i64 };
    let ascending = params.sort_order.map(|o| o == 1).unwrap_or(false);

    // Build query
    let mut builder = crate::db::QueryBuilder::new("SELECT item_hash FROM messages WHERE 1=1");

    if let Some(start) = params.start_date {
        builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lt("time", end);
    }

    // Secondary sort on item_hash for determinism
    if ascending {
        builder.order_by_raw("time ASC, item_hash ASC");
    } else {
        builder.order_by_raw("time DESC, item_hash ASC");
    }
    if !hashes_unlimited {
        builder.limit(per_page as i64);
        builder.offset(offset);
    }

    // Count query
    let mut count_builder = crate::db::QueryBuilder::new("SELECT COUNT(*) FROM messages WHERE 1=1");
    if let Some(start) = params.start_date {
        count_builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        count_builder.and_lt("time", end);
    }

    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));

    let (query, args) = builder.build();
    let hashes: Vec<(String,)> = sqlx::query_as_with(&query, args)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();

    let hash_list: Vec<String> = hashes.into_iter().map(|(h,)| h).collect();

    Json(json!({
        "hashes": hash_list,
        "pagination_per_page": per_page,
        "pagination_page": page,
        "pagination_total": total.0,
        "pagination_item": "hashes"
    }))
}

// ============================================================================
// Task 22: POST /ipfs/pubsub/pub and /p2p/pubsub/pub
// ============================================================================

/// Request body for pubsub publish
#[derive(Debug, Deserialize)]
pub struct PubSubPublishRequest {
    pub topic: String,
    pub data: String,
}

/// Publish a message via pubsub (RabbitMQ)
pub async fn pub_json(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PubSubPublishRequest>,
) -> impl IntoResponse {
    // Validate topic matches configured queue topic
    if body.topic != state.config.p2p.topic {
        return (StatusCode::FORBIDDEN, Json(json!({
            "status": "error",
            "failed": [format!("Invalid topic: {}", body.topic)]
        })));
    }

    // Validate data is valid JSON
    if serde_json::from_str::<serde_json::Value>(&body.data).is_err() {
        return (StatusCode::BAD_REQUEST, Json(json!({
            "status": "error",
            "failed": ["Data is not valid JSON"]
        })));
    }

    // Publish to RabbitMQ if available
    if let Some(ref rabbitmq) = state.rabbitmq {
        let rmq = rabbitmq.read().await;
        if rmq.is_connected() {
            // Parse the data as a message and publish to the p2p network
            match serde_json::from_str::<crate::types::Message>(&body.data) {
                Ok(message) => {
                    if let Err(e) = rmq.publish_to_network(&message).await {
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                            "status": "error",
                            "failed": [format!("Failed to publish: {}", e)]
                        })));
                    }
                }
                Err(_) => {
                    // Data is valid JSON but not a valid message - still OK for pubsub
                    // Just skip the RabbitMQ publish
                }
            }
        }
    }

    (StatusCode::OK, Json(json!({
        "status": "success",
        "failed": []
    })))
}

// ============================================================================
// Task 23: POST /ipfs/add_file
// ============================================================================

/// Add a file to IPFS via multipart upload
pub async fn ipfs_add_file(
    State(state): State<Arc<AppState>>,
    mut multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    // Find the "file" field
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or_default().to_string();
        if name != "file" {
            continue;
        }

        let file_name = field.file_name().unwrap_or("unknown").to_string();

        // Read the file content
        let data = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(json!({
                    "status": "error",
                    "message": format!("Failed to read file: {}", e)
                })));
            }
        };

        let size = data.len();

        // Upload to IPFS
        match state.ipfs.add_with_details(data).await {
            Ok(add_response) => {
                return (StatusCode::OK, Json(json!({
                    "status": "success",
                    "hash": add_response.hash,
                    "name": file_name,
                    "size": size,
                })));
            }
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                    "status": "error",
                    "message": format!("IPFS upload failed: {}", e)
                })));
            }
        }
    }

    // No file field found
    (StatusCode::BAD_REQUEST, Json(json!({
        "status": "error",
        "message": "No 'file' field in multipart form"
    })))
}

// ============================================================================
// Task 24: GET /programs/on/message
// ============================================================================

/// Query parameters for programs on message endpoint
#[derive(Debug, Deserialize)]
pub struct ProgramsOnMessageQuery {
    /// Sort order: 1 for ascending, -1 for descending (default: -1)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
}

/// Get programs that have an on.message trigger
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

    let results: Vec<serde_json::Value> = rows
        .into_iter()
        .filter_map(|(item_hash, item_content)| {
            let content: serde_json::Value = item_content
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(serde_json::Value::Null);

            // Extract the on.message field
            let on_message = content.get("on")?.get("message")?;

            Some(json!({
                "item_hash": item_hash,
                "content": {
                    "on": {
                        "message": on_message
                    }
                }
            }))
        })
        .collect();

    Json(json!(results))
}

// ===== Consumed Credits Endpoint =====

/// Get consumed credits for a resource (message)
/// Reference: aleph/web/controllers/accounts.py:get_resource_consumed_credits_controller
///
/// Returns the total credits consumed by a specific resource (item_hash).
/// Aggregates credit_history entries where payment_method = 'credit_expense' and origin = item_hash.
pub async fn get_consumed_credits(
    State(state): State<Arc<AppState>>,
    Path(item_hash): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "item_hash": item_hash,
            "consumed_credits": 0
        }));
    }

    // Check if credit_history table exists
    let table_exists: (bool,) = match sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'credit_history')"
    )
    .fetch_one(state.db())
    .await {
        Ok(r) => r,
        Err(_) => {
            return Json(json!({
                "item_hash": item_hash,
                "consumed_credits": 0
            }));
        }
    };

    if !table_exists.0 {
        return Json(json!({
            "item_hash": item_hash,
            "consumed_credits": 0
        }));
    }

    // Sum absolute amounts where payment_method = 'credit_expense' and origin = item_hash
    let consumed: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(amount)), 0)::bigint FROM credit_history \
         WHERE payment_method = 'credit_expense' AND origin = $1"
    )
    .bind(&item_hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(0);

    Json(json!({
        "item_hash": item_hash,
        "consumed_credits": consumed
    }))
}

// ===== Metrics JSON Endpoint =====

/// JSON metrics endpoint - matches pyaleph /metrics.json
/// Reference: aleph/web/controllers/main.py:metrics_json
///
/// Returns pyaleph-compatible metrics in JSON format with message counts,
/// file counts, pending message counts, and build info.
pub async fn metrics_json(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let version = env!("CARGO_PKG_VERSION").to_string();

    if !state.has_db() {
        return Json(json!({
            "pyaleph_build_info": {"version": version},
            "pyaleph_status_peers_total": 0,
            "pyaleph_status_sync_messages_total": 0,
            "pyaleph_status_sync_permanent_files_total": 0,
            "pyaleph_status_sync_pending_messages_total": 0,
            "pyaleph_status_sync_pending_txs_total": 0
        }));
    }

    // Use estimated counts from pg_class for performance (avoids full table scans)
    let messages_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'messages'"
    )
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    let files_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'file_pins'"
    )
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    let pending_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'pending_messages'"
    )
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    let pending_txs_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'pending_txs'"
    )
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    let peers_count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'peers'"
    )
    .fetch_optional(state.db())
    .await
    .ok()
    .flatten()
    .unwrap_or(0);

    Json(json!({
        "pyaleph_build_info": {"version": version},
        "pyaleph_status_peers_total": peers_count,
        "pyaleph_status_sync_messages_total": messages_count,
        "pyaleph_status_sync_permanent_files_total": files_count,
        "pyaleph_status_sync_pending_messages_total": pending_count,
        "pyaleph_status_sync_pending_txs_total": pending_txs_count
    }))
}

// ===== CCN/CRN Node Metrics Endpoints =====

/// Query parameters for CCN/CRN node metrics
/// Reference: aleph/web/controllers/main.py:MetricsQueryParams
#[derive(Debug, Deserialize)]
pub struct NodeMetricsQuery {
    pub start_date: Option<f64>,
    pub end_date: Option<f64>,
    pub sort: Option<String>,
}

/// Get CCN (Core Channel Node) metrics for a specific node
/// Reference: aleph/web/controllers/main.py:ccn_metric
///
/// Queries the ccn_metric_view (database view over scoring messages)
/// for a specific node_id. Returns time-series metrics data.
pub async fn get_ccn_metrics(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(params): Query<NodeMetricsQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Check if ccn_metric_view exists
    let view_exists: (bool,) = match sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'ccn_metric_view')"
    )
    .fetch_one(state.db())
    .await {
        Ok(r) => r,
        Err(_) => {
            return (StatusCode::NOT_FOUND, Json(json!({
                "error": "CCN metrics view not available"
            })));
        }
    };

    if !view_exists.0 {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "CCN metrics view not available"
        })));
    }

    // Default to last 2 weeks if no dates provided
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let two_weeks = 60.0 * 60.0 * 24.0 * 14.0;

    let start = params.start_date.unwrap_or_else(|| {
        params.end_date.map(|e| e - two_weeks).unwrap_or(now - two_weeks)
    });

    let sort_order = match params.sort.as_deref() {
        Some("1") | Some("asc") | Some("ASC") => "ASC",
        _ => "DESC",
    };

    let query = if params.end_date.is_some() {
        format!(
            "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, \
             metrics_latency, aggregate_latency, file_download_latency, \
             pending_messages, eth_height_remaining \
             FROM ccn_metric_view \
             WHERE node_id = $1 AND measured_at >= $2 AND measured_at <= $3 \
             ORDER BY measured_at {}",
            sort_order
        )
    } else {
        format!(
            "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, \
             metrics_latency, aggregate_latency, file_download_latency, \
             pending_messages, eth_height_remaining \
             FROM ccn_metric_view \
             WHERE node_id = $1 AND measured_at >= $2 \
             ORDER BY measured_at {}",
            sort_order
        )
    };

    let rows: Vec<(
        Option<String>,   // item_hash
        Option<f64>,      // measured_at
        Option<f64>,      // base_latency
        Option<f64>,      // base_latency_ipv4
        Option<f64>,      // metrics_latency
        Option<f64>,      // aggregate_latency
        Option<f64>,      // file_download_latency
        Option<i32>,      // pending_messages
        Option<i32>,      // eth_height_remaining
    )> = if let Some(end) = params.end_date {
        sqlx::query_as(&query)
            .bind(&node_id)
            .bind(start)
            .bind(end)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    } else {
        sqlx::query_as(&query)
            .bind(&node_id)
            .bind(start)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    };

    if rows.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "Not found"
        })));
    }

    // Transpose rows into column arrays (matching pyaleph format)
    let mut item_hashes: Vec<serde_json::Value> = Vec::new();
    let mut measured_ats: Vec<serde_json::Value> = Vec::new();
    let mut base_latencies: Vec<serde_json::Value> = Vec::new();
    let mut base_latencies_ipv4: Vec<serde_json::Value> = Vec::new();
    let mut metrics_latencies: Vec<serde_json::Value> = Vec::new();
    let mut aggregate_latencies: Vec<serde_json::Value> = Vec::new();
    let mut file_download_latencies: Vec<serde_json::Value> = Vec::new();
    let mut pending_messages_list: Vec<serde_json::Value> = Vec::new();
    let mut eth_height_remaining_list: Vec<serde_json::Value> = Vec::new();

    for row in &rows {
        item_hashes.push(json!(row.0));
        measured_ats.push(json!(row.1));
        base_latencies.push(json!(row.2));
        base_latencies_ipv4.push(json!(row.3));
        metrics_latencies.push(json!(row.4));
        aggregate_latencies.push(json!(row.5));
        file_download_latencies.push(json!(row.6));
        pending_messages_list.push(json!(row.7));
        eth_height_remaining_list.push(json!(row.8));
    }

    (StatusCode::OK, Json(json!({
        "metrics": {
            "item_hash": item_hashes,
            "measured_at": measured_ats,
            "base_latency": base_latencies,
            "base_latency_ipv4": base_latencies_ipv4,
            "metrics_latency": metrics_latencies,
            "aggregate_latency": aggregate_latencies,
            "file_download_latency": file_download_latencies,
            "pending_messages": pending_messages_list,
            "eth_height_remaining": eth_height_remaining_list,
        }
    })))
}

/// Get CRN (Compute Resource Node) metrics for a specific node
/// Reference: aleph/web/controllers/main.py:crn_metric
///
/// Queries the crn_metric_view (database view over scoring messages)
/// for a specific node_id. Returns time-series metrics data.
pub async fn get_crn_metrics(
    State(state): State<Arc<AppState>>,
    Path(node_id): Path<String>,
    Query(params): Query<NodeMetricsQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({
            "error": "Database not available"
        })));
    }

    // Check if crn_metric_view exists
    let view_exists: (bool,) = match sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = 'crn_metric_view')"
    )
    .fetch_one(state.db())
    .await {
        Ok(r) => r,
        Err(_) => {
            return (StatusCode::NOT_FOUND, Json(json!({
                "error": "CRN metrics view not available"
            })));
        }
    };

    if !view_exists.0 {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "CRN metrics view not available"
        })));
    }

    // Default to last 2 weeks if no dates provided
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let two_weeks = 60.0 * 60.0 * 24.0 * 14.0;

    let start = params.start_date.unwrap_or_else(|| {
        params.end_date.map(|e| e - two_weeks).unwrap_or(now - two_weeks)
    });

    let sort_order = match params.sort.as_deref() {
        Some("1") | Some("asc") | Some("ASC") => "ASC",
        _ => "DESC",
    };

    let query = if params.end_date.is_some() {
        format!(
            "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, \
             full_check_latency, diagnostic_vm_latency \
             FROM crn_metric_view \
             WHERE node_id = $1 AND measured_at >= $2 AND measured_at <= $3 \
             ORDER BY measured_at {}",
            sort_order
        )
    } else {
        format!(
            "SELECT item_hash, measured_at, base_latency, base_latency_ipv4, \
             full_check_latency, diagnostic_vm_latency \
             FROM crn_metric_view \
             WHERE node_id = $1 AND measured_at >= $2 \
             ORDER BY measured_at {}",
            sort_order
        )
    };

    let rows: Vec<(
        Option<String>,   // item_hash
        Option<f64>,      // measured_at
        Option<f64>,      // base_latency
        Option<f64>,      // base_latency_ipv4
        Option<f64>,      // full_check_latency
        Option<f64>,      // diagnostic_vm_latency
    )> = if let Some(end) = params.end_date {
        sqlx::query_as(&query)
            .bind(&node_id)
            .bind(start)
            .bind(end)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    } else {
        sqlx::query_as(&query)
            .bind(&node_id)
            .bind(start)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
    };

    if rows.is_empty() {
        return (StatusCode::NOT_FOUND, Json(json!({
            "error": "Not found"
        })));
    }

    // Transpose rows into column arrays (matching pyaleph format)
    let mut item_hashes: Vec<serde_json::Value> = Vec::new();
    let mut measured_ats: Vec<serde_json::Value> = Vec::new();
    let mut base_latencies: Vec<serde_json::Value> = Vec::new();
    let mut base_latencies_ipv4: Vec<serde_json::Value> = Vec::new();
    let mut full_check_latencies: Vec<serde_json::Value> = Vec::new();
    let mut diagnostic_vm_latencies: Vec<serde_json::Value> = Vec::new();

    for row in &rows {
        item_hashes.push(json!(row.0));
        measured_ats.push(json!(row.1));
        base_latencies.push(json!(row.2));
        base_latencies_ipv4.push(json!(row.3));
        full_check_latencies.push(json!(row.4));
        diagnostic_vm_latencies.push(json!(row.5));
    }

    (StatusCode::OK, Json(json!({
        "metrics": {
            "item_hash": item_hashes,
            "measured_at": measured_ats,
            "base_latency": base_latencies,
            "base_latency_ipv4": base_latencies_ipv4,
            "full_check_latency": full_check_latencies,
            "diagnostic_vm_latency": diagnostic_vm_latencies,
        }
    })))
}
