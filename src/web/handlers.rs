//! API request handlers
//!
//! These handlers match the pyaleph API format for client compatibility.
//! Reference: aleph/web/controllers/

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Timestamp as Unix timestamp (seconds)
    pub time: f64,
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
        Self {
            message_type: msg.message_type.clone(),
            chain: msg.chain.clone(),
            sender: msg.sender.clone(),
            signature: msg.signature.clone(),
            item_type: msg.item_type.clone(),
            item_hash: msg.item_hash.clone(),
            item_content: msg.item_content.clone(),
            channel: msg.channel.clone(),
            time: msg.time,
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
}

/// List messages - matches pyaleph /messages.json response format
///
/// Uses parameterized queries to prevent SQL injection.
pub async fn list_messages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    // Support both 'limit' and 'pagination' parameters (limit takes precedence)
    // pagination=0 means "no limit" (pyaleph compat) — cap at 10,000
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
    let offset = ((page - 1) * per_page) as i64;
    
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
    
    // Build query with safe parameterized filters
    let mut builder = crate::db::QueryBuilder::new("SELECT * FROM messages WHERE 1=1");
    
    // Parse addresses filter (parameterized)
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            builder.and_in("sender", &addr_list);
        }
    }
    
    // Parse message type filter (parameterized)
    if let Some(ref msg_type) = message_type_filter {
        builder.and_eq("message_type", msg_type.to_uppercase());
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
    
    // Parse refs filter (content.ref field - requires JSONB query)
    if let Some(ref refs) = params.refs {
        let ref_list = crate::db::parse_csv_param(refs);
        if !ref_list.is_empty() {
            builder.and_jsonb_text_in("item_content", "ref", &ref_list);
        }
    }
    
    // Parse tags filter (content.content.tags array - requires JSONB containment)
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        for tag in tag_list {
            builder.and_jsonb_array_contains("item_content", "content.tags", tag);
        }
    }
    
    // Parse contentTypes filter (content.type field)
    if let Some(ref content_types) = params.content_types {
        let type_list = crate::db::parse_csv_param(content_types);
        if !type_list.is_empty() {
            builder.and_jsonb_text_in("item_content", "type", &type_list);
        }
    }
    
    // Parse contentHashes filter (content.item_hash field)
    if let Some(ref content_hashes) = params.content_hashes {
        let hash_list = crate::db::parse_csv_param(content_hashes);
        if !hash_list.is_empty() {
            builder.and_jsonb_text_in("item_content", "item_hash", &hash_list);
        }
    }
    
    // Parse owners filter (content.address field for programs/instances)
    if let Some(ref owners) = params.owners {
        let owner_list = crate::db::parse_csv_param(owners);
        if !owner_list.is_empty() {
            builder.and_jsonb_text_in("item_content", "address", &owner_list);
        }
    }
    
    // Time filters (parameterized)
    if let Some(start) = params.start_date {
        builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lte("time", end);
    }
    
    // Block number filters (via chain_txs JOIN)
    // If startBlock or endBlock specified, filter by chain_txs.height
    if params.start_block.is_some() || params.end_block.is_some() {
        // Build subquery to get item_hashes matching block range
        let mut block_conditions = Vec::new();
        let mut block_params: Vec<String> = Vec::new();
        
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
    
    // Order and pagination - use sortBy to determine column
    let order_column = match params.sort_by {
        Some(SortBy::TxTime) => "created_at",
        Some(SortBy::Time) | None => "time",
    };
    // Support both order and sortOrder params (sortOrder takes precedence)
    let order_value = params.sort_order.or(params.order);
    let ascending = order_value.map(|o| o == 1).unwrap_or(false);
    builder.order_by(order_column, ascending);
    builder.limit(per_page as i64);
    builder.offset(offset);
    
    // Get total count first (before consuming args)
    let count_builder = crate::db::QueryBuilder::new("SELECT COUNT(*) FROM messages WHERE 1=1");
    // Re-apply the same filters for count
    let mut count_builder = crate::db::QueryBuilder::new("SELECT COUNT(*) FROM messages WHERE 1=1");
    
    if let Some(ref addresses) = params.addresses {
        let addr_list = crate::db::parse_csv_param(addresses);
        if !addr_list.is_empty() {
            count_builder.and_in("sender", &addr_list);
        }
    }
    if let Some(ref msg_type) = message_type_filter {
        count_builder.and_eq("message_type", msg_type.to_uppercase());
    }
    if let Some(ref channels) = params.channels {
        let channel_list = crate::db::parse_csv_param(channels);
        if !channel_list.is_empty() {
            count_builder.and_in("channel", &channel_list);
        }
    }
    // Add same filters to count builder
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
            count_builder.and_jsonb_text_in("item_content", "ref", &ref_list);
        }
    }
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        for tag in tag_list {
            count_builder.and_jsonb_array_contains("item_content", "content.tags", tag.clone());
        }
    }
    if let Some(ref content_types) = params.content_types {
        let type_list = crate::db::parse_csv_param(content_types);
        if !type_list.is_empty() {
            count_builder.and_jsonb_text_in("item_content", "type", &type_list);
        }
    }
    if let Some(ref content_hashes) = params.content_hashes {
        let hash_list = crate::db::parse_csv_param(content_hashes);
        if !hash_list.is_empty() {
            count_builder.and_jsonb_text_in("item_content", "item_hash", &hash_list);
        }
    }
    if let Some(ref owners) = params.owners {
        let owner_list = crate::db::parse_csv_param(owners);
        if !owner_list.is_empty() {
            count_builder.and_jsonb_text_in("item_content", "address", &owner_list);
        }
    }
    if let Some(start) = params.start_date {
        count_builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        count_builder.and_lte("time", end);
    }
    
    // Block number filters for count query
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
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));

    // Now get the messages with the main query
    let (query, args) = builder.build();
    let messages = sqlx::query_as_with::<_, crate::db::models::MessageDb, _>(&query, args)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();
    
    // Batch fetch confirmations for all messages
    let item_hashes: Vec<String> = messages.iter().map(|m| m.item_hash.clone()).collect();
    let mut confirmations_map: HashMap<String, Vec<ConfirmationResponse>> = HashMap::new();
    
    if !item_hashes.is_empty() {
        // Build parameterized IN query for confirmations
        let placeholders: Vec<String> = (1..=item_hashes.len())
            .map(|i| format!("${}", i))
            .collect();
        let query = format!(
            "SELECT item_hash, chain, hash, height FROM chain_txs WHERE item_hash IN ({})",
            placeholders.join(", ")
        );
        
        let mut q = sqlx::query_as::<_, (String, String, String, i64)>(&query);
        for hash in &item_hashes {
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
    
    Json(json!({
        "messages": message_responses,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
        "pagination_item": "messages",
    }))
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
            // Check if it's pending and get reception_time
            let pending = sqlx::query_as::<_, (String, f64)>(
                "SELECT item_hash, reception_time FROM pending_messages WHERE item_hash = $1 LIMIT 1"
            )
            .bind(&hash)
            .fetch_optional(state.db())
            .await
            .unwrap_or(None);

            if let Some((_item_hash, reception_time)) = pending {
                (StatusCode::OK, Json(json!({
                    "status": "pending",
                    "item_hash": hash,
                    "reception_time": reception_time,
                })))
            } else {
                (StatusCode::NOT_FOUND, Json(json!({
                    "error": "Message not found"
                })))
            }
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
    
    // Check processed messages
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE item_hash = $1)"
    )
    .bind(&hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(false);
    
    if exists {
        return (StatusCode::OK, Json(json!({
            "status": "processed",
            "item_hash": hash,
        })));
    }
    
    // Check pending messages
    let pending = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM pending_messages WHERE item_hash = $1)"
    )
    .bind(&hash)
    .fetch_one(state.db())
    .await
    .unwrap_or(false);
    
    if pending {
        return (StatusCode::OK, Json(json!({
            "status": "pending",
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
            
            (StatusCode::ACCEPTED, Json(json!({
                "status": "pending",
                "item_hash": msg.item_hash,
                "message": "Message received and queued for processing"
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
    Path(address): Path<String>,
    Query(params): Query<AggregateQuery>,
) -> impl IntoResponse {
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
    let limit = params.limit;
    
    // Build base query - different for with_info vs regular
    if with_info {
        // Query with join to get metadata
        let aggregates: Vec<(String, serde_json::Value, f64, f64, Option<String>, Option<String>)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                let mut query_str = String::from(
                    "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1 AND a.key = ANY($2)"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .bind(keys)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                let mut query_str = String::from(
                    "SELECT a.key, a.content, a.time as created, \
                     COALESCE(ae.time, a.time) as last_updated, \
                     a.last_revision_hash as last_update_item_hash, \
                     ae.item_hash as original_item_hash \
                     FROM aggregates a \
                     LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash \
                     WHERE a.address = $1"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
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

        // Build data and info maps
        let mut data = serde_json::Map::new();
        let mut info = serde_json::Map::new();

        for (key, content, created, last_updated, last_update_hash, original_hash) in aggregates {
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
        // Regular query without metadata
        let aggregates: Vec<(String, serde_json::Value)> = match &key_list {
            Some(keys) if !keys.is_empty() => {
                let mut query_str = String::from(
                    "SELECT key, content FROM aggregates WHERE address = $1 AND key = ANY($2)"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
                    .bind(keys)
                    .fetch_all(state.db())
                    .await
                    .unwrap_or_default()
            }
            _ => {
                let mut query_str = String::from(
                    "SELECT key, content FROM aggregates WHERE address = $1"
                );
                if let Some(lim) = limit {
                    query_str.push_str(&format!(" LIMIT {}", lim));
                }
                sqlx::query_as(&query_str)
                    .bind(&address)
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

        // Handle value_only - only works for single key
        if value_only {
            if let Some(ref keys) = key_list {
                if keys.len() == 1 {
                    // Find the matching aggregate and return just its value
                    for (key, content) in &aggregates {
                        if key == &keys[0] {
                            return (StatusCode::OK, Json(content.clone()));
                        }
                    }
                }
            }
        }

        // Build data map
        let mut data = serde_json::Map::new();
        for (key, content) in aggregates {
            data.insert(key, content);
        }

        (StatusCode::OK, Json(json!({
            "address": address,
            "data": data,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    pub size: i64,
    pub confirmed: bool,
    pub confirmations: Vec<ConfirmationResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    // pagination=0 means "no limit" (pyaleph compat) — cap at 10,000
    let raw_pagination = params.limit.or(params.pagination).unwrap_or(20);
    let per_page = if raw_pagination == 0 { 10_000 } else { raw_pagination.min(1000) };
    let offset = ((page - 1) * per_page) as i64;

    // Merge order and sort_order (order takes precedence)
    let order_param = params.order.or(params.sort_order);

    if !state.has_db() {
        return Json(json!({
            "posts": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
        }));
    }
    
    // Build merged post query matching Python pyaleph's make_select_merged_post_with_message_info_stmt()
    // - Only return originals (p.amends IS NULL)
    // - LEFT JOIN latest amend for content/type coalescing
    // - LEFT JOIN messages for both original and amend message-level fields
    let mut builder = crate::db::QueryBuilder::new(
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
    );
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
    
    // Time filters
    if let Some(start) = params.start_date {
        builder.and_gte("p.time", start);
        count_builder.and_gte("p.time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lte("p.time", end);
        count_builder.and_lte("p.time", end);
    }
    
    // Filter by tags - checks posts.content (inner content) for tags array
    // Matching Python pyaleph: content->'tags' has_any ARRAY[tag values]
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        for tag in tag_list {
            let check_obj = serde_json::json!({"tags": [&tag]});
            let check_str = check_obj.to_string().replace('\'', "''");
            let clause = format!("COALESCE(a.content, p.content) @> '{}'::jsonb", check_str);
            builder.and_raw(&clause);
            count_builder.and_raw(&clause);
        }
    }
    
    // Determine sort column (validate against allowed columns)
    let allowed_sort = &["time", "address", "post_type", "channel"];
    let sort_column = match params.sort_by.as_deref() {
        Some("time") | None => "COALESCE(a.time, p.time)".to_string(),
        Some("address") => "p.address".to_string(),
        Some("post_type") => "p.post_type".to_string(),
        Some("channel") => "p.channel".to_string(),
        _ => "COALESCE(a.created_at, p.created_at)".to_string(),
    };
    
    // Order: 1 = ascending, -1 = descending (default)
    let ascending = order_param.map(|o| o == 1).unwrap_or(false);
    
    // Add raw ORDER BY since we have table prefix
    let order_dir = if ascending { "ASC" } else { "DESC" };
    builder.and_raw(&format!("1=1 ORDER BY {} {} LIMIT {} OFFSET {}", sort_column, order_dir, per_page, offset));
    
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
    
    // Get original item_hashes for confirmation lookup
    let item_hashes: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let mut confirmations_map: HashMap<String, Vec<ConfirmationResponse>> = HashMap::new();
    
    if !item_hashes.is_empty() {
        let placeholders: Vec<String> = (1..=item_hashes.len())
            .map(|i| format!("${}", i))
            .collect();
        let conf_query = format!(
            "SELECT item_hash, chain, hash, height FROM chain_txs WHERE item_hash IN ({})",
            placeholders.join(", ")
        );
        
        let mut q = sqlx::query_as::<_, (String, String, String, i64)>(&conf_query);
        for hash in &item_hashes {
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
    
    // Build response — merged post format matching Python pyaleph
    let posts: Vec<PostResponseV0> = rows.iter().map(|row| {
        let confirmations = confirmations_map
            .get(&row.0)
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
        "pagination_per_page": per_page,
        "pagination_item": "posts",
    }))
}

/// Get balance for an address - matches pyaleph GetAccountBalanceResponse format
/// Reference: aleph/web/controllers/accounts.py:get_account_balance
/// Returns balance as float, with per-chain details map and locked_amount from costs
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
            "credit_balance": 0,
        }));
    }

    // Query all per-chain balances for this address
    let chain_balances: Vec<(String, rust_decimal::Decimal)> = sqlx::query_as(
        "SELECT chain, balance FROM balances WHERE address = $1"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();

    // Build details map and compute total
    let mut details = serde_json::Map::new();
    let mut total = rust_decimal::Decimal::ZERO;
    for (chain, balance) in &chain_balances {
        let bal_f64: f64 = balance.to_string().parse().unwrap_or(0.0);
        details.insert(chain.clone(), json!(bal_f64));
        total += balance;
    }
    let total_f64: f64 = total.to_string().parse().unwrap_or(0.0);

    // Query locked_amount from account_costs (total_cost for the address)
    let locked: rust_decimal::Decimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(total_cost), 0) FROM account_costs WHERE address = $1"
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

    // Try to get content from local storage first
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
            (StatusCode::NOT_FOUND, Json(json!({
                "status": "not_found",
                "hash": hash,
            })))
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
    
    let cost = sqlx::query_as::<_, crate::db::models::AccountCostDb>(
        "SELECT * FROM account_costs WHERE address IN (SELECT owner FROM programs WHERE item_hash = $1 UNION SELECT owner FROM instances WHERE item_hash = $1)"
    )
    .bind(&hash)
    .fetch_optional(state.db())
    .await;
    
    match cost {
        Ok(Some(c)) => Json(json!({
            "hash": hash,
            "storage_cost": c.storage_cost.to_string(),
            "compute_cost": c.compute_cost.to_string(),
            "total_cost": c.total_cost.to_string(),
        })),
        _ => Json(json!({
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
#[derive(Debug, Clone, Serialize)]
pub struct BalanceItem {
    pub address: String,
    pub chain: String,
    /// Balance as string to preserve precision for large numbers
    pub balance: String,
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
            balance: b.balance.to_string(),
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
    /// Items per page (default: 20, max: 1000)
    pub limit: Option<u32>,
    /// Alias for limit (pyaleph compatibility)
    pub pagination: Option<u32>,
    /// Page number (1-indexed)
    pub page: Option<u32>,
    /// Sort order: 1 for ascending, -1 for descending (default: -1, by last_updated)
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<i8>,
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
    // Support both limit and pagination params (limit takes precedence)
    let per_page = params.limit.or(params.pagination).unwrap_or(20).min(1000);
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
    
    // Build query with filters
    let mut builder = crate::db::QueryBuilder::new(
        "SELECT a.address, a.key, a.content, a.time as created,          COALESCE(ae.time, a.time) as last_updated          FROM aggregates a          LEFT JOIN aggregate_elements ae ON a.last_revision_hash = ae.item_hash          WHERE 1=1"
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
    
    // Order and pagination - order by last_updated (descending by default)
    let order_dir = if ascending { "ASC" } else { "DESC" };
    builder.and_raw(&format!(
        "1=1 ORDER BY COALESCE(ae.time, a.time) {} LIMIT {} OFFSET {}",
        order_dir, per_page, offset
    ));
    
    // Get total count
    let (count_query, count_args) = count_builder.build();
    let total: (i64,) = sqlx::query_as_with(&count_query, count_args)
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    // Get aggregates
    let (query, args) = builder.build();
    let aggregates: Vec<(String, String, serde_json::Value, f64, f64)> = 
        sqlx::query_as_with(&query, args)
            .fetch_all(state.db())
            .await
            .unwrap_or_default();
    
    // Format response with ISO timestamps
    let aggregate_items: Vec<AggregateListItem> = aggregates
        .into_iter()
        .map(|(address, key, content, created, last_updated)| {
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
    
    // First, check if message exists and get its type
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
            match message_type.as_str() {
                "PROGRAM" => {
                    // Get program details
                    let program = sqlx::query_as::<_, crate::db::models::ProgramDb>(
                        "SELECT * FROM programs WHERE item_hash = $1"
                    )
                    .bind(&item_hash)
                    .fetch_optional(state.db())
                    .await;
                    
                    match program {
                        Ok(Some(prog)) => {
                            // Calculate cost for this program
                            let hours = 24 * 30; // Monthly cost estimate
                            let storage_mib = 20480u64; // Default 20GB storage
                            
                            if let Some(cost) = state.cost.calculate_instance_cost(
                                prog.memory as u32,
                                prog.vcpus as u32,
                                storage_mib,
                                hours,
                                ProductPriceType::Program,
                                false, // internet_enabled - would need to parse content
                            ).await {
                                let required_tokens = cost.holding.to_string().parse::<f64>().unwrap_or(0.0);
                                let compute_units = state.cost.calculate_compute_units(prog.memory as u32, prog.vcpus as u32);
                                
                                (StatusCode::OK, Json(json!({
                                    "required_tokens": required_tokens,
                                    "payment_type": "hold",
                                    "cost": format!("{:.6} ALEPH", required_tokens),
                                    "detail": [
                                        {
                                            "type": "compute",
                                            "name": format!("{} compute units", compute_units),
                                            "cost_hold": cost.holding.to_string(),
                                            "cost_stream": cost.payg.to_string(),
                                            "cost_credit": cost.credit.to_string()
                                        }
                                    ],
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
                    // Get instance details
                    let instance = sqlx::query_as::<_, crate::db::models::InstanceDb>(
                        "SELECT * FROM instances WHERE item_hash = $1"
                    )
                    .bind(&item_hash)
                    .fetch_optional(state.db())
                    .await;
                    
                    match instance {
                        Ok(Some(inst)) => {
                            // Determine product type from payment_type and trusted_execution
                            let product_type = if inst.trusted_execution.is_some() {
                                ProductPriceType::InstanceConfidential
                            } else {
                                ProductPriceType::Instance
                            };
                            
                            // Determine payment type
                            let payment_type = inst.payment_type.as_deref().unwrap_or("hold");
                            
                            let hours = 24 * 30; // Monthly cost estimate
                            let storage_mib = 40960u64; // Default 40GB storage for instances
                            
                            if let Some(cost) = state.cost.calculate_instance_cost(
                                inst.memory as u32,
                                inst.vcpus as u32,
                                storage_mib,
                                hours,
                                product_type,
                                false, // internet_enabled
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
                                    "cost": format!("{:.6} ALEPH", required_tokens),
                                    "detail": [
                                        {
                                            "type": "compute",
                                            "name": format!("{} compute units", compute_units),
                                            "cost_hold": cost.holding.to_string(),
                                            "cost_stream": cost.payg.to_string(),
                                            "cost_credit": cost.credit.to_string()
                                        }
                                    ],
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
                    // For STORE messages, return storage cost
                    // Need to get file size from file_pins or content
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
                    let hours = 24 * 30; // Monthly
                    
                    if let Some(cost) = state.cost.calculate_storage_cost(size_mib.max(1), hours, ProductPriceType::Storage).await {
                        let required_tokens = cost.holding.to_string().parse::<f64>().unwrap_or(0.0);
                        
                        (StatusCode::OK, Json(json!({
                            "required_tokens": required_tokens,
                            "payment_type": "hold",
                            "cost": format!("{:.6} ALEPH", required_tokens),
                            "detail": [
                                {
                                    "type": "storage",
                                    "name": format!("{} MiB", size_mib),
                                    "cost_hold": cost.holding.to_string(),
                                    "cost_stream": cost.payg.to_string(),
                                    "cost_credit": cost.credit.to_string()
                                }
                            ],
                            "charged_address": msg.sender,
                        })))
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                            "error": "Unable to calculate storage cost"
                        })))
                    }
                }
                _ => {
                    // Not a priced message type
                    (StatusCode::BAD_REQUEST, Json(json!({
                        "error": format!("Message type '{}' does not have pricing. Only PROGRAM, INSTANCE, and STORE messages have prices.", message_type),
                        "item_hash": item_hash,
                        "message_type": message_type
                    })))
                }
            }
        }
        Ok(None) => {
            // Check if pending
            let pending = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pending_messages WHERE item_hash = $1)"
            )
            .bind(&item_hash)
            .fetch_one(state.db())
            .await
            .unwrap_or(false);
            
            if pending {
                (StatusCode::ACCEPTED, Json(json!({
                    "error": "Message still pending",
                    "item_hash": item_hash,
                    "status": "pending"
                })))
            } else {
                (StatusCode::NOT_FOUND, Json(json!({
                    "error": "Message not found",
                    "item_hash": item_hash
                })))
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "error": e.to_string()
        }))),
    }
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
    #[serde(rename = "sortOrder")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
        price: h.1.map(|p: rust_decimal::Decimal| p.to_string()),
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
    // Returns: original_item_hash, item_hash (coalesced), content (coalesced),
    //          address, ref_, channel, created (original created_at), last_updated (coalesced),
    //          original_type
    let mut builder = crate::db::QueryBuilder::new(
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
    );
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

    // Time filters
    if let Some(start) = params.start_date {
        builder.and_gte("p.time", start);
        count_builder.and_gte("p.time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lte("p.time", end);
        count_builder.and_lte("p.time", end);
    }

    // Filter by tags
    if let Some(ref tags) = params.tags {
        let tag_list = crate::db::parse_csv_param(tags);
        for tag in tag_list {
            let check_obj = serde_json::json!({"tags": [&tag]});
            let check_str = check_obj.to_string().replace('\'', "''");
            let clause = format!("COALESCE(a.content, p.content) @> '{}'::jsonb", check_str);
            builder.and_raw(&clause);
            count_builder.and_raw(&clause);
        }
    }

    // Sort column
    let sort_column = match params.sort_by.as_deref() {
        Some("time") | None => "COALESCE(a.created_at, p.created_at)".to_string(),
        Some("address") => "p.address".to_string(),
        Some("post_type") => "p.post_type".to_string(),
        Some("channel") => "p.channel".to_string(),
        _ => "COALESCE(a.created_at, p.created_at)".to_string(),
    };

    let ascending = order_param.map(|o| o == 1).unwrap_or(false);
    let order_dir = if ascending { "ASC" } else { "DESC" };
    builder.and_raw(&format!("1=1 ORDER BY {} {} LIMIT {} OFFSET {}", sort_column, order_dir, per_page, offset));

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
    let messages_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(state.db())
        .await
        .unwrap_or(0);

    let posts_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM posts")
        .fetch_one(state.db())
        .await
        .unwrap_or(0);

    let aggregates_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM aggregates")
        .fetch_one(state.db())
        .await
        .unwrap_or(0);

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
    
    // Use pg_class for fast approximate counts on large tables
    let messages: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'messages'"
    ).fetch_one(db).await.unwrap_or((0,));
    
    let rejected: (i64,) = sqlx::query_as(
        "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'rejected_messages'"
    ).fetch_one(db).await.unwrap_or((0,));
    
    let posts: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM posts")
        .fetch_one(db).await.unwrap_or((0,));
    
    let aggregates: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM aggregates")
        .fetch_one(db).await.unwrap_or((0,));
    
    let programs: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM programs")
        .fetch_one(db).await.unwrap_or((0,));
    
    let instances: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM instances")
        .fetch_one(db).await.unwrap_or((0,));
    
    let files: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM file_pins")
        .fetch_one(db).await.unwrap_or((0,));
    
    // Messages by type for breakdown
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
    for (t, count) in by_type {
        types_map.insert(t, json!(count));
    }

    // Content fetch stats — messages missing item_content
    let missing_content: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE item_content IS NULL AND item_type IN ('storage', 'ipfs')"
    ).fetch_one(db).await.unwrap_or((0,));

    let total_non_inline: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE item_type IN ('storage', 'ipfs')"
    ).fetch_one(db).await.unwrap_or((0,));

    let marked_unfetchable: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE item_content = '' AND item_type IN ('storage', 'ipfs')"
    ).fetch_one(db).await.unwrap_or((0,));

    let fetched_content = total_non_inline.0 - missing_content.0 - marked_unfetchable.0;
    let fetch_pct = if total_non_inline.0 > 0 {
        (fetched_content as f64 / total_non_inline.0 as f64 * 100.0)
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
            "messages": messages.0,
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
            "total_non_inline": total_non_inline.0,
            "fetched": fetched_content,
            "missing": missing_content.0,
            "unfetchable": marked_unfetchable.0,
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
    
    // Query distinct post types from posts table
    let post_types: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT post_type FROM posts WHERE address = $1 ORDER BY post_type"
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
    
    // Get total unique senders count
    let total: (i64,) = sqlx::query_as(
        "SELECT COUNT(DISTINCT sender) FROM messages"
    )
    .fetch_one(state.db())
    .await
    .unwrap_or((0,));
    
    // Query addresses with their message counts, ordered by total messages
    let order_clause = if ascending { "ASC" } else { "DESC" };
    let query = format!(
        "SELECT sender, COUNT(*) as total_messages \
         FROM messages \
         GROUP BY sender \
         ORDER BY total_messages {} \
         LIMIT $1 OFFSET $2",
        order_clause
    );
    
    let addresses: Vec<(String, i64)> = sqlx::query_as(&query)
        .bind(per_page as i64)
        .bind(offset)
        .fetch_all(state.db())
        .await
        .unwrap_or_default();
    
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
