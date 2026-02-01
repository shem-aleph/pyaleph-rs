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
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

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
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
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
    pub pagination: Option<u32>,
    /// Alias for pagination (pyaleph compatibility)
    pub limit: Option<u32>,
    pub page: Option<u32>,
    /// Start time filter (Unix timestamp)
    pub start_date: Option<f64>,
    /// End time filter (Unix timestamp)
    pub end_date: Option<f64>,
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
    let per_page = params.limit.or(params.pagination).unwrap_or(20).min(1000); // Max 1000 per page
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
    
    // Time filters (parameterized)
    if let Some(start) = params.start_date {
        builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        builder.and_lte("time", end);
    }
    
    // Order and pagination
    builder.order_by("time", false);
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
    if let Some(start) = params.start_date {
        count_builder.and_gte("time", start);
    }
    if let Some(end) = params.end_date {
        count_builder.and_lte("time", end);
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
    
    // Convert to response format with confirmations
    let message_responses: Vec<MessageResponse> = messages.iter()
        .map(|msg| {
            // TODO: Fetch actual confirmations from chain_txs table
            let confirmations = Vec::new();
            MessageResponse::from_db(msg, confirmations)
        })
        .collect();
    
    Json(json!({
        "messages": message_responses,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
    }))
}

/// Get a single message by hash - matches pyaleph format
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
            
            let response = MessageResponse::from_db(&msg, confirmations);
            
            (StatusCode::OK, Json(json!({
                "status": "processed",
                "message": response
            })))
        }
        Ok(None) => {
            // Check if it's pending
            let pending = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pending_messages WHERE item_hash = $1)"
            )
            .bind(&hash)
            .fetch_one(state.db())
            .await
            .unwrap_or(false);
            
            if pending {
                (StatusCode::OK, Json(json!({
                    "status": "pending",
                    "item_hash": hash,
                })))
            } else {
                (StatusCode::NOT_FOUND, Json(json!({
                    "status": "not_found",
                    "item_hash": hash,
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
}

/// Get aggregates for an address - matches pyaleph format
/// 
/// Uses parameterized queries to prevent SQL injection.
pub async fn get_aggregates(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
    Query(params): Query<AggregateQuery>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "data": {},
            "error": "Database not available"
        }));
    }
    
    // Parse keys filter safely
    let key_list: Option<Vec<String>> = params.keys.as_ref().map(|keys| {
        keys.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    
    // Query aggregates with parameterized query
    let aggregates: Vec<(String, serde_json::Value)> = match key_list {
        Some(ref keys) if !keys.is_empty() => {
            // Use ANY for safe IN clause
            sqlx::query_as(
                "SELECT key, content FROM aggregates WHERE address = $1 AND key = ANY($2)"
            )
            .bind(&address)
            .bind(keys)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
        }
        _ => {
            sqlx::query_as(
                "SELECT key, content FROM aggregates WHERE address = $1"
            )
            .bind(&address)
            .fetch_all(state.db())
            .await
            .unwrap_or_default()
        }
    };
    
    // Build data map
    let mut data = serde_json::Map::new();
    for (key, content) in aggregates {
        data.insert(key, content);
    }
    
    Json(json!({
        "address": address,
        "data": data,
    }))
}

/// Query parameters for posts
#[derive(Debug, Deserialize)]
pub struct PostsQuery {
    pub addresses: Option<String>,
    pub channels: Option<String>,
    pub types: Option<String>,
    pub refs: Option<String>,
    pub tags: Option<String>,
    pub hashes: Option<String>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

/// Get posts - matches pyaleph format
pub async fn get_posts(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PostsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20).min(1000);
    let offset = ((page - 1) * per_page) as i64;
    
    if !state.has_db() {
        return Json(json!({
            "posts": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
        }));
    }
    
    let posts = sqlx::query_as::<_, crate::db::models::PostDb>(
        "SELECT * FROM posts ORDER BY time DESC LIMIT $1 OFFSET $2"
    )
    .bind(per_page as i64)
    .bind(offset)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM posts")
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    Json(json!({
        "posts": posts,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
    }))
}

/// Get balance for an address - matches pyaleph format
pub async fn get_balance(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    if !state.has_db() {
        return Json(json!({
            "address": address,
            "balance": "0",
            "locked_balance": "0",
        }));
    }
    
    let balance = sqlx::query_as::<_, crate::db::models::BalanceDb>(
        "SELECT * FROM balances WHERE address = $1"
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
            "locked_balance": "0",
        })),
        None => Json(json!({
            "address": address,
            "balance": "0",
            "locked_balance": "0",
        })),
    }
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

/// Get storage info
pub async fn get_storage(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> impl IntoResponse {
    // Try to get from local storage first
    if let Some(ref storage) = state.storage {
        if storage.exists(&hash).await {
            let size = storage.get_size(&hash).await.unwrap_or(0);
            return (StatusCode::OK, Json(json!({
                "status": "available",
                "hash": hash,
                "size": size,
            })));
        }
    }
    
    // Check if it exists on IPFS
    if state.ipfs.exists(&hash).await {
        let size = state.ipfs.get_size(&hash).await.unwrap_or(0);
        return (StatusCode::OK, Json(json!({
            "status": "available",
            "hash": hash,
            "size": size,
            "location": "ipfs",
        })));
    }
    
    (StatusCode::NOT_FOUND, Json(json!({
        "status": "not_found",
        "hash": hash,
    })))
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

/// Get message content
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
            match msg.item_content {
                Some(content) => {
                    // Try to parse as JSON and return
                    match serde_json::from_str::<serde_json::Value>(&content) {
                        Ok(json) => (StatusCode::OK, Json(json)),
                        Err(_) => (StatusCode::OK, Json(json!({ "content": content }))),
                    }
                }
                None => {
                    // Content not inline, try to fetch from IPFS
                    match state.ipfs.get(&hash).await {
                        Ok(bytes) => {
                            match String::from_utf8(bytes) {
                                Ok(content) => {
                                    match serde_json::from_str::<serde_json::Value>(&content) {
                                        Ok(json) => (StatusCode::OK, Json(json)),
                                        Err(_) => (StatusCode::OK, Json(json!({ "content": content }))),
                                    }
                                }
                                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                                    "error": "Content is not valid UTF-8"
                                }))),
                            }
                        }
                        Err(e) => (StatusCode::NOT_FOUND, Json(json!({
                            "error": "Content not found",
                            "message": e.to_string()
                        }))),
                    }
                }
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
