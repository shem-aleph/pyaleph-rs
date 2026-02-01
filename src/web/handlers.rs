//! API request handlers

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

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Health check endpoint
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Query parameters for message list
#[derive(Debug, Deserialize)]
pub struct MessageQuery {
    pub addresses: Option<String>,
    pub channels: Option<String>,
    pub message_type: Option<String>,
    pub hashes: Option<String>,
    pub refs: Option<String>,
    pub tags: Option<String>,
    pub pagination: Option<u32>,
    pub page: Option<u32>,
}

/// List messages
pub async fn list_messages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MessageQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20);
    let offset = ((page - 1) * per_page) as i64;
    
    if !state.has_db() {
        return Json(json!({
            "messages": [],
            "pagination_total": 0,
            "pagination_page": page,
            "pagination_per_page": per_page,
            "error": "Database not available"
        }));
    }
    
    // Parse addresses filter
    let addresses: Option<Vec<String>> = params.addresses
        .map(|a| a.split(',').map(|s| s.trim().to_string()).collect());
    
    // Query database
    let messages = sqlx::query_as::<_, crate::db::models::MessageDb>(
        "SELECT * FROM messages ORDER BY time DESC LIMIT $1 OFFSET $2"
    )
    .bind(per_page as i64)
    .bind(offset)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    // Get total count
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages")
        .fetch_one(state.db())
        .await
        .unwrap_or((0,));
    
    Json(json!({
        "messages": messages,
        "pagination_total": total.0,
        "pagination_page": page,
        "pagination_per_page": per_page,
    }))
}

/// Get a single message by hash
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
        Ok(Some(msg)) => (StatusCode::OK, Json(json!({
            "status": "processed",
            "message": msg
        }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({
            "status": "not_found",
            "item_hash": hash,
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "status": "error",
            "message": e.to_string()
        }))),
    }
}

/// Post content request
#[derive(Debug, Deserialize)]
pub struct PostContentRequest {
    pub message: serde_json::Value,
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
            let sig_valid = state.crypto.verify_signature(
                &msg.chain,
                &msg.item_hash, // simplified - should be the actual signed content
                &msg.signature,
                &msg.sender,
            );
            
            if let Err(e) = sig_valid {
                return (StatusCode::BAD_REQUEST, Json(json!({
                    "status": "error",
                    "message": format!("Signature verification failed: {}", e)
                })));
            }
            
            // TODO: Store in pending_messages for processing
            // TODO: Process immediately for inline content
            
            (StatusCode::ACCEPTED, Json(json!({
                "status": "pending",
                "item_hash": msg.item_hash,
                "message": "Message received and queued for processing"
            })))
        }
        Err(e) => {
            (StatusCode::BAD_REQUEST, Json(json!({
                "status": "error",
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

/// Get aggregates for an address
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
    
    // Query aggregates
    let aggregates = sqlx::query_as::<_, crate::db::models::AggregateDb>(
        "SELECT * FROM aggregates WHERE address = $1"
    )
    .bind(&address)
    .fetch_all(state.db())
    .await
    .unwrap_or_default();
    
    // Build data map
    let mut data = serde_json::Map::new();
    for agg in aggregates {
        data.insert(agg.key, agg.content);
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

/// Get posts
pub async fn get_posts(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PostsQuery>,
) -> impl IntoResponse {
    let page = params.page.unwrap_or(1);
    let per_page = params.pagination.unwrap_or(20);
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

/// Get balance for an address
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
    if let Some(storage) = &state.storage {
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

/// Get pricing info
pub async fn get_pricing(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let storage_price = state.cost.get_price(&ProductPriceType::Storage);
    let instance_price = state.cost.get_price(&ProductPriceType::Instance);
    
    Json(json!({
        "storage": storage_price.map(|p| &p.storage),
        "compute_units": instance_price.and_then(|p| p.compute_unit.as_ref()),
    }))
}

/// Get node info
pub async fn get_info(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    Json(json!({
        "name": state.config.node.name,
        "version": env!("CARGO_PKG_VERSION"),
        "implementation": "aleph-core-rs",
        "database": state.has_db(),
        "chain_sync": {
            "ethereum": {
                "enabled": state.config.chains.ethereum.as_ref().map(|c| c.enabled).unwrap_or(false),
            }
        }
    }))
}
