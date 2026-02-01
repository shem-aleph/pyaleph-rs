//! API routes definition

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::handlers;
use super::state::AppState;

/// API v0 routes (compatible with pyaleph)
pub fn api_v0() -> Router<Arc<AppState>> {
    Router::new()
        // Info
        .route("/info", get(handlers::get_info))
        
        // Messages
        .route("/messages.json", get(handlers::list_messages))
        .route("/messages/:hash", get(handlers::get_message))
        .route("/messages", post(handlers::post_message))
        
        // Aggregates
        .route("/aggregates/:address.json", get(handlers::get_aggregates))
        
        // Posts
        .route("/posts.json", get(handlers::get_posts))
        
        // Storage
        .route("/storage/:hash", get(handlers::get_storage))
        
        // Balances
        .route("/addresses/:address/balance", get(handlers::get_balance))
        .route("/balance/:address", get(handlers::get_balance))
        .route("/credits/:address", get(handlers::get_credit_balance))
        
        // Programs & Instances
        .route("/programs/:address", get(handlers::get_programs))
        .route("/instances/:address", get(handlers::get_instances))
        
        // Pricing
        .route("/price", get(handlers::get_pricing))
        .route("/pricing", get(handlers::get_pricing))
}
