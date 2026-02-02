//! API routes definition
//!
//! Complete API compatible with pyaleph.
//! Reference: aleph/web/controllers/

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::handlers;
use super::state::AppState;
use super::websocket;

/// Create the full router with all routes
pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        // API v0 routes (main API)
        .nest("/api/v0", api_v0())
        // API ws0 routes (WebSocket compatibility with pyaleph)
        .nest("/api/ws0", api_ws0())
        .nest("/api/v1", api_v1())
        // Legacy routes (backwards compatibility)
        .merge(legacy_routes())
        // Internal/admin routes
        .nest("/_internal", internal_routes())
        .with_state(state)
}

/// API v0 routes (compatible with pyaleph)
pub fn api_v0() -> Router<Arc<AppState>> {
    Router::new()
        // Info & Health
        .route("/info", get(handlers::get_info))
        .route("/health", get(handlers::health_check))
        
        // Messages
        .route("/messages.json", get(handlers::list_messages))
        .route("/messages", get(handlers::list_messages))
        .route("/messages", post(handlers::post_message))
        .route("/messages/:hash", get(handlers::get_message))
        .route("/messages/:hash/status", get(handlers::get_message_status))
        .route("/messages/:hash/content", get(handlers::get_message_content))
        
        // Aggregates - use single route, handler can strip .json if needed
        .route("/aggregates/:address", get(handlers::get_aggregates))
        
        // Posts
        .route("/posts.json", get(handlers::get_posts))
        .route("/posts", get(handlers::get_posts))
        // P3 endpoints
        .route("/channels/list.json", get(handlers::list_channels))
        .route("/channels/list", get(handlers::list_channels))
        .route("/version", get(handlers::get_version))
        .route("/info/public.json", get(handlers::get_public_info))
        .route("/messages/page/:page", get(handlers::list_messages_page))
        
        .route("/posts/page/:page", get(handlers::list_posts_page))
        
        
        // Storage
        .route("/storage/:hash", get(handlers::get_storage))
        .route("/storage/:hash/raw", get(handlers::get_storage_raw))
        .route("/storage/upload", post(handlers::upload_file))
        
        // Storage metadata endpoints
        .route("/storage/add_json", post(handlers::add_json_storage))
        .route("/storage/by-message-hash/:hash", get(handlers::get_storage_by_message_hash))
        .route("/storage/by-ref/:address/:ref", get(handlers::get_storage_by_address_ref))
        .route("/storage/by-ref/:ref", get(handlers::get_storage_by_ref))
        
        // Hashes endpoint
        .route("/hashes", get(handlers::get_hashes))
        
        // Balances
        .route("/addresses/:address/balance", get(handlers::get_balance))
        .route("/balance/:address", get(handlers::get_balance))
        .route("/balances", get(handlers::get_balances))
        .route("/credits/:address", get(handlers::get_credit_balance))
        .route("/credit_balances", get(handlers::get_credit_balances))
        
        // Address stats, files, credit history
        .route("/addresses/stats.json", get(handlers::get_addresses_stats_v0))
        .route("/addresses/:address/files", get(handlers::get_address_files))
        .route("/addresses/:address/credit_history", get(handlers::get_address_credit_history))
        
        // Programs & Instances
        .route("/programs/:address", get(handlers::get_programs))
        .route("/programs", get(handlers::list_programs))
        .route("/instances/:address", get(handlers::get_instances))
        .route("/instances", get(handlers::list_instances))
        
        // VM allocation
        .route("/allocation/:hash", get(handlers::get_allocation))
        
        // Pricing & Costs
        .route("/price", get(handlers::get_pricing))
        .route("/price/:hash", get(handlers::get_message_price))
        .route("/pricing", get(handlers::get_pricing))
        // TODO: handler not implemented yet
        // .route("/price/:hash", get(handlers::get_message_price))
        .route("/cost/estimate", post(handlers::estimate_cost))
        .route("/cost/:hash", get(handlers::get_resource_cost))
        
        // Statistics
        .route("/stats", get(handlers::get_stats))
        .route("/monitor", get(handlers::get_monitor_stats))
        .route("/stats/:address", get(handlers::get_address_stats))
        
        // Pending messages - matches pyaleph /pending endpoint
        .route("/pending", get(handlers::get_pending_messages))
        
        // Chain sync status - matches pyaleph /sync/status
        .route("/sync/status", get(handlers::get_sync_status))
        
        // WebSocket - real-time message streaming
        .route("/ws", get(websocket::ws_handler))
}

/// API ws0 routes (WebSocket compatibility with pyaleph)
/// Python uses /api/ws0/messages while we have /api/v0/ws
/// API v1 routes
fn api_v1() -> Router<Arc<AppState>> {
    Router::new()
        .route("/posts.json", get(handlers::get_posts_v1))
        .route("/posts", get(handlers::get_posts_v1))
}

fn api_ws0() -> Router<Arc<AppState>> {
    Router::new()
        // WebSocket messages endpoint (pyaleph compatibility)
        // Supports query params: addresses, channels, msgTypes, hashes, history
        .route("/messages", get(websocket::ws_handler))
        // WebSocket status endpoint
        .route("/status", get(handlers::health_check))
}

/// Legacy routes for backwards compatibility
fn legacy_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Direct access to common endpoints
        .route("/messages.json", get(handlers::list_messages))
        .route("/aggregates/:address.json", get(handlers::get_aggregates))
        .route("/posts.json", get(handlers::get_posts))
        
        // Legacy pending/sync at root level
        .route("/pending", get(handlers::get_pending_messages))
        .route("/sync/status", get(handlers::get_sync_status))
        
        // Health check
        .route("/", get(handlers::health_check))
        .route("/health", get(handlers::health_check))
}

/// Internal/admin routes
fn internal_routes() -> Router<Arc<AppState>> {
    Router::new()
        // Prometheus metrics
        .route("/metrics", get(handlers::prometheus_metrics))
        
        // Detailed status
        .route("/status", get(handlers::get_detailed_status))
        
        // Chain sync status (also available at /sync)
        .route("/sync", get(handlers::get_sync_status))
        
        // Pending messages (also available at /pending)
        .route("/pending", get(handlers::get_pending_messages))
        
        // Cache stats
        .route("/cache", get(handlers::get_cache_stats))
        
        // Debug endpoints (should be protected in production)
        .route("/debug/config", get(handlers::get_config_debug))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_router_creation() {
        // Just verify routes can be created without panic
        let _routes = api_v0();
    }
}
