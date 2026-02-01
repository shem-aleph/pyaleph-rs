//! Web API module
//!
//! This module provides the HTTP API for the Aleph node, compatible with
//! the original pyaleph API.

pub mod routes;
pub mod handlers;
pub mod middleware;
pub mod state;
pub mod websocket;

use axum::Router;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::Config;
use state::AppState;

/// Start the web server
pub async fn start_server(config: &Config) -> anyhow::Result<()> {
    let state = Arc::new(AppState::new(config.clone()));
    let app = create_router(config, state);
    
    let addr = format!("{}:{}", config.api.host, config.api.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    info!("API server listening on {}", addr);
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// Start the web server with database
pub async fn start_server_with_db(config: &Config, pool: sqlx::PgPool) -> anyhow::Result<()> {
    let state = Arc::new(AppState::new(config.clone()).with_db(pool));
    let app = create_router(config, state);
    
    let addr = format!("{}:{}", config.api.host, config.api.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    info!("API server listening on {} (with database)", addr);
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// Create the API router with all routes
pub fn create_router(config: &Config, state: Arc<AppState>) -> Router {
    // CORS configuration
    let cors = if config.api.cors_enabled {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        CorsLayer::new()
    };
    
    Router::new()
        // Health check
        .route("/", axum::routing::get(handlers::health_check))
        .route("/health", axum::routing::get(handlers::health_check))
        
        // WebSocket endpoint
        .route("/ws", axum::routing::get(websocket::ws_handler))
        
        // API v0 routes (compatibility with pyaleph)
        .nest("/api/v0", routes::api_v0())
        
        // State and middleware
        .with_state(state)
        .layer(cors)
}
