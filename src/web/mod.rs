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
use tokio::sync::mpsc;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::Config;
use crate::network::rabbitmq::{P2PMessage, RabbitMQConfig, RabbitMQService};
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
    let state = Arc::new(AppState::new(config.clone()).with_db(pool.clone()));
    
    // Create RabbitMQ service and WebSocket connection if enabled
    let ws_state = state.ws_state.clone();
    
    if config.rabbitmq.enabled {
        // Create RabbitMQ config
        let rabbitmq_config = RabbitMQConfig {
            url: config.rabbitmq.url.clone(),
            pub_exchange: config.rabbitmq.pub_exchange.clone(),
            sub_exchange: config.rabbitmq.sub_exchange.clone(),
            message_exchange: config.rabbitmq.message_exchange.clone(),
            pending_message_exchange: config.rabbitmq.pending_message_exchange.clone(),
            pending_tx_exchange: config.rabbitmq.pending_tx_exchange.clone(),
            queue_incoming: "aleph-core-ws".to_string(),
            queue_outgoing: "aleph-core-outgoing".to_string(),
            routing_key: "#".to_string(),
        };
        
        // Create channel for passing messages from RabbitMQ to WebSocket
        let (p2p_tx, p2p_rx) = mpsc::channel::<P2PMessage>(10000);
        
        // Spawn RabbitMQ consumer
        let rmq_config = rabbitmq_config.clone();
        tokio::spawn(async move {
            crate::network::rabbitmq::run_consumer(rmq_config, p2p_tx).await;
        });
        
        // Spawn WebSocket-RabbitMQ bridge
        tokio::spawn(async move {
            websocket::connect_to_rabbitmq(ws_state, p2p_rx).await;
        });
        
        info!("WebSocket connected to RabbitMQ for real-time updates");
    }
    
    let app = create_router(config, state);
    
    let addr = format!("{}:{}", config.api.host, config.api.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    info!("API server listening on {} (with database)", addr);
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// Start the web server with full services (DB, RabbitMQ, etc.)
pub async fn start_server_full(
    config: &Config,
    pool: sqlx::PgPool,
    rabbitmq: Option<RabbitMQService>,
) -> anyhow::Result<()> {
    let mut state = AppState::new(config.clone()).with_db(pool.clone());
    
    if let Some(rmq) = rabbitmq {
        state = state.with_rabbitmq(rmq);
        state = state.with_p2p_status(true);
    }
    
    let state = Arc::new(state);
    
    // Start WebSocket-RabbitMQ bridge if configured
    if config.rabbitmq.enabled && config.api.websocket_enabled {
        let ws_state = state.ws_state.clone();
        let rabbitmq_config = RabbitMQConfig {
            url: config.rabbitmq.url.clone(),
            pub_exchange: config.rabbitmq.pub_exchange.clone(),
            sub_exchange: config.rabbitmq.sub_exchange.clone(),
            message_exchange: config.rabbitmq.message_exchange.clone(),
            pending_message_exchange: config.rabbitmq.pending_message_exchange.clone(),
            pending_tx_exchange: config.rabbitmq.pending_tx_exchange.clone(),
            queue_incoming: "aleph-core-ws".to_string(),
            queue_outgoing: "aleph-core-outgoing".to_string(),
            routing_key: "#".to_string(),
        };
        
        let (p2p_tx, p2p_rx) = mpsc::channel::<P2PMessage>(10000);
        
        // Spawn consumer
        let rmq_config = rabbitmq_config.clone();
        tokio::spawn(async move {
            crate::network::rabbitmq::run_consumer(rmq_config, p2p_tx).await;
        });
        
        // Spawn WebSocket bridge
        tokio::spawn(async move {
            websocket::connect_to_rabbitmq(ws_state, p2p_rx).await;
        });
        
        info!("WebSocket real-time streaming enabled");
    }
    
    let app = create_router(config, state);
    
    let addr = format!("{}:{}", config.api.host, config.api.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    info!("API server listening on {} (full mode)", addr);
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
        .route("/monitor.html", axum::routing::get(handlers::monitor_html))
        
        // WebSocket endpoint
        .route("/ws", axum::routing::get(websocket::ws_handler))
        
        // API v0 routes (compatibility with pyaleph)
        .nest("/api/v0", routes::api_v0())
        
        // API v1 routes
        .nest("/api/v1", routes::api_v1())
        
        // API ws0 routes (WebSocket compatibility)
        .nest("/api/ws0", routes::api_ws0())
        
        // State and middleware
        .with_state(state)
        .layer(cors)
}
