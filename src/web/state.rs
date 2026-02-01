//! Application state for API handlers

use std::sync::Arc;
use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::services::{CryptoService, StorageService, CostService, Metrics};
use crate::services::ipfs::IpfsService;
use crate::services::redis::RedisService;
use crate::network::rabbitmq::RabbitMQService;
use crate::web::websocket::WsState;

/// Shared application state
#[derive(Debug)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Option<PgPool>,
    pub crypto: Arc<CryptoService>,
    pub storage: Option<Arc<StorageService>>,
    pub ipfs: Arc<IpfsService>,
    pub cost: Arc<CostService>,
    pub metrics: Arc<Metrics>,
    pub redis: Option<Arc<RedisService>>,
    pub rabbitmq: Option<Arc<RwLock<RabbitMQService>>>,
    pub ws_state: Arc<WsState>,
    pub p2p_connected: bool,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            db: self.db.clone(),
            crypto: self.crypto.clone(),
            storage: self.storage.clone(),
            ipfs: self.ipfs.clone(),
            cost: self.cost.clone(),
            metrics: self.metrics.clone(),
            redis: self.redis.clone(),
            rabbitmq: self.rabbitmq.clone(),
            ws_state: self.ws_state.clone(),
            p2p_connected: self.p2p_connected,
        }
    }
}

impl AppState {
    /// Create new application state
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        let crypto = Arc::new(CryptoService::new());
        let ipfs = Arc::new(IpfsService::new(&config.ipfs));
        let cost = Arc::new(CostService::new());
        let metrics = Arc::new(Metrics::new());
        let ws_state = Arc::new(WsState::new());
        
        // Storage service (may fail if dirs can't be created)
        let storage = StorageService::new(&config.storage)
            .map(Arc::new)
            .ok();
        
        Self {
            config,
            db: None,
            crypto,
            storage,
            ipfs,
            cost,
            metrics,
            redis: None,
            rabbitmq: None,
            ws_state,
            p2p_connected: false,
        }
    }
    
    /// Set the database pool
    pub fn with_db(mut self, pool: PgPool) -> Self {
        self.db = Some(pool.clone());
        // Update WsState with DB access
        self.ws_state = Arc::new(WsState::new().with_db(pool));
        self
    }
    
    /// Set the RabbitMQ service
    pub fn with_rabbitmq(mut self, service: RabbitMQService) -> Self {
        self.rabbitmq = Some(Arc::new(RwLock::new(service)));
        self
    }
    
    /// Set the Redis service
    pub fn with_redis(mut self, service: RedisService) -> Self {
        self.redis = Some(Arc::new(service));
        self
    }
    
    /// Set P2P connection status
    pub fn with_p2p_status(mut self, connected: bool) -> Self {
        self.p2p_connected = connected;
        self
    }
    
    /// Get database pool (panics if not set)
    pub fn db(&self) -> &PgPool {
        self.db.as_ref().expect("Database pool not initialized")
    }
    
    /// Check if database is available
    pub fn has_db(&self) -> bool {
        self.db.is_some()
    }
    
    /// Get metrics reference
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
    
    /// Get WebSocket state
    pub fn ws(&self) -> &WsState {
        &self.ws_state
    }
}
