//! Application state for API handlers

use std::sync::Arc;
use sqlx::PgPool;

use crate::config::Config;
use crate::services::{CryptoService, StorageService, CostService};
use crate::services::ipfs::IpfsService;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: Option<PgPool>,
    pub crypto: Arc<CryptoService>,
    pub storage: Option<Arc<StorageService>>,
    pub ipfs: Arc<IpfsService>,
    pub cost: Arc<CostService>,
}

impl AppState {
    /// Create new application state
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        let crypto = Arc::new(CryptoService::new());
        let ipfs = Arc::new(IpfsService::new(&config.ipfs));
        let cost = Arc::new(CostService::new());
        
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
        }
    }
    
    /// Set the database pool
    pub fn with_db(mut self, pool: PgPool) -> Self {
        self.db = Some(pool);
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
}
