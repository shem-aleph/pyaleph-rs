//! Background jobs module
//!
//! Handles periodic tasks and message processing queues.

pub mod message_processor;
pub mod chain_sync;
pub mod cleanup;

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::config::Config;

/// Job manager for background tasks
pub struct JobManager {
    shutdown_tx: mpsc::Sender<()>,
}

impl JobManager {
    /// Start all background jobs
    pub async fn start(config: Arc<Config>) -> Self {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        
        // Start message processor
        let config_clone = config.clone();
        tokio::spawn(async move {
            info!("Starting message processor job");
            message_processor::run(config_clone).await;
        });
        
        // Start chain sync job (if chains are enabled)
        let config_clone = config.clone();
        tokio::spawn(async move {
            info!("Starting chain sync job");
            chain_sync::run(config_clone).await;
        });
        
        // Start cleanup job
        let config_clone = config.clone();
        tokio::spawn(async move {
            info!("Starting cleanup job");
            cleanup::run(config_clone).await;
        });
        
        Self { shutdown_tx }
    }
    
    /// Signal all jobs to stop
    pub async fn stop(&self) {
        let _ = self.shutdown_tx.send(()).await;
    }
}
