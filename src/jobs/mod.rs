//! Background jobs and scheduled tasks
//!
//! This module contains all the background processing jobs:
//! - Message processor: processes pending messages
//! - Chain sync: indexes blockchain events
//! - Garbage collector: cleans up orphaned files
//! - Balance tracker: tracks ALEPH token balances
//! - Cron scheduler: runs scheduled tasks
//! - Backfill: populates derived tables from messages on startup

pub mod message_processor;
pub mod chain_sync;
pub mod garbage_collector;
pub mod balance_tracker;
pub mod cleanup;
pub mod cron;
pub mod backfill;
pub mod p2p_consumer;

use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{info, error};
use sqlx::PgPool;

use crate::config::Config;
use crate::services::{Metrics, CryptoService};
use crate::services::ipfs::IpfsService;

/// Job manager that runs all background jobs
pub struct JobManager {
    handles: Vec<JoinHandle<()>>,
}

impl JobManager {
    /// Create a new job manager and start all jobs
    pub async fn start(
        db: PgPool,
        config: Arc<Config>,
        crypto: Arc<CryptoService>,
        ipfs: Arc<IpfsService>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut handles = Vec::new();
        
        // Run backfill before starting other jobs
        // This ensures derived tables (posts, aggregates) are populated
        // from any messages that were synced but not processed
        {
            info!("Checking if backfill is needed...");
            match backfill::run_startup_backfill(&db).await {
                Ok(result) => {
                    if result.posts_inserted > 0 || result.aggregates_inserted > 0 {
                        info!("Startup backfill completed: {}", result);
                    }
                }
                Err(e) => {
                    error!("Startup backfill failed (continuing anyway): {}", e);
                }
            }
        }
        
        // Start message processor
        {
            let ctx = Arc::new(message_processor::ProcessorContext::new(
                db.clone(),
                crypto.clone(),
                ipfs.clone(),
                config.clone(),
            ));
            
            let handle = tokio::spawn(async move {
                message_processor::run(ctx).await;
            });
            handles.push(handle);
            info!("Started message processor job");
        }
        
        // Start chain indexers
        {
            let chains_config = config.chains.clone();
            let db_clone = db.clone();
            let metrics_clone = metrics.clone();
            
            let handle = tokio::spawn(async move {
                let indexers = crate::chains::start_indexers(&chains_config).await;
                if !indexers.is_empty() {
                    crate::chains::run_chain_sync(indexers, db_clone, metrics_clone).await;
                }
            });
            handles.push(handle);
            info!("Started chain sync job");
        }
        
        // Start garbage collector
        if config.storage.garbage_collection {
            let db_clone = db.clone();
            let ipfs_clone = ipfs.clone();
            let config_clone = config.clone();
            let metrics_clone = metrics.clone();
            
            let handle = tokio::spawn(async move {
                garbage_collector::run(db_clone, ipfs_clone, config_clone, metrics_clone).await;
            });
            handles.push(handle);
            info!("Started garbage collector job");
        }
        
        // Start balance tracker
        if config.aleph.balances.enabled {
            let db_clone = db.clone();
            let config_clone = config.clone();
            let metrics_clone = metrics.clone();
            
            let handle = tokio::spawn(async move {
                balance_tracker::run(db_clone, config_clone, metrics_clone).await;
            });
            handles.push(handle);
            info!("Started balance tracker job");
        }
        
        // Start TX packer (for sync message publishing)
        {
            let db_clone = db.clone();
            let ipfs_clone = ipfs.clone();
            let config_clone = config.clone();
            
            let handle = tokio::spawn(async move {
                match crate::chains::tx_packer::TxPacker::new(
                    db_clone,
                    ipfs_clone,
                    config_clone,
                ).await {
                    Ok(packer) => packer.run().await,
                    Err(e) => error!("Failed to start TX packer: {}", e),
                }
            });
            handles.push(handle);
            info!("Started TX packer job");
        }
        
        // Start cron scheduler
        {
            let db_clone = db.clone();
            let config_clone = config.clone();
            let metrics_clone = metrics.clone();
            
            let handle = tokio::spawn(async move {
                cron::run_scheduler(db_clone, config_clone, metrics_clone).await;
            });
            handles.push(handle);
            info!("Started cron scheduler");
        }
        
        // Start P2P consumer (RabbitMQ → pending_messages)
        if config.rabbitmq.enabled {
            let db_clone = db.clone();
            let config_clone = config.clone();
            
            let handle = tokio::spawn(async move {
                let ctx = Arc::new(
                    p2p_consumer::P2pConsumerContext::new(db_clone, config_clone).await,
                );
                p2p_consumer::run(ctx).await;
            });
            handles.push(handle);
            info!("Started P2P consumer job");
        }
        
        // Start content fetch service (downloads content for storage/ipfs messages)
        if config.rabbitmq.enabled {
            let db_clone = db.clone();
            let config_clone = config.clone();
            let ipfs_clone = ipfs.clone();
            
            let handle = tokio::spawn(async move {
                let ctx = Arc::new(
                    crate::services::content_fetch::ContentFetchContext::new(
                        db_clone,
                        config_clone,
                        ipfs_clone,
                    ).await,
                );
                crate::services::content_fetch::run(ctx).await;
            });
            handles.push(handle);
            info!("Started content fetch service");
        }
        
        Self { handles }
    }
    
    /// Stop all jobs
    pub async fn stop(self) {
        for handle in self.handles {
            handle.abort();
        }
        info!("All jobs stopped");
    }
    
    /// Wait for all jobs to complete (they shouldn't unless there's an error)
    pub async fn wait(self) {
        for handle in self.handles {
            if let Err(e) = handle.await {
                error!("Job error: {}", e);
            }
        }
    }
}

/// Job configuration from config file
#[derive(Debug, Clone)]
pub struct JobConfig {
    /// Message processing batch size
    pub message_batch_size: i64,
    /// Message processing interval (ms)
    pub process_interval_ms: u64,
    /// Maximum retries for failed messages
    pub max_retries: i32,
    /// Garbage collection period (seconds)
    pub gc_period: u64,
    /// Balance update interval (seconds)
    pub balance_interval: u64,
    /// Chain sync interval (seconds)
    pub chain_sync_interval: u64,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            message_batch_size: 100,
            process_interval_ms: 1000,
            max_retries: 10,
            gc_period: 3600,
            balance_interval: 300,
            chain_sync_interval: 12,
        }
    }
}

impl From<&Config> for JobConfig {
    fn from(config: &Config) -> Self {
        Self {
            message_batch_size: config.aleph.jobs.message_batch_size,
            process_interval_ms: config.aleph.jobs.process_interval_ms,
            max_retries: config.aleph.jobs.max_retries,
            gc_period: config.aleph.jobs.garbage_collector_period,
            balance_interval: config.aleph.balances.update_interval,
            chain_sync_interval: config.aleph.jobs.chain_sync_interval,
        }
    }
}
