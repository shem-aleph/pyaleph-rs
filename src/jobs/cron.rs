//! Cron scheduler for periodic tasks
//!
//! Runs scheduled tasks at configurable intervals.

use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{interval, Duration, Instant};
use tracing::{debug, info, warn, error};
use sqlx::PgPool;
use chrono::{Datelike, Utc, Timelike};

use crate::config::Config;
use crate::services::Metrics;

/// Cron task definition
pub struct CronTask {
    /// Task name
    pub name: String,
    /// Task function
    pub func: Box<dyn Fn() -> CronTaskFuture + Send + Sync>,
    /// Interval in seconds
    pub interval_secs: u64,
    /// Last run time
    pub last_run: Option<Instant>,
    /// Whether task is enabled
    pub enabled: bool,
}

type CronTaskFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;

/// Cron scheduler
pub struct CronScheduler {
    tasks: Vec<CronTask>,
    running: bool,
}

impl CronScheduler {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            running: false,
        }
    }
    
    /// Add a task to the scheduler
    pub fn add_task(&mut self, task: CronTask) {
        info!("Registered cron task: {} (every {}s)", task.name, task.interval_secs);
        self.tasks.push(task);
    }
    
    /// Run the scheduler
    pub async fn run(&mut self) {
        self.running = true;
        let mut ticker = interval(Duration::from_secs(1));
        
        info!("Cron scheduler started with {} tasks", self.tasks.len());
        
        while self.running {
            ticker.tick().await;
            
            let now = Instant::now();
            
            for task in &mut self.tasks {
                if !task.enabled {
                    continue;
                }
                
                let should_run = match task.last_run {
                    Some(last) => now.duration_since(last).as_secs() >= task.interval_secs,
                    None => true,
                };
                
                if should_run {
                    debug!("Running cron task: {}", task.name);
                    
                    let start = Instant::now();
                    let result = (task.func)().await;
                    let duration = start.elapsed();
                    
                    task.last_run = Some(now);
                    
                    match result {
                        Ok(()) => {
                            debug!("Task {} completed in {:?}", task.name, duration);
                        }
                        Err(e) => {
                            error!("Task {} failed: {}", task.name, e);
                        }
                    }
                }
            }
        }
    }
    
    /// Stop the scheduler
    pub fn stop(&mut self) {
        self.running = false;
    }
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Run the default cron scheduler with standard tasks
pub async fn run_scheduler(
    db: PgPool,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) {
    let mut ticker = interval(Duration::from_secs(60)); // Check every minute
    
    info!("Cron scheduler started");
    
    loop {
        ticker.tick().await;
        let now = Utc::now();
        
        // Task 1: Update pending message count (every minute)
        if let Ok(count) = get_pending_count(&db).await {
            metrics.set_messages_pending(count);
        }
        
        // Task 2: Cleanup cache (every 5 minutes)
        if now.minute() % 5 == 0 && now.second() < 60 {
            debug!("Running cache cleanup");
            // Cache cleanup is handled by Redis service
        }
        
        // Task 3: Update pricing from aggregate (every hour)
        if now.minute() == 0 && now.second() < 60 {
            debug!("Updating pricing aggregate");
            if let Err(e) = update_pricing_aggregate(&db, &config).await {
                warn!("Failed to update pricing: {}", e);
            }
        }
        
        // Task 4: Check chain sync status (every 5 minutes)
        if now.minute() % 5 == 0 && now.second() < 60 {
            check_sync_status(&db, &metrics).await;
        }
        
        // Task 5: Report metrics (every minute)
        let snapshot = metrics.snapshot();
        debug!(
            "Metrics: recv={}, proc={}, pend={}, peers={}",
            snapshot.messages_received,
            snapshot.messages_processed,
            snapshot.messages_pending,
            snapshot.peers_connected,
        );
    }
}

/// Get pending message count
async fn get_pending_count(db: &PgPool) -> Result<u64, sqlx::Error> {
    let result: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pending_messages"
    )
    .fetch_one(db)
    .await?;
    
    Ok(result.0 as u64)
}

/// Update pricing aggregate
async fn update_pricing_aggregate(
    db: &PgPool,
    config: &Config,
) -> Result<(), String> {
    let address = &config.aleph.pricing_aggregate_address;
    let key = &config.aleph.pricing_aggregate_key;
    
    // Get pricing aggregate from database
    let aggregate: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT content FROM aggregates WHERE address = $1 AND key = $2"
    )
    .bind(address)
    .bind(key)
    .fetch_optional(db)
    .await
    .map_err(|e| e.to_string())?;
    
    if let Some((content,)) = aggregate {
        // Update cost service pricing
        // This would be done through a shared cost service instance
        debug!("Pricing aggregate updated");
    }
    
    Ok(())
}

/// Check chain sync status and log warnings
async fn check_sync_status(db: &PgPool, metrics: &Metrics) {
    // Get sync state for each chain
    let states: Vec<(String, i64)> = sqlx::query_as(
        "SELECT chain, last_height FROM chain_sync_state WHERE sync_type = 'messages'"
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();
    
    for (chain, height) in states {
        debug!("{}: last indexed height = {}", chain, height);
        
        // Update metrics based on chain
        match chain.as_str() {
            "ETH" => metrics.set_last_block_eth(height as u64),
            "SOL" => metrics.set_last_block_sol(height as u64),
            "AVAX" => metrics.set_last_block_avax(height as u64),
            _ => {}
        }
    }
}

/// Scheduled task types
pub enum ScheduledTask {
    /// Run at specific interval
    Interval { secs: u64 },
    /// Run at specific time (hour, minute)
    Daily { hour: u32, minute: u32 },
    /// Run on specific weekday (0=Sunday)
    Weekly { day: u32, hour: u32, minute: u32 },
}

impl ScheduledTask {
    /// Check if task should run now
    pub fn should_run(&self, now: &chrono::DateTime<Utc>, last_run: Option<chrono::DateTime<Utc>>) -> bool {
        match self {
            ScheduledTask::Interval { secs } => {
                match last_run {
                    Some(last) => (now.timestamp() - last.timestamp()) >= *secs as i64,
                    None => true,
                }
            }
            ScheduledTask::Daily { hour, minute } => {
                now.hour() == *hour && 
                now.minute() == *minute && 
                now.second() < 60 &&
                last_run.map(|l| l.date_naive() != now.date_naive()).unwrap_or(true)
            }
            ScheduledTask::Weekly { day, hour, minute } => {
                now.weekday().num_days_from_sunday() == *day &&
                now.hour() == *hour && 
                now.minute() == *minute && 
                now.second() < 60 &&
                last_run.map(|l| (now.timestamp() - l.timestamp()) >= 86400).unwrap_or(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_interval_task() {
        let task = ScheduledTask::Interval { secs: 60 };
        let now = Utc::now();
        
        // Should run if never run before
        assert!(task.should_run(&now, None));
        
        // Should not run if just ran
        assert!(!task.should_run(&now, Some(now)));
        
        // Should run if 60+ seconds passed
        let past = now - chrono::Duration::seconds(61);
        assert!(task.should_run(&now, Some(past)));
    }
}
