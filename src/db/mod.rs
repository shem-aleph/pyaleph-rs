//! Database module
//!
//! Handles PostgreSQL database connections and queries.

pub mod models;
pub mod accessors;
pub mod migrations;
pub mod pool;
pub mod query_builder;
pub mod sync_state;

pub use sync_state::SyncStateAccessor;
pub use models::*;
pub use query_builder::{QueryBuilder, parse_csv_param, validate_sort_column};

use sqlx::postgres::PgPool;
use crate::config::DatabaseConfig;

/// Create a database connection pool
pub async fn create_pool(config: &DatabaseConfig) -> Result<PgPool, sqlx::Error> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.url)
        .await?;
    
    Ok(pool)
}

/// Initialize database with migrations
pub async fn init_db(config: &DatabaseConfig) -> Result<PgPool, sqlx::Error> {
    let pool = create_pool(config).await?;
    migrations::run_migrations(&pool).await?;
    Ok(pool)
}
