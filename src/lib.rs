//! Aleph Core - Rust implementation of the Aleph.im network node
//!
//! This is a port of the Python pyaleph implementation to Rust for improved
//! performance, memory safety, and broader deployment options.
//!
//! # Architecture
//!
//! The crate is organized into several modules:
//!
//! - [`types`] - Core data types (Message, Chain, etc.)
//! - [`handlers`] - Message processing logic
//! - [`services`] - Business logic services (crypto, storage, cost)
//! - [`db`] - Database models and queries
//! - [`web`] - HTTP API handlers
//! - [`network`] - P2P and RabbitMQ integration
//! - [`chains`] - Blockchain connectors
//! - [`jobs`] - Background tasks
//!
//! # Example
//!
//! ```rust,no_run
//! use aleph_core::{Config, web, db};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config::default();
//!     let pool = db::create_pool(&config.database).await?;
//!     web::start_server_with_db(&config, pool).await?;
//!     Ok(())
//! }
//! ```

pub mod chains;
pub mod config;
pub mod db;
pub mod handlers;
pub mod jobs;
pub mod network;
pub mod permissions;
pub mod schemas;
pub mod services;
pub mod storage;
pub mod types;
pub mod utils;
pub mod web;

// Re-export commonly used items at crate root
pub use config::Config;

// Selective re-exports from types (avoid glob export)
pub use types::{
    Chain, Message, MessageType, MessageStatus, ErrorCode,
    ProcessingStatus, MessageOrigin, ItemType,
};
