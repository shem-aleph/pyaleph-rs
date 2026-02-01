//! Aleph Core - Rust implementation of the Aleph.im network node
//!
//! This is a port of the Python pyaleph implementation to Rust for improved
//! performance, memory safety, and broader deployment options.

pub mod chains;
pub mod config;
pub mod db;
pub mod handlers;
pub mod jobs;
pub mod network;
pub mod schemas;
pub mod services;
pub mod storage;
pub mod types;
pub mod utils;
pub mod web;

pub use config::Config;
pub use types::*;
