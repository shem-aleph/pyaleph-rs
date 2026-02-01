//! Database models

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use rust_decimal::Decimal;

/// Message database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct MessageDb {
    pub item_hash: String,
    pub message_type: String,
    pub chain: String,
    pub sender: String,
    pub signature: String,
    pub item_type: String,
    pub item_content: Option<String>,
    pub channel: Option<String>,
    pub time: f64,
    pub created_at: DateTime<Utc>,
}

/// Aggregate database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AggregateDb {
    pub address: String,
    pub key: String,
    pub content: serde_json::Value,
    pub time: f64,
    pub created_at: DateTime<Utc>,
}

/// Post database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct PostDb {
    pub item_hash: String,
    pub address: String,
    pub post_type: String,
    pub content: serde_json::Value,
    pub ref_: Option<String>,
    pub channel: Option<String>,
    pub time: f64,
    pub created_at: DateTime<Utc>,
}

/// Balance database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct BalanceDb {
    pub address: String,
    pub chain: String,
    pub balance: Decimal,
    pub updated_at: DateTime<Utc>,
}

/// Credit balance database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct CreditBalanceDb {
    pub address: String,
    pub balance: Decimal,
    pub expiration: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

/// File pin database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct FilePinDb {
    pub item_hash: String,
    pub owner: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Program database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ProgramDb {
    pub item_hash: String,
    pub owner: String,
    pub code_ref: String,
    pub runtime_ref: String,
    pub memory: i32,
    pub vcpus: i32,
    pub created_at: DateTime<Utc>,
}

/// Instance database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct InstanceDb {
    pub item_hash: String,
    pub owner: String,
    pub rootfs_ref: String,
    pub memory: i32,
    pub vcpus: i32,
    pub payment_type: Option<String>,
    pub created_at: DateTime<Utc>,
}
