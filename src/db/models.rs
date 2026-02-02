//! Database models
//!
//! These models match the pyaleph PostgreSQL schema for compatibility.
//! Reference: aleph/db/models.py

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use rust_decimal::Decimal;

/// Message database record
/// Matches: aleph/db/models.py MessageDb
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

/// Pending message record
/// For messages awaiting processing
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct PendingMessageDb {
    pub item_hash: String,
    pub message: serde_json::Value,
    pub reception_time: f64,
    pub fetched: bool,
    pub check_message: bool,
    pub retries: i32,
    pub next_attempt: f64,
    pub created_at: DateTime<Utc>,
    pub trusted_source: bool,
}

/// Rejected message record
/// For messages that failed validation
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RejectedMessageDb {
    pub item_hash: String,
    pub message: serde_json::Value,
    pub error_code: i32,
    pub error_message: Option<String>,
    pub rejected_at: DateTime<Utc>,
}

/// Forgotten message record
/// For messages deleted via FORGET
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ForgottenMessageDb {
    pub item_hash: String,
    pub forget_hash: String,
    pub reason: Option<String>,
    pub forgotten_at: DateTime<Utc>,
}

/// Aggregate database record
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AggregateDb {
    pub address: String,
    pub key: String,
    pub content: serde_json::Value,
    pub time: f64,
    /// Whether aggregate needs rebuilding
    pub dirty: bool,
    /// Hash of the last revision message
    pub last_revision_hash: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Aggregate element record
/// Individual contributions to an aggregate
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AggregateElementDb {
    pub id: i64,
    pub address: String,
    pub key: String,
    pub item_hash: String,
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
    #[serde(rename = "ref")]
    pub ref_: Option<String>,
    pub channel: Option<String>,
    pub time: f64,
    /// Original post hash (for amends)
    pub original_item_hash: Option<String>,
    /// Hash of the latest amend
    pub latest_amend: Option<String>,
    /// List of all amend hashes
    pub amends: Option<serde_json::Value>,
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

/// File tag record
/// For tagging files for garbage collection
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct FileTagDb {
    pub item_hash: String,
    pub tag: String,
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
    pub allow_amend: bool,
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
    pub payment_chain: Option<String>,
    pub allow_amend: bool,
    /// For confidential/trusted execution
    pub trusted_execution: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

/// VM version record
/// Tracks versions of programs/instances after amendments
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct VmVersionDb {
    pub item_hash: String,
    pub original_hash: String,
    pub version: i32,
    pub owner: String,
    pub created_at: DateTime<Utc>,
}

/// Chain transaction record
/// Tracks blockchain confirmations
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ChainTxDb {
    pub hash: String,
    pub chain: String,
    pub height: i64,
    pub item_hash: String,
    pub publisher: Option<String>,
    pub protocol: String,
    pub created_at: DateTime<Utc>,
}

/// Account cost record
/// Tracks resource usage costs per account
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct AccountCostDb {
    pub address: String,
    pub storage_cost: Decimal,
    pub compute_cost: Decimal,
    pub total_cost: Decimal,
    pub last_calculated: DateTime<Utc>,
}

/// Chain sync state record
/// Tracks synchronization progress per chain
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ChainSyncStateDb {
    pub chain: String,
    pub sync_type: String,
    pub last_height: i64,
    pub last_sync: DateTime<Utc>,
}
