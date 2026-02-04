//! PostgreSQL implementation of the Database trait for message handlers
//!
//! This bridges the abstract Database trait with the actual PgPool,
//! allowing handlers to perform database operations via the trait interface.

use async_trait::async_trait;
use sqlx::PgPool;

use crate::handlers::{Database, PostRecord, FilePinRecord};
use crate::types::{Message, MessageType, ItemType, Chain, ProcessingStatus};

/// Parse a MessageType from its DB string representation (UPPERCASE)
fn parse_message_type(s: &str) -> Result<MessageType, String> {
    match s {
        "AGGREGATE" => Ok(MessageType::Aggregate),
        "POST" => Ok(MessageType::Post),
        "STORE" => Ok(MessageType::Store),
        "PROGRAM" => Ok(MessageType::Program),
        "INSTANCE" => Ok(MessageType::Instance),
        "FORGET" => Ok(MessageType::Forget),
        _ => Err(format!("Unknown message type: {}", s)),
    }
}

/// Parse an ItemType from its DB string representation (lowercase)
fn parse_item_type(s: &str) -> Result<ItemType, String> {
    match s {
        "inline" => Ok(ItemType::Inline),
        "ipfs" => Ok(ItemType::Ipfs),
        "storage" => Ok(ItemType::Storage),
        _ => Err(format!("Unknown item type: {}", s)),
    }
}

/// PostgreSQL-backed implementation of the handlers::Database trait
pub struct PgDatabase {
    pool: PgPool,
}

impl PgDatabase {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Database for PgDatabase {
    async fn get_message(&self, item_hash: &str) -> Result<Option<Message>, String> {
        let row: Option<(String, String, String, String, String, String, Option<String>, Option<String>, f64)> =
            sqlx::query_as(
                "SELECT item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time \
                 FROM messages WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time)) => {
                let chain_parsed: Chain = serde_json::from_value(serde_json::Value::String(chain))
                    .map_err(|e| format!("Failed to parse chain: {}", e))?;
                Ok(Some(Message {
                    item_hash,
                    message_type: parse_message_type(&message_type)?,
                    chain: chain_parsed,
                    sender,
                    signature,
                    item_type: parse_item_type(&item_type)?,
                    item_content,
                    channel,
                    time,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_message(&self, message: &Message) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&message.item_hash)
        .bind(message.message_type.to_string())
        .bind(message.chain.to_string())
        .bind(&message.sender)
        .bind(&message.signature)
        .bind(message.item_type.to_string())
        .bind(&message.item_content)
        .bind(&message.channel)
        .bind(message.time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_message_status(&self, _item_hash: &str, _status: &ProcessingStatus) -> Result<(), String> {
        // Messages table doesn't have a status column; status is tracked by which table the message is in
        Ok(())
    }

    async fn get_aggregate(&self, address: &str, key: &str) -> Result<Option<serde_json::Value>, String> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT content FROM aggregates WHERE address = $1 AND key = $2"
        )
        .bind(address)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(v,)| v))
    }

    async fn store_aggregate(&self, address: &str, key: &str, content: &serde_json::Value, time: f64) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO aggregates (address, key, content, time, dirty, created_at) \
             VALUES ($1, $2, $3, $4, false, NOW()) \
             ON CONFLICT (address, key) DO UPDATE SET content = $3, time = $4, dirty = false"
        )
        .bind(address)
        .bind(key)
        .bind(content)
        .bind(time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_aggregate_time(&self, address: &str, key: &str) -> Result<Option<f64>, String> {
        let row: Option<(f64,)> = sqlx::query_as(
            "SELECT time FROM aggregates WHERE address = $1 AND key = $2"
        )
        .bind(address)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(t,)| t))
    }

    async fn mark_aggregate_dirty(&self, address: &str, key: &str) -> Result<(), String> {
        sqlx::query("UPDATE aggregates SET dirty = true WHERE address = $1 AND key = $2")
            .bind(address)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn mark_aggregate_clean(&self, address: &str, key: &str) -> Result<(), String> {
        sqlx::query("UPDATE aggregates SET dirty = false WHERE address = $1 AND key = $2")
            .bind(address)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_post(&self, item_hash: &str) -> Result<Option<PostRecord>, String> {
        let row: Option<(String, String, String, Option<String>, serde_json::Value, Option<String>, f64, Option<String>, Option<String>)> =
            sqlx::query_as(
                "SELECT item_hash, address, post_type, ref_, content, channel, time, original_item_hash, latest_amend \
                 FROM posts WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, address, post_type, ref_, content, channel, time, original_item_hash, latest_amend)) => {
                Ok(Some(PostRecord {
                    item_hash,
                    address,
                    post_type,
                    ref_,
                    content,
                    channel,
                    time,
                    original_item_hash,
                    latest_amend,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_post(&self, post: &PostRecord) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO posts (item_hash, address, post_type, content, ref_, channel, time, original_item_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&post.item_hash)
        .bind(&post.address)
        .bind(&post.post_type)
        .bind(&post.content)
        .bind(&post.ref_)
        .bind(&post.channel)
        .bind(post.time)
        .bind(&post.original_item_hash)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_post_latest_amend(&self, original_hash: &str, amend_hash: &str) -> Result<(), String> {
        sqlx::query("UPDATE posts SET latest_amend = $1 WHERE item_hash = $2")
            .bind(amend_hash)
            .bind(original_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_file_pin(&self, item_hash: &str) -> Result<Option<FilePinRecord>, String> {
        let row: Option<(String, String, i64, Option<String>, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as(
                "SELECT item_hash, owner, size, content_type, created_at FROM file_pins WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, owner, size, content_type, created_at)) => {
                Ok(Some(FilePinRecord {
                    item_hash,
                    owner,
                    size: size as u64,
                    content_type,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_file_pin(&self, pin: &FilePinRecord) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO file_pins (item_hash, owner, size, content_type, created_at) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&pin.item_hash)
        .bind(&pin.owner)
        .bind(pin.size as i64)
        .bind(&pin.content_type)
        .bind(pin.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_file_pin(&self, item_hash: &str, owner: &str) -> Result<(), String> {
        sqlx::query("UPDATE file_pins SET owner = $1 WHERE item_hash = $2")
            .bind(owner)
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remove_file_pin(&self, item_hash: &str, _owner: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM file_pins WHERE item_hash = $1")
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_forgotten_hashes(&self, hashes: &[String]) -> Result<Vec<String>, String> {
        if hashes.is_empty() {
            return Ok(vec![]);
        }
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT item_hash FROM forgotten_messages WHERE item_hash = ANY($1)"
        )
        .bind(hashes)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    async fn mark_forgotten(&self, item_hash: &str, forget_hash: &str, reason: Option<&str>) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO forgotten_messages (item_hash, forget_hash, reason, forgotten_at) \
             VALUES ($1, $2, $3, NOW()) ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(item_hash)
        .bind(forget_hash)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_dependent_vms(&self, file_hash: &str) -> Result<Vec<String>, String> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT item_hash FROM programs WHERE code_ref = $1 OR runtime_ref = $1 \
             UNION \
             SELECT item_hash FROM instances WHERE rootfs_ref = $1"
        )
        .bind(file_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    async fn get_balance(&self, address: &str, chain: &str) -> Result<Option<rust_decimal::Decimal>, String> {
        let row: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT balance FROM balances WHERE address = $1 AND chain = $2"
        )
        .bind(address)
        .bind(chain)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(b,)| b))
    }

    async fn get_credit_balance(&self, address: &str) -> Result<Option<rust_decimal::Decimal>, String> {
        let row: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT balance FROM credit_balances WHERE address = $1"
        )
        .bind(address)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(b,)| b))
    }
}
