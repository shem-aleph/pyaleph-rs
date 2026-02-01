//! Database accessors (queries)

use sqlx::PgPool;
use crate::db::models::*;

/// Message accessor functions
pub struct MessageAccessor;

impl MessageAccessor {
    pub async fn get_by_hash(pool: &PgPool, hash: &str) -> Result<Option<MessageDb>, sqlx::Error> {
        sqlx::query_as::<_, MessageDb>(
            "SELECT * FROM messages WHERE item_hash = $1"
        )
        .bind(hash)
        .fetch_optional(pool)
        .await
    }
    
    pub async fn list(
        pool: &PgPool,
        _addresses: Option<&[String]>,
        _message_type: Option<&str>,
        _channel: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<MessageDb>, sqlx::Error> {
        // TODO: Implement with proper filtering
        sqlx::query_as::<_, MessageDb>(
            "SELECT * FROM messages ORDER BY time DESC LIMIT $1 OFFSET $2"
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await
    }
    
    pub async fn insert(pool: &PgPool, message: &MessageDb) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(&message.item_hash)
        .bind(&message.message_type)
        .bind(&message.chain)
        .bind(&message.sender)
        .bind(&message.signature)
        .bind(&message.item_type)
        .bind(&message.item_content)
        .bind(&message.channel)
        .bind(message.time)
        .execute(pool)
        .await?;
        Ok(())
    }
}

/// Aggregate accessor functions
pub struct AggregateAccessor;

impl AggregateAccessor {
    pub async fn get(
        pool: &PgPool,
        address: &str,
        _keys: Option<&[String]>,
    ) -> Result<Vec<AggregateDb>, sqlx::Error> {
        // TODO: Implement with key filtering
        sqlx::query_as::<_, AggregateDb>(
            "SELECT * FROM aggregates WHERE address = $1"
        )
        .bind(address)
        .fetch_all(pool)
        .await
    }
}

/// Balance accessor functions
pub struct BalanceAccessor;

impl BalanceAccessor {
    pub async fn get(pool: &PgPool, address: &str) -> Result<Option<BalanceDb>, sqlx::Error> {
        sqlx::query_as::<_, BalanceDb>(
            "SELECT * FROM balances WHERE address = $1"
        )
        .bind(address)
        .fetch_optional(pool)
        .await
    }
}
