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

    /// Get a specific aggregate by owner address and key.
    ///
    /// Used by the security permission system to look up the "security" aggregate.
    /// Returns the aggregate content as a JSON value if found.
    pub async fn get_by_key(
        pool: &PgPool,
        owner: &str,
        key: &str,
    ) -> Result<Option<AggregateDb>, sqlx::Error> {
        sqlx::query_as::<_, AggregateDb>(
            "SELECT * FROM aggregates WHERE address = $1 AND key = $2"
        )
        .bind(owner)
        .bind(key)
        .fetch_optional(pool)
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

/// Peer accessor functions
/// Matches: aleph/db/accessors/peers.py
pub struct PeerAccessor;

impl PeerAccessor {
    /// Get all peer addresses of a given type, optionally filtered by last_seen
    pub async fn get_addresses_by_type(
        pool: &PgPool,
        peer_type: &str,
        min_last_seen: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<String>, sqlx::Error> {
        if let Some(last_seen) = min_last_seen {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT address FROM peers WHERE peer_type = $1 AND last_seen >= $2"
            )
            .bind(peer_type)
            .bind(last_seen)
            .fetch_all(pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        } else {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT address FROM peers WHERE peer_type = $1"
            )
            .bind(peer_type)
            .fetch_all(pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.0).collect())
        }
    }

    /// Upsert a peer (insert or update last_seen)
    pub async fn upsert(
        pool: &PgPool,
        peer_id: &str,
        peer_type: &str,
        address: &str,
        source: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(r#"
            INSERT INTO peers (peer_id, peer_type, address, source, last_seen)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (peer_id, peer_type)
            DO UPDATE SET address = $3, source = $4, last_seen = NOW()
        "#)
        .bind(peer_id)
        .bind(peer_type)
        .bind(address)
        .bind(source)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get all HTTP peers
    pub async fn get_http_peers(pool: &PgPool) -> Result<Vec<PeerDb>, sqlx::Error> {
        sqlx::query_as::<_, PeerDb>(
            "SELECT * FROM peers WHERE peer_type = 'HTTP'"
        )
        .fetch_all(pool)
        .await
    }

    /// Remove stale peers not seen since cutoff
    pub async fn remove_stale(
        pool: &PgPool,
        peer_type: &str,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM peers WHERE peer_type = $1 AND last_seen < $2"
        )
        .bind(peer_type)
        .bind(cutoff)
        .execute(pool)
        .await?;
        Ok(result.rows_affected())
    }
}
