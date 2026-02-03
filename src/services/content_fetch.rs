//! Peer content fetching service
//!
//! For messages with `item_type` = storage or ipfs, the actual content must be
//! fetched separately. This service reads peer API URLs from Redis (populated
//! by the alive-message handler) and tries to download content from random
//! peers before falling back to the IPFS gateway.
//!
//! Peer API: `GET {peer}/api/v0/storage/{item_hash}`
//! Response: `{"status": "success", "content": "<base64-encoded>"}`
//!
//! Reference: aleph/services/p2p/http.py

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use rand::seq::SliceRandom;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::services::ipfs::IpfsService;
use crate::types::ItemType;

/// Timeout for peer content fetch HTTP requests
const PEER_FETCH_TIMEOUT_SECS: u64 = 10;

/// Maximum peers to try before falling back to IPFS
const MAX_PEER_ATTEMPTS: usize = 3;

/// Interval between content-fetch runs (process unfetched pending messages)
const FETCH_INTERVAL_MS: u64 = 2000;

/// Batch size of unfetched messages to process per tick
const FETCH_BATCH_SIZE: i64 = 50;

/// Peer storage API response
#[derive(Debug, Deserialize)]
struct PeerStorageResponse {
    status: String,
    #[serde(default)]
    content: Option<String>,
}

/// Content fetch service context
pub struct ContentFetchContext {
    pub db: PgPool,
    pub config: Arc<Config>,
    pub ipfs: Arc<IpfsService>,
    pub http: Client,
    pub redis_client: Option<redis::aio::ConnectionManager>,
}

impl ContentFetchContext {
    pub async fn new(
        db: PgPool,
        config: Arc<Config>,
        ipfs: Arc<IpfsService>,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(PEER_FETCH_TIMEOUT_SECS))
            .build()
            .expect("Failed to create HTTP client");

        // Connect to Redis for api_servers set
        let redis_client = match redis::Client::open(config.redis.url.as_str()) {
            Ok(client) => match redis::aio::ConnectionManager::new(client).await {
                Ok(mgr) => Some(mgr),
                Err(e) => {
                    warn!("Content fetch: Redis connection failed: {}", e);
                    None
                }
            },
            Err(e) => {
                warn!("Content fetch: invalid Redis URL: {}", e);
                None
            }
        };

        Self {
            db,
            config,
            ipfs,
            http,
            redis_client,
        }
    }
}

/// Run the content fetch job — periodically fetches content for unfetched pending messages
pub async fn run(ctx: Arc<ContentFetchContext>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(FETCH_INTERVAL_MS));

    info!("Content fetch service started");

    loop {
        ticker.tick().await;

        // Find pending messages that need content fetched
        let unfetched = match get_unfetched_messages(&ctx.db, FETCH_BATCH_SIZE).await {
            Ok(msgs) => msgs,
            Err(e) => {
                debug!("Failed to query unfetched messages: {}", e);
                continue;
            }
        };

        if unfetched.is_empty() {
            continue;
        }

        debug!("Processing {} unfetched messages", unfetched.len());

        // Get available peer API servers from Redis
        let api_servers = get_api_servers(&ctx).await;

        for (item_hash, item_type_str) in unfetched {
            let item_type = match item_type_str.as_str() {
                "storage" => ItemType::Storage,
                "ipfs" => ItemType::Ipfs,
                _ => continue, // inline messages shouldn't be here
            };

            match fetch_content(&ctx, &item_hash, &item_type, &api_servers).await {
                Ok(content) => {
                    // Store the fetched content and mark as fetched
                    if let Err(e) = store_fetched_content(&ctx.db, &item_hash, &content).await {
                        warn!("Failed to store fetched content for {}: {}", item_hash, e);
                    } else {
                        debug!("Fetched and stored content for {}", item_hash);
                    }
                }
                Err(e) => {
                    debug!("Failed to fetch content for {}: {}", item_hash, e);
                    // Increment retry counter
                    let _ = increment_fetch_retry(&ctx.db, &item_hash).await;
                }
            }
        }
    }
}

/// Fetch content for a single message, trying peers then IPFS
async fn fetch_content(
    ctx: &ContentFetchContext,
    item_hash: &str,
    item_type: &ItemType,
    api_servers: &[String],
) -> anyhow::Result<String> {
    // Try random peers first
    if !api_servers.is_empty() {
        let mut rng = rand::thread_rng();
        let mut servers: Vec<&String> = api_servers.iter().collect();
        servers.shuffle(&mut rng);

        for (i, server) in servers.iter().take(MAX_PEER_ATTEMPTS).enumerate() {
            match fetch_from_peer(&ctx.http, server, item_hash).await {
                Ok(content) => {
                    // Verify hash
                    if verify_content_hash(item_hash, &content) {
                        debug!(
                            "Fetched {} from peer {} (attempt {})",
                            item_hash, server, i + 1
                        );
                        return Ok(content);
                    } else {
                        warn!(
                            "Hash mismatch for {} from peer {}",
                            item_hash, server
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        "Peer {} failed for {}: {} (attempt {})",
                        server, item_hash, e, i + 1
                    );
                }
            }
        }
    }

    // Fall back to IPFS
    if *item_type == ItemType::Ipfs {
        let bytes = ctx.ipfs.get(item_hash).await
            .map_err(|e| anyhow::anyhow!("IPFS fetch failed: {}", e))?;
        let content = String::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("IPFS content is not valid UTF-8: {}", e))?;
        return Ok(content);
    }

    // For storage type, also try IPFS as last resort
    match ctx.ipfs.get(item_hash).await {
        Ok(bytes) => {
            let content = String::from_utf8(bytes)
                .map_err(|e| anyhow::anyhow!("Content is not valid UTF-8: {}", e))?;
            Ok(content)
        }
        Err(e) => {
            Err(anyhow::anyhow!(
                "All fetch methods exhausted for {}: {}",
                item_hash,
                e
            ))
        }
    }
}

/// Fetch content from a single peer's storage API
async fn fetch_from_peer(
    http: &Client,
    peer_url: &str,
    item_hash: &str,
) -> anyhow::Result<String> {
    let url = format!(
        "{}/api/v0/storage/{}",
        peer_url.trim_end_matches('/'),
        item_hash
    );

    let response = http.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Peer returned status {}",
            response.status()
        ));
    }

    let body: PeerStorageResponse = response.json().await?;

    if body.status != "success" {
        return Err(anyhow::anyhow!(
            "Peer returned status '{}'",
            body.status
        ));
    }

    let b64_content = body
        .content
        .ok_or_else(|| anyhow::anyhow!("Peer response missing 'content' field"))?;

    // Decode base64
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&b64_content)
        .map_err(|e| anyhow::anyhow!("Base64 decode error: {}", e))?;

    String::from_utf8(decoded)
        .map_err(|e| anyhow::anyhow!("Decoded content is not valid UTF-8: {}", e))
}

/// Verify that content hashes to the expected item_hash (SHA256)
fn verify_content_hash(expected_hash: &str, content: &str) -> bool {
    let computed = hex::encode(Sha256::digest(content.as_bytes()));
    computed == expected_hash
}

/// Get API server URLs from Redis
async fn get_api_servers(ctx: &ContentFetchContext) -> Vec<String> {
    if let Some(ref redis) = ctx.redis_client {
        let mut conn = redis.clone();
        let result: Result<Vec<String>, redis::RedisError> =
            redis::cmd("SMEMBERS")
                .arg("api_servers")
                .query_async(&mut conn)
                .await;

        match result {
            Ok(servers) => {
                if !servers.is_empty() {
                    debug!("Got {} api_servers from Redis", servers.len());
                }
                servers
            }
            Err(e) => {
                debug!("Failed to get api_servers from Redis: {}", e);
                Vec::new()
            }
        }
    } else {
        Vec::new()
    }
}

/// Query unfetched pending messages (item_type = storage or ipfs, fetched = false)
async fn get_unfetched_messages(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    // Extract item_type from the JSONB message_data
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT item_hash,
               COALESCE(message_data->>'item_type', 'inline') as item_type
        FROM pending_messages
        WHERE fetched = false
          AND COALESCE(message_data->>'item_type', 'inline') != 'inline'
          AND next_attempt <= NOW()
          AND retries < 10
        ORDER BY next_attempt ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Store fetched content into the pending message and mark as fetched
async fn store_fetched_content(
    pool: &PgPool,
    item_hash: &str,
    content: &str,
) -> Result<(), sqlx::Error> {
    // Update the message_data JSONB to include item_content, and mark fetched
    sqlx::query(
        r#"
        UPDATE pending_messages
        SET fetched = true,
            message_data = jsonb_set(message_data, '{item_content}', to_jsonb($2::text))
        WHERE item_hash = $1
        "#,
    )
    .bind(item_hash)
    .bind(content)
    .execute(pool)
    .await?;

    Ok(())
}

/// Increment the retry counter and push next_attempt into the future
async fn increment_fetch_retry(
    pool: &PgPool,
    item_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE pending_messages
        SET retries = retries + 1,
            next_attempt = NOW() + (INTERVAL '1 second' * LEAST(POWER(2, retries) * 10, 3600))
        WHERE item_hash = $1
        "#,
    )
    .bind(item_hash)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_content_hash() {
        let content = r#"{"hello":"world"}"#;
        let hash = hex::encode(Sha256::digest(content.as_bytes()));
        assert!(verify_content_hash(&hash, content));
        assert!(!verify_content_hash("0000000000000000", content));
    }

    #[test]
    fn test_parse_peer_response() {
        let json = r#"{"status":"success","content":"aGVsbG8gd29ybGQ="}"#;
        let resp: PeerStorageResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "success");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(resp.content.unwrap())
            .unwrap();
        assert_eq!(decoded, b"hello world");
    }
}
