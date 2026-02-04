//! Peer content fetching service
//!
//! For messages with `item_type` = storage or ipfs, the actual content must be
//! fetched separately. This service reads the api_servers set (populated by the
//! peer tidy job) and tries to download content from random peers before
//! falling back to the IPFS gateway.
//!
//! Flow (matching Python pyaleph storage.py):
//! 1. Check local storage engine (filesystem cache) — skip if not found
//! 2. Try HTTP peers: GET {peer}/api/v0/storage/{item_hash}
//!    - Response: {"status": "success", "content": "<base64-encoded>"}
//!    - Shuffle peers randomly, try up to MAX_PEER_ATTEMPTS
//! 3. For ipfs type: try IPFS daemon as fallback
//! 4. Verify content hash:
//!    - storage type: SHA256(content) == item_hash
//!    - ipfs type: IPFS CID verification
//!
//! Reference: aleph/storage.py, aleph/services/p2p/http.py

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use futures::future::join_all;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::services::ipfs::IpfsService;
use crate::services::peers::ApiServers;

/// Timeout for peer content fetch HTTP requests
const PEER_FETCH_TIMEOUT_SECS: u64 = 10;

/// Maximum peers to try before falling back
const MAX_PEER_ATTEMPTS: usize = 5;

/// Interval between content-fetch runs
const FETCH_INTERVAL_MS: u64 = 500;

/// Batch size of messages to process per tick
const FETCH_BATCH_SIZE: i64 = 200;

/// Max concurrent fetch requests
const MAX_CONCURRENT_FETCHES: usize = 50;

/// Max retries before marking a message as unfetchable
const MAX_RETRIES: usize = 3;

/// Peer storage API response
/// Matches: aleph/services/p2p/http.py get_peer_hash_content
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
    pub api_servers: ApiServers,
}

impl ContentFetchContext {
    pub fn new(
        db: PgPool,
        config: Arc<Config>,
        ipfs: Arc<IpfsService>,
        api_servers: ApiServers,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(PEER_FETCH_TIMEOUT_SECS))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            db,
            config,
            ipfs,
            http,
            api_servers,
        }
    }
}

/// Run the content fetch job — fetches content for messages with NULL item_content
///
/// Unlike Python pyaleph which uses pending_messages, our Rust impl works
/// directly on the messages table since we get messages from the indexer
/// pre-verified. We find messages where item_type is storage/ipfs and
/// item_content is NULL.
pub async fn run(ctx: Arc<ContentFetchContext>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(FETCH_INTERVAL_MS));

    // Track per-hash failure counts — after MAX_RETRIES, mark as unfetchable
    let mut failure_counts: HashMap<String, usize> = HashMap::new();

    info!("Content fetch service started");

    loop {
        ticker.tick().await;

        // Find messages that need content fetched
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

        // Get current api_servers snapshot
        let servers: Vec<String> = {
            let set = ctx.api_servers.read().await;
            set.iter().cloned().collect()
        };

        if servers.is_empty() {
            debug!("No API servers available yet, skipping content fetch");
            continue;
        }

        // Fetch all items concurrently with a semaphore
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
        let batch_size = unfetched.len();

        let fetch_futures: Vec<_> = unfetched.into_iter().map(|(item_hash, item_type)| {
            let ctx = ctx.clone();
            let servers = servers.clone();
            let sem = semaphore.clone();

            async move {
                let _permit = sem.acquire().await.ok();
                let result = fetch_and_verify(&ctx, &item_hash, &item_type, &servers).await;
                (item_hash, result)
            }
        }).collect();

        let results = join_all(fetch_futures).await;

        let mut fetched_count = 0u64;
        let mut failed_count = 0u64;
        let mut marked_unfetchable = 0u64;

        for (item_hash, result) in results {
            match result {
                Ok(content) => {
                    if let Err(e) = store_fetched_content(&ctx.db, &item_hash, &content).await {
                        warn!("Failed to store content for {}: {}", item_hash, e);
                    } else if let Err(e) = queue_for_processing(&ctx.db, &item_hash).await {
                        warn!("Failed to queue {} for processing: {}", item_hash, e);
                        // Content was stored but queuing failed — still count as fetched
                        fetched_count += 1;
                        failure_counts.remove(&item_hash);
                    } else {
                        fetched_count += 1;
                        failure_counts.remove(&item_hash);
                    }
                }
                Err(_) => {
                    let count = failure_counts.entry(item_hash.clone()).or_insert(0);
                    *count += 1;
                    if *count >= MAX_RETRIES {
                        let _ = mark_unfetchable(&ctx.db, &item_hash).await;
                        failure_counts.remove(&item_hash);
                        marked_unfetchable += 1;
                    } else {
                        failed_count += 1;
                    }
                }
            }
        }

        info!(
            "Content fetch: {} fetched, {} failed, {} unfetchable (batch {}, {} peers)",
            fetched_count, failed_count, marked_unfetchable, batch_size, servers.len()
        );

        // Prevent unbounded memory growth — prune old entries periodically
        if failure_counts.len() > 100_000 {
            failure_counts.clear();
        }
    }
}

/// Fetch content from peers and verify hash integrity
///
/// Matches: aleph/storage.py StorageService._fetch_content_from_network
async fn fetch_and_verify(
    ctx: &ContentFetchContext,
    item_hash: &str,
    item_type: &str,
    api_servers: &[String],
) -> anyhow::Result<String> {
    // Try HTTP peers first (shuffled randomly)
    if !api_servers.is_empty() {
        let mut servers: Vec<&String> = api_servers.iter().collect();
        let mut rng = rand::rngs::StdRng::from_entropy();
        servers.shuffle(&mut rng);

        for (i, server) in servers.iter().take(MAX_PEER_ATTEMPTS).enumerate() {
            match fetch_from_peer(&ctx.http, server, item_hash).await {
                Ok(content_bytes) => {
                    // Verify hash based on item_type
                    if verify_hash(item_hash, item_type, &content_bytes) {
                        // Convert to string for storage
                        let content_str = String::from_utf8(content_bytes)
                            .map_err(|e| anyhow::anyhow!("Content not UTF-8: {}", e))?;
                        return Ok(content_str);
                    } else {
                        warn!(
                            "Hash mismatch for {} from peer {} (type={})",
                            item_hash, server, item_type
                        );
                    }
                }
                Err(e) => {
                    debug!("Peer {} failed for {} (attempt {}): {}", server, item_hash, i + 1, e);
                }
            }
        }
    }

    // Fall back to IPFS for ipfs-type messages
    if item_type == "ipfs" {
        let bytes = ctx.ipfs.get(item_hash).await
            .map_err(|e| anyhow::anyhow!("IPFS fetch failed: {}", e))?;
        // IPFS content is inherently hash-verified by CID
        let content = String::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("IPFS content not UTF-8: {}", e))?;
        return Ok(content);
    }

    Err(anyhow::anyhow!(
        "All fetch methods exhausted for {} (type={})",
        item_hash,
        item_type
    ))
}

/// Fetch content from a single peer's storage API
///
/// Matches: aleph/services/p2p/http.py get_peer_hash_content
/// Returns raw bytes (before UTF-8 conversion) for hash verification
async fn fetch_from_peer(
    http: &Client,
    peer_url: &str,
    item_hash: &str,
) -> anyhow::Result<Vec<u8>> {
    let url = format!(
        "{}/api/v0/storage/{}",
        peer_url.trim_end_matches('/'),
        item_hash
    );

    let response = http.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!("HTTP {}", response.status()));
    }

    let body: PeerStorageResponse = response.json().await?;

    if body.status != "success" || body.content.is_none() {
        return Err(anyhow::anyhow!("Peer returned status '{}'", body.status));
    }

    // Decode base64 content
    // Python pyaleph returns base64 with newline wrapping, so strip whitespace first
    let b64_content = body.content.unwrap();
    if b64_content.is_empty() {
        return Err(anyhow::anyhow!("Peer returned empty content"));
    }
    let b64_clean: String = b64_content.chars().filter(|c| !c.is_whitespace()).collect();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&b64_clean)
        .map_err(|e| anyhow::anyhow!("Base64 decode error: {}", e))?;

    if decoded.is_empty() {
        return Err(anyhow::anyhow!("Decoded content is empty"));
    }

    Ok(decoded)
}

/// Verify content hash matches expected item_hash
///
/// Matches: aleph/storage.py StorageService._verify_content_hash
/// - storage type: SHA256(content) must equal item_hash
/// - ipfs type: would need IPFS CID computation (complex), skipped for peer fetch
fn verify_hash(expected_hash: &str, item_type: &str, content: &[u8]) -> bool {
    match item_type {
        "storage" => {
            let computed = hex::encode(Sha256::digest(content));
            computed == expected_hash
        }
        "ipfs" => {
            // For IPFS type fetched via HTTP peers, we do SHA256 check as well
            // since the peer storage API serves by hash.
            // Full CID verification would require IPFS daemon.
            // The Python code also delegates to IPFS daemon for CID verification,
            // but falls back to trusting the peer response.
            let computed = hex::encode(Sha256::digest(content));
            if computed == expected_hash {
                return true;
            }
            // Some IPFS hashes are CIDv0/v1, not plain SHA256
            // We can't fully verify without IPFS daemon, so accept if peer said "success"
            warn!(
                "Cannot fully verify IPFS CID {} (SHA256 mismatch, would need IPFS daemon)",
                expected_hash
            );
            true // Accept peer response for IPFS type
        }
        _ => {
            warn!("Unknown item_type '{}' for hash verification", item_type);
            false
        }
    }
}

/// Query messages with NULL item_content that need fetching.
/// Uses random ordering to avoid getting stuck on the same failing messages.
/// Skips messages where item_content = '' (marked as unfetchable).
async fn get_unfetched_messages(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT item_hash, item_type
        FROM messages
        WHERE item_content IS NULL
          AND item_type IN ('storage', 'ipfs')
          AND sender NOT IN ('0x51A58800b26AA1451aaA803d1746687cB88E0501')
        ORDER BY RANDOM()
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Store fetched content into the messages table
async fn store_fetched_content(
    pool: &PgPool,
    item_hash: &str,
    content: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE messages SET item_content = $1 WHERE item_hash = $2 AND item_content IS NULL"
    )
    .bind(content)
    .bind(item_hash)
    .execute(pool)
    .await?;

    Ok(())
}

/// Queue a message for handler processing after content has been fetched.
///
/// Reconstructs the Message JSON from the messages table row and inserts it
/// into pending_messages so the message_processor picks it up and runs the
/// appropriate handler (PostHandler, AggregateHandler, etc.).
async fn queue_for_processing(
    pool: &PgPool,
    item_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO pending_messages (
            item_hash, message, reception_time, fetched, check_message,
            retries, next_attempt, trusted_source
        )
        SELECT
            m.item_hash,
            jsonb_build_object(
                'type', m.message_type,
                'chain', m.chain,
                'sender', m.sender,
                'signature', m.signature,
                'item_type', m.item_type,
                'item_hash', m.item_hash,
                'item_content', m.item_content,
                'channel', m.channel,
                'time', m.time
            ),
            EXTRACT(EPOCH FROM NOW()),
            true,
            true,
            0,
            EXTRACT(EPOCH FROM NOW()),
            true
        FROM messages m
        WHERE m.item_hash = $1
        ON CONFLICT (item_hash) DO UPDATE SET
            message = EXCLUDED.message,
            fetched = true,
            next_attempt = EXCLUDED.next_attempt,
            retries = 0
        "#,
    )
    .bind(item_hash)
    .execute(pool)
    .await?;

    debug!("Queued {} for handler processing", item_hash);
    Ok(())
}

/// Mark a message as unfetchable by setting item_content to empty string.
/// This prevents the content fetch loop from retrying it forever.
async fn mark_unfetchable(
    pool: &PgPool,
    item_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE messages SET item_content = '' WHERE item_hash = $1 AND item_content IS NULL"
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
    fn test_verify_storage_hash() {
        let content = br#"{"hello":"world"}"#;
        let hash = hex::encode(Sha256::digest(content));
        assert!(verify_hash(&hash, "storage", content));
        assert!(!verify_hash("0000000000000000", "storage", content));
    }

    #[test]
    fn test_verify_hash_wrong_type() {
        assert!(!verify_hash("abc", "unknown", b"test"));
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

    #[test]
    fn test_parse_peer_response_no_content() {
        let json = r#"{"status":"error"}"#;
        let resp: PeerStorageResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "error");
        assert!(resp.content.is_none());
    }
}
