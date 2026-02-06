//! Peer discovery and monitoring service
//!
//! Manages the list of known peers and checks which HTTP peers are alive.
//! Alive HTTP peers are used by the content fetch service to retrieve
//! storage/ipfs content.
//!
//! Architecture (matching Python pyaleph):
//! 1. Peers announce themselves via P2P pubsub "alive" messages
//! 2. Peers are stored in the `peers` table (peer_id, peer_type, address)
//! 3. `tidy_http_peers_job` periodically checks HTTP peers are reachable
//!    by hitting `/api/v0/version` — online peers go into `api_servers` set
//! 4. Content fetch service reads `api_servers` and tries to fetch from them
//!
//! Peer Discovery:
//! Since we don't have full P2P pubsub yet, we discover peers by querying the
//! Aleph network's "corechannel" aggregate which lists all CCN nodes with their
//! multiaddresses. We extract HTTP API URLs from these and seed the peers table.
//!
//! Reference: aleph/services/p2p/jobs.py, aleph/services/peers/monitor.py

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::accessors::PeerAccessor;
use crate::services::sharding::{ShardingService, PeerNode, extract_peer_id_from_multiaddr};

/// Check interval for HTTP peer health
const TIDY_INTERVAL_SECS: u64 = 60;

/// Timeout for version check requests
const VERSION_CHECK_TIMEOUT_SECS: u64 = 5;

/// Timeout for peer discovery requests (aggregate fetch can be large)
const DISCOVERY_TIMEOUT_SECS: u64 = 30;

/// The Ethereum address that owns the corechannel aggregate
const CORECHANNEL_OWNER: &str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10";

/// API servers used to fetch the corechannel aggregate for peer discovery
const DISCOVERY_API_SERVERS: &[&str] = &[
    "https://api2.aleph.im",
    "https://api1.aleph.im",
    "https://official.aleph.cloud",
];

/// Default HTTP API port for Aleph CCN nodes
const DEFAULT_CCN_HTTP_PORT: u16 = 4024;

/// Bootstrap API servers — well-known Aleph nodes
/// These seed the peers table on first run
const BOOTSTRAP_API_SERVERS: &[&str] = &[
    "https://api1.aleph.im",
    "https://api2.aleph.im",
    "https://api3.aleph.im",
];

/// Shared set of currently-alive API servers.
/// Content fetch reads from this; tidy job writes to it.
pub type ApiServers = Arc<RwLock<HashSet<String>>>;

/// Create a new shared API servers set
pub fn new_api_servers() -> ApiServers {
    Arc::new(RwLock::new(HashSet::new()))
}

// ── Aggregate response types for corechannel peer discovery ──────────────

/// Top-level aggregate API response
#[derive(Debug, Deserialize)]
struct AggregateResponse {
    data: Option<AggregateData>,
}

/// The data field contains keyed aggregates
#[derive(Debug, Deserialize)]
struct AggregateData {
    corechannel: Option<CoreChannelAggregate>,
}

/// The corechannel aggregate contains a list of nodes
#[derive(Debug, Deserialize)]
struct CoreChannelAggregate {
    #[serde(default)]
    nodes: Vec<CoreChannelNode>,
}

/// A single CCN node entry from the aggregate
#[derive(Debug, Deserialize)]
struct CoreChannelNode {
    #[serde(default)]
    name: String,
    #[serde(default)]
    multiaddress: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    score: f64,
}

// ── Peer Discovery ──────────────────────────────────────────────────────

/// Discover peers by fetching the corechannel aggregate from the Aleph network.
///
/// This queries the well-known aggregate that lists all CCN (Core Channel Node)
/// operators, extracts their IP addresses from multiaddresses, and constructs
/// HTTP API URLs (port 4024 is the standard CCN HTTP API port).
///
/// Returns a list of discovered (peer_id, http_address) pairs and the count of new peers.
async fn discover_peers_from_aggregate(pool: &PgPool, client: &Client) -> (usize, Vec<PeerNode>) {
    let mut discovered = 0;
    let mut peer_nodes = Vec::new();

    // Try each discovery server until one works
    for api_server in DISCOVERY_API_SERVERS {
        let url = format!(
            "{}/api/v0/aggregates/{}.json?keys=corechannel",
            api_server, CORECHANNEL_OWNER
        );

        debug!("Fetching corechannel aggregate from {}", api_server);

        let resp = match client.get(&url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(
                    "Aggregate fetch from {} returned status {}",
                    api_server,
                    r.status()
                );
                continue;
            }
            Err(e) => {
                warn!("Failed to fetch aggregate from {}: {}", api_server, e);
                continue;
            }
        };

        let aggregate: AggregateResponse = match resp.json().await {
            Ok(a) => a,
            Err(e) => {
                warn!("Failed to parse aggregate from {}: {}", api_server, e);
                continue;
            }
        };

        let nodes = match aggregate
            .data
            .and_then(|d| d.corechannel)
            .map(|cc| cc.nodes)
        {
            Some(n) if !n.is_empty() => n,
            _ => {
                warn!("No nodes found in corechannel aggregate from {}", api_server);
                continue;
            }
        };

        info!(
            "Got {} CCN nodes from corechannel aggregate ({})",
            nodes.len(),
            api_server
        );

        for node in &nodes {
            // Only consider active nodes with a multiaddress and reasonable score
            if node.multiaddress.is_empty() {
                continue;
            }
            if node.status != "active" {
                continue;
            }

            // Extract IP from multiaddress: /ip4/X.X.X.X/tcp/...
            let http_url = if let Some(ip) = extract_ip_from_multiaddr(&node.multiaddress) {
                format!("http://{}:{}", ip, DEFAULT_CCN_HTTP_PORT)
            } else {
                continue;
            };

            // Extract p2p peer ID from multiaddress for hash ring identity
            let node_peer_id = extract_peer_id_from_multiaddr(&node.multiaddress)
                .map(|s| s.to_string())
                .unwrap_or_else(|| http_url.clone());

            if let Err(e) = PeerAccessor::upsert(
                pool,
                &node_peer_id,
                "HTTP",
                &http_url,
                "CORECHANNEL",
            )
            .await
            {
                warn!("Failed to upsert discovered peer {}: {}", http_url, e);
            } else {
                peer_nodes.push(PeerNode {
                    node_id: node_peer_id,
                    http_address: http_url,
                });
                discovered += 1;
            }
        }

        // Success — don't try other servers
        break;
    }

    (discovered, peer_nodes)
}

/// Extract IPv4 address from a multiaddress string.
/// Handles formats like `/ip4/1.2.3.4/tcp/4025/p2p/Qm...`
fn extract_ip_from_multiaddr(multiaddr: &str) -> Option<&str> {
    let parts: Vec<&str> = multiaddr.split('/').collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "ip4" {
            if let Some(ip) = parts.get(i + 1) {
                // Basic validation: must have dots and digits
                if ip.contains('.') && ip.chars().all(|c| c.is_ascii_digit() || c == '.') {
                    return Some(ip);
                }
            }
        }
    }
    None
}

/// Peer discovery job — periodically discovers new peers from the Aleph network.
///
/// Runs on `discovery_interval` (default 300s). Fetches the corechannel aggregate
/// which lists all registered CCN nodes, extracts their HTTP API addresses, and
/// seeds them into the peers table for the tidy job to health-check.
pub async fn peer_discovery_job(
    pool: PgPool,
    config: Arc<Config>,
    sharding: Option<Arc<ShardingService>>,
) {
    let client = Client::builder()
        .timeout(Duration::from_secs(DISCOVERY_TIMEOUT_SECS))
        .build()
        .expect("Failed to create HTTP client for peer discovery");

    let interval_secs = config.p2p.discovery_interval.max(60);
    let interval = Duration::from_secs(interval_secs);

    info!(
        "Peer discovery job started (interval: {}s, source: corechannel aggregate)",
        interval_secs
    );

    // Initial discovery immediately
    let (count, peer_nodes) = discover_peers_from_aggregate(&pool, &client).await;
    info!("Initial peer discovery: seeded {} peers from corechannel", count);

    // Rebuild hash ring with discovered peers
    if let Some(ref svc) = sharding {
        svc.rebuild_from_peers(&peer_nodes).await;
    }

    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // consume first instant tick

    loop {
        ticker.tick().await;
        let (count, peer_nodes) = discover_peers_from_aggregate(&pool, &client).await;
        if count > 0 {
            info!("Peer discovery: refreshed {} peers from corechannel", count);
        } else {
            debug!("Peer discovery: no new peers found");
        }

        // Rebuild hash ring when peers change
        if let Some(ref svc) = sharding {
            svc.rebuild_from_peers(&peer_nodes).await;
        }
    }
}

// ── Bootstrap & Tidy ────────────────────────────────────────────────────

/// Seed the peers table with bootstrap API servers if the table is empty
pub async fn seed_bootstrap_peers(pool: &PgPool) {
    // Check if we have any HTTP peers
    match PeerAccessor::get_addresses_by_type(pool, "HTTP", None).await {
        Ok(addrs) if !addrs.is_empty() => {
            debug!(
                "Peers table already has {} HTTP peers, skipping bootstrap",
                addrs.len()
            );
            return;
        }
        Ok(_) => {
            info!("No HTTP peers found, seeding with bootstrap servers");
        }
        Err(e) => {
            warn!(
                "Failed to check peers table: {}, seeding bootstrap anyway",
                e
            );
        }
    }

    for server in BOOTSTRAP_API_SERVERS {
        if let Err(e) = PeerAccessor::upsert(pool, server, "HTTP", server, "BOOTSTRAP").await {
            warn!("Failed to seed bootstrap peer {}: {}", server, e);
        }
    }
    info!(
        "Seeded {} bootstrap HTTP peers",
        BOOTSTRAP_API_SERVERS.len()
    );
}

/// Check if a peer is online by hitting /api/v0/version
async fn check_peer_online(client: &Client, peer_uri: &str) -> bool {
    let url = format!("{}/api/v0/version", peer_uri.trim_end_matches('/'));
    match client.get(&url).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Tidy HTTP peers job — periodically checks HTTP peers are alive
/// and maintains the api_servers set.
///
/// Matches: aleph/services/p2p/jobs.py tidy_http_peers_job
pub async fn tidy_http_peers_job(pool: PgPool, api_servers: ApiServers, config: Arc<Config>) {
    let client = Client::builder()
        .timeout(Duration::from_secs(VERSION_CHECK_TIMEOUT_SECS))
        .build()
        .expect("Failed to create HTTP client for peer checking");

    // Seed bootstrap peers on startup
    seed_bootstrap_peers(&pool).await;

    let reconnect_delay = config.p2p.reconnect_delay;
    let interval = Duration::from_secs(reconnect_delay.max(TIDY_INTERVAL_SECS));

    info!("Peer tidy job started (interval: {}s)", interval.as_secs());

    // Initial check immediately
    check_all_http_peers(&pool, &client, &api_servers).await;

    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // consume first instant tick

    loop {
        ticker.tick().await;
        check_all_http_peers(&pool, &client, &api_servers).await;
    }
}

/// Check all HTTP peers and update the api_servers set
async fn check_all_http_peers(pool: &PgPool, client: &Client, api_servers: &ApiServers) {
    let peers = match PeerAccessor::get_addresses_by_type(pool, "HTTP", None).await {
        Ok(p) => p,
        Err(e) => {
            warn!("Failed to get HTTP peers: {}", e);
            return;
        }
    };

    if peers.is_empty() {
        debug!("No HTTP peers to check");
        return;
    }

    debug!("Checking {} HTTP peers", peers.len());

    // Check all peers concurrently (with some concurrency limit to be nice)
    let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
    let checks: Vec<_> = peers
        .iter()
        .map(|peer_uri| {
            let client = client.clone();
            let uri = peer_uri.clone();
            let sem = semaphore.clone();
            async move {
                let _permit = sem.acquire().await.ok();
                let online = check_peer_online(&client, &uri).await;
                (uri, online)
            }
        })
        .collect();

    let results = futures::future::join_all(checks).await;

    let mut servers = api_servers.write().await;
    let mut online_count = 0;

    for (uri, online) in results {
        if online {
            servers.insert(uri);
            online_count += 1;
        } else {
            servers.remove(&uri);
        }
    }

    info!(
        "Peer tidy: {}/{} HTTP peers online, {} total in api_servers",
        online_count,
        peers.len(),
        servers.len()
    );

    // Also update last_seen for online peers
    for server in servers.iter() {
        let _ = PeerAccessor::upsert(pool, server, "HTTP", server, "HTTP").await;
    }
}

/// Handle an incoming peer alive message (from P2P pubsub)
/// Matches: aleph/services/peers/monitor.py handle_incoming_host
pub async fn handle_peer_alive(
    pool: &PgPool,
    peer_id: &str,
    address: &str,
    peer_type: &str,
    source: &str,
) -> Result<(), sqlx::Error> {
    PeerAccessor::upsert(pool, peer_id, peer_type, address, source).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_servers_valid() {
        for server in BOOTSTRAP_API_SERVERS {
            assert!(server.starts_with("https://"));
        }
    }

    #[test]
    fn test_extract_ip_from_multiaddr() {
        assert_eq!(
            extract_ip_from_multiaddr("/ip4/1.2.3.4/tcp/4025/p2p/QmFoo"),
            Some("1.2.3.4")
        );
        assert_eq!(
            extract_ip_from_multiaddr("/ip4/192.168.1.1/tcp/4025"),
            Some("192.168.1.1")
        );
        assert_eq!(extract_ip_from_multiaddr("/dns4/example.com/tcp/4025"), None);
        assert_eq!(extract_ip_from_multiaddr(""), None);
    }

    #[test]
    fn test_discovery_api_servers_valid() {
        for server in DISCOVERY_API_SERVERS {
            assert!(server.starts_with("https://"));
        }
    }
}
