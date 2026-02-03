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
//! Since we don't have full P2P pubsub yet, we also support a bootstrap list
//! of known API servers from config to seed the peer table.
//!
//! Reference: aleph/services/p2p/jobs.py, aleph/services/peers/monitor.py

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::accessors::PeerAccessor;

/// Check interval for HTTP peer health
const TIDY_INTERVAL_SECS: u64 = 60;

/// Timeout for version check requests
const VERSION_CHECK_TIMEOUT_SECS: u64 = 5;

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

/// Seed the peers table with bootstrap API servers if the table is empty
pub async fn seed_bootstrap_peers(pool: &PgPool) {
    // Check if we have any HTTP peers
    match PeerAccessor::get_addresses_by_type(pool, "HTTP", None).await {
        Ok(addrs) if !addrs.is_empty() => {
            debug!("Peers table already has {} HTTP peers, skipping bootstrap", addrs.len());
            return;
        }
        Ok(_) => {
            info!("No HTTP peers found, seeding with bootstrap servers");
        }
        Err(e) => {
            warn!("Failed to check peers table: {}, seeding bootstrap anyway", e);
        }
    }

    for server in BOOTSTRAP_API_SERVERS {
        if let Err(e) = PeerAccessor::upsert(pool, server, "HTTP", server, "BOOTSTRAP").await {
            warn!("Failed to seed bootstrap peer {}: {}", server, e);
        }
    }
    info!("Seeded {} bootstrap HTTP peers", BOOTSTRAP_API_SERVERS.len());
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
pub async fn tidy_http_peers_job(
    pool: PgPool,
    api_servers: ApiServers,
    config: Arc<Config>,
) {
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
async fn check_all_http_peers(
    pool: &PgPool,
    client: &Client,
    api_servers: &ApiServers,
) {
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

    // Check all peers concurrently
    let checks: Vec<_> = peers.iter().map(|peer_uri| {
        let client = client.clone();
        let uri = peer_uri.clone();
        async move {
            let online = check_peer_online(&client, &uri).await;
            (uri, online)
        }
    }).collect();

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

    debug!("Peer tidy: {}/{} HTTP peers online", online_count, peers.len());

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
}
