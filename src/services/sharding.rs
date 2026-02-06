//! Content sharding service
//!
//! Implements consistent hashing to distribute content responsibility across
//! network nodes. Each content hash maps to K responsible nodes (replication
//! factor). Nodes only need to store content they're responsible for; other
//! content can be kept in a warm cache with TTL-based eviction.
//!
//! The hash ring uses SipHash for deterministic, collision-resistant placement
//! with configurable virtual nodes per physical node.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// A node in the network with its HTTP address
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerNode {
    pub node_id: String,
    pub http_address: String,
}

/// Result of a content routing decision
#[derive(Debug, Clone)]
pub enum ContentDecision {
    /// We are one of the responsible nodes for this content
    Owned {
        /// Other replicas (node_id, http_address) that also store this content
        replicas: Vec<PeerNode>,
    },
    /// We are NOT responsible for this content
    NotOwned {
        /// The nodes that ARE responsible (node_id, http_address)
        responsible: Vec<PeerNode>,
    },
}

/// Consistent hash ring for content routing.
///
/// Uses virtual nodes (vnodes) to spread each physical node across the ring,
/// producing a more uniform distribution. Each physical node gets `vnodes_per_node`
/// positions on a 64-bit ring (keyed by SipHash of `"{node_id}:{vnode_index}"`).
pub struct ContentRing {
    /// Ring positions → node_id mapping (sorted by position for binary search)
    ring: BTreeMap<u64, String>,
    /// node_id → PeerNode mapping
    nodes: HashMap<String, PeerNode>,
    /// Number of virtual nodes per physical node
    vnodes_per_node: usize,
    /// Replication factor (how many distinct physical nodes store each piece of content)
    replication_factor: usize,
}

impl ContentRing {
    /// Create a new empty hash ring.
    pub fn new(replication_factor: usize, vnodes_per_node: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
            vnodes_per_node,
            replication_factor,
        }
    }

    /// Add a node to the ring.
    pub fn add_node(&mut self, node_id: &str, http_address: &str) {
        let peer = PeerNode {
            node_id: node_id.to_string(),
            http_address: http_address.to_string(),
        };
        self.nodes.insert(node_id.to_string(), peer);

        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let position = sip_hash_64(&vnode_key);
            self.ring.insert(position, node_id.to_string());
        }
    }

    /// Remove a node from the ring.
    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.remove(node_id);

        for i in 0..self.vnodes_per_node {
            let vnode_key = format!("{}:{}", node_id, i);
            let position = sip_hash_64(&vnode_key);
            self.ring.remove(&position);
        }
    }

    /// Get the responsible nodes for a content hash.
    ///
    /// Walks clockwise from the hash position on the ring and collects
    /// `replication_factor` distinct physical nodes.
    pub fn get_responsible_nodes(&self, content_hash: &str) -> Vec<PeerNode> {
        if self.ring.is_empty() {
            return Vec::new();
        }

        let position = sip_hash_64(content_hash);
        let mut result = Vec::with_capacity(self.replication_factor);
        let mut seen_nodes = std::collections::HashSet::new();

        // Walk clockwise from position
        for (_pos, node_id) in self.ring.range(position..) {
            if seen_nodes.insert(node_id.clone()) {
                if let Some(peer) = self.nodes.get(node_id) {
                    result.push(peer.clone());
                }
            }
            if result.len() >= self.replication_factor {
                return result;
            }
        }

        // Wrap around to the beginning of the ring
        for (_pos, node_id) in self.ring.range(..position) {
            if seen_nodes.insert(node_id.clone()) {
                if let Some(peer) = self.nodes.get(node_id) {
                    result.push(peer.clone());
                }
            }
            if result.len() >= self.replication_factor {
                return result;
            }
        }

        result
    }

    /// Check if a given node is responsible for a content hash.
    pub fn is_responsible(&self, node_id: &str, content_hash: &str) -> bool {
        self.get_responsible_nodes(content_hash)
            .iter()
            .any(|p| p.node_id == node_id)
    }

    /// Number of physical nodes on the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of total positions (virtual nodes) on the ring.
    pub fn ring_size(&self) -> usize {
        self.ring.len()
    }
}

/// SipHash-2-4 producing a u64 hash value (deterministic, keyed).
///
/// We use a fixed key (0, 0) for determinism across nodes.
fn sip_hash_64(input: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

// ── ShardingService ────────────────────────────────────────────────────

/// High-level sharding service wrapping the consistent hash ring.
///
/// Thread-safe via `RwLock`; the ring is rebuilt whenever the peer set changes.
///
/// Debug is manually implemented because `RwLock<ContentRing>` doesn't derive it.
pub struct ShardingService {
    ring: RwLock<ContentRing>,
    /// Our own node's peer ID (used to check if we're responsible)
    our_node_id: String,
}

impl std::fmt::Debug for ShardingService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardingService")
            .field("our_node_id", &self.our_node_id)
            .finish_non_exhaustive()
    }
}

impl ShardingService {
    /// Create a new sharding service.
    pub fn new(our_node_id: String, replication_factor: usize, vnodes_per_node: usize) -> Self {
        Self {
            ring: RwLock::new(ContentRing::new(replication_factor, vnodes_per_node)),
            our_node_id,
        }
    }

    /// Rebuild the hash ring from the current set of peers.
    ///
    /// Called by the peer tidy job when the peer set changes.
    pub async fn rebuild_from_peers(&self, peers: &[PeerNode]) {
        let mut ring = self.ring.write().await;
        let old_count = ring.node_count();

        // Rebuild from scratch to handle removals cleanly
        let rf = ring.replication_factor;
        let vnodes = ring.vnodes_per_node;
        *ring = ContentRing::new(rf, vnodes);

        for peer in peers {
            ring.add_node(&peer.node_id, &peer.http_address);
        }

        let new_count = ring.node_count();
        if old_count != new_count {
            info!(
                "Hash ring rebuilt: {} → {} nodes ({} ring positions)",
                old_count, new_count, ring.ring_size()
            );
        } else {
            debug!(
                "Hash ring refreshed: {} nodes ({} ring positions)",
                new_count, ring.ring_size()
            );
        }
    }

    /// Get the routing decision for a content hash.
    pub async fn get_routing_decision(&self, content_hash: &str) -> ContentDecision {
        let ring = self.ring.read().await;
        let responsible = ring.get_responsible_nodes(content_hash);

        let is_ours = responsible.iter().any(|p| p.node_id == self.our_node_id);

        if is_ours {
            let replicas: Vec<PeerNode> = responsible
                .into_iter()
                .filter(|p| p.node_id != self.our_node_id)
                .collect();
            ContentDecision::Owned { replicas }
        } else {
            ContentDecision::NotOwned { responsible }
        }
    }

    /// Check if we are responsible for a content hash.
    pub async fn is_responsible(&self, content_hash: &str) -> bool {
        let ring = self.ring.read().await;
        ring.is_responsible(&self.our_node_id, content_hash)
    }

    /// Get responsible nodes for a content hash (for routing hints).
    pub async fn get_responsible_nodes(&self, content_hash: &str) -> Vec<PeerNode> {
        let ring = self.ring.read().await;
        ring.get_responsible_nodes(content_hash)
    }

    /// Get our node ID.
    pub fn our_node_id(&self) -> &str {
        &self.our_node_id
    }

    /// Current number of nodes on the ring.
    pub async fn node_count(&self) -> usize {
        let ring = self.ring.read().await;
        ring.node_count()
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Extract the p2p peer ID from a multiaddress string.
///
/// Multiaddresses look like: `/ip4/1.2.3.4/tcp/4025/p2p/QmPeerIdHere`
/// Returns the peer ID component (after `/p2p/`).
pub fn extract_peer_id_from_multiaddr(multiaddr: &str) -> Option<&str> {
    let parts: Vec<&str> = multiaddr.split('/').collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "p2p" {
            if let Some(peer_id) = parts.get(i + 1) {
                if !peer_id.is_empty() {
                    return Some(peer_id);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_ring_basic() {
        let mut ring = ContentRing::new(3, 64);
        ring.add_node("node-a", "http://a:4024");
        ring.add_node("node-b", "http://b:4024");
        ring.add_node("node-c", "http://c:4024");
        ring.add_node("node-d", "http://d:4024");

        assert_eq!(ring.node_count(), 4);
        assert_eq!(ring.ring_size(), 4 * 64);

        // Should return exactly 3 responsible nodes
        let responsible = ring.get_responsible_nodes("some-content-hash");
        assert_eq!(responsible.len(), 3);

        // All should be distinct
        let node_ids: Vec<&str> = responsible.iter().map(|p| p.node_id.as_str()).collect();
        let unique: std::collections::HashSet<&str> = node_ids.iter().cloned().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_content_ring_deterministic() {
        let mut ring1 = ContentRing::new(3, 64);
        ring1.add_node("node-a", "http://a:4024");
        ring1.add_node("node-b", "http://b:4024");
        ring1.add_node("node-c", "http://c:4024");

        let mut ring2 = ContentRing::new(3, 64);
        ring2.add_node("node-a", "http://a:4024");
        ring2.add_node("node-b", "http://b:4024");
        ring2.add_node("node-c", "http://c:4024");

        // Same ring, same content hash → same result
        let r1 = ring1.get_responsible_nodes("hash-123");
        let r2 = ring2.get_responsible_nodes("hash-123");
        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.node_id, b.node_id);
        }
    }

    #[test]
    fn test_content_ring_few_nodes() {
        let mut ring = ContentRing::new(3, 64);
        ring.add_node("node-a", "http://a:4024");
        ring.add_node("node-b", "http://b:4024");

        // Only 2 nodes but replication factor 3 — should return 2
        let responsible = ring.get_responsible_nodes("content-hash");
        assert_eq!(responsible.len(), 2);
    }

    #[test]
    fn test_content_ring_single_node() {
        let mut ring = ContentRing::new(3, 64);
        ring.add_node("solo", "http://solo:4024");

        let responsible = ring.get_responsible_nodes("any-hash");
        assert_eq!(responsible.len(), 1);
        assert_eq!(responsible[0].node_id, "solo");
    }

    #[test]
    fn test_content_ring_empty() {
        let ring = ContentRing::new(3, 64);
        let responsible = ring.get_responsible_nodes("any-hash");
        assert!(responsible.is_empty());
    }

    #[test]
    fn test_content_ring_remove_node() {
        let mut ring = ContentRing::new(2, 64);
        ring.add_node("node-a", "http://a:4024");
        ring.add_node("node-b", "http://b:4024");
        ring.add_node("node-c", "http://c:4024");
        assert_eq!(ring.node_count(), 3);

        ring.remove_node("node-b");
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.ring_size(), 2 * 64);

        // Results should only include remaining nodes
        let responsible = ring.get_responsible_nodes("any-hash");
        assert_eq!(responsible.len(), 2);
        for p in &responsible {
            assert_ne!(p.node_id, "node-b");
        }
    }

    #[test]
    fn test_is_responsible() {
        let mut ring = ContentRing::new(1, 64);
        ring.add_node("node-a", "http://a:4024");
        ring.add_node("node-b", "http://b:4024");

        // With replication factor 1, exactly one node is responsible
        let r = ring.get_responsible_nodes("test-content");
        assert_eq!(r.len(), 1);
        let responsible_id = &r[0].node_id;

        assert!(ring.is_responsible(responsible_id, "test-content"));
    }

    #[test]
    fn test_distribution_uniformity() {
        // Test that content is reasonably distributed across nodes
        let mut ring = ContentRing::new(1, 64);
        for i in 0..10 {
            ring.add_node(&format!("node-{}", i), &format!("http://n{}:4024", i));
        }

        let mut counts: HashMap<String, usize> = HashMap::new();
        for i in 0..1000 {
            let r = ring.get_responsible_nodes(&format!("content-{}", i));
            *counts.entry(r[0].node_id.clone()).or_default() += 1;
        }

        // Each node should get roughly 100 items (1000/10)
        // Allow ±60% variance (40..160) for statistical tolerance
        for (node_id, count) in &counts {
            assert!(
                *count > 40 && *count < 160,
                "Node {} got {} items (expected ~100)",
                node_id, count
            );
        }
    }

    #[test]
    fn test_minimal_redistribution() {
        // Adding a node should only affect ~1/N of keys
        let mut ring = ContentRing::new(1, 64);
        for i in 0..5 {
            ring.add_node(&format!("node-{}", i), &format!("http://n{}:4024", i));
        }

        // Record assignments before
        let mut before: HashMap<String, String> = HashMap::new();
        for i in 0..500 {
            let hash = format!("content-{}", i);
            let r = ring.get_responsible_nodes(&hash);
            before.insert(hash, r[0].node_id.clone());
        }

        // Add one more node
        ring.add_node("node-5", "http://n5:4024");

        // Count how many keys changed assignment
        let mut changed = 0;
        for i in 0..500 {
            let hash = format!("content-{}", i);
            let r = ring.get_responsible_nodes(&hash);
            if before[&hash] != r[0].node_id {
                changed += 1;
            }
        }

        // Should redistribute roughly 1/6 ≈ 83 keys. Allow generous range.
        assert!(
            changed < 200,
            "Too many keys redistributed: {} (expected ~83 for 1/6)",
            changed
        );
    }

    #[test]
    fn test_extract_peer_id() {
        assert_eq!(
            extract_peer_id_from_multiaddr("/ip4/1.2.3.4/tcp/4025/p2p/QmFooBar"),
            Some("QmFooBar")
        );
        assert_eq!(
            extract_peer_id_from_multiaddr("/ip4/10.0.0.1/tcp/4025"),
            None
        );
        assert_eq!(extract_peer_id_from_multiaddr(""), None);
    }

    #[tokio::test]
    async fn test_sharding_service() {
        let svc = ShardingService::new("node-a".to_string(), 3, 64);

        svc.rebuild_from_peers(&[
            PeerNode { node_id: "node-a".to_string(), http_address: "http://a:4024".to_string() },
            PeerNode { node_id: "node-b".to_string(), http_address: "http://b:4024".to_string() },
            PeerNode { node_id: "node-c".to_string(), http_address: "http://c:4024".to_string() },
            PeerNode { node_id: "node-d".to_string(), http_address: "http://d:4024".to_string() },
        ]).await;

        assert_eq!(svc.node_count().await, 4);

        // Check a routing decision
        let decision = svc.get_routing_decision("test-content").await;
        match decision {
            ContentDecision::Owned { replicas } => {
                // We're responsible, and should have 2 replicas (K=3, minus us)
                assert_eq!(replicas.len(), 2);
            }
            ContentDecision::NotOwned { responsible } => {
                // We're not responsible, should know 3 nodes that are
                assert_eq!(responsible.len(), 3);
                assert!(responsible.iter().all(|p| p.node_id != "node-a"));
            }
        }
    }
}
