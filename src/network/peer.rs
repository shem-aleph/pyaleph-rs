//! Peer management

use std::time::Instant;
use serde::{Deserialize, Serialize};

use super::PeerId;

/// Peer state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// Attempting to connect
    Connecting,
    /// Connected and active
    Connected,
    /// Temporarily disconnected, will retry
    Disconnected,
    /// Banned (too many errors or malicious behavior)
    Banned,
}

/// Extended peer information
#[derive(Debug, Clone)]
pub struct Peer {
    pub id: PeerId,
    pub addresses: Vec<String>,
    pub state: PeerState,
    pub connected_at: Option<Instant>,
    pub last_seen: Instant,
    pub version: Option<String>,
    pub capabilities: Vec<String>,
    
    // Stats
    pub messages_received: u64,
    pub messages_sent: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub errors: u32,
    
    // Reputation (simple scoring)
    pub reputation: i32,
}

impl Peer {
    pub fn new(id: PeerId, address: String) -> Self {
        Self {
            id,
            addresses: vec![address],
            state: PeerState::Connecting,
            connected_at: None,
            last_seen: Instant::now(),
            version: None,
            capabilities: vec![],
            messages_received: 0,
            messages_sent: 0,
            bytes_received: 0,
            bytes_sent: 0,
            errors: 0,
            reputation: 100, // Start with neutral reputation
        }
    }
    
    /// Mark peer as connected
    pub fn connected(&mut self) {
        self.state = PeerState::Connected;
        self.connected_at = Some(Instant::now());
        self.last_seen = Instant::now();
    }
    
    /// Mark peer as disconnected
    pub fn disconnected(&mut self) {
        self.state = PeerState::Disconnected;
    }
    
    /// Update last seen time
    pub fn seen(&mut self) {
        self.last_seen = Instant::now();
    }
    
    /// Record an error
    pub fn record_error(&mut self) {
        self.errors += 1;
        self.reputation -= 10;
        
        // Ban if too many errors
        if self.errors > 10 || self.reputation < -100 {
            self.state = PeerState::Banned;
        }
    }
    
    /// Check if peer is banned
    pub fn is_banned(&self) -> bool {
        self.state == PeerState::Banned
    }
    
    /// Check if peer is connected
    pub fn is_connected(&self) -> bool {
        self.state == PeerState::Connected
    }
    
    /// Add reputation
    pub fn add_reputation(&mut self, amount: i32) {
        self.reputation = (self.reputation + amount).clamp(-1000, 1000);
    }
}

/// Peer discovery methods
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryMethod {
    /// Manually configured bootstrap peer
    Bootstrap,
    /// Discovered via DHT
    Dht,
    /// Received from another peer
    PeerExchange,
    /// mDNS local discovery
    Mdns,
}
