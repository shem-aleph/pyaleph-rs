//! P2P Network module
//!
//! Handles peer-to-peer communication using TCP with length-prefixed framing.
//! The protocol uses JSON-encoded messages with a 4-byte big-endian length prefix.

pub mod peer;
pub mod protocol;
pub mod rabbitmq;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{info, debug, warn};

use crate::config::P2pConfig;
use crate::types::Message;

/// Peer ID (simplified - would be cryptographic in full impl)
pub type PeerId = String;

/// Maximum message size (10 MB)
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Network event
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// New peer connected
    PeerConnected(PeerId),
    /// Peer disconnected
    PeerDisconnected(PeerId),
    /// Message received from peer
    MessageReceived { from: PeerId, message: Message },
    /// Sync request from peer
    SyncRequest { from: PeerId, since_block: u64 },
}

/// Network service for P2P communication
pub struct NetworkService {
    config: P2pConfig,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    connections: Arc<RwLock<HashMap<PeerId, PeerConnection>>>,
    event_tx: mpsc::Sender<NetworkEvent>,
    event_rx: mpsc::Receiver<NetworkEvent>,
}

/// Information about a connected peer (clonable metadata)
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub id: PeerId,
    pub address: String,
    pub connected_at: std::time::Instant,
    pub last_seen: std::time::Instant,
    pub messages_received: u64,
    pub messages_sent: u64,
}

/// Active TCP connection state (not clonable)
struct PeerConnection {
    writer: Arc<Mutex<OwnedWriteHalf>>,
    _reader_task: JoinHandle<()>,
}

impl NetworkService {
    /// Create a new network service
    pub fn new(config: &P2pConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(1000);

        Self {
            config: config.clone(),
            peers: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx,
        }
    }

    /// Start the network service
    pub async fn start(&mut self) -> anyhow::Result<()> {
        if !self.config.enabled {
            info!("P2P networking disabled");
            return Ok(());
        }

        info!("Starting P2P network service");
        info!("  Listen addresses: {:?}", self.config.listen_addrs);
        info!("  Bootstrap peers: {:?}", self.config.bootstrap_peers);
        info!("  Topic: {}", self.config.topic);

        // Connect to bootstrap peers
        for peer_addr in &self.config.bootstrap_peers.clone() {
            if let Err(e) = self.connect_peer(peer_addr).await {
                warn!("Failed to connect to bootstrap peer {}: {}", peer_addr, e);
            }
        }

        Ok(())
    }

    /// Connect to a peer via TCP
    pub async fn connect_peer(&self, address: &str) -> anyhow::Result<PeerId> {
        info!("Connecting to peer: {}", address);

        let stream = tokio::net::TcpStream::connect(address).await?;
        let (reader, writer) = stream.into_split();

        let peer_id = format!("peer_{}", address.replace(':', "_").replace('/', "_"));

        // Store peer metadata
        let info = PeerInfo {
            id: peer_id.clone(),
            address: address.to_string(),
            connected_at: std::time::Instant::now(),
            last_seen: std::time::Instant::now(),
            messages_received: 0,
            messages_sent: 0,
        };
        self.peers.write().await.insert(peer_id.clone(), info);

        // Spawn read loop and store connection
        let reader_task = tokio::spawn(Self::read_loop(
            peer_id.clone(),
            reader,
            self.event_tx.clone(),
        ));

        let connection = PeerConnection {
            writer: Arc::new(Mutex::new(writer)),
            _reader_task: reader_task,
        };
        self.connections.write().await.insert(peer_id.clone(), connection);

        let _ = self.event_tx.send(NetworkEvent::PeerConnected(peer_id.clone())).await;

        Ok(peer_id)
    }

    /// Read loop for a peer connection (length-prefixed framing)
    async fn read_loop(
        peer_id: PeerId,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        event_tx: mpsc::Sender<NetworkEvent>,
    ) {
        let mut len_buf = [0u8; 4];
        loop {
            // Read 4-byte length prefix
            if reader.read_exact(&mut len_buf).await.is_err() {
                let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
                return;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > MAX_MESSAGE_SIZE {
                warn!("Message from {} too large ({} bytes), disconnecting", peer_id, len);
                let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
                return;
            }

            let mut msg_buf = vec![0u8; len];
            if reader.read_exact(&mut msg_buf).await.is_err() {
                let _ = event_tx.send(NetworkEvent::PeerDisconnected(peer_id)).await;
                return;
            }

            match serde_json::from_slice::<Message>(&msg_buf) {
                Ok(msg) => {
                    let _ = event_tx.send(NetworkEvent::MessageReceived {
                        from: peer_id.clone(),
                        message: msg,
                    }).await;
                }
                Err(e) => {
                    warn!("Failed to deserialize message from {}: {}", peer_id, e);
                }
            }
        }
    }

    /// Disconnect from a peer
    pub async fn disconnect_peer(&self, peer_id: &PeerId) -> anyhow::Result<()> {
        info!("Disconnecting from peer: {}", peer_id);

        self.peers.write().await.remove(peer_id);
        self.connections.write().await.remove(peer_id);

        let _ = self.event_tx.send(NetworkEvent::PeerDisconnected(peer_id.clone())).await;

        Ok(())
    }

    /// Broadcast a message to all connected peers
    pub async fn broadcast(&self, message: &Message) -> Vec<(PeerId, anyhow::Error)> {
        let peer_ids: Vec<PeerId> = self.connections.read().await.keys().cloned().collect();
        let mut errors = Vec::new();

        debug!("Broadcasting message to {} peers", peer_ids.len());

        for peer_id in &peer_ids {
            if let Err(e) = self.send_to_peer(peer_id, message).await {
                errors.push((peer_id.clone(), e));
            }
        }

        errors
    }

    /// Send a message to a specific peer using length-prefixed framing
    pub async fn send_to_peer(&self, peer_id: &PeerId, message: &Message) -> anyhow::Result<()> {
        let connections = self.connections.read().await;
        let conn = connections.get(peer_id)
            .ok_or_else(|| anyhow::anyhow!("No connection to peer: {}", peer_id))?;

        let payload = serde_json::to_vec(message)?;
        let len = (payload.len() as u32).to_be_bytes();

        let mut writer = conn.writer.lock().await;
        writer.write_all(&len).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;

        drop(writer);
        drop(connections);

        // Update stats
        if let Some(peer) = self.peers.write().await.get_mut(peer_id) {
            peer.messages_sent += 1;
        }

        Ok(())
    }

    /// Get list of connected peers
    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        self.peers.read().await.values().cloned().collect()
    }

    /// Get number of connected peers
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Get next network event
    pub async fn next_event(&mut self) -> Option<NetworkEvent> {
        self.event_rx.recv().await
    }
}

/// Start the network service as a background task
pub async fn start_network(config: &P2pConfig) -> anyhow::Result<Arc<RwLock<NetworkService>>> {
    let mut service = NetworkService::new(config);
    service.start().await?;

    let service = Arc::new(RwLock::new(service));

    // Start event processing loop
    let service_clone = service.clone();
    tokio::spawn(async move {
        loop {
            let event = {
                let mut svc = service_clone.write().await;
                svc.next_event().await
            };

            match event {
                Some(NetworkEvent::PeerConnected(peer)) => {
                    info!("Peer connected: {}", peer);
                }
                Some(NetworkEvent::PeerDisconnected(ref peer)) => {
                    info!("Peer disconnected: {}", peer);
                    // Clean up connection state
                    let svc = service_clone.read().await;
                    svc.peers.write().await.remove(peer);
                    svc.connections.write().await.remove(peer);
                }
                Some(NetworkEvent::MessageReceived { ref from, ref message }) => {
                    debug!("Message received from {}: {}", from, message.item_hash);
                    // Update peer stats
                    let svc = service_clone.read().await;
                    let mut peers = svc.peers.write().await;
                    if let Some(peer) = peers.get_mut(from) {
                        peer.messages_received += 1;
                        peer.last_seen = std::time::Instant::now();
                    }
                    drop(peers);
                }
                Some(NetworkEvent::SyncRequest { from, since_block }) => {
                    debug!("Sync request from {} since block {}", from, since_block);
                }
                None => {
                    // Channel closed
                    break;
                }
            }
        }
    });

    Ok(service)
}
