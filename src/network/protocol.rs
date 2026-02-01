//! Network protocol definitions

use serde::{Deserialize, Serialize};

use crate::types::{Message, ItemHash};
use super::PeerId;

/// Protocol version
pub const PROTOCOL_VERSION: &str = "aleph/1.0.0";

/// Protocol message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtocolMessage {
    /// Handshake message (sent on connection)
    Handshake(Handshake),
    
    /// Ping message
    Ping { nonce: u64 },
    
    /// Pong response
    Pong { nonce: u64 },
    
    /// New message announcement
    NewMessage {
        item_hash: ItemHash,
        message_type: String,
        sender: String,
    },
    
    /// Request message content
    GetMessage { item_hash: ItemHash },
    
    /// Message content response
    Message { message: Message },
    
    /// Request messages since a certain time
    GetMessagesSince { timestamp: f64, limit: u32 },
    
    /// Multiple messages response
    Messages { messages: Vec<Message> },
    
    /// Request known peers
    GetPeers,
    
    /// Peers response
    Peers { peers: Vec<PeerAddress> },
    
    /// Error response
    Error { code: u32, message: String },
}

/// Handshake message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handshake {
    /// Protocol version
    pub version: String,
    /// Node implementation
    pub implementation: String,
    /// Node capabilities
    pub capabilities: Vec<String>,
    /// Listening addresses
    pub listen_addrs: Vec<String>,
    /// Best known block (for chain sync)
    pub best_block: Option<u64>,
}

impl Default for Handshake {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION.to_string(),
            implementation: format!("aleph-core-rs/{}", env!("CARGO_PKG_VERSION")),
            capabilities: vec![
                "messages".to_string(),
                "aggregates".to_string(),
                "posts".to_string(),
                "programs".to_string(),
                "instances".to_string(),
            ],
            listen_addrs: vec![],
            best_block: None,
        }
    }
}

/// Peer address for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAddress {
    pub id: PeerId,
    pub addresses: Vec<String>,
}

/// Message encoding
pub mod encoding {
    use super::ProtocolMessage;
    
    /// Encode a protocol message to bytes
    pub fn encode(msg: &ProtocolMessage) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(msg)
    }
    
    /// Decode a protocol message from bytes
    pub fn decode(data: &[u8]) -> Result<ProtocolMessage, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

/// Error codes
pub mod errors {
    pub const INVALID_MESSAGE: u32 = 1;
    pub const NOT_FOUND: u32 = 2;
    pub const INTERNAL_ERROR: u32 = 3;
    pub const RATE_LIMITED: u32 = 4;
    pub const UNAUTHORIZED: u32 = 5;
}
