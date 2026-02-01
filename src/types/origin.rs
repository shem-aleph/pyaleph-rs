//! Message origin tracking
//!
//! Tracks how messages were received by the node.
//! Matches Python: aleph/types/message_status.py (MessageOrigin)

use serde::{Deserialize, Serialize};
use std::fmt;

/// Origin/source of a message
/// 
/// Used to track where a message came from for processing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageOrigin {
    /// Message came from on-chain transaction
    #[serde(rename = "onchain")]
    OnChain,
    /// Message came from P2P network
    P2P,
    /// Message came from IPFS content fetch
    Ipfs,
}

impl fmt::Display for MessageOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageOrigin::OnChain => write!(f, "onchain"),
            MessageOrigin::P2P => write!(f, "p2p"),
            MessageOrigin::Ipfs => write!(f, "ipfs"),
        }
    }
}

impl MessageOrigin {
    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "onchain" | "on-chain" | "on_chain" => Some(MessageOrigin::OnChain),
            "p2p" | "peer" => Some(MessageOrigin::P2P),
            "ipfs" => Some(MessageOrigin::Ipfs),
            _ => None,
        }
    }
    
    /// Whether messages from this origin should be trusted more
    /// 
    /// On-chain messages have been confirmed in a blockchain transaction
    /// and should be given priority in case of conflicts.
    pub fn is_trusted(&self) -> bool {
        matches!(self, MessageOrigin::OnChain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_serialization() {
        let origin = MessageOrigin::OnChain;
        let json = serde_json::to_string(&origin).unwrap();
        assert_eq!(json, "\"onchain\"");
        
        let origin = MessageOrigin::P2P;
        let json = serde_json::to_string(&origin).unwrap();
        assert_eq!(json, "\"p2p\"");
    }
    
    #[test]
    fn test_parsing() {
        assert_eq!(MessageOrigin::from_str("onchain"), Some(MessageOrigin::OnChain));
        assert_eq!(MessageOrigin::from_str("p2p"), Some(MessageOrigin::P2P));
        assert_eq!(MessageOrigin::from_str("ipfs"), Some(MessageOrigin::Ipfs));
        assert_eq!(MessageOrigin::from_str("invalid"), None);
    }
}
