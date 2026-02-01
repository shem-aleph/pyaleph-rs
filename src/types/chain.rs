//! Chain-related types

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported blockchain networks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Chain {
    ETH,
    SOL,
    AVAX,
    BASE,
    BSC,
    CSDK,
    DOT,
    NEO,
    NULS,
    TEZOS,
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Chain::ETH => write!(f, "ETH"),
            Chain::SOL => write!(f, "SOL"),
            Chain::AVAX => write!(f, "AVAX"),
            Chain::BASE => write!(f, "BASE"),
            Chain::BSC => write!(f, "BSC"),
            Chain::CSDK => write!(f, "CSDK"),
            Chain::DOT => write!(f, "DOT"),
            Chain::NEO => write!(f, "NEO"),
            Chain::NULS => write!(f, "NULS"),
            Chain::TEZOS => write!(f, "TEZOS"),
        }
    }
}

impl Chain {
    /// Get the chain from a string identifier
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ETH" => Some(Chain::ETH),
            "SOL" => Some(Chain::SOL),
            "AVAX" => Some(Chain::AVAX),
            "BASE" => Some(Chain::BASE),
            "BSC" => Some(Chain::BSC),
            "CSDK" => Some(Chain::CSDK),
            "DOT" => Some(Chain::DOT),
            "NEO" => Some(Chain::NEO),
            "NULS" => Some(Chain::NULS),
            "TEZOS" => Some(Chain::TEZOS),
            _ => None,
        }
    }
}

/// Protocol for chain synchronization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainSyncProtocol {
    /// Message sync tx where the messages are in the tx data
    #[serde(rename = "aleph")]
    OnChainSync,
    /// Message sync tx where the messages to fetch are in an IPFS hash
    #[serde(rename = "aleph-offchain")]
    OffChainSync,
    /// Messages sent by a smart contract
    #[serde(rename = "smart-contract")]
    SmartContract,
}

/// Type of chain event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainEventType {
    /// Messages sent on-chain using the Aleph smart contract
    Message,
    /// Synchronization messages sent by a CCN to the Aleph smart contract
    Sync,
}

/// A blockchain transaction reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainRef {
    pub chain: Chain,
    pub height: u64,
    pub hash: String,
}
