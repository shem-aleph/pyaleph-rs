//! Chain-related types

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Supported blockchain networks
/// 
/// Matches Python pyaleph chain support
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
    NULS2,  // Added: Python supports both NULS and NULS2
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
            Chain::NULS2 => write!(f, "NULS2"),
            Chain::TEZOS => write!(f, "TEZOS"),
        }
    }
}

impl FromStr for Chain {
    type Err = String;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ETH" | "ETHEREUM" => Ok(Chain::ETH),
            "SOL" | "SOLANA" => Ok(Chain::SOL),
            "AVAX" | "AVALANCHE" => Ok(Chain::AVAX),
            "BASE" => Ok(Chain::BASE),
            "BSC" | "BNB" => Ok(Chain::BSC),
            "CSDK" | "COSMOS" => Ok(Chain::CSDK),
            "DOT" | "POLKADOT" => Ok(Chain::DOT),
            "NEO" => Ok(Chain::NEO),
            "NULS" => Ok(Chain::NULS),
            "NULS2" => Ok(Chain::NULS2),
            "TEZOS" | "XTZ" => Ok(Chain::TEZOS),
            _ => Err(format!("Unknown chain: {}", s)),
        }
    }
}

impl Chain {
    /// Get the chain from a string identifier (returns Option for compatibility)
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }
    
    /// Check if this chain uses EVM-compatible signatures
    pub fn is_evm_compatible(&self) -> bool {
        matches!(self, Chain::ETH | Chain::AVAX | Chain::BASE | Chain::BSC)
    }
    
    /// Check if this chain uses Ed25519 signatures
    pub fn uses_ed25519(&self) -> bool {
        matches!(self, Chain::SOL)
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

/// Confirmation of a message on a blockchain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageConfirmation {
    pub chain: Chain,
    pub hash: String,
    pub height: u64,
}
