//! Chain-related types

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Supported blockchain networks
/// 
/// Matches Python pyaleph chain support (aleph_message.models.Chain)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Chain {
    // EVM chains
    ETH,
    AVAX,
    BASE,
    BSC,
    ARBITRUM,
    BLAST,
    BOB,
    CYBER,
    FRAXTAL,
    HYPE,
    INK,
    LENS,
    LINEA,
    LISK,
    METIS,
    MODE,
    NEO,
    OPTIMISM,
    POL,
    SONIC,
    UNICHAIN,
    WORLDCHAIN,
    ZORA,
    ETHERLINK,
    
    // Non-EVM chains
    SOL,      // Solana (Ed25519)
    ECLIPSE,  // Eclipse (Solana-compatible)
    CSDK,     // Cosmos SDK
    DOT,      // Polkadot/Substrate
    NULS,     // NULS v1
    NULS2,    // NULS v2
    TEZOS,    // Tezos (multiple sig schemes)
}

impl fmt::Display for Chain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Chain::ETH => write!(f, "ETH"),
            Chain::AVAX => write!(f, "AVAX"),
            Chain::BASE => write!(f, "BASE"),
            Chain::BSC => write!(f, "BSC"),
            Chain::ARBITRUM => write!(f, "ARBITRUM"),
            Chain::BLAST => write!(f, "BLAST"),
            Chain::BOB => write!(f, "BOB"),
            Chain::CYBER => write!(f, "CYBER"),
            Chain::FRAXTAL => write!(f, "FRAXTAL"),
            Chain::HYPE => write!(f, "HYPE"),
            Chain::INK => write!(f, "INK"),
            Chain::LENS => write!(f, "LENS"),
            Chain::LINEA => write!(f, "LINEA"),
            Chain::LISK => write!(f, "LISK"),
            Chain::METIS => write!(f, "METIS"),
            Chain::MODE => write!(f, "MODE"),
            Chain::NEO => write!(f, "NEO"),
            Chain::OPTIMISM => write!(f, "OPTIMISM"),
            Chain::POL => write!(f, "POL"),
            Chain::SONIC => write!(f, "SONIC"),
            Chain::UNICHAIN => write!(f, "UNICHAIN"),
            Chain::WORLDCHAIN => write!(f, "WORLDCHAIN"),
            Chain::ZORA => write!(f, "ZORA"),
            Chain::ETHERLINK => write!(f, "ETHERLINK"),
            Chain::SOL => write!(f, "SOL"),
            Chain::ECLIPSE => write!(f, "ECLIPSE"),
            Chain::CSDK => write!(f, "CSDK"),
            Chain::DOT => write!(f, "DOT"),
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
            "AVAX" | "AVALANCHE" => Ok(Chain::AVAX),
            "BASE" => Ok(Chain::BASE),
            "BSC" | "BNB" => Ok(Chain::BSC),
            "ARBITRUM" | "ARB" => Ok(Chain::ARBITRUM),
            "BLAST" => Ok(Chain::BLAST),
            "BOB" => Ok(Chain::BOB),
            "CYBER" => Ok(Chain::CYBER),
            "FRAXTAL" => Ok(Chain::FRAXTAL),
            "HYPE" => Ok(Chain::HYPE),
            "INK" => Ok(Chain::INK),
            "LENS" => Ok(Chain::LENS),
            "LINEA" => Ok(Chain::LINEA),
            "LISK" => Ok(Chain::LISK),
            "METIS" => Ok(Chain::METIS),
            "MODE" => Ok(Chain::MODE),
            "NEO" => Ok(Chain::NEO),
            "OPTIMISM" | "OP" => Ok(Chain::OPTIMISM),
            "POL" | "POLYGON" | "MATIC" => Ok(Chain::POL),
            "SONIC" => Ok(Chain::SONIC),
            "UNICHAIN" => Ok(Chain::UNICHAIN),
            "WORLDCHAIN" => Ok(Chain::WORLDCHAIN),
            "ZORA" => Ok(Chain::ZORA),
            "ETHERLINK" => Ok(Chain::ETHERLINK),
            "SOL" | "SOLANA" => Ok(Chain::SOL),
            "ECLIPSE" => Ok(Chain::ECLIPSE),
            "CSDK" | "COSMOS" => Ok(Chain::CSDK),
            "DOT" | "POLKADOT" => Ok(Chain::DOT),
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
    
    /// Check if this chain uses EVM-compatible signatures (EIP-191)
    pub fn is_evm_compatible(&self) -> bool {
        matches!(self, 
            Chain::ETH | Chain::AVAX | Chain::BASE | Chain::BSC |
            Chain::ARBITRUM | Chain::BLAST | Chain::BOB | Chain::CYBER |
            Chain::FRAXTAL | Chain::HYPE | Chain::INK | Chain::LENS |
            Chain::LINEA | Chain::LISK | Chain::METIS | Chain::MODE |
            Chain::NEO | Chain::OPTIMISM | Chain::POL | Chain::SONIC |
            Chain::UNICHAIN | Chain::WORLDCHAIN | Chain::ZORA | Chain::ETHERLINK
        )
    }
    
    /// Check if this chain uses Ed25519 signatures
    pub fn uses_ed25519(&self) -> bool {
        matches!(self, Chain::SOL | Chain::ECLIPSE)
    }
    
    /// Check if this chain uses Substrate (Sr25519/Ed25519)
    pub fn is_substrate(&self) -> bool {
        matches!(self, Chain::DOT)
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
