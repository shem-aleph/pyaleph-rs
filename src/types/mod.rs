//! Core types for the Aleph network
//!
//! This module defines all the fundamental types used throughout the Aleph
//! network implementation.

mod chain;
mod channel;
mod message;
mod message_status;
mod origin;
mod pricing;

pub use chain::*;
pub use channel::*;
pub use message::*;
pub use message_status::*;
pub use origin::*;
pub use pricing::*;

// Note: Deserialize and Serialize are re-exported by submodules

/// A blockchain address (hex-encoded string)
pub type Address = String;

/// An item hash (SHA256 hash of message content)
pub type ItemHash = String;

/// A transaction hash
pub type TxHash = String;

/// Content hash for IPFS
pub type IpfsHash = String;

/// Signature (hex-encoded)
pub type Signature = String;

/// Unix timestamp in seconds
pub type Timestamp = f64;
