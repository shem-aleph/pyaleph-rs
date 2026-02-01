//! Message types for the Aleph network
//!
//! Messages are the core data unit of the Aleph network. They are signed
//! by users and stored/replicated across the network.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{Address, Chain, ItemHash, Signature, Timestamp};

/// Type of message
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MessageType {
    /// Aggregate messages (key-value updates)
    Aggregate,
    /// Post messages (immutable content)
    Post,
    /// Store messages (file storage)
    Store,
    /// Program messages (serverless functions)
    Program,
    /// Instance messages (VM instances)
    Instance,
    /// Forget messages (data deletion requests)
    Forget,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageType::Aggregate => write!(f, "AGGREGATE"),
            MessageType::Post => write!(f, "POST"),
            MessageType::Store => write!(f, "STORE"),
            MessageType::Program => write!(f, "PROGRAM"),
            MessageType::Instance => write!(f, "INSTANCE"),
            MessageType::Forget => write!(f, "FORGET"),
        }
    }
}

/// How the message content is stored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemType {
    /// Content stored inline in the message
    Inline,
    /// Content stored in IPFS
    Ipfs,
    /// Content stored in Aleph storage
    Storage,
}

/// A message on the Aleph network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// The type of message
    #[serde(rename = "type")]
    pub message_type: MessageType,
    
    /// The blockchain chain this message was signed on
    pub chain: Chain,
    
    /// The sender's address
    pub sender: Address,
    
    /// The signature of the message
    pub signature: Signature,
    
    /// How the content is stored
    pub item_type: ItemType,
    
    /// Hash of the content
    pub item_hash: ItemHash,
    
    /// The actual content (when item_type is inline) or None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item_content: Option<String>,
    
    /// Channel this message belongs to
    pub channel: Option<String>,
    
    /// Timestamp when the message was created
    pub time: Timestamp,
}

impl Message {
    /// Verify the message signature
    pub fn verify_signature(&self) -> Result<bool, String> {
        // TODO: Implement signature verification based on chain type
        Ok(true)
    }
    
    /// Get the item hash as bytes
    pub fn item_hash_bytes(&self) -> Result<Vec<u8>, hex::FromHexError> {
        hex::decode(&self.item_hash)
    }
}

/// Pending message (not yet confirmed)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMessage {
    pub message: Message,
    pub reception_time: Timestamp,
    pub fetched: bool,
    pub check_message: bool,
    pub retries: u32,
    pub next_attempt: Timestamp,
}

/// Aggregate message content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateContent {
    pub address: Address,
    pub key: String,
    pub content: Value,
    pub time: Timestamp,
}

/// Post message content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostContent {
    pub address: Address,
    #[serde(rename = "type")]
    pub post_type: String,
    pub content: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_: Option<String>,
    pub time: Timestamp,
}

/// Store message content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreContent {
    pub address: Address,
    pub item_type: ItemType,
    pub item_hash: ItemHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub time: Timestamp,
}

/// Forget message content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgetContent {
    pub address: Address,
    pub hashes: Vec<ItemHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub time: Timestamp,
}

/// Program (serverless function) content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramContent {
    pub address: Address,
    /// Whether to allow amends
    pub allow_amend: bool,
    /// Runtime to use
    pub runtime: RuntimeInfo,
    /// Code reference
    pub code: CodeInfo,
    /// Environment variables
    #[serde(default)]
    pub variables: Option<Value>,
    /// Volumes to mount
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    /// Memory in MiB
    pub memory: u32,
    /// vCPUs
    pub vcpus: u32,
    pub time: Timestamp,
}

/// Instance (VM) content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceContent {
    pub address: Address,
    pub allow_amend: bool,
    /// Root filesystem
    pub rootfs: RootfsInfo,
    /// Environment variables
    #[serde(default)]
    pub variables: Option<Value>,
    /// Volumes to mount
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    /// Memory in MiB
    pub memory: u32,
    /// vCPUs
    pub vcpus: u32,
    /// SSH keys for access
    #[serde(default)]
    pub ssh_keys: Vec<String>,
    /// Payment info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<PaymentInfo>,
    pub time: Timestamp,
}

/// Runtime information for programs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub ref_: String,
    pub use_latest: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Code information for programs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeInfo {
    pub encoding: String,
    pub entrypoint: String,
    pub ref_: String,
    pub use_latest: bool,
}

/// Volume information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub mount: String,
    #[serde(flatten)]
    pub source: VolumeSource,
}

/// Volume source type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VolumeSource {
    Ephemeral { ephemeral: bool, size_mib: u32 },
    Persistent { persistence: String, name: String, size_mib: u32 },
    Immutable { ref_: String, use_latest: bool },
}

/// Root filesystem information for instances
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootfsInfo {
    pub parent: RootfsParent,
    pub persistence: String,
    pub size_mib: u32,
}

/// Root filesystem parent reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootfsParent {
    pub ref_: String,
    pub use_latest: bool,
}

/// Payment information for instances
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentInfo {
    pub chain: Chain,
    #[serde(rename = "type")]
    pub payment_type: PaymentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receiver: Option<Address>,
}

/// Payment type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PaymentType {
    Hold,
    Superfluid,
    Credit,
}
