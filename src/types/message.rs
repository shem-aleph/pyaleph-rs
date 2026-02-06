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

impl std::fmt::Display for ItemType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemType::Inline => write!(f, "inline"),
            ItemType::Ipfs => write!(f, "ipfs"),
            ItemType::Storage => write!(f, "storage"),
        }
    }
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
    /// Verify the message signature using the CryptoService
    /// 
    /// This is CRITICAL for security - never accept messages with invalid signatures.
    /// 
    /// # Arguments
    /// 
    /// * `crypto` - The cryptographic service to use for verification
    /// 
    /// # Returns
    /// 
    /// * `Ok(true)` if the signature is valid
    /// * `Ok(false)` if the signature is invalid
    /// * `Err(String)` if verification could not be performed
    pub fn verify_signature(&self, crypto: &crate::services::crypto::CryptoService) -> Result<bool, String> {
        // Use the correct verification buffer format: {chain}\n{sender}\n{type}\n{item_hash}
        crypto.verify_message_signature(
            &self.chain,
            &self.sender,
            &self.message_type,
            &self.item_hash,
            &self.signature,
        ).map_err(|e| e.to_string())
    }
    
    /// Get the item hash as bytes
    pub fn item_hash_bytes(&self) -> Result<Vec<u8>, hex::FromHexError> {
        hex::decode(&self.item_hash)
    }
    
    /// Compute and verify the item hash matches the content
    pub fn verify_item_hash(&self) -> Result<bool, String> {
        let content = self.item_content.as_ref()
            .ok_or_else(|| "No item_content to verify hash".to_string())?;
        
        use sha2::{Sha256, Digest};
        let computed_hash = hex::encode(Sha256::digest(content.as_bytes()));
        
        Ok(computed_hash == self.item_hash)
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
    #[serde(skip_serializing_if = "Option::is_none", rename = "ref")]
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
    #[serde(skip_serializing_if = "Option::is_none", rename = "ref")]
    pub ref_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<PaymentInfo>,
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

/// VM resource requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmResources {
    pub memory: u32,
    pub vcpus: u32,
    #[serde(default = "default_seconds")]
    pub seconds: u32,
}

fn default_seconds() -> u32 { 30 }

/// VM environment settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmEnvironment {
    #[serde(default)]
    pub reproducible: bool,
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub aleph_api: bool,
    #[serde(default)]
    pub shared_cache: bool,
    #[serde(default)]
    pub hypervisor: Option<String>,
}

/// VM node/CPU requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmRequirements {
    #[serde(default)]
    pub cpu: Option<CpuRequirements>,
    #[serde(default)]
    pub node: Option<NodeRequirements>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuRequirements {
    pub architecture: Option<String>,
    pub vendor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRequirements {
    pub node_hash: Option<String>,
    pub address_regex: Option<String>,
    pub owner: Option<String>,
}

fn default_true() -> bool { true }

/// Program (serverless function) content — matches on-chain JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramContent {
    pub address: Address,
    #[serde(default = "default_true")]
    pub allow_amend: bool,
    pub runtime: RuntimeInfo,
    pub code: CodeInfo,
    pub resources: VmResources,
    #[serde(default)]
    pub environment: Option<VmEnvironment>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub variables: Option<Value>,
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    #[serde(default)]
    pub payment: Option<PaymentInfo>,
    #[serde(default)]
    pub requirements: Option<VmRequirements>,
    #[serde(default)]
    pub replaces: Option<String>,
    #[serde(default)]
    pub data: Option<DataInfo>,
    #[serde(default)]
    pub export: Option<ExportInfo>,
    pub time: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataInfo {
    #[serde(rename = "ref")]
    pub ref_: String,
    pub use_latest: bool,
    #[serde(default)]
    pub mount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportInfo {
    pub encoding: Option<String>,
}

/// Instance (VM) content — matches on-chain JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceContent {
    pub address: Address,
    #[serde(default = "default_true")]
    pub allow_amend: bool,
    pub rootfs: RootfsInfo,
    pub resources: VmResources,
    #[serde(default)]
    pub environment: Option<VmEnvironment>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub variables: Option<Value>,
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    #[serde(default)]
    pub authorized_keys: Vec<String>,
    #[serde(default)]
    pub payment: Option<PaymentInfo>,
    #[serde(default)]
    pub requirements: Option<VmRequirements>,
    /// For amendments — references the original instance item_hash
    #[serde(default)]
    pub replaces: Option<String>,
    pub time: Timestamp,
}

/// Runtime information for programs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    #[serde(rename = "ref")]
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
    #[serde(rename = "ref")]
    pub ref_: String,
    pub use_latest: bool,
}

/// Volume information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    #[serde(default)]
    pub mount: String,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(flatten)]
    pub source: VolumeSource,
}

/// Volume source type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VolumeSource {
    Ephemeral { ephemeral: bool, size_mib: u32 },
    Persistent { persistence: String, name: String, size_mib: u32 },
    Immutable { #[serde(rename = "ref")] ref_: String, use_latest: bool },
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
    #[serde(rename = "ref")]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rootfs_parent_deser() {
        let json = r#"{"ref": "abc123", "use_latest": true}"#;
        let parent: RootfsParent = serde_json::from_str(json).unwrap();
        assert_eq!(parent.ref_, "abc123");
        assert!(parent.use_latest);
    }

    #[test]
    fn test_instance_content_real_data() {
        let json = r#"{
            "address": "0xf594985F5C271005a4dA1c4F107AB42a7166aEF7",
            "allow_amend": false,
            "authorized_keys": ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5"],
            "environment": {"reproducible": false, "internet": true, "aleph_api": true, "shared_cache": false, "hypervisor": "qemu"},
            "metadata": {"name": "SV-1"},
            "payment": {"chain": "ETH", "type": "hold"},
            "resources": {"memory": 8192, "seconds": 30, "vcpus": 4},
            "rootfs": {"parent": {"ref": "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717", "use_latest": true}, "persistence": "host", "size_mib": 81920},
            "time": 1770280454.236,
            "volumes": []
        }"#;
        let content: InstanceContent = serde_json::from_str(json).unwrap();
        assert_eq!(content.resources.memory, 8192);
        assert_eq!(content.resources.vcpus, 4);
        assert_eq!(content.authorized_keys.len(), 1);
        assert_eq!(content.rootfs.parent.ref_, "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717");
        assert!(!content.allow_amend);
    }

    #[test]
    fn test_instance_content_with_requirements() {
        let json = r#"{
            "address": "0x25020a5F992a70E61686D21A473f464d86BaF9BD",
            "allow_amend": false,
            "authorized_keys": [],
            "environment": {"aleph_api": true, "hypervisor": "qemu", "internet": true, "reproducible": false, "shared_cache": false},
            "payment": {"chain": "ETH", "type": "credit"},
            "requirements": {"node": {"node_hash": "d02cc93b18e23f62556cc574fa3696b350cae36e760c43186cbb866c6677c628"}},
            "resources": {"memory": 2048, "seconds": 30, "vcpus": 2},
            "rootfs": {"parent": {"ref": "abc123", "use_latest": true}, "persistence": "host", "size_mib": 20480},
            "time": 1770217145.481,
            "volumes": []
        }"#;
        let content: InstanceContent = serde_json::from_str(json).unwrap();
        assert!(content.requirements.is_some());
        assert_eq!(content.requirements.unwrap().node.unwrap().node_hash.unwrap(), "d02cc93b18e23f62556cc574fa3696b350cae36e760c43186cbb866c6677c628");
    }

    #[test]
    fn test_instance_content_with_volumes() {
        let json = r#"{
            "address": "0x1234567890abcdef",
            "allow_amend": false,
            "authorized_keys": [],
            "resources": {"memory": 2048, "seconds": 30, "vcpus": 1},
            "rootfs": {"parent": {"ref": "abc", "use_latest": true}, "persistence": "host", "size_mib": 20480},
            "time": 1768920524.78,
            "volumes": [{"comment": "persist one", "mount": "/mnt/persist-one", "name": "persist-one", "persistence": "host", "size_mib": 30000}]
        }"#;
        let content: InstanceContent = serde_json::from_str(json).unwrap();
        assert_eq!(content.volumes.len(), 1);
        assert_eq!(content.volumes[0].mount, "/mnt/persist-one");
    }

    #[test]
    fn test_instance_content_minimal() {
        // Test that defaults work (allow_amend defaults to true, etc.)
        let json = r#"{
            "address": "0xABC",
            "rootfs": {"parent": {"ref": "hash123", "use_latest": true}, "persistence": "host", "size_mib": 20480},
            "resources": {"memory": 2048, "vcpus": 1},
            "time": 1234567890.0
        }"#;
        let content: InstanceContent = serde_json::from_str(json).unwrap();
        assert!(content.allow_amend); // default true
        assert_eq!(content.resources.seconds, 30); // default 30
        assert!(content.authorized_keys.is_empty());
        assert!(content.volumes.is_empty());
    }

    #[test]
    fn test_runtime_info_deser() {
        let json = r#"{"ref": "runtime_hash_123", "use_latest": true, "comment": "ASGI"}"#;
        let info: RuntimeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.ref_, "runtime_hash_123");
    }

    #[test]
    fn test_code_info_deser() {
        let json = r#"{"encoding": "zip", "entrypoint": "main:app", "ref": "code_hash_456", "use_latest": true}"#;
        let info: CodeInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.ref_, "code_hash_456");
    }
}
