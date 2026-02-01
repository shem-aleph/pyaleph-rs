//! Message handlers module
//!
//! Processes incoming messages based on their type.

pub mod aggregate;
pub mod forget;
pub mod instance;
pub mod post;
pub mod program;
pub mod store;

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

use crate::types::{Message, MessageType, ProcessingStatus, ErrorCode};
use crate::services::crypto::CryptoService;

pub use aggregate::AggregateElement;

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("Invalid message content: {0}")]
    InvalidContent(String),
    
    #[error("Unauthorized sender")]
    Unauthorized,
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Insufficient balance")]
    InsufficientBalance,
    
    #[error("Insufficient credit")]
    InsufficientCredit,
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Content not found: {0}")]
    ContentNotFound(String),
    
    #[error("Target not found: {0}")]
    TargetNotFound(String),
    
    #[error("Not allowed: {0}")]
    NotAllowed(String),
    
    #[error("Duplicate operation: {0}")]
    Duplicate(String),
    
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),
}

impl HandlerError {
    /// Convert to the appropriate ErrorCode for API responses
    pub fn error_code(&self) -> ErrorCode {
        match self {
            HandlerError::InvalidContent(_) => ErrorCode::InvalidFormat,
            HandlerError::Unauthorized => ErrorCode::PermissionDenied,
            HandlerError::PermissionDenied(_) => ErrorCode::PermissionDenied,
            HandlerError::InsufficientBalance => ErrorCode::BalanceInsufficient,
            HandlerError::InsufficientCredit => ErrorCode::CreditInsufficient,
            HandlerError::Storage(_) => ErrorCode::FileUnavailable,
            HandlerError::Database(_) => ErrorCode::InternalError,
            HandlerError::ContentNotFound(_) => ErrorCode::ContentUnavailable,
            HandlerError::TargetNotFound(_) => ErrorCode::ContentUnavailable,
            HandlerError::NotAllowed(_) => ErrorCode::PermissionDenied,
            HandlerError::Duplicate(_) => ErrorCode::ForgottenDuplicate,
            HandlerError::InvalidSignature(_) => ErrorCode::InvalidSignature,
        }
    }
}

impl From<HandlerError> for ProcessingStatus {
    fn from(err: HandlerError) -> Self {
        let code = err.error_code();
        ProcessingStatus::rejected(code, err.to_string())
    }
}

/// Context for message processing
/// 
/// Contains all services needed by handlers.
pub struct HandlerContext {
    /// Database for persistence
    pub db: Option<Arc<dyn Database>>,
    /// Cryptographic service for signature verification
    pub crypto: Option<Arc<CryptoService>>,
    /// IPFS service for content fetching/pinning
    pub ipfs: Option<Arc<dyn IpfsService>>,
    /// Storage service for cost calculation
    pub storage: Option<Arc<dyn StorageService>>,
}

impl HandlerContext {
    pub fn new() -> Self {
        Self {
            db: None,
            crypto: None,
            ipfs: None,
            storage: None,
        }
    }
    
    /// Create a context with all services
    pub fn with_services(
        db: Arc<dyn Database>,
        crypto: Arc<CryptoService>,
    ) -> Self {
        Self {
            db: Some(db),
            crypto: Some(crypto),
            ipfs: None,
            storage: None,
        }
    }
}

impl Default for HandlerContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for database operations needed by handlers
#[async_trait]
pub trait Database: Send + Sync {
    // Message operations
    async fn get_message(&self, item_hash: &str) -> Result<Option<Message>, String>;
    async fn store_message(&self, message: &Message) -> Result<(), String>;
    async fn update_message_status(&self, item_hash: &str, status: &ProcessingStatus) -> Result<(), String>;
    
    // Aggregate operations
    async fn get_aggregate(&self, address: &str, key: &str) -> Result<Option<serde_json::Value>, String>;
    async fn store_aggregate(&self, address: &str, key: &str, content: &serde_json::Value, time: f64) -> Result<(), String>;
    async fn get_aggregate_elements(&self, address: &str, key: &str) -> Result<Vec<AggregateElement>, String>;
    async fn store_aggregate_element(&self, address: &str, element: &AggregateElement) -> Result<(), String>;
    async fn get_aggregate_time(&self, address: &str, key: &str) -> Result<Option<f64>, String>;
    async fn mark_aggregate_dirty(&self, address: &str, key: &str) -> Result<(), String>;
    async fn mark_aggregate_clean(&self, address: &str, key: &str) -> Result<(), String>;
    
    // Post operations
    async fn get_post(&self, item_hash: &str) -> Result<Option<PostRecord>, String>;
    async fn store_post(&self, post: &PostRecord) -> Result<(), String>;
    async fn update_post_latest_amend(&self, original_hash: &str, amend_hash: &str) -> Result<(), String>;
    
    // Store operations
    async fn get_file_pin(&self, item_hash: &str) -> Result<Option<FilePinRecord>, String>;
    async fn store_file_pin(&self, pin: &FilePinRecord) -> Result<(), String>;
    async fn update_file_pin(&self, item_hash: &str, owner: &str) -> Result<(), String>;
    async fn remove_file_pin(&self, item_hash: &str, owner: &str) -> Result<(), String>;
    
    // Forget operations
    async fn get_forgotten_hashes(&self, hashes: &[String]) -> Result<Vec<String>, String>;
    async fn mark_forgotten(&self, item_hash: &str, forget_hash: &str, reason: Option<&str>) -> Result<(), String>;
    async fn get_dependent_vms(&self, file_hash: &str) -> Result<Vec<String>, String>;
    
    // Balance operations
    async fn get_balance(&self, address: &str, chain: &str) -> Result<Option<rust_decimal::Decimal>, String>;
    async fn get_credit_balance(&self, address: &str) -> Result<Option<rust_decimal::Decimal>, String>;
}

/// Post record for database storage
#[derive(Debug, Clone)]
pub struct PostRecord {
    pub item_hash: String,
    pub address: String,
    pub post_type: String,
    pub ref_: Option<String>,
    pub content: serde_json::Value,
    pub channel: Option<String>,
    pub time: f64,
    pub original_item_hash: Option<String>,
    pub latest_amend: Option<String>,
}

/// File pin record for database storage
#[derive(Debug, Clone)]
pub struct FilePinRecord {
    pub item_hash: String,
    pub owner: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// IPFS service trait
#[async_trait]
pub trait IpfsService: Send + Sync {
    async fn pin(&self, hash: &str) -> Result<(), String>;
    async fn unpin(&self, hash: &str) -> Result<(), String>;
    async fn get_content(&self, hash: &str) -> Result<Vec<u8>, String>;
    async fn get_size(&self, hash: &str) -> Result<u64, String>;
}

/// Storage service trait for cost calculation
#[async_trait]
pub trait StorageService: Send + Sync {
    async fn calculate_cost(&self, size: u64, duration_hours: u64) -> Result<rust_decimal::Decimal, String>;
    async fn check_balance(&self, address: &str, required: rust_decimal::Decimal) -> Result<bool, String>;
}

/// Trait for message handlers
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Get the message type this handler processes
    fn message_type(&self) -> MessageType;
    
    /// Validate the message before processing
    async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError>;
    
    /// Process the message
    async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError>;
}

/// Process a message using the appropriate handler
pub async fn process_message(message: &Message, ctx: &HandlerContext) -> ProcessingStatus {
    // Get handler for message type
    let handler: Box<dyn MessageHandler> = match message.message_type {
        MessageType::Aggregate => Box::new(aggregate::AggregateHandler),
        MessageType::Post => Box::new(post::PostHandler),
        MessageType::Store => Box::new(store::StoreHandler),
        MessageType::Program => Box::new(program::ProgramHandler),
        MessageType::Instance => Box::new(instance::InstanceHandler),
        MessageType::Forget => Box::new(forget::ForgetHandler),
    };
    
    // Validate
    if let Err(e) = handler.validate(message, ctx).await {
        tracing::warn!("Message validation failed: {}", e);
        return e.into();
    }
    
    // Process
    match handler.process(message, ctx).await {
        Ok(()) => ProcessingStatus::processed(),
        Err(e) => {
            tracing::error!("Message processing failed: {}", e);
            e.into()
        }
    }
}
