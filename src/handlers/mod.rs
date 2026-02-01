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
use thiserror::Error;

use crate::types::{Message, MessageType, ProcessingStatus, ErrorCode};

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("Invalid message content: {0}")]
    InvalidContent(String),
    
    #[error("Unauthorized sender")]
    Unauthorized,
    
    #[error("Insufficient balance")]
    InsufficientBalance,
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Content not found: {0}")]
    ContentNotFound(String),
}

impl From<HandlerError> for ProcessingStatus {
    fn from(err: HandlerError) -> Self {
        match err {
            HandlerError::InvalidContent(msg) => {
                ProcessingStatus::rejected(ErrorCode::InvalidContent, msg)
            }
            HandlerError::Unauthorized => {
                ProcessingStatus::rejected(ErrorCode::UnauthorizedSender, "Unauthorized sender")
            }
            HandlerError::InsufficientBalance => {
                ProcessingStatus::rejected(ErrorCode::InsufficientBalance, "Insufficient balance")
            }
            HandlerError::Storage(msg) => {
                ProcessingStatus::rejected(ErrorCode::StorageError, msg)
            }
            HandlerError::Database(msg) => {
                ProcessingStatus::rejected(ErrorCode::InternalError, msg)
            }
            HandlerError::ContentNotFound(msg) => {
                ProcessingStatus::rejected(ErrorCode::ContentNotFound, msg)
            }
        }
    }
}

/// Context for message processing
pub struct HandlerContext {
    // TODO: Add database pool, storage service, etc.
}

impl HandlerContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HandlerContext {
    fn default() -> Self {
        Self::new()
    }
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
        return e.into();
    }
    
    // Process
    match handler.process(message, ctx).await {
        Ok(()) => ProcessingStatus::processed(),
        Err(e) => e.into(),
    }
}
