//! Message status types

use serde::{Deserialize, Serialize};

/// Overall message status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageStatus {
    /// Message is pending processing
    Pending,
    /// Message has been processed successfully
    Processed,
    /// Message was rejected
    Rejected,
    /// Message processing is forgotten/cancelled
    Forgotten,
}

/// Detailed error codes for rejected messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum ErrorCode {
    // Validation errors (1xx)
    InvalidSignature = 100,
    InvalidContent = 101,
    InvalidFormat = 102,
    InvalidChain = 103,
    InvalidTimestamp = 104,
    InvalidItemHash = 105,
    
    // Permission errors (2xx)
    UnauthorizedSender = 200,
    InsufficientBalance = 201,
    QuotaExceeded = 202,
    
    // Content errors (3xx)
    ContentNotFound = 300,
    ContentTooLarge = 301,
    ContentUnavailable = 302,
    
    // Internal errors (5xx)
    InternalError = 500,
    StorageError = 501,
    NetworkError = 502,
}

impl ErrorCode {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }
    
    pub fn from_i32(code: i32) -> Option<Self> {
        match code {
            100 => Some(ErrorCode::InvalidSignature),
            101 => Some(ErrorCode::InvalidContent),
            102 => Some(ErrorCode::InvalidFormat),
            103 => Some(ErrorCode::InvalidChain),
            104 => Some(ErrorCode::InvalidTimestamp),
            105 => Some(ErrorCode::InvalidItemHash),
            200 => Some(ErrorCode::UnauthorizedSender),
            201 => Some(ErrorCode::InsufficientBalance),
            202 => Some(ErrorCode::QuotaExceeded),
            300 => Some(ErrorCode::ContentNotFound),
            301 => Some(ErrorCode::ContentTooLarge),
            302 => Some(ErrorCode::ContentUnavailable),
            500 => Some(ErrorCode::InternalError),
            501 => Some(ErrorCode::StorageError),
            502 => Some(ErrorCode::NetworkError),
            _ => None,
        }
    }
    
    pub fn description(&self) -> &'static str {
        match self {
            ErrorCode::InvalidSignature => "Invalid message signature",
            ErrorCode::InvalidContent => "Invalid message content",
            ErrorCode::InvalidFormat => "Invalid message format",
            ErrorCode::InvalidChain => "Unsupported blockchain",
            ErrorCode::InvalidTimestamp => "Invalid timestamp",
            ErrorCode::InvalidItemHash => "Invalid item hash",
            ErrorCode::UnauthorizedSender => "Sender not authorized",
            ErrorCode::InsufficientBalance => "Insufficient balance",
            ErrorCode::QuotaExceeded => "Storage quota exceeded",
            ErrorCode::ContentNotFound => "Referenced content not found",
            ErrorCode::ContentTooLarge => "Content too large",
            ErrorCode::ContentUnavailable => "Content temporarily unavailable",
            ErrorCode::InternalError => "Internal server error",
            ErrorCode::StorageError => "Storage system error",
            ErrorCode::NetworkError => "Network error",
        }
    }
}

/// Processing status of a message with details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingStatus {
    pub status: MessageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<ErrorCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl ProcessingStatus {
    pub fn pending() -> Self {
        Self {
            status: MessageStatus::Pending,
            error_code: None,
            error_message: None,
        }
    }
    
    pub fn processed() -> Self {
        Self {
            status: MessageStatus::Processed,
            error_code: None,
            error_message: None,
        }
    }
    
    pub fn rejected(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status: MessageStatus::Rejected,
            error_code: Some(code),
            error_message: Some(message.into()),
        }
    }
    
    pub fn forgotten() -> Self {
        Self {
            status: MessageStatus::Forgotten,
            error_code: None,
            error_message: None,
        }
    }
}
