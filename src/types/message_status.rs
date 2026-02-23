//! Message status types
//!
//! These types match the Python reference implementation (pyaleph) for API compatibility.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Overall message status
/// 
/// Matches Python: aleph/types/message_status.py:11-17
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageStatus {
    /// Message is pending processing
    Pending,
    /// Message has been processed successfully
    Processed,
    /// Message was rejected
    Rejected,
    /// Message has been forgotten
    Forgotten,
    /// Message is being removed (in progress)
    Removing,
    /// Message has been fully removed
    Removed,
}

impl fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl MessageStatus {
    /// Get the status as a string slice
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageStatus::Pending => "pending",
            MessageStatus::Processed => "processed",
            MessageStatus::Rejected => "rejected",
            MessageStatus::Forgotten => "forgotten",
            MessageStatus::Removing => "removing",
            MessageStatus::Removed => "removed",
        }
    }
}

/// Detailed error codes for rejected messages
/// 
/// MUST match Python: aleph/types/message_status.py:31-50
/// These codes are part of the API contract with clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum ErrorCode {
    // Core error codes (Python: -1 to 6)
    InternalError = -1,
    InvalidFormat = 0,
    InvalidSignature = 1,
    PermissionDenied = 2,
    ContentUnavailable = 3,
    FileUnavailable = 4,
    BalanceInsufficient = 5,
    CreditInsufficient = 6,
    
    // Post-specific errors (100-102)
    PostAmendNoTarget = 100,
    PostAmendTargetNotFound = 101,
    PostAmendAmend = 102,
    
    // Store-specific errors (200-202)
    StoreRefNotFound = 200,
    StoreUpdateUpdate = 201,
    InvalidPaymentMethod = 202,
    
    // VM-specific errors (300-304)
    VmRefNotFound = 300,
    VmVolumeNotFound = 301,
    VmAmendNotAllowed = 302,
    VmUpdateUpdate = 303,
    VmVolumeTooSmall = 304,
    
    // Forget-specific errors (500-504)
    ForgetNoTarget = 500,
    ForgetTargetNotFound = 501,
    ForgetForget = 502,
    ForgetNotAllowed = 503,
    ForgottenDuplicate = 504,
}

impl ErrorCode {
    pub fn as_i32(&self) -> i32 {
        *self as i32
    }
    
    pub fn from_i32(code: i32) -> Option<Self> {
        match code {
            -1 => Some(ErrorCode::InternalError),
            0 => Some(ErrorCode::InvalidFormat),
            1 => Some(ErrorCode::InvalidSignature),
            2 => Some(ErrorCode::PermissionDenied),
            3 => Some(ErrorCode::ContentUnavailable),
            4 => Some(ErrorCode::FileUnavailable),
            5 => Some(ErrorCode::BalanceInsufficient),
            6 => Some(ErrorCode::CreditInsufficient),
            100 => Some(ErrorCode::PostAmendNoTarget),
            101 => Some(ErrorCode::PostAmendTargetNotFound),
            102 => Some(ErrorCode::PostAmendAmend),
            200 => Some(ErrorCode::StoreRefNotFound),
            201 => Some(ErrorCode::StoreUpdateUpdate),
            202 => Some(ErrorCode::InvalidPaymentMethod),
            300 => Some(ErrorCode::VmRefNotFound),
            301 => Some(ErrorCode::VmVolumeNotFound),
            302 => Some(ErrorCode::VmAmendNotAllowed),
            303 => Some(ErrorCode::VmUpdateUpdate),
            304 => Some(ErrorCode::VmVolumeTooSmall),
            500 => Some(ErrorCode::ForgetNoTarget),
            501 => Some(ErrorCode::ForgetTargetNotFound),
            502 => Some(ErrorCode::ForgetForget),
            503 => Some(ErrorCode::ForgetNotAllowed),
            504 => Some(ErrorCode::ForgottenDuplicate),
            _ => None,
        }
    }
    
    pub fn description(&self) -> &'static str {
        match self {
            ErrorCode::InternalError => "Internal server error",
            ErrorCode::InvalidFormat => "Invalid message format",
            ErrorCode::InvalidSignature => "Invalid message signature",
            ErrorCode::PermissionDenied => "Permission denied",
            ErrorCode::ContentUnavailable => "Content unavailable",
            ErrorCode::FileUnavailable => "File unavailable",
            ErrorCode::BalanceInsufficient => "Insufficient balance",
            ErrorCode::CreditInsufficient => "Insufficient credit",
            ErrorCode::PostAmendNoTarget => "Amend post requires a target reference",
            ErrorCode::PostAmendTargetNotFound => "Target post to amend not found",
            ErrorCode::PostAmendAmend => "Cannot amend an amend post",
            ErrorCode::StoreRefNotFound => "Store reference not found",
            ErrorCode::StoreUpdateUpdate => "Cannot update an update message",
            ErrorCode::InvalidPaymentMethod => "Messages with non-credit payment types are no longer allowed",
            ErrorCode::VmRefNotFound => "VM reference not found",
            ErrorCode::VmVolumeNotFound => "VM volume not found",
            ErrorCode::VmAmendNotAllowed => "VM does not allow amendments",
            ErrorCode::VmUpdateUpdate => "Cannot update an update message",
            ErrorCode::VmVolumeTooSmall => "VM volume too small",
            ErrorCode::ForgetNoTarget => "Forget message requires at least one target",
            ErrorCode::ForgetTargetNotFound => "Target to forget not found",
            ErrorCode::ForgetForget => "Cannot forget a forget message",
            ErrorCode::ForgetNotAllowed => "Not allowed to forget this content",
            ErrorCode::ForgottenDuplicate => "Content already forgotten",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.description(), self.as_i32())
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
    
    pub fn removing() -> Self {
        Self {
            status: MessageStatus::Removing,
            error_code: None,
            error_message: None,
        }
    }
    
    pub fn removed() -> Self {
        Self {
            status: MessageStatus::Removed,
            error_code: None,
            error_message: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_code_values() {
        // Verify critical error codes match Python
        assert_eq!(ErrorCode::InternalError.as_i32(), -1);
        assert_eq!(ErrorCode::InvalidFormat.as_i32(), 0);
        assert_eq!(ErrorCode::InvalidSignature.as_i32(), 1);
        assert_eq!(ErrorCode::PermissionDenied.as_i32(), 2);
        assert_eq!(ErrorCode::BalanceInsufficient.as_i32(), 5);
        assert_eq!(ErrorCode::PostAmendNoTarget.as_i32(), 100);
        assert_eq!(ErrorCode::ForgetNoTarget.as_i32(), 500);
    }
    
    #[test]
    fn test_message_status_serialization() {
        let status = MessageStatus::Pending;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"pending\"");
        
        let status = MessageStatus::Removing;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"removing\"");
    }
}
