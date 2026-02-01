//! Ethereum ABI decoding for Aleph contracts
//!
//! Decodes events from the Aleph smart contract.

use ethers::abi::{decode, ParamType, Token};
use ethers::types::Log;
use thiserror::Error;

use crate::types::Message;

#[derive(Debug, Error)]
pub enum AbiError {
    #[error("Invalid event signature")]
    InvalidSignature,
    
    #[error("Missing topic")]
    MissingTopic,
    
    #[error("Failed to decode: {0}")]
    DecodeFailed(String),
    
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),
}

/// Aleph contract event signatures (keccak256 hashes)
pub mod signatures {
    use ethers::types::H256;
    use sha3::{Digest, Keccak256};
    
    lazy_static::lazy_static! {
        /// Message(address indexed sender, string msgType, bytes content)
        pub static ref MESSAGE: H256 = event_signature("Message(address,string,bytes)");
        
        /// SyncMessage(bytes content)
        pub static ref SYNC_MESSAGE: H256 = event_signature("SyncMessage(bytes)");
    }
    
    fn event_signature(sig: &str) -> H256 {
        let hash = Keccak256::digest(sig.as_bytes());
        H256::from_slice(&hash)
    }
}

/// Decoded Aleph message event
#[derive(Debug, Clone)]
pub struct DecodedMessageEvent {
    pub sender: String,
    pub message_type: String,
    pub content: Vec<u8>,
}

/// Decoded sync message event
#[derive(Debug, Clone)]
pub struct DecodedSyncEvent {
    pub content: Vec<u8>,
}

/// Decode a Message event from a log
pub fn decode_message_event(log: &Log) -> Result<DecodedMessageEvent, AbiError> {
    // Check event signature
    if log.topics.is_empty() {
        return Err(AbiError::MissingTopic);
    }
    
    if log.topics[0] != *signatures::MESSAGE {
        return Err(AbiError::InvalidSignature);
    }
    
    // Sender is in topics[1] (indexed)
    if log.topics.len() < 2 {
        return Err(AbiError::MissingTopic);
    }
    
    let sender = format!("0x{}", hex::encode(&log.topics[1].as_bytes()[12..]));
    
    // Decode non-indexed parameters from data
    let params = vec![
        ParamType::String,  // msgType
        ParamType::Bytes,   // content
    ];
    
    let tokens = decode(&params, &log.data)
        .map_err(|e| AbiError::DecodeFailed(e.to_string()))?;
    
    let message_type = match &tokens[0] {
        Token::String(s) => s.clone(),
        _ => return Err(AbiError::DecodeFailed("Expected string for msgType".to_string())),
    };
    
    let content = match &tokens[1] {
        Token::Bytes(b) => b.clone(),
        _ => return Err(AbiError::DecodeFailed("Expected bytes for content".to_string())),
    };
    
    Ok(DecodedMessageEvent {
        sender,
        message_type,
        content,
    })
}

/// Decode a SyncMessage event from a log
pub fn decode_sync_event(log: &Log) -> Result<DecodedSyncEvent, AbiError> {
    // Check event signature
    if log.topics.is_empty() {
        return Err(AbiError::MissingTopic);
    }
    
    if log.topics[0] != *signatures::SYNC_MESSAGE {
        return Err(AbiError::InvalidSignature);
    }
    
    // Decode content from data
    let params = vec![ParamType::Bytes];
    
    let tokens = decode(&params, &log.data)
        .map_err(|e| AbiError::DecodeFailed(e.to_string()))?;
    
    let content = match &tokens[0] {
        Token::Bytes(b) => b.clone(),
        _ => return Err(AbiError::DecodeFailed("Expected bytes for content".to_string())),
    };
    
    Ok(DecodedSyncEvent { content })
}

/// Parse message content into a Message struct
pub fn parse_message_content(
    content: &[u8],
    _chain: crate::types::Chain,
    _tx_hash: &str,
    _block_number: u64,
) -> Result<Message, AbiError> {
    // Content should be JSON
    let content_str = std::str::from_utf8(content)
        .map_err(|e| AbiError::InvalidMessage(format!("Invalid UTF-8: {}", e)))?;
    
    let msg: Message = serde_json::from_str(content_str)
        .map_err(|e| AbiError::InvalidMessage(format!("Invalid JSON: {}", e)))?;
    
    Ok(msg)
}

/// Parse sync message content (may contain multiple messages)
pub fn parse_sync_content(content: &[u8]) -> Result<Vec<String>, AbiError> {
    // Sync messages contain IPFS hashes to fetch
    let content_str = std::str::from_utf8(content)
        .map_err(|e| AbiError::InvalidMessage(format!("Invalid UTF-8: {}", e)))?;
    
    // Parse as JSON array of hashes
    let hashes: Vec<String> = serde_json::from_str(content_str)
        .map_err(|e| AbiError::InvalidMessage(format!("Invalid JSON: {}", e)))?;
    
    Ok(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_event_signatures() {
        // These should be valid H256 hashes
        assert_eq!(signatures::MESSAGE.as_bytes().len(), 32);
        assert_eq!(signatures::SYNC_MESSAGE.as_bytes().len(), 32);
    }
}
