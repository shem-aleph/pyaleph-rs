//! Cryptographic services
//!
//! Handles signature verification for different chains.

use secp256k1::{Message as Secp256k1Message, PublicKey, Secp256k1, ecdsa::RecoverableSignature};
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::types::Chain;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Invalid signature format")]
    InvalidSignatureFormat,
    
    #[error("Invalid public key")]
    InvalidPublicKey,
    
    #[error("Signature verification failed")]
    VerificationFailed,
    
    #[error("Unsupported chain: {0}")]
    UnsupportedChain(String),
    
    #[error("Hex decode error: {0}")]
    HexError(#[from] hex::FromHexError),
    
    #[error("Secp256k1 error: {0}")]
    Secp256k1Error(#[from] secp256k1::Error),
}

/// Cryptographic service for signature operations
pub struct CryptoService {
    secp: Secp256k1<secp256k1::All>,
}

impl Default for CryptoService {
    fn default() -> Self {
        Self::new()
    }
}

impl CryptoService {
    pub fn new() -> Self {
        Self {
            secp: Secp256k1::new(),
        }
    }
    
    /// Verify a message signature
    pub fn verify_signature(
        &self,
        chain: &Chain,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        match chain {
            Chain::ETH | Chain::AVAX | Chain::BASE | Chain::BSC => {
                self.verify_ethereum_signature(message, signature, expected_address)
            }
            Chain::SOL => {
                self.verify_solana_signature(message, signature, expected_address)
            }
            Chain::TEZOS => {
                self.verify_tezos_signature(message, signature, expected_address)
            }
            _ => Err(CryptoError::UnsupportedChain(chain.to_string())),
        }
    }
    
    /// Verify an Ethereum-style signature (EIP-191 personal sign)
    fn verify_ethereum_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Decode signature (remove 0x prefix if present)
        let sig_bytes = hex::decode(signature.trim_start_matches("0x"))?;
        
        if sig_bytes.len() != 65 {
            return Err(CryptoError::InvalidSignatureFormat);
        }
        
        // Extract r, s, v components
        let r = &sig_bytes[0..32];
        let s = &sig_bytes[32..64];
        let v = sig_bytes[64];
        
        // Normalize v (Ethereum uses 27/28, secp256k1 uses 0/1)
        let recovery_id = if v >= 27 { v - 27 } else { v };
        
        // Create the Ethereum signed message hash
        let prefixed_message = format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
        let message_hash = Keccak256::digest(prefixed_message.as_bytes());
        
        // Create secp256k1 message
        let msg = Secp256k1Message::from_digest_slice(&message_hash)
            .map_err(|_| CryptoError::VerificationFailed)?;
        
        // Create recoverable signature
        let mut sig_bytes_compact = [0u8; 64];
        sig_bytes_compact[..32].copy_from_slice(r);
        sig_bytes_compact[32..].copy_from_slice(s);
        
        let rec_id = secp256k1::ecdsa::RecoveryId::from_i32(recovery_id as i32)
            .map_err(|_| CryptoError::InvalidSignatureFormat)?;
        
        let recoverable_sig = RecoverableSignature::from_compact(&sig_bytes_compact, rec_id)?;
        
        // Recover public key
        let public_key = self.secp.recover_ecdsa(&msg, &recoverable_sig)?;
        
        // Derive address from public key
        let recovered_address = self.pubkey_to_eth_address(&public_key);
        
        // Compare addresses (case-insensitive)
        Ok(recovered_address.to_lowercase() == expected_address.to_lowercase())
    }
    
    /// Convert a secp256k1 public key to an Ethereum address
    fn pubkey_to_eth_address(&self, pubkey: &PublicKey) -> String {
        let pubkey_bytes = pubkey.serialize_uncompressed();
        // Skip the first byte (0x04 prefix for uncompressed)
        let hash = Keccak256::digest(&pubkey_bytes[1..]);
        // Take last 20 bytes
        let address_bytes = &hash[12..];
        format!("0x{}", hex::encode(address_bytes))
    }
    
    /// Verify a Solana signature (Ed25519)
    fn verify_solana_signature(
        &self,
        _message: &str,
        _signature: &str,
        _expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // TODO: Implement Ed25519 verification for Solana
        // Requires ed25519-dalek crate
        Err(CryptoError::UnsupportedChain("SOL - not yet implemented".to_string()))
    }
    
    /// Verify a Tezos signature
    fn verify_tezos_signature(
        &self,
        _message: &str,
        _signature: &str,
        _expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // TODO: Implement Tezos signature verification
        Err(CryptoError::UnsupportedChain("TEZOS - not yet implemented".to_string()))
    }
    
    /// Hash content using SHA256
    pub fn sha256_hash(&self, content: &[u8]) -> String {
        use sha2::{Sha256, Digest as Sha2Digest};
        let hash = Sha256::digest(content);
        hex::encode(hash)
    }
    
    /// Hash content using Keccak256
    pub fn keccak256_hash(&self, content: &[u8]) -> String {
        let hash = Keccak256::digest(content);
        hex::encode(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sha256_hash() {
        let crypto = CryptoService::new();
        let hash = crypto.sha256_hash(b"hello");
        assert_eq!(hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }
    
    #[test]
    fn test_keccak256_hash() {
        let crypto = CryptoService::new();
        let hash = crypto.keccak256_hash(b"hello");
        assert_eq!(hash, "1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8");
    }
}
