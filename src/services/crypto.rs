//! Cryptographic services
//!
//! Handles signature verification for different chains.
//! This is CRITICAL for security - all messages must have valid signatures.

use ed25519_dalek::{Signature as Ed25519Signature, Verifier, VerifyingKey};
use secp256k1::{Message as Secp256k1Message, PublicKey, Secp256k1, ecdsa::RecoverableSignature};
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::types::Chain;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Invalid signature format: {0}")]
    InvalidSignatureFormat(String),
    
    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),
    
    #[error("Signature verification failed")]
    VerificationFailed,
    
    #[error("Unsupported chain: {0}")]
    UnsupportedChain(String),
    
    #[error("Hex decode error: {0}")]
    HexError(#[from] hex::FromHexError),
    
    #[error("Secp256k1 error: {0}")]
    Secp256k1Error(#[from] secp256k1::Error),
    
    #[error("Ed25519 error: {0}")]
    Ed25519Error(String),
    
    #[error("Base58 decode error: {0}")]
    Base58Error(String),
}

/// Cryptographic service for signature operations
/// 
/// This service handles signature verification for all supported blockchains.
/// It maintains a secp256k1 context for efficient verification of
/// Ethereum-style and other secp256k1-based signatures.
#[derive(Debug)]
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
    /// 
    /// This is the main entry point for signature verification.
    /// Routes to the appropriate chain-specific verifier.
    pub fn verify_signature(
        &self,
        chain: &Chain,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        match chain {
            // EVM-compatible chains use Ethereum signature scheme
            Chain::ETH | Chain::AVAX | Chain::BASE | Chain::BSC => {
                self.verify_ethereum_signature(message, signature, expected_address)
            }
            // Solana uses Ed25519
            Chain::SOL => {
                self.verify_solana_signature(message, signature, expected_address)
            }
            // Tezos uses Ed25519 or secp256k1 depending on address prefix
            Chain::TEZOS => {
                self.verify_tezos_signature(message, signature, expected_address)
            }
            // NULS chains - similar to Ethereum but different message format
            Chain::NULS | Chain::NULS2 => {
                self.verify_nuls_signature(message, signature, expected_address)
            }
            // Cosmos SDK chains use secp256k1
            Chain::CSDK => {
                self.verify_cosmos_signature(message, signature, expected_address)
            }
            _ => Err(CryptoError::UnsupportedChain(format!(
                "{} signature verification not yet implemented",
                chain
            ))),
        }
    }
    
    /// Verify an Ethereum-style signature (EIP-191 personal sign)
    /// 
    /// This handles ETH, AVAX, BASE, BSC and other EVM chains.
    fn verify_ethereum_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Decode signature (remove 0x prefix if present)
        let sig_hex = signature.trim_start_matches("0x");
        let sig_bytes = hex::decode(sig_hex)?;
        
        if sig_bytes.len() != 65 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 65 bytes, got {}",
                sig_bytes.len()
            )));
        }
        
        // Extract r, s, v components
        let r = &sig_bytes[0..32];
        let s = &sig_bytes[32..64];
        let v = sig_bytes[64];
        
        // Normalize v (Ethereum uses 27/28, secp256k1 uses 0/1)
        let recovery_id = if v >= 27 { v - 27 } else { v };
        
        if recovery_id > 1 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Invalid recovery id: {}",
                recovery_id
            )));
        }
        
        // Create the Ethereum signed message hash (EIP-191)
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
            .map_err(|_| CryptoError::InvalidSignatureFormat("Invalid recovery id".to_string()))?;
        
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
    /// 
    /// Solana uses Ed25519 signatures with base58-encoded addresses and signatures.
    fn verify_solana_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Decode signature (base58 encoded)
        let sig_bytes = bs58::decode(signature)
            .into_vec()
            .map_err(|e| CryptoError::Base58Error(e.to_string()))?;
        
        if sig_bytes.len() != 64 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 64 bytes for Ed25519 signature, got {}",
                sig_bytes.len()
            )));
        }
        
        // Decode public key (base58 encoded Solana address)
        let pubkey_bytes = bs58::decode(expected_address)
            .into_vec()
            .map_err(|e| CryptoError::Base58Error(e.to_string()))?;
        
        if pubkey_bytes.len() != 32 {
            return Err(CryptoError::InvalidPublicKey(format!(
                "Expected 32 bytes for Ed25519 public key, got {}",
                pubkey_bytes.len()
            )));
        }
        
        // Convert to ed25519-dalek types
        let sig_array: [u8; 64] = sig_bytes.try_into()
            .map_err(|_| CryptoError::InvalidSignatureFormat("Invalid signature length".to_string()))?;
        let signature = Ed25519Signature::from_bytes(&sig_array);
        
        let pubkey_array: [u8; 32] = pubkey_bytes.try_into()
            .map_err(|_| CryptoError::InvalidPublicKey("Invalid public key length".to_string()))?;
        let verifying_key = VerifyingKey::from_bytes(&pubkey_array)
            .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;
        
        // Verify signature
        match verifying_key.verify(message.as_bytes(), &signature) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// Verify a Tezos signature
    /// 
    /// Tezos supports multiple signature schemes based on address prefix:
    /// - tz1: Ed25519
    /// - tz2: secp256k1
    /// - tz3: P256
    fn verify_tezos_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Determine signature type from address prefix
        if expected_address.starts_with("tz1") {
            // Ed25519 signature
            self.verify_tezos_ed25519(message, signature, expected_address)
        } else if expected_address.starts_with("tz2") {
            // secp256k1 signature
            self.verify_tezos_secp256k1(message, signature, expected_address)
        } else if expected_address.starts_with("tz3") {
            // P256 - not implemented yet
            Err(CryptoError::UnsupportedChain(
                "Tezos tz3 (P256) signatures not yet supported".to_string()
            ))
        } else {
            Err(CryptoError::InvalidPublicKey(format!(
                "Invalid Tezos address prefix: {}",
                expected_address
            )))
        }
    }
    
    /// Verify Tezos Ed25519 signature (tz1 addresses)
    fn verify_tezos_ed25519(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Tezos signatures are typically base58check encoded with a prefix
        // The signature prefix for ed25519 is "edsig"
        let sig_bytes = if signature.starts_with("edsig") {
            // Decode base58check and strip prefix
            let decoded = bs58::decode(signature)
                .into_vec()
                .map_err(|e| CryptoError::Base58Error(e.to_string()))?;
            // Skip the 5-byte prefix
            if decoded.len() < 69 { // 5 prefix + 64 sig
                return Err(CryptoError::InvalidSignatureFormat(
                    "Tezos signature too short".to_string()
                ));
            }
            decoded[5..69].to_vec()
        } else {
            // Try hex-encoded
            hex::decode(signature.trim_start_matches("0x"))?
        };
        
        if sig_bytes.len() != 64 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 64 bytes, got {}",
                sig_bytes.len()
            )));
        }
        
        // For Tezos, we need the public key, not just the address
        // The address is a hash of the public key, so we can't recover the key from it
        // In practice, the public key should be provided alongside the message
        // For now, return an error indicating this limitation
        Err(CryptoError::UnsupportedChain(
            "Tezos Ed25519 verification requires public key (address only provided)".to_string()
        ))
    }
    
    /// Verify Tezos secp256k1 signature (tz2 addresses)
    fn verify_tezos_secp256k1(
        &self,
        _message: &str,
        _signature: &str,
        _expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Similar issue - need public key, not just address hash
        Err(CryptoError::UnsupportedChain(
            "Tezos secp256k1 verification requires public key (address only provided)".to_string()
        ))
    }
    
    /// Verify NULS/NULS2 signature
    /// 
    /// NULS uses a similar scheme to Ethereum but with different message formatting.
    fn verify_nuls_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // NULS uses secp256k1 like Ethereum
        let sig_hex = signature.trim_start_matches("0x");
        let sig_bytes = hex::decode(sig_hex)?;
        
        if sig_bytes.len() != 65 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 65 bytes, got {}",
                sig_bytes.len()
            )));
        }
        
        // NULS message format differs from Ethereum
        // It uses a simple SHA256 hash of the message
        use sha2::{Sha256, Digest as Sha2Digest};
        let message_hash = Sha256::digest(message.as_bytes());
        
        let msg = Secp256k1Message::from_digest_slice(&message_hash)
            .map_err(|_| CryptoError::VerificationFailed)?;
        
        let r = &sig_bytes[0..32];
        let s = &sig_bytes[32..64];
        let v = sig_bytes[64];
        
        let recovery_id = if v >= 27 { v - 27 } else { v };
        
        let mut sig_bytes_compact = [0u8; 64];
        sig_bytes_compact[..32].copy_from_slice(r);
        sig_bytes_compact[32..].copy_from_slice(s);
        
        let rec_id = secp256k1::ecdsa::RecoveryId::from_i32(recovery_id as i32)
            .map_err(|_| CryptoError::InvalidSignatureFormat("Invalid recovery id".to_string()))?;
        
        let recoverable_sig = RecoverableSignature::from_compact(&sig_bytes_compact, rec_id)?;
        let public_key = self.secp.recover_ecdsa(&msg, &recoverable_sig)?;
        
        // NULS address derivation is different from Ethereum
        // For now, use a simplified check
        let recovered_address = self.pubkey_to_nuls_address(&public_key);
        
        Ok(recovered_address == expected_address)
    }
    
    /// Convert a secp256k1 public key to a NULS address
    fn pubkey_to_nuls_address(&self, pubkey: &PublicKey) -> String {
        use sha2::{Sha256, Digest as Sha2Digest};
        
        let pubkey_bytes = pubkey.serialize();
        // NULS uses SHA256(RIPEMD160(pubkey))
        let sha_hash = Sha256::digest(&pubkey_bytes);
        
        // Simplified - real NULS uses RIPEMD160 + checksum + base58
        // This is a placeholder that won't match real addresses
        format!("NULSd{}", hex::encode(&sha_hash[..20]))
    }
    
    /// Verify Cosmos SDK signature
    /// 
    /// Cosmos chains use secp256k1 with a specific message format.
    fn verify_cosmos_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Cosmos SDK signs the SHA256 hash of the message
        use sha2::{Sha256, Digest as Sha2Digest};
        
        let sig_bytes = if signature.starts_with("0x") {
            hex::decode(signature.trim_start_matches("0x"))?
        } else {
            // Try base64
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, signature)
                .map_err(|e| CryptoError::InvalidSignatureFormat(e.to_string()))?
        };
        
        if sig_bytes.len() != 64 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 64 bytes for Cosmos signature, got {}",
                sig_bytes.len()
            )));
        }
        
        let message_hash = Sha256::digest(message.as_bytes());
        
        let msg = Secp256k1Message::from_digest_slice(&message_hash)
            .map_err(|_| CryptoError::VerificationFailed)?;
        
        // Cosmos signatures don't include recovery id, so we need to try both
        for recovery_id in 0..2 {
            let rec_id = match secp256k1::ecdsa::RecoveryId::from_i32(recovery_id) {
                Ok(id) => id,
                Err(_) => continue,
            };
            
            let recoverable_sig = match RecoverableSignature::from_compact(&sig_bytes, rec_id) {
                Ok(sig) => sig,
                Err(_) => continue,
            };
            
            if let Ok(public_key) = self.secp.recover_ecdsa(&msg, &recoverable_sig) {
                let recovered_address = self.pubkey_to_cosmos_address(&public_key);
                if recovered_address == expected_address {
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }
    
    /// Convert a secp256k1 public key to a Cosmos address
    fn pubkey_to_cosmos_address(&self, pubkey: &PublicKey) -> String {
        use sha2::{Sha256, Digest as Sha2Digest};
        
        let pubkey_bytes = pubkey.serialize();
        let sha_hash = Sha256::digest(&pubkey_bytes);
        
        // Cosmos uses bech32 encoding with RIPEMD160
        // Simplified version - real implementation needs bech32 encoding
        format!("cosmos1{}", hex::encode(&sha_hash[..20]))
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
    
    /// Compute the item hash for a message
    /// 
    /// This is the SHA256 hash of the serialized message content.
    pub fn compute_item_hash(&self, content: &str) -> String {
        self.sha256_hash(content.as_bytes())
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
    
    #[test]
    fn test_ethereum_address_derivation() {
        // This is a known test case
        let crypto = CryptoService::new();
        
        // Create a public key from known bytes (compressed)
        let pubkey_hex = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
        let pubkey_bytes = hex::decode(pubkey_hex).unwrap();
        let pubkey = PublicKey::from_slice(&pubkey_bytes).unwrap();
        
        let address = crypto.pubkey_to_eth_address(&pubkey);
        // Known address for this public key
        assert!(address.starts_with("0x"));
        assert_eq!(address.len(), 42);
    }
    
    #[test]
    fn test_chain_signature_routing() {
        let crypto = CryptoService::new();
        
        // Test that EVM chains route to Ethereum verification
        let eth_result = crypto.verify_signature(
            &Chain::ETH,
            "test",
            "0x00",
            "0x0000000000000000000000000000000000000000",
        );
        // Should fail with invalid signature, not unsupported chain
        assert!(matches!(eth_result, Err(CryptoError::InvalidSignatureFormat(_))));
    }
}
