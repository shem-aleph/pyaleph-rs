//! Cryptographic services
//!
//! Handles signature verification for different chains.
//! This is CRITICAL for security - all messages must have valid signatures.

use ed25519_dalek::{Signature as Ed25519Signature, Verifier, VerifyingKey};
use secp256k1::{Message as Secp256k1Message, PublicKey, Secp256k1, ecdsa::RecoverableSignature};
use sha3::{Digest, Keccak256};
use thiserror::Error;

use crate::types::{Chain, MessageType};

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
    
    #[error("JSON parse error: {0}")]
    JsonError(String),
}

/// Cryptographic service for signature operations
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
    
    /// Build the verification buffer that was signed
    /// 
    /// Format: "{chain}\n{sender}\n{type}\n{item_hash}"
    /// This matches Python pyaleph's get_verification_buffer()
    pub fn get_verification_buffer(
        chain: &Chain,
        sender: &str,
        msg_type: &MessageType,
        item_hash: &str,
    ) -> String {
        format!("{}\n{}\n{}\n{}", chain, sender, msg_type, item_hash)
    }
    
    /// Verify a message signature using the full message context
    /// 
    /// This is the main entry point for signature verification.
    pub fn verify_message_signature(
        &self,
        chain: &Chain,
        sender: &str,
        msg_type: &MessageType,
        item_hash: &str,
        signature: &str,
    ) -> Result<bool, CryptoError> {
        let verification_buffer = Self::get_verification_buffer(chain, sender, msg_type, item_hash);
        self.verify_signature(chain, &verification_buffer, signature, sender)
    }
    
    /// Verify a signature against a message
    /// 
    /// Routes to the appropriate chain-specific verifier.
    pub fn verify_signature(
        &self,
        chain: &Chain,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        match chain {
            // EVM-compatible chains use Ethereum signature scheme (EIP-191)
            Chain::ETH | Chain::AVAX | Chain::BASE | Chain::BSC |
            Chain::ARBITRUM | Chain::BLAST | Chain::BOB | Chain::CYBER |
            Chain::FRAXTAL | Chain::HYPE | Chain::INK | Chain::LENS |
            Chain::METIS | Chain::MODE | Chain::NEO | Chain::LINEA |
            Chain::LISK | Chain::OPTIMISM | Chain::POL | Chain::SONIC |
            Chain::UNICHAIN | Chain::WORLDCHAIN | Chain::ZORA |
            Chain::ETHERLINK => {
                self.verify_evm_signature(message, signature, expected_address)
            }
            // Solana uses Ed25519 with JSON-wrapped signature
            Chain::SOL | Chain::ECLIPSE => {
                self.verify_solana_signature(message, signature, expected_address)
            }
            // Tezos uses Ed25519/secp256k1/P256 depending on address prefix
            Chain::TEZOS => {
                self.verify_tezos_signature(message, signature, expected_address)
            }
            // NULS chains use secp256k1 with embedded public key
            Chain::NULS | Chain::NULS2 => {
                self.verify_nuls_signature(message, signature, expected_address)
            }
            // Cosmos SDK chains use secp256k1
            Chain::CSDK => {
                self.verify_cosmos_signature(message, signature, expected_address)
            }
            // Substrate (Polkadot) uses Sr25519 or Ed25519
            Chain::DOT => {
                self.verify_substrate_signature(message, signature, expected_address)
            }
            _ => Err(CryptoError::UnsupportedChain(format!(
                "{} signature verification not yet implemented",
                chain
            ))),
        }
    }
    
    /// Verify an EVM signature (EIP-191 personal sign)
    /// 
    /// This handles ETH and all EVM-compatible L2s.
    fn verify_evm_signature(
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
    /// Solana signatures are JSON: {"signature": "base58...", "publicKey": "base58..."}
    fn verify_solana_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Parse JSON signature format
        let sig_json: serde_json::Value = serde_json::from_str(signature)
            .map_err(|e| CryptoError::JsonError(format!("Invalid Solana signature JSON: {}", e)))?;
        
        let sig_b58 = sig_json.get("signature")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CryptoError::InvalidSignatureFormat("Missing 'signature' field".to_string()))?;
        
        let pubkey_b58 = sig_json.get("publicKey")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CryptoError::InvalidSignatureFormat("Missing 'publicKey' field".to_string()))?;
        
        // Check version if present (must be 1)
        if let Some(version) = sig_json.get("version") {
            if version.as_i64() != Some(1) {
                return Err(CryptoError::InvalidSignatureFormat(
                    format!("Unsupported signature version: {:?}", version)
                ));
            }
        }
        
        // Verify publicKey matches sender
        if pubkey_b58 != expected_address {
            return Ok(false);
        }
        
        // Decode signature (base58)
        let sig_bytes = bs58::decode(sig_b58)
            .into_vec()
            .map_err(|e| CryptoError::Base58Error(e.to_string()))?;
        
        if sig_bytes.len() != 64 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Expected 64 bytes for Ed25519 signature, got {}",
                sig_bytes.len()
            )));
        }
        
        // Decode public key (base58)
        let pubkey_bytes = bs58::decode(pubkey_b58)
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
        let signature_obj = Ed25519Signature::from_bytes(&sig_array);
        
        let pubkey_array: [u8; 32] = pubkey_bytes.try_into()
            .map_err(|_| CryptoError::InvalidPublicKey("Invalid public key length".to_string()))?;
        let verifying_key = VerifyingKey::from_bytes(&pubkey_array)
            .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;
        
        // Verify signature over the raw message bytes
        match verifying_key.verify(message.as_bytes(), &signature_obj) {
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
        // For now, Tezos verification is complex because:
        // 1. Multiple signature schemes
        // 2. Address is hash of pubkey, can't recover pubkey from signature alone
        // 3. Would need the public key to be embedded or provided separately
        //
        // Most Tezos messages on Aleph are rare - log and skip for now
        Err(CryptoError::UnsupportedChain(
            "Tezos signature verification requires public key (not just address)".to_string()
        ))
    }
    
    /// Verify NULS/NULS2 signature
    /// 
    /// NULS signatures have the public key embedded in the signature data.
    fn verify_nuls_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        use sha2::{Sha256, Digest as Sha2Digest};
        use ripemd::Ripemd160;
        use ripemd::Digest as RipemdDigest;
        
        // NULS signature format: hex-encoded bytes containing:
        // - 1 byte: public key type (usually 0x02 or 0x03 for compressed)
        // - 33 bytes: compressed public key
        // - variable: signature data
        let sig_hex = signature.trim_start_matches("0x");
        let sig_bytes = hex::decode(sig_hex)?;
        
        if sig_bytes.len() < 34 + 64 {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "NULS signature too short: {} bytes",
                sig_bytes.len()
            )));
        }
        
        // Extract public key (first 33 bytes after type byte)
        // Actually NULS format varies - let's try different parsing
        
        // Try format: pubkey_len(1) + pubkey(33) + sig_type(1) + sig(variable)
        let pubkey_len = sig_bytes[0] as usize;
        if pubkey_len != 33 || sig_bytes.len() < pubkey_len + 1 {
            return Err(CryptoError::InvalidSignatureFormat(
                "Invalid NULS signature structure".to_string()
            ));
        }
        
        let pubkey_bytes = &sig_bytes[1..1 + pubkey_len];
        let pubkey = PublicKey::from_slice(pubkey_bytes)
            .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;
        
        // Derive NULS address from public key
        let sha_hash = Sha256::digest(pubkey_bytes);
        let ripemd_hash = Ripemd160::digest(&sha_hash);
        
        // Extract chain ID from address
        let (chain_id, addr_type) = self.parse_nuls_address(expected_address)?;
        
        // Build address with proper prefix
        let derived_address = self.build_nuls_address(chain_id, addr_type, &ripemd_hash)?;
        
        if derived_address != expected_address {
            return Ok(false);
        }
        
        // Now verify the signature over the message
        let message_hash = Sha256::digest(Sha256::digest(message.as_bytes()));
        
        let msg = Secp256k1Message::from_digest_slice(&message_hash)
            .map_err(|_| CryptoError::VerificationFailed)?;
        
        // Extract signature (after pubkey)
        let sig_start = 1 + pubkey_len;
        if sig_bytes.len() < sig_start + 1 {
            return Err(CryptoError::InvalidSignatureFormat("Missing signature data".to_string()));
        }
        
        // NULS uses DER-encoded signatures
        let sig_data = &sig_bytes[sig_start..];
        let ecdsa_sig = secp256k1::ecdsa::Signature::from_der(sig_data)
            .map_err(|e| CryptoError::InvalidSignatureFormat(format!("Invalid DER signature: {}", e)))?;
        
        // Verify
        match self.secp.verify_ecdsa(&msg, &ecdsa_sig, &pubkey) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    /// Parse NULS address to extract chain ID and address type
    fn parse_nuls_address(&self, address: &str) -> Result<(u16, u8), CryptoError> {
        // NULS addresses start with "NULS" or chain-specific prefix
        // Format: prefix + base58check(chain_id[2] + type[1] + hash[20])
        if address.starts_with("NULSd6") {
            // NULS mainnet, chain_id=1
            Ok((1, 1))
        } else if address.starts_with("tNULS") {
            // NULS testnet
            Ok((2, 1))
        } else {
            // Try to decode as generic NULS address
            Ok((1, 1))
        }
    }
    
    /// Build NULS address from components
    fn build_nuls_address(&self, chain_id: u16, addr_type: u8, hash: &[u8]) -> Result<String, CryptoError> {
        use sha2::{Sha256, Digest as Sha2Digest};
        
        // Build address bytes: chain_id(2) + type(1) + hash(20)
        let mut addr_bytes = Vec::with_capacity(23);
        addr_bytes.extend_from_slice(&chain_id.to_le_bytes());
        addr_bytes.push(addr_type);
        addr_bytes.extend_from_slice(&hash[..20]);
        
        // XOR checksum
        let xor = addr_bytes.iter().fold(0u8, |acc, &b| acc ^ b);
        addr_bytes.push(xor);
        
        // Base58 encode
        let encoded = bs58::encode(&addr_bytes).into_string();
        
        // Add prefix based on chain
        let prefix = match chain_id {
            1 => "NULSd",
            2 => "tNULSe",
            _ => "NULS",
        };
        
        Ok(format!("{}{}", prefix, encoded))
    }
    
    /// Verify Cosmos SDK signature
    fn verify_cosmos_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        use sha2::{Sha256, Digest as Sha2Digest};
        use ripemd::Ripemd160;
        use ripemd::Digest as RipemdDigest;
        
        // Cosmos signatures can be hex or base64
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
        
        // Cosmos SDK signs SHA256 of the message
        let message_hash = Sha256::digest(message.as_bytes());
        
        let msg = Secp256k1Message::from_digest_slice(&message_hash)
            .map_err(|_| CryptoError::VerificationFailed)?;
        
        // Extract address prefix from expected address
        let prefix = expected_address.split('1').next().unwrap_or("cosmos");
        
        // Cosmos signatures don't include recovery id, try both
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
                let pubkey_bytes = public_key.serialize();
                let sha_hash = Sha256::digest(&pubkey_bytes);
                let ripemd_hash = Ripemd160::digest(&sha_hash);
                
                // Bech32 encode
                if let Ok(hrp) = bech32::Hrp::parse(prefix) {
                    if let Ok(address) = bech32::encode::<bech32::Bech32>(hrp, &ripemd_hash) {
                        if address == expected_address {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        
        Ok(false)
    }
    
    /// Verify Substrate (Polkadot) signature
    /// 
    /// Substrate uses Sr25519 (Schnorrkel) or Ed25519 depending on key type.
    fn verify_substrate_signature(
        &self,
        message: &str,
        signature: &str,
        expected_address: &str,
    ) -> Result<bool, CryptoError> {
        // Substrate signatures are typically hex-encoded
        // The signature includes a prefix byte indicating the crypto type:
        // 0x00 = Ed25519, 0x01 = Sr25519, 0x02 = ECDSA
        let sig_hex = signature.trim_start_matches("0x");
        let sig_bytes = hex::decode(sig_hex)?;
        
        if sig_bytes.is_empty() {
            return Err(CryptoError::InvalidSignatureFormat("Empty signature".to_string()));
        }
        
        // Check if it's a wrapped message format (common in Substrate)
        // Format: <Bytes>message</Bytes>
        let wrapped_message = format!("<Bytes>{}</Bytes>", message);
        
        // For Ed25519 signatures (64 bytes or 65 with type prefix)
        let (sig_data, _crypto_type) = if sig_bytes.len() == 65 {
            (&sig_bytes[1..], sig_bytes[0])
        } else if sig_bytes.len() == 64 {
            (&sig_bytes[..], 0x00u8) // Assume Ed25519
        } else {
            return Err(CryptoError::InvalidSignatureFormat(format!(
                "Invalid Substrate signature length: {}",
                sig_bytes.len()
            )));
        };
        
        // Decode the SS58 address to get the public key
        let pubkey_bytes = self.decode_ss58_address(expected_address)?;
        
        if pubkey_bytes.len() != 32 {
            return Err(CryptoError::InvalidPublicKey(format!(
                "Expected 32 bytes, got {}",
                pubkey_bytes.len()
            )));
        }
        
        // Try Ed25519 verification
        let sig_array: [u8; 64] = sig_data.try_into()
            .map_err(|_| CryptoError::InvalidSignatureFormat("Invalid signature length".to_string()))?;
        let signature_obj = Ed25519Signature::from_bytes(&sig_array);
        
        let pubkey_array: [u8; 32] = pubkey_bytes.try_into()
            .map_err(|_| CryptoError::InvalidPublicKey("Invalid public key length".to_string()))?;
        let verifying_key = VerifyingKey::from_bytes(&pubkey_array)
            .map_err(|e| CryptoError::InvalidPublicKey(e.to_string()))?;
        
        // Try both raw message and wrapped message
        if verifying_key.verify(message.as_bytes(), &signature_obj).is_ok() {
            return Ok(true);
        }
        if verifying_key.verify(wrapped_message.as_bytes(), &signature_obj).is_ok() {
            return Ok(true);
        }
        
        Ok(false)
    }
    
    /// Decode SS58 address to get public key bytes
    fn decode_ss58_address(&self, address: &str) -> Result<Vec<u8>, CryptoError> {
        // SS58 is base58check with a network prefix
        let decoded = bs58::decode(address)
            .into_vec()
            .map_err(|e| CryptoError::Base58Error(e.to_string()))?;
        
        if decoded.len() < 35 {
            return Err(CryptoError::InvalidPublicKey(
                "SS58 address too short".to_string()
            ));
        }
        
        // Format: prefix(1-2 bytes) + pubkey(32 bytes) + checksum(2 bytes)
        // Simple prefix is 1 byte, multi-byte prefix starts with value >= 64
        let prefix_len = if decoded[0] < 64 { 1 } else { 2 };
        let pubkey_end = decoded.len() - 2; // Remove 2-byte checksum
        
        Ok(decoded[prefix_len..pubkey_end].to_vec())
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
    pub fn compute_item_hash(&self, content: &str) -> String {
        self.sha256_hash(content.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_verification_buffer() {
        let buffer = CryptoService::get_verification_buffer(
            &Chain::ETH,
            "0x1234567890123456789012345678901234567890",
            &MessageType::POST,
            "abcd1234",
        );
        assert_eq!(buffer, "ETH\n0x1234567890123456789012345678901234567890\nPOST\nabcd1234");
    }
    
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
        let crypto = CryptoService::new();
        
        // Create a public key from known bytes (compressed)
        let pubkey_hex = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";
        let pubkey_bytes = hex::decode(pubkey_hex).unwrap();
        let pubkey = PublicKey::from_slice(&pubkey_bytes).unwrap();
        
        let address = crypto.pubkey_to_eth_address(&pubkey);
        assert!(address.starts_with("0x"));
        assert_eq!(address.len(), 42);
    }
}
