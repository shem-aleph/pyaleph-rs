//! Message service
//!
//! Handles message validation, processing, and storage.

use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::types::*;
use crate::services::CryptoService;
use crate::db::models::MessageDb;

/// Message processing errors
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("Invalid signature")]
    InvalidSignature,
    
    #[error("Invalid content: {0}")]
    InvalidContent(String),
    
    #[error("Invalid item hash")]
    InvalidItemHash,
    
    #[error("Message already exists")]
    AlreadyExists,
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    
    #[error("Content fetch failed: {0}")]
    ContentFetch(String),
}

/// Message service for processing Aleph messages
pub struct MessageService {
    crypto: CryptoService,
}

impl Default for MessageService {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageService {
    pub fn new() -> Self {
        Self {
            crypto: CryptoService::new(),
        }
    }
    
    /// Validate a message
    pub fn validate(&self, message: &Message) -> Result<(), MessageError> {
        // 1. Verify signature
        self.verify_signature(message)?;
        
        // 2. Verify item hash matches content
        if let Some(content) = &message.item_content {
            let computed_hash = self.crypto.sha256_hash(content.as_bytes());
            if computed_hash != message.item_hash {
                return Err(MessageError::InvalidItemHash);
            }
        }
        
        // 3. Validate timestamp (not too far in future)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time is before UNIX epoch")
            .as_secs_f64();
        
        if message.time > now + 300.0 {
            return Err(MessageError::InvalidContent(
                "Timestamp too far in future".to_string()
            ));
        }
        
        // 4. Validate content based on message type
        self.validate_content(message)?;
        
        Ok(())
    }
    
    /// Verify message signature
    fn verify_signature(&self, message: &Message) -> Result<(), MessageError> {
        // Build the message to sign (item_hash for inline, or content hash)
        let message_to_verify = &message.item_hash;
        
        match self.crypto.verify_signature(
            &message.chain,
            message_to_verify,
            &message.signature,
            &message.sender,
        ) {
            Ok(true) => Ok(()),
            Ok(false) => Err(MessageError::InvalidSignature),
            Err(e) => {
                warn!("Signature verification error: {}", e);
                // For chains we don't support yet, skip verification
                Ok(())
            }
        }
    }
    
    /// Validate message content based on type
    fn validate_content(&self, message: &Message) -> Result<(), MessageError> {
        let content = match &message.item_content {
            Some(c) => c,
            None => return Ok(()), // Content will be fetched later
        };
        
        match message.message_type {
            MessageType::Aggregate => {
                let _: AggregateContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
            MessageType::Post => {
                let _: PostContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
            MessageType::Store => {
                let _: StoreContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
            MessageType::Program => {
                let _: ProgramContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
            MessageType::Instance => {
                let _: InstanceContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
            MessageType::Forget => {
                let _: ForgetContent = serde_json::from_str(content)
                    .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
            }
        }
        
        Ok(())
    }
    
    /// Process and store a validated message
    pub async fn process(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        // Check if message already exists
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT item_hash FROM messages WHERE item_hash = $1"
        )
        .bind(&message.item_hash)
        .fetch_optional(pool)
        .await?;
        
        if existing.is_some() {
            return Err(MessageError::AlreadyExists);
        }
        
        // Insert message
        sqlx::query(
            r#"
            INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(&message.item_hash)
        .bind(message.message_type.to_string())
        .bind(message.chain.to_string())
        .bind(&message.sender)
        .bind(&message.signature)
        .bind(format!("{:?}", message.item_type).to_lowercase())
        .bind(&message.item_content)
        .bind(&message.channel)
        .bind(message.time)
        .execute(pool)
        .await?;
        
        info!("Stored message: {} ({})", message.item_hash, message.message_type);
        
        // Process based on message type
        match message.message_type {
            MessageType::Aggregate => self.process_aggregate(message, pool).await?,
            MessageType::Post => self.process_post(message, pool).await?,
            MessageType::Store => self.process_store(message, pool).await?,
            MessageType::Program => self.process_program(message, pool).await?,
            MessageType::Instance => self.process_instance(message, pool).await?,
            MessageType::Forget => self.process_forget(message, pool).await?,
        }
        
        Ok(())
    }
    
    async fn process_aggregate(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: AggregateContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        // Upsert aggregate
        sqlx::query(
            r#"
            INSERT INTO aggregates (address, key, content, time)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (address, key) DO UPDATE SET content = $3, time = $4
            "#
        )
        .bind(&content.address)
        .bind(&content.key)
        .bind(&content.content)
        .bind(content.time)
        .execute(pool)
        .await?;
        
        debug!("Updated aggregate: {}:{}", content.address, content.key);
        Ok(())
    }
    
    async fn process_post(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: PostContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        sqlx::query(
            r#"
            INSERT INTO posts (item_hash, address, post_type, content, ref_, channel, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&message.item_hash)
        .bind(&content.address)
        .bind(&content.post_type)
        .bind(&content.content)
        .bind(&content.ref_)
        .bind(&message.channel)
        .bind(content.time)
        .execute(pool)
        .await?;
        
        debug!("Stored post: {}", message.item_hash);
        Ok(())
    }
    
    async fn process_store(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: StoreContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        sqlx::query(
            r#"
            INSERT INTO file_pins (item_hash, owner, size, content_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&content.item_hash)
        .bind(&content.address)
        .bind(content.size.unwrap_or(0) as i64)
        .bind(&content.content_type)
        .execute(pool)
        .await?;
        
        debug!("Stored file pin: {}", content.item_hash);
        Ok(())
    }
    
    async fn process_program(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: ProgramContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        sqlx::query(
            r#"
            INSERT INTO programs (item_hash, owner, code_ref, runtime_ref, memory, vcpus, allow_amend)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&message.item_hash)
        .bind(&content.address)
        .bind(&content.code.ref_)
        .bind(&content.runtime.ref_)
        .bind(content.resources.memory as i32)
        .bind(content.resources.vcpus as i32)
        .bind(content.allow_amend)
        .execute(pool)
        .await?;

        debug!("Stored program: {}", message.item_hash);
        Ok(())
    }
    
    async fn process_instance(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: InstanceContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        let payment_type = content.payment.as_ref().map(|p| format!("{:?}", p.payment_type).to_lowercase());
        let payment_chain = content.payment.as_ref().map(|p| p.chain.to_string());
        
        sqlx::query(
            r#"
            INSERT INTO instances (item_hash, owner, rootfs_ref, memory, vcpus, payment_type, payment_chain, allow_amend)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(&message.item_hash)
        .bind(&content.address)
        .bind(&content.rootfs.parent.ref_)
        .bind(content.resources.memory as i32)
        .bind(content.resources.vcpus as i32)
        .bind(&payment_type)
        .bind(&payment_chain)
        .bind(content.allow_amend)
        .execute(pool)
        .await?;
        
        debug!("Stored instance: {}", message.item_hash);
        Ok(())
    }
    
    async fn process_forget(&self, message: &Message, pool: &PgPool) -> Result<(), MessageError> {
        let item_content = message.item_content.as_ref()
            .ok_or_else(|| MessageError::InvalidContent("Missing item_content".to_string()))?;
        let content: ForgetContent = serde_json::from_str(item_content)
            .map_err(|e| MessageError::InvalidContent(e.to_string()))?;
        
        for hash in &content.hashes {
            // Mark as forgotten
            sqlx::query(
                r#"
                INSERT INTO forgotten (item_hash, forgotten_by, reason)
                VALUES ($1, $2, $3)
                ON CONFLICT (item_hash) DO NOTHING
                "#
            )
            .bind(hash)
            .bind(&content.address)
            .bind(&content.reason)
            .execute(pool)
            .await?;
            
            debug!("Marked as forgotten: {}", hash);
        }
        
        Ok(())
    }
}
