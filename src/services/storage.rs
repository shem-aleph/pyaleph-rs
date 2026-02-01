//! Storage service
//!
//! Handles file storage and retrieval.

use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use thiserror::Error;
use sha2::{Sha256, Digest};

use crate::config::StorageConfig;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("File not found: {0}")]
    NotFound(String),
    
    #[error("File too large: {size} bytes (max: {max} bytes)")]
    FileTooLarge { size: u64, max: u64 },
    
    #[error("Invalid hash")]
    InvalidHash,
}

/// Storage service for file operations
pub struct StorageService {
    files_dir: PathBuf,
    cache_dir: PathBuf,
    max_file_size: u64,
    enable_cache: bool,
}

impl StorageService {
    /// Create a new storage service
    pub fn new(config: &StorageConfig) -> Result<Self, StorageError> {
        // Create directories if they don't exist
        std::fs::create_dir_all(&config.files_dir)?;
        std::fs::create_dir_all(&config.cache_dir)?;
        
        Ok(Self {
            files_dir: config.files_dir.clone(),
            cache_dir: config.cache_dir.clone(),
            max_file_size: config.max_file_size,
            enable_cache: config.enable_cache,
        })
    }
    
    /// Store content and return its hash
    pub async fn store(&self, content: &[u8]) -> Result<String, StorageError> {
        // Check size
        if content.len() as u64 > self.max_file_size {
            return Err(StorageError::FileTooLarge {
                size: content.len() as u64,
                max: self.max_file_size,
            });
        }
        
        // Calculate hash
        let hash = self.hash_content(content);
        
        // Get file path
        let file_path = self.hash_to_path(&hash);
        
        // Create parent directories
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // Write file if it doesn't exist
        if !file_path.exists() {
            let mut file = fs::File::create(&file_path).await?;
            file.write_all(content).await?;
            file.sync_all().await?;
        }
        
        Ok(hash)
    }
    
    /// Retrieve content by hash
    pub async fn get(&self, hash: &str) -> Result<Vec<u8>, StorageError> {
        // Check cache first
        if self.enable_cache {
            let cache_path = self.cache_dir.join(hash);
            if cache_path.exists() {
                return Ok(fs::read(&cache_path).await?);
            }
        }
        
        // Get from main storage
        let file_path = self.hash_to_path(hash);
        
        if !file_path.exists() {
            return Err(StorageError::NotFound(hash.to_string()));
        }
        
        let content = fs::read(&file_path).await?;
        
        // Verify hash
        let computed_hash = self.hash_content(&content);
        if computed_hash != hash {
            return Err(StorageError::InvalidHash);
        }
        
        // Update cache
        if self.enable_cache {
            let cache_path = self.cache_dir.join(hash);
            let _ = fs::write(&cache_path, &content).await;
        }
        
        Ok(content)
    }
    
    /// Check if content exists
    pub async fn exists(&self, hash: &str) -> bool {
        let file_path = self.hash_to_path(hash);
        file_path.exists()
    }
    
    /// Delete content by hash
    pub async fn delete(&self, hash: &str) -> Result<(), StorageError> {
        let file_path = self.hash_to_path(hash);
        
        if file_path.exists() {
            fs::remove_file(&file_path).await?;
        }
        
        // Also remove from cache
        if self.enable_cache {
            let cache_path = self.cache_dir.join(hash);
            let _ = fs::remove_file(&cache_path).await;
        }
        
        Ok(())
    }
    
    /// Get the size of stored content
    pub async fn get_size(&self, hash: &str) -> Result<u64, StorageError> {
        let file_path = self.hash_to_path(hash);
        
        if !file_path.exists() {
            return Err(StorageError::NotFound(hash.to_string()));
        }
        
        let metadata = fs::metadata(&file_path).await?;
        Ok(metadata.len())
    }
    
    /// Calculate SHA256 hash of content
    fn hash_content(&self, content: &[u8]) -> String {
        let hash = Sha256::digest(content);
        hex::encode(hash)
    }
    
    /// Convert hash to file path (using directory sharding)
    fn hash_to_path(&self, hash: &str) -> PathBuf {
        // Use first 2 characters as directory for sharding
        let prefix = &hash[..2.min(hash.len())];
        self.files_dir.join(prefix).join(hash)
    }
    
    /// Get total storage used
    pub async fn get_total_size(&self) -> Result<u64, StorageError> {
        let mut total = 0u64;
        let mut entries = fs::read_dir(&self.files_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let mut subentries = fs::read_dir(entry.path()).await?;
                while let Some(subentry) = subentries.next_entry().await? {
                    if subentry.file_type().await?.is_file() {
                        total += subentry.metadata().await?.len();
                    }
                }
            }
        }
        
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_store_and_retrieve() {
        let temp_dir = tempdir().unwrap();
        let config = StorageConfig {
            files_dir: temp_dir.path().join("files"),
            cache_dir: temp_dir.path().join("cache"),
            max_file_size: 1024 * 1024,
            enable_cache: true,
        };
        
        let service = StorageService::new(&config).unwrap();
        
        let content = b"Hello, Aleph!";
        let hash = service.store(content).await.unwrap();
        
        let retrieved = service.get(&hash).await.unwrap();
        assert_eq!(retrieved, content);
    }
    
    #[tokio::test]
    async fn test_file_too_large() {
        let temp_dir = tempdir().unwrap();
        let config = StorageConfig {
            files_dir: temp_dir.path().join("files"),
            cache_dir: temp_dir.path().join("cache"),
            max_file_size: 10, // Very small limit
            enable_cache: false,
        };
        
        let service = StorageService::new(&config).unwrap();
        
        let content = b"This is a long content that exceeds the limit";
        let result = service.store(content).await;
        
        assert!(matches!(result, Err(StorageError::FileTooLarge { .. })));
    }
}
