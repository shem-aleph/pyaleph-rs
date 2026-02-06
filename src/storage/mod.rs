//! Local content storage (file-based cache)
//!
//! Provides a disk-backed cache for message content. Files are stored
//! by their content hash under the configured data directory.
//! This acts as a read-through cache in front of IPFS.

pub mod tiered;

use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Local file storage for content caching
pub struct LocalStorage {
    base_dir: PathBuf,
}

impl LocalStorage {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            base_dir: data_dir.join("storage"),
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.base_dir).await
    }

    /// Get content by hash from local storage
    pub async fn get(&self, hash: &str) -> Option<Vec<u8>> {
        let path = self.path_for(hash);
        fs::read(&path).await.ok()
    }

    /// Store content by hash
    pub async fn put(&self, hash: &str, content: &[u8]) -> std::io::Result<()> {
        let path = self.path_for(hash);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = fs::File::create(&path).await?;
        file.write_all(content).await?;
        file.flush().await?;
        Ok(())
    }

    /// Check if content exists locally
    pub async fn exists(&self, hash: &str) -> bool {
        self.path_for(hash).exists()
    }

    /// Remove content by hash
    pub async fn remove(&self, hash: &str) -> std::io::Result<()> {
        let path = self.path_for(hash);
        if path.exists() {
            fs::remove_file(&path).await?;
        }
        Ok(())
    }

    /// Get the total size of cached content
    pub async fn cache_size(&self) -> std::io::Result<u64> {
        let mut total = 0u64;
        let mut entries = fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(metadata) = entry.metadata().await {
                total += metadata.len();
            }
        }
        Ok(total)
    }

    /// File path for a given content hash
    /// Uses two-level directory sharding: ab/cd/abcdef...
    fn path_for(&self, hash: &str) -> PathBuf {
        if hash.len() >= 4 {
            self.base_dir
                .join(&hash[0..2])
                .join(&hash[2..4])
                .join(hash)
        } else {
            self.base_dir.join(hash)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_put_and_get() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalStorage::new(tmp.path());
        storage.init().await.unwrap();

        storage.put("abcdef1234", b"hello world").await.unwrap();
        let content = storage.get("abcdef1234").await.unwrap();
        assert_eq!(content, b"hello world");
    }

    #[tokio::test]
    async fn test_exists() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalStorage::new(tmp.path());
        storage.init().await.unwrap();

        assert!(!storage.exists("missing").await);
        storage.put("abcdef1234", b"data").await.unwrap();
        assert!(storage.exists("abcdef1234").await);
    }

    #[tokio::test]
    async fn test_remove() {
        let tmp = TempDir::new().unwrap();
        let storage = LocalStorage::new(tmp.path());
        storage.init().await.unwrap();

        storage.put("abcdef1234", b"data").await.unwrap();
        assert!(storage.exists("abcdef1234").await);
        storage.remove("abcdef1234").await.unwrap();
        assert!(!storage.exists("abcdef1234").await);
    }

    #[tokio::test]
    async fn test_path_sharding() {
        let storage = LocalStorage::new(Path::new("/tmp/test"));
        let path = storage.path_for("abcdef1234");
        assert!(path.to_str().unwrap().contains("ab"));
        assert!(path.to_str().unwrap().contains("cd"));
    }

    #[tokio::test]
    async fn test_short_hash() {
        let storage = LocalStorage::new(Path::new("/tmp/test"));
        // Short hashes don't get sharded
        let path = storage.path_for("ab");
        assert_eq!(path, Path::new("/tmp/test/storage/ab"));
    }
}
