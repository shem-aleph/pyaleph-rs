//! IPFS service
//!
//! Handles interaction with IPFS for content storage and retrieval,
//! including directory pinning support.
//!
//! Reference: aleph/services/ipfs.py

use reqwest::Client;
use thiserror::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::config::IpfsConfig;

#[derive(Debug, Error)]
pub enum IpfsError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    
    #[error("IPFS API error: {message}")]
    Api { message: String },
    
    #[error("Content not found: {0}")]
    NotFound(String),
    
    #[error("Timeout fetching content")]
    Timeout,
}

/// Response from IPFS add operation
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AddResponse {
    pub hash: String,
    pub name: String,
    pub size: String,
}

/// IPFS service for content operations
#[derive(Debug)]
pub struct IpfsService {
    client: Client,
    api_url: String,
    gateway_url: String,
    pin_content: bool,
}

impl IpfsService {
    /// Create a new IPFS service
    pub fn new(config: &IpfsConfig) -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
            api_url: config.api_url.clone(),
            gateway_url: config.gateway_url.clone(),
            pin_content: config.pin_content,
        }
    }
    
    /// Add content to IPFS
    pub async fn add(&self, content: Vec<u8>) -> Result<String, IpfsError> {
        let url = format!("{}/api/v0/add", self.api_url);
        
        let form = reqwest::multipart::Form::new()
            .part("file", reqwest::multipart::Part::bytes(content));
        
        let response = self.client
            .post(&url)
            .multipart(form)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        let add_response: AddResponse = response.json().await?;
        
        // Pin if configured
        if self.pin_content {
            let _ = self.pin(&add_response.hash).await;
        }
        
        Ok(add_response.hash)
    }
    
    /// Get content from IPFS
    pub async fn get(&self, hash: &str) -> Result<Vec<u8>, IpfsError> {
        // Try API first
        let api_result = self.get_via_api(hash).await;
        if api_result.is_ok() {
            return api_result;
        }
        
        // Fall back to gateway
        self.get_via_gateway(hash).await
    }
    
    /// Get content via IPFS API
    async fn get_via_api(&self, hash: &str) -> Result<Vec<u8>, IpfsError> {
        let url = format!("{}/api/v0/cat?arg={}", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            if response.status().as_u16() == 404 {
                return Err(IpfsError::NotFound(hash.to_string()));
            }
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        Ok(response.bytes().await?.to_vec())
    }
    
    /// Get content via IPFS gateway
    async fn get_via_gateway(&self, hash: &str) -> Result<Vec<u8>, IpfsError> {
        let url = format!("{}/{}", self.gateway_url, hash);
        
        let response = self.client
            .get(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            if response.status().as_u16() == 404 {
                return Err(IpfsError::NotFound(hash.to_string()));
            }
            return Err(IpfsError::Api { 
                message: format!("Gateway returned status {}", response.status()) 
            });
        }
        
        Ok(response.bytes().await?.to_vec())
    }
    
    /// Pin content to local IPFS node
    pub async fn pin(&self, hash: &str) -> Result<(), IpfsError> {
        let url = format!("{}/api/v0/pin/add?arg={}", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        Ok(())
    }
    
    /// Unpin content from local IPFS node
    pub async fn unpin(&self, hash: &str) -> Result<(), IpfsError> {
        let url = format!("{}/api/v0/pin/rm?arg={}", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        Ok(())
    }
    
    /// Check if content exists
    pub async fn exists(&self, hash: &str) -> bool {
        let url = format!("{}/api/v0/object/stat?arg={}", self.api_url, hash);
        
        match self.client.post(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }
    
    /// Get the size of content
    pub async fn get_size(&self, hash: &str) -> Result<u64, IpfsError> {
        let url = format!("{}/api/v0/object/stat?arg={}", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(IpfsError::NotFound(hash.to_string()));
        }
        
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct StatResponse {
            cumulative_size: u64,
        }
        
        let stat: StatResponse = response.json().await?;
        Ok(stat.cumulative_size)
    }
    
    /// Check if IPFS is connected and responsive
    pub async fn is_connected(&self) -> bool {
        let url = format!("{}/api/v0/version", self.api_url);
        
        match self.client.post(&url).timeout(std::time::Duration::from_secs(5)).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }
    
    /// Pin a directory (recursive pin)
    pub async fn pin_directory(&self, hash: &str) -> Result<PinResult, IpfsError> {
        let url = format!("{}/api/v0/pin/add?arg={}&recursive=true", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .timeout(std::time::Duration::from_secs(300)) // 5 min timeout for large dirs
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        let result: PinResult = response.json().await?;
        Ok(result)
    }
    
    /// List all pins
    pub async fn list_pins(&self, pin_type: Option<&str>) -> Result<HashMap<String, PinInfo>, IpfsError> {
        let mut url = format!("{}/api/v0/pin/ls", self.api_url);
        
        if let Some(t) = pin_type {
            url.push_str(&format!("?type={}", t));
        }
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct PinListResponse {
            keys: HashMap<String, PinInfo>,
        }
        
        let result: PinListResponse = response.json().await?;
        Ok(result.keys)
    }
    
    /// Add directory from files
    pub async fn add_directory(&self, files: Vec<(String, Vec<u8>)>) -> Result<DirectoryAddResult, IpfsError> {
        let url = format!("{}/api/v0/add?wrap-with-directory=true", self.api_url);
        
        let mut form = reqwest::multipart::Form::new();
        
        for (name, content) in files {
            let part = reqwest::multipart::Part::bytes(content)
                .file_name(name);
            form = form.part("file", part);
        }
        
        let response = self.client
            .post(&url)
            .multipart(form)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        // Response is newline-delimited JSON
        let text = response.text().await?;
        let mut files = Vec::new();
        let mut directory_hash = String::new();
        
        for line in text.lines() {
            if let Ok(entry) = serde_json::from_str::<AddResponse>(line) {
                if entry.name.is_empty() {
                    // This is the wrapper directory
                    directory_hash = entry.hash;
                } else {
                    files.push(FileEntry {
                        name: entry.name,
                        hash: entry.hash,
                        size: entry.size.parse().unwrap_or(0),
                    });
                }
            }
        }
        
        Ok(DirectoryAddResult {
            hash: directory_hash,
            files,
        })
    }
    
    /// List directory contents
    pub async fn list_directory(&self, hash: &str) -> Result<Vec<DirectoryEntry>, IpfsError> {
        let url = format!("{}/api/v0/ls?arg={}", self.api_url, hash);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            if response.status().as_u16() == 500 {
                // Might not be a directory
                return Err(IpfsError::Api { 
                    message: "Not a directory".to_string() 
                });
            }
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct LsResponse {
            objects: Vec<LsObject>,
        }
        
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct LsObject {
            links: Vec<DirectoryEntry>,
        }
        
        let result: LsResponse = response.json().await?;
        
        Ok(result.objects.into_iter()
            .flat_map(|o| o.links)
            .collect())
    }
    
    /// Get IPFS node ID and info
    pub async fn get_node_info(&self) -> Result<NodeInfo, IpfsError> {
        let url = format!("{}/api/v0/id", self.api_url);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        let info: NodeInfo = response.json().await?;
        Ok(info)
    }
    
    /// Get IPFS repo stats
    pub async fn get_repo_stats(&self) -> Result<RepoStats, IpfsError> {
        let url = format!("{}/api/v0/repo/stat", self.api_url);
        
        let response = self.client
            .post(&url)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(IpfsError::Api { message: error_text });
        }
        
        let stats: RepoStats = response.json().await?;
        Ok(stats)
    }
}

/// Result of pinning operation
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PinResult {
    pub pins: Vec<String>,
}

/// Pin information
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PinInfo {
    #[serde(rename = "Type")]
    pub pin_type: String,
}

/// Result of adding a directory
#[derive(Debug)]
pub struct DirectoryAddResult {
    pub hash: String,
    pub files: Vec<FileEntry>,
}

/// File entry in a directory
#[derive(Debug)]
pub struct FileEntry {
    pub name: String,
    pub hash: String,
    pub size: u64,
}

/// Directory entry from ls
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DirectoryEntry {
    pub name: String,
    pub hash: String,
    pub size: u64,
    #[serde(rename = "Type")]
    pub entry_type: u32, // 1 = dir, 2 = file
}

impl DirectoryEntry {
    pub fn is_directory(&self) -> bool {
        self.entry_type == 1
    }
    
    pub fn is_file(&self) -> bool {
        self.entry_type == 2
    }
}

/// IPFS node info
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NodeInfo {
    #[serde(rename = "ID")]
    pub id: String,
    pub public_key: String,
    pub addresses: Vec<String>,
    pub agent_version: String,
}

/// IPFS repo statistics
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct RepoStats {
    pub repo_size: u64,
    pub storage_max: u64,
    pub num_objects: u64,
    pub repo_path: String,
}
