//! IPFS service
//!
//! Handles interaction with IPFS for content storage and retrieval.

use reqwest::Client;
use thiserror::Error;
use serde::Deserialize;

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
}
