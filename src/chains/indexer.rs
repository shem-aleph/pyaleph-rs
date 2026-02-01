//! Aleph multichain indexer client
//!
//! Queries https://multichain.api.aleph.cloud/ for blockchain events.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::types::Chain;

const DEFAULT_INDEXER_URL: &str = "https://multichain.api.aleph.cloud";

/// Indexer blockchain names
#[derive(Debug, Clone, Copy)]
pub enum IndexerBlockchain {
    Ethereum,
    Bsc,
    Solana,
    Avalanche,
}

impl IndexerBlockchain {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Bsc => "bsc",
            Self::Solana => "solana",
            Self::Avalanche => "avalanche",
        }
    }
}

impl From<Chain> for IndexerBlockchain {
    fn from(chain: Chain) -> Self {
        match chain {
            Chain::ETH => Self::Ethereum,
            Chain::SOL => Self::Solana,
            Chain::AVAX => Self::Avalanche,
            Chain::BSC => Self::Bsc,
            _ => Self::Ethereum, // Default fallback
        }
    }
}

/// Sync event from the indexer
#[derive(Debug, Clone, Deserialize)]
pub struct IndexerSyncEvent {
    pub transaction: String,
    pub height: u64,
    pub timestamp: u64, // milliseconds
    pub address: String,
    pub message: String,
}

/// Message event from the indexer  
#[derive(Debug, Clone, Deserialize)]
pub struct IndexerMessageEvent {
    pub transaction: String,
    pub height: u64,
    pub timestamp: u64, // milliseconds
    pub address: String,
    pub content: serde_json::Value,
}

/// GraphQL response wrapper
#[derive(Debug, Deserialize)]
struct GraphQLResponse<T> {
    data: T,
}

/// Sync events response
#[derive(Debug, Deserialize)]
struct SyncEventsData {
    #[serde(rename = "syncEvents")]
    sync_events: Vec<IndexerSyncEvent>,
}

/// Message events response
#[derive(Debug, Deserialize)]
struct MessageEventsData {
    #[serde(rename = "messageEvents")]
    message_events: Vec<IndexerMessageEvent>,
}

/// Parsed sync message content
#[derive(Debug, Clone, Deserialize)]
pub struct SyncMessageContent {
    pub protocol: String,
    pub version: u32,
    pub content: String, // IPFS hash
}

/// Aleph indexer client
pub struct IndexerClient {
    client: Client,
    base_url: String,
}

impl IndexerClient {
    /// Create a new indexer client
    pub fn new(base_url: Option<&str>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.unwrap_or(DEFAULT_INDEXER_URL).to_string(),
        }
    }

    /// Execute a GraphQL query
    async fn query<T: for<'de> Deserialize<'de>>(&self, query: &str) -> Result<T, IndexerError> {
        let response = self.client
            .post(&self.base_url)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await
            .map_err(|e| IndexerError::Request(e.to_string()))?;

        if !response.status().is_success() {
            return Err(IndexerError::Request(format!(
                "HTTP {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let result: GraphQLResponse<T> = response
            .json()
            .await
            .map_err(|e| IndexerError::Parse(e.to_string()))?;

        Ok(result.data)
    }

    /// Fetch sync events by date range (timestamps in milliseconds)
    pub async fn fetch_sync_events(
        &self,
        blockchain: IndexerBlockchain,
        start_date_ms: u64,
        end_date_ms: u64,
        limit: usize,
    ) -> Result<Vec<IndexerSyncEvent>, IndexerError> {
        let query = format!(
            r#"{{ syncEvents(blockchain: "{}", startDate: {}, endDate: {}, limit: {}) {{ transaction height timestamp address message }} }}"#,
            blockchain.as_str(),
            start_date_ms,
            end_date_ms,
            limit
        );

        debug!("Fetching sync events: {} to {}", start_date_ms, end_date_ms);

        let data: SyncEventsData = self.query(&query).await?;
        
        info!(
            "Fetched {} sync events from {} indexer",
            data.sync_events.len(),
            blockchain.as_str()
        );

        Ok(data.sync_events)
    }

    /// Fetch sync events by block range
    pub async fn fetch_sync_events_by_blocks(
        &self,
        blockchain: IndexerBlockchain,
        start_height: u64,
        end_height: u64,
        limit: usize,
    ) -> Result<Vec<IndexerSyncEvent>, IndexerError> {
        let query = format!(
            r#"{{ syncEvents(blockchain: "{}", startHeight: {}, endHeight: {}, limit: {}) {{ transaction height timestamp address message }} }}"#,
            blockchain.as_str(),
            start_height,
            end_height,
            limit
        );

        debug!("Fetching sync events: blocks {} to {}", start_height, end_height);

        let data: SyncEventsData = self.query(&query).await?;

        info!(
            "Fetched {} sync events from blocks {} to {}",
            data.sync_events.len(),
            start_height,
            end_height
        );

        Ok(data.sync_events)
    }

    /// Fetch message events by date range
    pub async fn fetch_message_events(
        &self,
        blockchain: IndexerBlockchain,
        start_date_ms: u64,
        end_date_ms: u64,
        limit: usize,
    ) -> Result<Vec<IndexerMessageEvent>, IndexerError> {
        let query = format!(
            r#"{{ messageEvents(blockchain: "{}", startDate: {}, endDate: {}, limit: {}) {{ transaction height timestamp address content }} }}"#,
            blockchain.as_str(),
            start_date_ms,
            end_date_ms,
            limit
        );

        debug!("Fetching message events: {} to {}", start_date_ms, end_date_ms);

        let data: MessageEventsData = self.query(&query).await?;

        info!(
            "Fetched {} message events from {} indexer",
            data.message_events.len(),
            blockchain.as_str()
        );

        Ok(data.message_events)
    }

    /// Parse sync message to get IPFS content hash
    pub fn parse_sync_message(message: &str) -> Result<SyncMessageContent, IndexerError> {
        serde_json::from_str(message)
            .map_err(|e| IndexerError::Parse(format!("Failed to parse sync message: {}", e)))
    }
}

/// Indexer error types
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("Request failed: {0}")]
    Request(String),

    #[error("Failed to parse response: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_sync_events() {
        let client = IndexerClient::new(None);
        
        // Fetch events from a known time range (Jan 1, 2026)
        let start = 1735689600000u64; // 2026-01-01
        let end = 1735776000000u64;   // 2026-01-02
        
        let events = client.fetch_sync_events(
            IndexerBlockchain::Ethereum,
            start,
            end,
            5,
        ).await;

        // Should succeed (might be empty if no events)
        assert!(events.is_ok());
    }

    #[test]
    fn test_parse_sync_message() {
        let msg = r#"{"protocol": "aleph-offchain", "version": 1, "content": "QmU1YbveJuJvgHFB35qBuaMQfhpY19zpBv8hzxyZEj3s41"}"#;
        
        let parsed = IndexerClient::parse_sync_message(msg);
        assert!(parsed.is_ok());
        
        let content = parsed.unwrap();
        assert_eq!(content.protocol, "aleph-offchain");
        assert_eq!(content.version, 1);
        assert!(content.content.starts_with("Qm"));
    }
}
