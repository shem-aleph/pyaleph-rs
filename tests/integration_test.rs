//! Integration tests against live Aleph API
//!
//! These tests require network access and compare our implementation
//! against the real Aleph.im API.

use reqwest::Client;
use serde_json::Value;

const ALEPH_API: &str = "https://api2.aleph.im/api/v0";

/// Test that we can fetch messages from the real API
#[tokio::test]
#[ignore] // Run with: cargo test --ignored
async fn test_fetch_messages_from_aleph() {
    let client = Client::new();
    
    let response = client
        .get(format!("{}/messages.json?pagination=5", ALEPH_API))
        .send()
        .await
        .expect("Failed to fetch messages");
    
    assert!(response.status().is_success());
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    
    // Check structure
    assert!(json.get("messages").is_some());
    assert!(json.get("pagination_total").is_some());
    
    let messages = json["messages"].as_array().expect("messages should be array");
    assert!(!messages.is_empty(), "Should have some messages");
    
    // Check message structure
    let msg = &messages[0];
    assert!(msg.get("type").is_some());
    assert!(msg.get("chain").is_some());
    assert!(msg.get("sender").is_some());
    assert!(msg.get("signature").is_some());
    assert!(msg.get("item_hash").is_some());
}

/// Test that we can fetch posts from the real API
#[tokio::test]
#[ignore]
async fn test_fetch_posts_from_aleph() {
    let client = Client::new();
    
    let response = client
        .get(format!("{}/posts.json?pagination=5", ALEPH_API))
        .send()
        .await
        .expect("Failed to fetch posts");
    
    assert!(response.status().is_success());
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    
    assert!(json.get("posts").is_some());
    
    let posts = json["posts"].as_array().expect("posts should be array");
    assert!(!posts.is_empty(), "Should have some posts");
}

/// Test that we can fetch aggregates from the real API
#[tokio::test]
#[ignore]
async fn test_fetch_aggregates_from_aleph() {
    let client = Client::new();
    
    // Use a known address with aggregates
    let address = "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8";
    
    let response = client
        .get(format!("{}/aggregates/{}.json", ALEPH_API, address))
        .send()
        .await
        .expect("Failed to fetch aggregates");
    
    assert!(response.status().is_success());
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    
    assert!(json.get("address").is_some());
    assert!(json.get("data").is_some());
}

/// Test that we can fetch balance from the real API
#[tokio::test]
#[ignore]
async fn test_fetch_balance_from_aleph() {
    let client = Client::new();
    
    let address = "0x06DE0C46884EbFF46558Cd1a9e7DA6B1c3E9D0a8";
    
    let response = client
        .get(format!("{}/addresses/{}/balance", ALEPH_API, address))
        .send()
        .await
        .expect("Failed to fetch balance");
    
    assert!(response.status().is_success());
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    
    assert!(json.get("balance").is_some() || json.get("address").is_some());
}

/// Test that we can fetch pricing from the real API
#[tokio::test]
#[ignore]
async fn test_fetch_pricing_from_aleph() {
    let client = Client::new();
    
    let response = client
        .get(format!("{}/price", ALEPH_API))
        .send()
        .await
        .expect("Failed to fetch pricing");
    
    assert!(response.status().is_success());
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    
    // Should have pricing info
    println!("Pricing response: {:?}", json);
}

/// Test message type distribution
#[tokio::test]
#[ignore]
async fn test_message_types_distribution() {
    let client = Client::new();
    
    let types = ["AGGREGATE", "POST", "STORE", "PROGRAM", "INSTANCE"];
    
    for msg_type in types {
        let response = client
            .get(format!(
                "{}/messages.json?msgType={}&pagination=1",
                ALEPH_API, msg_type
            ))
            .send()
            .await
            .expect("Failed to fetch messages");
        
        if response.status().is_success() {
            let json: Value = response.json().await.expect("Failed to parse JSON");
            let total = json["pagination_total"].as_u64().unwrap_or(0);
            println!("{}: {} messages", msg_type, total);
        }
    }
}

/// Compare our serialization with real messages
#[tokio::test]
#[ignore]
async fn test_message_deserialization() {
    use aleph_core::types::Message;
    
    let client = Client::new();
    
    let response = client
        .get(format!("{}/messages.json?pagination=10", ALEPH_API))
        .send()
        .await
        .expect("Failed to fetch messages");
    
    let json: Value = response.json().await.expect("Failed to parse JSON");
    let messages = json["messages"].as_array().expect("messages should be array");
    
    for msg in messages {
        // Try to deserialize into our Message type
        let result: Result<Message, _> = serde_json::from_value(msg.clone());
        
        match result {
            Ok(parsed) => {
                println!(
                    "✓ Parsed {} message: {}",
                    parsed.message_type,
                    parsed.item_hash
                );
            }
            Err(e) => {
                println!(
                    "✗ Failed to parse message: {} - Error: {}",
                    msg.get("item_hash").and_then(|v| v.as_str()).unwrap_or("?"),
                    e
                );
            }
        }
    }
}
