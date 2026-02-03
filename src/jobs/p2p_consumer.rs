//! P2P message consumer via RabbitMQ
//!
//! Connects to the p2p-service RabbitMQ bridge to receive messages from the
//! Aleph GossipSub network. Messages arrive on the `p2p-subscribe` exchange
//! with routing keys like `p2p.ALEPH-TEST.{peer_id}`.
//!
//! Also subscribes to the `ALIVE` topic for peer discovery, storing HTTP API
//! server URLs in Redis for peer content fetching.
//!
//! Reference: aleph/services/p2p/protocol.py, aleph/jobs/process_pending_messages.py

use std::sync::Arc;
use std::time::Duration;

use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties, Consumer,
};
use futures::StreamExt;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::types::Message;

/// Dedup cache capacity — matches pyaleph's 200K entry LRU
const DEDUP_CACHE_SIZE: usize = 200_000;

/// Reconnect delay on RabbitMQ connection failure
const RECONNECT_DELAY_SECS: u64 = 5;

/// Dedup key: (sender, item_hash, signature)
type DedupKey = (String, String, String);

/// Alive message from a peer announcing its API endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliveMessage {
    /// HTTP API address, e.g. "http://1.2.3.4:4024"
    pub address: String,
    /// Peer type: "HTTP", "P2P", or "IPFS"
    pub peer_type: String,
    /// Optional: peer_id for identification
    #[serde(default)]
    pub peer_id: Option<String>,
}

/// Context for the P2P consumer job
pub struct P2pConsumerContext {
    pub db: PgPool,
    pub config: Arc<Config>,
    pub dedup_cache: Mutex<LruCache<DedupKey, ()>>,
    pub redis_client: Option<redis::aio::ConnectionManager>,
}

impl P2pConsumerContext {
    pub async fn new(db: PgPool, config: Arc<Config>) -> Self {
        let dedup_cache = Mutex::new(LruCache::new(
            std::num::NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
        ));

        // Try connecting to Redis
        let redis_client = match redis::Client::open(config.redis.url.as_str()) {
            Ok(client) => match redis::aio::ConnectionManager::new(client).await {
                Ok(mgr) => {
                    info!("Connected to Redis at {}", config.redis.url);
                    Some(mgr)
                }
                Err(e) => {
                    warn!("Failed to connect to Redis: {} — alive messages won't update api_servers", e);
                    None
                }
            },
            Err(e) => {
                warn!("Invalid Redis URL '{}': {}", config.redis.url, e);
                None
            }
        };

        Self {
            db,
            config,
            dedup_cache,
            redis_client,
        }
    }
}

/// Run the P2P consumer job. Loops forever, reconnecting on failure.
pub async fn run(ctx: Arc<P2pConsumerContext>) {
    if !ctx.config.rabbitmq.enabled {
        info!("RabbitMQ integration disabled, p2p consumer not starting");
        return;
    }

    let queue_topic = ctx.config.p2p.topic.clone();
    let alive_topic = "ALIVE".to_string();

    info!(
        "Starting P2P consumer — exchange={}, topics=[{}, {}]",
        ctx.config.rabbitmq.sub_exchange, queue_topic, alive_topic
    );

    loop {
        match run_consumer_loop(&ctx, &queue_topic, &alive_topic).await {
            Ok(()) => {
                // Clean exit (shouldn't happen normally)
                warn!("P2P consumer loop exited cleanly, restarting...");
            }
            Err(e) => {
                error!("P2P consumer error: {}", e);
            }
        }

        info!(
            "Reconnecting to RabbitMQ in {}s...",
            RECONNECT_DELAY_SECS
        );
        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}

/// Inner consumer loop — connects, declares queues, and consumes.
async fn run_consumer_loop(
    ctx: &P2pConsumerContext,
    queue_topic: &str,
    alive_topic: &str,
) -> anyhow::Result<()> {
    let conn = Connection::connect(
        &ctx.config.rabbitmq.url,
        ConnectionProperties::default(),
    )
    .await?;
    info!("Connected to RabbitMQ: {}", ctx.config.rabbitmq.url);

    let channel = conn.create_channel().await?;
    channel
        .basic_qos(ctx.config.rabbitmq.prefetch_count, BasicQosOptions::default())
        .await?;

    // Declare the subscribe exchange (should already exist from p2p-service, but
    // declare idempotently to be safe)
    channel
        .exchange_declare(
            &ctx.config.rabbitmq.sub_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    // --- Message queue (ALEPH-TEST topic) ---
    let msg_queue_name = format!("aleph-rs.messages.{}", queue_topic);
    channel
        .queue_declare(
            &msg_queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    // Routing key: p2p.ALEPH-TEST.* (all peer IDs)
    let msg_routing_key = format!("p2p.{}.*", queue_topic);
    channel
        .queue_bind(
            &msg_queue_name,
            &ctx.config.rabbitmq.sub_exchange,
            &msg_routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!(
        "Bound queue {} to {}/{}",
        msg_queue_name, ctx.config.rabbitmq.sub_exchange, msg_routing_key
    );

    // --- Alive queue (ALIVE topic) ---
    let alive_queue_name = format!("aleph-rs.alive.{}", alive_topic);
    channel
        .queue_declare(
            &alive_queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let alive_routing_key = format!("p2p.{}.*", alive_topic);
    channel
        .queue_bind(
            &alive_queue_name,
            &ctx.config.rabbitmq.sub_exchange,
            &alive_routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    info!(
        "Bound queue {} to {}/{}",
        alive_queue_name, ctx.config.rabbitmq.sub_exchange, alive_routing_key
    );

    // Start consuming both queues
    let msg_consumer = channel
        .basic_consume(
            &msg_queue_name,
            "aleph-rs-msg",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let alive_consumer = channel
        .basic_consume(
            &alive_queue_name,
            "aleph-rs-alive",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    info!("P2P consumers started, waiting for messages...");

    // Process both streams concurrently
    tokio::select! {
        _ = process_message_stream(ctx, msg_consumer) => {
            warn!("Message consumer stream ended");
        }
        _ = process_alive_stream(ctx, alive_consumer) => {
            warn!("Alive consumer stream ended");
        }
    }

    Ok(())
}

/// Process the main message stream (ALEPH-TEST topic)
async fn process_message_stream(
    ctx: &P2pConsumerContext,
    mut consumer: Consumer,
) {
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let routing_key = delivery.routing_key.as_str().to_string();

                // Messages from p2p-service are URL-encoded JSON
                let raw = match std::str::from_utf8(&delivery.data) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        warn!("Non-UTF8 message on {}: {}", routing_key, e);
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                        continue;
                    }
                };

                // URL-decode
                let decoded = urlencoding::decode(&raw)
                    .unwrap_or_else(|_| raw.clone().into());

                // Parse as Aleph Message
                match serde_json::from_str::<Message>(&decoded) {
                    Ok(message) => {
                        // Dedup check
                        let dedup_key = (
                            message.sender.clone(),
                            message.item_hash.clone(),
                            message.signature.clone(),
                        );

                        {
                            let mut cache = ctx.dedup_cache.lock().await;
                            if cache.get(&dedup_key).is_some() {
                                debug!(
                                    "Duplicate message skipped: {}",
                                    message.item_hash
                                );
                                let _ =
                                    delivery.ack(BasicAckOptions::default()).await;
                                continue;
                            }
                            cache.put(dedup_key, ());
                        }

                        // Insert into pending_messages
                        if let Err(e) = insert_pending_message(&ctx.db, &message).await
                        {
                            // ON CONFLICT is handled — duplicates are fine
                            debug!(
                                "Insert pending {} result: {}",
                                message.item_hash, e
                            );
                        } else {
                            debug!(
                                "Queued pending message: {} (type={}, sender={})",
                                message.item_hash, message.message_type, message.sender
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Failed to parse message on {}: {} (first 200 chars: {})",
                            routing_key,
                            e,
                            &decoded[..decoded.len().min(200)]
                        );
                    }
                }

                // Always ack to avoid redelivery storms
                if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                    warn!("Failed to ack message: {}", e);
                }
            }
            Err(e) => {
                error!("Message delivery error: {}", e);
                return;
            }
        }
    }
}

/// Process the alive stream (ALIVE topic) — peer discovery
async fn process_alive_stream(
    ctx: &P2pConsumerContext,
    mut consumer: Consumer,
) {
    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                let raw = match std::str::from_utf8(&delivery.data) {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        let _ = delivery.ack(BasicAckOptions::default()).await;
                        continue;
                    }
                };

                let decoded = urlencoding::decode(&raw)
                    .unwrap_or_else(|_| raw.clone().into());

                match serde_json::from_str::<AliveMessage>(&decoded) {
                    Ok(alive) => {
                        if alive.peer_type == "HTTP" || alive.peer_type == "P2P" {
                            // Store API server URL in Redis
                            if let Some(ref redis) = ctx.redis_client {
                                let mut conn = redis.clone();
                                let result: Result<(), redis::RedisError> =
                                    redis::cmd("SADD")
                                        .arg("api_servers")
                                        .arg(&alive.address)
                                        .query_async(&mut conn)
                                        .await;

                                match result {
                                    Ok(()) => {
                                        debug!(
                                            "Added api_server: {} (type={})",
                                            alive.address, alive.peer_type
                                        );
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to SADD api_server {}: {}",
                                            alive.address, e
                                        );
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to parse alive message: {}", e);
                    }
                }

                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
            Err(e) => {
                error!("Alive delivery error: {}", e);
                return;
            }
        }
    }
}

/// Insert a message into the pending_messages table
async fn insert_pending_message(pool: &PgPool, message: &Message) -> Result<(), sqlx::Error> {
    let message_json = serde_json::to_value(message).unwrap_or_default();
    let now = chrono::Utc::now().timestamp() as f64;

    sqlx::query(
        r#"
        INSERT INTO pending_messages (item_hash, message_data, reception_time, fetched, check_message, retries, next_attempt, trusted_source)
        VALUES ($1, $2, to_timestamp($3), $4, $5, $6, to_timestamp($7), $8)
        ON CONFLICT (item_hash) DO NOTHING
        "#,
    )
    .bind(&message.item_hash)
    .bind(&message_json)
    .bind(now)
    .bind(message.item_type == crate::types::ItemType::Inline) // inline = already fetched
    .bind(true) // check_message
    .bind(0i32) // retries
    .bind(now) // next_attempt = now (process immediately)
    .bind(false) // not trusted (came from p2p)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alive_message() {
        let json = r#"{"address":"http://1.2.3.4:4024","peer_type":"HTTP"}"#;
        let alive: AliveMessage = serde_json::from_str(json).unwrap();
        assert_eq!(alive.address, "http://1.2.3.4:4024");
        assert_eq!(alive.peer_type, "HTTP");
    }

    #[test]
    fn test_url_decode_message() {
        // Simulates a URL-encoded JSON message from p2p-service
        let encoded = "%7B%22type%22%3A%22POST%22%2C%22chain%22%3A%22ETH%22%7D";
        let decoded = urlencoding::decode(encoded).unwrap();
        assert!(decoded.contains("POST"));
        assert!(decoded.contains("ETH"));
    }

    #[test]
    fn test_dedup_cache_capacity() {
        let cache: LruCache<DedupKey, ()> = LruCache::new(
            std::num::NonZeroUsize::new(DEDUP_CACHE_SIZE).unwrap(),
        );
        assert_eq!(cache.cap().get(), DEDUP_CACHE_SIZE);
    }
}
