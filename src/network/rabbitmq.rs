//! RabbitMQ integration for p2p-service connectivity
//!
//! Connects to the Aleph p2p-service via RabbitMQ message queue.
//! Exchange names and routing follow the pyaleph configuration for compatibility.
//!
//! Reference: aleph/config.py default rabbitmq settings

use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection,
    ConnectionProperties, Consumer,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::types::Message;

/// RabbitMQ configuration matching pyaleph defaults
/// 
/// These exchange names MUST match the p2p-service configuration
/// for proper network communication.
#[derive(Debug, Clone)]
pub struct RabbitMQConfig {
    /// RabbitMQ connection URL
    pub url: String,
    
    /// Exchange for publishing messages to the p2p network
    /// Default: "p2p-publish" (matches pyaleph)
    pub pub_exchange: String,
    
    /// Exchange for receiving messages from the p2p network
    /// Default: "p2p-subscribe" (matches pyaleph)
    pub sub_exchange: String,
    
    /// Exchange for processed messages
    /// Default: "aleph-messages" (matches pyaleph)
    pub message_exchange: String,
    
    /// Exchange for pending messages awaiting processing
    /// Default: "aleph-pending-messages" (matches pyaleph)
    pub pending_message_exchange: String,
    
    /// Exchange for pending blockchain transactions
    /// Default: "aleph-pending-txs" (matches pyaleph)
    pub pending_tx_exchange: String,
    
    /// Queue name for incoming messages
    pub queue_incoming: String,
    
    /// Queue name for outgoing messages
    pub queue_outgoing: String,
    
    /// Routing key for messages
    pub routing_key: String,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        // These defaults match pyaleph config.py
        Self {
            url: "amqp://localhost:5672".to_string(),
            pub_exchange: "p2p-publish".to_string(),      // CRITICAL: Must match p2p-service
            sub_exchange: "p2p-subscribe".to_string(),    // CRITICAL: Must match p2p-service
            message_exchange: "aleph-messages".to_string(),
            pending_message_exchange: "aleph-pending-messages".to_string(),
            pending_tx_exchange: "aleph-pending-txs".to_string(),
            queue_incoming: "aleph-incoming".to_string(),
            queue_outgoing: "aleph-outgoing".to_string(),
            routing_key: "#".to_string(), // Subscribe to all topics
        }
    }
}

/// P2P message envelope from RabbitMQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PMessage {
    /// Message content (serialized Aleph message)
    pub content: String,
    /// Source peer ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_peer: Option<String>,
    /// Topic
    pub topic: String,
}

/// Pending message for processing queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingMessageEnvelope {
    /// The message to process
    pub message: Message,
    /// When the message was received
    pub reception_time: f64,
    /// Number of processing attempts
    pub retries: u32,
    /// Next attempt timestamp
    pub next_attempt: f64,
    /// Whether content has been fetched (for IPFS/storage messages)
    pub fetched: bool,
}

/// RabbitMQ service for P2P communication
/// 
/// The message channels are used for async message passing when P2P
/// federation is enabled. Currently used for WebSocket real-time updates.
#[derive(Debug)]
pub struct RabbitMQService {
    config: RabbitMQConfig,
    connection: Option<Connection>,
    channel: Option<Channel>,
    /// Sender for publishing messages to the P2P network
    #[allow(dead_code)]
    message_tx: mpsc::Sender<P2PMessage>,
    /// Receiver for incoming P2P messages (consumed by message processor)
    #[allow(dead_code)]
    message_rx: mpsc::Receiver<P2PMessage>,
}

impl RabbitMQService {
    /// Create a new RabbitMQ service
    pub fn new(config: RabbitMQConfig) -> Self {
        let (message_tx, message_rx) = mpsc::channel(1000);
        
        Self {
            config,
            connection: None,
            channel: None,
            message_tx,
            message_rx,
        }
    }
    
    /// Connect to RabbitMQ and set up exchanges
    pub async fn connect(&mut self) -> Result<(), lapin::Error> {
        info!("Connecting to RabbitMQ: {}", self.config.url);
        
        let connection = Connection::connect(
            &self.config.url,
            ConnectionProperties::default(),
        ).await?;
        
        let channel = connection.create_channel().await?;
        
        // Declare all exchanges needed for p2p-service compatibility
        self.setup_exchanges(&channel).await?;
        
        // Set up queues and bindings
        self.setup_queues(&channel).await?;
        
        self.connection = Some(connection);
        self.channel = Some(channel);
        
        info!("Connected to RabbitMQ successfully");
        Ok(())
    }
    
    /// Set up exchanges matching pyaleph configuration
    async fn setup_exchanges(&self, channel: &Channel) -> Result<(), lapin::Error> {
        // Publishing exchange (to p2p network)
        channel.exchange_declare(
            &self.config.pub_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        info!("Declared exchange: {}", self.config.pub_exchange);
        
        // Subscribe exchange (from p2p network)
        channel.exchange_declare(
            &self.config.sub_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        info!("Declared exchange: {}", self.config.sub_exchange);
        
        // Processed messages exchange
        channel.exchange_declare(
            &self.config.message_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        info!("Declared exchange: {}", self.config.message_exchange);
        
        // Pending messages exchange
        channel.exchange_declare(
            &self.config.pending_message_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        info!("Declared exchange: {}", self.config.pending_message_exchange);
        
        // Pending transactions exchange
        channel.exchange_declare(
            &self.config.pending_tx_exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        info!("Declared exchange: {}", self.config.pending_tx_exchange);
        
        Ok(())
    }
    
    /// Set up queues and bindings
    async fn setup_queues(&self, channel: &Channel) -> Result<(), lapin::Error> {
        // Incoming queue (messages from p2p network)
        channel.queue_declare(
            &self.config.queue_incoming,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        
        // Bind to p2p-subscribe exchange
        channel.queue_bind(
            &self.config.queue_incoming,
            &self.config.sub_exchange,
            &self.config.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;
        info!("Bound queue {} to exchange {}", self.config.queue_incoming, self.config.sub_exchange);
        
        // Also bind to pending messages exchange
        channel.queue_bind(
            &self.config.queue_incoming,
            &self.config.pending_message_exchange,
            &self.config.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;
        
        Ok(())
    }
    
    /// Start consuming messages
    pub async fn start_consuming(&self) -> Result<Consumer, lapin::Error> {
        let channel = self.channel.as_ref()
            .ok_or_else(|| lapin::Error::InvalidChannel(0))?;
        
        let consumer = channel.basic_consume(
            &self.config.queue_incoming,
            "aleph-core",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await?;
        
        info!("Started consuming from queue: {}", self.config.queue_incoming);
        Ok(consumer)
    }
    
    /// Publish a message to the P2P network
    pub async fn publish_to_network(&self, message: &Message) -> Result<(), lapin::Error> {
        let channel = self.channel.as_ref()
            .ok_or_else(|| lapin::Error::InvalidChannel(0))?;
        
        let p2p_msg = P2PMessage {
            content: serde_json::to_string(message).unwrap_or_default(),
            from_peer: None, // Will be filled by p2p-service
            topic: format!("messages.{}", message.message_type),
        };
        
        let payload = serde_json::to_vec(&p2p_msg).unwrap_or_default();
        let routing_key = format!("messages.{}", message.message_type.to_string().to_lowercase());
        
        channel.basic_publish(
            &self.config.pub_exchange,
            &routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // Persistent
        ).await?;
        
        debug!("Published message {} to {}", message.item_hash, self.config.pub_exchange);
        Ok(())
    }
    
    /// Publish a pending message for processing
    pub async fn publish_pending(&self, envelope: &PendingMessageEnvelope) -> Result<(), lapin::Error> {
        let channel = self.channel.as_ref()
            .ok_or_else(|| lapin::Error::InvalidChannel(0))?;
        
        let payload = serde_json::to_vec(envelope).unwrap_or_default();
        let routing_key = format!("pending.{}", envelope.message.message_type.to_string().to_lowercase());
        
        channel.basic_publish(
            &self.config.pending_message_exchange,
            &routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2),
        ).await?;
        
        debug!("Published pending message {} to {}", envelope.message.item_hash, self.config.pending_message_exchange);
        Ok(())
    }
    
    /// Notify about a processed message
    pub async fn publish_processed(&self, message: &Message) -> Result<(), lapin::Error> {
        let channel = self.channel.as_ref()
            .ok_or_else(|| lapin::Error::InvalidChannel(0))?;
        
        let payload = serde_json::to_vec(message).unwrap_or_default();
        let routing_key = format!("processed.{}", message.message_type.to_string().to_lowercase());
        
        channel.basic_publish(
            &self.config.message_exchange,
            &routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_content_type("application/json".into()),
        ).await?;
        
        debug!("Published processed message {} to {}", message.item_hash, self.config.message_exchange);
        Ok(())
    }
    
    /// Get next received message
    pub async fn next_message(&mut self) -> Option<P2PMessage> {
        self.message_rx.recv().await
    }
    
    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
    
    /// Close connection
    pub async fn close(&mut self) -> Result<(), lapin::Error> {
        if let Some(connection) = self.connection.take() {
            connection.close(0, "Shutdown").await?;
        }
        self.channel = None;
        info!("RabbitMQ connection closed");
        Ok(())
    }
}

/// Start the RabbitMQ consumer loop
///
/// On initial connection failure, logs a warning and retries every 5 minutes
/// (RabbitMQ is optional — only needed when p2p-service is running).
pub async fn run_consumer(
    config: RabbitMQConfig,
    message_tx: mpsc::Sender<P2PMessage>,
) {
    let mut service = RabbitMQService::new(config.clone());
    let mut has_connected = false;

    loop {
        // Try to connect
        if let Err(e) = service.connect().await {
            if !has_connected {
                warn!("RabbitMQ not available ({}), consumer disabled. Will retry every 5 minutes.", e);
            } else {
                warn!("RabbitMQ connection lost ({}), retrying in 5 minutes...", e);
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            continue;
        }

        if !has_connected {
            info!("RabbitMQ consumer connected successfully");
        }
        has_connected = true;

        // Start consuming
        let consumer = match service.start_consuming().await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to start RabbitMQ consumer: {}, retrying in 5 minutes", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
                continue;
            }
        };

        // Process messages
        let mut consumer = consumer;
        while let Some(delivery) = consumer.next().await {
            match delivery {
                Ok(delivery) => {
                    // Parse message
                    match serde_json::from_slice::<P2PMessage>(&delivery.data) {
                        Ok(msg) => {
                            debug!("Received P2P message from {:?} on topic {}", msg.from_peer, msg.topic);
                            if message_tx.send(msg).await.is_err() {
                                warn!("Message channel closed");
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse P2P message: {}", e);
                        }
                    }

                    // Acknowledge
                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                        warn!("Failed to ack message: {}", e);
                    }
                }
                Err(e) => {
                    warn!("Consumer error: {}", e);
                    break;
                }
            }
        }

        // Connection lost after successful connection, retry at long interval
        warn!("RabbitMQ connection lost, reconnecting in 5 minutes...");
        tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config_matches_pyaleph() {
        let config = RabbitMQConfig::default();
        
        // These must match pyaleph defaults
        assert_eq!(config.pub_exchange, "p2p-publish");
        assert_eq!(config.sub_exchange, "p2p-subscribe");
        assert_eq!(config.message_exchange, "aleph-messages");
        assert_eq!(config.pending_message_exchange, "aleph-pending-messages");
        assert_eq!(config.pending_tx_exchange, "aleph-pending-txs");
    }
}
