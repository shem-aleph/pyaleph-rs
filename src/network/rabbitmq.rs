//! RabbitMQ integration for p2p-service connectivity
//!
//! Connects to the Aleph p2p-service via RabbitMQ message queue.

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

/// RabbitMQ configuration
#[derive(Debug, Clone)]
pub struct RabbitMQConfig {
    pub url: String,
    pub exchange: String,
    pub queue_incoming: String,
    pub queue_outgoing: String,
    pub routing_key: String,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            url: "amqp://localhost:5672".to_string(),
            exchange: "aleph-p2p".to_string(),
            queue_incoming: "aleph-incoming".to_string(),
            queue_outgoing: "aleph-outgoing".to_string(),
            routing_key: "messages".to_string(),
        }
    }
}

/// P2P message from RabbitMQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PMessage {
    /// Message content (serialized Aleph message)
    pub content: String,
    /// Source peer ID
    pub from_peer: Option<String>,
    /// Topic
    pub topic: String,
}

/// RabbitMQ service for P2P communication
pub struct RabbitMQService {
    config: RabbitMQConfig,
    connection: Option<Connection>,
    channel: Option<Channel>,
    message_tx: mpsc::Sender<P2PMessage>,
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
    
    /// Connect to RabbitMQ
    pub async fn connect(&mut self) -> Result<(), lapin::Error> {
        info!("Connecting to RabbitMQ: {}", self.config.url);
        
        let connection = Connection::connect(
            &self.config.url,
            ConnectionProperties::default(),
        ).await?;
        
        let channel = connection.create_channel().await?;
        
        // Declare exchange
        channel.exchange_declare(
            &self.config.exchange,
            lapin::ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ).await?;
        
        // Declare incoming queue
        channel.queue_declare(
            &self.config.queue_incoming,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        ).await?;
        
        // Bind queue to exchange
        channel.queue_bind(
            &self.config.queue_incoming,
            &self.config.exchange,
            &self.config.routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await?;
        
        self.connection = Some(connection);
        self.channel = Some(channel);
        
        info!("Connected to RabbitMQ successfully");
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
    
    /// Publish a message to the network
    pub async fn publish(&self, message: &Message) -> Result<(), lapin::Error> {
        let channel = self.channel.as_ref()
            .ok_or_else(|| lapin::Error::InvalidChannel(0))?;
        
        let p2p_msg = P2PMessage {
            content: serde_json::to_string(message).unwrap_or_default(),
            from_peer: None,
            topic: "aleph-messages".to_string(),
        };
        
        let payload = serde_json::to_vec(&p2p_msg).unwrap_or_default();
        
        channel.basic_publish(
            &self.config.exchange,
            &self.config.routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        ).await?;
        
        debug!("Published message: {}", message.item_hash);
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
pub async fn run_consumer(
    config: RabbitMQConfig,
    message_tx: mpsc::Sender<P2PMessage>,
) {
    let mut service = RabbitMQService::new(config.clone());
    
    loop {
        // Try to connect
        if let Err(e) = service.connect().await {
            error!("Failed to connect to RabbitMQ: {}", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            continue;
        }
        
        // Start consuming
        let consumer = match service.start_consuming().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to start consumer: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
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
                            debug!("Received P2P message from {:?}", msg.from_peer);
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
                    error!("Consumer error: {}", e);
                    break;
                }
            }
        }
        
        // Connection lost, retry
        warn!("RabbitMQ connection lost, reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
