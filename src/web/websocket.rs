//! WebSocket support for real-time subscriptions
//!
//! Allows clients to subscribe to messages in real-time.

use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::types::Message;

/// Subscription request
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionRequest {
    /// Subscribe to messages
    Subscribe {
        /// Filter by addresses
        #[serde(default)]
        addresses: Vec<String>,
        /// Filter by channels
        #[serde(default)]
        channels: Vec<String>,
        /// Filter by message types
        #[serde(default)]
        message_types: Vec<String>,
    },
    /// Unsubscribe
    Unsubscribe,
    /// Ping
    Ping { nonce: u64 },
}

/// Subscription response
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionResponse {
    /// Subscription confirmed
    Subscribed { subscription_id: String },
    /// Unsubscribed
    Unsubscribed,
    /// Pong response
    Pong { nonce: u64 },
    /// New message
    Message { message: Message },
    /// Error
    Error { code: u32, message: String },
}

/// Subscription filter
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
    pub addresses: Vec<String>,
    pub channels: Vec<String>,
    pub message_types: Vec<String>,
}

impl SubscriptionFilter {
    pub fn matches(&self, message: &Message) -> bool {
        // If no filters, match everything
        if self.addresses.is_empty() && self.channels.is_empty() && self.message_types.is_empty() {
            return true;
        }
        
        // Check address filter
        if !self.addresses.is_empty() && !self.addresses.contains(&message.sender) {
            return false;
        }
        
        // Check channel filter
        if !self.channels.is_empty() {
            match &message.channel {
                Some(ch) if self.channels.contains(ch) => {}
                _ => return false,
            }
        }
        
        // Check message type filter
        if !self.message_types.is_empty() {
            let msg_type = message.message_type.to_string();
            if !self.message_types.contains(&msg_type) {
                return false;
            }
        }
        
        true
    }
}

/// WebSocket state for managing subscriptions
pub struct WsState {
    pub message_tx: broadcast::Sender<Message>,
}

impl WsState {
    pub fn new() -> Self {
        let (message_tx, _) = broadcast::channel(1000);
        Self { message_tx }
    }
    
    /// Broadcast a message to all subscribers
    pub fn broadcast(&self, message: Message) {
        let _ = self.message_tx.send(message);
    }
}

impl Default for WsState {
    fn default() -> Self {
        Self::new()
    }
}

/// WebSocket upgrade handler
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(ws_state): State<Arc<WsState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, ws_state))
}

/// Handle a WebSocket connection
async fn handle_socket(socket: WebSocket, ws_state: Arc<WsState>) {
    let (mut sender, mut receiver) = socket.split();
    
    // Create subscription state
    let mut filter = SubscriptionFilter::default();
    let mut subscribed = false;
    let mut message_rx = ws_state.message_tx.subscribe();
    
    info!("WebSocket client connected");
    
    loop {
        tokio::select! {
            // Handle incoming messages from client
            msg = receiver.next() => {
                match msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        match serde_json::from_str::<SubscriptionRequest>(&text) {
                            Ok(req) => {
                                let response = handle_request(req, &mut filter, &mut subscribed);
                                let json = serde_json::to_string(&response).unwrap();
                                if sender.send(WsMessage::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let response = SubscriptionResponse::Error {
                                    code: 1,
                                    message: format!("Invalid request: {}", e),
                                };
                                let json = serde_json::to_string(&response).unwrap();
                                if sender.send(WsMessage::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) => {
                        info!("WebSocket client disconnected");
                        break;
                    }
                    Some(Ok(WsMessage::Ping(data))) => {
                        if sender.send(WsMessage::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!("WebSocket error: {}", e);
                        break;
                    }
                    None => break,
                    _ => {}
                }
            }
            
            // Forward messages to subscribed clients
            msg = message_rx.recv() => {
                if !subscribed {
                    continue;
                }
                
                match msg {
                    Ok(message) => {
                        if filter.matches(&message) {
                            let response = SubscriptionResponse::Message { message };
                            let json = serde_json::to_string(&response).unwrap();
                            if sender.send(WsMessage::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        // Channel lagged, reconnect
                        message_rx = ws_state.message_tx.subscribe();
                    }
                }
            }
        }
    }
}

/// Handle a subscription request
fn handle_request(
    request: SubscriptionRequest,
    filter: &mut SubscriptionFilter,
    subscribed: &mut bool,
) -> SubscriptionResponse {
    match request {
        SubscriptionRequest::Subscribe {
            addresses,
            channels,
            message_types,
        } => {
            *filter = SubscriptionFilter {
                addresses,
                channels,
                message_types,
            };
            *subscribed = true;
            
            let subscription_id = uuid::Uuid::new_v4().to_string();
            debug!("Client subscribed with filter: {:?}", filter);
            
            SubscriptionResponse::Subscribed { subscription_id }
        }
        SubscriptionRequest::Unsubscribe => {
            *subscribed = false;
            *filter = SubscriptionFilter::default();
            
            SubscriptionResponse::Unsubscribed
        }
        SubscriptionRequest::Ping { nonce } => {
            SubscriptionResponse::Pong { nonce }
        }
    }
}
