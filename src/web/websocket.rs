//! WebSocket support for real-time subscriptions
//!
//! Allows clients to subscribe to messages in real-time with filtering.
//! Integrates with RabbitMQ for live message updates.
//!
//! Reference: aleph/web/controllers/websocket.py

use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State, Query,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::{broadcast, RwLock, mpsc};
use tracing::{debug, info, warn};

use crate::types::Message;
use crate::network::rabbitmq::P2PMessage;

/// WebSocket query parameters
#[derive(Debug, Deserialize)]
pub struct WsQuery {
    /// Subscribe to specific addresses
    #[serde(default)]
    pub addresses: Option<String>,
    /// Subscribe to specific channels
    #[serde(default)]
    pub channels: Option<String>,
    /// Subscribe to specific message types
    #[serde(default, rename = "msgTypes")]
    pub message_types: Option<String>,
    /// Subscribe to specific item hashes
    #[serde(default)]
    pub hashes: Option<String>,
    /// Request message history (last N messages)
    #[serde(default)]
    pub history: Option<u32>,
}

/// Subscription request from client
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
        /// Filter by item hashes
        #[serde(default)]
        hashes: Vec<String>,
    },
    /// Update subscription filters
    Update {
        #[serde(default)]
        add_addresses: Vec<String>,
        #[serde(default)]
        remove_addresses: Vec<String>,
        #[serde(default)]
        add_channels: Vec<String>,
        #[serde(default)]
        remove_channels: Vec<String>,
    },
    /// Unsubscribe
    Unsubscribe,
    /// Ping
    Ping { nonce: u64 },
    /// Request history
    History {
        limit: u32,
        #[serde(default)]
        before: Option<f64>,
    },
}

/// Subscription response to client
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionResponse {
    /// Subscription confirmed
    Subscribed {
        subscription_id: String,
        filter_count: usize,
    },
    /// Subscription updated
    Updated {
        filter_count: usize,
    },
    /// Unsubscribed
    Unsubscribed,
    /// Pong response
    Pong { nonce: u64 },
    /// New message
    Message { 
        message: Message,
        #[serde(skip_serializing_if = "Option::is_none")]
        confirmation: Option<MessageConfirmation>,
    },
    /// History messages
    History {
        messages: Vec<Message>,
        has_more: bool,
    },
    /// Error
    Error { code: u32, message: String },
}

/// Message confirmation info
#[derive(Debug, Clone, Serialize)]
pub struct MessageConfirmation {
    pub chain: String,
    pub tx_hash: String,
    pub height: u64,
}

/// Subscription filter
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
    pub addresses: HashSet<String>,
    pub channels: HashSet<String>,
    pub message_types: HashSet<String>,
    pub hashes: HashSet<String>,
}

impl SubscriptionFilter {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn from_query(query: &WsQuery) -> Self {
        let mut filter = Self::new();
        
        if let Some(ref addrs) = query.addresses {
            for addr in addrs.split(',') {
                filter.addresses.insert(addr.trim().to_string());
            }
        }
        
        if let Some(ref channels) = query.channels {
            for ch in channels.split(',') {
                filter.channels.insert(ch.trim().to_string());
            }
        }
        
        if let Some(ref types) = query.message_types {
            for t in types.split(',') {
                filter.message_types.insert(t.trim().to_uppercase());
            }
        }
        
        if let Some(ref hashes) = query.hashes {
            for h in hashes.split(',') {
                filter.hashes.insert(h.trim().to_string());
            }
        }
        
        filter
    }
    
    pub fn filter_count(&self) -> usize {
        self.addresses.len() + self.channels.len() + 
        self.message_types.len() + self.hashes.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty() && 
        self.channels.is_empty() && 
        self.message_types.is_empty() &&
        self.hashes.is_empty()
    }
    
    pub fn matches(&self, message: &Message) -> bool {
        // If no filters, match everything
        if self.is_empty() {
            return true;
        }
        
        // Check hash filter (highest priority)
        if !self.hashes.is_empty() {
            if self.hashes.contains(&message.item_hash) {
                return true;
            }
        }
        
        // Check address filter
        if !self.addresses.is_empty() {
            if !self.addresses.contains(&message.sender) {
                return false;
            }
        }
        
        // Check channel filter
        if !self.channels.is_empty() {
            match &message.channel {
                Some(ch) if self.channels.contains(ch) => {}
                Some(_) => return false,
                None => {
                    // If filtering by channel and message has no channel, don't match
                    return false;
                }
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
    
    pub fn update(&mut self, request: &SubscriptionRequest) {
        if let SubscriptionRequest::Update {
            add_addresses,
            remove_addresses,
            add_channels,
            remove_channels,
        } = request {
            for addr in add_addresses {
                self.addresses.insert(addr.clone());
            }
            for addr in remove_addresses {
                self.addresses.remove(addr);
            }
            for ch in add_channels {
                self.channels.insert(ch.clone());
            }
            for ch in remove_channels {
                self.channels.remove(ch);
            }
        }
    }
}

/// WebSocket state for managing subscriptions
#[derive(Debug)]
pub struct WsState {
    /// Broadcast channel for new messages
    pub message_tx: broadcast::Sender<Message>,
    /// Database pool for history queries
    pub db: Option<sqlx::PgPool>,
    /// Connected client count
    pub client_count: Arc<RwLock<u64>>,
}

impl WsState {
    pub fn new() -> Self {
        let (message_tx, _) = broadcast::channel(10000); // Large buffer for bursts
        Self { 
            message_tx, 
            db: None,
            client_count: Arc::new(RwLock::new(0)),
        }
    }
    
    pub fn with_db(mut self, db: sqlx::PgPool) -> Self {
        self.db = Some(db);
        self
    }
    
    /// Broadcast a message to all subscribers
    pub fn broadcast(&self, message: Message) {
        let _ = self.message_tx.send(message);
    }
    
    /// Get connected client count
    pub async fn connected_clients(&self) -> u64 {
        *self.client_count.read().await
    }
    
    /// Increment client count
    async fn client_connected(&self) {
        let mut count = self.client_count.write().await;
        *count += 1;
    }
    
    /// Decrement client count
    async fn client_disconnected(&self) {
        let mut count = self.client_count.write().await;
        *count = count.saturating_sub(1);
    }
}

impl Default for WsState {
    fn default() -> Self {
        Self::new()
    }
}

/// WebSocket upgrade handler with query parameters
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(app_state): State<Arc<crate::web::state::AppState>>,
    Query(query): Query<WsQuery>,
) -> impl IntoResponse {
    let ws_state = app_state.ws_state.clone();
    ws.on_upgrade(move |socket| handle_socket(socket, ws_state, query))
}

/// Handle a WebSocket connection
async fn handle_socket(socket: WebSocket, ws_state: Arc<WsState>, query: WsQuery) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(RwLock::new(sender));
    
    // Create subscription state from query
    let filter = SubscriptionFilter::from_query(&query);
    let subscribed = !filter.is_empty();
    let mut message_rx = ws_state.message_tx.subscribe();
    
    // Update client count
    ws_state.client_connected().await;
    
    let subscription_id = uuid::Uuid::new_v4().to_string();
    info!("WebSocket client connected: {}", subscription_id);
    
    // Send initial subscription confirmation if filters provided
    if subscribed {
        let response = SubscriptionResponse::Subscribed {
            subscription_id: subscription_id.clone(),
            filter_count: filter.filter_count(),
        };
        send_response(&sender, &response).await;
        
        // Send history if requested
        if let Some(limit) = query.history {
            if let Some(ref db) = ws_state.db {
                if let Ok(messages) = get_message_history(db, &filter, limit, None).await {
                    let response = SubscriptionResponse::History {
                        messages,
                        has_more: false,
                    };
                    send_response(&sender, &response).await;
                }
            }
        }
    }
    
    // Clone sender for message forwarding task
    let sender_clone = sender.clone();
    let filter_arc = Arc::new(RwLock::new(filter.clone()));
    let subscribed_arc = Arc::new(RwLock::new(subscribed));
    
    // Spawn message forwarding task
    let filter_for_forward = filter_arc.clone();
    let subscribed_for_forward = subscribed_arc.clone();
    let forward_task = tokio::spawn(async move {
        loop {
            match message_rx.recv().await {
                Ok(message) => {
                    let subscribed = *subscribed_for_forward.read().await;
                    if !subscribed {
                        continue;
                    }
                    
                    let filter = filter_for_forward.read().await;
                    if filter.matches(&message) {
                        let response = SubscriptionResponse::Message { 
                            message,
                            confirmation: None,
                        };
                        if !send_response(&sender_clone, &response).await {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("WebSocket client lagged {} messages", n);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });
    
    // Handle incoming messages from client
    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(WsMessage::Text(text)) => {
                match serde_json::from_str::<SubscriptionRequest>(&text) {
                    Ok(req) => {
                        let response = handle_request(
                            req, 
                            &filter_arc, 
                            &subscribed_arc,
                            &subscription_id,
                            ws_state.db.as_ref(),
                        ).await;
                        send_response(&sender, &response).await;
                    }
                    Err(e) => {
                        let response = SubscriptionResponse::Error {
                            code: 1,
                            message: format!("Invalid request: {}", e),
                        };
                        send_response(&sender, &response).await;
                    }
                }
            }
            Ok(WsMessage::Close(_)) => {
                info!("WebSocket client disconnected: {}", subscription_id);
                break;
            }
            Ok(WsMessage::Ping(data)) => {
                let mut sender_guard = sender.write().await;
                let _ = sender_guard.send(WsMessage::Pong(data)).await;
            }
            Err(e) => {
                warn!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }
    
    // Cleanup
    forward_task.abort();
    ws_state.client_disconnected().await;
}

/// Send a response to the client
async fn send_response(
    sender: &Arc<RwLock<SplitSink<WebSocket, WsMessage>>>,
    response: &SubscriptionResponse,
) -> bool {
    let json = match serde_json::to_string(response) {
        Ok(j) => j,
        Err(_) => return false,
    };
    
    let mut sender_guard = sender.write().await;
    sender_guard.send(WsMessage::Text(json)).await.is_ok()
}

/// Handle a subscription request
async fn handle_request(
    request: SubscriptionRequest,
    filter: &Arc<RwLock<SubscriptionFilter>>,
    subscribed: &Arc<RwLock<bool>>,
    subscription_id: &str,
    db: Option<&sqlx::PgPool>,
) -> SubscriptionResponse {
    match request {
        SubscriptionRequest::Subscribe {
            addresses,
            channels,
            message_types,
            hashes,
        } => {
            let mut f = filter.write().await;
            *f = SubscriptionFilter {
                addresses: addresses.into_iter().collect(),
                channels: channels.into_iter().collect(),
                message_types: message_types.into_iter().map(|t| t.to_uppercase()).collect(),
                hashes: hashes.into_iter().collect(),
            };
            
            *subscribed.write().await = true;
            
            debug!("Client {} subscribed with {} filters", subscription_id, f.filter_count());
            
            SubscriptionResponse::Subscribed {
                subscription_id: subscription_id.to_string(),
                filter_count: f.filter_count(),
            }
        }
        SubscriptionRequest::Update { .. } => {
            let mut f = filter.write().await;
            f.update(&request);
            
            SubscriptionResponse::Updated {
                filter_count: f.filter_count(),
            }
        }
        SubscriptionRequest::Unsubscribe => {
            *subscribed.write().await = false;
            *filter.write().await = SubscriptionFilter::default();
            
            SubscriptionResponse::Unsubscribed
        }
        SubscriptionRequest::Ping { nonce } => {
            SubscriptionResponse::Pong { nonce }
        }
        SubscriptionRequest::History { limit, before } => {
            if let Some(db) = db {
                let f = filter.read().await;
                match get_message_history(db, &f, limit.min(1000), before).await {
                    Ok(messages) => {
                        let has_more = messages.len() >= limit as usize;
                        SubscriptionResponse::History { messages, has_more }
                    }
                    Err(e) => {
                        SubscriptionResponse::Error {
                            code: 2,
                            message: format!("History query failed: {}", e),
                        }
                    }
                }
            } else {
                SubscriptionResponse::Error {
                    code: 3,
                    message: "Database not available".to_string(),
                }
            }
        }
    }
}

/// Get message history from database
async fn get_message_history(
    db: &sqlx::PgPool,
    filter: &SubscriptionFilter,
    limit: u32,
    before: Option<f64>,
) -> Result<Vec<Message>, String> {
    let mut query = String::from("SELECT * FROM messages WHERE 1=1");
    
    // Apply filters
    if !filter.addresses.is_empty() {
        let addrs: Vec<String> = filter.addresses.iter().map(|a| format!("'{}'", a)).collect();
        query.push_str(&format!(" AND sender IN ({})", addrs.join(",")));
    }
    
    if !filter.channels.is_empty() {
        let channels: Vec<String> = filter.channels.iter().map(|c| format!("'{}'", c)).collect();
        query.push_str(&format!(" AND channel IN ({})", channels.join(",")));
    }
    
    if !filter.message_types.is_empty() {
        let types: Vec<String> = filter.message_types.iter().map(|t| format!("'{}'", t)).collect();
        query.push_str(&format!(" AND message_type IN ({})", types.join(",")));
    }
    
    if !filter.hashes.is_empty() {
        let hashes: Vec<String> = filter.hashes.iter().map(|h| format!("'{}'", h)).collect();
        query.push_str(&format!(" AND item_hash IN ({})", hashes.join(",")));
    }
    
    // Apply before filter
    if let Some(t) = before {
        query.push_str(&format!(" AND time < {}", t));
    }
    
    // Order and limit
    query.push_str(&format!(" ORDER BY time DESC LIMIT {}", limit));
    
    // Execute query and convert to Message
    let rows: Vec<crate::db::models::MessageDb> = sqlx::query_as(&query)
        .fetch_all(db)
        .await
        .map_err(|e| e.to_string())?;
    
    // Convert to Message type
    let messages: Vec<Message> = rows.into_iter()
        .filter_map(|row| {
            serde_json::from_str(&serde_json::json!({
                "type": row.message_type,
                "chain": row.chain,
                "sender": row.sender,
                "signature": row.signature,
                "item_type": row.item_type,
                "item_hash": row.item_hash,
                "item_content": row.item_content,
                "channel": row.channel,
                "time": row.time,
            }).to_string()).ok()
        })
        .collect();
    
    Ok(messages)
}

/// WebSocket upgrade handler for /api/ws0/status
/// Streams node status metrics (message counts) to clients, sending updates only when values change.
pub async fn status_ws_handler(
    State(state): State<Arc<crate::web::state::AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| status_ws(socket, state))
}

/// Handle a status WebSocket connection
async fn status_ws(mut socket: WebSocket, state: Arc<crate::web::state::AppState>) {
    let mut previous_json: Option<String> = None;

    loop {
        // Build status metrics
        let status = if state.has_db() {
            // Query message counts using pg_class estimates (fast, no seq scan)
            let messages: i64 = sqlx::query_scalar(
                "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'messages'"
            )
            .fetch_one(state.db())
            .await
            .unwrap_or(0);

            let pending: i64 = sqlx::query_scalar(
                "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'pending_messages'"
            )
            .fetch_one(state.db())
            .await
            .unwrap_or(0);

            let files: i64 = sqlx::query_scalar(
                "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'file_pins'"
            )
            .fetch_one(state.db())
            .await
            .unwrap_or(0);

            serde_json::json!({
                "pyaleph_status_sync_messages_total": messages,
                "pyaleph_status_sync_pending_messages_total": pending,
                "pyaleph_status_sync_permanent_files_total": files,
            })
        } else {
            serde_json::json!({})
        };

        let json_str = status.to_string();

        // Only send if changed
        if previous_json.as_ref() != Some(&json_str) {
            if socket.send(WsMessage::Text(json_str.clone().into())).await.is_err() {
                break;
            }
            previous_json = Some(json_str);
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Connect WebSocket to RabbitMQ for live updates
pub async fn connect_to_rabbitmq(
    ws_state: Arc<WsState>,
    mut rabbitmq_rx: mpsc::Receiver<P2PMessage>,
) {
    info!("Connecting WebSocket to RabbitMQ message stream");
    
    while let Some(p2p_msg) = rabbitmq_rx.recv().await {
        // Parse the P2P message content into an Aleph message
        if let Ok(message) = serde_json::from_str::<Message>(&p2p_msg.content) {
            // Broadcast to WebSocket subscribers
            ws_state.broadcast(message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Chain, ItemType, MessageType};
    
    #[test]
    fn test_filter_matches_all() {
        let filter = SubscriptionFilter::default();
        let message = create_test_message();
        
        // Empty filter matches everything
        assert!(filter.matches(&message));
    }
    
    #[test]
    fn test_filter_by_address() {
        let mut filter = SubscriptionFilter::default();
        filter.addresses.insert("0x1234".to_string());
        
        let mut message = create_test_message();
        message.sender = "0x1234".to_string();
        assert!(filter.matches(&message));
        
        message.sender = "0x5678".to_string();
        assert!(!filter.matches(&message));
    }
    
    #[test]
    fn test_filter_by_channel() {
        let mut filter = SubscriptionFilter::default();
        filter.channels.insert("test-channel".to_string());
        
        let mut message = create_test_message();
        message.channel = Some("test-channel".to_string());
        assert!(filter.matches(&message));
        
        message.channel = Some("other-channel".to_string());
        assert!(!filter.matches(&message));
        
        message.channel = None;
        assert!(!filter.matches(&message));
    }
    
    #[test]
    fn test_filter_by_type() {
        let mut filter = SubscriptionFilter::default();
        filter.message_types.insert("POST".to_string());
        
        let mut message = create_test_message();
        message.message_type = MessageType::Post;
        assert!(filter.matches(&message));
        
        message.message_type = MessageType::Aggregate;
        assert!(!filter.matches(&message));
    }
    
    fn create_test_message() -> Message {
        Message {
            message_type: MessageType::Post,
            chain: Chain::ETH,
            sender: "0x1234".to_string(),
            signature: "0xsig".to_string(),
            item_type: ItemType::Inline,
            item_hash: "abc123".to_string(),
            item_content: Some("{}".to_string()),
            channel: Some("test".to_string()),
            time: 1234567890.0,
        }
    }
}
