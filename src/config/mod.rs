//! Configuration management for Aleph Core
//!
//! Full compatibility with Python pyaleph configuration.
//! Reference: aleph/config.py

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::collections::HashMap;

/// Main configuration structure - matches pyaleph config.py
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Node configuration
    #[serde(default)]
    pub node: NodeConfig,
    
    /// Database configuration
    #[serde(default)]
    pub database: DatabaseConfig,
    
    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,
    
    /// API configuration
    #[serde(default)]
    pub api: ApiConfig,
    
    /// Chain configurations
    #[serde(default)]
    pub chains: ChainsConfig,
    
    /// P2P network configuration
    #[serde(default)]
    pub p2p: P2pConfig,
    
    /// IPFS configuration
    #[serde(default)]
    pub ipfs: IpfsConfig,
    
    /// RabbitMQ configuration - matches pyaleph
    #[serde(default)]
    pub rabbitmq: RabbitMQConfig,
    
    /// Redis cache configuration
    #[serde(default)]
    pub redis: RedisConfig,
    
    /// Aleph-specific configuration
    #[serde(default)]
    pub aleph: AlephConfig,
    
    /// Sentry error tracking
    #[serde(default)]
    pub sentry: SentryConfig,
    
    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            database: DatabaseConfig::default(),
            storage: StorageConfig::default(),
            api: ApiConfig::default(),
            chains: ChainsConfig::default(),
            p2p: P2pConfig::default(),
            ipfs: IpfsConfig::default(),
            rabbitmq: RabbitMQConfig::default(),
            redis: RedisConfig::default(),
            aleph: AlephConfig::default(),
            sentry: SentryConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, config::ConfigError> {
        let path = path.into();
        let settings = config::Config::builder()
            .add_source(config::File::from(path))
            .add_source(config::Environment::with_prefix("ALEPH").separator("__"))
            .build()?;
        
        settings.try_deserialize()
    }
    
    /// Load configuration from environment variables only
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("ALEPH").separator("__"))
            .build()?;
        
        settings.try_deserialize()
    }
    
    /// Load from file with environment overrides
    pub fn load(path: Option<impl Into<PathBuf>>) -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder();
        
        // Add file source if provided
        if let Some(p) = path {
            let path: PathBuf = p.into();
            if path.exists() {
                builder = builder.add_source(config::File::from(path));
            }
        }
        
        // Add environment variables
        builder = builder.add_source(config::Environment::with_prefix("ALEPH").separator("__"));
        
        builder.build()?.try_deserialize()
    }
    
    /// Validate configuration
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        
        // Validate database URL
        if self.database.url.is_empty() {
            errors.push("database.url is required".to_string());
        }
        
        // Validate API port
        if self.api.port == 0 {
            errors.push("api.port must be > 0".to_string());
        }
        
        // Validate at least one chain is enabled
        let any_chain_enabled = self.chains.ethereum.as_ref().map(|c| c.enabled).unwrap_or(false)
            || self.chains.solana.as_ref().map(|c| c.enabled).unwrap_or(false)
            || self.chains.avalanche.as_ref().map(|c| c.enabled).unwrap_or(false)
            || self.chains.bsc.as_ref().map(|c| c.enabled).unwrap_or(false);
        
        if !any_chain_enabled {
            errors.push("At least one chain must be enabled".to_string());
        }
        
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier
    pub id: Option<String>,
    
    /// Node name (for display)
    #[serde(default = "default_node_name")]
    pub name: String,
    
    /// Data directory
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    
    /// Log level
    #[serde(default = "default_log_level")]
    pub log_level: String,
    
    /// Node operator address (for rewards)
    pub operator_address: Option<String>,
    
    /// Whether this is a core channel node (CCN)
    #[serde(default = "default_true")]
    pub is_ccn: bool,
}

fn default_node_name() -> String { "aleph-core".to_string() }
fn default_data_dir() -> PathBuf { PathBuf::from("./data") }
fn default_log_level() -> String { "info".to_string() }
fn default_true() -> bool { true }

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: None,
            name: default_node_name(),
            data_dir: default_data_dir(),
            log_level: default_log_level(),
            operator_address: None,
            is_ccn: true,
        }
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    #[serde(default = "default_db_url")]
    pub url: String,
    
    /// Maximum connections in pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    
    /// Minimum connections in pool
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,
    
    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,
    
    /// Idle timeout in seconds
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: u64,
    
    /// Enable read replicas
    #[serde(default)]
    pub read_replicas: Vec<String>,
    
    /// Statement cache size
    #[serde(default = "default_statement_cache_size")]
    pub statement_cache_size: u32,
}

fn default_db_url() -> String { "postgres://localhost/aleph".to_string() }
fn default_max_connections() -> u32 { 20 }
fn default_min_connections() -> u32 { 2 }
fn default_connect_timeout() -> u64 { 30 }
fn default_idle_timeout() -> u64 { 600 }
fn default_statement_cache_size() -> u32 { 100 }

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: default_db_url(),
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            connect_timeout: default_connect_timeout(),
            idle_timeout: default_idle_timeout(),
            read_replicas: Vec::new(),
            statement_cache_size: default_statement_cache_size(),
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage directory for files
    #[serde(default = "default_files_dir")]
    pub files_dir: PathBuf,
    
    /// Maximum file size in bytes
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,
    
    /// Maximum unauthenticated upload size (25MB)
    #[serde(default = "default_max_unauth_file_size")]
    pub max_unauthenticated_file_size: u64,
    
    /// Enable local caching
    #[serde(default = "default_true")]
    pub enable_cache: bool,
    
    /// Cache directory
    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,
    
    /// Grace period for file deletion (seconds)
    #[serde(default = "default_grace_period")]
    pub grace_period: u64,
    
    /// Enable garbage collection
    #[serde(default = "default_true")]
    pub garbage_collection: bool,
    
    /// Garbage collection period (seconds)
    #[serde(default = "default_gc_period")]
    pub gc_period: u64,
}

fn default_files_dir() -> PathBuf { PathBuf::from("./data/files") }
fn default_max_file_size() -> u64 { 100 * 1024 * 1024 } // 100MB
fn default_max_unauth_file_size() -> u64 { 25 * 1024 * 1024 } // 25MB
fn default_cache_dir() -> PathBuf { PathBuf::from("./data/cache") }
fn default_grace_period() -> u64 { 86400 } // 24 hours
fn default_gc_period() -> u64 { 3600 } // 1 hour

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            files_dir: default_files_dir(),
            max_file_size: default_max_file_size(),
            max_unauthenticated_file_size: default_max_unauth_file_size(),
            enable_cache: true,
            cache_dir: default_cache_dir(),
            grace_period: default_grace_period(),
            garbage_collection: true,
            gc_period: default_gc_period(),
        }
    }
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Bind address
    #[serde(default = "default_host")]
    pub host: String,
    
    /// Port
    #[serde(default = "default_port")]
    pub port: u16,
    
    /// Enable CORS
    #[serde(default = "default_true")]
    pub cors_enabled: bool,
    
    /// CORS allowed origins
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
    
    /// Rate limiting (requests per second per IP)
    #[serde(default = "default_rate_limit")]
    pub rate_limit: Option<u32>,
    
    /// Rate limit window (seconds)
    #[serde(default = "default_rate_window")]
    pub rate_limit_window: u64,
    
    /// Enable authentication
    #[serde(default)]
    pub auth_enabled: bool,
    
    /// Auth public key for token verification
    pub auth_public_key: Option<String>,
    
    /// Request body size limit (bytes)
    #[serde(default = "default_body_limit")]
    pub body_limit: usize,
    
    /// Enable WebSocket
    #[serde(default = "default_true")]
    pub websocket_enabled: bool,
    
    /// WebSocket ping interval (seconds)
    #[serde(default = "default_ws_ping")]
    pub websocket_ping_interval: u64,
}

fn default_host() -> String { "0.0.0.0".to_string() }
fn default_port() -> u16 { 8080 }
fn default_cors_origins() -> Vec<String> { vec!["*".to_string()] }
fn default_rate_limit() -> Option<u32> { Some(100) }
fn default_rate_window() -> u64 { 60 }
fn default_body_limit() -> usize { 10 * 1024 * 1024 } // 10MB
fn default_ws_ping() -> u64 { 30 }

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            cors_enabled: true,
            cors_origins: default_cors_origins(),
            rate_limit: default_rate_limit(),
            rate_limit_window: default_rate_window(),
            auth_enabled: false,
            auth_public_key: None,
            body_limit: default_body_limit(),
            websocket_enabled: true,
            websocket_ping_interval: default_ws_ping(),
        }
    }
}

/// Chain configurations
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChainsConfig {
    pub ethereum: Option<EthereumConfig>,
    pub solana: Option<SolanaConfig>,
    pub tezos: Option<TezosConfig>,
    pub avalanche: Option<AvalancheConfig>,
    pub bsc: Option<BscConfig>,
    pub nuls: Option<NulsConfig>,
    pub cosmos: Option<CosmosConfig>,
}

/// Ethereum chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumConfig {
    /// RPC endpoint
    #[serde(default = "default_eth_rpc")]
    pub rpc_url: String,
    
    /// Backup RPC endpoints
    #[serde(default)]
    pub backup_rpc_urls: Vec<String>,
    
    /// Aleph contract address
    #[serde(default = "default_eth_contract")]
    pub contract_address: String,
    
    /// Chain ID
    #[serde(default = "default_eth_chain_id")]
    pub chain_id: u64,
    
    /// Enable indexing
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Starting block
    #[serde(default = "default_eth_start_block")]
    pub start_block: u64,
    
    /// Confirmation blocks required
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
    
    /// Block batch size for indexing
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    
    /// Authorized sync message emitters
    #[serde(default)]
    pub authorized_emitters: Vec<String>,
    
    /// Use external indexer (AlephIndexerReader)
    #[serde(default)]
    pub use_indexer: bool,
    
    /// Indexer URL
    pub indexer_url: Option<String>,
}

fn default_eth_rpc() -> String { "https://eth-mainnet.g.alchemy.com/v2/demo".to_string() }
fn default_eth_contract() -> String { "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59".to_string() }
fn default_eth_chain_id() -> u64 { 1 }
fn default_eth_start_block() -> u64 { 10000000 }
fn default_confirmations() -> u64 { 10 }
fn default_batch_size() -> u64 { 1000 }

impl Default for EthereumConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_eth_rpc(),
            backup_rpc_urls: Vec::new(),
            contract_address: default_eth_contract(),
            chain_id: default_eth_chain_id(),
            enabled: true,
            start_block: default_eth_start_block(),
            confirmations: default_confirmations(),
            batch_size: default_batch_size(),
            authorized_emitters: Vec::new(),
            use_indexer: false,
            indexer_url: None,
        }
    }
}

/// Solana chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaConfig {
    #[serde(default = "default_sol_rpc")]
    pub rpc_url: String,
    pub program_id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
}

fn default_sol_rpc() -> String { "https://api.mainnet-beta.solana.com".to_string() }

impl Default for SolanaConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_sol_rpc(),
            program_id: String::new(),
            enabled: false,
            confirmations: default_confirmations(),
        }
    }
}

/// Tezos chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TezosConfig {
    #[serde(default = "default_tez_rpc")]
    pub rpc_url: String,
    pub contract_address: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
}

fn default_tez_rpc() -> String { "https://mainnet.api.tez.ie".to_string() }

impl Default for TezosConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_tez_rpc(),
            contract_address: String::new(),
            enabled: false,
            confirmations: default_confirmations(),
        }
    }
}

/// Avalanche chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvalancheConfig {
    #[serde(default = "default_avax_rpc")]
    pub rpc_url: String,
    #[serde(default = "default_avax_contract")]
    pub contract_address: String,
    #[serde(default = "default_avax_chain_id")]
    pub chain_id: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub start_block: u64,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
}

fn default_avax_rpc() -> String { "https://api.avax.network/ext/bc/C/rpc".to_string() }
fn default_avax_contract() -> String { "0xDCaf7e6f6E2Ca3f9fc52d1F9a8c5E6F6D3e5c6f7".to_string() }
fn default_avax_chain_id() -> u64 { 43114 }

impl Default for AvalancheConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_avax_rpc(),
            contract_address: default_avax_contract(),
            chain_id: default_avax_chain_id(),
            enabled: false,
            start_block: 0,
            confirmations: default_confirmations(),
        }
    }
}

/// BSC chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BscConfig {
    #[serde(default = "default_bsc_rpc")]
    pub rpc_url: String,
    #[serde(default = "default_bsc_contract")]
    pub contract_address: String,
    #[serde(default = "default_bsc_chain_id")]
    pub chain_id: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub start_block: u64,
    #[serde(default = "default_confirmations")]
    pub confirmations: u64,
}

fn default_bsc_rpc() -> String { "https://bsc-dataseed.binance.org".to_string() }
fn default_bsc_contract() -> String { "0x82D2f8E02Afb160Dd5A480a617692e62de9038C4".to_string() }
fn default_bsc_chain_id() -> u64 { 56 }

impl Default for BscConfig {
    fn default() -> Self {
        Self {
            rpc_url: default_bsc_rpc(),
            contract_address: default_bsc_contract(),
            chain_id: default_bsc_chain_id(),
            enabled: false,
            start_block: 0,
            confirmations: default_confirmations(),
        }
    }
}

/// NULS chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NulsConfig {
    pub api_url: String,
    pub chain_id: u64,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub start_height: u64,
}

impl Default for NulsConfig {
    fn default() -> Self {
        Self {
            api_url: "https://api.nuls.io".to_string(),
            chain_id: 1,
            enabled: false,
            start_height: 0,
        }
    }
}

/// Cosmos SDK chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CosmosConfig {
    pub rpc_url: String,
    pub chain_id: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub start_height: u64,
    pub prefix: String,
}

impl Default for CosmosConfig {
    fn default() -> Self {
        Self {
            rpc_url: "https://rpc.cosmos.network".to_string(),
            chain_id: "cosmoshub-4".to_string(),
            enabled: false,
            start_height: 0,
            prefix: "cosmos".to_string(),
        }
    }
}

/// P2P network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2pConfig {
    /// Enable P2P networking
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Listen addresses
    #[serde(default = "default_listen_addrs")]
    pub listen_addrs: Vec<String>,
    
    /// Bootstrap peers
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    
    /// Topic for message exchange
    #[serde(default = "default_topic")]
    pub topic: String,
    
    /// Maximum peer connections
    #[serde(default = "default_max_peers")]
    pub max_peers: u32,
    
    /// Peer discovery interval (seconds)
    #[serde(default = "default_discovery_interval")]
    pub discovery_interval: u64,
    
    /// Reconnection delay (seconds)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
}

fn default_listen_addrs() -> Vec<String> { vec!["/ip4/0.0.0.0/tcp/4025".to_string()] }
fn default_topic() -> String { "aleph-messages".to_string() }
fn default_max_peers() -> u32 { 50 }
fn default_discovery_interval() -> u64 { 300 }
fn default_reconnect_delay() -> u64 { 60 }

impl Default for P2pConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addrs: default_listen_addrs(),
            bootstrap_peers: vec![],
            topic: default_topic(),
            max_peers: default_max_peers(),
            discovery_interval: default_discovery_interval(),
            reconnect_delay: default_reconnect_delay(),
        }
    }
}

/// IPFS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsConfig {
    /// IPFS API endpoint
    #[serde(default = "default_ipfs_api")]
    pub api_url: String,
    
    /// Gateway URL for fetching content
    #[serde(default = "default_ipfs_gateway")]
    pub gateway_url: String,
    
    /// Enable IPFS pinning
    #[serde(default = "default_true")]
    pub pin_content: bool,
    
    /// Timeout for IPFS operations (seconds)
    #[serde(default = "default_ipfs_timeout")]
    pub timeout: u64,
    
    /// Maximum concurrent fetches
    #[serde(default = "default_ipfs_concurrency")]
    pub max_concurrent_fetches: u32,
    
    /// Enable directory pinning
    #[serde(default = "default_true")]
    pub pin_directories: bool,
}

fn default_ipfs_api() -> String { "http://localhost:5001".to_string() }
fn default_ipfs_gateway() -> String { "https://ipfs.io/ipfs".to_string() }
fn default_ipfs_timeout() -> u64 { 30 }
fn default_ipfs_concurrency() -> u32 { 10 }

impl Default for IpfsConfig {
    fn default() -> Self {
        Self {
            api_url: default_ipfs_api(),
            gateway_url: default_ipfs_gateway(),
            pin_content: true,
            timeout: default_ipfs_timeout(),
            max_concurrent_fetches: default_ipfs_concurrency(),
            pin_directories: true,
        }
    }
}

/// RabbitMQ configuration - matches pyaleph defaults
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMQConfig {
    /// Connection URL
    #[serde(default = "default_rabbitmq_url")]
    pub url: String,
    
    /// Publishing exchange (to p2p network)
    #[serde(default = "default_pub_exchange")]
    pub pub_exchange: String,
    
    /// Subscribe exchange (from p2p network)
    #[serde(default = "default_sub_exchange")]
    pub sub_exchange: String,
    
    /// Processed messages exchange
    #[serde(default = "default_message_exchange")]
    pub message_exchange: String,
    
    /// Pending messages exchange
    #[serde(default = "default_pending_message_exchange")]
    pub pending_message_exchange: String,
    
    /// Pending transactions exchange
    #[serde(default = "default_pending_tx_exchange")]
    pub pending_tx_exchange: String,
    
    /// Enable RabbitMQ integration
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Reconnection delay (seconds)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
    
    /// Prefetch count for consumers
    #[serde(default = "default_prefetch")]
    pub prefetch_count: u16,
}

fn default_rabbitmq_url() -> String { "amqp://localhost:5672".to_string() }
fn default_pub_exchange() -> String { "p2p-publish".to_string() }
fn default_sub_exchange() -> String { "p2p-subscribe".to_string() }
fn default_message_exchange() -> String { "aleph-messages".to_string() }
fn default_pending_message_exchange() -> String { "aleph-pending-messages".to_string() }
fn default_pending_tx_exchange() -> String { "aleph-pending-txs".to_string() }
fn default_prefetch() -> u16 { 100 }

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            url: default_rabbitmq_url(),
            pub_exchange: default_pub_exchange(),
            sub_exchange: default_sub_exchange(),
            message_exchange: default_message_exchange(),
            pending_message_exchange: default_pending_message_exchange(),
            pending_tx_exchange: default_pending_tx_exchange(),
            enabled: true,
            reconnect_delay: default_reconnect_delay(),
            prefetch_count: default_prefetch(),
        }
    }
}

/// Redis configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis URL
    #[serde(default = "default_redis_url")]
    pub url: String,
    
    /// Enable Redis caching
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Connection pool size
    #[serde(default = "default_redis_pool")]
    pub pool_size: u32,
    
    /// Default TTL (seconds)
    #[serde(default = "default_redis_ttl")]
    pub default_ttl: u64,
    
    /// Key prefix
    #[serde(default = "default_redis_prefix")]
    pub key_prefix: String,
}

fn default_redis_url() -> String { "redis://localhost:6379".to_string() }
fn default_redis_pool() -> u32 { 10 }
fn default_redis_ttl() -> u64 { 300 }
fn default_redis_prefix() -> String { "aleph:".to_string() }

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: default_redis_url(),
            enabled: true,
            pool_size: default_redis_pool(),
            default_ttl: default_redis_ttl(),
            key_prefix: default_redis_prefix(),
        }
    }
}

/// Aleph-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlephConfig {
    /// Balance tracking configuration
    #[serde(default)]
    pub balances: BalancesConfig,
    
    /// Credit balance configuration
    #[serde(default)]
    pub credit_balances: CreditBalancesConfig,
    
    /// Job processing configuration
    #[serde(default)]
    pub jobs: JobsConfig,
    
    /// Cache TTL configuration
    #[serde(default)]
    pub cache: CacheConfig,
    
    /// Pricing aggregate address
    #[serde(default = "default_pricing_address")]
    pub pricing_aggregate_address: String,
    
    /// Pricing aggregate key
    #[serde(default = "default_pricing_key")]
    pub pricing_aggregate_key: String,
}

fn default_pricing_address() -> String { "0x4D52380D3191274a04846c89c069E6C3F2Ed94e4".to_string() }
fn default_pricing_key() -> String { "pricing".to_string() }

impl Default for AlephConfig {
    fn default() -> Self {
        Self {
            balances: BalancesConfig::default(),
            credit_balances: CreditBalancesConfig::default(),
            jobs: JobsConfig::default(),
            cache: CacheConfig::default(),
            pricing_aggregate_address: default_pricing_address(),
            pricing_aggregate_key: default_pricing_key(),
        }
    }
}

/// Balance tracking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancesConfig {
    /// Enable balance tracking
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Update interval (seconds)
    #[serde(default = "default_balance_interval")]
    pub update_interval: u64,
    
    /// Staking contract addresses by chain
    #[serde(default)]
    pub staking_contracts: HashMap<String, String>,
}

fn default_balance_interval() -> u64 { 300 }

impl Default for BalancesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            update_interval: default_balance_interval(),
            staking_contracts: HashMap::new(),
        }
    }
}

/// Credit balance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditBalancesConfig {
    /// Enable credit balance tracking
    #[serde(default = "default_true")]
    pub enabled: bool,
    
    /// Credit expiration (seconds)
    #[serde(default = "default_credit_expiration")]
    pub expiration: u64,
}

fn default_credit_expiration() -> u64 { 2592000 } // 30 days

impl Default for CreditBalancesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            expiration: default_credit_expiration(),
        }
    }
}

/// Job processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsConfig {
    /// Message processing batch size
    #[serde(default = "default_batch_size_i64")]
    pub message_batch_size: i64,
    
    /// Message processing interval (ms)
    #[serde(default = "default_process_interval")]
    pub process_interval_ms: u64,
    
    /// Maximum retries before rejection
    #[serde(default = "default_max_retries")]
    pub max_retries: i32,
    
    /// Base retry delay (seconds)
    #[serde(default = "default_retry_delay")]
    pub base_retry_delay: f64,
    
    /// Garbage collection period (seconds)
    #[serde(default = "default_gc_period")]
    pub garbage_collector_period: u64,
    
    /// Chain sync check interval (seconds)
    #[serde(default = "default_chain_sync_interval")]
    pub chain_sync_interval: u64,
}

fn default_batch_size_i64() -> i64 { 100 }
fn default_process_interval() -> u64 { 1000 }
fn default_max_retries() -> i32 { 10 }
fn default_retry_delay() -> f64 { 60.0 }
fn default_chain_sync_interval() -> u64 { 12 }

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            message_batch_size: default_batch_size_i64(),
            process_interval_ms: default_process_interval(),
            max_retries: default_max_retries(),
            base_retry_delay: default_retry_delay(),
            garbage_collector_period: default_gc_period(),
            chain_sync_interval: default_chain_sync_interval(),
        }
    }
}

/// Cache TTL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Message cache TTL (seconds)
    #[serde(default = "default_message_ttl")]
    pub message_ttl: u64,
    
    /// Aggregate cache TTL (seconds)
    #[serde(default = "default_aggregate_ttl")]
    pub aggregate_ttl: u64,
    
    /// Balance cache TTL (seconds)
    #[serde(default = "default_balance_ttl")]
    pub balance_ttl: u64,
    
    /// File info cache TTL (seconds)
    #[serde(default = "default_file_ttl")]
    pub file_ttl: u64,
}

fn default_message_ttl() -> u64 { 60 }
fn default_aggregate_ttl() -> u64 { 30 }
fn default_balance_ttl() -> u64 { 120 }
fn default_file_ttl() -> u64 { 300 }

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            message_ttl: default_message_ttl(),
            aggregate_ttl: default_aggregate_ttl(),
            balance_ttl: default_balance_ttl(),
            file_ttl: default_file_ttl(),
        }
    }
}

/// Sentry error tracking configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SentryConfig {
    /// Sentry DSN
    pub dsn: Option<String>,
    
    /// Enable Sentry
    #[serde(default)]
    pub enabled: bool,
    
    /// Environment (production, staging, development)
    pub environment: Option<String>,
    
    /// Sample rate (0.0 to 1.0)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f32,
}

fn default_sample_rate() -> f32 { 1.0 }

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    #[serde(default = "default_log_level")]
    pub level: String,
    
    /// Log format (json, text)
    #[serde(default = "default_log_format")]
    pub format: String,
    
    /// Log to file
    pub file: Option<String>,
    
    /// Enable structured logging
    #[serde(default = "default_true")]
    pub structured: bool,
}

fn default_log_format() -> String { "text".to_string() }

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            file: None,
            structured: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = Config::default();
        
        assert_eq!(config.api.port, 8080);
        assert_eq!(config.rabbitmq.pub_exchange, "p2p-publish");
        assert_eq!(config.rabbitmq.sub_exchange, "p2p-subscribe");
    }
    
    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        config.chains.ethereum = Some(EthereumConfig::default());
        
        // Should be valid with defaults
        assert!(config.validate().is_ok());
        
        // Empty DB URL should fail
        config.database.url = String::new();
        assert!(config.validate().is_err());
    }
}
