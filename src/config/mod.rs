//! Configuration management for Aleph Core

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Node configuration
    pub node: NodeConfig,
    
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Storage configuration
    pub storage: StorageConfig,
    
    /// API configuration
    pub api: ApiConfig,
    
    /// Chain configurations
    pub chains: ChainsConfig,
    
    /// P2P network configuration
    pub p2p: P2pConfig,
    
    /// IPFS configuration
    pub ipfs: IpfsConfig,
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
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, config::ConfigError> {
        let path = path.into();
        let settings = config::Config::builder()
            .add_source(config::File::from(path))
            .add_source(config::Environment::with_prefix("ALEPH"))
            .build()?;
        
        settings.try_deserialize()
    }
    
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::Environment::with_prefix("ALEPH"))
            .build()?;
        
        settings.try_deserialize()
    }
}

/// Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier
    pub id: Option<String>,
    
    /// Node name (for display)
    pub name: String,
    
    /// Data directory
    pub data_dir: PathBuf,
    
    /// Log level
    pub log_level: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: None,
            name: "aleph-core".to_string(),
            data_dir: PathBuf::from("./data"),
            log_level: "info".to_string(),
        }
    }
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    pub url: String,
    
    /// Maximum connections in pool
    pub max_connections: u32,
    
    /// Connection timeout in seconds
    pub connect_timeout: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgres://localhost/aleph".to_string(),
            max_connections: 10,
            connect_timeout: 30,
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage directory for files
    pub files_dir: PathBuf,
    
    /// Maximum file size in bytes
    pub max_file_size: u64,
    
    /// Enable local caching
    pub enable_cache: bool,
    
    /// Cache directory
    pub cache_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            files_dir: PathBuf::from("./data/files"),
            max_file_size: 100 * 1024 * 1024, // 100MB
            enable_cache: true,
            cache_dir: PathBuf::from("./data/cache"),
        }
    }
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Bind address
    pub host: String,
    
    /// Port
    pub port: u16,
    
    /// Enable CORS
    pub cors_enabled: bool,
    
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
    
    /// Rate limiting (requests per second)
    pub rate_limit: Option<u32>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 8080,
            cors_enabled: true,
            cors_origins: vec!["*".to_string()],
            rate_limit: Some(100),
        }
    }
}

/// Chain configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainsConfig {
    pub ethereum: Option<EthereumConfig>,
    pub solana: Option<SolanaConfig>,
    pub tezos: Option<TezosConfig>,
}

impl Default for ChainsConfig {
    fn default() -> Self {
        Self {
            ethereum: Some(EthereumConfig::default()),
            solana: None,
            tezos: None,
        }
    }
}

/// Ethereum chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EthereumConfig {
    /// RPC endpoint
    pub rpc_url: String,
    
    /// Aleph contract address
    pub contract_address: String,
    
    /// Chain ID
    pub chain_id: u64,
    
    /// Enable indexing
    pub enabled: bool,
    
    /// Starting block
    pub start_block: u64,
}

impl Default for EthereumConfig {
    fn default() -> Self {
        Self {
            rpc_url: "https://eth-mainnet.g.alchemy.com/v2/demo".to_string(),
            contract_address: "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59".to_string(),
            chain_id: 1,
            enabled: true,
            start_block: 10000000,
        }
    }
}

/// Solana chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaConfig {
    pub rpc_url: String,
    pub program_id: String,
    pub enabled: bool,
}

/// Tezos chain configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TezosConfig {
    pub rpc_url: String,
    pub contract_address: String,
    pub enabled: bool,
}

/// P2P network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2pConfig {
    /// Enable P2P networking
    pub enabled: bool,
    
    /// Listen addresses
    pub listen_addrs: Vec<String>,
    
    /// Bootstrap peers
    pub bootstrap_peers: Vec<String>,
    
    /// Topic for message exchange
    pub topic: String,
}

impl Default for P2pConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addrs: vec!["/ip4/0.0.0.0/tcp/4025".to_string()],
            bootstrap_peers: vec![],
            topic: "aleph-messages".to_string(),
        }
    }
}

/// IPFS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpfsConfig {
    /// IPFS API endpoint
    pub api_url: String,
    
    /// Gateway URL for fetching content
    pub gateway_url: String,
    
    /// Enable IPFS pinning
    pub pin_content: bool,
}

impl Default for IpfsConfig {
    fn default() -> Self {
        Self {
            api_url: "http://localhost:5001".to_string(),
            gateway_url: "https://ipfs.io/ipfs".to_string(),
            pin_content: true,
        }
    }
}
