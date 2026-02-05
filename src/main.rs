//! Aleph Core - Main entry point
//!
//! This is the main binary for running an Aleph network node.

use aleph_core::{Config, web, db, jobs, services};
use aleph_core::services::CryptoService;
use aleph_core::services::ipfs::IpfsService;
use aleph_core::services::peers;
use aleph_core::jobs::message_processor::ProcessorContext;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Aleph Core Node
#[derive(Parser, Debug)]
#[command(name = "aleph-core")]
#[command(version = "0.1.0")]
#[command(about = "Rust implementation of the Aleph.im Core Node")]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    
    /// Port to listen on (overrides config)
    #[arg(short, long)]
    port: Option<u16>,
    
    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
    
    /// Skip database connection (API-only mode)
    #[arg(long)]
    no_db: bool,
    
    /// Run database migrations and exit
    #[arg(long)]
    migrate: bool,
    
    /// Enable chain synchronization (sync from blockchain/indexer)
    #[arg(long)]
    sync: bool,
    
    /// Use indexer-based sync (recommended, uses multichain.api.aleph.cloud)
    #[arg(long)]
    indexer_sync: bool,
    
    /// Use direct RPC sync (eth_getLogs, bypasses rate-limited multichain indexer)
    #[arg(long)]
    rpc_sync: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    
    FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(true)
        .with_thread_ids(true)
        .init();
    
    info!("Starting Aleph Core v{}", env!("CARGO_PKG_VERSION"));
    
    // Load configuration
    let mut config = if args.config.exists() {
        info!("Loading config from {:?}", args.config);
        Config::from_file(&args.config)?
    } else {
        info!("Using default configuration");
        Config::default()
    };
    
    // Override port if specified
    if let Some(port) = args.port {
        config.api.port = port;
    }
    
    info!("Configuration loaded:");
    info!("  API: {}:{}", config.api.host, config.api.port);
    info!("  Database: {}", config.database.url);
    info!("  Data dir: {:?}", config.node.data_dir);
    
    // Handle migration mode
    if args.migrate {
        info!("Running migrations...");
        let pool = db::create_pool(&config.database).await?;
        db::migrations::run_migrations(&pool).await?;
        info!("Migrations complete");
        return Ok(());
    }
    
    // Connect to database (unless --no-db)
    if args.no_db {
        warn!("Running without database (--no-db)");
        info!("Starting API server on {}:{}", config.api.host, config.api.port);
        web::start_server(&config).await?;
    } else {
        // Try to connect to database
        match db::init_db(&config.database).await {
            Ok(pool) => {
                info!("Database connected and migrated");

                // Run startup backfill (populate derived tables from messages)
                match jobs::backfill::run_startup_backfill(&pool).await {
                    Ok(result) => {
                        info!("Startup backfill: {}", result);
                    }
                    Err(e) => {
                        warn!("Startup backfill failed (continuing anyway): {}", e);
                    }
                }

                let config_arc = Arc::new(config.clone());
                
                // Start chain sync if enabled
                if args.rpc_sync {
                    info!("Starting RPC-based chain sync (direct eth_getLogs)");
                    let pool_clone = pool.clone();
                    let ipfs_url = config.ipfs.api_url.clone();
                    let config_arc_rpc = config_arc.clone();
                    tokio::spawn(async move {
                        jobs::chain_sync::run_rpc_sync(
                            config_arc_rpc,
                            pool_clone,
                            &ipfs_url,
                        ).await;
                    });
                } else if args.indexer_sync {
                    info!("Starting indexer-based chain sync (multichain.api.aleph.cloud)");
                    let pool_clone = pool.clone();
                    let ipfs_url = config.ipfs.api_url.clone();
                    tokio::spawn(async move {
                        jobs::chain_sync::run_indexer_sync(
                            config_arc.clone(),
                            pool_clone,
                            &ipfs_url,
                        ).await;
                    });
                } else if args.sync {
                    info!("Starting direct RPC chain sync");
                    let pool_clone = pool.clone();
                    let config_arc_sync = config_arc.clone();
                    tokio::spawn(async move {
                        jobs::chain_sync::run_with_db(config_arc_sync, pool_clone).await;
                    });
                }
                
                // Start message processor (processes pending messages into derived tables)
                let crypto = Arc::new(CryptoService::new());
                let ipfs = Arc::new(IpfsService::new(&config.ipfs));
                {
                    info!("Starting message processor");
                    let pool_clone = pool.clone();
                    let config_arc_proc = Arc::new(config.clone());
                    
                    let processor_ctx = Arc::new(ProcessorContext::new(
                        pool_clone,
                        crypto.clone(),
                        ipfs.clone(),
                        config_arc_proc,
                    ));
                    
                    tokio::spawn(async move {
                        jobs::message_processor::run(processor_ctx).await;
                    });
                }
                
                // Start P2P consumer (RabbitMQ → pending_messages)
                if config.rabbitmq.enabled {
                    info!("Starting P2P consumer (RabbitMQ)");
                    let pool_clone = pool.clone();
                    let config_arc_p2p = Arc::new(config.clone());
                    tokio::spawn(async move {
                        let ctx = Arc::new(
                            jobs::p2p_consumer::P2pConsumerContext::new(
                                pool_clone,
                                config_arc_p2p,
                            ).await,
                        );
                        jobs::p2p_consumer::run(ctx).await;
                    });
                }
                
                // Start peer monitoring and content fetch services
                // These always run — not gated on RabbitMQ
                let api_servers = peers::new_api_servers();
                
                {
                    info!("Starting peer tidy job (HTTP peer health checking)");
                    let pool_clone = pool.clone();
                    let api_servers_clone = api_servers.clone();
                    let config_arc_peers = Arc::new(config.clone());
                    tokio::spawn(async move {
                        peers::tidy_http_peers_job(
                            pool_clone,
                            api_servers_clone,
                            config_arc_peers,
                        ).await;
                    });
                }

                {
                    info!("Starting peer discovery job (corechannel aggregate)");
                    let pool_clone = pool.clone();
                    let config_arc_disc = Arc::new(config.clone());
                    tokio::spawn(async move {
                        peers::peer_discovery_job(
                            pool_clone,
                            config_arc_disc,
                        ).await;
                    });
                }

                {
                    info!("Starting content fetch service (peer-based)");
                    let pool_clone = pool.clone();
                    let config_arc_cf = Arc::new(config.clone());
                    let ipfs_clone = ipfs.clone();
                    let api_servers_clone = api_servers.clone();
                    tokio::spawn(async move {
                        // Small delay to let peer tidy job populate api_servers first
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        let ctx = Arc::new(
                            services::content_fetch::ContentFetchContext::new(
                                pool_clone,
                                config_arc_cf,
                                ipfs_clone,
                                api_servers_clone,
                            ),
                        );
                        services::content_fetch::run(ctx).await;
                    });
                }
                
                // Start cleanup job (periodic maintenance)
                {
                    let pool_clone = pool.clone();
                    let config_arc_cleanup = Arc::new(config.clone());
                    tokio::spawn(async move {
                        jobs::cleanup::run(pool_clone, config_arc_cleanup).await;
                    });
                }

                // Start garbage collector
                {
                    let pool_clone = pool.clone();
                    let ipfs_clone = ipfs.clone();
                    let config_arc_gc = Arc::new(config.clone());
                    let metrics = Arc::new(services::Metrics::new());
                    tokio::spawn(async move {
                        jobs::garbage_collector::run(pool_clone, ipfs_clone, config_arc_gc, metrics).await;
                    });
                }

                info!("Starting API server on {}:{}", config.api.host, config.api.port);
                web::start_server_with_db(&config, pool).await?;
            }
            Err(e) => {
                warn!("Database connection failed: {}", e);
                warn!("Starting in API-only mode");
                web::start_server(&config).await?;
            }
        }
    }
    
    Ok(())
}
