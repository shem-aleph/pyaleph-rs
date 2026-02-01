//! Aleph Core - Main entry point
//!
//! This is the main binary for running an Aleph network node.

use aleph_core::{Config, web, db};
use clap::Parser;
use std::path::PathBuf;
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
    
    let subscriber = FmtSubscriber::builder()
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
