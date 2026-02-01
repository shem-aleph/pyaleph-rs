# Configuration Guide

pyaleph-rs uses a hierarchical configuration system compatible with the Python pyaleph.

## Configuration Sources

Configuration is loaded in order (later sources override earlier):

1. Default values
2. `config.toml` in current directory
3. Custom config file (`--config path/to/config.toml`)
4. Environment variables (`ALEPH_` prefix)

## Environment Variables

All configuration options can be set via environment variables:

```bash
# Format: ALEPH_{SECTION}_{KEY}
export ALEPH_API_PORT=8080
export ALEPH_DATABASE_URL="postgres://localhost/aleph"
export ALEPH_CHAINS_ETHEREUM_RPC_URL="https://eth.llamarpc.com"
```

## Full Configuration Reference

```toml
#
# Aleph Core Node Configuration
#

[aleph]
# Unique node identifier
node_id = "my-node"

# Data directory for files and cache
data_dir = "./data"

# Log level: trace, debug, info, warn, error
log_level = "info"

# Private key file for node identity (optional)
private_key_file = ""

# Network: mainnet, testnet, devnet
network = "mainnet"

#
# API Server
#
[api]
# Bind address
host = "0.0.0.0"

# Port
port = 8080

# Enable CORS
cors_enabled = true

# CORS allowed origins (comma-separated, or "*")
cors_origins = "*"

# Request body size limit
max_body_size = "10mb"

# Enable WebSocket endpoint
websocket_enabled = true

#
# PostgreSQL Database
#
[database]
# Connection URL
url = "postgres://user:password@localhost:5432/aleph"

# Connection pool size
max_connections = 20
min_connections = 5

# Connection timeout (seconds)
connect_timeout = 30

# Idle connection timeout (seconds)
idle_timeout = 600

# Run migrations on startup
run_migrations = true

#
# Redis Cache
#
[redis]
# Enable Redis caching
enabled = true

# Connection URL
url = "redis://localhost:6379"

# Key prefix
prefix = "aleph:"

# Default TTL (seconds)
default_ttl = 3600

# Connection pool size
pool_size = 10

#
# RabbitMQ (P2P Bridge)
#
[rabbitmq]
# Enable RabbitMQ integration
enabled = true

# Connection URL
url = "amqp://guest:guest@localhost:5672"

# Exchange names (should match p2p-service)
publish_exchange = "p2p-publish"
subscribe_exchange = "p2p-subscribe"

# Queue names
message_queue = "aleph-messages"

# Prefetch count
prefetch_count = 100

#
# IPFS
#
[ipfs]
# IPFS API URL
api_url = "http://localhost:5001"

# Public gateway URL
gateway_url = "https://ipfs.aleph.im/ipfs"

# Pin files by default
auto_pin = true

# Request timeout (seconds)
timeout = 30

#
# File Storage
#
[storage]
# Storage directory
path = "./data/files"

# Enable storage service
enabled = true

# Max file size (bytes)
max_file_size = 104857600  # 100MB

# Garbage collection interval (seconds)
gc_interval = 3600

# File retention period (days)
retention_days = 30

#
# Chain Indexers
#

[chains.ethereum]
enabled = true
chain_id = 1
rpc_url = "https://eth.llamarpc.com"
contract_address = "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59"
start_block = 10000000
confirmations = 10
batch_size = 1000
poll_interval = 15

[chains.avalanche]
enabled = true
chain_id = 43114
rpc_url = "https://api.avax.network/ext/bc/C/rpc"
contract_address = "0xc0..."
start_block = 1000000
confirmations = 10
batch_size = 1000
poll_interval = 5

[chains.bsc]
enabled = true
chain_id = 56
rpc_url = "https://bsc-dataseed.binance.org"
contract_address = "0xc0..."
start_block = 1000000
confirmations = 15
batch_size = 1000
poll_interval = 5

[chains.solana]
enabled = false
rpc_url = "https://api.mainnet-beta.solana.com"
program_id = "..."
start_slot = 0

[chains.tezos]
enabled = false
rpc_url = "https://mainnet.api.tez.ie"
contract_address = "KT1..."
start_level = 0

#
# Message Processing
#
[messages]
# Enable message processor
processor_enabled = true

# Number of worker threads
workers = 4

# Retry configuration
max_retries = 5
retry_delay = 1000  # ms
retry_backoff = 2.0

# Batch size for processing
batch_size = 100

# Signature verification
verify_signatures = true

#
# Metrics
#
[metrics]
# Enable Prometheus metrics
enabled = true

# Metrics endpoint path
path = "/_internal/metrics"

# Include detailed histograms
detailed = true

#
# Sentry Error Tracking
#
[sentry]
enabled = false
dsn = ""
environment = "production"
sample_rate = 1.0

#
# P2P Network
#
[p2p]
# Enable P2P networking
enabled = true

# Listen addresses
listen_addresses = ["/ip4/0.0.0.0/tcp/4025"]

# Bootstrap peers
bootstrap_peers = []

# Max connections
max_peers = 50

# Pubsub topics
topics = ["aleph-messages"]
```

## Minimal Configuration

For a basic setup:

```toml
[api]
port = 8080

[database]
url = "postgres://localhost/aleph"
```

## Production Configuration

Recommended production settings:

```toml
[aleph]
log_level = "info"

[api]
host = "127.0.0.1"  # Behind reverse proxy
port = 8080

[database]
url = "postgres://aleph:password@localhost/aleph"
max_connections = 50
min_connections = 10

[redis]
enabled = true
url = "redis://localhost:6379"

[chains.ethereum]
enabled = true
rpc_url = "https://your-dedicated-node.com"
confirmations = 20

[metrics]
enabled = true

[sentry]
enabled = true
dsn = "https://..."
```

## Docker Compose Example

```yaml
version: '3.8'

services:
  aleph-core:
    image: aleph-core:latest
    environment:
      - ALEPH_DATABASE_URL=postgres://aleph:password@postgres/aleph
      - ALEPH_REDIS_URL=redis://redis:6379
      - ALEPH_API_PORT=8080
    ports:
      - "8080:8080"
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_USER=aleph
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=aleph
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7
    volumes:
      - redisdata:/data

volumes:
  pgdata:
  redisdata:
```
