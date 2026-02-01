# pyaleph-rs

Rust implementation of the Aleph.im Core Node (port of [pyaleph](https://github.com/aleph-im/pyaleph)).

## Status

🚧 **Work in Progress** - Core scaffolding complete, API functional.

### Implemented

- ✅ Core types (chains, messages, pricing, status)
- ✅ Configuration system (TOML/environment variables)
- ✅ Web API (Axum-based, compatible with pyaleph API v0)
- ✅ Database layer (PostgreSQL via sqlx)
- ✅ Services (crypto, storage, cost, IPFS)
- ✅ Chain indexers (Ethereum with ABI decoding)
- ✅ Message handlers (all 6 message types)
- ✅ Background jobs (message processor, chain sync, cleanup)
- ✅ P2P network scaffolding

### TODO

- [ ] Full libp2p integration
- [ ] Complete chain sync persistence
- [ ] Integration tests against live Aleph API
- [ ] Benchmarking vs pyaleph
- [ ] Additional chain support (Solana, Tezos)

## Building

```bash
# Development build
cargo build

# Release build
cargo build --release

# Run tests
cargo test
```

## Running

```bash
# With default config (no database)
cargo run -- --no-db

# With custom port
cargo run -- --no-db -p 8080

# With database
cargo run

# Run migrations only
cargo run -- --migrate
```

## Configuration

Configuration can be provided via:
1. TOML file (default: `config.toml`)
2. Environment variables (prefix: `ALEPH_`)

Example config:

```toml
[node]
name = "my-node"
data_dir = "./data"
log_level = "info"

[database]
url = "postgres://localhost/aleph"
max_connections = 10

[api]
host = "0.0.0.0"
port = 8080
cors_enabled = true

[chains.ethereum]
enabled = true
rpc_url = "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
contract_address = "0x27B98C76b96f7e6DD2cF4eE25AceB3c1B4412e59"
chain_id = 1
start_block = 10000000
```

## API Endpoints

Compatible with pyaleph API v0:

- `GET /health` - Health check
- `GET /api/v0/info` - Node info
- `GET /api/v0/messages.json` - List messages
- `POST /api/v0/messages` - Submit message
- `GET /api/v0/aggregates/:address.json` - Get aggregates
- `GET /api/v0/posts.json` - List posts
- `GET /api/v0/balance/:address` - Get balance
- `GET /api/v0/pricing` - Get pricing info
- `GET /api/v0/programs/:address` - List programs
- `GET /api/v0/instances/:address` - List instances

## Architecture

```
src/
├── types/       # Core data types
├── config/      # Configuration management
├── web/         # HTTP API (Axum)
├── db/          # Database layer (sqlx)
├── services/    # Business logic services
├── chains/      # Blockchain indexers
├── handlers/    # Message handlers
├── jobs/        # Background tasks
└── network/     # P2P networking
```

## License

MIT

## Credits

Port of [pyaleph](https://github.com/aleph-im/pyaleph) by Aleph.im team.
