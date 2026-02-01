# Architecture Overview

pyaleph-rs is a Rust implementation of the Aleph.im Core Node, designed for high performance and reliability.

## System Architecture

```
                                    ┌─────────────────┐
                                    │   Blockchain    │
                                    │   Networks      │
                                    │ (ETH/AVAX/BSC)  │
                                    └────────┬────────┘
                                             │
                                             ▼
┌─────────────────┐              ┌─────────────────────┐
│   WebSocket     │◄────────────►│   Chain Indexers    │
│   Clients       │              │   (Ethereum, etc)   │
└────────┬────────┘              └──────────┬──────────┘
         │                                  │
         ▼                                  ▼
┌─────────────────┐              ┌─────────────────────┐
│   HTTP API      │◄────────────►│  Message Processor  │
│   (Axum)        │              │  (Background Jobs)  │
└────────┬────────┘              └──────────┬──────────┘
         │                                  │
         ▼                                  ▼
┌─────────────────────────────────────────────────────┐
│                   Service Layer                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │  Crypto  │ │  Message │ │   Cost   │ │ Storage│ │
│  │  Service │ │  Service │ │  Service │ │ Service│ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
└────────────────────────┬────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  PostgreSQL │  │    Redis    │  │    IPFS     │
│  (Primary)  │  │   (Cache)   │  │  (Storage)  │
└─────────────┘  └─────────────┘  └─────────────┘
```

## Component Details

### Web API (`src/web/`)

Built with [Axum](https://github.com/tokio-rs/axum), a fast async web framework.

- **handlers.rs** - Request handlers for all endpoints
- **routes.rs** - Route definitions and middleware
- **state.rs** - Shared application state
- **websocket.rs** - WebSocket support for real-time subscriptions
- **middleware.rs** - CORS, logging, metrics

### Message Handlers (`src/handlers/`)

Each message type has a dedicated handler:

| Handler | Message Type | Responsibility |
|---------|--------------|----------------|
| `aggregate.rs` | AGGREGATE | Key-value store with deep merge |
| `post.rs` | POST | Content posts with amend support |
| `store.rs` | STORE | File storage references |
| `program.rs` | PROGRAM | VM program definitions |
| `instance.rs` | INSTANCE | VM instance lifecycle |
| `forget.rs` | FORGET | Content deletion requests |

### Chain Indexers (`src/chains/`)

Blockchain indexers for message ingestion:

- **ethereum.rs** - Ethereum and EVM-compatible chains
- **solana.rs** - Solana program indexing
- **tezos.rs** - Tezos contract indexing
- **tx_packer.rs** - Transaction packing for sync messages
- **abi.rs** - Ethereum ABI encoding/decoding

### Services (`src/services/`)

Core business logic:

| Service | Purpose |
|---------|---------|
| `crypto.rs` | Signature verification (ed25519, secp256k1, EIP-191) |
| `message.rs` | Message validation and processing |
| `cost.rs` | Pricing calculations |
| `storage.rs` | Local file storage |
| `ipfs.rs` | IPFS integration |
| `redis.rs` | Cache layer |
| `metrics.rs` | Prometheus metrics |

### Background Jobs (`src/jobs/`)

Async workers for various tasks:

- **message_processor.rs** - Process pending messages
- **chain_sync.rs** - Sync blockchain state
- **balance_tracker.rs** - Track ALEPH balances
- **garbage_collector.rs** - Clean orphaned files
- **cron.rs** - Scheduled tasks

### Database (`src/db/`)

PostgreSQL with sqlx:

- **migrations.rs** - Schema migrations
- **models.rs** - Database models
- **accessors.rs** - Query functions
- **query_builder.rs** - Safe parameterized queries

## Data Flow

### Message Submission

```
1. Client submits message to POST /api/v0/messages
2. Handler validates JSON structure
3. CryptoService verifies signature
4. MessageService validates content
5. Message stored in pending_messages table
6. MessageProcessor picks up message
7. Type-specific handler processes message
8. Message moved to messages table
9. WebSocket clients notified
10. P2P broadcast via RabbitMQ
```

### Chain Indexing

```
1. ChainSyncJob polls for new blocks
2. EthereumIndexer fetches logs from contract
3. ABI decoder parses event data
4. Messages extracted and validated
5. New messages stored in pending_messages
6. Sync state updated in chain_sync table
```

### Message Query

```
1. Client requests GET /api/v0/messages.json
2. Handler parses query parameters
3. QueryBuilder constructs safe SQL
4. Database query executed
5. Results serialized to JSON
6. Response with pagination metadata
```

## Concurrency Model

- **Tokio runtime** - Async I/O and task scheduling
- **Connection pooling** - sqlx pool for PostgreSQL
- **Worker threads** - Configurable message processors
- **Broadcast channels** - WebSocket message distribution

## Security Considerations

1. **SQL Injection Prevention**
   - Parameterized queries via QueryBuilder
   - No string interpolation in SQL

2. **Signature Verification**
   - All messages cryptographically signed
   - Multiple algorithm support

3. **Input Validation**
   - Request body size limits
   - JSON schema validation
   - Address format validation

4. **Rate Limiting**
   - Per-IP rate limits (optional)
   - Configurable thresholds

## Performance Optimizations

1. **Zero-copy parsing** where possible
2. **Connection pooling** for database
3. **Redis caching** for hot data
4. **Batch processing** for chain indexing
5. **Async everything** - no blocking I/O

## Monitoring

### Prometheus Metrics

Available at `/_internal/metrics`:

- `aleph_messages_total` - Total messages by type
- `aleph_messages_pending` - Pending message count
- `aleph_api_requests_total` - API request count
- `aleph_api_request_duration_seconds` - Request latency histogram
- `aleph_chain_sync_height` - Current sync height per chain
- `aleph_db_connections` - Database pool stats

### Health Checks

- `GET /health` - Basic health check
- `GET /_internal/status` - Detailed status

## Extension Points

### Adding a New Chain

1. Create `src/chains/newchain.rs`
2. Implement `ChainIndexer` trait
3. Add config section to `src/config/mod.rs`
4. Register in `src/chains/mod.rs`

### Adding a New Message Type

1. Add type to `MessageType` enum in `src/types/message.rs`
2. Create handler in `src/handlers/`
3. Register in `src/handlers/mod.rs`
4. Add database migration if needed

### Adding a New API Endpoint

1. Add handler function in `src/web/handlers.rs`
2. Add route in `src/web/routes.rs`
3. Update API documentation
