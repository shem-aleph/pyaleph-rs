# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Rust port of the Python [pyaleph](https://github.com/aleph-im/pyaleph) Aleph.im Core Channel Node (CCN). Single binary that replaces the Python implementation with full API compatibility. The crate name is `aleph_core` and the binary is `aleph-core`.

## Build & Development Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo check                    # Type-check without building
cargo test                     # Run unit tests (no external deps needed)
cargo test message_            # Run tests matching pattern
cargo test --ignored           # Run integration tests (need network access)
cargo clippy -- -D warnings    # Lint
cargo fmt                      # Format
cargo fmt --check              # Check formatting
```

Running locally:
```bash
./target/release/aleph-core --no-db -p 8080              # API-only mode (no Postgres)
./target/release/aleph-core -c config.toml                # With database
./target/release/aleph-core --migrate                     # Run migrations only
./target/release/aleph-core --indexer-sync -c config.toml # Sync from multichain indexer
```

Docker: `docker compose up -d` brings up the full stack (Postgres, Redis, RabbitMQ, IPFS, p2p-service, pyaleph-rs).

## Architecture

### Message Pipeline (the core flow)

Messages enter the system through three paths:
1. **HTTP API** (`POST /api/v0/messages`) — direct submission
2. **P2P Consumer** (`jobs/p2p_consumer.rs`) — RabbitMQ ← p2p-service ← libp2p GossipSub
3. **Chain Sync** (`jobs/chain_sync.rs`) — blockchain indexing (Ethereum, Avalanche, BSC, Solana, Tezos) or multichain indexer

All paths insert into the `pending_messages` table. The **Message Processor** (`jobs/message_processor.rs`) polls this table, validates signatures, fetches content if needed, dispatches to the appropriate handler, and writes results to derived tables.

### Handler Dispatch

`handlers/mod.rs` defines the `MessageHandler` trait. Each of the 6 message types has its own handler:
- `AggregateHandler` → upserts into `aggregates` table (key-value pairs per address)
- `PostHandler` → inserts into `posts` table (supports amend via `ref_`)
- `StoreHandler` → manages `file_pins` (IPFS content pinning)
- `ProgramHandler` / `InstanceHandler` → VM allocation records
- `ForgetHandler` → marks messages as forgotten, cascades to derived tables

The `HandlerContext` struct provides all services (DB, crypto, IPFS, storage) to handlers. The `Database` trait in `handlers/mod.rs` abstracts DB operations for testability.

### Key Modules

- **`types/`** — Core domain types: `Message`, `Chain`, `MessageType`, `ProcessingStatus`, `ItemType`. `Message` is the fundamental unit of the Aleph protocol.
- **`config/mod.rs`** — Single large config file with `Config` struct. Loaded from TOML + env vars (`ALEPH__` prefix with `__` separator). Full pyaleph config compatibility.
- **`web/`** — Axum HTTP server. `routes.rs` defines all endpoints. `handlers.rs` contains request handlers. `state.rs` has `AppState` (shared via `Arc<AppState>`). `websocket.rs` for real-time subscriptions.
- **`db/`** — PostgreSQL via sqlx (raw queries, not an ORM). `migrations.rs` runs schema creation inline. `query_builder.rs` for dynamic query construction. `accessors.rs` for typed DB access. SQL migration files in `migrations/`.
- **`services/`** — `CryptoService` (signature verification: Ed25519, secp256k1, EIP-191), `CostService` (pricing), `IpfsService`, `RedisService` (with in-memory fallback), `StorageService`, `Metrics` (Prometheus), `peers.rs` (HTTP peer discovery + content fetching), `content_fetch.rs` (fetches missing message content from peers/IPFS).
- **`chains/`** — Blockchain indexers. `indexer.rs` for multichain GraphQL indexer. `rpc_sync.rs` for direct `eth_getLogs`. `ethereum.rs`, `solana.rs`, `tezos.rs`, etc. `tx_packer.rs` packs sync messages for on-chain submission.
- **`jobs/`** — Background tasks spawned via `tokio::spawn`. `JobManager` orchestrates them. `backfill.rs` populates derived tables on startup. `p2p_consumer.rs` bridges RabbitMQ to pending_messages.
- **`network/`** — P2P networking with TCP connections using length-prefixed JSON framing. `rabbitmq.rs` (RabbitMQ AMQP integration), `peer.rs` (peer management), `protocol.rs` (P2P protocol definitions).
- **`storage/`** — Local disk-backed content cache with two-level directory sharding (ab/cd/hash). Acts as read-through cache in front of IPFS.
- **`schemas/`** — Message content schema validation. Validates required fields and types for all 6 message types before handler dispatch.
- **`permissions.rs`** — Security aggregate authorization (delegated posting).

### Database Schema

Migrations are in `src/db/migrations.rs` (inline SQL, run on startup) and `migrations/*.sql`. Key tables:
- `messages` — all confirmed messages (item_hash PK)
- `pending_messages` — queue for unprocessed messages
- `aggregates` — key-value store per address (JSONB content)
- `posts` — post messages with content
- `file_pins` — IPFS file pin tracking
- `programs`, `instances` — VM allocations
- `balances`, `credit_balances` — token balances
- `chain_txs` — blockchain transaction records
- `chain_sync_state` — indexer progress tracking

### API Structure

Routes defined in `web/routes.rs`. Three API versions:
- `/api/v0/*` — main API (pyaleph compatible)
- `/api/v1/*` — v1 endpoints (posts, address stats with pagination)
- `/api/ws0/messages` — WebSocket (pyaleph compatibility path)
- `/_internal/*` — metrics, status, debug

Legacy routes at root level for backwards compatibility.

## Conventions

- **Error handling**: `anyhow::Result` for application code, `thiserror` for library errors (see `HandlerError` in `handlers/mod.rs`).
- **Async runtime**: Tokio with `#[tokio::main]` and `#[tokio::test]`.
- **Commit messages**: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`).
- **Branch naming**: `feat/`, `fix/`, `docs/`, `refactor/`, `test/`.
- **Config env vars**: `ALEPH__SECTION__KEY` format (double underscore separator).
- **Reference implementation**: Python pyaleph paths are noted in doc comments (e.g., `//! Reference: aleph/jobs/process_pending_messages.py`).

## External Dependencies

Runtime services: PostgreSQL 14+, Redis (optional), RabbitMQ (optional, for P2P), IPFS/Kubo (optional, for content storage). The node can run in API-only mode (`--no-db`) with no external services.
