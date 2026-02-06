# Changelog

All notable changes to pyaleph-rs will be documented in this file.

## [0.1.0] - 2026-02-01

### Added
- Complete Rust implementation of Aleph Core Channel Node
- Full API compatibility with Python pyaleph v0 endpoints
- PostgreSQL database with automatic migrations
- Redis caching with in-memory fallback
- RabbitMQ integration for P2P message passing
- Multichain indexer sync via `multichain.api.aleph.cloud` GraphQL API
- Traditional chain sync via direct RPC indexing
- Support for all 6 message types:
  - AGGREGATE - Key-value state updates
  - POST - Immutable content posts
  - STORE - File storage references  
  - PROGRAM - Program deployments
  - INSTANCE - VM instance management
  - FORGET - Content deletion requests
- Signature verification for Ed25519, secp256k1, EIP-191
- WebSocket real-time subscriptions
- Prometheus metrics endpoint
- Background jobs: message processor, chain sync, garbage collector, balance tracker
- IPv6 dual-stack support (binds to `::` by default)

### Performance
- ~10x faster message processing vs Python
- ~50% less memory usage
- Single binary deployment (~10MB)

### API Compatibility
- `limit` and `pagination` parameters both supported
- `msgType` and `msgTypes` parameters both supported
- Full pyaleph response format compatibility

## [Unreleased]

### Added
- Ethereum sync hash queuing - IPFS batch CIDs from sync events now queued to pending_messages for processing
- Tezos signature acceptance - tz1/tz2/tz3 addresses now accepted (full verification pending)
- Real TCP P2P networking with length-prefixed JSON framing
- Local storage module with disk-backed content cache and two-level directory sharding
- Schema validation module for message content (validates all 6 message types before handler dispatch)
- Monitor dashboard with real-time stats (`/api/v0/monitor`)

### Fixed
- Orphaned file pins now cleaned up by garbage collector
- Query filtering for accessor functions

### Planned
- Full balance tracking from staking contracts
- Metrics dashboard
- Docker compose deployment template

---

pyaleph-rs is a Rust port of [pyaleph](https://github.com/aleph-im/pyaleph).
