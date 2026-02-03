# pyaleph-rs TODO

## High Priority (Spec Compliance)

### FORGET Handler Gaps (from Olivier)
- [ ] Add `removing` status transition before deleting content
- [ ] Support forgetting by `aggregates` list (not just `hashes`)
- [ ] Cascading forget — when forgetting a message, also forget related content
- [ ] Permission checks — verify target is `PROCESSED` or `REMOVING` before allowing forget
- [ ] Track `forgotten_by` list (append when already forgotten)

### Message Statuses
- [x] `pending` / `processed` / `rejected` — implemented
- [x] `forgotten` / `removing` / `removed` — types exist
- [ ] Actually use `removing` → `removed` flow in FORGET handler

### Signature Verification
- [x] Correct verification buffer format (`{chain}\n{sender}\n{type}\n{item_hash}`)
- [x] EVM chains (ETH + all L2s)
- [x] Solana JSON signature format
- [ ] Base64 signatures (some Cosmos chains starting with 'H', 'G')
- [ ] Tezos (needs public key, not just address)

## Medium Priority (Performance)

### Sync Optimizations
- [x] Parallel IPFS fetches (100 concurrent)
- [x] Parallel message processing by address
- [x] Batch DB operations
- [ ] Skip content fetch for storage messages during initial sync (fetch on-demand)
- [ ] Tune PostgreSQL for bulk inserts (increase work_mem, etc.)

### Database
- [ ] Aggregate handler — connect to DB (currently "No database configured")
- [ ] Store handler — connect to DB
- [ ] Add indexes for common query patterns

## Low Priority (Nice to Have)

- [ ] WebSocket subscriptions
- [ ] Metrics endpoint improvements
- [ ] RabbitMQ integration (currently failing to connect)
- [ ] Multi-chain sync (currently ETH only via indexer)

## Completed

- [x] Signature verification with correct buffer format
- [x] All EVM L2 chains support
- [x] Parallel message processing (safe by address)
- [x] Batch inserts and deletes
- [x] Posts table population
- [x] trusted_source flag for indexer messages
- [x] Deduplication in batch inserts

---
*Last updated: 2026-02-02*
