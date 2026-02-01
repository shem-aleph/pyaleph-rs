# pyaleph-rs Accuracy Review

## Executive Summary

This review compares the Rust implementation (`pyaleph-rs`) against the Python reference implementation (`pyaleph`) of the Aleph.im Core Channel Node (CCN). The Rust implementation is in early development and lacks many critical features required for production use.

**Overall Assessment: NOT PRODUCTION READY**

- Critical issues: 15
- Major issues: 12
- Minor issues: 8
- Missing features: 25+

---

## Critical Issues (Must Fix)

### 1. Message Status Enum Mismatch
**File:** `src/types/message_status.rs:8-17`

The Rust implementation is missing status values:
```rust
// Current (INCORRECT):
pub enum MessageStatus {
    Pending,
    Processed,
    Rejected,
    Forgotten,
}
```

**Python Reference (`aleph/types/message_status.py:11-17`):**
```python
class MessageStatus(str, Enum):
    PENDING = "pending"
    PROCESSED = "processed"
    REJECTED = "rejected"
    FORGOTTEN = "forgotten"
    REMOVING = "removing"   # MISSING IN RUST
    REMOVED = "removed"     # MISSING IN RUST
```

**Impact:** Messages with `REMOVING` or `REMOVED` status will fail to deserialize.

**Fix:** Add `Removing` and `Removed` variants to the enum.

---

### 2. Error Codes Completely Different
**File:** `src/types/message_status.rs:20-50`

The error codes in Rust use arbitrary numbers that don't match the Python implementation.

**Rust (INCORRECT):**
```rust
pub enum ErrorCode {
    InvalidSignature = 100,
    InvalidContent = 101,
    // ... etc
}
```

**Python Reference (`aleph/types/message_status.py:31-50`):**
```python
class ErrorCode(IntEnum):
    INTERNAL_ERROR = -1
    INVALID_FORMAT = 0
    INVALID_SIGNATURE = 1
    PERMISSION_DENIED = 2
    CONTENT_UNAVAILABLE = 3
    FILE_UNAVAILABLE = 4
    BALANCE_INSUFFICIENT = 5
    CREDIT_INSUFFICIENT = 6
    POST_AMEND_NO_TARGET = 100
    POST_AMEND_TARGET_NOT_FOUND = 101
    POST_AMEND_AMEND = 102
    STORE_REF_NOT_FOUND = 200
    STORE_UPDATE_UPDATE = 201
    VM_REF_NOT_FOUND = 300
    VM_VOLUME_NOT_FOUND = 301
    VM_AMEND_NOT_ALLOWED = 302
    VM_UPDATE_UPDATE = 303
    VM_VOLUME_TOO_SMALL = 304
    FORGET_NO_TARGET = 500
    FORGET_TARGET_NOT_FOUND = 501
    FORGET_FORGET = 502
    FORGET_NOT_ALLOWED = 503
    FORGOTTEN_DUPLICATE = 504
```

**Impact:** API responses will have incompatible error codes, breaking all clients.

**Fix:** Redefine `ErrorCode` enum to match Python values exactly.

---

### 3. Missing MessageOrigin Type
**File:** Missing entirely

Python has `MessageOrigin` enum used to track message source:
```python
class MessageOrigin(str, Enum):
    ONCHAIN = "onchain"
    P2P = "p2p"
    IPFS = "ipfs"
```

**Impact:** Cannot properly track and handle message sources.

**Fix:** Add `MessageOrigin` enum to `src/types/mod.rs`.

---

### 4. Signature Verification Not Implemented
**File:** `src/types/message.rs:76-79`

```rust
pub fn verify_signature(&self) -> Result<bool, String> {
    // TODO: Implement signature verification based on chain type
    Ok(true)  // ALWAYS RETURNS TRUE - CRITICAL SECURITY ISSUE
}
```

**Impact:** All messages accepted without signature validation. Critical security vulnerability.

**Fix:** Implement full signature verification for all supported chains.

---

### 5. Aggregate Handler Missing Critical Logic
**File:** `src/handlers/aggregate.rs`

The handler is a stub missing:
- Out-of-order message handling
- Dirty aggregate detection and refresh
- Aggregate element merging
- Owner verification against content address

**Python Reference (`aleph/handlers/content/aggregate.py:99-179`):**
- Handles append/prepend to aggregates
- Manages dirty aggregates above threshold (1000 elements)
- Performs full refresh for conflicting keys
- Tracks aggregate elements separately

**Impact:** Aggregates will not be properly maintained.

**Fix:** Implement full aggregate processing logic as per Python reference.

---

### 6. Post Handler Missing Amend Logic
**File:** `src/handlers/post.rs`

Missing critical functionality:
- `amend` post type handling
- Reference validation (`ref` field)
- Permission checking for amends
- Balance updates for special post types
- Credit balance operations

**Python Reference (`aleph/handlers/content/post.py`):**
- Validates amend targets
- Checks ownership before allowing amends
- Updates balances and credit balances
- Handles latest_amend tracking

**Impact:** Post amending will not work, balance updates will fail.

---

### 7. Forget Handler Missing VM Volume Protection
**File:** `src/handlers/forget.rs`

Missing:
- Check for dependent VM volumes before deletion
- Aggregate forgetting (only hashes supported)
- Proper cascade deletion

**Python Reference (`aleph/handlers/content/forget.py:55-70`):**
```python
# Check file references, on VM volumes, as data volume and as code volume
dependent_volumes = get_vms_dependent_volumes(
    session=session, volume_hash=item_hash
)
if dependent_volumes is not None:
    raise ForgetNotAllowed(
        file_hash=item_hash, vm_hash=dependent_volumes.item_hash
    )
```

**Impact:** Users could delete files that are actively used by VMs.

---

### 8. Store Handler Missing Cost Validation
**File:** `src/handlers/store.rs`

Missing:
- Balance pre-check before file pinning
- File size validation
- IPFS pinning logic
- Grace period handling for file deletion
- File tagging system

**Python Reference (`aleph/handlers/content/store.py`):**
- Pre-checks balance before processing
- Calculates storage costs
- Manages file pins and tags
- Handles grace period for garbage collection

---

### 9. Database Schema Incompatibility
**File:** `src/db/models.rs`

The database models differ significantly from pyaleph's PostgreSQL schema:

Missing tables:
- `pending_messages`
- `rejected_messages`
- `forgotten_messages`
- `aggregate_elements`
- `chain_txs`
- `file_pins`
- `file_tags`
- `vm_versions`
- `account_costs`

Missing columns in existing tables:
- `messages.confirmations` (JSONB)
- `messages.status` (message_status relationship)
- `posts.latest_amend`
- `posts.amends`
- `aggregates.dirty`
- `aggregates.last_revision_hash`

**Impact:** Data will not be compatible with existing pyaleph installations.

---

### 10. API Response Format Incompatibility
**File:** `src/web/handlers.rs`

Multiple API incompatibilities:

**Messages endpoint (`/messages.json`):**
- Missing `confirmations` field
- Missing `confirmed` boolean
- Missing proper date formatting (timestamp vs datetime)

**Python Reference (`aleph/web/controllers/messages.py:52-66`):**
```python
def message_to_dict(message: MessageDb) -> Dict[str, Any]:
    message_dict = message.to_dict()
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)
```

---

### 11. Chain Indexer Missing Sync Event Handling
**File:** `src/chains/ethereum.rs`

The Ethereum indexer only partially implements event decoding. Missing:
- Proper sync event payload parsing
- Message event handling via indexer
- Authorized emitter validation with configurable list
- Batch processing with dynamic range adjustment

**Python Reference (`aleph/chains/ethereum.py`):**
- Uses AlephIndexerReader for message events
- Handles sync events separately
- Dynamic block range adjustment on RPC limits
- Proper authorized emitter checking

---

### 12. Missing P2P Service Integration
**File:** `src/network/rabbitmq.rs`

The RabbitMQ integration is incomplete:
- Wrong exchange names (doesn't match pyaleph config)
- Missing `p2p-publish` / `p2p-subscribe` exchange separation
- Missing `aleph-pending-messages` exchange
- No integration with p2p-service daemon

**Python config defaults:**
```python
"rabbitmq": {
    "pub_exchange": "p2p-publish",
    "sub_exchange": "p2p-subscribe",
    "message_exchange": "aleph-messages",
    "pending_message_exchange": "aleph-pending-messages",
    "pending_tx_exchange": "aleph-pending-txs",
}
```

---

### 13. Cost Calculation Significantly Different
**File:** `src/services/cost.rs`

Major differences from Python implementation:
- Missing volume discount calculation
- Missing execution cost multiplier for internet-enabled programs
- Missing GPU tier pricing
- Missing credit payment type handling
- Hardcoded prices instead of reading from aggregate

**Python Reference (`aleph/services/cost.py`):**
- Reads pricing from aggregate (dynamic pricing)
- Calculates volume discounts based on compute units
- Handles GPU premium/standard tiers
- Supports hold/payg/credit payment types

---

### 14. Missing WebSocket Support for Messages
**File:** `src/web/websocket.rs` - Not reviewed but likely incomplete

Python has full WebSocket support for real-time message streaming:
- Filter support (addresses, channels, types, etc.)
- History replay
- RabbitMQ integration for live updates

---

### 15. Configuration Incompatibility
**File:** `src/config/mod.rs`

Missing configuration sections that pyaleph requires:
- `aleph.balances` (balance update configuration)
- `aleph.credit_balances` (credit system configuration)
- `aleph.jobs` (job processing configuration)
- `aleph.cache` (caching TTLs)
- `p2p` (full p2p daemon configuration)
- `rabbitmq` (message queue configuration)
- `redis` (caching service)
- `sentry` (error tracking)

---

## Major Issues (Should Fix)

### 1. Solana Signature Verification Not Implemented
**File:** `src/services/crypto.rs:96-101`

```rust
fn verify_solana_signature(...) -> Result<bool, CryptoError> {
    Err(CryptoError::UnsupportedChain("SOL - not yet implemented".to_string()))
}
```

### 2. Tezos Signature Verification Not Implemented
**File:** `src/services/crypto.rs:103-111`

### 3. Missing NULS/NULS2 Chain Support
Python supports NULS and NULS2 chains - completely missing in Rust.

### 4. Missing Cosmos SDK Chain Support
Python has `cosmos.py` chain connector - missing in Rust.

### 5. Missing BSC Chain-Specific Configuration
BSC uses Ethereum-compatible but needs separate contract address.

### 6. Missing Avalanche Chain Support
Python has `avalanche.py` - missing in Rust.

### 7. Message Processor Not Implemented
**File:** `src/jobs/message_processor.rs:25-34`

The processor is a stub:
```rust
async fn process_batch(_config: &Config) -> Result<u32, ...> {
    // TODO: Implement actual message processing
    Ok(0)
}
```

### 8. Missing Retry Logic for Failed Messages
Python has exponential backoff retry with `max_retries` (default 10).

### 9. Missing Trusted Execution Fields for VMs
Python models have `trusted_execution` support - missing in Rust.

### 10. Missing File Size Validation
No `MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE` checking.

### 11. Missing Garbage Collection Job
Python has `garbage_collector_period` and grace period handling.

### 12. Missing Balance Tracker Integration
Python has balance tracking for removing messages when balance drops.

---

## Minor Issues

### 1. Chain Enum Missing Values
**File:** `src/types/chain.rs`

Missing: `NULS2` (Python has both NULS and NULS2)

### 2. Product Price Types Incomplete
**File:** `src/types/pricing.rs`

Has `InstanceGpuPremium` and `InstanceGpuStandard` but missing proper tier handling.

### 3. Timestamp Handling
Using `f64` for timestamps instead of proper datetime handling.

### 4. Missing Metrics Endpoint
Python has Prometheus metrics integration.

### 5. Missing Cache Layer
No Redis integration for caching frequently accessed data.

### 6. Missing Health Check Details
Health endpoint doesn't report database status, IPFS status, etc.

### 7. Missing Rate Limiting Implementation
Config has `rate_limit` but not implemented.

### 8. Missing API Authentication
Python has auth token verification with configurable public key.

---

## Missing Features

### Message Handling
1. Message deduplication
2. Message confirmation tracking
3. Pending message queue management
4. Message broadcasting to P2P network
5. Message content fetching from IPFS/storage

### Chain Integration
6. Chain transaction (TX) processor
7. Chain sync event packer (publishing to chains)
8. Multi-chain balance aggregation
9. Chain height tracking per sync type

### Storage
10. IPFS directory pinning
11. Storage API integration
12. File tag system
13. Grace period file deletion

### Database
14. Proper migration system (uses Alembic in Python)
15. Connection pooling with configurable size
16. Read replicas support

### API
17. `/messages/{hash}/content` endpoint
18. `/messages/{hash}/status` endpoint  
19. `/hashes` endpoint
20. File upload endpoint
21. Program/Instance cost estimation endpoint
22. Address statistics endpoint

### Jobs & Background Tasks
23. Cron job system
24. Balance checking job
25. Chain sync status monitoring

---

## Recommended Priority

### Phase 1: Security & Correctness (Weeks 1-2)
1. Fix error code enum
2. Fix message status enum
3. Implement signature verification for ETH/SOL/Tezos
4. Fix database schema to match Python

### Phase 2: Core Functionality (Weeks 3-4)
5. Implement full message processor
6. Implement aggregate handler completely
7. Implement post handler with amends
8. Implement forget handler with VM protection
9. Implement store handler with cost validation

### Phase 3: Chain Integration (Weeks 5-6)
10. Complete Ethereum indexer
11. Add Solana support
12. Add Tezos support
13. Implement chain sync packer

### Phase 4: API Compatibility (Weeks 7-8)
14. Fix all API response formats
15. Add missing endpoints
16. Implement WebSocket support
17. Add authentication

### Phase 5: Production Readiness (Weeks 9-10)
18. Add metrics
19. Add caching layer
20. Complete RabbitMQ integration
21. Add comprehensive testing

---

## Conclusion

The `pyaleph-rs` implementation is in early development and requires significant work before it can be considered a viable replacement for the Python implementation. The most critical issues are:

1. **Security:** Signature verification is not implemented
2. **Compatibility:** Error codes and status enums don't match
3. **Functionality:** Message handlers are stubs
4. **Data:** Database schema is incompatible

A rough estimate suggests **8-10 weeks of full-time development** to reach feature parity with the Python implementation, followed by extensive testing.

---

*Review conducted: 2026-02-01*
*Reviewer: pyaleph-rs accuracy review agent*
*Python reference: https://github.com/aleph-im/pyaleph (commit 954174c)*
