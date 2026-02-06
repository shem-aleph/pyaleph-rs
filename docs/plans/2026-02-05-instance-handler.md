# Instance & Program Handler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fully implement INSTANCE (and PROGRAM) message processing to match pyaleph behavior — validation, DB storage, volume handling, amendments, cost calculation, balance checking, and permission verification.

**Architecture:** Both INSTANCE and PROGRAM share a common VM processing pattern (pyaleph uses a single `VmMessageHandler` for both). We keep separate Rust handler structs but extract shared logic into a `vm_common` module. The DB schema is enriched with proper columns matching pyaleph's model, plus a `vm_machine_volumes` table for volume storage. A migration SQL script handles the dev server's existing tables.

**Tech Stack:** Rust, sqlx (raw SQL), serde, rust_decimal, tokio, existing CostService + permissions module.

**Reference implementation:** `../pyaleph/src/aleph/handlers/content/vm.py`, `../pyaleph/src/aleph/db/accessors/vms.py`, `../pyaleph/src/aleph/db/models/vms.py`

---

## Critical Bug: InstanceContent Type Doesn't Match Real Data

The current `InstanceContent` struct (types/message.rs:217-241) has **wrong field names** that don't match actual on-chain instance messages:

| Real JSON field | Current Rust field | Issue |
|---|---|---|
| `resources.memory` | `memory` (top-level) | Nested vs flat |
| `resources.vcpus` | `vcpus` (top-level) | Nested vs flat |
| `resources.seconds` | (missing) | Not captured |
| `authorized_keys` | `ssh_keys` | Wrong name |
| `environment` | (missing) | Entire object missing |
| `metadata` | (missing) | Entire object missing |
| `replaces` | (missing) | Amendment support missing |
| `requirements` | (missing) | Node/CPU targeting missing |
| `rootfs.parent.ref` | `rootfs.parent.ref_` | Missing `#[serde(rename)]` |

Similarly, `RootfsParent.ref_`, `RuntimeInfo.ref_`, `CodeInfo.ref_`, and `VolumeSource::Immutable.ref_` all lack `#[serde(rename = "ref")]` since `ref` is a Rust keyword.

---

### Task 1: Fix serde renames for `ref` fields

All structs using `ref_` as a field name need `#[serde(rename = "ref")]` since the JSON uses `ref`.

**Files:**
- Modify: `src/types/message.rs:246` (RuntimeInfo.ref_)
- Modify: `src/types/message.rs:257` (CodeInfo.ref_)
- Modify: `src/types/message.rs:275` (VolumeSource::Immutable.ref_)
- Modify: `src/types/message.rs:289` (RootfsParent.ref_)

**Step 1: Add `#[serde(rename = "ref")]` to all ref_ fields**

```rust
// RootfsParent (line 289)
#[serde(rename = "ref")]
pub ref_: String,

// RuntimeInfo (line 246)
#[serde(rename = "ref")]
pub ref_: String,

// CodeInfo (line 257)
#[serde(rename = "ref")]
pub ref_: String,

// VolumeSource::Immutable (line 275)
Immutable { #[serde(rename = "ref")] ref_: String, use_latest: bool },
```

**Step 2: Add a unit test for deserialization from real JSON**

In `src/types/message.rs` tests:

```rust
#[test]
fn test_rootfs_parent_deser() {
    let json = r#"{"ref": "abc123", "use_latest": true}"#;
    let parent: RootfsParent = serde_json::from_str(json).unwrap();
    assert_eq!(parent.ref_, "abc123");
    assert!(parent.use_latest);
}
```

**Step 3: Run tests**

Run: `cargo test test_rootfs_parent_deser`

**Step 4: Commit**

```
git commit -m "fix: add serde rename for ref fields to match JSON format"
```

---

### Task 2: Fix InstanceContent to match real on-chain format

The struct needs `resources`, `environment`, `metadata`, `authorized_keys`, `replaces`, and `requirements` fields matching the actual JSON.

**Files:**
- Modify: `src/types/message.rs:217-241` (InstanceContent + new sub-structs)

**Step 1: Replace InstanceContent and add supporting structs**

```rust
/// VM resource requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmResources {
    pub memory: u32,
    pub vcpus: u32,
    #[serde(default = "default_seconds")]
    pub seconds: u32,
}

fn default_seconds() -> u32 { 30 }

/// VM environment settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmEnvironment {
    #[serde(default)]
    pub reproducible: bool,
    #[serde(default)]
    pub internet: bool,
    #[serde(default)]
    pub aleph_api: bool,
    #[serde(default)]
    pub shared_cache: bool,
    #[serde(default)]
    pub hypervisor: Option<String>,
}

/// VM node/CPU requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VmRequirements {
    #[serde(default)]
    pub cpu: Option<CpuRequirements>,
    #[serde(default)]
    pub node: Option<NodeRequirements>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuRequirements {
    pub architecture: Option<String>,
    pub vendor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRequirements {
    pub node_hash: Option<String>,
    pub address_regex: Option<String>,
    pub owner: Option<String>,
}

/// Instance (VM) content — matches on-chain JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceContent {
    pub address: Address,
    #[serde(default = "default_true")]
    pub allow_amend: bool,
    pub rootfs: RootfsInfo,
    pub resources: VmResources,
    #[serde(default)]
    pub environment: Option<VmEnvironment>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub variables: Option<Value>,
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    #[serde(default)]
    pub authorized_keys: Vec<String>,
    #[serde(default)]
    pub payment: Option<PaymentInfo>,
    #[serde(default)]
    pub requirements: Option<VmRequirements>,
    /// For amendments — references the original instance item_hash
    #[serde(default)]
    pub replaces: Option<String>,
    pub time: Timestamp,
}

fn default_true() -> bool { true }
```

Also fix `ProgramContent` similarly:

```rust
/// Program (serverless function) content — matches on-chain JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramContent {
    pub address: Address,
    #[serde(default = "default_true")]
    pub allow_amend: bool,
    pub runtime: RuntimeInfo,
    pub code: CodeInfo,
    pub resources: VmResources,
    #[serde(default)]
    pub environment: Option<VmEnvironment>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub variables: Option<Value>,
    #[serde(default)]
    pub volumes: Vec<VolumeInfo>,
    #[serde(default)]
    pub payment: Option<PaymentInfo>,
    #[serde(default)]
    pub requirements: Option<VmRequirements>,
    #[serde(default)]
    pub replaces: Option<String>,
    #[serde(default)]
    pub data: Option<DataInfo>,
    #[serde(default)]
    pub export: Option<ExportInfo>,
    pub time: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataInfo {
    #[serde(rename = "ref")]
    pub ref_: String,
    pub use_latest: bool,
    #[serde(default)]
    pub mount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportInfo {
    pub encoding: Option<String>,
}
```

**Step 2: Add deserialization test using real API data**

```rust
#[test]
fn test_instance_content_real_data() {
    let json = r#"{
        "address": "0xf594985F5C271005a4dA1c4F107AB42a7166aEF7",
        "allow_amend": false,
        "authorized_keys": ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5"],
        "environment": {"reproducible": false, "internet": true, "aleph_api": true, "shared_cache": false, "hypervisor": "qemu"},
        "metadata": {"name": "SV-1"},
        "payment": {"chain": "ETH", "type": "hold"},
        "resources": {"memory": 8192, "seconds": 30, "vcpus": 4},
        "rootfs": {"parent": {"ref": "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717", "use_latest": true}, "persistence": "host", "size_mib": 81920},
        "time": 1770280454.236,
        "volumes": []
    }"#;
    let content: InstanceContent = serde_json::from_str(json).unwrap();
    assert_eq!(content.resources.memory, 8192);
    assert_eq!(content.resources.vcpus, 4);
    assert_eq!(content.authorized_keys.len(), 1);
    assert_eq!(content.rootfs.parent.ref_, "b6ff5c3a8205d1ca4c7c3369300eeafff498b558f71b851aa2114afd0a532717");
    assert!(!content.allow_amend);
}

#[test]
fn test_instance_content_with_requirements() {
    let json = r#"{
        "address": "0x25020a5F992a70E61686D21A473f464d86BaF9BD",
        "allow_amend": false,
        "authorized_keys": [],
        "environment": {"aleph_api": true, "hypervisor": "qemu", "internet": true, "reproducible": false, "shared_cache": false},
        "payment": {"chain": "ETH", "type": "credit"},
        "requirements": {"node": {"node_hash": "d02cc93b18e23f62556cc574fa3696b350cae36e760c43186cbb866c6677c628"}},
        "resources": {"memory": 2048, "seconds": 30, "vcpus": 2},
        "rootfs": {"parent": {"ref": "abc123", "use_latest": true}, "persistence": "host", "size_mib": 20480},
        "time": 1770217145.481,
        "volumes": []
    }"#;
    let content: InstanceContent = serde_json::from_str(json).unwrap();
    assert!(content.requirements.is_some());
    assert_eq!(content.requirements.unwrap().node.unwrap().node_hash.unwrap(), "d02cc93b18e23f62556cc574fa3696b350cae36e760c43186cbb866c6677c628");
}

#[test]
fn test_instance_content_with_volumes() {
    let json = r#"{
        "address": "0x8dE1089DaF0AA tried3f74",
        "allow_amend": false,
        "authorized_keys": [],
        "resources": {"memory": 2048, "seconds": 30, "vcpus": 1},
        "rootfs": {"parent": {"ref": "abc", "use_latest": true}, "persistence": "host", "size_mib": 20480},
        "time": 1768920524.78,
        "volumes": [{"comment": "persist one", "mount": "/mnt/persist-one", "name": "persist-one", "persistence": "host", "size_mib": 30000}]
    }"#;
    let content: InstanceContent = serde_json::from_str(json).unwrap();
    assert_eq!(content.volumes.len(), 1);
    assert_eq!(content.volumes[0].mount, "/mnt/persist-one");
}
```

**Step 3: Fix all handler code that references old field names**

Update `src/handlers/instance.rs` and `src/handlers/program.rs` to use `content.resources.memory` instead of `content.memory`, etc.

**Step 4: Run tests**

Run: `cargo test test_instance_content`

**Step 5: Commit**

```
git commit -m "fix: InstanceContent and ProgramContent types to match on-chain JSON format"
```

---

### Task 3: Migrate DB schema — enrich instances table and add vm_machine_volumes

The current `instances` table is too sparse. We need columns matching pyaleph's VmBaseDb + VmInstanceDb, and a separate volumes table.

**Files:**
- Modify: `src/db/migrations.rs` — update `create_instances_table`, `create_programs_table`, `create_vm_versions_table`, add `create_vm_machine_volumes_table`
- Create: `migrations/003_enrich_vm_tables.sql` — ALTER TABLE migration for dev server

**Step 1: Write the migration SQL file for the dev server**

File: `migrations/003_enrich_vm_tables.sql`

```sql
-- Enrich instances table to match pyaleph VmInstanceDb
ALTER TABLE instances ADD COLUMN IF NOT EXISTS replaces VARCHAR(128);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS environment_reproducible BOOLEAN DEFAULT FALSE;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS environment_internet BOOLEAN DEFAULT TRUE;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS environment_aleph_api BOOLEAN DEFAULT TRUE;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS environment_shared_cache BOOLEAN DEFAULT FALSE;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS environment_hypervisor VARCHAR(20);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS resources_seconds INTEGER DEFAULT 30;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS variables JSONB;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS authorized_keys JSONB;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS rootfs_use_latest BOOLEAN DEFAULT TRUE;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS rootfs_persistence VARCHAR(20);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS rootfs_size_mib INTEGER;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS cpu_architecture VARCHAR(20);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS cpu_vendor VARCHAR(50);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS node_owner VARCHAR(256);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS node_address_regex VARCHAR(256);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS node_hash VARCHAR(128);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS payment_receiver VARCHAR(256);
ALTER TABLE instances ADD COLUMN IF NOT EXISTS time DOUBLE PRECISION;
CREATE INDEX IF NOT EXISTS idx_instances_replaces ON instances(replaces) WHERE replaces IS NOT NULL;

-- Enrich programs table similarly
ALTER TABLE programs ADD COLUMN IF NOT EXISTS replaces VARCHAR(128);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS environment_reproducible BOOLEAN DEFAULT FALSE;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS environment_internet BOOLEAN DEFAULT TRUE;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS environment_aleph_api BOOLEAN DEFAULT TRUE;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS environment_shared_cache BOOLEAN DEFAULT FALSE;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS environment_hypervisor VARCHAR(20);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS resources_seconds INTEGER DEFAULT 30;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS variables JSONB;
ALTER TABLE programs ADD COLUMN IF NOT EXISTS payment_type VARCHAR(20);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS payment_chain VARCHAR(10);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS cpu_architecture VARCHAR(20);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS node_hash VARCHAR(128);
ALTER TABLE programs ADD COLUMN IF NOT EXISTS time DOUBLE PRECISION;
CREATE INDEX IF NOT EXISTS idx_programs_replaces ON programs(replaces) WHERE replaces IS NOT NULL;

-- Fix vm_versions to match pyaleph (vm_hash + current_version + last_updated)
-- The existing schema has (item_hash, original_hash, version, owner)
-- pyaleph uses (vm_hash PK, owner, current_version, last_updated)
-- We need to migrate to match:
ALTER TABLE vm_versions RENAME COLUMN item_hash TO vm_hash;
ALTER TABLE vm_versions ADD COLUMN IF NOT EXISTS current_version VARCHAR(128);
ALTER TABLE vm_versions ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ;
-- Backfill: current_version = original_hash for existing rows
UPDATE vm_versions SET current_version = original_hash WHERE current_version IS NULL;
UPDATE vm_versions SET last_updated = created_at WHERE last_updated IS NULL;

-- Create vm_machine_volumes table for instance/program volumes
CREATE TABLE IF NOT EXISTS vm_machine_volumes (
    id BIGSERIAL PRIMARY KEY,
    vm_hash VARCHAR(128) NOT NULL,
    volume_type VARCHAR(20) NOT NULL,  -- 'immutable', 'ephemeral', 'persistent'
    comment TEXT,
    mount VARCHAR(256),
    size_mib INTEGER,
    ref VARCHAR(128),
    use_latest BOOLEAN,
    persistence VARCHAR(20),
    name VARCHAR(256),
    parent_ref VARCHAR(128),
    parent_use_latest BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_vm_volumes_vm_hash ON vm_machine_volumes(vm_hash);
```

**Step 2: Update `create_instances_table` in migrations.rs for fresh installs**

Replace the current function with the enriched schema (all columns from step 1 as CREATE TABLE).

**Step 3: Update `create_programs_table` similarly**

**Step 4: Update `create_vm_versions_table` to match pyaleph**

```sql
CREATE TABLE IF NOT EXISTS vm_versions (
    vm_hash VARCHAR(128) PRIMARY KEY,
    owner VARCHAR(256) NOT NULL,
    current_version VARCHAR(128) NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT NOW()
)
```

**Step 5: Add `create_vm_machine_volumes_table` to migrations.rs**

```rust
async fn create_vm_machine_volumes_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS vm_machine_volumes (
            id BIGSERIAL PRIMARY KEY,
            vm_hash VARCHAR(128) NOT NULL,
            volume_type VARCHAR(20) NOT NULL,
            comment TEXT,
            mount VARCHAR(256),
            size_mib INTEGER,
            ref VARCHAR(128),
            use_latest BOOLEAN,
            persistence VARCHAR(20),
            name VARCHAR(256),
            parent_ref VARCHAR(128),
            parent_use_latest BOOLEAN
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}
```

Add call to `run_migrations()` and add index.

**Step 6: Run migration on dev server**

```bash
ssh root@2a01:240:ad00:2503:3:d785:ba28:c781 \
  "PGPASSWORD=aleph psql -U aleph -h localhost -d aleph -f -" < migrations/003_enrich_vm_tables.sql
```

**Step 7: Verify**

```bash
ssh root@... "PGPASSWORD=aleph psql -U aleph -h localhost -d aleph -c '\d instances'"
```

**Step 8: Compile check**

Run: `cargo check`

**Step 9: Commit**

```
git commit -m "feat: enrich VM tables schema to match pyaleph (instances, programs, vm_versions, vm_machine_volumes)"
```

---

### Task 4: Add VM database operations to Database trait and pg_database

**Files:**
- Modify: `src/handlers/mod.rs` — add VM methods to Database trait
- Modify: `src/db/pg_database.rs` — implement them
- Modify: `src/permissions.rs` — add stubs to MockDb

**Step 1: Add VM operations to the Database trait**

```rust
// VM operations (instances + programs)
async fn store_instance(&self, item_hash: &str, content: &InstanceContent, sender: &str) -> Result<(), String>;
async fn store_program(&self, item_hash: &str, content: &ProgramContent, sender: &str) -> Result<(), String>;
async fn get_instance(&self, item_hash: &str) -> Result<Option<VmRecord>, String>;
async fn get_program(&self, item_hash: &str) -> Result<Option<VmRecord>, String>;
async fn store_vm_volumes(&self, vm_hash: &str, volumes: &[VolumeInfo]) -> Result<(), String>;
async fn upsert_vm_version(&self, vm_hash: &str, owner: &str, current_version: &str, time: f64) -> Result<(), String>;
async fn is_vm_amend_allowed(&self, vm_hash: &str) -> Result<Option<bool>, String>;
async fn delete_vm_updates(&self, vm_hash: &str) -> Result<Vec<String>, String>;
async fn check_volume_refs_exist(&self, refs: &[String], use_latest_refs: &[String]) -> Result<(Vec<String>, Vec<String>), String>;
async fn get_total_cost_for_address(&self, address: &str, payment_type: &str) -> Result<rust_decimal::Decimal, String>;
async fn store_account_costs(&self, costs: &[AccountCostRecord]) -> Result<(), String>;
```

Also add a `VmRecord` struct to mod.rs:

```rust
#[derive(Debug, Clone)]
pub struct VmRecord {
    pub item_hash: String,
    pub owner: String,
    pub allow_amend: bool,
    pub replaces: Option<String>,
    pub time: f64,
}

#[derive(Debug, Clone)]
pub struct AccountCostRecord {
    pub owner: String,
    pub item_hash: String,
    pub cost_type: String,
    pub name: String,
    pub ref_hash: Option<String>,
    pub payment_type: String,
    pub cost_hold: rust_decimal::Decimal,
    pub cost_stream: rust_decimal::Decimal,
    pub cost_credit: rust_decimal::Decimal,
}
```

**Step 2: Implement in pg_database.rs**

Key implementations:
- `store_instance`: INSERT INTO instances with all columns from InstanceContent
- `store_program`: INSERT INTO programs with all columns from ProgramContent
- `get_instance` / `get_program`: SELECT returning VmRecord (item_hash, owner, allow_amend, replaces, time)
- `store_vm_volumes`: INSERT INTO vm_machine_volumes for each volume
- `upsert_vm_version`: INSERT ... ON CONFLICT (vm_hash) DO UPDATE SET current_version, last_updated WHERE last_updated < excluded
- `is_vm_amend_allowed`: JOIN vm_versions → instances/programs to check allow_amend on current version
- `delete_vm_updates`: DELETE FROM instances/programs WHERE replaces = $1, return deleted hashes
- `check_volume_refs_exist`: SELECT from file_pins + file_tags to check existence
- `get_total_cost_for_address`: SUM costs from account_costs for address+payment_type
- `store_account_costs`: INSERT INTO account_costs with per-item cost breakdown

**Step 3: Add stubs to MockDb in permissions.rs**

**Step 4: Compile check**

Run: `cargo check`

**Step 5: Commit**

```
git commit -m "feat: add VM database operations to Database trait and PgDatabase"
```

---

### Task 5: Implement shared VM validation logic

Extract common validation that both INSTANCE and PROGRAM handlers need.

**Files:**
- Create: `src/handlers/vm_common.rs`
- Modify: `src/handlers/mod.rs` — add `pub mod vm_common;`

**Step 1: Create vm_common.rs with shared validation functions**

```rust
//! Shared validation and processing logic for INSTANCE and PROGRAM messages.
//! Reference: aleph/handlers/content/vm.py

use crate::types::{VolumeInfo, VolumeSource, VmRequirements};
use super::{HandlerContext, HandlerError, VmRecord};

/// Validate volume references exist in file_pins / file_tags.
/// Returns list of missing refs.
/// Reference: aleph/handlers/content/vm.py find_missing_volumes()
pub async fn validate_volume_refs(
    refs_to_check: &[String],
    use_latest_refs: &[String],
    ctx: &HandlerContext,
) -> Result<(), HandlerError> {
    if refs_to_check.is_empty() && use_latest_refs.is_empty() {
        return Ok(());
    }
    if let Some(ref db) = ctx.db {
        let (missing_pins, missing_tags) = db.check_volume_refs_exist(refs_to_check, use_latest_refs).await
            .map_err(HandlerError::Database)?;
        if !missing_pins.is_empty() || !missing_tags.is_empty() {
            let all_missing: Vec<_> = missing_pins.into_iter().chain(missing_tags).collect();
            return Err(HandlerError::NotAllowed(format!(
                "Volume references not found: {}", all_missing.join(", ")
            )));
        }
    }
    Ok(())
}

/// Collect all volume refs that need to be checked for existence.
/// Returns (pin_refs, tag_refs) — pin_refs for use_latest=false, tag_refs for use_latest=true.
pub fn collect_volume_refs(volumes: &[VolumeInfo]) -> (Vec<String>, Vec<String>) {
    let mut pin_refs = Vec::new();
    let mut tag_refs = Vec::new();
    for vol in volumes {
        match &vol.source {
            VolumeSource::Immutable { ref_, use_latest } => {
                if *use_latest {
                    tag_refs.push(ref_.clone());
                } else {
                    pin_refs.push(ref_.clone());
                }
            }
            VolumeSource::Persistent { .. } | VolumeSource::Ephemeral { .. } => {}
        }
    }
    (pin_refs, tag_refs)
}

/// Validate amendment (replaces) constraints.
/// Reference: aleph/handlers/content/vm.py check_dependencies() lines 378-392
pub async fn validate_amendment(
    replaces: &str,
    ctx: &HandlerContext,
    is_instance: bool,
) -> Result<(), HandlerError> {
    if let Some(ref db) = ctx.db {
        // Look up the original VM
        let original = if is_instance {
            db.get_instance(replaces).await.map_err(HandlerError::Database)?
        } else {
            db.get_program(replaces).await.map_err(HandlerError::Database)?
        };

        let original = original.ok_or_else(|| HandlerError::NotAllowed(
            format!("Referenced VM not found: {}", replaces)
        ))?;

        // Cannot amend an amendment (no chain: A→B→C)
        if original.replaces.is_some() {
            return Err(HandlerError::NotAllowed(
                "Cannot amend an amendment (only direct updates allowed)".to_string()
            ));
        }

        // Check allow_amend on the current version
        let amend_allowed = db.is_vm_amend_allowed(replaces).await
            .map_err(HandlerError::Database)?;
        match amend_allowed {
            Some(false) => return Err(HandlerError::NotAllowed(
                format!("VM {} does not allow amendments", replaces)
            )),
            None => return Err(HandlerError::NotAllowed(
                format!("Could not determine amend status for VM {}", replaces)
            )),
            Some(true) => {} // OK
        }
    }
    Ok(())
}

/// Check sender authorization via security aggregate.
/// Reference: aleph/handlers/content/content_handler.py check_permissions()
pub async fn check_vm_permissions(
    sender: &str,
    content_address: &str,
    ctx: &HandlerContext,
) -> Result<(), HandlerError> {
    // If sender == content.address, no delegation check needed
    if sender.to_lowercase() == content_address.to_lowercase() {
        return Ok(());
    }
    // Delegation check via security aggregate
    if let Some(ref db) = ctx.db {
        let security = db.get_aggregate(content_address, "security").await
            .map_err(HandlerError::Database)?;
        if let Some(security_value) = security {
            if let Some(authorizations) = security_value.get("authorizations") {
                if let Some(arr) = authorizations.as_array() {
                    for auth in arr {
                        if let Some(addr) = auth.get("address").and_then(|a| a.as_str()) {
                            if addr.to_lowercase() == sender.to_lowercase() {
                                // Check types includes the message type
                                // For simplicity, allow any listed authorization
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }
    Err(HandlerError::PermissionDenied(format!(
        "Sender {} not authorized to act for address {}", sender, content_address
    )))
}
```

**Step 2: Compile check**

Run: `cargo check`

**Step 3: Commit**

```
git commit -m "feat: add shared VM validation logic (volumes, amendments, permissions)"
```

---

### Task 6: Implement the full InstanceHandler

Replace the stub with complete processing matching pyaleph.

**Files:**
- Modify: `src/handlers/instance.rs` — full rewrite

**Step 1: Implement validate()**

```rust
async fn validate(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
    let content: InstanceContent = parse_content(message)?;

    // Validate resources
    if content.resources.memory == 0 {
        return Err(HandlerError::InvalidContent("Memory must be > 0".to_string()));
    }
    if content.resources.vcpus == 0 {
        return Err(HandlerError::InvalidContent("vCPUs must be > 0".to_string()));
    }

    // Check permissions (sender authorized for content.address)
    vm_common::check_vm_permissions(&message.sender, &content.address, ctx).await?;

    // Validate rootfs parent ref exists
    let (mut pin_refs, mut tag_refs) = vm_common::collect_volume_refs(&content.volumes);
    if content.rootfs.parent.use_latest {
        tag_refs.push(content.rootfs.parent.ref_.clone());
    } else {
        pin_refs.push(content.rootfs.parent.ref_.clone());
    }
    vm_common::validate_volume_refs(&pin_refs, &tag_refs, ctx).await?;

    // Validate amendment if replaces is set
    if let Some(ref replaces) = content.replaces {
        vm_common::validate_amendment(replaces, ctx, true).await?;
    }

    Ok(())
}
```

**Step 2: Implement process()**

```rust
async fn process(&self, message: &Message, ctx: &HandlerContext) -> Result<(), HandlerError> {
    let content: InstanceContent = parse_content(message)?;

    if let Some(ref db) = ctx.db {
        // Store instance in database
        db.store_instance(&message.item_hash, &content, &message.sender).await
            .map_err(HandlerError::Database)?;

        // Store volumes
        if !content.volumes.is_empty() {
            db.store_vm_volumes(&message.item_hash, &content.volumes).await
                .map_err(HandlerError::Database)?;
        }

        // Upsert vm_versions
        let program_ref = content.replaces.as_deref().unwrap_or(&message.item_hash);
        db.upsert_vm_version(
            &message.item_hash,
            &content.address,
            program_ref,
            content.time,
        ).await.map_err(HandlerError::Database)?;

        // Calculate and store costs
        if let Some(ref cost_service) = ctx.cost {
            let costs = cost_service.calculate_instance_costs(
                &message.item_hash,
                &content,
            );
            if !costs.is_empty() {
                db.store_account_costs(&costs).await
                    .map_err(HandlerError::Database)?;
            }
        }
    }

    tracing::info!(
        "Processed instance: hash={} address={} memory={}MB vcpus={} payment={:?}",
        &message.item_hash[..16],
        content.address,
        content.resources.memory,
        content.resources.vcpus,
        content.payment.as_ref().map(|p| &p.payment_type),
    );

    Ok(())
}
```

**Step 3: Add `cost` field to HandlerContext**

In `src/handlers/mod.rs`, add `pub cost: Option<Arc<CostService>>` to `HandlerContext`.

**Step 4: Update handler context creation in message_processor.rs**

Wire the existing CostService into the HandlerContext.

**Step 5: Compile and test**

Run: `cargo check && cargo test`

**Step 6: Commit**

```
git commit -m "feat: implement full InstanceHandler with DB storage, volumes, amendments, and costs"
```

---

### Task 7: Implement the full ProgramHandler

Mirror the InstanceHandler pattern for programs.

**Files:**
- Modify: `src/handlers/program.rs` — full rewrite

**Step 1: Implement validate() and process()**

Same pattern as InstanceHandler but:
- Validates `code` and `runtime` refs exist (via file_pins/file_tags)
- Stores to `programs` table via `db.store_program()`
- Includes code_ref, runtime_ref columns
- Cost calculation uses `calculate_program_costs` instead

**Step 2: Compile and test**

**Step 3: Commit**

```
git commit -m "feat: implement full ProgramHandler with DB storage, volumes, amendments, and costs"
```

---

### Task 8: Add cost calculation for instances and programs

Extend the existing CostService with VM-specific cost methods.

**Files:**
- Modify: `src/services/cost.rs` — add `calculate_instance_costs` and `calculate_program_costs`

**Step 1: Add instance cost calculation**

Reference: `aleph/services/cost.py _calculate_executable_costs()`

```rust
/// Calculate costs for an instance message
/// Returns list of AccountCostRecord entries (execution + volumes)
pub fn calculate_instance_costs(
    &self,
    item_hash: &str,
    content: &InstanceContent,
) -> Vec<AccountCostRecord> {
    let owner = &content.address;
    let payment_type = content.payment.as_ref()
        .map(|p| format!("{:?}", p.payment_type).to_lowercase())
        .unwrap_or_else(|| "hold".to_string());

    let mut costs = Vec::new();

    // 1. Compute unit cost
    let compute_units = self.calculate_compute_units(content.resources.memory, content.resources.vcpus);
    let price_type = ProductPriceType::Instance; // or GPU variant
    let prices = self.get_prices(price_type);
    if let Some(compute_price) = prices.compute_unit {
        costs.push(AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: "EXECUTION".to_string(),
            name: "execution".to_string(),
            ref_hash: None,
            payment_type: payment_type.clone(),
            cost_hold: compute_price.holding * Decimal::from(compute_units),
            cost_stream: Decimal::ZERO, // streams use different pricing
            cost_credit: compute_price.credit * Decimal::from(compute_units),
        });
    }

    // 2. Rootfs volume cost
    let rootfs_mib = Decimal::from(content.rootfs.size_mib);
    costs.push(AccountCostRecord {
        owner: owner.to_string(),
        item_hash: item_hash.to_string(),
        cost_type: "EXECUTION_INSTANCE_VOLUME_ROOTFS".to_string(),
        name: "rootfs".to_string(),
        ref_hash: Some(content.rootfs.parent.ref_.clone()),
        payment_type: payment_type.clone(),
        cost_hold: prices.storage.holding * rootfs_mib,
        cost_stream: Decimal::ZERO,
        cost_credit: prices.storage.credit * rootfs_mib,
    });

    // 3. Machine volume costs (immutable, persistent, ephemeral)
    for (i, vol) in content.volumes.iter().enumerate() {
        let (cost_type, size, ref_hash) = match &vol.source {
            VolumeSource::Immutable { ref_, .. } => (
                "EXECUTION_VOLUME_INMUTABLE", 0u32, Some(ref_.clone()),
            ),
            VolumeSource::Persistent { size_mib, .. } => (
                "EXECUTION_VOLUME_PERSISTENT", *size_mib, None,
            ),
            VolumeSource::Ephemeral { size_mib, .. } => (
                "EXECUTION_VOLUME_PERSISTENT", *size_mib, None,
            ),
        };
        let size_dec = Decimal::from(size);
        costs.push(AccountCostRecord {
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            cost_type: cost_type.to_string(),
            name: format!("volume_{}", i),
            ref_hash,
            payment_type: payment_type.clone(),
            cost_hold: prices.storage.holding * size_dec,
            cost_stream: Decimal::ZERO,
            cost_credit: prices.storage.credit * size_dec,
        });
    }

    costs
}
```

**Step 2: Add unit tests for cost calculation**

**Step 3: Commit**

```
git commit -m "feat: add VM cost calculation to CostService"
```

---

### Task 9: Add balance validation to VM handlers

Before processing, check that the sender has sufficient balance for the payment type.

**Files:**
- Modify: `src/handlers/vm_common.rs` — add `validate_balance` function
- Modify: `src/handlers/instance.rs` — call it in validate()
- Modify: `src/handlers/program.rs` — call it in validate()

**Step 1: Implement balance validation**

Reference: `aleph/services/cost_validation.py validate_balance_for_payment()`

```rust
/// Validate that address has sufficient balance for the VM costs.
/// - HOLD: balance >= existing_cost + new_cost
/// - CREDIT: balance >= (existing_per_sec + new_per_sec) * 86400 (1-day minimum)
/// - SUPERFLUID: pass (validated on-chain)
pub async fn validate_balance(
    address: &str,
    payment: &Option<PaymentInfo>,
    message_cost: Decimal,
    ctx: &HandlerContext,
) -> Result<(), HandlerError> {
    let payment_type = payment.as_ref()
        .map(|p| p.payment_type)
        .unwrap_or(PaymentType::Hold);

    match payment_type {
        PaymentType::Superfluid => Ok(()), // Validated on-chain via streams
        PaymentType::Hold => {
            if let Some(ref db) = ctx.db {
                let chain = payment.as_ref()
                    .map(|p| p.chain.to_string())
                    .unwrap_or_else(|| "ETH".to_string());
                let balance = db.get_balance(address, &chain).await
                    .map_err(HandlerError::Database)?
                    .unwrap_or(Decimal::ZERO);
                let existing_cost = db.get_total_cost_for_address(address, "hold").await
                    .map_err(HandlerError::Database)?;
                let required = existing_cost + message_cost;
                if balance < required {
                    return Err(HandlerError::NotAllowed(format!(
                        "Insufficient balance: have {}, need {}", balance, required
                    )));
                }
            }
            Ok(())
        }
        PaymentType::Credit => {
            if let Some(ref db) = ctx.db {
                let balance = db.get_credit_balance(address).await
                    .map_err(HandlerError::Database)?
                    .unwrap_or(Decimal::ZERO);
                let existing_per_sec = db.get_total_cost_for_address(address, "credit").await
                    .map_err(HandlerError::Database)?;
                let total_per_sec = existing_per_sec + message_cost;
                let day_seconds = Decimal::from(86400);
                let required = total_per_sec * day_seconds;
                if balance < required {
                    return Err(HandlerError::NotAllowed(format!(
                        "Insufficient credits: have {}, need {} (1-day minimum for per-sec cost {})",
                        balance, required, total_per_sec
                    )));
                }
            }
            Ok(())
        }
    }
}
```

**Step 2: Call from both handlers' validate()**

**Step 3: Commit**

```
git commit -m "feat: add balance validation for VM messages (hold, credit, superfluid)"
```

---

### Task 10: Update forget handler for enriched derived tables

The forget handler's `delete_derived_data` now needs to also delete from `vm_machine_volumes` and `vm_versions`.

**Files:**
- Modify: `src/db/pg_database.rs` — update `delete_derived_data()`

**Step 1: Add volume and version cleanup**

In the INSTANCE and PROGRAM branches of `delete_derived_data`, also:

```rust
"INSTANCE" => {
    sqlx::query("DELETE FROM vm_machine_volumes WHERE vm_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
    sqlx::query("DELETE FROM instances WHERE item_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
    // Refresh vm_versions for amendments
    sqlx::query("DELETE FROM vm_versions WHERE vm_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
}
"PROGRAM" => {
    sqlx::query("DELETE FROM vm_machine_volumes WHERE vm_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
    sqlx::query("DELETE FROM programs WHERE item_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
    sqlx::query("DELETE FROM vm_versions WHERE vm_hash = $1")
        .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
}
```

**Step 2: Also delete account_costs by item_hash**

Add to the common section:
```rust
sqlx::query("DELETE FROM account_costs WHERE item_hash = $1")
```

Wait — `account_costs` is per-address, not per-item_hash. We need to check the actual schema. If we've changed it to per-item (as pyaleph does), this works. Otherwise skip.

**Step 3: Compile and test**

**Step 4: Commit**

```
git commit -m "fix: update forget handler to clean up vm_machine_volumes and vm_versions"
```

---

### Task 11: Backfill existing instance data into enriched tables

Existing instances in the `messages` table have never been written to the `instances` derived table. We need a one-time backfill on the dev server.

**Files:**
- Modify: `src/jobs/backfill.rs` — add instance backfill job

**Step 1: Add a backfill function**

```rust
/// Backfill instances table from messages where message_type = 'INSTANCE'
/// and item_hash not already in instances table.
pub async fn backfill_instances(db: &PgPool) -> Result<u64, anyhow::Error> {
    // Select all INSTANCE messages not yet in instances table
    // Parse item_content JSON and insert into instances
    // This runs at startup if instances table is empty
    ...
}
```

This needs to parse `item_content` JSON from the messages table and insert into the enriched instances table. It should be idempotent (ON CONFLICT DO NOTHING).

**Step 2: Call from JobManager startup, gated by a check**

Only run if instances table count < messages table INSTANCE count.

**Step 3: Test on dev server**

**Step 4: Commit**

```
git commit -m "feat: add instance/program backfill job for existing messages"
```

---

### Task 12: account_costs schema migration

The current `account_costs` table is per-address (no item_hash). pyaleph stores per-item cost breakdowns. We need to migrate.

**Files:**
- Modify: `src/db/migrations.rs` — update `create_account_costs_table`
- Add to: `migrations/003_enrich_vm_tables.sql`

**Step 1: New account_costs schema**

```sql
-- Drop and recreate account_costs to match pyaleph per-item schema
DROP TABLE IF EXISTS account_costs;
CREATE TABLE account_costs (
    id BIGSERIAL PRIMARY KEY,
    owner VARCHAR(256) NOT NULL,
    item_hash VARCHAR(128) NOT NULL,
    cost_type VARCHAR(50) NOT NULL,
    name VARCHAR(256) NOT NULL,
    ref_hash VARCHAR(128),
    payment_type VARCHAR(20) NOT NULL,
    cost_hold DECIMAL(78, 18) NOT NULL DEFAULT 0,
    cost_stream DECIMAL(78, 18) NOT NULL DEFAULT 0,
    cost_credit DECIMAL(78, 18) NOT NULL DEFAULT 0,
    UNIQUE(owner, item_hash, cost_type, name)
);
CREATE INDEX IF NOT EXISTS idx_account_costs_owner ON account_costs(owner);
CREATE INDEX IF NOT EXISTS idx_account_costs_item_hash ON account_costs(item_hash);
```

**Step 2: Update migrations.rs for fresh installs**

**Step 3: Run on dev server**

**Step 4: Commit**

```
git commit -m "feat: migrate account_costs to per-item schema matching pyaleph"
```

---

### Task 13: Build, deploy, and verify on dev server

**Step 1: Build release**

```bash
cargo build --release
```

**Step 2: Run migration SQL on dev server**

**Step 3: Deploy binary**

```bash
scp target/release/aleph-core root@[dev-server]:/root/aleph-core
ssh root@[dev-server] "systemctl restart pyaleph-rs"
```

**Step 4: Verify instances are being stored**

```bash
# Check instances table is being populated
ssh root@... "PGPASSWORD=aleph psql -U aleph -h localhost -d aleph -c 'SELECT COUNT(*) FROM instances'"

# Compare with messages count
ssh root@... "PGPASSWORD=aleph psql -U aleph -h localhost -d aleph -c \"SELECT COUNT(*) FROM messages WHERE message_type = 'INSTANCE'\""

# Check a specific instance
curl -s 'http://[dev-server]:8080/api/v0/messages.json?msgType=INSTANCE&limit=1' | python3 -m json.tool | head -20
```

**Step 5: Verify new INSTANCE message processing works**

Submit a test instance message and check it appears in both `messages` and `instances` tables.

**Step 6: Commit all remaining changes**

```
git commit -m "chore: deploy and verify instance handler on dev server"
```

---

## Dependency Order

```
Task 1 (serde renames)
  └→ Task 2 (fix InstanceContent/ProgramContent types)
       └→ Task 3 (DB schema migration)
            ├→ Task 4 (Database trait + pg_database impl)
            │    └→ Task 5 (vm_common shared logic)
            │         ├→ Task 6 (InstanceHandler)
            │         └→ Task 7 (ProgramHandler)
            └→ Task 12 (account_costs migration)
                 └→ Task 8 (CostService VM costs)
                      └→ Task 9 (balance validation)
Task 10 (forget cleanup update) — can run after Task 3
Task 11 (backfill) — can run after Task 6
Task 13 (deploy) — final
```

## Key Risk: InstanceContent deserialization

The biggest risk is that changing `InstanceContent` breaks existing code paths. The message processor stores `item_content` as raw JSON in the messages table, and handlers only parse it when processing. The type change needs to be backward-compatible with all existing messages on chain. Test with real data from the API.
