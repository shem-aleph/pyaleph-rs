//! PostgreSQL implementation of the Database trait for message handlers
//!
//! This bridges the abstract Database trait with the actual PgPool,
//! allowing handlers to perform database operations via the trait interface.

use async_trait::async_trait;
use sqlx::PgPool;

use crate::handlers::{Database, PostRecord, FilePinRecord, VmRecord, AccountCostRecord};
use crate::types::{Message, MessageType, ItemType, Chain, ProcessingStatus, InstanceContent, ProgramContent, VolumeInfo, VolumeSource};

/// Parse a MessageType from its DB string representation (UPPERCASE)
fn parse_message_type(s: &str) -> Result<MessageType, String> {
    match s {
        "AGGREGATE" => Ok(MessageType::Aggregate),
        "POST" => Ok(MessageType::Post),
        "STORE" => Ok(MessageType::Store),
        "PROGRAM" => Ok(MessageType::Program),
        "INSTANCE" => Ok(MessageType::Instance),
        "FORGET" => Ok(MessageType::Forget),
        _ => Err(format!("Unknown message type: {}", s)),
    }
}

/// Parse an ItemType from its DB string representation (lowercase)
fn parse_item_type(s: &str) -> Result<ItemType, String> {
    match s {
        "inline" => Ok(ItemType::Inline),
        "ipfs" => Ok(ItemType::Ipfs),
        "storage" => Ok(ItemType::Storage),
        _ => Err(format!("Unknown item type: {}", s)),
    }
}

/// PostgreSQL-backed implementation of the handlers::Database trait
pub struct PgDatabase {
    pool: PgPool,
}

impl PgDatabase {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Database for PgDatabase {
    async fn get_message(&self, item_hash: &str) -> Result<Option<Message>, String> {
        let row: Option<(String, String, String, String, String, String, Option<String>, Option<String>, f64)> =
            sqlx::query_as(
                "SELECT item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time \
                 FROM messages WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time)) => {
                let chain_parsed: Chain = serde_json::from_value(serde_json::Value::String(chain))
                    .map_err(|e| format!("Failed to parse chain: {}", e))?;
                Ok(Some(Message {
                    item_hash,
                    message_type: parse_message_type(&message_type)?,
                    chain: chain_parsed,
                    sender,
                    signature,
                    item_type: parse_item_type(&item_type)?,
                    item_content,
                    channel,
                    time,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_message(&self, message: &Message) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO messages (item_hash, message_type, chain, sender, signature, item_type, item_content, channel, time, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&message.item_hash)
        .bind(message.message_type.to_string())
        .bind(message.chain.to_string())
        .bind(&message.sender)
        .bind(&message.signature)
        .bind(message.item_type.to_string())
        .bind(&message.item_content)
        .bind(&message.channel)
        .bind(message.time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_message_status(&self, _item_hash: &str, _status: &ProcessingStatus) -> Result<(), String> {
        // Messages table doesn't have a status column; status is tracked by which table the message is in
        Ok(())
    }

    async fn get_aggregate(&self, address: &str, key: &str) -> Result<Option<serde_json::Value>, String> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT content FROM aggregates WHERE address = $1 AND key = $2"
        )
        .bind(address)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(v,)| v))
    }

    async fn store_aggregate(&self, address: &str, key: &str, content: &serde_json::Value, time: f64) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO aggregates (address, key, content, time, dirty, created_at) \
             VALUES ($1, $2, $3, $4, false, NOW()) \
             ON CONFLICT (address, key) DO UPDATE SET content = $3, time = $4, dirty = false"
        )
        .bind(address)
        .bind(key)
        .bind(content)
        .bind(time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_aggregate_time(&self, address: &str, key: &str) -> Result<Option<f64>, String> {
        let row: Option<(f64,)> = sqlx::query_as(
            "SELECT time FROM aggregates WHERE address = $1 AND key = $2"
        )
        .bind(address)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(t,)| t))
    }

    async fn mark_aggregate_dirty(&self, address: &str, key: &str) -> Result<(), String> {
        sqlx::query("UPDATE aggregates SET dirty = true WHERE address = $1 AND key = $2")
            .bind(address)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn mark_aggregate_clean(&self, address: &str, key: &str) -> Result<(), String> {
        sqlx::query("UPDATE aggregates SET dirty = false WHERE address = $1 AND key = $2")
            .bind(address)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_post(&self, item_hash: &str) -> Result<Option<PostRecord>, String> {
        let row: Option<(String, String, String, Option<String>, serde_json::Value, Option<String>, f64, Option<String>, Option<String>)> =
            sqlx::query_as(
                "SELECT item_hash, address, post_type, ref_, content, channel, time, original_item_hash, latest_amend \
                 FROM posts WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, address, post_type, ref_, content, channel, time, original_item_hash, latest_amend)) => {
                Ok(Some(PostRecord {
                    item_hash,
                    address,
                    post_type,
                    ref_,
                    content,
                    channel,
                    time,
                    original_item_hash,
                    latest_amend,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_post(&self, post: &PostRecord) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO posts (item_hash, address, post_type, content, ref_, channel, time, original_item_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&post.item_hash)
        .bind(&post.address)
        .bind(&post.post_type)
        .bind(&post.content)
        .bind(&post.ref_)
        .bind(&post.channel)
        .bind(post.time)
        .bind(&post.original_item_hash)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_post_latest_amend(&self, original_hash: &str, amend_hash: &str) -> Result<(), String> {
        sqlx::query("UPDATE posts SET latest_amend = $1 WHERE item_hash = $2")
            .bind(amend_hash)
            .bind(original_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_file_pin(&self, item_hash: &str) -> Result<Option<FilePinRecord>, String> {
        let row: Option<(String, String, i64, Option<String>, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as(
                "SELECT item_hash, owner, size, content_type, created_at FROM file_pins WHERE item_hash = $1"
            )
            .bind(item_hash)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        match row {
            Some((item_hash, owner, size, content_type, created_at)) => {
                Ok(Some(FilePinRecord {
                    item_hash,
                    owner,
                    size: size as u64,
                    content_type,
                    created_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn store_file_pin(&self, pin: &FilePinRecord) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO file_pins (item_hash, owner, size, content_type, created_at) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(&pin.item_hash)
        .bind(&pin.owner)
        .bind(pin.size as i64)
        .bind(&pin.content_type)
        .bind(pin.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn update_file_pin(&self, item_hash: &str, owner: &str) -> Result<(), String> {
        sqlx::query("UPDATE file_pins SET owner = $1 WHERE item_hash = $2")
            .bind(owner)
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn remove_file_pin(&self, item_hash: &str, _owner: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM file_pins WHERE item_hash = $1")
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_forgotten_hashes(&self, hashes: &[String]) -> Result<Vec<String>, String> {
        if hashes.is_empty() {
            return Ok(vec![]);
        }
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT item_hash FROM forgotten_messages WHERE item_hash = ANY($1)"
        )
        .bind(hashes)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    async fn mark_forgotten(&self, item_hash: &str, forget_hash: &str, reason: Option<&str>) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO forgotten_messages (item_hash, forget_hash, reason, forgotten_at) \
             VALUES ($1, $2, $3, NOW()) ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(item_hash)
        .bind(forget_hash)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn delete_message(&self, item_hash: &str) -> Result<(), String> {
        sqlx::query("DELETE FROM messages WHERE item_hash = $1")
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn delete_derived_data(&self, item_hash: &str, message_type: &str) -> Result<(), String> {
        match message_type {
            "POST" => {
                sqlx::query("DELETE FROM posts WHERE item_hash = $1")
                    .bind(item_hash)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "AGGREGATE" => {
                sqlx::query("DELETE FROM aggregate_elements WHERE item_hash = $1")
                    .bind(item_hash)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            "STORE" => {
                // file_pins already handled by remove_file_pin
            }
            "PROGRAM" => {
                sqlx::query("DELETE FROM vm_machine_volumes WHERE vm_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM programs WHERE item_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM vm_versions WHERE vm_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM account_costs WHERE item_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
            }
            "INSTANCE" => {
                sqlx::query("DELETE FROM vm_machine_volumes WHERE vm_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM instances WHERE item_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM vm_versions WHERE vm_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
                sqlx::query("DELETE FROM account_costs WHERE item_hash = $1")
                    .bind(item_hash).execute(&self.pool).await.map_err(|e| e.to_string())?;
            }
            _ => {}
        }
        // Clean up confirmations
        sqlx::query("DELETE FROM chain_txs WHERE item_hash = $1")
            .bind(item_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_dependent_vms(&self, file_hash: &str) -> Result<Vec<String>, String> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT item_hash FROM programs WHERE code_ref = $1 OR runtime_ref = $1 \
             UNION \
             SELECT item_hash FROM instances WHERE rootfs_ref = $1"
        )
        .bind(file_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(rows.into_iter().map(|(h,)| h).collect())
    }

    async fn get_balance(&self, address: &str, chain: &str) -> Result<Option<rust_decimal::Decimal>, String> {
        let row: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT balance FROM balances WHERE address = $1 AND chain = $2"
        )
        .bind(address)
        .bind(chain)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(b,)| b))
    }

    async fn get_credit_balance(&self, address: &str) -> Result<Option<rust_decimal::Decimal>, String> {
        let row: Option<(rust_decimal::Decimal,)> = sqlx::query_as(
            "SELECT balance FROM credit_balances WHERE address = $1"
        )
        .bind(address)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(b,)| b))
    }

    async fn store_instance(&self, item_hash: &str, content: &InstanceContent, sender: &str) -> Result<(), String> {
        let env = content.environment.as_ref();
        let req = content.requirements.as_ref();
        let payment_type = content.payment.as_ref().map(|p| format!("{:?}", p.payment_type).to_lowercase());
        let payment_chain = content.payment.as_ref().map(|p| p.chain.to_string());
        let payment_receiver = content.payment.as_ref().and_then(|p| p.receiver.clone());
        let authorized_keys_json = serde_json::to_value(&content.authorized_keys).ok();
        let metadata = content.metadata.clone();
        let variables = content.variables.clone();

        sqlx::query(
            "INSERT INTO instances (item_hash, owner, rootfs_ref, memory, vcpus, payment_type, payment_chain, \
             allow_amend, replaces, environment_reproducible, environment_internet, environment_aleph_api, \
             environment_shared_cache, environment_hypervisor, resources_seconds, metadata, variables, \
             authorized_keys, rootfs_use_latest, rootfs_persistence, rootfs_size_mib, \
             cpu_architecture, cpu_vendor, node_owner, node_address_regex, node_hash, \
             payment_receiver, time, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, \
                     $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, NOW()) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(item_hash)
        .bind(sender)
        .bind(&content.rootfs.parent.ref_)
        .bind(content.resources.memory as i32)
        .bind(content.resources.vcpus as i32)
        .bind(&payment_type)
        .bind(&payment_chain)
        .bind(content.allow_amend)
        .bind(&content.replaces)
        .bind(env.map(|e| e.reproducible).unwrap_or(false))
        .bind(env.map(|e| e.internet).unwrap_or(true))
        .bind(env.map(|e| e.aleph_api).unwrap_or(true))
        .bind(env.map(|e| e.shared_cache).unwrap_or(false))
        .bind(env.and_then(|e| e.hypervisor.clone()))
        .bind(content.resources.seconds as i32)
        .bind(&metadata)
        .bind(&variables)
        .bind(&authorized_keys_json)
        .bind(content.rootfs.parent.use_latest)
        .bind(&content.rootfs.persistence)
        .bind(content.rootfs.size_mib as i32)
        .bind(req.and_then(|r| r.cpu.as_ref()).and_then(|c| c.architecture.clone()))
        .bind(req.and_then(|r| r.cpu.as_ref()).and_then(|c| c.vendor.clone()))
        .bind(req.and_then(|r| r.node.as_ref()).and_then(|n| n.owner.clone()))
        .bind(req.and_then(|r| r.node.as_ref()).and_then(|n| n.address_regex.clone()))
        .bind(req.and_then(|r| r.node.as_ref()).and_then(|n| n.node_hash.clone()))
        .bind(&payment_receiver)
        .bind(content.time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn store_program(&self, item_hash: &str, content: &ProgramContent, sender: &str) -> Result<(), String> {
        let env = content.environment.as_ref();
        let req = content.requirements.as_ref();
        let payment_type = content.payment.as_ref().map(|p| format!("{:?}", p.payment_type).to_lowercase());
        let payment_chain = content.payment.as_ref().map(|p| p.chain.to_string());
        let metadata = content.metadata.clone();
        let variables = content.variables.clone();

        sqlx::query(
            "INSERT INTO programs (item_hash, owner, code_ref, runtime_ref, memory, vcpus, allow_amend, \
             replaces, environment_reproducible, environment_internet, environment_aleph_api, \
             environment_shared_cache, environment_hypervisor, resources_seconds, metadata, variables, \
             payment_type, payment_chain, cpu_architecture, node_hash, time, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, NOW()) \
             ON CONFLICT (item_hash) DO NOTHING"
        )
        .bind(item_hash)
        .bind(sender)
        .bind(&content.code.ref_)
        .bind(&content.runtime.ref_)
        .bind(content.resources.memory as i32)
        .bind(content.resources.vcpus as i32)
        .bind(content.allow_amend)
        .bind(&content.replaces)
        .bind(env.map(|e| e.reproducible).unwrap_or(false))
        .bind(env.map(|e| e.internet).unwrap_or(true))
        .bind(env.map(|e| e.aleph_api).unwrap_or(true))
        .bind(env.map(|e| e.shared_cache).unwrap_or(false))
        .bind(env.and_then(|e| e.hypervisor.clone()))
        .bind(content.resources.seconds as i32)
        .bind(&metadata)
        .bind(&variables)
        .bind(&payment_type)
        .bind(&payment_chain)
        .bind(req.and_then(|r| r.cpu.as_ref()).and_then(|c| c.architecture.clone()))
        .bind(req.and_then(|r| r.node.as_ref()).and_then(|n| n.node_hash.clone()))
        .bind(content.time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn get_instance(&self, item_hash: &str) -> Result<Option<VmRecord>, String> {
        let row: Option<(String, String, bool, Option<String>, Option<f64>)> = sqlx::query_as(
            "SELECT item_hash, owner, allow_amend, replaces, time FROM instances WHERE item_hash = $1"
        )
        .bind(item_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(item_hash, owner, allow_amend, replaces, time)| VmRecord {
            item_hash, owner, allow_amend, replaces, time: time.unwrap_or(0.0),
        }))
    }

    async fn get_program(&self, item_hash: &str) -> Result<Option<VmRecord>, String> {
        let row: Option<(String, String, bool, Option<String>, Option<f64>)> = sqlx::query_as(
            "SELECT item_hash, owner, allow_amend, replaces, time FROM programs WHERE item_hash = $1"
        )
        .bind(item_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(row.map(|(item_hash, owner, allow_amend, replaces, time)| VmRecord {
            item_hash, owner, allow_amend, replaces, time: time.unwrap_or(0.0),
        }))
    }

    async fn store_vm_volumes(&self, vm_hash: &str, volumes: &[VolumeInfo]) -> Result<(), String> {
        for vol in volumes {
            let (volume_type, size_mib, ref_hash, use_latest, persistence, name) = match &vol.source {
                VolumeSource::Immutable { ref_, use_latest } => {
                    ("immutable", None, Some(ref_.clone()), Some(*use_latest), None, None)
                }
                VolumeSource::Persistent { persistence, name, size_mib } => {
                    ("persistent", Some(*size_mib as i32), None, None, Some(persistence.clone()), Some(name.clone()))
                }
                VolumeSource::Ephemeral { size_mib, .. } => {
                    ("ephemeral", Some(*size_mib as i32), None, None, None, None)
                }
            };
            sqlx::query(
                "INSERT INTO vm_machine_volumes (vm_hash, volume_type, comment, mount, size_mib, ref_hash, use_latest, persistence, name) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
            )
            .bind(vm_hash)
            .bind(volume_type)
            .bind(&vol.comment)
            .bind(&vol.mount)
            .bind(size_mib)
            .bind(&ref_hash)
            .bind(use_latest)
            .bind(&persistence)
            .bind(&name)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn upsert_vm_version(&self, vm_hash: &str, owner: &str, current_version: &str, time: f64) -> Result<(), String> {
        sqlx::query(
            "INSERT INTO vm_versions (vm_hash, original_hash, version, owner, current_version, last_updated, created_at) \
             VALUES ($1, $1, 1, $2, $3, to_timestamp($4), NOW()) \
             ON CONFLICT (vm_hash) DO UPDATE SET current_version = $3, last_updated = to_timestamp($4) \
             WHERE vm_versions.last_updated < to_timestamp($4) OR vm_versions.last_updated IS NULL"
        )
        .bind(vm_hash)
        .bind(owner)
        .bind(current_version)
        .bind(time)
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn is_vm_amend_allowed(&self, vm_hash: &str) -> Result<Option<bool>, String> {
        // Check the current version of the VM to see if it allows amendments
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT current_version FROM vm_versions WHERE vm_hash = $1"
        )
        .bind(vm_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        match row {
            Some((Some(current_version),)) => {
                // Check allow_amend on the current version in instances or programs
                let instance_row: Option<(bool,)> = sqlx::query_as(
                    "SELECT allow_amend FROM instances WHERE item_hash = $1"
                )
                .bind(&current_version)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;

                if let Some((allow,)) = instance_row {
                    return Ok(Some(allow));
                }

                let program_row: Option<(bool,)> = sqlx::query_as(
                    "SELECT allow_amend FROM programs WHERE item_hash = $1"
                )
                .bind(&current_version)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;

                Ok(program_row.map(|(allow,)| allow))
            }
            Some((None,)) => {
                // No current_version set, fall back to checking the vm_hash directly
                let row: Option<(bool,)> = sqlx::query_as(
                    "SELECT allow_amend FROM instances WHERE item_hash = $1 \
                     UNION SELECT allow_amend FROM programs WHERE item_hash = $1 LIMIT 1"
                )
                .bind(vm_hash)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;
                Ok(row.map(|(a,)| a))
            }
            None => Ok(None),
        }
    }

    async fn delete_vm_updates(&self, vm_hash: &str) -> Result<Vec<String>, String> {
        // Delete all amendment instances/programs that reference this VM and return their hashes
        let inst_rows: Vec<(String,)> = sqlx::query_as(
            "DELETE FROM instances WHERE replaces = $1 RETURNING item_hash"
        )
        .bind(vm_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        let prog_rows: Vec<(String,)> = sqlx::query_as(
            "DELETE FROM programs WHERE replaces = $1 RETURNING item_hash"
        )
        .bind(vm_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;

        Ok(inst_rows.into_iter().chain(prog_rows).map(|(h,)| h).collect())
    }

    async fn check_volume_refs_exist(&self, refs: &[String], use_latest_refs: &[String]) -> Result<(Vec<String>, Vec<String>), String> {
        let mut missing_pins = Vec::new();
        let mut missing_tags = Vec::new();

        if !refs.is_empty() {
            // Check file_pins for non-use_latest refs
            let existing: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT item_hash FROM file_pins WHERE item_hash = ANY($1)"
            )
            .bind(refs)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

            let existing_set: std::collections::HashSet<_> = existing.into_iter().map(|(h,)| h).collect();
            for r in refs {
                if !existing_set.contains(r) {
                    missing_pins.push(r.clone());
                }
            }
        }

        if !use_latest_refs.is_empty() {
            // Check file_tags for use_latest refs
            let existing: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT item_hash FROM file_tags WHERE item_hash = ANY($1)"
            )
            .bind(use_latest_refs)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

            let existing_set: std::collections::HashSet<_> = existing.into_iter().map(|(h,)| h).collect();
            for r in use_latest_refs {
                if !existing_set.contains(r) {
                    missing_tags.push(r.clone());
                }
            }
        }

        Ok((missing_pins, missing_tags))
    }

    async fn get_total_cost_for_address(&self, address: &str, payment_type: &str) -> Result<rust_decimal::Decimal, String> {
        let column = match payment_type {
            "hold" | "holding" => "cost_hold",
            "credit" => "cost_credit",
            _ => "cost_hold",
        };
        let query = format!("SELECT COALESCE(SUM({}), 0) FROM account_costs WHERE owner = $1 AND payment_type = $2", column);
        let row: (rust_decimal::Decimal,) = sqlx::query_as(&query)
            .bind(address)
            .bind(payment_type)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(row.0)
    }

    async fn store_account_costs(&self, costs: &[AccountCostRecord]) -> Result<(), String> {
        for cost in costs {
            sqlx::query(
                "INSERT INTO account_costs (owner, item_hash, cost_type, name, ref_hash, payment_type, cost_hold, cost_stream, cost_credit) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (owner, item_hash, cost_type, name) DO UPDATE SET \
                 ref_hash = $5, payment_type = $6, cost_hold = $7, cost_stream = $8, cost_credit = $9"
            )
            .bind(&cost.owner)
            .bind(&cost.item_hash)
            .bind(&cost.cost_type)
            .bind(&cost.name)
            .bind(&cost.ref_hash)
            .bind(&cost.payment_type)
            .bind(cost.cost_hold)
            .bind(cost.cost_stream)
            .bind(cost.cost_credit)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
