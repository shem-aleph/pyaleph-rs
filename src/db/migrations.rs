//! Database migrations
//!
//! Schema matches pyaleph PostgreSQL for data compatibility.
//! Reference: aleph/db/models.py, aleph/db/migrations/

use sqlx::{PgPool, Error};
use tracing::info;

/// Run all migrations
pub async fn run_migrations(pool: &PgPool) -> Result<(), Error> {
    info!("Running database migrations...");
    
    // Create tables in order of dependencies
    create_messages_table(pool).await?;
    create_pending_messages_table(pool).await?;
    create_rejected_messages_table(pool).await?;
    create_forgotten_messages_table(pool).await?;
    create_aggregates_table(pool).await?;
    create_aggregate_elements_table(pool).await?;
    create_posts_table(pool).await?;
    create_balances_table(pool).await?;
    create_credit_balances_table(pool).await?;
    create_file_pins_table(pool).await?;
    create_file_tags_table(pool).await?;
    create_files_table(pool).await?;
    alter_file_pins_v2(pool).await?;
    create_file_tags_v2_table(pool).await?;
    create_programs_table(pool).await?;
    create_instances_table(pool).await?;
    create_vm_versions_table(pool).await?;
    create_vm_machine_volumes_table(pool).await?;
    create_chain_txs_table(pool).await?;
    create_account_costs_table(pool).await?;
    create_chain_sync_state_table(pool).await?;
    create_pending_txs_table(pool).await?;
    create_peers_table(pool).await?;
    widen_narrow_columns(pool).await?;
    denormalize_messages(pool).await?;
    create_message_confirmations_table(pool).await?;
    alter_chain_txs(pool).await?;
    create_message_counts_table(pool).await?;
    create_indexes(pool).await?;

    info!("Database migrations completed");
    Ok(())
}

async fn create_messages_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS messages (
            item_hash VARCHAR(128) PRIMARY KEY,
            message_type VARCHAR(20) NOT NULL,
            chain VARCHAR(64) NOT NULL,
            sender VARCHAR(256) NOT NULL,
            signature TEXT NOT NULL,
            item_type VARCHAR(20) NOT NULL,
            item_content TEXT,
            channel VARCHAR(256),
            time DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_pending_messages_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS pending_messages (
            item_hash VARCHAR(128) PRIMARY KEY,
            message JSONB NOT NULL,
            reception_time DOUBLE PRECISION NOT NULL,
            fetched BOOLEAN DEFAULT FALSE,
            check_message BOOLEAN DEFAULT TRUE,
            retries INTEGER DEFAULT 0,
            next_attempt DOUBLE PRECISION NOT NULL,
            trusted_source BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_rejected_messages_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS rejected_messages (
            item_hash VARCHAR(128) PRIMARY KEY,
            message JSONB NOT NULL,
            error_code INTEGER NOT NULL,
            error_message TEXT,
            rejected_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_forgotten_messages_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS forgotten_messages (
            item_hash VARCHAR(128) PRIMARY KEY,
            forget_hash VARCHAR(128) NOT NULL,
            reason TEXT,
            forgotten_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_aggregates_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS aggregates (
            address VARCHAR(256) NOT NULL,
            key VARCHAR(256) NOT NULL,
            content JSONB NOT NULL,
            time DOUBLE PRECISION NOT NULL,
            dirty BOOLEAN DEFAULT FALSE,
            last_revision_hash VARCHAR(128),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (address, key)
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_aggregate_elements_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS aggregate_elements (
            id BIGSERIAL PRIMARY KEY,
            address VARCHAR(256) NOT NULL,
            key VARCHAR(256) NOT NULL,
            item_hash VARCHAR(128) NOT NULL,
            content JSONB NOT NULL,
            time DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_posts_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS posts (
            item_hash VARCHAR(128) PRIMARY KEY,
            address VARCHAR(256) NOT NULL,
            post_type VARCHAR(50) NOT NULL,
            content JSONB NOT NULL,
            ref_ VARCHAR(128),
            channel TEXT,
            time DOUBLE PRECISION NOT NULL,
            original_item_hash VARCHAR(128),
            latest_amend VARCHAR(128),
            amends JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_balances_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS balances (
            address VARCHAR(256) NOT NULL,
            chain VARCHAR(10) NOT NULL,
            balance DECIMAL(78, 18) NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (address, chain)
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_credit_balances_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS credit_balances (
            address VARCHAR(256) PRIMARY KEY,
            balance DECIMAL(78, 18) NOT NULL DEFAULT 0,
            expiration TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_file_pins_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS file_pins (
            item_hash VARCHAR(128) NOT NULL,
            owner VARCHAR(256) NOT NULL,
            size BIGINT NOT NULL DEFAULT 0,
            content_type VARCHAR(100),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (item_hash, owner)
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_file_tags_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS file_tags (
            item_hash VARCHAR(128) NOT NULL,
            tag VARCHAR(100) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (item_hash, tag)
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_programs_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS programs (
            item_hash VARCHAR(128) PRIMARY KEY,
            owner VARCHAR(256) NOT NULL,
            code_ref VARCHAR(128) NOT NULL,
            runtime_ref VARCHAR(128) NOT NULL,
            memory INTEGER NOT NULL,
            vcpus INTEGER NOT NULL,
            allow_amend BOOLEAN DEFAULT TRUE,
            replaces VARCHAR(128),
            environment_reproducible BOOLEAN DEFAULT FALSE,
            environment_internet BOOLEAN DEFAULT TRUE,
            environment_aleph_api BOOLEAN DEFAULT TRUE,
            environment_shared_cache BOOLEAN DEFAULT FALSE,
            environment_hypervisor VARCHAR(20),
            resources_seconds INTEGER DEFAULT 30,
            metadata JSONB,
            variables JSONB,
            payment_type VARCHAR(20),
            payment_chain VARCHAR(10),
            cpu_architecture VARCHAR(20),
            node_hash VARCHAR(128),
            time DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_instances_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS instances (
            item_hash VARCHAR(128) PRIMARY KEY,
            owner VARCHAR(256) NOT NULL,
            rootfs_ref VARCHAR(128) NOT NULL,
            memory INTEGER NOT NULL,
            vcpus INTEGER NOT NULL,
            payment_type VARCHAR(20),
            payment_chain VARCHAR(10),
            allow_amend BOOLEAN DEFAULT TRUE,
            trusted_execution JSONB,
            replaces VARCHAR(128),
            environment_reproducible BOOLEAN DEFAULT FALSE,
            environment_internet BOOLEAN DEFAULT TRUE,
            environment_aleph_api BOOLEAN DEFAULT TRUE,
            environment_shared_cache BOOLEAN DEFAULT FALSE,
            environment_hypervisor VARCHAR(20),
            resources_seconds INTEGER DEFAULT 30,
            metadata JSONB,
            variables JSONB,
            authorized_keys JSONB,
            rootfs_use_latest BOOLEAN DEFAULT TRUE,
            rootfs_persistence VARCHAR(20),
            rootfs_size_mib INTEGER,
            cpu_architecture VARCHAR(20),
            cpu_vendor VARCHAR(50),
            node_owner VARCHAR(256),
            node_address_regex VARCHAR(256),
            node_hash VARCHAR(128),
            payment_receiver VARCHAR(256),
            time DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_vm_versions_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS vm_versions (
            vm_hash VARCHAR(128) PRIMARY KEY,
            original_hash VARCHAR(128) NOT NULL,
            version INTEGER NOT NULL,
            owner VARCHAR(256) NOT NULL,
            current_version VARCHAR(128),
            last_updated TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_vm_machine_volumes_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS vm_machine_volumes (
            id BIGSERIAL PRIMARY KEY,
            vm_hash VARCHAR(128) NOT NULL,
            volume_type VARCHAR(20) NOT NULL,
            comment TEXT,
            mount VARCHAR(256),
            size_mib INTEGER,
            ref_hash VARCHAR(128),
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

async fn create_chain_txs_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS chain_txs (
            hash VARCHAR(256) NOT NULL,
            chain VARCHAR(10) NOT NULL,
            height BIGINT NOT NULL,
            item_hash VARCHAR(128) NOT NULL,
            publisher VARCHAR(256),
            protocol VARCHAR(50) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (hash, item_hash)
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_account_costs_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS account_costs (
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
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_chain_sync_state_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS chain_sync_state (
            chain VARCHAR(64) PRIMARY KEY,
            last_block BIGINT NOT NULL DEFAULT 0,
            last_sync TIMESTAMPTZ DEFAULT NOW(),
            last_sync_timestamp BIGINT DEFAULT 0
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

async fn create_pending_txs_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS pending_txs (
            ipfs_hash VARCHAR(128) PRIMARY KEY,
            chain VARCHAR(10) NOT NULL,
            item_hashes JSONB NOT NULL,
            tx_hash VARCHAR(128),
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

/// Create files table — tracks known file metadata
async fn create_files_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS files (
            hash VARCHAR(128) PRIMARY KEY,
            size BIGINT NOT NULL DEFAULT 0,
            file_type VARCHAR(20) NOT NULL DEFAULT 'file',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;
    Ok(())
}

/// Add new columns to file_pins for typed pins, refs, and grace periods
async fn alter_file_pins_v2(pool: &PgPool) -> Result<(), Error> {
    // Add pin_type column
    sqlx::query(
        "ALTER TABLE file_pins ADD COLUMN IF NOT EXISTS pin_type VARCHAR(20) NOT NULL DEFAULT 'message'"
    )
    .execute(pool)
    .await?;

    // Add ref_ column for file versioning
    sqlx::query(
        "ALTER TABLE file_pins ADD COLUMN IF NOT EXISTS ref_ TEXT"
    )
    .execute(pool)
    .await?;

    // Add delete_by for grace period expiry
    sqlx::query(
        "ALTER TABLE file_pins ADD COLUMN IF NOT EXISTS delete_by TIMESTAMPTZ"
    )
    .execute(pool)
    .await?;

    // Add message_hash — the message that created this pin
    sqlx::query(
        "ALTER TABLE file_pins ADD COLUMN IF NOT EXISTS message_hash VARCHAR(128)"
    )
    .execute(pool)
    .await?;

    // Indexes for new columns
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_file_pins_pin_type ON file_pins(pin_type)"
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_file_pins_delete_by ON file_pins(delete_by) WHERE delete_by IS NOT NULL"
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_file_pins_message_hash ON file_pins(message_hash)"
    )
    .execute(pool)
    .await?;

    // Unique index for typed pins — allows multiple pin types per file+owner
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_file_pins_unique_typed ON file_pins(item_hash, owner, pin_type)"
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Create file_tags_v2 table — matches Python pyaleph file tag schema
async fn create_file_tags_v2_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS file_tags_v2 (
            tag VARCHAR(512) PRIMARY KEY,
            owner VARCHAR(256) NOT NULL,
            file_hash VARCHAR(128) NOT NULL,
            last_updated DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#)
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_file_tags_v2_owner ON file_tags_v2(owner)"
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_file_tags_v2_file_hash ON file_tags_v2(file_hash)"
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Widen columns that are too narrow for real-world data.
/// Some Aleph messages have ref fields, addresses, or post types that exceed
/// the original VARCHAR limits (e.g., IPFS CIDs, long post types).
async fn widen_narrow_columns(pool: &PgPool) -> Result<(), Error> {
    // Posts: ref_ and original_item_hash can be IPFS CIDs (>128 chars)
    sqlx::query("ALTER TABLE posts ALTER COLUMN ref_ TYPE TEXT")
        .execute(pool).await.ok();
    sqlx::query("ALTER TABLE posts ALTER COLUMN original_item_hash TYPE TEXT")
        .execute(pool).await.ok();
    sqlx::query("ALTER TABLE posts ALTER COLUMN post_type TYPE VARCHAR(256)")
        .execute(pool).await.ok();
    // Note: messages.item_hash is VARCHAR(128) PRIMARY KEY — widening it requires
    // an exclusive lock and full table rewrite on a huge table. Skip for now;
    // 128 chars is sufficient for all known hash formats (SHA256 hex = 64 chars).
    Ok(())
}

/// Add denormalized columns to messages table for query performance.
/// Reference: aleph/db/migrations/versions/0046_add_message_fields.py
async fn denormalize_messages(pool: &PgPool) -> Result<(), Error> {
    // status tracks processing state (processed, pending, rejected, forgotten)
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS status VARCHAR NOT NULL DEFAULT 'processed'"
    ).execute(pool).await?;

    // reception_time — when the node first received this message
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS reception_time DOUBLE PRECISION"
    ).execute(pool).await?;

    // Denormalized fields extracted from message content JSON
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS owner VARCHAR(256)"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_type VARCHAR(50)"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_ref TEXT"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_key VARCHAR(256)"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_item_hash VARCHAR(128)"
    ).execute(pool).await?;

    // Chain confirmation metadata
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS first_confirmed_at TIMESTAMPTZ"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS first_confirmed_height BIGINT"
    ).execute(pool).await?;

    // Payment type denormalized from content
    sqlx::query(
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS payment_type VARCHAR(20)"
    ).execute(pool).await?;

    info!("Added denormalized columns to messages table");
    Ok(())
}

/// Create message_confirmations table — links messages to chain_txs.
/// Referenced by chain_sync.rs for recording blockchain confirmations.
async fn create_message_confirmations_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS message_confirmations (
            item_hash VARCHAR(128) NOT NULL,
            tx_hash VARCHAR(256) NOT NULL,
            PRIMARY KEY (item_hash, tx_hash)
        )
    "#)
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_msg_confirmations_item_hash ON message_confirmations(item_hash)"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_msg_confirmations_tx_hash ON message_confirmations(tx_hash)"
    ).execute(pool).await?;

    Ok(())
}

/// Add missing columns to chain_txs that the processor and chain_sync reference.
async fn alter_chain_txs(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(
        "ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS datetime TIMESTAMPTZ"
    ).execute(pool).await?;
    sqlx::query(
        "ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS content JSONB"
    ).execute(pool).await?;

    Ok(())
}

/// Create message_counts table and trigger for fast count queries.
/// Reference: aleph/db/migrations/versions/0047_message_counts.py, 0050_fix_counts.py
async fn create_message_counts_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS message_counts (
            type VARCHAR NOT NULL DEFAULT '',
            status VARCHAR NOT NULL DEFAULT '',
            sender VARCHAR NOT NULL DEFAULT '',
            owner VARCHAR NOT NULL DEFAULT '',
            count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (type, status, sender, owner)
        )
    "#)
    .execute(pool)
    .await?;

    // Create or replace the trigger function that maintains message_counts.
    // Tracks 5 dimension combinations per message change for fast filtered counts.
    sqlx::query(r#"
        CREATE OR REPLACE FUNCTION update_message_counts() RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
                UPDATE message_counts SET count = count - 1
                WHERE (type, status, sender, owner) IN (
                    (OLD.message_type, OLD.status, OLD.sender, COALESCE(OLD.owner, '')),
                    (OLD.message_type, OLD.status, '', ''),
                    ('', OLD.status, OLD.sender, ''),
                    ('', '', OLD.sender, ''),
                    (OLD.message_type, '', '', '')
                );
            END IF;
            IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
                INSERT INTO message_counts (type, status, sender, owner, count)
                VALUES
                    (NEW.message_type, NEW.status, NEW.sender, COALESCE(NEW.owner, ''), 1),
                    (NEW.message_type, NEW.status, '', '', 1),
                    ('', NEW.status, NEW.sender, '', 1),
                    ('', '', NEW.sender, '', 1),
                    (NEW.message_type, '', '', '', 1)
                ON CONFLICT (type, status, sender, owner)
                DO UPDATE SET count = message_counts.count + 1;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    "#)
    .execute(pool)
    .await?;

    // Attach trigger to messages table (DROP first for idempotency)
    sqlx::query(
        "DROP TRIGGER IF EXISTS trg_message_counts ON messages"
    ).execute(pool).await?;

    sqlx::query(r#"
        CREATE TRIGGER trg_message_counts
        AFTER INSERT OR UPDATE OR DELETE ON messages
        FOR EACH ROW EXECUTE FUNCTION update_message_counts()
    "#)
    .execute(pool)
    .await?;

    info!("Created message_counts table and trigger");
    Ok(())
}

async fn create_indexes(pool: &PgPool) -> Result<(), Error> {
    // Messages indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(message_type)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(time DESC)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel)")
        .execute(pool).await?;
    
    // Pending messages indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pending_next_attempt ON pending_messages(next_attempt)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pending_retries ON pending_messages(retries)")
        .execute(pool).await?;
    // Composite index for the processor query: WHERE next_attempt <= $1 AND retries < 10 ORDER BY reception_time
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pending_processing ON pending_messages(next_attempt, reception_time ASC) WHERE retries < 10")
        .execute(pool).await?;
    
    // Aggregates indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_aggregates_address ON aggregates(address)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_aggregates_dirty ON aggregates(dirty) WHERE dirty = TRUE")
        .execute(pool).await?;
    
    // Aggregate elements indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_agg_elements_addr_key ON aggregate_elements(address, key)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_agg_elements_time ON aggregate_elements(time)")
        .execute(pool).await?;
    
    // Posts indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_posts_address ON posts(address)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_posts_type ON posts(post_type)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_posts_time ON posts(time DESC)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_posts_ref ON posts(ref_) WHERE ref_ IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_posts_original ON posts(original_item_hash) WHERE original_item_hash IS NOT NULL")
        .execute(pool).await?;
    
    // File pins indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_file_pins_owner ON file_pins(owner)")
        .execute(pool).await?;
    
    // Chain transactions indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_chain_txs_item_hash ON chain_txs(item_hash)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_chain_txs_height ON chain_txs(chain, height)")
        .execute(pool).await?;
    
    // Programs/Instances indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_programs_owner ON programs(owner)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_instances_owner ON instances(owner)")
        .execute(pool).await?;
    
    // VM versions indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_vm_versions_original ON vm_versions(original_hash)")
        .execute(pool).await?;

    // VM enrichment indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_instances_replaces ON instances(replaces) WHERE replaces IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_programs_replaces ON programs(replaces) WHERE replaces IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_vm_volumes_vm_hash ON vm_machine_volumes(vm_hash)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_account_costs_owner ON account_costs(owner)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_account_costs_item_hash ON account_costs(item_hash)")
        .execute(pool).await?;

    // Forgotten messages indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_forgotten_forget_hash ON forgotten_messages(forget_hash)")
        .execute(pool).await?;
    
    // Messages additional indexes for monitoring queries
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_item_type ON messages(item_type)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at DESC)")
        .execute(pool).await?;

    // Peers indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_peers_type ON peers(peer_type)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen)")
        .execute(pool).await?;

    // Denormalized messages indexes
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_type_status_time ON messages(message_type, status, time DESC)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_owner_time ON messages(owner, time DESC)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_status ON messages(status)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_content_ref ON messages(content_ref) WHERE content_ref IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_content_type ON messages(content_type) WHERE content_type IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_content_key ON messages(content_key) WHERE content_key IS NOT NULL")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_reception_time ON messages(reception_time DESC)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_messages_payment_type ON messages(payment_type) WHERE payment_type IS NOT NULL")
        .execute(pool).await?;

    // file_pins coalesced ref index for store handler lookups
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_file_pins_coalesced_ref ON file_pins(COALESCE(ref_, item_hash)) WHERE pin_type = 'message'")
        .execute(pool).await?;

    // account_costs composite index
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_account_costs_owner_payment_type ON account_costs(owner, payment_type)")
        .execute(pool).await?;

    info!("Created database indexes");
    Ok(())
}

/// Create peers table
/// Matches: aleph/db/models/peers.py PeerDb
async fn create_peers_table(pool: &PgPool) -> Result<(), Error> {
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS peers (
            peer_id TEXT NOT NULL,
            peer_type TEXT NOT NULL,
            address TEXT NOT NULL,
            source TEXT NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (peer_id, peer_type)
        )
    "#)
    .execute(pool)
    .await?;
    
    info!("Created peers table");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Migration tests would go here
}
