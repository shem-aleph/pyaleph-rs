//! Backfill job — populates derived tables (posts, aggregates) from the messages table.
//!
//! This handles the gap where messages were synced via the indexer directly into the
//! `messages` table but never processed through the message_processor pipeline
//! (which only watches `pending_messages`).
//!
//! The backfill runs automatically on startup and can also be triggered manually.
//! It's idempotent — uses ON CONFLICT DO NOTHING, so it's safe to re-run.
//!
//! Currently backfills:
//! - POST messages → posts table
//! - AGGREGATE messages → aggregate_elements + aggregates tables
//!
//! Only processes messages with inline item_content (non-NULL).
//! Messages with item_type = 'storage'/'ipfs' that lack item_content are skipped
//! (they'd need content fetching from IPFS first).

use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{info, warn, error};

/// Batch size for backfill processing
const BACKFILL_BATCH_SIZE: i64 = 5000;

/// Interval between batches to avoid overwhelming the DB (ms)
const BATCH_PAUSE_MS: u64 = 50;

/// Tracks whether a backfill is currently running (prevents concurrent runs)
static BACKFILL_RUNNING: AtomicBool = AtomicBool::new(false);

/// Result of a backfill run
#[derive(Debug)]
pub struct BackfillResult {
    pub posts_inserted: u64,
    pub posts_skipped: u64,
    pub posts_errors: u64,
    pub aggregates_inserted: u64,
    pub aggregates_errors: u64,
    pub instances_inserted: u64,
    pub instances_errors: u64,
    pub programs_inserted: u64,
    pub programs_errors: u64,
    pub messages_denorm_updated: u64,
    pub duration_secs: f64,
}

impl std::fmt::Display for BackfillResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Backfill complete in {:.1}s: posts inserted={}, skipped={}, errors={} | aggregates inserted={}, errors={} | instances inserted={}, errors={} | programs inserted={}, errors={} | messages denorm={}",
            self.duration_secs,
            self.posts_inserted,
            self.posts_skipped,
            self.posts_errors,
            self.aggregates_inserted,
            self.aggregates_errors,
            self.instances_inserted,
            self.instances_errors,
            self.programs_inserted,
            self.programs_errors,
            self.messages_denorm_updated,
        )
    }
}

/// Run the full backfill on startup.
///
/// This checks if there are messages in the `messages` table that don't have
/// corresponding entries in derived tables, and backfills them.
///
/// Safe to call multiple times — idempotent via ON CONFLICT DO NOTHING.
pub async fn run_startup_backfill(pool: &PgPool) -> Result<BackfillResult, String> {
    // Prevent concurrent backfills
    if BACKFILL_RUNNING.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return Err("Backfill already running".to_string());
    }

    let start = Instant::now();

    // Skip backfill when the pending queue is large — the message processor will handle
    // everything. Backfill only matters after sync is mostly done and there are stragglers.
    let pending_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pending_messages")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Failed to count pending: {}", e))?;

    if pending_count.0 > 10_000 {
        BACKFILL_RUNNING.store(false, Ordering::SeqCst);
        info!(
            "Backfill skipped: {} pending messages in queue — processor will handle them",
            pending_count.0
        );
        return Ok(BackfillResult {
            posts_inserted: 0, posts_skipped: 0, posts_errors: 0,
            aggregates_inserted: 0, aggregates_errors: 0,
            instances_inserted: 0, instances_errors: 0,
            programs_inserted: 0, programs_errors: 0,
            messages_denorm_updated: 0,
            duration_secs: start.elapsed().as_secs_f64(),
        });
    }

    info!("Starting backfill: checking for unprocessed messages...");

    // Check how many POST messages lack entries in the posts table
    let missing_posts: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM messages m
        WHERE m.message_type = 'POST'
        AND m.item_content IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.item_hash = m.item_hash)
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count missing posts: {}", e))?;

    // Check missing aggregates
    let missing_aggregates: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM messages m
        WHERE m.message_type = 'AGGREGATE'
        AND m.item_content IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM aggregate_elements ae WHERE ae.item_hash = m.item_hash)
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count missing aggregates: {}", e))?;

    // Check missing instances
    let missing_instances: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM messages m
        WHERE m.message_type = 'INSTANCE'
        AND m.item_content IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM instances i WHERE i.item_hash = m.item_hash)
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count missing instances: {}", e))?;

    // Check missing programs
    let missing_programs: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM messages m
        WHERE m.message_type = 'PROGRAM'
        AND m.item_content IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM programs p WHERE p.item_hash = m.item_hash)
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count missing programs: {}", e))?;

    info!(
        "Backfill needed: {} posts, {} aggregates, {} instances, {} programs to process",
        missing_posts.0, missing_aggregates.0, missing_instances.0, missing_programs.0
    );

    // Check if denormalized columns need backfilling
    let missing_denorm: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM messages WHERE owner IS NULL AND item_content IS NOT NULL AND item_content != ''"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to count missing denorm: {}", e))?;

    info!(
        "Backfill needed: {} denormalized messages to update",
        missing_denorm.0
    );

    if missing_posts.0 == 0 && missing_aggregates.0 == 0 && missing_instances.0 == 0 && missing_programs.0 == 0 && missing_denorm.0 == 0 {
        BACKFILL_RUNNING.store(false, Ordering::SeqCst);
        let result = BackfillResult {
            posts_inserted: 0,
            posts_skipped: 0,
            posts_errors: 0,
            aggregates_inserted: 0,
            aggregates_errors: 0,
            instances_inserted: 0,
            instances_errors: 0,
            programs_inserted: 0,
            programs_errors: 0,
            messages_denorm_updated: 0,
            duration_secs: start.elapsed().as_secs_f64(),
        };
        info!("Backfill: nothing to do, all tables up to date");
        return Ok(result);
    }

    // Run backfills
    let posts_result = backfill_posts(pool).await;
    let aggregates_result = backfill_aggregates(pool).await;
    let instances_result = backfill_instances(pool).await;
    let programs_result = backfill_programs(pool).await;
    let denorm_result = backfill_message_denorm(pool).await;

    BACKFILL_RUNNING.store(false, Ordering::SeqCst);

    let (posts_inserted, posts_skipped, posts_errors) = posts_result
        .map_err(|e| format!("Posts backfill failed: {}", e))?;
    let (aggregates_inserted, aggregates_errors) = aggregates_result
        .map_err(|e| format!("Aggregates backfill failed: {}", e))?;
    let (instances_inserted, instances_errors) = instances_result
        .map_err(|e| format!("Instances backfill failed: {}", e))?;
    let (programs_inserted, programs_errors) = programs_result
        .map_err(|e| format!("Programs backfill failed: {}", e))?;
    let messages_denorm_updated = denorm_result
        .map_err(|e| format!("Message denorm backfill failed: {}", e))?;

    let result = BackfillResult {
        posts_inserted,
        posts_skipped,
        posts_errors,
        aggregates_inserted,
        aggregates_errors,
        instances_inserted,
        instances_errors,
        programs_inserted,
        programs_errors,
        messages_denorm_updated,
        duration_secs: start.elapsed().as_secs_f64(),
    };

    info!("{}", result);
    Ok(result)
}

/// Backfill the posts table from POST messages in the messages table.
///
/// Uses a server-side SQL approach for maximum throughput:
/// Parses item_content JSON directly in PostgreSQL to extract fields.
async fn backfill_posts(pool: &PgPool) -> Result<(u64, u64, u64), String> {
    info!("Backfilling posts table...");

    let mut total_inserted: u64 = 0;
    let mut total_skipped: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut last_time: f64 = 0.0;
    let mut batch_num: u64 = 0;

    loop {
        batch_num += 1;

        // Use a single SQL INSERT...SELECT to backfill in batches.
        // This parses item_content JSON directly in PostgreSQL — no Rust deserialization needed.
        // We process in time order to handle amend→original dependencies correctly.
        let result = sqlx::query(
            r#"
            WITH raw_batch AS (
                SELECT
                    m.item_hash,
                    m.channel,
                    m.time AS msg_time,
                    replace(m.item_content, '\u0000', '')::jsonb AS jb
                FROM messages m
                WHERE m.message_type = 'POST'
                AND m.item_content IS NOT NULL
                AND m.item_content != ''
                AND m.time > $1
                AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.item_hash = m.item_hash)
                ORDER BY m.time ASC
                LIMIT $2
            ),
            batch AS (
                SELECT
                    item_hash,
                    jb->>'address' AS address,
                    jb->>'type' AS post_type,
                    jb->'content' AS content,
                    jb->>'ref' AS ref_,
                    channel,
                    COALESCE((jb->>'time')::double precision, msg_time) AS content_time,
                    msg_time
                FROM raw_batch
            )
            INSERT INTO posts (item_hash, address, post_type, content, ref_, channel, time, original_item_hash)
            SELECT
                b.item_hash,
                b.address,
                b.post_type,
                COALESCE(b.content, '{}'::jsonb),
                b.ref_,
                b.channel,
                b.content_time,
                -- For amends, set original_item_hash to the ref (simplified; doesn't chase chains)
                CASE WHEN LOWER(b.post_type) = 'amend' THEN b.ref_ ELSE NULL END
            FROM batch b
            WHERE b.address IS NOT NULL
            AND b.post_type IS NOT NULL
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(last_time)
        .bind(BACKFILL_BATCH_SIZE)
        .execute(pool)
        .await
        .map_err(|e| format!("Posts backfill batch {} failed: {}", batch_num, e))?;

        let rows_affected = result.rows_affected();

        // Check if we got a full batch (need to continue) by looking at actual messages available
        let next_batch: Vec<(f64,)> = sqlx::query_as(
            r#"
            SELECT m.time FROM messages m
            WHERE m.message_type = 'POST'
            AND m.item_content IS NOT NULL
            AND m.time > $1
            ORDER BY m.time ASC
            LIMIT $2
            "#
        )
        .bind(last_time)
        .bind(BACKFILL_BATCH_SIZE)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Batch cursor query failed: {}", e))?;

        let batch_size = next_batch.len() as u64;
        let inserted = rows_affected;
        let skipped = batch_size.saturating_sub(inserted);

        total_inserted += inserted;
        total_skipped += skipped;

        if batch_size == 0 {
            break;
        }

        // Advance cursor to the last message time in this batch
        if let Some(last) = next_batch.last() {
            last_time = last.0;
        } else {
            break;
        }

        if batch_num % 20 == 0 {
            info!(
                "Posts backfill progress: batch={}, inserted={}, skipped={}, cursor_time={:.0}",
                batch_num, total_inserted, total_skipped, last_time
            );
        }

        // Don't go too fast
        if batch_size as i64 >= BACKFILL_BATCH_SIZE {
            tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_PAUSE_MS)).await;
        } else {
            // Last partial batch — we're done
            break;
        }
    }

    // Now handle amend chains: update latest_amend on original posts
    info!("Updating latest_amend references for amend posts...");
    let amend_result = sqlx::query(
        r#"
        WITH latest_amends AS (
            SELECT DISTINCT ON (original_item_hash)
                original_item_hash,
                item_hash AS latest_amend_hash
            FROM posts
            WHERE LOWER(post_type) = 'amend'
            AND original_item_hash IS NOT NULL
            ORDER BY original_item_hash, time DESC
        )
        UPDATE posts p
        SET latest_amend = la.latest_amend_hash
        FROM latest_amends la
        WHERE p.item_hash = la.original_item_hash
        AND (p.latest_amend IS NULL OR p.latest_amend != la.latest_amend_hash)
        "#
    )
    .execute(pool)
    .await;

    match amend_result {
        Ok(r) => info!("Updated {} latest_amend references", r.rows_affected()),
        Err(e) => warn!("Failed to update latest_amend references: {}", e),
    }

    info!(
        "Posts backfill complete: inserted={}, skipped={}, errors={}",
        total_inserted, total_skipped, total_errors
    );

    Ok((total_inserted, total_skipped, total_errors))
}

/// Backfill the aggregate_elements and aggregates tables from AGGREGATE messages.
///
/// Aggregates are trickier because they need to be merged in time order.
/// Strategy: insert all elements first, then rebuild aggregates.
async fn backfill_aggregates(pool: &PgPool) -> Result<(u64, u64), String> {
    info!("Backfilling aggregates...");

    let mut total_inserted: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut last_time: f64 = 0.0;
    let mut batch_num: u64 = 0;

    // Check if aggregate_elements table exists
    let table_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'aggregate_elements')"
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to check aggregate_elements table: {}", e))?;

    if !table_exists.0 {
        warn!("aggregate_elements table does not exist, skipping aggregate backfill");
        return Ok((0, 0));
    }

    // Ensure unique index on item_hash exists for ON CONFLICT
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_elements_item_hash ON aggregate_elements(item_hash)"
    )
    .execute(pool)
    .await
    .map_err(|e| format!("Failed to create aggregate_elements unique index: {}", e))?;

    loop {
        batch_num += 1;

        // Insert aggregate elements
        let result = sqlx::query(
            r#"
            WITH raw_batch AS (
                SELECT
                    m.item_hash,
                    m.time AS msg_time,
                    replace(m.item_content, '\u0000', '')::jsonb AS jb
                FROM messages m
                WHERE m.message_type = 'AGGREGATE'
                AND m.item_content IS NOT NULL
                AND m.item_content != ''
                AND m.time > $1
                AND NOT EXISTS (SELECT 1 FROM aggregate_elements ae WHERE ae.item_hash = m.item_hash)
                ORDER BY m.time ASC
                LIMIT $2
            ),
            batch AS (
                SELECT
                    item_hash,
                    jb->>'key' AS agg_key,
                    jb->>'address' AS address,
                    jb->'content' AS content,
                    COALESCE((jb->>'time')::double precision, msg_time) AS time,
                    msg_time
                FROM raw_batch
            )
            INSERT INTO aggregate_elements (item_hash, key, address, content, time)
            SELECT
                b.item_hash,
                b.agg_key,
                b.address,
                COALESCE(b.content, '{}'::jsonb),
                b.time
            FROM batch b
            WHERE b.agg_key IS NOT NULL
            AND b.address IS NOT NULL
            ON CONFLICT (item_hash) DO NOTHING
            "#
        )
        .bind(last_time)
        .bind(BACKFILL_BATCH_SIZE)
        .execute(pool)
        .await
        .map_err(|e| format!("Aggregate backfill batch {} failed: {}", batch_num, e))?;

        total_inserted += result.rows_affected();

        // Get batch cursor
        let next_batch: Vec<(f64,)> = sqlx::query_as(
            r#"
            SELECT m.time FROM messages m
            WHERE m.message_type = 'AGGREGATE'
            AND m.item_content IS NOT NULL
            AND m.time > $1
            ORDER BY m.time ASC
            LIMIT $2
            "#
        )
        .bind(last_time)
        .bind(BACKFILL_BATCH_SIZE)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Batch cursor query failed: {}", e))?;

        let batch_size = next_batch.len() as u64;

        if batch_size == 0 {
            break;
        }

        if let Some(last) = next_batch.last() {
            last_time = last.0;
        } else {
            break;
        }

        if batch_num % 20 == 0 {
            info!(
                "Aggregate elements backfill progress: batch={}, inserted={}, cursor_time={:.0}",
                batch_num, total_inserted, last_time
            );
        }

        if batch_size as i64 >= BACKFILL_BATCH_SIZE {
            tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_PAUSE_MS)).await;
        } else {
            break;
        }
    }

    // Now rebuild all aggregates from their elements.
    // For each (owner, key) pair, merge all elements in time order.
    info!("Rebuilding aggregates from elements...");

    let rebuild_result = sqlx::query(
        r#"
        INSERT INTO aggregates (address, key, content, time, last_revision_hash, dirty)
        SELECT
            ae.address,
            ae.key,
            -- Use the latest element's content as a placeholder.
            -- Marked dirty so they get properly rebuilt on first access.
            (
                SELECT ae2.content
                FROM aggregate_elements ae2
                WHERE ae2.key = ae.key AND ae2.address = ae.address
                ORDER BY ae2.time DESC
                LIMIT 1
            ),
            MIN(ae.time),
            (
                SELECT ae3.item_hash
                FROM aggregate_elements ae3
                WHERE ae3.key = ae.key AND ae3.address = ae.address
                ORDER BY ae3.time DESC
                LIMIT 1
            ),
            true  -- Mark as dirty so they get properly rebuilt on first access
        FROM aggregate_elements ae
        GROUP BY ae.address, ae.key
        ON CONFLICT (address, key) DO UPDATE SET
            dirty = true
        "#
    )
    .execute(pool)
    .await;

    match rebuild_result {
        Ok(r) => info!("Rebuilt/updated {} aggregates (marked dirty for proper merge on access)", r.rows_affected()),
        Err(e) => {
            warn!("Aggregate rebuild failed (may need manual intervention): {}", e);
            total_errors += 1;
        }
    }

    info!(
        "Aggregate backfill complete: elements inserted={}, errors={}",
        total_inserted, total_errors
    );

    Ok((total_inserted, total_errors))
}

/// Backfill the instances table from INSTANCE messages in the messages table.
///
/// Parses item_content JSON in PostgreSQL and inserts into the enriched instances table.
/// Idempotent via ON CONFLICT DO NOTHING.
async fn backfill_instances(pool: &PgPool) -> Result<(u64, u64), String> {
    info!("Backfilling instances table...");

    let result = sqlx::query(
        r#"
        WITH parsed AS (
            SELECT m.item_hash, m.sender, m.time AS msg_time,
                   replace(m.item_content, '\u0000', '')::jsonb AS jb
            FROM messages m
            WHERE m.message_type = 'INSTANCE'
            AND m.item_content IS NOT NULL AND m.item_content != ''
            AND NOT EXISTS (SELECT 1 FROM instances i WHERE i.item_hash = m.item_hash)
        )
        INSERT INTO instances (item_hash, owner, rootfs_ref, memory, vcpus, payment_type, payment_chain,
            allow_amend, replaces, environment_reproducible, environment_internet, environment_aleph_api,
            environment_shared_cache, environment_hypervisor, resources_seconds, metadata, variables,
            authorized_keys, rootfs_use_latest, rootfs_persistence, rootfs_size_mib, node_hash, time, created_at)
        SELECT
            p.item_hash, p.sender,
            p.jb->'rootfs'->'parent'->>'ref',
            COALESCE((p.jb->'resources'->>'memory')::integer, 0),
            COALESCE((p.jb->'resources'->>'vcpus')::integer, 0),
            p.jb->'payment'->>'type',
            p.jb->'payment'->>'chain',
            COALESCE((p.jb->>'allow_amend')::boolean, true),
            p.jb->>'replaces',
            COALESCE((p.jb->'environment'->>'reproducible')::boolean, false),
            COALESCE((p.jb->'environment'->>'internet')::boolean, true),
            COALESCE((p.jb->'environment'->>'aleph_api')::boolean, true),
            COALESCE((p.jb->'environment'->>'shared_cache')::boolean, false),
            p.jb->'environment'->>'hypervisor',
            COALESCE((p.jb->'resources'->>'seconds')::integer, 30),
            p.jb->'metadata',
            p.jb->'variables',
            p.jb->'authorized_keys',
            COALESCE((p.jb->'rootfs'->'parent'->>'use_latest')::boolean, true),
            p.jb->'rootfs'->>'persistence',
            (p.jb->'rootfs'->>'size_mib')::integer,
            p.jb->'requirements'->'node'->>'node_hash',
            COALESCE((p.jb->>'time')::double precision, p.msg_time),
            NOW()
        FROM parsed p
        ON CONFLICT (item_hash) DO NOTHING
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| format!("Instance backfill failed: {}", e))?;

    let inserted = result.rows_affected();
    info!("Instances backfill complete: inserted={}", inserted);
    Ok((inserted, 0))
}

/// Backfill the programs table from PROGRAM messages in the messages table.
async fn backfill_programs(pool: &PgPool) -> Result<(u64, u64), String> {
    info!("Backfilling programs table...");

    let result = sqlx::query(
        r#"
        WITH parsed AS (
            SELECT m.item_hash, m.sender, m.time AS msg_time,
                   replace(m.item_content, '\u0000', '')::jsonb AS jb
            FROM messages m
            WHERE m.message_type = 'PROGRAM'
            AND m.item_content IS NOT NULL AND m.item_content != ''
            AND NOT EXISTS (SELECT 1 FROM programs p WHERE p.item_hash = m.item_hash)
        )
        INSERT INTO programs (item_hash, owner, code_ref, runtime_ref, memory, vcpus, allow_amend,
            replaces, environment_reproducible, environment_internet, environment_aleph_api,
            environment_shared_cache, environment_hypervisor, resources_seconds, metadata, variables,
            payment_type, payment_chain, node_hash, time, created_at)
        SELECT
            p.item_hash, p.sender,
            p.jb->'code'->>'ref',
            p.jb->'runtime'->>'ref',
            COALESCE((p.jb->'resources'->>'memory')::integer, 0),
            COALESCE((p.jb->'resources'->>'vcpus')::integer, 0),
            COALESCE((p.jb->>'allow_amend')::boolean, true),
            p.jb->>'replaces',
            COALESCE((p.jb->'environment'->>'reproducible')::boolean, false),
            COALESCE((p.jb->'environment'->>'internet')::boolean, true),
            COALESCE((p.jb->'environment'->>'aleph_api')::boolean, true),
            COALESCE((p.jb->'environment'->>'shared_cache')::boolean, false),
            p.jb->'environment'->>'hypervisor',
            COALESCE((p.jb->'resources'->>'seconds')::integer, 30),
            p.jb->'metadata',
            p.jb->'variables',
            p.jb->'payment'->>'type',
            p.jb->'payment'->>'chain',
            p.jb->'requirements'->'node'->>'node_hash',
            COALESCE((p.jb->>'time')::double precision, p.msg_time),
            NOW()
        FROM parsed p
        ON CONFLICT (item_hash) DO NOTHING
        "#
    )
    .execute(pool)
    .await
    .map_err(|e| format!("Program backfill failed: {}", e))?;

    let inserted = result.rows_affected();
    info!("Programs backfill complete: inserted={}", inserted);
    Ok((inserted, 0))
}

/// Backfill denormalized columns on the messages table and populate message_counts.
///
/// Extracts fields from item_content JSON (stored as TEXT) into dedicated columns
/// for fast filtering without JSON parsing at query time. Also populates
/// first_confirmed_at/first_confirmed_height from chain_txs, and rebuilds
/// the message_counts materialization.
async fn backfill_message_denorm(pool: &PgPool) -> Result<u64, String> {
    info!("Backfilling denormalized message columns...");

    // Disable the message_counts trigger during backfill to avoid:
    // 1. Wasted trigger overhead during bulk Phase 1 UPDATEs
    // 2. Race condition: Phase 3 TRUNCATEs message_counts and rebuilds, but
    //    concurrent processor inserts between TRUNCATE and rebuild lose counts
    sqlx::query("ALTER TABLE messages DISABLE TRIGGER trg_message_counts")
        .execute(pool)
        .await
        .map_err(|e| format!("Failed to disable message_counts trigger: {}", e))?;

    // Use a helper closure pattern: run the actual work, then always re-enable the trigger
    let result = backfill_message_denorm_inner(pool).await;

    // Always re-enable the trigger, even on error
    if let Err(e) = sqlx::query("ALTER TABLE messages ENABLE TRIGGER trg_message_counts")
        .execute(pool)
        .await
    {
        error!("Failed to re-enable message_counts trigger: {} — manual intervention needed", e);
    }

    result
}

/// Inner implementation of message denorm backfill (runs with trigger disabled).
async fn backfill_message_denorm_inner(pool: &PgPool) -> Result<u64, String> {
    let mut total_updated: u64 = 0;
    let mut last_time: f64 = 0.0;
    let mut batch_num: u64 = 0;

    // Phase 1: Backfill content-derived columns in batches
    loop {
        batch_num += 1;

        // Use a single CTE that updates and returns the cursor + count,
        // avoiding a separate query that can race with the UPDATE on duplicate times.
        let row: Option<(Option<f64>, i64)> = sqlx::query_as(
            r#"
            WITH batch AS (
                SELECT item_hash, time,
                       replace(item_content, '\u0000', '')::jsonb AS jb
                FROM messages
                WHERE owner IS NULL
                AND item_content IS NOT NULL AND item_content != ''
                AND time > $1
                ORDER BY time ASC
                LIMIT $2
            ),
            updated AS (
                UPDATE messages m SET
                    owner = b.jb->>'address',
                    content_type = b.jb->>'type',
                    content_ref = b.jb->>'ref',
                    content_key = b.jb->>'key',
                    content_item_hash = b.jb->>'item_hash',
                    payment_type = b.jb->'payment'->>'type',
                    status = 'processed'
                FROM batch b
                WHERE m.item_hash = b.item_hash
                RETURNING m.time
            )
            SELECT MAX(time) AS max_time, COUNT(*) AS cnt FROM updated
            "#
        )
        .bind(last_time)
        .bind(BACKFILL_BATCH_SIZE)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("Message denorm batch {} failed: {}", batch_num, e))?;

        let (max_time, batch_count) = match row {
            Some((Some(t), c)) if c > 0 => (t, c as u64),
            _ => break, // No rows updated — we're done
        };

        total_updated += batch_count;
        last_time = max_time;

        if batch_num % 20 == 0 {
            info!(
                "Message denorm progress: batch={}, updated={}, cursor_time={:.0}",
                batch_num, total_updated, last_time
            );
        }

        if batch_count as i64 >= BACKFILL_BATCH_SIZE {
            tokio::time::sleep(tokio::time::Duration::from_millis(BATCH_PAUSE_MS)).await;
        } else {
            break;
        }
    }

    // Also set status='processed' for any rows that still have NULL owner
    // (messages with no item_content, e.g. storage/ipfs types)
    let status_result = sqlx::query(
        "UPDATE messages SET status = 'processed' WHERE status IS NULL OR status = ''"
    )
    .execute(pool)
    .await;

    match status_result {
        Ok(r) if r.rows_affected() > 0 => {
            info!("Set status='processed' on {} messages without content", r.rows_affected());
        }
        Ok(_) => {}
        Err(e) => warn!("Failed to set default status: {}", e),
    }

    // Phase 2: Backfill first_confirmed_at / first_confirmed_height from chain_txs
    info!("Backfilling chain confirmation metadata...");
    let confirm_result = sqlx::query(
        r#"
        UPDATE messages m SET
            first_confirmed_at = ct.min_confirmed,
            first_confirmed_height = ct.min_height
        FROM (
            SELECT item_hash,
                   MIN(COALESCE(confirmed_at, created_at)) AS min_confirmed,
                   MIN(height) AS min_height
            FROM chain_txs
            GROUP BY item_hash
        ) ct
        WHERE m.item_hash = ct.item_hash
        AND m.first_confirmed_at IS NULL
        "#
    )
    .execute(pool)
    .await;

    match confirm_result {
        Ok(r) => info!("Backfilled chain confirmations for {} messages", r.rows_affected()),
        Err(e) => warn!("Chain confirmation backfill failed: {}", e),
    }

    // Phase 3: Rebuild message_counts from scratch
    info!("Rebuilding message_counts table...");
    let truncate_result = sqlx::query("TRUNCATE message_counts")
        .execute(pool)
        .await;
    if let Err(e) = truncate_result {
        warn!("Failed to truncate message_counts (may not exist yet): {}", e);
    }

    // Insert the 5 dimension combinations
    let counts_result = sqlx::query(
        r#"
        INSERT INTO message_counts (type, status, sender, owner, count)
        SELECT message_type, status, sender, COALESCE(owner, ''), COUNT(*)
        FROM messages GROUP BY message_type, status, sender, COALESCE(owner, '')
        UNION ALL
        SELECT message_type, status, '', '', COUNT(*)
        FROM messages GROUP BY message_type, status
        UNION ALL
        SELECT '', status, sender, '', COUNT(*)
        FROM messages GROUP BY status, sender
        UNION ALL
        SELECT '', '', sender, '', COUNT(*)
        FROM messages GROUP BY sender
        UNION ALL
        SELECT message_type, '', '', '', COUNT(*)
        FROM messages GROUP BY message_type
        ON CONFLICT (type, status, sender, owner)
        DO UPDATE SET count = EXCLUDED.count
        "#
    )
    .execute(pool)
    .await;

    match counts_result {
        Ok(r) => info!("Populated message_counts with {} rows", r.rows_affected()),
        Err(e) => warn!("message_counts population failed: {}", e),
    }

    info!("Message denorm backfill complete: {} rows updated", total_updated);
    Ok(total_updated)
}

/// Check if backfill is needed (quick check, doesn't run it)
pub async fn needs_backfill(pool: &PgPool) -> Result<bool, String> {
    let result: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM (
            SELECT 1 FROM messages m
            WHERE m.message_type IN ('POST', 'AGGREGATE')
            AND m.item_content IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM posts p WHERE p.item_hash = m.item_hash
                AND m.message_type = 'POST'
            )
            AND NOT EXISTS (
                SELECT 1 FROM aggregate_elements ae WHERE ae.item_hash = m.item_hash
                AND m.message_type = 'AGGREGATE'
            )
            LIMIT 1
        ) sub
        "#
    )
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Backfill check failed: {}", e))?;

    Ok(result.0 > 0)
}
