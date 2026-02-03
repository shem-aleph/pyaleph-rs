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
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn, error, debug};

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
    pub duration_secs: f64,
}

impl std::fmt::Display for BackfillResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Backfill complete in {:.1}s: posts inserted={}, skipped={}, errors={} | aggregates inserted={}, errors={}",
            self.duration_secs,
            self.posts_inserted,
            self.posts_skipped,
            self.posts_errors,
            self.aggregates_inserted,
            self.aggregates_errors,
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

    info!(
        "Backfill needed: {} posts, {} aggregates to process",
        missing_posts.0, missing_aggregates.0
    );

    if missing_posts.0 == 0 && missing_aggregates.0 == 0 {
        BACKFILL_RUNNING.store(false, Ordering::SeqCst);
        let result = BackfillResult {
            posts_inserted: 0,
            posts_skipped: 0,
            posts_errors: 0,
            aggregates_inserted: 0,
            aggregates_errors: 0,
            duration_secs: start.elapsed().as_secs_f64(),
        };
        info!("Backfill: nothing to do, all tables up to date");
        return Ok(result);
    }

    // Run backfills
    let posts_result = backfill_posts(pool).await;
    let aggregates_result = backfill_aggregates(pool).await;

    BACKFILL_RUNNING.store(false, Ordering::SeqCst);

    let (posts_inserted, posts_skipped, posts_errors) = posts_result
        .map_err(|e| format!("Posts backfill failed: {}", e))?;
    let (aggregates_inserted, aggregates_errors) = aggregates_result
        .map_err(|e| format!("Aggregates backfill failed: {}", e))?;

    let result = BackfillResult {
        posts_inserted,
        posts_skipped,
        posts_errors,
        aggregates_inserted,
        aggregates_errors,
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
            WITH batch AS (
                SELECT
                    m.item_hash,
                    m.item_content::jsonb->>'address' AS address,
                    m.item_content::jsonb->>'type' AS post_type,
                    m.item_content::jsonb->'content' AS content,
                    m.item_content::jsonb->>'ref' AS ref_,
                    m.channel,
                    COALESCE((m.item_content::jsonb->>'time')::double precision, m.time) AS content_time,
                    m.time AS msg_time
                FROM messages m
                WHERE m.message_type = 'POST'
                AND m.item_content IS NOT NULL
                AND m.time > $1
                AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.item_hash = m.item_hash)
                ORDER BY m.time ASC
                LIMIT $2
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

    loop {
        batch_num += 1;

        // Insert aggregate elements
        let result = sqlx::query(
            r#"
            WITH batch AS (
                SELECT
                    m.item_hash,
                    m.item_content::jsonb->>'key' AS agg_key,
                    m.item_content::jsonb->>'address' AS owner,
                    m.item_content::jsonb->'content' AS content,
                    COALESCE(
                        to_timestamp((m.item_content::jsonb->>'time')::double precision),
                        m.created_at
                    ) AS creation_datetime,
                    m.time AS msg_time
                FROM messages m
                WHERE m.message_type = 'AGGREGATE'
                AND m.item_content IS NOT NULL
                AND m.time > $1
                AND NOT EXISTS (SELECT 1 FROM aggregate_elements ae WHERE ae.item_hash = m.item_hash)
                ORDER BY m.time ASC
                LIMIT $2
            )
            INSERT INTO aggregate_elements (item_hash, key, owner, content, creation_datetime)
            SELECT
                b.item_hash,
                b.agg_key,
                b.owner,
                COALESCE(b.content, '{}'::jsonb),
                b.creation_datetime
            FROM batch b
            WHERE b.agg_key IS NOT NULL
            AND b.owner IS NOT NULL
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
        INSERT INTO aggregates (key, owner, content, creation_datetime, last_revision_hash, dirty)
        SELECT
            ae.key,
            ae.owner,
            -- Use jsonb_object_agg or a custom merge; for now just use the latest element's content
            -- as a placeholder. Proper merge needs the jsonb_merge aggregate function.
            -- If jsonb_merge exists, use it; otherwise fall back to marking as dirty.
            (
                SELECT ae2.content
                FROM aggregate_elements ae2
                WHERE ae2.key = ae.key AND ae2.owner = ae.owner
                ORDER BY ae2.creation_datetime DESC
                LIMIT 1
            ),
            MIN(ae.creation_datetime),
            (
                SELECT ae3.item_hash
                FROM aggregate_elements ae3
                WHERE ae3.key = ae.key AND ae3.owner = ae.owner
                ORDER BY ae3.creation_datetime DESC
                LIMIT 1
            ),
            true  -- Mark as dirty so they get properly rebuilt on first access
        FROM aggregate_elements ae
        GROUP BY ae.key, ae.owner
        ON CONFLICT (key, owner) DO UPDATE SET
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
