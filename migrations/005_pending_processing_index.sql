-- Migration 005: Add composite index for pending_messages processing query
-- The message processor queries: WHERE next_attempt <= $1 AND retries < $2 ORDER BY reception_time ASC
-- Without this index, Postgres does a full seq scan + 697MB disk sort on every batch.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pending_processing
    ON pending_messages (next_attempt, reception_time ASC)
    WHERE retries < 10;
