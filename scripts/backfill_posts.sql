-- Backfill posts table from messages table
-- Run this manually: PGPASSWORD=aleph psql -h localhost -U aleph -d aleph -f backfill_posts.sql
--
-- Safe to re-run: uses ON CONFLICT DO NOTHING

\timing on

-- Step 1: Insert posts from inline POST messages
-- Processes ALL POST messages with item_content, extracts fields from JSON
DO $$
DECLARE
    batch_size INT := 10000;
    total_inserted BIGINT := 0;
    batch_inserted BIGINT;
    last_time DOUBLE PRECISION := 0;
    max_time DOUBLE PRECISION;
BEGIN
    -- Get the max time to know when we're done
    SELECT MAX(time) INTO max_time 
    FROM messages WHERE message_type = 'POST' AND item_content IS NOT NULL;
    
    IF max_time IS NULL THEN
        RAISE NOTICE 'No POST messages to backfill';
        RETURN;
    END IF;
    
    RAISE NOTICE 'Starting posts backfill...';
    
    LOOP
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
            AND m.time > last_time
            AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.item_hash = m.item_hash)
            ORDER BY m.time ASC
            LIMIT batch_size
        ),
        inserted AS (
            INSERT INTO posts (item_hash, address, post_type, content, ref_, channel, time, original_item_hash)
            SELECT
                b.item_hash,
                b.address,
                b.post_type,
                COALESCE(b.content, '{}'::jsonb),
                b.ref_,
                b.channel,
                b.content_time,
                CASE WHEN LOWER(b.post_type) = 'amend' THEN b.ref_ ELSE NULL END
            FROM batch b
            WHERE b.address IS NOT NULL
            AND b.post_type IS NOT NULL
            ON CONFLICT (item_hash) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) INTO batch_inserted FROM inserted;
        
        -- Advance cursor
        SELECT MAX(m.time) INTO last_time
        FROM messages m
        WHERE m.message_type = 'POST'
        AND m.item_content IS NOT NULL
        AND m.time > last_time
        ORDER BY m.time ASC
        LIMIT batch_size;
        
        -- Actually advance properly
        SELECT MAX(sub.time) INTO last_time FROM (
            SELECT m.time
            FROM messages m
            WHERE m.message_type = 'POST'
            AND m.item_content IS NOT NULL
            AND m.time > COALESCE(last_time, 0)
            ORDER BY m.time ASC
            LIMIT batch_size
        ) sub;
        
        total_inserted := total_inserted + batch_inserted;
        
        IF last_time IS NULL OR last_time >= max_time THEN
            EXIT;
        END IF;
        
        IF total_inserted % 50000 < batch_size THEN
            RAISE NOTICE 'Progress: % posts inserted, cursor at time=%', total_inserted, last_time;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Posts backfill complete: % posts inserted', total_inserted;
END $$;

-- Step 2: Update latest_amend for original posts
UPDATE posts p
SET latest_amend = la.latest_amend_hash
FROM (
    SELECT DISTINCT ON (original_item_hash)
        original_item_hash,
        item_hash AS latest_amend_hash
    FROM posts
    WHERE LOWER(post_type) = 'amend'
    AND original_item_hash IS NOT NULL
    ORDER BY original_item_hash, time DESC
) la
WHERE p.item_hash = la.original_item_hash
AND (p.latest_amend IS NULL OR p.latest_amend != la.latest_amend_hash);

-- Step 3: Show results
SELECT 
    COUNT(*) AS total_posts,
    COUNT(*) FILTER (WHERE LOWER(post_type) = 'amend') AS amend_posts,
    COUNT(*) FILTER (WHERE latest_amend IS NOT NULL) AS posts_with_amends,
    MIN(time) AS earliest,
    MAX(time) AS latest
FROM posts;
