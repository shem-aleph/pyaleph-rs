-- Migration 003: P2P consumer support
-- Adds trusted_source column and extra indexes for the P2P consumer job.

-- Add trusted_source column to pending_messages if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pending_messages' AND column_name = 'trusted_source'
    ) THEN
        ALTER TABLE pending_messages ADD COLUMN trusted_source BOOLEAN DEFAULT false;
    END IF;
END $$;

-- Index for the content-fetch query: unfetched + item_type + next_attempt
CREATE INDEX IF NOT EXISTS idx_pending_messages_unfetched
    ON pending_messages(next_attempt)
    WHERE fetched = false;

-- Aggregate elements table (needed by message processor, may already exist)
CREATE TABLE IF NOT EXISTS aggregate_elements (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    key VARCHAR(256) NOT NULL,
    content JSONB NOT NULL,
    item_hash VARCHAR(64) NOT NULL REFERENCES messages(item_hash),
    time DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aggregate_elements_address_key
    ON aggregate_elements(address, key);

-- Rejected messages table (if not already present from initial migration)
CREATE TABLE IF NOT EXISTS rejected_messages (
    id SERIAL PRIMARY KEY,
    item_hash VARCHAR(64) NOT NULL,
    message JSONB NOT NULL,
    error_code INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
