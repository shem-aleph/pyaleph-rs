-- Migration 006: Denormalize messages table and add supporting schema
-- Reference: pyaleph migrations 0046_add_message_fields, 0047_message_counts, 0050_fix_counts
-- Safe for live databases: uses IF NOT EXISTS / IF EXISTS guards throughout.

-- 1. Denormalized columns on messages table
ALTER TABLE messages ADD COLUMN IF NOT EXISTS status VARCHAR NOT NULL DEFAULT 'processed';
ALTER TABLE messages ADD COLUMN IF NOT EXISTS reception_time DOUBLE PRECISION;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS owner VARCHAR(256);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_type VARCHAR(50);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_ref TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_key VARCHAR(256);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS content_item_hash VARCHAR(128);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS first_confirmed_at TIMESTAMPTZ;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS first_confirmed_height BIGINT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS payment_type VARCHAR(20);

-- 2. Indexes for denormalized columns
CREATE INDEX IF NOT EXISTS ix_messages_type_status_time ON messages(message_type, status, time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_owner_time ON messages(owner, time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS ix_messages_content_ref ON messages(content_ref) WHERE content_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_content_type ON messages(content_type) WHERE content_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_content_key ON messages(content_key) WHERE content_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_messages_reception_time ON messages(reception_time DESC);
CREATE INDEX IF NOT EXISTS ix_messages_payment_type ON messages(payment_type) WHERE payment_type IS NOT NULL;

-- 3. message_confirmations table (links messages to chain_txs)
CREATE TABLE IF NOT EXISTS message_confirmations (
    item_hash VARCHAR(128) NOT NULL,
    tx_hash VARCHAR(256) NOT NULL,
    PRIMARY KEY (item_hash, tx_hash)
);
CREATE INDEX IF NOT EXISTS idx_msg_confirmations_item_hash ON message_confirmations(item_hash);
CREATE INDEX IF NOT EXISTS idx_msg_confirmations_tx_hash ON message_confirmations(tx_hash);

-- 4. Add missing columns to chain_txs
ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ;
ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS datetime TIMESTAMPTZ;
ALTER TABLE chain_txs ADD COLUMN IF NOT EXISTS content JSONB;

-- 5. message_counts table for fast filtered counts
CREATE TABLE IF NOT EXISTS message_counts (
    type VARCHAR NOT NULL DEFAULT '',
    status VARCHAR NOT NULL DEFAULT '',
    sender VARCHAR NOT NULL DEFAULT '',
    owner VARCHAR NOT NULL DEFAULT '',
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (type, status, sender, owner)
);

-- 6. Trigger function to maintain message_counts on INSERT/UPDATE/DELETE
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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_message_counts ON messages;
CREATE TRIGGER trg_message_counts
AFTER INSERT OR UPDATE OR DELETE ON messages
FOR EACH ROW EXECUTE FUNCTION update_message_counts();

-- 7. Additional indexes on existing tables
CREATE INDEX IF NOT EXISTS ix_file_pins_coalesced_ref ON file_pins(COALESCE(ref_, item_hash)) WHERE pin_type = 'message';
CREATE INDEX IF NOT EXISTS ix_account_costs_owner_payment_type ON account_costs(owner, payment_type);
