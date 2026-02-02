-- Initial database schema for Aleph Core
-- Compatible with pyaleph schema for migration purposes

-- Messages table (core)
CREATE TABLE IF NOT EXISTS messages (
    item_hash VARCHAR(64) PRIMARY KEY,
    message_type VARCHAR(20) NOT NULL,
    chain VARCHAR(10) NOT NULL,
    sender VARCHAR(255) NOT NULL,
    signature TEXT NOT NULL,
    item_type VARCHAR(20) NOT NULL,
    item_content TEXT,
    channel VARCHAR(255),
    time DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_messages_sender ON messages(sender);
CREATE INDEX idx_messages_channel ON messages(channel);
CREATE INDEX idx_messages_type ON messages(message_type);
CREATE INDEX idx_messages_time ON messages(time DESC);

-- Aggregates table (key-value storage)
CREATE TABLE IF NOT EXISTS aggregates (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    key VARCHAR(256) NOT NULL,
    content JSONB NOT NULL,
    time DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(address, key)
);

CREATE INDEX idx_aggregates_address ON aggregates(address);
CREATE INDEX idx_aggregates_address_key ON aggregates(address, key);

-- Posts table
CREATE TABLE IF NOT EXISTS posts (
    item_hash VARCHAR(64) PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    post_type VARCHAR(64) NOT NULL,
    content JSONB NOT NULL,
    ref_ VARCHAR(64),
    channel VARCHAR(255),
    time DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_posts_address ON posts(address);
CREATE INDEX idx_posts_type ON posts(post_type);
CREATE INDEX idx_posts_channel ON posts(channel);
CREATE INDEX idx_posts_ref ON posts(ref_);
CREATE INDEX idx_posts_time ON posts(time DESC);

-- Balances table
CREATE TABLE IF NOT EXISTS balances (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL,
    chain VARCHAR(10) NOT NULL,
    balance DECIMAL(36, 18) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(address, chain)
);

CREATE INDEX idx_balances_address ON balances(address);

-- Credit balances table
CREATE TABLE IF NOT EXISTS credit_balances (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255) NOT NULL UNIQUE,
    balance DECIMAL(36, 18) NOT NULL DEFAULT 0,
    expiration TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_credit_balances_address ON credit_balances(address);

-- File pins table (storage tracking)
CREATE TABLE IF NOT EXISTS file_pins (
    item_hash VARCHAR(64) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    size BIGINT NOT NULL,
    content_type VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_file_pins_owner ON file_pins(owner);

-- Programs table
CREATE TABLE IF NOT EXISTS programs (
    item_hash VARCHAR(64) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    code_ref VARCHAR(64) NOT NULL,
    runtime_ref VARCHAR(64) NOT NULL,
    memory INTEGER NOT NULL,
    vcpus INTEGER NOT NULL,
    allow_amend BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_programs_owner ON programs(owner);

-- Instances table
CREATE TABLE IF NOT EXISTS instances (
    item_hash VARCHAR(64) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    rootfs_ref VARCHAR(64) NOT NULL,
    memory INTEGER NOT NULL,
    vcpus INTEGER NOT NULL,
    payment_type VARCHAR(20),
    payment_chain VARCHAR(10),
    allow_amend BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_instances_owner ON instances(owner);

-- Forgotten content table
CREATE TABLE IF NOT EXISTS forgotten (
    item_hash VARCHAR(64) PRIMARY KEY,
    forgotten_by VARCHAR(255) NOT NULL,
    reason TEXT,
    forgotten_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Chain sync state table
CREATE TABLE IF NOT EXISTS chain_sync_state (
    chain VARCHAR(10) PRIMARY KEY,
    last_block BIGINT NOT NULL DEFAULT 0,
    last_sync TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Pending messages table (processing queue)
CREATE TABLE IF NOT EXISTS pending_messages (
    id SERIAL PRIMARY KEY,
    item_hash VARCHAR(64) UNIQUE NOT NULL,
    message_data JSONB NOT NULL,
    reception_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    fetched BOOLEAN DEFAULT false,
    check_message BOOLEAN DEFAULT true,
    retries INTEGER DEFAULT 0,
    next_attempt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_pending_messages_next_attempt ON pending_messages(next_attempt) WHERE fetched = false;
