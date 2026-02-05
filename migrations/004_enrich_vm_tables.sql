-- Migration 004: Enrich VM tables for full instance/program handler support
-- Adds columns to instances, programs, vm_versions to match pyaleph VmInstanceDb/VmFunctionDb.
-- Creates vm_machine_volumes table. Recreates account_costs as per-item schema.

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

-- Fix vm_versions schema: rename item_hash -> vm_hash, add current_version + last_updated
-- First check if column exists to handle idempotent re-runs
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'vm_versions' AND column_name = 'item_hash') THEN
        ALTER TABLE vm_versions RENAME COLUMN item_hash TO vm_hash;
    END IF;
END $$;
ALTER TABLE vm_versions ADD COLUMN IF NOT EXISTS current_version VARCHAR(128);
ALTER TABLE vm_versions ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ;
UPDATE vm_versions SET current_version = original_hash WHERE current_version IS NULL;
UPDATE vm_versions SET last_updated = created_at WHERE last_updated IS NULL;

-- Create vm_machine_volumes table for instance/program volumes
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
);
CREATE INDEX IF NOT EXISTS idx_vm_volumes_vm_hash ON vm_machine_volumes(vm_hash);

-- Recreate account_costs as per-item schema matching pyaleph
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
