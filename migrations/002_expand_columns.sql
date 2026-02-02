-- Migration 002: Expand column widths
-- Fix for: value too long for type character varying(100)

ALTER TABLE messages ALTER COLUMN sender TYPE VARCHAR(255);
ALTER TABLE messages ALTER COLUMN channel TYPE VARCHAR(255);
ALTER TABLE posts ALTER COLUMN channel TYPE VARCHAR(255);

