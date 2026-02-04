-- Migration 002: Expand column widths
-- Fix for: value too long for type character varying(100)
-- Applied to DB as VARCHAR(256) and posts.channel as TEXT

ALTER TABLE messages ALTER COLUMN sender TYPE VARCHAR(256);
ALTER TABLE messages ALTER COLUMN channel TYPE VARCHAR(256);
ALTER TABLE posts ALTER COLUMN channel TYPE TEXT;
