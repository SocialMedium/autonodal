-- Migration: Add Ezekia integration fields to people table
-- Run with: psql $DATABASE_URL -f sql/migration_ezekia.sql

-- Add synced_at timestamp for tracking external syncs
ALTER TABLE people ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ;

-- Add first_name and last_name for better name handling
ALTER TABLE people ADD COLUMN IF NOT EXISTS first_name VARCHAR(100);
ALTER TABLE people ADD COLUMN IF NOT EXISTS last_name VARCHAR(100);

-- Add additional contact fields
ALTER TABLE people ADD COLUMN IF NOT EXISTS phone VARCHAR(50);
ALTER TABLE people ADD COLUMN IF NOT EXISTS twitter_url TEXT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS github_url TEXT;

-- Add profile photo
ALTER TABLE people ADD COLUMN IF NOT EXISTS profile_photo_url TEXT;

-- Add city/country for granular location
ALTER TABLE people ADD COLUMN IF NOT EXISTS city VARCHAR(100);
ALTER TABLE people ADD COLUMN IF NOT EXISTS country VARCHAR(100);

-- Add enrichment data JSONB for storing raw API responses
ALTER TABLE people ADD COLUMN IF NOT EXISTS enrichment_data JSONB;
ALTER TABLE people ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;

-- Add ezekia_data JSONB for Ezekia-specific metadata
ALTER TABLE people ADD COLUMN IF NOT EXISTS ezekia_data JSONB;

-- Index for efficient sync queries
CREATE INDEX IF NOT EXISTS idx_people_source ON people (source, source_id);
CREATE INDEX IF NOT EXISTS idx_people_synced ON people (synced_at) WHERE source = 'ezekia';

-- Verify
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'people' 
ORDER BY ordinal_position;
