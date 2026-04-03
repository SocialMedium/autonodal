#!/usr/bin/env node
// Migrate: create event_listings table for EventMedium ingestion
require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function migrate() {
  console.log('Running event_listings migration...\n');

  const migrations = [
    `CREATE TABLE IF NOT EXISTS event_listings (
      id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id         UUID NOT NULL REFERENCES tenants(id),
      external_url      TEXT NOT NULL,
      name              TEXT NOT NULL,
      description       TEXT,
      event_date        DATE NOT NULL,
      city              TEXT,
      country           TEXT,
      region            TEXT,
      themes            TEXT[],
      rsvp_count        INTEGER DEFAULT 0,
      expected_attendees INTEGER,
      status            TEXT DEFAULT 'upcoming',
      theme_score       DECIMAL(4,3) DEFAULT 0,
      ingested_at       TIMESTAMPTZ DEFAULT NOW(),
      updated_at        TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(external_url, tenant_id)
    )`,
    `CREATE INDEX IF NOT EXISTS idx_event_listings_tenant_region ON event_listings(tenant_id, region, event_date)`,
    `CREATE INDEX IF NOT EXISTS idx_event_listings_status ON event_listings(status, event_date)`
  ];

  for (const sql of migrations) {
    try {
      await pool.query(sql);
      console.log('✓', sql.substring(0, 70) + '...');
    } catch (err) {
      console.log('⚠', sql.substring(0, 70) + '...', err.message);
    }
  }

  console.log('\n✓ event_listings migration complete!');
  await pool.end();
}

migrate().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
