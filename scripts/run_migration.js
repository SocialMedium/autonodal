#!/usr/bin/env node
// Run Ezekia migration without psql
require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function migrate() {
  console.log('Running Ezekia migration...\n');
  
  const migrations = [
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS synced_at TIMESTAMPTZ`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS first_name VARCHAR(100)`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS last_name VARCHAR(100)`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS phone VARCHAR(50)`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS twitter_url TEXT`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS github_url TEXT`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS profile_photo_url TEXT`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS city VARCHAR(100)`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS country VARCHAR(100)`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS enrichment_data JSONB`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ`,
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS ezekia_data JSONB`,
    `CREATE INDEX IF NOT EXISTS idx_people_source ON people (source, source_id)`,
    `CREATE INDEX IF NOT EXISTS idx_people_synced ON people (synced_at) WHERE source = 'ezekia'`
  ];

  for (const sql of migrations) {
    try {
      await pool.query(sql);
      console.log('✓', sql.substring(0, 60) + '...');
    } catch (err) {
      console.log('⚠', sql.substring(0, 60) + '...', err.message);
    }
  }

  console.log('\n✓ Migration complete!');
  await pool.end();
}

migrate().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
