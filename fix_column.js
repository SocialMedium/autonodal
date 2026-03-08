require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function fix() {
  console.log('Adding missing column...');
  await pool.query(`
    ALTER TABLE signal_events 
    ADD COLUMN IF NOT EXISTS evidence_snippet TEXT
  `);
  console.log('✓ Fixed! Column added.');
  pool.end();
}

fix();
