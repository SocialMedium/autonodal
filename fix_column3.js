require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function fix() {
  console.log('Adding source_url column...');
  await pool.query(`
    ALTER TABLE signal_events 
    ADD COLUMN IF NOT EXISTS source_url TEXT
  `);
  console.log('✓ Fixed!');
  pool.end();
}

fix();
