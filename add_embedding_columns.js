require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function addColumns() {
  console.log('📊 Adding embedding columns...\n');
  
  await pool.query(`
    ALTER TABLE signal_events 
    ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
  `);
  console.log('✓ Added embedded_at to signal_events');
  
  await pool.query(`
    ALTER TABLE people 
    ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
  `);
  console.log('✓ Added embedded_at to people');
  
  await pool.query(`
    ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
  `);
  console.log('✓ Added embedded_at to companies');
  
  console.log('\n✅ Database ready for embeddings!');
  pool.end();
}

addColumns().catch(console.error);
