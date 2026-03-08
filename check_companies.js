require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL, 
  ssl: { rejectUnauthorized: false }
});

async function check() {
  console.log('🔍 Checking companies...\n');
  
  const companies = await pool.query(`
    SELECT id, name, created_at 
    FROM companies 
    ORDER BY created_at DESC 
    LIMIT 15
  `);
  
  console.log('Recent companies:');
  console.table(companies.rows);
  
  const signals = await pool.query(`
    SELECT 
      se.id,
      se.company_id,
      c.name as company_name,
      se.signal_type,
      se.evidence_snippet
    FROM signal_events se
    LEFT JOIN companies c ON se.company_id = c.id
    LIMIT 5
  `);
  
  console.log('\nSignal-Company linkage:');
  console.table(signals.rows);
  
  pool.end();
}

check().catch(console.error);
