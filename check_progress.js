require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function check() {
  const r1 = await pool.query("SELECT COUNT(*) FROM interactions WHERE interaction_type = 'research_note'");
  const r2 = await pool.query("SELECT COUNT(DISTINCT person_id) FROM interactions WHERE interaction_type = 'research_note'");
  const r3 = await pool.query("SELECT COUNT(*) FROM people WHERE source = 'ezekia'");
  console.log('Research notes stored:', r1.rows[0].count);
  console.log('Unique people with notes:', r2.rows[0].count);
  console.log('Ezekia people:', r3.rows[0].count);
  pool.end();
}
check();
