require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function checkCounts() {
  console.log('📊 DATABASE RECORD COUNTS\n');
  
  const tables = [
    'people',
    'clients', 
    'projects',
    'searches',
    'search_candidates',
    'interactions',
    'placements',
    'team_proximity'
  ];
  
  for (const table of tables) {
    try {
      const result = await pool.query(`SELECT COUNT(*) FROM ${table}`);
      const count = result.rows[0].count;
      const icon = count > 0 ? '✅' : '⚪';
      console.log(`${icon} ${table.padEnd(20)} ${count}`);
    } catch (error) {
      console.log(`❌ ${table.padEnd(20)} Error: ${error.message}`);
    }
  }
  
  await pool.end();
}

checkCounts();
