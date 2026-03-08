require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function dropAndRecreate() {
  console.log('🗑️  DROPPING OLD TABLES');
  console.log('═'.repeat(60));
  
  try {
    // Drop existing tables
    await pool.query('DROP TABLE IF EXISTS placements CASCADE');
    console.log('✅ Dropped placements');
    
    await pool.query('DROP TABLE IF EXISTS client_financials CASCADE');
    console.log('✅ Dropped client_financials');
    
    await pool.query('DROP TABLE IF EXISTS team_proximity CASCADE');
    console.log('✅ Dropped team_proximity');
    
    console.log('\n📄 CREATING NEW TABLES');
    console.log('═'.repeat(60));
    
    // Read and execute SQL
    const sqlPath = path.join(__dirname, '..', 'sql', 'team_proximity_financials.sql');
    const sql = fs.readFileSync(sqlPath, 'utf8');
    
    await pool.query(sql);
    
    console.log('\n✅ SUCCESS!');
    console.log('═'.repeat(60));
    console.log('Tables created:');
    console.log('  ✅ team_proximity');
    console.log('  ✅ placements');
    console.log('  ✅ client_financials');
    console.log('═'.repeat(60));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

dropAndRecreate();