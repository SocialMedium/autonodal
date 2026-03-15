require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function setupTeamProximity() {
  console.log('🚀 SETTING UP TEAM PROXIMITY & FINANCIAL INTELLIGENCE');
  console.log('═'.repeat(60));
  
  try {
    const sqlPath = path.join(__dirname, '..', 'sql', 'team_proximity_financials.sql');
    const sql = fs.readFileSync(sqlPath, 'utf8');
    
    console.log('📄 Executing SQL schema...\n');
    
    await pool.query(sql);
    
    console.log('\n✅ SUCCESS!');
    console.log('═'.repeat(60));
    console.log('Tables created:');
    console.log('  ✅ team_proximity');
    console.log('  ✅ conversions');
    console.log('  ✅ account_financials');
    console.log('Triggers created:');
    console.log('  ✅ Auto-create team_proximity on placement insert');
    console.log('═'.repeat(60));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

setupTeamProximity();