require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function makePersonOptional() {
  console.log('🔧 Making person_id optional in placements table...\n');
  
  try {
    await pool.query(`
      ALTER TABLE placements 
      ALTER COLUMN person_id DROP NOT NULL
    `);
    
    console.log('✅ Success! person_id is now optional');
    console.log('\nYou can now create placements with just:');
    console.log('  - Client');
    console.log('  - Role title');
    console.log('  - Fee');
    console.log('  - (Candidate name optional)');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pool.end();
  }
}

makePersonOptional();