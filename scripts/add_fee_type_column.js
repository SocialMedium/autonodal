require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function addFeeTypeColumn() {
  console.log('🔧 Adding fee_type column to placements table...\n');
  
  try {
    // Add the column if it doesn't exist
    await pool.query(`
      ALTER TABLE placements 
      ADD COLUMN IF NOT EXISTS fee_category VARCHAR(50) DEFAULT 'placement'
    `);
    
    console.log('✅ Added fee_category column');
    console.log('\nCategories:');
    console.log('  - placement: Final placement fees');
    console.log('  - retainer: Stage 1, Stage 2 fees');
    console.log('  - project: Research, mapping, other project work');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pool.end();
  }
}

addFeeTypeColumn();
