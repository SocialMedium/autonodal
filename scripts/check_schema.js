// Check database schema
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

async function checkSchema() {
  try {
    console.log('=== Checking users table columns ===');
    const usersColumns = await pool.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = 'users' AND column_name LIKE '%name%' 
      ORDER BY ordinal_position
    `);
    console.log('Users name columns:', usersColumns.rows);
    
    console.log('\n=== Checking conversions table date columns ===');
    const placementsColumns = await pool.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name = 'conversions' AND column_name LIKE '%date%'
      ORDER BY ordinal_position
    `);
    console.log('Placements date columns:', placementsColumns.rows);
    
    console.log('\n=== Sample user record ===');
    const sampleUser = await pool.query('SELECT * FROM users LIMIT 1');
    console.log('Sample user:', sampleUser.rows[0]);
    
    console.log('\n=== Sample placement record ===');
    const samplePlacement = await pool.query(`
      SELECT 
        p.*,
        u.name as user_name,
        u.full_name as user_full_name,
        u.email as user_email
      FROM conversions p
      LEFT JOIN users u ON p.placed_by_user_id = u.id
      LIMIT 1
    `);
    console.log('Sample placement:', samplePlacement.rows[0]);
    
    await pool.end();
  } catch (error) {
    console.error('Error:', error.message);
    await pool.end();
  }
}

checkSchema();
