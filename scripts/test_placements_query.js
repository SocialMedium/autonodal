require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function testQuery() {
  try {
    // Get Acme Corp ID
    const client = await pool.query(`SELECT id, name FROM accounts WHERE name = 'Acme Corp'`);
    console.log('Acme Corp:', client.rows[0]);
    
    const clientId = client.rows[0].id;
    
    // Simple query - just get placements for this client
    const placements = await pool.query(`
      SELECT * FROM conversions WHERE client_id = $1
    `, [clientId]);
    
    console.log('\nPlacements:', placements.rows.length);
    placements.rows.forEach(p => {
      console.log('  -', p.role_title, '$' + p.placement_fee);
    });
    
  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    await pool.end();
  }
}

testQuery();