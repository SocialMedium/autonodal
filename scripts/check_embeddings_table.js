require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function check() {
  const result = await pool.query(`
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'embeddings' 
    ORDER BY ordinal_position
  `);
  
  console.log('Embeddings table columns:');
  result.rows.forEach(r => {
    console.log(`  ${r.column_name}: ${r.data_type}`);
  });
  
  await pool.end();
}

check();
