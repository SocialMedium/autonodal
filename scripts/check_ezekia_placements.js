require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function checkEzekiaData() {
  console.log('🔍 CHECKING EZEKIA DATA FOR PLACEMENTS\n');
  
  try {
    const candidates = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN status = 'placed' THEN 1 END) as placed_count,
        COUNT(CASE WHEN status = 'shortlisted' THEN 1 END) as shortlisted_count,
        array_agg(DISTINCT status) as all_statuses
      FROM pipeline_contacts
    `);

    console.log('📊 PIPELINE_CONTACTS TABLE:');
    console.log('  Total candidates:', candidates.rows[0].total);
    console.log('  Placed:', candidates.rows[0].placed_count);
    console.log('  Shortlisted:', candidates.rows[0].shortlisted_count);
    console.log('  All statuses:', candidates.rows[0].all_statuses?.join(', '), '\n');
    
    const placedSample = await pool.query(`
      SELECT 
        p.full_name,
        s.title as search_title,
        c.name as client_name,
        sc.status,
        sc.added_date
      FROM pipeline_contacts sc
      JOIN people p ON sc.person_id = p.id
      JOIN opportunities s ON sc.search_id = s.id
      JOIN engagements pr ON s.project_id = pr.id
      JOIN accounts c ON pr.client_id = c.id
      WHERE sc.status = 'placed'
      ORDER BY sc.added_date DESC
      LIMIT 5
    `);
    
    if (placedSample.rows.length > 0) {
      console.log('✅ SAMPLE PLACED CANDIDATES:');
      placedSample.rows.forEach(row => {
        console.log('  •', row.full_name);
        console.log('    Client:', row.client_name);
        console.log('    Role:', row.search_title);
        console.log('    Date:', row.added_date || 'N/A');
        console.log('');
      });
    } else {
      console.log('⚠️  No placed candidates found\n');
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pool.end();
  }
}

checkEzekiaData();