require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function addPlacement() {
  const args = process.argv.slice(2);
  
  if (args.length < 3) {
    console.log('Usage: node scripts/add_placement.js "Client Name" "Role Title" FeeAmount [Date]');
    console.log('Example: node scripts/add_placement.js "Acme Corp" "VP Engineering" 45000 "2024-03-15"');
    process.exit(1);
  }
  
  const [clientName, roleTitle, feeAmount, dateStr] = args;
  const placementDate = dateStr || new Date().toISOString().split('T')[0];
  
  console.log('💼 ADDING PLACEMENT');
  console.log('═'.repeat(60));
  console.log(`Client: ${clientName}`);
  console.log(`Role: ${roleTitle}`);
  console.log(`Fee: $${parseFloat(feeAmount).toLocaleString()}`);
  console.log(`Date: ${placementDate}`);
  console.log('');
  
  try {
    // Get current user
    const userResult = await pool.query(
      `SELECT id, name, email FROM users ORDER BY created_at ASC LIMIT 1`
    );
    
    if (userResult.rows.length === 0) {
      console.error('❌ No users found. Please create a user first.');
      process.exit(1);
    }
    
    const user = userResult.rows[0];
    console.log(`Consultant: ${user.name || user.email}`);
    console.log('');
    
    // Find or create client
    let client = await pool.query(
      `SELECT id, name FROM accounts WHERE LOWER(name) = LOWER($1) LIMIT 1`,
      [clientName]
    );
    
    if (client.rows.length === 0) {
      console.log('🆕 Creating new client...');
      client = await pool.query(
  `INSERT INTO accounts (name, relationship_status) VALUES ($1, 'active') RETURNING id, name`,
  [clientName]
);

      console.log(`✅ Client created: ${clientName}`);
    } else {
      console.log(`✅ Client found: ${clientName}`);
    }
    
    const clientId = client.rows[0].id;
    
    // Determine role level
    const title = roleTitle.toLowerCase();
    let roleLevel = 'Mid';
    if (/\b(ceo|cfo|coo|cto|cmo|cpo|chief|president)\b/.test(title)) {
      roleLevel = 'C-level';
    } else if (/\b(vp|vice president|svp|evp)\b/.test(title)) {
      roleLevel = 'VP';
    } else if (/\b(director|head)\b/.test(title)) {
      roleLevel = 'Director';
    } else if (/\b(manager)\b/.test(title)) {
      roleLevel = 'Manager';
    } else if (/\b(senior|sr)\b/.test(title)) {
      roleLevel = 'Senior';
    }
    
    // Create placement
    const placement = await pool.query(`
      INSERT INTO conversions (
        client_id,
        placed_by_user_id,
        role_title,
        role_level,
        placement_fee,
        start_date,
        invoice_date,
        payment_status,
        source
      ) VALUES ($1, $2, $3, $4, $5, $6, $6, 'paid', 'manual')
      RETURNING id
    `, [
      clientId,
      user.id,
      roleTitle,
      roleLevel,
      parseFloat(feeAmount),
      placementDate
    ]);
    
    console.log('');
    console.log('✅ PLACEMENT CREATED!');
    console.log(`   ID: ${placement.rows[0].id}`);
    console.log('');
    console.log('🔄 Updating client financials...');
    
    // Update client financials
    await pool.query(`
      INSERT INTO account_financials (
        client_id, total_invoiced, total_paid, total_placements,
        first_placement_date, last_placement_date, payment_reliability, computed_at
      )
      SELECT 
        $1,
        SUM(placement_fee),
        SUM(CASE WHEN payment_status = 'paid' THEN placement_fee ELSE 0 END),
        COUNT(*),
        MIN(start_date),
        MAX(start_date),
        1.0,
        NOW()
      FROM conversions WHERE client_id = $1
      ON CONFLICT (client_id) DO UPDATE SET
        total_invoiced = EXCLUDED.total_invoiced,
        total_paid = EXCLUDED.total_paid,
        total_placements = EXCLUDED.total_placements,
        first_placement_date = EXCLUDED.first_placement_date,
        last_placement_date = EXCLUDED.last_placement_date,
        computed_at = EXCLUDED.computed_at
    `, [clientId]);
    
    console.log('✅ Client financials updated');
    console.log('');
    console.log('═'.repeat(60));
    console.log('🎉 Done!');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

addPlacement();