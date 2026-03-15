#!/usr/bin/env node

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

// ============================================================================
// EMAIL PROXIMITY
// ============================================================================

async function calculateEmailProximity(userId) {
  console.log(`📧 Calculating email proximity for user ${userId}...`);
  
  const result = await pool.query(`
    WITH email_counts AS (
      SELECT 
        i.person_id,
        COUNT(*) as email_count,
        MAX(i.interaction_date) as last_email_date
      FROM interactions i
      WHERE i.user_id = $1 
        AND i.interaction_type = 'email'
        AND i.person_id IS NOT NULL
      GROUP BY i.person_id
    )
    INSERT INTO team_proximity (
      person_id,
      team_member_id,
      relationship_type,
      relationship_strength,
      source,
      interaction_count,
      last_interaction_date
    )
    SELECT 
      ec.person_id,
      $1,
      CASE 
        WHEN ec.email_count >= 10 THEN 'email_frequent'
        WHEN ec.email_count >= 3 THEN 'email_moderate'
        ELSE 'email_minimal'
      END,
      CASE 
        WHEN ec.email_count >= 10 THEN 0.85
        WHEN ec.email_count >= 3 THEN 0.60
        ELSE 0.30
      END,
      'gmail',
      ec.email_count,
      ec.last_email_date
    FROM email_counts ec
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = EXCLUDED.relationship_strength,
      last_interaction_date = EXCLUDED.last_interaction_date,
      updated_at = NOW()
  `, [userId]);
  
  console.log(`  ✅ Created ${result.rowCount} email proximity records\n`);
  return result.rowCount;
}

async function calculateAllEmailProximity() {
  console.log('📧 EMAIL PROXIMITY CALCULATION');
  console.log('═'.repeat(60));
  
  const users = await pool.query(`
    SELECT id, full_name FROM users WHERE role IN ('admin', 'consultant')
  `);
  
  let totalEmails = 0;
  
  for (const user of users.rows) {
    console.log(`\n👤 ${user.full_name}`);
    const count = await calculateEmailProximity(user.id);
    totalEmails += count;
  }
  
  console.log('\n' + '═'.repeat(60));
  console.log(`Total email relationships: ${totalEmails}`);
  console.log('═'.repeat(60) + '\n');
}

// ============================================================================
// SHARED PROJECT PROXIMITY
// ============================================================================

async function calculateSharedProjectProximity() {
  console.log('🤝 SHARED PROJECT PROXIMITY CALCULATION');
  console.log('═'.repeat(60));
  
  const result = await pool.query(`
    WITH project_members AS (
      SELECT DISTINCT
        sc.person_id,
        s.assigned_to as team_member_id,
        s.id as search_id,
        s.title as search_title
      FROM pipeline_contacts sc
      JOIN opportunities s ON sc.search_id = s.id
      WHERE s.assigned_to IS NOT NULL