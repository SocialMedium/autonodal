#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/calculate_team_proximity.js
// Compute team_proximity from all interaction types:
//   - Email (gmail_sync)
//   - Research notes (ezekia, ezekia_enrich)
//   - LinkedIn messages (linkedin_import)
//   - Meetings (gcal_sync)
//   - Shared projects (pipeline_contacts)
//
// Usage:
//   node scripts/calculate_team_proximity.js          # Full recalc
//   node scripts/calculate_team_proximity.js --user=X # Single user
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

// ============================================================================
// EMAIL PROXIMITY
// ============================================================================

async function calculateEmailProximity(userId) {
  const result = await pool.query(`
    WITH counts AS (
      SELECT person_id, COUNT(*) AS cnt, MAX(interaction_at) AS latest
      FROM interactions
      WHERE user_id = $1
        AND interaction_type IN ('email', 'email_sent', 'email_received')
        AND person_id IS NOT NULL
      GROUP BY person_id
    )
    INSERT INTO team_proximity (
      person_id, team_member_id, relationship_type, relationship_strength,
      source, interaction_count, last_interaction_date, tenant_id
    )
    SELECT
      c.person_id, $1,
      CASE WHEN c.cnt >= 10 THEN 'email_frequent'
           WHEN c.cnt >= 3  THEN 'email_moderate'
           ELSE 'email_minimal' END,
      CASE WHEN c.cnt >= 10 THEN 0.85
           WHEN c.cnt >= 3  THEN 0.60
           ELSE 0.30 END,
      'gmail_sync', c.cnt, c.latest, $2
    FROM counts c
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
      last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
      updated_at = NOW()
  `, [userId, TENANT_ID]);
  return result.rowCount;
}

// ============================================================================
// RESEARCH NOTE PROXIMITY
// ============================================================================

async function calculateNoteProximity(userId) {
  const result = await pool.query(`
    WITH counts AS (
      SELECT person_id, COUNT(*) AS cnt, MAX(interaction_at) AS latest
      FROM interactions
      WHERE user_id = $1
        AND interaction_type IN ('research_note', 'note')
        AND person_id IS NOT NULL
      GROUP BY person_id
    )
    INSERT INTO team_proximity (
      person_id, team_member_id, relationship_type, relationship_strength,
      source, interaction_count, last_interaction_date, tenant_id
    )
    SELECT
      c.person_id, $1,
      'research_note',
      CASE WHEN c.cnt >= 20 THEN 0.90
           WHEN c.cnt >= 10 THEN 0.80
           WHEN c.cnt >= 5  THEN 0.65
           WHEN c.cnt >= 2  THEN 0.45
           ELSE 0.25 END,
      'ezekia', c.cnt, c.latest, $2
    FROM counts c
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
      last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
      updated_at = NOW()
  `, [userId, TENANT_ID]);
  return result.rowCount;
}

// ============================================================================
// LINKEDIN MESSAGE PROXIMITY
// ============================================================================

async function calculateLinkedInProximity(userId) {
  const result = await pool.query(`
    WITH counts AS (
      SELECT person_id, COUNT(*) AS cnt, MAX(interaction_at) AS latest
      FROM interactions
      WHERE user_id = $1
        AND interaction_type = 'linkedin_message'
        AND person_id IS NOT NULL
      GROUP BY person_id
    )
    INSERT INTO team_proximity (
      person_id, team_member_id, relationship_type, relationship_strength,
      source, interaction_count, last_interaction_date, tenant_id
    )
    SELECT
      c.person_id, $1,
      'linkedin_message',
      CASE WHEN c.cnt >= 10 THEN 0.80
           WHEN c.cnt >= 3  THEN 0.55
           ELSE 0.30 END,
      'linkedin_import', c.cnt, c.latest, $2
    FROM counts c
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
      last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
      updated_at = NOW()
  `, [userId, TENANT_ID]);
  return result.rowCount;
}

// ============================================================================
// MEETING PROXIMITY
// ============================================================================

async function calculateMeetingProximity(userId) {
  const result = await pool.query(`
    WITH counts AS (
      SELECT person_id, COUNT(*) AS cnt, MAX(interaction_at) AS latest
      FROM interactions
      WHERE user_id = $1
        AND interaction_type = 'meeting'
        AND person_id IS NOT NULL
      GROUP BY person_id
    )
    INSERT INTO team_proximity (
      person_id, team_member_id, relationship_type, relationship_strength,
      source, interaction_count, last_interaction_date, tenant_id
    )
    SELECT
      c.person_id, $1,
      CASE WHEN c.cnt >= 5 THEN 'meeting_frequent'
           WHEN c.cnt >= 2 THEN 'meeting_moderate'
           ELSE 'meeting_single' END,
      CASE WHEN c.cnt >= 5 THEN 0.90
           WHEN c.cnt >= 2 THEN 0.70
           ELSE 0.45 END,
      'gcal_sync', c.cnt, c.latest, $2
    FROM counts c
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
      last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
      updated_at = NOW()
  `, [userId, TENANT_ID]);
  return result.rowCount;
}

// ============================================================================
// SHARED PROJECT PROXIMITY
// ============================================================================

async function calculateProjectProximity() {
  const result = await pool.query(`
    WITH project_links AS (
      SELECT DISTINCT
        sc.person_id,
        s.lead_consultant_id AS team_member_id,
        COUNT(DISTINCT s.id) AS project_count,
        MAX(s.updated_at) AS latest
      FROM pipeline_contacts sc
      JOIN opportunities s ON sc.search_id = s.id
      WHERE s.lead_consultant_id IS NOT NULL AND sc.person_id IS NOT NULL
      GROUP BY sc.person_id, s.lead_consultant_id
    )
    INSERT INTO team_proximity (
      person_id, team_member_id, relationship_type, relationship_strength,
      source, interaction_count, last_interaction_date, tenant_id
    )
    SELECT
      pl.person_id, pl.team_member_id,
      'shared_project',
      CASE WHEN pl.project_count >= 3 THEN 0.85
           WHEN pl.project_count >= 2 THEN 0.65
           ELSE 0.40 END,
      'pipeline', pl.project_count, pl.latest, $1
    FROM project_links pl
    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
      interaction_count = EXCLUDED.interaction_count,
      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
      last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
      updated_at = NOW()
  `, [TENANT_ID]);
  return result.rowCount;
}

// ============================================================================
// MAIN
// ============================================================================

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Team Proximity Calculation');
  console.log('═══════════════════════════════════════════════════════\n');

  var args = process.argv.slice(2);
  var singleUser = (args.find(a => a.startsWith('--user=')) || '').split('=')[1];

  var users;
  if (singleUser) {
    users = await pool.query("SELECT id, name FROM users WHERE id = $1 OR name ILIKE $2 OR email ILIKE $2", [singleUser, '%' + singleUser + '%']);
  } else {
    users = await pool.query("SELECT id, name FROM users WHERE tenant_id = $1", [TENANT_ID]);
  }

  console.log('Users: ' + users.rows.length + '\n');

  var totals = { email: 0, note: 0, linkedin: 0, meeting: 0, project: 0 };

  for (var u of users.rows) {
    console.log('👤 ' + u.name);
    var e = await calculateEmailProximity(u.id);
    var n = await calculateNoteProximity(u.id);
    var l = await calculateLinkedInProximity(u.id);
    var m = await calculateMeetingProximity(u.id);
    console.log('   email: ' + e + ' | notes: ' + n + ' | linkedin: ' + l + ' | meetings: ' + m);
    totals.email += e; totals.note += n; totals.linkedin += l; totals.meeting += m;
  }

  console.log('\n📋 Shared projects...');
  totals.project = await calculateProjectProximity();
  console.log('   projects: ' + totals.project);

  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  Results');
  console.log('  Email:    ' + totals.email);
  console.log('  Notes:    ' + totals.note);
  console.log('  LinkedIn: ' + totals.linkedin);
  console.log('  Meetings: ' + totals.meeting);
  console.log('  Projects: ' + totals.project);
  console.log('  Total:    ' + (totals.email + totals.note + totals.linkedin + totals.meeting + totals.project));
  console.log('═══════════════════════════════════════════════════════');

  return totals;
}

module.exports = { run, calculateEmailProximity, calculateNoteProximity, calculateLinkedInProximity, calculateMeetingProximity, calculateProjectProximity };

if (require.main === module) {
  run()
    .then(() => { pool.end(); process.exit(0); })
    .catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
}
