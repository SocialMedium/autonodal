#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — compute commercial relationship_state for each company.
// Bulk classifier via CTE — single query per tenant, no per-row round trips.
// Usage: node scripts/compute_relationship_state.js [--tenant <uuid>] [--full]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const FULL = process.argv.includes('--full');
const tenantIdx = process.argv.indexOf('--tenant');
const TENANT_ID = tenantIdx !== -1 ? process.argv[tenantIdx + 1] : null;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

const OPEN_STATUSES = [
  'briefing', 'research', 'sourcing', 'outreach', 'shortlist',
  'interviewing', 'presenting', 'placement', 'live',
];

async function run() {
  console.log(`Computing relationship_state${TENANT_ID ? ' for tenant ' + TENANT_ID : ' (all tenants)'}${FULL ? ' [FULL]' : ' [stale only]'}...`);

  const tenantFilter = TENANT_ID ? 'AND c.tenant_id = $1' : '';
  const staleFilter = FULL ? '' : `AND (c.relationship_state IS NULL OR c.relationship_state_computed_at < NOW() - INTERVAL '30 days')`;
  const params = TENANT_ID ? [TENANT_ID] : [];

  // Single bulk classifier — all logic in one CTE chain
  const { rowCount, rows } = await pool.query(`
    WITH target_companies AS (
      SELECT c.id, c.tenant_id, LOWER(TRIM(c.name)) AS name_norm
      FROM companies c
      WHERE c.tenant_id IS NOT NULL
        ${tenantFilter}
        ${staleFilter}
    ),
    -- Open mandate via accounts → engagements → opportunities
    open_mandate AS (
      SELECT DISTINCT tc.id AS company_id
      FROM target_companies tc
      JOIN accounts a ON a.tenant_id = tc.tenant_id
        AND (a.company_id = tc.id OR LOWER(TRIM(a.name)) = tc.name_norm)
      JOIN engagements e ON e.client_id = a.id AND e.tenant_id = tc.tenant_id
      JOIN opportunities o ON o.project_id = e.id AND o.tenant_id = tc.tenant_id
      WHERE o.status::text = ANY($${params.length + 1}::text[])
    ),
    -- Most recent conversion.start_date per company
    last_mandate AS (
      SELECT tc.id AS company_id, MAX(conv.start_date) AS last_date
      FROM target_companies tc
      LEFT JOIN accounts a ON a.tenant_id = tc.tenant_id
        AND (a.company_id = tc.id OR LOWER(TRIM(a.name)) = tc.name_norm)
      JOIN conversions conv ON conv.tenant_id = tc.tenant_id
        AND (conv.client_id = a.id OR LOWER(TRIM(conv.client_name_raw)) = tc.name_norm)
      WHERE conv.placement_fee > 0
        AND conv.start_date IS NOT NULL
      GROUP BY tc.id
    ),
    -- Max team proximity per company
    max_proximity AS (
      SELECT tc.id AS company_id, MAX(tp.relationship_strength) AS max_strength
      FROM target_companies tc
      JOIN people p ON p.current_company_id = tc.id AND p.tenant_id = tc.tenant_id
      JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = tc.tenant_id
      GROUP BY tc.id
    ),
    classified AS (
      SELECT
        tc.id,
        CASE
          WHEN om.company_id IS NOT NULL THEN 'active_client'
          WHEN lm.last_date IS NOT NULL AND lm.last_date > NOW() - INTERVAL '18 months' THEN 'active_client'
          WHEN lm.last_date IS NOT NULL THEN 'ex_client'
          WHEN COALESCE(mp.max_strength, 0) >= 0.6 THEN 'warm_non_client'
          WHEN COALESCE(mp.max_strength, 0) >= 0.3 THEN 'cool_non_client'
          ELSE 'cold_non_client'
        END AS state
      FROM target_companies tc
      LEFT JOIN open_mandate om ON om.company_id = tc.id
      LEFT JOIN last_mandate lm ON lm.company_id = tc.id
      LEFT JOIN max_proximity mp ON mp.company_id = tc.id
    )
    UPDATE companies c
    SET relationship_state = classified.state,
        relationship_state_computed_at = NOW()
    FROM classified
    WHERE c.id = classified.id
    RETURNING classified.state
  `, [...params, OPEN_STATUSES]);

  const counts = {};
  rows.forEach(r => { counts[r.state] = (counts[r.state] || 0) + 1; });

  console.log(`  ${rowCount.toLocaleString()} companies updated\n`);
  console.log(`  State distribution:`);
  Object.entries(counts).sort((a, b) => b[1] - a[1]).forEach(([s, n]) =>
    console.log(`    ${s.padEnd(18)} ${n.toLocaleString()}`)
  );

  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
