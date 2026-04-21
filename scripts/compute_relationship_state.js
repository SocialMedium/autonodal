#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — compute commercial relationship_state for each company.
// Only recomputes rows older than 30 days unless --full.
// Usage: node scripts/compute_relationship_state.js [--tenant <uuid>] [--full]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { computeRelationshipState } = require('../lib/relationship_state');

const FULL = process.argv.includes('--full');
const tenantIdx = process.argv.indexOf('--tenant');
const TENANT_ID = tenantIdx !== -1 ? process.argv[tenantIdx + 1] : null;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
  max: 5,
});

async function run() {
  console.log(`Computing relationship_state${TENANT_ID ? ' for tenant ' + TENANT_ID : ' (all tenants)'}${FULL ? ' [FULL]' : ' [stale only]'}...`);

  // Stale = null state, or computed > 30 days ago
  const staleCondition = FULL ? 'TRUE' : `(relationship_state IS NULL OR relationship_state_computed_at < NOW() - INTERVAL '30 days')`;

  const where = TENANT_ID
    ? `WHERE tenant_id = $1 AND ${staleCondition}`
    : `WHERE ${staleCondition}`;
  const params = TENANT_ID ? [TENANT_ID] : [];

  const { rows: companies } = await pool.query(
    `SELECT id, tenant_id, name FROM companies ${where} LIMIT 100000`,
    params
  );

  console.log(`  ${companies.length.toLocaleString()} companies to process`);

  const counts = {};
  let done = 0;

  for (const c of companies) {
    try {
      const state = await computeRelationshipState(pool, c.id, c.tenant_id);
      await pool.query(
        `UPDATE companies SET relationship_state = $1, relationship_state_computed_at = NOW() WHERE id = $2`,
        [state, c.id]
      );
      counts[state] = (counts[state] || 0) + 1;
      done++;
      if (done % 500 === 0) process.stdout.write(`  ${done}...\r`);
    } catch (e) {
      // Skip on error, don't poison the batch
    }
  }

  console.log(`\n\n  State distribution:`);
  Object.entries(counts).sort((a, b) => b[1] - a[1]).forEach(([s, n]) => console.log(`    ${s.padEnd(18)} ${n.toLocaleString()}`));
  console.log(`\n  Total processed: ${done.toLocaleString()}`);

  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
