#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — recompute phase for every non-closed signal, log transitions.
// Bulk-writes in batches (user feedback: never per-row through the proxy).
// Usage: node scripts/advance_signal_phases.js [--dry-run]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { currentPhase } = require('../lib/signal_lifecycle');

const DRY_RUN = process.argv.includes('--dry-run');
const BATCH_SIZE = 2000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

async function run() {
  console.log(`Advancing signal phases${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const { rows: signals } = await pool.query(`
    SELECT se.id, se.tenant_id, se.company_id, se.signal_type, se.phase,
           COALESCE(se.first_detected_at, se.detected_at) AS first_detected_at
    FROM signal_events se
    WHERE (se.phase IS NULL OR se.phase != 'closed')
      AND se.polarity IS NOT NULL
  `);
  console.log(`  Evaluating ${signals.length.toLocaleString()} signals...`);

  const { rows: compoundRows } = await pool.query(`
    SELECT company_id, ARRAY_AGG(DISTINCT signal_type::text) AS types
    FROM signal_events
    WHERE company_id IS NOT NULL
      AND detected_at > NOW() - INTERVAL '18 months'
    GROUP BY company_id
  `);
  const compoundMap = {};
  compoundRows.forEach(r => { compoundMap[r.company_id] = r.types; });

  const updates = [];
  const transitionRows = [];
  const counts = {};
  let closed = 0;

  for (const s of signals) {
    const result = currentPhase({
      signal_type: s.signal_type,
      first_detected_at: s.first_detected_at,
      compound_types: compoundMap[s.company_id] || [],
    });
    counts[result.phase] = (counts[result.phase] || 0) + 1;
    if (result.phase === s.phase) continue;

    if (result.phase === 'closed') closed++;
    updates.push({
      id: s.id,
      phase: result.phase,
      critical_at: result.critical_at,
      closing_at: result.closing_at,
      closed_at: result.closed_at,
    });
    transitionRows.push({
      tenant_id: s.tenant_id,
      signal_id: s.id,
      from_phase: s.phase,
      to_phase: result.phase,
      age_days: result.age_days,
    });
  }

  console.log(`\n  Phase distribution:`);
  Object.entries(counts).forEach(([phase, n]) => console.log(`    ${phase.padEnd(10)} ${n.toLocaleString()}`));
  console.log(`\n  ${updates.length.toLocaleString()} transitions${DRY_RUN ? ' (not written)' : ' to write'}`);
  console.log(`  ${closed.toLocaleString()} newly closed`);

  if (DRY_RUN || updates.length === 0) { await pool.end(); return; }

  // Bulk UPDATE via UNNEST
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    await pool.query(
      `UPDATE signal_events AS se SET
         phase = v.phase,
         critical_at = v.critical_at::timestamptz,
         closing_at = v.closing_at::timestamptz,
         closed_at = v.closed_at::timestamptz
       FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[])
         AS v(id, phase, critical_at, closing_at, closed_at)
       WHERE se.id = v.id`,
      [
        batch.map(b => b.id),
        batch.map(b => b.phase),
        batch.map(b => b.critical_at ? new Date(b.critical_at).toISOString() : null),
        batch.map(b => b.closing_at ? new Date(b.closing_at).toISOString() : null),
        batch.map(b => b.closed_at ? new Date(b.closed_at).toISOString() : null),
      ]
    );
    process.stdout.write(`    updates: ${Math.min(i + BATCH_SIZE, updates.length).toLocaleString()}/${updates.length.toLocaleString()}\r`);
  }
  console.log();

  // Bulk INSERT transitions
  for (let i = 0; i < transitionRows.length; i += BATCH_SIZE) {
    const batch = transitionRows.slice(i, i + BATCH_SIZE);
    await pool.query(
      `INSERT INTO signal_phase_transitions (tenant_id, signal_id, from_phase, to_phase, age_days)
       SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::int[])`,
      [
        batch.map(b => b.tenant_id),
        batch.map(b => b.signal_id),
        batch.map(b => b.from_phase),
        batch.map(b => b.to_phase),
        batch.map(b => b.age_days),
      ]
    );
    process.stdout.write(`    transitions: ${Math.min(i + BATCH_SIZE, transitionRows.length).toLocaleString()}/${transitionRows.length.toLocaleString()}\r`);
  }
  console.log();

  console.log(`  ✅ ${updates.length.toLocaleString()} signals updated, ${transitionRows.length.toLocaleString()} transitions logged`);
  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
