#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — recompute phase for every non-closed signal, log transitions.
// Usage: node scripts/advance_signal_phases.js [--dry-run]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { currentPhase } = require('../lib/signal_lifecycle');

const DRY_RUN = process.argv.includes('--dry-run');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

async function run() {
  console.log(`Advancing signal phases${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  // Fetch all non-closed signals. Pull compound types per company/window to apply compression.
  const { rows: signals } = await pool.query(`
    SELECT se.id, se.tenant_id, se.signal_type, se.phase,
           COALESCE(se.first_detected_at, se.detected_at) AS first_detected_at
    FROM signal_events se
    WHERE (se.phase IS NULL OR se.phase != 'closed')
      AND se.polarity IS NOT NULL
    ORDER BY se.tenant_id, se.company_id, se.detected_at
  `);

  console.log(`  Evaluating ${signals.length.toLocaleString()} signals...`);

  // Group compound signal types per company (18-month window)
  const { rows: compoundRows } = await pool.query(`
    SELECT company_id,
           ARRAY_AGG(DISTINCT signal_type::text) AS types
    FROM signal_events
    WHERE company_id IS NOT NULL
      AND detected_at > NOW() - INTERVAL '18 months'
    GROUP BY company_id
  `);
  const compoundMap = {};
  compoundRows.forEach(r => { compoundMap[r.company_id] = r.types; });

  let transitions = 0;
  let closed = 0;
  const counts = {};

  for (const s of signals) {
    const result = currentPhase({
      signal_type: s.signal_type,
      first_detected_at: s.first_detected_at,
      compound_types: compoundMap[s.company_id] || [],
    });

    counts[result.phase] = (counts[result.phase] || 0) + 1;

    if (result.phase === s.phase) continue;

    transitions++;
    if (result.phase === 'closed') closed++;

    if (!DRY_RUN) {
      await pool.query(
        `UPDATE signal_events
         SET phase = $1, critical_at = $2, closing_at = $3, closed_at = $4
         WHERE id = $5`,
        [result.phase, result.critical_at, result.closing_at, result.closed_at, s.id]
      );
      await pool.query(
        `INSERT INTO signal_phase_transitions (tenant_id, signal_id, from_phase, to_phase, age_days)
         VALUES ($1, $2, $3, $4, $5)`,
        [s.tenant_id, s.id, s.phase, result.phase, result.age_days]
      );
    }
  }

  console.log(`\n  Phase distribution:`);
  Object.entries(counts).forEach(([phase, n]) => console.log(`    ${phase.padEnd(10)} ${n.toLocaleString()}`));
  console.log(`\n  ${transitions.toLocaleString()} transitions${DRY_RUN ? ' (not written)' : ' written'}`);
  console.log(`  ${closed.toLocaleString()} newly closed`);

  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
