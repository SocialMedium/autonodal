#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Backfill polarity + first_detected_at on existing signal_events
// Safe to re-run. Only touches rows where the target column is NULL.
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { POLARITY } = require('../lib/signal_polarity');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

async function run() {
  console.log('Backfilling polarity + first_detected_at on signal_events...');

  // first_detected_at defaults to detected_at where NULL
  const { rowCount: fdUpdated } = await pool.query(
    `UPDATE signal_events SET first_detected_at = detected_at WHERE first_detected_at IS NULL`
  );
  console.log(`  ✅ first_detected_at: backfilled ${fdUpdated.toLocaleString()} rows`);

  // Polarity per signal_type
  let totalPolarity = 0;
  const unmatched = [];
  for (const [type, polarity] of Object.entries(POLARITY)) {
    const { rowCount } = await pool.query(
      `UPDATE signal_events SET polarity = $1 WHERE polarity IS NULL AND signal_type = $2::signal_type`,
      [polarity, type]
    );
    if (rowCount > 0) {
      console.log(`  ✅ ${type.padEnd(22)} → ${polarity.padEnd(8)} (${rowCount.toLocaleString()} rows)`);
      totalPolarity += rowCount;
    }
  }

  // Check for unmatched signal types
  const { rows: unmatchedTypes } = await pool.query(
    `SELECT signal_type::text, COUNT(*) AS cnt FROM signal_events WHERE polarity IS NULL GROUP BY signal_type ORDER BY cnt DESC`
  );
  if (unmatchedTypes.length > 0) {
    console.log('\n  ⚠️  Unmatched signal types (polarity left NULL):');
    unmatchedTypes.forEach(r => console.log(`     ${r.signal_type}: ${r.cnt.toLocaleString()}`));
  }

  console.log(`\nDone. Total polarity backfilled: ${totalPolarity.toLocaleString()}`);
  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
