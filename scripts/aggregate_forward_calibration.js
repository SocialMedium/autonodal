#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Monthly aggregation — turn prospective outcomes into a calibration file.
// Runs on the 1st of each month for the previous month.
// Usage: node scripts/aggregate_forward_calibration.js [--month YYYY-MM]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

function targetMonth() {
  const arg = process.argv.indexOf('--month');
  if (arg !== -1) return process.argv[arg + 1];
  // Default: previous month
  const d = new Date();
  d.setDate(1);
  d.setMonth(d.getMonth() - 1);
  return d.toISOString().slice(0, 7);
}

function percentile(sortedArr, p) {
  if (!sortedArr.length) return null;
  const idx = (p / 100) * (sortedArr.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedArr[lo];
  return Math.round(sortedArr[lo] + (sortedArr[hi] - sortedArr[lo]) * (idx - lo));
}

async function run() {
  const month = targetMonth();
  const [year, mo] = month.split('-').map(Number);
  const start = new Date(Date.UTC(year, mo - 1, 1));
  const end = new Date(Date.UTC(year, mo, 1));

  console.log(`Aggregating forward calibration for ${month}...`);

  // Pull all outcomes resolved in the target month
  const { rows: outcomes } = await pool.query(`
    SELECT so.id, so.tenant_id, so.signal_id, so.outcome,
           so.claimed_at, so.converted_at, so.lead_time_days,
           so.revenue_local, so.revenue_currency, so.resolved_at,
           se.signal_type, se.polarity,
           se.first_detected_at, se.detected_at
    FROM signal_outcomes so
    LEFT JOIN signal_events se ON se.id = so.signal_id
    WHERE so.resolved_at >= $1 AND so.resolved_at < $2
    ORDER BY so.resolved_at
  `, [start, end]);

  console.log(`  ${outcomes.length} outcomes found`);

  // Aggregate per signal type
  const bySignalType = {};
  for (const o of outcomes) {
    const t = o.signal_type;
    if (!t) continue;
    if (!bySignalType[t]) {
      bySignalType[t] = {
        signal_type: t,
        polarity: o.polarity,
        outcomes: { converted_mandate: 0, contact_only: 0, no_response: 0, wrong_moment: 0, window_expired: 0, declined: 0 },
        total: 0,
        converted_lead_times: [],
        total_revenue: 0,
      };
    }
    const bucket = bySignalType[t];
    bucket.total++;
    bucket.outcomes[o.outcome] = (bucket.outcomes[o.outcome] || 0) + 1;
    if (o.outcome === 'converted_mandate') {
      if (o.lead_time_days) bucket.converted_lead_times.push(o.lead_time_days);
      if (o.revenue_local) bucket.total_revenue += parseFloat(o.revenue_local);
    }
  }

  // Compute per-type stats
  const summary = {};
  for (const [type, b] of Object.entries(bySignalType)) {
    b.converted_lead_times.sort((a, b) => a - b);
    summary[type] = {
      polarity: b.polarity,
      total_outcomes: b.total,
      outcomes: b.outcomes,
      conversion_rate: b.total > 0 ? parseFloat((b.outcomes.converted_mandate / b.total).toFixed(3)) : 0,
      converted_count: b.outcomes.converted_mandate,
      lead_time_percentiles: b.converted_lead_times.length > 0 ? {
        p25: percentile(b.converted_lead_times, 25),
        p50: percentile(b.converted_lead_times, 50),
        p75: percentile(b.converted_lead_times, 75),
        p90: percentile(b.converted_lead_times, 90),
        n: b.converted_lead_times.length,
      } : null,
      total_revenue: parseFloat(b.total_revenue.toFixed(2)),
    };
  }

  const totals = {
    total_outcomes: outcomes.length,
    total_converted: outcomes.filter(o => o.outcome === 'converted_mandate').length,
    total_revenue: outcomes.reduce((s, o) => s + (parseFloat(o.revenue_local) || 0), 0),
  };

  const output = {
    metadata: {
      month,
      window: { start: start.toISOString(), end: end.toISOString() },
      computed_at: new Date().toISOString(),
    },
    totals,
    per_signal_type: summary,
  };

  const outDir = path.join(__dirname, '..', 'reports');
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `forward_calibration_${month.replace('-', '_')}.json`);
  fs.writeFileSync(outPath, JSON.stringify(output, null, 2));

  console.log(`\n  Total outcomes:   ${totals.total_outcomes}`);
  console.log(`  Converted:        ${totals.total_converted}`);
  console.log(`  Overall rate:     ${totals.total_outcomes > 0 ? ((totals.total_converted / totals.total_outcomes) * 100).toFixed(1) + '%' : 'n/a'}`);
  console.log(`  Revenue:          ${totals.total_revenue.toFixed(2)}`);
  console.log(`  Output:           ${outPath}`);

  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
