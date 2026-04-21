#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Compute per-signal-type lead-time percentiles from raw calibration data
// Source: reports/newsapi_signal_calibration.json > client_details[].signals[].days_before
// Output: reports/signal_timing_percentiles.json
// ═══════════════════════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');

const SOURCE = path.join(__dirname, '..', 'reports', 'newsapi_signal_calibration.json');
const OUTPUT = path.join(__dirname, '..', 'reports', 'signal_timing_percentiles.json');

function percentile(sortedArr, p) {
  if (!sortedArr.length) return null;
  const idx = (p / 100) * (sortedArr.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedArr[lo];
  return Math.round(sortedArr[lo] + (sortedArr[hi] - sortedArr[lo]) * (idx - lo));
}

function run() {
  console.log(`Reading ${SOURCE}...`);
  const raw = JSON.parse(fs.readFileSync(SOURCE, 'utf8'));

  if (!Array.isArray(raw.client_details)) {
    console.error('ERROR: expected raw.client_details to be an array');
    process.exit(1);
  }

  // Bucket observations by signal type
  const observations = {};  // type → [days_before]
  let totalSignals = 0, totalClients = 0;

  for (const client of raw.client_details) {
    if (client.skipped) continue;
    totalClients++;
    for (const sig of (client.signals || [])) {
      if (typeof sig.days_before !== 'number') continue;
      if (sig.days_before < 0 || sig.days_before > 1095) continue; // clamp to 3yr
      if (!observations[sig.type]) observations[sig.type] = [];
      observations[sig.type].push(sig.days_before);
      totalSignals++;
    }
  }

  console.log(`  Parsed ${totalSignals.toLocaleString()} signals across ${totalClients} clients`);

  // Compute percentiles per type
  const percentiles = {};
  for (const [type, arr] of Object.entries(observations)) {
    arr.sort((a, b) => a - b);
    percentiles[type] = {
      p10: percentile(arr, 10),
      p25: percentile(arr, 25),
      p50: percentile(arr, 50),
      p75: percentile(arr, 75),
      p90: percentile(arr, 90),
      min: arr[0],
      max: arr[arr.length - 1],
      mean: Math.round(arr.reduce((s, v) => s + v, 0) / arr.length),
      n: arr.length,
    };
  }

  const output = {
    metadata: {
      source: path.basename(SOURCE),
      computed_at: new Date().toISOString(),
      total_observations: totalSignals,
      total_clients: totalClients,
      method: 'percentile from raw days_before observations',
    },
    percentiles,
  };

  fs.writeFileSync(OUTPUT, JSON.stringify(output, null, 2));
  console.log(`\nOutput: ${OUTPUT}`);

  // Summary table
  console.log('\n  Signal type          │   n   │ P25 │ P50 │ P75 │ P90');
  console.log('  ─────────────────────┼───────┼─────┼─────┼─────┼─────');
  for (const [type, p] of Object.entries(percentiles)) {
    console.log(`  ${type.padEnd(20)} │ ${String(p.n).padStart(5)} │ ${String(p.p25).padStart(3)} │ ${String(p.p50).padStart(3)} │ ${String(p.p75).padStart(3)} │ ${String(p.p90).padStart(3)}`);
  }
}

run();
