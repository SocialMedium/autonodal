#!/usr/bin/env node
/**
 * Deep Signal Analysis
 *
 * Cleans dataset to 2019-2026, removes zero-signal clients,
 * then analyses maturity vs speed, signal density, and
 * produces refined calibration metrics.
 *
 * Usage: node scripts/analyze_signal_depth.js
 */

const fs = require('fs');
const path = require('path');

const reportsDir = path.join(__dirname, '..', 'reports');
const calibration = JSON.parse(
  fs.readFileSync(path.join(reportsDir, 'newsapi_signal_calibration.json'), 'utf8')
);

const clients = calibration.client_details;

// ─────────────────────────────────────────────────────
// STEP 1: Clean the dataset
// ─────────────────────────────────────────────────────

console.log('═══════════════════════════════════════════════════════');
console.log('  DEEP SIGNAL ANALYSIS');
console.log('═══════════════════════════════════════════════════════\n');

const cutoffDate = new Date('2018-01-01');

const allEligible = clients.filter(c =>
  !c.skipped && new Date(c.first_invoice_date) >= cutoffDate
);

const withSignals = allEligible.filter(c => c.signal_count > 0);
const withoutSignals = allEligible.filter(c => c.signal_count === 0);

console.log('  DATASET CLEANUP');
console.log('  ─────────────────────────────────────────────');
console.log(`  Total clients (all time):     ${clients.length}`);
console.log(`  After 2018+ filter:           ${allEligible.length}`);
console.log(`  With signals (analysis set):  ${withSignals.length}`);
console.log(`  Without signals (excluded):   ${withoutSignals.length}`);
console.log(`  Clean correlation rate:        ${(withSignals.length / allEligible.length * 100).toFixed(1)}%\n`);

const totalRevAll = allEligible.reduce((s, c) => s + (c.total_revenue || 0), 0);
const totalRevSignal = withSignals.reduce((s, c) => s + (c.total_revenue || 0), 0);
console.log(`  Revenue (2018+ eligible):     $${Math.round(totalRevAll).toLocaleString()}`);
console.log(`  Revenue (signal-preceded):    $${Math.round(totalRevSignal).toLocaleString()} (${(totalRevSignal / totalRevAll * 100).toFixed(1)}%)\n`);

// ─────────────────────────────────────────────────────
// STEP 2: Company maturity segmentation
// ─────────────────────────────────────────────────────
// employee_count_band is mostly null (93% of signal clients),
// so we use two proxies:
//   a) employee_count_band where available
//   b) placement revenue as a maturity proxy (higher fees = bigger company)

console.log('  COMPANY MATURITY SEGMENTATION');
console.log('  ─────────────────────────────────────────────\n');

// 2a: employee_count_band (sparse but precise where available)
function normaliseBand(band) {
  if (!band) return null;
  const s = String(band).toLowerCase();
  if (s.includes('1-10') || s === 'a') return '1-10 (micro)';
  if (s.includes('11-50') || s === 'b') return '11-50 (small)';
  if (s.includes('51-200') || s === 'c') return '51-200 (growth)';
  if (s.includes('201-500') || s === 'd') return '201-500 (scaleup)';
  if (s.includes('501-1000') || s === 'e') return '501-1000 (mid-market)';
  if (s.includes('1001-5000') || s === 'f') return '1001-5000 (large)';
  if (s.includes('5001') || s.includes('5000+') || s === 'g') return '5001+ (enterprise)';
  return null;
}

const sizeSegments = {};
let sizeKnown = 0;

for (const c of withSignals) {
  const bandLabel = normaliseBand(c.employee_count_band);
  if (!bandLabel) continue;
  sizeKnown++;

  if (!sizeSegments[bandLabel]) {
    sizeSegments[bandLabel] = { clients: [], lead_times: [], revenues: [], signal_counts: [], signal_types: {} };
  }
  sizeSegments[bandLabel].clients.push(c);
  if (c.earliest_signal_days_before != null) {
    sizeSegments[bandLabel].lead_times.push(c.earliest_signal_days_before);
  }
  sizeSegments[bandLabel].revenues.push(c.total_revenue || 0);
  sizeSegments[bandLabel].signal_counts.push(c.signal_count);
  for (const type of (c.signal_types || [])) {
    sizeSegments[bandLabel].signal_types[type] = (sizeSegments[bandLabel].signal_types[type] || 0) + 1;
  }
}

console.log(`  Clients with employee_count_band: ${sizeKnown}/${withSignals.length}`);
if (sizeKnown > 0) {
  console.log('');
  console.log('  Size Band              Clients  Median Lead  Avg Lead   Avg Signals  Avg Revenue');
  console.log('  ─────────────────────────────────────────────────────────────────────────────────');

  const sizeOrder = ['1-10 (micro)', '11-50 (small)', '51-200 (growth)', '201-500 (scaleup)', '501-1000 (mid-market)', '1001-5000 (large)', '5001+ (enterprise)'];

  for (const band of sizeOrder) {
    const data = sizeSegments[band];
    if (!data || data.clients.length < 1) continue;

    const leadTimes = data.lead_times.sort((a, b) => a - b);
    const medianLead = leadTimes.length > 0 ? leadTimes[Math.floor(leadTimes.length / 2)] : null;
    const avgLead = leadTimes.length > 0 ? Math.round(leadTimes.reduce((a, b) => a + b, 0) / leadTimes.length) : null;
    const avgSignals = Math.round(data.signal_counts.reduce((a, b) => a + b, 0) / data.signal_counts.length);
    const avgRevenue = Math.round(data.revenues.reduce((a, b) => a + b, 0) / data.revenues.length);

    console.log(`  ${band.padEnd(22)} ${String(data.clients.length).padStart(5)}    ${String(medianLead != null ? medianLead + 'd' : '-').padStart(9)}   ${String(avgLead != null ? avgLead + 'd' : '-').padStart(7)}    ${String(avgSignals).padStart(8)}     $${avgRevenue.toLocaleString().padStart(9)}`);
  }
}

// 2b: Revenue tier as maturity proxy (all clients have this)
console.log('\n\n  REVENUE TIER AS MATURITY PROXY');
console.log('  (Higher placement fees typically = larger/more mature company)\n');

const revTiers = {
  'Under $20K': { min: 0, max: 20000 },
  '$20K-$50K': { min: 20000, max: 50000 },
  '$50K-$100K': { min: 50000, max: 100000 },
  '$100K-$200K': { min: 100000, max: 200000 },
  '$200K-$500K': { min: 200000, max: 500000 },
  '$500K+': { min: 500000, max: Infinity }
};

const revSegments = {};

for (const [label, range] of Object.entries(revTiers)) {
  const inTier = withSignals.filter(c =>
    c.total_revenue >= range.min && c.total_revenue < range.max
  );
  if (inTier.length === 0) continue;

  const leadTimes = inTier
    .filter(c => c.earliest_signal_days_before != null)
    .map(c => c.earliest_signal_days_before)
    .sort((a, b) => a - b);
  const medianLead = leadTimes.length > 0 ? leadTimes[Math.floor(leadTimes.length / 2)] : null;
  const avgLead = leadTimes.length > 0 ? Math.round(leadTimes.reduce((a, b) => a + b, 0) / leadTimes.length) : null;
  const avgSignals = Math.round(inTier.reduce((s, c) => s + c.signal_count, 0) / inTier.length);
  const avgRev = Math.round(inTier.reduce((s, c) => s + (c.total_revenue || 0), 0) / inTier.length);

  revSegments[label] = {
    count: inTier.length,
    median_lead_days: medianLead,
    avg_lead_days: avgLead,
    avg_signals: avgSignals,
    avg_revenue: avgRev,
    p25_lead: leadTimes.length >= 4 ? leadTimes[Math.floor(leadTimes.length * 0.25)] : null,
    p75_lead: leadTimes.length >= 4 ? leadTimes[Math.floor(leadTimes.length * 0.75)] : null
  };
}

console.log('  Revenue Tier       Clients  Median Lead  Avg Lead   Avg Signals  Avg Revenue');
console.log('  ─────────────────────────────────────────────────────────────────────────────');

for (const [label, stats] of Object.entries(revSegments)) {
  console.log(`  ${label.padEnd(20)} ${String(stats.count).padStart(5)}    ${String(stats.median_lead_days != null ? stats.median_lead_days + 'd' : '-').padStart(9)}   ${String(stats.avg_lead_days != null ? stats.avg_lead_days + 'd' : '-').padStart(7)}    ${String(stats.avg_signals).padStart(8)}     $${stats.avg_revenue.toLocaleString().padStart(9)}`);
}

// ─────────────────────────────────────────────────────
// STEP 3: Speed analysis — maturity vs lead time
// ─────────────────────────────────────────────────────

console.log('\n\n  MATURITY vs SPEED TO ACTION');
console.log('  ─────────────────────────────────────────────\n');

// Use revenue tiers for correlation since we have full coverage
const speedPoints = Object.entries(revSegments)
  .filter(([, s]) => s.median_lead_days != null && s.count >= 3)
  .map(([label, s], idx) => ({
    label,
    tier_order: idx + 1,
    median_lead: s.median_lead_days,
    avg_lead: s.avg_lead_days,
    count: s.count,
    avg_revenue: s.avg_revenue
  }));

if (speedPoints.length >= 3) {
  const n = speedPoints.length;
  const sumX = speedPoints.reduce((s, d) => s + d.tier_order, 0);
  const sumY = speedPoints.reduce((s, d) => s + d.median_lead, 0);
  const sumXY = speedPoints.reduce((s, d) => s + d.tier_order * d.median_lead, 0);
  const sumX2 = speedPoints.reduce((s, d) => s + d.tier_order ** 2, 0);
  const sumY2 = speedPoints.reduce((s, d) => s + d.median_lead ** 2, 0);

  const denom = Math.sqrt((n * sumX2 - sumX ** 2) * (n * sumY2 - sumY ** 2));
  const r = denom > 0 ? (n * sumXY - sumX * sumY) / denom : 0;
  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX ** 2);

  console.log(`  Correlation (revenue tier vs lead time):  r = ${r.toFixed(3)}`);
  console.log(`  Slope: ${slope.toFixed(1)} days per revenue tier step`);

  if (r > 0.3) {
    console.log('  -> Higher-revenue clients have LONGER signal-to-mandate lead times');
    console.log(`  -> Each revenue tier step adds ~${Math.round(slope)} days to the mandate window`);
  } else if (r < -0.3) {
    console.log('  -> Higher-revenue clients have SHORTER signal-to-mandate lead times');
  } else {
    console.log('  -> No strong linear relationship between revenue tier and speed');
  }

  console.log('\n  REVENUE TIER -> SPEED LADDER:');
  for (const dp of speedPoints.sort((a, b) => a.median_lead - b.median_lead)) {
    const bar = '█'.repeat(Math.min(60, Math.round(dp.median_lead / 10)));
    console.log(`  ${dp.label.padEnd(20)} ${String(dp.median_lead).padStart(4)}d  ${bar}  (n=${dp.count})`);
  }
}

// ─────────────────────────────────────────────────────
// STEP 4: Signal density analysis
// ─────────────────────────────────────────────────────

console.log('\n\n  SIGNAL DENSITY vs OUTCOMES');
console.log('  ─────────────────────────────────────────────\n');

const densityBuckets = {
  '1-5 signals': withSignals.filter(c => c.signal_count >= 1 && c.signal_count <= 5),
  '6-20 signals': withSignals.filter(c => c.signal_count >= 6 && c.signal_count <= 20),
  '21-50 signals': withSignals.filter(c => c.signal_count >= 21 && c.signal_count <= 50),
  '51-100 signals': withSignals.filter(c => c.signal_count >= 51 && c.signal_count <= 100),
  '100+ signals': withSignals.filter(c => c.signal_count > 100)
};

console.log('  Density Bucket      Clients  Avg Revenue   Median Lead  Avg Signals');
console.log('  ────────────────────────────────────────────────────────────────────');

for (const [bucket, inBucket] of Object.entries(densityBuckets)) {
  if (inBucket.length === 0) continue;

  const avgRev = Math.round(inBucket.reduce((s, c) => s + (c.total_revenue || 0), 0) / inBucket.length);
  const leads = inBucket.filter(c => c.earliest_signal_days_before != null)
    .map(c => c.earliest_signal_days_before).sort((a, b) => a - b);
  const medianLead = leads.length > 0 ? leads[Math.floor(leads.length / 2)] : null;
  const avgSig = Math.round(inBucket.reduce((s, c) => s + c.signal_count, 0) / inBucket.length);

  console.log(`  ${bucket.padEnd(20)} ${String(inBucket.length).padStart(5)}     $${avgRev.toLocaleString().padStart(9)}     ${String(medianLead != null ? medianLead + 'd' : '-').padStart(7)}     ${String(avgSig).padStart(6)}`);
}

// Density vs revenue correlation
const densityRevPoints = withSignals
  .filter(c => c.total_revenue > 0 && c.signal_count > 0)
  .map(c => ({ signals: c.signal_count, revenue: c.total_revenue }));

if (densityRevPoints.length >= 10) {
  const n = densityRevPoints.length;
  const sumX = densityRevPoints.reduce((s, d) => s + d.signals, 0);
  const sumY = densityRevPoints.reduce((s, d) => s + d.revenue, 0);
  const sumXY = densityRevPoints.reduce((s, d) => s + d.signals * d.revenue, 0);
  const sumX2 = densityRevPoints.reduce((s, d) => s + d.signals ** 2, 0);
  const sumY2 = densityRevPoints.reduce((s, d) => s + d.revenue ** 2, 0);

  const denom = Math.sqrt((n * sumX2 - sumX ** 2) * (n * sumY2 - sumY ** 2));
  const r = denom > 0 ? (n * sumXY - sumX * sumY) / denom : 0;
  console.log(`\n  Correlation (signal density vs revenue):  r = ${r.toFixed(3)}`);
  if (r > 0.3) console.log('  -> More signals = higher-value mandates');
  else if (r < -0.3) console.log('  -> More signals = lower-value mandates (surprising)');
  else console.log('  -> No strong linear relationship between signal count and mandate value');
}

// ─────────────────────────────────────────────────────
// STEP 5: Year-over-year analysis
// ─────────────────────────────────────────────────────

console.log('\n\n  YEAR-OVER-YEAR SIGNAL CORRELATION');
console.log('  ─────────────────────────────────────────────\n');

const yearStats = {};

for (const c of allEligible) {
  const year = new Date(c.first_invoice_date).getFullYear();
  if (!yearStats[year]) {
    yearStats[year] = { total: 0, with_signals: 0, total_revenue: 0, signal_revenue: 0, avg_signals: [], lead_times: [] };
  }
  yearStats[year].total++;
  yearStats[year].total_revenue += (c.total_revenue || 0);
  if (c.signal_count > 0) {
    yearStats[year].with_signals++;
    yearStats[year].signal_revenue += (c.total_revenue || 0);
    yearStats[year].avg_signals.push(c.signal_count);
    if (c.earliest_signal_days_before != null) {
      yearStats[year].lead_times.push(c.earliest_signal_days_before);
    }
  }
}

console.log('  Year  Clients  With Signals  Rate     Revenue%    Median Lead  Avg Signals');
console.log('  ──────────────────────────────────────────────────────────────────────────');

for (const [year, data] of Object.entries(yearStats).sort()) {
  const rate = (data.with_signals / data.total * 100).toFixed(0);
  const revPct = data.total_revenue > 0
    ? (data.signal_revenue / data.total_revenue * 100).toFixed(0) : '-';
  const leads = data.lead_times.sort((a, b) => a - b);
  const medianLead = leads.length > 0 ? leads[Math.floor(leads.length / 2)] : null;
  const avgSig = data.avg_signals.length > 0
    ? Math.round(data.avg_signals.reduce((a, b) => a + b, 0) / data.avg_signals.length) : 0;

  console.log(`  ${year}    ${String(data.total).padStart(5)}       ${String(data.with_signals).padStart(5)}    ${rate.padStart(3)}%      ${String(revPct).padStart(4)}%       ${String(medianLead != null ? medianLead + 'd' : '-').padStart(7)}         ${String(avgSig).padStart(4)}`);
}

// ─────────────────────────────────────────────────────
// STEP 6: Signal type effectiveness by sector
// ─────────────────────────────────────────────────────

console.log('\n\n  SIGNAL TYPE EFFECTIVENESS BY SECTOR');
console.log('  ─────────────────────────────────────────────\n');

const sectorData = {};
for (const c of withSignals) {
  const sector = c.sector || 'unknown';
  if (!sectorData[sector]) sectorData[sector] = { clients: 0, signal_types: {} };
  sectorData[sector].clients++;
  for (const type of (c.signal_types || [])) {
    sectorData[sector].signal_types[type] = (sectorData[sector].signal_types[type] || 0) + 1;
  }
}

for (const [sector, data] of Object.entries(sectorData).sort(([, a], [, b]) => b.clients - a.clients)) {
  if (data.clients < 3) continue;

  const topTypes = Object.entries(data.signal_types)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 5);

  console.log(`  ${sector} (n=${data.clients}):`);
  for (const [type, count] of topTypes) {
    const pct = (count / data.clients * 100).toFixed(0);
    const bar = '█'.repeat(Math.min(40, Math.round(count / data.clients * 20)));
    console.log(`    ${type.padEnd(25)} ${pct.padStart(3)}% of clients  ${bar}`);
  }
  console.log('');
}

// ─────────────────────────────────────────────────────
// STEP 7: Recalibrate sleep timers on clean dataset
// ─────────────────────────────────────────────────────

console.log('\n  RECALIBRATED SLEEP TIMERS (2018+, signal clients only)');
console.log('  ─────────────────────────────────────────────\n');

const cleanTimingData = {};

for (const c of withSignals) {
  for (const signal of (c.signals || [])) {
    const type = signal.type;
    const daysBefore = signal.days_before;
    if (daysBefore == null || daysBefore < 0) continue;
    if (!cleanTimingData[type]) cleanTimingData[type] = [];
    cleanTimingData[type].push({
      days_before: daysBefore,
      sector: c.sector,
      revenue: c.total_revenue || 0
    });
  }
}

const cleanSleepTimers = {};

console.log('  Signal Type              n     Median   P25     P75    Avg    Peak Window');
console.log('  ────────────────────────────────────────────────────────────────────────');

for (const [type, points] of Object.entries(cleanTimingData).sort(([, a], [, b]) => b.length - a.length)) {
  if (points.length < 5) continue;

  const days = points.map(p => p.days_before).sort((a, b) => a - b);
  const n = days.length;

  const stats = {
    sample_size: n,
    source: 'newsapi_clean_2019plus',
    median: days[Math.floor(n / 2)],
    p10: days[Math.floor(n * 0.1)],
    p25: days[Math.floor(n * 0.25)],
    p75: days[Math.floor(n * 0.75)],
    p90: days[Math.floor(n * 0.9)],
    avg: Math.round(days.reduce((a, b) => a + b, 0) / n),
    min: days[0],
    max: days[n - 1],
    timer: {
      dormant_until_days: days[Math.floor(n * 0.1)],
      rising_from_days: days[Math.floor(n * 0.25)],
      peak_start_days: days[Math.floor(n * 0.4)],
      peak_end_days: days[Math.floor(n * 0.7)],
      declining_until_days: days[Math.floor(n * 0.9)],
      dormant_after_days: days[n - 1]
    }
  };

  cleanSleepTimers[type] = stats;

  console.log(`  ${type.padEnd(25)} ${String(n).padStart(5)}   ${String(stats.median).padStart(5)}d  ${String(stats.p25).padStart(5)}d  ${String(stats.p75).padStart(5)}d  ${String(stats.avg).padStart(5)}d   ${stats.timer.peak_start_days}-${stats.timer.peak_end_days}d`);
}

// ─────────────────────────────────────────────────────
// STEP 8: Revenue quartile analysis
// ─────────────────────────────────────────────────────

console.log('\n\n  REVENUE QUARTILE ANALYSIS');
console.log('  (Are higher-value mandates more signal-dense?)\n');

const sortedByRevenue = [...withSignals]
  .filter(c => c.total_revenue > 0)
  .sort((a, b) => a.total_revenue - b.total_revenue);

if (sortedByRevenue.length >= 8) {
  const qLen = Math.floor(sortedByRevenue.length / 4);
  const q1 = sortedByRevenue.slice(0, qLen);
  const q2 = sortedByRevenue.slice(qLen, qLen * 2);
  const q3 = sortedByRevenue.slice(qLen * 2, qLen * 3);
  const q4 = sortedByRevenue.slice(qLen * 3);

  const quartiles = { 'Q1 (lowest)': q1, 'Q2': q2, 'Q3': q3, 'Q4 (highest)': q4 };

  console.log('  Quartile       Clients  Avg Revenue    Avg Signals  Median Lead  Top Signal Types');
  console.log('  ──────────────────────────────────────────────────────────────────────────────────');

  for (const [label, cq] of Object.entries(quartiles)) {
    const avgRev = Math.round(cq.reduce((s, c) => s + c.total_revenue, 0) / cq.length);
    const avgSig = Math.round(cq.reduce((s, c) => s + c.signal_count, 0) / cq.length);
    const leads = cq.filter(c => c.earliest_signal_days_before != null)
      .map(c => c.earliest_signal_days_before).sort((a, b) => a - b);
    const medianLead = leads.length > 0 ? leads[Math.floor(leads.length / 2)] : null;

    const typeCounts = {};
    for (const c of cq) {
      for (const t of (c.signal_types || [])) typeCounts[t] = (typeCounts[t] || 0) + 1;
    }
    const topTypes = Object.entries(typeCounts).sort(([, a], [, b]) => b - a).slice(0, 3).map(([t]) => t).join(', ');

    console.log(`  ${label.padEnd(15)} ${String(cq.length).padStart(5)}     $${avgRev.toLocaleString().padStart(9)}       ${String(avgSig).padStart(6)}       ${String(medianLead != null ? medianLead + 'd' : '-').padStart(5)}   ${topTypes}`);
  }
}

// ─────────────────────────────────────────────────────
// STEP 9: Compound pattern effectiveness
// ─────────────────────────────────────────────────────

console.log('\n\n  COMPOUND PATTERN EFFECTIVENESS');
console.log('  (Multi-signal clients vs single-signal clients)\n');

const singleType = withSignals.filter(c => (c.signal_types || []).length === 1);
const multiType = withSignals.filter(c => (c.signal_types || []).length > 1);

if (singleType.length > 0 && multiType.length > 0) {
  const singleAvgRev = Math.round(singleType.reduce((s, c) => s + (c.total_revenue || 0), 0) / singleType.length);
  const multiAvgRev = Math.round(multiType.reduce((s, c) => s + (c.total_revenue || 0), 0) / multiType.length);
  const singleAvgSig = Math.round(singleType.reduce((s, c) => s + c.signal_count, 0) / singleType.length);
  const multiAvgSig = Math.round(multiType.reduce((s, c) => s + c.signal_count, 0) / multiType.length);

  const singleLeads = singleType.filter(c => c.earliest_signal_days_before != null).map(c => c.earliest_signal_days_before).sort((a, b) => a - b);
  const multiLeads = multiType.filter(c => c.earliest_signal_days_before != null).map(c => c.earliest_signal_days_before).sort((a, b) => a - b);
  const singleMedian = singleLeads.length > 0 ? singleLeads[Math.floor(singleLeads.length / 2)] : null;
  const multiMedian = multiLeads.length > 0 ? multiLeads[Math.floor(multiLeads.length / 2)] : null;

  console.log('  Category         Clients  Avg Revenue   Avg Signals  Median Lead');
  console.log('  ─────────────────────────────────────────────────────────────────');
  console.log(`  Single type       ${String(singleType.length).padStart(5)}     $${singleAvgRev.toLocaleString().padStart(9)}       ${String(singleAvgSig).padStart(6)}       ${String(singleMedian != null ? singleMedian + 'd' : '-').padStart(5)}`);
  console.log(`  Multi type        ${String(multiType.length).padStart(5)}     $${multiAvgRev.toLocaleString().padStart(9)}       ${String(multiAvgSig).padStart(6)}       ${String(multiMedian != null ? multiMedian + 'd' : '-').padStart(5)}`);

  if (multiAvgRev > singleAvgRev) {
    console.log(`\n  -> Compound signals precede ${((multiAvgRev / singleAvgRev - 1) * 100).toFixed(0)}% higher-value mandates`);
  }
}

// ─────────────────────────────────────────────────────
// STEP 10: Save outputs
// ─────────────────────────────────────────────────────

const output = {
  metadata: {
    run_date: new Date().toISOString(),
    filter: '2018+ clients with signals only',
    original_clients: clients.length,
    filtered_clients: allEligible.length,
    analysis_set: withSignals.length,
    excluded: withoutSignals.length,
    clean_correlation_rate: (withSignals.length / allEligible.length * 100).toFixed(1) + '%',
    clean_revenue_rate: (totalRevSignal / totalRevAll * 100).toFixed(1) + '%'
  },
  revenue_tier_segments: revSegments,
  size_segments: Object.fromEntries(
    Object.entries(sizeSegments).map(([k, v]) => [k, {
      count: v.clients.length,
      median_lead: v.lead_times.length > 0 ? v.lead_times.sort((a, b) => a - b)[Math.floor(v.lead_times.length / 2)] : null,
      avg_signals: Math.round(v.signal_counts.reduce((a, b) => a + b, 0) / v.signal_counts.length),
      top_signal_types: Object.entries(v.signal_types).sort(([, a], [, b]) => b - a).slice(0, 5)
    }])
  ),
  maturity_speed: speedPoints,
  density_analysis: Object.fromEntries(
    Object.entries(densityBuckets).map(([k, v]) => [k, {
      count: v.length,
      avg_revenue: v.length > 0 ? Math.round(v.reduce((s, c) => s + (c.total_revenue || 0), 0) / v.length) : 0,
      avg_signals: v.length > 0 ? Math.round(v.reduce((s, c) => s + c.signal_count, 0) / v.length) : 0
    }])
  ),
  year_over_year: yearStats,
  clean_sleep_timers: cleanSleepTimers,
  compound_effectiveness: {
    single_type: { count: singleType.length, avg_revenue: singleType.length > 0 ? Math.round(singleType.reduce((s, c) => s + (c.total_revenue || 0), 0) / singleType.length) : 0 },
    multi_type: { count: multiType.length, avg_revenue: multiType.length > 0 ? Math.round(multiType.reduce((s, c) => s + (c.total_revenue || 0), 0) / multiType.length) : 0 }
  }
};

fs.writeFileSync(path.join(reportsDir, 'deep_signal_analysis.json'), JSON.stringify(output, null, 2));
fs.writeFileSync(path.join(reportsDir, 'clean_sleep_timers.json'), JSON.stringify(cleanSleepTimers, null, 2));

console.log('\n\n═══════════════════════════════════════════════════════');
console.log('  KEY FINDINGS');
console.log('═══════════════════════════════════════════════════════\n');

console.log('  1. CLEAN CORRELATION RATE (2018+):');
console.log(`     ${(withSignals.length / allEligible.length * 100).toFixed(1)}% of clients had detectable signals`);
console.log(`     ${(totalRevSignal / totalRevAll * 100).toFixed(1)}% of revenue was signal-preceded\n`);

if (speedPoints.length >= 2) {
  const fastest = speedPoints.sort((a, b) => a.median_lead - b.median_lead)[0];
  const slowest = speedPoints[speedPoints.length - 1];
  console.log('  2. MATURITY vs SPEED:');
  console.log(`     Fastest tier: ${fastest.label} at ${fastest.median_lead}d median lead`);
  console.log(`     Slowest tier: ${slowest.label} at ${slowest.median_lead}d median lead`);
  console.log(`     Difference: ${slowest.median_lead - fastest.median_lead} days\n`);
}

console.log('  3. Outputs saved to:');
console.log('     reports/deep_signal_analysis.json');
console.log('     reports/clean_sleep_timers.json\n');

console.log('═══════════════════════════════════════════════════════\n');
