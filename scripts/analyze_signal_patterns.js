#!/usr/bin/env node
/**
 * Signal Sequence & Flow Pattern Analysis
 * Reads from reports/newsapi_signal_calibration.json — no API calls.
 * Outputs to reports/signal_patterns.json + console.
 */

const fs = require('fs');
const path = require('path');

const INPUT = path.join(__dirname, '..', 'reports', 'newsapi_signal_calibration.json');
const OUTPUT = path.join(__dirname, '..', 'reports', 'signal_patterns.json');

function median(arr) {
  if (!arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function avg(arr) { return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0; }
function pct(n, d) { return d > 0 ? Math.round((n / d) * 100) : 0; }
function fmt$(n) { return n >= 1000 ? '$' + Math.round(n / 1000) + 'K' : '$' + Math.round(n); }
function pad(s, n) { return String(s).substring(0, n).padEnd(n); }
function padr(s, n) { return String(s).substring(0, n).padStart(n); }

// ═══════════════════════════════════════════════════════════════════════════
// LOAD DATA
// ═══════════════════════════════════════════════════════════════════════════

const raw = JSON.parse(fs.readFileSync(INPUT, 'utf8'));
const allClients = raw.client_details || [];
const clients = allClients.filter(c => !c.skipped && c.signal_count > 0);
const multiSignal = clients.filter(c => c.signal_count >= 3);
const multiType = clients.filter(c => (c.signal_types || []).length >= 2);

console.log('═══════════════════════════════════════════════════════════');
console.log('  SIGNAL SEQUENCE & FLOW PATTERN ANALYSIS');
console.log('  From ' + clients.reduce((a, c) => a + c.signal_count, 0).toLocaleString() + ' signals across ' + clients.length + ' clients');
console.log('  Multi-signal (3+): ' + multiSignal.length + ' | Multi-type (2+): ' + multiType.length);
console.log('═══════════════════════════════════════════════════════════\n');

// ═══════════════════════════════════════════════════════════════════════════
// 1. SIGNAL SEQUENCE ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════

const sequences = {};
const openingCounts = {};
const closingCounts = {};

for (const client of multiType) {
  const sorted = [...client.signals].sort((a, b) => new Date(b.date) - new Date(a.date)); // earliest last (highest days_before first)
  // Actually sort by days_before descending (earliest signal = highest days_before)
  const bySeniority = [...client.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));

  const seen = new Set();
  const sequence = [];
  for (const signal of bySeniority) {
    if (!seen.has(signal.type)) {
      seen.add(signal.type);
      sequence.push(signal.type);
    }
  }

  if (sequence.length < 2) continue;

  const seqKey = sequence.join(' → ');
  if (!sequences[seqKey]) sequences[seqKey] = [];
  sequences[seqKey].push({
    client: client.client_name,
    revenue: client.total_revenue || 0,
    span_days: (client.earliest_signal_days_before || 0) - (client.latest_signal_days_before || 0),
    signal_count: client.signal_count,
  });

  // Opening signal (first detected, furthest from mandate)
  openingCounts[sequence[0]] = (openingCounts[sequence[0]] || 0) + 1;
  // Closing signal (last unique type before mandate)
  closingCounts[sequence[sequence.length - 1]] = (closingCounts[sequence[sequence.length - 1]] || 0) + 1;
}

const rankedSeqs = Object.entries(sequences)
  .map(([seq, clients]) => ({
    sequence: seq,
    frequency: clients.length,
    avg_revenue: avg(clients.map(c => c.revenue)),
    avg_span: avg(clients.map(c => c.span_days)),
    avg_signals: avg(clients.map(c => c.signal_count)),
  }))
  .sort((a, b) => b.frequency - a.frequency);

console.log('  TOP 15 SIGNAL SEQUENCES:');
console.log('  ' + '─'.repeat(90));
console.log('  ' + pad('Sequence', 55) + padr('Freq', 6) + padr('Avg Rev', 10) + padr('Avg Span', 10));
console.log('  ' + '─'.repeat(90));
rankedSeqs.slice(0, 15).forEach(s => {
  console.log('  ' + pad(s.sequence, 55) + padr(s.frequency, 6) + padr(fmt$(s.avg_revenue), 10) + padr(Math.round(s.avg_span) + 'd', 10));
});

const totalOpening = Object.values(openingCounts).reduce((a, b) => a + b, 0);
const totalClosing = Object.values(closingCounts).reduce((a, b) => a + b, 0);
console.log('\n  MOST COMMON OPENING SIGNAL (first detected):');
Object.entries(openingCounts).sort((a, b) => b[1] - a[1]).slice(0, 5).forEach(([t, c]) => {
  console.log('    ' + pad(t, 25) + pct(c, totalOpening) + '% (' + c + ')');
});
console.log('\n  MOST COMMON CLOSING SIGNAL (nearest to mandate):');
Object.entries(closingCounts).sort((a, b) => b[1] - a[1]).slice(0, 5).forEach(([t, c]) => {
  console.log('    ' + pad(t, 25) + pct(c, totalClosing) + '% (' + c + ')');
});

// ═══════════════════════════════════════════════════════════════════════════
// 2. VELOCITY ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════

const velocityBuckets = { ACCELERATING: [], STEADY: [], DECELERATING: [] };

for (const client of multiSignal) {
  const sorted = [...client.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));
  if (sorted.length < 4) continue;

  const mid = Math.floor(sorted.length / 2);
  const early = sorted.slice(0, mid);
  const late = sorted.slice(mid);

  const earlySpan = Math.max(1, (early[0].days_before || 0) - (early[early.length - 1].days_before || 0));
  const lateSpan = Math.max(1, (late[0].days_before || 0) - (late[late.length - 1].days_before || 0));

  const earlyDensity = early.length / earlySpan;
  const lateDensity = late.length / lateSpan;
  const ratio = lateDensity / Math.max(0.001, earlyDensity);

  const pattern = ratio > 1.5 ? 'ACCELERATING' : ratio < 0.67 ? 'DECELERATING' : 'STEADY';
  velocityBuckets[pattern].push({
    client: client.client_name,
    revenue: client.total_revenue || 0,
    lead_time: client.earliest_signal_days_before || 0,
    ratio,
  });
}

console.log('\n  ' + '─'.repeat(60));
console.log('  VELOCITY PATTERNS:');
console.log('  ' + '─'.repeat(60));
console.log('  ' + pad('Pattern', 18) + padr('Clients', 10) + padr('Avg Revenue', 12) + padr('Avg Lead', 10));
console.log('  ' + '─'.repeat(60));
for (const [pattern, clients] of Object.entries(velocityBuckets)) {
  console.log('  ' + pad(pattern, 18) + padr(clients.length, 10) + padr(fmt$(avg(clients.map(c => c.revenue))), 12) + padr(Math.round(avg(clients.map(c => c.lead_time))) + 'd', 10));
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. DENSITY CONCENTRATION
// ═══════════════════════════════════════════════════════════════════════════

const densityBuckets = { BURST: [], MIXED: [], EVENLY_SPREAD: [] };

for (const client of multiSignal) {
  const sorted = [...client.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));
  if (sorted.length < 3) continue;

  const totalSpan = (sorted[0].days_before || 0) - (sorted[sorted.length - 1].days_before || 0);
  if (totalSpan < 1) { densityBuckets.BURST.push({ client: client.client_name, revenue: client.total_revenue || 0, span: totalSpan }); continue; }

  const gaps = [];
  for (let i = 1; i < sorted.length; i++) {
    gaps.push(Math.abs((sorted[i - 1].days_before || 0) - (sorted[i].days_before || 0)));
  }

  const avgGap = avg(gaps);
  if (avgGap === 0) { densityBuckets.BURST.push({ client: client.client_name, revenue: client.total_revenue || 0, span: totalSpan }); continue; }

  const stdDev = Math.sqrt(gaps.reduce((sum, g) => sum + Math.pow(g - avgGap, 2), 0) / gaps.length);
  const cv = stdDev / avgGap;

  const pattern = cv > 1.5 ? 'BURST' : cv < 0.5 ? 'EVENLY_SPREAD' : 'MIXED';
  densityBuckets[pattern].push({ client: client.client_name, revenue: client.total_revenue || 0, span: totalSpan });
}

console.log('\n  ' + '─'.repeat(60));
console.log('  DENSITY PATTERNS:');
console.log('  ' + '─'.repeat(60));
console.log('  ' + pad('Pattern', 18) + padr('Clients', 10) + padr('Avg Revenue', 12) + padr('Avg Span', 10));
console.log('  ' + '─'.repeat(60));
for (const [pattern, clients] of Object.entries(densityBuckets)) {
  console.log('  ' + pad(pattern, 18) + padr(clients.length, 10) + padr(fmt$(avg(clients.map(c => c.revenue))), 12) + padr(Math.round(avg(clients.map(c => c.span))) + 'd', 10));
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. SIGNAL RUNWAY LENGTH
// ═══════════════════════════════════════════════════════════════════════════

const runwayBuckets = {
  'Under 7 days': [], '1-4 weeks': [], '1-3 months': [],
  '3-6 months': [], '6-12 months': [], '12+ months': [],
};

for (const client of clients) {
  const span = (client.earliest_signal_days_before || 0) - (client.latest_signal_days_before || 0);
  const bucket = span < 7 ? 'Under 7 days' : span < 28 ? '1-4 weeks' : span < 90 ? '1-3 months'
    : span < 180 ? '3-6 months' : span < 365 ? '6-12 months' : '12+ months';
  runwayBuckets[bucket].push({ revenue: client.total_revenue || 0, signals: client.signal_count, span });
}

console.log('\n  ' + '─'.repeat(60));
console.log('  SIGNAL RUNWAY LENGTH:');
console.log('  ' + '─'.repeat(60));
console.log('  ' + pad('Runway', 18) + padr('Clients', 10) + padr('Avg Revenue', 12) + padr('Avg Sigs', 10));
console.log('  ' + '─'.repeat(60));
for (const [bucket, items] of Object.entries(runwayBuckets)) {
  if (items.length === 0) continue;
  console.log('  ' + pad(bucket, 18) + padr(items.length, 10) + padr(fmt$(avg(items.map(i => i.revenue))), 12) + padr(Math.round(avg(items.map(i => i.signals))), 10));
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. TRANSITION MATRIX (Markov Chain)
// ═══════════════════════════════════════════════════════════════════════════

const transitions = {};
const typeTotals = {};

for (const client of multiSignal) {
  const sorted = [...client.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));

  for (let i = 0; i < sorted.length - 1; i++) {
    const from = sorted[i].type;
    const to = sorted[i + 1].type;
    if (from === to) continue;

    const gap = Math.abs((sorted[i].days_before || 0) - (sorted[i + 1].days_before || 0));
    if (gap > 90) continue;

    typeTotals[from] = (typeTotals[from] || 0) + 1;
    if (!transitions[from]) transitions[from] = {};
    if (!transitions[from][to]) transitions[from][to] = { count: 0, gaps: [] };
    transitions[from][to].count++;
    transitions[from][to].gaps.push(gap);
  }
}

const allTypes = [...new Set([...Object.keys(transitions), ...Object.keys(typeTotals)])].sort();
const shortNames = {
  capital_raising: 'cap', strategic_hiring: 'hire', leadership_change: 'lead',
  geographic_expansion: 'expand', ma_activity: 'ma', product_launch: 'prod',
  partnership: 'part', layoffs: 'layoff', restructuring: 'restr',
};

console.log('\n  ' + '─'.repeat(80));
console.log('  SIGNAL TYPE TRANSITION MATRIX (P(B|A) within 90 days):');
console.log('  ' + '─'.repeat(80));

// Header
let header = '  ' + pad('', 22);
allTypes.forEach(t => { header += padr(shortNames[t] || t.substring(0, 6), 8); });
console.log(header);

for (const from of allTypes) {
  let row = '  ' + pad(shortNames[from] || from, 22);
  const total = typeTotals[from] || 1;
  for (const to of allTypes) {
    if (from === to) { row += padr('-', 8); continue; }
    const cnt = transitions[from]?.[to]?.count || 0;
    const prob = cnt / total;
    row += padr(prob > 0 ? prob.toFixed(2) : '.', 8);
  }
  console.log(row);
}

// Key transitions
console.log('\n  KEY TRANSITION INSIGHTS:');
const topTransitions = [];
for (const from of Object.keys(transitions)) {
  for (const [to, data] of Object.entries(transitions[from])) {
    const prob = data.count / (typeTotals[from] || 1);
    topTransitions.push({ from, to, prob, count: data.count, medianGap: median(data.gaps) });
  }
}
topTransitions.sort((a, b) => b.prob - a.prob);
topTransitions.slice(0, 8).forEach(t => {
  console.log('    After ' + pad(t.from, 22) + '→ ' + pct(t.prob * 100, 100) + '% chance of ' + t.to + ' within ' + Math.round(t.medianGap) + 'd (n=' + t.count + ')');
});

// ═══════════════════════════════════════════════════════════════════════════
// 6. SILENCE & REBOOT PATTERNS
// ═══════════════════════════════════════════════════════════════════════════

const reboots = [];

for (const client of multiSignal) {
  const sorted = [...client.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));
  if (sorted.length < 4) continue;

  let maxGap = 0;
  let maxGapIdx = -1;

  for (let i = 1; i < sorted.length; i++) {
    const gap = Math.abs((sorted[i - 1].days_before || 0) - (sorted[i].days_before || 0));
    if (gap > maxGap) { maxGap = gap; maxGapIdx = i; }
  }

  if (maxGap > 60 && maxGapIdx > 0) {
    reboots.push({
      client: client.client_name,
      revenue: client.total_revenue || 0,
      gap_days: maxGap,
      signals_before: maxGapIdx,
      signals_after: sorted.length - maxGapIdx,
      type_before_gap: sorted[maxGapIdx - 1].type,
      type_after_gap: sorted[maxGapIdx].type,
      reboot_to_mandate: sorted[maxGapIdx].days_before || 0,
      first_signal_to_mandate: sorted[0].days_before || 0,
    });
  }
}

const rebootTriggers = {};
reboots.forEach(r => { rebootTriggers[r.type_after_gap] = (rebootTriggers[r.type_after_gap] || 0) + 1; });

console.log('\n  ' + '─'.repeat(60));
console.log('  SILENCE & REBOOT PATTERNS:');
console.log('  ' + '─'.repeat(60));
console.log('  Clients with silence gaps (>60 days): ' + reboots.length);
console.log('  Median silence duration:              ' + Math.round(median(reboots.map(r => r.gap_days))) + 'd');
console.log('  Median reboot-to-mandate:             ' + Math.round(median(reboots.map(r => r.reboot_to_mandate))) + 'd');
console.log('  Median first-signal-to-mandate:       ' + Math.round(median(reboots.map(r => r.first_signal_to_mandate))) + 'd');
const rebootMedian = median(reboots.map(r => r.reboot_to_mandate));
const firstMedian = median(reboots.map(r => r.first_signal_to_mandate));
console.log('  → Reboots convert ' + (rebootMedian < firstMedian ? 'FASTER' : 'SLOWER') + ' by ' + Math.abs(Math.round(firstMedian - rebootMedian)) + ' days');

console.log('  Most common reboot trigger:');
Object.entries(rebootTriggers).sort((a, b) => b[1] - a[1]).slice(0, 5).forEach(([t, c]) => {
  console.log('    ' + pad(t, 25) + c + ' (' + pct(c, reboots.length) + '%)');
});

// ═══════════════════════════════════════════════════════════════════════════
// 7. REVENUE TIER FINGERPRINTS
// ═══════════════════════════════════════════════════════════════════════════

const tierDefs = [
  ['Under $20K', 0, 20000],
  ['$20K-$50K', 20000, 50000],
  ['$50K-$100K', 50000, 100000],
  ['$100K-$200K', 100000, 200000],
  ['$200K+', 200000, Infinity],
];

console.log('\n  ' + '─'.repeat(60));
console.log('  REVENUE TIER FINGERPRINTS:');
console.log('  ' + '─'.repeat(60));

const tierData = {};
for (const [label, min, max] of tierDefs) {
  const tierClients = clients.filter(c => c.total_revenue >= min && c.total_revenue < max);
  if (tierClients.length < 3) continue;

  // Compute fingerprint
  const velocities = {};
  for (const vc of Object.entries(velocityBuckets)) {
    const inTier = vc[1].filter(v => tierClients.some(c => c.client_name === v.client));
    velocities[vc[0]] = inTier.length;
  }
  const topVelocity = Object.entries(velocities).sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown';

  const densities = {};
  for (const dc of Object.entries(densityBuckets)) {
    const inTier = dc[1].filter(d => tierClients.some(c => c.client_name === d.client));
    densities[dc[0]] = inTier.length;
  }
  const topDensity = Object.entries(densities).sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown';

  const avgRunway = avg(tierClients.map(c => (c.earliest_signal_days_before || 0) - (c.latest_signal_days_before || 0)));
  const avgSigs = avg(tierClients.map(c => c.signal_count));

  // Opening signal
  const openers = {};
  for (const c of tierClients) {
    const sorted = [...c.signals].sort((a, b) => (b.days_before || 0) - (a.days_before || 0));
    if (sorted[0]) openers[sorted[0].type] = (openers[sorted[0].type] || 0) + 1;
  }
  const topOpener = Object.entries(openers).sort((a, b) => b[1] - a[1])[0];

  const rebootRate = reboots.filter(r => tierClients.some(c => c.client_name === r.client)).length;

  tierData[label] = { clients: tierClients.length, topVelocity, topDensity, avgRunway, avgSigs, topOpener, rebootRate };

  console.log('\n  ' + label + ' (' + tierClients.length + ' clients):');
  console.log('    Velocity:       mostly ' + topVelocity);
  console.log('    Density:        mostly ' + topDensity);
  console.log('    Avg runway:     ' + Math.round(avgRunway) + 'd');
  console.log('    Avg signals:    ' + Math.round(avgSigs));
  console.log('    Opening signal: ' + (topOpener ? topOpener[0] + ' (' + pct(topOpener[1], tierClients.length) + '%)' : 'varied'));
  console.log('    Reboot rate:    ' + pct(rebootRate, tierClients.length) + '%');
}

// ═══════════════════════════════════════════════════════════════════════════
// SAVE TO FILE
// ═══════════════════════════════════════════════════════════════════════════

const output = {
  metadata: {
    generated_at: new Date().toISOString(),
    total_clients: clients.length,
    total_signals: clients.reduce((a, c) => a + c.signal_count, 0),
    multi_signal_clients: multiSignal.length,
    multi_type_clients: multiType.length,
  },
  top_sequences: rankedSeqs.slice(0, 30),
  opening_signals: openingCounts,
  closing_signals: closingCounts,
  velocity: Object.fromEntries(Object.entries(velocityBuckets).map(([k, v]) => [k, {
    count: v.length, avg_revenue: avg(v.map(c => c.revenue)), avg_lead_time: avg(v.map(c => c.lead_time)),
  }])),
  density: Object.fromEntries(Object.entries(densityBuckets).map(([k, v]) => [k, {
    count: v.length, avg_revenue: avg(v.map(c => c.revenue)), avg_span: avg(v.map(c => c.span)),
  }])),
  runway: Object.fromEntries(Object.entries(runwayBuckets).map(([k, v]) => [k, {
    count: v.length, avg_revenue: avg(v.map(i => i.revenue)), avg_signals: avg(v.map(i => i.signals)),
  }])),
  transition_matrix: transitions,
  top_transitions: topTransitions.slice(0, 20),
  reboot_patterns: {
    count: reboots.length,
    median_gap_days: median(reboots.map(r => r.gap_days)),
    median_reboot_to_mandate: median(reboots.map(r => r.reboot_to_mandate)),
    triggers: rebootTriggers,
    reboots: reboots.slice(0, 30),
  },
  tier_fingerprints: tierData,
};

fs.writeFileSync(OUTPUT, JSON.stringify(output, null, 2));
console.log('\n═══════════════════════════════════════════════════════════');
console.log('  Output saved to: reports/signal_patterns.json');
console.log('═══════════════════════════════════════════════════════════');
