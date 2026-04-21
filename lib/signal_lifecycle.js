// ═══════════════════════════════════════════════════════════════════════════════
// lib/signal_lifecycle.js — Percentile-based signal lifecycle phases
// ═══════════════════════════════════════════════════════════════════════════════
//
// Phase thresholds are empirical, driven by the calibrated percentile
// distribution of historical signal → mandate lead times:
//   fresh    (age < P25)      — window opens commercially, plan outreach
//   warming  (P25 ≤ age < P50) — half of comparable signals haven't converted yet
//   hot      (P50 ≤ age < P75) — peak conversion density
//   critical (P75 ≤ age < P90) — last call, most remaining conversions close here
//   closing  (P90 ≤ age < P90 + grace) — window effectively closed
//   closed   (age ≥ closed_after, or outcome logged)
// ═══════════════════════════════════════════════════════════════════════════════

const fs = require('fs');
const path = require('path');

let PERCENTILES = {};
let FALLBACK_APPLIED = false;

const DEFAULT_PERCENTILES = {
  p25: 30, p50: 70, p75: 180, p90: 450, n: 0,
};

// Grace period after P90 before marking as closed (days)
const CLOSING_GRACE_DAYS = 30;

function _loadPercentiles() {
  try {
    const p = path.join(__dirname, '..', 'reports', 'signal_timing_percentiles.json');
    if (fs.existsSync(p)) {
      const raw = JSON.parse(fs.readFileSync(p, 'utf8'));
      PERCENTILES = raw.percentiles || {};
      return;
    }
  } catch (e) {
    console.warn('[signal_lifecycle] Could not load percentiles:', e.message);
  }
  FALLBACK_APPLIED = true;
}

_loadPercentiles();

function getPercentiles(signalType) {
  return PERCENTILES[signalType] || DEFAULT_PERCENTILES;
}

/**
 * Compute the current phase for a signal based on age and percentile distribution.
 * @param {object} signal — { signal_type, first_detected_at, compound_types? }
 * @param {Date} [now]
 * @returns {object} { phase, age_days, percentiles, critical_at, closing_at, closed_at, description }
 */
function currentPhase(signal, now = new Date()) {
  const startedAt = signal.first_detected_at || signal.detected_at;
  if (!startedAt) return { phase: 'fresh', age_days: 0 };

  const startMs = startedAt instanceof Date ? startedAt.getTime() : new Date(startedAt).getTime();
  const ageDays = (now.getTime() - startMs) / 86400000;

  const p = getPercentiles(signal.signal_type);
  const compression = compoundCompression(signal.compound_types || []);

  // Apply compression (paired signals compress the calendar ~25%)
  const p25 = p.p25 * compression;
  const p50 = p.p50 * compression;
  const p75 = p.p75 * compression;
  const p90 = p.p90 * compression;
  const closedAt = p90 + CLOSING_GRACE_DAYS;

  let phase;
  if (ageDays < p25) phase = 'fresh';
  else if (ageDays < p50) phase = 'warming';
  else if (ageDays < p75) phase = 'hot';
  else if (ageDays < p90) phase = 'critical';
  else if (ageDays < closedAt) phase = 'closing';
  else phase = 'closed';

  // Precomputed phase transition timestamps
  const critical_at = new Date(startMs + p75 * 86400000);
  const closing_at = new Date(startMs + p90 * 86400000);
  const closed_at = new Date(startMs + closedAt * 86400000);

  return {
    phase,
    age_days: Math.round(ageDays),
    percentiles: p,
    compression,
    critical_at,
    closing_at,
    closed_at,
    days_to_critical: Math.max(0, Math.round(p75 - ageDays)),
    days_to_closing: Math.max(0, Math.round(p90 - ageDays)),
    description: phaseDescription(phase, signal.signal_type, Math.round(ageDays), p),
  };
}

/**
 * Compound signals compress the calendar.
 * A paired capital_raising + strategic_hiring converts ~25% faster than either alone.
 * @param {string[]} signalTypes — all signal types present for this company in window
 */
function compoundCompression(signalTypes) {
  if (!Array.isArray(signalTypes) || signalTypes.length < 2) return 1.0;
  if (signalTypes.length >= 3) return 0.65;
  return 0.75;
}

function phaseDescription(phase, signalType, ageDays, p) {
  const label = (signalType || 'signal').replace(/_/g, ' ');
  switch (phase) {
    case 'fresh':
      return `${label} detected ${ageDays}d ago. Window opening — plan outreach (P25 @ ${p.p25}d).`;
    case 'warming':
      return `${label} maturing. Half of comparable signals convert past this point (P50 @ ${p.p50}d).`;
    case 'hot':
      return `${label} in peak conversion density. Act now (P75 @ ${p.p75}d).`;
    case 'critical':
      return `${label} in critical window. Last call — most remaining conversions close here (P90 @ ${p.p90}d).`;
    case 'closing':
      return `${label} window effectively closed. ${ageDays}d since detection.`;
    case 'closed':
      return `${label} archived. ${ageDays}d since detection, past typical conversion window.`;
    default:
      return `Signal detected ${ageDays}d ago.`;
  }
}

module.exports = {
  currentPhase,
  compoundCompression,
  getPercentiles,
  PHASES: ['fresh', 'warming', 'hot', 'critical', 'closing', 'closed'],
};
