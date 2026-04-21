// ═══════════════════════════════════════════════════════════════════════════════
// lib/talent_refresh_scoring.js — Score people exposed to negative signals
// ═══════════════════════════════════════════════════════════════════════════════
//
// Talent receptivity after distress signals peaks at days 30–90.
//   Before day 30 the person is processing — likely to decline outreach.
//   After day 90 they've landed or settled in — window closes.
//
// This is a DIFFERENT distribution from positive-signal commercial conversion.
// Scored separately, surfaces on a weekly cadence, never competes in the
// commercial lead ranking.
// ═══════════════════════════════════════════════════════════════════════════════

const { isNegative } = require('./signal_polarity');

const SENIORITY_WEIGHT = {
  c_suite:    1.00,
  vp:         0.90,
  director:   0.80,
  senior_ic:  0.65,
  manager:    0.60,
  mid_level:  0.40,
  junior:     0.20,
};

/**
 * Receptivity curve — bell-shaped around days 30-90 post-signal.
 * Returns 0-1.
 */
function talentReceptivity(daysSinceSignal) {
  if (daysSinceSignal < 0) return 0;
  if (daysSinceSignal < 14) return 0.15; // processing
  if (daysSinceSignal < 30) return 0.45; // assessing
  if (daysSinceSignal < 60) return 0.95; // peak receptivity
  if (daysSinceSignal < 90) return 0.85; // still receptive
  if (daysSinceSignal < 120) return 0.55; // landing
  if (daysSinceSignal < 180) return 0.25; // probably settled
  return 0.05;
}

/**
 * Score a talent refresh candidate.
 * @param {object} person — { seniority_level }
 * @param {object} signal — { signal_type, polarity, first_detected_at }
 * @param {number} consultantProximity — 0-1
 * @returns {number|null} score 0-1, or null if signal is not negative polarity
 */
function talentRefreshScore(person, signal, consultantProximity = 0) {
  if (!isNegative(signal.signal_type) && signal.polarity !== 'negative') return null;

  const startedAt = signal.first_detected_at || signal.detected_at;
  if (!startedAt) return null;
  const daysSince = Math.floor((Date.now() - new Date(startedAt).getTime()) / 86400000);

  const receptivity = talentReceptivity(daysSince);
  const seniority = SENIORITY_WEIGHT[person?.seniority_level] ?? 0.50;
  const proximity = Math.max(0, Math.min(1, consultantProximity));

  const score = receptivity * seniority * proximity;
  return parseFloat(score.toFixed(4));
}

module.exports = {
  talentRefreshScore,
  talentReceptivity,
  SENIORITY_WEIGHT,
};
