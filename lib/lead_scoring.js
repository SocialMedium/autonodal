// ═══════════════════════════════════════════════════════════════════════════════
// lib/lead_scoring.js — Matrix lead scoring for commercial leads
// ═══════════════════════════════════════════════════════════════════════════════
//
// Positive polarity is a HARD FILTER. Non-positive signals return null —
// they do not compete in the commercial lead ranking.
//
// Ranking formula:
//   relationship_weight × phase_currency × signal_weight × proximity_factor
// ═══════════════════════════════════════════════════════════════════════════════

const { isPositive } = require('./signal_polarity');

const RELATIONSHIP_WEIGHT = {
  active_client:    1.00,
  warm_non_client:  0.85,
  ex_client:        0.55,
  cool_non_client:  0.45,
  cold_non_client:  0.20,
};

const PHASE_CURRENCY = {
  fresh:    0.70,  // plan, don't act
  warming:  1.00,  // peak actionability
  hot:      0.95,  // still strong
  critical: 0.80,  // urgent, compressed window
  closing:  0.40,  // last call
  closed:   0.00,
};

// Signal type weights — derived from newsapi calibration signal_yield %.
// Normalised so the highest-converting type = 1.0.
// Conservative defaults; override via explicit calibrated weights if available.
const SIGNAL_WEIGHT = {
  capital_raising:      1.00,
  strategic_hiring:     0.95,
  ma_activity:          0.90,
  geographic_expansion: 0.85,
  partnership:          0.70,
  product_launch:       0.65,
  leadership_change:    0.60,  // neutral polarity — included for talent, not lead
};

/**
 * Compute lead score for a commercial lead.
 * @param {object} signal — { signal_type, polarity, phase }
 * @param {object} company — { relationship_state }
 * @param {number} userProximity — 0–1 proximity score for the consultant
 * @returns {number|null} lead score, null if signal is not positive polarity
 */
function leadScore(signal, company, userProximity = 0) {
  if (!isPositive(signal.signal_type) && signal.polarity !== 'positive') return null;

  const rw = RELATIONSHIP_WEIGHT[company?.relationship_state] ?? 0.20;
  const pc = PHASE_CURRENCY[signal.phase] ?? 0.70;
  const sw = SIGNAL_WEIGHT[signal.signal_type] ?? 0.60;
  const pf = Math.max(0, Math.min(1, userProximity));

  const score = rw * pc * sw * pf;
  return parseFloat(score.toFixed(4));
}

/**
 * Rank leads — stable sort by score desc, then by phase currency, then by proximity.
 * @param {Array<{signal, company, proximity}>} leads
 * @returns {Array} leads sorted with _score attached
 */
function rankLeads(leads) {
  return leads
    .map(l => ({ ...l, _score: leadScore(l.signal, l.company, l.proximity) }))
    .filter(l => l._score !== null && l._score > 0)
    .sort((a, b) => {
      if (b._score !== a._score) return b._score - a._score;
      const bc = PHASE_CURRENCY[b.signal.phase] ?? 0;
      const ac = PHASE_CURRENCY[a.signal.phase] ?? 0;
      if (bc !== ac) return bc - ac;
      return (b.proximity || 0) - (a.proximity || 0);
    });
}

module.exports = {
  leadScore,
  rankLeads,
  RELATIONSHIP_WEIGHT,
  PHASE_CURRENCY,
  SIGNAL_WEIGHT,
};
