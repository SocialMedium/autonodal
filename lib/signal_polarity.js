// ═══════════════════════════════════════════════════════════════════════════════
// lib/signal_polarity.js — Signal polarity lookup
// ═══════════════════════════════════════════════════════════════════════════════
//
// Polarity reflects commercial intent, not tone.
//   positive = predicts expansion of headcount / mandate (commercial lead)
//   negative = predicts contraction (still useful for talent refresh, not leads)
//   neutral  = bidirectional; resolve via signal text or secondary data
// ═══════════════════════════════════════════════════════════════════════════════

const POLARITY = {
  capital_raising:      'positive',
  geographic_expansion: 'positive',
  product_launch:       'positive',
  partnership:          'positive',
  strategic_hiring:     'positive',
  ma_activity:          'positive',   // acquirer side — flip if target detected
  leadership_change:    'neutral',    // CFO departure → CFO search
  restructuring:        'negative',
  layoffs:              'negative',
};

function polarityFor(signalType) {
  return POLARITY[signalType] || null;
}

function isPositive(signalType) { return POLARITY[signalType] === 'positive'; }
function isNegative(signalType) { return POLARITY[signalType] === 'negative'; }
function isNeutral(signalType) { return POLARITY[signalType] === 'neutral'; }

module.exports = { POLARITY, polarityFor, isPositive, isNegative, isNeutral };
