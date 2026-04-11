// ═══════════════════════════════════════════════════════════════════════════════
// lib/signal_weighter.js — Signal confidence weighting by source authority
//
// Applied when creating signal_events records. Official primary sources
// (statistical agencies, registers, courts) get a confidence boost;
// editorial and community sources get a discount.
// ═══════════════════════════════════════════════════════════════════════════════

const SOURCE_WEIGHTS = {
  official_primary:    1.30,  // Statistical agencies, registers, courts, patents
  official_secondary:  1.20,  // Central banks, IPAs, regulators, procurement
  wire_distribution:   1.00,  // PRN, BW, GNW press releases
  research_commercial: 0.90,  // Gartner, PitchBook, analyst firms
  editorial:           0.80,  // News, trade press
  podcast_transcript:  0.70,  // Podcast content
  community:           0.60,  // Social, forums
};

const LEAD_TIME_DECAY = {
  official_primary:    { half_life_days: 180 },
  official_secondary:  { half_life_days: 90  },
  wire_distribution:   { half_life_days: 30  },
  research_commercial: { half_life_days: 60  },
  editorial:           { half_life_days: 14  },
  podcast_transcript:  { half_life_days: 30  },
  community:           { half_life_days: 7   },
};

/**
 * Apply source authority weight to a base confidence score.
 * @param {number} baseConfidence — raw confidence from signal extraction (0–1)
 * @param {object} source — rss_sources row with authority_tier
 * @returns {number} weighted confidence, capped at 0.98
 */
function applySourceWeight(baseConfidence, source) {
  const weight = source.confidence_multiplier || SOURCE_WEIGHTS[source.authority_tier] || 1.0;
  return parseFloat(Math.min(baseConfidence * weight, 0.98).toFixed(3));
}

/**
 * Apply time-based decay to a signal's confidence.
 * Long-lead-time sources (patents, official data) decay slowly;
 * editorial decays fast.
 * @param {number} confidence — current confidence
 * @param {object} source — rss_sources row with authority_tier
 * @param {Date|string} detectedAt — when the signal was detected
 * @returns {number} decayed confidence
 */
function applyLeadTimeDecay(confidence, source, detectedAt) {
  const tier = source.authority_tier;
  const halfLife = LEAD_TIME_DECAY[tier]?.half_life_days || 30;
  const ageInDays = (Date.now() - new Date(detectedAt).getTime()) / (1000 * 60 * 60 * 24);
  if (ageInDays <= 0) return confidence;
  const decayFactor = Math.pow(0.5, ageInDays / halfLife);
  return parseFloat((confidence * decayFactor).toFixed(3));
}

/**
 * Compound confidence when multiple authority tiers confirm the same entity/event.
 * Uses probability union: P(A|B) = 1 - (1-P(A))(1-P(B))
 * @param {Array<{confidence: number}>} signals — array of corroborating signals
 * @returns {number} compound confidence, capped at 0.98
 */
function compoundConfidence(signals) {
  if (!signals || signals.length === 0) return 0;
  if (signals.length === 1) return signals[0].confidence || 0;

  let combined = signals[0].confidence;
  for (let i = 1; i < signals.length; i++) {
    combined = 1 - ((1 - combined) * (1 - signals[i].confidence));
  }
  return parseFloat(Math.min(combined, 0.98).toFixed(3));
}

module.exports = { applySourceWeight, applyLeadTimeDecay, compoundConfidence, SOURCE_WEIGHTS, LEAD_TIME_DECAY };
