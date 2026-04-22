// ═══════════════════════════════════════════════════════════════════════════════
// lib/scoring/proximity.js — 4-factor relationship proximity scoring
// Pure functions, no DB. Unit-testable.
//
// See docs/audits/proximity_audit_2026-04-22.md and Proximity Intelligence
// build prompt for the science.
// ═══════════════════════════════════════════════════════════════════════════════

// Channel half-lives in days (for currency decay)
const HALF_LIFE_DAYS = {
  in_person_meeting: 180,
  video_call:        120,
  phone_call:        120,
  email_reciprocal:   90,
  email_one_way:      45,
  linkedin_message:   30,
  linkedin_reaction:  14,
  research_note:      60,
  unknown:            45,
};

// Weights applied to each channel's interaction count for the weight score
const CHANNEL_WEIGHTS = {
  in_person_meeting: 1.00,
  video_call:        0.80,
  phone_call:        0.80,
  email_reciprocal:  0.60,
  email_one_way:     0.30,
  linkedin_message:  0.20,
  linkedin_reaction: 0.15,
  research_note:     0.35,
};

const COMPOSITE_WEIGHTS = { currency: 0.35, history: 0.20, weight: 0.25, reciprocity: 0.20 };

const HISTORY_SATURATION_YEARS = 5;  // history_score ≈ 0.85 at this tenure
const HISTORY_FLOOR = 0.15;          // never drops below once a relationship exists
const WEIGHT_SATURATION = 10;        // weighted interactions that max out weight_score
const RECIPROCITY_MIN_N = 3;         // below this, return neutral 0.5

function clamp(x, lo = 0, hi = 1) { return Math.max(lo, Math.min(hi, x)); }

// ─── Factor 1: Currency ────────────────────────────────────────────────────────
// Exponential decay on days-since-last-contact, half-life depends on channel.
function currencyScore(lastContactDays, channel) {
  if (lastContactDays == null || lastContactDays < 0) return 0;
  const half = HALF_LIFE_DAYS[channel] || HALF_LIFE_DAYS.unknown;
  return clamp(Math.pow(0.5, lastContactDays / half));
}

// ─── Factor 2: History ─────────────────────────────────────────────────────────
// Logistic curve on tenure. Floor = HISTORY_FLOOR once a relationship exists.
function historyScore(tenureYears) {
  if (tenureYears == null || tenureYears <= 0) return HISTORY_FLOOR;
  // Logistic tuned so years=5 → ~0.85, years=10 → ~0.96, years=1 → ~0.50
  const k = 0.55;  // steepness
  const x0 = 1.0;  // midpoint (years)
  const raw = 1 / (1 + Math.exp(-k * (tenureYears - x0)));
  return clamp(Math.max(HISTORY_FLOOR, raw));
}

// ─── Factor 3: Weight ──────────────────────────────────────────────────────────
// Weighted sum of interactions in last 12mo by channel, saturates at WEIGHT_SATURATION.
// `counts` is { in_person_meeting, video_call, email_reciprocal, email_one_way, linkedin_message, linkedin_reaction, research_note, phone_call }.
function weightScore(counts) {
  if (!counts) return 0;
  let weighted = 0;
  for (const [channel, n] of Object.entries(counts)) {
    weighted += (n || 0) * (CHANNEL_WEIGHTS[channel] || 0);
  }
  return clamp(weighted / WEIGHT_SATURATION);
}

// Helper — returns the channel contributing the most weighted mass
function dominantChannel(counts) {
  if (!counts) return null;
  let best = null, bestMass = 0;
  for (const [ch, n] of Object.entries(counts)) {
    const mass = (n || 0) * (CHANNEL_WEIGHTS[ch] || 0);
    if (mass > bestMass) { bestMass = mass; best = ch; }
  }
  return best;
}

// ─── Factor 4: Reciprocity ─────────────────────────────────────────────────────
// inbound / (inbound + outbound). Neutral 0.5 below RECIPROCITY_MIN_N.
function reciprocityScore(inbound, outbound) {
  const total = (inbound || 0) + (outbound || 0);
  if (total < RECIPROCITY_MIN_N) return 0.5;
  return clamp(inbound / total);
}

// ─── Composite ─────────────────────────────────────────────────────────────────
// Returns { score, factors } where factors is the JSONB payload.
function composeProximity({
  lastContactAt, firstContactAt, lastChannel,
  counts12mo,          // { channel: count }
  inbound12mo, outbound12mo,
  now = new Date(),
}) {
  const nowMs = now.getTime();
  const lastDays = lastContactAt ? Math.floor((nowMs - new Date(lastContactAt).getTime()) / 86400000) : null;
  const tenureYears = firstContactAt ? (nowMs - new Date(firstContactAt).getTime()) / (365.25 * 86400000) : 0;

  const currency = currencyScore(lastDays, lastChannel);
  const history  = historyScore(tenureYears);
  const weight   = weightScore(counts12mo);
  const recip    = reciprocityScore(inbound12mo, outbound12mo);

  const composite = clamp(
    COMPOSITE_WEIGHTS.currency    * currency
  + COMPOSITE_WEIGHTS.history     * history
  + COMPOSITE_WEIGHTS.weight      * weight
  + COMPOSITE_WEIGHTS.reciprocity * recip
  );

  const weightedInteractions = Object.entries(counts12mo || {}).reduce(
    (s, [ch, n]) => s + (n || 0) * (CHANNEL_WEIGHTS[ch] || 0), 0);

  const factors = {
    currency:   { score: +currency.toFixed(3), last_contact_days: lastDays, channel: lastChannel || null },
    history:    { score: +history.toFixed(3),  tenure_years: +tenureYears.toFixed(2), first_contact_at: firstContactAt || null },
    weight:     { score: +weight.toFixed(3),   weighted_interactions_12mo: +weightedInteractions.toFixed(2), dominant_channel: dominantChannel(counts12mo) },
    reciprocity:{ score: +recip.toFixed(3),    inbound: inbound12mo || 0, outbound: outbound12mo || 0, ratio: (inbound12mo + outbound12mo) > 0 ? +(inbound12mo / (inbound12mo + outbound12mo)).toFixed(3) : null },
  };

  return { score: +composite.toFixed(4), factors };
}

// Band classifier (canonical bands used across UI + API + MCP)
function band(score) {
  if (score == null) return null;
  if (score >= 0.7) return 'strong';
  if (score >= 0.4) return 'warm';
  if (score >= 0.2) return 'cool';
  return 'cold';
}

module.exports = {
  currencyScore,
  historyScore,
  weightScore,
  reciprocityScore,
  composeProximity,
  dominantChannel,
  band,
  HALF_LIFE_DAYS,
  CHANNEL_WEIGHTS,
  COMPOSITE_WEIGHTS,
};
