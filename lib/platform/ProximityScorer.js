// ═══════════════════════════════════════════════════════════════════════════════
// lib/platform/ProximityScorer.js — Pure Proximity Scoring Functions
// ═══════════════════════════════════════════════════════════════════════════════
//
// No DB access, no tenant state. Called by ProximityEngine and HuddleEngine
// with interaction data from each member's sandbox.

const INTERACTION_WEIGHTS = {
  meeting_in_person:    1.00,
  meeting_video:        0.85,
  meeting:              0.85,
  call:                 0.75,
  email_reciprocal:     0.60,
  email_received:       0.60,
  email_sent:           0.30,
  email_outbound:       0.30,
  linkedin_message:     0.25,
  intro_made:           0.70,
  intro_received:       0.80,
  messaging_reciprocal: 0.65,
  messaging_outbound:   0.35,
  whatsapp:             0.65,
  telegram:             0.65,
  event_shared:         0.20,
  linkedin_connect:     0.05,
  research_note:        0.10,
};

const DECAY_HALF_LIVES_DAYS = {
  meeting_in_person:    180,
  meeting_video:        120,
  meeting:              120,
  call:                  90,
  email_reciprocal:      60,
  email_received:        60,
  email_sent:            30,
  email_outbound:        30,
  messaging_reciprocal:  45,
  messaging_outbound:    20,
  whatsapp:              45,
  telegram:              45,
  linkedin_message:      21,
  event_shared:          30,
  linkedin_connect:     365,
  research_note:         90,
};

/**
 * Compute relationship strength score for a single person<->member edge.
 *
 * @param {Object} params
 * @param {Array}  params.interactions  [{type, occurred_at, direction}]
 * @param {Date}   params.firstContact  earliest known interaction date
 * @param {Date}   [params.now]         override for testing
 * @returns {Object} { strength, currency, history, depth, reciprocity, label, action, ... }
 */
function computeStrength(params) {
  var interactions = params.interactions;
  var firstContact = params.firstContact;
  var now = params.now || new Date();

  if (!interactions || !interactions.length) {
    return {
      strength: 0, currency: 0, history: 0, depth: 0, reciprocity: 0,
      label: 'Cold', action: 'No known relationship',
      currencyLabel: 'Never', depthType: 'none',
    };
  }

  var sorted = interactions.slice().sort(function(a, b) {
    return new Date(b.occurred_at) - new Date(a.occurred_at);
  });
  var mostRecent = sorted[0];
  var now_ms = now.getTime();

  // CURRENCY: decay from most recent interaction
  var daysSince = (now_ms - new Date(mostRecent.occurred_at).getTime()) / 86400000;
  var halfLife = DECAY_HALF_LIVES_DAYS[mostRecent.type] || 60;
  var baseWeight = INTERACTION_WEIGHTS[mostRecent.type] || 0.3;
  var currency = baseWeight * Math.pow(0.5, daysSince / halfLife);

  // HISTORY: tenure floor — long relationships never read as cold
  var tenureYears = firstContact
    ? (now_ms - new Date(firstContact).getTime()) / (365.25 * 86400000)
    : 0;
  var history = Math.min(0.30, tenureYears * 0.06);

  // DEPTH: weighted average of all interactions
  var totalDepth = 0;
  for (var i = 0; i < interactions.length; i++) {
    var ix = interactions[i];
    var w = INTERACTION_WEIGHTS[ix.type] || 0.1;
    var recency = Math.pow(0.95, (now_ms - new Date(ix.occurred_at).getTime()) / 86400000);
    totalDepth += w * recency;
  }
  var depth = Math.min(1.0, totalDepth / interactions.length);

  // RECIPROCITY: do they engage back?
  var inbound = 0;
  for (var j = 0; j < interactions.length; j++) {
    if (interactions[j].direction === 'inbound') inbound++;
  }
  var reciprocity = interactions.length > 0 ? inbound / interactions.length : 0;
  var reciprocityMultiplier = 0.7 + (reciprocity * 0.6);

  // COMBINED
  var raw = (
    (currency * 0.40) +
    (history * 0.25) +
    (depth * 0.20) +
    (reciprocity * 0.15)
  ) * reciprocityMultiplier;

  var strength = Math.min(1.0, Math.round(raw * 1000) / 1000);

  // LABELS
  var classResult;
  if (strength >= 0.70) classResult = { label: 'Strong', action: 'Reach out directly' };
  else if (strength >= 0.45) classResult = { label: 'Warm', action: 'Brief re-warm first' };
  else if (strength >= 0.20) classResult = { label: 'Cool', action: 'Needs re-engagement' };
  else classResult = { label: 'Cold', action: 'Find a warmer path first' };

  var currencyLabelText;
  if (daysSince <= 7) currencyLabelText = 'This week';
  else if (daysSince <= 30) currencyLabelText = 'This month';
  else if (daysSince <= 90) currencyLabelText = 'Last quarter';
  else if (daysSince <= 365) currencyLabelText = Math.round(daysSince / 30) + ' months ago';
  else currencyLabelText = Math.round(daysSince / 365) + ' year' + (daysSince > 730 ? 's' : '') + ' ago';

  var depthTypeResult;
  var mt = mostRecent.type;
  if (mt === 'meeting_in_person' || mt === 'meeting_video' || mt === 'meeting') depthTypeResult = 'meeting';
  else if (mt === 'call') depthTypeResult = 'call';
  else if (mt && mt.indexOf('email') >= 0) depthTypeResult = 'email';
  else if (mt && (mt.indexOf('messaging') >= 0 || mt === 'whatsapp' || mt === 'telegram')) depthTypeResult = 'messaging';
  else depthTypeResult = 'linkedin';

  return {
    strength: strength,
    currency: Math.round(currency * 1000) / 1000,
    history: Math.round(history * 1000) / 1000,
    depth: Math.round(depth * 1000) / 1000,
    reciprocity: Math.round(reciprocity * 1000) / 1000,
    label: classResult.label,
    action: classResult.action,
    currencyLabel: currencyLabelText,
    depthType: depthTypeResult,
  };
}

/**
 * Rank all member paths to a target person.
 * Returns ordered array — index 0 is best entry point.
 * Never exposes source_platform.
 */
function rankEntryPoints(proximityEdges) {
  return proximityEdges
    .map(function(edge) {
      return {
        member_tenant_id: edge.member_tenant_id,
        strength_score: parseFloat(edge.strength_score) || 0,
        depth_type: edge.depth_type,
        currency_label: edge.currency_label,
        entry_recommendation: edge.entry_recommendation,
        entry_action: edge.entry_action,
        last_contact: edge.last_contact,
        // source_platform intentionally excluded
      };
    })
    .sort(function(a, b) { return b.strength_score - a.strength_score; });
}

/**
 * Build the best entry point recommendation for a huddle_people row.
 */
function buildEntryRecommendation(rankedPaths, memberNames) {
  memberNames = memberNames || {};

  if (!rankedPaths.length) {
    return {
      best_member_tenant_id: null,
      best_strength_score: 0,
      best_entry_label: 'Cold',
      best_entry_reason: 'No warm path in this huddle',
      best_depth_type: null,
    };
  }

  var best = rankedPaths[0];
  var backup = rankedPaths[1];
  var bestName = memberNames[best.member_tenant_id] || 'A member';

  var parts = [
    bestName + ' · ' + best.entry_recommendation,
    best.currency_label + ' · ' + best.depth_type + ' contact',
  ];
  if (backup) {
    parts.push('Backup: another member at ' + backup.entry_recommendation + ' strength');
  }

  return {
    best_member_tenant_id: best.member_tenant_id,
    best_strength_score: best.strength_score,
    best_depth_type: best.depth_type,
    best_entry_label: best.entry_recommendation,
    best_entry_reason: parts.join(' · '),
  };
}

module.exports = { computeStrength, rankEntryPoints, buildEntryRecommendation, INTERACTION_WEIGHTS };
