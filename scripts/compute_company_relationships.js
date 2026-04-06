#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/compute_company_relationships.js
// Aggregates interaction data into company-level relationship scores.
// Safe to re-run (upserts). Scoped to tenant.
//
// Usage:
//   node scripts/compute_company_relationships.js
//   node scripts/compute_company_relationships.js --tenant <uuid>
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = (() => {
  const idx = process.argv.indexOf('--tenant');
  return idx !== -1 ? process.argv[idx + 1] : (process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001');
})();

// ═══════════════════════════════════════════════════════════════════════════════
// SCORING FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

// Signal type weights
var SIGNAL_WEIGHTS = {
  capital_raising: 1.0, ma_activity: 0.95, geographic_expansion: 0.85,
  strategic_hiring: 0.80, leadership_change: 0.75, product_launch: 0.65,
  restructuring: 0.70, layoffs: 0.60, partnership: 0.55,
};

function signalRecencyMultiplier(days) {
  if (days <= 7) return 1.0;
  if (days <= 14) return 0.9;
  if (days <= 30) return 0.75;
  if (days <= 60) return 0.5;
  if (days <= 90) return 0.25;
  return 0.1;
}

function getElevationTier(relScore, sigScore) {
  if (relScore >= 0.45 && sigScore >= 0.6) return 'critical';
  if (relScore >= 0.20 && sigScore >= 0.3) return 'active';
  if (relScore >= 0.01 && sigScore >= 0.1) return 'monitor';
  if (relScore < 0.20 && sigScore >= 0.3) return 'gap';
  return 'quiet';
}

function currencyScore(daysSince) {
  if (daysSince === null || daysSince === undefined) return 0;
  if (daysSince <= 14) return 1.0;
  if (daysSince <= 30) return 0.85;
  if (daysSince <= 60) return 0.70;
  if (daysSince <= 90) return 0.55;
  if (daysSince <= 180) return 0.35;
  if (daysSince <= 365) return 0.15;
  return 0.05;
}

function depthScore(bestType, hasInbound) {
  var weights = {
    meeting_held: 1.0, meeting: 1.0,
    call_completed: 0.75, call: 0.75,
    email_sent: 0.4, email: 0.4,
    email_received: 0.6,
    linkedin_message: 0.2,
  };
  return weights[bestType] || 0.1;
}

function coverageScore(activeContacts, teamMembers) {
  var base = activeContacts >= 3 ? 1.0 : activeContacts === 2 ? 0.75 : activeContacts === 1 ? 0.5 : 0;
  var multiplier = teamMembers >= 2 ? 1.2 : 1.0;
  return Math.min(1.0, base * multiplier);
}

function reciprocityScore(inboundRatio) {
  if (inboundRatio === null || inboundRatio === undefined) return 0;
  if (inboundRatio >= 0.4) return 1.0;
  if (inboundRatio >= 0.2) return 0.7;
  if (inboundRatio >= 0.1) return 0.4;
  return 0.2;
}

function computeFinalScore(currency, depth, coverage, reciprocity) {
  return Math.round((currency * 0.35 + depth * 0.25 + coverage * 0.25 + reciprocity * 0.15) * 1000) / 1000;
}

function tierFromScore(score) {
  if (score >= 0.70) return 'strong';
  if (score >= 0.45) return 'warm';
  if (score >= 0.20) return 'cool';
  if (score > 0) return 'cold';
  return 'none';
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function compute() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  COMPUTE COMPANY RELATIONSHIPS');
  console.log('  Tenant: ' + TENANT_ID);
  console.log('═══════════════════════════════════════════════════════════════\n');

  var startTime = Date.now();

  // Get all companies with at least one person
  var { rows: companies } = await pool.query(`
    SELECT c.id, c.name, COUNT(DISTINCT p.id) AS people_count
    FROM companies c
    JOIN people p ON p.current_company_id = c.id AND p.tenant_id = $1
    WHERE c.tenant_id = $1
    GROUP BY c.id, c.name
    ORDER BY COUNT(DISTINCT p.id) DESC
  `, [TENANT_ID]);

  console.log('Companies to score: ' + companies.length + '\n');

  var stats = { total: 0, strong: 0, warm: 0, cool: 0, cold: 0, none: 0, skipped: 0 };
  var BATCH_SIZE = 100;

  for (var i = 0; i < companies.length; i++) {
    var co = companies[i];

    // Get all interactions with people at this company
    var { rows: interactions } = await pool.query(`
      SELECT
        i.interaction_type, i.direction, i.interaction_at, i.user_id,
        EXTRACT(EPOCH FROM (NOW() - i.interaction_at)) / 86400 AS days_ago
      FROM interactions i
      JOIN people p ON p.id = i.person_id AND p.current_company_id = $1
      WHERE i.tenant_id = $2
        AND i.interaction_type NOT IN ('system_note', 'research_note', 'note')
      ORDER BY i.interaction_at DESC
    `, [co.id, TENANT_ID]);

    if (interactions.length === 0) {
      // No real interactions — but may have signals
      var { rows: noIxSignals } = await pool.query(`
        SELECT signal_type, detected_at, EXTRACT(EPOCH FROM (NOW() - detected_at))/86400 AS days_ago
        FROM signal_events WHERE company_id = $1 AND detected_at > NOW() - INTERVAL '90 days'
          AND (tenant_id IS NULL OR tenant_id = $2)
        ORDER BY detected_at DESC
      `, [co.id, TENANT_ID]);
      var noIxSigScores = noIxSignals.map(function(s) { return { type: s.signal_type, score: (SIGNAL_WEIGHTS[s.signal_type]||0.3) * signalRecencyMultiplier(s.days_ago), at: s.detected_at }; }).sort(function(a,b) { return b.score - a.score; });
      var noIxSigScore = Math.min(1.0, noIxSigScores.slice(0,3).reduce(function(s,x){return s+x.score},0));
      var noIxSig30 = noIxSignals.filter(function(s){return s.days_ago<=30});
      var noIxElevTier = noIxSigScore >= 0.3 ? 'gap' : 'quiet';

      await upsert(co.id, {
        tier: 'none', score: 0,
        activeContacts: 0, totalContacts: parseInt(co.people_count),
        teamMembers: 0, lastAt: null, lastType: null,
        inboundRatio: null, isStale: true, staleReason: 'no_interactions',
        signalScore: noIxSigScore, signalCount30d: noIxSig30.length,
        signalTypesActive: [...new Set(noIxSig30.map(function(s){return s.signal_type}))],
        highestSignalType: noIxSigScores[0]?.type || null,
        highestSignalAt: noIxSigScores[0]?.at || null,
        elevatedScore: Math.round(noIxSigScore * 0.4 * 1000) / 1000,
        elevationTier: noIxElevTier,
        factors: { currency: 0, depth: 0, coverage: 0, reciprocity: 0, signal_score: noIxSigScore, total_interactions_12m: 0 },
      });
      stats.none++;
      stats.total++;
      continue;
    }

    // Currency: days since most recent
    var daysSince = interactions[0].days_ago;
    var currency = currencyScore(daysSince);

    // Depth: best interaction type in last 90 days
    var recent90 = interactions.filter(function(ix) { return ix.days_ago <= 90; });
    var typeOrder = ['meeting_held', 'meeting', 'call_completed', 'call', 'email_received', 'email_sent', 'email', 'linkedin_message'];
    var bestType = null;
    for (var t = 0; t < typeOrder.length; t++) {
      if (recent90.some(function(ix) { return ix.interaction_type === typeOrder[t]; })) { bestType = typeOrder[t]; break; }
    }
    if (!bestType && recent90.length > 0) bestType = recent90[0].interaction_type;
    var depth = bestType ? depthScore(bestType) : 0;

    // Coverage: active contacts (last 12 months) + team members
    var recent12m = interactions.filter(function(ix) { return ix.days_ago <= 365; });
    var activePersonIds = new Set();
    var teamMemberIds = new Set();
    recent12m.forEach(function(ix) {
      // We don't have person_id in this query — count unique user_ids as team members
      if (ix.user_id) teamMemberIds.add(ix.user_id);
    });

    // Get actual active contact count
    var { rows: [contactCounts] } = await pool.query(`
      SELECT
        COUNT(DISTINCT CASE WHEN i.interaction_at > NOW() - INTERVAL '12 months' THEN i.person_id END) AS active_contacts,
        COUNT(DISTINCT i.person_id) AS total_contacts
      FROM interactions i
      JOIN people p ON p.id = i.person_id AND p.current_company_id = $1
      WHERE i.tenant_id = $2
        AND i.interaction_type NOT IN ('system_note', 'research_note', 'note')
    `, [co.id, TENANT_ID]);

    var activeContacts = parseInt(contactCounts.active_contacts) || 0;
    var totalContacts = parseInt(contactCounts.total_contacts) || parseInt(co.people_count) || 0;
    var teamMembers = teamMemberIds.size;
    var coverage = coverageScore(activeContacts, teamMembers);

    // Reciprocity: inbound ratio in last 12 months
    var inboundCount = recent12m.filter(function(ix) { return ix.direction === 'inbound'; }).length;
    var totalRecent = recent12m.length;
    var inboundRatio = totalRecent > 0 ? Math.round(inboundCount / totalRecent * 1000) / 1000 : null;
    var reciprocity = reciprocityScore(inboundRatio);

    // Final score
    var score = computeFinalScore(currency, depth, coverage, reciprocity);
    var tier = tierFromScore(score);

    // Staleness
    var isStale = daysSince > 365;
    var staleReason = isStale ? 'no_recent_contact' : null;

    // Signal scoring — company-level market activity
    var { rows: signals } = await pool.query(`
      SELECT signal_type, detected_at,
        EXTRACT(EPOCH FROM (NOW() - detected_at)) / 86400 AS days_ago
      FROM signal_events
      WHERE company_id = $1 AND detected_at > NOW() - INTERVAL '90 days'
        AND (tenant_id IS NULL OR tenant_id = $2)
      ORDER BY detected_at DESC
    `, [co.id, TENANT_ID]);

    var signalScores = signals.map(function(s) {
      var w = SIGNAL_WEIGHTS[s.signal_type] || 0.3;
      return { type: s.signal_type, score: w * signalRecencyMultiplier(s.days_ago), at: s.detected_at };
    }).sort(function(a, b) { return b.score - a.score; });

    var signalScore = Math.min(1.0, signalScores.slice(0, 3).reduce(function(s, x) { return s + x.score; }, 0));
    var signals30d = signals.filter(function(s) { return s.days_ago <= 30; });
    var signalTypes30d = [...new Set(signals30d.map(function(s) { return s.signal_type; }))];
    var highestSignal = signalScores[0] || null;

    // Elevated score
    var elevatedScore = Math.round((score * 0.6 + signalScore * 0.4) * 1000) / 1000;
    var elevationTier = getElevationTier(score, signalScore);

    await upsert(co.id, {
      tier: tier, score: score,
      activeContacts: activeContacts, totalContacts: totalContacts,
      teamMembers: teamMembers,
      lastAt: interactions[0].interaction_at,
      lastType: interactions[0].interaction_type,
      inboundRatio: inboundRatio,
      isStale: isStale, staleReason: staleReason,
      signalScore: signalScore, signalCount30d: signals30d.length,
      signalTypesActive: signalTypes30d,
      highestSignalType: highestSignal?.type || null,
      highestSignalAt: highestSignal?.at || null,
      elevatedScore: elevatedScore, elevationTier: elevationTier,
      factors: {
        currency: currency, currency_days_since: Math.round(daysSince),
        depth: depth, depth_best_type: bestType,
        coverage: coverage, coverage_active_contacts: activeContacts, coverage_team_members: teamMembers,
        reciprocity: reciprocity, reciprocity_inbound_ratio: inboundRatio,
        signal_score: signalScore, signal_count_90d: signals.length,
        total_interactions_90d: recent90.length,
        total_interactions_12m: recent12m.length,
      },
    });

    stats[tier]++;
    stats.total++;

    if ((i + 1) % BATCH_SIZE === 0) {
      console.log('  Progress: ' + (i + 1) + '/' + companies.length + ' (' + Math.round((i + 1) / companies.length * 100) + '%)');
    }
  }

  var duration = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  COMPANY RELATIONSHIPS COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  Total:   ' + stats.total);
  console.log('  Strong:  ' + stats.strong);
  console.log('  Warm:    ' + stats.warm);
  console.log('  Cool:    ' + stats.cool);
  console.log('  Cold:    ' + stats.cold);
  console.log('  None:    ' + stats.none);
  console.log('  Duration: ' + duration + 's');

  await pool.end();
  return stats;
}

async function upsert(companyId, data) {
  await pool.query(`
    INSERT INTO company_relationships (
      tenant_id, company_id, relationship_tier, relationship_score,
      active_contact_count, total_contact_count, team_member_count,
      last_interaction_at, last_interaction_type, inbound_ratio,
      is_stale, stale_reason, stale_since, score_factors,
      signal_score, signal_count_30d, signal_types_active,
      highest_signal_type, highest_signal_at,
      elevated_score, elevation_tier, computed_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NOW())
    ON CONFLICT (tenant_id, company_id) DO UPDATE SET
      relationship_tier = EXCLUDED.relationship_tier,
      relationship_score = EXCLUDED.relationship_score,
      active_contact_count = EXCLUDED.active_contact_count,
      total_contact_count = EXCLUDED.total_contact_count,
      team_member_count = EXCLUDED.team_member_count,
      last_interaction_at = EXCLUDED.last_interaction_at,
      last_interaction_type = EXCLUDED.last_interaction_type,
      inbound_ratio = EXCLUDED.inbound_ratio,
      is_stale = EXCLUDED.is_stale,
      stale_reason = EXCLUDED.stale_reason,
      stale_since = CASE WHEN EXCLUDED.is_stale AND NOT company_relationships.is_stale THEN NOW() ELSE company_relationships.stale_since END,
      score_factors = EXCLUDED.score_factors,
      signal_score = EXCLUDED.signal_score,
      signal_count_30d = EXCLUDED.signal_count_30d,
      signal_types_active = EXCLUDED.signal_types_active,
      highest_signal_type = EXCLUDED.highest_signal_type,
      highest_signal_at = EXCLUDED.highest_signal_at,
      elevated_score = EXCLUDED.elevated_score,
      elevation_tier = EXCLUDED.elevation_tier,
      computed_at = NOW(),
      updated_at = NOW()
  `, [
    TENANT_ID, companyId, data.tier, data.score,
    data.activeContacts, data.totalContacts, data.teamMembers,
    data.lastAt, data.lastType, data.inboundRatio,
    data.isStale, data.staleReason,
    data.isStale ? new Date() : null,
    JSON.stringify(data.factors),
    data.signalScore || 0, data.signalCount30d || 0,
    data.signalTypesActive || [],
    data.highestSignalType || null, data.highestSignalAt || null,
    data.elevatedScore || 0, data.elevationTier || 'quiet',
  ]);
}

if (require.main === module) {
  compute().then(function() { process.exit(0); }).catch(function(e) { console.error(e); process.exit(1); });
}

module.exports = { compute };
