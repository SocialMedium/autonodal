/**
 * MitchelLake Signal Intelligence Platform
 * Score Computation Engine
 * 
 * Computes engagement, activity, receptivity, timing, and other scores for people
 */

require('dotenv').config();
const pool = require('../lib/db');

// Score computation weights
const WEIGHTS = {
  engagement: {
    response_rate: 0.4,
    recency: 0.3,
    interaction_depth: 0.3
  },
  activity: {
    external_signals: 0.5,
    content_published: 0.3,
    visibility_events: 0.2
  },
  receptivity: {
    tenure: 0.3,
    company_stability: 0.3,
    response_patterns: 0.4
  },
  flight_risk: {
    company_trouble: 0.4,
    short_tenure: 0.3,
    high_activity: 0.3
  },
  timing: {
    receptivity_weight: 0.4,
    activity_weight: 0.3,
    not_recent_move: 0.3
  }
};

// Decay function for recency (half-life in days)
function recencyDecay(days, halfLife = 30) {
  return Math.pow(0.5, days / halfLife);
}

// Normalize score to 0-1 range
function normalize(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

/**
 * Compute engagement score based on interactions
 */
async function computeEngagementScore(personId) {
  const result = await pool.query(`
    SELECT 
      interaction_type,
      created_at,
      response_received
    FROM interactions
    WHERE person_id = $1
    ORDER BY created_at DESC
    LIMIT 50
  `, [personId]);

  if (result.rows.length === 0) {
    return { score: 0, factors: { no_interactions: true } };
  }

  const interactions = result.rows;
  
  // Response rate
  const outbound = interactions.filter(i => 
    ['email_sent', 'linkedin_message', 'call'].includes(i.interaction_type)
  );
  const responses = outbound.filter(i => i.response_received);
  const responseRate = outbound.length > 0 ? responses.length / outbound.length : 0;

  // Recency of last interaction
  const lastInteraction = new Date(interactions[0].created_at);
  const daysSince = (Date.now() - lastInteraction) / (1000 * 60 * 60 * 24);
  const recencyScore = recencyDecay(daysSince, 30);

  // Interaction depth (weighted by type)
  const depthWeights = {
    meeting: 1.0,
    call: 0.8,
    email_received: 0.6,
    linkedin_message: 0.5,
    email_sent: 0.3,
    intro_made: 0.7
  };
  
  const depthSum = interactions.reduce((sum, i) => {
    const weight = depthWeights[i.interaction_type] || 0.3;
    const age = (Date.now() - new Date(i.created_at)) / (1000 * 60 * 60 * 24);
    return sum + weight * recencyDecay(age, 60);
  }, 0);
  const depthScore = normalize(depthSum / 10);

  // Weighted combination
  const score = normalize(
    WEIGHTS.engagement.response_rate * responseRate +
    WEIGHTS.engagement.recency * recencyScore +
    WEIGHTS.engagement.interaction_depth * depthScore
  );

  return {
    score,
    factors: {
      response_rate: responseRate,
      recency_score: recencyScore,
      depth_score: depthScore,
      interaction_count: interactions.length,
      days_since_contact: Math.round(daysSince)
    }
  };
}

/**
 * Compute activity score based on external signals and content
 */
async function computeActivityScore(personId) {
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  // Get person signals in last 30 days
  const signalsResult = await pool.query(`
    SELECT signal_type, detected_at
    FROM person_signals
    WHERE person_id = $1 AND detected_at > $2
  `, [personId, thirtyDaysAgo]);

  // Get content published in last 30 days
  const contentResult = await pool.query(`
    SELECT published_at
    FROM person_content
    WHERE person_id = $1 AND published_at > $2
  `, [personId, thirtyDaysAgo]);

  const signalCount = signalsResult.rows.length;
  const contentCount = contentResult.rows.length;

  // Visibility events (speaking, awards, etc.)
  const visibilitySignals = signalsResult.rows.filter(s =>
    ['speaking_engagement', 'publication', 'award', 'podcast_appearance'].includes(s.signal_type)
  );

  // Normalize scores
  const signalScore = normalize(signalCount / 5); // 5 signals = max
  const contentScore = normalize(contentCount / 3); // 3 pieces = max
  const visibilityScore = normalize(visibilitySignals.length / 2);

  const score = normalize(
    WEIGHTS.activity.external_signals * signalScore +
    WEIGHTS.activity.content_published * contentScore +
    WEIGHTS.activity.visibility_events * visibilityScore
  );

  return {
    score,
    factors: {
      signal_count: signalCount,
      content_count: contentCount,
      visibility_events: visibilitySignals.length
    }
  };
}

/**
 * Compute receptivity score based on tenure and company stability
 */
async function computeReceptivityScore(personId) {
  // Get person's current role info
  const personResult = await pool.query(`
    SELECT 
      p.current_company_name,
      
      p.created_at,
      c.id as company_id
    FROM people p
    LEFT JOIN companies c ON LOWER(c.name) = LOWER(p.current_company_name)
    WHERE p.id = $1
  `, [personId]);

  if (personResult.rows.length === 0) {
    return { score: 0.5, factors: { person_not_found: true } };
  }
  const person = personResult.rows[0];
  // Derive role start date from career_history JSONB
  let roleStartDate = null;
  try {
    const hist = Array.isArray(person.career_history) ? person.career_history :
      (person.career_history ? JSON.parse(person.career_history) : []);
    const cur = hist.find(r => r.current || !r.end_date) || hist[0];
    roleStartDate = cur?.start_date || null;
  } catch(e) {}
  // Tenure score (2-5 years is ideal sweet spot)
  let tenureScore = 0.5;
  if (roleStartDate) {
    const tenureMonths = (Date.now() - new Date(roleStartDate)) / (1000 * 60 * 60 * 24 * 30);
    if (tenureMonths < 6) {
      tenureScore = 0.2;
    } else if (tenureMonths < 24) {
      tenureScore = 0.5;
    } else if (tenureMonths < 48) {
      tenureScore = 0.9;
    } else if (tenureMonths < 72) {
      tenureScore = 0.7;
    } else {
      tenureScore = 0.5;
    }
  }

  // Company stability score (check for negative company signals)
  let companyStabilityScore = 0.5;
  if (person.company_id) {
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    const companySignals = await pool.query(`
      SELECT signal_type
      FROM signal_events
      WHERE company_id = $1 AND detected_at > $2
    `, [person.company_id, threeMonthsAgo]);

    const negativeSignals = companySignals.rows.filter(s =>
      ['layoffs', 'down_round', 'funding_struggle', 'restructuring'].includes(s.signal_type)
    );
    const positiveSignals = companySignals.rows.filter(s =>
      ['capital_raising', 'product_launch', 'partnership'].includes(s.signal_type)
    );

    if (negativeSignals.length > 0) {
      companyStabilityScore = 0.8; // Instability = more receptive
    } else if (positiveSignals.length > 0) {
      companyStabilityScore = 0.3; // Stability = less receptive
    }
  }

  // Response pattern score (from engagement)
  const engagement = await computeEngagementScore(personId);
  const responsePatternScore = engagement.factors.response_rate || 0.5;

  const score = normalize(
    WEIGHTS.receptivity.tenure * tenureScore +
    WEIGHTS.receptivity.company_stability * companyStabilityScore +
    WEIGHTS.receptivity.response_patterns * responsePatternScore
  );

  return {
    score,
    factors: {
      tenure_score: tenureScore,
      company_stability_score: companyStabilityScore,
      response_pattern_score: responsePatternScore
    }
  };
}

/**
 * Compute flight risk score
 */
async function computeFlightRiskScore(personId, activityScore, receptivityFactors) {
  const companyTroubleScore = receptivityFactors.company_stability_score > 0.6 ? 
    receptivityFactors.company_stability_score : 0.2;
  
  const shortTenureScore = receptivityFactors.tenure_score < 0.5 ? 0.7 : 0.3;
  const highActivityScore = activityScore > 0.6 ? activityScore : 0.3;

  const score = normalize(
    WEIGHTS.flight_risk.company_trouble * companyTroubleScore +
    WEIGHTS.flight_risk.short_tenure * shortTenureScore +
    WEIGHTS.flight_risk.high_activity * highActivityScore
  );

  return {
    score,
    factors: {
      company_trouble: companyTroubleScore,
      tenure_risk: shortTenureScore,
      activity_level: highActivityScore
    }
  };
}

/**
 * Compute overall timing score
 */
function computeTimingScore(receptivityScore, activityScore, recentMove) {
  const notRecentMoveScore = recentMove ? 0.1 : 0.9;

  const score = normalize(
    WEIGHTS.timing.receptivity_weight * receptivityScore +
    WEIGHTS.timing.activity_weight * activityScore +
    WEIGHTS.timing.not_recent_move * notRecentMoveScore
  );

  return {
    score,
    factors: {
      receptivity: receptivityScore,
      activity: activityScore,
      not_recent_move: notRecentMoveScore
    }
  };
}

/**
 * Compute all scores for a person and update the database
 */
async function computePersonScores(personId) {
  console.log(`Computing scores for person ${personId}...`);

  // Compute individual scores
  const engagement = await computeEngagementScore(personId);
  const activity = await computeActivityScore(personId);
  const receptivity = await computeReceptivityScore(personId);
  const flightRisk = await computeFlightRiskScore(personId, activity.score, receptivity.factors);
  
  // Check for recent role change
  const personResult = await pool.query(`
    SELECT career_history FROM people WHERE id = $1
  `, [personId]);
  let roleStartDate2 = null;
  try {
    const h = personResult.rows[0]?.career_history;
    const hist = Array.isArray(h) ? h : (h ? JSON.parse(h) : []);
    const cur = hist.find(r => r.current || !r.end_date) || hist[0];
    roleStartDate2 = cur?.start_date || null;
  } catch(e) {}
  const recentMove = roleStartDate2 &&
    (Date.now() - new Date(roleStartDate2)) < (1000 * 60 * 60 * 24 * 180);

  const timing = computeTimingScore(receptivity.score, activity.score, recentMove);

  // Combine all factors
  const allFactors = {
    engagement: engagement.factors,
    activity: activity.factors,
    receptivity: receptivity.factors,
    flight_risk: flightRisk.factors,
    timing: timing.factors,
    computed_at: new Date().toISOString()
  };

  // Upsert to person_scores
  await pool.query(`
    INSERT INTO person_scores (person_id, engagement_score, activity_score, 
      receptivity_score, flight_risk_score, timing_score, score_factors, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (person_id) DO UPDATE SET
      engagement_score = EXCLUDED.engagement_score,
      activity_score = EXCLUDED.activity_score,
      receptivity_score = EXCLUDED.receptivity_score,
      flight_risk_score = EXCLUDED.flight_risk_score,
      timing_score = EXCLUDED.timing_score,
      score_factors = EXCLUDED.score_factors,
      updated_at = NOW()
  `, [
    personId,
    engagement.score,
    activity.score,
    receptivity.score,
    flightRisk.score,
    timing.score,
    JSON.stringify(allFactors)
  ]);

  return {
    engagement: engagement.score,
    activity: activity.score,
    receptivity: receptivity.score,
    flight_risk: flightRisk.score,
    timing: timing.score,
    factors: allFactors
  };
}

/**
 * Main function - compute scores for all people
 */
async function main() {
  console.log('Starting score computation...');

  try {
    // Get all people that need score updates
    // (either never computed or not updated in last hour)
    const result = await pool.query(`
      SELECT p.id
      FROM people p
      LEFT JOIN person_scores ps ON p.id = ps.person_id
      WHERE ps.updated_at IS NULL 
        OR ps.updated_at < NOW() - INTERVAL '1 hour'
      ORDER BY ps.updated_at ASC NULLS FIRST
      LIMIT 100
    `);

    console.log(`Found ${result.rows.length} people to update`);

    let updated = 0;
    let errors = 0;

    for (const row of result.rows) {
      try {
        await computePersonScores(row.id);
        updated++;
      } catch (err) {
        console.error(`Error computing scores for person ${row.id}:`, err.message);
        errors++;
      }
    }

    console.log(`\n✅ Score computation complete`);
    console.log(`   Updated: ${updated}`);
    console.log(`   Errors: ${errors}`);

  } catch (err) {
    console.error('Fatal error:', err);
    process.exit(1);
  } finally {
    // pool managed by lib/db
  }
}

// Export for use in other modules
module.exports = { computePersonScores };

// Run if called directly
if (require.main === module) {
  main();
}
