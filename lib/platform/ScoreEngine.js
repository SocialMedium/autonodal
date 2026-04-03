// =============================================================================
// lib/platform/ScoreEngine.js — People Scoring Engine
// =============================================================================
//
// Computes composite scores for people who have not been scored recently.
// Dimensions: engagement, activity, receptivity, timing, relationship_strength.
// Sources: interactions, person_signals, team_proximity tables.
// Upserts results to the person_scores table.

const { BaseService } = require('./BaseService');

const SCORE_STALE_HOURS = 24;

class ScoreEngine extends BaseService {
  /**
   * Run scoring for all stale or un-scored people in this tenant.
   * @returns {Promise<{scored: number, skipped: number, errors: number}>}
   */
  async run() {
    const stats = { scored: 0, skipped: 0, errors: 0 };

    const people = await this.db.queryAll(
      `SELECT p.id, p.name
       FROM people p
       LEFT JOIN person_scores ps ON ps.person_id = p.id
       WHERE ps.id IS NULL
          OR ps.scored_at < NOW() - INTERVAL '${SCORE_STALE_HOURS} hours'
       ORDER BY p.updated_at DESC NULLS LAST`
    );

    this.log.info(`Scoring ${people.length} people`);

    for (const person of people) {
      try {
        const scores = await this._computeScores(person.id);
        if (!scores) {
          stats.skipped++;
          continue;
        }

        await this.db.upsert(
          'person_scores',
          {
            tenant_id: this.tenantId,
            person_id: person.id,
            engagement: scores.engagement,
            activity: scores.activity,
            receptivity: scores.receptivity,
            timing: scores.timing,
            relationship_strength: scores.relationship_strength,
            composite: scores.composite,
            scored_at: new Date(),
          },
          ['person_id'],
          ['engagement', 'activity', 'receptivity', 'timing',
           'relationship_strength', 'composite', 'scored_at']
        );
        stats.scored++;
      } catch (err) {
        stats.errors++;
        this.log.error(`Person ${person.id}: ${err.message}`);
      }
    }

    this.log.info(`Done: ${stats.scored} scored, ${stats.errors} errors`);
    return stats;
  }

  /**
   * Compute all score dimensions for one person.
   * @param {string} personId
   * @returns {Promise<object|null>} Score dimensions or null if insufficient data
   */
  async _computeScores(personId) {
    const [interactions, signals, proximity] = await Promise.all([
      this._getInteractions(personId),
      this._getSignals(personId),
      this._getProximity(personId),
    ]);

    const engagement = this._scoreEngagement(interactions);
    const activity = this._scoreActivity(signals);
    const receptivity = this._scoreReceptivity(interactions);
    const timing = this._scoreTiming(signals);
    const relationship_strength = this._scoreRelationship(proximity);

    const composite = (
      engagement * 0.25 +
      activity * 0.20 +
      receptivity * 0.20 +
      timing * 0.15 +
      relationship_strength * 0.20
    );

    return { engagement, activity, receptivity, timing, relationship_strength, composite };
  }

  /** Fetch recent interactions for a person. */
  async _getInteractions(personId) {
    return this.db.queryAll(
      `SELECT type, direction, created_at, response_time_hours
       FROM interactions
       WHERE person_id = $1
       ORDER BY created_at DESC
       LIMIT 100`,
      [personId]
    );
  }

  /** Fetch person-level signals. */
  async _getSignals(personId) {
    return this.db.queryAll(
      `SELECT signal_type, confidence, detected_at
       FROM person_signals
       WHERE person_id = $1
       ORDER BY detected_at DESC
       LIMIT 50`,
      [personId]
    );
  }

  /** Fetch team proximity records. */
  async _getProximity(personId) {
    return this.db.queryAll(
      `SELECT user_id, strength_score, last_interaction_at
       FROM team_proximity
       WHERE person_id = $1`,
      [personId]
    );
  }

  /**
   * Engagement: frequency and recency of interactions (0-1).
   */
  _scoreEngagement(interactions) {
    if (interactions.length === 0) return 0;
    const now = Date.now();
    const recencyDays = interactions.map(i =>
      (now - new Date(i.created_at).getTime()) / (1000 * 60 * 60 * 24)
    );
    const recentCount = recencyDays.filter(d => d <= 30).length;
    const frequencyScore = Math.min(recentCount / 10, 1);
    const newestDays = Math.min(...recencyDays);
    const recencyScore = Math.max(0, 1 - newestDays / 90);
    return (frequencyScore * 0.6 + recencyScore * 0.4);
  }

  /**
   * Activity: volume and freshness of signals (0-1).
   */
  _scoreActivity(signals) {
    if (signals.length === 0) return 0;
    const highConf = signals.filter(s => s.confidence >= 0.6).length;
    return Math.min(highConf / 5, 1);
  }

  /**
   * Receptivity: response rates and response times (0-1).
   */
  _scoreReceptivity(interactions) {
    const inbound = interactions.filter(i => i.direction === 'inbound');
    if (inbound.length === 0) return 0.5; // neutral if no data
    const withResponse = inbound.filter(i => i.response_time_hours != null);
    const responseRate = withResponse.length / inbound.length;
    const avgTime = withResponse.length > 0
      ? withResponse.reduce((s, i) => s + i.response_time_hours, 0) / withResponse.length
      : 48;
    const speedScore = Math.max(0, 1 - avgTime / 72);
    return (responseRate * 0.5 + speedScore * 0.5);
  }

  /**
   * Timing: how recently signals appeared (0-1).
   */
  _scoreTiming(signals) {
    if (signals.length === 0) return 0;
    const now = Date.now();
    const daysAgo = signals.map(s =>
      (now - new Date(s.detected_at).getTime()) / (1000 * 60 * 60 * 24)
    );
    const newest = Math.min(...daysAgo);
    return Math.max(0, 1 - newest / 60);
  }

  /**
   * Relationship strength: aggregate team proximity (0-1).
   */
  _scoreRelationship(proximity) {
    if (proximity.length === 0) return 0;
    const maxStrength = Math.max(...proximity.map(p => p.strength_score || 0));
    return Math.min(maxStrength, 1);
  }
}

module.exports = { ScoreEngine };
