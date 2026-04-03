// =============================================================================
// lib/platform/ProximityEngine.js — Network Graph / Team Proximity Builder
// =============================================================================
//
// Maps team members (users) to people through their interactions.
// Computes a strength_score based on recency, interaction depth, and
// response rate. Upserts results to the team_proximity table.

const { BaseService } = require('./BaseService');

class ProximityEngine extends BaseService {
  /**
   * Build or refresh the team proximity graph for this tenant.
   * @returns {Promise<{pairs: number, updated: number, errors: number}>}
   */
  async run() {
    const stats = { pairs: 0, updated: 0, errors: 0 };

    // Get all active team members (users) for this tenant
    const users = await this.db.queryAll(
      `SELECT id, name, email FROM users WHERE active = true`
    );
    this.log.info(`Building proximity graph for ${users.length} team members`);

    for (const user of users) {
      try {
        const count = await this._buildUserProximity(user);
        stats.updated += count;
      } catch (err) {
        stats.errors++;
        this.log.error(`User ${user.id}: ${err.message}`);
      }
    }

    stats.pairs = stats.updated;
    this.log.info(`Done: ${stats.updated} pairs updated, ${stats.errors} errors`);
    return stats;
  }

  /**
   * Compute proximity for every person a user has interacted with.
   * @param {object} user - User row
   * @returns {Promise<number>} Number of proximity records upserted
   */
  async _buildUserProximity(user) {
    // Find distinct people this user has interacted with
    const people = await this.db.queryAll(
      `SELECT DISTINCT person_id
       FROM interactions
       WHERE user_id = $1 AND person_id IS NOT NULL`,
      [user.id]
    );

    let count = 0;

    for (const { person_id } of people) {
      try {
        const score = await this._computeStrength(user.id, person_id);

        await this.db.upsert(
          'team_proximity',
          {
            tenant_id: this.tenantId,
            user_id: user.id,
            person_id,
            strength_score: score.strength,
            interaction_count: score.totalInteractions,
            last_interaction_at: score.lastInteraction,
            updated_at: new Date(),
          },
          ['user_id', 'person_id'],
          ['strength_score', 'interaction_count', 'last_interaction_at', 'updated_at']
        );
        count++;
      } catch (err) {
        this.log.warn(`Proximity error (user=${user.id}, person=${person_id}): ${err.message}`);
      }
    }

    return count;
  }

  /**
   * Compute the strength score for a user-person pair.
   * Factors: recency, depth (interaction count), response rate.
   * @param {string} userId
   * @param {string} personId
   * @returns {Promise<{strength: number, totalInteractions: number, lastInteraction: Date|null}>}
   */
  async _computeStrength(userId, personId) {
    const interactions = await this.db.queryAll(
      `SELECT type, direction, created_at, response_time_hours
       FROM interactions
       WHERE user_id = $1 AND person_id = $2
       ORDER BY created_at DESC
       LIMIT 200`,
      [userId, personId]
    );

    if (interactions.length === 0) {
      return { strength: 0, totalInteractions: 0, lastInteraction: null };
    }

    const lastInteraction = new Date(interactions[0].created_at);
    const totalInteractions = interactions.length;

    // Recency score: how recently the last interaction occurred (0-1)
    const daysSinceLast = (Date.now() - lastInteraction.getTime()) / (1000 * 60 * 60 * 24);
    const recency = Math.max(0, 1 - daysSinceLast / 180);

    // Depth score: interaction volume over last 90 days (0-1)
    const recentCount = interactions.filter(i => {
      const days = (Date.now() - new Date(i.created_at).getTime()) / (1000 * 60 * 60 * 24);
      return days <= 90;
    }).length;
    const depth = Math.min(recentCount / 15, 1);

    // Response rate: how often the person responds (0-1)
    const outbound = interactions.filter(i => i.direction === 'outbound');
    const responded = outbound.filter(i => i.response_time_hours != null);
    const responseRate = outbound.length > 0
      ? responded.length / outbound.length
      : 0.5;

    const strength = Math.min(
      recency * 0.4 + depth * 0.35 + responseRate * 0.25,
      1
    );

    return { strength, totalInteractions, lastInteraction };
  }
}

module.exports = { ProximityEngine };
