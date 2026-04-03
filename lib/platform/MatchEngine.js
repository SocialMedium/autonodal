// =============================================================================
// lib/platform/MatchEngine.js — Vector-Based Opportunity Matching
// =============================================================================
//
// Finds active opportunities for the tenant, embeds their brief text, and
// searches the 'people' Qdrant collection for the closest vector matches.
// Inserts or updates search_matches with the vector_score.

const { BaseService } = require('./BaseService');

const DEFAULT_MATCH_LIMIT = 50;
const MIN_SCORE_THRESHOLD = 0.3;

class MatchEngine extends BaseService {
  /**
   * Run vector matching for all active opportunities in this tenant.
   * @returns {Promise<{opportunities: number, matches: number, errors: number}>}
   */
  async run() {
    const stats = { opportunities: 0, matches: 0, errors: 0 };

    const opportunities = await this.db.queryAll(
      `SELECT id, title, brief, requirements, status
       FROM opportunities
       WHERE status = 'active'
       ORDER BY updated_at DESC`
    );

    stats.opportunities = opportunities.length;
    this.log.info(`Matching against ${opportunities.length} active opportunities`);

    for (const opp of opportunities) {
      try {
        const matchCount = await this._matchOpportunity(opp);
        stats.matches += matchCount;
      } catch (err) {
        stats.errors++;
        this.log.error(`Opportunity ${opp.id}: ${err.message}`);
      }
    }

    this.log.info(
      `Done: ${stats.matches} matches across ${stats.opportunities} opportunities, ${stats.errors} errors`
    );
    return stats;
  }

  /**
   * Embed an opportunity and find matching people.
   * @param {object} opp - Opportunity row
   * @returns {Promise<number>} Number of matches upserted
   */
  async _matchOpportunity(opp) {
    const briefText = this._buildBriefText(opp);
    if (!briefText || briefText.trim().length === 0) {
      this.log.warn(`Opportunity ${opp.id}: no brief text, skipping`);
      return 0;
    }

    const vector = await this.embed(briefText);

    const results = await this.qdrant.search('people', vector, {
      limit: DEFAULT_MATCH_LIMIT,
      scoreThreshold: MIN_SCORE_THRESHOLD,
    });

    let matchCount = 0;

    for (const result of results) {
      try {
        const personId = result.payload?.person_id || result.id;

        await this.db.upsert(
          'search_matches',
          {
            tenant_id: this.tenantId,
            opportunity_id: opp.id,
            person_id: personId,
            vector_score: result.score,
            matched_at: new Date(),
          },
          ['opportunity_id', 'person_id'],
          ['vector_score', 'matched_at']
        );
        matchCount++;
      } catch (err) {
        this.log.warn(
          `Match upsert error (opp=${opp.id}, person=${result.id}): ${err.message}`
        );
      }
    }

    this.log.info(`Opportunity "${opp.title}": ${matchCount} matches`);
    return matchCount;
  }

  /**
   * Build embeddable text from opportunity fields.
   * @param {object} opp - Opportunity row
   * @returns {string}
   */
  _buildBriefText(opp) {
    const parts = [];
    if (opp.title) parts.push(opp.title);
    if (opp.brief) parts.push(opp.brief);
    if (opp.requirements) parts.push(opp.requirements);
    return parts.join('\n\n');
  }
}

module.exports = { MatchEngine };
