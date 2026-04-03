// =============================================================================
// lib/platform/TriangulateEngine.js — Cross-Signal Pattern Detection
// =============================================================================
//
// Finds companies with multiple signal types in a 30-day window and
// detects compound patterns (e.g., funding + hiring, leadership change +
// restructuring). Creates triangulated signal_events with is_triangulated flag.

const { BaseService } = require('./BaseService');

/**
 * Compound pattern definitions.
 * Each pattern maps a name to a set of signal types that, when co-occurring,
 * indicate a stronger or more actionable signal.
 */
const COMPOUND_PATTERNS = {
  growth_surge: {
    label: 'Growth Surge',
    requires: ['capital_raising', 'hiring_spree'],
    minTypes: 2,
    confidenceBoost: 0.15,
  },
  leadership_transition: {
    label: 'Leadership Transition',
    requires: ['leadership_change', 'restructuring'],
    minTypes: 2,
    confidenceBoost: 0.12,
  },
  expansion_play: {
    label: 'Expansion Play',
    requires: ['geographic_expansion', 'hiring_spree'],
    minTypes: 2,
    confidenceBoost: 0.10,
  },
  strategic_pivot: {
    label: 'Strategic Pivot',
    requires: ['restructuring', 'product_launch'],
    minTypes: 2,
    confidenceBoost: 0.10,
  },
  acquisition_signal: {
    label: 'Acquisition Signal',
    requires: ['acquisition', 'leadership_change'],
    minTypes: 2,
    confidenceBoost: 0.18,
  },
  distress_indicator: {
    label: 'Distress Indicator',
    requires: ['layoffs', 'restructuring'],
    minTypes: 2,
    confidenceBoost: 0.14,
  },
  market_entry: {
    label: 'Market Entry',
    requires: ['capital_raising', 'geographic_expansion'],
    minTypes: 2,
    confidenceBoost: 0.12,
  },
};

const WINDOW_DAYS = 30;

class TriangulateEngine extends BaseService {
  /**
   * Run cross-signal triangulation for this tenant.
   * @returns {Promise<{companies: number, patterns: number, errors: number}>}
   */
  async run() {
    const stats = { companies: 0, patterns: 0, errors: 0 };

    // Find companies with 2+ distinct signal types in the window
    const candidates = await this.db.queryAll(
      `SELECT company_id, ARRAY_AGG(DISTINCT signal_type) AS signal_types,
              MAX(confidence) AS max_confidence
       FROM signal_events
       WHERE signal_date >= CURRENT_DATE - INTERVAL '${WINDOW_DAYS} days'
         AND (is_triangulated IS NULL OR is_triangulated = false)
       GROUP BY company_id
       HAVING COUNT(DISTINCT signal_type) >= 2`
    );

    this.log.info(`Found ${candidates.length} companies with multi-signal activity`);

    for (const candidate of candidates) {
      try {
        const detected = await this._detectPatterns(candidate);
        stats.patterns += detected;
        if (detected > 0) stats.companies++;
      } catch (err) {
        stats.errors++;
        this.log.error(`Company ${candidate.company_id}: ${err.message}`);
      }
    }

    this.log.info(
      `Done: ${stats.patterns} patterns across ${stats.companies} companies, ${stats.errors} errors`
    );
    return stats;
  }

  /**
   * Check a candidate company against all compound patterns.
   * @param {object} candidate - { company_id, signal_types, max_confidence }
   * @returns {Promise<number>} Number of patterns inserted
   */
  async _detectPatterns(candidate) {
    const { company_id, signal_types, max_confidence } = candidate;
    let inserted = 0;

    for (const [patternKey, pattern] of Object.entries(COMPOUND_PATTERNS)) {
      const matchingTypes = pattern.requires.filter(t => signal_types.includes(t));
      if (matchingTypes.length < pattern.minTypes) continue;

      try {
        // Deduplicate: same company + pattern within the window
        const existing = await this.db.queryOne(
          `SELECT id FROM signal_events
           WHERE company_id = $1
             AND signal_type = $2
             AND is_triangulated = true
             AND signal_date >= CURRENT_DATE - INTERVAL '${WINDOW_DAYS} days'`,
          [company_id, patternKey]
        );
        if (existing) continue;

        const confidence = Math.min(
          (max_confidence || 0.5) + pattern.confidenceBoost,
          1.0
        );

        await this.db.insert('signal_events', {
          tenant_id: this.tenantId,
          company_id,
          signal_type: patternKey,
          signal_date: new Date().toISOString().slice(0, 10),
          confidence,
          is_triangulated: true,
          evidence: JSON.stringify({
            pattern_label: pattern.label,
            constituent_signals: matchingTypes,
          }),
          created_at: new Date(),
        });

        this.log.info(
          `Pattern "${pattern.label}" detected for company ${company_id} ` +
          `(signals: ${matchingTypes.join(', ')})`
        );
        inserted++;
      } catch (err) {
        this.log.warn(`Pattern insert error (${patternKey}): ${err.message}`);
      }
    }

    return inserted;
  }
}

module.exports = { TriangulateEngine };
