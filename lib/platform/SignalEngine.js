// =============================================================================
// lib/platform/SignalEngine.js — Signal Detection from Embedded Documents
// =============================================================================
//
// Scans embedded documents that have not yet been processed for signals.
// Uses signal_keywords.js detectSignals() for pattern matching, resolves
// company names to the companies table, and inserts deduplicated signal_events.

const { BaseService } = require('./BaseService');
const { detectSignals, extractCompanyNames } = require('../signal_keywords');

class SignalEngine extends BaseService {
  /**
   * Run signal detection for this tenant.
   * @returns {Promise<{scanned: number, signals: number, errors: number}>}
   */
  async run() {
    const stats = { scanned: 0, signals: 0, errors: 0 };

    const docs = await this.db.queryAll(
      `SELECT id, title, content, published_at, source_url
       FROM external_documents
       WHERE status = 'embedded'
       ORDER BY published_at DESC NULLS LAST`
    );

    this.log.info(`Found ${docs.length} embedded documents to scan`);

    for (const doc of docs) {
      try {
        await this._processDocument(doc, stats);
        stats.scanned++;

        await this.db.query(
          `UPDATE external_documents SET status = 'processed' WHERE id = $1`,
          [doc.id]
        );
      } catch (err) {
        stats.errors++;
        this.log.error(`Doc ${doc.id}: ${err.message}`);
      }
    }

    this.log.info(
      `Done: ${stats.scanned} scanned, ${stats.signals} signals, ${stats.errors} errors`
    );
    return stats;
  }

  /**
   * Detect signals in a single document and persist them.
   * @param {object} doc - Document row
   * @param {object} stats - Mutable stats counter
   */
  async _processDocument(doc, stats) {
    const text = [doc.title, doc.content].filter(Boolean).join('\n\n');
    if (!text.trim()) return;

    const signals = detectSignals(text);
    if (signals.length === 0) return;

    const companyNames = extractCompanyNames(text);
    const companies = await this._resolveCompanies(companyNames);

    const signalDate = doc.published_at
      ? new Date(doc.published_at).toISOString().slice(0, 10)
      : new Date().toISOString().slice(0, 10);

    for (const signal of signals) {
      for (const company of companies) {
        try {
          // Deduplicate: same company + signal type + date
          const existing = await this.db.queryOne(
            `SELECT id FROM signal_events
             WHERE company_id = $1 AND signal_type = $2 AND signal_date = $3`,
            [company.id, signal.type, signalDate]
          );
          if (existing) continue;

          await this.db.insert('signal_events', {
            tenant_id: this.tenantId,
            company_id: company.id,
            document_id: doc.id,
            signal_type: signal.type,
            signal_date: signalDate,
            confidence: signal.confidence,
            evidence: JSON.stringify(signal.evidence || []),
            source_url: doc.source_url,
            created_at: new Date(),
          });
          stats.signals++;
        } catch (sigErr) {
          this.log.warn(
            `Signal insert error (company=${company.id}, type=${signal.type}): ${sigErr.message}`
          );
        }
      }
    }
  }

  /**
   * Resolve company names to rows in the companies table (find or create).
   * @param {string[]} names - Extracted company names
   * @returns {Promise<object[]>} Array of company rows with at least { id, name }
   */
  async _resolveCompanies(names) {
    const companies = [];

    for (const name of names) {
      if (!name || name.length < 2) continue;

      try {
        // Try exact match first
        let company = await this.db.queryOne(
          `SELECT id, name FROM companies WHERE LOWER(name) = LOWER($1)`,
          [name]
        );

        // Create if not found
        if (!company) {
          const result = await this.db.insert('companies', {
            tenant_id: this.tenantId,
            name: name.trim(),
            created_at: new Date(),
          });
          company = result.rows[0];
        }

        if (company) companies.push(company);
      } catch (err) {
        this.log.warn(`Company resolve error for "${name}": ${err.message}`);
      }
    }

    return companies;
  }
}

module.exports = { SignalEngine };
