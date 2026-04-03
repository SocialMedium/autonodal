// =============================================================================
// lib/platform/EmbedService.js — Document Embedding Pipeline
// =============================================================================
//
// Finds external_documents that have not been embedded yet (embedded_at IS NULL,
// status = 'pending'), generates embeddings via OpenAI, upserts vectors into
// the Qdrant 'documents' collection, and marks documents as embedded.
// Processes in batches of 20 with a 1-second pause between batches.

const { BaseService } = require('./BaseService');

const BATCH_SIZE = 20;
const BATCH_DELAY_MS = 1000;

class EmbedService extends BaseService {
  /**
   * Run the embedding pipeline for this tenant.
   * @returns {Promise<{total: number, embedded: number, errors: number}>}
   */
  async run() {
    const stats = { total: 0, embedded: 0, errors: 0 };

    const docs = await this.db.queryAll(
      `SELECT id, title, content, source_type, source_url, published_at
       FROM external_documents
       WHERE embedded_at IS NULL AND status = 'pending'
       ORDER BY created_at ASC`
    );

    stats.total = docs.length;
    this.log.info(`Found ${docs.length} documents to embed`);

    for (let i = 0; i < docs.length; i += BATCH_SIZE) {
      const batch = docs.slice(i, i + BATCH_SIZE);
      await this._processBatch(batch, stats);

      // Pause between batches to respect rate limits
      if (i + BATCH_SIZE < docs.length) {
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
      }
    }

    this.log.info(`Done: ${stats.embedded} embedded, ${stats.errors} errors`);
    return stats;
  }

  /**
   * Embed and upsert a batch of documents.
   * @param {object[]} batch - Array of document rows
   * @param {object} stats - Mutable stats counter
   */
  async _processBatch(batch, stats) {
    for (const doc of batch) {
      try {
        const text = this._buildEmbedText(doc);
        if (!text || text.trim().length === 0) {
          this.log.warn(`Doc ${doc.id}: empty text, skipping`);
          stats.errors++;
          continue;
        }

        const vector = await this.embed(text);

        await this.qdrant.upsert('documents', [
          {
            id: doc.id,
            vector,
            payload: {
              title: doc.title,
              source_type: doc.source_type,
              source_url: doc.source_url,
              published_at: doc.published_at,
            },
          },
        ]);

        await this.db.query(
          `UPDATE external_documents
           SET embedded_at = NOW(), status = 'embedded'
           WHERE id = $1`,
          [doc.id]
        );

        stats.embedded++;
      } catch (err) {
        stats.errors++;
        this.log.error(`Doc ${doc.id}: ${err.message}`);

        // Mark as errored so it does not block future runs
        await this.db.query(
          `UPDATE external_documents SET status = 'embed_error' WHERE id = $1`,
          [doc.id]
        ).catch(() => {});
      }
    }
  }

  /**
   * Combine title and content into embeddable text.
   * @param {object} doc - Document row
   * @returns {string}
   */
  _buildEmbedText(doc) {
    const parts = [];
    if (doc.title) parts.push(doc.title);
    if (doc.content) parts.push(doc.content);
    return parts.join('\n\n');
  }
}

module.exports = { EmbedService };
