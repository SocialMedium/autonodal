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
const PEOPLE_BATCH_SIZE = 50;
const BATCH_DELAY_MS = 1000;

class EmbedService extends BaseService {
  /**
   * Run the embedding pipeline for this tenant.
   * Embeds both documents and people that haven't been embedded yet.
   * @returns {Promise<{total: number, embedded: number, errors: number}>}
   */
  async run() {
    const stats = { total: 0, embedded: 0, errors: 0 };

    // Phase 1: Documents
    await this._embedDocuments(stats);

    // Phase 2: People (unemebedded or stale > 7 days)
    await this._embedPeople(stats);

    this.log.info(`Done: ${stats.embedded} embedded, ${stats.errors} errors`);
    return stats;
  }

  async _embedDocuments(stats) {
    const docs = await this.db.queryAll(
      `SELECT id, title, content, source_type, source_url, published_at
       FROM external_documents
       WHERE embedded_at IS NULL AND status = 'pending'
       ORDER BY created_at ASC`
    );

    stats.total += docs.length;
    this.log.info(`Found ${docs.length} documents to embed`);

    for (let i = 0; i < docs.length; i += BATCH_SIZE) {
      const batch = docs.slice(i, i + BATCH_SIZE);
      await this._processDocBatch(batch, stats);

      if (i + BATCH_SIZE < docs.length) {
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
      }
    }
  }

  async _embedPeople(stats) {
    const people = await this.db.queryAll(
      `SELECT id, full_name, first_name, last_name, headline, bio,
              current_title, current_company_name, location, tenant_id,
              expertise_tags, career_history,
              is_investor, investor_type, investor_stage_focus, investor_sector_focus, investor_geo_focus
       FROM people
       WHERE full_name IS NOT NULL AND full_name != ''
         AND (embedded_at IS NULL OR embedded_at < NOW() - INTERVAL '7 days')
       ORDER BY embedded_at ASC NULLS FIRST
       LIMIT 500`
    );

    if (people.length === 0) return;

    stats.total += people.length;
    this.log.info(`Found ${people.length} people to embed`);

    for (let i = 0; i < people.length; i += PEOPLE_BATCH_SIZE) {
      const batch = people.slice(i, i + PEOPLE_BATCH_SIZE);
      await this._processPeopleBatch(batch, stats);

      if (i + PEOPLE_BATCH_SIZE < people.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
  }

  /**
   * Embed and upsert a batch of documents.
   */
  async _processDocBatch(batch, stats) {
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
   * Embed and upsert a batch of people.
   */
  async _processPeopleBatch(batch, stats) {
    try {
      const texts = batch.map(p => this._buildPersonText(p));
      const vectors = [];
      for (const text of texts) {
        vectors.push(await this.embed(text));
      }

      const points = batch.map((person, idx) => ({
        id: person.id,
        vector: vectors[idx],
        payload: {
          type: 'person',
          person_id: person.id,
          name: person.full_name,
          full_name: person.full_name,
          title: person.current_title,
          current_title: person.current_title,
          company: person.current_company_name,
          location: person.location,
          seniority: null,
          content_preview: [person.current_title, person.current_company_name, person.location].filter(Boolean).join(' · '),
          is_investor: person.is_investor || false,
          investor_type: person.investor_type || null,
        },
      }));

      await this.qdrant.upsert('people', points);

      const ids = batch.map(p => p.id);
      await this.db.query(
        `UPDATE people SET embedded_at = NOW() WHERE id = ANY($1::uuid[])`,
        [ids]
      );

      stats.embedded += batch.length;
      this.log.info(`People batch: ${batch.length} embedded`);
    } catch (err) {
      stats.errors += batch.length;
      this.log.error(`People batch error: ${err.message}`);
    }
  }

  /**
   * Build embeddable text for a person record.
   */
  _buildPersonText(person) {
    const parts = [];
    if (person.full_name) parts.push(`Name: ${person.full_name}`);
    if (person.current_title) parts.push(`Title: ${person.current_title}`);
    if (person.current_company_name) parts.push(`Company: ${person.current_company_name}`);
    if (person.headline) parts.push(`Headline: ${person.headline}`);
    if (person.bio) parts.push(`Bio: ${person.bio}`);
    if (person.location) parts.push(`Location: ${person.location}`);
    if (person.expertise_tags && person.expertise_tags.length) {
      parts.push(`Expertise: ${person.expertise_tags.join(', ')}`);
    }
    let career = person.career_history;
    if (typeof career === 'string') try { career = JSON.parse(career); } catch (e) { career = null; }
    if (Array.isArray(career) && career.length) {
      parts.push(`Career: ${career.slice(0, 5).map(j => `${j.title} at ${j.company}`).join('; ')}`);
    }
    if (person.is_investor) {
      const inv = ['Investor'];
      if (person.investor_type) inv.push(`Type: ${person.investor_type}`);
      if (person.investor_stage_focus) inv.push(`Stage: ${person.investor_stage_focus}`);
      if (person.investor_sector_focus) inv.push(`Sectors: ${person.investor_sector_focus}`);
      if (person.investor_geo_focus) inv.push(`Geography: ${person.investor_geo_focus}`);
      parts.push(inv.join(', '));
    }
    return parts.join('\n');
  }

  /**
   * Combine title and content into embeddable text.
   */
  _buildEmbedText(doc) {
    const parts = [];
    if (doc.title) parts.push(doc.title);
    if (doc.content) parts.push(doc.content);
    return parts.join('\n\n');
  }
}

module.exports = { EmbedService };
