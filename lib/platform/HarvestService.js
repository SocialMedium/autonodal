// =============================================================================
// lib/platform/HarvestService.js — RSS / Atom / Podcast Feed Harvester
// =============================================================================
//
// Fetches all enabled rss_sources for the tenant, parses each feed, and
// stores new items in external_documents. Deduplicates by source_url_hash.
// Handles YouTube Atom feeds, podcast enclosures, and standard RSS.

const crypto = require('crypto');
const Parser = require('rss-parser');
const { BaseService } = require('./BaseService');

const parser = new Parser({
  timeout: 15000,
  headers: { 'User-Agent': 'MLXIntelligence/1.0' },
  customFields: {
    item: [
      ['yt:videoId', 'ytVideoId'],
      ['media:group', 'mediaGroup'],
      ['media:thumbnail', 'mediaThumbnail'],
      ['itunes:image', 'itunesImage'],
    ],
  },
});

class HarvestService extends BaseService {
  /**
   * Run the harvest cycle for this tenant.
   * @returns {Promise<{sources: number, newDocs: number, errors: number}>}
   */
  async run() {
    const stats = { sources: 0, newDocs: 0, errors: 0 };

    const sources = await this.db.queryAll(
      `SELECT * FROM rss_sources WHERE enabled = true`
    );
    stats.sources = sources.length;
    this.log.info(`Fetching ${sources.length} enabled sources`);

    for (const source of sources) {
      try {
        const count = await this._harvestSource(source);
        stats.newDocs += count;

        await this.db.query(
          `UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = 0
           WHERE id = $1`,
          [source.id]
        );
      } catch (err) {
        stats.errors++;
        this.log.error(`Source ${source.id} (${source.name}): ${err.message}`);

        await this.db.query(
          `UPDATE rss_sources
           SET consecutive_errors = COALESCE(consecutive_errors, 0) + 1,
               last_error = $2, last_error_at = NOW()
           WHERE id = $1`,
          [source.id, err.message.slice(0, 500)]
        );
      }
    }

    this.log.info(`Done: ${stats.newDocs} new docs, ${stats.errors} errors`);
    return stats;
  }

  /**
   * Harvest a single RSS source and store new items.
   * @param {object} source - Row from rss_sources
   * @returns {Promise<number>} Number of new documents inserted
   */
  async _harvestSource(source) {
    const feed = await parser.parseURL(source.feed_url);
    let inserted = 0;

    for (const item of (feed.items || [])) {
      try {
        const url = item.link || item.guid;
        if (!url) continue;

        const urlHash = crypto.createHash('md5').update(url).digest('hex');

        // Deduplicate by hash
        const existing = await this.db.queryOne(
          `SELECT id FROM external_documents WHERE source_url_hash = $1`,
          [urlHash]
        );
        if (existing) continue;

        const sourceType = this._classifySourceType(source, item);
        const doc = this._buildDocument(source, item, urlHash, sourceType);

        await this.db.insert('external_documents', doc);
        inserted++;
      } catch (itemErr) {
        this.log.warn(`Item error in source ${source.id}: ${itemErr.message}`);
      }
    }

    this.log.info(`Source "${source.name}": ${inserted} new items`);
    return inserted;
  }

  /**
   * Classify the source type based on feed content.
   */
  _classifySourceType(source, item) {
    if (item.ytVideoId || (source.feed_url && source.feed_url.includes('youtube.com'))) {
      return 'youtube';
    }
    if (item.enclosure && item.enclosure.type && item.enclosure.type.startsWith('audio/')) {
      return 'podcast';
    }
    return source.source_type || 'rss';
  }

  /**
   * Build a document row from a feed item.
   */
  _buildDocument(source, item, urlHash, sourceType) {
    const doc = {
      tenant_id: this.tenantId,
      source_id: source.id,
      source_url: item.link || item.guid,
      source_url_hash: urlHash,
      title: (item.title || '').slice(0, 500),
      content: item.contentSnippet || item.content || item.summary || '',
      author: item.creator || item.author || null,
      published_at: item.isoDate || item.pubDate || null,
      source_type: sourceType,
      status: 'pending',
      created_at: new Date(),
    };

    // YouTube-specific fields
    if (sourceType === 'youtube') {
      doc.metadata = JSON.stringify({
        video_id: item.ytVideoId || null,
        thumbnail: this._extractThumbnail(item),
      });
    }

    // Podcast-specific fields
    if (sourceType === 'podcast') {
      const enclosure = item.enclosure || {};
      doc.metadata = JSON.stringify({
        audio_url: enclosure.url || null,
        audio_type: enclosure.type || null,
        duration: item.itunes?.duration || null,
        image: item.itunesImage?.href || item.itunesImage || null,
      });
    }

    return doc;
  }

  /**
   * Extract thumbnail URL from a YouTube item.
   */
  _extractThumbnail(item) {
    if (item.mediaThumbnail && item.mediaThumbnail.$) {
      return item.mediaThumbnail.$.url;
    }
    if (item.mediaGroup && item.mediaGroup['media:thumbnail']) {
      const thumb = item.mediaGroup['media:thumbnail'];
      return thumb.$ ? thumb.$.url : null;
    }
    if (item.ytVideoId) {
      return `https://img.youtube.com/vi/${item.ytVideoId}/hqdefault.jpg`;
    }
    return null;
  }
}

module.exports = { HarvestService };
