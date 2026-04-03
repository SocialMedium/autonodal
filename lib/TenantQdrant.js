// ═══════════════════════════════════════════════════════════════════════════════
// lib/TenantQdrant.js — Context-Gated Vector Store Client
// ═══════════════════════════════════════════════════════════════════════════════
//
// Wraps every Qdrant operation with a mandatory tenant_id filter.
// A query that omits the tenant filter is impossible via this client.
//
// Usage:
//   const tq = new TenantQdrant(tenantId);
//   const results = await tq.search('people', vector, { limit: 20 });
//   // Always filtered to this tenant's vectors only

const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
});

// Collections that contain tenant-scoped data
const TENANT_COLLECTIONS = [
  'documents',
  'people',
  'person_content',
  'companies',
  'searches',
  'interests',
  'signal_events',
  'interactions',
  'opportunities',
  'research_notes',
];

class TenantQdrant {
  constructor(tenantId) {
    if (!tenantId) {
      throw new Error('TenantQdrant requires a tenantId.');
    }
    this.tenantId = tenantId;
  }

  /**
   * Build the mandatory tenant filter.
   * Injected into every search, scroll, and recommend operation.
   */
  _tenantFilter(additionalFilter) {
    const tenantCondition = {
      must: [
        { key: 'tenant_id', match: { value: this.tenantId } },
      ],
    };

    if (!additionalFilter) return tenantCondition;

    return {
      must: [
        ...tenantCondition.must,
        ...(additionalFilter.must || []),
      ],
      should: additionalFilter.should || undefined,
      must_not: additionalFilter.must_not || undefined,
    };
  }

  /**
   * Upsert vectors — always stamps tenant_id into payload.
   * External code cannot override the tenant_id.
   */
  async upsert(collection, points) {
    const scopedPoints = points.map(p => ({
      ...p,
      payload: {
        ...p.payload,
        tenant_id: this.tenantId, // Always overwrite
      },
    }));

    return qdrant.upsert(collection, {
      wait: true,
      points: scopedPoints,
    });
  }

  /**
   * Vector similarity search — always filtered to this tenant.
   */
  async search(collection, vector, opts = {}) {
    const { limit = 10, filter = null, withPayload = true, scoreThreshold } = opts;

    const params = {
      vector,
      limit,
      filter: this._tenantFilter(filter),
      with_payload: withPayload,
    };
    if (scoreThreshold) params.score_threshold = scoreThreshold;

    return qdrant.search(collection, params);
  }

  /**
   * Scroll (list) vectors — always filtered to this tenant.
   */
  async scroll(collection, opts = {}) {
    const { filter = null, limit = 100, withPayload = true, offset } = opts;

    return qdrant.scroll(collection, {
      filter: this._tenantFilter(filter),
      limit,
      with_payload: withPayload,
      offset,
    });
  }

  /**
   * Delete vectors by IDs — verifies ownership before deletion.
   */
  async delete(collection, ids) {
    if (!ids || ids.length === 0) return;

    // Verify all points belong to this tenant
    const existing = await qdrant.retrieve(collection, {
      ids,
      with_payload: true,
    });

    const foreign = existing.filter(p => p.payload?.tenant_id !== this.tenantId);
    if (foreign.length > 0) {
      throw new Error(
        `TENANT ISOLATION VIOLATION: Attempted to delete ${foreign.length} ` +
        `points belonging to a different tenant in collection "${collection}".`
      );
    }

    return qdrant.delete(collection, { points: ids });
  }

  /**
   * Delete vectors by filter — tenant filter always applied.
   */
  async deleteByFilter(collection, filter) {
    return qdrant.delete(collection, {
      filter: this._tenantFilter(filter),
    });
  }

  /**
   * Count vectors for this tenant in a collection.
   */
  async count(collection, filter = null) {
    const result = await qdrant.count(collection, {
      filter: this._tenantFilter(filter),
      exact: true,
    });
    return result.count;
  }
}

module.exports = { TenantQdrant, qdrant, TENANT_COLLECTIONS };
