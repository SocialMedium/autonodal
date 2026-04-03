// =============================================================================
// lib/platform/BaseService.js — Tenant-Aware Base Service
// =============================================================================
//
// All platform services extend this class. It provides:
//   - TenantDB for RLS-gated Postgres queries
//   - TenantQdrant for tenant-scoped vector operations
//   - OpenAI client with a convenience embed() method
//   - Prefixed logger
//
// Usage:
//   class MyService extends BaseService {
//     async run() { ... }
//   }

const { TenantDB } = require('../TenantDB');
const { TenantQdrant } = require('../TenantQdrant');
const OpenAI = require('openai');

const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBEDDING_DIMENSIONS = 1536;
const MAX_EMBED_CHARS = 8191 * 4; // ~8191 tokens at ~4 chars/token

class BaseService {
  /**
   * @param {string} tenantId - UUID of the tenant
   */
  constructor(tenantId) {
    if (!tenantId) {
      throw new Error(`${this.constructor.name} requires a tenantId.`);
    }
    this.tenantId = tenantId;
    this.db = new TenantDB(tenantId);
    this.qdrant = new TenantQdrant(tenantId);
    this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    const prefix = `[${this.constructor.name}:${tenantId.slice(0, 8)}]`;
    this.log = {
      info:  (...args) => console.log(prefix, ...args),
      warn:  (...args) => console.warn(prefix, ...args),
      error: (...args) => console.error(prefix, ...args),
    };
  }

  /**
   * Generate an embedding vector for a text string.
   * Truncates to stay within the model token limit.
   * @param {string} text - Text to embed
   * @returns {Promise<number[]>} 1536-dim embedding vector
   */
  async embed(text) {
    if (!text || text.trim().length === 0) {
      throw new Error('Cannot embed empty text');
    }
    const truncated = text.length > MAX_EMBED_CHARS
      ? text.slice(0, MAX_EMBED_CHARS)
      : text;

    const res = await this.openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: truncated,
      dimensions: EMBEDDING_DIMENSIONS,
    });
    return res.data[0].embedding;
  }
}

module.exports = { BaseService, EMBEDDING_MODEL, EMBEDDING_DIMENSIONS };
