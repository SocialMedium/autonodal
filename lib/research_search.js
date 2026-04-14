/**
 * ResearchMedium integration — queries publications collection in shared Qdrant instance
 *
 * The publications collection contains 2M+ academic paper vectors with payloads.
 * Same Qdrant Cloud instance as Autonodal (text-embedding-3-small, 1536d, Cosine).
 *
 * This module is READ-ONLY — never writes to RM collections or database.
 * All functions are wrapped in try/catch — RM unavailability never breaks Autonodal search.
 */

const https = require('https');

const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const PUBLICATIONS_COLLECTION = 'publications';
const DEFAULT_SCORE_THRESHOLD = 0.35;
const DEFAULT_LIMIT = 10;
const TIMEOUT_MS = 8000;

// ═══════════════════════════════════════════════════════════════════════════════
// Qdrant Query Helper (dedicated to publications — isolated from tenant Qdrant)
// ═══════════════════════════════════════════════════════════════════════════════

function qdrantPublicationsSearch(vector, limit, filter = null, scoreThreshold = null) {
  return new Promise((resolve, reject) => {
    if (!QDRANT_URL || !QDRANT_API_KEY) {
      return resolve([]);
    }

    const body = { vector, limit, with_payload: true };
    if (filter) body.filter = filter;
    if (scoreThreshold) body.score_threshold = scoreThreshold;

    const url = new URL(`/collections/${PUBLICATIONS_COLLECTION}/points/search`, QDRANT_URL);
    const req = https.request({
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'api-key': QDRANT_API_KEY,
      },
      timeout: TIMEOUT_MS,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          resolve(data.result || []);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Publications Qdrant timeout')); });
    req.write(JSON.stringify(body));
    req.end();
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// Payload Mapping — normalizes RM's payload fields to a clean API response
// ═══════════════════════════════════════════════════════════════════════════════

function mapPublication(point) {
  const p = point.payload || {};

  // Normalize authors — could be string, array, or comma-separated
  let authors = p.authors || p.author || p.creator || '';
  if (Array.isArray(authors)) authors = authors.join(', ');
  const authorList = authors.split(/[,;]\s*/).filter(Boolean);
  const authorsFormatted = authorList.length > 3
    ? authorList.slice(0, 3).join(', ') + ` et al. (${authorList.length})`
    : authorList.join(', ');

  // Normalize subjects/keywords
  let subjects = p.subjects || p.keywords || p.tags || p.categories || [];
  if (typeof subjects === 'string') subjects = subjects.split(/[,;]\s*/).filter(Boolean);

  // Normalize year
  const year = p.year || (p.published_date ? parseInt(p.published_date) : null)
    || (p.date ? new Date(p.date).getFullYear() : null)
    || (p.publication_date ? new Date(p.publication_date).getFullYear() : null);

  // Normalize abstract
  const abstract = p.abstract || p.description || p.summary || '';
  const abstractTruncated = abstract.length > 300 ? abstract.substring(0, 297) + '...' : abstract;

  // DOI
  const doi = p.doi || null;
  const doiUrl = doi ? `https://doi.org/${doi.replace(/^https?:\/\/doi\.org\//, '')}` : null;

  return {
    id: point.id,
    title: p.title || 'Untitled Publication',
    authors: authorsFormatted,
    authors_full: authors,
    abstract: abstractTruncated,
    abstract_full: abstract,
    year,
    source: p.source || p.repository || p.publisher || p.journal || null,
    doi,
    doi_url: doiUrl,
    score: Math.round(point.score * 100) / 100,
    match_score: Math.round(point.score * 100),
    has_industry_coauthor: p.has_industry_coauthor || false,
    subjects: Array.isArray(subjects) ? subjects.slice(0, 10) : [],
    citation_count: p.citation_count || p.citations || null,
    url: p.url || doiUrl || null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLIC API
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Search publications collection by vector similarity
 */
async function searchPublications(queryEmbedding, options = {}) {
  try {
    const limit = options.limit || DEFAULT_LIMIT;
    const scoreThreshold = options.scoreThreshold || DEFAULT_SCORE_THRESHOLD;

    // Build optional Qdrant filter
    let filter = null;
    const mustConditions = [];

    if (options.yearFrom) {
      mustConditions.push({ key: 'year', range: { gte: parseInt(options.yearFrom) } });
    }
    if (options.yearTo) {
      mustConditions.push({ key: 'year', range: { lte: parseInt(options.yearTo) } });
    }
    if (options.industryCoauthorOnly) {
      mustConditions.push({ key: 'has_industry_coauthor', match: { value: true } });
    }

    if (mustConditions.length > 0) {
      filter = { must: mustConditions };
    }

    const raw = await qdrantPublicationsSearch(queryEmbedding, limit, filter, scoreThreshold);
    return raw.map(mapPublication);
  } catch (err) {
    console.error('[ResearchSearch] Publications search failed:', err.message);
    return [];
  }
}

/**
 * Compute research momentum — is this topic accelerating or declining?
 */
function computeResearchMomentum(publications) {
  if (!publications || publications.length < 5) return null;

  const currentYear = new Date().getFullYear();
  const recent = publications.filter(p => p.year && p.year >= currentYear - 2).length;
  const older = publications.filter(p => p.year && p.year < currentYear - 2).length;

  if (older === 0) return { label: 'emerging', icon: 'sparkles', color: '#8B5CF6' };

  const ratio = recent / older;
  if (ratio > 2) return { label: 'accelerating', icon: 'trending-up', color: '#10B981' };
  if (ratio > 1) return { label: 'growing', icon: 'bar-chart-2', color: '#3B82F6' };
  if (ratio > 0.5) return { label: 'steady', icon: 'arrow-right', color: '#64748B' };
  return { label: 'declining', icon: 'trending-down', color: '#F59E0B' };
}

/**
 * Get research trends — paper count by year for a query
 */
async function getResearchTrends(queryEmbedding, options = {}) {
  try {
    const raw = await qdrantPublicationsSearch(
      queryEmbedding,
      options.limit || 100,
      null,
      options.scoreThreshold || 0.30
    );

    const yearCounts = {};
    for (const point of raw) {
      const p = point.payload || {};
      const year = p.year || (p.published_date ? parseInt(p.published_date) : null);
      if (year && year > 1950 && year <= new Date().getFullYear() + 1) {
        yearCounts[year] = (yearCounts[year] || 0) + 1;
      }
    }

    return Object.entries(yearCounts)
      .map(([year, count]) => ({ year: parseInt(year), count }))
      .sort((a, b) => a.year - b.year);
  } catch (err) {
    console.error('[ResearchSearch] Trends query failed:', err.message);
    return [];
  }
}

module.exports = {
  searchPublications,
  computeResearchMomentum,
  getResearchTrends,
  PUBLICATIONS_COLLECTION,
};
