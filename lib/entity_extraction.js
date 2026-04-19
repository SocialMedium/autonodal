// ═══════════════════════════════════════════════════════════════════════════════
// Entity Extraction — Auto-detect people and companies in artifact content
// ═══════════════════════════════════════════════════════════════════════════════
//
// Simple string-matching against the tenant's known entities.
// No LLM, no NER model — deliberately deterministic.
// False negatives acceptable; false positives mitigated by confidence scoring.

// In-memory cache per tenant (5-minute TTL)
const _entityCache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

function _getCached(tenantId) {
  const entry = _entityCache.get(tenantId);
  if (entry && Date.now() - entry.at < CACHE_TTL) return entry;
  return null;
}

const EXCLUDED_COMPANY_NAMES = new Set([
  'the', 'and', 'group', 'team', 'global', 'capital', 'digital',
  'technology', 'partners', 'consulting', 'services', 'solutions',
  'management', 'advisory', 'international', 'australia', 'singapore',
  'london', 'new york', 'search', 'talent', 'executive', 'limited',
]);

async function extractEntities(contentMarkdown, tenantId, db) {
  const results = { people: [], companies: [] };
  if (!contentMarkdown || contentMarkdown.length < 10) return results;

  // 1. Get or build entity lookup for this tenant
  let cache = _getCached(tenantId);
  if (!cache) {
    const [peopleRes, companiesRes] = await Promise.all([
      db.query(
        `SELECT id, full_name FROM people
         WHERE tenant_id = $1 AND full_name IS NOT NULL AND LENGTH(full_name) >= 5
         LIMIT 10000`,
        [tenantId]
      ),
      db.query(
        `SELECT id, name FROM companies
         WHERE tenant_id = $1 AND name IS NOT NULL AND LENGTH(name) >= 3
         LIMIT 5000`,
        [tenantId]
      ),
    ]);
    cache = {
      people: peopleRes.rows,
      companies: companiesRes.rows,
      at: Date.now(),
    };
    _entityCache.set(tenantId, cache);
  }

  // 2. Normalise content
  const contentLower = contentMarkdown.toLowerCase();

  // 3. Match people — full name match
  const seenPeople = new Set();
  for (const person of cache.people) {
    const name = person.full_name.toLowerCase();
    if (contentLower.includes(name) && !seenPeople.has(person.id)) {
      seenPeople.add(person.id);
      results.people.push({
        person_id: person.id,
        name: person.full_name,
        confidence: 0.85,
      });
    }
  }

  // 4. Match companies — name match, skip common words
  const seenCompanies = new Set();
  for (const company of cache.companies) {
    const name = company.name.toLowerCase().trim();
    if (EXCLUDED_COMPANY_NAMES.has(name)) continue;
    if (name.length < 3) continue;
    if (contentLower.includes(name) && !seenCompanies.has(company.id)) {
      seenCompanies.add(company.id);
      results.companies.push({
        company_id: company.id,
        name: company.name,
        confidence: name.length > 5 ? 0.80 : 0.60,
      });
    }
  }

  return results;
}

// Invalidate cache when entities change (call from people/company insert paths)
function invalidateEntityCache(tenantId) {
  _entityCache.delete(tenantId);
}

module.exports = { extractEntities, invalidateEntityCache };
