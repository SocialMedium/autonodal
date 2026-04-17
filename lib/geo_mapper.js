// ═══════════════════════════════════════════════════════════════════════════════
// GEO MAPPER — Resolves person/company location to region code
// PIPELINE-CONTEXT: Uses pool.query intentionally — reads geo_priorities
// configuration table (platform-level, not tenant-scoped).
// Used by compute_network_topology.js and compute_triangulation.js
// ═══════════════════════════════════════════════════════════════════════════════

class GeoMapper {
  constructor() {
    this.priorities = null;
    this.codeMap = null; // country_code → region_code lookup
  }

  async load(pool) {
    const { rows } = await pool.query('SELECT * FROM geo_priorities ORDER BY weight_boost DESC');
    this.priorities = rows;

    // Build fast country_code → region lookup
    this.codeMap = new Map();
    for (const row of rows) {
      for (const code of (row.country_codes || [])) {
        this.codeMap.set(code.trim().toUpperCase(), row.region_code);
      }
    }
  }

  resolve(countryCode, locationText) {
    if (!this.priorities) throw new Error('GeoMapper not loaded — call await load(pool) first');

    // Priority 1: country_code exact match (fastest, most reliable)
    if (countryCode) {
      const cc = countryCode.trim().toUpperCase();
      const match = this.codeMap.get(cc);
      if (match) {
        return { region_code: match, confidence: 1.0, method: 'country_code', match: cc };
      }
    }

    // Priority 2: location keyword substring match
    if (locationText) {
      const loc = locationText.toLowerCase();
      // Iterate in weight_boost DESC order (home markets checked first)
      for (const row of this.priorities) {
        for (const kw of (row.location_keywords || [])) {
          if (loc.includes(kw)) {
            return { region_code: row.region_code, confidence: 0.8, method: 'location_keyword', match: kw };
          }
        }
      }
    }

    return { region_code: 'UNKNOWN', confidence: 0, method: 'none', match: null };
  }

  // Resolve from company geography field (broader matching)
  resolveCompanyGeo(geography, countryCode) {
    if (!this.priorities) throw new Error('GeoMapper not loaded');

    // Try country_code first
    if (countryCode) {
      const cc = countryCode.trim().toUpperCase();
      const match = this.codeMap.get(cc);
      if (match) return { region_code: match, confidence: 1.0, method: 'country_code' };
    }

    // Try geography field
    if (geography) {
      const geo = geography.toLowerCase();
      for (const row of this.priorities) {
        for (const kw of (row.location_keywords || [])) {
          if (geo.includes(kw)) {
            return { region_code: row.region_code, confidence: 0.8, method: 'geography_keyword', match: kw };
          }
        }
        // Also check region_name
        if (geo.includes(row.region_name.toLowerCase())) {
          return { region_code: row.region_code, confidence: 0.7, method: 'region_name' };
        }
      }
    }

    return { region_code: 'UNKNOWN', confidence: 0, method: 'none' };
  }

  // Resolve from free text (evidence_summary, signal text, etc.)
  resolveFromText(text) {
    if (!text || !this.priorities) return { region_code: 'UNKNOWN', confidence: 0, method: 'none' };

    const lower = text.toLowerCase();
    for (const row of this.priorities) {
      for (const kw of (row.location_keywords || [])) {
        if (lower.includes(kw)) {
          return { region_code: row.region_code, confidence: 0.6, method: 'text_scan', match: kw };
        }
      }
    }
    return { region_code: 'UNKNOWN', confidence: 0, method: 'none' };
  }

  // Get priority info for a region
  getPriority(regionCode) {
    if (!this.priorities) return null;
    return this.priorities.find(p => p.region_code === regionCode) || null;
  }

  // Get all regions sorted by weight
  getRegions() {
    return this.priorities || [];
  }
}

module.exports = { GeoMapper };
