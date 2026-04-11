// ═══════════════════════════════════════════════════════════════════════════════
// lib/gdelt_client.js — GDELT DOC 2.0 API Client
//
// Queries the Global Database of Events, Language, and Tone for pre-processed
// global news intelligence. Free, no auth, 15-minute update cycle, 100+ languages.
//
// API docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-documentation/
// ═══════════════════════════════════════════════════════════════════════════════

const GDELT_DOC_BASE = 'https://api.gdeltproject.org/api/v2/doc/doc';

// GDELT theme codes mapped to our signal taxonomy
const GDELT_THEMES = {
  geographic_expansion: [
    'ECON_GEOGRAPHIC_EXPANSION', 'BUS_MARKET_EXPANSION',
    'BUS_OPENING', 'BUS_NEW_OFFICE',
  ],
  capital_raising: [
    'ECON_FUNDING', 'BUS_INVESTMENT', 'BUS_FUNDING_ROUND',
    'ECON_IPO', 'BUS_VENTURE_CAPITAL',
  ],
  ma_activity: [
    'BUS_ACQUISITION', 'BUS_MERGER', 'BUS_MERGER_ACQUISITION', 'ECON_MA',
  ],
  leadership_change: [
    'BUS_APPOINTMENT', 'BUS_EXECUTIVE_CHANGE',
    'BUS_CEO_CHANGE', 'BUS_RESIGNATION',
  ],
  strategic_hiring: [
    'BUS_HIRING', 'BUS_EMPLOYMENT', 'ECON_JOB_CREATION',
  ],
  layoffs: [
    'BUS_LAYOFFS', 'ECON_LAYOFFS', 'BUS_DOWNSIZING', 'ECON_UNEMPLOYMENT',
  ],
  restructuring: [
    'BUS_RESTRUCTURING', 'BUS_BANKRUPTCY', 'ECON_RESTRUCTURING',
  ],
  product_launch: [
    'BUS_PRODUCT_LAUNCH', 'BUS_NEW_PRODUCT', 'TECH_LAUNCH',
  ],
  partnership: [
    'BUS_PARTNERSHIP', 'BUS_ALLIANCE', 'BUS_JOINT_VENTURE',
  ],
};

// Keyword queries per signal type
const SIGNAL_QUERIES = {
  geographic_expansion: [
    '"country manager" OR "regional director" OR "head of" OR "expands to" OR "opens office"',
    '"market entry" OR "enters market" OR "launches in" OR "new region"',
    '"country manager" OR "regional VP" OR "head of APAC" OR "head of EMEA"',
    '"managing director" APAC OR EMEA OR SEA OR "Southeast Asia"',
  ],
  capital_raising: [
    '"series A" OR "series B" OR "series C" OR "raised" OR "funding round"',
    '"investment" OR "venture capital" OR "IPO" OR "capital raise"',
  ],
  ma_activity: [
    '"acquires" OR "acquisition" OR "merger" OR "merges with" OR "buys"',
    '"takeover" OR "buyout" OR "divests" OR "sells division"',
  ],
  leadership_change: [
    '"appointed" OR "named CEO" OR "named CFO" OR "named CTO" OR "joins as"',
    '"resigns" OR "steps down" OR "departure" OR "new chief"',
  ],
  restructuring: [
    '"layoffs" OR "redundancies" OR "job cuts" OR "workforce reduction"',
    '"restructuring" OR "downsizing" OR "cost cutting"',
  ],
  product_launch: [
    '"product launch" OR "launches" OR "introduces" OR "unveils"',
  ],
};

// GDELT dates are YYYYMMDDHHMMSS
function parseGDELTDate(dateStr) {
  if (!dateStr || dateStr.length < 14) return new Date();
  return new Date(
    dateStr.slice(0, 4) + '-' + dateStr.slice(4, 6) + '-' + dateStr.slice(6, 8) +
    'T' + dateStr.slice(8, 10) + ':' + dateStr.slice(10, 12) + ':' + dateStr.slice(12, 14) + 'Z'
  );
}

/**
 * Query the GDELT DOC 2.0 API.
 * @param {object} opts
 * @param {string} opts.query — keyword/phrase query (boolean AND/OR/NOT)
 * @param {string} [opts.timespan='15min'] — 15min, 1h, 1d, 1w
 * @param {number} [opts.maxrecords=100] — max 250
 * @param {string} [opts.sort='DateDesc']
 * @param {string} [opts.sourcelang] — filter by language code
 * @param {string} [opts.theme] — filter by GDELT theme code
 * @returns {Promise<Array>} parsed articles
 */
async function queryGDELT({
  query,
  timespan = '15min',
  maxrecords = 100,
  sort = 'DateDesc',
  sourcelang = null,
  theme = null,
}) {
  const params = new URLSearchParams({
    query,
    mode: 'artlist',
    format: 'json',
    timespan,
    maxrecords: String(maxrecords),
    sort,
  });

  if (sourcelang) params.set('sourcelang', sourcelang);
  if (theme) params.set('theme', theme);

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const res = await fetch(GDELT_DOC_BASE + '?' + params.toString(), {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Signal Intelligence Platform)' },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    if (!res.ok) {
      console.error('[GDELT] API returned ' + res.status);
      return [];
    }

    const data = await res.json();
    if (!data || !data.articles) return [];

    return data.articles.map(a => ({
      gdelt_id:      (a.seendate || '') + '_' + (a.url || '').slice(-20),
      url:           a.url,
      title:         a.title,
      source:        a.domain,
      language:      a.language || 'en',
      published_at:  parseGDELTDate(a.seendate),
      gdelt_tone:    parseFloat(a.tone) || 0,
      gdelt_themes:  a.themes ? a.themes.split(';').filter(Boolean) : [],
      social_shares: parseInt(a.socialshares) || 0,
    }));
  } catch (err) {
    console.error('[GDELT] Query failed:', err.message);
    return [];
  }
}

module.exports = { queryGDELT, GDELT_THEMES, SIGNAL_QUERIES, GDELT_DOC_BASE, parseGDELTDate };
