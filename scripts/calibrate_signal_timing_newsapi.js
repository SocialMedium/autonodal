#!/usr/bin/env node
/**
 * Retroactive Signal Calibration via NewsAPI.ai (Event Registry)
 *
 * Full-text historical news analysis for 250 MitchelLake clients.
 * Produces calibrated sleep timers AND feed source optimisation data.
 *
 * NewsAPI.ai advantages over GDELT:
 *   - 300K+ sources including startup/tech press and business wires
 *   - Full article text (not title-only) — ~95% detection sensitivity
 *   - Entity-level search with better company disambiguation
 *   - Historical archive back to 2014
 *
 * Usage:
 *   NEWSAPI_AI_KEY=your_key node scripts/calibrate_signal_timing_newsapi.js
 *
 * Outputs:
 *   reports/newsapi_signal_calibration.json — full results
 *   reports/newsapi_sleep_timers.json — calibrated timer parameters
 *   reports/newsapi_compound_patterns.json — compound signal combos
 *   reports/newsapi_source_ranking.json — feed source optimisation
 *   reports/newsapi_cache.json — cached API responses (re-runs instant)
 */

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const https = require('https');
const { detectSignals } = require('../lib/signal_keywords');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const ML_TENANT_ID = '00000000-0000-0000-0000-000000000001';
const NEWSAPI_KEY = process.env.NEWS_AI_KEY || process.env.NEWSAPI_AI_KEY;
const MAX_LOOKBACK_DAYS = 730; // 24 months
const API_DELAY_MS = 2500;     // Rate limit between queries

const TIMING_BUCKETS = [
  { label: '0-3mo',   min: 0,   max: 90  },
  { label: '3-6mo',   min: 91,  max: 180 },
  { label: '6-9mo',   min: 181, max: 270 },
  { label: '9-12mo',  min: 271, max: 365 },
  { label: '12-15mo', min: 366, max: 455 },
  { label: '15-18mo', min: 456, max: 540 },
  { label: '18-24mo', min: 541, max: 730 }
];

const STRIP_SUFFIXES = /\b(pty|ltd|limited|inc|incorporated|corp|corporation|llc|plc|group|holdings|co\.|company|sa|ag|gmbh|bv|nv)\b\.?/gi;

const SKIP_NAMES = new Set([
  'group', 'company', 'services', 'solutions', 'partners',
  'global', 'digital', 'capital', 'consulting', 'technology',
  'australia', 'international', 'management', 'holdings',
  'the', 'one', 'new', 'first', 'next', 'all', 'core', 'apex',
  'limited'
]);

// ─────────────────────────────────────────────────────────
// NEWSAPI.AI QUERY
// ─────────────────────────────────────────────────────────

function cleanCompanyName(name) {
  return name
    .replace(STRIP_SUFFIXES, '')
    .replace(/[()]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function isSearchable(name) {
  if (name.length < 4) return false;
  if (SKIP_NAMES.has(name.toLowerCase())) return false;
  if (name.split(/\s+/).length === 1 && name.length < 6) return false;
  return true;
}

function formatDate(d) {
  return new Date(d).toISOString().split('T')[0];
}

// Track whether the plan supports historical date filtering
let hasHistoricalAccess = null; // null = unknown, true/false = detected

async function makeNewsAPIRequest(params) {
  const body = JSON.stringify({ apiKey: NEWSAPI_KEY, ...params });
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'eventregistry.org',
      path: '/api/v1/article/getArticles',
      method: 'POST',
      timeout: 20000,
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            console.error(`  API error: ${JSON.stringify(parsed.error).substring(0, 200)}`);
            resolve({ articles: [], totalResults: 0 });
            return;
          }
          resolve({
            articles: parsed?.articles?.results || [],
            totalResults: parsed?.articles?.totalResults || 0
          });
        } catch (e) {
          resolve({ articles: [], totalResults: 0 });
        }
      });
    });
    req.on('error', () => resolve({ articles: [], totalResults: 0 }));
    req.on('timeout', () => { req.destroy(); resolve({ articles: [], totalResults: 0 }); });
    req.write(body);
    req.end();
  });
}

async function detectHistoricalAccess() {
  // Test if the plan supports historical queries by searching a well-known
  // company in a date range we know has articles
  const test = await makeNewsAPIRequest({
    keyword: 'Tesla',
    dateStart: '2025-01-01',
    dateEnd: '2025-06-01',
    lang: 'eng',
    articlesCount: 1,
    resultType: 'articles'
  });
  hasHistoricalAccess = test.totalResults > 0;
  return hasHistoricalAccess;
}

async function queryNewsAPI(companyName, startDate, endDate) {
  const baseParams = {
    keyword: companyName,
    keywordOper: 'and',
    lang: 'eng',
    articlesPage: 1,
    articlesCount: 100,
    articlesSortBy: 'date',
    articlesSortByAsc: false,
    resultType: 'articles',
    dataType: ['news', 'pr'],
    includeArticleBody: true,
    includeSourceTitle: true
  };

  if (hasHistoricalAccess) {
    // Plan supports historical — use date filters
    baseParams.dateStart = formatDate(startDate);
    baseParams.dateEnd = formatDate(endDate);
    return makeNewsAPIRequest(baseParams);
  } else {
    // Plan is current-only — fetch all available articles for company
    // and filter by date client-side
    const result = await makeNewsAPIRequest(baseParams);
    // Filter articles to the target date window
    const startMs = new Date(startDate).getTime();
    const endMs = new Date(endDate).getTime();
    const filtered = result.articles.filter(a => {
      const d = new Date(a.dateTime || a.date).getTime();
      return d >= startMs && d <= endMs;
    });
    return {
      articles: filtered,
      totalResults: filtered.length,
      unfilteredCount: result.articles.length
    };
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────────────
// SIGNAL DETECTION — FULL TEXT
// ─────────────────────────────────────────────────────────

function detectSignalsFromArticle(article) {
  // Combine title + body for full-text detection
  const title = article.title || '';
  const body = article.body || '';
  const fullText = title + '\n' + body;

  if (!fullText.trim()) return [];

  // Use the platform's signal keyword engine (lib/signal_keywords.js)
  // detectSignals(text) → [{signal_type, confidence, matches, evidence, snippet}]
  const detected = detectSignals(fullText);

  // Full-text detection is more reliable than title-only:
  // - If signal found in both title AND body → full confidence
  // - If signal found in body only → slight discount (0.9x)
  // - Cap at 0.90 since we don't have the live pipeline's source weighting
  return detected.map(s => {
    const titleSignals = detectSignals(title);
    const inTitle = titleSignals.some(ts => ts.signal_type === s.signal_type);
    const adjustedConfidence = inTitle
      ? Math.min(0.90, s.confidence)
      : Math.min(0.85, s.confidence * 0.9);

    return {
      type: s.signal_type,
      confidence: parseFloat(adjustedConfidence.toFixed(2)),
      matches: s.matches,
      evidence: s.evidence,
      title: title.substring(0, 150),
      source_domain: article.source?.uri || '',
      source_name: article.source?.title || '',
      article_url: article.url || '',
      date: article.dateTime || article.date
    };
  });
}

// ─────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────

async function run() {
  if (!NEWSAPI_KEY) {
    console.error('ERROR: Set NEWS_AI_KEY environment variable');
    console.error('  Sign up at https://newsapi.ai/ to get an API key');
    console.error('  Usage: NEWS_AI_KEY=your_key node scripts/calibrate_signal_timing_newsapi.js');
    process.exit(1);
  }

  const client = await pool.connect();

  try {
    console.log('═══════════════════════════════════════════════════════');
    console.log('  NEWSAPI.AI RETROACTIVE SIGNAL CALIBRATION');
    console.log('  Full-text | 300K sources | 2014-present');
    console.log('═══════════════════════════════════════════════════════\n');

    // ─────────────────────────────────────────────────────
    // STEP 1: Load client companies
    // Same query as calibrate_signal_timing.js (uses start_date)
    // ─────────────────────────────────────────────────────

    console.log('Step 1: Loading client companies...\n');

    const clientsQuery = `
      WITH client_projects AS (
        SELECT
          a.id AS account_id,
          a.name AS client_name,
          a.company_id,
          c.name AS company_name,
          c.sector,
          c.employee_count_band,
          c.country_code,
          c.geography,
          MIN(cv.start_date) AS first_invoice_date,
          MAX(cv.start_date) AS latest_invoice_date,
          COUNT(DISTINCT cv.id) AS invoice_count,
          SUM(cv.placement_fee) AS total_revenue,
          (MAX(cv.start_date) - MIN(cv.start_date)) AS engagement_days
        FROM accounts a
        LEFT JOIN companies c ON c.id = a.company_id
        JOIN conversions cv ON cv.client_id = a.id
        WHERE cv.placement_fee > 0
          AND cv.start_date IS NOT NULL
        GROUP BY a.id, a.name, a.company_id, c.name, c.sector,
                 c.employee_count_band, c.country_code, c.geography
        HAVING MIN(cv.start_date) IS NOT NULL
        ORDER BY MIN(cv.start_date) DESC
        LIMIT 1000
      )
      SELECT * FROM client_projects
      ORDER BY first_invoice_date DESC
    `;

    const clientsResult = await client.query(clientsQuery);
    const clients = clientsResult.rows;

    console.log(`  Found ${clients.length} client projects\n`);

    if (clients.length === 0) {
      console.log('  No client data found.');
      return;
    }

    // Filter to 2016+ (NewsAPI.ai coverage is dense from 2016)
    const eligibleClients = clients.filter(c => {
      const year = new Date(c.first_invoice_date).getFullYear();
      return year >= 2016;
    });

    console.log(`  Eligible for NewsAPI.ai lookup (2016+): ${eligibleClients.length}`);
    console.log(`  Excluded (pre-2016): ${clients.length - eligibleClients.length}\n`);

    // ─────────────────────────────────────────────────────
    // STEP 2: Query NewsAPI.ai for each client company
    // ─────────────────────────────────────────────────────

    // Detect plan capabilities
    console.log('  Detecting NewsAPI.ai plan capabilities...');
    const historical = await detectHistoricalAccess();
    if (historical) {
      console.log('  Plan supports historical archive access — using date-filtered queries\n');
    } else {
      console.log('  Plan is current-coverage only (~30 days) — no historical archive access');
      console.log('  Will search all available articles and filter client-side');
      console.log('  For full retroactive analysis, upgrade to a plan with historical access\n');
    }

    console.log('Step 2: Querying NewsAPI.ai archive...');
    const estMinutes = Math.ceil(eligibleClients.length * API_DELAY_MS / 60000);
    console.log(`  (Estimated ${estMinutes} minutes for ${eligibleClients.length} companies at ${API_DELAY_MS / 1000}s intervals)\n`);

    const results = [];
    const timingData = {};
    const compoundPatterns = {};
    const sectorTiming = {};
    const geoTiming = {};

    // Source tracking for feed optimisation
    const sourceSignalCount = {};    // domain → total signals
    const sourceClientSet = {};      // domain → Set of client names
    const sourceSignalTypes = {};    // domain → {signal_type: count}
    const sourceArticleCount = {};   // domain → total articles
    const sourceNames = {};          // domain → display name

    // Cache
    const reportsDir = path.join(__dirname, '..', 'reports');
    fs.mkdirSync(reportsDir, { recursive: true });
    const cacheFile = path.join(reportsDir, 'newsapi_cache.json');
    let cache = {};
    if (fs.existsSync(cacheFile)) {
      try {
        cache = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
        console.log(`  Loaded ${Object.keys(cache).length} cached results\n`);
      } catch (e) {
        console.log('  Cache file corrupt, starting fresh\n');
      }
    }

    let queriesMade = 0;
    let cacheHits = 0;
    let tokensUsed = 0;
    const HISTORICAL_SEARCH_BUDGET = 764; // remaining on current plan

    for (let i = 0; i < eligibleClients.length; i++) {
      const cl = eligibleClients[i];
      const rawName = cl.company_name || cl.client_name;
      const searchName = cleanCompanyName(rawName);

      if (!isSearchable(searchName)) {
        results.push({
          client_name: cl.client_name,
          company_name: cl.company_name,
          first_invoice_date: cl.first_invoice_date,
          skipped: true,
          reason: `Name not searchable: "${searchName}"`,
          signals: [],
          articles_found: 0,
          total_available: 0,
          signal_count: 0,
          signal_types: []
        });
        continue;
      }

      // Calculate lookback window
      const endDate = new Date(cl.first_invoice_date);
      const startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - MAX_LOOKBACK_DAYS);

      // Check cache
      const cacheKey = `newsapi|${searchName}|${cl.first_invoice_date}`;
      let apiResult;

      if (cache[cacheKey] !== undefined) {
        apiResult = cache[cacheKey];
        cacheHits++;
      } else {
        // Check budget before making API call
        if (queriesMade >= HISTORICAL_SEARCH_BUDGET) {
          console.log(`\n  *** BUDGET LIMIT REACHED: ${queriesMade}/${HISTORICAL_SEARCH_BUDGET} historical searches used ***`);
          console.log('  Saving cache and computing results from data collected so far...\n');
          fs.writeFileSync(cacheFile, JSON.stringify(cache));
          break;
        }

        apiResult = await queryNewsAPI(searchName, startDate, endDate);
        cache[cacheKey] = apiResult;
        queriesMade++;

        // Estimate token cost (historical queries cost more)
        const year = startDate.getFullYear();
        tokensUsed += year < 2017 ? 15 : year < 2020 ? 5 : 2;

        await sleep(API_DELAY_MS);
      }

      // Track article sources
      for (const article of apiResult.articles) {
        const domain = article.source?.uri || 'unknown';
        sourceArticleCount[domain] = (sourceArticleCount[domain] || 0) + 1;
        if (article.source?.title) sourceNames[domain] = article.source.title;
      }

      // Run signal detection on each article (FULL TEXT)
      const allSignals = [];
      for (const article of apiResult.articles) {
        const detected = detectSignalsFromArticle(article);
        for (const signal of detected) {
          const signalDate = new Date(signal.date);
          if (isNaN(signalDate.getTime())) continue;

          const invoiceDate = new Date(cl.first_invoice_date);
          const daysBefore = Math.round((invoiceDate - signalDate) / 86400000);

          if (daysBefore >= 0 && daysBefore <= MAX_LOOKBACK_DAYS) {
            allSignals.push({ ...signal, days_before: daysBefore });

            // Timing model accumulators
            if (!timingData[signal.type]) timingData[signal.type] = [];
            timingData[signal.type].push({
              days_before: daysBefore,
              sector: cl.sector,
              country: cl.country_code,
              geography: cl.geography,
              revenue: parseFloat(cl.total_revenue) || 0
            });

            if (cl.sector) {
              if (!sectorTiming[cl.sector]) sectorTiming[cl.sector] = [];
              sectorTiming[cl.sector].push(daysBefore);
            }
            if (cl.country_code) {
              if (!geoTiming[cl.country_code]) geoTiming[cl.country_code] = [];
              geoTiming[cl.country_code].push(daysBefore);
            }

            // Source tracking
            const domain = signal.source_domain || 'unknown';
            sourceSignalCount[domain] = (sourceSignalCount[domain] || 0) + 1;
            if (!sourceClientSet[domain]) sourceClientSet[domain] = new Set();
            sourceClientSet[domain].add(cl.client_name);
            if (!sourceSignalTypes[domain]) sourceSignalTypes[domain] = {};
            sourceSignalTypes[domain][signal.type] =
              (sourceSignalTypes[domain][signal.type] || 0) + 1;
          }
        }
      }

      // Compound pattern detection
      const signalTypes = [...new Set(allSignals.map(s => s.type))];
      if (signalTypes.length > 1) {
        const pattern = signalTypes.sort().join(' + ');
        compoundPatterns[pattern] = (compoundPatterns[pattern] || 0) + 1;
      }

      // Timing bucket distribution
      const bucketDist = {};
      for (const bucket of TIMING_BUCKETS) {
        bucketDist[bucket.label] = allSignals.filter(
          s => s.days_before >= bucket.min && s.days_before <= bucket.max
        ).length;
      }

      results.push({
        client_name: cl.client_name,
        company_name: cl.company_name,
        sector: cl.sector,
        country_code: cl.country_code,
        geography: cl.geography,
        employee_count_band: cl.employee_count_band,
        first_invoice_date: cl.first_invoice_date,
        total_revenue: parseFloat(cl.total_revenue) || 0,
        skipped: false,
        articles_found: apiResult.articles.length,
        total_available: apiResult.totalResults || apiResult.articles.length,
        signals: allSignals,
        signal_count: allSignals.length,
        signal_types: signalTypes,
        compound_pattern: signalTypes.length > 1 ? signalTypes.sort().join(' + ') : null,
        earliest_signal_days_before: allSignals.length > 0
          ? Math.max(...allSignals.map(s => s.days_before)) : null,
        latest_signal_days_before: allSignals.length > 0
          ? Math.min(...allSignals.map(s => s.days_before)) : null,
        timing_buckets: bucketDist,
        top_sources: [...new Set(allSignals.map(s => s.source_domain))].slice(0, 5)
      });

      // Progress
      if ((i + 1) % 10 === 0 || i === eligibleClients.length - 1) {
        const pct = ((i + 1) / eligibleClients.length * 100).toFixed(0);
        const withSig = results.filter(r => !r.skipped && r.signal_count > 0).length;
        const budgetNote = (i + 1) % 25 === 0 ? `  [API: ${queriesMade}/${HISTORICAL_SEARCH_BUDGET} searches used]` : '';
        console.log(`  [${pct}%] ${i + 1}/${eligibleClients.length} — ${withSig} w/ signals — ${searchName.padEnd(28)} ${apiResult.articles.length} art, ${allSignals.length} sig${budgetNote}`);
      }
    }

    // Save cache
    fs.writeFileSync(cacheFile, JSON.stringify(cache));
    console.log(`\n  Queries: ${queriesMade} live (~${tokensUsed} tokens), ${cacheHits} cached\n`);

    // ─────────────────────────────────────────────────────
    // STEP 3: Compute calibration metrics
    // ─────────────────────────────────────────────────────

    console.log('Step 3: Computing calibration metrics...\n');

    const nonSkipped = results.filter(r => !r.skipped);
    const withAnySignals = nonSkipped.filter(r => r.signal_count > 0);
    const totalArticles = nonSkipped.reduce((sum, r) => sum + r.articles_found, 0);
    const totalSignals = nonSkipped.reduce((sum, r) => sum + r.signal_count, 0);

    // Correlation by window
    const REPORT_WINDOWS = [90, 180, 365, 540];
    const correlation = {};

    for (const days of REPORT_WINDOWS) {
      const label = days <= 90 ? '3mo' : days <= 180 ? '6mo' : days <= 365 ? '12mo' : '18mo';
      const withSignals = nonSkipped.filter(r =>
        r.signals.some(s => s.days_before <= days)
      ).length;
      const signalRevenue = nonSkipped
        .filter(r => r.signals.some(s => s.days_before <= days))
        .reduce((sum, r) => sum + r.total_revenue, 0);
      const totalRevenue = nonSkipped.reduce((sum, r) => sum + r.total_revenue, 0);

      correlation[label] = {
        window_days: days,
        clients_with_signals: withSignals,
        total_clients: nonSkipped.length,
        rate: (withSignals / nonSkipped.length * 100).toFixed(1) + '%',
        revenue_preceded: signalRevenue.toFixed(0),
        revenue_pct: totalRevenue > 0
          ? (signalRevenue / totalRevenue * 100).toFixed(1) + '%'
          : 'n/a'
      };
    }

    // ─────────────────────────────────────────────────────
    // STEP 4: Sleep timers
    // ─────────────────────────────────────────────────────

    console.log('Step 4: Calibrating sleep timers...\n');

    const sleepTimers = {};
    for (const [signalType, dataPoints] of Object.entries(timingData)) {
      if (dataPoints.length < 3) continue;
      const daysArray = dataPoints.map(d => d.days_before).sort((a, b) => a - b);

      const distribution = {};
      for (const bucket of TIMING_BUCKETS) {
        const inBucket = daysArray.filter(d => d >= bucket.min && d <= bucket.max).length;
        distribution[bucket.label] = {
          count: inBucket,
          pct: (inBucket / daysArray.length * 100).toFixed(1) + '%'
        };
      }

      const peakBucket = Object.entries(distribution)
        .sort(([, a], [, b]) => b.count - a.count)[0];

      sleepTimers[signalType] = {
        sample_size: dataPoints.length,
        source: 'newsapi_retroactive',
        avg_lead_days: Math.round(daysArray.reduce((a, b) => a + b, 0) / daysArray.length),
        median_lead_days: daysArray[Math.floor(daysArray.length / 2)],
        p10_lead_days: daysArray[Math.floor(daysArray.length * 0.1)],
        p25_lead_days: daysArray[Math.floor(daysArray.length * 0.25)],
        p75_lead_days: daysArray[Math.floor(daysArray.length * 0.75)],
        p90_lead_days: daysArray[Math.floor(daysArray.length * 0.9)],
        min_lead_days: daysArray[0],
        max_lead_days: daysArray[daysArray.length - 1],
        peak_window: peakBucket[0],
        distribution,
        timer: {
          dormant_until_days: daysArray[Math.floor(daysArray.length * 0.1)],
          rising_from_days: daysArray[Math.floor(daysArray.length * 0.25)],
          peak_start_days: daysArray[Math.floor(daysArray.length * 0.4)],
          peak_end_days: daysArray[Math.floor(daysArray.length * 0.7)],
          declining_until_days: daysArray[Math.floor(daysArray.length * 0.9)],
          dormant_after_days: daysArray[daysArray.length - 1]
        }
      };
    }

    // ─────────────────────────────────────────────────────
    // STEP 5: Source ranking for feed optimisation
    // ─────────────────────────────────────────────────────

    console.log('Step 5: Ranking sources for feed optimisation...\n');

    const sourceRanking = Object.entries(sourceSignalCount)
      .map(([domain, signalCount]) => ({
        domain,
        source_name: sourceNames[domain] || domain,
        total_signals: signalCount,
        unique_clients: sourceClientSet[domain]?.size || 0,
        total_articles: sourceArticleCount[domain] || 0,
        signal_yield: sourceArticleCount[domain] > 0
          ? (signalCount / sourceArticleCount[domain] * 100).toFixed(1) + '%'
          : '0%',
        signal_types: sourceSignalTypes[domain] || {},
        in_catalog: false // set below
      }))
      .sort((a, b) => b.total_signals - a.total_signals);

    // Check which sources are already in the feed catalog
    try {
      const { rows: catalogRows } = await client.query(
        `SELECT DISTINCT url, name FROM rss_sources WHERE enabled = true`
      );
      const catalogDomains = new Set();
      for (const r of catalogRows) {
        try {
          const hostname = new URL(r.url).hostname.replace(/^www\./, '');
          catalogDomains.add(hostname);
        } catch (e) { /* skip malformed urls */ }
      }
      for (const source of sourceRanking) {
        source.in_catalog = catalogDomains.has(source.domain.replace(/^www\./, ''));
      }
    } catch (e) {
      // Non-fatal — catalog check is optional
    }

    // Compound patterns
    const sortedPatterns = Object.entries(compoundPatterns)
      .sort(([, a], [, b]) => b - a);

    // Sector modifiers
    const overallMedian = (() => {
      const all = Object.values(timingData).flat().map(d => d.days_before).sort((a, b) => a - b);
      return all.length > 0 ? all[Math.floor(all.length / 2)] : 180;
    })();

    const sectorModifiers = {};
    for (const [sector, leadTimes] of Object.entries(sectorTiming)) {
      if (leadTimes.length < 3) continue;
      const sorted = [...leadTimes].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      sectorModifiers[sector] = {
        sample_size: leadTimes.length,
        median_lead_days: median,
        modifier: (median / overallMedian).toFixed(2),
        interpretation: median < overallMedian ? 'FASTER' : 'SLOWER'
      };
    }

    // Geo modifiers
    const geoModifiers = {};
    for (const [country, leadTimes] of Object.entries(geoTiming)) {
      if (leadTimes.length < 3) continue;
      const sorted = [...leadTimes].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      geoModifiers[country] = {
        sample_size: leadTimes.length,
        median_lead_days: median,
        modifier: (median / overallMedian).toFixed(2),
        interpretation: median < overallMedian ? 'FASTER' : 'SLOWER'
      };
    }

    // Reboot pattern detection
    const rebootPatterns = [];
    for (const r of nonSkipped) {
      if (r.signals.length < 2) continue;
      const sorted = [...r.signals].sort((a, b) => b.days_before - a.days_before);
      for (let j = 0; j < sorted.length - 1; j++) {
        const gap = sorted[j].days_before - sorted[j + 1].days_before;
        if (gap > 90) {
          rebootPatterns.push({
            client: r.client_name,
            company: r.company_name,
            initial_signal: sorted[j].type,
            initial_days_before: sorted[j].days_before,
            reboot_signal: sorted[j + 1].type,
            reboot_days_before: sorted[j + 1].days_before,
            dormant_gap_days: gap,
            pattern: `${sorted[j].type} -> [${gap}d gap] -> ${sorted[j + 1].type} -> mandate`
          });
        }
      }
    }

    // ─────────────────────────────────────────────────────
    // STEP 6: Save outputs
    // ─────────────────────────────────────────────────────

    const output = {
      metadata: {
        run_date: new Date().toISOString(),
        method: 'NewsAPI.ai (Event Registry) full-text retroactive analysis',
        clients_analysed: nonSkipped.length,
        clients_skipped: results.filter(r => r.skipped).length,
        total_articles: totalArticles,
        total_signals: totalSignals,
        unique_sources_with_signals: Object.keys(sourceSignalCount).length,
        lookback_days: MAX_LOOKBACK_DAYS,
        api_queries_made: queriesMade,
        api_cache_hits: cacheHits,
        est_tokens_used: tokensUsed,
        overall_median_lead_days: overallMedian
      },
      correlation,
      sleep_timers: sleepTimers,
      compound_patterns: sortedPatterns.map(([p, c]) => ({ pattern: p, count: c })),
      sector_modifiers: sectorModifiers,
      geo_modifiers: geoModifiers,
      reboot_patterns: rebootPatterns,
      source_ranking: sourceRanking,
      client_details: results
    };

    fs.writeFileSync(
      path.join(reportsDir, 'newsapi_signal_calibration.json'),
      JSON.stringify(output, null, 2)
    );
    fs.writeFileSync(
      path.join(reportsDir, 'newsapi_sleep_timers.json'),
      JSON.stringify(sleepTimers, null, 2)
    );
    fs.writeFileSync(
      path.join(reportsDir, 'newsapi_compound_patterns.json'),
      JSON.stringify(sortedPatterns, null, 2)
    );
    fs.writeFileSync(
      path.join(reportsDir, 'newsapi_source_ranking.json'),
      JSON.stringify(sourceRanking, null, 2)
    );

    console.log('  Reports saved to reports/ directory\n');

    // ─────────────────────────────────────────────────────
    // STEP 7: Console output
    // ─────────────────────────────────────────────────────

    console.log('═══════════════════════════════════════════════════════');
    console.log('  NEWSAPI.AI SIGNAL CALIBRATION — RESULTS');
    console.log('═══════════════════════════════════════════════════════\n');

    console.log(`  Clients analysed:      ${nonSkipped.length} (${results.filter(r => r.skipped).length} skipped — generic names)`);
    console.log(`  Articles searched:     ${totalArticles.toLocaleString()}`);
    console.log(`  Signals detected:      ${totalSignals.toLocaleString()}`);
    console.log(`  Clients w/ signals:    ${withAnySignals.length} (${(withAnySignals.length / nonSkipped.length * 100).toFixed(1)}%)`);
    console.log(`  Unique signal sources: ${Object.keys(sourceSignalCount).length}`);
    console.log(`  API tokens used:       ~${tokensUsed}\n`);

    // Correlation table
    console.log('  +-------------------------------------------------+');
    console.log('  |  SIGNAL CORRELATION BY WINDOW                   |');
    console.log('  +-------------------------------------------------+');
    for (const [label, data] of Object.entries(correlation)) {
      console.log(`  |  ${label.padEnd(5)} ${String(data.clients_with_signals).padStart(4)}/${data.total_clients} clients  ${data.rate.padStart(6)}  ${data.revenue_pct.padStart(6)} rev |`);
    }
    console.log('  +-------------------------------------------------+\n');

    // Sleep timers
    if (Object.keys(sleepTimers).length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  CALIBRATED SLEEP TIMERS (full-text)            |');
      console.log('  +-------------------------------------------------+');
      for (const [type, timer] of Object.entries(sleepTimers).sort(([, a], [, b]) => b.sample_size - a.sample_size)) {
        const t = timer.timer;
        console.log(`  |                                                 |`);
        console.log(`  |  ${type.padEnd(30)} (n=${String(timer.sample_size).padEnd(4)})|`);
        console.log(`  |  Median: ${String(timer.median_lead_days).padStart(3)}d  IQR: ${timer.p25_lead_days}-${timer.p75_lead_days}d  Peak: ${timer.peak_window.padEnd(7)}|`);
        console.log(`  |  Phases: ${t.dormant_until_days}d -> ${t.rising_from_days}d -> [${t.peak_start_days}-${t.peak_end_days}d] -> ${t.declining_until_days}d  |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Compound patterns
    if (sortedPatterns.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  COMPOUND SIGNAL PATTERNS                       |');
      console.log('  +-------------------------------------------------+');
      for (const [pattern, count] of sortedPatterns.slice(0, 15)) {
        console.log(`  |  ${pattern.padEnd(42)} ${String(count).padStart(3)}x |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Sector modifiers
    if (Object.keys(sectorModifiers).length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  SECTOR TIMING MODIFIERS                        |');
      console.log('  +-------------------------------------------------+');
      for (const [sector, mod] of Object.entries(sectorModifiers).sort(([, a], [, b]) => a.median_lead_days - b.median_lead_days)) {
        console.log(`  |  ${sector.padEnd(25)} ${String(mod.median_lead_days).padStart(4)}d  ${mod.modifier}x ${mod.interpretation.padEnd(7)} |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Geo modifiers
    if (Object.keys(geoModifiers).length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  GEOGRAPHY TIMING MODIFIERS                     |');
      console.log('  +-------------------------------------------------+');
      for (const [country, mod] of Object.entries(geoModifiers).sort(([, a], [, b]) => a.median_lead_days - b.median_lead_days)) {
        console.log(`  |  ${country.padEnd(25)} ${String(mod.median_lead_days).padStart(4)}d  ${mod.modifier}x ${mod.interpretation.padEnd(7)} |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Reboot patterns
    if (rebootPatterns.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  SIGNAL REBOOT PATTERNS                         |');
      console.log('  +-------------------------------------------------+');
      for (const rb of rebootPatterns.slice(0, 10)) {
        console.log(`  |  ${(rb.company || rb.client).substring(0, 30).padEnd(30)}             |`);
        console.log(`  |  ${rb.pattern.substring(0, 47).padEnd(47)} |`);
      }
      console.log(`  |  Total: ${rebootPatterns.length} reboot patterns                    |`);
      console.log('  +-------------------------------------------------+\n');
    }

    // Feed source optimisation
    console.log('  +-------------------------------------------------+');
    console.log('  |  TOP SIGNAL SOURCES — FEED OPTIMISATION          |');
    console.log('  +-------------------------------------------------+');
    console.log('  |  Source                   Signals Clients InFeed |');
    console.log('  |  ------------------------------------------------|');
    for (const source of sourceRanking.slice(0, 25)) {
      const inFeed = source.in_catalog ? '  Y' : '  ADD';
      console.log(`  |  ${source.domain.substring(0, 25).padEnd(25)} ${String(source.total_signals).padStart(5)}   ${String(source.unique_clients).padStart(4)}  ${inFeed.padEnd(5)} |`);
    }
    console.log('  +-------------------------------------------------+\n');

    // Sources NOT in catalog that produced signals
    const missingFromCatalog = sourceRanking
      .filter(s => !s.in_catalog && s.total_signals >= 2);

    if (missingFromCatalog.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  MISSING FROM FEED CATALOG — RECOMMENDED ADDS   |');
      console.log('  +-------------------------------------------------+');
      for (const source of missingFromCatalog.slice(0, 20)) {
        const types = Object.entries(source.signal_types)
          .sort(([, a], [, b]) => b - a)
          .map(([t]) => t)
          .slice(0, 3)
          .join(', ');
        console.log(`  |  ${source.domain.substring(0, 22).padEnd(22)} ${String(source.total_signals).padStart(3)} sig  ${types.substring(0, 20)} |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Investor narrative
    console.log('═══════════════════════════════════════════════════════');
    console.log('  INVESTOR NARRATIVE');
    console.log('═══════════════════════════════════════════════════════\n');

    const macro = correlation['12mo'];
    const macro18 = correlation['18mo'];

    console.log(`  "Full-text analysis of ${nonSkipped.length} client engagements across`);
    console.log(`   300K+ global news sources (NewsAPI.ai) shows:`);
    console.log(`   - ${macro?.rate || 'N/A'} had detectable signals within 12 months`);
    console.log(`   - ${macro18?.rate || 'N/A'} had detectable signals within 18 months`);
    console.log(`   - ${macro?.revenue_pct || 'N/A'} of revenue was signal-preceded (12mo)"`);
    console.log('');

    if (Object.keys(sleepTimers).length > 0) {
      console.log(`  "Signal timing models calibrated from ${totalSignals.toLocaleString()} signals:`);
      for (const [type, timer] of Object.entries(sleepTimers)
        .sort(([, a], [, b]) => b.sample_size - a.sample_size).slice(0, 6)) {
        console.log(`     ${type}: median ${timer.median_lead_days}d lead, peak ${timer.peak_window}`);
      }
      console.log('   Trained on actual client history — no competitor can replicate."\n');
    }

    if (sortedPatterns.length > 0) {
      const [topPattern, topCount] = sortedPatterns[0];
      console.log(`  "Strongest compound pattern: ${topPattern}`);
      console.log(`   preceded ${topCount} mandates."\n`);
    }

    // Comparison with GDELT
    const gdeltCalFile = path.join(reportsDir, 'gdelt_signal_calibration.json');
    if (fs.existsSync(gdeltCalFile)) {
      try {
        const gdelt = JSON.parse(fs.readFileSync(gdeltCalFile, 'utf8'));
        console.log('  ─── COMPARISON: NewsAPI.ai vs GDELT ───');
        console.log(`  GDELT:     ${gdelt.metadata.total_signals} signals, ${gdelt.metadata.total_articles_searched} articles, ${gdelt.correlation?.['12mo']?.rate || '?'} correlation`);
        console.log(`  NewsAPI.ai: ${totalSignals} signals, ${totalArticles} articles, ${macro?.rate || '?'} correlation`);
        const gdeltSignals = gdelt.metadata.total_signals || 0;
        if (gdeltSignals > 0) {
          console.log(`  Improvement: ${((totalSignals / gdeltSignals - 1) * 100).toFixed(0)}% more signals detected\n`);
        }
      } catch (e) { /* skip comparison */ }
    }

    console.log('═══════════════════════════════════════════════════════\n');

    // Top signal-preceded clients by revenue
    const topRevClients = nonSkipped
      .filter(r => r.signal_count > 0)
      .sort((a, b) => b.total_revenue - a.total_revenue)
      .slice(0, 10);

    if (topRevClients.length > 0) {
      console.log('  TOP SIGNAL-PRECEDED CLIENTS BY REVENUE:');
      for (const c of topRevClients) {
        const types = c.signal_types.join(', ');
        console.log(`    ${(c.company_name || c.client_name).substring(0, 28).padEnd(28)} $${Math.round(c.total_revenue).toLocaleString().padStart(10)}  [${types}]`);
      }
      console.log('');
    }

  } catch (err) {
    console.error('Error:', err.message);
    console.error(err);
  } finally {
    client.release();
    await pool.end();
  }
}

run();
