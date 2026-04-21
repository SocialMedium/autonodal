#!/usr/bin/env node
/**
 * Retroactive Signal Detection via GDELT
 *
 * For each historical MitchelLake client, searches GDELT news archives
 * for market signals in the 24 months before engagement.
 *
 * Uses the same signal keyword detection as the live pipeline
 * (lib/signal_keywords.js) to identify what WOULD have been detected
 * if the platform had been running.
 *
 * Usage: node scripts/calibrate_signal_timing_gdelt.js
 *
 * Outputs:
 *   reports/gdelt_signal_calibration.json — full results
 *   reports/gdelt_sleep_timers.json — calibrated timers from historical data
 *   reports/gdelt_compound_patterns.json — compound patterns
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
const MAX_LOOKBACK_DAYS = 730; // 24 months
const GDELT_DELAY_MS = 1500;  // Rate limiting between queries
const GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc';

const TIMING_BUCKETS = [
  { label: '0-3mo',   min: 0,   max: 90  },
  { label: '3-6mo',   min: 91,  max: 180 },
  { label: '6-9mo',   min: 181, max: 270 },
  { label: '9-12mo',  min: 271, max: 365 },
  { label: '12-15mo', min: 366, max: 455 },
  { label: '15-18mo', min: 456, max: 540 },
  { label: '18-24mo', min: 541, max: 730 }
];

// Company name suffixes to strip before searching
const STRIP_SUFFIXES = /\b(pty|ltd|limited|inc|incorporated|corp|corporation|llc|plc|group|holdings|co\.|company|sa|ag|gmbh|bv|nv)\b\.?/gi;

// Names too generic to search meaningfully
const SKIP_NAMES = new Set([
  'group', 'company', 'services', 'solutions', 'partners',
  'global', 'digital', 'capital', 'consulting', 'technology',
  'australia', 'international', 'management', 'holdings',
  'the', 'one', 'new', 'first', 'next', 'all', 'core', 'apex'
]);

// ─────────────────────────────────────────────────────────
// GDELT QUERY
// ─────────────────────────────────────────────────────────

function formatGdeltDate(d) {
  const dt = new Date(d);
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, '0');
  const day = String(dt.getUTCDate()).padStart(2, '0');
  return `${y}${m}${day}000000`;
}

function cleanCompanyName(name) {
  return name
    .replace(STRIP_SUFFIXES, '')
    .replace(/[()]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function isSearchable(name) {
  if (name.length < 4) return false;
  const lower = name.toLowerCase();
  if (SKIP_NAMES.has(lower)) return false;
  // Single common word
  if (name.split(/\s+/).length === 1 && name.length < 6) return false;
  return true;
}

async function queryGDELT(companyName, startDate, endDate) {
  const params = new URLSearchParams({
    query: `"${companyName}"`,
    mode: 'ArtList',
    format: 'json',
    maxrecords: '250',
    startdatetime: formatGdeltDate(startDate),
    enddatetime: formatGdeltDate(endDate),
    sourcelang: 'english'
  });

  const url = `${GDELT_BASE_URL}?${params.toString()}`;

  return new Promise((resolve) => {
    const req = https.get(url, { timeout: 15000 }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          resolve(parsed.articles || []);
        } catch (e) {
          // GDELT returns empty or non-JSON for no results
          resolve([]);
        }
      });
    });
    req.on('error', (e) => {
      resolve([]);
    });
    req.on('timeout', () => {
      req.destroy();
      resolve([]);
    });
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────────────
// SIGNAL DETECTION ON GDELT ARTICLES
// ─────────────────────────────────────────────────────────

function detectSignalsFromArticle(article) {
  // Use the platform's signal keyword detection engine
  // detectSignals(text) → [{signal_type, confidence, matches, evidence, snippet}]
  const text = article.title || '';
  if (!text) return [];

  const detected = detectSignals(text);

  // Title-only detection is less reliable than full content —
  // cap confidence at 0.75 and apply a 0.85x discount
  return detected.map(s => ({
    type: s.signal_type,
    confidence: Math.min(parseFloat((s.confidence * 0.85).toFixed(2)), 0.75),
    matches: s.matches,
    title: article.title,
    source: article.domain,
    date: article.seendate,
    url: article.url
  }));
}

// ─────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────

async function run() {
  const client = await pool.connect();

  try {
    console.log('═══════════════════════════════════════════════════════');
    console.log('  RETROACTIVE SIGNAL DETECTION VIA GDELT');
    console.log('═══════════════════════════════════════════════════════\n');

    // ─────────────────────────────────────────────────────
    // STEP 1: Load client companies
    // Same query as calibrate_signal_timing.js
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
        LIMIT 250
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

    // Filter to clients from 2016+ (GDELT coverage starts ~2015)
    const eligibleClients = clients.filter(c => {
      const year = new Date(c.first_invoice_date).getFullYear();
      return year >= 2016;
    });

    console.log(`  Eligible for GDELT lookup (2016+): ${eligibleClients.length}`);
    console.log(`  Excluded (pre-2016): ${clients.length - eligibleClients.length}\n`);

    // ─────────────────────────────────────────────────────
    // STEP 2: Query GDELT for each client company
    // ─────────────────────────────────────────────────────

    console.log('Step 2: Querying GDELT historical news archive...');
    const estMinutes = Math.ceil(eligibleClients.length * GDELT_DELAY_MS / 60000);
    console.log(`  (Estimated ${estMinutes} minutes for ${eligibleClients.length} companies at ${GDELT_DELAY_MS / 1000}s intervals)\n`);

    const results = [];
    const timingData = {};        // signal_type → [{days_before, sector, ...}]
    const compoundPatterns = {};   // "type1+type2" → count
    const sectorTiming = {};       // sector → [days_before]
    const geoTiming = {};          // country → [days_before]

    // Cache file to avoid re-querying on re-runs
    const reportsDir = path.join(__dirname, '..', 'reports');
    fs.mkdirSync(reportsDir, { recursive: true });
    const cacheFile = path.join(reportsDir, 'gdelt_cache.json');
    let cache = {};
    if (fs.existsSync(cacheFile)) {
      try {
        cache = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
        console.log(`  Loaded ${Object.keys(cache).length} cached GDELT results\n`);
      } catch (e) {
        console.log('  Cache file corrupt, starting fresh\n');
      }
    }

    let queriesMade = 0;
    let cacheHits = 0;

    for (let i = 0; i < eligibleClients.length; i++) {
      const cl = eligibleClients[i];
      const rawName = cl.company_name || cl.client_name;
      const searchName = cleanCompanyName(rawName);

      // Skip generic/unsearchable names
      if (!isSearchable(searchName)) {
        results.push({
          client_name: cl.client_name,
          company_name: cl.company_name,
          first_invoice_date: cl.first_invoice_date,
          skipped: true,
          reason: `Name not searchable: "${searchName}"`,
          signals: [],
          articles_found: 0,
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
      const cacheKey = `${searchName}|${cl.first_invoice_date}`;
      let articles;

      if (cache[cacheKey] !== undefined) {
        articles = cache[cacheKey];
        cacheHits++;
      } else {
        articles = await queryGDELT(searchName, startDate, endDate);
        cache[cacheKey] = articles;
        queriesMade++;

        // Rate limit
        await sleep(GDELT_DELAY_MS);
      }

      // Run signal detection on each article
      const allSignals = [];
      for (const article of articles) {
        const detected = detectSignalsFromArticle(article);
        for (const signal of detected) {
          // Parse GDELT seendate (YYYYMMDDTHHMMSSZ format)
          let signalDate;
          try {
            const sd = signal.date;
            if (sd && sd.length >= 8) {
              signalDate = new Date(
                `${sd.substring(0, 4)}-${sd.substring(4, 6)}-${sd.substring(6, 8)}`
              );
            }
          } catch (e) {
            continue;
          }
          if (!signalDate || isNaN(signalDate.getTime())) continue;

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
              geography: cl.geography
            });

            // Sector timing
            if (cl.sector) {
              if (!sectorTiming[cl.sector]) sectorTiming[cl.sector] = [];
              sectorTiming[cl.sector].push(daysBefore);
            }

            // Geo timing
            if (cl.country_code) {
              if (!geoTiming[cl.country_code]) geoTiming[cl.country_code] = [];
              geoTiming[cl.country_code].push(daysBefore);
            }
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
        articles_found: articles.length,
        signals: allSignals,
        signal_count: allSignals.length,
        signal_types: signalTypes,
        compound_pattern: signalTypes.length > 1 ? signalTypes.sort().join(' + ') : null,
        earliest_signal_days_before: allSignals.length > 0
          ? Math.max(...allSignals.map(s => s.days_before)) : null,
        latest_signal_days_before: allSignals.length > 0
          ? Math.min(...allSignals.map(s => s.days_before)) : null,
        timing_buckets: bucketDist
      });

      // Progress
      if ((i + 1) % 10 === 0 || i === eligibleClients.length - 1) {
        const pct = ((i + 1) / eligibleClients.length * 100).toFixed(0);
        console.log(`  [${pct}%] ${i + 1}/${eligibleClients.length} — ${searchName.padEnd(30)} ${articles.length} articles, ${allSignals.length} signals`);
      }
    }

    // Save cache for re-runs
    fs.writeFileSync(cacheFile, JSON.stringify(cache));
    console.log(`\n  Queries: ${queriesMade} live, ${cacheHits} cached\n`);

    // ─────────────────────────────────────────────────────
    // STEP 3: Compute calibration metrics
    // ─────────────────────────────────────────────────────

    console.log('Step 3: Computing calibration metrics...\n');

    const nonSkipped = results.filter(r => !r.skipped);
    const withAnySignals = nonSkipped.filter(r => r.signal_count > 0);

    // Correlation by window
    const REPORT_WINDOWS = [90, 180, 365, 540];
    const correlation = {};

    for (const days of REPORT_WINDOWS) {
      const label = days <= 90 ? '3mo' : days <= 180 ? '6mo' : days <= 365 ? '12mo' : '18mo';
      const withSignals = nonSkipped.filter(r =>
        r.signals.some(s => s.days_before <= days)
      ).length;

      const signalPrecededRevenue = nonSkipped
        .filter(r => r.signals.some(s => s.days_before <= days))
        .reduce((sum, r) => sum + r.total_revenue, 0);

      const totalRevenue = nonSkipped.reduce((sum, r) => sum + r.total_revenue, 0);

      correlation[label] = {
        window_days: days,
        clients_with_signals: withSignals,
        total_eligible: nonSkipped.length,
        rate: (withSignals / nonSkipped.length * 100).toFixed(1) + '%',
        revenue_preceded: signalPrecededRevenue.toFixed(2),
        revenue_preceded_pct: totalRevenue > 0
          ? (signalPrecededRevenue / totalRevenue * 100).toFixed(1) + '%'
          : 'n/a'
      };
    }

    // Sleep timers per signal type
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
        source: 'gdelt_retroactive',
        avg_lead_days: Math.round(daysArray.reduce((a, b) => a + b, 0) / daysArray.length),
        median_lead_days: daysArray[Math.floor(daysArray.length / 2)],
        p25_lead_days: daysArray[Math.floor(daysArray.length * 0.25)],
        p75_lead_days: daysArray[Math.floor(daysArray.length * 0.75)],
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

    // Compound patterns sorted
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
      for (let i = 0; i < sorted.length - 1; i++) {
        const gap = sorted[i].days_before - sorted[i + 1].days_before;
        if (gap > 90) {
          rebootPatterns.push({
            client: r.client_name,
            company: r.company_name,
            initial_signal: sorted[i].type,
            initial_days_before: sorted[i].days_before,
            reboot_signal: sorted[i + 1].type,
            reboot_days_before: sorted[i + 1].days_before,
            dormant_gap_days: gap,
            pattern: `${sorted[i].type} -> [${gap}d gap] -> ${sorted[i + 1].type} -> mandate`
          });
        }
      }
    }

    // ─────────────────────────────────────────────────────
    // STEP 4: Save output
    // ─────────────────────────────────────────────────────

    const totalArticles = nonSkipped.reduce((sum, r) => sum + r.articles_found, 0);
    const totalSignals = nonSkipped.reduce((sum, r) => sum + r.signal_count, 0);

    const output = {
      metadata: {
        run_date: new Date().toISOString(),
        method: 'GDELT retroactive signal detection',
        clients_analysed: nonSkipped.length,
        clients_skipped: results.filter(r => r.skipped).length,
        total_articles_searched: totalArticles,
        total_signals_detected: totalSignals,
        lookback_days: MAX_LOOKBACK_DAYS,
        gdelt_queries_made: queriesMade,
        gdelt_cache_hits: cacheHits,
        overall_median_lead_days: overallMedian
      },
      correlation,
      sleep_timers: sleepTimers,
      compound_patterns: sortedPatterns.map(([pattern, count]) => ({ pattern, count })),
      sector_modifiers: sectorModifiers,
      geo_modifiers: geoModifiers,
      reboot_patterns: rebootPatterns,
      client_details: results
    };

    fs.writeFileSync(
      path.join(reportsDir, 'gdelt_signal_calibration.json'),
      JSON.stringify(output, null, 2)
    );
    fs.writeFileSync(
      path.join(reportsDir, 'gdelt_sleep_timers.json'),
      JSON.stringify(sleepTimers, null, 2)
    );
    fs.writeFileSync(
      path.join(reportsDir, 'gdelt_compound_patterns.json'),
      JSON.stringify(sortedPatterns, null, 2)
    );

    console.log('  Reports saved to reports/ directory\n');

    // ─────────────────────────────────────────────────────
    // STEP 5: Console output
    // ─────────────────────────────────────────────────────

    console.log('═══════════════════════════════════════════════════════');
    console.log('  GDELT RETROACTIVE SIGNAL CALIBRATION — RESULTS');
    console.log('═══════════════════════════════════════════════════════\n');

    console.log(`  Clients analysed:    ${nonSkipped.length} (${results.filter(r => r.skipped).length} skipped — generic names)`);
    console.log(`  Articles searched:   ${totalArticles.toLocaleString()}`);
    console.log(`  Signals detected:    ${totalSignals.toLocaleString()}`);
    console.log(`  Clients w/ signals:  ${withAnySignals.length} (${(withAnySignals.length / nonSkipped.length * 100).toFixed(1)}%)\n`);

    // Correlation table
    console.log('  +-------------------------------------------------+');
    console.log('  |  SIGNAL CORRELATION BY WINDOW                   |');
    console.log('  +-------------------------------------------------+');
    for (const [label, data] of Object.entries(correlation)) {
      console.log(`  |  ${label.padEnd(5)} ${String(data.clients_with_signals).padStart(4)}/${data.total_eligible} clients  ${data.rate.padStart(6)}  ${data.revenue_preceded_pct.padStart(6)} rev |`);
    }
    console.log('  +-------------------------------------------------+\n');

    // Sleep timers
    if (Object.keys(sleepTimers).length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  SIGNAL SLEEP TIMERS (GDELT-calibrated)         |');
      console.log('  +-------------------------------------------------+');
      for (const [type, timer] of Object.entries(sleepTimers).sort(([, a], [, b]) => b.sample_size - a.sample_size)) {
        const t = timer.timer;
        console.log(`  |                                                 |`);
        console.log(`  |  ${type.padEnd(30)} (n=${String(timer.sample_size).padEnd(4)})|`);
        console.log(`  |  Median: ${String(timer.median_lead_days).padStart(3)}d  IQR: ${timer.p25_lead_days}-${timer.p75_lead_days}d  Peak: ${timer.peak_window.padEnd(7)}|`);
        console.log(`  |  Phases: ${t.dormant_until_days}d -> ${t.rising_from_days}d -> [${t.peak_start_days}-${t.peak_end_days}d] -> ${t.declining_until_days}d -> ${t.dormant_after_days}d |`);
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

    // Revenue-weighted correlation
    const totalRev = nonSkipped.reduce((sum, r) => sum + r.total_revenue, 0);
    const signalRev12 = nonSkipped
      .filter(r => r.signals.some(s => s.days_before <= 365))
      .reduce((sum, r) => sum + r.total_revenue, 0);

    // Investor narrative
    console.log('═══════════════════════════════════════════════════════');
    console.log('  INVESTOR NARRATIVE');
    console.log('═══════════════════════════════════════════════════════\n');

    const macro = correlation['12mo'];
    const macro18 = correlation['18mo'];

    console.log(`  "Retroactive analysis of our last ${nonSkipped.length} client engagements`);
    console.log(`   against 8+ years of global news archives (GDELT) shows:`);
    console.log(`   - ${macro?.rate || 'N/A'} had detectable signals within 12 months`);
    console.log(`   - ${macro18?.rate || 'N/A'} had detectable signals within 18 months`);
    if (totalRev > 0) {
      console.log(`   - ${(signalRev12 / totalRev * 100).toFixed(1)}% of revenue ($${Math.round(signalRev12).toLocaleString()}) was signal-preceded"`);
    }
    console.log('');

    if (Object.keys(sleepTimers).length > 0) {
      console.log('  "Signal-specific timing models calibrated from');
      console.log(`   ${totalSignals.toLocaleString()} signal-to-mandate data points:`);
      for (const [type, timer] of Object.entries(sleepTimers).sort(([, a], [, b]) => b.sample_size - a.sample_size).slice(0, 6)) {
        console.log(`     ${type}: median ${timer.median_lead_days}d lead, peak ${timer.peak_window}`);
      }
      console.log('   Trained on actual client history — no competitor can replicate."\n');
    }

    if (sortedPatterns.length > 0) {
      const [topPattern, topCount] = sortedPatterns[0];
      console.log(`  "Strongest compound pattern: ${topPattern}`);
      console.log(`   preceded ${topCount} mandates in our history."\n`);
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
        console.log(`    ${(c.company_name || c.client_name).padEnd(30)} $${Math.round(c.total_revenue).toLocaleString().padStart(10)}  [${types}]`);
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
