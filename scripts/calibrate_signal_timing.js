#!/usr/bin/env node
/**
 * Retrospective Signal Timing Intelligence
 *
 * Analyses last 250 MitchelLake client projects (by Xero first invoice date)
 * against signal history to calibrate proprietary timing models.
 *
 * Outputs:
 *   1. Correlation report (summary stats)
 *   2. Signal sleep timer calibration (empirical curves per signal type)
 *   3. Compound pattern analysis (which combinations precede mandates)
 *   4. Sector/geography timing modifiers
 *   5. Reboot pattern detection
 *
 * Usage: node scripts/calibrate_signal_timing.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const ML_TENANT_ID = '00000000-0000-0000-0000-000000000001';

// Maximum lookback window for macro signals
const MAX_LOOKBACK_DAYS = 730; // 24 months — captures full cycle signals

// Sleep timer buckets (months)
const TIMING_BUCKETS = [
  { label: '0-3mo',   min: 0,   max: 90  },
  { label: '3-6mo',   min: 91,  max: 180 },
  { label: '6-9mo',   min: 181, max: 270 },
  { label: '9-12mo',  min: 271, max: 365 },
  { label: '12-15mo', min: 366, max: 455 },
  { label: '15-18mo', min: 456, max: 540 },
  { label: '18-24mo', min: 541, max: 730 }
];

async function run() {
  const client = await pool.connect();

  try {
    console.log('═══════════════════════════════════════════════════════');
    console.log('  SIGNAL TIMING INTELLIGENCE — CALIBRATION RUN');
    console.log('═══════════════════════════════════════════════════════\n');

    // ─────────────────────────────────────────────────────────
    // STEP 1: Get last 250 client projects from Xero/conversions data
    // ─────────────────────────────────────────────────────────
    //
    // Schema: accounts (id, name, company_id, tenant_id)
    //         conversions (client_id → accounts.id, start_date, placement_fee)
    //         companies (id, name, sector, country_code, employee_count_band)
    //
    // Note: invoice_date is unpopulated; start_date is the engagement anchor
    //       (earliest placement start date serves as proxy for first invoice)

    console.log('Step 1: Fetching last 250 client projects...\n');

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
      console.log('  No client data found. Ensure Xero invoices have been ingested.');
      console.log('  Run: node scripts/ingest_xero_invoices.js <path-to-csv>');
      return;
    }

    // ─────────────────────────────────────────────────────────
    // STEP 2: For each client, extract full signal history
    //         in the 24-month window before engagement
    // ─────────────────────────────────────────────────────────

    console.log('Step 2: Extracting signal history for each client...\n');

    const results = [];

    // Aggregation accumulators for timing model
    const timingData = {};        // signal_type → array of { days_before, sector, ... }
    const compoundPatterns = {};   // "type1+type2" → count of mandates
    const sectorTiming = {};       // sector → array of lead_time_days
    const geoTiming = {};          // country_code → array of lead_time_days

    for (const cl of clients) {
      const result = {
        client_name: cl.client_name,
        company_name: cl.company_name,
        company_id: cl.company_id,
        sector: cl.sector,
        country_code: cl.country_code,
        geography: cl.geography,
        employee_count_band: cl.employee_count_band,
        first_invoice_date: cl.first_invoice_date,
        latest_invoice_date: cl.latest_invoice_date,
        total_revenue: parseFloat(cl.total_revenue) || 0,
        engagement_days: parseInt(cl.engagement_days) || 0,

        // Signal analysis
        signals_found: [],
        signal_types_found: [],
        signal_count: 0,
        earliest_signal_days_before: null,
        latest_signal_days_before: null,

        // Document mentions
        document_mentions: 0,
        document_sources: [],

        // Career moves at client company
        career_moves: 0,

        // Compound patterns
        compound_pattern: null,

        // Timing bucket distribution
        timing_buckets: {}
      };

      // ── 2a. Signal events in 24-month window before first invoice ──
      if (cl.company_id) {
        const signalQuery = `
          SELECT
            se.signal_type,
            se.confidence_score,
            se.detected_at,
            se.company_name AS signal_company,
            se.evidence_summary,
            se.source_url,
            ed.title AS doc_title,
            ($2::date - se.detected_at::date) AS days_before_engagement
          FROM signal_events se
          LEFT JOIN external_documents ed ON ed.id = se.source_document_id
          WHERE se.company_id = $3
            AND (se.tenant_id IS NULL OR se.tenant_id = $1)
            AND se.detected_at BETWEEN ($2::date - interval '${MAX_LOOKBACK_DAYS} days') AND $2::date
          ORDER BY se.detected_at ASC
        `;

        const signalResult = await client.query(signalQuery, [
          ML_TENANT_ID, cl.first_invoice_date, cl.company_id
        ]);

        result.signals_found = signalResult.rows.map(r => ({
          type: r.signal_type,
          confidence: parseFloat(r.confidence_score),
          days_before: parseInt(r.days_before_engagement),
          detected_at: r.detected_at,
          title: (r.doc_title || r.evidence_summary || '')?.substring(0, 120)
        }));

        result.signal_count = signalResult.rows.length;
        result.signal_types_found = [...new Set(signalResult.rows.map(r => r.signal_type))];

        if (signalResult.rows.length > 0) {
          const leadTimes = signalResult.rows.map(r => parseInt(r.days_before_engagement));
          result.earliest_signal_days_before = Math.max(...leadTimes);
          result.latest_signal_days_before = Math.min(...leadTimes);

          // ── Feed into timing model accumulators ──
          for (const signal of signalResult.rows) {
            const type = signal.signal_type;
            const daysBefore = parseInt(signal.days_before_engagement);

            if (!timingData[type]) timingData[type] = [];
            timingData[type].push({
              days_before: daysBefore,
              sector: cl.sector,
              country: cl.country_code,
              geography: cl.geography,
              company_size: cl.employee_count_band,
              revenue: parseFloat(cl.total_revenue) || 0
            });
          }

          // ── Sector and geo timing ──
          const minLead = Math.min(...leadTimes);
          if (cl.sector) {
            if (!sectorTiming[cl.sector]) sectorTiming[cl.sector] = [];
            sectorTiming[cl.sector].push(minLead);
          }
          if (cl.country_code) {
            if (!geoTiming[cl.country_code]) geoTiming[cl.country_code] = [];
            geoTiming[cl.country_code].push(minLead);
          }

          // ── Compound pattern detection ──
          if (result.signal_types_found.length > 1) {
            const pattern = result.signal_types_found.sort().join(' + ');
            result.compound_pattern = pattern;
            compoundPatterns[pattern] = (compoundPatterns[pattern] || 0) + 1;
          }

          // ── Timing bucket distribution ──
          for (const bucket of TIMING_BUCKETS) {
            const count = leadTimes.filter(d => d >= bucket.min && d <= bucket.max).length;
            result.timing_buckets[bucket.label] = count;
          }
        }
      }

      // ── 2b. Document mentions (company name in external_documents) ──
      const searchName = cl.company_name || cl.client_name;
      if (searchName && searchName.length > 3) {
        const docQuery = `
          SELECT COUNT(*) AS mention_count,
                 ARRAY_AGG(DISTINCT source_name) FILTER (WHERE source_name IS NOT NULL) AS sources
          FROM external_documents
          WHERE (tenant_id IS NULL OR tenant_id = $1)
            AND published_at BETWEEN ($2::date - interval '${MAX_LOOKBACK_DAYS} days') AND $2::date
            AND (title ILIKE '%' || $3 || '%' OR summary ILIKE '%' || $3 || '%')
        `;

        try {
          const docResult = await client.query(docQuery, [
            ML_TENANT_ID, cl.first_invoice_date, searchName
          ]);
          result.document_mentions = parseInt(docResult.rows[0]?.mention_count || 0);
          result.document_sources = docResult.rows[0]?.sources || [];
        } catch (e) {
          // Non-fatal — ILIKE on large text column may be slow
        }
      }

      // ── 2c. Career moves at client company (person_signals in 24mo window) ──
      if (cl.company_id) {
        try {
          const careerQuery = `
            SELECT COUNT(DISTINCT ps.person_id) AS moves
            FROM person_signals ps
            WHERE ps.company_id = $3
              AND (ps.tenant_id IS NULL OR ps.tenant_id = $1)
              AND ps.signal_type IN ('new_role', 'promotion', 'company_exit', 'board_appointment')
              AND ps.detected_at BETWEEN ($2::date - interval '730 days') AND $2::date
          `;
          const careerResult = await client.query(careerQuery, [
            ML_TENANT_ID, cl.first_invoice_date, cl.company_id
          ]);
          result.career_moves = parseInt(careerResult.rows[0]?.moves || 0);
        } catch (e) {
          // Non-fatal
        }
      }

      results.push(result);
      if (results.length % 25 === 0) {
        console.log(`  Processed ${results.length}/${clients.length}...`);
      }
    }

    // ─────────────────────────────────────────────────────────
    // STEP 3: Build signal sleep timer calibration
    // ─────────────────────────────────────────────────────────

    console.log('\nStep 3: Calibrating signal sleep timers...\n');

    const sleepTimers = {};

    for (const [signalType, dataPoints] of Object.entries(timingData)) {
      if (dataPoints.length < 3) continue; // Need minimum sample

      const daysArray = dataPoints.map(d => d.days_before).sort((a, b) => a - b);

      // Compute distribution across timing buckets
      const distribution = {};
      for (const bucket of TIMING_BUCKETS) {
        const inBucket = daysArray.filter(d => d >= bucket.min && d <= bucket.max).length;
        distribution[bucket.label] = {
          count: inBucket,
          pct: (inBucket / daysArray.length * 100).toFixed(1) + '%'
        };
      }

      // Find peak window (bucket with most signals preceding mandates)
      const peakBucket = Object.entries(distribution)
        .sort(([,a], [,b]) => b.count - a.count)[0];

      sleepTimers[signalType] = {
        sample_size: dataPoints.length,
        avg_lead_days: Math.round(daysArray.reduce((a, b) => a + b, 0) / daysArray.length),
        median_lead_days: daysArray[Math.floor(daysArray.length / 2)],
        min_lead_days: daysArray[0],
        max_lead_days: daysArray[daysArray.length - 1],
        p25_lead_days: daysArray[Math.floor(daysArray.length * 0.25)],
        p75_lead_days: daysArray[Math.floor(daysArray.length * 0.75)],
        peak_window: peakBucket[0],
        distribution: distribution,

        // The sleep timer parameters (percentile-based phases)
        timer: {
          dormant_until_days: daysArray[Math.floor(daysArray.length * 0.1)],  // 10th percentile
          rising_from_days: daysArray[Math.floor(daysArray.length * 0.25)],   // 25th percentile
          peak_start_days: daysArray[Math.floor(daysArray.length * 0.4)],     // 40th percentile
          peak_end_days: daysArray[Math.floor(daysArray.length * 0.7)],       // 70th percentile
          declining_until_days: daysArray[Math.floor(daysArray.length * 0.9)],// 90th percentile
          dormant_after_days: daysArray[daysArray.length - 1]                 // max observed
        }
      };
    }

    // ─────────────────────────────────────────────────────────
    // STEP 4: Compound pattern analysis
    // ─────────────────────────────────────────────────────────

    console.log('Step 4: Analysing compound signal patterns...\n');

    const sortedPatterns = Object.entries(compoundPatterns)
      .sort(([,a], [,b]) => b - a);

    // ─────────────────────────────────────────────────────────
    // STEP 5: Sector and geography timing modifiers
    // ─────────────────────────────────────────────────────────

    console.log('Step 5: Computing sector/geography timing modifiers...\n');

    const sectorModifiers = {};
    const overallMedian = (() => {
      const allLeadTimes = Object.values(timingData)
        .flat()
        .map(d => d.days_before)
        .sort((a, b) => a - b);
      return allLeadTimes.length > 0
        ? allLeadTimes[Math.floor(allLeadTimes.length / 2)]
        : 180;
    })();

    for (const [sector, leadTimes] of Object.entries(sectorTiming)) {
      if (leadTimes.length < 3) continue;
      const sorted = leadTimes.sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      sectorModifiers[sector] = {
        sample_size: leadTimes.length,
        median_lead_days: median,
        modifier: (median / overallMedian).toFixed(2),
        interpretation: median < overallMedian ? 'FASTER than average' : 'SLOWER than average'
      };
    }

    const geoModifiers = {};
    for (const [country, leadTimes] of Object.entries(geoTiming)) {
      if (leadTimes.length < 3) continue;
      const sorted = leadTimes.sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)];
      geoModifiers[country] = {
        sample_size: leadTimes.length,
        median_lead_days: median,
        modifier: (median / overallMedian).toFixed(2),
        interpretation: median < overallMedian ? 'FASTER than average' : 'SLOWER than average'
      };
    }

    // ─────────────────────────────────────────────────────────
    // STEP 6: Correlation summary
    // ─────────────────────────────────────────────────────────

    console.log('Step 6: Computing correlation summary...\n');

    const REPORT_WINDOWS = [90, 180, 365, 540];
    const correlation = {};

    for (const days of REPORT_WINDOWS) {
      const label = days <= 90 ? '3mo' : days <= 180 ? '6mo' : days <= 365 ? '12mo' : '18mo';

      const withSignals = results.filter(r =>
        r.signals_found.some(s => s.days_before <= days)
      ).length;

      const withDocs = results.filter(r => r.document_mentions > 0).length;

      const withAny = results.filter(r =>
        r.signals_found.some(s => s.days_before <= days) || r.document_mentions > 0
      ).length;

      const signalPrecededRevenue = results
        .filter(r => r.signals_found.some(s => s.days_before <= days))
        .reduce((sum, r) => sum + r.total_revenue, 0);

      const totalRevenue = results.reduce((sum, r) => sum + r.total_revenue, 0);

      correlation[label] = {
        window_days: days,
        clients_with_signals: withSignals,
        clients_with_docs: withDocs,
        clients_with_any: withAny,
        signal_rate: (withSignals / results.length * 100).toFixed(1) + '%',
        any_signal_rate: (withAny / results.length * 100).toFixed(1) + '%',
        revenue_preceded: signalPrecededRevenue.toFixed(2),
        revenue_preceded_pct: totalRevenue > 0
          ? (signalPrecededRevenue / totalRevenue * 100).toFixed(1) + '%'
          : 'n/a'
      };
    }

    // Career move correlation
    const withCareerMoves = results.filter(r => r.career_moves > 0).length;

    // ─────────────────────────────────────────────────────────
    // STEP 7: Reboot pattern detection
    // ─────────────────────────────────────────────────────────

    console.log('Step 7: Detecting signal reboot patterns...\n');

    const rebootPatterns = [];

    for (const r of results) {
      if (r.signals_found.length < 2) continue;

      // Sort signals by days_before (largest first = earliest)
      const sorted = [...r.signals_found].sort((a, b) => b.days_before - a.days_before);

      // Look for gaps > 90 days between signals (dormant -> reboot)
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
            days_to_mandate: sorted[i + 1].days_before,
            pattern: `${sorted[i].type} -> [${gap}d dormant] -> ${sorted[i + 1].type} -> mandate`
          });
        }
      }
    }

    // ─────────────────────────────────────────────────────────
    // STEP 7b: Forward-looking analysis — signal companies
    //          that are existing clients (useful while signal
    //          history is still young, < 12mo)
    // ─────────────────────────────────────────────────────────

    console.log('Step 7b: Forward-looking — signal companies × client history...\n');

    const forwardQuery = `
      SELECT
        a.id AS account_id,
        a.name AS client_name,
        a.relationship_tier,
        c.name AS company_name,
        c.sector,
        c.country_code,
        se.signal_type,
        se.confidence_score,
        se.detected_at,
        ed.title AS doc_title,
        EXTRACT(days FROM NOW() - se.detected_at)::int AS signal_age_days,
        (SELECT COUNT(*) FROM conversions cv WHERE cv.client_id = a.id AND cv.placement_fee > 0) AS placement_count,
        (SELECT SUM(cv.placement_fee) FROM conversions cv WHERE cv.client_id = a.id AND cv.placement_fee > 0) AS total_revenue,
        (SELECT MAX(cv.start_date) FROM conversions cv WHERE cv.client_id = a.id) AS last_placement_date
      FROM accounts a
      JOIN companies c ON c.id = a.company_id
      JOIN signal_events se ON se.company_id = a.company_id
      LEFT JOIN external_documents ed ON ed.id = se.source_document_id
      WHERE a.company_id IS NOT NULL
        AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      ORDER BY se.detected_at DESC
    `;

    const forwardResult = await client.query(forwardQuery, [ML_TENANT_ID]);
    const forwardRows = forwardResult.rows;

    // Group by client
    const clientSignalMap = {};
    for (const row of forwardRows) {
      if (!clientSignalMap[row.account_id]) {
        clientSignalMap[row.account_id] = {
          client_name: row.client_name,
          company_name: row.company_name,
          sector: row.sector,
          country_code: row.country_code,
          relationship_tier: row.relationship_tier,
          placement_count: parseInt(row.placement_count) || 0,
          total_revenue: parseFloat(row.total_revenue) || 0,
          last_placement_date: row.last_placement_date,
          signals: []
        };
      }
      clientSignalMap[row.account_id].signals.push({
        type: row.signal_type,
        confidence: parseFloat(row.confidence_score),
        age_days: row.signal_age_days,
        title: (row.doc_title || '').substring(0, 100)
      });
    }

    const forwardClients = Object.values(clientSignalMap)
      .sort((a, b) => b.signals.length - a.signals.length);

    // Signal type distribution across client companies
    const clientSignalTypes = {};
    for (const fc of forwardClients) {
      for (const s of fc.signals) {
        clientSignalTypes[s.type] = (clientSignalTypes[s.type] || 0) + 1;
      }
    }

    console.log(`  Found ${forwardClients.length} existing clients with active signals\n`);

    // ─────────────────────────────────────────────────────────
    // STEP 8: Save full output
    // ─────────────────────────────────────────────────────────

    const output = {
      metadata: {
        run_date: new Date().toISOString(),
        clients_analysed: results.length,
        lookback_days: MAX_LOOKBACK_DAYS,
        overall_median_lead_days: overallMedian,
        date_range: {
          earliest_invoice: results[results.length - 1]?.first_invoice_date,
          latest_invoice: results[0]?.first_invoice_date
        }
      },

      // 1. Headline correlation
      correlation,

      // 2. Signal sleep timers (THE KEY OUTPUT)
      sleep_timers: sleepTimers,

      // 3. Compound patterns
      compound_patterns: sortedPatterns.map(([pattern, count]) => ({ pattern, count })),

      // 4. Sector timing modifiers
      sector_modifiers: sectorModifiers,

      // 5. Geography timing modifiers
      geo_modifiers: geoModifiers,

      // 6. Reboot patterns
      reboot_patterns: rebootPatterns,

      // 7. Career move correlation
      career_correlation: {
        companies_with_moves: withCareerMoves,
        rate: (withCareerMoves / results.length * 100).toFixed(1) + '%'
      },

      // 8. Per-client detail
      client_details: results,

      // 9. Forward-looking: existing clients with active signals
      forward_looking: {
        clients_with_signals: forwardClients.length,
        total_signals_on_clients: forwardRows.length,
        signal_type_distribution: clientSignalTypes,
        clients: forwardClients
      }
    };

    const reportsDir = path.join(__dirname, '..', 'reports');
    fs.mkdirSync(reportsDir, { recursive: true });

    // Full report
    fs.writeFileSync(
      path.join(reportsDir, 'signal_timing_calibration.json'),
      JSON.stringify(output, null, 2)
    );

    // Sleep timers only (for import into platform scoring)
    // If no empirical timers were calibrated, write spec-based defaults
    const effectiveTimers = Object.keys(sleepTimers).length > 0 ? sleepTimers : {
      capital_raising: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 300, median_lead_days: 300,
        peak_window: '9-12mo',
        timer: { dormant_until_days: 30, rising_from_days: 90, peak_start_days: 180, peak_end_days: 365, declining_until_days: 540, dormant_after_days: 730 }
      },
      ma_activity: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 240, median_lead_days: 240,
        peak_window: '6-9mo',
        timer: { dormant_until_days: 14, rising_from_days: 90, peak_start_days: 180, peak_end_days: 365, declining_until_days: 450, dormant_after_days: 540 }
      },
      geographic_expansion: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 420, median_lead_days: 420,
        peak_window: '12-18mo',
        timer: { dormant_until_days: 60, rising_from_days: 180, peak_start_days: 365, peak_end_days: 540, declining_until_days: 630, dormant_after_days: 730 }
      },
      leadership_change: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 90, median_lead_days: 75,
        peak_window: '0-3mo',
        timer: { dormant_until_days: 7, rising_from_days: 14, peak_start_days: 30, peak_end_days: 120, declining_until_days: 180, dormant_after_days: 270 }
      },
      restructuring: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 270, median_lead_days: 270,
        peak_window: '6-12mo',
        timer: { dormant_until_days: 30, rising_from_days: 90, peak_start_days: 180, peak_end_days: 365, declining_until_days: 450, dormant_after_days: 540 }
      },
      layoffs: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 270, median_lead_days: 270,
        peak_window: '6-12mo',
        timer: { dormant_until_days: 30, rising_from_days: 90, peak_start_days: 180, peak_end_days: 365, declining_until_days: 450, dormant_after_days: 540 }
      },
      strategic_hiring: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 150, median_lead_days: 150,
        peak_window: '3-6mo',
        timer: { dormant_until_days: 14, rising_from_days: 60, peak_start_days: 90, peak_end_days: 210, declining_until_days: 270, dormant_after_days: 365 }
      },
      product_launch: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 180, median_lead_days: 180,
        peak_window: '3-9mo',
        timer: { dormant_until_days: 14, rising_from_days: 60, peak_start_days: 90, peak_end_days: 270, declining_until_days: 365, dormant_after_days: 450 }
      },
      partnership: {
        sample_size: 0, source: 'spec_default',
        avg_lead_days: 210, median_lead_days: 210,
        peak_window: '6-9mo',
        timer: { dormant_until_days: 30, rising_from_days: 90, peak_start_days: 180, peak_end_days: 270, declining_until_days: 365, dormant_after_days: 450 }
      }
    };
    fs.writeFileSync(
      path.join(reportsDir, 'sleep_timers.json'),
      JSON.stringify(effectiveTimers, null, 2)
    );

    // Compound patterns only
    fs.writeFileSync(
      path.join(reportsDir, 'compound_patterns.json'),
      JSON.stringify(sortedPatterns, null, 2)
    );

    console.log('  Reports saved to reports/ directory\n');

    // ─────────────────────────────────────────────────────────
    // STEP 9: Console output
    // ─────────────────────────────────────────────────────────

    console.log('═══════════════════════════════════════════════════════');
    console.log('  SIGNAL TIMING INTELLIGENCE — RESULTS');
    console.log('═══════════════════════════════════════════════════════\n');

    console.log(`  Clients analysed:  ${results.length}`);
    console.log(`  Date range:        ${output.metadata.date_range.earliest_invoice} -> ${output.metadata.date_range.latest_invoice}`);
    console.log(`  Lookback window:   ${MAX_LOOKBACK_DAYS} days (${(MAX_LOOKBACK_DAYS / 30).toFixed(0)} months)\n`);

    // Correlation
    console.log('  +-------------------------------------------------+');
    console.log('  |  SIGNAL CORRELATION BY WINDOW                   |');
    console.log('  +-------------------------------------------------+');
    for (const [label, data] of Object.entries(correlation)) {
      console.log(`  |  ${label.padEnd(5)} ${data.signal_rate.padStart(6)} signals  ${data.any_signal_rate.padStart(6)} any  ${data.revenue_preceded_pct.padStart(6)} rev |`);
    }
    console.log('  +-------------------------------------------------+\n');

    // Sleep timers
    console.log('  +-------------------------------------------------+');
    console.log('  |  SIGNAL SLEEP TIMERS (calibrated from data)     |');
    console.log('  +-------------------------------------------------+');
    for (const [type, timer] of Object.entries(sleepTimers)) {
      console.log(`  |                                                 |`);
      console.log(`  |  ${type.padEnd(30)} (n=${String(timer.sample_size).padEnd(3)}) |`);
      console.log(`  |  Median lead: ${String(timer.median_lead_days).padStart(4)}d  Peak: ${timer.peak_window.padEnd(8)}        |`);
      console.log(`  |  Range: ${timer.p25_lead_days}d - ${timer.p75_lead_days}d (IQR)                  |`);
      const t = timer.timer;
      console.log(`  |  Dormant -> Rising: ${t.dormant_until_days}d                      |`);
      console.log(`  |  Rising -> Peak:    ${t.rising_from_days}d - ${t.peak_start_days}d                 |`);
      console.log(`  |  Peak:             ${t.peak_start_days}d - ${t.peak_end_days}d                 |`);
      console.log(`  |  Declining:        ${t.peak_end_days}d - ${t.declining_until_days}d                 |`);
      console.log(`  |  Dormant after:    ${t.dormant_after_days}d                       |`);
    }
    console.log('  +-------------------------------------------------+\n');

    // Compound patterns
    if (sortedPatterns.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  COMPOUND SIGNAL PATTERNS                       |');
      console.log('  +-------------------------------------------------+');
      for (const [pattern, count] of sortedPatterns.slice(0, 10)) {
        console.log(`  |  ${pattern.padEnd(40)} ${String(count).padStart(3)}x |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Sector modifiers
    if (Object.keys(sectorModifiers).length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  SECTOR TIMING MODIFIERS                        |');
      console.log('  +-------------------------------------------------+');
      for (const [sector, mod] of Object.entries(sectorModifiers).sort(([,a],[,b]) => a.median_lead_days - b.median_lead_days)) {
        console.log(`  |  ${sector.padEnd(25)} ${String(mod.median_lead_days).padStart(4)}d  ${mod.modifier}x  ${mod.interpretation.padEnd(8)} |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Reboot patterns
    if (rebootPatterns.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  SIGNAL REBOOT PATTERNS                         |');
      console.log('  +-------------------------------------------------+');
      for (const rb of rebootPatterns.slice(0, 10)) {
        console.log(`  |  ${(rb.company || rb.client).substring(0, 25).padEnd(25)}              |`);
        console.log(`  |  ${rb.pattern.substring(0, 47).padEnd(47)} |`);
      }
      console.log(`  |  Total reboot patterns detected: ${String(rebootPatterns.length).padStart(3)}            |`);
      console.log('  +-------------------------------------------------+\n');
    }

    // Forward-looking: existing clients with active signals
    if (forwardClients.length > 0) {
      console.log('  +-------------------------------------------------+');
      console.log('  |  EXISTING CLIENTS WITH ACTIVE SIGNALS            |');
      console.log('  +-------------------------------------------------+');
      console.log(`  |  ${forwardClients.length} clients, ${forwardRows.length} total signals                  |`);
      console.log('  |                                                 |');
      for (const fc of forwardClients.slice(0, 15)) {
        const types = [...new Set(fc.signals.map(s => s.type))].join(', ');
        console.log(`  |  ${(fc.company_name || fc.client_name).substring(0, 30).padEnd(30)} ${String(fc.signals.length).padStart(3)} sig |`);
        console.log(`  |    ${types.substring(0, 45).padEnd(45)} |`);
        console.log(`  |    ${fc.placement_count} placements, $${Math.round(fc.total_revenue).toLocaleString()} rev   |`);
      }
      if (forwardClients.length > 15) {
        console.log(`  |  ... and ${forwardClients.length - 15} more                              |`);
      }
      console.log('  |                                                 |');
      console.log('  |  Signal types on client companies:              |');
      for (const [type, count] of Object.entries(clientSignalTypes).sort(([,a],[,b]) => b - a)) {
        console.log(`  |    ${type.padEnd(30)} ${String(count).padStart(5)} |`);
      }
      console.log('  +-------------------------------------------------+\n');
    }

    // Key narrative
    console.log('═══════════════════════════════════════════════════════');
    console.log('  INVESTOR NARRATIVE');
    console.log('═══════════════════════════════════════════════════════\n');

    const macro = correlation['12mo'];

    console.log(`  "${macro?.any_signal_rate || 'N/A'} of our last ${results.length} client engagements`);
    console.log(`   were preceded by detectable macro signals within 12 months."\n`);

    if (Object.keys(sleepTimers).length > 0) {
      console.log('  "We have calibrated signal-specific timing models from');
      console.log(`   ${Object.values(timingData).flat().length} signal-to-mandate data points:`);
      for (const [type, timer] of Object.entries(sleepTimers)) {
        console.log(`     ${type}: peak mandate window ${timer.peak_window}, median ${timer.median_lead_days}d lead`);
      }
      console.log('   These timers are proprietary — trained on 25 years');
      console.log('   of placement data that no competitor can replicate."\n');
    }

    if (sortedPatterns.length > 0) {
      const topPattern = sortedPatterns[0];
      console.log(`  "The strongest compound signal pattern is ${topPattern[0]},`);
      console.log(`   which preceded ${topPattern[1]} mandates in our history."\n`);
    }

    // Temporal coverage note
    const signalEra = await client.query('SELECT MIN(detected_at) AS first, MAX(detected_at) AS last FROM signal_events');
    const signalFirst = signalEra.rows[0]?.first;
    const signalLast = signalEra.rows[0]?.last;
    if (signalFirst) {
      const signalDays = Math.round((new Date(signalLast) - new Date(signalFirst)) / 86400000);
      console.log('  ─── TEMPORAL COVERAGE NOTE ───');
      console.log(`  Signal collection: ${signalDays} days (${new Date(signalFirst).toISOString().slice(0, 10)} -> ${new Date(signalLast).toISOString().slice(0, 10)})`);
      if (signalDays < 365) {
        console.log(`  Signal history is ${signalDays} days old — too young for retrospective calibration.`);
        console.log('  Sleep timers use spec defaults until 12+ months of signal data accumulates.');
        console.log('  Re-run this script quarterly to calibrate as data grows.\n');
      }
    }

    console.log('═══════════════════════════════════════════════════════\n');

  } catch (err) {
    console.error('Error:', err.message);
    if (err.message.includes('does not exist')) {
      console.error('\nTable or column not found. Run schema discovery:');
      console.error("  psql $DATABASE_URL -c \"\\d accounts\"");
      console.error("  psql $DATABASE_URL -c \"\\d conversions\"");
      console.error("  psql $DATABASE_URL -c \"\\d signal_events\"");
    }
    console.error('\nFull error:', err);
  } finally {
    client.release();
    await pool.end();
  }
}

run();
