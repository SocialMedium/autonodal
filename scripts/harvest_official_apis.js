#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/harvest_official_apis.js — Structured Government/Institutional API Harvester
// Deterministic ingestion: structured JSON → external_documents → signal_events
// No LLM — all parsing is field-level extraction from known schemas
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/harvest_official_apis.js                         -- all enabled sources
//   node scripts/harvest_official_apis.js --source uk_find_a_tender  -- single source
//   node scripts/harvest_official_apis.js --source us_patentsview --days 30
//   node scripts/harvest_official_apis.js --dry-run               -- fetch, don't store
//   node scripts/harvest_official_apis.js --stats                 -- show source stats
//
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const axios = require('axios');
const { Pool } = require('pg');

const { SOURCES } = require('../lib/official_api_sources');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const RATE_LIMIT_MS = 500;  // 2 req/sec default — polite for gov APIs
const MAX_PAGES = 10;       // Max pages to follow per source per run

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ═══════════════════════════════════════════════════════════════════════════════
// COMPANY RESOLUTION
// ═══════════════════════════════════════════════════════════════════════════════

const companyCache = new Map();

async function findOrCreateCompany(name) {
  if (!name || name.length < 2) return null;
  const key = name.toLowerCase().trim();
  if (companyCache.has(key)) return companyCache.get(key);

  try {
    // Try exact match first
    const exact = await pool.query(
      `SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name.trim()]
    );
    if (exact.rows.length > 0) {
      companyCache.set(key, exact.rows[0].id);
      return exact.rows[0].id;
    }

    // Try fuzzy
    const fuzzy = await pool.query(
      `SELECT id FROM companies WHERE name ILIKE $1 OR $2 = ANY(aliases) LIMIT 1`,
      [`%${name.trim()}%`, key]
    );
    if (fuzzy.rows.length > 0) {
      companyCache.set(key, fuzzy.rows[0].id);
      return fuzzy.rows[0].id;
    }

    // Create new
    const ins = await pool.query(
      `INSERT INTO companies (name, source) VALUES ($1, 'official_api')
       ON CONFLICT DO NOTHING RETURNING id`,
      [name.trim()]
    );
    if (ins.rows.length > 0) {
      companyCache.set(key, ins.rows[0].id);
      return ins.rows[0].id;
    }

    // Refetch after conflict
    const refetch = await pool.query(
      `SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name.trim()]
    );
    if (refetch.rows.length > 0) {
      companyCache.set(key, refetch.rows[0].id);
      return refetch.rows[0].id;
    }
    return null;
  } catch {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FETCH WITH RETRY + AUTH
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchApi(url, sourceConfig) {
  const headers = {
    'User-Agent': sourceConfig.userAgent || 'MitchelLake Signal Intelligence/1.0',
    'Accept': 'application/json',
    ...(sourceConfig.parseHeaders?.() || {}),
  };

  // Add auth headers if source requires it
  if (sourceConfig.getAuthHeaders) {
    const authHeaders = sourceConfig.getAuthHeaders();
    if (!authHeaders) {
      console.log(`    ⚠️  Auth required but no key configured — skipping`);
      return null;
    }
    Object.assign(headers, authHeaders);
  }

  try {
    const resp = await axios.get(url, {
      headers,
      timeout: 30000,
      maxRedirects: 3,
      validateStatus: (s) => s < 500,
    });

    if (resp.status >= 400) {
      console.log(`    ⚠️  HTTP ${resp.status}: ${url.substring(0, 80)}`);
      return null;
    }

    return resp.data;
  } catch (e) {
    console.log(`    ❌ Fetch error: ${e.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// HARVEST SINGLE SOURCE
// ═══════════════════════════════════════════════════════════════════════════════

async function harvestSource(sourceConfig, sourceRow, options = {}) {
  const { dryRun = false, days = null } = options;
  const watermark = sourceRow.watermark || {};

  // Override watermark if --days specified
  if (days) {
    watermark.last_published = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
  }

  let url = sourceConfig.buildUrl(watermark);
  let newDocs = 0, newSignals = 0, totalFetched = 0;
  let latestPublished = watermark.last_published;

  for (let page = 0; page < MAX_PAGES && url; page++) {
    console.log(`    Page ${page + 1}: ${url.substring(0, 100)}...`);

    const data = await fetchApi(url, sourceConfig);
    if (!data) break;

    const items = sourceConfig.parseResponse(data);
    totalFetched += items.length;

    if (items.length === 0) break;

    for (const item of items) {
      // Track latest published date for watermark
      if (item.published_at && item.published_at > (latestPublished || '')) {
        latestPublished = item.published_at;
      }

      if (dryRun) {
        console.log(`      📝 [DRY] ${(item.title || '').substring(0, 80)}`);
        newDocs++;
        continue;
      }

      // Convert to document
      const doc = sourceConfig.toDocument(item);
      if (!doc || !doc.source_url_hash) continue;

      // Insert document (dedup by hash)
      const docResult = await pool.query(
        `INSERT INTO external_documents
           (source_type, source_name, source_url, source_url_hash, title, content,
            author, published_at, fetched_at, processing_status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), 'pending', NOW())
         ON CONFLICT (source_url_hash, tenant_id) DO NOTHING RETURNING id`,
        [doc.source_type, doc.source_name, doc.source_url, doc.source_url_hash,
         doc.title, doc.content, doc.author, doc.published_at ? new Date(doc.published_at) : new Date()]
      );

      if (docResult.rows.length === 0) continue; // Already exists
      const docId = docResult.rows[0].id;
      newDocs++;

      // Create signal if applicable (deterministic — no LLM)
      const signal = sourceConfig.toSignal(item, docId);
      if (signal) {
        const companyId = await findOrCreateCompany(signal.company_name);

        try {
          await pool.query(
            `INSERT INTO signal_events
               (signal_type, company_id, company_name, confidence_score,
                evidence_summary, source_url, triage_status,
                detected_at, signal_date, scoring_breakdown, created_at, updated_at)
             VALUES ($1::signal_type, $2, $3, $4, $5, $6, 'new',
                     NOW(), $7, $8, NOW(), NOW())`,
            [
              signal.signal_type,
              companyId,
              signal.company_name,
              signal.confidence_score,
              signal.evidence_summary,
              signal.source_url,
              signal.signal_date ? new Date(signal.signal_date) : new Date(),
              JSON.stringify(signal.scoring_breakdown || {}),
            ]
          );
          newSignals++;
        } catch (sigErr) {
          if (!sigErr.message.includes('duplicate')) {
            console.warn(`      ⚠️  Signal error: ${sigErr.message}`);
          }
        }
      }

      // Mark document as processed (signals computed deterministically)
      await pool.query(
        `UPDATE external_documents SET signals_computed_at = NOW(), processing_status = 'processed' WHERE id = $1`,
        [docId]
      );
    }

    // Pagination
    url = sourceConfig.buildNextUrl?.(url, data) || null;
    if (url) await sleep(RATE_LIMIT_MS);
  }

  // Update watermark + stats
  if (!dryRun) {
    const newWatermark = { ...watermark };
    if (latestPublished) newWatermark.last_published = latestPublished;

    await pool.query(`
      UPDATE official_api_sources SET
        last_fetched_at = NOW(),
        watermark = $1,
        total_fetched = total_fetched + $2,
        total_signals = total_signals + $3,
        last_error = NULL,
        consecutive_errors = 0
      WHERE source_key = $4
    `, [JSON.stringify(newWatermark), newDocs, newSignals, sourceConfig.sourceKey]);
  }

  return { newDocs, newSignals, totalFetched };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const showStats = args.includes('--stats');
  const sourceFilter = args.find((a, i) => args[i - 1] === '--source');
  const daysArg = args.find((a, i) => args[i - 1] === '--days');
  const days = daysArg ? parseInt(daysArg) : null;

  if (showStats) {
    const { rows } = await pool.query(`
      SELECT source_key, name, region, category, enabled,
             last_fetched_at, total_fetched, total_signals, consecutive_errors
      FROM official_api_sources ORDER BY source_key
    `);
    console.table(rows);
    await pool.end();
    return;
  }

  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  OFFICIAL API HARVESTER — Structured Government/Institutional Data');
  console.log('═══════════════════════════════════════════════════════════════════');
  if (dryRun) console.log('  MODE: DRY RUN — fetch only, no storage');
  console.log();

  const startTime = Date.now();

  // Get enabled sources from DB
  let whereClause = 'enabled = true';
  const params = [];
  if (sourceFilter) {
    whereClause += ' AND source_key = $1';
    params.push(sourceFilter);
  }

  const { rows: sourceRows } = await pool.query(
    `SELECT * FROM official_api_sources WHERE ${whereClause} ORDER BY last_fetched_at ASC NULLS FIRST`,
    params
  );

  console.log(`Found ${sourceRows.length} sources to harvest\n`);

  let grandDocs = 0, grandSignals = 0, errors = 0;

  for (const sourceRow of sourceRows) {
    const sourceConfig = SOURCES[sourceRow.source_key];
    if (!sourceConfig) {
      console.log(`  ⚠️  No config for source: ${sourceRow.source_key}`);
      continue;
    }

    // Skip if not due (unless --source specified or --days override)
    if (!sourceFilter && !days && sourceRow.last_fetched_at) {
      const minsSinceFetch = (Date.now() - new Date(sourceRow.last_fetched_at).getTime()) / 60000;
      if (minsSinceFetch < sourceRow.fetch_interval_minutes) {
        console.log(`  ⏭️  ${sourceRow.name} — not due (${Math.round(minsSinceFetch)}/${sourceRow.fetch_interval_minutes} min)`);
        continue;
      }
    }

    console.log(`  📡 ${sourceRow.name} (${sourceRow.region}/${sourceRow.category})`);

    try {
      const result = await harvestSource(sourceConfig, sourceRow, { dryRun, days });
      grandDocs += result.newDocs;
      grandSignals += result.newSignals;
      console.log(`    ✅ ${result.totalFetched} fetched, +${result.newDocs} new docs, +${result.newSignals} signals\n`);
    } catch (e) {
      console.log(`    ❌ ${e.message}\n`);
      errors++;
      if (!dryRun) {
        await pool.query(`
          UPDATE official_api_sources SET
            last_error = $1,
            consecutive_errors = consecutive_errors + 1,
            last_fetched_at = NOW()
          WHERE source_key = $2
        `, [(e.message || 'unknown').substring(0, 500), sourceRow.source_key]);
      }
    }

    await sleep(RATE_LIMIT_MS);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('═══════════════════════════════════════════════════════════════════');
  console.log(`  Complete: +${grandDocs} docs, +${grandSignals} signals, ${errors} errors (${duration}s)`);
  console.log('═══════════════════════════════════════════════════════════════════');

  await pool.end();
  return { docs: grandDocs, signals: grandSignals, errors, duration };
}

if (require.main === module) {
  main()
    .then(() => process.exit(0))
    .catch(err => { console.error('Fatal:', err); process.exit(1); });
}

module.exports = { harvestOfficialApis: main };
