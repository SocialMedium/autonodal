#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/discover_ats.js — ATS Discovery for Companies
// Crawls careers pages, detects ATS provider, registers job feeds
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/discover_ats.js                    -- process all undetected
//   node scripts/discover_ats.js --limit 500        -- process up to 500
//   node scripts/discover_ats.js --retry-errors     -- retry previously errored
//   node scripts/discover_ats.js --company-id <id>  -- test a single company

require('dotenv').config();

const db = require('../lib/db');
const { ML_TENANT_ID } = require('../lib/tenant');
const { detectATS } = require('../lib/ats_detector');

const BATCH_SIZE = 20;
const DELAY_BETWEEN_BATCHES = 2000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseArgs() {
  const args = process.argv.slice(2);
  const result = { limit: null, retryErrors: false, companyId: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--limit' && args[i + 1]) result.limit = parseInt(args[i + 1]);
    if (args[i] === '--retry-errors') result.retryErrors = true;
    if (args[i] === '--company-id' && args[i + 1]) result.companyId = args[i + 1];
  }
  return result;
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  ATS DISCOVERY — Job Feed Detection');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();

  const { limit, retryErrors, companyId } = parseArgs();
  const TENANT_ID = ML_TENANT_ID;

  let whereClause, params;
  if (companyId) {
    whereClause = 'c.id = $2';
    params = [TENANT_ID, companyId];
  } else if (retryErrors) {
    whereClause = 'c.ats_detected_at IS NOT NULL AND c.ats_type IS NULL AND c.ats_error IS NOT NULL';
    params = [TENANT_ID];
  } else {
    whereClause = 'c.ats_detected_at IS NULL AND c.website_url IS NOT NULL';
    params = [TENANT_ID];
  }

  const companies = await db.queryAll(`
    SELECT c.id, c.name, c.website_url, c.linkedin_url, c.careers_url, c.domain
    FROM companies c
    WHERE ${whereClause}
      AND c.tenant_id = $1
    ORDER BY
      CASE WHEN c.id IN (SELECT DISTINCT entity_id FROM watchlist_items WHERE entity_type = 'company') THEN 0
           WHEN c.id IN (SELECT DISTINCT company_id FROM signal_events WHERE detected_at > NOW() - INTERVAL '30 days' AND company_id IS NOT NULL) THEN 1
           ELSE 2 END,
      c.name ASC
    LIMIT ${parseInt(limit) || 99999}
  `, params);

  console.log(`Processing ${companies.length} companies...\n`);

  let detected = 0, failed = 0;
  const startTime = Date.now();

  for (let i = 0; i < companies.length; i += BATCH_SIZE) {
    const batch = companies.slice(i, i + BATCH_SIZE);

    await Promise.all(batch.map(async (company) => {
      try {
        const result = await detectATS(company);

        if (result) {
          await db.query(`
            UPDATE companies SET
              ats_type        = $1,
              ats_feed_url    = $2,
              careers_url     = $3,
              ats_detected_at = NOW(),
              ats_error       = NULL
            WHERE id = $4 AND tenant_id = $5
          `, [result.ats_type, result.ats_feed_url, result.careers_url,
              company.id, TENANT_ID]);

          // Register as an rss_source so the harvest pipeline picks it up
          await db.query(`
            INSERT INTO rss_sources (
              name, url, source_type, category,
              enabled, poll_interval_minutes, signal_priority, notes, tenant_id
            ) VALUES ($1, $2, 'jobs', 'company_jobs', true, 360, 'high', $3, $4)
            ON CONFLICT DO NOTHING
          `, [
            `${company.name} — Jobs (${result.ats_type})`,
            result.ats_feed_url,
            `company_id:${company.id} ats:${result.ats_type}`,
            TENANT_ID,
          ]);

          detected++;
          console.log(`  ✓ ${company.name} → ${result.ats_type}: ${result.ats_feed_url}`);
        } else {
          await db.query(`
            UPDATE companies SET
              ats_detected_at = NOW(),
              ats_error = 'no_ats_detected'
            WHERE id = $1 AND tenant_id = $2
          `, [company.id, TENANT_ID]);
          failed++;
        }
      } catch (e) {
        await db.query(`
          UPDATE companies SET
            ats_detected_at = NOW(),
            ats_error = $1
          WHERE id = $2 AND tenant_id = $3
        `, [(e.message || 'unknown').substring(0, 200), company.id, TENANT_ID]);
        failed++;
      }
    }));

    const progress = Math.min(i + BATCH_SIZE, companies.length);
    console.log(`Processed ${progress}/${companies.length} | Detected: ${detected} | No ATS: ${failed}`);

    if (i + BATCH_SIZE < companies.length) {
      await sleep(DELAY_BETWEEN_BATCHES);
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log();
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log(`  Complete: ${detected} ATS feeds discovered, ${failed} no ATS found (${duration}s)`);
  console.log('═══════════════════════════════════════════════════════════════════');

  // Summary by ATS type
  const summary = await db.queryAll(`
    SELECT ats_type, COUNT(*) AS count
    FROM companies
    WHERE tenant_id = $1 AND ats_detected_at IS NOT NULL AND ats_type IS NOT NULL
    GROUP BY ats_type
    ORDER BY count DESC
  `, [TENANT_ID]);
  if (summary.length) console.table(summary);

  return { detected, failed, duration };
}

if (require.main === module) {
  main()
    .then(() => process.exit(0))
    .catch(err => { console.error('Fatal:', err); process.exit(1); });
}

module.exports = { discoverATS: main };
