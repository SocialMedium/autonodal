#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/backfill_career_from_signals.js
// Scan existing documents for career data and enrich people records
//
// Usage:
//   node scripts/backfill_career_from_signals.js              # All unprocessed docs
//   node scripts/backfill_career_from_signals.js --limit=50   # Cap at N docs
//   node scripts/backfill_career_from_signals.js --dry-run    # Preview only
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { TenantDB } = require('../lib/TenantDB');
const { enrichCareersFromDocument } = require('../lib/career-enrichment');

const TENANT_ID = '00000000-0000-0000-0000-000000000001';

const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 0;
const DELAY_MS = 1500; // Pace Claude calls

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Career Enrichment from Signal Documents');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN' : 'LIVE');
  if (LIMIT) console.log('  Limit:', LIMIT);
  console.log('═══════════════════════════════════════════════════════');

  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('ERROR: ANTHROPIC_API_KEY not configured');
    process.exit(1);
  }

  const db = new TenantDB(TENANT_ID);

  // Find documents likely to contain career data
  // Priority: articles mentioning appointments, joins, promoted, CFO, CEO, etc.
  const { rows: docs } = await db.query(`
    SELECT id, title, source_name, LENGTH(content) as content_len, source_url
    FROM external_documents
    WHERE tenant_id = $1
      AND processing_status != 'career_enriched'
      AND content IS NOT NULL
      AND (
        title ILIKE '%appoint%'
        OR title ILIKE '%named%'
        OR title ILIKE '%joins%'
        OR title ILIKE '%hire%'
        OR title ILIKE '%promoted%'
        OR title ILIKE '%chief%'
        OR title ILIKE '%CEO%'
        OR title ILIKE '%CFO%'
        OR title ILIKE '%CTO%'
        OR title ILIKE '%COO%'
        OR title ILIKE '%CMO%'
        OR title ILIKE '%president%'
        OR title ILIKE '%director%'
        OR title ILIKE '%vice president%'
        OR title ILIKE '%VP %'
        OR title ILIKE '%board of%'
        OR title ILIKE '%leadership%'
        OR title ILIKE '%executive%'
        OR content ILIKE '%previously served%'
        OR content ILIKE '%brings % years%'
        OR content ILIKE '%prior to joining%'
        OR content ILIKE '%formerly%'
      )
    ORDER BY published_at DESC NULLS LAST
    ${LIMIT ? `LIMIT ${LIMIT}` : ''}
  `, [TENANT_ID]);

  console.log(`\nFound ${docs.length} documents with career-relevant content\n`);

  if (!docs.length) {
    console.log('Nothing to process.');
    process.exit(0);
  }

  if (DRY_RUN) {
    docs.slice(0, 20).forEach(d => {
      console.log(`  [${d.content_len || '?'} chars] ${d.title}`);
      console.log(`    Source: ${d.source_name} | URL: ${(d.source_url || '').substring(0, 80)}`);
    });
    console.log(`\nWould process ${docs.length} documents`);
    console.log(`Estimated Claude cost: ~$${(docs.length * 0.003).toFixed(2)} (Sonnet)`);
    process.exit(0);
  }

  let totalEnriched = 0, totalCreated = 0, totalSkipped = 0, errors = 0;
  const startTime = Date.now();

  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    try {
      process.stdout.write(`  [${i + 1}/${docs.length}] ${(doc.title || '').substring(0, 60)}... `);
      const result = await enrichCareersFromDocument(db, doc.id, TENANT_ID);
      totalEnriched += result.enriched || 0;
      totalCreated += result.created || 0;
      totalSkipped += result.skipped || 0;
      console.log(`→ ${result.people_found || 0} people, ${result.enriched || 0} enriched, ${result.created || 0} created`);
    } catch (err) {
      errors++;
      console.log(`→ ERROR: ${err.message}`);
    }

    // Rate limit Claude
    await new Promise(r => setTimeout(r, DELAY_MS));

    // Progress summary every 25 docs
    if ((i + 1) % 25 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      console.log(`\n  --- Progress: ${i + 1}/${docs.length} | enriched=${totalEnriched} created=${totalCreated} errors=${errors} (${elapsed}s) ---\n`);
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  DONE');
  console.log(`  Documents processed: ${docs.length}`);
  console.log(`  People enriched:     ${totalEnriched}`);
  console.log(`  People created:      ${totalCreated}`);
  console.log(`  Skipped:             ${totalSkipped}`);
  console.log(`  Errors:              ${errors}`);
  console.log(`  Duration:            ${duration}s`);
  console.log('═══════════════════════════════════════════════════════');

  process.exit(0);
}

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
