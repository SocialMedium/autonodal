#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/batch_search_enrich.js
// Batch Google News enrichment for priority people + companies,
// then re-embed and re-run signal matching
//
// Usage:
//   node scripts/batch_search_enrich.js                    # Full pipeline
//   node scripts/batch_search_enrich.js --people-only      # Just people
//   node scripts/batch_search_enrich.js --companies-only   # Just companies
//   node scripts/batch_search_enrich.js --limit=50         # Cap per category
//   node scripts/batch_search_enrich.js --dry-run          # Preview
//   node scripts/batch_search_enrich.js --skip-embed       # Skip re-embedding
//   node scripts/batch_search_enrich.js --skip-match       # Skip re-matching
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { TenantDB } = require('../lib/TenantDB');
const { enrichPersonFromSearch } = require('../lib/search-enrichment');
const { enrichCompanyFromSearch } = require('../lib/company-search-enrichment');

const TENANT_ID = '00000000-0000-0000-0000-000000000001';
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const PEOPLE_ONLY = args.includes('--people-only');
const COMPANIES_ONLY = args.includes('--companies-only');
const SKIP_EMBED = args.includes('--skip-embed');
const SKIP_MATCH = args.includes('--skip-match');
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 100;
const DELAY_MS = 2000; // Pace Serper + Claude

async function run() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log('  Batch Search Enrichment Pipeline');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN' : 'LIVE');
  console.log('  Limit per category:', LIMIT);
  console.log('═══════════════════════════════════════════════════════════\n');

  if (!process.env.SERPER_API_KEY) { console.error('ERROR: SERPER_API_KEY not configured'); process.exit(1); }
  if (!process.env.ANTHROPIC_API_KEY) { console.error('ERROR: ANTHROPIC_API_KEY not configured'); process.exit(1); }

  const db = new TenantDB(TENANT_ID);
  const enrichedIds = { people: [], companies: [] };

  // ═══ PHASE 1: PEOPLE ENRICHMENT ═══
  if (!COMPANIES_ONLY) {
    console.log('── Phase 1: People Search Enrichment ──');

    // Priority: people with interactions (team knows them), then pipeline contacts, then by note count
    const { rows: people } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name,
        (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.tenant_id = $1) AS ix_count,
        (SELECT COUNT(*) FROM person_signals ps WHERE ps.person_id = p.id AND ps.tenant_id = $1) AS sig_count
      FROM people p
      WHERE p.tenant_id = $1
        AND p.full_name IS NOT NULL AND LENGTH(p.full_name) > 3
        AND (p.current_title IS NOT NULL OR p.current_company_name IS NOT NULL)
        AND (p.enriched_at IS NULL OR p.enriched_at < NOW() - INTERVAL '7 days')
      ORDER BY
        CASE WHEN EXISTS (SELECT 1 FROM interactions i WHERE i.person_id = p.id AND i.tenant_id = $1) THEN 0 ELSE 1 END,
        (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.tenant_id = $1) DESC,
        p.updated_at DESC NULLS LAST
      LIMIT $2
    `, [TENANT_ID, LIMIT]);

    console.log(`  Found ${people.length} priority people to enrich\n`);

    if (DRY_RUN) {
      people.slice(0, 15).forEach(p => console.log(`  ${p.full_name} | ${p.current_title || '-'} @ ${p.current_company_name || '-'} | ${p.ix_count} interactions`));
      console.log(`  ... and ${Math.max(0, people.length - 15)} more`);
      console.log(`  Estimated cost: ~$${(people.length * 0.012).toFixed(2)} (Serper + Claude)\n`);
    } else {
      let enriched = 0, failed = 0;
      for (let i = 0; i < people.length; i++) {
        const p = people[i];
        try {
          process.stdout.write(`  [${i + 1}/${people.length}] ${p.full_name}... `);
          const result = await enrichPersonFromSearch(db, p.id, TENANT_ID);
          if (result.enriched) {
            enrichedIds.people.push(p.id);
            enriched++;
            console.log(`✓ ${result.career_roles || 0} roles, ${result.signals_stored || 0} signals`);
          } else {
            console.log(`– ${result.reason || 'no data'}`);
          }
        } catch (e) {
          failed++;
          console.log(`✗ ${e.message}`);
        }
        await sleep(DELAY_MS);

        if ((i + 1) % 25 === 0) {
          console.log(`\n  --- People progress: ${i + 1}/${people.length} | enriched=${enriched} failed=${failed} ---\n`);
        }
      }
      console.log(`\n  People: ${enriched} enriched, ${failed} failed out of ${people.length}\n`);
    }
  }

  // ═══ PHASE 2: COMPANY ENRICHMENT ═══
  if (!PEOPLE_ONLY) {
    console.log('── Phase 2: Company Search Enrichment ──');

    // Priority: client companies first, then companies with people/signals
    const { rows: companies } = await db.query(`
      SELECT c.id, c.name, c.sector, c.is_client,
        (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) AS people_count,
        (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.tenant_id = $1) AS signal_count
      FROM companies c
      WHERE c.tenant_id = $1
        AND c.name IS NOT NULL AND LENGTH(c.name) > 2
        AND c.name NOT LIKE '#<%'
        AND (c.updated_at IS NULL OR c.updated_at < NOW() - INTERVAL '7 days')
      ORDER BY
        c.is_client DESC,
        (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.tenant_id = $1) DESC,
        (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) DESC
      LIMIT $2
    `, [TENANT_ID, LIMIT]);

    console.log(`  Found ${companies.length} priority companies to enrich\n`);

    if (DRY_RUN) {
      companies.slice(0, 15).forEach(c => console.log(`  ${c.is_client ? '[CLIENT] ' : ''}${c.name} | ${c.people_count} people, ${c.signal_count} signals`));
      console.log(`  ... and ${Math.max(0, companies.length - 15)} more`);
      console.log(`  Estimated cost: ~$${(companies.length * 0.012).toFixed(2)} (Serper + Claude)\n`);
    } else {
      let enriched = 0, failed = 0;
      for (let i = 0; i < companies.length; i++) {
        const c = companies[i];
        try {
          process.stdout.write(`  [${i + 1}/${companies.length}] ${c.name}... `);
          const result = await enrichCompanyFromSearch(db, c.id, TENANT_ID);
          if (result.enriched) {
            enrichedIds.companies.push(c.id);
            enriched++;
            console.log(`✓ ${result.signals_stored || 0} signals, ${result.leadership_updated || 0} leaders`);
          } else {
            console.log(`– ${result.reason || 'no data'}`);
          }
        } catch (e) {
          failed++;
          console.log(`✗ ${e.message}`);
        }
        await sleep(DELAY_MS);

        if ((i + 1) % 25 === 0) {
          console.log(`\n  --- Company progress: ${i + 1}/${companies.length} | enriched=${enriched} failed=${failed} ---\n`);
        }
      }
      console.log(`\n  Companies: ${enriched} enriched, ${failed} failed out of ${companies.length}\n`);
    }
  }

  if (DRY_RUN) {
    console.log('DRY RUN complete. No changes made.');
    process.exit(0);
  }

  // ═══ PHASE 3: RE-EMBED ENRICHED RECORDS ═══
  if (!SKIP_EMBED && (enrichedIds.people.length || enrichedIds.companies.length)) {
    console.log('── Phase 3: Re-embedding Enriched Records ──');
    console.log(`  ${enrichedIds.people.length} people + ${enrichedIds.companies.length} companies to re-embed`);

    try {
      // Re-embed people by running the existing embed pipeline
      if (enrichedIds.people.length) {
        // Mark as needing re-embed
        await db.query(
          'UPDATE people SET embedded_at = NULL WHERE id = ANY($1) AND tenant_id = $2',
          [enrichedIds.people, TENANT_ID]
        );
        console.log(`  Marked ${enrichedIds.people.length} people for re-embedding`);
      }

      if (enrichedIds.companies.length) {
        await db.query(
          'UPDATE companies SET embedded_at = NULL WHERE id = ANY($1) AND tenant_id = $2',
          [enrichedIds.companies, TENANT_ID]
        );
        console.log(`  Marked ${enrichedIds.companies.length} companies for re-embedding`);
      }

      // Trigger embed scripts
      const { execSync } = require('child_process');
      if (enrichedIds.people.length) {
        console.log('  Running people embed...');
        execSync('node scripts/embed_people.js 2>&1', { stdio: 'inherit', timeout: 300000 });
      }
      if (enrichedIds.companies.length) {
        console.log('  Running company embed...');
        execSync('node scripts/embed_companies.js 2>&1', { stdio: 'inherit', timeout: 300000 });
      }
      console.log('  Re-embedding complete.\n');
    } catch (e) {
      console.error('  Embedding error:', e.message);
      console.log('  Run manually: node scripts/embed_people.js && node scripts/embed_companies.js\n');
    }
  }

  // ═══ PHASE 4: RE-RUN SIGNAL MATCHING ═══
  if (!SKIP_MATCH) {
    console.log('── Phase 4: Signal Matching ──');
    try {
      const { execSync } = require('child_process');
      console.log('  Running match engine...');
      execSync('node -e "require(\'dotenv\').config(); const {MatchEngine}=require(\'./lib/platform/MatchEngine\'); new MatchEngine(\'' + TENANT_ID + '\').run().then(r => {console.log(\'  Matches:\', JSON.stringify(r)); process.exit(0)}).catch(e => {console.error(e.message); process.exit(1)})"', { stdio: 'inherit', timeout: 120000 });
      console.log('  Matching complete.\n');
    } catch (e) {
      console.error('  Match error:', e.message);
    }
  }

  console.log('═══════════════════════════════════════════════════════════');
  console.log('  PIPELINE COMPLETE');
  console.log(`  People enriched:    ${enrichedIds.people.length}`);
  console.log(`  Companies enriched: ${enrichedIds.companies.length}`);
  console.log('═══════════════════════════════════════════════════════════');

  process.exit(0);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
