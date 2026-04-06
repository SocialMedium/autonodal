#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/backfill_ezekia_careers.js
// Batch re-fetch career_history for Ezekia people missing it
//
// Usage:
//   node scripts/backfill_ezekia_careers.js              # Run full batch
//   node scripts/backfill_ezekia_careers.js --dry-run    # Preview only
//   node scripts/backfill_ezekia_careers.js --limit=500  # Cap at N people
//   node scripts/backfill_ezekia_careers.js --batch=20   # Concurrent batch size
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { ezekiaFetch } = require('../lib/ezekia');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = '00000000-0000-0000-0000-000000000001';

// Parse CLI args
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 0;
const BATCH_SIZE = parseInt((args.find(a => a.startsWith('--batch=')) || '').split('=')[1]) || 10;
const DELAY_MS = 800; // 800ms between items — single API call per person

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Ezekia Career Backfill');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN (no writes)' : 'LIVE');
  if (LIMIT) console.log('  Limit:', LIMIT);
  console.log('  Batch size:', BATCH_SIZE);
  console.log('═══════════════════════════════════════════════════════');

  if (!process.env.EZEKIA_API_TOKEN) {
    console.error('ERROR: EZEKIA_API_TOKEN not configured');
    process.exit(1);
  }

  // 1. Find all Ezekia people missing career_history
  const { rows: candidates } = await pool.query(`
    SELECT id, source_id, full_name, current_title, current_company_name
    FROM people
    WHERE tenant_id = $1
      AND source = 'ezekia'
      AND source_id IS NOT NULL
      AND (career_history IS NULL OR career_history = 'null'::jsonb OR career_history = '[]'::jsonb)
    ORDER BY updated_at DESC NULLS LAST
    ${LIMIT ? `LIMIT ${LIMIT}` : ''}
  `, [TENANT_ID]);

  console.log(`\nFound ${candidates.length} Ezekia people without career history\n`);

  if (!candidates.length) {
    console.log('Nothing to do.');
    pool.end();
    return;
  }

  let enriched = 0, skipped = 0, errors = 0, noCareer = 0;
  let consecutiveErrors = 0;
  const startTime = Date.now();

  // Process in batches
  for (let i = 0; i < candidates.length; i += BATCH_SIZE) {
    const batch = candidates.slice(i, i + BATCH_SIZE);

    // Process batch sequentially to respect rate limits
    for (const person of batch) {
      try {
        // Single API call — only fetch positions + education (saves rate limit)
        const params = new URLSearchParams();
        ['profile.positions', 'profile.education'].forEach(f => params.append('fields[]', f));
        const ezData = await ezekiaFetch(`/api/people/${person.source_id}?${params}`);
        const raw = ezData?.data || ezData;

        if (!raw) {
          skipped++;
          consecutiveErrors = 0;
          continue;
        }

        // Extract positions
        const positions = raw.profile?.positions || [];
        if (!positions.length) {
          noCareer++;
          consecutiveErrors = 0;
          continue;
        }

        // Transform using existing logic
        const careerHistory = positions.map(pos => ({
          company: pos.company?.name,
          title: pos.title,
          location: typeof pos.location === 'object' ? pos.location?.name : pos.location,
          industry: pos.industry?.name || pos.industry?.value,
          start_date: pos.startDate,
          end_date: pos.endDate === '9999-12-31' ? null : pos.endDate,
          current: pos.primary || pos.tense || false,
          description: pos.summary,
          achievements: pos.achievements,
        })).filter(r => r.company && !String(r.company).startsWith('#<'));

        // Extract skills and industries
        const allSkills = new Set();
        positions.forEach(pos => {
          (pos.skills || []).forEach(s => { if (typeof s === 'string' && s.length > 1) allSkills.add(s); });
        });
        const industries = [...new Set(positions.map(p => p.industry?.name || p.industry?.value).filter(Boolean))];

        // Extract education
        const education = (raw.profile?.education || []).map(edu => ({
          institution: edu.school_name || edu.school,
          degree: edu.degree,
          field: edu.field_of_study,
          start_date: edu.start_date,
          end_date: edu.end_date,
        })).filter(e => e.institution);

        if (!careerHistory.length) {
          noCareer++;
          consecutiveErrors = 0;
          continue;
        }

        if (DRY_RUN) {
          console.log(`  [DRY] ${person.full_name}: ${careerHistory.length} roles, ${allSkills.size} skills, ${industries.length} industries`);
          enriched++;
        } else {
          // Build update
          const updates = [];
          const params = [person.id, TENANT_ID];
          let idx = 2;

          idx++; updates.push(`career_history = $${idx}`);
          params.push(JSON.stringify(careerHistory));

          if (allSkills.size > 0) {
            idx++; updates.push(`expertise_tags = $${idx}`);
            params.push([...allSkills].slice(0, 30));
          }

          if (industries.length > 0) {
            idx++; updates.push(`industries = $${idx}`);
            params.push(industries.slice(0, 10));
          }

          if (education.length > 0) {
            idx++; updates.push(`education = $${idx}`);
            params.push(JSON.stringify(education));
          }

          // Derive seniority from current position
          const currentRole = careerHistory.find(r => r.current) || careerHistory[0];
          if (currentRole?.title) {
            const title = currentRole.title.toLowerCase();
            let seniority = null;
            if (/\b(ceo|cto|cfo|coo|cmo|cio|cpo|chief|founder|co-founder|managing director|president)\b/.test(title)) seniority = 'c_suite';
            else if (/\b(vp|vice president|svp|evp)\b/.test(title)) seniority = 'vp';
            else if (/\b(director|head of|general manager)\b/.test(title)) seniority = 'director';
            else if (/\b(senior|lead|principal|staff)\b/.test(title)) seniority = 'senior';
            else if (/\b(manager|supervisor)\b/.test(title)) seniority = 'manager';
            if (seniority) {
              idx++; updates.push(`seniority_level = $${idx}`);
              params.push(seniority);
            }
          }

          await pool.query(
            `UPDATE people SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $2`,
            params
          );
          enriched++;
        }

        consecutiveErrors = 0;
      } catch (err) {
        errors++;
        consecutiveErrors++;
        if (err.message.includes('429') || err.message.includes('rate')) {
          console.warn(`  [RATE LIMIT] Pausing 5s...`);
          await sleep(5000);
          consecutiveErrors = 0; // Rate limit isn't a data error
        } else if (err.message.includes('404')) {
          skipped++;
          consecutiveErrors = 0;
        } else {
          console.error(`  [ERROR] ${person.full_name} (${person.source_id}): ${err.message}`);
        }

        if (consecutiveErrors >= 15) {
          console.error('\n  ABORT: 15 consecutive errors. Check API token / connectivity.');
          break;
        }
      }

      await sleep(DELAY_MS);
    }

    // Progress log every batch
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const processed = i + batch.length;
    const rate = (processed / (elapsed || 1)).toFixed(1);
    const eta = candidates.length > processed
      ? Math.round((candidates.length - processed) / rate)
      : 0;
    console.log(
      `  [${processed}/${candidates.length}] ` +
      `enriched=${enriched} no_career=${noCareer} skipped=${skipped} errors=${errors} ` +
      `(${elapsed}s, ${rate}/s, ETA ${eta}s)`
    );

    if (consecutiveErrors >= 15) break;
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  DONE');
  console.log(`  Processed: ${enriched + noCareer + skipped + errors}`);
  console.log(`  Enriched:  ${enriched} (career history added)`);
  console.log(`  No career: ${noCareer} (Ezekia has no positions)`);
  console.log(`  Skipped:   ${skipped} (404 or no data)`);
  console.log(`  Errors:    ${errors}`);
  console.log(`  Duration:  ${duration}s`);
  console.log('═══════════════════════════════════════════════════════');

  pool.end();
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

run().catch(err => {
  console.error('Fatal error:', err);
  pool.end();
  process.exit(1);
});
