#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/enrich_linkedin_brightdata.js
// Enrich people with LinkedIn career data via Bright Data Web Scraper API
//
// Flow:
//   1. Query people with linkedin_url but no career_history
//   2. Submit URLs in batches to Bright Data (async trigger)
//   3. Poll for results
//   4. Update people records with career, skills, education
//
// Usage:
//   node scripts/enrich_linkedin_brightdata.js                # Run full batch
//   node scripts/enrich_linkedin_brightdata.js --limit=100    # Cap at N people
//   node scripts/enrich_linkedin_brightdata.js --dry-run      # Preview only
//   node scripts/enrich_linkedin_brightdata.js --resume=s_xxx # Resume from snapshot
//   node scripts/enrich_linkedin_brightdata.js --source=linkedin_import  # Filter by source
//   node scripts/enrich_linkedin_brightdata.js --all                   # Include people who already have career (refresh titles)
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = '00000000-0000-0000-0000-000000000001';
const BD_API_KEY = process.env.BRIGHTDATA_API_KEY;
const BD_DATASET_ID = 'gd_l1viktl72bvl7bjuj0'; // LinkedIn People Profile
const BD_BASE = 'https://api.brightdata.com/datasets/v3';
const BATCH_SIZE = 1000; // Bright Data handles large batches well
const POLL_INTERVAL_MS = 15000; // 15s between status checks
const CHECKPOINT_FILE = './brightdata_enrich_checkpoint.json';

// Parse CLI args
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 0;
const RESUME_SNAPSHOT = (args.find(a => a.startsWith('--resume=')) || '').split('=')[1] || null;
const SOURCE_FILTER = (args.find(a => a.startsWith('--source=')) || '').split('=')[1] || null;

async function bdFetch(path, method = 'GET', body = null) {
  const opts = {
    method,
    headers: {
      'Authorization': `Bearer ${BD_API_KEY}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${BD_BASE}${path}`, opts);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Bright Data API ${res.status}: ${text.slice(0, 200)}`);
  }

  return text ? JSON.parse(text) : null;
}

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  LinkedIn Enrichment via Bright Data');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN' : 'LIVE');
  if (LIMIT) console.log('  Limit:', LIMIT);
  if (SOURCE_FILTER) console.log('  Source filter:', SOURCE_FILTER);
  if (RESUME_SNAPSHOT) console.log('  Resuming snapshot:', RESUME_SNAPSHOT);
  console.log('═══════════════════════════════════════════════════════');

  if (!BD_API_KEY) {
    console.error('ERROR: BRIGHTDATA_API_KEY not configured in .env');
    process.exit(1);
  }

  // If resuming, skip straight to polling
  if (RESUME_SNAPSHOT) {
    await pollAndProcess(RESUME_SNAPSHOT);
    pool.end();
    return;
  }

  // 1. Find people with LinkedIn URLs (all — refresh titles/companies + backfill career)
  let sourceWhere = '';
  const params = [TENANT_ID];
  if (SOURCE_FILTER) {
    sourceWhere = ' AND p.source = $2';
    params.push(SOURCE_FILTER);
  }

  const REFRESH_ALL = args.includes('--all');

  const { rows: candidates } = await pool.query(`
    SELECT p.id, p.full_name, p.linkedin_url, p.source,
           (p.career_history IS NOT NULL AND p.career_history != 'null'::jsonb AND p.career_history != '[]'::jsonb) AS has_career
    FROM people p
    WHERE p.tenant_id = $1
      AND p.linkedin_url IS NOT NULL
      AND p.linkedin_url != ''
      ${!REFRESH_ALL ? "AND (p.career_history IS NULL OR p.career_history = 'null'::jsonb OR p.career_history = '[]'::jsonb)" : ''}
      AND (p.enriched_at IS NULL OR p.enriched_at < NOW() - INTERVAL '1 day')
      ${sourceWhere}
    ORDER BY
      CASE WHEN p.career_history IS NULL OR p.career_history = 'null'::jsonb OR p.career_history = '[]'::jsonb THEN 0 ELSE 1 END,
      CASE p.source
        WHEN 'linkedin_import' THEN 1
        WHEN 'linkedin_import_pending' THEN 2
        WHEN 'google_contacts' THEN 3
        WHEN 'bullhorn_csv' THEN 4
        ELSE 5
      END,
      p.updated_at DESC NULLS LAST
    ${LIMIT ? `LIMIT ${LIMIT}` : ''}
  `, params);

  console.log(`\nFound ${candidates.length} people with LinkedIn URLs and no career history`);

  if (!candidates.length) {
    console.log('Nothing to do.');
    pool.end();
    return;
  }

  // Source breakdown
  const bySource = {};
  candidates.forEach(c => { bySource[c.source] = (bySource[c.source] || 0) + 1; });
  console.log('By source:', bySource);

  if (DRY_RUN) {
    console.log('\nDRY RUN — would submit', candidates.length, 'LinkedIn URLs to Bright Data');
    console.log('Estimated cost: ~$' + (candidates.length * 0.015).toFixed(2));
    console.log('Sample URLs:');
    candidates.slice(0, 5).forEach(c => console.log('  ', c.full_name, ':', c.linkedin_url));
    pool.end();
    return;
  }

  // 2. Submit in batches
  for (let i = 0; i < candidates.length; i += BATCH_SIZE) {
    const batch = candidates.slice(i, i + BATCH_SIZE);
    console.log(`\nSubmitting batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} URLs)...`);

    // Build input array — map person ID into the request for matching later
    const input = batch.map(c => ({ url: c.linkedin_url }));

    // Save mapping for later
    const mapping = {};
    batch.forEach(c => {
      // Normalize LinkedIn URL for matching
      const key = normalizeLinkedInUrl(c.linkedin_url);
      mapping[key] = c.id;
    });

    try {
      const result = await bdFetch(
        `/trigger?dataset_id=${BD_DATASET_ID}&format=json&include_errors=true`,
        'POST',
        input
      );

      const snapshotId = result.snapshot_id;
      console.log('  Snapshot ID:', snapshotId);

      // Save checkpoint
      const checkpoint = {
        snapshot_id: snapshotId,
        batch_start: i,
        batch_size: batch.length,
        total: candidates.length,
        mapping,
        submitted_at: new Date().toISOString(),
      };
      fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
      console.log('  Checkpoint saved to', CHECKPOINT_FILE);

      // 3. Poll for results
      await pollAndProcess(snapshotId, mapping);

    } catch (err) {
      console.error('  Batch submission failed:', err.message);
      // Save what we have and continue
      continue;
    }
  }

  console.log('\nAll batches complete.');
  pool.end();
}

async function pollAndProcess(snapshotId, mapping = null) {
  // Load mapping from checkpoint if not provided
  if (!mapping && fs.existsSync(CHECKPOINT_FILE)) {
    const cp = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
    if (cp.snapshot_id === snapshotId) {
      mapping = cp.mapping;
    }
  }

  console.log('\nPolling for results (snapshot:', snapshotId, ')...');

  // Poll until ready
  let status = 'collecting';
  let attempts = 0;
  const maxAttempts = 120; // 30 minutes max

  while (status !== 'ready' && attempts < maxAttempts) {
    attempts++;
    await sleep(POLL_INTERVAL_MS);

    try {
      const progress = await bdFetch(`/progress/${snapshotId}`);
      status = progress.status || progress.state || 'unknown';
      const pct = progress.progress_percentage || progress.progress || '?';
      process.stdout.write(`\r  Status: ${status} (${pct}%) [${attempts}/${maxAttempts}]    `);

      if (status === 'failed' || status === 'error') {
        console.error('\n  Collection FAILED:', JSON.stringify(progress));
        return;
      }
    } catch (err) {
      // Some status codes are normal during collection
      if (err.message.includes('404')) {
        process.stdout.write(`\r  Status: preparing... [${attempts}/${maxAttempts}]    `);
      } else {
        console.error('\n  Poll error:', err.message);
      }
    }
  }

  if (status !== 'ready') {
    console.log('\n  Timed out waiting for results. Resume later with:');
    console.log(`  node scripts/enrich_linkedin_brightdata.js --resume=${snapshotId}`);
    return;
  }

  console.log('\n  Results ready. Downloading...');

  // 4. Download results (may return 202 while preparing — retry)
  try {
    let profiles = [];
    for (let dl = 0; dl < 10; dl++) {
      const res = await fetch(`${BD_BASE}/snapshot/${snapshotId}?format=json`, {
        headers: { 'Authorization': `Bearer ${BD_API_KEY}` },
      });
      if (res.status === 202) {
        console.log('  Download preparing... retrying in 10s');
        await sleep(10000);
        continue;
      }
      if (!res.ok) throw new Error(`Download failed: ${res.status}`);
      const text = await res.text();
      // Could be JSON array or NDJSON
      if (text.startsWith('[')) {
        profiles = JSON.parse(text);
      } else {
        profiles = text.trim().split('\n').filter(l => l.trim()).map(l => JSON.parse(l));
      }
      break;
    }
    console.log(`  Downloaded ${profiles.length} profiles`);

    // 5. Process and update
    let enriched = 0, partial = 0, noMatch = 0, noCareer = 0, errors = 0;

    for (const profile of profiles) {
      try {
        if (!profile || !profile.url) { noMatch++; continue; }

        const key = normalizeLinkedInUrl(profile.url || profile.input_url || '');
        let personId = mapping ? mapping[key] : null;

        // Fallback: match by LinkedIn URL in DB
        if (!personId) {
          const { rows } = await pool.query(
            `SELECT id FROM people WHERE tenant_id = $1 AND linkedin_url ILIKE $2 LIMIT 1`,
            [TENANT_ID, '%' + key + '%']
          );
          if (rows.length) personId = rows[0].id;
        }

        if (!personId) { noMatch++; continue; }

        // Extract career history (experience field — may be empty for private profiles)
        const experience = profile.experience || [];
        const careerHistory = [];
        for (const exp of experience) {
          // Bright Data nests grouped roles under positions[]
          if (exp.positions && exp.positions.length) {
            for (const pos of exp.positions) {
              careerHistory.push({
                title: pos.subtitle || pos.title || exp.title,
                company: exp.company || exp.title,
                location: exp.location || pos.location,
                start_date: pos.meta ? extractDate(pos.meta, 'start') : null,
                end_date: pos.meta ? extractDate(pos.meta, 'end') : null,
                current: pos.meta ? /present/i.test(pos.meta) : false,
                description: pos.description || null,
              });
            }
          } else {
            careerHistory.push({
              title: exp.title,
              company: exp.company || exp.company_name,
              location: exp.location,
              start_date: exp.start_date || null,
              end_date: exp.end_date || null,
              current: !exp.end_date || /present/i.test(exp.end_date || ''),
              description: exp.description || null,
            });
          }
        }

        // Extract education
        const eduRawField = profile.education || profile.educations_details || [];
        const eduRaw = Array.isArray(eduRawField) ? eduRawField : [];
        const education = eduRaw.map(e => ({
          institution: e.title || e.school || e.institution || e.school_name,
          degree: e.degree || e.degree_name,
          field: e.field || e.field_of_study,
          start_year: e.start_year,
          end_year: e.end_year,
        })).filter(e => e.institution);

        // Extract skills
        const skills = (profile.skills || [])
          .map(s => typeof s === 'string' ? s : s.name || s.skill)
          .filter(Boolean)
          .slice(0, 30);

        // Build update — take whatever data we got
        const updates = [];
        const updateParams = [personId, TENANT_ID];
        let idx = 2;

        if (careerHistory.length) {
          idx++; updates.push(`career_history = $${idx}`);
          updateParams.push(JSON.stringify(careerHistory));
        }
        if (education.length) {
          idx++; updates.push(`education = $${idx}`);
          updateParams.push(JSON.stringify(education));
        }
        if (skills.length) {
          idx++; updates.push(`expertise_tags = $${idx}`);
          updateParams.push(skills);
        }
        // Refresh current company name from LinkedIn (COALESCE — don't overwrite good data with null)
        const currentCo = typeof profile.current_company === 'object' ? profile.current_company : null;
        if (currentCo?.name) {
          idx++; updates.push(`current_company_name = $${idx}`);
          updateParams.push(currentCo.name);
        }
        // Title from career history current role (Bright Data doesn't have a top-level title field)
        if (careerHistory.length) {
          const currentRole = careerHistory.find(r => r.current) || careerHistory[0];
          if (currentRole?.title) {
            idx++; updates.push(`current_title = $${idx}`);
            updateParams.push(currentRole.title);
          }
        }
        // Update name fields if missing
        if (profile.first_name) {
          idx++; updates.push(`first_name = COALESCE(first_name, $${idx})`);
          updateParams.push(profile.first_name);
        }
        if (profile.last_name) {
          idx++; updates.push(`last_name = COALESCE(last_name, $${idx})`);
          updateParams.push(profile.last_name);
        }
        // Update location/city — always take LinkedIn's as it's more current
        if (profile.city) {
          idx++; updates.push(`location = $${idx}`);
          updateParams.push(profile.city);
        }
        if (profile.country_code) {
          idx++; updates.push(`country = COALESCE(country, $${idx})`);
          updateParams.push(profile.country_code);
        }
        // Update about/bio if we got it and existing is empty
        if (profile.about && profile.about.length > 10) {
          idx++; updates.push(`bio = COALESCE(NULLIF(bio, ''), $${idx})`);
          updateParams.push(profile.about);
        }

        // Derive seniority from best available title
        const titleSrc = careerHistory.length
          ? (careerHistory.find(r => r.current) || careerHistory[0])?.title
          : (profile.position || null);
        if (titleSrc) {
          const t = titleSrc.toLowerCase();
          let sen = null;
          if (/\b(ceo|cto|cfo|coo|cmo|cio|cpo|chief|founder|co-founder|managing director|president)\b/.test(t)) sen = 'c_suite';
          else if (/\b(vp|vice president|svp|evp)\b/.test(t)) sen = 'vp';
          else if (/\b(director|head of|general manager)\b/.test(t)) sen = 'director';
          else if (/\b(senior|lead|principal|staff)\b/.test(t)) sen = 'senior';
          else if (/\b(manager|supervisor)\b/.test(t)) sen = 'manager';
          if (sen) { idx++; updates.push(`seniority_level = COALESCE(seniority_level, $${idx})`); updateParams.push(sen); }
        }

        // Extract industry from profile
        const industry = profile.industry || (typeof profile.current_company === 'object' ? profile.current_company?.industry : null);
        if (industry) {
          idx++; updates.push(`industries = array_cat(COALESCE(industries, '{}'), $${idx})`);
          updateParams.push([industry]);
        }

        if (!updates.length) { noCareer++; continue; }

        await pool.query(
          `UPDATE people SET ${updates.join(', ')}, enriched_at = NOW(), updated_at = NOW() WHERE id = $1 AND tenant_id = $2`,
          updateParams
        );
        if (careerHistory.length) enriched++;
        else partial++; // Got education/bio/location but no career

      } catch (err) {
        errors++;
        console.error('  Profile processing error:', err.message);
      }
    }

    console.log('\n═══════════════════════════════════════════════════════');
    console.log('  RESULTS');
    console.log(`  Profiles received: ${profiles.length}`);
    console.log(`  Enriched (career): ${enriched}`);
    console.log(`  Partial (edu/bio): ${partial}`);
    console.log(`  No usable data:    ${noCareer}`);
    console.log(`  No DB match:       ${noMatch}`);
    console.log(`  Errors:            ${errors}`);
    console.log('═══════════════════════════════════════════════════════');

    // Clean up checkpoint
    if (fs.existsSync(CHECKPOINT_FILE)) fs.unlinkSync(CHECKPOINT_FILE);

  } catch (err) {
    console.error('  Download/process error:', err.message);
    console.log('  Resume later with:');
    console.log(`  node scripts/enrich_linkedin_brightdata.js --resume=${snapshotId}`);
  }
}

function normalizeLinkedInUrl(url) {
  // Extract the /in/username part, lowercased, no trailing slash
  const match = String(url).match(/linkedin\.com\/in\/([^/?#]+)/i);
  return match ? match[1].toLowerCase().replace(/\/$/, '') : String(url).toLowerCase();
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Extract dates from Bright Data's "Jan 2023 - Present 3 years 4 months" meta strings
function extractDate(meta, which) {
  if (!meta) return null;
  // Format: "Jan 2023 - Present 3 years" or "Mar 2019 - Dec 2022 3 years 9 months"
  const parts = meta.split(' - ');
  if (which === 'start' && parts[0]) {
    const m = parts[0].trim().match(/([A-Za-z]+)\s+(\d{4})/);
    if (m) return `${m[2]}-01-01`; // Approximate to year
    const y = parts[0].trim().match(/(\d{4})/);
    if (y) return `${y[1]}-01-01`;
  }
  if (which === 'end' && parts[1]) {
    if (/present/i.test(parts[1])) return null;
    const m = parts[1].trim().match(/([A-Za-z]+)\s+(\d{4})/);
    if (m) return `${m[2]}-01-01`;
    const y = parts[1].trim().match(/(\d{4})/);
    if (y) return `${y[1]}-01-01`;
  }
  return null;
}

run().catch(err => {
  console.error('Fatal error:', err);
  pool.end();
  process.exit(1);
});
