#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/enrich_companies_brightdata.js
// Enrich companies with website, LinkedIn URL, domain, industry, size
// via Bright Data Web Scraper API (Company Profile dataset)
//
// Strategy:
//   1. Query companies with most linked people but no website_url
//   2. Filter out junk entries (N/A, Self-Employed, cities, etc.)
//   3. Search LinkedIn for company by name via Bright Data
//   4. Update company records with website, linkedin_url, domain, industry
//
// Usage:
//   node scripts/enrich_companies_brightdata.js                  # Run full batch
//   node scripts/enrich_companies_brightdata.js --limit=500      # Cap at N companies
//   node scripts/enrich_companies_brightdata.js --dry-run        # Preview only
//   node scripts/enrich_companies_brightdata.js --resume=s_xxx   # Resume from snapshot
//   node scripts/enrich_companies_brightdata.js --min-people=5   # Only enrich companies with 5+ people
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const BD_API_KEY = process.env.BRIGHTDATA_API_KEY;
const BD_DATASET_ID = process.env.BD_COMPANY_DATASET_ID || 'gd_l1viktl72bvl7bjv0'; // LinkedIn Company Profile
const BD_BASE = 'https://api.brightdata.com/datasets/v3';
const BATCH_SIZE = 500;
const POLL_INTERVAL_MS = 15000;
const CHECKPOINT_FILE = './brightdata_company_enrich_checkpoint.json';

// Parse CLI args
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 0;
const MIN_PEOPLE = parseInt((args.find(a => a.startsWith('--min-people=')) || '').split('=')[1]) || 1;
const RESUME_SNAPSHOT = (args.find(a => a.startsWith('--resume=')) || '').split('=')[1] || null;

// Companies to skip — not real companies
const JUNK_NAMES = new Set([
  'n/a', 'na', 'none', 'unknown', '-', 'freelance', 'self-employed', 'self employed',
  'independent', 'consultant', 'independent consultant', 'contract', 'contractor',
  'currently employed', 'not employed', 'unemployed', 'looking', 'seeking',
  'melbourne', 'sydney', 'sydney , australia', 'australia', 'australia.',
  'brisbane', 'perth', 'singapore', 'london', 'new zealand',
  'career', 'university', 'student', 'retired', 'volunteer',
]);

function isJunkCompany(name) {
  if (!name || name.length < 3) return true;
  var lower = name.toLowerCase().trim();
  if (JUNK_NAMES.has(lower)) return true;
  // Pattern-based junk: starts with numbers, contains only special chars, etc.
  if (/^\d/.test(name) && name.length < 5) return true;
  if (/^(mr|mrs|ms|dr)\b/i.test(name)) return true;
  if (/^(the |a |an )/i.test(name) && name.length < 8) return true;
  // Skip entries that look like job titles
  if (/\b(developer|engineer|manager|director|analyst|specialist|coordinator)\b/i.test(name) && !/\b(inc|ltd|pty|corp|group|co)\b/i.test(name)) return true;
  return false;
}

async function bdFetch(path, method, body) {
  var opts = {
    method: method || 'GET',
    headers: {
      'Authorization': 'Bearer ' + BD_API_KEY,
      'Content-Type': 'application/json',
    },
  };
  if (body) opts.body = JSON.stringify(body);

  var res = await fetch(BD_BASE + path, opts);
  var text = await res.text();
  if (!res.ok) throw new Error('Bright Data API ' + res.status + ': ' + text.slice(0, 200));
  return text ? JSON.parse(text) : null;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Company Enrichment via Bright Data');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN' : 'LIVE');
  console.log('  Dataset ID:', BD_DATASET_ID);
  if (LIMIT) console.log('  Limit:', LIMIT);
  console.log('  Min people:', MIN_PEOPLE);
  if (RESUME_SNAPSHOT) console.log('  Resuming snapshot:', RESUME_SNAPSHOT);
  console.log('═══════════════════════════════════════════════════════');

  if (!BD_API_KEY) {
    console.error('ERROR: BRIGHTDATA_API_KEY not configured in .env');
    process.exit(1);
  }

  if (RESUME_SNAPSHOT) {
    await pollAndProcess(RESUME_SNAPSHOT);
    await pool.end();
    return;
  }

  // 1. Find companies worth enriching — have people linked, no website
  var { rows: candidates } = await pool.query(`
    SELECT c.id, c.name,
           COUNT(p.id) AS people_count
    FROM companies c
    LEFT JOIN people p ON p.current_company_id = c.id AND p.tenant_id = $1
    WHERE c.tenant_id = $1
      AND c.website_url IS NULL
      AND c.linkedin_url IS NULL
      AND c.name IS NOT NULL
      AND LENGTH(c.name) >= 3
    GROUP BY c.id, c.name
    HAVING COUNT(p.id) >= $2
    ORDER BY COUNT(p.id) DESC
    ${LIMIT ? 'LIMIT ' + LIMIT : ''}
  `, [TENANT_ID, MIN_PEOPLE]);

  // Filter out junk
  candidates = candidates.filter(function(c) { return !isJunkCompany(c.name); });

  console.log('\nFound ' + candidates.length + ' companies to enrich (after junk filter)');
  console.log('Top 10:');
  candidates.slice(0, 10).forEach(function(c) { console.log('  ' + c.name + ' (' + c.people_count + ' people)'); });

  if (!candidates.length) {
    console.log('Nothing to do.');
    await pool.end();
    return;
  }

  if (DRY_RUN) {
    console.log('\nDRY RUN — would submit ' + candidates.length + ' company names to Bright Data');
    console.log('Estimated cost: ~$' + (candidates.length * 0.02).toFixed(2));
    await pool.end();
    return;
  }

  // 2. Submit in batches — search by company name on LinkedIn
  for (var i = 0; i < candidates.length; i += BATCH_SIZE) {
    var batch = candidates.slice(i, i + BATCH_SIZE);
    console.log('\nSubmitting batch ' + (Math.floor(i / BATCH_SIZE) + 1) + ' (' + batch.length + ' companies)...');

    // Build input: search LinkedIn by company name
    var input = batch.map(function(c) {
      return { url: 'https://www.linkedin.com/company/' + encodeURIComponent(c.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '')) };
    });

    // Save mapping: normalised slug → company id
    var mapping = {};
    batch.forEach(function(c, idx) {
      mapping[input[idx].url] = { id: c.id, name: c.name };
    });

    try {
      var result = await bdFetch(
        '/trigger?dataset_id=' + BD_DATASET_ID + '&format=json&include_errors=true',
        'POST',
        input
      );

      var snapshotId = result.snapshot_id;
      console.log('  Snapshot ID:', snapshotId);

      fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify({
        snapshot_id: snapshotId,
        batch_start: i,
        batch_size: batch.length,
        total: candidates.length,
        mapping: mapping,
        submitted_at: new Date().toISOString(),
      }, null, 2));

      await pollAndProcess(snapshotId, mapping);
    } catch (err) {
      console.error('  Batch submission failed:', err.message);
    }
  }

  console.log('\nAll batches complete.');
  await pool.end();
}

async function pollAndProcess(snapshotId, mapping) {
  if (!mapping && fs.existsSync(CHECKPOINT_FILE)) {
    var cp = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
    if (cp.snapshot_id === snapshotId) mapping = cp.mapping;
  }

  console.log('\nPolling for results (snapshot: ' + snapshotId + ')...');

  var status = 'collecting';
  var attempts = 0;
  var maxAttempts = 120;

  while (status !== 'ready' && attempts < maxAttempts) {
    attempts++;
    await sleep(POLL_INTERVAL_MS);
    try {
      var progress = await bdFetch('/progress/' + snapshotId);
      status = progress.status || progress.state || 'unknown';
      var pct = progress.progress_percentage || progress.progress || '?';
      process.stdout.write('\r  Status: ' + status + ' (' + pct + '%) [' + attempts + '/' + maxAttempts + ']    ');
      if (status === 'failed' || status === 'error') {
        console.error('\n  Collection FAILED:', JSON.stringify(progress));
        return;
      }
    } catch (err) {
      if (err.message.includes('404')) {
        process.stdout.write('\r  Status: preparing... [' + attempts + '/' + maxAttempts + ']    ');
      }
    }
  }

  if (status !== 'ready') {
    console.log('\n  Timed out. Resume with: node scripts/enrich_companies_brightdata.js --resume=' + snapshotId);
    return;
  }

  console.log('\n  Results ready. Downloading...');

  try {
    var profiles = [];
    for (var dl = 0; dl < 10; dl++) {
      var res = await fetch(BD_BASE + '/snapshot/' + snapshotId + '?format=json', {
        headers: { 'Authorization': 'Bearer ' + BD_API_KEY },
      });
      if (res.status === 202) { await sleep(10000); continue; }
      if (!res.ok) throw new Error('Download failed: ' + res.status);
      var text = await res.text();
      profiles = text.startsWith('[') ? JSON.parse(text) : text.trim().split('\n').filter(Boolean).map(JSON.parse);
      break;
    }

    console.log('  Downloaded ' + profiles.length + ' company profiles');

    var enriched = 0, noMatch = 0, noData = 0, errors = 0;

    for (var pi = 0; pi < profiles.length; pi++) {
      var profile = profiles[pi];
      try {
        if (!profile || (!profile.url && !profile.input_url)) { noMatch++; continue; }

        // Match back to our company
        var inputUrl = profile.input_url || profile.url || '';
        var matched = mapping ? mapping[inputUrl] : null;

        // Fallback: try to match by name
        if (!matched && profile.name) {
          var { rows } = await pool.query(
            "SELECT id, name FROM companies WHERE tenant_id = $1 AND LOWER(name) = LOWER($2) LIMIT 1",
            [TENANT_ID, profile.name]
          );
          if (rows.length) matched = { id: rows[0].id, name: rows[0].name };
        }

        if (!matched) { noMatch++; continue; }

        // Extract data
        var website = profile.website || profile.company_url || null;
        var linkedinUrl = profile.url || profile.linkedin_url || null;
        var domain = null;
        if (website) {
          try { domain = new URL(website.startsWith('http') ? website : 'https://' + website).hostname.replace(/^www\./, ''); }
          catch (e) {}
        }
        var industry = profile.industry || null;
        var employeeCount = profile.company_size || profile.employees_count || profile.staff_count || null;
        var description = profile.description || profile.about || null;
        var headquarters = profile.headquarters || profile.location || null;

        if (!website && !linkedinUrl && !domain) { noData++; continue; }

        // Update company
        var updates = [];
        var updateParams = [matched.id, TENANT_ID];
        var idx = 2;

        if (website) { idx++; updates.push('website_url = COALESCE(website_url, $' + idx + ')'); updateParams.push(website.startsWith('http') ? website : 'https://' + website); }
        if (linkedinUrl) { idx++; updates.push('linkedin_url = COALESCE(linkedin_url, $' + idx + ')'); updateParams.push(linkedinUrl); }
        if (domain) { idx++; updates.push('domain = COALESCE(domain, $' + idx + ')'); updateParams.push(domain); }
        if (industry) { idx++; updates.push('industry = COALESCE(industry, $' + idx + ')'); updateParams.push(industry); }
        if (employeeCount) { idx++; updates.push('employee_count = COALESCE(employee_count, $' + idx + ')'); updateParams.push(typeof employeeCount === 'number' ? employeeCount : parseInt(String(employeeCount).replace(/[^0-9]/g, '')) || null); }
        if (description) { idx++; updates.push('description = COALESCE(description, $' + idx + ')'); updateParams.push(String(description).slice(0, 2000)); }
        if (headquarters) { idx++; updates.push('headquarters = COALESCE(headquarters, $' + idx + ')'); updateParams.push(headquarters); }

        if (!updates.length) { noData++; continue; }

        await pool.query(
          'UPDATE companies SET ' + updates.join(', ') + ', updated_at = NOW() WHERE id = $1 AND tenant_id = $2',
          updateParams
        );
        enriched++;

      } catch (err) {
        errors++;
        if (errors <= 5) console.error('  Error:', err.message.substring(0, 80));
      }
    }

    console.log('\n═══════════════════════════════════════════════════════');
    console.log('  RESULTS');
    console.log('  Profiles received: ' + profiles.length);
    console.log('  Enriched:          ' + enriched);
    console.log('  No usable data:    ' + noData);
    console.log('  No DB match:       ' + noMatch);
    console.log('  Errors:            ' + errors);
    console.log('═══════════════════════════════════════════════════════');

    if (fs.existsSync(CHECKPOINT_FILE)) fs.unlinkSync(CHECKPOINT_FILE);

  } catch (err) {
    console.error('  Download error:', err.message);
    console.log('  Resume with: node scripts/enrich_companies_brightdata.js --resume=' + snapshotId);
  }
}

run().catch(function(err) {
  console.error('Fatal:', err);
  pool.end();
  process.exit(1);
});
