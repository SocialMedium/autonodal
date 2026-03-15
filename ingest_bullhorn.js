#!/usr/bin/env node
/**
 * Bullhorn Historical Candidate Ingestion
 * 
 * Ingests 5 CSV files covering 2003-2016 (~89K candidates)
 * 
 * Two CSV formats handled:
 *   - Standard (03-13): UTF-8 BOM, 2 header rows, fields: firstName,lastName,occupation,companyName,email,mobile,category,dateAdded
 *   - Legacy (14-16):   Windows line endings, 1 header row, fields: GivenName,FamilyName,Occupation,,E-mail 1 - Value,Phone 1 - Value
 * 
 * Dedup strategy (in priority order):
 *   1. Email match against existing people
 *   2. normalised full_name + company match
 *   3. Create new record if no match
 * 
 * Usage:
 *   node ingest_bullhorn.js                  # dry run (no DB writes)
 *   node ingest_bullhorn.js --import         # live import
 *   node ingest_bullhorn.js --import --resume # skip already-processed files
 * 
 * Run from: ~/Downloads/mitchellake-signals/
 */

require('dotenv').config();
const fs   = require('fs');
const path = require('path');
const { Pool } = require('pg');

// ── Config ────────────────────────────────────────────────────────────────────

const DRY_RUN  = !process.argv.includes('--import');
const RESUME   = process.argv.includes('--resume');
const BATCH_SZ = 100;
const CHECKPOINT_FILE = './bullhorn_ingest_checkpoint.json';

const FILES = [
  { path: './candidates_03_07.csv', period: '2003-2007', format: 'standard' },
  { path: './candidates_08_09.csv', period: '2008-2009', format: 'standard' },
  { path: './candidates_10_11.csv', period: '2010-2011', format: 'standard' },
  { path: './candidates_12_13.csv', period: '2012-2013', format: 'standard' },
  { path: './candidates_14-16.csv', period: '2014-2016', format: 'legacy'   },
];

// ── DB Pool ───────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

// ── CSV Parsers ───────────────────────────────────────────────────────────────

/**
 * Parse standard Bullhorn CSV (UTF-8 BOM, 2 header rows)
 * Returns array of normalised row objects
 */
function parseStandardCSV(content) {
  // Strip BOM
  const clean = content.replace(/^\uFEFF/, '');
  const lines = clean.split('\n').map(l => l.trim()).filter(Boolean);
  
  // Row 0 = display headers, Row 1 = field names, Row 2+ = data
  const rows = [];
  for (let i = 2; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    if (cols.length < 2) continue;
    rows.push({
      firstName:   clean_str(cols[0]),
      lastName:    clean_str(cols[1]),
      occupation:  clean_str(cols[2]),
      companyName: clean_str(cols[3]),
      email:       clean_email(cols[4]),
      mobile:      clean_str(cols[5]),
      category:    clean_str(cols[6]),
      dateAdded:   clean_str(cols[7]),
    });
  }
  return rows;
}

/**
 * Parse legacy format (Windows \r line endings, 1 header row, different column names)
 */
function parseLegacyCSV(content) {
  // Normalise Windows line endings
  const clean = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = clean.split('\n').map(l => l.trim()).filter(Boolean);
  
  // Row 0 = headers, Row 1+ = data
  // Columns: GivenName, FamilyName, Occupation, CompanyName, E-mail 1 - Value, Phone 1 - Value
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = parseCSVLine(lines[i]);
    if (cols.length < 2) continue;
    rows.push({
      firstName:   clean_str(cols[0]),
      lastName:    clean_str(cols[1]),
      occupation:  clean_str(cols[2]),
      companyName: clean_str(cols[3]),
      email:       clean_email(cols[4]),
      mobile:      clean_str(cols[5]),
      category:    '',
      dateAdded:   '',
    });
  }
  return rows;
}

/** Handle quoted CSV fields correctly */
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i+1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

function clean_str(s)    { return (s || '').trim().replace(/^"|"$/g, '') || null; }
function clean_email(s)  {
  const e = clean_str(s);
  if (!e) return null;
  const lower = e.toLowerCase();
  // Basic validation
  return lower.includes('@') && lower.includes('.') ? lower : null;
}
function normalize_name(s) {
  return (s || '').toLowerCase().trim().replace(/\s+/g, ' ');
}

// ── Dedup & Upsert ────────────────────────────────────────────────────────────

/**
 * Find existing person by email (best match) or name+company (fuzzy)
 */
async function findExisting(client, row) {
  // 1. Email match — most reliable
  if (row.email) {
    const r = await client.query(
      `SELECT id, source FROM people WHERE email = $1 LIMIT 1`,
      [row.email]
    );
    if (r.rows.length) return { person: r.rows[0], matchType: 'email' };
  }

  // 2. Name + company match
  const fullName = `${row.firstName || ''} ${row.lastName || ''}`.trim();
  if (fullName.length > 3 && row.companyName) {
    const r = await client.query(
      `SELECT id, source FROM people 
       WHERE normalized_name = $1 
         AND lower(current_company_name) = lower($2)
       LIMIT 1`,
      [normalize_name(fullName), row.companyName]
    );
    if (r.rows.length) return { person: r.rows[0], matchType: 'name_company' };
  }

  // 3. Name only (lower confidence — only use if unique)
  if (fullName.length > 5) {
    const r = await client.query(
      `SELECT id, source FROM people 
       WHERE normalized_name = $1
       LIMIT 2`,
      [normalize_name(fullName)]
    );
    if (r.rows.length === 1) return { person: r.rows[0], matchType: 'name_only' };
  }

  return null;
}

async function upsertPerson(client, row, period, dryRun) {
  const fullName = `${row.firstName || ''} ${row.lastName || ''}`.trim();
  if (!fullName || fullName.length < 3) return { action: 'skipped', reason: 'no_name' };

  const existing = await findExisting(client, row);

  if (existing) {
    // Only enrich blank fields — never overwrite richer data
    if (!dryRun) {
      await client.query(`
        UPDATE people SET
          email              = COALESCE(email, $1),
          phone              = COALESCE(phone, $2),
          enrichment_data    = jsonb_set(
                                 COALESCE(enrichment_data, '{}'),
                                 '{bullhorn}',
                                 $3::jsonb,
                                 true
                               ),
          updated_at         = NOW()
        WHERE id = $4
      `, [
        row.email,
        row.mobile,
        JSON.stringify({ occupation: row.occupation, company: row.companyName, period, dateAdded: row.dateAdded, category: row.category }),
        existing.person.id
      ]);
    }
    return { action: 'matched', matchType: existing.matchType };
  }

  // Create new record
  if (!dryRun) {
    await client.query(`
      INSERT INTO people (
        full_name, first_name, last_name, normalized_name,
        current_title, current_company_name,
        email, phone,
        source, source_id,
        enrichment_data,
        status,
        created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4,
        $5, $6,
        $7, $8,
        'bullhorn_csv', $9,
        $10,
        'cold',
        NOW(), NOW()
      )
      ON CONFLICT DO NOTHING
    `, [
      fullName,
      row.firstName,
      row.lastName,
      normalize_name(fullName),
      row.occupation,
      row.companyName,
      row.email,
      row.mobile,
      // source_id: email if available, else name+company hash
      row.email || `${normalize_name(fullName)}__${(row.companyName||'').toLowerCase().trim()}`,
      JSON.stringify({ occupation: row.occupation, company: row.companyName, period, dateAdded: row.dateAdded, category: row.category }),
    ]);
  }
  return { action: 'created' };
}

// ── Checkpoint ────────────────────────────────────────────────────────────────

function loadCheckpoint() {
  try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8')); }
  catch { return { completed: [], offsets: {} }; }
}

function saveCheckpoint(cp) {
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(cp, null, 2));
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function processFile(fileInfo, checkpoint, dryRun) {
  const filePath = fileInfo.path;
  const fileName = path.basename(filePath);

  if (RESUME && checkpoint.completed.includes(fileName)) {
    console.log(`⏭️  Skipping ${fileName} (already completed)`);
    return null;
  }

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`📂 ${fileName}  [${fileInfo.period}]${dryRun ? '  DRY RUN' : ''}`);
  console.log('─'.repeat(60));

  if (!fs.existsSync(filePath)) {
    console.log(`  ❌ File not found: ${filePath}`);
    return null;
  }

  const content = fs.readFileSync(filePath, 'utf8');
  const rows = fileInfo.format === 'legacy'
    ? parseLegacyCSV(content)
    : parseStandardCSV(content);

  console.log(`  Parsed ${rows.length.toLocaleString()} rows`);

  const stats = { total: rows.length, created: 0, matched: 0, skipped: 0, errors: 0 };
  const matchTypes = { email: 0, name_company: 0, name_only: 0 };

  // Resume from saved offset if available
  const startOffset = (RESUME && checkpoint.offsets?.[fileName]) || 0;
  if (startOffset > 0) console.log(`  Resuming from row ${startOffset.toLocaleString()}`);

  for (let i = startOffset; i < rows.length; i += BATCH_SZ) {
    const batch = rows.slice(i, i + BATCH_SZ);
    let client;
    try {
      client = await pool.connect();
      for (const row of batch) {
        try {
          const result = await upsertPerson(client, row, fileInfo.period, dryRun);
          if (result.action === 'created')  stats.created++;
          if (result.action === 'matched') { stats.matched++; if (result.matchType) matchTypes[result.matchType]++; }
          if (result.action === 'skipped')  stats.skipped++;
        } catch (e) {
          stats.errors++;
          if (stats.errors <= 3) console.log(`\n  ⚠️  Row error: ${e.message}`);
        }
      }
      client.release();
    } catch (e) {
      if (client) try { client.release(true); } catch {}
      console.log(`\n  ⚠️  Connection error at row ${i}: ${e.message} — retrying in 3s...`);
      // Save offset so we can resume
      if (!checkpoint.offsets) checkpoint.offsets = {};
      checkpoint.offsets[fileName] = i;
      saveCheckpoint(checkpoint);
      await new Promise(r => setTimeout(r, 3000));
      i -= BATCH_SZ; // retry this batch
      continue;
    }

    // Progress + save offset every 1000 rows
    const done = Math.min(i + BATCH_SZ, rows.length);
    process.stdout.write(`\r  Progress: ${done.toLocaleString()}/${rows.length.toLocaleString()} — created: ${stats.created.toLocaleString()}, matched: ${stats.matched.toLocaleString()}, errors: ${stats.errors}`);
    if (done % 1000 === 0 && !dryRun) {
      if (!checkpoint.offsets) checkpoint.offsets = {};
      checkpoint.offsets[fileName] = done;
      saveCheckpoint(checkpoint);
    }
  }

  console.log(`\n`);
  console.log(`  ✅ Complete`);
  console.log(`     Created:  ${stats.created.toLocaleString()}`);
  console.log(`     Matched:  ${stats.matched.toLocaleString()} (email: ${matchTypes.email}, name+co: ${matchTypes.name_company}, name: ${matchTypes.name_only})`);
  console.log(`     Skipped:  ${stats.skipped.toLocaleString()}`);
  console.log(`     Errors:   ${stats.errors}`);

  return stats;
}

async function main() {
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║   Bullhorn Historical Candidate Ingestion                ║');
  console.log(`║   Mode: ${DRY_RUN ? 'DRY RUN (no writes)              ' : 'LIVE IMPORT                      '}    ║`);
  console.log('╚══════════════════════════════════════════════════════════╝\n');

  if (DRY_RUN) {
    console.log('⚠️  DRY RUN — no data will be written to the database.');
    console.log('   Run with --import flag to perform actual ingestion.\n');
  }

  // Quick DB connection check
  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected\n');
  } catch (e) {
    console.error('❌ Database connection failed:', e.message);
    process.exit(1);
  }

  const checkpoint = loadCheckpoint();
  const totals = { total: 0, created: 0, matched: 0, skipped: 0, errors: 0 };
  const startTime = Date.now();

  for (const fileInfo of FILES) {
    const stats = await processFile(fileInfo, checkpoint, DRY_RUN);
    if (stats) {
      totals.total   += stats.total;
      totals.created += stats.created;
      totals.matched += stats.matched;
      totals.skipped += stats.skipped;
      totals.errors  += stats.errors;

      if (!DRY_RUN) {
        checkpoint.completed.push(path.basename(fileInfo.path));
        saveCheckpoint(checkpoint);
      }
    }
  }

  const mins = ((Date.now() - startTime) / 60000).toFixed(1);
  console.log(`\n${'═'.repeat(60)}`);
  console.log('FINAL SUMMARY');
  console.log('═'.repeat(60));
  console.log(`  Total rows processed:  ${totals.total.toLocaleString()}`);
  console.log(`  New people created:    ${totals.created.toLocaleString()}`);
  console.log(`  Matched existing:      ${totals.matched.toLocaleString()}`);
  console.log(`  Skipped (no name):     ${totals.skipped.toLocaleString()}`);
  console.log(`  Errors:                ${totals.errors}`);
  console.log(`  Duration:              ${mins} minutes`);

  if (DRY_RUN) {
    console.log(`\n  To run the actual import:`);
    console.log(`  node ingest_bullhorn.js --import`);
  } else {
    console.log(`\n  Next step: embed new people into Qdrant`);
    console.log(`  node scripts/embed_people.js`);
  }

  await pool.end();
}

main().catch(e => {
  console.error('\n❌ Fatal error:', e.message);
  process.exit(1);
});