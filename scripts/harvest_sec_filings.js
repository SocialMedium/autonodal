#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// harvest_sec_filings.js — SEC EDGAR Filing Harvester
// ═══════════════════════════════════════════════════════════════════════════════
//
// Free API, no key needed. SEC requires:
//   - User-Agent with company name + email
//   - Max 10 requests/second
//
// Harvests high-signal filing types:
//   8-K   → Material events (exec changes, M&A, restructuring, contracts)
//   S-1   → IPO filings (massive hiring signal)
//   SC13D → Activist investors (pressure = leadership change)
//   DEFA14A → Proxy fights (board changes)
//
// Usage:
//   node scripts/harvest_sec_filings.js --seed          Seed search queries
//   node scripts/harvest_sec_filings.js                 Harvest latest filings
//   node scripts/harvest_sec_filings.js --full          Full search (last 30 days)
//   node scripts/harvest_sec_filings.js --days 7        Custom lookback
//   node scripts/harvest_sec_filings.js --stats         Stats dashboard
//   node scripts/harvest_sec_filings.js --dry-run       Fetch without inserting
//
// Dependencies: dotenv, pg
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

// SEC requires User-Agent with contact info
const SEC_USER_AGENT = 'MitchelLake Group signals@mitchellake.com';
const SEC_BASE = 'https://efts.sec.gov/LATEST';
const RATE_LIMIT_MS = 150; // ~6-7 req/sec, well under 10/sec limit

// ─────────────────────────────────────────────────────────────────────────────
// HIGH-SIGNAL SEARCH QUERIES
// ─────────────────────────────────────────────────────────────────────────────
// EFTS supports boolean: AND, OR, NOT, "exact phrase", wildcards
// Each query targets a specific signal type

const SIGNAL_SEARCHES = [
  // ═══ EXECUTIVE APPOINTMENTS (8-K Item 5.02) ═══
  {
    name: 'CEO Appointments',
    query: '"appointed" AND ("chief executive" OR "CEO")',
    forms: '8-K',
    signal_type: 'leadership_change',
    confidence_base: 0.85,
    hiring_implication: 'New CEO typically triggers org restructuring, new hires at C-suite and VP level within 6-12 months',
  },
  {
    name: 'CFO Appointments',
    query: '"appointed" AND ("chief financial" OR "CFO")',
    forms: '8-K',
    signal_type: 'leadership_change',
    confidence_base: 0.85,
    hiring_implication: 'New CFO signals finance team rebuild, possible IPO prep or M&A activity',
  },
  {
    name: 'CTO/CIO Appointments',
    query: '"appointed" AND ("chief technology" OR "CTO" OR "chief information" OR "CIO")',
    forms: '8-K',
    signal_type: 'leadership_change',
    confidence_base: 0.80,
    hiring_implication: 'New CTO/CIO signals technology strategy shift, engineering leadership hiring',
  },
  {
    name: 'Executive Departures',
    query: '"resignation" OR "stepping down" OR "departure" AND ("officer" OR "executive")',
    forms: '8-K',
    signal_type: 'leadership_change',
    confidence_base: 0.80,
    hiring_implication: 'Executive departure creates immediate backfill need and potential cascade of internal promotions',
  },
  {
    name: 'Board Changes',
    query: '"board of directors" AND ("appointed" OR "elected" OR "resigned")',
    forms: '8-K',
    signal_type: 'leadership_change',
    confidence_base: 0.70,
    hiring_implication: 'Board changes may signal strategic pivot, potential exec search mandates',
  },

  // ═══ M&A ACTIVITY (8-K Item 1.01, 2.01) ═══
  {
    name: 'Acquisitions',
    query: '"acquisition" OR "acquired" OR "merger agreement" OR "definitive agreement"',
    forms: '8-K',
    signal_type: 'ma_activity',
    confidence_base: 0.85,
    hiring_implication: 'M&A creates integration leadership needs, potential redundancies, and new combined-entity roles',
  },

  // ═══ IPO FILINGS ═══
  {
    name: 'IPO Filings',
    query: '"initial public offering" OR "proposed maximum aggregate offering"',
    forms: 'S-1,S-1/A,F-1',
    signal_type: 'capital_raising',
    confidence_base: 0.90,
    hiring_implication: 'IPO filing signals massive hiring: CFO, IR, compliance, legal, board members needed',
  },

  // ═══ RESTRUCTURING (8-K Item 2.05, 2.06) ═══
  {
    name: 'Restructuring & Layoffs',
    query: '"restructuring" OR "workforce reduction" OR "cost reduction" OR "layoff"',
    forms: '8-K',
    signal_type: 'restructuring',
    confidence_base: 0.80,
    hiring_implication: 'Restructuring creates turnaround leadership needs; recovery hiring follows 3-6 months later',
  },

  // ═══ FUNDRAISING ═══
  {
    name: 'Debt/Equity Offerings',
    query: '"securities purchase agreement" OR "private placement" OR "convertible notes"',
    forms: '8-K',
    signal_type: 'capital_raising',
    confidence_base: 0.75,
    hiring_implication: 'Capital raise enables growth hiring across the organization',
  },

  // ═══ ACTIVIST INVESTORS ═══
  {
    name: 'Activist Positions',
    query: '"Schedule 13D" OR "beneficial ownership"',
    forms: 'SC 13D,SC 13D/A',
    signal_type: 'restructuring',
    confidence_base: 0.75,
    hiring_implication: 'Activist investor = pressure for board/leadership change, potential CEO search',
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

function md5(str) { return crypto.createHash('md5').update(str).digest('hex'); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function truncate(str, maxLen = 500) { return !str ? '' : str.length > maxLen ? str.slice(0, maxLen) + '...' : str; }

function formatDate(d) {
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

// ─────────────────────────────────────────────────────────────────────────────
// EDGAR EFTS API
// ─────────────────────────────────────────────────────────────────────────────

function edgarSearch(query, forms, startDate, endDate, from = 0, size = 50) {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      q: query,
      dateRange: 'custom',
      startdt: formatDate(startDate),
      enddt: formatDate(endDate),
      forms: forms,
      from: from.toString(),
      size: size.toString(),
    });

    const url = `${SEC_BASE}/search-index?${params.toString()}`;

    https.get(url, {
      headers: {
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'application/json',
      },
      timeout: 30000,
    }, (res) => {
      if (res.statusCode === 429) {
        reject(new Error('Rate limited by SEC — wait 60s'));
        return;
      }
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
          resolve(data);
        } catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
      res.on('error', reject);
    }).on('error', reject).on('timeout', function() { this.destroy(); reject(new Error('Timeout')); });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPANY LOOKUP & CREATION
// ─────────────────────────────────────────────────────────────────────────────

const companyCache = new Map();

async function findOrCreateCompany(name, ticker = null) {
  if (!name || name.length < 2) return null;
  const key = name.toLowerCase();
  if (companyCache.has(key)) return companyCache.get(key);

  // Try by name first
  let result = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);

  // Try by ticker if we have one
  if (result.rows.length === 0 && ticker) {
    result = await pool.query(`SELECT id FROM companies WHERE LOWER(ticker) = LOWER($1) LIMIT 1`, [ticker]);
  }

  if (result.rows.length > 0) {
    companyCache.set(key, result.rows[0].id);
    return result.rows[0].id;
  }

  try {
    const ins = await pool.query(
      `INSERT INTO companies (name, ticker, created_at, updated_at) VALUES ($1, $2, NOW(), NOW())
       ON CONFLICT DO NOTHING RETURNING id`,
      [name, ticker]
    );
    const id = ins.rows?.[0]?.id || null;
    if (id) companyCache.set(key, id);
    else {
      const refetch = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);
      if (refetch.rows.length > 0) { companyCache.set(key, refetch.rows[0].id); return refetch.rows[0].id; }
    }
    return id;
  } catch (e) { return null; }
}

// ─────────────────────────────────────────────────────────────────────────────
// PROCESS FILING RESULTS
// ─────────────────────────────────────────────────────────────────────────────

async function processFilings(hits, searchConfig, dryRun = false) {
  let newDocs = 0, newSignals = 0;

  for (const hit of hits) {
    const filing = hit._source || hit;
    const id = hit._id;

    // Extract fields from EFTS response
    const companyName = filing.display_names?.[0] || filing.entity_name || 'Unknown';
    const ticker = filing.tickers?.[0] || null;
    const cik = filing.ciks?.[0] || null;
    const formType = filing.form_type || filing.file_type || '';
    const filedAt = filing.file_date || filing.period_of_report || null;
    const fileUrl = filing.file_url
      ? `https://www.sec.gov/Archives/edgar/data/${filing.file_url}`
      : (id ? `https://www.sec.gov/Archives/edgar/data/${id}` : null);
    const description = filing.display_description || '';

    // Build content from available text
    const title = `${formType}: ${companyName}${ticker ? ` (${ticker})` : ''} — ${truncate(description || searchConfig.name, 100)}`;
    const content = [
      `Filing: ${formType}`,
      `Company: ${companyName}${ticker ? ` (${ticker})` : ''}`,
      `CIK: ${cik || 'N/A'}`,
      `Filed: ${filedAt || 'N/A'}`,
      description ? `Description: ${description}` : '',
      searchConfig.hiring_implication ? `Hiring implication: ${searchConfig.hiring_implication}` : '',
    ].filter(Boolean).join('\n');

    const sourceUrl = fileUrl || `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=${formType}`;
    const sourceUrlHash = md5(id || sourceUrl);

    if (dryRun) {
      console.log(`  📝 [DRY] ${title}`);
      newDocs++;
      newSignals++;
      continue;
    }

    // Insert document
    const docResult = await pool.query(
      `INSERT INTO external_documents
         (source_type, source_name, source_url, source_url_hash, title, content,
          author, published_at, fetched_at, processing_status, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), 'pending', NOW())
       ON CONFLICT (source_url_hash, tenant_id) DO NOTHING RETURNING id`,
      ['sec_filing', `SEC EDGAR (${searchConfig.name})`, sourceUrl, sourceUrlHash,
       title.slice(0, 255), content, companyName, filedAt ? new Date(filedAt) : new Date()]
    );

    if (docResult.rows.length === 0) continue;
    const docId = docResult.rows[0].id;
    newDocs++;

    // Find/create company
    const companyId = await findOrCreateCompany(companyName, ticker);

    // Create signal
    try {
      await pool.query(
        `INSERT INTO signal_events
           (signal_type, company_id, company_name, confidence_score,
            evidence_summary, evidence_snippet, evidence_snippets,
            evidence_doc_ids, source_document_id, source_url,
            triage_status, signal_category, detected_at, signal_date,
            scoring_breakdown, hiring_implications, created_at, updated_at)
         VALUES ($1::signal_type, $2, $3, $4, $5, $6, $7,
                 $8, $9, $10, 'new', $11, NOW(), $12,
                 $13, $14, NOW(), NOW())`,
        [
          searchConfig.signal_type,
          companyId,
          companyName,
          searchConfig.confidence_base,
          `${formType} filing: ${truncate(title, 200)}`,
          truncate(description || title, 500),
          JSON.stringify([description || title]),
          [docId],
          docId,
          sourceUrl,
          searchConfig.signal_type.split('_')[0],
          filedAt ? new Date(filedAt) : new Date(),
          JSON.stringify({
            form_type: formType,
            search_query: searchConfig.name,
            ticker: ticker,
            cik: cik,
          }),
          JSON.stringify({
            signal_type: searchConfig.signal_type,
            company: companyName,
            ticker: ticker,
            form_type: formType,
            implication: searchConfig.hiring_implication,
          }),
        ]
      );
      newSignals++;
    } catch (sigErr) {
      if (!sigErr.message.includes('duplicate')) {
        console.warn(`  ⚠️  Signal error: ${sigErr.message}`);
      }
    }

    // Mark doc as processed
    await pool.query(
      `UPDATE external_documents SET signals_computed_at = NOW(), processing_status = 'processed' WHERE id = $1`,
      [docId]
    );
  }

  return { newDocs, newSignals };
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN HARVEST
// ─────────────────────────────────────────────────────────────────────────────

async function harvestFilings(options = {}) {
  const { days = 7, dryRun = false, maxPerQuery = 100 } = options;

  const startDate = daysAgo(days);
  const endDate = new Date();

  console.log(`\n📡 Harvesting SEC EDGAR filings...${dryRun ? ' [DRY RUN]' : ''}`);
  console.log(`   Date range: ${formatDate(startDate)} → ${formatDate(endDate)} (${days} days)`);
  console.log(`   Searches:   ${SIGNAL_SEARCHES.length} queries\n`);

  let totalDocs = 0, totalSignals = 0, totalErrors = 0;

  for (const search of SIGNAL_SEARCHES) {
    console.log(`\n─── ${search.name} (${search.forms}) ───`);
    console.log(`  Query: ${truncate(search.query, 80)}`);

    try {
      const result = await edgarSearch(search.query, search.forms, startDate, endDate, 0, maxPerQuery);
      const total = result.hits?.total?.value || result.total || 0;
      const hits = result.hits?.hits || [];

      console.log(`  📄 ${total} total results, processing ${hits.length}`);

      if (hits.length > 0) {
        const { newDocs, newSignals } = await processFilings(hits, search, dryRun);
        console.log(`  ✅ ${newDocs} new doc(s), ${newSignals} signal(s)`);
        totalDocs += newDocs;
        totalSignals += newSignals;
      } else {
        console.log(`  ⏭️  No results`);
      }

    } catch (err) {
      totalErrors++;
      console.error(`  ❌ ${err.message}`);
      if (err.message.includes('Rate limited')) {
        console.log('  ⏳ Waiting 60s for rate limit...');
        await sleep(60000);
      }
    }

    await sleep(RATE_LIMIT_MS);
  }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`📊 SEC FILING HARVEST COMPLETE${dryRun ? ' [DRY RUN]' : ''}`);
  console.log(`   Queries:    ${SIGNAL_SEARCHES.length}`);
  console.log(`   Documents:  ${totalDocs} new`);
  console.log(`   Signals:    ${totalSignals}`);
  console.log(`   Errors:     ${totalErrors}`);
  console.log('═══════════════════════════════════════════════════\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// WATCHLIST: Monitor specific companies via EDGAR submissions API
// ─────────────────────────────────────────────────────────────────────────────

async function harvestWatchlist(options = {}) {
  const { dryRun = false } = options;

  // Get companies with CIK from our database (if any have been tagged)
  const { rows: companies } = await pool.query(`
    SELECT id, name, ticker FROM companies
    WHERE ticker IS NOT NULL AND ticker != ''
    ORDER BY name LIMIT 100
  `);

  if (companies.length === 0) {
    console.log('\n⚠️  No companies with tickers in database. Use signal search instead.\n');
    return;
  }

  console.log(`\n🔍 Checking EDGAR for ${companies.length} watchlist companies...\n`);

  let checked = 0, newFilings = 0;

  for (const company of companies) {
    try {
      // Use EFTS to search by ticker
      const result = await edgarSearch(
        `"${company.ticker}"`,
        '8-K,S-1,SC 13D',
        daysAgo(7),
        new Date(),
        0,
        10
      );

      const hits = result.hits?.hits || [];
      if (hits.length > 0) {
        console.log(`  📄 ${company.name} (${company.ticker}): ${hits.length} recent filing(s)`);
        if (!dryRun) {
          const { newDocs } = await processFilings(hits, {
            name: `Watchlist: ${company.name}`,
            signal_type: 'leadership_change',
            confidence_base: 0.7,
            hiring_implication: `Filing activity for watched company ${company.name}`,
          }, dryRun);
          newFilings += newDocs;
        }
      }
      checked++;
    } catch (err) {
      console.warn(`  ⚠️  ${company.name}: ${err.message}`);
    }

    await sleep(RATE_LIMIT_MS);
  }

  console.log(`\n  Checked: ${checked}, New filings: ${newFilings}\n`);
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS
// ─────────────────────────────────────────────────────────────────────────────

async function showStats() {
  console.log('\n📊 SEC Filing Harvester Statistics\n');

  const docs = await pool.query(`
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE processing_status = 'processed') AS processed,
           COUNT(*) FILTER (WHERE embedded_at IS NOT NULL) AS embedded,
           MIN(published_at) AS oldest, MAX(published_at) AS newest
    FROM external_documents WHERE source_type = 'sec_filing'
  `);
  const d = docs.rows[0];
  console.log(`Filings: ${d.total} total | ${d.processed} processed | ${d.embedded} embedded`);
  if (d.oldest) console.log(`  Range: ${new Date(d.oldest).toISOString().slice(0, 10)} → ${new Date(d.newest).toISOString().slice(0, 10)}`);

  const bySource = await pool.query(`
    SELECT source_name, COUNT(*) AS cnt
    FROM external_documents WHERE source_type = 'sec_filing'
    GROUP BY source_name ORDER BY cnt DESC
  `);
  console.log('\nBy search type:');
  bySource.rows.forEach(r => console.log(`  ${r.source_name.padEnd(45)} ${r.cnt}`));

  const sigs = await pool.query(`
    SELECT signal_type, COUNT(*) AS cnt, ROUND(AVG(confidence_score), 2) AS avg_conf
    FROM signal_events
    WHERE source_document_id IN (SELECT id FROM external_documents WHERE source_type = 'sec_filing')
    GROUP BY signal_type ORDER BY cnt DESC
  `);
  console.log('\nSignals from filings:');
  if (sigs.rows.length === 0) console.log('  (none yet)');
  else sigs.rows.forEach(r => console.log(`  ${r.signal_type.padEnd(25)} ${String(r.cnt).padStart(5)}  (avg: ${r.avg_conf})`));

  // Top companies by filing signals
  const topCos = await pool.query(`
    SELECT company_name, COUNT(*) AS cnt, array_agg(DISTINCT signal_type) AS types
    FROM signal_events
    WHERE source_document_id IN (SELECT id FROM external_documents WHERE source_type = 'sec_filing')
      AND company_name IS NOT NULL
    GROUP BY company_name ORDER BY cnt DESC LIMIT 20
  `);
  console.log('\nTop companies by filing signals:');
  for (const r of topCos.rows) {
    console.log(`  ${r.company_name.padEnd(35)} ${String(r.cnt).padStart(3)} signals  [${r.types.join(', ')}]`);
  }

  console.log('');
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);

  console.log('═══════════════════════════════════════════════════');
  console.log('  MitchelLake Signal Intelligence');
  console.log('  SEC EDGAR Filing Harvester');
  console.log('═══════════════════════════════════════════════════');

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');

    if (args.includes('--stats')) {
      await showStats();
    } else if (args.includes('--watchlist')) {
      await harvestWatchlist({ dryRun: args.includes('--dry-run') });
    } else {
      const daysIdx = args.indexOf('--days');
      const days = daysIdx >= 0 ? parseInt(args[daysIdx + 1])
        : args.includes('--full') ? 30 : 7;
      const maxIdx = args.indexOf('--max');
      const maxPerQuery = maxIdx >= 0 ? parseInt(args[maxIdx + 1]) : 50;

      await harvestFilings({
        days,
        dryRun: args.includes('--dry-run'),
        maxPerQuery,
      });
    }

  } catch (err) {
    console.error('\n❌ Fatal:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
