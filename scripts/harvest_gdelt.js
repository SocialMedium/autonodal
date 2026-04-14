#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/harvest_gdelt.js — GDELT Global News Intelligence Harvester
//
// Queries GDELT DOC 2.0 API for each signal type, deduplicates against
// existing documents, inserts new items, and extracts signals.
//
// Runs every 15 minutes via scheduler. Free, no auth, 100+ languages.
//
// Usage:
//   node scripts/harvest_gdelt.js              # Full harvest
//   node scripts/harvest_gdelt.js --dry-run    # Query only, don't insert
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const crypto = require('crypto');
const { Pool } = require('pg');
const { queryGDELT, SIGNAL_QUERIES } = require('../lib/gdelt_client');
const { extractSignalFromGDELT, buildGDELTEvidence } = require('../lib/gdelt_signal_extractor');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const DRY_RUN = process.argv.includes('--dry-run');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Signal type → GDELT query configuration
// Consolidated: 1 query per signal type to stay within GDELT rate limits
// Total: 6 queries × 6s delay = ~36s per run
const HARVEST_CONFIGS = [
  {
    signal_type: 'geographic_expansion',
    timespan: '2h',
    maxrecords: 75,
    queries: ['"country manager" OR "regional director" OR "expands to" OR "opens office" OR "market entry"'],
  },
  {
    signal_type: 'capital_raising',
    timespan: '2h',
    maxrecords: 75,
    queries: ['"series A" OR "series B" OR "series C" OR "funding round" OR "raised" OR "IPO"'],
  },
  {
    signal_type: 'ma_activity',
    timespan: '2h',
    maxrecords: 75,
    queries: ['"acquires" OR "acquisition" OR "merger" OR "merges with" OR "takeover" OR "buyout"'],
  },
  {
    signal_type: 'leadership_change',
    timespan: '2h',
    maxrecords: 75,
    queries: ['"appointed" OR "named CEO" OR "named CFO" OR "named CTO" OR "resigns" OR "steps down"'],
  },
  {
    signal_type: 'restructuring',
    timespan: '2h',
    maxrecords: 50,
    queries: ['"layoffs" OR "redundancies" OR "job cuts" OR "restructuring" OR "downsizing"'],
  },
  {
    signal_type: 'product_launch',
    timespan: '2h',
    maxrecords: 50,
    queries: ['"product launch" OR "launches" OR "introduces" OR "unveils"'],
  },
];

async function matchCompanyFromTitle(title, domain) {
  if (!title) return null;

  // Extract capitalised multi-word phrases as company name candidates
  var candidates = title.match(/\b[A-Z][A-Za-z]+(?:\s[A-Z][A-Za-z]+){0,2}\b/g) || [];
  var stopwords = ['The', 'New', 'Says', 'How', 'Why', 'What', 'CEO', 'CFO', 'CTO', 'Has', 'Will', 'For', 'And', 'But',
    'Inc', 'Ltd', 'Corp', 'Global', 'Group', 'World', 'First', 'Next', 'One', 'All', 'Just', 'Most', 'Top',
    'Native', 'Level', 'Signal', 'Standard', 'Prime', 'Rise', 'Core', 'Apex', 'Edge', 'Nova', 'Alpha', 'Beta',
    'Bold', 'True', 'Clear', 'Bright', 'Pure', 'Open', 'Live', 'Real', 'Fast', 'Smart'];

  for (var i = 0; i < Math.min(candidates.length, 5); i++) {
    var c = candidates[i];
    if (c.length < 3 || stopwords.includes(c)) continue;
    // For short names (<=5 chars), only exact match to avoid false positives
    var r;
    if (c.length <= 5) {
      r = await pool.query(
        "SELECT id, name FROM companies WHERE tenant_id = $1 AND LOWER(name) = LOWER($2) LIMIT 1",
        [TENANT_ID, c]
      );
    } else {
      r = await pool.query(
        "SELECT id, name FROM companies WHERE tenant_id = $1 AND (LOWER(name) = LOWER($2) OR name ILIKE $3) LIMIT 1",
        [TENANT_ID, c, c + '%']
      );
    }
    if (r.rows.length > 0) return r.rows[0];
  }

  // Domain-based fallback
  if (domain) {
    var slug = domain.replace(/^www\./, '').split('.')[0];
    var r2 = await pool.query(
      "SELECT id, name FROM companies WHERE tenant_id = $1 AND (domain ILIKE $2 OR LOWER(name) LIKE $3) LIMIT 1",
      [TENANT_ID, '%' + domain + '%', '%' + slug + '%']
    );
    if (r2.rows.length > 0) return r2.rows[0];
  }

  return null;
}

async function harvestGDELT() {
  console.log('[GDELT] Starting harvest at ' + new Date().toISOString());
  if (DRY_RUN) console.log('[GDELT] DRY RUN — no inserts');

  var totalNew = 0, totalDuplicates = 0, totalSignals = 0, totalErrors = 0;
  var languages = {};

  for (var ci = 0; ci < HARVEST_CONFIGS.length; ci++) {
    var config = HARVEST_CONFIGS[ci];
    var configNew = 0;

    for (var qi = 0; qi < config.queries.length; qi++) {
      var query = config.queries[qi];
      try {
        var articles = await queryGDELT({
          query: query,
          timespan: config.timespan,
          maxrecords: config.maxrecords,
        });

        if (!articles.length) continue;

        for (var ai = 0; ai < articles.length; ai++) {
          var article = articles[ai];

          // Track language distribution
          languages[article.language] = (languages[article.language] || 0) + 1;

          // Skip low-signal non-English articles (keep high-tone ones)
          if (article.language !== 'en' && Math.abs(article.gdelt_tone) < 5) continue;

          if (DRY_RUN) {
            configNew++;
            continue;
          }

          var hash = crypto.createHash('md5').update(article.url || article.gdelt_id).digest('hex');

          try {
            var result = await pool.query(`
              INSERT INTO external_documents (
                tenant_id, source_url, source_url_hash, source_type, source_name,
                title, content, published_at,
                gdelt_id, gdelt_tone, gdelt_themes, source_language
              ) VALUES ($1, $2, $3, 'gdelt', $4, $5, $6, $7, $8, $9, $10, $11)
              ON CONFLICT (source_url_hash, tenant_id) DO NOTHING
              RETURNING id
            `, [
              TENANT_ID,
              article.url,
              hash,
              article.source || 'gdelt',
              String(article.title || '').slice(0, 500),
              String(article.title || ''),
              article.published_at,
              article.gdelt_id,
              article.gdelt_tone,
              article.gdelt_themes,
              article.language,
            ]);

            if (result.rowCount > 0) {
              configNew++;
              totalNew++;

              // Extract signal inline
              var signal = extractSignalFromGDELT(article);
              if (signal) {
                var company = await matchCompanyFromTitle(article.title, article.source);
                if (company || signal.signal_type === 'geographic_expansion') {
                  await pool.query(`
                    INSERT INTO signal_events (
                      tenant_id, company_id, company_name, signal_type,
                      confidence_score, evidence_summary, source_url,
                      detected_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                  `, [
                    TENANT_ID,
                    company ? company.id : null,
                    company ? company.name : null,
                    signal.signal_type,
                    signal.confidence,
                    buildGDELTEvidence(article, signal),
                    article.url,
                  ]);
                  totalSignals++;
                }
              }
            } else {
              totalDuplicates++;
            }
          } catch (insertErr) {
            if (insertErr.code !== '23505') {
              totalErrors++;
              if (totalErrors <= 3) console.error('[GDELT] Insert error:', insertErr.message.substring(0, 80));
            } else {
              totalDuplicates++;
            }
          }
        }

        // GDELT requires 5s between requests
        await sleep(6000);
      } catch (err) {
        console.error('[GDELT] Query failed for "' + query.substring(0, 40) + '": ' + err.message);
      }
    }

    console.log('   ' + config.signal_type + ': ' + configNew + ' new docs');
  }

  // Language breakdown
  var langEntries = Object.entries(languages).sort(function(a, b) { return b[1] - a[1]; });
  var nonEnglish = langEntries.filter(function(e) { return e[0] !== 'English'; });

  console.log('\n[GDELT] Complete: ' + totalNew + ' new docs, ' + totalSignals + ' signals, ' + totalDuplicates + ' dupes skipped');
  if (nonEnglish.length > 0) {
    console.log('[GDELT] Non-English coverage: ' + nonEnglish.slice(0, 10).map(function(e) { return e[0] + ':' + e[1]; }).join(', '));
  }

  return { totalNew: totalNew, totalSignals: totalSignals, totalDuplicates: totalDuplicates, totalErrors: totalErrors, languages: languages };
}

module.exports = { harvestGDELT };

if (require.main === module) {
  harvestGDELT()
    .then(function(r) { console.log('Result:', JSON.stringify(r)); pool.end(); process.exit(0); })
    .catch(function(e) { console.error('Fatal:', e); pool.end(); process.exit(1); });
}
