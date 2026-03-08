#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// process_intelligence.js — LLM-Powered Intelligence Extraction
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/process_intelligence.js                Process pending documents
//   node scripts/process_intelligence.js --limit 50     Process N documents
//   node scripts/process_intelligence.js --reprocess    Reprocess all documents
//   node scripts/process_intelligence.js --source podcast   Only process type
//   node scripts/process_intelligence.js --dry-run      Show what would be processed
//   node scripts/process_intelligence.js --stats        Show processing stats
//
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = 'claude-sonnet-4-20250514';
const MAX_CONTENT_LENGTH = 12000; // chars to send to Claude
const RATE_LIMIT_MS = 1500; // between API calls
const BATCH_SIZE = 1; // one at a time for quality

// ═══════════════════════════════════════════════════════════════════════════════
// EXTRACTION PROMPT
// ═══════════════════════════════════════════════════════════════════════════════

function buildExtractionPrompt(doc) {
  const content = (doc.content || doc.summary || doc.title || '').slice(0, MAX_CONTENT_LENGTH);
  
  return `You are an intelligence analyst for MitchelLake, an executive search firm specializing in technology leadership (VP+ roles). Analyze this content and extract structured intelligence.

SOURCE: ${doc.source_name || 'Unknown'} (${doc.source_type || 'unknown'})
TITLE: ${doc.title || 'Untitled'}
DATE: ${doc.published_at || 'Unknown'}
URL: ${doc.source_url || ''}

CONTENT:
${content}

Extract ALL intelligence signals from this content. Return valid JSON only — no markdown, no backticks, no preamble.

{
  "signals": [
    {
      "company_name": "Exact company name or null if no specific company",
      "signal_type": "one of: capital_raising, ma_activity, product_launch, partnership, geographic_expansion, restructuring, leadership_change, layoffs, strategic_hiring",
      "signal_category": "one of: thematic, geographic, motion, growth, hiring, change, innovation, trend, gap",
      "confidence": 0.0 to 1.0,
      "headline": "One-line summary of this specific signal (max 120 chars)",
      "evidence": "Direct quote or paraphrase from the content supporting this signal (max 300 chars)",
      "geography": "Country or region mentioned, or null",
      "sector": "Industry sector, or null",
      "people_mentioned": ["Name — Title at Company", ...],
      "hiring_implications": "What executive hiring need does this create? Be specific about roles. Null if none.",
      "temporal_context": "Is this happening now, planned, rumored, or historical?",
      "action_verb": "The key action: raising, expanding, restructuring, appointing, launching, acquiring, partnering, cutting, hiring, innovating"
    }
  ],
  "themes": ["Classify this content across: THEMATIC (what domain/topic), GEOGRAPHIC (where), MOTION (who is moving/acting), GROWTH (scaling signals), HIRING (talent demand), CHANGE (restructuring/pivots), INNOVATION (new tech/products), TRENDS (market patterns), GAPS (unmet needs/talent shortages)"],
  "executive_summary": "2-3 sentence summary of what matters for executive search. Focus on: who is moving, who is hiring, what companies are growing/struggling, what talent gaps exist. Be specific about names, roles, and companies.",
  "relevance_score": 0.0 to 1.0,
  "skip_reason": "If relevance_score < 0.2, explain why this isn't useful for executive search"
}

RULES:
- Only extract signals you have genuine evidence for — no speculation
- confidence 0.9+ = named company + specific action + credible source
- confidence 0.6-0.8 = named company + implied action or strong rumor
- confidence 0.3-0.5 = indirect mention or general trend with partial evidence
- confidence < 0.3 = vague mention, skip it
- For podcast transcripts: extract the ACTUAL companies and events discussed, not the podcast itself
- "This Week in Startups" is NOT a signal — the companies discussed IN the episode are
- If the content is just a podcast/newsletter description with no substance, set relevance_score to 0.1
- People mentioned should be "Name — Title at Company" format
- hiring_implications should suggest specific VP+/C-level roles MitchelLake could fill`;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLAUDE API
// ═══════════════════════════════════════════════════════════════════════════════

async function callClaude(prompt) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: MODEL,
      max_tokens: 2000,
      messages: [{ role: 'user', content: prompt }],
    });

    const req = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 60000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          if (data.error) return reject(new Error(data.error.message || JSON.stringify(data.error)));
          
          const text = data.content?.[0]?.text || '';
          const usage = data.usage || {};
          resolve({ text, usage });
        } catch (e) { reject(e); }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Claude API timeout')); });
    req.write(body);
    req.end();
  });
}

function parseJSON(text) {
  // Strip markdown code fences if present
  let cleaned = text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
  
  // Try parsing as-is
  try { return JSON.parse(cleaned); } catch (e) {}
  
  // Try extracting JSON object
  const match = cleaned.match(/\{[\s\S]*\}/);
  if (match) {
    try { return JSON.parse(match[0]); } catch (e) {}
  }
  
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// ENTITY RESOLUTION
// ═══════════════════════════════════════════════════════════════════════════════

// Cache company lookups
const companyCache = new Map();

async function resolveCompany(companyName) {
  if (!companyName) return null;
  
  const key = companyName.toLowerCase().trim();
  if (companyCache.has(key)) return companyCache.get(key);

  // Try exact match
  let { rows } = await pool.query(
    `SELECT id, name FROM companies WHERE LOWER(name) = $1`, [key]
  );

  // Try alias match
  if (rows.length === 0) {
    ({ rows } = await pool.query(
      `SELECT id, name FROM companies WHERE $1 = ANY(SELECT LOWER(unnest(aliases)))`, [key]
    ));
  }

  // Try fuzzy match (starts with)
  if (rows.length === 0) {
    ({ rows } = await pool.query(
      `SELECT id, name FROM companies WHERE LOWER(name) LIKE $1 LIMIT 1`, [key + '%']
    ));
  }

  // Try contains match for short names
  if (rows.length === 0 && key.length >= 4) {
    ({ rows } = await pool.query(
      `SELECT id, name FROM companies WHERE LOWER(name) LIKE $1 LIMIT 1`, ['%' + key + '%']
    ));
  }

  if (rows.length > 0) {
    companyCache.set(key, rows[0]);
    return rows[0];
  }

  // Create new company
  try {
    const { rows: [newCo] } = await pool.query(
      `INSERT INTO companies (id, name, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, NOW(), NOW())
       ON CONFLICT DO NOTHING
       RETURNING id, name`,
      [companyName.trim()]
    );
    
    if (newCo) {
      companyCache.set(key, newCo);
      return newCo;
    }
  } catch (e) {
    // Race condition — try lookup again
    const { rows: retry } = await pool.query(
      `SELECT id, name FROM companies WHERE LOWER(name) = $1`, [key]
    );
    if (retry.length > 0) {
      companyCache.set(key, retry[0]);
      return retry[0];
    }
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PROCESS SINGLE DOCUMENT
// ═══════════════════════════════════════════════════════════════════════════════

async function processDocument(doc) {
  const prompt = buildExtractionPrompt(doc);
  
  const { text, usage } = await callClaude(prompt);
  const parsed = parseJSON(text);
  
  if (!parsed) {
    console.error(`    ⚠️ Failed to parse Claude response for doc ${doc.id}`);
    return { signals_created: 0, tokens: usage };
  }

  // Store extraction results on the document
  await pool.query(`
    UPDATE external_documents
    SET extracted_signals = $1,
        extracted_entities = $2,
        processing_status = 'processed',
        signals_computed_at = NOW()
    WHERE id = $3
  `, [
    JSON.stringify(parsed.signals || []),
    JSON.stringify({
      themes: parsed.themes || [],
      executive_summary: parsed.executive_summary || '',
      relevance_score: parsed.relevance_score || 0,
      skip_reason: parsed.skip_reason || null,
    }),
    doc.id,
  ]);

  // Skip low-relevance content
  if ((parsed.relevance_score || 0) < 0.2) {
    return { signals_created: 0, skipped: true, tokens: usage };
  }

  // Create signal_events for each extracted signal
  let signalsCreated = 0;

  for (const signal of (parsed.signals || [])) {
    if (!signal.signal_type || (signal.confidence || 0) < 0.3) continue;

    // Resolve company
    const company = signal.company_name ? await resolveCompany(signal.company_name) : null;

    try {
      await pool.query(`
        INSERT INTO signal_events (
          id, signal_type, company_id, company_name,
          confidence_score, evidence_summary, evidence_snippet,
          triage_status, detected_at, signal_date,
          source_document_id, source_url,
          signal_category, hiring_implications,
          scoring_breakdown, created_at, updated_at
        ) VALUES (
          gen_random_uuid(), $1::signal_type, $2, $3,
          $4, $5, $6,
          'new', NOW(), $7,
          $8, $9,
          $10, $11,
          $12, NOW(), NOW()
        )
      `, [
        signal.signal_type,
        company?.id || null,
        signal.company_name || (company?.name) || null,
        signal.confidence || 0.5,
        signal.headline || null,
        signal.evidence || null,
        doc.published_at || null,
        doc.id,
        doc.source_url || null,
        signal.signal_category || null,
        signal.hiring_implications ? JSON.stringify({
          implications: signal.hiring_implications,
          geography: signal.geography,
          sector: signal.sector,
          people_mentioned: signal.people_mentioned,
          temporal_context: signal.temporal_context,
        }) : null,
        JSON.stringify({
          source: doc.source_name,
          source_type: doc.source_type,
          themes: parsed.themes || [],
          relevance_score: parsed.relevance_score,
        }),
      ]);

      signalsCreated++;
    } catch (e) {
      // Skip duplicates or constraint errors
      if (!e.message.includes('duplicate') && !e.message.includes('violates')) {
        console.error(`    ⚠️ Signal insert error: ${e.message.slice(0, 100)}`);
      }
    }
  }

  return { signals_created: signalsCreated, tokens: usage, relevance: parsed.relevance_score };
}

// ═══════════════════════════════════════════════════════════════════════════════
// STATS
// ═══════════════════════════════════════════════════════════════════════════════

async function showStats() {
  const { rows: [totals] } = await pool.query(`
    SELECT
      COUNT(*) AS total,
      COUNT(*) FILTER (WHERE processing_status = 'processed') AS processed,
      COUNT(*) FILTER (WHERE processing_status IS NULL OR processing_status = 'pending') AS pending,
      COUNT(*) FILTER (WHERE signals_computed_at IS NOT NULL) AS with_signals
    FROM external_documents
  `);

  const { rows: byType } = await pool.query(`
    SELECT source_type, COUNT(*) AS total,
           COUNT(*) FILTER (WHERE processing_status = 'processed') AS processed
    FROM external_documents
    GROUP BY source_type ORDER BY total DESC
  `);

  const { rows: signalStats } = await pool.query(`
    SELECT signal_type, COUNT(*) AS cnt,
           ROUND(AVG(confidence_score), 2) AS avg_conf
    FROM signal_events
    GROUP BY signal_type ORDER BY cnt DESC
  `);

  const { rows: [qualityStats] } = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE confidence_score >= 0.8) AS high_conf,
      COUNT(*) FILTER (WHERE confidence_score >= 0.5 AND confidence_score < 0.8) AS mid_conf,
      COUNT(*) FILTER (WHERE confidence_score < 0.5) AS low_conf,
      COUNT(*) FILTER (WHERE company_name IS NOT NULL AND company_name != 'Unknown Company') AS with_company,
      COUNT(*) FILTER (WHERE company_name IS NULL OR company_name = 'Unknown Company') AS no_company
    FROM signal_events
  `);

  console.log('\n📊 INTELLIGENCE PROCESSING STATS');
  console.log('─'.repeat(60));

  console.log('\n  Documents:');
  console.log(`    Total:     ${totals.total}`);
  console.log(`    Processed: ${totals.processed}`);
  console.log(`    Pending:   ${totals.pending}`);

  console.log('\n  By source type:');
  byType.forEach(r => {
    console.log(`    ${r.source_type.padEnd(15)} ${r.processed}/${r.total} processed`);
  });

  console.log('\n  Signals:');
  signalStats.forEach(r => {
    console.log(`    ${r.signal_type.padEnd(25)} ${String(r.cnt).padStart(5)} signals  avg conf: ${r.avg_conf}`);
  });

  console.log('\n  Signal quality:');
  console.log(`    High confidence (≥0.8):  ${qualityStats.high_conf}`);
  console.log(`    Medium (0.5-0.8):        ${qualityStats.mid_conf}`);
  console.log(`    Low (<0.5):              ${qualityStats.low_conf}`);
  console.log(`    With company name:       ${qualityStats.with_company}`);
  console.log(`    No company:              ${qualityStats.no_company}`);
  console.log('');
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);

  console.log('═══════════════════════════════════════════════════');
  console.log('  MitchelLake Signal Intelligence');
  console.log('  LLM-Powered Intelligence Extraction');
  console.log('═══════════════════════════════════════════════════');

  if (!ANTHROPIC_API_KEY) {
    console.error('❌ Missing ANTHROPIC_API_KEY in .env');
    process.exit(1);
  }

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');
    console.log(`✅ Model: ${MODEL}`);

    if (args.includes('--stats')) {
      await showStats();
      await pool.end();
      return;
    }

    // Determine what to process
    const limit = parseInt(args.find(a => a.startsWith('--limit'))?.split('=')?.[1] || args[args.indexOf('--limit') + 1]) || 100;
    const reprocess = args.includes('--reprocess');
    const dryRun = args.includes('--dry-run');
    const sourceFilter = args.find(a => a.startsWith('--source'))?.split('=')?.[1] || args[args.indexOf('--source') + 1] || null;

    // Build query
    let where = reprocess 
      ? 'WHERE 1=1' 
      : `WHERE (processing_status IS NULL OR processing_status = 'pending')`;
    const params = [];
    let paramIdx = 0;

    if (sourceFilter) {
      paramIdx++;
      where += ` AND source_type = $${paramIdx}`;
      params.push(sourceFilter);
    }

    // Prioritize documents with actual content
    // and recent ones first
    paramIdx++;
    params.push(limit);

    const { rows: docs } = await pool.query(`
      SELECT id, source_type, source_name, source_url, title, content, summary,
             published_at, author
      FROM external_documents
      ${where}
      AND (content IS NOT NULL AND LENGTH(content) > 100)
      ORDER BY 
        CASE source_type
          WHEN 'newsletter' THEN 1
          WHEN 'news_pr' THEN 2
          WHEN 'vc_blog' THEN 3
          WHEN 'sec_filing' THEN 4
          WHEN 'rss' THEN 5
          WHEN 'podcast' THEN 6
          WHEN 'google_news' THEN 7
          ELSE 8
        END,
        published_at DESC NULLS LAST
      LIMIT $${paramIdx}
    `, params);

    console.log(`\n📄 Documents to process: ${docs.length}`);
    if (sourceFilter) console.log(`   Filtered to: ${sourceFilter}`);
    console.log(`   Priority: newsletters > news > VC blogs > SEC > RSS > podcasts`);

    if (dryRun) {
      console.log('\n🏃 DRY RUN — showing what would be processed:\n');
      docs.slice(0, 20).forEach((d, i) => {
        const contentLen = (d.content || '').length;
        console.log(`  ${(i+1).toString().padStart(3)}. [${d.source_type}] ${(d.title || 'Untitled').slice(0, 70)}`);
        console.log(`       ${d.source_name || ''} · ${d.published_at ? new Date(d.published_at).toLocaleDateString() : 'no date'} · ${contentLen} chars`);
      });
      if (docs.length > 20) console.log(`  ... and ${docs.length - 20} more`);
      await pool.end();
      return;
    }

    // Process documents
    let processed = 0, signalsTotal = 0, skipped = 0, errors = 0;
    let totalInputTokens = 0, totalOutputTokens = 0;
    const startTime = Date.now();

    for (const doc of docs) {
      const title = (doc.title || 'Untitled').slice(0, 60);
      process.stdout.write(`  ${(processed + 1).toString().padStart(4)}/${docs.length} [${doc.source_type}] ${title}...`);

      try {
        const result = await processDocument(doc);
        
        if (result.skipped) {
          process.stdout.write(` ⏭️ low relevance\n`);
          skipped++;
        } else {
          process.stdout.write(` ✅ ${result.signals_created} signals (rel: ${(result.relevance || 0).toFixed(1)})\n`);
          signalsTotal += result.signals_created;
        }

        if (result.tokens) {
          totalInputTokens += result.tokens.input_tokens || 0;
          totalOutputTokens += result.tokens.output_tokens || 0;
        }

        processed++;
      } catch (err) {
        errors++;
        process.stdout.write(` ❌ ${err.message.slice(0, 60)}\n`);

        if (err.message.includes('429') || err.message.includes('rate') || err.message.includes('overloaded')) {
          console.log('  ⏳ Rate limited, waiting 60s...');
          await new Promise(r => setTimeout(r, 60000));
        }
      }

      // Progress summary every 25 docs
      if (processed % 25 === 0 && processed > 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const cost = ((totalInputTokens * 3 + totalOutputTokens * 15) / 1_000_000).toFixed(4);
        console.log(`\n  ── Progress: ${processed}/${docs.length} | ${signalsTotal} signals | ${skipped} skipped | $${cost} | ${elapsed}s ──\n`);
      }

      // Rate limit
      await new Promise(r => setTimeout(r, RATE_LIMIT_MS));
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    const cost = ((totalInputTokens * 3 + totalOutputTokens * 15) / 1_000_000).toFixed(4);

    console.log('\n═══════════════════════════════════════════════════');
    console.log('📊 PROCESSING COMPLETE');
    console.log(`   Documents processed: ${processed}`);
    console.log(`   Signals created:     ${signalsTotal}`);
    console.log(`   Skipped (low rel):   ${skipped}`);
    console.log(`   Errors:              ${errors}`);
    console.log(`   Input tokens:        ${totalInputTokens.toLocaleString()}`);
    console.log(`   Output tokens:       ${totalOutputTokens.toLocaleString()}`);
    console.log(`   Estimated cost:      $${cost}`);
    console.log(`   Time:                ${elapsed}s`);
    console.log('═══════════════════════════════════════════════════\n');

    // Show updated stats
    await showStats();

  } catch (err) {
    console.error('\n❌ Fatal:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
