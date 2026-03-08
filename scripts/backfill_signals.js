#!/usr/bin/env node
/**
 * Google News RSS Backfill - Historical Signal Harvesting
 * Uses Google News RSS with search queries to pull 3 months of history
 * 
 * Usage: node scripts/backfill_signals.js [--limit N] [--region AU|UK|SG|US]
 */

require('dotenv').config();

const Parser = require('rss-parser');
const crypto = require('crypto');
const Anthropic = require('@anthropic-ai/sdk');

const db = require('../lib/db');

const parser = new Parser({
  timeout: 15000,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
  }
});

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY
});

// ═══════════════════════════════════════════════════════════════════════════════
// SEARCH QUERIES BY REGION AND SIGNAL TYPE
// ═══════════════════════════════════════════════════════════════════════════════

const SEARCH_QUERIES = {
  // AUSTRALIA
  AU: [
    // Capital Raising
    { query: 'startup funding australia million', signals: ['capital_raising'], name: 'AU Funding' },
    { query: 'series A australia tech', signals: ['capital_raising'], name: 'AU Series A' },
    { query: 'raises funding melbourne sydney', signals: ['capital_raising'], name: 'AU Raises' },
    { query: 'venture capital australia investment', signals: ['capital_raising'], name: 'AU VC' },
    // Leadership
    { query: 'CEO appointed australia', signals: ['leadership_change'], name: 'AU CEO Appointed' },
    { query: 'new CEO australia tech', signals: ['leadership_change'], name: 'AU New CEO' },
    { query: 'chief executive australia company', signals: ['leadership_change'], name: 'AU Chief Exec' },
    // M&A
    { query: 'acquisition australia company', signals: ['ma_activity'], name: 'AU Acquisition' },
    { query: 'merger australia tech', signals: ['ma_activity'], name: 'AU Merger' },
    // Expansion
    { query: 'expands australia office', signals: ['geographic_expansion'], name: 'AU Expansion' },
  ],
  
  // UK
  UK: [
    { query: 'startup funding UK million london', signals: ['capital_raising'], name: 'UK Funding' },
    { query: 'series A UK tech', signals: ['capital_raising'], name: 'UK Series A' },
    { query: 'raises funding london fintech', signals: ['capital_raising'], name: 'UK Fintech' },
    { query: 'CEO appointed UK', signals: ['leadership_change'], name: 'UK CEO' },
    { query: 'acquisition UK company', signals: ['ma_activity'], name: 'UK M&A' },
    { query: 'expands UK office', signals: ['geographic_expansion'], name: 'UK Expansion' },
  ],
  
  // SINGAPORE / SEA
  SG: [
    { query: 'startup funding singapore million', signals: ['capital_raising'], name: 'SG Funding' },
    { query: 'series A singapore southeast asia', signals: ['capital_raising'], name: 'SG Series A' },
    { query: 'raises funding singapore fintech', signals: ['capital_raising'], name: 'SG Fintech' },
    { query: 'CEO appointed singapore', signals: ['leadership_change'], name: 'SG CEO' },
    { query: 'acquisition singapore company', signals: ['ma_activity'], name: 'SG M&A' },
    { query: 'expands asia pacific office', signals: ['geographic_expansion'], name: 'APAC Expansion' },
  ],
  
  // GLOBAL / US (Wire services)
  US: [
    // Reuters-specific
    { query: 'allinurl:reuters.com funding startup', signals: ['capital_raising'], name: 'Reuters Funding' },
    { query: 'allinurl:reuters.com CEO appointed', signals: ['leadership_change'], name: 'Reuters CEO' },
    { query: 'allinurl:reuters.com acquisition', signals: ['ma_activity'], name: 'Reuters M&A' },
    // AP-specific
    { query: 'allinurl:apnews.com business funding', signals: ['capital_raising'], name: 'AP Business' },
    // General
    { query: 'venture capital funding tech startup', signals: ['capital_raising'], name: 'Global VC' },
    { query: 'private equity acquisition', signals: ['ma_activity'], name: 'Global PE' },
    { query: 'CEO appointment tech company', signals: ['leadership_change'], name: 'Global CEO' },
  ],
  
  // Executive Search specific
  EXEC: [
    { query: 'chief executive officer appointed', signals: ['leadership_change'], name: 'CEO Appointments' },
    { query: 'CFO appointed company', signals: ['leadership_change'], name: 'CFO Appointments' },
    { query: 'CTO appointed tech', signals: ['leadership_change'], name: 'CTO Appointments' },
    { query: 'board director appointed', signals: ['leadership_change'], name: 'Board Appointments' },
    { query: 'executive departure company', signals: ['leadership_change'], name: 'Exec Departures' },
    { query: 'C-suite reshuffle', signals: ['leadership_change', 'restructuring'], name: 'C-Suite Changes' },
  ]
};

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL ANALYSIS (same as harvest_signals.js)
// ═══════════════════════════════════════════════════════════════════════════════

const SIGNAL_ANALYSIS_PROMPT = `You are an expert analyst for an executive search firm. Analyze this news article for signals that indicate hiring opportunities.

ARTICLE:
Title: {title}
Source: {source}
Published: {published}
Content: {content}

Extract signals in these categories:

1. COMPANY SIGNALS (things that create hiring demand):
   - capital_raising: Funding rounds, IPO, debt financing
   - geographic_expansion: New markets, offices, regions
   - ma_activity: Mergers, acquisitions, divestitures
   - leadership_change: C-suite departures, appointments
   - restructuring: Layoffs, reorganization
   - product_launch: New products
   - rapid_growth: Revenue milestones, customer wins

2. PEOPLE SIGNALS (individual career moves):
   - new_appointment: Someone named to a role
   - departure: Someone leaving a role
   - promotion: Internal advancement

3. HIRING IMPLICATIONS:
   - What specific roles might they hire?
   - What's the likely timeline?
   - What seniority level?

Return ONLY valid JSON:
{
  "has_signals": true/false,
  "company": { "name": "Company Name or null", "sector": "Industry or null", "geography": "Location or null" },
  "signals": [
    {
      "type": "signal_type",
      "confidence": 0.0-1.0,
      "summary": "One sentence",
      "evidence": "Quote from article",
      "hiring_implications": { "likely_roles": [], "timeline": "immediate/3-6 months/6-12 months", "seniority": "C-level/VP/Director" }
    }
  ],
  "people_mentioned": [
    { "name": "Full Name", "title": "Their title", "company": "Company", "signal_type": "new_appointment/departure", "context": "What happened" }
  ],
  "relevance_score": 0.0-1.0,
  "summary": "2-3 sentence summary"
}

If no relevant signals: {"has_signals": false, "relevance_score": 0, "signals": [], "people_mentioned": []}`;

// ═══════════════════════════════════════════════════════════════════════════════
// FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

function buildGoogleNewsUrl(query, timeframe = '3m', lang = 'en', country = 'US') {
  const encodedQuery = encodeURIComponent(`${query} when:${timeframe}`);
  return `https://news.google.com/rss/search?q=${encodedQuery}&hl=${lang}&gl=${country}&ceid=${country}:${lang}`;
}

function hashUrl(url) {
  return crypto.createHash('sha256').update(url).digest('hex');
}

async function fetchGoogleNewsRSS(searchConfig, timeframe) {
  const url = buildGoogleNewsUrl(searchConfig.query, timeframe);
  
  try {
    const feed = await parser.parseURL(url);
    return feed.items || [];
  } catch (error) {
    console.error(`    Error fetching ${searchConfig.name}:`, error.message);
    return [];
  }
}

async function storeDocument(item, searchConfig) {
  const urlHash = hashUrl(item.link || item.guid);
  
  // Check if exists
  const existing = await db.queryOne(
    'SELECT id FROM external_documents WHERE source_url_hash = $1',
    [urlHash]
  );
  
  if (existing) return null;
  
  // Extract actual source from Google News redirect
  let actualSource = 'Google News';
  try {
    const sourceMatch = item.link?.match(/url=([^&]+)/);
    if (sourceMatch) {
      const decoded = decodeURIComponent(sourceMatch[1]);
      const domain = new URL(decoded).hostname.replace('www.', '');
      actualSource = domain;
    }
  } catch (e) {}
  
  const doc = await db.queryOne(`
    INSERT INTO external_documents (
      source_type, source_name, source_url, source_url_hash,
      title, content, published_at, fetched_at
    ) VALUES ('google_news', $1, $2, $3, $4, $5, $6, NOW())
    RETURNING *
  `, [
    actualSource,
    item.link,
    urlHash,
    item.title,
    item.contentSnippet || item.content || '',
    item.pubDate ? new Date(item.pubDate) : new Date()
  ]);
  
  return doc;
}

async function analyzeWithLLM(document, searchConfig) {
  const content = document.content || document.title;
  if (content.length < 30) {
    return { has_signals: false, signals: [], people_mentioned: [] };
  }
  
  const prompt = SIGNAL_ANALYSIS_PROMPT
    .replace('{title}', document.title || '')
    .replace('{source}', document.source_name || 'Unknown')
    .replace('{published}', document.published_at || 'Unknown')
    .replace('{content}', content.substring(0, 3000));
  
  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1500,
      messages: [{ role: 'user', content: prompt }]
    });
    
    const text = response.content[0].text;
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
    return { has_signals: false, signals: [], people_mentioned: [] };
  } catch (error) {
    console.error(`    LLM error:`, error.message);
    return { has_signals: false, signals: [], people_mentioned: [] };
  }
}

async function storeSignals(document, analysis) {
  if (!analysis.has_signals || !analysis.signals?.length) return 0;
  
  let stored = 0;
  
  // Find or create company
  let companyId = null;
  if (analysis.company?.name) {
    const company = await db.queryOne(
      'SELECT id FROM companies WHERE LOWER(name) = LOWER($1)',
      [analysis.company.name]
    );
    
    if (company) {
      companyId = company.id;
    } else {
      const newCompany = await db.queryOne(`
        INSERT INTO companies (name, sector, geography, created_at)
        VALUES ($1, $2, $3, NOW()) RETURNING id
      `, [analysis.company.name, analysis.company.sector, analysis.company.geography]);
      companyId = newCompany?.id;
    }
  }
  
  for (const signal of analysis.signals) {
    try {
      await db.query(`
        INSERT INTO signal_events (
          company_id, signal_type, signal_category, confidence_score,
          evidence_summary, evidence_snippet, source_document_id,
          source_url, detected_at, hiring_implications
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9)
      `, [
        companyId,
        signal.type,
        categorizeSignal(signal.type),
        signal.confidence,
        signal.summary,
        signal.evidence?.substring(0, 500),
        document.id,
        document.source_url,
        signal.hiring_implications ? JSON.stringify(signal.hiring_implications) : null
      ]);
      stored++;
    } catch (err) {
      // Ignore duplicates
    }
  }
  
  // Store people mentioned
  for (const person of analysis.people_mentioned || []) {
    try {
      const existing = await db.queryOne(
        "SELECT id FROM people WHERE LOWER(full_name) = LOWER($1)",
        [person.name]
      );
      
      if (existing) {
        await db.query(`
          INSERT INTO person_signals (
            person_id, signal_type, signal_source, confidence_score,
            evidence_summary, detected_at, source_url
          ) VALUES ($1, $2, 'news', 0.7, $3, NOW(), $4)
        `, [existing.id, person.signal_type, person.context, document.source_url]);
      }
    } catch (err) {}
  }
  
  await db.query(`
    UPDATE external_documents 
    SET extracted_signals = $1, summary = $2, signals_computed_at = NOW()
    WHERE id = $3
  `, [JSON.stringify(analysis.signals), analysis.summary, document.id]);
  
  return stored;
}

function categorizeSignal(signalType) {
  const categories = {
    capital_raising: 'capital', geographic_expansion: 'growth', ma_activity: 'ma',
    leadership_change: 'leadership', restructuring: 'distress', product_launch: 'growth',
    rapid_growth: 'growth', new_appointment: 'people', departure: 'people', promotion: 'people'
  };
  return categories[signalType] || 'other';
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const limit = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : 20;
  const regionFilter = args.includes('--region') ? args[args.indexOf('--region') + 1].toUpperCase() : null;
  const timeframe = args.includes('--timeframe') ? args[args.indexOf('--timeframe') + 1] : '3m';

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  GOOGLE NEWS BACKFILL - MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Timeframe: ${timeframe} (${timeframe === '3m' ? '~Q4 2025 to now' : timeframe})`);
  console.log(`  Limit per query: ${limit}`);
  if (regionFilter) console.log(`  Region: ${regionFilter}`);
  console.log('');

  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY not set');
    process.exit(1);
  }

  const stats = { queries: 0, articles: 0, newArticles: 0, signals: 0, errors: 0 };
  
  // Get queries to run
  const regions = regionFilter ? [regionFilter] : Object.keys(SEARCH_QUERIES);
  
  for (const region of regions) {
    const queries = SEARCH_QUERIES[region];
    if (!queries) continue;
    
    console.log(`\n📍 Region: ${region}`);
    console.log('─'.repeat(60));
    
    for (const searchConfig of queries) {
      console.log(`\n  🔍 ${searchConfig.name}`);
      stats.queries++;
      
      const items = await fetchGoogleNewsRSS(searchConfig, timeframe);
      console.log(`     Found ${items.length} items`);
      
      const toProcess = items.slice(0, limit);
      stats.articles += toProcess.length;
      
      for (const item of toProcess) {
        try {
          const doc = await storeDocument(item, searchConfig);
          if (!doc) continue;
          
          stats.newArticles++;
          const shortTitle = item.title?.substring(0, 50) || 'No title';
          console.log(`     📄 ${shortTitle}...`);
          
          const analysis = await analyzeWithLLM(doc, searchConfig);
          
          if (analysis.has_signals) {
            console.log(`        ✨ ${analysis.signals.length} signal(s) (${(analysis.relevance_score * 100).toFixed(0)}%)`);
            const stored = await storeSignals(doc, analysis);
            stats.signals += stored;
          }
          
          await new Promise(r => setTimeout(r, 300));
        } catch (err) {
          console.error(`     ❌ ${err.message}`);
          stats.errors++;
        }
      }
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  BACKFILL COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Queries run: ${stats.queries}`);
  console.log(`  Articles fetched: ${stats.articles}`);
  console.log(`  New articles: ${stats.newArticles}`);
  console.log(`  Signals detected: ${stats.signals}`);
  console.log(`  Errors: ${stats.errors}`);
  console.log('═══════════════════════════════════════════════════════════════');

  process.exit(0);
}

main().catch(err => {
  console.error('Backfill failed:', err);
  process.exit(1);
});
