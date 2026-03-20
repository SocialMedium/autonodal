#!/usr/bin/env node
/**
 * LLM-Powered RSS Signal Harvester
 * Fetches RSS feeds and uses AI to extract executive search signals
 * 
 * Usage: node scripts/harvest_signals.js [--limit N] [--source NAME]
 */

require('dotenv').config();

const Parser = require('rss-parser');
const crypto = require('crypto');
const Anthropic = require('@anthropic-ai/sdk');

const db = require('../lib/db');

const parser = new Parser({
  timeout: 10000,
  headers: {
    'User-Agent': 'MitchelLake-SignalBot/1.0'
  }
});

const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL ANALYSIS PROMPT
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
   - restructuring: Layoffs, reorganization (creates backfill needs)
   - product_launch: New products (need product/tech leaders)
   - rapid_growth: Revenue milestones, customer wins

2. PEOPLE SIGNALS (individual career moves):
   - new_appointment: Someone named to a role
   - departure: Someone leaving a role
   - promotion: Internal advancement
   - board_appointment: Board seat

3. HIRING IMPLICATIONS:
   - What specific roles might they hire?
   - What's the likely timeline?
   - What seniority level?

Return ONLY valid JSON in this exact format:
{
  "has_signals": true/false,
  "company": {
    "name": "Company Name or null",
    "sector": "Industry sector or null",
    "geography": "HQ location or null"
  },
  "signals": [
    {
      "type": "signal_type from list above",
      "confidence": 0.0-1.0,
      "summary": "One sentence description",
      "evidence": "Quote from article supporting this",
      "hiring_implications": {
        "likely_roles": ["CEO", "CFO", "VP Sales"],
        "timeline": "immediate/3-6 months/6-12 months",
        "seniority": "C-level/VP/Director/Manager"
      }
    }
  ],
  "people_mentioned": [
    {
      "name": "Full Name",
      "title": "Their title",
      "company": "Their company",
      "signal_type": "new_appointment/departure/promotion",
      "context": "What happened"
    }
  ],
  "relevance_score": 0.0-1.0,
  "summary": "2-3 sentence summary of key signals"
}

If no relevant signals, return: {"has_signals": false, "relevance_score": 0, "signals": [], "people_mentioned": []}`;

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchRSSFeed(source) {
  try {
    console.log(`  Fetching: ${source.name}...`);
    const feed = await parser.parseURL(source.url);
    console.log(`    Found ${feed.items?.length || 0} items`);
    return feed.items || [];
  } catch (error) {
    console.error(`    Error fetching ${source.name}:`, error.message);
    
    // Update error status
    await db.query(`
      UPDATE rss_sources 
      SET last_error = $1, consecutive_errors = consecutive_errors + 1
      WHERE id = $2
    `, [error.message, source.id]);
    
    return [];
  }
}

function hashUrl(url) {
  return crypto.createHash('sha256').update(url).digest('hex');
}

async function storeDocument(item, source) {
  const urlHash = hashUrl(item.link || item.guid);
  
  // Check if already exists
  const existing = await db.queryOne(
    'SELECT id FROM external_documents WHERE source_url_hash = $1',
    [urlHash]
  );
  
  if (existing) {
    return null; // Already processed
  }
  
  // Extract image from RSS item (enclosure, media:content, or og:image in content)
  const imageUrl = item.enclosure?.url
    || item['media:content']?.$.url
    || item['media:thumbnail']?.$.url
    || item.itunes?.image
    || (item.content?.match(/<img[^>]+src=["']([^"']+)["']/)?.[1])
    || null;

  // Store new document
  const doc = await db.queryOne(`
    INSERT INTO external_documents (source_type,
      source_id, source_name, source_url, source_url_hash,
      title, content, published_at, fetched_at, image_url
    ) VALUES ('rss', $1, $2, $3, $4, $5, $6, $7, NOW(), $8)
    RETURNING *
  `, [
    source.id,
    source.name,
    item.link,
    urlHash,
    item.title,
    item.contentSnippet || item.content || item.summary || '',
    item.pubDate ? new Date(item.pubDate) : new Date(),
    imageUrl
  ]);
  
  return doc;
}

async function analyzeWithLLM(document, source) {
  const content = document.content || document.title;
  
  // Skip very short content
  if (content.length < 50) {
    return { has_signals: false, signals: [], people_mentioned: [] };
  }
  
  const prompt = SIGNAL_ANALYSIS_PROMPT
    .replace('{title}', document.title || '')
    .replace('{source}', source.name)
    .replace('{published}', document.published_at || 'Unknown')
    .replace('{content}', content.substring(0, 4000)); // Limit content length
  
  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1500,
      messages: [{ role: 'user', content: prompt }]
    });
    
    const text = response.content[0].text;
    
    // Parse JSON from response
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
    
    return { has_signals: false, signals: [], people_mentioned: [] };
    
  } catch (error) {
    console.error(`    LLM error:`, error.message);
    return { has_signals: false, signals: [], people_mentioned: [], error: error.message };
  }
}

async function storeSignals(document, analysis, source) {
  if (!analysis.has_signals || !analysis.signals?.length) {
    return 0;
  }
  
  let stored = 0;
  
  // Find or create company if detected
  let companyId = null;
  if (analysis.company?.name) {
    const company = await db.queryOne(
      'SELECT id FROM companies WHERE LOWER(name) = LOWER($1)',
      [analysis.company.name]
    );
    
    if (company) {
      companyId = company.id;
    } else {
      // Create new company
      const newCompany = await db.queryOne(`
        INSERT INTO companies (name, sector, geography, created_at)
        VALUES ($1, $2, $3, NOW())
        RETURNING id
      `, [analysis.company.name, analysis.company.sector, analysis.company.geography]);
      companyId = newCompany?.id;
    }
  }
  
  // Store each signal
  for (const signal of analysis.signals) {
    try {
      await db.query(`
        INSERT INTO signal_events (
          company_id, company_name, signal_type, signal_category, confidence_score,
          evidence_summary, evidence_snippet, source_document_id,
          source_url, detected_at, hiring_implications
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10)
      `, [
        companyId,
        analysis.company?.name || null,
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
      console.error(`    Error storing signal:`, err.message);
    }
  }
  
  // Store people mentioned
  for (const person of analysis.people_mentioned || []) {
    try {
      // Check if person exists
      const existing = await db.queryOne(
        "SELECT id FROM people WHERE LOWER(full_name) = LOWER($1)",
        [person.name]
      );
      
      if (existing) {
        // Add signal to existing person
        await db.query(`
          INSERT INTO person_signals (
            person_id, signal_type, signal_source, confidence_score,
            evidence_summary, detected_at, source_url
          ) VALUES ($1, $2, 'news', $3, $4, NOW(), $5)
        `, [
          existing.id,
          person.signal_type,
          0.7,
          person.context,
          document.source_url
        ]);
      }
      // If person doesn't exist, we could create them - but skip for now
    } catch (err) {
      // Ignore duplicate errors
    }
  }
  
  // Update document with analysis
  await db.query(`
    UPDATE external_documents 
    SET extracted_signals = $1, 
        summary = $2,
        signals_computed_at = NOW()
    WHERE id = $3
  `, [
    JSON.stringify(analysis.signals),
    analysis.summary,
    document.id
  ]);
  
  return stored;
}

function categorizeSignal(signalType) {
  const categories = {
    capital_raising: 'capital',
    geographic_expansion: 'growth',
    ma_activity: 'ma',
    leadership_change: 'leadership',
    restructuring: 'distress',
    product_launch: 'growth',
    rapid_growth: 'growth',
    new_appointment: 'people',
    departure: 'people',
    promotion: 'people',
    board_appointment: 'people'
  };
  return categories[signalType] || 'other';
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const limit = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : 50;
  const sourceName = args.includes('--source') ? args[args.indexOf('--source') + 1] : null;

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  LLM SIGNAL HARVESTER - MitchelLake Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Articles per source: ${limit}`);
  if (sourceName) console.log(`  Source filter: ${sourceName}`);
  console.log('');

  // Check for API key
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('❌ ANTHROPIC_API_KEY not set in .env');
    process.exit(1);
  }

  try {
    // Get enabled sources
    let query = 'SELECT * FROM rss_sources WHERE enabled = true';
    const params = [];
    if (sourceName) {
      query += ' AND LOWER(name) LIKE LOWER($1)';
      params.push(`%${sourceName}%`);
    }
    
    const sources = await db.queryAll(query, params);
    console.log(`Found ${sources.length} RSS source(s)\n`);

    const stats = {
      sources: 0,
      articles: 0,
      newArticles: 0,
      signals: 0,
      errors: 0
    };

    for (const source of sources) {
      console.log(`\n📡 ${source.name}`);
      stats.sources++;
      
      // Fetch RSS feed
      const items = await fetchRSSFeed(source);
      
      // Process items (limit per source)
      const toProcess = items.slice(0, limit);
      stats.articles += toProcess.length;
      
      for (const item of toProcess) {
        try {
          // Store document
          const doc = await storeDocument(item, source);
          
          if (!doc) {
            continue; // Already processed
          }
          
          stats.newArticles++;
          console.log(`    📄 ${item.title?.substring(0, 60)}...`);
          
          // Analyze with LLM
          const analysis = await analyzeWithLLM(doc, source);
          
          if (analysis.has_signals) {
            console.log(`       ✨ ${analysis.signals.length} signal(s) detected (relevance: ${(analysis.relevance_score * 100).toFixed(0)}%)`);
            
            // Store signals
            const stored = await storeSignals(doc, analysis, source);
            stats.signals += stored;
          }
          
          // Rate limit for LLM
          await new Promise(r => setTimeout(r, 500));
          
        } catch (err) {
          console.error(`    ❌ Error:`, err.message);
          stats.errors++;
        }
      }
      
      // Update source last fetched
      await db.query(
        'UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = 0 WHERE id = $1',
        [source.id]
      );
    }

    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  HARVEST COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`  Sources processed: ${stats.sources}`);
    console.log(`  Articles fetched: ${stats.articles}`);
    console.log(`  New articles: ${stats.newArticles}`);
    console.log(`  Signals detected: ${stats.signals}`);
    console.log(`  Errors: ${stats.errors}`);
    console.log('═══════════════════════════════════════════════════════════════');

  } catch (error) {
    console.error('Harvest failed:', error);
    process.exit(1);
  }

  process.exit(0);
}

main();
