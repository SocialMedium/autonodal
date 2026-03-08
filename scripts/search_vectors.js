#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// search_vectors.js — Semantic Search across People & Documents
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/search_vectors.js "fintech VP engineering APAC"
//   node scripts/search_vectors.js --people "scaled SaaS platforms"
//   node scripts/search_vectors.js --docs "Series B funding AI"
//   node scripts/search_vectors.js --all "executive leadership transition"
//
// Dependencies: dotenv, pg, openai
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;
const TOP_K = 15;

// ─────────────────────────────────────────────────────────────────────────────

function httpRequest(method, url, body = null, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const options = {
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      method,
      headers: { 'Content-Type': 'application/json', ...headers },
      timeout: 30000,
    };
    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function embedQuery(text) {
  const data = await httpRequest('POST', 'https://api.openai.com/v1/embeddings', {
    model: 'text-embedding-3-small',
    input: text,
  }, {
    'Authorization': `Bearer ${OPENAI_API_KEY}`,
  });
  return data.data[0].embedding;
}

async function qdrantSearch(collection, vector, limit = TOP_K, filter = null) {
  const body = {
    vector: vector,
    limit: limit,
    with_payload: true,
  };
  if (filter) body.filter = filter;

  const result = await httpRequest('POST', `${QDRANT_URL}/collections/${collection}/points/search`, body, {
    'api-key': QDRANT_API_KEY,
  });
  return result.result || [];
}

// ─────────────────────────────────────────────────────────────────────────────

async function searchPeople(query, vector) {
  console.log('\n👤 PEOPLE MATCHES');
  console.log('─'.repeat(60));

  const results = await qdrantSearch('people', vector, TOP_K);

  if (results.length === 0) {
    console.log('  (no results)');
    return;
  }

  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    const p = r.payload;
    const score = (r.score * 100).toFixed(1);
    const notes = p.has_research_notes ? '📝' : '  ';

    console.log(`\n  ${i + 1}. ${p.full_name || 'Unknown'}  [${score}% match] ${notes}`);
    if (p.current_title && p.current_company) {
      console.log(`     ${p.current_title} @ ${p.current_company}`);
    } else if (p.current_title) {
      console.log(`     ${p.current_title}`);
    }
    if (p.location) console.log(`     📍 ${p.location}`);
    if (p.seniority) console.log(`     🎯 ${p.seniority}`);
    if (p.expertise && p.expertise.length > 0) {
      console.log(`     🏷️  ${p.expertise.slice(0, 5).join(', ')}`);
    }
    if (p.industry) console.log(`     🏢 ${p.industry}`);
    console.log(`     Source: ${p.source || 'unknown'} | ID: ${r.id}`);
  }
}

async function searchDocuments(query, vector) {
  console.log('\n📄 DOCUMENT MATCHES');
  console.log('─'.repeat(60));

  const results = await qdrantSearch('documents', vector, TOP_K);

  if (results.length === 0) {
    console.log('  (no results)');
    return;
  }

  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    const d = r.payload;
    const score = (r.score * 100).toFixed(1);

    console.log(`\n  ${i + 1}. [${score}%] ${(d.title || 'Untitled').slice(0, 80)}`);
    console.log(`     Source: ${d.source_type}/${d.source_name || 'unknown'}`);
    if (d.author) console.log(`     Author: ${d.author}`);
    if (d.published_at) console.log(`     Date: ${d.published_at.slice(0, 10)}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);

  const doAll = args.includes('--all');
  const doPeople = args.includes('--people') || doAll;
  const doDocs = args.includes('--docs') || doAll;
  const defaultBoth = !doPeople && !doDocs;

  // Extract query (everything that's not a flag)
  const query = args.filter(a => !a.startsWith('--')).join(' ').trim();

  if (!query) {
    console.log('Usage: node scripts/search_vectors.js "your search query"');
    console.log('       node scripts/search_vectors.js --people "VP engineering fintech"');
    console.log('       node scripts/search_vectors.js --docs "Series B funding"');
    console.log('       node scripts/search_vectors.js --all "AI leadership"');
    process.exit(0);
  }

  console.log('═══════════════════════════════════════════════════');
  console.log('  MitchelLake Semantic Search');
  console.log('═══════════════════════════════════════════════════');
  console.log(`  Query: "${query}"`);

  try {
    // Generate query embedding
    console.log('  Embedding query...');
    const vector = await embedQuery(query);

    if (doPeople || defaultBoth) {
      await searchPeople(query, vector);
    }

    if (doDocs || defaultBoth) {
      await searchDocuments(query, vector);
    }

  } catch (err) {
    console.error('\n❌ Error:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }

  console.log('\n═══════════════════════════════════════════════════\n');
}

main();
