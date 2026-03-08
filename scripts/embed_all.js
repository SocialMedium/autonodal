#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// embed_all.js — Embed Documents, People & Research Notes into Qdrant
// ═══════════════════════════════════════════════════════════════════════════════
//
// Generates OpenAI embeddings and upserts to Qdrant vector collections.
// Resume-safe: tracks progress via embedded_at / embeddings table.
//
// Usage:
//   node scripts/embed_all.js                   Embed everything (documents + people)
//   node scripts/embed_all.js --documents       Documents only
//   node scripts/embed_all.js --people          People only  
//   node scripts/embed_all.js --stats           Show embedding stats
//   node scripts/embed_all.js --estimate        Cost estimate without embedding
//   node scripts/embed_all.js --reset-docs      Re-embed all documents
//   node scripts/embed_all.js --reset-people    Re-embed all people
//
// Dependencies: dotenv, pg, openai
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

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;

const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBEDDING_DIMS = 1536;
const COST_PER_1M_TOKENS = 0.020; // $0.020 per 1M tokens

const BATCH_SIZE = 50;           // Items per OpenAI API call
const RATE_LIMIT_MS = 200;       // Delay between batches
const MAX_TEXT_LENGTH = 8000;    // Max chars to embed (model limit ~8191 tokens)

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function truncate(str, maxLen = MAX_TEXT_LENGTH) { return !str ? '' : str.length > maxLen ? str.slice(0, maxLen) : str; }

function estimateTokens(text) {
  // Rough estimate: ~4 chars per token for English text
  return Math.ceil((text || '').length / 4);
}

// ─────────────────────────────────────────────────────────────────────────────
// OPENAI EMBEDDINGS API
// ─────────────────────────────────────────────────────────────────────────────

async function generateEmbeddings(texts) {
  // Filter out empty/null texts
  const cleanTexts = texts.map(t => truncate(t || '').trim()).filter(t => t.length > 0);
  if (cleanTexts.length === 0) return [];

  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: EMBEDDING_MODEL,
      input: cleanTexts,
    });

    const url = new URL('https://api.openai.com/v1/embeddings');
    const options = {
      hostname: url.hostname,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 60000,
    };

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
          if (data.error) {
            reject(new Error(`OpenAI API: ${data.error.message}`));
            return;
          }
          resolve({
            embeddings: data.data.map(d => d.embedding),
            usage: data.usage,
          });
        } catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(body);
    req.end();
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// QDRANT API
// ─────────────────────────────────────────────────────────────────────────────

async function qdrantRequest(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const url = new URL(path, QDRANT_URL);
    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: method,
      headers: {
        'Content-Type': 'application/json',
        'api-key': QDRANT_API_KEY,
      },
      timeout: 30000,
    };

    const req = https.request(options, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
          resolve(data);
        } catch (e) { reject(new Error(`Qdrant parse: ${e.message}`)); }
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Qdrant timeout')); });
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function ensureQdrantCollection(name) {
  const check = await qdrantRequest('GET', `/collections/${name}`);
  if (check.status === 'ok') {
    console.log(`  ✅ Qdrant collection '${name}' exists`);
    return true;
  }

  console.log(`  📦 Creating Qdrant collection '${name}'...`);
  const result = await qdrantRequest('PUT', `/collections/${name}`, {
    vectors: {
      size: EMBEDDING_DIMS,
      distance: 'Cosine',
    },
    optimizers_config: {
      default_segment_number: 2,
    },
  });
  console.log(`  ✅ Created '${name}'`);
  return true;
}

async function qdrantUpsert(collection, points) {
  if (points.length === 0) return;

  const result = await qdrantRequest('PUT', `/collections/${collection}/points?wait=true`, {
    points: points,
  });

  if (result.status !== 'ok') {
    throw new Error(`Qdrant upsert failed: ${JSON.stringify(result)}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TEXT BUILDERS — What we embed for each entity
// ─────────────────────────────────────────────────────────────────────────────

function buildDocumentText(doc) {
  const parts = [];
  if (doc.title) parts.push(doc.title);
  if (doc.source_name) parts.push(`Source: ${doc.source_name}`);
  if (doc.author) parts.push(`Author: ${doc.author}`);
  if (doc.content) parts.push(doc.content);
  return parts.join('\n').trim();
}

function buildPersonText(person) {
  const parts = [];

  // Core identity
  if (person.full_name) parts.push(person.full_name);
  if (person.headline) parts.push(person.headline);
  if (person.current_title && person.current_company_name) {
    parts.push(`${person.current_title} at ${person.current_company_name}`);
  } else {
    if (person.current_title) parts.push(person.current_title);
    if (person.current_company_name) parts.push(person.current_company_name);
  }

  // Bio and expertise
  if (person.bio) parts.push(person.bio);
  if (person.expertise_tags && person.expertise_tags.length > 0) {
    parts.push(`Expertise: ${person.expertise_tags.join(', ')}`);
  }
  if (person.seniority_level) parts.push(`Seniority: ${person.seniority_level}`);
  if (person.location) parts.push(`Location: ${person.location}`);

  // Industry context
  if (person.industries) parts.push(`Industry: ${person.industries}`);
  if (person.functional_area) parts.push(`Function: ${person.functional_area}`);

  return parts.join('\n').trim();
}

function buildPersonWithNotesText(person, notes) {
  let text = buildPersonText(person);

  // Append research notes (the goldmine intel)
  if (notes && notes.length > 0) {
    const noteTexts = notes
      .map(n => n.summary || '')
      .filter(t => t.length > 20) // Skip very short notes
      .slice(0, 10);              // Max 10 notes per person

    if (noteTexts.length > 0) {
      text += '\n\nResearch Intelligence:\n' + noteTexts.join('\n---\n');
    }
  }

  return text;
}

// ─────────────────────────────────────────────────────────────────────────────
// EMBED DOCUMENTS
// ─────────────────────────────────────────────────────────────────────────────

async function embedDocuments(options = {}) {
  const { reset = false } = options;

  console.log('\n📄 EMBEDDING DOCUMENTS');
  console.log('─'.repeat(60));

  await ensureQdrantCollection('documents');

  if (reset) {
    console.log('  🔄 Resetting document embeddings...');
    await pool.query(`UPDATE external_documents SET embedded_at = NULL`);
  }

  // Count pending
  const { rows: [{ count: pendingCount }] } = await pool.query(
    `SELECT COUNT(*) as count FROM external_documents WHERE embedded_at IS NULL`
  );
  const { rows: [{ count: totalCount }] } = await pool.query(
    `SELECT COUNT(*) as count FROM external_documents`
  );

  console.log(`  Total documents: ${totalCount}`);
  console.log(`  Already embedded: ${totalCount - pendingCount}`);
  console.log(`  Pending: ${pendingCount}`);

  if (parseInt(pendingCount) === 0) {
    console.log('  ✅ All documents already embedded!\n');
    return { embedded: 0, tokens: 0, cost: 0 };
  }

  let embedded = 0, totalTokens = 0, errors = 0;
  const startTime = Date.now();

  while (true) {
    // Fetch batch of unembedded documents
    const { rows: docs } = await pool.query(`
      SELECT id, title, content, source_type, source_name, author, published_at
      FROM external_documents
      WHERE embedded_at IS NULL
      ORDER BY published_at DESC NULLS LAST
      LIMIT $1
    `, [BATCH_SIZE]);

    if (docs.length === 0) break;

    // Build texts
    const texts = docs.map(buildDocumentText);
    const validIndices = texts.map((t, i) => t.length > 10 ? i : -1).filter(i => i >= 0);

    if (validIndices.length === 0) {
      // Mark these as embedded (with no vector — too sparse)
      const ids = docs.map(d => d.id);
      await pool.query(`UPDATE external_documents SET embedded_at = NOW() WHERE id = ANY($1)`, [ids]);
      embedded += docs.length;
      continue;
    }

    const validTexts = validIndices.map(i => texts[i]);

    try {
      const { embeddings, usage } = await generateEmbeddings(validTexts);
      totalTokens += usage.total_tokens;

      // Build Qdrant points
      const points = [];
      embeddings.forEach((vector, idx) => {
        const docIdx = validIndices[idx];
        const doc = docs[docIdx];
        points.push({
          id: doc.id,
          vector: vector,
          payload: {
            entity_type: 'document',
            title: (doc.title || '').slice(0, 200),
            source_type: doc.source_type,
            source_name: doc.source_name,
            author: doc.author,
            published_at: doc.published_at ? doc.published_at.toISOString() : null,
          },
        });
      });

      // Upsert to Qdrant
      await qdrantUpsert('documents', points);

      // Mark as embedded in PostgreSQL
      const allIds = docs.map(d => d.id);
      await pool.query(`UPDATE external_documents SET embedded_at = NOW() WHERE id = ANY($1)`, [allIds]);

      embedded += docs.length;
      const cost = (totalTokens / 1_000_000) * COST_PER_1M_TOKENS;
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rate = (embedded / (elapsed || 1)).toFixed(1);

      if (embedded % 500 === 0 || docs.length < BATCH_SIZE) {
        console.log(`  📊 ${embedded}/${pendingCount} embedded | ${totalTokens.toLocaleString()} tokens | $${cost.toFixed(4)} | ${rate}/sec | ${elapsed}s`);
      }

    } catch (err) {
      errors++;
      console.error(`  ❌ Batch error: ${err.message}`);

      if (err.message.includes('Rate limit')) {
        console.log('  ⏳ Rate limited — waiting 30s...');
        await sleep(30000);
      } else {
        // Skip this batch — mark as embedded to avoid infinite loop
        const ids = docs.map(d => d.id);
        await pool.query(`UPDATE external_documents SET embedded_at = NOW() WHERE id = ANY($1)`, [ids]);
        embedded += docs.length;
      }
    }

    await sleep(RATE_LIMIT_MS);
  }

  const cost = (totalTokens / 1_000_000) * COST_PER_1M_TOKENS;
  console.log(`  ✅ Documents done: ${embedded} embedded | ${totalTokens.toLocaleString()} tokens | $${cost.toFixed(4)} | ${errors} errors\n`);
  return { embedded, tokens: totalTokens, cost };
}

// ─────────────────────────────────────────────────────────────────────────────
// EMBED PEOPLE (with research notes)
// ─────────────────────────────────────────────────────────────────────────────

async function embedPeople(options = {}) {
  const { reset = false } = options;

  console.log('\n👤 EMBEDDING PEOPLE');
  console.log('─'.repeat(60));

  await ensureQdrantCollection('people');

  if (reset) {
    console.log('  🔄 Resetting people embeddings...');
    await pool.query(`UPDATE people SET embedded_at = NULL`);
  }

  // Only embed people with meaningful data (skip bare-name Google Contacts)
  const { rows: [{ count: pendingCount }] } = await pool.query(`
    SELECT COUNT(*) as count FROM people
    WHERE embedded_at IS NULL
    AND (
      current_title IS NOT NULL
      OR headline IS NOT NULL
      OR bio IS NOT NULL
      OR expertise_tags IS NOT NULL
      OR (current_company_name IS NOT NULL AND current_title IS NOT NULL)
    )
  `);
  const { rows: [{ count: totalEmbeddable }] } = await pool.query(`
    SELECT COUNT(*) as count FROM people
    WHERE current_title IS NOT NULL
      OR headline IS NOT NULL
      OR bio IS NOT NULL
      OR expertise_tags IS NOT NULL
      OR (current_company_name IS NOT NULL AND current_title IS NOT NULL)
  `);
  const { rows: [{ count: totalPeople }] } = await pool.query(
    `SELECT COUNT(*) as count FROM people`
  );

  console.log(`  Total people: ${totalPeople}`);
  console.log(`  Embeddable (have profile data): ${totalEmbeddable}`);
  console.log(`  Already embedded: ${totalEmbeddable - pendingCount}`);
  console.log(`  Pending: ${pendingCount}`);
  console.log(`  Skipping: ${totalPeople - totalEmbeddable} (name-only contacts)`);

  if (parseInt(pendingCount) === 0) {
    console.log('  ✅ All embeddable people already embedded!\n');
    return { embedded: 0, tokens: 0, cost: 0 };
  }

  let embedded = 0, totalTokens = 0, errors = 0;
  const startTime = Date.now();

  while (true) {
    // Fetch batch of unembedded people
    const { rows: people } = await pool.query(`
      SELECT id, full_name, headline, current_title, current_company_name,
             bio, expertise_tags, seniority_level, location, industries, functional_area,
             source
      FROM people
      WHERE embedded_at IS NULL
      AND (
        current_title IS NOT NULL
        OR headline IS NOT NULL
        OR bio IS NOT NULL
        OR expertise_tags IS NOT NULL
        OR (current_company_name IS NOT NULL AND current_title IS NOT NULL)
      )
      ORDER BY id
      LIMIT $1
    `, [BATCH_SIZE]);

    if (people.length === 0) break;

    // Fetch research notes for these people (the intelligence goldmine)
    const personIds = people.map(p => p.id);
    const { rows: allNotes } = await pool.query(`
      SELECT person_id, summary
      FROM interactions
      WHERE person_id = ANY($1)
      AND interaction_type = 'research_note'
      ORDER BY created_at DESC
    `, [personIds]);

    // Group notes by person
    const notesByPerson = {};
    for (const note of allNotes) {
      if (!notesByPerson[note.person_id]) notesByPerson[note.person_id] = [];
      notesByPerson[note.person_id].push(note);
    }

    // Build texts with notes enrichment
    const texts = people.map(p => {
      const notes = notesByPerson[p.id] || [];
      return buildPersonWithNotesText(p, notes);
    });

    const validIndices = texts.map((t, i) => t.length > 10 ? i : -1).filter(i => i >= 0);

    if (validIndices.length === 0) {
      const ids = people.map(p => p.id);
      await pool.query(`UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)`, [ids]);
      embedded += people.length;
      continue;
    }

    const validTexts = validIndices.map(i => texts[i]);

    try {
      const { embeddings, usage } = await generateEmbeddings(validTexts);
      totalTokens += usage.total_tokens;

      // Build Qdrant points
      const points = [];
      embeddings.forEach((vector, idx) => {
        const personIdx = validIndices[idx];
        const person = people[personIdx];
        const hasNotes = (notesByPerson[person.id] || []).length > 0;

        points.push({
          id: person.id,
          vector: vector,
          payload: {
            entity_type: 'person',
            full_name: person.full_name,
            current_title: person.current_title,
            current_company: person.current_company_name,
            seniority: person.seniority_level,
            location: person.location,
            industries: person.industries,
            source: person.source,
            has_research_notes: hasNotes,
            expertise: person.expertise_tags || [],
          },
        });
      });

      await qdrantUpsert('people', points);

      // Mark as embedded
      const allIds = people.map(p => p.id);
      await pool.query(`UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)`, [allIds]);

      embedded += people.length;
      const cost = (totalTokens / 1_000_000) * COST_PER_1M_TOKENS;
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rate = (embedded / (elapsed || 1)).toFixed(1);

      if (embedded % 500 === 0 || people.length < BATCH_SIZE) {
        console.log(`  📊 ${embedded}/${pendingCount} embedded | ${totalTokens.toLocaleString()} tokens | $${cost.toFixed(4)} | ${rate}/sec | ${elapsed}s`);
      }

    } catch (err) {
      errors++;
      console.error(`  ❌ Batch error: ${err.message}`);

      if (err.message.includes('Rate limit')) {
        console.log('  ⏳ Rate limited — waiting 30s...');
        await sleep(30000);
      } else {
        const ids = people.map(p => p.id);
        await pool.query(`UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)`, [ids]);
        embedded += people.length;
      }
    }

    await sleep(RATE_LIMIT_MS);
  }

  const cost = (totalTokens / 1_000_000) * COST_PER_1M_TOKENS;
  console.log(`  ✅ People done: ${embedded} embedded | ${totalTokens.toLocaleString()} tokens | $${cost.toFixed(4)} | ${errors} errors\n`);
  return { embedded, tokens: totalTokens, cost };
}

// ─────────────────────────────────────────────────────────────────────────────
// COST ESTIMATE
// ─────────────────────────────────────────────────────────────────────────────

async function showEstimate() {
  console.log('\n💰 EMBEDDING COST ESTIMATE');
  console.log('─'.repeat(60));

  // Documents
  const { rows: docSample } = await pool.query(`
    SELECT title, content FROM external_documents
    WHERE embedded_at IS NULL
    ORDER BY RANDOM() LIMIT 100
  `);
  const { rows: [{ count: docCount }] } = await pool.query(
    `SELECT COUNT(*) as count FROM external_documents WHERE embedded_at IS NULL`
  );

  const avgDocTokens = docSample.length > 0
    ? docSample.reduce((s, d) => s + estimateTokens(buildDocumentText(d)), 0) / docSample.length
    : 0;
  const docTotalTokens = avgDocTokens * parseInt(docCount);
  const docCost = (docTotalTokens / 1_000_000) * COST_PER_1M_TOKENS;

  console.log(`\n  📄 Documents:`);
  console.log(`     Count: ${parseInt(docCount).toLocaleString()}`);
  console.log(`     Avg tokens/doc: ~${Math.round(avgDocTokens)}`);
  console.log(`     Est. total tokens: ~${Math.round(docTotalTokens).toLocaleString()}`);
  console.log(`     Est. cost: $${docCost.toFixed(4)}`);

  // People
  const { rows: peopleSample } = await pool.query(`
    SELECT full_name, headline, current_title, current_company_name, bio,
           expertise_tags, seniority_level, location, industries, functional_area
    FROM people
    WHERE embedded_at IS NULL
    AND (current_title IS NOT NULL OR headline IS NOT NULL OR bio IS NOT NULL
         OR expertise_tags IS NOT NULL)
    ORDER BY RANDOM() LIMIT 100
  `);
  const { rows: [{ count: peopleCount }] } = await pool.query(`
    SELECT COUNT(*) as count FROM people
    WHERE embedded_at IS NULL
    AND (current_title IS NOT NULL OR headline IS NOT NULL OR bio IS NOT NULL
         OR expertise_tags IS NOT NULL
         OR (current_company_name IS NOT NULL AND current_title IS NOT NULL))
  `);

  const avgPeopleTokens = peopleSample.length > 0
    ? peopleSample.reduce((s, p) => s + estimateTokens(buildPersonText(p)), 0) / peopleSample.length
    : 0;

  // Add estimated note tokens (some people have notes)
  const { rows: [{ avg_notes }] } = await pool.query(`
    SELECT ROUND(AVG(cnt), 1) as avg_notes FROM (
      SELECT person_id, COUNT(*) as cnt FROM interactions
      WHERE interaction_type = 'research_note'
      GROUP BY person_id
    ) t
  `);
  const noteBoost = (parseFloat(avg_notes || 0)) * 50; // ~50 tokens per note avg
  const adjPeopleTokens = avgPeopleTokens + noteBoost * 0.3; // Only 30% of people have notes

  const peopleTotalTokens = adjPeopleTokens * parseInt(peopleCount);
  const peopleCost = (peopleTotalTokens / 1_000_000) * COST_PER_1M_TOKENS;

  console.log(`\n  👤 People:`);
  console.log(`     Count: ${parseInt(peopleCount).toLocaleString()}`);
  console.log(`     Avg tokens/person: ~${Math.round(adjPeopleTokens)}`);
  console.log(`     Est. total tokens: ~${Math.round(peopleTotalTokens).toLocaleString()}`);
  console.log(`     Est. cost: $${peopleCost.toFixed(4)}`);

  const totalCost = docCost + peopleCost;
  const totalTokens = docTotalTokens + peopleTotalTokens;
  const estMinutes = ((parseInt(docCount) + parseInt(peopleCount)) / BATCH_SIZE * (RATE_LIMIT_MS + 300)) / 60000;

  console.log(`\n  ═══════════════════════════════════════`);
  console.log(`  TOTAL: ~${Math.round(totalTokens).toLocaleString()} tokens | $${totalCost.toFixed(4)} | ~${Math.round(estMinutes)} minutes`);
  console.log(`  ═══════════════════════════════════════\n`);
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS
// ─────────────────────────────────────────────────────────────────────────────

async function showStats() {
  console.log('\n📊 EMBEDDING STATISTICS');
  console.log('─'.repeat(60));

  // Documents
  const { rows: docStats } = await pool.query(`
    SELECT source_type,
           COUNT(*) as total,
           COUNT(*) FILTER (WHERE embedded_at IS NOT NULL) as embedded
    FROM external_documents
    GROUP BY source_type
    ORDER BY total DESC
  `);
  console.log('\n  📄 Documents:');
  let docTotal = 0, docEmbedded = 0;
  docStats.forEach(r => {
    docTotal += parseInt(r.total);
    docEmbedded += parseInt(r.embedded);
    const pct = (parseInt(r.embedded) / parseInt(r.total) * 100).toFixed(0);
    console.log(`     ${(r.source_type || 'unknown').padEnd(20)} ${r.embedded}/${r.total} (${pct}%)`);
  });
  console.log(`     ${'TOTAL'.padEnd(20)} ${docEmbedded}/${docTotal} (${(docEmbedded/docTotal*100).toFixed(0)}%)`);

  // People
  const { rows: peopleStats } = await pool.query(`
    SELECT source,
           COUNT(*) as total,
           COUNT(*) FILTER (WHERE embedded_at IS NOT NULL) as embedded
    FROM people
    GROUP BY source
    ORDER BY total DESC
  `);
  console.log('\n  👤 People:');
  let pTotal = 0, pEmbedded = 0;
  peopleStats.forEach(r => {
    pTotal += parseInt(r.total);
    pEmbedded += parseInt(r.embedded);
    const pct = parseInt(r.total) > 0 ? (parseInt(r.embedded) / parseInt(r.total) * 100).toFixed(0) : '0';
    console.log(`     ${(r.source || 'unknown').padEnd(20)} ${r.embedded}/${r.total} (${pct}%)`);
  });
  console.log(`     ${'TOTAL'.padEnd(20)} ${pEmbedded}/${pTotal} (${pTotal > 0 ? (pEmbedded/pTotal*100).toFixed(0) : 0}%)`);

  // Qdrant collection stats
  console.log('\n  🔷 Qdrant Collections:');
  for (const coll of ['documents', 'people', 'companies']) {
    try {
      const info = await qdrantRequest('GET', `/collections/${coll}`);
      if (info.status === 'ok') {
        const count = info.result?.points_count || info.result?.vectors_count || 0;
        console.log(`     ${coll.padEnd(20)} ${count} vectors`);
      } else {
        console.log(`     ${coll.padEnd(20)} (not created)`);
      }
    } catch (e) {
      console.log(`     ${coll.padEnd(20)} (error: ${e.message})`);
    }
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
  console.log('  Vector Embedding Engine');
  console.log('═══════════════════════════════════════════════════');

  // Validate config
  if (!OPENAI_API_KEY) { console.error('❌ Missing OPENAI_API_KEY'); process.exit(1); }
  if (!QDRANT_URL) { console.error('❌ Missing QDRANT_URL'); process.exit(1); }
  if (!QDRANT_API_KEY) { console.error('❌ Missing QDRANT_API_KEY'); process.exit(1); }

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');
    console.log(`✅ OpenAI model: ${EMBEDDING_MODEL}`);
    console.log(`✅ Qdrant: ${QDRANT_URL}`);

    if (args.includes('--stats')) {
      await showStats();
    } else if (args.includes('--estimate')) {
      await showEstimate();
    } else {
      const doDocuments = args.includes('--documents') || (!args.includes('--people'));
      const doPeople = args.includes('--people') || (!args.includes('--documents'));
      const resetDocs = args.includes('--reset-docs');
      const resetPeople = args.includes('--reset-people');

      let totalEmbedded = 0, totalTokens = 0, totalCost = 0;
      const startTime = Date.now();

      if (doDocuments) {
        const docResult = await embedDocuments({ reset: resetDocs });
        totalEmbedded += docResult.embedded;
        totalTokens += docResult.tokens;
        totalCost += docResult.cost;
      }

      if (doPeople) {
        const peopleResult = await embedPeople({ reset: resetPeople });
        totalEmbedded += peopleResult.embedded;
        totalTokens += peopleResult.tokens;
        totalCost += peopleResult.cost;
      }

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);

      console.log('═══════════════════════════════════════════════════');
      console.log('📊 EMBEDDING COMPLETE');
      console.log(`   Entities embedded: ${totalEmbedded.toLocaleString()}`);
      console.log(`   Tokens used:       ${totalTokens.toLocaleString()}`);
      console.log(`   Cost:              $${totalCost.toFixed(4)}`);
      console.log(`   Time:              ${elapsed}s`);
      console.log('═══════════════════════════════════════════════════\n');

      // Show final Qdrant stats
      await showStats();
    }

  } catch (err) {
    console.error('\n❌ Fatal:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
