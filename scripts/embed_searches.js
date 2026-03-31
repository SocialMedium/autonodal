#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/embed_searches.js — Embed search briefs into Qdrant
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

let openai;
try {
  const OpenAI = require('openai');
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
} catch (e) {
  console.error('OpenAI not available:', e.message);
  process.exit(1);
}

let qdrantClient;
try {
  const { QdrantClient } = require('@qdrant/js-client-rest');
  qdrantClient = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY });
} catch (e) {
  console.error('Qdrant not available:', e.message);
  process.exit(1);
}

const EMBEDDING_MODEL = 'text-embedding-3-small';

async function ensureCollection() {
  try {
    const exists = await qdrantClient.collectionExists('searches');
    if (!exists.exists) {
      await qdrantClient.createCollection('searches', {
        vectors: { size: 1536, distance: 'Cosine' },
        optimizers_config: { default_segment_number: 2 },
        replication_factor: 1
      });
      console.log('Created Qdrant collection: searches');
    }
    return true;
  } catch (e) {
    console.error('Collection error:', e.message);
    return false;
  }
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  EMBED SEARCH BRIEFS → QDRANT');
  console.log('═══════════════════════════════════════════════════════════════════\n');

  if (!await ensureCollection()) return;

  const { rows: searches } = await pool.query(`
    SELECT id, title, brief_summary, role_overview, required_experience,
           preferred_experience, ideal_background, key_responsibilities,
           location, seniority_level, target_companies, target_industries,
           must_have_keywords, tenant_id
    FROM opportunities
    WHERE status IN ('sourcing', 'interviewing', 'offer')
  `);

  console.log(`Found ${searches.length} active searches to embed\n`);

  let embedded = 0;
  let skipped = 0;

  for (const s of searches) {
    const text = [
      s.title ? `Role: ${s.title}` : '',
      s.brief_summary,
      s.role_overview,
      s.key_responsibilities ? `Responsibilities: ${s.key_responsibilities}` : '',
      s.required_experience ? `Required: ${s.required_experience}` : '',
      s.preferred_experience ? `Preferred: ${s.preferred_experience}` : '',
      s.ideal_background ? `Ideal background: ${s.ideal_background}` : '',
      s.location ? `Location: ${s.location}` : '',
      s.seniority_level ? `Seniority: ${s.seniority_level}` : '',
      Array.isArray(s.target_industries) && s.target_industries.length
        ? `Industries: ${s.target_industries.join(', ')}` : '',
      Array.isArray(s.must_have_keywords) && s.must_have_keywords.length
        ? `Keywords: ${s.must_have_keywords.join(', ')}` : '',
    ].filter(Boolean).join('\n');

    if (text.trim().length < 20) {
      console.log(`  ⏭  ${s.title} — too short, skipping`);
      skipped++;
      continue;
    }

    try {
      const response = await openai.embeddings.create({
        model: EMBEDDING_MODEL,
        input: text.slice(0, 8000)
      });
      const vector = response.data[0].embedding;

      await qdrantClient.upsert('searches', {
        wait: true,
        points: [{
          id: Date.now() * 1000 + embedded,
          vector,
          payload: {
            type: 'search_brief',
            search_id: s.id,
            title: s.title,
            tenant_id: s.tenant_id,
            embedded_at: new Date().toISOString()
          }
        }]
      });

      await pool.query(
        `UPDATE opportunities SET embedded = true, embedded_at = NOW() WHERE id = $1`,
        [s.id]
      );

      console.log(`  ✅ ${s.title} (${text.length} chars)`);
      embedded++;
    } catch (e) {
      console.log(`  ❌ ${s.title}: ${e.message}`);
    }
  }

  // Verify
  try {
    const info = await qdrantClient.getCollection('searches');
    console.log(`\nQdrant 'searches' collection: ${info.points_count} vectors`);
  } catch (e) {}

  console.log(`\n✅ Done: ${embedded} embedded, ${skipped} skipped`);
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
