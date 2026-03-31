#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/embed_events.js - Embed events into Qdrant for semantic search
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const db = require('../lib/db');

let openai;
try {
  const OpenAI = require('openai');
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
} catch (e) {
  console.warn('⚠️  OpenAI not available:', e.message);
}

let qdrantClient;
try {
  const { QdrantClient } = require('@qdrant/js-client-rest');
  qdrantClient = new QdrantClient({
    url: process.env.QDRANT_URL,
    apiKey: process.env.QDRANT_API_KEY
  });
} catch (e) {
  console.warn('⚠️  Qdrant not available:', e.message);
}

const EMBEDDING_MODEL = 'text-embedding-3-small';
const COLLECTION = 'events';
const BATCH_SIZE = 20;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function ensureCollection() {
  if (!qdrantClient) return false;
  try {
    const exists = await qdrantClient.collectionExists(COLLECTION);
    if (!exists.exists) {
      await qdrantClient.createCollection(COLLECTION, {
        vectors: { size: 1536, distance: 'Cosine' },
        optimizers_config: { default_segment_number: 2 },
        replication_factor: 1
      });
      console.log(`✅ Created Qdrant collection: ${COLLECTION}`);
    }
    return true;
  } catch (error) {
    console.error(`❌ Qdrant collection error: ${error.message}`);
    return false;
  }
}

async function embedBatch(texts) {
  if (!openai) throw new Error('OpenAI not configured');
  const truncated = texts.map(t => t.slice(0, 8000));
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: truncated
  });
  return response.data.map(d => d.embedding);
}

async function embedEvents() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - EVENT EMBEDDING');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();

  if (!openai || !qdrantClient) {
    console.log('⚠️  OpenAI or Qdrant not configured — skipping event embedding');
    return { embedded: 0 };
  }

  const ready = await ensureCollection();
  if (!ready) {
    console.log('⚠️  Could not ensure Qdrant collection — skipping');
    return { embedded: 0 };
  }

  const events = await db.queryAll(`
    SELECT id, title, theme, region, city, description, format, event_date
    FROM events
    WHERE embedded_at IS NULL
      AND description IS NOT NULL
      AND description != ''
    ORDER BY created_at DESC
    LIMIT 200
  `);

  console.log(`📡 Found ${events.length} events to embed`);

  let totalEmbedded = 0;

  for (let i = 0; i < events.length; i += BATCH_SIZE) {
    const batch = events.slice(i, i + BATCH_SIZE);

    const texts = batch.map(e => {
      return [
        e.title,
        e.theme ? `Theme: ${e.theme}` : '',
        e.region ? `Region: ${e.region}` : '',
        e.city ? `City: ${e.city}` : '',
        e.format ? `Format: ${e.format}` : '',
        e.event_date ? `Date: ${e.event_date}` : '',
        e.description
      ].filter(Boolean).join(' ');
    });

    try {
      const embeddings = await embedBatch(texts);

      const points = batch.map((e, idx) => ({
        id: Date.now() * 1000 + 50000 + i + idx,
        vector: embeddings[idx],
        payload: {
          type: 'event',
          event_id: e.id,
          theme: e.theme,
          region: e.region,
          event_date: e.event_date,
          format: e.format,
          title: e.title,
          content_preview: (e.description || '').slice(0, 500)
        }
      }));

      await qdrantClient.upsert(COLLECTION, { wait: true, points });

      const ids = batch.map(e => e.id);
      await db.query(
        `UPDATE events SET embedded_at = NOW() WHERE id = ANY($1)`,
        [ids]
      );

      totalEmbedded += batch.length;
      console.log(`   ✅ Embedded batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} events`);
    } catch (err) {
      console.log(`   ❌ Batch error: ${err.message}`);
    }

    if (i + BATCH_SIZE < events.length) await sleep(200);
  }

  console.log();
  console.log(`   📊 Total embedded: ${totalEmbedded}`);
  console.log();

  return { embedded: totalEmbedded };
}

if (require.main === module) {
  embedEvents()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { embedEvents };
