#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/backfill_embeddings.js — Backfill person embeddings into Qdrant
//
// Embeds all people who don't yet have embedded_at set.
// Uses richer text composition: name, title, company, bio, career history,
// expertise, industries, education, seniority, location.
//
// Usage:
//   node scripts/backfill_embeddings.js              # Full run
//   node scripts/backfill_embeddings.js --dry-run    # Count only
//   node scripts/backfill_embeddings.js --limit 1000 # Cap at N people
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const OpenAI = require('openai');
const { QdrantClient } = require('@qdrant/js-client-rest');

const DRY_RUN = process.argv.includes('--dry-run');
const LIMIT_ARG = (() => {
  const idx = process.argv.indexOf('--limit');
  return idx !== -1 ? parseInt(process.argv[idx + 1]) : null;
})();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY
});

const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBED_BATCH_SIZE = 50;  // OpenAI batch size
const QDRANT_BATCH_SIZE = 100; // Qdrant upsert batch
const DELAY_MS = 200;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════════════════════════════════════
// RICH EMBEDDING TEXT COMPOSITION
// ═══════════════════════════════════════════════════════════════════════════════

function buildPersonText(p) {
  const parts = [];

  // Core identity
  if (p.full_name) parts.push(p.full_name);
  if (p.current_title) parts.push(p.current_title);
  if (p.current_company_name) parts.push(`at ${p.current_company_name}`);
  if (p.location) parts.push(p.location);

  // Professional summary
  if (p.headline) parts.push(p.headline);
  if (p.bio) parts.push(p.bio.slice(0, 1000));

  // Seniority and function
  if (p.seniority_level) parts.push(`Seniority: ${p.seniority_level}`);
  if (p.functional_area) parts.push(`Function: ${p.functional_area}`);

  // Expertise tags
  if (Array.isArray(p.expertise_tags) && p.expertise_tags.length > 0) {
    parts.push(`Expertise: ${p.expertise_tags.join(', ')}`);
  }

  // Industries
  if (Array.isArray(p.industries) && p.industries.length > 0) {
    parts.push(`Industries: ${p.industries.join(', ')}`);
  }

  // Career history (JSONB)
  if (p.career_history) {
    try {
      const history = typeof p.career_history === 'string'
        ? JSON.parse(p.career_history)
        : p.career_history;
      if (Array.isArray(history) && history.length > 0) {
        const careerText = history.slice(0, 8).map(r => {
          const title = r.title || r.role || r.position || '';
          const company = r.company || r.company_name || r.org || '';
          return [title, company].filter(Boolean).join(' at ');
        }).filter(Boolean).join(', ');
        if (careerText) parts.push(`Career: ${careerText}`);
      }
    } catch {}
  }

  // Education (JSONB)
  if (p.education) {
    try {
      const edu = typeof p.education === 'string'
        ? JSON.parse(p.education)
        : p.education;
      if (Array.isArray(edu) && edu.length > 0) {
        const eduText = edu.slice(0, 3).map(e => {
          return [e.degree, e.field_of_study || e.field, e.institution || e.school]
            .filter(Boolean).join(' ');
        }).filter(Boolean).join(', ');
        if (eduText) parts.push(`Education: ${eduText}`);
      }
    } catch {}
  }

  // Years of experience
  if (p.years_experience) parts.push(`${p.years_experience} years experience`);

  return parts.filter(Boolean).join('\n').slice(0, 8000);
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN BACKFILL
// ═══════════════════════════════════════════════════════════════════════════════

async function backfill() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  BACKFILL PERSON EMBEDDINGS → QDRANT');
  if (DRY_RUN) console.log('  ⚠  DRY RUN — no writes');
  console.log('═══════════════════════════════════════════════════════════════════\n');

  // Ensure collection exists
  try {
    const exists = await qdrant.collectionExists('people');
    if (!exists.exists) {
      await qdrant.createCollection('people', {
        vectors: { size: 1536, distance: 'Cosine' },
        optimizers_config: { default_segment_number: 2 },
        replication_factor: 1
      });
      console.log('Created Qdrant collection: people');
    }
  } catch (e) {
    console.error('Qdrant error:', e.message);
    process.exit(1);
  }

  // Fetch unembedded people with useful data
  const limitClause = LIMIT_ARG ? `LIMIT ${LIMIT_ARG}` : '';
  const { rows: people } = await pool.query(`
    SELECT id, full_name, current_title, current_company_name,
           headline, bio, location, seniority_level, functional_area,
           expertise_tags, industries, career_history, education,
           years_experience, tenant_id
    FROM people
    WHERE embedded_at IS NULL
      AND full_name IS NOT NULL AND full_name != ''
    ORDER BY
      CASE WHEN current_title IS NOT NULL AND current_company_name IS NOT NULL THEN 0
           WHEN current_title IS NOT NULL OR current_company_name IS NOT NULL THEN 1
           ELSE 2 END,
      updated_at DESC
    ${limitClause}
  `);

  console.log(`Found ${people.length} people to embed\n`);
  if (DRY_RUN) {
    // Show composition sample
    for (const p of people.slice(0, 3)) {
      const text = buildPersonText(p);
      console.log(`--- ${p.full_name} (${text.length} chars) ---`);
      console.log(text.slice(0, 300) + '...\n');
    }
    console.log('DRY RUN complete — no vectors written');
    await pool.end();
    return;
  }

  const startTime = Date.now();
  let embedded = 0;
  let skipped = 0;
  let failed = 0;

  // Process in batches
  for (let i = 0; i < people.length; i += EMBED_BATCH_SIZE) {
    const batch = people.slice(i, i + EMBED_BATCH_SIZE);

    // Build texts
    const texts = batch.map(p => buildPersonText(p));
    const validIndices = [];
    const validTexts = [];
    for (let j = 0; j < texts.length; j++) {
      if (texts[j].trim().length >= 10) {
        validIndices.push(j);
        validTexts.push(texts[j]);
      } else {
        skipped++;
      }
    }

    if (validTexts.length === 0) continue;

    try {
      // Batch embed via OpenAI
      const response = await openai.embeddings.create({
        model: EMBEDDING_MODEL,
        input: validTexts
      });

      // Build Qdrant points
      const points = validIndices.map((batchIdx, respIdx) => {
        const p = batch[batchIdx];
        return {
          id: Date.now() * 1000 + 40000 + i + batchIdx, // match existing ID scheme
          vector: response.data[respIdx].embedding,
          payload: {
            type: 'person',
            person_id: p.id,
            name: p.full_name,
            full_name: p.full_name,
            title: p.current_title,
            current_title: p.current_title,
            company: p.current_company_name,
            location: p.location,
            seniority: p.seniority_level,
            tenant_id: p.tenant_id,
            content_preview: texts[batchIdx].slice(0, 500)
          }
        };
      });

      // Upsert to Qdrant
      await qdrant.upsert('people', { wait: false, points });

      // Mark embedded in DB
      const embeddedIds = validIndices.map(batchIdx => batch[batchIdx].id);
      await pool.query(
        `UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)`,
        [embeddedIds]
      );

      embedded += validIndices.length;
    } catch (err) {
      if (err.status === 429) {
        // Rate limited — wait and retry
        console.log('  ⏳ Rate limited, waiting 10s...');
        await sleep(10000);
        i -= EMBED_BATCH_SIZE; // Retry this batch
        continue;
      }
      console.log(`  ❌ Batch ${Math.floor(i / EMBED_BATCH_SIZE) + 1} error: ${err.message}`);
      failed += batch.length;
    }

    // Progress report
    const total = embedded + skipped + failed;
    if (total % 500 < EMBED_BATCH_SIZE || i + EMBED_BATCH_SIZE >= people.length) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      const rate = (embedded / Math.max((Date.now() - startTime) / 1000, 1)).toFixed(1);
      console.log(
        `  Progress: ${embedded} embedded, ${skipped} skipped, ${failed} failed ` +
        `(${total}/${people.length}) — ${elapsed}s elapsed, ${rate}/sec`
      );
    }

    await sleep(DELAY_MS);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(0);

  // Final stats
  console.log('\n═══════════════════════════════════════════════════════════════════');
  console.log('  BACKFILL COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log(`  Embedded:  ${embedded}`);
  console.log(`  Skipped:   ${skipped} (insufficient text)`);
  console.log(`  Failed:    ${failed}`);
  console.log(`  Duration:  ${duration}s`);

  // Qdrant final count
  try {
    const info = await qdrant.getCollection('people');
    console.log(`  Qdrant:    ${info.points_count} vectors in 'people' collection`);
  } catch {}

  // DB coverage
  const { rows: [cov] } = await pool.query(`
    SELECT COUNT(*) AS total,
           COUNT(CASE WHEN embedded_at IS NOT NULL THEN 1 END) AS embedded,
           ROUND(COUNT(CASE WHEN embedded_at IS NOT NULL THEN 1 END)::numeric / COUNT(*) * 100, 1) AS pct
    FROM people WHERE full_name IS NOT NULL AND full_name != ''
  `);
  console.log(`  Coverage:  ${cov.embedded}/${cov.total} people (${cov.pct}%)`);

  await pool.end();
}

backfill().catch(e => { console.error(e); process.exit(1); });
