#!/usr/bin/env node
/**
 * MitchelLake Signal Pipeline Scheduler — Production
 * 
 * Runs all intelligence pipelines on Railway as a worker service.
 * Includes full Qdrant/OpenAI embedding and Claude cognitive analysis.
 * 
 * ┌──────────────────────────────────────────────────────────────┐
 * │  PIPELINE FLOW                                               │
 * │                                                              │
 * │  INGEST (every 30 min)                                       │
 * │  RSS Harvest → Embed via OpenAI → Store in Qdrant           │
 * │             → Claude Signal Detection → Score & Triangulate  │
 * │                                                              │
 * │  ENRICH (every 4 hours)                                      │
 * │  Poll content sources → Transcribe pods → Claude analysis   │
 * │                       → Embed → Update person composites     │
 * │                                                              │
 * │  COMPUTE (hourly)                                            │
 * │  Score people → Match to searches → Detect re-engage windows│
 * │                                                              │
 * │  REPORT (daily 6am)                                          │
 * │  Generate daily brief → Email digest (Phase 2)              │
 * └──────────────────────────────────────────────────────────────┘
 * 
 * Railway deployment:
 *   Service: Worker
 *   Start Command: node scripts/scheduler.js
 *   
 * CLI:
 *   node scripts/scheduler.js              # Run scheduler daemon
 *   node scripts/scheduler.js --run-now    # Run all pipelines once
 *   node scripts/scheduler.js --run <key>  # Run single pipeline
 *   node scripts/scheduler.js --status     # Show pipeline status
 */

require('dotenv').config();
const cron = require('node-cron');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════════
// CONNECTIONS
// ═══════════════════════════════════════════════════════════════════════════════

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10
});

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

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const EMBEDDING_MODEL = 'text-embedding-3-small';

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER: Call Claude for cognitive analysis
// ═══════════════════════════════════════════════════════════════════════════════

async function callClaude(systemPrompt, userMessage, maxTokens = 2048) {
  if (!ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not set');

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: maxTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }]
    })
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Claude API ${response.status}: ${err.slice(0, 200)}`);
  }

  const data = await response.json();
  return data.content[0]?.text || '';
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER: Generate embedding via OpenAI
// ═══════════════════════════════════════════════════════════════════════════════

async function embed(text) {
  if (!openai) throw new Error('OpenAI not configured');
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: text.slice(0, 8000)
  });
  return response.data[0].embedding;
}

async function embedBatch(texts) {
  if (!openai) throw new Error('OpenAI not configured');
  // OpenAI supports up to 2048 items per batch
  const results = [];
  for (let i = 0; i < texts.length; i += 50) {
    const batch = texts.slice(i, i + 50).map(t => t.slice(0, 8000));
    const response = await openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: batch
    });
    results.push(...response.data.map(d => d.embedding));
    if (i + 50 < texts.length) await sleep(200); // Rate limit courtesy
  }
  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER: Upsert to Qdrant
// ═══════════════════════════════════════════════════════════════════════════════

async function qdrantUpsert(collection, points) {
  if (!qdrantClient) throw new Error('Qdrant not configured');
  
  // Ensure collection exists
  try {
    await qdrantClient.getCollection(collection);
  } catch (e) {
    await qdrantClient.createCollection(collection, {
      vectors: { size: 1536, distance: 'Cosine' }
    });
    console.log(`     Created Qdrant collection: ${collection}`);
  }

  // Upsert in batches of 100
  for (let i = 0; i < points.length; i += 100) {
    const batch = points.slice(i, i + 100);
    await qdrantClient.upsert(collection, { points: batch });
    if (i + 100 < points.length) await sleep(100);
  }
}

async function qdrantSearch(collection, vector, limit = 20, filter = null) {
  if (!qdrantClient) throw new Error('Qdrant not configured');
  const params = { vector, limit, with_payload: true };
  if (filter) params.filter = filter;
  return qdrantClient.search(collection, params);
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER: Utilities
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function md5(text) {
  return require('crypto').createHash('md5').update(text).digest('hex');
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 1: RSS HARVEST + EMBED + SIGNAL DETECTION
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineIngestSignals() {
  const stats = { fetched: 0, new_docs: 0, embedded: 0, signals: 0, errors: 0 };

  // ─── Step 1: Fetch RSS feeds ───
  console.log('   📡 Step 1: Fetching RSS feeds...');

  const sources = await pool.query(`
    SELECT * FROM rss_sources WHERE enabled = true
  `);

  const Parser = require('rss-parser');
  const parser = new Parser({
    timeout: 10000,
    customFields: {
      item: [['media:content', 'mediaContent'], ['media:thumbnail', 'mediaThumbnail'], ['itunes:image', 'itunesImage']],
      feed: [['itunes:image', 'itunesImage']]
    }
  });

  function extractImage(item, feedImage) {
    // Try item-level sources first
    if (item.enclosure?.url && item.enclosure.type?.startsWith('image')) return item.enclosure.url;
    if (item.mediaContent?.$?.url) return item.mediaContent.$.url;
    if (item.mediaThumbnail?.$?.url) return item.mediaThumbnail.$.url;
    // Per-episode itunes:image (some podcasts have per-episode artwork)
    if (item.itunesImage?.$?.href) return item.itunesImage.$.href;
    if (item.itunes?.image) return item.itunes.image;
    // Try extracting from HTML content
    const htmlContent = item.content || item['content:encoded'] || '';
    const imgMatch = htmlContent.match(/<img[^>]+src=["']([^"']+)["']/i);
    if (imgMatch) return imgMatch[1];
    // Try og:image in description
    const ogMatch = htmlContent.match(/og:image[^"]*content=["']([^"']+)["']/i);
    if (ogMatch) return ogMatch[1];
    // Fall back to feed-level image (podcast show artwork)
    if (feedImage) return feedImage;
    return null;
  }

  for (const source of sources.rows) {
    try {
      const feed = await parser.parseURL(source.url);
      stats.fetched++;

      // Extract feed-level image (podcast show artwork, channel logo)
      const feedImage = feed.image?.url
        || feed.itunesImage?.$?.href
        || feed.itunes?.image
        || null;

      for (const item of (feed.items || []).slice(0, 20)) {
        const url = item.link || item.guid;
        if (!url) continue;

        const urlHash = md5(url);
        const exists = await pool.query(
          'SELECT id FROM external_documents WHERE source_url_hash = $1', [urlHash]
        );
        if (exists.rows.length > 0) continue;

        const content = (item.contentSnippet || item.content || '')
          .replace(/<[^>]+>/g, ' ').slice(0, 10000);
        const imageUrl = extractImage(item, feedImage);

        await pool.query(`
          INSERT INTO external_documents (
            tenant_id, source_id, source_url, source_url_hash, source_type, source_name, title, content,
            published_at, author, image_url
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          ON CONFLICT (source_url_hash, tenant_id) DO NOTHING
        `, [
          source.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001',
          source.id, url, urlHash,
          source.source_type || 'rss',
          source.name,
          String(item.title || item.summary || '').slice(0, 500),
          content,
          item.isoDate || item.pubDate || new Date().toISOString(),
          (item.creator || item.author || '').slice(0, 200),
          imageUrl
        ]);
        stats.new_docs++;
      }

      await pool.query(`
        UPDATE rss_sources SET last_fetched_at = NOW(), 
        consecutive_errors = 0 WHERE id = $1
      `, [source.id]);

    } catch (e) {
      stats.errors++;
      await pool.query(`
        UPDATE rss_sources SET consecutive_errors = consecutive_errors + 1,
        last_error = $1 WHERE id = $2
      `, [e.message.slice(0, 500), source.id]).catch(() => {});
    }

    await sleep(300); // Rate limit between feeds
  }

  console.log(`     ${stats.fetched} feeds fetched, ${stats.new_docs} new documents`);

  // ─── Step 2: Embed new documents via OpenAI → Qdrant ───
  if (openai && qdrantClient) {
    console.log('   📐 Step 2: Embedding new documents...');

    const pending = await pool.query(`
      SELECT id, title, content FROM external_documents 
      WHERE embedded_at IS NULL 
      ORDER BY published_at DESC LIMIT 200
    `);

    if (pending.rows.length > 0) {
      const texts = pending.rows.map(d => 
        `${d.title || ''}\n\n${(d.content || '').slice(0, 6000)}`
      );

      const embeddings = await embedBatch(texts);

      const points = pending.rows.map((doc, i) => ({
        id: doc.id.replace(/-/g, '').slice(0, 32), // Qdrant needs specific ID format
        vector: embeddings[i],
        payload: {
          document_id: doc.id,
          title: doc.title,
          content_preview: (doc.content || '').slice(0, 500),
          type: 'external_document'
        }
      }));

      // Use integer IDs for Qdrant
      const qdrantPoints = points.map((p, i) => ({
        id: Date.now() * 1000 + i, // Unique integer ID
        vector: p.vector,
        payload: p.payload
      }));

      await qdrantUpsert('documents', qdrantPoints);

      const ids = pending.rows.map(d => d.id);
      await pool.query(`
        UPDATE external_documents SET embedded_at = NOW() 
        WHERE id = ANY($1)
      `, [ids]);

      stats.embedded = pending.rows.length;
      console.log(`     ${stats.embedded} documents embedded in Qdrant`);
    }
  }

  // ─── Step 3: Claude Signal Detection + Triangulation ───
  console.log('   🧠 Step 3: Claude signal analysis...');

  const unprocessed = await pool.query(`
    SELECT id, title, content, source_name, published_at, tenant_id
    FROM external_documents
    WHERE signals_computed_at IS NULL AND embedded_at IS NOT NULL
      AND COALESCE(processing_status, 'pending') != 'context_only'
      AND source_type NOT IN ('google_doc', 'google_slides', 'google_sheets', 'google_drive')
      AND published_at > NOW() - INTERVAL '3 months'
    ORDER BY published_at DESC LIMIT 200
  `);

  const VALID_SIGNAL_TYPES = new Set([
    'capital_raising', 'geographic_expansion', 'strategic_hiring',
    'ma_activity', 'partnership', 'product_launch',
    'leadership_change', 'layoffs', 'restructuring'
  ]);

  if (unprocessed.rows.length > 0 && ANTHROPIC_API_KEY) {
    // Batch documents for Claude (5 at a time to manage context)
    for (let i = 0; i < unprocessed.rows.length; i += 5) {
      const batch = unprocessed.rows.slice(i, i + 5);
      const batchIds = batch.map(d => d.id);

      const docsText = batch.map((d, idx) =>
        `--- DOCUMENT ${idx + 1} ---\nTitle: ${d.title}\nSource: ${d.source_name}\nDate: ${d.published_at}\n\n${(d.content || '').slice(0, 3000)}`
      ).join('\n\n');

      try {
        const analysis = await callClaude(
          `You are MitchelLake's signal intelligence analyst. MitchelLake is an executive search firm focused on technology leadership roles across ANZ, SEA, UK, and globally.

Analyze these documents and extract actionable signals. For each signal, identify:
1. Signal type (MUST be one of these exact values): capital_raising, geographic_expansion, strategic_hiring, ma_activity, partnership, product_launch, leadership_change, layoffs, restructuring
2. Company name (exact)
3. Confidence: 0.0-1.0
4. Why this matters for executive search
5. Likely hiring implications (what roles they'll need)
6. Urgency: immediate, this_week, this_month, watch

Return ONLY valid JSON array:
[{
  "document_index": 1,
  "company": "Company Name",
  "signal_type": "type",
  "confidence": 0.8,
  "summary": "what happened",
  "hiring_implications": "what roles they'll likely need",
  "urgency": "this_week",
  "evidence": "key quote or fact"
}]`,
          docsText,
          4096
        );

        // Parse signals
        let signals = [];
        try {
          const cleaned = analysis.replace(/```json\s*/g, '').replace(/```/g, '').trim();
          signals = JSON.parse(cleaned);
        } catch (e) {
          console.warn('     ⚠️  Could not parse Claude response');
        }

        // Store signals
        for (const signal of signals) {
          if (!signal.company || !signal.signal_type) continue;
          if (!VALID_SIGNAL_TYPES.has(signal.signal_type)) {
            console.warn(`     ⚠️  Skipping invalid signal_type: ${signal.signal_type}`);
            continue;
          }

          // Find or create company — scope to tenant via source document
          // For short names (<=8 chars), disambiguate by picking the record with most people/signals
          // to avoid matching client "Factory" (AU) with signal about "Factory AI" (US)
          let companyId;
          const docTenantId = batch[signal.document_index - 1]?.tenant_id || batch[0]?.tenant_id;
          const signalCompanyName = (signal.company || '').trim();

          // Check if the evidence contains a longer/qualified name
          const evidence = signal.summary || signal.evidence || '';
          const qualifiedName = evidence.match(new RegExp(signalCompanyName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '[\\s.,]+(Inc|AI|Labs|Group|Ltd|Corp|Technologies|Tech|Digital|Platform|Health|Capital|Ventures|Studio)', 'i'));
          const searchName = qualifiedName ? signalCompanyName + ' ' + qualifiedName[1] : signalCompanyName;

          const companyResult = await pool.query(
            `SELECT id, name, is_client,
                    (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count
             FROM companies c
             WHERE LOWER(name) = LOWER($1) AND (tenant_id = $2 OR tenant_id IS NULL)
             ORDER BY is_client DESC NULLS LAST, (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) DESC
             LIMIT 1`,
            [searchName, docTenantId]
          );

          if (companyResult.rows.length > 0) {
            // For short common names, only match client if evidence mentions their geography/context
            const match = companyResult.rows[0];
            if (signalCompanyName.length <= 8 && match.is_client) {
              // Check if evidence contradicts the client match (different geo, different product)
              // Skip client match if evidence clearly refers to a different entity
              const evidLower = evidence.toLowerCase();
              const clientName = match.name.toLowerCase();
              if (evidLower.includes(clientName)) {
                companyId = match.id;
              } else {
                // Create a new company record for this distinct entity
                companyId = null;
              }
            } else {
              companyId = match.id;
            }
          }

          if (!companyId) {
            const newCompany = await pool.query(
              `INSERT INTO companies (name, tenant_id) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING id`,
              [signal.company, docTenantId]
            );
            companyId = newCompany.rows[0]?.id;
          }

          if (companyId) {
            await pool.query(`
              INSERT INTO signal_events (
                company_id, company_name, signal_type, signal_category,
                confidence_score, evidence_summary, hiring_implications,
                source_document_id, detected_at, image_url, source_url
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10)
              ON CONFLICT DO NOTHING
            `, [
              companyId,
              signal.company,
              signal.signal_type,
              'market',
              signal.confidence || 0.5,
              signal.summary,
              JSON.stringify(signal.hiring_implications || ''),
              batch[signal.document_index - 1]?.id || batch[0]?.id,
              (batch[signal.document_index - 1] || batch[0])?.image_url || null,
              (batch[signal.document_index - 1] || batch[0])?.source_url || null
            ]);

            stats.signals++;
          }
        }

        // Mark this batch as processed only after successful analysis + storage
        await pool.query(`
          UPDATE external_documents SET signals_computed_at = NOW()
          WHERE id = ANY($1)
        `, [batchIds]);

      } catch (e) {
        console.warn(`     ⚠️  Claude analysis error: ${e.message}`);
        console.warn(`     ↳ ${batch.length} documents will be retried next run`);
        stats.errors++;
      }

      await sleep(500); // Rate limit between Claude calls
    }

    console.log(`     ${stats.signals} signals detected via Claude`);
  }

  // ─── Step 4: Cross-reference signals with MLX network ───
  console.log('   🔗 Step 4: Cross-referencing with MLX network...');
  
  const recentSignals = await pool.query(`
    SELECT se.*, c.name as company_name, c.tenant_id as company_tenant_id
    FROM signal_events se
    JOIN companies c ON se.company_id = c.id
    WHERE se.detected_at > NOW() - INTERVAL '1 day'
    AND se.triage_notes IS NULL
  `);

  let crossRefCount = 0;
  for (const signal of recentSignals.rows) {
    // Find people in our network at this company — scoped to same tenant
    const affected = await pool.query(`
      SELECT id, full_name, current_title
      FROM people
      WHERE LOWER(current_company_name) LIKE LOWER($1)
        AND tenant_id = $2
    `, [`%${signal.company_name}%`, signal.company_tenant_id]);

    if (affected.rows.length > 0) {
      // Store cross-ref as triage note
      await pool.query(`
        UPDATE signal_events SET 
          triage_notes = $1,
          updated_at = NOW()
        WHERE id = $2
      `, [
        JSON.stringify({
          network_overlap: true,
          affected_count: affected.rows.length,
          people: affected.rows.slice(0, 20).map(p => ({
            id: p.id,
            name: p.full_name,
            title: p.current_title
          }))
        }),
        signal.id
      ]);
      crossRefCount++;
    } else {
      await pool.query(`
        UPDATE signal_events SET updated_at = NOW() WHERE id = $1
      `, [signal.id]);
    }
  }

  if (crossRefCount > 0) {
    console.log(`     ${crossRefCount} signals cross-referenced with MLX network`);
  }

  // ─── Step 5: Media Sentiment Analysis ───
  console.log('   📊 Step 5: Media sentiment analysis...');

  // Ensure table exists
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS document_sentiment (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      document_id UUID NOT NULL,
      sentiment VARCHAR(10) NOT NULL DEFAULT 'neutral',
      confidence FLOAT DEFAULT 0.5,
      themes TEXT[] DEFAULT '{}',
      summary TEXT,
      source_type VARCHAR(50),
      computed_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(document_id)
    )`);
  } catch (e) {}

  // Get recent documents without sentiment analysis (podcasts, blogs, newsletters)
  const unsentiment = await pool.query(`
    SELECT ed.id, ed.title, ed.content, ed.source_name, ed.source_type, ed.published_at
    FROM external_documents ed
    LEFT JOIN document_sentiment ds ON ds.document_id = ed.id
    WHERE ds.id IS NULL
      AND ed.source_type IN ('podcast', 'blog', 'newsletter', 'news_enrich', 'rss')
      AND ed.published_at > NOW() - INTERVAL '7 days'
      AND ed.title IS NOT NULL
    ORDER BY ed.published_at DESC
    LIMIT 30
  `);

  let sentimentCount = 0;
  if (unsentiment.rows.length > 0 && ANTHROPIC_API_KEY) {
    // Batch 10 at a time
    for (let i = 0; i < unsentiment.rows.length; i += 10) {
      const batch = unsentiment.rows.slice(i, i + 10);

      const docsText = batch.map((d, idx) =>
        `[${idx + 1}] "${d.title}" — ${d.source_name} (${d.source_type})\n${(d.content || '').slice(0, 800)}`
      ).join('\n\n');

      try {
        const result = await callClaude(
          `Rate the market sentiment of each document for the executive search / talent intelligence market.

For each document return: sentiment (bullish/bearish/neutral), confidence (0.0-1.0), and up to 3 theme tags.

Bullish = growth signals, hiring, expansion, investment, positive momentum
Bearish = layoffs, closures, contraction, regulatory pressure, negative momentum
Neutral = informational, mixed, or irrelevant to market health

Return ONLY a JSON array:
[{"index":1,"sentiment":"bullish","confidence":0.8,"themes":["AI","hiring"],"summary":"one sentence"}]`,
          docsText, 2048
        );

        let parsed = [];
        try {
          const cleaned = result.replace(/```json\s*/g, '').replace(/```/g, '').trim();
          parsed = JSON.parse(cleaned);
        } catch (e) {}

        for (const item of (Array.isArray(parsed) ? parsed : [])) {
          const doc = batch[item.index - 1];
          if (!doc || !['bullish', 'bearish', 'neutral'].includes(item.sentiment)) continue;

          await pool.query(`
            INSERT INTO document_sentiment (document_id, sentiment, confidence, themes, summary, source_type, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (document_id) DO NOTHING
          `, [doc.id, item.sentiment, item.confidence || 0.5, item.themes || [], item.summary || '', doc.source_type]);
          sentimentCount++;
        }
      } catch (e) {
        console.warn(`     ⚠️  Sentiment analysis error: ${e.message}`);
      }
      await sleep(500);
    }
    console.log(`     ${sentimentCount} documents sentiment-scored`);
  }
  stats.sentiment = sentimentCount;

  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 2: EMBED INTELLIGENCE (research notes, drops, interactions)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineEmbedIntelligence() {
  const stats = { notes: 0, drops: 0, interactions: 0, signals: 0, case_studies: 0 };

  if (!openai || !qdrantClient) {
    console.log('     ⚠️  OpenAI or Qdrant not configured, skipping');
    return stats;
  }

  // ─── Research notes from Ezekia ───
  console.log('   📝 Embedding research notes...');
  
  // person_notes table not yet created - skip research note embedding for now
  const notes = { rows: [] };
  // TODO: Create person_notes table or embed from Ezekia research notes

  if (notes.rows.length > 0) {
    const texts = notes.rows.map(n =>
      `Person: ${n.full_name} (${n.current_title || ''} @ ${n.current_company_name || ''})\nProject: ${n.project_name || ''}\nDate: ${n.note_date || ''}\n\n${n.note_text}`
    );

    const embeddings = await embedBatch(texts);

    const points = notes.rows.map((n, i) => ({
      id: Date.now() * 1000 + i,
      vector: embeddings[i],
      payload: {
        type: 'research_note',
        person_id: n.person_id,
        person_name: n.full_name,
        project: n.project_name,
        content_preview: n.note_text.slice(0, 500)
      }
    }));

    await qdrantUpsert('people', points);

    // person_notes update skipped - table pending

    stats.notes = notes.rows.length;
    console.log(`     ${stats.notes} research notes embedded`);
  }

  // ─── Intelligence drops ───
  console.log('   🧠 Embedding intelligence drops...');
  
  const drops = await pool.query(`
    SELECT id, raw_input, transcription, extraction, drop_category
    FROM intelligence_drops
    WHERE status = 'complete' AND embedded_at IS NULL
    LIMIT 100
  `).catch(() => ({ rows: [] }));

  if (drops.rows.length > 0) {
    const texts = drops.rows.map(d => {
      const content = d.transcription || d.raw_input || '';
      const summary = d.extraction?.summary || '';
      return `${summary}\n\n${content}`.slice(0, 8000);
    });

    const embeddings = await embedBatch(texts);

    const points = drops.rows.map((d, i) => ({
      id: Date.now() * 1000 + 10000 + i,
      vector: embeddings[i],
      payload: {
        type: 'intelligence_drop',
        drop_id: d.id,
        category: d.drop_category,
        content_preview: (d.transcription || d.raw_input || '').slice(0, 500)
      }
    }));

    await qdrantUpsert('people', points);

    await pool.query(`
      UPDATE intelligence_drops SET embedded_at = NOW() WHERE id = ANY($1)
    `, [drops.rows.map(d => d.id)]);

    stats.drops = drops.rows.length;
    console.log(`     ${stats.drops} intelligence drops embedded`);
  }

  // ─── Signal events ───
  console.log('   📡 Embedding signal events...');

  try {
    const signals = await pool.query(`
      SELECT id, signal_type, company_name, evidence_summary, source_url, signal_date, confidence_score
      FROM signal_events
      WHERE embedded_at IS NULL
      ORDER BY signal_date DESC NULLS LAST
      LIMIT 200
    `);

    console.log(`     ${signals.rows.length} unembedded signals found`);

    if (signals.rows.length > 0) {
      const texts = signals.rows.map(s =>
        `Signal: ${s.signal_type} at ${s.company_name || 'Unknown'}\nDate: ${s.signal_date || ''}\nConfidence: ${s.confidence_score || ''}\n\n${s.evidence_summary || ''}`
      );

      const sigEmbeddings = await embedBatch(texts);

      const sigPoints = signals.rows.map((s, i) => ({
        id: s.id,
        vector: sigEmbeddings[i],
        payload: {
          type: 'signal_event',
          signal_id: s.id,
          signal_type: s.signal_type,
          company_name: s.company_name,
          signal_date: s.signal_date,
          confidence: s.confidence_score,
          content_preview: (s.evidence_summary || '').slice(0, 500)
        }
      }));

      await qdrantUpsert('signal_events', sigPoints);

      await pool.query(`
        UPDATE signal_events SET embedded_at = NOW() WHERE id = ANY($1)
      `, [signals.rows.map(s => s.id)]);

      stats.signals = signals.rows.length;
      console.log(`     ${stats.signals} signal events embedded`);
    }
  } catch (embedErr) {
    console.error('     ⚠️ Signal embedding error:', embedErr.message);
  }

  // ─── Case studies ───
  console.log('   📋 Embedding case studies...');

  const cases = await pool.query(`
    SELECT id, title, client_name, role_title, sector, geography, challenge, approach, outcome,
           themes, capabilities, change_vectors, engagement_type
    FROM case_studies
    WHERE embedded_at IS NULL AND status != 'deleted'
    LIMIT 200
  `).catch(() => ({ rows: [] }));

  if (cases.rows.length > 0) {
    const texts = cases.rows.map(c => {
      const themes = Array.isArray(c.themes) ? c.themes.join(', ') : '';
      const caps = Array.isArray(c.capabilities) ? c.capabilities.join(', ') : '';
      const vectors = Array.isArray(c.change_vectors) ? c.change_vectors.join(', ') : '';
      return [
        `Case Study: ${c.title || c.client_name}`,
        `Client: ${c.client_name || ''}`,
        `Role: ${c.role_title || ''}`,
        c.engagement_type ? `Type: ${c.engagement_type}` : '',
        c.sector ? `Sector: ${c.sector}` : '',
        c.geography ? `Geography: ${c.geography}` : '',
        themes ? `Themes: ${themes}` : '',
        caps ? `Capabilities: ${caps}` : '',
        vectors ? `Change Vectors: ${vectors}` : '',
        c.challenge ? `\nContext: ${c.challenge}` : '',
        c.approach ? `\nApproach: ${c.approach}` : '',
        c.outcome ? `\nOutcome: ${c.outcome}` : ''
      ].filter(Boolean).join('\n');
    });

    const csEmbeddings = await embedBatch(texts);

    const csPoints = cases.rows.map((c, i) => ({
      id: Date.now() * 1000 + 30000 + i,
      vector: csEmbeddings[i],
      payload: {
        type: 'case_study',
        case_study_id: c.id,
        client_name: c.client_name,
        role_title: c.role_title,
        title: c.title || c.client_name,
        content_preview: (c.description || '').slice(0, 500)
      }
    }));

    await qdrantUpsert('case_studies', csPoints);

    await pool.query(`
      UPDATE case_studies SET embedded_at = NOW() WHERE id = ANY($1)
    `, [cases.rows.map(c => c.id)]);

    stats.case_studies = cases.rows.length;
    console.log(`     ${stats.case_studies} case studies embedded`);
  }

  // ─── People (unembedded or updated since last embedding) ───
  console.log('   👥 Embedding unembedded people...');

  const unembeddedPeople = await pool.query(`
    SELECT id, full_name, current_title, current_company_name, location,
           headline, bio, seniority_level, functional_area,
           expertise_tags, industries, career_history, education,
           years_experience, tenant_id
    FROM people
    WHERE (embedded_at IS NULL OR updated_at > embedded_at)
      AND full_name IS NOT NULL AND full_name != ''
    ORDER BY
      CASE WHEN embedded_at IS NULL THEN 0 ELSE 1 END,
      CASE WHEN current_title IS NOT NULL AND current_company_name IS NOT NULL THEN 0
           WHEN current_title IS NOT NULL OR current_company_name IS NOT NULL THEN 1
           ELSE 2 END,
      updated_at DESC
    LIMIT 2000
  `).catch(() => ({ rows: [] }));

  if (unembeddedPeople.rows.length > 0) {
    // Richer text composition
    const texts = unembeddedPeople.rows.map(p => {
      const parts = [];
      if (p.full_name) parts.push(p.full_name);
      if (p.current_title) parts.push(p.current_title);
      if (p.current_company_name) parts.push(`at ${p.current_company_name}`);
      if (p.location) parts.push(p.location);
      if (p.headline) parts.push(p.headline);
      if (p.bio) parts.push(p.bio.slice(0, 500));
      if (p.seniority_level) parts.push(`Seniority: ${p.seniority_level}`);
      if (p.functional_area) parts.push(`Function: ${p.functional_area}`);
      if (Array.isArray(p.expertise_tags) && p.expertise_tags.length) parts.push(`Expertise: ${p.expertise_tags.join(', ')}`);
      if (Array.isArray(p.industries) && p.industries.length) parts.push(`Industries: ${p.industries.join(', ')}`);
      if (p.career_history) {
        try {
          const hist = typeof p.career_history === 'string' ? JSON.parse(p.career_history) : p.career_history;
          if (Array.isArray(hist) && hist.length) {
            const ct = hist.slice(0, 5).map(r => [r.title||r.role, r.company||r.company_name].filter(Boolean).join(' at ')).filter(Boolean).join(', ');
            if (ct) parts.push(`Career: ${ct}`);
          }
        } catch {}
      }
      if (p.education) {
        try {
          const edu = typeof p.education === 'string' ? JSON.parse(p.education) : p.education;
          if (Array.isArray(edu) && edu.length) {
            const et = edu.slice(0, 2).map(e => [e.degree, e.field_of_study||e.field, e.institution||e.school].filter(Boolean).join(' ')).filter(Boolean).join(', ');
            if (et) parts.push(`Education: ${et}`);
          }
        } catch {}
      }
      return parts.filter(Boolean).join('\n').slice(0, 8000);
    });

    const pplEmbeddings = await embedBatch(texts);

    const pplPoints = unembeddedPeople.rows.map((p, i) => ({
      id: Date.now() * 1000 + 40000 + i,
      vector: pplEmbeddings[i],
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
        content_preview: texts[i].slice(0, 500)
      }
    }));

    await qdrantUpsert('people', pplPoints);

    await pool.query(`
      UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)
    `, [unembeddedPeople.rows.map(p => p.id)]);

    stats.people = unembeddedPeople.rows.length;
    console.log(`     ${stats.people} people embedded`);
  }

  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 3: SCORE COMPUTATION
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineComputeScores() {
  const stats = { scored: 0 };

  console.log('   📊 Computing person scores...');

  // Get people with signals or interactions that need rescoring
  const people = await pool.query(`
    SELECT p.id, p.full_name, p.current_company_name, p.current_title,
           p.tenant_id, p.created_at,
           (SELECT COUNT(*) FROM person_signals ps WHERE ps.person_id = p.id) as signal_count,
           (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) as interaction_count,
           (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id) as last_interaction,
           (SELECT COUNT(*) FROM linkedin_connections lc WHERE lc.matched_person_id = p.id) as connection_count
    FROM people p
    WHERE p.id IN (
      SELECT DISTINCT person_id FROM person_signals WHERE detected_at > NOW() - INTERVAL '7 days'
      UNION
      SELECT DISTINCT person_id FROM interactions WHERE created_at > NOW() - INTERVAL '7 days'
      UNION
      SELECT DISTINCT matched_person_id FROM linkedin_connections WHERE matched_person_id IS NOT NULL
    )
    LIMIT 500
  `);

  for (const person of people.rows) {
    // Check for company signals (flight risk indicator) — scoped to same tenant
    const companySignals = await pool.query(`
      SELECT signal_type, confidence_score AS confidence FROM signal_events se
      JOIN companies c ON se.company_id = c.id
      WHERE LOWER(c.name) LIKE LOWER($1)
        AND (c.tenant_id = $2 OR c.tenant_id IS NULL)
        AND se.detected_at > NOW() - INTERVAL '30 days'
    `, [`%${person.current_company_name || 'NONE'}%`, person.tenant_id]).catch(() => ({ rows: [] }));

    const hasLayoffs = companySignals.rows.some(s => 
      ['layoffs', 'restructuring'].includes(s.signal_type)
    );
    const hasFunding = companySignals.rows.some(s => 
      ['capital_raising', 'ipo'].includes(s.signal_type)
    );

    // Compute scores
    const daysSinceInteraction = person.last_interaction 
      ? Math.floor((Date.now() - new Date(person.last_interaction)) / (1000*60*60*24))
      : 999;

    const engagement = Math.max(0, Math.min(1,
      (person.interaction_count > 0 ? 0.3 : 0) +
      (daysSinceInteraction < 30 ? 0.3 : daysSinceInteraction < 90 ? 0.15 : 0) +
      (person.connection_count > 0 ? 0.2 : 0) +
      (person.interaction_count > 5 ? 0.2 : person.interaction_count * 0.04)
    ));

    const activity = Math.max(0, Math.min(1,
      (person.signal_count > 0 ? 0.4 : 0) +
      (person.signal_count > 3 ? 0.3 : person.signal_count * 0.1) +
      (companySignals.rows.length > 0 ? 0.3 : 0)
    ));

    const flightRisk = Math.max(0, Math.min(1,
      (hasLayoffs ? 0.5 : 0) +
      (activity > 0.5 ? 0.2 : 0) +
      (engagement < 0.2 ? 0.1 : 0) +
      (companySignals.rows.filter(s => s.signal_type === 'leadership_departure').length > 0 ? 0.2 : 0)
    ));

    const receptivity = Math.max(0, Math.min(1,
      (daysSinceInteraction < 90 ? 0.3 : 0) +
      (person.connection_count > 0 ? 0.2 : 0) +
      (flightRisk > 0.3 ? 0.3 : 0) +
      (!hasFunding ? 0.1 : 0) +
      (engagement > 0.3 ? 0.1 : 0)
    ));

    const timing = Math.max(0, Math.min(1,
      receptivity * 0.4 + activity * 0.3 + flightRisk * 0.3
    ));

    // Upsert scores
    await pool.query(`
      INSERT INTO person_scores (person_id, engagement_score, activity_score, receptivity_score, flight_risk_score, timing_score, computed_at, score_factors)
      VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
      ON CONFLICT (person_id) DO UPDATE SET
        engagement_score = $2, activity_score = $3, receptivity_score = $4,
        flight_risk_score = $5, timing_score = $6, computed_at = NOW(), score_factors = $7
    `, [
      person.id, 
      engagement, activity, receptivity, flightRisk, timing,
      JSON.stringify({
        signal_count: person.signal_count,
        interaction_count: person.interaction_count,
        days_since_interaction: daysSinceInteraction,
        connection_count: person.connection_count,
        company_signals: companySignals.rows.map(s => s.signal_type),
        has_layoffs: hasLayoffs,
        has_funding: hasFunding
      })
    ]).catch(() => {});

    stats.scored++;
  }

  console.log(`     ${stats.scored} people scored`);
  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 4: SEARCH MATCHING (Vector similarity via Qdrant)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineMatchSearches() {
  const stats = { searches: 0, matches: 0, embedded: 0 };

  if (!openai || !qdrantClient) {
    console.log('     ⚠️  OpenAI or Qdrant not configured, skipping');
    return stats;
  }

  console.log('   🎯 Matching candidates to active searches...');

  // Active searches = sourcing, interviewing, or offer
  const searches = await pool.query(`
    SELECT id, title, brief_summary, role_overview, required_experience,
           preferred_experience, ideal_background, key_responsibilities,
           location, seniority_level, target_companies, target_industries,
           must_have_keywords, tenant_id
    FROM opportunities
    WHERE status IN ('sourcing', 'interviewing', 'offer')
  `).catch(e => { console.log('     ⚠️  Query error:', e.message); return { rows: [] }; });

  console.log(`     Found ${searches.rows.length} active searches`);

  for (const search of searches.rows) {
    // Build rich embedding text from all brief fields
    const briefText = [
      search.title ? `Role: ${search.title}` : '',
      search.brief_summary,
      search.role_overview,
      search.key_responsibilities ? `Responsibilities: ${search.key_responsibilities}` : '',
      search.required_experience ? `Required: ${search.required_experience}` : '',
      search.preferred_experience ? `Preferred: ${search.preferred_experience}` : '',
      search.ideal_background ? `Ideal background: ${search.ideal_background}` : '',
      search.location ? `Location: ${search.location}` : '',
      search.seniority_level ? `Seniority: ${search.seniority_level}` : '',
      Array.isArray(search.target_industries) && search.target_industries.length
        ? `Industries: ${search.target_industries.join(', ')}` : '',
      Array.isArray(search.must_have_keywords) && search.must_have_keywords.length
        ? `Keywords: ${search.must_have_keywords.join(', ')}` : '',
    ].filter(Boolean).join('\n');

    if (briefText.trim().length < 20) {
      console.log(`     ⏭  ${search.title} — insufficient brief text, skipping`);
      continue;
    }

    try {
      const searchVector = await embed(briefText);

      // Also upsert search embedding into Qdrant for future use
      await qdrantClient.upsert('searches', {
        wait: true,
        points: [{
          id: Date.now() * 1000 + stats.searches,
          vector: searchVector,
          payload: {
            type: 'search_brief',
            search_id: search.id,
            title: search.title,
            tenant_id: search.tenant_id,
            status: 'active',
            embedded_at: new Date().toISOString()
          }
        }]
      });
      stats.embedded++;

      // Mark as embedded in DB
      await pool.query(
        `UPDATE opportunities SET embedded = true, embedded_at = NOW() WHERE id = $1`,
        [search.id]
      ).catch(() => {});

      // Search Qdrant people collection for matching candidates
      const results = await qdrantSearch('people', searchVector, 50);

      let searchMatches = 0;
      for (const result of results) {
        if (result.score < 0.25) continue; // Minimum similarity threshold

        const personId = result.payload?.person_id || result.payload?.id;
        if (!personId) continue;

        await pool.query(`
          INSERT INTO search_matches (search_id, person_id, overall_match_score, match_reasons,
            tenant_id, status, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, 'suggested', NOW(), NOW())
          ON CONFLICT (search_id, person_id) DO UPDATE SET
            overall_match_score = EXCLUDED.overall_match_score,
            match_reasons = EXCLUDED.match_reasons,
            updated_at = NOW()
        `, [
          search.id,
          personId,
          result.score,
          JSON.stringify({
            similarity: parseFloat(result.score.toFixed(3)),
            source: result.payload?.type || 'person',
            name: result.payload?.full_name || result.payload?.name || null,
            title: result.payload?.title || result.payload?.current_title || null,
            content_preview: (result.payload?.content_preview || '').slice(0, 200)
          }),
          search.tenant_id,
        ]).catch(e => {
          if (!e.message.includes('violates')) console.log(`     ⚠️  Match insert error: ${e.message}`);
        });

        searchMatches++;
      }

      if (searchMatches > 0) {
        console.log(`     ✅ ${search.title}: ${searchMatches} matches (top score: ${results[0]?.score?.toFixed(3)})`);
      }
      stats.matches += searchMatches;
      stats.searches++;
    } catch (e) {
      console.log(`     ⚠️  ${search.title}: ${e.message}`);
    }

    await sleep(200); // Rate limit between OpenAI calls
  }

  console.log(`     ${stats.searches} searches processed, ${stats.matches} matches found, ${stats.embedded} briefs embedded`);
  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 5: CONTENT ENRICHMENT (candidate blogs, podcasts)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineEnrichContent() {
  const stats = { sources_polled: 0, new_items: 0, analyzed: 0 };

  console.log('   📝 Polling candidate content sources...');

  const sources = await pool.query(`
    SELECT * FROM person_content_sources 
    WHERE is_active = true 
    AND (last_polled_at IS NULL OR last_polled_at < NOW() - INTERVAL '4 hours')
    LIMIT 50
  `).catch(() => ({ rows: [] }));

  if (sources.rows.length === 0) {
    console.log('     No content sources to poll');
    return stats;
  }

  const Parser = require('rss-parser');
  const parser = new Parser({ timeout: 10000 });

  for (const source of sources.rows) {
    try {
      const feed = await parser.parseURL(source.feed_url);
      stats.sources_polled++;

      for (const item of (feed.items || []).slice(0, 10)) {
        const contentHash = md5(item.link || item.guid || item.title);
        
        const exists = await pool.query(
          'SELECT id FROM person_content WHERE content_hash = $1', [contentHash]
        ).catch(() => ({ rows: [] }));
        
        if (exists.rows.length > 0) continue;

        const content = (item.contentSnippet || item.content || '').replace(/<[^>]+>/g, ' ').slice(0, 10000);

        // Claude analysis of content
        let analysis = {};
        if (ANTHROPIC_API_KEY && content.length > 100) {
          try {
            const raw = await callClaude(
              `Analyze this content from a person in our executive search network. Extract:
1. Key topics discussed
2. Notable opinions or insights
3. Companies or people mentioned
4. Signals about their career thinking (are they hinting at change?)
5. Expertise areas demonstrated
Return JSON: {"topics":[],"insights":[],"entities":[],"career_signals":[],"expertise":[]}`,
              `Title: ${item.title}\n\n${content}`,
              1024
            );
            analysis = JSON.parse(raw.replace(/```json\s*/g, '').replace(/```/g, '').trim());
          } catch (e) {
            // Analysis optional, continue without it
          }
          stats.analyzed++;
          await sleep(300);
        }

        await pool.query(`
          INSERT INTO person_content (
            person_id, source_id, title, url, content_hash,
            content, published_at, analysis
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          ON CONFLICT (content_hash) DO NOTHING
        `, [
          source.person_id, source.id,
          String(item.title || item.summary || '').slice(0, 500),
          item.link || '',
          contentHash, content,
          item.isoDate || new Date().toISOString(),
          JSON.stringify(analysis)
        ]).catch(() => {});

        stats.new_items++;
      }

      await pool.query(`
        UPDATE person_content_sources SET last_polled_at = NOW() WHERE id = $1
      `, [source.id]);

    } catch (e) {
      await pool.query(`
        UPDATE person_content_sources SET last_error = $1 WHERE id = $2
      `, [e.message.slice(0, 500), source.id]).catch(() => {});
    }

    await sleep(500);
  }

  // Embed new content
  if (stats.new_items > 0 && openai && qdrantClient) {
    console.log('   📐 Embedding new content...');
    const newContent = await pool.query(`
      SELECT id, person_id, title, content FROM person_content
      WHERE embedded_at IS NULL LIMIT 100
    `).catch(() => ({ rows: [] }));

    if (newContent.rows.length > 0) {
      const texts = newContent.rows.map(c => `${c.title}\n\n${(c.content || '').slice(0, 6000)}`);
      const embeddings = await embedBatch(texts);

      const points = newContent.rows.map((c, i) => ({
        id: Date.now() * 1000 + 50000 + i,
        vector: embeddings[i],
        payload: {
          type: 'person_content',
          person_id: c.person_id,
          title: c.title,
          content_preview: (c.content || '').slice(0, 300)
        }
      }));

      await qdrantUpsert('person_content', points);
      await pool.query(`
        UPDATE person_content SET embedded_at = NOW() WHERE id = ANY($1)
      `, [newContent.rows.map(c => c.id)]);
    }
  }

  console.log(`     ${stats.sources_polled} sources polled, ${stats.new_items} new items, ${stats.analyzed} analyzed by Claude`);
  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 6: DAILY BRIEF (Claude generates intelligence summary)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineDailyBrief() {
  if (!ANTHROPIC_API_KEY) {
    console.log('     ⚠️  Anthropic not configured, skipping daily brief');
    return {};
  }

  console.log('   📋 Generating daily intelligence brief...');

  // Gather 24h of intelligence
  const [signals, newPeople, drops, scores] = await Promise.all([
    pool.query(`
      SELECT se.*, c.name as company_name FROM signal_events se
      JOIN companies c ON se.company_id = c.id
      WHERE se.detected_at > NOW() - INTERVAL '24 hours'
      ORDER BY se.confidence_score DESC LIMIT 30
    `).catch(() => ({ rows: [] })),

    pool.query(`
      SELECT COUNT(*) as count FROM people WHERE created_at > NOW() - INTERVAL '24 hours'
    `).catch(() => ({ rows: [{ count: 0 }] })),

    pool.query(`
      SELECT COUNT(*) as count, 
             COUNT(*) FILTER (WHERE drop_category = 'gossip') as gossip,
             COUNT(*) FILTER (WHERE drop_category = 'meeting') as meetings
      FROM intelligence_drops WHERE created_at > NOW() - INTERVAL '24 hours'
    `).catch(() => ({ rows: [{ count: 0, gossip: 0, meetings: 0 }] })),

    pool.query(`
      SELECT p.full_name, p.current_title, p.current_company_name, ps.*
      FROM person_scores ps
      JOIN people p ON ps.person_id = p.id
      WHERE ps.flight_risk_score > 0.6 OR ps.timing_score > 0.7
      ORDER BY ps.timing_score DESC LIMIT 20
    `).catch(() => ({ rows: [] }))
  ]);

  const briefInput = {
    signals: signals.rows.map(s => `${s.company_name}: ${s.signal_type} (${s.confidence}) - ${s.summary}`).join('\n'),
    new_people: newPeople.rows[0].count,
    drops: drops.rows[0],
    high_timing: scores.rows.slice(0, 10).map(s => 
      `${s.full_name} (${s.current_title} @ ${s.current_company_name}) - timing: ${s.timing_score}, flight_risk: ${s.flight_risk_score}`
    ).join('\n')
  };

  const brief = await callClaude(
    `You are the daily intelligence briefing generator for MitchelLake, an executive search firm.
Generate a concise, actionable morning brief. Structure:
1. TOP 3 PRIORITIES (what needs action today)
2. DEAL LEADS (companies likely to hire)
3. TALENT IN MOTION (candidates showing move signals)
4. THEMES (patterns across signals)
5. CONTENT OPPORTUNITY (what to write/share today)

Be specific. Name names. Suggest concrete actions. Keep it under 500 words.`,
    `SIGNALS (last 24h):\n${briefInput.signals}\n\nNEW PEOPLE: ${briefInput.new_people}\n\nINTELLIGENCE DROPS: ${JSON.stringify(briefInput.drops)}\n\nHIGH-TIMING CANDIDATES:\n${briefInput.high_timing}`,
    2048
  );

  // Store brief
  await pool.query(`
    INSERT INTO intelligence_drops (
      user_id, input_type, raw_input, drop_category, status,
      extraction, acknowledgment
    ) VALUES (
      (SELECT id FROM users LIMIT 1),
      'system', $1, 'daily_brief', 'complete',
      $2, 'Daily brief generated'
    )
  `, [brief, JSON.stringify({ brief, generated_at: new Date().toISOString() })]).catch(() => {});

  console.log(`     Daily brief generated (${brief.length} chars)`);
  return { brief_length: brief.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// WAITLIST DAILY DIGEST
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineWaitlistDigest() {
  console.log('   📋 Checking waitlist activity...');

  const { rows: [counts] } = await pool.query(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE status = 'pending') as pending,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as new_24h
    FROM waitlist
  `);

  const { rows: recent } = await pool.query(`
    SELECT name, email, company, created_at
    FROM waitlist
    WHERE created_at > NOW() - INTERVAL '24 hours'
    ORDER BY created_at DESC
  `);

  if (recent.length === 0) {
    console.log('     No new waitlist signups in the last 24h');
    return { new_signups: 0, total: parseInt(counts.total), pending: parseInt(counts.pending) };
  }

  const digestLines = recent.map(r =>
    `  • ${r.name || 'Anonymous'} (${r.email})${r.company ? ' — ' + r.company : ''}`
  );

  const digest = [
    `WAITLIST DAILY DIGEST — ${new Date().toISOString().split('T')[0]}`,
    ``,
    `New signups (24h): ${recent.length}`,
    `Total pending: ${counts.pending}`,
    `Total waitlist: ${counts.total}`,
    ``,
    `NEW REGISTRATIONS:`,
    ...digestLines,
    ``,
    `Review at: /waitlist.html`
  ].join('\n');

  console.log('\n' + digest + '\n');

  // Store as intelligence drop for visibility in daily brief
  await pool.query(`
    INSERT INTO intelligence_drops (
      user_id, input_type, raw_input, drop_category, status,
      extraction, acknowledgment
    ) VALUES (
      (SELECT id FROM users WHERE role = 'admin' LIMIT 1),
      'system', $1, 'daily_brief', 'complete',
      $2, 'Waitlist digest generated'
    )
  `, [
    digest,
    JSON.stringify({ type: 'waitlist_digest', new_signups: recent.length, total: parseInt(counts.total), pending: parseInt(counts.pending), entries: recent })
  ]).catch(e => console.error('     Failed to store waitlist digest:', e.message));

  return { new_signups: recent.length, total: parseInt(counts.total), pending: parseInt(counts.pending) };
}

// ═══════════════════════════════════════════════════════════════════════════════
// EVENTMEDIUM EVENTS INGESTION
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineIngestEvents() {
  // Delegate to the unified harvest_events script
  const { harvestEvents } = require('./harvest_events');
  return harvestEvents();
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 10: DAILY NETWORK INSIGHTS
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineNetworkInsights() {
  if (!ANTHROPIC_API_KEY) { console.log('     ⚠️  No API key, skipping insights'); return; }

  console.log('   🧭 Generating daily network insights...');

  // Get all users with network data
  const { rows: users } = await pool.query(`
    SELECT u.id, u.name, u.email, u.region, u.tenant_id,
           t.vertical, t.profile, t.focus_geographies, t.focus_sectors
    FROM users u
    JOIN tenants t ON t.id = u.tenant_id
    WHERE t.onboarding_status = 'complete'
      OR (SELECT COUNT(*) FROM team_proximity WHERE team_member_id = u.id) > 10
  `);

  let generated = 0;

  for (const user of users) {
    try {
      // Skip if already generated today
      const { rows: existing } = await pool.query(
        `SELECT id FROM daily_insights WHERE tenant_id = $1 AND user_id = $2 AND insight_date = CURRENT_DATE`,
        [user.tenant_id, user.id]
      );
      if (existing.length > 0) continue;

      // Get network geo distribution
      const { rows: geoDist } = await pool.query(`
        SELECT
          CASE
            WHEN p.country_code IN ('AU','NZ') OR p.location ILIKE '%australia%' THEN 'OCE'
            WHEN p.country_code IN ('SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN') OR p.location ILIKE '%singapore%' OR p.location ILIKE '%india%' OR p.location ILIKE '%asia%' THEN 'ASIA'
            WHEN p.country_code IN ('GB','UK','IE','DE','FR','NL') OR p.location ILIKE '%london%' OR p.location ILIKE '%europe%' THEN 'EUR'
            WHEN p.country_code IN ('AE','SA','IL','TR') OR p.location ILIKE '%dubai%' THEN 'MENA'
            WHEN p.country_code IN ('US','CA','BR','MX') OR p.location ILIKE '%united states%' OR p.location ILIKE '%new york%' THEN 'AMER'
            ELSE 'OTHER' END AS region,
          COUNT(DISTINCT tp.person_id) AS cnt
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
        WHERE tp.team_member_id = $2 AND tp.tenant_id = $1
        GROUP BY region ORDER BY cnt DESC
      `, [user.tenant_id, user.id]);

      // Get sector distribution
      const { rows: sectorDist } = await pool.query(`
        SELECT c.sector, COUNT(DISTINCT tp.person_id) AS cnt
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
        JOIN companies c ON c.id = p.current_company_id AND c.sector IS NOT NULL
        WHERE tp.team_member_id = $2 AND tp.tenant_id = $1
        GROUP BY c.sector ORDER BY cnt DESC LIMIT 5
      `, [user.tenant_id, user.id]);

      // Get signals in user's focus areas (include platform-wide signals)
      const userRegion = user.region || '';
      const { rows: focusSignals } = await pool.query(`
        SELECT se.signal_type, se.company_name, c.geography
        FROM signal_events se
        LEFT JOIN companies c ON c.id = se.company_id
        WHERE (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '24 hours'
          AND se.confidence_score >= 0.5
        ORDER BY se.confidence_score DESC LIMIT 20
      `, [user.tenant_id]);

      // Get recent billing/revenue activity for this tenant
      const { rows: recentRevenue } = await pool.query(`
        SELECT conv.client_name_raw, conv.role_title, conv.placement_fee, conv.currency,
               conv.start_date, conv.payment_status, conv.consultant_name
        FROM conversions conv
        WHERE conv.tenant_id = $1
          AND (conv.created_at > NOW() - INTERVAL '7 days' OR conv.start_date > NOW() - INTERVAL '7 days')
        ORDER BY conv.created_at DESC LIMIT 10
      `, [user.tenant_id]).catch(() => ({ rows: [] }));

      // Get signals in network-strong but NON-focus areas (the crossover)
      const networkRegions = geoDist.filter(g => g.region !== 'OTHER').map(g => g.region);
      const focusRegions = userRegion.split(',').map(r => r.trim()).filter(Boolean);
      const crossoverRegions = networkRegions.filter(r => !focusRegions.includes(r));

      // Parse user profile for intents
      let intents = [];
      let sectors = [];
      try {
        const profile = typeof user.profile === 'string' ? JSON.parse(user.profile) : user.profile;
        intents = profile?.intents || [];
        sectors = profile?.sectors || [];
      } catch {}

      if (geoDist.length === 0 && focusSignals.length === 0) continue;

      // Build Claude prompt
      const networkSummary = geoDist.map(g => `${g.region}: ${g.cnt} contacts`).join(', ');
      const sectorSummary = sectorDist.map(s => `${s.sector}: ${s.cnt}`).join(', ');
      const signalSummary = focusSignals.slice(0, 10).map(s => `${s.company_name} (${s.signal_type})`).join(', ');
      const crossoverNote = crossoverRegions.length > 0
        ? `Network is strong in ${crossoverRegions.join(', ')} but user focus is ${focusRegions.join(', ') || 'not set'}.`
        : '';

      const revenueSummary = recentRevenue.length > 0
        ? recentRevenue.map(r => `${r.client_name_raw}: ${r.role_title || 'placement'} (${r.currency || 'AUD'} ${r.placement_fee ? Math.round(r.placement_fee).toLocaleString() : '?'}, ${r.payment_status || 'pending'}${r.consultant_name ? ', ' + r.consultant_name : ''})`).join('; ')
        : '';

      const prompt = `Generate a 3-4 sentence daily network intelligence insight for a professional.

User: ${user.name || user.email}
Role/Vertical: ${user.vertical || 'revenue'}
Focus regions: ${focusRegions.join(', ') || 'Global'}
Intents: ${intents.join(', ') || 'general intelligence'}
Focus sectors: ${sectors.join(', ') || 'cross-sector'}

Network distribution: ${networkSummary}
Sector presence: ${sectorSummary}
${crossoverNote}

Today's signals (last 24h): ${signalSummary || 'No signals detected'}
${revenueSummary ? `\nRecent revenue activity (last 7 days): ${revenueSummary}` : ''}

Write a concise, actionable insight. Highlight:
1. Any geographic arbitrage (network strength vs focus gap)
2. Cross-sector signals that affect their intents
3. Revenue/billing patterns — client relationships signalling, repeat business, or new market entry
4. One specific action they could take today

No greetings. No filler. Start with the insight. 3-4 sentences max.`;

      const insightText = await callClaude(
        'You generate brief, actionable daily intelligence insights for professionals. No filler. No greetings. Direct observations only.',
        prompt, 300
      );

      // Generate headline
      const headline = await callClaude(
        'Generate a 6-8 word headline for this insight. No quotes. No period.',
        insightText, 30
      );

      // Store
      await pool.query(`
        INSERT INTO daily_insights (tenant_id, user_id, insight_date, insight_type, headline, body, structured_data, network_snapshot)
        VALUES ($1, $2, CURRENT_DATE, 'daily_crossover', $3, $4, $5, $6)
        ON CONFLICT (tenant_id, user_id, insight_date, insight_type) DO UPDATE SET
          headline = EXCLUDED.headline, body = EXCLUDED.body,
          structured_data = EXCLUDED.structured_data, network_snapshot = EXCLUDED.network_snapshot,
          generated_at = NOW()
      `, [
        user.tenant_id, user.id,
        headline.trim(),
        insightText.trim(),
        JSON.stringify({ intents, sectors, focus_regions: focusRegions, crossover_regions: crossoverRegions, signal_count: focusSignals.length }),
        JSON.stringify({ geography: geoDist, sectors: sectorDist, total_contacts: geoDist.reduce((s, g) => s + parseInt(g.cnt), 0) }),
      ]);

      generated++;
      console.log(`     ✅ ${user.name || user.email}: "${headline.trim().slice(0, 50)}"`);
    } catch (err) {
      console.log(`     ⚠️  ${user.name || user.email}: ${err.message}`);
    }

    await sleep(500); // Rate limit
  }

  console.log(`     ${generated} insights generated`);
  return { generated };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 11: DAILY DIGEST EMAIL
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineDailyDigestEmail() {
  let sent = 0;
  try {
    const { sendDailyDigest } = require('../lib/email');

    // Get all users in active tenants
    const { rows: users } = await pool.query(`
      SELECT u.id, u.email, u.name, u.tenant_id
      FROM users u
      JOIN tenants t ON t.id = u.tenant_id
      WHERE t.onboarding_status = 'complete'
        AND u.email IS NOT NULL
    `);

    for (const user of users) {
      try {
        // Get top signals for this user's tenant (last 24h)
        const { rows: signals } = await pool.query(`
          SELECT signal_type, company_name, confidence_score, evidence_summary
          FROM signal_events
          WHERE (tenant_id IS NULL OR tenant_id = $1)
            AND detected_at > NOW() - INTERVAL '24 hours'
          ORDER BY confidence_score DESC
          LIMIT 5
        `, [user.tenant_id]);

        if (signals.length === 0) continue; // Skip if no signals

        // Get today's insight
        const { rows: [insight] } = await pool.query(`
          SELECT headline, body FROM daily_insights
          WHERE (user_id = $1 OR user_id IS NULL) AND tenant_id = $2
          AND insight_date >= CURRENT_DATE - 1
          ORDER BY insight_date DESC LIMIT 1
        `, [user.id, user.tenant_id]);

        // Get upcoming event count
        const { rows: [ev] } = await pool.query(`
          SELECT COUNT(*) AS cnt FROM events
          WHERE (tenant_id IS NULL OR tenant_id = $1)
            AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 7
        `, [user.tenant_id]);

        await sendDailyDigest({
          to: user.email,
          name: user.name,
          signals,
          insight: insight || null,
          eventCount: parseInt(ev?.cnt) || 0,
        });

        sent++;
        await sleep(200); // Rate limit
      } catch (e) {
        console.log(`     ⚠️  ${user.email}: ${e.message}`);
      }
    }
  } catch (e) {
    console.log('     ⚠️  Daily digest error:', e.message);
  }

  console.log(`     ${sent} digest emails sent`);
  return { sent };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE REGISTRY
// ═══════════════════════════════════════════════════════════════════════════════

const PIPELINES = {
  ingest_signals: {
    name: 'Ingest & Detect Signals',
    icon: '📡',
    fn: pipelineIngestSignals,
    schedule: '2,32 * * * *',
    description: 'RSS → Embed → Claude Signal Detection → Network Cross-ref'
  },
  embed_intelligence: {
    name: 'Embed Intelligence',
    icon: '🧠',
    fn: pipelineEmbedIntelligence,
    schedule: '15 */4 * * *',
    description: 'Embed research notes, drops, and interactions into Qdrant'
  },
  compute_scores: {
    name: 'Compute Scores',
    icon: '📊',
    fn: pipelineComputeScores,
    schedule: '8 * * * *',
    description: 'Update engagement, timing, flight risk scores'
  },
  match_searches: {
    name: 'Match Searches',
    icon: '🎯',
    fn: pipelineMatchSearches,
    schedule: '30 */6 * * *',
    description: 'Match candidates to active search briefs via Qdrant vectors'
  },
  enrich_content: {
    name: 'Enrich Content',
    icon: '📝',
    fn: pipelineEnrichContent,
    schedule: '45 */4 * * *',
    description: 'Poll candidate blogs/podcasts, analyze via Claude, embed'
  },
  daily_brief: {
    name: 'Daily Brief',
    icon: '📋',
    fn: pipelineDailyBrief,
    schedule: '3 6 * * *',
    description: 'Generate daily intelligence briefing via Claude'
  },
  network_insights: {
    name: 'Network Insights',
    icon: '🧭',
    fn: pipelineNetworkInsights,
    schedule: '30 6 * * *',
    description: 'Daily crossover intelligence — network vs focus gap analysis per user'
  },
  company_relationships: {
    name: 'Company Relationships',
    icon: '🏢',
    fn: async () => {
      const { compute } = require('./compute_company_relationships');
      return compute();
    },
    schedule: '12 3 * * *',
    description: 'Score company-level relationship quality from interaction data'
  },
  detect_rel_changes: {
    name: 'Relationship Changes',
    icon: '🔄',
    fn: async () => {
      const { detect } = require('./detect_relationship_changes');
      return detect();
    },
    schedule: '18 */6 * * *',
    description: 'Detect staleness and relationship tier changes every 6h'
  },
  daily_digest_email: {
    name: 'Weekly Digest Email',
    icon: '📧',
    fn: pipelineDailyDigestEmail,
    schedule: '45 6 * * 1',
    description: 'Email daily signal digest + insight to all active users'
  },
  waitlist_digest: {
    name: 'Waitlist Digest',
    icon: '📝',
    fn: pipelineWaitlistDigest,
    schedule: '22 7 * * *',
    description: 'Daily waitlist activity digest — new signups, pending count'
  },
  harvest_events: {
    name: 'Harvest Events',
    icon: '📅',
    fn: async () => {
      const { harvestEvents } = require('./harvest_events');
      const result = await harvestEvents();
      // Run entity linking after harvest
      try {
        const { linkEvents } = require('./link_events');
        await linkEvents();
      } catch (e) { console.warn('Event linking skipped:', e.message); }
      // Run embedding after linking
      try {
        const { embedEvents } = require('./embed_events');
        await embedEvents();
      } catch (e) { console.warn('Event embedding skipped:', e.message); }
      return result;
    },
    schedule: '14 */2 * * *',
    description: 'Fetch EventMedium RSS feeds → Link entities → Embed to Qdrant'
  },
  sync_xero: {
    name: 'Sync Xero Invoices',
    icon: '💰',
    fn: async () => {
      const { pipelineSyncXero } = require('./sync_xero');
      return pipelineSyncXero();
    },
    schedule: '6 7,19 * * *',
    description: 'Fetch new/updated invoices from Xero, update placements & financials'
  },
  signal_dispatch: {
    name: 'Signal Dispatch Generator',
    icon: '🎯',
    fn: async () => {
      const { generateDispatches } = require('./generate_dispatches');
      return generateDispatches();
    },
    schedule: '24 */2 * * *',
    description: 'Generate intelligence briefs with proximity maps, approach angles, and thought leadership content'
  },
  compute_network_topology: {
    name: 'Compute Network Topology',
    icon: '🌐',
    fn: async () => {
      const { computeNetworkTopology } = require('./compute_network_topology');
      return computeNetworkTopology();
    },
    schedule: '28 2 * * *',
    description: 'Compute network density, company adjacency scores, and geo mapping'
  },
  compute_triangulation: {
    name: 'Compute Triangulation',
    icon: '🔺',
    fn: async () => {
      const { computeTriangulation } = require('./compute_triangulation');
      return computeTriangulation();
    },
    schedule: '30 */2 * * *',
    description: 'Triangulate signals × network × geo into ranked opportunities with explainable scores'
  },

  compute_signal_grabs: {
    name: 'Signal Grabs',
    icon: '📰',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'compute_signal_grabs.js'), { timeout: 120000, stdio: 'inherit' });
    },
    schedule: '16 5 * * *',
    description: 'Generate daily editorial intelligence grabs from signal clusters'
  },

  weekly_wrap: {
    name: 'Weekly Wrap',
    icon: '📰',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'generate_weekly_wrap.js'), { timeout: 180000, stdio: 'inherit' });
    },
    schedule: '36 6 * * 0',
    description: 'Generate weekly regional intelligence wrap with key numbers and insights'
  },

  ingest_events: {
    name: 'EventMedium Ingestion',
    icon: '🎪',
    fn: pipelineIngestEvents,
    schedule: '42 */4 * * *',
    description: 'Poll EventMedium feed, upsert upcoming events with region bucketing and theme scoring'
  },

  harvest_podcasts: {
    name: 'Podcast Harvest',
    icon: '🎙️',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'seed_harvest_podcasts.js'), { timeout: 300000, stdio: 'inherit' });
    },
    schedule: '48 4 * * *',
    description: 'Harvest new episodes from all podcast RSS feeds into external_documents'
  },

  compute_signal_index: {
    name: 'Compute Signal Index',
    icon: '📈',
    fn: async () => {
      const { computeSignalIndex } = require('./compute_signal_index');
      return computeSignalIndex();
    },
    schedule: '5,35 * * * *',
    description: 'Compute market health index, signal stocks, sector indices (every 30m)'
  },

  backfill_newsletters: {
    name: 'Backfill Newsletters',
    icon: '📰',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'backfill_newsletters.js'), { timeout: 600000, stdio: 'inherit' });
    },
    description: 'Backfill newsletter emails from Gmail (last 6 months) into signal pipeline'
  },

  sync_gmail: {
    name: 'Gmail Sync',
    icon: '📧',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'sync_gmail.js'), { timeout: 600000, stdio: 'inherit' });
    },
    schedule: '4 */4 * * *',
    description: 'Delta sync Gmail threads for connected accounts → interactions + team_proximity'
  },

  gmail_match: {
    name: 'Gmail Match & Signals',
    icon: '🔗',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'gmail_match.js'), { timeout: 180000, stdio: 'inherit' });
    },
    schedule: '11 */2 * * *',
    description: 'Match synced emails to people, compute engagement signals, update person_scores'
  },

  sync_drive: {
    name: 'Google Drive Sync',
    icon: '📁',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'sync_drive.js'), { timeout: 300000, stdio: 'inherit' });
    },
    schedule: '34 */2 * * *',
    description: 'Scan connected Google accounts for new/modified Drive documents, ingest as companion data'
  },

  sync_contacts: {
    name: 'Google Contacts Sync',
    icon: '👥',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'sync_contacts_delta.js'), { timeout: 300000, stdio: 'inherit' });
    },
    schedule: '45 */6 * * *',  // Every 6 hours at :45
    description: 'Delta sync Google Contacts → fill-blank enrichment on people records'
  },

  sync_calendar: {
    name: 'Google Calendar Sync',
    icon: '📅',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'sync_calendar.js'), { timeout: 300000, stdio: 'inherit' });
    },
    schedule: '20 */4 * * *',  // Every 4 hours at :20
    description: 'Sync calendar events → meeting interactions + team_proximity + upcoming meeting signals'
  },

  extract_companies: {
    name: 'Company Extraction',
    icon: '🏢',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'extract_companies_from_contacts.js'), { timeout: 900000, stdio: 'inherit' });
    },
    schedule: '52 5 * * *',
    description: 'Extract company records from contact names + email domains, link people, derive sectors'
  },

  sync_telegram: {
    name: 'Telegram Sync',
    icon: '💬',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'sync_telegram.js'), { timeout: 600000, stdio: 'inherit' });
    },
    schedule: '40 */4 * * *',  // Every 4 hours at :40
    description: 'MTProto sync of Telegram private chats → interactions + team_proximity'
  },

  migrate_wip_schema: {
    name: 'WIP Schema Migration',
    icon: '🔧',
    fn: async () => {
      const fs = require('fs');
      const sqlPath = require('path').join(__dirname, '..', 'sql', 'migration_wip_workbook.sql');
      if (!fs.existsSync(sqlPath)) throw new Error('Migration file not found: ' + sqlPath);
      const sql = fs.readFileSync(sqlPath, 'utf8');
      // Run each statement separately (some may fail if already applied)
      const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 5 && !s.startsWith('--'));
      let applied = 0, skipped = 0;
      for (const stmt of statements) {
        try { await pool.query(stmt); applied++; } catch (e) { skipped++; }
      }
      return { applied, skipped, total: statements.length };
    },
    description: 'Apply WIP workbook schema changes (relaxed NOT NULL, new columns, receivables table)'
  },

  ingest_wip_invoices: {
    name: 'Ingest Invoice Ledgers',
    icon: '💷',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'ingest_invoice_ledgers.js'), { timeout: 300000, stdio: 'inherit' });
    },
    description: 'Ingest Xero invoice exports from ML UK + ML AU sheets in the WIP workbook'
  },

  ingest_wip_consultants: {
    name: 'Ingest Consultant WIP',
    icon: '📊',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'ingest_consultant_wip.js'), { timeout: 600000, stdio: 'inherit' });
    },
    description: 'Ingest 25 consultant WIP sheets (~1,300 opportunity/placement records) from the WIP workbook'
  },

  ingest_receivables: {
    name: 'Ingest Receivables',
    icon: '📋',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'ingest_receivables.js'), { timeout: 120000, stdio: 'inherit' });
    },
    description: 'Ingest outstanding receivables/debtors from the WIP workbook'
  },

  cleanup_broken_podcasts: {
    name: 'Cleanup Broken Podcasts',
    icon: '🧹',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'cleanup_broken_podcasts.js'), { timeout: 30000, stdio: 'inherit' });
    },
    schedule: null,
    description: 'Delete episodes from My First Million, Equity TechCrunch, Masters of Scale'
  },

  import_case_studies_bulk: {
    name: 'Bulk Import Case Studies',
    icon: '📚',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'import_case_studies_csv.js'), { timeout: 120000, stdio: 'inherit' });
    },
    schedule: null, // Manual only — no cron
    description: 'One-time bulk import of 177 case studies from PDF export'
  },

  classify_documents: {
    name: 'Document Classification',
    icon: '🏷️',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'classify_documents.js') + ' --limit 50', { timeout: 600000, stdio: 'inherit' });
    },
    schedule: '45 */2 * * *',
    description: 'Classify Drive documents, extract case studies, identify shortlisted candidates in pitch decks'
  },

  harvest_gdelt: {
    name: 'GDELT Global Intelligence',
    icon: '🌍',
    fn: async () => {
      const { harvestGDELT } = require('./harvest_gdelt');
      return harvestGDELT();
    },
    schedule: '10 */2 * * *',
    description: 'Query GDELT for global signals across 100+ languages — geographic expansion, M&A, capital raising, leadership changes'
  },

  watchdog: {
    name: 'Platform Watchdog',
    icon: '🐕',
    fn: async () => {
      const { pipelineWatchdog } = require('./watchdog');
      return pipelineWatchdog();
    },
    schedule: '56 */2 * * *',
    description: 'Monitor pipeline freshness, data quality, external services, RSS health — alert on critical/high issues'
  },

  harvest_official_apis: {
    name: 'Official API Harvest',
    icon: '🏛️',
    fn: async () => {
      const { harvestOfficialApis } = require('./harvest_official_apis');
      return harvestOfficialApis();
    },
    schedule: '38 */6 * * *',
    description: 'Harvest structured government/institutional APIs — procurement, patents, statistics, filings'
  },

  discover_ats: {
    name: 'ATS Discovery',
    icon: '🔍',
    fn: async () => {
      const { execSync } = require('child_process');
      execSync('node ' + require('path').join(__dirname, 'discover_ats.js') + ' --limit 1000', { timeout: 600000, stdio: 'inherit' });
    },
    schedule: '50 2 * * 1',
    description: 'Detect ATS providers for companies, register job feeds for harvesting'
  },

  harvest_jobs: {
    name: 'Job Feed Harvest',
    icon: '💼',
    fn: async () => {
      const { harvestAllJobFeeds } = require('./harvest_jobs');
      return harvestAllJobFeeds();
    },
    schedule: '44 */6 * * *',
    description: 'Fetch job postings from discovered ATS feeds, detect removals, evaluate signals'
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE STATE & RUNNER
// ═══════════════════════════════════════════════════════════════════════════════

const pipelineState = {};
Object.keys(PIPELINES).forEach(k => {
  pipelineState[k] = { status: 'idle', last_run: null, last_duration_ms: null, last_error: null, run_count: 0 };
});

const INIT_SQL = `
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_key VARCHAR(50) NOT NULL,
  pipeline_name VARCHAR(100),
  status VARCHAR(20) DEFAULT 'running',
  started_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  duration_ms INTEGER,
  items_processed JSONB,
  error_message TEXT,
  triggered_by VARCHAR(20) DEFAULT 'scheduler'
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_key ON pipeline_runs(pipeline_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at DESC);
`;

// ─── Concurrency semaphore — max 3 pipelines at once ───
const MAX_CONCURRENT = 3;
let runningCount = 0;
const waitQueue = [];

function acquireSemaphore() {
  if (runningCount < MAX_CONCURRENT) {
    runningCount++;
    return Promise.resolve();
  }
  return new Promise(resolve => waitQueue.push(resolve));
}

function releaseSemaphore() {
  runningCount--;
  if (waitQueue.length > 0 && runningCount < MAX_CONCURRENT) {
    runningCount++;
    waitQueue.shift()();
  }
}

async function runPipeline(key, triggeredBy = 'scheduler') {
  const pipeline = PIPELINES[key];
  if (!pipeline) { console.error(`❌ Unknown: ${key}`); return; }
  if (pipelineState[key].status === 'running') { console.log(`⏭️  ${pipeline.name} already running`); return; }

  await acquireSemaphore();

  const start = Date.now();
  pipelineState[key].status = 'running';
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`${pipeline.icon} ${pipeline.name}`);
  console.log(`   ${pipeline.description}`);
  console.log(`${'─'.repeat(60)}`);

  let runId;
  try {
    const r = await pool.query(
      'INSERT INTO pipeline_runs (pipeline_key, pipeline_name, triggered_by) VALUES ($1,$2,$3) RETURNING id',
      [key, pipeline.name, triggeredBy]
    );
    runId = r.rows[0].id;
  } catch (e) {}

  try {
    const result = await pipeline.fn();
    const duration = Date.now() - start;

    pipelineState[key] = { status: 'complete', last_run: new Date().toISOString(), last_duration_ms: duration, last_error: null, run_count: (pipelineState[key].run_count || 0) + 1 };

    if (runId) {
      await pool.query(
        'UPDATE pipeline_runs SET status=$1, completed_at=NOW(), duration_ms=$2, items_processed=$3 WHERE id=$4',
        ['complete', duration, JSON.stringify(result), runId]
      ).catch(() => {});
    }

    console.log(`   ✅ Done in ${(duration/1000).toFixed(1)}s`);
    releaseSemaphore();
    return result;

  } catch (e) {
    const duration = Date.now() - start;
    pipelineState[key] = { ...pipelineState[key], status: 'error', last_run: new Date().toISOString(), last_duration_ms: duration, last_error: e.message };

    if (runId) {
      await pool.query(
        'UPDATE pipeline_runs SET status=$1, completed_at=NOW(), duration_ms=$2, error_message=$3 WHERE id=$4',
        ['error', duration, e.message, runId]
      ).catch(() => {});
    }

    console.error(`   ❌ Failed (${(duration/1000).toFixed(1)}s): ${e.message}`);
    releaseSemaphore();
  }

  // Reset to idle
  setTimeout(() => { if (pipelineState[key].status !== 'running') pipelineState[key].status = 'idle'; }, 5000);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SCHEDULER
// ═══════════════════════════════════════════════════════════════════════════════

async function startScheduler() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   MitchelLake Signal Pipeline Scheduler                   ║');
  console.log('║   Qdrant + OpenAI + Anthropic Claude                      ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  // Check services
  console.log('Checking services:');
  console.log(`  Database:  ${process.env.DATABASE_URL ? '✅' : '❌'}`);
  console.log(`  Qdrant:    ${qdrantClient ? '✅' : '⚠️  not configured'}`);
  console.log(`  OpenAI:    ${openai ? '✅' : '⚠️  not configured'}`);
  console.log(`  Anthropic: ${ANTHROPIC_API_KEY ? '✅' : '⚠️  not configured'}`);
  console.log('');

  try {
    await pool.query(INIT_SQL);
    console.log('✅ Pipeline tracking table ready\n');
  } catch (e) {}

  // Schedule
  Object.entries(PIPELINES).forEach(([key, pipeline]) => {
    if (pipeline.schedule && cron.validate(pipeline.schedule)) {
      cron.schedule(pipeline.schedule, () => runPipeline(key, 'scheduler'));
      console.log(`  📅 ${pipeline.icon} ${pipeline.name} → ${pipeline.schedule}`);
    }
  });

  console.log('\n✅ All pipelines scheduled\n');
  console.log('Press Ctrl+C to stop\n');

  // Skip initial pipeline run — let cron schedules handle it
  // Running ingest_signals on startup saturates DB connections
  // and blocks web process auth/API queries
  console.log('  ℹ️  Pipelines will run on their cron schedules (ingest: */30, scores: */2h, index: */30m)');
}

async function runAll() {
  console.log('\n🚀 Running all pipelines...\n');
  for (const key of Object.keys(PIPELINES)) {
    await runPipeline(key, 'manual');
  }
  console.log('\n✅ All pipelines complete');
}

// ─── API routes for server.js integration ───
function registerRoutes(app, authenticateToken) {
  app.get('/api/pipelines', authenticateToken, (req, res) => {
    const status = {};
    Object.entries(PIPELINES).forEach(([key, p]) => {
      status[key] = { name: p.name, icon: p.icon, schedule: p.schedule, description: p.description, ...pipelineState[key] };
    });
    res.json(status);
  });

  app.post('/api/pipelines/:key/run', authenticateToken, async (req, res) => {
    const { key } = req.params;
    if (!PIPELINES[key]) return res.status(404).json({ error: 'Unknown pipeline' });
    res.json({ message: `${key} triggered` });
    runPipeline(key, 'api');
  });

  app.get('/api/pipelines/runs', authenticateToken, async (req, res) => {
    const r = await pool.query('SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 50').catch(() => ({ rows: [] }));
    res.json(r.rows);
  });
}

// ─── CLI ───
const args = process.argv.slice(2);
if (args.includes('--status')) { console.log(JSON.stringify(pipelineState, null, 2)); process.exit(0); }
else if (args.includes('--run-now')) { runAll().then(() => { pool.end(); process.exit(0); }); }
else if (args.includes('--run')) { const k = args[args.indexOf('--run') + 1]; runPipeline(k, 'manual').then(() => { pool.end(); process.exit(0); }); }
else { startScheduler(); }

module.exports = { runPipeline, PIPELINES, pipelineState, registerRoutes, startScheduler };
