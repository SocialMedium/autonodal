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
  max: 5
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
  const parser = new Parser({ timeout: 10000 });

  for (const source of sources.rows) {
    try {
      const feed = await parser.parseURL(source.url);
      stats.fetched++;

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

        await pool.query(`
          INSERT INTO external_documents (
            source_id, source_url, source_url_hash, source_type, source_name, title, content, 
            published_at, author
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (source_url_hash) DO NOTHING
        `, [
          source.id, url, urlHash,
          'rss',
          source.name,
          (item.title || '').slice(0, 500),
          content,
          item.isoDate || item.pubDate || new Date().toISOString(),
          (item.creator || item.author || '').slice(0, 200)
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
    SELECT id, title, content, source_name, published_at
    FROM external_documents 
    WHERE signals_computed_at IS NULL AND embedded_at IS NOT NULL
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

          // Find or create company
          let companyId;
          const companyResult = await pool.query(
            `SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`,
            [signal.company]
          );

          if (companyResult.rows.length > 0) {
            companyId = companyResult.rows[0].id;
          } else {
            const newCompany = await pool.query(
              `INSERT INTO companies (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`,
              [signal.company]
            );
            companyId = newCompany.rows[0]?.id;
          }

          if (companyId) {
            await pool.query(`
              INSERT INTO signal_events (
                company_id, signal_type, signal_category,
                confidence_score, evidence_summary, hiring_implications,
                source_document_id, detected_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
              ON CONFLICT DO NOTHING
            `, [
              companyId,
              signal.signal_type,
              'market',
              signal.confidence || 0.5,
              signal.summary,
              JSON.stringify(signal.hiring_implications || ''),
              batch[signal.document_index - 1]?.id || batch[0]?.id
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
    SELECT se.*, c.name as company_name 
    FROM signal_events se
    JOIN companies c ON se.company_id = c.id
    WHERE se.detected_at > NOW() - INTERVAL '1 day'
    AND se.triage_notes IS NULL
  `);

  let crossRefCount = 0;
  for (const signal of recentSignals.rows) {
    // Find people in our network at this company
    const affected = await pool.query(`
      SELECT id, full_name, current_title 
      FROM people 
      WHERE LOWER(current_company_name) LIKE LOWER($1)
    `, [`%${signal.company_name}%`]);

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

  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE 2: EMBED INTELLIGENCE (research notes, drops, interactions)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineEmbedIntelligence() {
  const stats = { notes: 0, drops: 0, interactions: 0 };

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
           p.created_at,
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
    // Check for company signals (flight risk indicator)
    const companySignals = await pool.query(`
      SELECT signal_type, confidence FROM signal_events se
      JOIN companies c ON se.company_id = c.id
      WHERE LOWER(c.name) LIKE LOWER($1)
      AND se.detected_at > NOW() - INTERVAL '30 days'
    `, [`%${person.current_company_name || 'NONE'}%`]).catch(() => ({ rows: [] }));

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
  const stats = { searches: 0, matches: 0 };

  if (!openai || !qdrantClient) {
    console.log('     ⚠️  OpenAI or Qdrant not configured, skipping');
    return stats;
  }

  console.log('   🎯 Matching candidates to active searches...');

  const searches = await pool.query(`
    SELECT id, title, brief_summary, role_overview, required_experience, 
           ideal_background, location, seniority_level, target_companies
    FROM searches WHERE status = 'active'
  `).catch(() => ({ rows: [] }));

  for (const search of searches.rows) {
    // Build search embedding from brief
    const briefText = [
      search.title,
      search.brief_summary,
      search.role_overview,
      search.required_experience,
      search.ideal_background,
      search.location ? `Location: ${search.location}` : '',
      search.seniority_level ? `Seniority: ${search.seniority_level}` : ''
    ].filter(Boolean).join('\n');

    if (!briefText.trim()) continue;

    const searchVector = await embed(briefText);

    // Search Qdrant people collection
    const results = await qdrantSearch('people', searchVector, 50);

    for (const result of results) {
      if (result.score < 0.3) continue; // Minimum similarity threshold

      const personId = result.payload?.person_id;
      if (!personId) continue;

      await pool.query(`
        INSERT INTO search_matches (search_id, person_id, overall_match_score, match_reasons, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (search_id, person_id) DO UPDATE SET
          overall_match_score = $3, match_reasons = $4, updated_at = NOW()
      `, [
        search.id,
        personId,
        result.score,
        JSON.stringify({
          similarity: result.score,
          source: result.payload?.type || 'person',
          content_preview: result.payload?.content_preview?.slice(0, 200)
        })
      ]).catch(() => {});

      stats.matches++;
    }

    stats.searches++;
  }

  console.log(`     ${stats.searches} searches processed, ${stats.matches} matches found`);
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
          (item.title || '').slice(0, 500),
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
      ORDER BY se.confidence_score_score DESC LIMIT 30
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
// PIPELINE REGISTRY
// ═══════════════════════════════════════════════════════════════════════════════

const PIPELINES = {
  ingest_signals: {
    name: 'Ingest & Detect Signals',
    icon: '📡',
    fn: pipelineIngestSignals,
    schedule: '*/30 * * * *',
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
    schedule: '0 * * * *',
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
    schedule: '0 6 * * *',
    description: 'Generate daily intelligence briefing via Claude'
  },
  sync_xero: {
    name: 'Sync Xero Invoices',
    icon: '💰',
    fn: async () => {
      const { pipelineSyncXero } = require('./sync_xero');
      return pipelineSyncXero();
    },
    schedule: '0 7,19 * * *',
    description: 'Fetch new/updated invoices from Xero, update placements & financials'
  },
  signal_dispatch: {
    name: 'Signal Dispatch Generator',
    icon: '🎯',
    fn: async () => {
      const { generateDispatches } = require('./generate_dispatches');
      return generateDispatches();
    },
    schedule: '0 */2 * * *',
    description: 'Generate intelligence briefs with proximity maps, approach angles, and thought leadership content'
  },
  compute_network_topology: {
    name: 'Compute Network Topology',
    icon: '🌐',
    fn: async () => {
      const { computeNetworkTopology } = require('./compute_network_topology');
      return computeNetworkTopology();
    },
    schedule: '0 2 * * *',
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

async function runPipeline(key, triggeredBy = 'scheduler') {
  const pipeline = PIPELINES[key];
  if (!pipeline) { console.error(`❌ Unknown: ${key}`); return; }
  if (pipelineState[key].status === 'running') { console.log(`⏭️  ${pipeline.name} already running`); return; }

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

  // Run ingest on startup
  console.log('🚀 Running initial pipeline cycle...\n');
  await runPipeline('ingest_signals', 'startup');
  await runPipeline('compute_scores', 'startup');
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

module.exports = { runPipeline, PIPELINES, pipelineState, registerRoutes };
