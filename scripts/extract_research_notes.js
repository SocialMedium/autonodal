#!/usr/bin/env node
/**
 * MitchelLake Research Note Intelligence Extractor
 * 
 * Reads 4,600+ research notes from interactions table,
 * sends to Claude for structured extraction, stores results,
 * and embeds in Qdrant for semantic search.
 * 
 * Extracts:
 *  - Compensation expectations (salary, bonus, equity)
 *  - Notice periods
 *  - Location constraints
 *  - Motivations & preferences
 *  - Deal-breakers
 *  - Assessment data (strengths, gaps)
 *  - Timing signals
 *  - Relationship mentions
 * 
 * Usage:
 *   node scripts/extract_research_notes.js              # Process all unprocessed
 *   node scripts/extract_research_notes.js --limit 50   # Process 50 notes
 *   node scripts/extract_research_notes.js --dry-run    # Preview without saving
 *   node scripts/extract_research_notes.js --embed-only # Just embed already-extracted
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';

let openai;
try {
  const OpenAI = require('openai');
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
} catch (e) {}

let qdrantClient;
try {
  const { QdrantClient } = require('@qdrant/js-client-rest');
  qdrantClient = new QdrantClient({
    url: process.env.QDRANT_URL,
    apiKey: process.env.QDRANT_API_KEY
  });
} catch (e) {}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════════════════════════════════════
// SCHEMA: Add extraction columns to interactions
// ═══════════════════════════════════════════════════════════════════════════════

const SCHEMA_SQL = `
-- Add extraction columns if they don't exist
DO $$ BEGIN
  ALTER TABLE interactions ADD COLUMN IF NOT EXISTS extracted_intelligence JSONB;
  ALTER TABLE interactions ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMPTZ;
  ALTER TABLE interactions ADD COLUMN IF NOT EXISTS extraction_version INTEGER DEFAULT 0;
  ALTER TABLE interactions ADD COLUMN IF NOT EXISTS note_quality VARCHAR(20);
  ALTER TABLE interactions ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Create person_constraints table for structured constraint data
CREATE TABLE IF NOT EXISTS person_constraints (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  person_id UUID NOT NULL REFERENCES people(id),
  constraint_type VARCHAR(50) NOT NULL,
  value TEXT NOT NULL,
  detail JSONB,
  hard_or_soft VARCHAR(10) DEFAULT 'soft',
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ,
  confidence NUMERIC(3,2) DEFAULT 0.7,
  source_interaction_id UUID REFERENCES interactions(id),
  source_type VARCHAR(30) DEFAULT 'research_note',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_person_constraints_person ON person_constraints(person_id);
CREATE INDEX IF NOT EXISTS idx_person_constraints_type ON person_constraints(constraint_type);
`;

// ═══════════════════════════════════════════════════════════════════════════════
// CALL CLAUDE: Extract intelligence from research notes
// ═══════════════════════════════════════════════════════════════════════════════

async function extractBatch(notes) {
  const notesText = notes.map((n, i) => 
    `--- NOTE ${i + 1} (Person: ${n.full_name}, ID: ${n.person_id}) ---\n${stripHtml(n.summary).slice(0, 2000)}`
  ).join('\n\n');

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 4096,
      system: `You are an intelligence extractor for MitchelLake, an executive search firm. You receive raw research notes about candidates — often messy, abbreviated, with shorthand.

Your job: extract ALL structured intelligence from each note.

Common shorthand:
- "Super" = superannuation (Australian retirement, typically 11.5%)
- "STI" = short-term incentive (bonus)
- "LTI" = long-term incentive (equity/options)
- "ESOP" = employee stock option plan
- "K" after number = thousand (e.g., "240K" = $240,000)
- "Syd/Melb/Bris" = Sydney/Melbourne/Brisbane
- "flex" = flexible
- "prods" = products

For EACH note, return a JSON object with this structure. Return an array of objects, one per note:

[{
  "note_index": 1,
  "quality": "high|medium|low|noise",
  
  "compensation": {
    "base_salary": {"amount": null, "currency": "AUD", "note": ""},
    "bonus": {"amount": null, "type": "STI|LTI|both", "note": ""},
    "equity": {"has_equity": false, "note": ""},
    "total_package": {"note": ""},
    "flexibility": "rigid|some_flex|very_flexible"
  },
  
  "notice_period": {"weeks": null, "note": ""},
  
  "location": {
    "current": "",
    "preferred": [],
    "flexibility": "local_only|state|national|international|remote_ok",
    "note": ""
  },
  
  "motivations": [],
  "dealbreakers": [],
  "preferences": [],
  
  "timing": {
    "availability": "immediate|weeks|months|not_now",
    "note": ""
  },
  
  "assessment": {
    "strengths": [],
    "gaps": [],
    "experience_highlights": [],
    "seniority": ""
  },
  
  "relationships_mentioned": [],
  "companies_mentioned": [],
  
  "signals": [{
    "type": "open_to_move|not_looking|frustrated|happy|flight_risk|returning_market|passive",
    "confidence": 0.0-1.0,
    "evidence": ""
  }],
  
  "key_quote": ""
}]

Rules:
- If info not present, use null/empty — don't guess
- Compensation: extract exact numbers when stated
- "quality" = "noise" for email threads, auto-replies, empty content
- "quality" = "high" for notes with compensation, timing, or detailed assessment
- "quality" = "medium" for notes with some useful context
- "quality" = "low" for minimal useful data
- Extract ALL mentioned people and companies
- Return ONLY valid JSON array`,
      messages: [{ role: 'user', content: notesText }]
    })
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Claude ${response.status}: ${err.slice(0, 200)}`);
  }

  const data = await response.json();
  const text = data.content[0]?.text || '';
  const cleaned = text.replace(/```json\s*/g, '').replace(/```/g, '').trim();
  return JSON.parse(cleaned);
}

function stripHtml(html) {
  if (!html) return '';
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

// ═══════════════════════════════════════════════════════════════════════════════
// STORE: Save extracted intelligence
// ═══════════════════════════════════════════════════════════════════════════════

async function storeExtraction(note, extraction) {
  // Update interaction with extraction
  await pool.query(`
    UPDATE interactions SET 
      extracted_intelligence = $1,
      extracted_at = NOW(),
      extraction_version = 1,
      note_quality = $2
    WHERE id = $3
  `, [
    JSON.stringify(extraction),
    extraction.quality || 'low',
    note.id
  ]);

  // Skip noise
  if (extraction.quality === 'noise') return { constraints: 0 };

  let constraintCount = 0;

  // Store compensation constraints
  if (extraction.compensation?.base_salary?.amount) {
    await upsertConstraint(note.person_id, 'compensation_base', 
      `${extraction.compensation.base_salary.amount}`,
      { ...extraction.compensation.base_salary, full_package: extraction.compensation },
      extraction.compensation.flexibility === 'rigid' ? 'hard' : 'soft',
      note.id
    );
    constraintCount++;
  }

  // Store notice period
  if (extraction.notice_period?.weeks) {
    await upsertConstraint(note.person_id, 'notice_period',
      `${extraction.notice_period.weeks} weeks`,
      extraction.notice_period,
      'hard', note.id
    );
    constraintCount++;
  }

  // Store location constraints
  if (extraction.location?.preferred?.length > 0) {
    await upsertConstraint(note.person_id, 'location',
      extraction.location.preferred.join(', '),
      extraction.location,
      extraction.location.flexibility === 'local_only' ? 'hard' : 'soft',
      note.id
    );
    constraintCount++;
  }

  // Store dealbreakers
  for (const db of (extraction.dealbreakers || [])) {
    await upsertConstraint(note.person_id, 'dealbreaker', db, {}, 'hard', note.id);
    constraintCount++;
  }

  // Store timing
  if (extraction.timing?.availability && extraction.timing.availability !== 'not_now') {
    await upsertConstraint(note.person_id, 'timing',
      extraction.timing.availability,
      extraction.timing,
      'soft', note.id
    );
    constraintCount++;
  }

  // Store motivations
  for (const mot of (extraction.motivations || [])) {
    await upsertConstraint(note.person_id, 'motivation', mot, {}, 'soft', note.id);
    constraintCount++;
  }

  // Store signals as person_signals
  for (const signal of (extraction.signals || [])) {
    if (signal.confidence >= 0.5) {
      await pool.query(`
        INSERT INTO person_signals (person_id, signal_type, signal_category, confidence, detail, source, detected_at)
        VALUES ($1, $2, 'computed', $3, $4, 'research_note_extraction', NOW())
        ON CONFLICT DO NOTHING
      `, [
        note.person_id,
        signal.type,
        signal.confidence,
        JSON.stringify({ evidence: signal.evidence, source_interaction: note.id })
      ]).catch(() => {});
    }
  }

  return { constraints: constraintCount };
}

async function upsertConstraint(personId, type, value, detail, hardSoft, sourceId) {
  await pool.query(`
    INSERT INTO person_constraints (person_id, constraint_type, value, detail, hard_or_soft, source_interaction_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT DO NOTHING
  `, [personId, type, value, JSON.stringify(detail), hardSoft, sourceId]).catch(() => {});
}

// ═══════════════════════════════════════════════════════════════════════════════
// EMBED: Vectorize research notes for semantic search
// ═══════════════════════════════════════════════════════════════════════════════

async function embedNotes(notes) {
  if (!openai || !qdrantClient) {
    console.log('  ⚠️  OpenAI or Qdrant not configured, skipping embedding');
    return 0;
  }

  const toEmbed = notes.filter(n => {
    const text = stripHtml(n.summary);
    return text.length > 50;
  });

  if (toEmbed.length === 0) return 0;

  console.log(`  📐 Embedding ${toEmbed.length} notes...`);

  let embedded = 0;
  for (let i = 0; i < toEmbed.length; i += 50) {
    const batch = toEmbed.slice(i, i + 50);
    
    const texts = batch.map(n => 
      `Candidate: ${n.full_name}\nRole: ${n.current_title || ''} @ ${n.current_company_name || ''}\n\n${stripHtml(n.summary).slice(0, 6000)}`
    );

    const response = await openai.embeddings.create({
      model: 'text-embedding-3-small',
      input: texts
    });

    const points = batch.map((n, idx) => ({
      id: Date.now() * 1000 + i + idx,
      vector: response.data[idx].embedding,
      payload: {
        type: 'research_note',
        person_id: n.person_id,
        person_name: n.full_name,
        interaction_id: n.id,
        content_preview: stripHtml(n.summary).slice(0, 400),
        quality: n.note_quality || 'unknown'
      }
    }));

    // Upsert to Qdrant
    try {
      await qdrantClient.getCollection('people');
    } catch (e) {
      await qdrantClient.createCollection('people', {
        vectors: { size: 1536, distance: 'Cosine' }
      });
    }

    await qdrantClient.upsert('people', { points });

    // Mark as embedded
    const ids = batch.map(n => n.id);
    await pool.query(`UPDATE interactions SET embedded_at = NOW() WHERE id = ANY($1)`, [ids]);

    embedded += batch.length;
    console.log(`     ${embedded}/${toEmbed.length} embedded`);
    
    if (i + 50 < toEmbed.length) await sleep(200);
  }

  return embedded;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const embedOnly = args.includes('--embed-only');
  const limitIdx = args.indexOf('--limit');
  const limit = limitIdx >= 0 ? parseInt(args[limitIdx + 1]) : 500;

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   Research Note Intelligence Extractor                    ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  // Init schema
  console.log('Setting up schema...');
  await pool.query(SCHEMA_SQL);
  console.log('✅ Schema ready\n');

  // Get stats
  const stats = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE extracted_at IS NOT NULL) as extracted,
      COUNT(*) FILTER (WHERE embedded_at IS NOT NULL) as embedded,
      COUNT(*) FILTER (WHERE LENGTH(summary) > 50 AND summary NOT LIKE '%wrote:%') as useful
    FROM interactions WHERE interaction_type = 'research_note'
  `);
  
  const { total, extracted, embedded, useful } = stats.rows[0];
  console.log(`Research notes: ${total} total, ${useful} useful, ${extracted} extracted, ${embedded} embedded\n`);

  if (embedOnly) {
    // Just embed already-extracted notes that aren't yet embedded
    const notes = await pool.query(`
      SELECT i.id, i.person_id, i.summary, i.note_quality,
             p.full_name, p.current_title, p.current_company_name
      FROM interactions i
      JOIN people p ON i.person_id = p.id
      WHERE i.interaction_type = 'research_note'
      AND i.extracted_at IS NOT NULL
      AND i.embedded_at IS NULL
      AND i.note_quality != 'noise'
      LIMIT $1
    `, [limit]);

    console.log(`Embedding ${notes.rows.length} extracted notes...\n`);
    const count = await embedNotes(notes.rows);
    console.log(`\n✅ Embedded ${count} notes`);
    await pool.end();
    return;
  }

  // Get unprocessed notes (filter out obvious email noise)
  const notes = await pool.query(`
    SELECT i.id, i.person_id, i.summary, i.interaction_at,
           p.full_name, p.current_title, p.current_company_name
    FROM interactions i
    JOIN people p ON i.person_id = p.id
    WHERE i.interaction_type = 'research_note'
    AND i.extracted_at IS NULL
    AND i.summary IS NOT NULL
    AND LENGTH(i.summary) > 30
    ORDER BY i.interaction_at DESC
    LIMIT $1
  `, [limit]);

  console.log(`Processing ${notes.rows.length} research notes in batches of 5...\n`);

  let totalProcessed = 0;
  let totalConstraints = 0;
  let totalHigh = 0;
  let totalMedium = 0;
  let totalNoise = 0;
  let errors = 0;
  const notesForEmbedding = [];

  // Process in batches of 5
  for (let i = 0; i < notes.rows.length; i += 5) {
    const batch = notes.rows.slice(i, i + 5);
    
    try {
      console.log(`  Batch ${Math.floor(i/5) + 1}/${Math.ceil(notes.rows.length/5)}: ${batch.map(n => n.full_name).join(', ')}`);
      
      const extractions = await extractBatch(batch);

      for (let j = 0; j < batch.length; j++) {
        const note = batch[j];
        const extraction = extractions[j];
        
        if (!extraction) continue;

        if (!dryRun) {
          const result = await storeExtraction(note, extraction);
          totalConstraints += result.constraints;
        }

        if (extraction.quality === 'high') totalHigh++;
        else if (extraction.quality === 'medium') totalMedium++;
        else if (extraction.quality === 'noise') totalNoise++;

        // Collect non-noise for embedding
        if (extraction.quality !== 'noise') {
          note.note_quality = extraction.quality;
          notesForEmbedding.push(note);
        }

        totalProcessed++;

        // Log interesting extractions
        if (extraction.quality === 'high') {
          const comp = extraction.compensation?.base_salary?.amount;
          const loc = extraction.location?.preferred?.join(', ');
          const signals = (extraction.signals || []).map(s => s.type).join(', ');
          console.log(`    ⭐ ${note.full_name}: ${comp ? '$'+comp : ''}${loc ? ' · '+loc : ''}${signals ? ' · '+signals : ''}`);
        }
      }
    } catch (e) {
      console.error(`    ❌ Batch error: ${e.message}`);
      errors++;
    }

    // Rate limit: 300ms between Claude calls
    if (i + 5 < notes.rows.length) await sleep(300);

    // Progress
    if ((i + 5) % 50 === 0) {
      console.log(`\n  ─── Progress: ${totalProcessed}/${notes.rows.length} | High: ${totalHigh} | Constraints: ${totalConstraints} | Errors: ${errors} ───\n`);
    }
  }

  console.log('\n────────────────────────────────────────');
  console.log('EXTRACTION COMPLETE');
  console.log(`  Processed: ${totalProcessed}`);
  console.log(`  High quality: ${totalHigh}`);
  console.log(`  Medium quality: ${totalMedium}`);
  console.log(`  Noise filtered: ${totalNoise}`);
  console.log(`  Constraints stored: ${totalConstraints}`);
  console.log(`  Errors: ${errors}`);

  // Embed
  if (!dryRun && notesForEmbedding.length > 0) {
    console.log(`\n  Embedding ${notesForEmbedding.length} notes in Qdrant...`);
    const embeddedCount = await embedNotes(notesForEmbedding);
    console.log(`  ✅ Embedded: ${embeddedCount}`);
  }

  // Summary of constraint types
  if (!dryRun) {
    const constraintStats = await pool.query(`
      SELECT constraint_type, COUNT(*) as ct 
      FROM person_constraints 
      GROUP BY constraint_type 
      ORDER BY ct DESC
    `).catch(() => ({ rows: [] }));

    if (constraintStats.rows.length > 0) {
      console.log('\n  Constraint inventory:');
      constraintStats.rows.forEach(r => {
        console.log(`    ${r.constraint_type}: ${r.ct}`);
      });
    }
  }

  console.log('\n✅ Done');
  await pool.end();
}

main().catch(e => {
  console.error('Fatal:', e);
  pool.end();
  process.exit(1);
});
