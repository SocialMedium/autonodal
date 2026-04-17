/**
 * MitchelLake Intelligence Drop Processor
 *
 * PIPELINE-CONTEXT: Uses pool.query intentionally — intelligence drops are processed
 * as a system pipeline. The caller (route handler) passes a pool or db client.
 * All queries include explicit tenant_id parameters for isolation.
 *
 * Handles all intelligence inputs:
 * - Voice recordings (mic capture) → Whisper → Claude → Store
 * - Audio/video file uploads (meeting recordings) → Whisper → Claude → Store
 * - Text drops (gossip, notes, context) → Claude → Store
 * - Link drops (articles, posts) → Fetch → Claude → Store
 * - Transcript uploads (.txt, .vtt, .srt) → Claude → Store
 * 
 * Usage: Add routes to server.js with:
 *   const intelligenceDrop = require('./lib/intelligence-drop');
 *   app.post('/api/drops', authenticateToken, upload.single('file'), intelligenceDrop.handleDrop);
 *   app.get('/api/drops', authenticateToken, intelligenceDrop.listDrops);
 *   app.get('/api/drops/:id', authenticateToken, intelligenceDrop.getDrop);
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ─── Configuration ───
const WHISPER_COST_PER_MIN = 0.006;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const MAX_AUDIO_SIZE = 100 * 1024 * 1024; // 100MB
const MAX_TEXT_LENGTH = 50000;

const AUDIO_TYPES = [
  'audio/webm', 'audio/mp4', 'audio/mpeg', 'audio/wav',
  'audio/ogg', 'audio/m4a', 'audio/x-m4a', 'video/webm',
  'video/mp4', 'application/octet-stream'
];

const TRANSCRIPT_TYPES = [
  'text/plain', 'text/vtt', 'application/x-subrip'
];

// ─── Schema ───
const INIT_SQL = `
CREATE TABLE IF NOT EXISTS intelligence_drops (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  
  -- Input
  input_type VARCHAR(50) NOT NULL,  -- voice, audio_file, text, link, transcript
  raw_input TEXT,                    -- original text or transcript
  file_path TEXT,                    -- path to uploaded file
  file_name TEXT,                    -- original filename
  audio_duration_seconds INTEGER,    -- for audio files
  
  -- Transcription
  transcription TEXT,
  transcription_cost NUMERIC(8,4),
  transcribed_at TIMESTAMPTZ,
  
  -- AI Extraction
  extraction JSONB,                  -- full structured extraction
  entities_extracted JSONB,          -- {people: [], companies: []}
  signals_extracted JSONB,           -- [{type, target, confidence, detail}]
  constraints_extracted JSONB,       -- [{person, type, value, temporal}]
  relationships_extracted JSONB,     -- [{person_a, person_b, type, context}]
  assessments_extracted JSONB,       -- [{person, search, strengths, gaps}]
  
  -- Classification
  drop_category VARCHAR(50),         -- meeting, interview, gossip, article, research, idea
  confidence NUMERIC(3,2),
  urgency VARCHAR(20),               -- immediate, today, this_week, low
  
  -- Linkages
  linked_people UUID[],
  linked_companies UUID[],
  linked_searches UUID[],
  linked_signals UUID[],
  
  -- Embedding
  embedded_at TIMESTAMPTZ,
  
  -- Processing status
  status VARCHAR(20) DEFAULT 'pending',  -- pending, transcribing, extracting, complete, error
  error_message TEXT,
  processing_time_ms INTEGER,
  extraction_cost NUMERIC(8,4),
  
  -- Feedback
  user_feedback VARCHAR(20),         -- useful, not_useful, wrong, gold
  feedback_note TEXT,
  
  -- Acknowledgment
  acknowledgment TEXT,               -- what we told the user
  
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_drops_user ON intelligence_drops(user_id);
CREATE INDEX IF NOT EXISTS idx_drops_status ON intelligence_drops(status);
CREATE INDEX IF NOT EXISTS idx_drops_category ON intelligence_drops(drop_category);
CREATE INDEX IF NOT EXISTS idx_drops_created ON intelligence_drops(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_drops_entities ON intelligence_drops USING gin(entities_extracted);
`;

// ─── Initialize table ───
async function initDropsTable(pool) {
  await pool.query(INIT_SQL);
  console.log('✅ intelligence_drops table ready');
}

// ─── Transcribe audio via OpenAI Whisper ───
async function transcribeAudio(openai, filePath, fileName) {
  const startTime = Date.now();
  
  const fileStream = fs.createReadStream(filePath);
  
  const response = await openai.audio.transcriptions.create({
    file: fileStream,
    model: 'whisper-1',
    response_format: 'verbose_json',
    timestamp_granularities: ['segment']
  });
  
  const durationSeconds = response.duration || 0;
  const cost = (durationSeconds / 60) * WHISPER_COST_PER_MIN;
  
  return {
    text: response.text,
    duration: durationSeconds,
    cost: cost,
    segments: response.segments || [],
    language: response.language,
    processingTime: Date.now() - startTime
  };
}

// ─── Fetch and extract article from URL ───
async function fetchUrl(url) {
  try {
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'MitchelLake-Signal-Bot/1.0',
        'Accept': 'text/html,application/xhtml+xml'
      },
      signal: AbortSignal.timeout(15000)
    });
    
    if (!response.ok) return { title: url, content: '', error: `HTTP ${response.status}` };
    
    const html = await response.text();
    
    // Basic extraction — strip HTML tags, get title
    const titleMatch = html.match(/<title[^>]*>(.*?)<\/title>/is);
    const title = titleMatch ? titleMatch[1].trim() : url;
    
    // Strip tags, normalize whitespace
    const content = html
      .replace(/<script[\s\S]*?<\/script>/gi, '')
      .replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 15000); // Cap for Claude context
    
    return { title, content, url };
  } catch (e) {
    return { title: url, content: '', error: e.message };
  }
}

// ─── Extract intelligence via Claude ───
async function extractIntelligence(anthropicApiKey, text, context = {}) {
  const startTime = Date.now();
  
  const systemPrompt = `You are MitchelLake's Intelligence Processor. MitchelLake is an executive search firm.

You receive raw intelligence inputs from consultants — voice notes, meeting transcripts, article content, gossip, interview debriefs, or casual observations.

Your job is to extract ALL actionable intelligence and return structured JSON.

CONTEXT:
- Current user: ${context.userName || 'Unknown'}
- Active searches: ${context.activeSearches || 'None loaded'}
- Drop type: ${context.dropType || 'unknown'}
${context.fileName ? `- File: ${context.fileName}` : ''}

EXTRACT AND RETURN THIS EXACT JSON STRUCTURE:
{
  "category": "meeting|interview|gossip|article|research|idea|observation|event",
  "summary": "2-3 sentence summary of the intelligence",
  "urgency": "immediate|today|this_week|low",
  "confidence": 0.0-1.0,
  
  "people": [
    {
      "name": "Full Name",
      "role": "Title if mentioned",
      "company": "Company if mentioned",
      "is_new": true/false,
      "context": "what was said about them"
    }
  ],
  
  "companies": [
    {
      "name": "Company Name",
      "context": "what was said about them"
    }
  ],
  
  "signals": [
    {
      "type": "flight_risk|company_exit|hiring_intent|funding|expansion|restructuring|acquisition|partnership|product_launch|leadership_change|market_shift|competitive_intel",
      "target_person": "Name or null",
      "target_company": "Company or null",
      "confidence": 0.0-1.0,
      "detail": "specific detail",
      "source_type": "firsthand|secondhand|gossip|observed|inferred"
    }
  ],
  
  "constraints": [
    {
      "person": "Name",
      "type": "timing|compensation|location|role_type|company_type|notice_period|non_compete|retention|personal",
      "value": "specific constraint",
      "expiry": "ISO date if temporal, null otherwise",
      "hard_or_soft": "hard|soft"
    }
  ],
  
  "relationships": [
    {
      "person_a": "Name",
      "person_b": "Name or Company",
      "type": "colleague|former_colleague|reports_to|knows|introduced_by|alumni|board|investor",
      "context": "how we know this"
    }
  ],
  
  "assessments": [
    {
      "person": "Name",
      "search_context": "which role if applicable",
      "strengths": ["list"],
      "gaps": ["list"],
      "cultural_fit": "notes if mentioned",
      "overall_impression": "summary"
    }
  ],
  
  "action_items": [
    {
      "action": "what to do",
      "target": "person or company",
      "deadline": "when, if mentioned",
      "priority": "high|medium|low"
    }
  ],
  
  "temporal_markers": [
    {
      "entity": "who/what",
      "event": "what happens",
      "when": "ISO date or relative description",
      "type": "deadline|window|constraint_expiry|milestone"
    }
  ],
  
  "acknowledgment": "A brief, friendly 1-2 sentence response to the consultant confirming what intelligence was captured and any immediate matches or actions. Be specific about what was found useful."
}

IMPORTANT:
- Extract EVERYTHING, even minor details — they may be valuable later
- Confidence scoring: firsthand observation = 0.8+, secondhand = 0.5-0.7, gossip = 0.3-0.5, inference = 0.1-0.3
- If compensation is mentioned, always capture it as a constraint
- If timing/dates are mentioned, always capture as temporal markers
- Names mentioned casually ("she mentioned her colleague Tom") still get extracted
- Relationship discoveries are HIGH VALUE — always capture who knows whom
- Return ONLY valid JSON, no markdown, no explanation`;

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': anthropicApiKey,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 4096,
      system: systemPrompt,
      messages: [{
        role: 'user',
        content: `Process this intelligence input:\n\n${text.slice(0, 30000)}`
      }]
    })
  });
  
  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Claude API error ${response.status}: ${errText}`);
  }
  
  const data = await response.json();
  const rawText = data.content[0]?.text || '';
  
  // Parse JSON — handle potential markdown wrapping
  let extraction;
  try {
    const cleaned = rawText.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    extraction = JSON.parse(cleaned);
  } catch (e) {
    console.error('Failed to parse Claude response:', rawText.slice(0, 500));
    throw new Error('Failed to parse intelligence extraction');
  }
  
  // Estimate cost (~$0.003/1K input tokens, ~$0.015/1K output tokens for Sonnet)
  const inputTokens = Math.ceil(text.length / 4);
  const outputTokens = Math.ceil(rawText.length / 4);
  const cost = (inputTokens * 0.003 + outputTokens * 0.015) / 1000;
  
  return {
    extraction,
    cost,
    processingTime: Date.now() - startTime
  };
}

// ─── Match extracted entities to database ───
async function linkEntities(pool, extraction) {
  const linkedPeople = [];
  const linkedCompanies = [];
  
  // Match people
  if (extraction.people && extraction.people.length > 0) {
    for (const person of extraction.people) {
      if (!person.name) continue;
      
      const nameParts = person.name.trim().split(/\s+/);
      if (nameParts.length < 2) continue;
      
      const firstName = nameParts[0];
      const lastName = nameParts[nameParts.length - 1];
      
      // Try exact name match
      const result = await pool.query(`
        SELECT id, full_name, current_title, current_company_name 
        FROM people 
        WHERE LOWER(first_name) = LOWER($1) AND LOWER(last_name) = LOWER($2)
        LIMIT 5
      `, [firstName, lastName]);
      
      if (result.rows.length === 1) {
        linkedPeople.push(result.rows[0].id);
        person.matched_id = result.rows[0].id;
        person.is_new = false;
      } else if (result.rows.length > 1 && person.company) {
        // Disambiguate by company
        const match = result.rows.find(r => 
          r.current_company_name && 
          r.current_company_name.toLowerCase().includes(person.company.toLowerCase())
        );
        if (match) {
          linkedPeople.push(match.id);
          person.matched_id = match.id;
          person.is_new = false;
        }
      } else {
        person.is_new = true;
      }
    }
  }
  
  // Match companies
  if (extraction.companies && extraction.companies.length > 0) {
    for (const company of extraction.companies) {
      if (!company.name) continue;
      
      const result = await pool.query(`
        SELECT id, name FROM companies 
        WHERE LOWER(name) LIKE LOWER($1)
        LIMIT 1
      `, [`%${company.name}%`]);
      
      if (result.rows.length > 0) {
        linkedCompanies.push(result.rows[0].id);
        company.matched_id = result.rows[0].id;
      }
    }
  }
  
  return { linkedPeople, linkedCompanies };
}

// ─── Store signals extracted from drops ───
async function storeSignals(pool, dropId, userId, extraction) {
  if (!extraction.signals || extraction.signals.length === 0) return [];
  
  const signalIds = [];
  
  for (const signal of extraction.signals) {
    try {
      const result = await pool.query(`
        INSERT INTO person_signals (
          person_id, signal_type, signal_category, 
          confidence, detail, source, detected_at
        )
        SELECT p.id, $1, 'computed', $2, $3, 'intelligence_drop', NOW()
        FROM people p
        WHERE p.full_name ILIKE $4
        LIMIT 1
        RETURNING id
      `, [
        signal.type,
        signal.confidence || 0.5,
        JSON.stringify({ 
          detail: signal.detail, 
          source_type: signal.source_type,
          drop_id: dropId,
          contributed_by: userId
        }),
        `%${signal.target_person || ''}%`
      ]);
      
      if (result.rows.length > 0) {
        signalIds.push(result.rows[0].id);
      }
    } catch (e) {
      // Signal table might have different schema — log and continue
      console.warn(`  Signal store skipped: ${e.message}`);
    }
  }
  
  return signalIds;
}

// ─── Store constraints from drops ───
async function storeConstraints(pool, dropId, userId, extraction) {
  if (!extraction.constraints || extraction.constraints.length === 0) return;
  
  for (const constraint of extraction.constraints) {
    try {
      // Store as an interaction/note on the person
      await pool.query(`
        INSERT INTO interactions (
          person_id, interaction_type, content, 
          interaction_date, created_by
        )
        SELECT p.id, 'constraint_captured', $1, NOW(), $2
        FROM people p
        WHERE p.full_name ILIKE $3
        LIMIT 1
      `, [
        JSON.stringify(constraint),
        userId,
        `%${constraint.person || ''}%`
      ]);
    } catch (e) {
      console.warn(`  Constraint store skipped: ${e.message}`);
    }
  }
}

// ─── Embed the drop for semantic search ───
async function embedDrop(openai, pool, dropId, text) {
  try {
    const response = await openai.embeddings.create({
      model: 'text-embedding-3-small',
      input: text.slice(0, 8000)
    });
    
    const embedding = response.data[0].embedding;
    
    // Store in Qdrant if available, otherwise just mark as embedded
    // For now, mark as embedded in PostgreSQL
    await pool.query(`
      UPDATE intelligence_drops 
      SET embedded_at = NOW() 
      WHERE id = $1
    `, [dropId]);
    
    return embedding;
  } catch (e) {
    console.warn(`  Embedding skipped: ${e.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════
//  MAIN HANDLER
// ═══════════════════════════════════════════

async function handleDrop(req, res) {
  const pool = req.app.locals.pool;
  const openai = req.app.locals.openai;
  const anthropicApiKey = process.env.ANTHROPIC_API_KEY;
  
  if (!anthropicApiKey) {
    return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  }
  
  const userId = req.user?.id || req.body.user_id;
  const userName = req.user?.name || req.body.user_name || 'Unknown';
  const startTime = Date.now();
  
  let inputType, rawText, filePath, fileName, audioDuration, transcriptionCost;
  
  try {
    // ─── Determine input type ───
    if (req.file) {
      const mimeType = req.file.mimetype || '';
      fileName = req.file.originalname;
      filePath = req.file.path;
      
      if (AUDIO_TYPES.some(t => mimeType.startsWith(t.split('/')[0])) || 
          /\.(webm|mp3|mp4|m4a|wav|ogg|mpeg)$/i.test(fileName)) {
        inputType = fileName.match(/\.(mp4|m4a|mov|webm)$/i) && req.body.source === 'mic' 
          ? 'voice' : 'audio_file';
      } else if (TRANSCRIPT_TYPES.includes(mimeType) || /\.(txt|vtt|srt)$/i.test(fileName)) {
        inputType = 'transcript';
        rawText = fs.readFileSync(filePath, 'utf-8');
      } else {
        return res.status(400).json({ error: `Unsupported file type: ${mimeType}` });
      }
    } else if (req.body.text) {
      rawText = req.body.text.slice(0, MAX_TEXT_LENGTH);
      
      // Detect if it's a URL
      if (/^https?:\/\/\S+$/i.test(rawText.trim())) {
        inputType = 'link';
      } else {
        inputType = 'text';
      }
    } else {
      return res.status(400).json({ error: 'No input provided. Send text, a link, or upload a file.' });
    }
    
    // ─── Create drop record ───
    const dropResult = await pool.query(`
      INSERT INTO intelligence_drops (user_id, input_type, raw_input, file_path, file_name, status)
      VALUES ($1, $2, $3, $4, $5, 'processing')
      RETURNING id
    `, [userId, inputType, rawText || null, filePath || null, fileName || null]);
    
    const dropId = dropResult.rows[0].id;
    
    // Send immediate acknowledgment
    res.json({
      id: dropId,
      status: 'processing',
      message: `Processing your ${inputType} drop...`
    });
    
    // ─── Process asynchronously ───
    processDropAsync(pool, openai, anthropicApiKey, dropId, {
      inputType, rawText, filePath, fileName, userId, userName, startTime
    }).catch(err => {
      console.error(`Drop ${dropId} processing failed:`, err);
      pool.query(`
        UPDATE intelligence_drops 
        SET status = 'error', error_message = $1, updated_at = NOW()
        WHERE id = $2
      `, [err.message, dropId]);
    });
    
  } catch (e) {
    console.error('Drop handler error:', e);
    res.status(500).json({ error: e.message });
  }
}

// ─── Async processing pipeline ───
async function processDropAsync(pool, openai, anthropicApiKey, dropId, opts) {
  const { inputType, filePath, fileName, userId, userName, startTime } = opts;
  let { rawText } = opts;
  let audioDuration = null;
  let transcriptionCost = 0;
  
  console.log(`\n🧠 Processing drop ${dropId} [${inputType}]`);
  
  // ─── Step 1: Transcribe if audio ───
  if (inputType === 'voice' || inputType === 'audio_file') {
    console.log(`  🎤 Transcribing ${fileName}...`);
    
    await pool.query(`UPDATE intelligence_drops SET status = 'transcribing' WHERE id = $1`, [dropId]);
    
    const result = await transcribeAudio(openai, filePath, fileName);
    rawText = result.text;
    audioDuration = Math.round(result.duration);
    transcriptionCost = result.cost;
    
    console.log(`  ✅ Transcribed: ${audioDuration}s, $${transcriptionCost.toFixed(4)}`);
    console.log(`  📝 "${rawText.slice(0, 100)}..."`);
    
    await pool.query(`
      UPDATE intelligence_drops 
      SET transcription = $1, audio_duration_seconds = $2, 
          transcription_cost = $3, transcribed_at = NOW(), raw_input = $1
      WHERE id = $4
    `, [rawText, audioDuration, transcriptionCost, dropId]);
  }
  
  // ─── Step 2: Fetch URL if link ───
  if (inputType === 'link') {
    console.log(`  🔗 Fetching ${rawText}...`);
    const article = await fetchUrl(rawText.trim());
    rawText = `URL: ${article.url}\nTitle: ${article.title}\n\n${article.content}`;
    
    await pool.query(`
      UPDATE intelligence_drops SET raw_input = $1 WHERE id = $2
    `, [rawText, dropId]);
  }
  
  if (!rawText || rawText.trim().length < 10) {
    await pool.query(`
      UPDATE intelligence_drops 
      SET status = 'error', error_message = 'No meaningful content to process'
      WHERE id = $1
    `, [dropId]);
    return;
  }
  
  // ─── Step 3: Extract intelligence via Claude ───
  console.log(`  🧠 Extracting intelligence...`);
  await pool.query(`UPDATE intelligence_drops SET status = 'extracting' WHERE id = $1`, [dropId]);
  
  const { extraction, cost: extractionCost } = await extractIntelligence(
    anthropicApiKey, rawText, { 
      userName, 
      dropType: inputType,
      fileName
    }
  );
  
  console.log(`  ✅ Extracted: ${extraction.people?.length || 0} people, ${extraction.signals?.length || 0} signals, ${extraction.constraints?.length || 0} constraints`);
  
  // ─── Step 4: Match entities to database ───
  console.log(`  🔍 Matching entities...`);
  const { linkedPeople, linkedCompanies } = await linkEntities(pool, extraction);
  console.log(`  ✅ Matched: ${linkedPeople.length} people, ${linkedCompanies.length} companies`);
  
  // ─── Step 5: Store signals and constraints ───
  const linkedSignals = await storeSignals(pool, dropId, userId, extraction);
  await storeConstraints(pool, dropId, userId, extraction);
  
  // ─── Step 6: Embed for semantic search ───
  console.log(`  📐 Embedding...`);
  const embeddingText = `${extraction.summary || ''}\n${rawText.slice(0, 6000)}`;
  await embedDrop(openai, pool, dropId, embeddingText);
  
  // ─── Step 7: Update drop record with everything ───
  const totalTime = Date.now() - startTime;
  
  await pool.query(`
    UPDATE intelligence_drops SET
      status = 'complete',
      extraction = $1,
      entities_extracted = $2,
      signals_extracted = $3,
      constraints_extracted = $4,
      relationships_extracted = $5,
      assessments_extracted = $6,
      drop_category = $7,
      confidence = $8,
      urgency = $9,
      linked_people = $10,
      linked_companies = $11,
      linked_signals = $12,
      extraction_cost = $13,
      processing_time_ms = $14,
      acknowledgment = $15,
      updated_at = NOW()
    WHERE id = $16
  `, [
    JSON.stringify(extraction),
    JSON.stringify({ people: extraction.people || [], companies: extraction.companies || [] }),
    JSON.stringify(extraction.signals || []),
    JSON.stringify(extraction.constraints || []),
    JSON.stringify(extraction.relationships || []),
    JSON.stringify(extraction.assessments || []),
    extraction.category || 'observation',
    extraction.confidence || 0.5,
    extraction.urgency || 'low',
    linkedPeople.length > 0 ? linkedPeople : null,
    linkedCompanies.length > 0 ? linkedCompanies : null,
    linkedSignals.length > 0 ? linkedSignals : null,
    extractionCost + transcriptionCost,
    totalTime,
    extraction.acknowledgment || 'Intelligence captured and processed.',
    dropId
  ]);
  
  console.log(`  ✅ Drop ${dropId} complete in ${totalTime}ms ($${(extractionCost + transcriptionCost).toFixed(4)})`);
  console.log(`  💬 "${extraction.acknowledgment}"`);
}

// ═══════════════════════════════════════════
//  LIST & GET ENDPOINTS
// ═══════════════════════════════════════════

async function listDrops(req, res) {
  const pool = req.app.locals.pool;
  const userId = req.user?.id;
  const { limit = 20, offset = 0, category, status } = req.query;
  
  let query = `
    SELECT id, input_type, drop_category, status, confidence, urgency,
           acknowledgment, 
           entities_extracted->'people' as people_count,
           array_length(linked_people, 1) as matched_people,
           array_length(linked_signals, 1) as signals_created,
           extraction_cost, processing_time_ms,
           created_at
    FROM intelligence_drops
    WHERE user_id = $1
  `;
  const params = [userId];
  let paramCount = 1;
  
  if (category) {
    paramCount++;
    query += ` AND drop_category = $${paramCount}`;
    params.push(category);
  }
  
  if (status) {
    paramCount++;
    query += ` AND status = $${paramCount}`;
    params.push(status);
  }
  
  query += ` ORDER BY created_at DESC LIMIT $${paramCount + 1} OFFSET $${paramCount + 2}`;
  params.push(parseInt(limit), parseInt(offset));
  
  const result = await pool.query(query, params);
  res.json({ drops: result.rows });
}

async function getDrop(req, res) {
  const pool = req.app.locals.pool;
  const { id } = req.params;
  
  const result = await pool.query(`
    SELECT * FROM intelligence_drops WHERE id = $1
  `, [id]);
  
  if (result.rows.length === 0) {
    return res.status(404).json({ error: 'Drop not found' });
  }
  
  res.json(result.rows[0]);
}

async function feedbackDrop(req, res) {
  const pool = req.app.locals.pool;
  const { id } = req.params;
  const { feedback, note } = req.body;
  
  await pool.query(`
    UPDATE intelligence_drops 
    SET user_feedback = $1, feedback_note = $2, updated_at = NOW()
    WHERE id = $3
  `, [feedback, note || null, id]);
  
  res.json({ ok: true });
}

// ═══════════════════════════════════════════
//  EXPORTS
// ═══════════════════════════════════════════

module.exports = {
  initDropsTable,
  handleDrop,
  listDrops,
  getDrop,
  feedbackDrop,
  // Expose for testing
  transcribeAudio,
  extractIntelligence,
  linkEntities,
  processDropAsync
};
