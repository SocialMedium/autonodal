#!/usr/bin/env node
/**
 * Document Classification + Case Study Extraction
 *
 * Runs on external_documents that have content but no classification.
 * Uses Claude to identify document type, extract structured data, and
 * populate the case_studies and document_people tables.
 *
 * This is companion intelligence — it links TO signals, never creates them.
 *
 * Usage:
 *   node scripts/classify_documents.js
 *   node scripts/classify_documents.js --rerun   # Re-classify all
 *   node scripts/classify_documents.js --limit 5  # Process N docs
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const CLASSIFICATION_VERSION = 'v1';
const RERUN = process.argv.includes('--rerun');
const LIMIT = (() => { const i = process.argv.indexOf('--limit'); return i !== -1 ? parseInt(process.argv[i + 1]) || 10 : 10; })();

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function callClaude(system, user) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 4096,
      system,
      messages: [{ role: 'user', content: user }],
    }),
  });
  if (!res.ok) throw new Error(`Claude API ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.content.find(c => c.type === 'text')?.text || '';
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLASSIFICATION
// ═══════════════════════════════════════════════════════════════════════════════

const SYSTEM_PROMPT = `You are a document classifier for MitchelLake, a retained executive search firm with 25 years of experience across APAC, UK, and US in growth and innovation ventures.

You are analysing internal documents from the firm's Google Drive. These include case studies, pitch decks, credential packs, proposals, research notes, meeting notes, and other operational documents.

Your job is to classify the document and extract structured data. Return ONLY valid JSON.

Document types:
- case_study: Describes a completed engagement — client, role, challenge, outcome
- pitch_deck: Presentation for a prospective client — includes shortlisted candidates, firm capabilities, proposed approach
- credentials: Firm credentials pack — past work, sectors, capabilities, testimonials
- proposal: Formal proposal for a specific engagement — scope, fees, timeline
- research_note: Market research, sector analysis, talent mapping
- meeting_notes: Notes from a client or candidate meeting
- candidate_profile: Individual candidate assessment or profile
- industry_analysis: Sector or market report
- internal_ops: Internal operational document (not client-facing)
- other: Does not fit above categories

For case studies, extract all structured fields. For pitch decks, extract shortlisted candidates. For all types, extract people and companies mentioned.`;

async function classifyDocument(doc) {
  const contentPreview = (doc.content || '').substring(0, 12000);
  if (contentPreview.length < 50) return null;

  const userPrompt = `Classify this document and extract structured data.

DOCUMENT TITLE: ${doc.title || 'Untitled'}
SOURCE TYPE: ${doc.source_type || 'unknown'}
CONTENT:
${contentPreview}

Return JSON with this structure:
{
  "document_type": "case_study|pitch_deck|credentials|proposal|research_note|meeting_notes|candidate_profile|industry_analysis|internal_ops|other",
  "confidence": 0.0-1.0,
  "content_summary": "2-3 sentence summary of what this document contains",
  "relevance_tags": ["bd", "delivery", "thought_leadership", "market_intel", "client_material"],

  "people_mentioned": [
    {
      "name": "Full Name",
      "title": "Their title if mentioned",
      "company": "Their company if mentioned",
      "role_in_document": "shortlisted|placed|referenced|authored|interviewed|target|mentioned",
      "context": "Brief note on why they appear"
    }
  ],

  "companies_mentioned": [
    { "name": "Company Name", "context": "client|target|competitor|employer|referenced" }
  ],

  "case_study": null or {
    "client_name": "Client company name",
    "engagement_type": "executive_search|board_advisory|leadership_assessment|team_build|succession|market_mapping",
    "role_title": "The role that was searched for",
    "seniority_level": "c_suite|vp|director|head|senior",
    "sector": "Industry sector",
    "geography": "Region or country",
    "year": null or YYYY,
    "challenge": "What the client needed (1-2 sentences)",
    "approach": "How MitchelLake approached it (1-2 sentences)",
    "outcome": "Result achieved (1-2 sentences)",
    "impact_note": "Broader impact or follow-on (1 sentence, null if not stated)",
    "themes": ["relevant themes — e.g., cross-border, founder-transition, high-growth"],
    "change_vectors": ["directional shifts this case reflects"],
    "capabilities": ["firm capabilities demonstrated — e.g., cross-border, post-acquisition, turnaround"]
  },

  "themes": ["thematic tags for linking to dispatches/signals"],
  "sectors": ["sector tags"],
  "geographies": ["geography tags"]
}

If the document is clearly not a case study, set case_study to null.
If you cannot determine people or companies, return empty arrays.
Extract what the document actually contains — do not invent data.`;

  const raw = await callClaude(SYSTEM_PROMPT, userPrompt);
  try {
    return JSON.parse(raw.replace(/```json\n?|\n?```/g, '').trim());
  } catch (e) {
    console.error(`  JSON parse failed for doc ${doc.id}:`, e.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PERSISTENCE
// ═══════════════════════════════════════════════════════════════════════════════

async function saveClassification(docId, tenantId, classification) {
  // Update external_documents
  await pool.query(`
    UPDATE external_documents
    SET document_type = $1, content_summary = $2, relevance_tags = $3,
        classified_at = NOW(), classification_version = $4
    WHERE id = $5
  `, [
    classification.document_type,
    classification.content_summary,
    classification.relevance_tags || [],
    CLASSIFICATION_VERSION,
    docId,
  ]);

  // Insert people mentioned → document_people
  for (const person of (classification.people_mentioned || [])) {
    if (!person.name || person.name.length < 2) continue;

    // Try to match to existing person
    let personId = null;
    try {
      const { rows } = await pool.query(
        `SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
        [person.name.trim(), tenantId]
      );
      if (rows.length) personId = rows[0].id;
    } catch (e) {}

    try {
      await pool.query(`
        INSERT INTO document_people (document_id, person_id, person_name, person_title, person_company, mention_role, context_note)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (document_id, person_name, mention_role) DO UPDATE
        SET person_id = COALESCE(EXCLUDED.person_id, document_people.person_id),
            person_title = COALESCE(EXCLUDED.person_title, document_people.person_title),
            person_company = COALESCE(EXCLUDED.person_company, document_people.person_company),
            context_note = EXCLUDED.context_note
      `, [docId, personId, person.name.trim(), person.title || null, person.company || null,
          person.role_in_document || 'mentioned', person.context || null]);
    } catch (e) { /* dupe or constraint */ }
  }

  // If it's a case study, extract to case_studies table
  if (classification.document_type === 'case_study' && classification.case_study) {
    const cs = classification.case_study;

    // Try to match client to existing company
    let clientId = null;
    if (cs.client_name) {
      try {
        const { rows } = await pool.query(
          `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
          [`%${cs.client_name}%`, tenantId]
        );
        if (rows.length) clientId = rows[0].id;
      } catch (e) {}
    }

    // Compute completeness score
    const fields = [cs.client_name, cs.engagement_type, cs.role_title, cs.sector,
                    cs.geography, cs.challenge, cs.approach, cs.outcome];
    const completeness = fields.filter(Boolean).length / fields.length;

    try {
      await pool.query(`
        INSERT INTO case_studies (
          tenant_id, document_id, title, client_name, client_id, engagement_type,
          role_title, seniority_level, sector, geography, year,
          challenge, approach, outcome, impact_note,
          themes, change_vectors, capabilities,
          completeness, extracted_by, status
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'system','draft')
        ON CONFLICT DO NOTHING
      `, [
        tenantId, docId,
        cs.client_name ? `${cs.role_title || 'Engagement'} — ${cs.client_name}` : (classification.content_summary || 'Case Study').substring(0, 200),
        cs.client_name || null, clientId, cs.engagement_type || null,
        cs.role_title || null, cs.seniority_level || null,
        cs.sector || null, cs.geography || null, cs.year || null,
        cs.challenge || null, cs.approach || null, cs.outcome || null, cs.impact_note || null,
        cs.themes || [], cs.change_vectors || [], cs.capabilities || [],
        completeness
      ]);
    } catch (e) {
      console.error(`  Case study insert failed for doc ${docId}:`, e.message);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Document Classification + Case Study Extraction');
  console.log('═══════════════════════════════════════════════════\n');

  if (!ANTHROPIC_API_KEY) { console.error('ANTHROPIC_API_KEY not set'); process.exit(1); }

  // Ensure tables exist
  try {
    const fs = require('fs');
    const migrationPath = require('path').join(__dirname, '..', 'sql', 'migration_case_studies.sql');
    if (fs.existsSync(migrationPath)) {
      await pool.query(fs.readFileSync(migrationPath, 'utf8'));
    }
  } catch (e) { /* tables may already exist */ }

  // Find unclassified documents with content
  const whereClause = RERUN
    ? `WHERE ed.content IS NOT NULL AND LENGTH(ed.content) > 100 AND ed.source_name = 'Google Drive'`
    : `WHERE ed.content IS NOT NULL AND LENGTH(ed.content) > 100 AND ed.source_name = 'Google Drive' AND ed.classified_at IS NULL`;

  const { rows: docs } = await pool.query(`
    SELECT ed.id, ed.title, ed.content, ed.source_type, ed.tenant_id
    FROM external_documents ed
    ${whereClause}
    ORDER BY ed.created_at DESC
    LIMIT $1
  `, [LIMIT]);

  console.log(`  Found ${docs.length} documents to classify\n`);
  if (docs.length === 0) { await pool.end(); return; }

  let classified = 0, caseStudies = 0, peopleExtracted = 0;

  for (const doc of docs) {
    try {
      console.log(`  Classifying: ${(doc.title || 'Untitled').substring(0, 60)}...`);

      const result = await classifyDocument(doc);
      if (!result) { console.log('    → skipped (no result)'); continue; }

      await saveClassification(doc.id, doc.tenant_id, result);
      classified++;

      console.log(`    → ${result.document_type} (${(result.confidence * 100).toFixed(0)}% confidence)`);
      if (result.people_mentioned?.length) {
        peopleExtracted += result.people_mentioned.length;
        console.log(`    → ${result.people_mentioned.length} people extracted`);
      }
      if (result.document_type === 'case_study' && result.case_study) {
        caseStudies++;
        console.log(`    → Case study: ${result.case_study.client_name || 'unknown client'} / ${result.case_study.sector || 'unknown sector'}`);
      }

      await sleep(2000); // Rate limit
    } catch (e) {
      console.error(`    → Error: ${e.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`  Classified: ${classified} documents`);
  console.log(`  Case studies extracted: ${caseStudies}`);
  console.log(`  People extracted: ${peopleExtracted}`);
  console.log('═══════════════════════════════════════════════════');

  await pool.end();
}

module.exports = { classifyDocument, saveClassification };

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
