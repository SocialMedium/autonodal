#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/enrich_from_document.js — Match-Only Enrichment Pipeline
// Matches rows from enrichment documents to existing platform people,
// enriches fields only where empty, requires interaction history.
// NO NEW PEOPLE CREATED — match-only.
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   DRY_RUN=true  node scripts/enrich_from_document.js <file> [sheet]
//   DRY_RUN=false node scripts/enrich_from_document.js <file> [sheet]
//
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const path = require('path');
const fs   = require('fs');
const XLSX = require('xlsx');
const db   = require('../lib/db');
const { auditColumns } = require('../lib/enrichment_auditor');
const { ML_TENANT_ID } = require('../lib/tenant');

const TENANT_ID = process.env.TENANT_ID_OVERRIDE || process.env.ENRICH_TENANT_ID || ML_TENANT_ID;
const DRY_RUN   = process.env.DRY_RUN !== 'false'; // Default: safe dry run

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

// Parse LinkedIn URL from plain string or JSON array format ["url"]
function extractLinkedIn(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  if (s.startsWith('[')) {
    try {
      const p = JSON.parse(s);
      if (Array.isArray(p) && p[0]) return String(p[0]).replace(/\/$/, '');
    } catch {}
  }
  const m = s.match(/https?:\/\/[^\s"',\]]+linkedin\.com\/in\/[^\s"',\]]+/);
  return m ? m[0].replace(/\/$/, '') : null;
}

// Match person — LinkedIn URL > Email > Full Name (exact only)
async function matchPerson(name, linkedinUrl, email) {
  if (linkedinUrl) {
    const slug = linkedinUrl.split('/in/')[1]?.split(/[/?#]/)[0];
    if (slug) {
      const { rows } = await db.query(
        `SELECT id, full_name AS name, linkedin_url FROM people
         WHERE tenant_id = $1 AND (
           linkedin_url = $2
           OR linkedin_url ILIKE $3
           OR linkedin_url ILIKE $4
         ) LIMIT 1`,
        [TENANT_ID, linkedinUrl, `%/in/${slug}`, `%/in/${slug}/%`]
      );
      if (rows[0]) return { ...rows[0], method: 'linkedin_url', confidence: 0.98 };
    }
  }
  if (email) {
    const { rows } = await db.query(
      `SELECT id, full_name AS name, linkedin_url FROM people
       WHERE tenant_id = $1 AND LOWER(email) = LOWER($2) LIMIT 1`,
      [TENANT_ID, email.trim()]
    );
    if (rows[0]) return { ...rows[0], method: 'email', confidence: 0.95 };
  }
  if (name) {
    const { rows } = await db.query(
      `SELECT id, full_name AS name, linkedin_url FROM people
       WHERE tenant_id = $1 AND LOWER(full_name) = LOWER($2) LIMIT 2`,
      [TENANT_ID, name.trim()]
    );
    if (rows.length === 1) return { ...rows[0], method: 'full_name', confidence: 0.75 };
    if (rows.length > 1)  return { ...rows[0], method: 'full_name_ambiguous', confidence: 0.40 };
  }
  return null;
}

// Check for real interaction history (not just system notes)
async function hasInteraction(personId) {
  const { rows } = await db.query(
    `SELECT 1 FROM interactions
     WHERE tenant_id = $1 AND person_id = $2
       AND interaction_type IN (
         'email','email_sent','email_received','meeting',
         'linkedin_message','note'
       )
     LIMIT 1`,
    [TENANT_ID, personId]
  );
  return rows.length > 0;
}

// Get helper for row by column header
function makeGetter(headers) {
  const indexMap = new Map();
  headers.forEach((h, i) => { if (h) indexMap.set(h.toLowerCase().trim(), i); });
  return (row, col) => {
    const i = indexMap.get(col.toLowerCase().trim());
    return i !== undefined ? row[i] : null;
  };
}

// Build scoring criteria JSON from row
function buildCriteria(row, get) {
  const CRITERIA = [
    'Stage & Check-Size Fit',
    'Domain / Investment Thesis Fit',
    'Geography & Market Fit',
    'Value-Add Potential',
    'Mission, Motivation & Timing Alignment',
    'Constraints & Exclusions (Hard Gate)',
    'Professional Expertise Alignment',
    'Mission & Purpose Alignment',
    'Geographic Practicality',
    'Communication & Cultural Fit',
    'Professional Standing & Credibility',
    'Motivational Compatibility',
  ];

  const criteria = {};
  for (const c of CRITERIA) {
    const score     = parseFloat(get(row, `${c}_score`));
    const rationale = get(row, `${c}_rationale`);
    if (!isNaN(score) || rationale) {
      criteria[c] = { score: isNaN(score) ? null : score, rationale: rationale || null };
    }
  }
  return Object.keys(criteria).length > 0 ? criteria : null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function run(filePath, sheetName) {
  console.log(`\n${'='.repeat(60)}`);
  console.log('  INVESTOR ENRICHMENT — MATCH ONLY');
  console.log(`  File: ${path.basename(filePath)}`);
  console.log(`  Mode: ${DRY_RUN ? 'DRY RUN (no writes)' : 'LIVE'}`);
  console.log('='.repeat(60));

  // Load file — detect format by extension
  const ext = path.extname(filePath).toLowerCase();
  let headers, dataRows, headerIdx = 0;

  if (ext === '.csv') {
    const Papa    = require('papaparse');
    const content = fs.readFileSync(filePath, 'utf8');
    const result  = Papa.parse(content, { header: false, skipEmptyLines: true });
    headers  = result.data[0].map(h => h ? String(h).trim() : null);
    dataRows = result.data.slice(1);
    // headerIdx stays 0 — not used for CSV, only for logging

  } else {
    // XLSX / XLS
    const wb    = XLSX.readFile(filePath);
    const sheet = wb.Sheets[sheetName || wb.SheetNames[0]];
    const raw   = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null });

    let maxCols = 0;
    for (let i = 0; i < Math.min(5, raw.length); i++) {
      const n = raw[i].filter(v => v !== null && v !== '').length;
      if (n > maxCols) { maxCols = n; headerIdx = i; }
    }
    headers  = raw[headerIdx].map(h => h ? String(h) : null);
    dataRows = raw.slice(headerIdx + 1).filter(r =>
      r.some(v => v !== null && v !== '')
    );
  }

  console.log(`\nHeader row: ${headerIdx} | Columns: ${headers.filter(Boolean).length} | Data rows: ${dataRows.length}`);

  // Column audit
  const audit = auditColumns(headers.filter(Boolean));
  console.log(`\nMapped:   ${audit.mapped.length} columns`);
  console.log(`Unmapped: ${audit.unmapped.length} columns`);
  console.log(`Criteria: ${audit.scoring_criteria.length} scoring columns`);
  audit.warnings.forEach(w => console.log(`  ${w}`));

  if (!audit.has_name && !audit.has_linkedin) {
    throw new Error('ABORT: No identifier column found');
  }

  const get = makeGetter(headers);

  const stats = {
    total: 0, matched: 0, enriched: 0,
    no_match: 0, no_linkedin: 0,
    by_method: {},
  };

  // Record people count before (safety)
  const { rows: [before] } = await db.query(
    'SELECT COUNT(*) AS cnt FROM people WHERE tenant_id = $1', [TENANT_ID]
  );
  console.log(`\nPeople count before: ${before.cnt}`);

  // Register enrichment document
  let docId = null;
  if (!DRY_RUN) {
    const { rows: [doc] } = await db.query(
      `INSERT INTO enrichment_documents (tenant_id, filename, document_type, row_count, status)
       VALUES ($1, $2, 'investor_list', $3, 'processing') RETURNING id`,
      [TENANT_ID, path.basename(filePath), dataRows.length]
    );
    docId = doc.id;
  }

  // Process all rows — match on LinkedIn URL, write investor notes as interactions
  for (let i = 0; i < dataRows.length; i++) {
    const row = dataRows[i];
    stats.total++;

    const name     = get(row, 'Full Name');
    const liRaw    = get(row, 'Linkedin');
    const liUrl    = extractLinkedIn(liRaw);
    const score    = parseFloat(get(row, 'overall_score')) || parseFloat(get(row, 'overall_ai_score')) || null;
    const criteria = buildCriteria(row, get);

    // LinkedIn is the only match key
    if (!liUrl) { stats.no_linkedin++; continue; }

    // Match on LinkedIn URL
    const match = await matchPerson(null, liUrl, null);
    if (!match) {
      stats.no_match++;
      continue;
    }
    stats.matched++;
    stats.by_method[match.method] = (stats.by_method[match.method] || 0) + 1;

    // Flag as investor on the person record
    if (!DRY_RUN) {
      await db.query(
        `UPDATE people SET is_investor = true, investor_fit_score = COALESCE($2, investor_fit_score),
         enrichment_source = COALESCE(enrichment_source, 'socialmedium_ai'), updated_at = NOW()
         WHERE id = $1`,
        [match.id, score]
      );
    }

    // Build the note content — investor fit intelligence
    const jotNote    = get(row, 'JT Note');
    const network    = get(row, 'network');
    const rationale  = get(row, 'overall_rationale');
    const title      = get(row, 'user2_last_job_title');
    const company    = get(row, 'user2_last_company');
    const m1 = get(row, '1st Approach'), d1 = get(row, 'Date ') || get(row, 'Date');
    const m2 = get(row, '2nd ') || get(row, '2nd'), d2 = get(row, 'Date .1');
    const fb = get(row, 'Feedback');

    const noteParts = [
      `Investor Fit Assessment — SocialMedium AI Matching`,
      ``,
      score ? `Overall Score: ${score}/5.0` : null,
      jotNote ? `Classification: ${jotNote}` : null,
      network ? `Network Source: ${network}` : null,
      rationale ? `\nRationale: ${rationale}` : null,
      title || company ? `\nProfile: ${[title, company].filter(Boolean).join(' at ')}` : null,
    ].filter(v => v !== null);

    // Add criteria scores
    if (criteria && Object.keys(criteria).length > 0) {
      noteParts.push('', 'Scoring Criteria:');
      for (const [name, val] of Object.entries(criteria)) {
        const s = val.score !== null ? `${val.score}/5` : '—';
        noteParts.push(`  ${name}: ${s}${val.rationale ? ' — ' + val.rationale.substring(0, 120) : ''}`);
      }
    }

    // Add approach history
    if (m1 || d1 || m2 || d2) {
      noteParts.push('', 'Approach History:');
      if (m1 || d1) noteParts.push(`  1st: ${m1 || '—'}${d1 ? ' (' + d1 + ')' : ''}`);
      if (m2 || d2) noteParts.push(`  2nd: ${m2 || '—'}${d2 ? ' (' + d2 + ')' : ''}`);
      if (fb) noteParts.push(`  Feedback: ${fb}`);
    }

    const noteText = noteParts.join('\n');
    const subject = `Investor Fit: ${score ? score + '/5.0' : 'assessed'}${jotNote ? ' — ' + jotNote : ''}`;

    if (!DRY_RUN) {
      // Insert as a research_note interaction on the person
      await db.query(
        `INSERT INTO interactions (
           tenant_id, person_id, interaction_type, subject, summary,
           source, interaction_at, created_at
         ) VALUES ($1, $2, 'research_note', $3, $4, 'socialmedium_ai', NOW(), NOW())`,
        [TENANT_ID, match.id, subject, noteText]
      );

      // Audit log
      await db.query(
        `INSERT INTO enrichment_log
           (document_id, person_id, tenant_id, source_row, source_name, source_linkedin,
            match_method, match_confidence, had_interaction, action, fields_updated, new_values)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,'enriched',$9,$10)`,
        [docId, match.id, TENANT_ID, i, name, liUrl, match.method, match.confidence,
         ['is_investor', 'interaction_note'], JSON.stringify({ score, classification: jotNote })]
      );
    }

    stats.enriched++;
    if (stats.enriched <= 20 || stats.enriched % 50 === 0) {
      console.log(`  ✓ ${match.name} | score: ${score || '-'} | ${jotNote || ''}`);
    }
  }

  // Verify no new people created
  const { rows: [after] } = await db.query(
    'SELECT COUNT(*) AS cnt FROM people WHERE tenant_id = $1', [TENANT_ID]
  );
  console.log(`\nPeople count after: ${after.cnt}`);
  if (parseInt(after.cnt) !== parseInt(before.cnt)) {
    console.log(`  ⚠  SAFETY: People count changed by ${parseInt(after.cnt) - parseInt(before.cnt)}!`);
  } else {
    console.log(`  ✓  SAFETY: Zero new people created`);
  }

  // Update enrichment document record
  if (!DRY_RUN && docId) {
    await db.query(`
      UPDATE enrichment_documents SET
        matched_count = $2, enriched_count = $3, skipped_count = $4,
        column_audit = $5, status = 'complete', processed_at = NOW()
      WHERE id = $1
    `, [docId, stats.matched, stats.enriched, stats.no_match + stats.no_linkedin,
        JSON.stringify(audit)]);
  }

  // Summary
  console.log(`\n${'='.repeat(60)}`);
  console.log('  SUMMARY');
  console.log('='.repeat(60));
  Object.entries(stats).forEach(([k, v]) => {
    if (typeof v === 'object') console.log(`  ${k.padEnd(20)}: ${JSON.stringify(v)}`);
    else console.log(`  ${k.padEnd(20)}: ${v}`);
  });
  if (DRY_RUN) console.log('\n  ⚠  DRY RUN — set DRY_RUN=false to apply changes');

  return stats;
}

if (require.main === module) {
  const [,, file, sheet] = process.argv;
  if (!file) { console.error('Usage: node scripts/enrich_from_document.js <file> [sheet]'); process.exit(1); }
  run(path.resolve(file), sheet)
    .then(() => process.exit(0))
    .catch(e => { console.error('Fatal:', e.message); process.exit(1); });
}

module.exports = { run };
