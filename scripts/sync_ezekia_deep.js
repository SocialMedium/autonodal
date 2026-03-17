#!/usr/bin/env node
/**
 * Deep Ezekia Sync — Candidates + Pipeline Tags + Notes
 *
 * For each project in our opportunities table that came from Ezekia:
 * 1. Get all candidates with pipeline tags (meta.candidate)
 * 2. Match candidates to our people table (by name/email/ezekia ID)
 * 3. Store as pipeline_contacts with pipeline status
 * 4. Get project notes and store as interactions
 * 5. Get company contacts via Ezekia company→people endpoint
 *
 * Usage:
 *   node scripts/sync_ezekia_deep.js              # All projects
 *   node scripts/sync_ezekia_deep.js --project=ID  # Single project by Ezekia ID
 *   node scripts/sync_ezekia_deep.js --company=NAME # All projects for a company
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');
const { ezekiaFetch } = require('../lib/ezekia');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const ML_TENANT = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);

// ═══════════════════════════════════════════════════════════════════════════════
// USER RESOLUTION — match Ezekia owner names/emails to our users
// ═══════════════════════════════════════════════════════════════════════════════

let userCache = null;
async function loadUsers() {
  if (userCache) return userCache;
  const { rows } = await pool.query('SELECT id, name, email FROM users WHERE tenant_id = $1', [ML_TENANT]);
  userCache = rows;
  return rows;
}

async function resolveUser(ezekiaName, ezekiaEmail) {
  const users = await loadUsers();
  // Try email match first
  if (ezekiaEmail) {
    const byEmail = users.find(u => u.email.toLowerCase() === ezekiaEmail.toLowerCase());
    if (byEmail) return byEmail.id;
  }
  // Try name match
  if (ezekiaName) {
    const nameL = ezekiaName.toLowerCase().trim();
    const byName = users.find(u => u.name.toLowerCase() === nameL);
    if (byName) return byName.id;
    // Partial match (first name)
    const firstName = nameL.split(' ')[0];
    const byFirst = users.find(u => u.name.toLowerCase().startsWith(firstName));
    if (byFirst) return byFirst.id;
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PERSON MATCHING — find existing person or create new
// ═══════════════════════════════════════════════════════════════════════════════

async function matchPerson(candidate) {
  const ezekiaId = String(candidate.id);
  const fullName = candidate.fullName || `${candidate.firstName || ''} ${candidate.lastName || ''}`.trim();
  const email = candidate.emails?.[0]?.email;
  const linkedinUrl = candidate.links?.find(l => l.url?.includes('linkedin'))?.url;

  // 1. Match by Ezekia source_id
  const { rows: bySource } = await pool.query(
    "SELECT id FROM people WHERE source = 'ezekia' AND source_id = $1 AND tenant_id = $2",
    [ezekiaId, ML_TENANT]
  );
  if (bySource.length) return bySource[0].id;

  // 2. Match by email
  if (email) {
    const { rows: byEmail } = await pool.query(
      'SELECT id FROM people WHERE email = $1 AND tenant_id = $2',
      [email, ML_TENANT]
    );
    if (byEmail.length) return byEmail[0].id;
  }

  // 3. Match by LinkedIn URL
  if (linkedinUrl) {
    const slug = linkedinUrl.toLowerCase().replace(/\/+$/, '').split('?')[0];
    const { rows: byLinkedin } = await pool.query(
      "SELECT id FROM people WHERE linkedin_url ILIKE $1 AND tenant_id = $2",
      ['%' + slug.split('/in/')[1] + '%', ML_TENANT]
    );
    if (byLinkedin.length) return byLinkedin[0].id;
  }

  // 4. Exact name match
  if (fullName && fullName.length > 2) {
    const { rows: byName } = await pool.query(
      'SELECT id FROM people WHERE LOWER(TRIM(full_name)) = LOWER(TRIM($1)) AND tenant_id = $2',
      [fullName, ML_TENANT]
    );
    if (byName.length) return byName[0].id;
  }

  // 5. Create new person
  const currentPos = candidate.profile?.positions?.find(p => p.primary || p.tense) || candidate.profile?.positions?.[0];
  const { rows: [created] } = await pool.query(`
    INSERT INTO people (full_name, first_name, last_name, email, linkedin_url,
      current_title, current_company_name, source, source_id, tenant_id, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'ezekia', $8, $9, NOW(), NOW())
    RETURNING id
  `, [
    fullName, candidate.firstName, candidate.lastName,
    email || null, linkedinUrl || null,
    currentPos?.title || null, currentPos?.company?.name || null,
    ezekiaId, ML_TENANT
  ]);
  return created.id;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAP PIPELINE TAG → STATUS
// ═══════════════════════════════════════════════════════════════════════════════

function mapPipelineTag(tag) {
  if (!tag) return 'identified';
  const t = tag.toLowerCase();
  if (t.includes('placed') || t.includes('accepted')) return 'placed';
  if (t.includes('offer')) return 'offer';
  if (t.includes('client') || t.includes('interview') || t.includes('present')) return 'client_interview';
  if (t.includes('shortlist')) return 'shortlisted';
  if (t.includes('screen') || t.includes('assess')) return 'screening';
  if (t.includes('approach') || t.includes('contact') || t.includes('outreach')) return 'approached';
  if (t.includes('reject') || t.includes('decline') || t.includes('withdraw')) return 'rejected';
  if (t.includes('no reach') || t.includes('not suitable')) return 'rejected';
  return 'identified';
}

// ═══════════════════════════════════════════════════════════════════════════════
// SYNC CANDIDATES FOR A PROJECT
// ═══════════════════════════════════════════════════════════════════════════════

async function syncProjectCandidates(ezekiaProjectId, opportunityId) {
  let page = 1;
  let synced = 0, created = 0, errors = 0;

  while (true) {
    const res = await ezekiaFetch(`/api/projects/${ezekiaProjectId}/candidates?per_page=100&page=${page}&fields=meta.candidate`);
    const candidates = res?.data || [];
    if (!candidates.length) break;

    for (const cand of candidates) {
      try {
        const personId = await matchPerson(cand);
        if (!personId) continue;

        // Extract pipeline tag
        const meta = cand.meta?.candidate || {};
        const tags = meta.pipelineTags || [];
        const primaryTag = tags.find(t => !t.hidden) || tags[0];
        const status = mapPipelineTag(primaryTag?.text);

        // Upsert pipeline_contact
        await pool.query(`
          INSERT INTO pipeline_contacts (search_id, person_id, status, source, source_details,
            first_contact_at, tenant_id, created_at, updated_at)
          VALUES ($1, $2, $3, 'ezekia_deep_sync', $4, $5, $6, NOW(), NOW())
          ON CONFLICT (search_id, person_id) DO UPDATE SET
            status = EXCLUDED.status,
            source_details = EXCLUDED.source_details,
            updated_at = NOW()
        `, [
          opportunityId, personId, status,
          primaryTag?.text || 'Identified',
          meta.addedAt ? new Date(meta.addedAt) : new Date(),
          ML_TENANT
        ]);
        synced++;

        // Tag the person with the Ezekia source_id if not set
        await pool.query(
          "UPDATE people SET source = 'ezekia', source_id = $1 WHERE id = $2 AND source_id IS NULL AND tenant_id = $3",
          [String(cand.id), personId, ML_TENANT]
        );
      } catch (e) {
        errors++;
        if (errors <= 3) LOG('⚠️', `    Candidate error ${cand.fullName}: ${e.message}`);
      }
    }

    const meta = res?.meta;
    if (meta?.lastPage && page >= meta.lastPage) break;
    page++;
    await new Promise(r => setTimeout(r, 200));
  }

  return { synced, created, errors };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SYNC NOTES FOR A PROJECT
// ═══════════════════════════════════════════════════════════════════════════════

async function syncProjectNotes(ezekiaProjectId, opportunityId) {
  let imported = 0;
  try {
    const res = await ezekiaFetch(`/api/projects/${ezekiaProjectId}/notes`);
    const researchNotes = res?.data?.researchNotes || [];
    const systemNotes = res?.data?.systemNotes || [];

    // We don't have a person_id for project-level notes — store them linked to the opportunity
    // Use a special interaction_type to mark them
    for (const note of [...researchNotes, ...systemNotes].slice(0, 100)) {
      const text = note.textStripped || note.text || '';
      if (!text || text.length < 5) continue;

      const extId = 'ezekia_proj_note_' + note.id;
      const { rows: existing } = await pool.query(
        'SELECT id FROM interactions WHERE external_id = $1 AND tenant_id = $2',
        [extId, ML_TENANT]
      );
      if (existing.length) continue;

      // Try to resolve the author to a user
      const userId = await resolveUser(note.author);

      // If note mentions a person (notable.type === 'person'), link to them
      let personId = null;
      if (note.notable?.type === 'person' && note.notable?.id) {
        const { rows: match } = await pool.query(
          "SELECT id FROM people WHERE source = 'ezekia' AND source_id = $1 AND tenant_id = $2",
          [String(note.notable.id), ML_TENANT]
        );
        if (match.length) personId = match[0].id;
      }

      await pool.query(`
        INSERT INTO interactions (person_id, user_id, interaction_type, direction, subject, summary,
          source, external_id, channel, interaction_at, tenant_id, created_at)
        VALUES ($1, $2, $3, 'inbound', $4, $5, 'ezekia_deep_sync', $6, 'crm', $7, $8, NOW())
        ON CONFLICT DO NOTHING
      `, [
        personId, userId || null,
        note.type === 'system' ? 'system_note' : 'research_note',
        (note.type === 'system' ? 'Ezekia: ' : '') + (note.author || 'Note').slice(0, 100),
        text.slice(0, 10000),
        extId,
        note.date ? new Date(note.date) : new Date(),
        ML_TENANT
      ]);
      imported++;
    }
  } catch (e) {
    LOG('⚠️', `    Notes error: ${e.message}`);
  }
  return imported;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  LOG('🔄', '═══ Deep Ezekia Sync — Candidates + Pipeline + Notes ═══');

  const args = process.argv.slice(2);
  const singleProject = args.find(a => a.startsWith('--project='))?.split('=')[1];
  const companyFilter = args.find(a => a.startsWith('--company='))?.split('=')[1];

  // Get all opportunities with their Ezekia project codes
  let query = `
    SELECT o.id, o.title, o.code, o.status,
           e.name as project_name, a.name as client_name
    FROM opportunities o
    LEFT JOIN engagements e ON e.id = o.project_id
    LEFT JOIN accounts a ON a.id = e.client_id
    WHERE o.tenant_id = $1
  `;
  const params = [ML_TENANT];

  if (singleProject) {
    query += ' AND o.code = $2';
    params.push(singleProject);
  } else if (companyFilter) {
    query += ' AND a.name ILIKE $2';
    params.push('%' + companyFilter + '%');
  }

  query += ' ORDER BY o.created_at DESC';

  const { rows: opportunities } = await pool.query(query, params);
  LOG('📋', `Found ${opportunities.length} opportunities to sync`);

  // Build Ezekia project ID lookup — code field stores the Ezekia ID
  // Try to extract Ezekia project ID from code (format: "ezekia_XXXXX" or just the number)
  let totalCandidates = 0, totalNotes = 0, totalErrors = 0;
  let projectsSynced = 0;

  // Also get projects from Ezekia to build ID mapping
  LOG('🔗', 'Building Ezekia project ID mapping...');
  const ezekiaProjects = new Map();
  let page = 1;
  while (true) {
    try {
      const res = await ezekiaFetch(`/api/projects?per_page=100&page=${page}&fields[]=relationships.company`);
      const projects = res?.data || [];
      if (!projects.length) break;
      for (const p of projects) {
        ezekiaProjects.set(p.name?.toLowerCase().trim(), { id: p.id, name: p.name, company: p.relationships?.company?.name });
        // Also map by company name + project name combo
        const compName = p.relationships?.company?.name;
        if (compName) ezekiaProjects.set((compName + ' | ' + p.name).toLowerCase(), { id: p.id, name: p.name, company: compName });
      }
      const meta = res?.meta;
      if (meta?.lastPage && page >= meta.lastPage) break;
      page++;
      await new Promise(r => setTimeout(r, 150));
    } catch (e) { break; }
  }
  LOG('📊', `Loaded ${ezekiaProjects.size} Ezekia projects for matching`);

  for (const opp of opportunities) {
    // Try to find matching Ezekia project
    let ezekiaId = null;

    // Check code field for Ezekia ID
    if (opp.code && opp.code.match(/^\d+$/)) {
      ezekiaId = parseInt(opp.code);
    } else if (opp.code && opp.code.startsWith('ezekia_')) {
      ezekiaId = parseInt(opp.code.replace('ezekia_', ''));
    }

    // Try matching by title
    if (!ezekiaId) {
      const titleMatch = ezekiaProjects.get(opp.title?.toLowerCase().trim());
      if (titleMatch) ezekiaId = titleMatch.id;
    }

    // Try matching by client + title
    if (!ezekiaId && opp.client_name) {
      const comboMatch = ezekiaProjects.get((opp.client_name + ' | ' + opp.title).toLowerCase());
      if (comboMatch) ezekiaId = comboMatch.id;
    }

    if (!ezekiaId) {
      // Try fuzzy match on project name
      for (const [key, val] of ezekiaProjects) {
        if (opp.title && key.includes(opp.title.toLowerCase().trim())) {
          ezekiaId = val.id;
          break;
        }
      }
    }

    if (!ezekiaId) {
      totalErrors++;
      continue; // Can't find Ezekia project
    }

    LOG('🔍', `[${opp.status}] ${opp.client_name || '?'} — ${opp.title} (Ezekia #${ezekiaId})`);

    // Sync candidates
    const candResult = await syncProjectCandidates(ezekiaId, opp.id);
    totalCandidates += candResult.synced;
    if (candResult.synced) LOG('👥', `    ${candResult.synced} candidates synced`);

    // Sync notes
    const notesImported = await syncProjectNotes(ezekiaId, opp.id);
    totalNotes += notesImported;
    if (notesImported) LOG('📝', `    ${notesImported} notes imported`);

    projectsSynced++;
    await new Promise(r => setTimeout(r, 300));
  }

  LOG('🔄', '═══ Deep Sync Complete ═══');
  LOG('📊', `Projects synced: ${projectsSynced} / ${opportunities.length}`);
  LOG('👥', `Candidates: ${totalCandidates}`);
  LOG('📝', `Notes: ${totalNotes}`);
  LOG('⚠️', `Unmatched projects: ${totalErrors}`);

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
