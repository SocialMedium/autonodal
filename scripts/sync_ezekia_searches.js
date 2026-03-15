#!/usr/bin/env node
/**
 * MitchelLake Signal Intelligence Platform
 * Sync Ezekia Projects → clients + projects + searches
 *
 * Imports ALL projects (active and closed) — historical searches are signals:
 *   - We've worked with this company before
 *   - We've placed into this type of role before
 *   - Shortlisted candidates may now be available
 *   - Thematic vector matching across briefs
 *
 * Usage:
 *   node scripts/sync_ezekia_searches.js           # All projects
 *   node scripts/sync_ezekia_searches.js --active  # Active only
 *   node scripts/sync_ezekia_searches.js --test    # Dry run (no writes)
 */

require('dotenv').config();
const db = require('../lib/db');
const ezekia = require('../lib/ezekia');

const ACTIVE_STATUSES = ['Interviewing', 'Urgent', 'Overdue', 'Active', 'In Progress', 'Shortlisting', 'Research'];

// Map Ezekia status text → search_status enum
function mapStatus(statusText) {
  if (!statusText) return 'on_hold';
  const s = statusText.toLowerCase();
  if (s.includes('interviewing'))        return 'interviewing';
  if (s.includes('urgent'))              return 'sourcing';
  if (s.includes('overdue'))             return 'sourcing';
  if (s.includes('shortlist'))           return 'shortlist';
  if (s.includes('research'))            return 'research';
  if (s.includes('brief'))               return 'briefing';
  if (s.includes('outreach'))            return 'outreach';
  if (s.includes('offer'))               return 'offer';
  if (s.includes('negotiat'))            return 'negotiation';
  if (s.includes('placed'))              return 'placed';
  if (s.includes('terminat') || s.includes('cancelled') || s.includes('closed')) return 'cancelled';
  if (s.includes('hold') || s.includes('paused')) return 'on_hold';
  return 'on_hold';
}

// Map Ezekia status → project status (simple)
function mapProjectStatus(statusText) {
  if (!statusText) return 'active';
  const s = statusText.toLowerCase();
  if (s.includes('placed'))                            return 'completed';
  if (s.includes('terminat') || s.includes('cancel')) return 'cancelled';
  if (s.includes('hold') || s.includes('paused'))     return 'on_hold';
  return 'active';
}

// Derive seniority from title
function deriveSeniority(title) {
  if (!title) return null;
  const t = title.toLowerCase();
  if (t.includes('chief') || t.includes(' ceo') || t.includes(' coo') || t.includes(' cfo') || t.includes(' cto') || t.includes(' cmo') || t.includes('president') || t.includes('chairman')) return 'c_suite';
  if (t.includes('vp ') || t.includes('vice president') || t.includes('partner') || t.includes(' svp') || t.includes(' evp')) return 'vp';
  if (t.includes('director') || t.includes(' gm') || t.includes('general manager')) return 'director';
  if (t.includes('manager') || t.includes('head of') || t.includes('lead')) return 'manager';
  if (t.includes('senior') || t.includes('sr.') || t.includes('principal')) return 'senior_ic';
  if (t.includes('junior') || t.includes('jr.') || t.includes('associate') || t.includes('analyst')) return 'junior';
  return 'mid_level';
}

// Find or create a client record for the company
async function upsertClient(company, isDryRun) {
  if (!company) return null;

  // Find existing company record
  const existingCompany = await db.queryOne(
    `SELECT id FROM companies WHERE name ILIKE $1 LIMIT 1`,
    [company.name]
  );
  const companyId = existingCompany?.id || null;

  // Find existing client
  const existingClient = await db.queryOne(
    `SELECT id FROM accounts WHERE name ILIKE $1 LIMIT 1`,
    [company.name]
  );

  if (existingClient) return existingClient.id;

  if (isDryRun) {
    console.log(`  [DRY RUN] Would create client: ${company.name}`);
    return null;
  }

  const result = await db.query(
    `INSERT INTO accounts (company_id, name, relationship_status, relationship_tier, first_engagement_date, created_at, updated_at)
     VALUES ($1, $2, 'active', 'standard', NOW(), NOW(), NOW())
     RETURNING id`,
    [companyId, company.name]
  );

  return result.rows[0]?.id || null;
}

// Find or create a project record
async function upsertProject(ezekiaProject, clientId, isDryRun) {
  const externalId = `ezekia_${ezekiaProject.id}`;
  
  const existing = await db.queryOne(
    `SELECT id FROM engagements WHERE code = $1`,
    [externalId]
  );
  if (existing) return { id: existing.id, isNew: false };

  if (isDryRun) {
    console.log(`  [DRY RUN] Would create project: ${ezekiaProject.name}`);
    return { id: null, isNew: false };
  }

  const projectStatus = mapProjectStatus(ezekiaProject.manager?.status?.text);
  const industries = ezekiaProject.industries?.map(i => i.name).join(', ') || null;

  const result = await db.query(
    `INSERT INTO engagements (
      client_id, name, code, project_type, description, status,
      kick_off_date, target_completion_date, client_context,
      created_at, updated_at
    ) VALUES ($1,$2,$3,'search',$4,$5,$6,$7,$8,NOW(),NOW())
    RETURNING id`,
    [
      clientId,
      ezekiaProject.name,
      externalId,
      ezekiaProject.description || null,
      projectStatus,
      ezekiaProject.startDate ? new Date(ezekiaProject.startDate) : null,
      ezekiaProject.endDate ? new Date(ezekiaProject.endDate) : null,
      industries ? `Industries: ${industries}` : null
    ]
  );

  return { id: result.rows[0]?.id, isNew: true };
}

// Find or create a search record
async function upsertSearch(ezekiaProject, projectId, leadUserId, isDryRun) {
  const externalId = `ezekia_${ezekiaProject.id}`;

  const existing = await db.queryOne(
    `SELECT id FROM opportunities WHERE code = $1`,
    [externalId]
  );
  if (existing) return { id: existing.id, isNew: false };

  if (isDryRun) {
    console.log(`  [DRY RUN] Would create search: ${ezekiaProject.name}`);
    return { id: null, isNew: false };
  }

  const status = mapStatus(ezekiaProject.manager?.status?.text);
  const seniority = deriveSeniority(ezekiaProject.name);
  const industries = ezekiaProject.industries?.map(i => i.name) || [];

  const result = await db.query(
    `INSERT INTO opportunities (
      project_id, title, code, status, seniority_level,
      lead_consultant_id, kick_off_date,
      brief_summary, target_industries,
      created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW())
    RETURNING id`,
    [
      projectId,
      ezekiaProject.name,
      externalId,
      status,
      seniority,
      leadUserId,
      ezekiaProject.startDate ? new Date(ezekiaProject.startDate) : null,
      ezekiaProject.description || null,
      industries.length ? industries : null
    ]
  );

  return { id: result.rows[0]?.id, isNew: true };
}

// Resolve Ezekia owner email → local user ID
async function resolveUserId(ownerEmail) {
  if (!ownerEmail) return null;
  const user = await db.queryOne(
    `SELECT id FROM users WHERE email ILIKE $1`,
    [ownerEmail]
  );
  return user?.id || null;
}

async function main() {
  const args = process.argv.slice(2);
  const activeOnly = args.includes('--active');
  const isDryRun = args.includes('--test');

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  EZEKIA SEARCH IMPORT — MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  if (isDryRun) console.log('  ⚠️  DRY RUN — no writes will occur');
  if (activeOnly) console.log('  📌 Active searches only');
  console.log();

  const stats = { total: 0, clients: 0, projects: 0, searches: 0, skipped: 0, errors: 0 };

  // Cache user IDs
  const userCache = {};
  const resolveUser = async (email) => {
    if (!email) return null;
    if (!(email in userCache)) userCache[email] = await resolveUserId(email);
    return userCache[email];
  };

  let page = 1;
  let lastPage = null;

  while (true) {
    console.log(`Fetching page ${page}${lastPage ? ' of ' + lastPage : ''}...`);

    let response;
    try {
      response = await ezekia.getProjects({ page, per_page: 100 });
    } catch (err) {
      console.error(`  ✗ API error on page ${page}:`, err.message);
      stats.errors++;
      if (stats.errors > 50) break;
      await new Promise(r => setTimeout(r, 3000));
      continue;
    }

    lastPage = response.meta?.lastPage;
    const projects = response.data || [];
    if (projects.length === 0) break;

    for (const project of projects) {
      stats.total++;
      const statusText = project.manager?.status?.text || '';

      // Skip if active only and not active
      if (activeOnly && !ACTIVE_STATUSES.some(s => statusText.includes(s))) {
        stats.skipped++;
        continue;
      }

      try {
        const company = project.relationships?.company;
        const ownerEmail = project.owner?.email;
        const leadUserId = await resolveUser(ownerEmail);

        // 1. Upsert client
        const clientId = await upsertClient(company, isDryRun);
        if (clientId && !isDryRun) stats.clients++;

        // 2. Upsert project
        if (clientId || isDryRun) {
          const { id: projectId, isNew: projectIsNew } = await upsertProject(project, clientId, isDryRun);
          if (projectIsNew) stats.projects++;

          // 3. Upsert search
          if (projectId || isDryRun) {
            const { id: searchId, isNew: searchIsNew } = await upsertSearch(project, projectId, leadUserId, isDryRun);
            if (searchIsNew) stats.searches++;

            if (!isDryRun && (projectIsNew || searchIsNew)) {
              console.log(`  ✓ [${statusText}] ${company?.name || '?'} — ${project.name}`);
            }
          }
        }
      } catch (err) {
        console.error(`  ✗ Error on project ${project.id} (${project.name}):`, err.message);
        stats.errors++;
      }
    }

    console.log(`  Page ${page}/${lastPage || '?'} done — running totals: ${stats.searches} searches, ${stats.errors} errors`);

    if (lastPage && page >= lastPage) break;
    page++;
    await new Promise(r => setTimeout(r, 150));
  }

  console.log();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  IMPORT COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Total Ezekia projects:  ${stats.total}`);
  console.log(`  Skipped:                ${stats.skipped}`);
  console.log(`  Clients created:        ${stats.clients}`);
  console.log(`  Projects created:       ${stats.projects}`);
  console.log(`  Searches created:       ${stats.searches}`);
  console.log(`  Errors:                 ${stats.errors}`);
  console.log();
  console.log('  Next steps:');
  console.log('  → node scripts/match_searches.js   (match candidates to active searches)');
  console.log('  → node scripts/embed_all.js         (embed search briefs into Qdrant)');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
