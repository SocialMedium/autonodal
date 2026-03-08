#!/usr/bin/env node
/**
 * MitchelLake — Immutable Deep Probe
 * 
 * Pulls ALL available data about Immutable from:
 *   1. Ezekia Projects (assignments where Immutable is the client)
 *   2. Ezekia Project Candidates (with research notes)
 *   3. Ezekia People (contacts at Immutable)
 *   4. Gmail (emails to/from @immutable.com)
 *   5. Google News (recent signals)
 * 
 * Outputs raw JSON structures for field mapping + a human-readable summary.
 * 
 * Usage:
 *   node probe_immutable.js
 *   node probe_immutable.js --company="Culture Amp"    # probe a different client
 */

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

let ezekia;
try {
  ezekia = require('./lib/ezekia');
} catch (e) {
  console.warn('⚠️  lib/ezekia.js not found — skipping Ezekia probes');
}

let googleLib;
try {
  googleLib = require('./lib/google');
} catch (e) {
  console.warn('⚠️  lib/google.js not found — skipping Google probes');
}

// ─── Config ──────────────────────────────────────────────────────────────────

const TARGET = process.argv.find(a => a.startsWith('--company='))
  ?.replace('--company=', '') || 'Immutable';

const TARGET_LOWER = TARGET.toLowerCase();
const OUTPUT_DIR = './probe_output';

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 1: EZEKIA PROJECTS — Find all assignments for this client
// ═══════════════════════════════════════════════════════════════════════════════

async function probeEzekiaProjects() {
  if (!ezekia) return null;

  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 1: Ezekia Projects for "${TARGET}"                      `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const matchingProjects = [];
  let page = 1;
  let totalPages = 1;

  while (page <= totalPages) {
    try {
      console.log(`  Scanning projects page ${page}...`);
      const response = await ezekia.getProjects({ page, per_page: 100 });
      const projects = response.data || [];
      totalPages = response.meta?.lastPage || 1;

      for (const project of projects) {
        const companyName = project.relationships?.company?.name || '';
        if (companyName.toLowerCase().includes(TARGET_LOWER)) {
          matchingProjects.push(project);
          const status = project.manager?.status?.text || 'Unknown';
          console.log(`  ✅ Found: "${project.name}" — ${status}`);
        }
      }

      page++;
      await sleep(300);
    } catch (err) {
      console.error(`  ❌ Error on page ${page}:`, err.message);
      break;
    }
  }

  console.log(`\n  📊 Found ${matchingProjects.length} projects for ${TARGET}`);

  // Dump first project's FULL structure for field mapping
  if (matchingProjects.length > 0) {
    console.log('\n  ── RAW PROJECT STRUCTURE (first match) ──');
    console.log('  Top-level keys:', Object.keys(matchingProjects[0]));
    if (matchingProjects[0].manager) {
      console.log('  manager keys:', Object.keys(matchingProjects[0].manager));
    }
    if (matchingProjects[0].relationships) {
      console.log('  relationships keys:', Object.keys(matchingProjects[0].relationships));
    }
  }

  return matchingProjects;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 2: EZEKIA CANDIDATES — Get candidates + research notes per project
// ═══════════════════════════════════════════════════════════════════════════════

async function probeEzekiaCandidates(projects) {
  if (!ezekia || !projects?.length) return null;

  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 2: Candidates & Research Notes (${projects.length} projects)       `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const allCandidates = [];
  let totalNotes = 0;
  let firstCandidateRaw = null;

  for (const project of projects) {
    try {
      console.log(`\n  📁 "${project.name}" (Ezekia ID: ${project.id})`);
      let candidatePage = 1;
      let candidatePages = 1;

      while (candidatePage <= candidatePages) {
        const response = await ezekia.getProjectCandidates(project.id, {
          page: candidatePage,
          per_page: 50,
          fields: 'meta.candidate,profile.positions,manager.researchNotes,manager.mostRecentResearcherNote'
        });

        const candidates = response.data || [];
        candidatePages = response.meta?.lastPage || 1;

        for (const candidate of candidates) {
          // Save first raw candidate for field mapping
          if (!firstCandidateRaw) {
            firstCandidateRaw = candidate;
          }

          const notes = candidate.manager?.researchNotes || [];
          const noteCount = notes.length;
          totalNotes += noteCount;

          // Check if this person works at the CLIENT company (= client contact, not candidate)
          const positions = candidate.profile?.positions || [];
          const currentPos = positions.find(p => p.primary) || positions[0];
          const worksAtClient = currentPos?.company?.name?.toLowerCase().includes(TARGET_LOWER);

          allCandidates.push({
            ezekia_id: candidate.id,
            name: candidate.fullName || `${candidate.firstName || ''} ${candidate.lastName || ''}`.trim(),
            email: candidate.emails?.[0]?.email,
            current_title: currentPos?.title,
            current_company: currentPos?.company?.name,
            current_company_id: currentPos?.company?.id,
            is_client_contact: worksAtClient,
            note_count: noteCount,
            notes: notes.map(n => ({
              id: n.id,
              text: n.textStripped || n.text,
              date: n.date
            })),
            project_id: project.id,
            project_name: project.name,
            // Pipeline info
            pipeline_tags: candidate.manager?.pipelineTags || candidate.pipelineTags,
            rank: candidate.rank,
            added_by: candidate.manager?.addedBy?.fullName || candidate.addedBy,
            // Raw keys for mapping
            _top_keys: Object.keys(candidate),
            _manager_keys: candidate.manager ? Object.keys(candidate.manager) : [],
            _profile_keys: candidate.profile ? Object.keys(candidate.profile) : []
          });

          if (worksAtClient) {
            console.log(`    👤 CLIENT CONTACT: ${candidate.fullName} — ${currentPos?.title}`);
          } else if (noteCount > 0) {
            console.log(`    📝 ${candidate.fullName} — ${noteCount} note(s)`);
          }
        }

        candidatePage++;
        await sleep(300);
      }
    } catch (err) {
      console.error(`  ❌ Error on project ${project.id}:`, err.message);
    }
  }

  const clientContacts = allCandidates.filter(c => c.is_client_contact);
  const withNotes = allCandidates.filter(c => c.note_count > 0);

  console.log(`\n  📊 SUMMARY`);
  console.log(`     Total candidates across projects: ${allCandidates.length}`);
  console.log(`     Client contacts found: ${clientContacts.length}`);
  console.log(`     Candidates with notes: ${withNotes.length}`);
  console.log(`     Total research notes: ${totalNotes}`);

  // Dump raw structure
  if (firstCandidateRaw) {
    console.log('\n  ── RAW CANDIDATE STRUCTURE (first match) ──');
    console.log('  Top-level keys:', Object.keys(firstCandidateRaw));
    if (firstCandidateRaw.manager) {
      console.log('  manager keys:', Object.keys(firstCandidateRaw.manager));
    }
    if (firstCandidateRaw.profile) {
      console.log('  profile keys:', Object.keys(firstCandidateRaw.profile));
    }
    if (firstCandidateRaw.relationships) {
      console.log('  relationships keys:', Object.keys(firstCandidateRaw.relationships));
    }
  }

  return { allCandidates, clientContacts, withNotes };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 3: EZEKIA PEOPLE — Search for people at this company
// ═══════════════════════════════════════════════════════════════════════════════

async function probeEzekiaPeople(ezekiaCompanyId) {
  if (!ezekia) return null;

  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 3: Ezekia People at "${TARGET}"                         `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  // Strategy: Paginate through people endpoint, filter by company
  // Ezekia may not support direct company filtering, so we scan + filter
  // We'll check first 500 people (5 pages) to keep it fast
  // and also try the companies endpoint for linked people

  const contactsFound = [];

  // Approach A: Try Ezekia companies endpoint
  if (ezekiaCompanyId) {
    try {
      console.log(`  Trying GET /api/companies/${ezekiaCompanyId}...`);
      const companyDetail = await ezekia.ezekiaFetch
        ? await (async () => {
            // Direct API call if available
            const resp = await fetch(`${process.env.EZEKIA_API_URL}/api/companies/${ezekiaCompanyId}`, {
              headers: {
                'Authorization': `Bearer ${process.env.EZEKIA_API_TOKEN || process.env.EZEKIA_API_KEY}`,
                'Accept': 'application/json'
              }
            });
            return resp.json();
          })()
        : null;

      if (companyDetail) {
        console.log('  Company detail keys:', Object.keys(companyDetail.data || companyDetail));
        const data = companyDetail.data || companyDetail;
        if (data.relationships) {
          console.log('  Company relationships keys:', Object.keys(data.relationships));
        }
        if (data.people || data.relationships?.people) {
          const people = data.people || data.relationships?.people;
          console.log(`  ✅ Found ${Array.isArray(people) ? people.length : '?'} people linked to company`);
        }
      }
    } catch (err) {
      console.log(`  ⚠️  Company detail endpoint failed: ${err.message}`);
    }
  }

  // Approach B: Search people by company name in positions
  console.log(`\n  Scanning Ezekia people for "${TARGET}" employees...`);
  let page = 1;
  const maxPages = 10; // Scan first 1000 people

  while (page <= maxPages) {
    try {
      const response = await ezekia.getPeople({
        page,
        per_page: 100,
        fields: 'profile.positions'
      });

      const people = response.data || [];
      if (people.length === 0) break;

      const totalPages = response.meta?.lastPage || 1;
      if (page > totalPages) break;

      for (const person of people) {
        const positions = person.profile?.positions || person.positions || [];
        const currentPos = positions.find(p => p.primary) || positions[0];
        const companyName = currentPos?.company?.name || '';

        if (companyName.toLowerCase().includes(TARGET_LOWER)) {
          contactsFound.push({
            ezekia_id: person.id,
            name: person.fullName || `${person.firstName || ''} ${person.lastName || ''}`.trim(),
            email: person.emails?.[0]?.email,
            title: currentPos?.title,
            company: companyName,
            company_id: currentPos?.company?.id,
            phone: person.phones?.[0]?.number,
            linkedin: person.links?.find(l => l.label?.toLowerCase() === 'linkedin')?.url
          });
          console.log(`    👤 ${person.fullName} — ${currentPos?.title}`);
        }
      }

      page++;
      await sleep(300);
    } catch (err) {
      console.error(`  ❌ Error on people page ${page}:`, err.message);
      break;
    }
  }

  // Approach C: Check our own database
  console.log(`\n  Checking local database for "${TARGET}" employees...`);
  try {
    const { rows } = await pool.query(`
      SELECT p.id, p.full_name, p.email, p.current_title, p.current_company_name,
             p.phone, p.linkedin_url, p.source, p.source_id
      FROM people p
      WHERE p.current_company_name ILIKE $1
      ORDER BY p.full_name
      LIMIT 50
    `, [`%${TARGET}%`]);

    if (rows.length > 0) {
      console.log(`  ✅ Found ${rows.length} people in local DB at "${TARGET}":`);
      rows.forEach(r => console.log(`    👤 ${r.full_name} — ${r.current_title} (source: ${r.source})`));
    } else {
      console.log(`  ⚠️  No people in local DB with company matching "${TARGET}"`);
    }
  } catch (err) {
    console.error('  DB query error:', err.message);
  }

  console.log(`\n  📊 Total contacts found via Ezekia scan: ${contactsFound.length}`);
  return contactsFound;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 4: GMAIL — Emails to/from this company's domain
// ═══════════════════════════════════════════════════════════════════════════════

async function probeGmail() {
  if (!googleLib) return null;

  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 4: Gmail — Emails with "${TARGET}"                      `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  // Get Google account credentials from DB
  let account;
  try {
    const { rows } = await pool.query(`
      SELECT * FROM user_google_accounts 
      ORDER BY last_sync_at DESC NULLS LAST 
      LIMIT 1
    `);
    account = rows[0];
  } catch (err) {
    console.log('  ⚠️  No user_google_accounts table or no accounts:', err.message);
    return null;
  }

  if (!account) {
    console.log('  ⚠️  No Google account connected. Skip Gmail probe.');
    return null;
  }

  console.log(`  Using account: ${account.google_email}`);

  try {
    // Refresh token if needed
    if (account.token_expires_at && new Date(account.token_expires_at) < new Date()) {
      console.log('  Refreshing access token...');
      const newTokens = await googleLib.refreshAccessToken(account.refresh_token);
      await pool.query(
        'UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2 WHERE id = $3',
        [newTokens.access_token, new Date(newTokens.expiry_date), account.id]
      );
      account.access_token = newTokens.access_token;
    }

    const auth = googleLib.getAuthenticatedClient(account.access_token, account.refresh_token);
    const gmail = googleLib.getGmailClient(auth);

    // Guess domain from company name — works for most tech companies
    const domainGuesses = guessDomains(TARGET);
    console.log(`  Domain guesses: ${domainGuesses.join(', ')}`);

    const emailSummaries = [];

    for (const domain of domainGuesses) {
      console.log(`\n  Searching: from:${domain} OR to:${domain}`);

      const query = `from:${domain} OR to:${domain}`;
      const response = await googleLib.listMessages(gmail, query, 25);
      const messages = response.messages || [];

      console.log(`  Found ${messages.length} messages`);

      // Get details for first 10 messages
      const limit = Math.min(messages.length, 10);
      for (let i = 0; i < limit; i++) {
        try {
          const msg = await googleLib.getMessage(gmail, messages[i].id, 'metadata');
          const parsed = googleLib.parseMessage(msg);

          emailSummaries.push({
            id: msg.id,
            threadId: msg.threadId,
            date: parsed.date,
            from: parsed.from,
            to: parsed.to,
            subject: parsed.subject,
            // NO BODY — just metadata for hyperlink summaries
            snippet: msg.snippet?.substring(0, 150),
            labels: msg.labelIds
          });

          const direction = parsed.from?.includes(domain) ? '📥' : '📤';
          console.log(`    ${direction} ${parsed.date?.substring(0, 10)} — ${parsed.subject?.substring(0, 60)}`);

          await sleep(100);
        } catch (err) {
          console.error(`    ❌ Error fetching message:`, err.message);
        }
      }

      if (messages.length > limit) {
        console.log(`    ... and ${messages.length - limit} more`);
      }
    }

    console.log(`\n  📊 Total email threads found: ${emailSummaries.length}`);
    console.log(`\n  💡 DESIGN NOTE: These should be stored as interaction summaries`);
    console.log(`     with hyperlinks to Gmail (https://mail.google.com/mail/u/0/#inbox/<msg_id>)`);
    console.log(`     NOT full email bodies — keep it clean.`);

    return emailSummaries;
  } catch (err) {
    console.error('  ❌ Gmail error:', err.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 5: GOOGLE NEWS — Recent signals about this company
// ═══════════════════════════════════════════════════════════════════════════════

async function probeGoogleNews() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 5: Google News — Signals for "${TARGET}"                `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  // Use Google News RSS feed (no API key needed)
  let Parser;
  try {
    Parser = require('rss-parser');
  } catch (e) {
    console.log('  ⚠️  rss-parser not installed. Run: npm install rss-parser');
    return null;
  }
  const parser = new Parser();
  const encoded = encodeURIComponent(`"${TARGET}" hiring OR funding OR expansion OR acquisition`);
  const url = `https://news.google.com/rss/search?q=${encoded}&hl=en&gl=AU&ceid=AU:en`;

  try {
    console.log(`  Fetching: ${url}\n`);
    const feed = await parser.parseURL(url);
    const articles = (feed.items || []).slice(0, 15);

    console.log(`  Found ${articles.length} news articles:\n`);

    const signals = articles.map(item => {
      const published = item.pubDate ? new Date(item.pubDate).toISOString().substring(0, 10) : '?';
      console.log(`  📰 ${published} — ${item.title}`);
      console.log(`     ${item.link}\n`);

      return {
        title: item.title,
        link: item.link,
        published: item.pubDate,
        source: item.creator || item.source?.title,
        snippet: item.contentSnippet?.substring(0, 200)
      };
    });

    return signals;
  } catch (err) {
    console.error('  ❌ Google News RSS error:', err.message);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 6: LOCAL DB — What we already know
// ═══════════════════════════════════════════════════════════════════════════════

async function probeLocalDB() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║ PHASE 6: Local Database — Existing "${TARGET}" Data            `);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  const results = {};

  // Company record
  try {
    const { rows } = await pool.query(`
      SELECT * FROM companies WHERE name ILIKE $1 LIMIT 5
    `, [`%${TARGET}%`]);
    results.companies = rows;
    console.log(`  Companies matching: ${rows.length}`);
    rows.forEach(r => console.log(`    🏢 ${r.name} (id: ${r.id}, is_client: ${r.is_client}, sector: ${r.sector})`));
  } catch (e) {}

  // Client record
  try {
    const { rows } = await pool.query(`
      SELECT cl.*, cf.total_invoiced, cf.total_placements 
      FROM clients cl
      LEFT JOIN client_financials cf ON cf.client_id = cl.id
      WHERE cl.name ILIKE $1 LIMIT 5
    `, [`%${TARGET}%`]);
    results.clients = rows;
    console.log(`\n  Clients matching: ${rows.length}`);
    rows.forEach(r => console.log(`    💰 ${r.name} — $${r.total_invoiced || 0} revenue, ${r.total_placements || 0} placements`));
  } catch (e) {}

  // Client contacts
  try {
    const { rows } = await pool.query(`
      SELECT cc.* FROM client_contacts cc
      JOIN clients cl ON cc.client_id = cl.id
      WHERE cl.name ILIKE $1
    `, [`%${TARGET}%`]);
    results.client_contacts = rows;
    console.log(`\n  Client contacts: ${rows.length}`);
    if (rows.length === 0) console.log('    ⚠️  NONE — this is what we need to fix!');
    rows.forEach(r => console.log(`    👤 ${r.name} — ${r.title} (${r.role})`));
  } catch (e) {}

  // Placements
  try {
    const { rows } = await pool.query(`
      SELECT pl.role_title, pl.start_date, pl.placement_fee, pl.fee_category,
             pe.full_name AS candidate_name
      FROM placements pl
      JOIN clients cl ON pl.client_id = cl.id
      LEFT JOIN people pe ON pl.person_id = pe.id
      WHERE cl.name ILIKE $1
      ORDER BY pl.start_date DESC
    `, [`%${TARGET}%`]);
    results.placements = rows;
    console.log(`\n  Placements: ${rows.length}`);
    rows.forEach(r => console.log(`    📋 ${r.start_date?.toISOString().substring(0,10) || '?'} — ${r.role_title} — $${r.placement_fee} (${r.candidate_name || 'no candidate'})`));
  } catch (e) {}

  // Signals
  try {
    const { rows } = await pool.query(`
      SELECT se.signal_type, se.signal_category, se.confidence_score, se.evidence_summary, se.detected_at
      FROM signal_events se
      JOIN companies c ON se.company_id = c.id
      WHERE c.name ILIKE $1
      ORDER BY se.detected_at DESC LIMIT 10
    `, [`%${TARGET}%`]);
    results.signals = rows;
    console.log(`\n  Signals: ${rows.length}`);
    rows.forEach(r => console.log(`    ⚡ ${r.signal_type} (${r.confidence_score}) — ${r.evidence_summary?.substring(0, 80)}`));
  } catch (e) {}

  // Research notes (interactions)
  try {
    const { rows } = await pool.query(`
      SELECT i.summary, i.interaction_at, i.interaction_type, p.full_name
      FROM interactions i
      JOIN people p ON i.person_id = p.id
      WHERE p.current_company_name ILIKE $1
        AND i.interaction_type = 'research_note'
      ORDER BY i.interaction_at DESC LIMIT 10
    `, [`%${TARGET}%`]);
    results.research_notes = rows;
    console.log(`\n  Research notes: ${rows.length}`);
    rows.forEach(r => console.log(`    📝 ${r.full_name} — ${r.summary?.substring(0, 80)}`));
  } catch (e) {}

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function guessDomains(companyName) {
  const base = companyName.toLowerCase()
    .replace(/\b(pty|ltd|inc|corp|limited|llc|group|holdings)\b/gi, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
  return [
    `@${base}.com`,
    `@${base}.com.au`,
    `@${base}.io`,
    `@${base}.co`,
  ];
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  MITCHELLAKE DEEP PROBE: "${TARGET}"`);
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Time: ${new Date().toISOString()}`);

  const output = { target: TARGET, timestamp: new Date().toISOString() };

  // Phase 1: Ezekia Projects
  const projects = await probeEzekiaProjects();
  output.ezekia_projects = projects;

  // Get the Ezekia company ID from projects
  let ezekiaCompanyId = null;
  if (projects?.length > 0) {
    ezekiaCompanyId = projects[0].relationships?.company?.id;
    console.log(`\n  Ezekia Company ID for ${TARGET}: ${ezekiaCompanyId}`);
  }

  // Phase 2: Candidates + Research Notes
  const candidates = await probeEzekiaCandidates(projects);
  output.ezekia_candidates = candidates;

  // Phase 3: People at this company
  const contacts = await probeEzekiaPeople(ezekiaCompanyId);
  output.ezekia_contacts = contacts;

  // Phase 4: Gmail
  const emails = await probeGmail();
  output.gmail = emails;

  // Phase 5: Google News
  const news = await probeGoogleNews();
  output.google_news = news;

  // Phase 6: Local DB
  const localData = await probeLocalDB();
  output.local_db = localData;

  // ── Write output ──
  try {
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    const filename = `${OUTPUT_DIR}/probe_${TARGET.toLowerCase().replace(/\s+/g, '_')}_${new Date().toISOString().substring(0, 10)}.json`;
    fs.writeFileSync(filename, JSON.stringify(output, null, 2));
    console.log(`\n\n  📄 Full output written to: ${filename}`);
  } catch (err) {
    console.error('  Could not write output file:', err.message);
  }

  // ── Final Summary ──
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log(`  PROBE SUMMARY: "${TARGET}"`);
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Ezekia Projects:         ${projects?.length || 0}`);
  console.log(`  Ezekia Company ID:       ${ezekiaCompanyId || 'NOT FOUND'}`);
  console.log(`  Total Candidates:        ${candidates?.allCandidates?.length || 0}`);
  console.log(`  Client Contacts (Ezekia):${candidates?.clientContacts?.length || 0}`);
  console.log(`  Candidates with Notes:   ${candidates?.withNotes?.length || 0}`);
  console.log(`  People at Company:       ${contacts?.length || 0}`);
  console.log(`  Gmail Threads:           ${emails?.length || 0}`);
  console.log(`  Google News Articles:    ${news?.length || 0}`);
  console.log(`  Local DB Placements:     ${localData?.placements?.length || 0}`);
  console.log(`  Local DB Contacts:       ${localData?.client_contacts?.length || 0}`);
  console.log('═══════════════════════════════════════════════════════════════');

  console.log('\n  💡 DESIGN PRINCIPLES FOR INGESTION:');
  console.log('  • Emails → Store as interaction summaries with Gmail hyperlinks');
  console.log('    Format: https://mail.google.com/mail/u/0/#inbox/<message_id>');
  console.log('  • News → Store as signal_events with source_url hyperlinks');
  console.log('  • Notes → Store as interactions with date + project context');
  console.log('  • NO raw email bodies in the database — metadata + link only');
  console.log('  • Client contacts → client_contacts table linked to people');

  await pool.end();
}

main().catch(err => {
  console.error('Fatal error:', err);
  pool.end();
  process.exit(1);
});
