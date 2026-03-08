#!/usr/bin/env node
/**
 * MitchelLake — LinkedIn Enrichment Sync
 * ───────────────────────────────────────
 * Ingests from the full LinkedIn data export:
 *   1. Connections.csv     → updates people.email, linkedin_url, connected_on in team_proximity
 *   2. Invitations.csv     → logs invitation messages as interactions
 *   3. Recommendations_Received.csv → logs as interactions (high-signal relationship text)
 *   4. Recommendations_Given.csv    → logs as interactions (shows your relationship depth)
 *
 * Usage:
 *   node scripts/sync_linkedin_enrichment.js
 *   node scripts/sync_linkedin_enrichment.js --dry-run
 *   node scripts/sync_linkedin_enrichment.js --skip-connections
 *   node scripts/sync_linkedin_enrichment.js --skip-invitations
 *   node scripts/sync_linkedin_enrichment.js --skip-recommendations
 */

require('dotenv').config();
const fs   = require('fs');
const path = require('path');
const { Pool } = require('pg');

// ─── Config ──────────────────────────────────────────────────────────────────

const EXPORT_DIR = process.env.LINKEDIN_EXPORT_DIR ||
  '/Users/jonathantanner/Downloads/Complete_LinkedInDataExport_02-20-2026';

const MY_LINKEDIN_URL = 'https://www.linkedin.com/in/digitalventures';

const DRY_RUN             = process.argv.includes('--dry-run');
const SKIP_CONNECTIONS    = process.argv.includes('--skip-connections');
const SKIP_INVITATIONS    = process.argv.includes('--skip-invitations');
const SKIP_RECOMMENDATIONS = process.argv.includes('--skip-recommendations');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 30000,
  max: 5,
});

// ─── CSV Parser ───────────────────────────────────────────────────────────────

function parseCSV(filePath, skipRows = 0) {
  if (!fs.existsSync(filePath)) {
    console.warn(`  ⚠  File not found: ${filePath}`);
    return [];
  }

  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');

  // Skip header rows (LinkedIn adds notes at the top)
  const dataLines = lines.slice(skipRows);
  if (dataLines.length < 2) return [];

  const headers = parseCSVLine(dataLines[0]);
  const rows = [];

  for (let i = 1; i < dataLines.length; i++) {
    const line = dataLines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h.trim()] = (values[idx] || '').trim();
    });
    rows.push(row);
  }

  return rows;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

// ─── People Index (loaded once into memory) ──────────────────────────────────

let peopleByLinkedIn = new Map(); // normalised linkedin_url → person
let peopleByName     = new Map(); // "first last" lowercase → [person]

async function loadPeopleIndex() {
  console.log('  Loading people index into memory...');
  const res = await pool.query(
    `SELECT id, full_name, email, linkedin_url FROM people WHERE linkedin_url IS NOT NULL OR full_name IS NOT NULL`
  );

  for (const p of res.rows) {
    if (p.linkedin_url) {
      const key = normaliseUrl(p.linkedin_url);
      if (key) peopleByLinkedIn.set(key, p);
    }
    if (p.full_name) {
      const key = p.full_name.toLowerCase().trim();
      if (!peopleByName.has(key)) peopleByName.set(key, []);
      peopleByName.get(key).push(p);
    }
  }
  console.log(`  ✓ Indexed ${res.rows.length} people (${peopleByLinkedIn.size} with LinkedIn URLs)`);
}



function normaliseUrl(url) {
  if (!url) return null;
  return url.replace(/\/$/, '').toLowerCase().trim();
}

function findPersonByLinkedIn(linkedinUrl) {
  if (!linkedinUrl) return null;
  const norm = normaliseUrl(linkedinUrl);
  if (!norm) return null;
  // Try exact match first, then without trailing slash variants
  return peopleByLinkedIn.get(norm) ||
         peopleByLinkedIn.get(norm + '/') ||
         peopleByLinkedIn.get(norm.replace(/\/$/, '')) ||
         null;
}

function findPersonByName(firstName, lastName, company = null) {
  if (!firstName || !lastName) return null;
  const key = `${firstName} ${lastName}`.toLowerCase().trim();
  const matches = peopleByName.get(key);
  if (!matches || matches.length === 0) return null;
  if (matches.length === 1) return matches[0];
  // Multiple — return first (company matching would need DB query, skip for now)
  return matches[0];
}

async function getJonUserId() {
  const res = await pool.query(
    `SELECT id FROM users WHERE email = 'jon@mitchellake.com' OR role = 'admin' LIMIT 1`
  );
  return res.rows[0]?.id || null;
}

function parseLinkedInDate(dateStr) {
  if (!dateStr) return null;
  // Handles "18 Feb 2026", "02/02/10", "10/21/10, 05:12 AM", "2/4/26, 2:34 AM"
  try {
    // Try "DD Mon YYYY" format
    const longMatch = dateStr.match(/^(\d{1,2})\s+(\w+)\s+(\d{4})/);
    if (longMatch) return new Date(dateStr);

    // Try "M/D/YY, H:MM AM" format
    const shortMatch = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2}),?\s*/);
    if (shortMatch) {
      const [, m, d, y] = shortMatch;
      const year = parseInt(y) < 50 ? `20${y}` : `19${y}`;
      return new Date(`${year}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`);
    }

    return new Date(dateStr);
  } catch {
    return null;
  }
}

// ─── 1. CONNECTIONS ───────────────────────────────────────────────────────────

async function syncConnections(jonUserId) {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  📇 Connections.csv');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const filePath = path.join(EXPORT_DIR, 'Connections.csv');
  const rows = parseCSV(filePath, 3); // Skip 3 header/notes rows

  console.log(`  Loaded ${rows.length} connections`);

  const stats = {
    total: rows.length,
    emailsAdded: 0,
    linkedinUrlsAdded: 0,
    connectedOnWritten: 0,
    notMatched: 0,
  };

  // Collect all matched records first (in-memory, no DB)
  const emailUpdates    = []; // { id, email, linkedinUrl }
  const proximityRows   = []; // { personId, connectedOn }

  for (const row of rows) {
    const linkedinUrl = row['URL'];
    const email       = row['Email Address'];
    const firstName   = row['First Name'];
    const lastName    = row['Last Name'];
    const company     = row['Company'];
    const connectedOn = parseLinkedInDate(row['Connected On']);

    let person = findPersonByLinkedIn(linkedinUrl);
    if (!person) person = findPersonByName(firstName, lastName, company);

    if (!person) { stats.notMatched++; continue; }

    if (email && email.includes('@')) { emailUpdates.push({ id: person.id, email, linkedinUrl }); stats.emailsAdded++; }
    if (linkedinUrl) stats.linkedinUrlsAdded++;
    if (connectedOn) { proximityRows.push({ personId: person.id, connectedOn }); stats.connectedOnWritten++; }
  }

  if (DRY_RUN) return stats; // dry run stops here

  // Bulk update emails in batches of 200
  const BATCH = 200;
  for (let i = 0; i < emailUpdates.length; i += BATCH) {
    const batch = emailUpdates.slice(i, i + BATCH);
    const vals  = batch.flatMap(r => [r.id, r.email, r.linkedinUrl]);
    const placeholders = batch.map((_, j) => `($${j*3+1}, $${j*3+2}, $${j*3+3})`).join(',');
    await pool.query(
      `UPDATE people SET
         email        = COALESCE(NULLIF(people.email,''), v.email),
         linkedin_url = COALESCE(NULLIF(people.linkedin_url,''), v.linkedin_url),
         updated_at   = NOW()
       FROM (VALUES ${placeholders}) AS v(id, email, linkedin_url)
       WHERE people.id = v.id::uuid`,
      vals
    );
    process.stdout.write(`\r  Updating emails... ${Math.min(i+BATCH, emailUpdates.length)}/${emailUpdates.length}`);
  }
  if (emailUpdates.length) console.log();

  // Deduplicate proximity rows (same person can appear twice in connections export)
  const proxMap = new Map();
  for (const r of proximityRows) {
    const key = `${r.personId}_${jonUserId}`;
    if (!proxMap.has(key) || r.connectedOn < proxMap.get(key).connectedOn) proxMap.set(key, r);
  }
  const dedupedProximity = Array.from(proxMap.values());
  console.log(`  Deduped proximity: ${proximityRows.length} → ${dedupedProximity.length}`);

  // Bulk insert proximity in batches of 200
  for (let i = 0; i < dedupedProximity.length; i += BATCH) {
    const batch = dedupedProximity.slice(i, i + BATCH);
    const vals  = batch.flatMap(r => [r.personId, jonUserId, r.connectedOn]);
    const placeholders = batch.map((_, j) => `($${j*3+1}, $${j*3+2}, 'linkedin_connection', 0.3, 'linkedin', $${j*3+3})`).join(',');
    await pool.query(
      `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, connected_date)
       VALUES ${placeholders}
       ON CONFLICT (person_id, team_member_id, relationship_type)
       DO UPDATE SET connected_date = LEAST(team_proximity.connected_date, EXCLUDED.connected_date),
                     updated_at = NOW()`,
      vals
    );
    process.stdout.write(`\r  Updating proximity... ${Math.min(i+BATCH, dedupedProximity.length)}/${dedupedProximity.length}`);
  }
  if (proximityRows.length) console.log();

  console.log(`  ✓ Matched:             ${stats.total - stats.notMatched}`);
  console.log(`  ✓ Emails added:        ${stats.emailsAdded}`);
  console.log(`  ✓ LinkedIn URLs added: ${stats.linkedinUrlsAdded}`);
  console.log(`  ✓ Connected-on dates:  ${stats.connectedOnWritten}`);
  console.log(`  ✗ Not matched:         ${stats.notMatched}`);

  return stats;
}

// ─── 2. INVITATIONS ───────────────────────────────────────────────────────────

async function syncInvitations(jonUserId) {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  ✉️  Invitations.csv');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const filePath = path.join(EXPORT_DIR, 'Invitations.csv');
  const rows = parseCSV(filePath, 0);

  console.log(`  Loaded ${rows.length} invitations`);

  const stats = { total: rows.length, logged: 0, noMessage: 0, notMatched: 0 };

  for (const row of rows) {
    const message        = row['Message'];
    const direction      = row['Direction'];
    const sentAt         = parseLinkedInDate(row['Sent At']);
    const inviterUrl     = row['inviterProfileUrl'];
    const inviteeUrl     = row['inviteeProfileUrl'];

    // Determine the other party
    const isOutgoing = direction === 'OUTGOING';
    const otherUrl   = isOutgoing ? inviteeUrl : inviterUrl;
    const otherName  = isOutgoing ? row['To'] : row['From'];

    // Find the person
    let person = findPersonByLinkedIn(otherUrl);
    if (!person) {
      const parts = otherName.trim().split(' ');
      person = findPersonByName(parts[0], parts.slice(1).join(' '));
    }

    if (!person) {
      stats.notMatched++;
      continue;
    }

    // Build interaction summary
    const summary = message
      ? `LinkedIn connection ${isOutgoing ? 'request sent' : 'request received'}: "${message}"`
      : `LinkedIn connection ${isOutgoing ? 'request sent' : 'request received'} (no message)`;

    if (!message) stats.noMessage++;

    const externalId = `linkedin_invitation_${normaliseUrl(otherUrl)}_${sentAt?.toISOString() || row['Sent At']}`;

    if (!DRY_RUN) {
      await pool.query(
        `INSERT INTO interactions
           (person_id, user_id, interaction_type, interaction_at, summary, source, external_id, created_at)
         VALUES ($1, $2, 'linkedin_message', $3, $4, 'linkedin_invitation', $5, NOW())
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [person.id, jonUserId, sentAt || new Date(), summary, externalId]
      );
    }

    stats.logged++;
  }

  console.log(`  ✓ Logged:      ${stats.logged}`);
  console.log(`  - No message:  ${stats.noMessage}`);
  console.log(`  ✗ Not matched: ${stats.notMatched}`);

  return stats;
}

// ─── 3. RECOMMENDATIONS ───────────────────────────────────────────────────────

async function syncRecommendations(jonUserId) {
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  ⭐ Recommendations');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const stats = { received: 0, given: 0, notMatched: 0 };

  // Received
  const receivedPath = path.join(EXPORT_DIR, 'Recommendations_Received.csv');
  const received = parseCSV(receivedPath, 0);
  console.log(`  Loaded ${received.length} received recommendations`);

  for (const row of received) {
    const firstName    = row['First Name'];
    const lastName     = row['Last Name'];
    const company      = row['Company'];
    const text         = row['Text'];
    const creationDate = parseLinkedInDate(row['Creation Date']);

    if (!text) continue;

    const person = findPersonByName(firstName, lastName, company);
    if (!person) { stats.notMatched++; continue; }

    const summary = `LinkedIn recommendation received from ${firstName} ${lastName} (${row['Job Title']} at ${company}): "${text.substring(0, 500)}${text.length > 500 ? '...' : ''}"`;
    const externalId = `linkedin_rec_received_${person.id}_${creationDate?.toISOString() || row['Creation Date']}`;

    if (!DRY_RUN) {
      await pool.query(
        `INSERT INTO interactions
           (person_id, user_id, interaction_type, interaction_at, summary, source, external_id, created_at)
         VALUES ($1, $2, 'note', $3, $4, 'linkedin_recommendation', $5, NOW())
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [person.id, jonUserId, creationDate || new Date(), summary, externalId]
      );
    }
    stats.received++;
  }

  // Given
  const givenPath = path.join(EXPORT_DIR, 'Recommendations_Given.csv');
  const given = parseCSV(givenPath, 0);
  console.log(`  Loaded ${given.length} given recommendations`);

  for (const row of given) {
    const firstName    = row['First Name'];
    const lastName     = row['Last Name'];
    const company      = row['Company'];
    const text         = row['Text'];
    const creationDate = parseLinkedInDate(row['Creation Date']);

    if (!text) continue;

    const person = findPersonByName(firstName, lastName, company);
    if (!person) { stats.notMatched++; continue; }

    const summary = `LinkedIn recommendation given to ${firstName} ${lastName} (${row['Job Title']} at ${company}): "${text.substring(0, 500)}${text.length > 500 ? '...' : ''}"`;
    const externalId = `linkedin_rec_given_${person.id}_${creationDate?.toISOString() || row['Creation Date']}`;

    if (!DRY_RUN) {
      await pool.query(
        `INSERT INTO interactions
           (person_id, user_id, interaction_type, interaction_at, summary, source, external_id, created_at)
         VALUES ($1, $2, 'note', $3, $4, 'linkedin_recommendation', $5, NOW())
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [person.id, jonUserId, creationDate || new Date(), summary, externalId]
      );
    }
    stats.given++;
  }

  console.log(`  ✓ Received logged: ${stats.received}`);
  console.log(`  ✓ Given logged:    ${stats.given}`);
  console.log(`  ✗ Not matched:     ${stats.notMatched}`);

  return stats;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  MitchelLake — LinkedIn Enrichment Sync');
  if (DRY_RUN) console.log('  ⚠  DRY RUN — no data will be written');
  console.log('═══════════════════════════════════════════════════════\n');

  if (!fs.existsSync(EXPORT_DIR)) {
    console.error(`✗ Export directory not found: ${EXPORT_DIR}`);
    console.error('  Set LINKEDIN_EXPORT_DIR in .env or update the default path');
    process.exit(1);
  }

  const jonUserId = await getJonUserId();
  if (!jonUserId) {
    console.error('✗ Could not find Jon user in database');
    process.exit(1);
  }
  console.log(`  ✓ User: ${jonUserId}`);

  await loadPeopleIndex();

  const startTime = Date.now();
  const allStats  = {};

  if (!SKIP_CONNECTIONS)    allStats.connections    = await syncConnections(jonUserId);
  if (!SKIP_INVITATIONS)    allStats.invitations    = await syncInvitations(jonUserId);
  if (!SKIP_RECOMMENDATIONS) allStats.recommendations = await syncRecommendations(jonUserId);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  ✅ LINKEDIN ENRICHMENT COMPLETE');
  console.log('═══════════════════════════════════════════════════════');

  if (allStats.connections) {
    console.log(`  Connections processed:    ${allStats.connections.total}`);
    console.log(`  Emails added:             ${allStats.connections.emailsAdded}`);
    console.log(`  LinkedIn URLs added:      ${allStats.connections.linkedinUrlsAdded}`);
    console.log(`  Connected-on dates:       ${allStats.connections.connectedOnWritten}`);
  }
  if (allStats.invitations) {
    console.log(`  Invitations logged:       ${allStats.invitations.logged}`);
  }
  if (allStats.recommendations) {
    console.log(`  Recommendations received: ${allStats.recommendations.received}`);
    console.log(`  Recommendations given:    ${allStats.recommendations.given}`);
  }

  console.log(`  Elapsed: ${elapsed}s`);
  if (DRY_RUN) console.log('\n  ⚠  Re-run without --dry-run to write data');
  console.log('═══════════════════════════════════════════════════════\n');

  await pool.end();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});