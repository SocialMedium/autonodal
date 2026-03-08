#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/ingest_linkedin_messages.js
//
// Processes LinkedIn data export files to seed relationship baselines
// and interaction history into the MitchelLake Signal Intelligence Platform.
//
// Usage:
//   node scripts/ingest_linkedin_messages.js
//   node scripts/ingest_linkedin_messages.js --dry-run
//   node scripts/ingest_linkedin_messages.js --skip-invitations
//   node scripts/ingest_linkedin_messages.js --skip-profile
//
// Input files (copy from your LinkedIn export to data/linkedin/):
//   messages.csv     ← HIGHEST PRIORITY
//   invitations.csv  ← Secondary
//   profile.csv      ← JT's own profile baseline
//   positions.csv    ← JT's career history
//
// DO NOT re-run on connections.csv — already imported.
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const fs      = require('fs');
const path    = require('path');
const crypto  = require('crypto');
const { Pool } = require('pg');
const { runJob, logProgress, withRetry, sleep } = require('../lib/job_runner');

// ─── CLI flags ──────────────────────────────────────────────────────────────
const DRY_RUN          = process.argv.includes('--dry-run');
const SKIP_INVITATIONS = process.argv.includes('--skip-invitations');
const SKIP_PROFILE     = process.argv.includes('--skip-profile');

// ─── Config ─────────────────────────────────────────────────────────────────
const LINKEDIN_DIR  = process.env.LINKEDIN_DUMP_PATH || './data/linkedin';
const BATCH_SIZE    = 100;
const UNMATCHED_OUT = './data/unmatched_profiles.csv';

// ─── Colours ────────────────────────────────────────────────────────────────
const c = {
  green:  (s) => `\x1b[32m${s}\x1b[0m`,
  red:    (s) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s) => `\x1b[33m${s}\x1b[0m`,
  blue:   (s) => `\x1b[34m${s}\x1b[0m`,
  dim:    (s) => `\x1b[2m${s}\x1b[0m`,
};

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// ═══════════════════════════════════════════════════════════════════════════
// CSV PARSER
// Handles LinkedIn's CSV format which uses quoted fields with embedded commas
// ═══════════════════════════════════════════════════════════════════════════

function parseCSV(filePath) {
  if (!fs.existsSync(filePath)) return null;

  const raw = fs.readFileSync(filePath, 'utf8');
  const lines = raw.split('\n');
  if (lines.length < 2) return [];

  // Find header line — LinkedIn sometimes prepends blank lines or metadata rows
  let headerIdx = 0;
  for (let i = 0; i < Math.min(10, lines.length); i++) {
    if (lines[i].includes(',') && lines[i].trim().length > 0) {
      headerIdx = i;
      break;
    }
  }

  const headers = parseCSVLine(lines[headerIdx]).map(h => h.trim());
  const rows = [];

  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = (values[idx] || '').trim();
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
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

function md5(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  try {
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d;
  } catch {
    return null;
  }
}

// Normalise a LinkedIn URL to a consistent format for matching
function normaliseLinkedInUrl(url) {
  if (!url) return null;
  return url.toLowerCase()
    .replace(/^https?:\/\/(www\.)?linkedin\.com/, '')
    .replace(/\/$/, '')
    .trim();
}

// Strip common suffixes that don't appear in the database
function normaliseName(name) {
  return name
    .replace(/,?\s+(MBA|PhD|CPA|CFA|MD|JD|MSc|BSc|BA|MA|BEng|MEng|GAICD|FAICD|MAICD|AM|OAM|AO)\b/gi, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function daysBetween(date1, date2) {
  return Math.abs((date1 - date2) / (1000 * 60 * 60 * 24));
}

// ═══════════════════════════════════════════════════════════════════════════
// PERSON MATCHING
// ═══════════════════════════════════════════════════════════════════════════

// Cache to avoid repeated DB lookups for the same profile
const personCache = new Map();

async function findOrCreatePerson(pool, linkedinUrl, fullName, dryRun) {
  const cacheKey = linkedinUrl || fullName;
  if (personCache.has(cacheKey)) return personCache.get(cacheKey);

  // 1. Match by LinkedIn URL (normalised)
  if (linkedinUrl) {
    const normUrl = normaliseLinkedInUrl(linkedinUrl);
    const { rows } = await pool.query(
      `SELECT id, full_name, linkedin_url FROM people
       WHERE lower(linkedin_url) LIKE $1
       LIMIT 1`,
      [`%${normUrl}%`]
    );
    if (rows.length > 0) {
      const result = { id: rows[0].id, matched: true, method: 'linkedin_url', created: false };
      personCache.set(cacheKey, result);
      return result;
    }
  }

  // 2. Match by normalised full name
  if (fullName) {
    const normName = normaliseName(fullName);
    const { rows } = await pool.query(
      `SELECT id, full_name FROM people
       WHERE lower(full_name) = $1
       LIMIT 1`,
      [normName]
    );
    if (rows.length > 0) {
      const result = { id: rows[0].id, matched: true, method: 'name', created: false };
      personCache.set(cacheKey, result);
      return result;
    }
  }

  // 3. No match — create stub or return unmatched
  if (!dryRun) {
    const nameParts = (fullName || '').split(' ');
    const firstName = nameParts[0] || '';
    const lastName  = nameParts.slice(1).join(' ') || '';

    const { rows } = await pool.query(
      `INSERT INTO people (full_name, first_name, last_name, linkedin_url, source)
       VALUES ($1, $2, $3, $4, 'linkedin_import_pending')
       ON CONFLICT DO NOTHING
       RETURNING id`,
      [fullName, firstName, lastName, linkedinUrl]
    );

    if (rows.length > 0) {
      const result = { id: rows[0].id, matched: false, method: 'created_stub', created: true };
      personCache.set(cacheKey, result);
      return result;
    }
  }

  const result = { id: null, matched: false, method: 'unmatched', created: false };
  personCache.set(cacheKey, result);
  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// RELATIONSHIP STRENGTH CALCULATOR
// ═══════════════════════════════════════════════════════════════════════════

function computeRelationshipStrength(existing, stats) {
  let strength = existing ?? 0.10;

  // +0.10 per thread, max +0.40
  const threadBonus = Math.min(stats.threadCount * 0.10, 0.40);
  strength += threadBonus;

  // +0.20 if they ever replied to us
  if (stats.hasReplied) strength += 0.20;

  // Recency bonuses
  if (stats.lastMessageDate) {
    const daysSince = daysBetween(new Date(), stats.lastMessageDate);
    if (daysSince <= 30)  strength += 0.10;
    else if (daysSince <= 90) strength += 0.05;
  }

  return Math.min(strength, 1.00);
}

// ═══════════════════════════════════════════════════════════════════════════
// UPSERT TEAM PROXIMITY
// ═══════════════════════════════════════════════════════════════════════════

async function upsertTeamProximity(pool, personId, userId, proximityType, strength, context, lastContactDate, source, dryRun) {
  if (dryRun) return;

  await pool.query(
    `INSERT INTO team_proximity
       (person_id, team_member_id, relationship_type, relationship_strength,
        notes, last_interaction_date, source)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
       relationship_strength  = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
       notes                  = COALESCE(EXCLUDED.notes, team_proximity.notes),
       last_interaction_date  = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
       updated_at             = NOW()`,
    [personId, userId, proximityType, strength, context, lastContactDate, source]
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// PASS 1 + 2: PROCESS MESSAGES
// ═══════════════════════════════════════════════════════════════════════════

async function processMessages(pool, jtUserId, jtLinkedInUrl, jtName, dryRun) {
  const filePath = path.join(LINKEDIN_DIR, 'messages.csv');
  const rows = parseCSV(filePath);

  if (!rows) {
    console.log(c.yellow(`  ⚠ messages.csv not found at ${filePath} — skipping`));
    return { messagesProcessed: 0, interactionsCreated: 0, peopleMatched: 0, peopleCreated: 0, threadsFound: 0, relationshipsUpdated: 0, unmatched: [] };
  }

  console.log(c.blue(`  Found ${rows.length} messages`));

  // ── Group by person to track stats for relationship scoring ────────────
  const personStats = new Map(); // cacheKey → { threadIds, messageCount, hasReplied, lastMessageDate, linkedinUrl, fullName }
  const unmatched   = [];

  let interactionsCreated = 0;
  let peopleMatched       = 0;
  let peopleCreated       = 0;
  let i                   = 0;

  for (const row of rows) {
    i++;
    logProgress('messages', i, rows.length, `processing messages`);

    const convId      = row['CONVERSATION ID'] || row['Conversation ID'] || '';
    const from        = row['FROM']            || row['From']            || '';
    const senderUrl   = row['SENDER PROFILE URL'] || row['Sender Profile Url'] || '';
    const content     = row['CONTENT']         || row['Content']         || '';
    const dateStr     = row['DATE']            || row['Date']            || '';
    const subject     = row['SUBJECT']         || row['Subject']         || '';

    const interactionAt = parseDate(dateStr);
    if (!interactionAt) continue;

    // Is this message FROM JT (outbound)?
    const isOutbound = (
      (jtLinkedInUrl && senderUrl && normaliseLinkedInUrl(senderUrl) === normaliseLinkedInUrl(jtLinkedInUrl)) ||
      (jtName && normaliseName(from) === normaliseName(jtName))
    );

    const direction = isOutbound ? 'outbound' : 'inbound';

    // The "other" person is the non-JT participant
    const otherUrl  = isOutbound ? '' : senderUrl;
    const otherName = isOutbound ? '' : from;

    // For outbound messages we can't easily identify the recipient from this row alone
    // so we skip person matching — relationship stats are built from inbound messages
    if (isOutbound) {
      // Still create the interaction against the conversation
      // We'll link it properly in the relationship pass
      continue;
    }

    const cacheKey = otherUrl || otherName;
    if (!cacheKey) continue;

    // ── Match person ───────────────────────────────────────────────────────
    const person = await findOrCreatePerson(pool, otherUrl, otherName, dryRun);

    if (!person.id) {
      unmatched.push({ linkedinUrl: otherUrl, name: otherName, convId, date: dateStr, sample: content.slice(0, 100) });
      continue;
    }

    if (person.created) peopleCreated++;
    else if (person.matched) peopleMatched++;

    // ── Track stats for this person ────────────────────────────────────────
    if (!personStats.has(cacheKey)) {
      personStats.set(cacheKey, {
        personId:        person.id,
        linkedinUrl:     otherUrl,
        fullName:        otherName,
        threadIds:       new Set(),
        messageCount:    0,
        hasReplied:      false,
        lastMessageDate: null,
      });
    }

    const stats = personStats.get(cacheKey);
    stats.threadIds.add(convId);
    stats.messageCount++;
    stats.hasReplied = true; // inbound = they replied
    if (!stats.lastMessageDate || interactionAt > stats.lastMessageDate) {
      stats.lastMessageDate = interactionAt;
    }

    // ── Log interaction ────────────────────────────────────────────────────
    const externalId = md5(`${convId}|${dateStr}|${from}`);

    if (!dryRun) {
      const { rowCount } = await pool.query(
        `INSERT INTO interactions
           (person_id, user_id, interaction_type, direction, subject, summary,
            channel, source, external_id, interaction_at)
         VALUES ($1, $2, 'linkedin_message', $3, $4, $5, 'linkedin', 'linkedin_import', $6, $7)
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [
          person.id,
          jtUserId,
          direction,
          subject.slice(0, 500) || null,
          content.slice(0, 500) || null,
          externalId,
          interactionAt,
        ]
      );
      if (rowCount > 0) interactionsCreated++;
    } else {
      interactionsCreated++; // dry-run count
    }

    // Batch commit
    if (i % BATCH_SIZE === 0 && !dryRun) {
      // pg auto-commits each query — nothing needed here, but good checkpoint for logging
    }
  }

  // ── Pass 3: Upsert relationship scores ────────────────────────────────────
  console.log(c.yellow(`\n  ▶ Computing relationship scores for ${personStats.size} contacts...`));

  let scoreCount = 0;
  for (const [, stats] of personStats) {
    const strength = computeRelationshipStrength(null, {
      threadCount:     stats.threadIds.size,
      hasReplied:      stats.hasReplied,
      lastMessageDate: stats.lastMessageDate,
    });

    const context = `${stats.threadIds.size} LinkedIn thread(s), ${stats.messageCount} message(s)`;

    await upsertTeamProximity(
      pool,
      stats.personId,
      jtUserId,
      'linkedin_message',
      strength,
      context,
      stats.lastMessageDate ? stats.lastMessageDate.toISOString().split('T')[0] : null,
      'linkedin_import',
      dryRun
    );

    scoreCount++;
    logProgress('proximity', scoreCount, personStats.size, 'updating relationship scores');
  }

  return {
    messagesProcessed: rows.length,
    interactionsCreated,
    peopleMatched,
    peopleCreated,
    threadsFound: [...personStats.values()].reduce((sum, s) => sum + s.threadIds.size, 0),
    relationshipsUpdated: scoreCount,
    unmatched,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// INVITATIONS
// ═══════════════════════════════════════════════════════════════════════════

async function processInvitations(pool, jtUserId, jtName, dryRun) {
  const filePath = path.join(LINKEDIN_DIR, 'invitations.csv');
  const rows = parseCSV(filePath);

  if (!rows) {
    console.log(c.yellow(`  ⚠ invitations.csv not found at ${filePath} — skipping`));
    return { processed: 0, withMessage: 0 };
  }

  console.log(c.blue(`  Found ${rows.length} invitations`));

  let processed  = 0;
  let withMessage = 0;

  for (const row of rows) {
    const from      = row['From']      || '';
    const to        = row['To']        || '';
    const sentAt    = row['Sent At']   || '';
    const message   = row['Message']   || '';
    const direction = row['Direction'] || '';

    const interactionAt = parseDate(sentAt);
    if (!interactionAt) continue;

    // Determine direction
    const isOutbound = direction.toLowerCase() === 'sent' ||
                       normaliseName(from) === normaliseName(jtName);

    const otherName = isOutbound ? to : from;
    const dir       = isOutbound ? 'outbound' : 'inbound';

    const person = await findOrCreatePerson(pool, null, otherName, dryRun);
    if (!person.id) continue;

    const hasMsg = message.trim().length > 0;
    if (hasMsg) withMessage++;

    const externalId = md5(`${from}|${to}|${sentAt}`);

    if (!dryRun) {
      await pool.query(
        `INSERT INTO interactions
           (person_id, user_id, interaction_type, direction, summary,
            channel, source, external_id, interaction_at)
         VALUES ($1, $2, 'linkedin_invite', $3, $4, 'linkedin', 'linkedin_import', $5, $6)
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [
          person.id,
          jtUserId,
          dir,
          hasMsg ? message.slice(0, 500) : null,
          externalId,
          interactionAt,
        ]
      );

      // Invitation with message = small relationship boost
      if (hasMsg) {
        await upsertTeamProximity(
          pool, person.id, jtUserId, 'linkedin_message',
          0.15, 'LinkedIn invitation with personal note', 
          interactionAt.toISOString().split('T')[0],
          'linkedin_import', dryRun
        );
      }
    }

    processed++;
  }

  return { processed, withMessage };
}

// ═══════════════════════════════════════════════════════════════════════════
// PROFILE + POSITIONS
// ═══════════════════════════════════════════════════════════════════════════

async function processProfile(pool, jtUserId, dryRun) {
  const profilePath   = path.join(LINKEDIN_DIR, 'Profile.csv');
  const positionsPath = path.join(LINKEDIN_DIR, 'Positions.csv');

  // Try case-insensitive fallbacks
  const profileFile   = [profilePath, path.join(LINKEDIN_DIR, 'profile.csv')].find(fs.existsSync);
  const positionsFile = [positionsPath, path.join(LINKEDIN_DIR, 'positions.csv')].find(fs.existsSync);

  if (!profileFile && !positionsFile) {
    console.log(c.yellow('  ⚠ profile.csv and positions.csv not found — skipping'));
    return;
  }

  let bio = null;
  let currentTitle = null;
  let currentCompany = null;
  let careerHistory = [];

  if (profileFile) {
    const rows = parseCSV(profileFile);
    if (rows && rows.length > 0) {
      const p = rows[0];
      bio          = p['Summary'] || p['Headline'] || null;
      currentTitle = p['Headline'] || null;
    }
  }

  if (positionsFile) {
    const rows = parseCSV(positionsFile) || [];
    careerHistory = rows.map(r => ({
      title:   r['Title']        || r['Position Title'] || '',
      company: r['Company Name'] || r['Company']        || '',
      start:   r['Started On']   || r['Start Date']     || '',
      end:     r['Finished On']  || r['End Date']       || '',
      current: (r['Finished On'] || r['End Date'] || '').trim() === '',
    }));

    const current = careerHistory.find(r => r.current);
    if (current) {
      currentTitle   = currentTitle || current.title;
      currentCompany = current.company;
    }
  }

  if (!dryRun) {
    // Get JT's people record (match by user_id linkage or first person created from linkedin source)
    const { rows } = await pool.query(
      `SELECT id FROM people WHERE source = 'ezekia' ORDER BY created_at ASC LIMIT 1`
    );

    if (rows.length > 0) {
      await pool.query(
        `UPDATE people SET
           bio                  = COALESCE(bio, $2),
           current_title        = COALESCE(current_title, $3),
           current_company_name = COALESCE(current_company_name, $4),
           career_history       = COALESCE(career_history, $5)
         WHERE id = $1`,
        [rows[0].id, bio, currentTitle, currentCompany, JSON.stringify(careerHistory)]
      );
      console.log(c.green('  ✓ JT profile baseline updated'));
    }
  } else {
    console.log(c.dim(`  [dry-run] Would update JT profile: title="${currentTitle}", company="${currentCompany}", ${careerHistory.length} positions`));
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// WRITE UNMATCHED PROFILES CSV
// ═══════════════════════════════════════════════════════════════════════════

function writeUnmatched(unmatched) {
  if (unmatched.length === 0) return;

  const dir = path.dirname(UNMATCHED_OUT);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const header = 'linkedin_url,name,conversation_id,date,sample_message\n';
  const lines  = unmatched.map(u =>
    `"${u.linkedinUrl}","${u.name}","${u.convId}","${u.date}","${u.sample.replace(/"/g, '""')}"`
  );

  fs.writeFileSync(UNMATCHED_OUT, header + lines.join('\n'));
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('');
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log(c.blue('  MitchelLake — LinkedIn Message Ingestion            '));
  if (DRY_RUN) console.log(c.yellow('  ⚠  DRY RUN — no data will be written               '));
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log('');

  // ── Validate environment ─────────────────────────────────────────────────
  if (!process.env.DATABASE_URL) {
    console.log(c.red('✗ DATABASE_URL not set in .env'));
    process.exit(1);
  }

  if (!fs.existsSync(LINKEDIN_DIR)) {
    console.log(c.red(`✗ LinkedIn data directory not found: ${LINKEDIN_DIR}`));
    console.log('  Create it and copy your LinkedIn export files there:');
    console.log('  mkdir -p data/linkedin && cp ~/Downloads/messages.csv data/linkedin/');
    process.exit(1);
  }

  // ── Get JT's user ID ─────────────────────────────────────────────────────
  const { rows: userRows } = await pool.query(
    `SELECT id, email, name FROM users ORDER BY created_at ASC LIMIT 1`
  );
  if (userRows.length === 0) {
    console.log(c.red('✗ No users found in database'));
    process.exit(1);
  }

  const jtUserId = userRows[0].id;
  const jtName   = userRows[0].name || userRows[0].email || '';
  console.log(c.green(`✓ Running as: ${jtName} (${jtUserId})`));

  // Try to get JT's LinkedIn URL from people table
  const { rows: jtPersonRows } = await pool.query(
    `SELECT linkedin_url FROM people WHERE source = 'ezekia' AND linkedin_url IS NOT NULL LIMIT 1`
  );
  const jtLinkedInUrl = jtPersonRows[0]?.linkedin_url || null;

  // ── Process files ────────────────────────────────────────────────────────
  let msgStats  = { messagesProcessed: 0, interactionsCreated: 0, peopleMatched: 0, peopleCreated: 0, threadsFound: 0, relationshipsUpdated: 0, unmatched: [] };
  let invStats  = { processed: 0, withMessage: 0 };

  // Messages
  console.log(c.yellow('\n▶ Processing messages.csv...'));
  msgStats = await processMessages(pool, jtUserId, jtLinkedInUrl, jtName, DRY_RUN);
  writeUnmatched(msgStats.unmatched);

  // Invitations
  if (!SKIP_INVITATIONS) {
    console.log(c.yellow('\n▶ Processing invitations.csv...'));
    invStats = await processInvitations(pool, jtUserId, jtName, DRY_RUN);
  }

  // Profile + positions
  if (!SKIP_PROFILE) {
    console.log(c.yellow('\n▶ Processing profile.csv + positions.csv...'));
    await processProfile(pool, jtUserId, DRY_RUN);
  }

  // ── Summary ──────────────────────────────────────────────────────────────
  console.log('');
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log(c.blue('  📊 LINKEDIN MESSAGE INGESTION COMPLETE'));
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log(`  Messages processed:     ${msgStats.messagesProcessed.toLocaleString()}`);
  console.log(`  People matched:         ${msgStats.peopleMatched.toLocaleString()}`);
  console.log(`  People created (stub):  ${msgStats.peopleCreated.toLocaleString()}`);
  console.log(`  Interactions logged:    ${msgStats.interactionsCreated.toLocaleString()}`);
  console.log(`  Threads found:          ${msgStats.threadsFound.toLocaleString()}`);
  console.log(`  Relationship scores:    ${msgStats.relationshipsUpdated.toLocaleString()}`);
  console.log('');
  if (!SKIP_INVITATIONS) {
    console.log(`  Invitations processed:  ${invStats.processed.toLocaleString()}`);
    console.log(`  With message:           ${invStats.withMessage.toLocaleString()}`);
    console.log('');
  }
  if (msgStats.unmatched.length > 0) {
    console.log(c.yellow(`  Unmatched profiles:     ${msgStats.unmatched.length} → ${UNMATCHED_OUT}`));
  } else {
    console.log(c.green('  Unmatched profiles:     0 ✅'));
  }
  console.log(c.blue('═══════════════════════════════════════════════════════'));

  if (DRY_RUN) {
    console.log(c.yellow('\n  ⚠  DRY RUN complete — re-run without --dry-run to write data'));
  }

  await pool.end();
}

main().catch(err => {
  console.error(c.red(`\nFatal error: ${err.message}`));
  console.error(err);
  process.exit(1);
});