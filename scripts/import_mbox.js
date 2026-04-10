#!/usr/bin/env node

/**
 * import_mbox.js — Import a Google Takeout MBOX file into MitchelLake platform
 *
 * Usage:
 *   node scripts/import_mbox.js --file ~/Downloads/Sent.mbox --user-id UUID --tenant-id UUID
 *   node scripts/import_mbox.js --file ~/Downloads/All\ mail.mbox --user-id UUID --tenant-id UUID --dry-run
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { Pool } = require('pg');

// ============================================================================
// CLI ARGUMENT PARSING
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const parsed = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--file' && args[i + 1]) parsed.file = args[++i];
    else if (args[i] === '--user-id' && args[i + 1]) parsed.userId = args[++i];
    else if (args[i] === '--tenant-id' && args[i + 1]) parsed.tenantId = args[++i];
    else if (args[i] === '--dry-run') parsed.dryRun = true;
    else if (args[i] === '--help' || args[i] === '-h') parsed.help = true;
  }
  return parsed;
}

const args = parseArgs();

if (args.help || !args.file || !args.userId || !args.tenantId) {
  console.log(`
Usage: node scripts/import_mbox.js --file <path> --user-id <uuid> --tenant-id <uuid> [--dry-run]

Options:
  --file        Path to MBOX file (required)
  --user-id     UUID of the importing user (required)
  --tenant-id   UUID of the tenant (required)
  --dry-run     Parse and classify without writing to DB
  --help        Show this help
`);
  process.exit(args.help ? 0 : 1);
}

const MBOX_PATH = path.resolve(args.file);
const USER_ID = args.userId;
const TENANT_ID = args.tenantId;
const DRY_RUN = !!args.dryRun;

if (!fs.existsSync(MBOX_PATH)) {
  console.error(`File not found: ${MBOX_PATH}`);
  process.exit(1);
}

// ============================================================================
// DATABASE
// ============================================================================

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
  max: 10,
});

// ============================================================================
// EMAIL CLASSIFICATION — skip automated / bulk emails
// ============================================================================

const NOREPLY_PATTERNS = [
  'noreply', 'no-reply', 'no_reply', 'donotreply', 'do-not-reply',
  'notifications', 'mailer-daemon', 'postmaster', 'bounce', 'auto-confirm',
];

const AUTOMATED_DOMAINS = [
  'github.com', 'gitlab.com', 'bitbucket.org',
  'jira.atlassian.com', 'atlassian.net', 'atlassian.com',
  'slack.com', 'slackbot.com',
  'calendar-notification@google.com', 'calendar.google.com',
  'notifications@linkedin.com', 'linkedin.com/notifications',
  'facebookmail.com', 'amazonses.com',
  'sendgrid.net', 'mailchimp.com', 'mandrillapp.com',
  'intercom.io', 'zendesk.com', 'freshdesk.com',
  'stripe.com', 'paypal.com',
];

const AUTOMATED_SUBJECT_PATTERNS = [
  /^\[.*?\]\s/,                          // [JIRA-123] style
  /^out of office/i,
  /^automatic reply/i,
  /^auto:/i,
  /^undeliverable/i,
  /^returned mail/i,
  /^delivery (status )?notification/i,
  /^mail delivery (failed|subsystem)/i,
  /^(new|updated?) pull request/i,
  /^re:\s*\[.*?\]\s/,                    // Re: [JIRA-123] style
];

function extractEmailAddress(raw) {
  if (!raw) return null;
  const match = raw.match(/<([^>]+)>/);
  return (match ? match[1] : raw).trim().toLowerCase();
}

function extractDisplayName(raw) {
  if (!raw) return null;
  const match = raw.match(/^"?([^"<]+)"?\s*</);
  if (match) return match[1].trim();
  // If no angle brackets, it might just be an email
  if (!raw.includes('<')) return null;
  return null;
}

function parseAddressList(raw) {
  if (!raw) return [];
  // Split on commas but respect quoted strings and angle brackets
  const addresses = [];
  let depth = 0;
  let current = '';
  for (const ch of raw) {
    if (ch === '<') depth++;
    else if (ch === '>') depth--;
    else if (ch === ',' && depth === 0) {
      if (current.trim()) addresses.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current.trim()) addresses.push(current.trim());
  return addresses;
}

function isAutomatedAddress(email) {
  if (!email) return true;
  const lower = email.toLowerCase();
  const localPart = lower.split('@')[0];
  const domain = lower.split('@')[1] || '';

  for (const pattern of NOREPLY_PATTERNS) {
    if (localPart.includes(pattern)) return true;
  }
  for (const autoDomain of AUTOMATED_DOMAINS) {
    if (domain === autoDomain || lower.includes(autoDomain)) return true;
  }
  return false;
}

function isAutomatedSubject(subject) {
  if (!subject) return false;
  for (const pattern of AUTOMATED_SUBJECT_PATTERNS) {
    if (pattern.test(subject)) return true;
  }
  return false;
}

function classifyEmail(headers) {
  const from = extractEmailAddress(headers['from']);

  // Only skip truly automated system emails
  if (isAutomatedAddress(from)) return { skip: true, reason: 'automated_sender' };
  if (isAutomatedSubject(headers['subject'])) return { skip: true, reason: 'automated_subject' };

  // Classify but KEEP newsletters — valuable for company intelligence
  if (headers['list-unsubscribe'] || headers['list-id'] || headers['precedence'] === 'bulk' || headers['precedence'] === 'list') {
    return { skip: false, type: 'newsletter', reason: null };
  }

  // Tag mass emails but still import (lower proximity weight)
  const toAddrs = parseAddressList(headers['to'] || '');
  const ccAddrs = parseAddressList(headers['cc'] || '');
  if (toAddrs.length + ccAddrs.length > 10) {
    return { skip: false, type: 'mass', reason: null };
  }

  return { skip: false, type: 'personal', reason: null };
}

// Legacy compat
function shouldSkipEmail(headers) {
  const c = classifyEmail(headers);
  return c.skip ? c.reason : null;
}

// ============================================================================
// MBOX STREAMING PARSER
// ============================================================================

async function* parseMbox(filePath) {
  const stream = fs.createReadStream(filePath, { encoding: 'utf-8', highWaterMark: 64 * 1024 });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = {};
  let headerKey = null;
  let inHeaders = false;
  let bodyLines = [];
  let messageCount = 0;
  let hasMessage = false;

  for await (const line of rl) {
    // "From " at start of line = new message boundary (MBOX standard)
    if (line.startsWith('From ') && (line.length > 5) && /^From \S/.test(line)) {
      // Yield previous message if any
      if (hasMessage) {
        const body = bodyLines.join('\n');
        yield { headers, body: body.slice(0, 2000) }; // keep limited body for snippet
        messageCount++;
      }
      // Start new message
      headers = {};
      headerKey = null;
      inHeaders = true;
      bodyLines = [];
      hasMessage = true;
      continue;
    }

    if (inHeaders) {
      if (line === '') {
        // End of headers, start of body
        inHeaders = false;
        continue;
      }
      // Continuation header (starts with whitespace)
      if (/^\s+/.test(line) && headerKey) {
        headers[headerKey] += ' ' + line.trim();
        continue;
      }
      // New header
      const colonIdx = line.indexOf(':');
      if (colonIdx > 0) {
        headerKey = line.slice(0, colonIdx).toLowerCase().trim();
        const value = line.slice(colonIdx + 1).trim();
        // Some headers like Received can appear multiple times; we only keep the first
        if (!headers[headerKey]) {
          headers[headerKey] = value;
        }
      }
    } else {
      // Body — only collect up to ~2000 chars for snippet extraction
      if (bodyLines.join('\n').length < 2500) {
        bodyLines.push(line);
      }
    }
  }

  // Yield last message
  if (hasMessage) {
    const body = bodyLines.join('\n');
    yield { headers, body: body.slice(0, 2000) };
  }
}

// ============================================================================
// EXTRACT TEXT BODY SNIPPET
// ============================================================================

function extractTextSnippet(body, maxLen = 500) {
  if (!body) return '';
  // Strip MIME boundaries and HTML if present
  let text = body;

  // If it looks like MIME multipart, try to extract text/plain part
  if (text.includes('Content-Type:')) {
    const textMatch = text.match(/Content-Type:\s*text\/plain[^\n]*\n(?:Content-Transfer-Encoding:[^\n]*\n)?(?:Content-Disposition:[^\n]*\n)?\n([\s\S]*?)(?:\n--|\n\nContent-Type:|$)/i);
    if (textMatch) {
      text = textMatch[1];
    }
  }

  // Strip HTML tags if still present
  text = text.replace(/<[^>]+>/g, ' ');
  // Decode common HTML entities
  text = text.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&nbsp;/g, ' ');
  // Collapse whitespace
  text = text.replace(/\s+/g, ' ').trim();

  return text.slice(0, maxLen);
}

// ============================================================================
// PERSON MATCHING
// ============================================================================

let personEmailCache = null; // email_lower -> person_id
let personNameCache = null;  // normalized_name -> person_id

async function loadPersonCaches() {
  console.log('Loading people records for matching...');
  const { rows } = await pool.query(
    `SELECT id, full_name, email FROM people WHERE tenant_id = $1 AND email IS NOT NULL AND email != ''`,
    [TENANT_ID]
  );

  personEmailCache = new Map();
  personNameCache = new Map();

  for (const row of rows) {
    if (row.email) {
      personEmailCache.set(row.email.toLowerCase().trim(), row.id);
    }
    if (row.full_name) {
      const normalized = row.full_name.toLowerCase().trim().replace(/\s+/g, ' ');
      personNameCache.set(normalized, row.id);
    }
  }

  console.log(`  Loaded ${personEmailCache.size} email mappings, ${personNameCache.size} name mappings`);
}

function matchPerson(emailAddr, displayName) {
  if (!personEmailCache) return null;

  // Try email match first
  if (emailAddr) {
    const pid = personEmailCache.get(emailAddr.toLowerCase().trim());
    if (pid) return pid;
  }

  // Try name match
  if (displayName) {
    const normalized = displayName.toLowerCase().trim().replace(/\s+/g, ' ');
    const pid = personNameCache.get(normalized);
    if (pid) return pid;
  }

  return null;
}

// ============================================================================
// GET USER EMAIL (to determine direction)
// ============================================================================

async function getUserEmail() {
  const { rows } = await pool.query(
    `SELECT email, full_name FROM users WHERE id = $1`,
    [USER_ID]
  );
  if (rows.length === 0) {
    console.error(`User not found: ${USER_ID}`);
    process.exit(1);
  }
  console.log(`User: ${rows[0].full_name} <${rows[0].email}>`);
  return rows[0].email?.toLowerCase();
}

// ============================================================================
// DEDUP CHECK — batch load existing message IDs
// ============================================================================

async function loadExistingMessageIds() {
  console.log('Loading existing email message IDs for dedup...');
  const { rows } = await pool.query(
    `SELECT email_message_id FROM interactions
     WHERE user_id = $1 AND tenant_id = $2 AND email_message_id IS NOT NULL`,
    [USER_ID, TENANT_ID]
  );
  const set = new Set(rows.map(r => r.email_message_id));
  console.log(`  Found ${set.size} existing message IDs`);
  return set;
}

// ============================================================================
// MAIN IMPORT
// ============================================================================

async function main() {
  const startTime = Date.now();
  console.log('='.repeat(60));
  console.log('MBOX IMPORT — MitchelLake Platform');
  console.log('='.repeat(60));
  console.log(`File:      ${MBOX_PATH}`);
  console.log(`User ID:   ${USER_ID}`);
  console.log(`Tenant ID: ${TENANT_ID}`);
  console.log(`Dry run:   ${DRY_RUN}`);
  console.log(`File size: ${(fs.statSync(MBOX_PATH).size / (1024 * 1024)).toFixed(1)} MB`);
  console.log('='.repeat(60));

  let userEmail;
  let existingMessageIds;

  if (!DRY_RUN) {
    userEmail = await getUserEmail();
    await loadPersonCaches();
    existingMessageIds = await loadExistingMessageIds();
  } else {
    // In dry-run, still try to load user email for direction classification
    try {
      userEmail = await getUserEmail();
    } catch { userEmail = null; }
    existingMessageIds = new Set();
  }

  // Stats
  const stats = {
    total: 0,
    skipped: { newsletter: 0, automated_sender: 0, automated_subject: 0, mass_email: 0, no_date: 0, duplicate: 0, no_headers: 0 },
    kept: 0,
    matched: 0,
    unmatched: 0,
    created: 0,
    errors: 0,
  };

  // Batch for inserts
  const BATCH_SIZE = 100;
  let batch = [];
  // Track person interaction counts for proximity update
  const personInteractions = new Map(); // person_id -> { count, lastDate }

  async function flushBatch() {
    if (batch.length === 0 || DRY_RUN) return;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const row of batch) {
        try {
          await client.query(`
            INSERT INTO interactions (
              person_id, user_id, interaction_type, direction, subject,
              source, channel, interaction_at, tenant_id,
              email_message_id, email_thread_id, email_subject, email_snippet,
              email_from, email_to, email_cc,
              created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
            ON CONFLICT DO NOTHING
          `, [
            row.personId,
            USER_ID,
            'email',
            row.direction,
            (row.subject || '').slice(0, 500),
            'mbox_import',
            'email',
            row.interactionAt,
            TENANT_ID,
            row.messageId,
            row.threadId,
            row.subject,
            row.snippet,
            row.emailFrom,
            row.emailTo,
            row.emailCc,
          ]);
          stats.created++;
        } catch (err) {
          stats.errors++;
          if (stats.errors <= 5) {
            console.error(`  Insert error: ${err.message}`);
          }
        }
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      console.error(`Batch error: ${err.message}`);
      stats.errors += batch.length;
    } finally {
      client.release();
    }
    batch = [];
  }

  // Process emails
  console.log('\nProcessing emails...\n');

  for await (const { headers, body } of parseMbox(MBOX_PATH)) {
    stats.total++;

    if (stats.total % 1000 === 0) {
      console.log(`  [${stats.total}] kept=${stats.kept} skipped=${Object.values(stats.skipped).reduce((a, b) => a + b, 0)} matched=${stats.matched} created=${stats.created}`);
      // Flush any pending batch at progress milestones
      await flushBatch();
    }

    // Validate minimum headers
    if (!headers['from'] && !headers['to']) {
      stats.skipped.no_headers++;
      continue;
    }

    // Parse date
    let emailDate;
    try {
      emailDate = headers['date'] ? new Date(headers['date']) : null;
      if (emailDate && isNaN(emailDate.getTime())) emailDate = null;
    } catch {
      emailDate = null;
    }
    if (!emailDate) {
      stats.skipped.no_date++;
      continue;
    }

    // Classification — skip only truly automated, keep newsletters + mass
    const classification = classifyEmail(headers);
    if (classification.skip) {
      const key = classification.reason.replace('/', '_').replace(' ', '_');
      stats.skipped[key] = (stats.skipped[key] || 0) + 1;
      continue;
    }
    const emailType = classification.type; // 'personal', 'newsletter', 'mass'

    // Dedup by Message-ID
    const messageId = headers['message-id'] ? headers['message-id'].replace(/[<>]/g, '').trim() : null;
    if (messageId && existingMessageIds.has(messageId)) {
      stats.skipped.duplicate++;
      continue;
    }
    if (messageId) existingMessageIds.add(messageId); // also dedup within file

    stats.kept++;

    // Determine direction
    const fromAddr = extractEmailAddress(headers['from']);
    const direction = (userEmail && fromAddr === userEmail) ? 'outbound' : 'inbound';

    // Extract all person addresses (From + To + CC, excluding user)
    const contactAddresses = [];
    const allRawAddresses = [
      ...(headers['from'] ? [headers['from']] : []),
      ...parseAddressList(headers['to'] || ''),
      ...parseAddressList(headers['cc'] || ''),
    ];

    for (const raw of allRawAddresses) {
      const addr = extractEmailAddress(raw);
      const name = extractDisplayName(raw);
      if (!addr) continue;
      if (userEmail && addr === userEmail) continue;
      if (isAutomatedAddress(addr)) continue;
      contactAddresses.push({ addr, name });
    }

    // Match people
    const matchedPersonIds = new Set();
    for (const { addr, name } of contactAddresses) {
      const pid = matchPerson(addr, name);
      if (pid) matchedPersonIds.add(pid);
    }

    // Thread ID from In-Reply-To or References
    const threadId = (headers['in-reply-to'] || headers['references'] || '').replace(/[<>]/g, '').split(/\s+/)[0]?.trim() || null;

    // Snippet
    const snippet = extractTextSnippet(body);

    // Build email_to and email_cc arrays
    const emailToArr = parseAddressList(headers['to'] || '').map(extractEmailAddress).filter(Boolean);
    const emailCcArr = parseAddressList(headers['cc'] || '').map(extractEmailAddress).filter(Boolean);

    if (matchedPersonIds.size > 0) {
      stats.matched++;
      // Create one interaction per matched person
      for (const personId of matchedPersonIds) {
        // Track for proximity
        const existing = personInteractions.get(personId);
        if (!existing || emailDate > existing.lastDate) {
          personInteractions.set(personId, {
            count: (existing?.count || 0) + 1,
            lastDate: emailDate,
          });
        } else {
          existing.count++;
        }

        batch.push({
          personId,
          direction,
          subject: headers['subject'] || null,
          interactionAt: emailDate,
          messageId,
          threadId,
          snippet,
          emailFrom: fromAddr,
          emailTo: emailToArr,
          emailCc: emailCcArr,
        });
      }
    } else {
      stats.unmatched++;
      // Still create interaction with null person_id? No — schema requires person_id NOT NULL.
      // Skip unmatched emails.
    }

    if (batch.length >= BATCH_SIZE) {
      await flushBatch();
    }
  }

  // Flush remaining
  await flushBatch();

  // ── Update team_proximity ──
  if (!DRY_RUN && personInteractions.size > 0) {
    console.log(`\nUpdating team_proximity for ${personInteractions.size} people...`);

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let proxUpdated = 0;

      for (const [personId, data] of personInteractions) {
        const strength = data.count >= 10 ? 0.85 : data.count >= 3 ? 0.60 : 0.30;
        const relType = data.count >= 10 ? 'email_frequent' : data.count >= 3 ? 'email_moderate' : 'email_minimal';

        await client.query(`
          INSERT INTO team_proximity (
            person_id, team_member_id, relationship_type, relationship_strength,
            source, interaction_count, last_interaction_date, created_at, updated_at
          ) VALUES ($1, $2, $3, $4, 'mbox_import', $5, $6, NOW(), NOW())
          ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
            interaction_count = team_proximity.interaction_count + $5,
            relationship_strength = GREATEST(team_proximity.relationship_strength, $4),
            last_interaction_date = GREATEST(team_proximity.last_interaction_date, $6::date),
            source = 'mbox_import',
            updated_at = NOW()
        `, [personId, USER_ID, relType, strength, data.count, data.lastDate]);
        proxUpdated++;
      }

      await client.query('COMMIT');
      console.log(`  Updated ${proxUpdated} proximity records`);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error(`Proximity update error: ${err.message}`);
    } finally {
      client.release();
    }
  }

  // ── Final report ──
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalSkipped = Object.values(stats.skipped).reduce((a, b) => a + b, 0);

  console.log('\n' + '='.repeat(60));
  console.log('IMPORT COMPLETE');
  console.log('='.repeat(60));
  console.log(`  Total emails parsed:    ${stats.total}`);
  console.log(`  Kept (relevant):        ${stats.kept}`);
  console.log(`  Skipped:                ${totalSkipped}`);
  for (const [reason, count] of Object.entries(stats.skipped).sort((a, b) => b[1] - a[1])) {
    if (count > 0) console.log(`    - ${reason}: ${count}`);
  }
  console.log(`  People matched:         ${stats.matched} emails matched to known people`);
  console.log(`  People unmatched:       ${stats.unmatched} emails (no person match, skipped)`);
  console.log(`  Interactions created:   ${stats.created}`);
  console.log(`  Proximity updated:      ${personInteractions.size} people`);
  if (stats.errors > 0) console.log(`  Errors:                 ${stats.errors}`);
  console.log(`  Elapsed:                ${elapsed}s`);
  if (DRY_RUN) console.log('\n  ** DRY RUN — no data written **');
  console.log('='.repeat(60));

  await pool.end();
}

main().catch(err => {
  console.error('Fatal error:', err);
  pool.end();
  process.exit(1);
});
