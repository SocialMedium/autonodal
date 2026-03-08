#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/sync_gmail.js
//
// Delta sync of Gmail threads → interactions table.
// Designed to run on a 15-minute cron.
//
// PRIVACY-BY-DESIGN:
//   Stores ONLY: subject, thread metadata, Gmail URL, participant emails.
//   Email body content is NEVER stored or logged. Ever.
//
// Usage:
//   node scripts/sync_gmail.js
//   node scripts/sync_gmail.js --user-id <uuid>
//   node scripts/sync_gmail.js --full-scan
//   node scripts/sync_gmail.js --dry-run
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool }    = require('pg');
const { google }  = require('googleapis');
const { runJob, logProgress, withRetry, sleep } = require('../lib/job_runner');

// ─── CLI flags ───────────────────────────────────────────────────────────────
const DRY_RUN   = process.argv.includes('--dry-run');
const FULL_SCAN = process.argv.includes('--full-scan');
const USER_ID   = (() => {
  const idx = process.argv.indexOf('--user-id');
  return idx !== -1 ? process.argv[idx + 1] : null;
})();

// ─── Config ──────────────────────────────────────────────────────────────────
const LOOKBACK_DAYS    = parseInt(process.env.GMAIL_LOOKBACK_DAYS    || '90');
const MAX_THREADS      = parseInt(process.env.GMAIL_MAX_THREADS_PER_RUN || '500');
const RATE_LIMIT_MS    = 100; // 10 calls/sec max
const SKIP_LABELS      = new Set(['SPAM', 'TRASH', 'PROMOTIONS', 'CATEGORY_PROMOTIONS', 'CATEGORY_SOCIAL', 'CATEGORY_UPDATES', 'CATEGORY_FORUMS']);
const CV_PATTERN       = /(resume|cv|curriculum.vitae).*(\.pdf|\.doc|\.docx)$/i;

// ─── Colours ─────────────────────────────────────────────────────────────────
const c = {
  green:  (s) => `\x1b[32m${s}\x1b[0m`,
  red:    (s) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s) => `\x1b[33m${s}\x1b[0m`,
  blue:   (s) => `\x1b[34m${s}\x1b[0m`,
  dim:    (s) => `\x1b[2m${s}\x1b[0m`,
};

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// ═══════════════════════════════════════════════════════════════════════════
// GOOGLE AUTH
// ═══════════════════════════════════════════════════════════════════════════

async function getOAuthClient(account) {
  const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );

  oauth2Client.setCredentials({
    access_token:  account.access_token,
    refresh_token: account.refresh_token,
    expiry_date:   account.token_expires_at ? new Date(account.token_expires_at).getTime() : null,
  });

  // Auto-refresh if expired
  const expiresAt = account.token_expires_at ? new Date(account.token_expires_at) : null;
  if (!expiresAt || expiresAt < new Date(Date.now() + 60000)) {
    try {
      const { credentials } = await oauth2Client.refreshAccessToken();
      oauth2Client.setCredentials(credentials);

      if (!DRY_RUN) {
        await pool.query(
          `UPDATE user_google_accounts SET
             access_token     = $2,
             token_expires_at = $3
           WHERE id = $1`,
          [account.id, credentials.access_token, new Date(credentials.expiry_date)]
        );
      }
    } catch (err) {
      throw new Error(`Token refresh failed for ${account.google_email}: ${err.message}`);
    }
  }

  return oauth2Client;
}

// ═══════════════════════════════════════════════════════════════════════════
// PERSON EMAIL LOOKUP CACHE
// ═══════════════════════════════════════════════════════════════════════════

const emailCache = new Map();

async function findPersonByEmail(email) {
  if (!email) return null;
  const key = email.toLowerCase();
  if (emailCache.has(key)) return emailCache.get(key);

  const { rows } = await pool.query(
    `SELECT id, full_name FROM people
     WHERE lower(email) = $1 OR lower(email_alt) = $1
     LIMIT 1`,
    [key]
  );

  const result = rows[0] || null;
  emailCache.set(key, result);
  return result;
}

// ═══════════════════════════════════════════════════════════════════════════
// THREAD PROCESSING
// ═══════════════════════════════════════════════════════════════════════════

function extractEmails(headerValue) {
  if (!headerValue) return [];
  const matches = headerValue.match(/[\w.+-]+@[\w-]+\.[\w.]+/g);
  return matches ? [...new Set(matches.map(e => e.toLowerCase()))] : [];
}

function getHeader(headers, name) {
  const h = headers.find(h => h.name.toLowerCase() === name.toLowerCase());
  return h ? h.value : '';
}

function hasSkipLabel(labelIds) {
  if (!labelIds) return false;
  return labelIds.some(l => SKIP_LABELS.has(l));
}

async function processThread(gmail, thread, userEmail, userId, dryRun) {
  const threadId = thread.id;

  // Get full thread data
  const threadData = await withRetry(() =>
    gmail.users.threads.get({
      userId: 'me',
      id: threadId,
      format: 'metadata',
      metadataHeaders: ['From', 'To', 'Cc', 'Subject', 'Date'],
    })
  );

  const messages = threadData.data.messages || [];
  if (messages.length === 0) return null;

  // Skip unwanted labels
  const allLabels = messages.flatMap(m => m.labelIds || []);
  if (hasSkipLabel(allLabels)) return null;

  // Get thread metadata from first message
  const firstMsg  = messages[0];
  const lastMsg   = messages[messages.length - 1];
  const headers   = firstMsg.payload?.headers || [];

  const subject   = getHeader(headers, 'Subject').slice(0, 500) || '(no subject)';
  const fromRaw   = getHeader(headers, 'From');
  const toRaw     = getHeader(headers, 'To');
  const ccRaw     = getHeader(headers, 'Cc');
  const dateStr   = getHeader(headers, 'Date');

  const fromEmails = extractEmails(fromRaw);
  const toEmails   = extractEmails(toRaw);
  const ccEmails   = extractEmails(ccRaw);
  const allEmails  = [...new Set([...fromEmails, ...toEmails, ...ccEmails])];
  const externalEmails = allEmails.filter(e => e !== userEmail.toLowerCase());

  if (externalEmails.length === 0) return null;

  // Determine direction from first message
  const isOutbound = fromEmails.includes(userEmail.toLowerCase());
  const direction  = isOutbound ? 'outbound' : 'inbound';

  // Thread stats
  const inboundCount  = messages.filter(m => {
    const mFrom = getHeader(m.payload?.headers || [], 'From');
    return !extractEmails(mFrom).includes(userEmail.toLowerCase());
  }).length;
  const outboundCount = messages.length - inboundCount;
  const hasReply      = inboundCount > 0 && outboundCount > 0;
  const isDeepThread  = messages.length >= 3;

  // Check for CV attachments
  const hasCvAttachment = messages.some(m =>
    (m.payload?.parts || []).some(p =>
      p.filename && CV_PATTERN.test(p.filename)
    )
  );

  // Parse interaction date
  let interactionAt;
  try {
    interactionAt = dateStr ? new Date(dateStr) : new Date(parseInt(lastMsg.internalDate));
    if (isNaN(interactionAt.getTime())) interactionAt = new Date();
  } catch {
    interactionAt = new Date();
  }

  // Gmail URL
  const gmailUrl = `https://mail.google.com/mail/u/0/#inbox/${threadId}`;

  // Find matched person(s)
  let matchedPerson = null;
  for (const email of externalEmails) {
    const person = await findPersonByEmail(email);
    if (person) {
      matchedPerson = { ...person, email };
      break;
    }
  }

  // Track unknown contacts (2+ threads threshold enforced in upsert)
  if (!matchedPerson && externalEmails.length > 0) {
    const primaryEmail = externalEmails[0];
    const displayName  = fromRaw.replace(/<.*>/, '').replace(/"/g, '').trim() || null;

    if (!dryRun) {
      await pool.query(
        `INSERT INTO new_contacts_review (email, name, thread_count, last_thread_date, discovered_by_user_id)
         VALUES ($1, $2, 1, $3, $4)
         ON CONFLICT (email) DO UPDATE SET
           thread_count     = new_contacts_review.thread_count + 1,
           last_thread_date = GREATEST(new_contacts_review.last_thread_date, EXCLUDED.last_thread_date),
           name             = COALESCE(new_contacts_review.name, EXCLUDED.name)`,
        [primaryEmail, displayName, interactionAt, userId]
      );
    }
    return null; // Don't log interaction for unmatched people
  }

  if (!matchedPerson) return null;

  // Build metadata (privacy-safe — no body content)
  const metadata = {
    gmail_url:          gmailUrl,
    message_count:      messages.length,
    has_reply:          hasReply,
    is_deep_thread:     isDeepThread,
    has_cv_attachment:  hasCvAttachment,
    external_emails:    externalEmails,
  };

  const interactionType = isOutbound ? 'email_sent' : 'email_received';

  if (!dryRun) {
    await pool.query(
      `INSERT INTO interactions
         (person_id, user_id, interaction_type, direction, subject,
          channel, source, external_id, interaction_at,
          requires_response, response_received, metadata)
       VALUES ($1, $2, $3, $4, $5, 'email', 'gmail_sync', $6, $7, $8, $9, $10)
       ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
      [
        matchedPerson.id,
        userId,
        interactionType,
        direction,
        subject,
        threadId,
        interactionAt,
        isOutbound && !hasReply,  // requires_response
        hasReply,                 // response_received
        JSON.stringify(metadata),
      ]
    );
  }

  return {
    personId:      matchedPerson.id,
    threadCount:   1,
    hasReply,
    isDeepThread,
    interactionAt,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// TEAM PROXIMITY UPDATE
// ═══════════════════════════════════════════════════════════════════════════

async function updateTeamProximity(pool, personId, userId, emailThreadCount, lastDate, dryRun) {
  let relationshipType, strength;

  if (emailThreadCount >= 10) {
    relationshipType = 'email_frequent';
    strength = 0.85;
  } else if (emailThreadCount >= 3) {
    relationshipType = 'email_moderate';
    strength = 0.60;
  } else {
    relationshipType = 'email_minimal';
    strength = 0.30;
  }

  const context = `${emailThreadCount} email thread(s) via Gmail sync`;

  if (!dryRun) {
    await pool.query(
      `INSERT INTO team_proximity
         (person_id, team_member_id, relationship_type, relationship_strength,
          notes, last_interaction_date, source)
       VALUES ($1, $2, $3, $4, $5, $6, 'gmail_sync')
       ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
         relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
         notes                 = COALESCE(EXCLUDED.notes, team_proximity.notes),
         last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
         updated_at            = NOW()`,
      [personId, userId, relationshipType, strength, context, lastDate]
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// SYNC ONE ACCOUNT
// ═══════════════════════════════════════════════════════════════════════════

async function syncAccount(account) {
  console.log(c.yellow(`\n  ▶ Syncing: ${account.google_email}`));

  let auth;
  try {
    auth = await getOAuthClient(account);
  } catch (err) {
    console.log(c.red(`  ✗ Auth failed: ${err.message} — skipping`));
    return { threadsScanned: 0, interactionsCreated: 0, skipped: true };
  }

  const gmail = google.gmail({ version: 'v1', auth });

  // ── Get thread list ─────────────────────────────────────────────────────
  let threadIds = [];
  let useHistory = !FULL_SCAN && account.gmail_history_id;

  if (useHistory) {
    // Delta sync via History API
    try {
      console.log(c.dim(`  Using history API from historyId: ${account.gmail_history_id}`));
      let pageToken;
      do {
        const res = await withRetry(() =>
          gmail.users.history.list({
            userId:          'me',
            startHistoryId:  account.gmail_history_id,
            historyTypes:    ['messageAdded'],
            pageToken,
          })
        );
        const history = res.data.history || [];
        for (const h of history) {
          for (const m of (h.messagesAdded || [])) {
            if (m.message?.threadId) threadIds.push(m.message.threadId);
          }
        }
        pageToken = res.data.nextPageToken;
      } while (pageToken && threadIds.length < MAX_THREADS);

      // Deduplicate thread IDs
      threadIds = [...new Set(threadIds)];
    } catch (err) {
      if (err.code === 404 || err.message?.includes('startHistoryId')) {
        console.log(c.yellow('  History ID too old — falling back to full scan'));
        useHistory = false;
      } else {
        throw err;
      }
    }
  }

  if (!useHistory) {
    // Full scan of last N days
    const after = Math.floor((Date.now() - LOOKBACK_DAYS * 24 * 60 * 60 * 1000) / 1000);
    console.log(c.dim(`  Full scan: last ${LOOKBACK_DAYS} days`));

    let pageToken;
    do {
      const res = await withRetry(() =>
        gmail.users.threads.list({
          userId:    'me',
          q:         `-label:spam -label:trash after:${after}`,
          maxResults: 100,
          pageToken,
        })
      );
      const threads = res.data.threads || [];
      threadIds.push(...threads.map(t => t.id));
      pageToken = res.data.nextPageToken;
      await sleep(RATE_LIMIT_MS);
    } while (pageToken && threadIds.length < MAX_THREADS);
  }

  console.log(c.blue(`  Found ${threadIds.length} threads to process`));

  // ── Get new historyId ───────────────────────────────────────────────────
  let newHistoryId = account.gmail_history_id;
  try {
    const profile = await gmail.users.getProfile({ userId: 'me' });
    newHistoryId = profile.data.historyId;
  } catch {}

  // ── Process threads ─────────────────────────────────────────────────────
  let interactionsCreated = 0;
  const personThreadCounts = new Map(); // personId → { count, lastDate }

  for (let i = 0; i < threadIds.length; i++) {
    logProgress('gmail_sync', i + 1, threadIds.length, `processing threads`);

    try {
      const result = await withRetry(() =>
        processThread(gmail, { id: threadIds[i] }, account.google_email, account.user_id, DRY_RUN)
      );

      if (result) {
        interactionsCreated++;
        const existing = personThreadCounts.get(result.personId) || { count: 0, lastDate: null };
        existing.count++;
        if (!existing.lastDate || result.interactionAt > existing.lastDate) {
          existing.lastDate = result.interactionAt;
        }
        personThreadCounts.set(result.personId, existing);
      }
    } catch (err) {
      console.log(c.dim(`  ⚠ Thread ${threadIds[i]} failed: ${err.message}`));
    }

    await sleep(RATE_LIMIT_MS);
  }

  // ── Update team proximity ───────────────────────────────────────────────
  console.log(c.yellow(`  ▶ Updating proximity for ${personThreadCounts.size} people...`));
  for (const [personId, stats] of personThreadCounts) {
    await updateTeamProximity(pool, personId, account.user_id, stats.count, stats.lastDate, DRY_RUN);
  }

  // ── Save new historyId ──────────────────────────────────────────────────
  if (!DRY_RUN && newHistoryId) {
    await pool.query(
      `UPDATE user_google_accounts SET
         gmail_history_id  = $2,
         gmail_last_sync_at = NOW()
       WHERE id = $1`,
      [account.id, newHistoryId]
    );
  }

  return {
    threadsScanned:     threadIds.length,
    interactionsCreated,
    proximityUpdated:   personThreadCounts.size,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('');
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log(c.blue('  MitchelLake — Gmail Sync                            '));
  if (DRY_RUN)   console.log(c.yellow('  ⚠  DRY RUN — no data will be written               '));
  if (FULL_SCAN) console.log(c.yellow(`  ↩  FULL SCAN — last ${LOOKBACK_DAYS} days            `));
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log('');

  if (!process.env.DATABASE_URL)      { console.log(c.red('✗ DATABASE_URL not set')); process.exit(1); }
  if (!process.env.GOOGLE_CLIENT_ID)  { console.log(c.red('✗ GOOGLE_CLIENT_ID not set')); process.exit(1); }
  if (!process.env.GOOGLE_CLIENT_SECRET) { console.log(c.red('✗ GOOGLE_CLIENT_SECRET not set')); process.exit(1); }

  await runJob(pool, 'gmail_sync', async () => {
    // Get accounts to sync
    let query = `SELECT * FROM user_google_accounts WHERE access_token IS NOT NULL`;
    const params = [];
    if (USER_ID) {
      query += ` AND user_id = $1`;
      params.push(USER_ID);
    }

    const { rows: accounts } = await pool.query(query, params);

    if (accounts.length === 0) {
      console.log(c.yellow('  No connected Google accounts found'));
      return { records_in: 0, records_out: 0 };
    }

    console.log(c.green(`✓ Found ${accounts.length} connected account(s)`));

    let totalThreads      = 0;
    let totalInteractions = 0;
    let totalProximity    = 0;

    for (const account of accounts) {
      try {
        const result = await syncAccount(account);
        totalThreads      += result.threadsScanned      || 0;
        totalInteractions += result.interactionsCreated || 0;
        totalProximity    += result.proximityUpdated    || 0;
      } catch (err) {
        console.log(c.red(`  ✗ Account ${account.google_email} failed: ${err.message}`));
      }
    }

    console.log('');
    console.log(c.blue('═══════════════════════════════════════════════════════'));
    console.log(c.blue('  📧 GMAIL SYNC COMPLETE'));
    console.log(c.blue('═══════════════════════════════════════════════════════'));
    console.log(`  Threads scanned:        ${totalThreads.toLocaleString()}`);
    console.log(`  Interactions logged:    ${totalInteractions.toLocaleString()}`);
    console.log(`  Proximity updated:      ${totalProximity.toLocaleString()}`);
    if (DRY_RUN) console.log(c.yellow('\n  ⚠  DRY RUN — re-run without --dry-run to write data'));
    console.log(c.blue('═══════════════════════════════════════════════════════'));

    return {
      records_in:  totalThreads,
      records_out: totalInteractions,
      metadata:    { proximity_updated: totalProximity },
    };
  });

  await pool.end();
}

main().catch(err => {
  console.error(c.red(`\nFatal error: ${err.message}`));
  console.error(err);
  process.exit(1);
});
