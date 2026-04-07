#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/sync_calendar.js
//
// Google Calendar sync → interactions + team_proximity.
// Two functions:
//   1. Meeting history — who met whom, for relationship strength
//   2. Forward-looking events — upcoming meetings as signals
//
// PRIVACY-BY-DESIGN:
//   Stores: event title, attendees (email only), time, duration, location.
//   Does NOT store event descriptions, attachments, or notes.
//
// Usage:
//   node scripts/sync_calendar.js
//   node scripts/sync_calendar.js --user-id <uuid>
//   node scripts/sync_calendar.js --full-scan
//   node scripts/sync_calendar.js --dry-run
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool }    = require('pg');
const { google }  = require('googleapis');
const { runJob, withRetry, sleep } = require('../lib/job_runner');

// ─── CLI flags ───────────────────────────────────────────────────────────────
const DRY_RUN   = process.argv.includes('--dry-run');
const FULL_SCAN = process.argv.includes('--full-scan');
const USER_ID   = (() => {
  const idx = process.argv.indexOf('--user-id');
  return idx !== -1 ? process.argv[idx + 1] : null;
})();

// ─── Config ──────────────────────────────────────────────────────────────────
const LOOKBACK_DAYS       = parseInt(process.env.CALENDAR_LOOKBACK_DAYS || '90');
const LOOKAHEAD_DAYS      = parseInt(process.env.CALENDAR_LOOKAHEAD_DAYS || '30');
const MAX_EVENTS_PER_RUN  = parseInt(process.env.CALENDAR_MAX_EVENTS || '2500');
const RATE_LIMIT_MS       = 150;

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
// GOOGLE AUTH (same pattern as sync_gmail.js)
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
let teamEmailSet = null;

async function loadTeamEmails() {
  if (teamEmailSet) return teamEmailSet;
  const { rows } = await pool.query(`SELECT email FROM users WHERE email IS NOT NULL`);
  teamEmailSet = new Set(rows.map(r => r.email.toLowerCase()));
  // Also add Google account emails
  const { rows: gRows } = await pool.query(`SELECT google_email FROM user_google_accounts WHERE google_email IS NOT NULL`);
  gRows.forEach(r => teamEmailSet.add(r.google_email.toLowerCase()));
  return teamEmailSet;
}

async function lookupPersonByEmail(email, tenantId) {
  if (!email) return null;
  const key = email.toLowerCase();
  if (emailCache.has(key)) return emailCache.get(key);

  const { rows } = await pool.query(
    `SELECT id FROM people
     WHERE tenant_id = $1 AND (LOWER(email) = $2 OR LOWER(email_alt) = $2)
     LIMIT 1`,
    [tenantId, key]
  );
  const personId = rows.length ? rows[0].id : null;
  emailCache.set(key, personId);
  return personId;
}

// ═══════════════════════════════════════════════════════════════════════════
// FETCH CALENDAR EVENTS
// ═══════════════════════════════════════════════════════════════════════════

async function fetchCalendarEvents(auth, timeMin, timeMax, syncToken) {
  const calendar = google.calendar({ version: 'v3', auth });
  const allEvents = [];
  let pageToken = null;
  let newSyncToken = null;

  // If we have a sync token and not full-scanning, use incremental sync
  if (syncToken && !FULL_SCAN) {
    try {
      let page = 0;
      do {
        const params = { calendarId: 'primary', syncToken, maxResults: 250 };
        if (pageToken) params.pageToken = pageToken;

        const res = await withRetry(() => calendar.events.list(params), 3, 500);
        const items = (res.data.items || []).filter(e => e.status !== 'cancelled');
        allEvents.push(...items);
        pageToken = res.data.nextPageToken;
        newSyncToken = res.data.nextSyncToken || newSyncToken;
        page++;
        if (allEvents.length >= MAX_EVENTS_PER_RUN) break;
        await sleep(RATE_LIMIT_MS);
      } while (pageToken);

      return { events: allEvents, syncToken: newSyncToken };
    } catch (err) {
      // Sync token expired — fall through to full fetch
      if (err.code === 410 || (err.message && err.message.includes('Sync token'))) {
        console.log(c.yellow('    Sync token expired, falling back to full fetch'));
      } else {
        throw err;
      }
    }
  }

  // Full fetch
  let page = 0;
  do {
    const params = {
      calendarId: 'primary',
      timeMin: timeMin.toISOString(),
      timeMax: timeMax.toISOString(),
      maxResults: 250,
      singleEvents: true,
      orderBy: 'startTime',
    };
    if (pageToken) params.pageToken = pageToken;

    const res = await withRetry(() => calendar.events.list(params), 3, 500);
    allEvents.push(...(res.data.items || []));
    pageToken = res.data.nextPageToken;
    newSyncToken = res.data.nextSyncToken || newSyncToken;
    page++;
    if (allEvents.length >= MAX_EVENTS_PER_RUN) break;
    await sleep(RATE_LIMIT_MS);
  } while (pageToken);

  return { events: allEvents, syncToken: newSyncToken };
}

// ═══════════════════════════════════════════════════════════════════════════
// PROCESS EVENTS
// ═══════════════════════════════════════════════════════════════════════════

function extractAttendeeEmails(event) {
  if (!event.attendees) return [];
  return event.attendees
    .filter(a => a.email && !a.self && !a.resource)
    .map(a => ({
      email: a.email.toLowerCase(),
      responseStatus: a.responseStatus,
      displayName: a.displayName || null,
    }));
}

function eventDurationMinutes(event) {
  const start = event.start?.dateTime ? new Date(event.start.dateTime) : null;
  const end = event.end?.dateTime ? new Date(event.end.dateTime) : null;
  if (!start || !end) return null;
  return Math.round((end - start) / 60000);
}

function isAllDayEvent(event) {
  return !!(event.start?.date && !event.start?.dateTime);
}

async function processEvent(event, account, tenantId) {
  // Skip all-day events (OOO, holidays), cancelled, declined
  if (isAllDayEvent(event)) return null;
  if (event.status === 'cancelled') return null;

  // Check if user declined
  const selfAttendee = (event.attendees || []).find(a => a.self);
  if (selfAttendee && selfAttendee.responseStatus === 'declined') return null;

  const attendees = extractAttendeeEmails(event);
  if (attendees.length === 0) return null; // Solo events — no relationship signal

  const teamEmails = await loadTeamEmails();
  const externalAttendees = attendees.filter(a => !teamEmails.has(a.email));
  if (externalAttendees.length === 0) return null; // Internal-only meetings — skip

  const startTime = event.start?.dateTime ? new Date(event.start.dateTime) : new Date(event.start.date);
  const duration = eventDurationMinutes(event);
  const isPast = startTime < new Date();

  return {
    eventId: event.id,
    title: event.summary || 'Untitled meeting',
    startTime,
    duration,
    isPast,
    location: event.location || null,
    conferenceLink: event.hangoutLink || event.conferenceData?.entryPoints?.[0]?.uri || null,
    attendees: externalAttendees,
    allAttendees: attendees,
    organizer: event.organizer?.email || account.google_email,
    recurringEventId: event.recurringEventId || null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// STORE INTERACTIONS + PROXIMITY
// ═══════════════════════════════════════════════════════════════════════════

async function storeInteraction(parsed, userId, tenantId) {
  if (DRY_RUN) return { created: 0, matched: 0 };

  let matched = 0;

  for (const attendee of parsed.attendees) {
    const personId = await lookupPersonByEmail(attendee.email, tenantId);
    if (!personId) continue;
    matched++;

    const direction = parsed.organizer === attendee.email ? 'inbound' : 'outbound';

    // Store as interaction (no tenant_id — matches gmail sync pattern)
    await pool.query(
      `INSERT INTO interactions
         (person_id, user_id, interaction_type, direction,
          subject, channel, source, external_id, duration_minutes,
          interaction_at)
       VALUES ($1, $2, 'meeting', $3, $4, 'calendar', 'gcal_sync', $5, $6, $7)
       ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
      [personId, userId, direction, parsed.title,
       'gcal:' + parsed.eventId, parsed.duration, parsed.startTime]
    );
  }

  return { created: matched > 0 ? 1 : 0, matched };
}

async function updateCalendarProximity(userId, tenantId) {
  if (DRY_RUN) return;

  // Count meetings per person from calendar interactions
  const { rows } = await pool.query(
    `SELECT person_id,
            COUNT(DISTINCT external_id) AS meeting_count,
            MAX(interaction_at) AS last_meeting
     FROM interactions
     WHERE user_id = $1 AND tenant_id = $2
       AND source = 'gcal_sync' AND interaction_type = 'meeting'
       AND interaction_at >= NOW() - INTERVAL '180 days'
     GROUP BY person_id`,
    [userId, tenantId]
  );

  for (const row of rows) {
    const count = parseInt(row.meeting_count);
    let relationshipType, strength;

    if (count >= 5) {
      relationshipType = 'meeting_frequent';
      strength = 0.90; // Meetings are higher-signal than emails
    } else if (count >= 2) {
      relationshipType = 'meeting_moderate';
      strength = 0.70;
    } else {
      relationshipType = 'meeting_single';
      strength = 0.45;
    }

    await pool.query(
      `INSERT INTO team_proximity
         (person_id, team_member_id, relationship_type, relationship_strength,
          notes, last_interaction_date, interaction_count, source)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'gcal_sync')
       ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
         relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
         notes                 = EXCLUDED.notes,
         interaction_count     = EXCLUDED.interaction_count,
         last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
         updated_at            = NOW()`,
      [row.person_id, userId, relationshipType, strength,
       `${count} meeting(s) via Calendar sync`, row.last_meeting, count]
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// STORE UPCOMING MEETINGS AS FORWARD-LOOKING SIGNALS
// ═══════════════════════════════════════════════════════════════════════════

async function storeUpcomingSignals(parsed, userId, tenantId) {
  if (DRY_RUN || parsed.isPast) return 0;

  let signals = 0;

  for (const attendee of parsed.attendees) {
    const personId = await lookupPersonByEmail(attendee.email, tenantId);
    if (!personId) continue;

    // Find the person's company
    const { rows } = await pool.query(
      `SELECT current_company_id, current_company_name FROM people WHERE id = $1`,
      [personId]
    );
    if (!rows.length || !rows[0].current_company_id) continue;

    const companyId = rows[0].current_company_id;
    const companyName = rows[0].current_company_name;

    // Only create signals for meetings in next 14 days (actionable)
    const daysOut = Math.ceil((parsed.startTime - new Date()) / 86400000);
    if (daysOut > 14) continue;

    // Upsert a calendar_meeting signal — one per company per meeting
    await pool.query(
      `INSERT INTO signal_events
         (tenant_id, company_id, company_name, signal_type, confidence_score,
          evidence_summary, source_url, detected_at, source_name)
       VALUES ($1, $2, $3, 'meeting_upcoming', 0.95, $4, $5, $6, 'Google Calendar')
       ON CONFLICT DO NOTHING`,
      [tenantId, companyId, companyName,
       `Upcoming meeting: "${parsed.title}" with ${attendee.displayName || attendee.email} — ${daysOut === 0 ? 'today' : daysOut === 1 ? 'tomorrow' : daysOut + ' days'}`,
       parsed.conferenceLink,
       parsed.startTime]
    );
    signals++;
  }

  return signals;
}

// ═══════════════════════════════════════════════════════════════════════════
// SYNC ONE ACCOUNT
// ═══════════════════════════════════════════════════════════════════════════

async function syncAccount(account) {
  console.log(c.yellow(`\n  ▶ Syncing calendar: ${account.google_email}`));

  let auth;
  try {
    auth = await getOAuthClient(account);
  } catch (err) {
    console.log(c.red(`  ✗ Auth failed: ${err.message} — skipping`));
    return { events_fetched: 0, interactions: 0, signals: 0, errors: 1 };
  }

  // Get tenant_id for this user
  const { rows: [user] } = await pool.query(
    `SELECT tenant_id FROM users WHERE id = $1`, [account.user_id]
  );
  if (!user) {
    console.log(c.red(`  ✗ User not found — skipping`));
    return { events_fetched: 0, interactions: 0, signals: 0, errors: 1 };
  }
  const tenantId = user.tenant_id;

  const now = new Date();
  const timeMin = new Date(now.getTime() - LOOKBACK_DAYS * 86400000);
  const timeMax = new Date(now.getTime() + LOOKAHEAD_DAYS * 86400000);

  // Fetch sync token if we have one
  const syncToken = (!FULL_SCAN && account.calendar_sync_token) || null;

  let result;
  try {
    result = await fetchCalendarEvents(auth, timeMin, timeMax, syncToken);
  } catch (err) {
    console.log(c.red(`  ✗ Calendar fetch failed: ${err.message}`));
    return { events_fetched: 0, interactions: 0, signals: 0, errors: 1 };
  }

  console.log(`    📅 Fetched ${result.events.length} calendar events`);

  let interactions = 0, signals = 0, skipped = 0;

  for (const event of result.events) {
    try {
      const parsed = await processEvent(event, account, tenantId);
      if (!parsed) { skipped++; continue; }

      if (parsed.isPast) {
        const r = await storeInteraction(parsed, account.user_id, tenantId);
        interactions += r.matched;
      } else {
        signals += await storeUpcomingSignals(parsed, account.user_id, tenantId);
      }
    } catch (err) {
      console.log(c.dim(`    ⚠ Event error: ${err.message}`));
    }
  }

  // Update proximity scores based on meeting history
  await updateCalendarProximity(account.user_id, tenantId);

  // Store sync token for next delta run
  if (!DRY_RUN && result.syncToken) {
    await pool.query(
      `UPDATE user_google_accounts SET calendar_sync_token = $2 WHERE id = $1`,
      [account.id, result.syncToken]
    );
  }

  console.log(c.green(`    ✓ ${interactions} interactions, ${signals} upcoming signals, ${skipped} skipped`));
  return { events_fetched: result.events.length, interactions, signals, errors: 0 };
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  GOOGLE CALENDAR SYNC — MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  if (DRY_RUN) console.log(c.yellow('  ⚠ DRY RUN — no writes'));
  if (FULL_SCAN) console.log(c.yellow('  ⚠ FULL SCAN — ignoring sync tokens'));

  await runJob(pool, 'calendar_sync', async () => {
    // Ensure calendar_sync_token column exists
    try {
      await pool.query(`ALTER TABLE user_google_accounts ADD COLUMN IF NOT EXISTS calendar_sync_token TEXT`);
    } catch (e) { /* already exists */ }

    // Get accounts with calendar scope
    const filter = USER_ID
      ? `AND uga.user_id = '${USER_ID}'`
      : '';

    const { rows: accounts } = await pool.query(`
      SELECT uga.*, u.tenant_id
      FROM user_google_accounts uga
      JOIN users u ON u.id = uga.user_id
      WHERE uga.sync_enabled = true
        AND (uga.scopes::text LIKE '%calendar%' OR uga.scopes::text LIKE '%Calendar%')
        ${filter}
      ORDER BY uga.google_email
    `);

    if (accounts.length === 0) {
      console.log('\n  No accounts with calendar permission found.');
      console.log('  Users need to re-connect Google to grant calendar access.');
      return { records_in: 0, records_out: 0 };
    }

    console.log(`\n  Found ${accounts.length} account(s) with calendar access\n`);

    let totalEvents = 0, totalInteractions = 0, totalSignals = 0, totalErrors = 0;

    for (const account of accounts) {
      const stats = await syncAccount(account);
      totalEvents += stats.events_fetched;
      totalInteractions += stats.interactions;
      totalSignals += stats.signals;
      totalErrors += stats.errors;
    }

    console.log(`\n  ────────────────────────────────────`);
    console.log(`  📅 Events fetched:    ${totalEvents}`);
    console.log(`  🤝 Interactions:      ${totalInteractions}`);
    console.log(`  ⚡ Upcoming signals:  ${totalSignals}`);
    if (totalErrors) console.log(c.red(`  ✗ Errors:             ${totalErrors}`));

    return {
      records_in: totalEvents,
      records_out: totalInteractions + totalSignals,
      metadata: { interactions: totalInteractions, signals: totalSignals, errors: totalErrors }
    };
  });

  await pool.end();
}

main().catch(err => {
  console.error(c.red('FATAL: ' + err.message));
  process.exit(1);
});
