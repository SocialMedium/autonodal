#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/sync_contacts_delta.js
//
// Delta sync of Google Contacts → people table enrichment.
// Runs every 6 hours. Uses Google People API with syncToken for efficiency.
//
// FILL-BLANK ONLY: Never overwrites intentionally-set data.
// Only fills fields that are NULL or empty.
//
// Usage:
//   node scripts/sync_contacts_delta.js
//   node scripts/sync_contacts_delta.js --user-id <uuid>
//   node scripts/sync_contacts_delta.js --full-scan
//   node scripts/sync_contacts_delta.js --dry-run
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool }   = require('pg');
const { google } = require('googleapis');
const { runJob, logProgress, withRetry, sleep } = require('../lib/job_runner');

// ─── CLI flags ───────────────────────────────────────────────────────────────
const DRY_RUN   = process.argv.includes('--dry-run');
const FULL_SCAN = process.argv.includes('--full-scan');
const USER_ID   = (() => {
  const idx = process.argv.indexOf('--user-id');
  return idx !== -1 ? process.argv[idx + 1] : null;
})();

// ─── Config ──────────────────────────────────────────────────────────────────
const BATCH_SIZE     = 100;
const RATE_LIMIT_MS  = 110; // ~9 requests/sec to stay under 10/sec limit
const PAGE_SIZE      = 200; // Max allowed by People API

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
// CONTACT PARSING
// ═══════════════════════════════════════════════════════════════════════════

function extractLinkedInUrl(urls) {
  if (!urls) return null;
  const li = urls.find(u =>
    (u.value || '').toLowerCase().includes('linkedin.com') ||
    (u.type  || '').toLowerCase().includes('linkedin')
  );
  return li ? li.value : null;
}

function isDefaultPhoto(url) {
  if (!url) return true;
  return /silhouette|default|placeholder|anonymous/i.test(url);
}

function parseContact(person) {
  const emails = (person.emailAddresses || []).map(e => e.value?.toLowerCase()).filter(Boolean);
  const phones = person.phoneNumbers || [];
  const urls   = person.urls || [];
  const orgs   = person.organizations || [];
  const bios   = person.biographies || [];
  const photos = person.photos || [];

  return {
    emails,
    primaryEmail:   emails[0] || null,
    altEmail:       emails[1] || null,
    phone:          phones[0]?.value || null,
    linkedinUrl:    extractLinkedInUrl(urls),
    bio:            bios[0]?.value?.slice(0, 500) || null,
    currentCompany: orgs[0]?.name || null,
    currentTitle:   orgs[0]?.title || null,
    photoUrl:       (!isDefaultPhoto(photos[0]?.url)) ? photos[0]?.url : null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// PERSON MATCHING
// ═══════════════════════════════════════════════════════════════════════════

async function findPersonByEmails(emails, linkedinUrl) {
  if (emails.length === 0 && !linkedinUrl) return null;

  // Match by any email
  if (emails.length > 0) {
    const placeholders = emails.map((_, i) => `$${i + 1}`).join(', ');
    const { rows } = await pool.query(
      `SELECT id FROM people
       WHERE lower(email) IN (${placeholders})
          OR lower(email_alt) IN (${placeholders})
       LIMIT 1`,
      emails
    );
    if (rows.length > 0) return rows[0].id;
  }

  // Match by LinkedIn URL
  if (linkedinUrl) {
    const norm = linkedinUrl.toLowerCase().replace(/^https?:\/\/(www\.)?linkedin\.com/, '').replace(/\/$/, '');
    const { rows } = await pool.query(
      `SELECT id FROM people WHERE lower(linkedin_url) LIKE $1 LIMIT 1`,
      [`%${norm}%`]
    );
    if (rows.length > 0) return rows[0].id;
  }

  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// ENRICHMENT
// ═══════════════════════════════════════════════════════════════════════════

const fieldCounters = {
  phone:        0,
  email_alt:    0,
  linkedin_url: 0,
  bio:          0,
  company:      0,
  photo:        0,
};

async function enrichPerson(personId, contact, dryRun) {
  // Build SET clauses for fill-blank only
  const updates = [];
  const values  = [personId];
  let   idx     = 2;

  // Fetch current values to check what's already set
  const { rows } = await pool.query(
    `SELECT phone, email_alt, linkedin_url, bio, current_company_name, current_title, profile_photo_url
     FROM people WHERE id = $1`,
    [personId]
  );
  if (rows.length === 0) return;
  const current = rows[0];

  if (!current.phone && contact.phone) {
    updates.push(`phone = $${idx++}`);
    values.push(contact.phone);
    fieldCounters.phone++;
  }
  if (!current.email_alt && contact.altEmail && contact.altEmail !== current.email) {
    updates.push(`email_alt = $${idx++}`);
    values.push(contact.altEmail);
    fieldCounters.email_alt++;
  }
  if (!current.linkedin_url && contact.linkedinUrl) {
    updates.push(`linkedin_url = $${idx++}`);
    values.push(contact.linkedinUrl);
    fieldCounters.linkedin_url++;
  }
  if (!current.bio && contact.bio) {
    updates.push(`bio = $${idx++}`);
    values.push(contact.bio);
    fieldCounters.bio++;
  }
  if (!current.current_company_name && contact.currentCompany) {
    updates.push(`current_company_name = $${idx++}`);
    values.push(contact.currentCompany);
    if (!current.current_title && contact.currentTitle) {
      updates.push(`current_title = $${idx++}`);
      values.push(contact.currentTitle);
    }
    fieldCounters.company++;
  }
  if (!current.profile_photo_url && contact.photoUrl) {
    updates.push(`profile_photo_url = $${idx++}`);
    values.push(contact.photoUrl);
    fieldCounters.photo++;
  }

  // Always update the sync timestamp
  updates.push(`contacts_last_updated_at = NOW()`);

  if (updates.length === 1 && !dryRun) {
    // Only the timestamp — still worth recording the sync
    await pool.query(
      `UPDATE people SET contacts_last_updated_at = NOW() WHERE id = $1`,
      [personId]
    );
    return;
  }

  if (dryRun) return;

  await pool.query(
    `UPDATE people SET ${updates.join(', ')} WHERE id = $1`,
    values
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// FETCH CONTACTS
// ═══════════════════════════════════════════════════════════════════════════

async function fetchContacts(people, syncToken) {
  const contacts = [];
  let pageToken   = null;
  let newSyncToken = null;

  const requestFields = [
    'emailAddresses',
    'phoneNumbers',
    'urls',
    'organizations',
    'biographies',
    'photos',
    'names',
  ].join(',');

  if (syncToken && !FULL_SCAN) {
    // Delta fetch
    try {
      let hasMore = true;
      while (hasMore) {
        const res = await withRetry(() =>
          people.people.connections.list({
            resourceName:      'people/me',
            personFields:      requestFields,
            syncToken,
            pageToken:         pageToken || undefined,
            requestSyncToken:  true,
          })
        );

        const conns = res.data.connections || [];
        contacts.push(...conns);
        newSyncToken = res.data.nextSyncToken || newSyncToken;
        pageToken    = res.data.nextPageToken;
        hasMore      = !!pageToken;
        await sleep(RATE_LIMIT_MS);
      }
    } catch (err) {
      if (err.message?.includes('sync token') || err.code === 400) {
        console.log(c.yellow('  Sync token expired — falling back to full fetch'));
        return fetchContacts(people, null); // Recursive full fetch
      }
      throw err;
    }
  } else {
    // Full fetch
    let hasMore = true;
    while (hasMore) {
      const res = await withRetry(() =>
        people.people.connections.list({
          resourceName:     'people/me',
          personFields:     requestFields,
          pageSize:         PAGE_SIZE,
          pageToken:        pageToken || undefined,
          requestSyncToken: true,
        })
      );

      const conns = res.data.connections || [];
      contacts.push(...conns);
      newSyncToken = res.data.nextSyncToken || newSyncToken;
      pageToken    = res.data.nextPageToken;
      hasMore      = !!pageToken;
      await sleep(RATE_LIMIT_MS);
    }
  }

  return { contacts, newSyncToken };
}

// ═══════════════════════════════════════════════════════════════════════════
// SYNC ONE ACCOUNT
// ═══════════════════════════════════════════════════════════════════════════

async function syncAccount(account) {
  console.log(c.yellow(`\n  ▶ Syncing contacts: ${account.google_email}`));

  let auth;
  try {
    auth = await getOAuthClient(account);
  } catch (err) {
    console.log(c.red(`  ✗ Auth failed: ${err.message} — skipping`));
    return { contactsFetched: 0, peopleMatched: 0, peopleUpdated: 0 };
  }

  const people = google.people({ version: 'v1', auth });

  const syncToken = (!FULL_SCAN && account.contacts_sync_token) || null;
  const mode      = syncToken ? 'delta' : 'full';
  console.log(c.dim(`  Mode: ${mode} fetch`));

  const { contacts, newSyncToken } = await fetchContacts(people, syncToken);
  console.log(c.blue(`  Fetched ${contacts.length} contact(s)`));

  let matched = 0;
  let updated = 0;

  for (let i = 0; i < contacts.length; i++) {
    logProgress('contacts_sync', i + 1, contacts.length, 'enriching people');

    const contact  = parseContact(contacts[i]);
    const personId = await findPersonByEmails(contact.emails, contact.linkedinUrl);

    if (!personId) continue; // Skip — don't auto-create from Contacts alone
    matched++;

    await enrichPerson(personId, contact, DRY_RUN);
    updated++;

    await sleep(10); // Small delay to avoid DB overload
  }

  // Save new sync token
  if (!DRY_RUN && newSyncToken) {
    await pool.query(
      `UPDATE user_google_accounts SET
         contacts_sync_token    = $2,
         contacts_last_sync_at  = NOW()
       WHERE id = $1`,
      [account.id, newSyncToken]
    );
  }

  return {
    contactsFetched: contacts.length,
    peopleMatched:   matched,
    peopleUpdated:   updated,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('');
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log(c.blue('  MitchelLake — Google Contacts Delta Sync            '));
  if (DRY_RUN)   console.log(c.yellow('  ⚠  DRY RUN — no data will be written               '));
  if (FULL_SCAN) console.log(c.yellow('  ↩  FULL SCAN — ignoring sync token                 '));
  console.log(c.blue('═══════════════════════════════════════════════════════'));
  console.log('');

  if (!process.env.DATABASE_URL)         { console.log(c.red('✗ DATABASE_URL not set')); process.exit(1); }
  if (!process.env.GOOGLE_CLIENT_ID)     { console.log(c.red('✗ GOOGLE_CLIENT_ID not set')); process.exit(1); }
  if (!process.env.GOOGLE_CLIENT_SECRET) { console.log(c.red('✗ GOOGLE_CLIENT_SECRET not set')); process.exit(1); }

  // Reset counters
  Object.keys(fieldCounters).forEach(k => fieldCounters[k] = 0);

  await runJob(pool, 'contacts_delta_sync', async () => {
    let query  = `SELECT * FROM user_google_accounts WHERE access_token IS NOT NULL`;
    const params = [];
    if (USER_ID) {
      query += ` AND user_id = $1`;
      params.push(USER_ID);
    }

    const { rows: accounts } = await pool.query(query, params);

    // Decrypt tokens from at-rest AES-256-GCM storage
    const { decryptToken } = require('../lib/crypto');
    accounts.forEach(a => {
      if (a.access_token) a.access_token = decryptToken(a.access_token);
      if (a.refresh_token) a.refresh_token = decryptToken(a.refresh_token);
    });

    if (accounts.length === 0) {
      console.log(c.yellow('  No connected Google accounts found'));
      return { records_in: 0, records_out: 0 };
    }

    console.log(c.green(`✓ Found ${accounts.length} connected account(s)`));

    let totalFetched = 0;
    let totalMatched = 0;
    let totalUpdated = 0;

    for (const account of accounts) {
      try {
        const result = await syncAccount(account);
        totalFetched += result.contactsFetched;
        totalMatched += result.peopleMatched;
        totalUpdated += result.peopleUpdated;
      } catch (err) {
        console.log(c.red(`  ✗ Account ${account.google_email} failed: ${err.message}`));
      }
    }

    console.log('');
    console.log(c.blue('═══════════════════════════════════════════════════════'));
    console.log(c.blue('  📇 CONTACTS DELTA SYNC COMPLETE'));
    console.log(c.blue('═══════════════════════════════════════════════════════'));
    console.log(`  Contacts fetched:     ${totalFetched.toLocaleString()}`);
    console.log(`  People matched:       ${totalMatched.toLocaleString()}`);
    console.log(`  People skipped:       ${(totalFetched - totalMatched).toLocaleString()} (no match)`);
    console.log(`  Fields updated:`);
    console.log(`    Phone:              ${fieldCounters.phone}`);
    console.log(`    Email (alt):        ${fieldCounters.email_alt}`);
    console.log(`    LinkedIn URL:       ${fieldCounters.linkedin_url}`);
    console.log(`    Bio:                ${fieldCounters.bio}`);
    console.log(`    Company/Title:      ${fieldCounters.company}`);
    console.log(`    Photo:              ${fieldCounters.photo}`);
    if (DRY_RUN) console.log(c.yellow('\n  ⚠  DRY RUN — re-run without --dry-run to write data'));
    console.log(c.blue('═══════════════════════════════════════════════════════'));

    return {
      records_in:  totalFetched,
      records_out: totalUpdated,
      metadata:    { ...fieldCounters },
    };
  });

  await pool.end();
}

main().catch(err => {
  console.error(c.red(`\nFatal error: ${err.message}`));
  console.error(err);
  process.exit(1);
});
