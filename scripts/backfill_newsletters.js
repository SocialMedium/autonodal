#!/usr/bin/env node
// ============================================================================
// Backfill newsletters from Gmail — last 6 months
// Searches Gmail for newsletter emails, extracts body, stores as documents
// ============================================================================

require('dotenv').config();
const { Pool } = require('pg');
const { google } = require('googleapis');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const LOOKBACK_DAYS = 180; // 6 months

const NEWSLETTER_QUERIES = [
  'from:newsletter subject:(market OR funding OR startup OR tech OR AI OR venture)',
  'from:digest subject:(market OR funding OR startup OR tech OR AI OR venture)',
  'from:cbinsights',
  'from:morningbrew',
  'from:substack',
  'from:pitchbook',
  'from:theinformation',
  'from:briefing',
  'from:crunchbase',
];

async function getGoogleClient(account) {
  const oauth2 = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET
  );

  // Refresh token if needed
  let accessToken = account.access_token;
  if (account.refresh_token) {
    oauth2.setCredentials({
      access_token: account.access_token,
      refresh_token: account.refresh_token
    });
    try {
      const { credentials } = await oauth2.refreshAccessToken();
      accessToken = credentials.access_token;
      await pool.query('UPDATE user_google_accounts SET access_token = $1 WHERE id = $2', [accessToken, account.id]);
    } catch (e) {
      console.log('  Token refresh failed:', e.message);
    }
  }

  oauth2.setCredentials({ access_token: accessToken });
  return google.gmail({ version: 'v1', auth: oauth2 });
}

function extractBody(payload) {
  if (!payload) return '';

  // Direct body
  if (payload.body?.data) {
    try { return Buffer.from(payload.body.data, 'base64').toString('utf8'); } catch (e) {}
  }

  // Multipart
  if (payload.parts) {
    // Prefer text/plain
    for (const part of payload.parts) {
      if (part.mimeType === 'text/plain' && part.body?.data) {
        try { return Buffer.from(part.body.data, 'base64').toString('utf8'); } catch (e) {}
      }
    }
    // Fallback to text/html stripped
    for (const part of payload.parts) {
      if (part.mimeType === 'text/html' && part.body?.data) {
        try {
          const html = Buffer.from(part.body.data, 'base64').toString('utf8');
          return html.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
                     .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
                     .replace(/<[^>]+>/g, ' ')
                     .replace(/&nbsp;/g, ' ')
                     .replace(/&amp;/g, '&')
                     .replace(/\s+/g, ' ')
                     .trim();
        } catch (e) {}
      }
    }
    // Recurse into nested parts
    for (const part of payload.parts) {
      if (part.parts) {
        const nested = extractBody(part);
        if (nested.length > 50) return nested;
      }
    }
  }
  return '';
}

function getHeader(headers, name) {
  return (headers || []).find(h => h.name?.toLowerCase() === name.toLowerCase())?.value || '';
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log(' Newsletter Backfill — Last 6 months');
  console.log('═══════════════════════════════════════════════════════════');

  // Get all Google accounts
  const { rows: accounts } = await pool.query(
    'SELECT * FROM user_google_accounts WHERE sync_enabled = true'
  );
  console.log(`Found ${accounts.length} Google account(s)\n`);

  let totalIngested = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  for (const account of accounts) {
    console.log(`\n📧 ${account.google_email}`);

    let gmail;
    try {
      gmail = await getGoogleClient(account);
    } catch (e) {
      console.log('  ❌ Auth failed:', e.message);
      continue;
    }

    const afterDate = new Date(Date.now() - LOOKBACK_DAYS * 86400000).toISOString().slice(0, 10).replace(/-/g, '/');

    for (const query of NEWSLETTER_QUERIES) {
      const fullQuery = `${query} after:${afterDate} -label:spam -label:trash`;
      console.log(`  🔍 ${query.split(' ')[0]}...`);

      let messageIds = [];
      let pageToken = null;

      try {
        do {
          const res = await gmail.users.messages.list({
            userId: 'me',
            q: fullQuery,
            maxResults: 100,
            pageToken
          });
          messageIds.push(...(res.data.messages || []).map(m => m.id));
          pageToken = res.data.nextPageToken;
          await new Promise(r => setTimeout(r, 200));
        } while (pageToken && messageIds.length < 500);
      } catch (e) {
        console.log(`    ❌ Search failed: ${e.message}`);
        continue;
      }

      console.log(`    Found ${messageIds.length} messages`);

      for (const msgId of messageIds) {
        try {
          // Check if already ingested
          const hash = crypto.createHash('md5').update(msgId).digest('hex');
          const { rows: exists } = await pool.query(
            'SELECT id FROM external_documents WHERE source_url_hash = $1', [hash]
          );
          if (exists.length) { totalSkipped++; continue; }

          // Fetch full message
          const msg = await gmail.users.messages.get({
            userId: 'me',
            id: msgId,
            format: 'full'
          });

          const headers = msg.data.payload?.headers || [];
          const subject = getHeader(headers, 'Subject').slice(0, 255) || 'Newsletter';
          const from = getHeader(headers, 'From');
          const date = getHeader(headers, 'Date');
          const senderName = from.replace(/<.*>/, '').replace(/"/g, '').trim() || 'Newsletter';

          const body = extractBody(msg.data.payload);
          if (body.length < 100) { totalSkipped++; continue; }

          let publishedAt;
          try { publishedAt = new Date(date).toISOString(); } catch (e) { publishedAt = new Date().toISOString(); }

          await pool.query(`
            INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
              tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
            VALUES ($1, $2, $3, 'newsletter', $4, $5, $6, $7, $8, 'pending', NOW())
            ON CONFLICT (source_url_hash) DO NOTHING
          `, [
            subject, body.slice(0, 50000), senderName,
            `https://mail.google.com/mail/u/0/#inbox/${msgId}`,
            hash, TENANT_ID, account.user_id, publishedAt
          ]);

          totalIngested++;
          if (totalIngested % 50 === 0) console.log(`    Progress: ${totalIngested} ingested...`);

          await new Promise(r => setTimeout(r, 100)); // Rate limit
        } catch (e) {
          totalErrors++;
        }
      }
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(` Ingested: ${totalIngested}`);
  console.log(` Skipped (already exists): ${totalSkipped}`);
  console.log(` Errors: ${totalErrors}`);
  console.log('═══════════════════════════════════════════════════════════');
  console.log('\nThese will be embedded + signal-scanned on the next pipeline run.');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
