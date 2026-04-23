#!/usr/bin/env node
/**
 * Gmail Bulk Importer — Phase 1: Just get the emails in
 * 
 * Fetches all email headers and stores them. No matching, no signals.
 * Phase 2 (separate script) does batch matching + signal extraction.
 * 
 * Usage:
 *   node scripts/gmail_import.js              # Last 90 days
 *   node scripts/gmail_import.js --full       # All email
 *   node scripts/gmail_import.js --days 30    # Last 30 days
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3
});

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── Get valid access token ───
const { encryptToken, decryptToken } = require('../lib/crypto');
async function getToken() {
  const account = await pool.query('SELECT * FROM user_google_accounts WHERE sync_enabled = true LIMIT 1');
  if (account.rows.length === 0) throw new Error('No Gmail account connected');

  const acct = account.rows[0];
  acct.access_token = decryptToken(acct.access_token);
  acct.refresh_token = decryptToken(acct.refresh_token);
  const now = new Date();
  const expires = new Date(acct.token_expires_at);

  if (expires <= new Date(now.getTime() + 5 * 60 * 1000)) {
    console.log('  🔄 Refreshing token...');
    const resp = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        refresh_token: acct.refresh_token,
        client_id: process.env.GOOGLE_CLIENT_ID,
        client_secret: process.env.GOOGLE_CLIENT_SECRET,
        grant_type: 'refresh_token'
      })
    });
    if (!resp.ok) throw new Error(`Token refresh failed: ${await resp.text()}`);
    const tokens = await resp.json();

    await pool.query(
      'UPDATE user_google_accounts SET access_token=$1, token_expires_at=$2 WHERE id=$3',
      [encryptToken(tokens.access_token), new Date(Date.now() + tokens.expires_in * 1000), acct.id]
    );
    return { token: tokens.access_token, userId: acct.user_id, email: acct.google_email };
  }

  return { token: acct.access_token, userId: acct.user_id, email: acct.google_email };
}

// ─── Gmail API call ───
async function gmail(token, endpoint, params = {}) {
  const url = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`);
  Object.entries(params).forEach(([k, v]) => { if (v != null) url.searchParams.set(k, v); });
  
  const resp = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
  
  if (resp.status === 429) {
    console.log('  ⏳ Rate limited, waiting 10s...');
    await sleep(10000);
    return gmail(token, endpoint, params);
  }
  if (!resp.ok) throw new Error(`Gmail ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
  return resp.json();
}

// ─── Extract email address from header ───
function extractEmail(s) {
  if (!s) return null;
  const m = s.match(/<([^>]+)>/) || s.match(/([^\s<,]+@[^\s>,]+)/);
  return m ? m[1].toLowerCase() : null;
}

function extractAllEmails(s) {
  if (!s) return [];
  return (s.match(/[^\s<,]+@[^\s>,]+/g) || []).map(e => e.toLowerCase());
}

function getHeader(headers, name) {
  const h = (headers || []).find(h => h.name.toLowerCase() === name.toLowerCase());
  return h ? h.value : null;
}

// ─── Main ───
async function main() {
  const args = process.argv.slice(2);
  const fullSync = args.includes('--full');
  const daysIdx = args.indexOf('--days');
  const days = daysIdx >= 0 ? parseInt(args[daysIdx + 1]) : 90;

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   Gmail Bulk Import — Phase 1                             ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  const { token, userId, email: userEmail } = await getToken();
  console.log(`  Account: ${userEmail}`);
  console.log(`  User ID: ${userId}\n`);

  // Test a single insert first
  console.log('  Testing insert...');
  try {
    await pool.query(`
      INSERT INTO interactions (
        user_id, interaction_type, direction, subject, summary,
        channel, source, email_message_id, email_thread_id,
        email_subject, email_snippet, email_from, email_to,
        email_labels, email_has_attachments, interaction_at, visibility, owner_user_id
      ) VALUES ($1, 'email', 'inbound', 'TEST', 'TEST',
        'email', 'gmail_sync', 'test_delete_me', 'test_thread',
        'TEST', 'TEST', 'test@test.com', $2,
        $3, false, NOW(), 'team', $1)
    `, [userId, ['test@test.com'], ['INBOX']]);
    
    // Clean up test
    await pool.query("DELETE FROM interactions WHERE email_message_id = 'test_delete_me'");
    console.log('  ✅ Insert works!\n');
  } catch (e) {
    console.error('  ❌ Insert test failed:', e.message);
    console.error('  Fix the schema issue before proceeding.');
    await pool.end();
    return;
  }

  // Build query
  const query = fullSync ? '' : `newer_than:${days}d`;
  console.log(`  Query: "${query || 'all mail'}"`);

  let pageToken = null;
  let totalStored = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let page = 0;
  const maxPages = fullSync ? 200 : 50; // 20K full, 5K recent

  while (page < maxPages) {
    page++;
    
    // List messages
    const list = await gmail(token, 'messages', {
      q: query || undefined,
      maxResults: 100,
      pageToken
    });

    if (!list.messages || list.messages.length === 0) {
      console.log('  No more messages');
      break;
    }

    // Fetch details in batches of 20
    for (let i = 0; i < list.messages.length; i += 20) {
      const batch = list.messages.slice(i, i + 20);
      
      const details = await Promise.allSettled(
        batch.map(m => gmail(token, `messages/${m.id}`, {
          format: 'metadata',
          metadataHeaders: 'From,To,Cc,Subject,Date,In-Reply-To'
        }))
      );

      for (const result of details) {
        if (result.status === 'rejected') { totalErrors++; continue; }
        const msg = result.value;
        
        const headers = msg.payload?.headers || [];
        const from = getHeader(headers, 'From');
        const to = getHeader(headers, 'To');
        const subject = getHeader(headers, 'Subject');
        const dateStr = getHeader(headers, 'Date');
        const inReplyTo = getHeader(headers, 'In-Reply-To');
        
        const fromEmail = extractEmail(from);
        const toEmails = extractAllEmails(to);
        const ccEmails = extractAllEmails(getHeader(headers, 'Cc'));
        const allRecipients = [...toEmails, ...ccEmails];
        const gmailLabels = msg.labelIds || [];
        
        const isSent = gmailLabels.includes('SENT');
        const direction = isSent ? 'outbound' : 'inbound';
        const emailDate = dateStr ? new Date(dateStr) : new Date(parseInt(msg.internalDate));
        
        // Skip invalid dates
        if (isNaN(emailDate.getTime())) continue;

        try {
          const subj = (subject || '(no subject)').slice(0, 500);
          const snip = (msg.snippet || '').slice(0, 500);
          
          await pool.query(`
            INSERT INTO interactions (
              user_id, interaction_type, direction, subject, summary,
              channel, source, email_message_id, email_thread_id,
              email_subject, email_snippet, email_from, email_to,
              email_labels, email_has_attachments, interaction_at,
              visibility, owner_user_id
            ) VALUES ($1, 'email', $2, $3, $4,
              'email', 'gmail_sync', $5, $6,
              $7, $8, $9, $10,
              $11, $12, $13,
              'team', $14)
          `, [
            userId,
            direction,
            subj,
            snip,
            msg.id,
            msg.threadId,
            subj,             // email_subject (separate param)
            snip,             // email_snippet (separate param)
            fromEmail || '',
            allRecipients,
            gmailLabels,
            msg.payload?.parts?.some(p => p.filename?.length > 0) || false,
            emailDate,
            userId            // owner_user_id (separate param)
          ]);
          totalStored++;
        } catch (e) {
          if (e.message.includes('duplicate')) {
            totalSkipped++;
          } else {
            totalErrors++;
            if (totalErrors <= 5) console.log('    ⚠️', e.message.slice(0, 100));
          }
        }
      }

      await sleep(50); // Small rate limit between fetches
    }

    console.log(`  Page ${page}: ${totalStored} stored | ${totalSkipped} dupes | ${totalErrors} errors`);

    pageToken = list.nextPageToken;
    if (!pageToken) break;
    await sleep(100);
  }

  // Update sync timestamp
  await pool.query('UPDATE user_google_accounts SET last_sync_at = NOW()');

  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  IMPORT COMPLETE`);
  console.log(`  Stored:  ${totalStored}`);
  console.log(`  Dupes:   ${totalSkipped}`);
  console.log(`  Errors:  ${totalErrors}`);
  console.log(`${'═'.repeat(50)}`);

  // Quick stats
  const stats = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(DISTINCT email_thread_id) as threads,
      COUNT(DISTINCT email_from) as unique_senders,
      MIN(interaction_at) as oldest,
      MAX(interaction_at) as newest
    FROM interactions WHERE source = 'gmail_sync'
  `);
  const s = stats.rows[0];
  console.log(`\n  ${s.total} emails across ${s.threads} threads`);
  console.log(`  ${s.unique_senders} unique senders`);
  console.log(`  Range: ${s.oldest?.toISOString().slice(0,10)} → ${s.newest?.toISOString().slice(0,10)}`);

  console.log('\n  Next: node scripts/gmail_match.js  (match + signals)');
  
  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
