#!/usr/bin/env node
/**
 * Google Drive Auto-Sync
 *
 * Scans connected Google accounts for new/modified documents.
 * Ingests text content into external_documents as context_only (companion data).
 * Triggers classification pipeline for new documents.
 *
 * Designed to run on a 2-hour schedule.
 *
 * Usage:
 *   node scripts/sync_drive.js
 *   node scripts/sync_drive.js --full          # Re-scan all, not just recent
 *   node scripts/sync_drive.js --user-id <id>  # Sync specific user
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const FULL_SCAN = process.argv.includes('--full');
const USER_ID = (() => { const i = process.argv.indexOf('--user-id'); return i !== -1 ? process.argv[i + 1] : null; })();

const MIME_TYPES = [
  'application/vnd.google-apps.document',
  'application/vnd.google-apps.spreadsheet',
  'application/vnd.google-apps.presentation',
  'application/pdf',
];
const MAX_FILES_PER_ACCOUNT = 50;
const MAX_CONTENT_LENGTH = 50000;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════════════════════════════════════
// TOKEN MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════

async function getGoogleToken(account) {
  // Refresh if expired
  if (account.token_expires_at && new Date(account.token_expires_at) <= new Date(Date.now() + 5 * 60 * 1000)) {
    if (account.refresh_token && process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
      try {
        const res = await fetch('https://oauth2.googleapis.com/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            refresh_token: account.refresh_token,
            client_id: process.env.GOOGLE_CLIENT_ID,
            client_secret: process.env.GOOGLE_CLIENT_SECRET,
            grant_type: 'refresh_token'
          })
        });
        if (res.ok) {
          const tokens = await res.json();
          await pool.query(
            'UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2, updated_at = NOW() WHERE id = $3',
            [tokens.access_token, new Date(Date.now() + tokens.expires_in * 1000), account.id]
          );
          return tokens.access_token;
        }
      } catch (e) { /* fall through */ }
    }
  }
  return account.access_token;
}

// ═══════════════════════════════════════════════════════════════════════════════
// DRIVE FILE LISTING
// ═══════════════════════════════════════════════════════════════════════════════

async function listRecentFiles(token, sinceDate) {
  const mimeFilter = MIME_TYPES.map(m => `mimeType = '${m}'`).join(' or ');
  let query = `trashed = false and (${mimeFilter})`;
  if (sinceDate && !FULL_SCAN) {
    query += ` and modifiedTime > '${sinceDate}'`;
  }

  const allFiles = [];
  let pageToken = null;

  do {
    const params = new URLSearchParams({
      q: query,
      fields: 'nextPageToken,files(id,name,mimeType,modifiedTime,size,webViewLink)',
      pageSize: '100',
      orderBy: 'modifiedTime desc',
    });
    if (pageToken) params.set('pageToken', pageToken);

    const res = await fetch('https://www.googleapis.com/drive/v3/files?' + params, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!res.ok) {
      const err = await res.text();
      console.error('    Drive API error:', err.substring(0, 200));
      break;
    }
    const data = await res.json();
    allFiles.push(...(data.files || []));
    pageToken = data.nextPageToken;
  } while (pageToken && allFiles.length < MAX_FILES_PER_ACCOUNT);

  return allFiles.slice(0, MAX_FILES_PER_ACCOUNT);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONTENT EXTRACTION
// ═══════════════════════════════════════════════════════════════════════════════

async function extractContent(token, file) {
  let content = '';

  if (file.mimeType === 'application/vnd.google-apps.document' ||
      file.mimeType === 'application/vnd.google-apps.presentation') {
    const res = await fetch(
      `https://www.googleapis.com/drive/v3/files/${file.id}/export?mimeType=text/plain`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (res.ok) content = await res.text();

  } else if (file.mimeType === 'application/vnd.google-apps.spreadsheet') {
    const sheetsRes = await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${file.id}?fields=sheets.properties.title`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (sheetsRes.ok) {
      const sheetsData = await sheetsRes.json();
      const sheetNames = (sheetsData.sheets || []).map(s => s.properties.title);
      const parts = [];
      for (const name of sheetNames.slice(0, 10)) {
        const valRes = await fetch(
          `https://sheets.googleapis.com/v4/spreadsheets/${file.id}/values/${encodeURIComponent(name)}`,
          { headers: { Authorization: `Bearer ${token}` } }
        );
        if (valRes.ok) {
          const valData = await valRes.json();
          parts.push(`--- Sheet: ${name} ---\n${(valData.values || []).map(r => r.join('\t')).join('\n')}`);
        }
      }
      content = parts.join('\n\n');
    }

  } else if (file.mimeType === 'application/pdf') {
    content = `[PDF document: ${file.name}]`;
  }

  if (content.length > MAX_CONTENT_LENGTH) {
    content = content.substring(0, MAX_CONTENT_LENGTH) + '\n\n[... truncated]';
  }
  return content;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SYNC ACCOUNT
// ═══════════════════════════════════════════════════════════════════════════════

async function syncAccount(account) {
  const token = await getGoogleToken(account);
  if (!token) { console.log(`    No valid token for ${account.google_email}`); return { synced: 0, skipped: 0 }; }

  const tenantId = account.tenant_id || '00000000-0000-0000-0000-000000000001';

  // Get last sync time — use last classified doc or 30 days back
  let sinceDate;
  if (!FULL_SCAN) {
    const { rows: [last] } = await pool.query(
      `SELECT MAX(published_at) AS last_mod FROM external_documents WHERE source_name = 'Google Drive' AND uploaded_by_user_id = $1 AND tenant_id = $2`,
      [account.user_id, tenantId]
    );
    sinceDate = last?.last_mod
      ? new Date(new Date(last.last_mod).getTime() - 24 * 60 * 60 * 1000).toISOString() // 1 day overlap
      : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  }

  console.log(`  Scanning Drive for ${account.google_email}${sinceDate ? ` (since ${sinceDate.substring(0,10)})` : ' (full scan)'}...`);

  const files = await listRecentFiles(token, sinceDate);
  console.log(`    Found ${files.length} files`);

  let synced = 0, skipped = 0;

  for (const file of files) {
    const sourceUrlHash = crypto.createHash('md5').update('gdrive:' + file.id).digest('hex');

    // Check if already ingested with same modification time
    const { rows: existing } = await pool.query(
      `SELECT id, published_at FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2`,
      [sourceUrlHash, tenantId]
    );

    if (existing.length > 0) {
      const existingMod = existing[0].published_at ? new Date(existing[0].published_at).getTime() : 0;
      const fileMod = file.modifiedTime ? new Date(file.modifiedTime).getTime() : 0;
      if (existingMod >= fileMod) { skipped++; continue; } // Not modified since last ingest
    }

    try {
      const content = await extractContent(token, file);
      if (!content || content.length < 50) { skipped++; continue; }

      const sourceType = file.mimeType.includes('document') ? 'google_doc' :
                         file.mimeType.includes('spreadsheet') ? 'google_sheet' :
                         file.mimeType.includes('presentation') ? 'google_slides' : 'pdf';

      if (existing.length > 0) {
        // Update existing
        await pool.query(`
          UPDATE external_documents SET title = $1, content = $2, source_url = $3, published_at = $4, updated_at = NOW()
          WHERE id = $5
        `, [file.name, content, file.webViewLink, file.modifiedTime, existing[0].id]);
        // Reset classification so it gets re-classified
        await pool.query(`UPDATE external_documents SET classified_at = NULL WHERE id = $1`, [existing[0].id]);
      } else {
        // Insert new — always context_only
        await pool.query(`
          INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
            tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
          VALUES ($1, $2, 'Google Drive', $3, $4, $5, $6, $7, $8, 'context_only', NOW())
        `, [file.name, content, sourceType, file.webViewLink, sourceUrlHash,
            tenantId, account.user_id, file.modifiedTime]);
      }

      synced++;
      console.log(`    ✓ ${file.name.substring(0, 50)}`);
      await sleep(500); // Rate limit
    } catch (e) {
      console.error(`    ✗ ${file.name}: ${e.message}`);
    }
  }

  return { synced, skipped };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Google Drive Sync');
  console.log('═══════════════════════════════════════════════════\n');

  // Get connected accounts
  let query = `SELECT uga.*, u.tenant_id FROM user_google_accounts uga JOIN users u ON u.id = uga.user_id WHERE uga.access_token IS NOT NULL AND uga.sync_enabled = true`;
  const params = [];
  if (USER_ID) { query += ` AND uga.user_id = $1`; params.push(USER_ID); }

  const { rows: accounts } = await pool.query(query, params);

  if (accounts.length === 0) {
    console.log('  No connected Google accounts found');
    await pool.end();
    return;
  }

  console.log(`  ${accounts.length} connected account(s)\n`);

  let totalSynced = 0, totalSkipped = 0;

  for (const account of accounts) {
    try {
      const result = await syncAccount(account);
      totalSynced += result.synced;
      totalSkipped += result.skipped;
    } catch (e) {
      console.error(`  Account ${account.google_email} failed: ${e.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`  Drive sync complete: ${totalSynced} ingested, ${totalSkipped} skipped`);
  console.log('═══════════════════════════════════════════════════');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
