#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/encrypt_existing_tokens.js — One-time migration to encrypt OAuth tokens
//
// Prerequisites:
//   1. Set ENCRYPTION_KEY in .env or Railway env vars (64 hex chars)
//   2. Run: node scripts/encrypt_existing_tokens.js
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { encryptToken } = require('../lib/crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function migrate() {
  if (!process.env.ENCRYPTION_KEY) {
    console.error('ENCRYPTION_KEY not set — this script must run in an environment where the same key is configured as the app, or the encrypted tokens will be unreadable by the server.');
    console.error('Run via: railway run node scripts/encrypt_existing_tokens.js');
    process.exit(1);
  }

  console.log('Encrypting OAuth tokens...\n');

  const { rows } = await pool.query(
    `SELECT id, google_email, access_token, refresh_token FROM user_google_accounts WHERE access_token IS NOT NULL`
  );

  let migrated = 0;
  let skipped = 0;

  for (const row of rows) {
    // Detect plaintext: Google tokens start with ya29. or look like JWTs
    const isPlaintext = /^ya29\.|^eyJ/.test(row.access_token);
    if (!isPlaintext) {
      console.log('  SKIP', row.google_email, '(already encrypted)');
      skipped++;
      continue;
    }

    const encAccess = encryptToken(row.access_token);
    const encRefresh = row.refresh_token ? encryptToken(row.refresh_token) : null;

    await pool.query(
      `UPDATE user_google_accounts SET access_token = $1, refresh_token = $2 WHERE id = $3`,
      [encAccess, encRefresh, row.id]
    );

    console.log('  OK', row.google_email);
    migrated++;
  }

  console.log(`\nDone: ${migrated} encrypted, ${skipped} skipped`);
  pool.end();
}

migrate().catch(e => { console.error(e); process.exit(1); });
