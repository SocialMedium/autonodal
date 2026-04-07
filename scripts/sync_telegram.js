#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/sync_telegram.js
//
// Telegram MTProto sync → interactions + team_proximity.
// Mirrors sync_gmail.js pattern: delta sync via message IDs, name matching,
// proximity scoring by message frequency.
//
// PRIVACY-BY-DESIGN:
//   Stores: sender name, message direction, timestamp, chat context.
//   Does NOT store message body text. Only metadata for relationship scoring.
//
// Usage:
//   node scripts/sync_telegram.js
//   node scripts/sync_telegram.js --user-id <uuid>
//   node scripts/sync_telegram.js --full-scan
//   node scripts/sync_telegram.js --dry-run
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool } = require('pg');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Api } = require('telegram/tl');
const { runJob, withRetry, sleep } = require('../lib/job_runner');

// ─── CLI flags ───────────────────────────────────────────────────────────────
const DRY_RUN   = process.argv.includes('--dry-run');
const FULL_SCAN = process.argv.includes('--full-scan');
const USER_ID   = (() => {
  const idx = process.argv.indexOf('--user-id');
  return idx !== -1 ? process.argv[idx + 1] : null;
})();

// ─── Config ──────────────────────────────────────────────────────────────────
const LOOKBACK_DAYS  = parseInt(process.env.TELEGRAM_LOOKBACK_DAYS || '90');
const MAX_MESSAGES   = parseInt(process.env.TELEGRAM_MAX_MESSAGES || '5000');
const MAX_DIALOGS    = parseInt(process.env.TELEGRAM_MAX_DIALOGS || '100');
const RATE_LIMIT_MS  = 300; // Telegram is strict on flood control
const API_ID         = parseInt(process.env.TELEGRAM_API_ID || '0');
const API_HASH       = process.env.TELEGRAM_API_HASH || '';

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
// PERSON LOOKUP CACHE
// ═══════════════════════════════════════════════════════════════════════════

const nameCache = new Map();
const phoneCache = new Map();

async function loadPeopleIndex(tenantId) {
  const { rows } = await pool.query(
    `SELECT id, full_name, phone FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL`,
    [tenantId]
  );
  for (const p of rows) {
    const norm = p.full_name.toLowerCase().trim();
    if (!nameCache.has(norm)) nameCache.set(norm, p);
    if (p.phone) {
      const normPhone = p.phone.replace(/[\s\-()]/g, '');
      phoneCache.set(normPhone, p);
      // Also store last 10 digits for fuzzy match
      if (normPhone.length >= 10) phoneCache.set(normPhone.slice(-10), p);
    }
  }
  return rows.length;
}

function findPerson(name, phone) {
  if (name) {
    const norm = name.toLowerCase().trim();
    if (nameCache.has(norm)) return nameCache.get(norm);
    // Try first+last name combinations
    const parts = norm.split(/\s+/);
    if (parts.length >= 2) {
      // Try "first last"
      const fl = parts[0] + ' ' + parts[parts.length - 1];
      if (nameCache.has(fl)) return nameCache.get(fl);
    }
  }
  if (phone) {
    const normPhone = phone.replace(/[\s\-()]/g, '');
    if (phoneCache.has(normPhone)) return phoneCache.get(normPhone);
    if (normPhone.length >= 10 && phoneCache.has(normPhone.slice(-10))) {
      return phoneCache.get(normPhone.slice(-10));
    }
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// TELEGRAM CLIENT
// ═══════════════════════════════════════════════════════════════════════════

async function createClient(sessionString) {
  const client = new TelegramClient(
    new StringSession(sessionString),
    API_ID,
    API_HASH,
    { connectionRetries: 3, retryDelay: 1000 }
  );
  await client.connect();
  return client;
}

// ═══════════════════════════════════════════════════════════════════════════
// SYNC ONE ACCOUNT
// ═══════════════════════════════════════════════════════════════════════════

async function syncAccount(account) {
  console.log(c.yellow(`\n  ▶ Syncing Telegram: ${account.phone} (@${account.username || 'no-username'})`));

  let client;
  try {
    client = await createClient(account.session_string);
  } catch (err) {
    console.log(c.red(`  ✗ Auth failed: ${err.message} — session may have expired`));
    return { dialogs: 0, messages: 0, matched: 0, interactions: 0, errors: 1 };
  }

  const me = await client.getMe();
  const myId = me.id.toString();

  // Get tenant_id
  const { rows: [user] } = await pool.query(
    `SELECT tenant_id FROM users WHERE id = $1`, [account.user_id]
  );
  if (!user) {
    await client.disconnect();
    return { dialogs: 0, messages: 0, matched: 0, interactions: 0, errors: 1 };
  }
  const tenantId = user.tenant_id;

  // Load people index for this tenant
  const peopleCount = await loadPeopleIndex(tenantId);
  console.log(`    📋 ${peopleCount} people in index`);

  // Calculate cutoff date
  const cutoffDate = new Date(Date.now() - LOOKBACK_DAYS * 86400000);
  const minMessageId = FULL_SCAN ? 0 : (account.last_message_id || 0);

  // Get dialogs (chats)
  let dialogs;
  try {
    dialogs = await client.getDialogs({ limit: MAX_DIALOGS });
  } catch (err) {
    console.log(c.red(`  ✗ Failed to fetch dialogs: ${err.message}`));
    await client.disconnect();
    return { dialogs: 0, messages: 0, matched: 0, interactions: 0, errors: 1 };
  }

  // Filter to 1:1 private chats (skip groups, channels, bots)
  const privateChatEntities = dialogs.filter(d => {
    if (!d.entity) return false;
    const className = d.entity.className;
    return className === 'User' && !d.entity.bot;
  });

  console.log(`    💬 ${privateChatEntities.length} private chats (of ${dialogs.length} total)`);

  let totalMessages = 0;
  let totalMatched = 0;
  let totalInteractions = 0;
  let maxMessageId = minMessageId;

  // Per-contact message counts for proximity
  const contactStats = new Map(); // personId → { count, lastDate, name }

  for (const dialog of privateChatEntities) {
    const entity = dialog.entity;
    const contactName = [entity.firstName, entity.lastName].filter(Boolean).join(' ');
    const contactPhone = entity.phone ? '+' + entity.phone : null;

    // Try to match to a person in the DB
    const person = findPerson(contactName, contactPhone);

    try {
      // Fetch messages from this chat
      const messages = await client.getMessages(entity, {
        limit: Math.min(200, MAX_MESSAGES - totalMessages),
        minId: minMessageId,
      });

      if (!messages.length) continue;

      // Filter to messages within lookback window
      const relevantMsgs = messages.filter(m => {
        if (!m.date) return false;
        const msgDate = new Date(m.date * 1000);
        return msgDate >= cutoffDate;
      });

      if (!relevantMsgs.length) continue;

      totalMessages += relevantMsgs.length;

      // Track max message ID for delta sync
      for (const msg of relevantMsgs) {
        if (msg.id > maxMessageId) maxMessageId = msg.id;
      }

      if (person) {
        totalMatched++;

        // Count messages by direction
        let inbound = 0, outbound = 0;
        let lastDate = null;
        for (const msg of relevantMsgs) {
          const isOutbound = msg.out;
          if (isOutbound) outbound++; else inbound++;
          const msgDate = new Date(msg.date * 1000);
          if (!lastDate || msgDate > lastDate) lastDate = msgDate;
        }

        // Aggregate for proximity
        const existing = contactStats.get(person.id) || { count: 0, lastDate: null, name: contactName };
        existing.count += relevantMsgs.length;
        if (!existing.lastDate || (lastDate && lastDate > existing.lastDate)) existing.lastDate = lastDate;
        contactStats.set(person.id, existing);

        // Store interaction (one per chat, not per message — privacy + efficiency)
        if (!DRY_RUN) {
          const externalId = `tg:${account.telegram_user_id}:${entity.id}:${LOOKBACK_DAYS}d`;
          try {
            await pool.query(
              `INSERT INTO interactions
                 (person_id, user_id, interaction_type, direction, subject,
                  channel, source, external_id, interaction_at, metadata)
               VALUES ($1, $2, 'telegram_sync', $3, $4, 'telegram', 'telegram_sync', $5, $6, $7)
               ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO UPDATE SET
                 subject = EXCLUDED.subject, interaction_at = EXCLUDED.interaction_at,
                 metadata = EXCLUDED.metadata, updated_at = NOW()`,
              [
                person.id,
                account.user_id,
                outbound >= inbound ? 'outbound' : 'inbound',
                `Telegram: ${relevantMsgs.length} messages (${inbound} in, ${outbound} out)`,
                externalId,
                lastDate || new Date(),
                JSON.stringify({ message_count: relevantMsgs.length, inbound, outbound, contact_name: contactName, contact_phone: contactPhone }),
              ]
            );
            totalInteractions++;
          } catch (e) {
            console.log(c.dim(`    ⚠ Interaction error for ${contactName}: ${e.message}`));
          }
        }
      }

      // Rate limit between chats
      if (totalMessages >= MAX_MESSAGES) break;
      await sleep(RATE_LIMIT_MS);

    } catch (err) {
      if (err.errorMessage === 'FLOOD_WAIT') {
        const wait = (err.seconds || 30) * 1000;
        console.log(c.yellow(`    ⏳ Flood wait: ${err.seconds}s`));
        await sleep(wait);
      } else {
        console.log(c.dim(`    ⚠ Chat error (${contactName}): ${err.message}`));
      }
    }
  }

  // Update team_proximity for matched contacts
  if (!DRY_RUN && contactStats.size > 0) {
    for (const [personId, stats] of contactStats) {
      const count = stats.count;
      let relationshipType, strength;

      if (count >= 30) {
        relationshipType = 'telegram_frequent';
        strength = 0.90; // Telegram messages are high-signal personal comms
      } else if (count >= 10) {
        relationshipType = 'telegram_moderate';
        strength = 0.70;
      } else if (count >= 3) {
        relationshipType = 'telegram_light';
        strength = 0.45;
      } else {
        relationshipType = 'telegram_minimal';
        strength = 0.25;
      }

      await pool.query(
        `INSERT INTO team_proximity
           (person_id, team_member_id, relationship_type, relationship_strength,
            notes, last_interaction_date, interaction_count, source)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'telegram_sync')
         ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
           relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
           interaction_count = EXCLUDED.interaction_count,
           notes = EXCLUDED.notes,
           last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
           updated_at = NOW()`,
        [personId, account.user_id, relationshipType, strength,
         `${count} Telegram messages (${LOOKBACK_DAYS}d)`, stats.lastDate, count]
      );
    }
  }

  // Save sync cursor
  if (!DRY_RUN && maxMessageId > minMessageId) {
    await pool.query(
      `UPDATE user_telegram_accounts SET last_message_id = $2, last_sync_at = NOW() WHERE id = $1`,
      [account.id, maxMessageId]
    );
  }

  await client.disconnect();

  console.log(c.green(`    ✓ ${totalMessages} messages, ${totalMatched} contacts matched, ${totalInteractions} interactions`));
  return { dialogs: privateChatEntities.length, messages: totalMessages, matched: totalMatched, interactions: totalInteractions, errors: 0 };
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  TELEGRAM SYNC — MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  if (DRY_RUN) console.log(c.yellow('  ⚠ DRY RUN — no writes'));
  if (FULL_SCAN) console.log(c.yellow('  ⚠ FULL SCAN — ignoring message ID cursor'));

  if (!API_ID || !API_HASH) {
    console.log(c.red('  ✗ Missing TELEGRAM_API_ID or TELEGRAM_API_HASH'));
    process.exit(1);
  }

  await runJob(pool, 'telegram_sync', async () => {
    const filter = USER_ID ? `AND uta.user_id = '${USER_ID}'` : '';
    const { rows: accounts } = await pool.query(`
      SELECT uta.*, u.tenant_id
      FROM user_telegram_accounts uta
      JOIN users u ON u.id = uta.user_id
      WHERE uta.sync_enabled = true AND uta.session_string IS NOT NULL
        ${filter}
      ORDER BY uta.phone
    `);

    if (accounts.length === 0) {
      console.log('\n  No Telegram accounts connected for sync.');
      return { records_in: 0, records_out: 0 };
    }

    console.log(`\n  Found ${accounts.length} account(s) to sync\n`);

    let totalMsgs = 0, totalInteractions = 0, totalErrors = 0;

    for (const account of accounts) {
      try {
        const stats = await syncAccount(account);
        totalMsgs += stats.messages;
        totalInteractions += stats.interactions;
        totalErrors += stats.errors;
      } catch (err) {
        console.log(c.red(`  ✗ Account error: ${err.message}`));
        totalErrors++;
      }
    }

    console.log(`\n  ────────────────────────────────────`);
    console.log(`  💬 Messages scanned:  ${totalMsgs}`);
    console.log(`  🤝 Interactions:      ${totalInteractions}`);
    if (totalErrors) console.log(c.red(`  ✗ Errors:             ${totalErrors}`));

    return {
      records_in: totalMsgs,
      records_out: totalInteractions,
      metadata: { interactions: totalInteractions, errors: totalErrors }
    };
  });

  await pool.end();
}

main().catch(err => {
  console.error(c.red('FATAL: ' + err.message));
  process.exit(1);
});
