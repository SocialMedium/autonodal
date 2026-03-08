#!/usr/bin/env node
/**
 * Sync Google Contacts to MitchelLake
 * Usage: node scripts/sync_contacts.js
 */

require('dotenv').config();

const db = require('../lib/db');
const googleLib = require('../lib/google');

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  GOOGLE CONTACTS SYNC - MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  
  try {
    // Get all connected Google accounts
    const accounts = await db.queryAll(
      "SELECT * FROM user_google_accounts WHERE sync_enabled = true AND scopes::text LIKE '%contacts%'"
    );
    
    if (accounts.length === 0) {
      console.log('No Google accounts with contacts permission found.');
      console.log('Connect your Google account at /api/auth/google');
      process.exit(1);
    }
    
    console.log(`Found ${accounts.length} Google account(s) to sync\n`);
    
    let totalStats = { total: 0, created: 0, updated: 0, skipped: 0, errors: 0 };
    
    for (const account of accounts) {
      console.log(`\nSyncing contacts for: ${account.google_email}`);
      
      // Check if token needs refresh
      if (account.token_expires_at && new Date(account.token_expires_at) < new Date()) {
        console.log('  Refreshing access token...');
        const newTokens = await googleLib.refreshAccessToken(account.refresh_token);
        await db.queryAll(
          'UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2 WHERE id = $3',
          [newTokens.access_token, new Date(newTokens.expiry_date), account.id]
        );
        account.access_token = newTokens.access_token;
      }
      
      const stats = await googleLib.syncContacts(db, account);
      
      // Aggregate stats
      totalStats.total += stats.total;
      totalStats.created += stats.created;
      totalStats.updated += stats.updated;
      totalStats.skipped += stats.skipped;
      totalStats.errors += stats.errors;
      
      // Update last sync
      await db.queryAll(
        'UPDATE user_google_accounts SET last_sync_at = NOW() WHERE id = $1',
        [account.id]
      );
    }
    
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  SYNC COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`  Total contacts processed: ${totalStats.total}`);
    console.log(`  Created: ${totalStats.created}`);
    console.log(`  Updated: ${totalStats.updated}`);
    console.log(`  Skipped: ${totalStats.skipped}`);
    console.log(`  Errors: ${totalStats.errors}`);
    console.log('═══════════════════════════════════════════════════════════════');
    
  } catch (error) {
    console.error('Sync failed:', error);
    process.exit(1);
  }
  
  process.exit(0);
}

main();
