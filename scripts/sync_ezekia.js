#!/usr/bin/env node
/**
 * Ezekia Sync Script
 * 
 * Syncs candidates from Ezekia CRM to MitchelLake Signal platform
 * 
 * Usage:
 *   npm run sync:ezekia           # Incremental sync (since last sync)
 *   npm run sync:ezekia -- --full # Full sync (all records)
 *   npm run sync:ezekia -- --test # Test connection only
 */

require('dotenv').config();
const db = require('../lib/db');
const ezekia = require('../lib/ezekia');

async function main() {
  const args = process.argv.slice(2);
  const isFullSync = args.includes('--full');
  const isTest = args.includes('--test');

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  EZEKIA SYNC - MitchelLake Signal Intelligence Platform');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log();

  // Check configuration
  if (!process.env.EZEKIA_API_TOKEN) {
    console.error('ERROR: EZEKIA_API_TOKEN not configured in .env');
    console.error('');
    console.error('To get your API token:');
    console.error('1. Log into Ezekia');
    console.error('2. Go to Settings > Integrations > API Tokens');
    console.error('3. Click "Generate New Token"');
    console.error('4. Add to .env: EZEKIA_API_TOKEN=your-token');
    process.exit(1);
  }

  console.log(`API URL: ${process.env.EZEKIA_API_URL || 'https://ezekia.com'}`);
  console.log();

  // Test connection
  console.log('Testing Ezekia connection...');
  try {
    const testResponse = await ezekia.getPeople({ page: 1, per_page: 1 });
    const totalPeople = testResponse.meta?.total || testResponse.total || 'unknown';
    console.log(`✓ Connected! Total people in Ezekia: ${totalPeople}`);
    console.log();
  } catch (err) {
    console.error('✗ Connection failed:', err.message);
    process.exit(1);
  }

  if (isTest) {
    console.log('Test mode - connection successful, exiting.');
    process.exit(0);
  }

  // Get last sync time for incremental sync
  let updatedSince = null;
  if (!isFullSync) {
    const lastSync = await ezekia.getLastSyncTime(db);
    if (lastSync) {
      updatedSince = new Date(lastSync).toISOString();
      console.log(`Last sync: ${updatedSince}`);
      console.log('Running incremental sync (use --full for complete resync)');
    } else {
      console.log('No previous sync found, running full sync');
    }
  } else {
    console.log('Running FULL sync (this may take a while for large databases)');
  }
  console.log();

  // Confirm before large sync
  const testCount = await ezekia.getPeople({ page: 1, per_page: 1 });
  const totalCount = testCount.meta?.total || testCount.total || 0;
  
  if (totalCount > 10000 && isFullSync) {
    console.log(`WARNING: About to sync ${totalCount.toLocaleString()} records.`);
    console.log('This could take 30+ minutes.');
    console.log('');
    // In production, you might want to add a confirmation prompt here
  }

  // Run sync
  const stats = await ezekia.syncAllPeople(db, {
    batchSize: 100,
    updatedSince
  });

  // Summary
  console.log();
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  SYNC COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log();
  console.log(`  Records processed: ${stats.total.toLocaleString()}`);
  console.log(`  New records:       ${stats.created.toLocaleString()}`);
  console.log(`  Updated records:   ${stats.updated.toLocaleString()}`);
  console.log(`  Errors:            ${stats.errors.toLocaleString()}`);
  console.log(`  Duration:          ${stats.duration.toFixed(1)}s`);
  console.log();

  // Log to audit
  try {
    await db.insert('audit_logs', {
      action: 'ezekia_sync',
      details: {
        type: isFullSync ? 'full' : 'incremental',
        stats
      }
    });
  } catch (err) {
    // Audit logging is optional
  }

  process.exit(stats.errors > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
