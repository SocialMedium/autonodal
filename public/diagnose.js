#!/usr/bin/env node

/**
 * MitchelLake Dashboard Diagnostic Script
 * 
 * This script checks:
 * 1. Database connectivity
 * 2. Required tables exist
 * 3. Sample data is present
 * 4. API endpoints respond correctly
 * 
 * Usage: node diagnose.js
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  blue: '\x1b[36m',
  gray: '\x1b[90m'
};

function log(symbol, message, color = '') {
  console.log(`${color}${symbol} ${message}${colors.reset}`);
}

function success(message) {
  log('✓', message, colors.green);
}

function error(message) {
  log('✗', message, colors.red);
}

function warning(message) {
  log('⚠', message, colors.yellow);
}

function info(message) {
  log('ℹ', message, colors.blue);
}

async function checkDatabase() {
  console.log('\n' + '='.repeat(60));
  console.log('DATABASE CONNECTIVITY');
  console.log('='.repeat(60));

  try {
    const result = await pool.query('SELECT NOW()');
    success(`Connected to PostgreSQL`);
    info(`Server time: ${result.rows[0].now}`);
    return true;
  } catch (err) {
    error(`Database connection failed: ${err.message}`);
    return false;
  }
}

async function checkTables() {
  console.log('\n' + '='.repeat(60));
  console.log('TABLE EXISTENCE CHECK');
  console.log('='.repeat(60));

  const requiredTables = [
    'users',
    'people',
    'companies',
    'signal_events',
    'external_documents',
    'searches',
    'rss_sources'
  ];

  const results = {};

  for (const table of requiredTables) {
    try {
      const result = await pool.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = $1
        )
      `, [table]);

      if (result.rows[0].exists) {
        const countResult = await pool.query(`SELECT COUNT(*) FROM ${table}`);
        const count = parseInt(countResult.rows[0].count);
        success(`Table '${table}' exists (${count.toLocaleString()} rows)`);
        results[table] = count;
      } else {
        error(`Table '${table}' NOT FOUND`);
        results[table] = -1;
      }
    } catch (err) {
      error(`Error checking '${table}': ${err.message}`);
      results[table] = -1;
    }
  }

  return results;
}

async function checkSignalEventsColumns() {
  console.log('\n' + '='.repeat(60));
  console.log('SIGNAL_EVENTS TABLE STRUCTURE');
  console.log('='.repeat(60));

  try {
    const result = await pool.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'signal_events'
      ORDER BY ordinal_position
    `);

    if (result.rows.length === 0) {
      error('signal_events table not found or has no columns');
      return false;
    }

    console.log(`\nColumns found: ${result.rows.length}`);
    result.rows.forEach(col => {
      const nullable = col.is_nullable === 'YES' ? '(nullable)' : '(required)';
      info(`  ${col.column_name}: ${col.data_type} ${nullable}`);
    });

    // Check for critical columns
    const criticalColumns = ['id', 'signal_type', 'confidence_score', 'detected_at'];
    const existingColumns = result.rows.map(r => r.column_name);
    
    let allPresent = true;
    criticalColumns.forEach(col => {
      if (existingColumns.includes(col)) {
        success(`  ✓ Critical column '${col}' present`);
      } else {
        error(`  ✗ Critical column '${col}' MISSING`);
        allPresent = false;
      }
    });

    return allPresent;
  } catch (err) {
    error(`Error checking signal_events structure: ${err.message}`);
    return false;
  }
}

async function checkRecentSignals() {
  console.log('\n' + '='.repeat(60));
  console.log('RECENT SIGNALS');
  console.log('='.repeat(60));

  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '24 hours') as last_24h,
        COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '7 days') as last_7d,
        MAX(detected_at) as most_recent
      FROM signal_events
    `);

    const stats = result.rows[0];
    
    info(`Total signals: ${parseInt(stats.total).toLocaleString()}`);
    info(`Last 24 hours: ${parseInt(stats.last_24h).toLocaleString()}`);
    info(`Last 7 days: ${parseInt(stats.last_7d).toLocaleString()}`);
    
    if (stats.most_recent) {
      const mostRecent = new Date(stats.most_recent);
      const hoursAgo = Math.floor((Date.now() - mostRecent.getTime()) / 3600000);
      info(`Most recent: ${mostRecent.toISOString()} (${hoursAgo}h ago)`);
    } else {
      warning('No signals found in database');
    }

    // Show sample signals
    if (parseInt(stats.total) > 0) {
      console.log('\nSample signals (last 5):');
      const samples = await pool.query(`
        SELECT 
          se.signal_type,
          se.confidence_score,
          se.detected_at,
          c.name as company_name,
          SUBSTRING(se.evidence_summary, 1, 80) as summary_preview
        FROM signal_events se
        LEFT JOIN companies c ON se.company_id = c.id
        ORDER BY se.detected_at DESC
        LIMIT 5
      `);

      samples.rows.forEach(signal => {
        const confidence = (signal.confidence_score * 100).toFixed(0);
        const timeAgo = Math.floor((Date.now() - new Date(signal.detected_at).getTime()) / 3600000);
        console.log(`  ${colors.gray}[${timeAgo}h ago]${colors.reset} ${signal.signal_type} @ ${signal.company_name || 'Unknown'} (${confidence}%)`);
        if (signal.summary_preview) {
          console.log(`    "${signal.summary_preview}..."`);
        }
      });
    }

    return parseInt(stats.total) > 0;
  } catch (err) {
    error(`Error checking recent signals: ${err.message}`);
    return false;
  }
}

async function checkRSSSources() {
  console.log('\n' + '='.repeat(60));
  console.log('RSS SOURCES');
  console.log('='.repeat(60));

  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE enabled = true) as enabled,
        COUNT(*) FILTER (WHERE last_fetched_at IS NOT NULL) as fetched
      FROM rss_sources
    `);

    const stats = result.rows[0];
    
    info(`Total sources: ${stats.total}`);
    info(`Enabled: ${stats.enabled}`);
    info(`Ever fetched: ${stats.fetched}`);

    // Show sample sources
    const samples = await pool.query(`
      SELECT name, enabled, last_fetched_at, error_count
      FROM rss_sources
      ORDER BY last_fetched_at DESC NULLS LAST
      LIMIT 5
    `);

    console.log('\nSample sources:');
    samples.rows.forEach(source => {
      const status = source.enabled ? '✓ Enabled' : '✗ Disabled';
      const lastFetch = source.last_fetched_at 
        ? new Date(source.last_fetched_at).toLocaleString()
        : 'Never';
      console.log(`  ${status} ${source.name}`);
      console.log(`    Last fetch: ${lastFetch}, Errors: ${source.error_count || 0}`);
    });

    return parseInt(stats.enabled) > 0;
  } catch (err) {
    error(`Error checking RSS sources: ${err.message}`);
    return false;
  }
}

async function testAPIQuery() {
  console.log('\n' + '='.repeat(60));
  console.log('API QUERY TEST');
  console.log('='.repeat(60));

  // Test the exact query the API uses
  try {
    const result = await pool.query(`
      SELECT 
        se.id,
        se.signal_type,
        se.signal_category,
        se.confidence_score,
        se.evidence_summary,
        se.evidence_snippet,
        se.detected_at,
        c.name as company_name,
        c.sector,
        c.geography,
        ed.source_name
      FROM signal_events se
      LEFT JOIN companies c ON se.company_id = c.id
      LEFT JOIN external_documents ed ON se.source_document_id = ed.id
      ORDER BY se.detected_at DESC
      LIMIT 10
    `);

    success(`Query executed successfully, returned ${result.rows.length} rows`);
    
    if (result.rows.length > 0) {
      console.log('\nFirst signal returned:');
      const first = result.rows[0];
      console.log(`  ID: ${first.id}`);
      console.log(`  Type: ${first.signal_type}`);
      console.log(`  Company: ${first.company_name || 'Unknown'}`);
      console.log(`  Confidence: ${(first.confidence_score * 100).toFixed(0)}%`);
      console.log(`  Detected: ${new Date(first.detected_at).toLocaleString()}`);
      console.log(`  Summary: ${first.evidence_summary?.substring(0, 100)}...`);
    } else {
      warning('Query returned 0 rows - this is why dashboard shows no signals');
    }

    return result.rows.length > 0;
  } catch (err) {
    error(`API query failed: ${err.message}`);
    console.log('\nFull error:');
    console.log(err);
    return false;
  }
}

async function checkExternalDocuments() {
  console.log('\n' + '='.repeat(60));
  console.log('EXTERNAL DOCUMENTS');
  console.log('='.repeat(60));

  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE signals_computed_at IS NOT NULL) as processed,
        MAX(published_at) as most_recent
      FROM external_documents
    `);

    const stats = result.rows[0];
    
    info(`Total documents: ${parseInt(stats.total).toLocaleString()}`);
    info(`Signals computed: ${parseInt(stats.processed).toLocaleString()}`);
    
    if (stats.most_recent) {
      info(`Most recent doc: ${new Date(stats.most_recent).toLocaleString()}`);
    }

    return parseInt(stats.total) > 0;
  } catch (err) {
    error(`Error checking documents: ${err.message}`);
    return false;
  }
}

async function generateReport() {
  console.log('\n\n' + '='.repeat(60));
  console.log('DIAGNOSTIC REPORT');
  console.log('='.repeat(60));

  const dbOk = await checkDatabase();
  if (!dbOk) {
    error('\n❌ Database connection failed. Check DATABASE_URL in .env');
    process.exit(1);
  }

  const tables = await checkTables();
  const structureOk = await checkSignalEventsColumns();
  const hasSignals = await checkRecentSignals();
  const hasRSS = await checkRSSSources();
  const apiQueryOk = await testAPIQuery();
  const hasDocs = await checkExternalDocuments();

  console.log('\n' + '='.repeat(60));
  console.log('SUMMARY & RECOMMENDATIONS');
  console.log('='.repeat(60));

  if (tables.signal_events === 0) {
    warning('\n⚠️  No signals in database');
    console.log('\nTO FIX: Run signal harvesting:');
    console.log('  node scripts/harvest_signals.js --limit 20');
  } else if (tables.signal_events > 0 && !apiQueryOk) {
    error('\n❌ Signals exist but API query fails');
    console.log('\nTO FIX: Check column names in query vs schema');
  } else if (apiQueryOk) {
    success('\n✅ Dashboard should work! Signals are queryable.');
  }

  if (tables.people > 0) {
    success(`\n✅ ${tables.people.toLocaleString()} people in database`);
  }

  if (!hasRSS) {
    warning('\n⚠️  No enabled RSS sources');
    console.log('\nTO FIX: Add RSS sources:');
    console.log('  node scripts/add_rss_sources.js');
  }

  console.log('\n' + '='.repeat(60));
  console.log('Next steps:');
  console.log('1. If signals exist: Check server.js has /api/signals/brief endpoint');
  console.log('2. If no signals: Run harvest_signals.js to populate');
  console.log('3. Update dashboard.html with the fixed version provided');
  console.log('='.repeat(60) + '\n');
}

// Run diagnostics
generateReport()
  .then(() => {
    pool.end();
    process.exit(0);
  })
  .catch(err => {
    console.error('Fatal error:', err);
    pool.end();
    process.exit(1);
  });