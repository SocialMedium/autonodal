#!/usr/bin/env node
/**
 * Delete episodes from broken podcast feeds that redirect to our own site.
 * One-time cleanup script.
 */
require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const BROKEN_SOURCES = [
  'My First Million',
  'Equity - TechCrunch',
  'Equity',
  'Masters of Scale',
];

// Also delete anything sourced from art19 or megaphone feeds that redirect
const BROKEN_URLS = [
  'art19.com',
  'myfirstmillion',
];

async function main() {
  console.log('Cleaning up broken podcast episodes...\n');

  for (const name of BROKEN_SOURCES) {
    const { rowCount } = await pool.query(
      `DELETE FROM external_documents WHERE source_name ILIKE $1 AND source_type = 'podcast'`,
      [`%${name}%`]
    );
    console.log(`  ${name}: deleted ${rowCount} episodes`);
  }

  // Delete anything with art19 or myfirstmillion in the source URL
  for (const pattern of BROKEN_URLS) {
    const { rowCount } = await pool.query(
      `DELETE FROM external_documents WHERE source_url ILIKE $1`,
      [`%${pattern}%`]
    );
    if (rowCount > 0) console.log(`  URL pattern "${pattern}": deleted ${rowCount} documents`);
  }

  // Also clean from rss_sources table
  for (const name of BROKEN_SOURCES) {
    const { rowCount } = await pool.query(
      `DELETE FROM rss_sources WHERE name ILIKE $1`,
      [`%${name}%`]
    );
    if (rowCount > 0) console.log(`  ${name}: removed from rss_sources`);
  }
  // Remove art19 sources
  const { rowCount: art19Count } = await pool.query(
    `DELETE FROM rss_sources WHERE url ILIKE '%art19%'`
  );
  if (art19Count > 0) console.log(`  art19 sources: removed ${art19Count}`);

  console.log('\nDone.');
  await pool.end();
}

main().catch(e => { console.error(e); pool.end(); process.exit(1); });
