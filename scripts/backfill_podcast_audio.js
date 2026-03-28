#!/usr/bin/env node
// Backfill audio_url for existing podcast episodes by re-reading RSS feeds
require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');
const http = require('http');
const { parseString } = require('xml2js');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    client.get(url, { headers: { 'User-Agent': 'MLX-Intelligence/1.0' }, timeout: 15000 }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchUrl(res.headers.location).then(resolve).catch(reject);
      }
      const chunks = []; res.on('data', c => chunks.push(c)); res.on('end', () => resolve(Buffer.concat(chunks).toString()));
    }).on('error', reject).on('timeout', function() { this.destroy(); reject(new Error('timeout')); });
  });
}

async function main() {
  console.log('Backfilling podcast audio URLs...');

  // Get all podcast RSS sources
  const { rows: sources } = await pool.query(`
    SELECT DISTINCT source_name, rs.url
    FROM external_documents ed
    JOIN rss_sources rs ON rs.name = ed.source_name
    WHERE ed.source_type = 'podcast' AND ed.audio_url IS NULL
    LIMIT 50
  `).catch(() => ({ rows: [] }));

  console.log(`Found ${sources.length} podcast sources to scan`);
  let totalUpdated = 0;

  for (const src of sources) {
    try {
      const xml = await fetchUrl(src.url);
      const parsed = await new Promise((resolve, reject) => {
        parseString(xml, { explicitArray: true }, (err, result) => err ? reject(err) : resolve(result));
      });

      const items = parsed?.rss?.channel?.[0]?.item || parsed?.feed?.entry || [];
      let updated = 0;

      for (const item of items) {
        const encUrl = item?.enclosure?.[0]?.$?.url;
        if (!encUrl) continue;

        const title = (item?.title?.[0]?._ || item?.title?.[0] || '').trim();
        if (!title) continue;

        // Match on first 80 chars with wildcard — titles may be truncated or have encoding diffs
        const matchTitle = title.slice(0, 80).replace(/[%_]/g, '');
        const { rowCount } = await pool.query(`
          UPDATE external_documents SET audio_url = $1
          WHERE source_name = $2 AND title ILIKE $3 AND audio_url IS NULL AND source_type = 'podcast'
        `, [encUrl, src.source_name, matchTitle + '%']);

        updated += rowCount;
      }

      if (updated > 0) {
        console.log(`  ${src.source_name}: ${updated} audio URLs set`);
        totalUpdated += updated;
      }
    } catch (e) {
      // Skip failed feeds
    }
  }

  console.log(`Done: ${totalUpdated} episodes updated with audio URLs`);
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
