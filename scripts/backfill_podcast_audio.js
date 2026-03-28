#!/usr/bin/env node
// Backfill audio_url for existing podcast episodes by re-reading RSS feeds
require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');
const http = require('http');
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
      // Parse items with regex — no xml2js dependency
      const itemRegex = /<item[^>]*>([\s\S]*?)<\/item>/gi;
      let match, updated = 0, itemCount = 0;

      while ((match = itemRegex.exec(xml)) !== null) {
        itemCount++;
        const itemXml = match[1];

        // Extract enclosure URL
        const encMatch = itemXml.match(/enclosure[^>]*url=["']([^"']+)["']/i);
        if (!encMatch) continue;
        const encUrl = encMatch[1].replace(/&amp;/g, '&');

        // Extract title
        const titleMatch = itemXml.match(/<title[^>]*>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/i);
        if (!titleMatch) continue;
        const title = titleMatch[1].replace(/<[^>]+>/g, '').trim();
        if (!title) continue;

        const matchTitle = title.slice(0, 80).replace(/[%_]/g, '');
        const { rowCount } = await pool.query(`
          UPDATE external_documents SET audio_url = $1
          WHERE source_name = $2 AND title ILIKE $3 AND audio_url IS NULL AND source_type = 'podcast'
        `, [encUrl, src.source_name, matchTitle + '%']);

        updated += rowCount;
      }

      console.log(`  ${src.source_name}: ${itemCount} items, ${updated} audio URLs set`);
      totalUpdated += updated;
    } catch (e) {
      console.log(`  ${src.source_name}: failed — ${(e.message || '').slice(0, 60)}`);
    }
  }

  console.log(`Done: ${totalUpdated} episodes updated with audio URLs`);
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
