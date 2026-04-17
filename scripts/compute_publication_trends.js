#!/usr/bin/env node
/**
 * Publication Trends Pipeline — Weekly
 *
 * PIPELINE-CONTEXT: Runs as weekly cron, uses platformPool directly.
 * Detects acceleration in publication volume by keyword/topic area.
 * Outputs trend data to market_health_history for research momentum tracking.
 *
 * NOTE: Does NOT insert into signal_events because 'research_momentum' is not
 * in the signal_type ENUM. Instead, stores trend data in a dedicated table
 * and exposes via the /api/research/trends endpoint.
 *
 * Schedule: weekly Sunday at 5:00am
 * Usage: node scripts/compute_publication_trends.js
 */

require('dotenv').config();
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const QDRANT_URL = process.env.QDRANT_URL;
const QDRANT_API_KEY = process.env.QDRANT_API_KEY;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function qdrantScroll(offset, limit) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      limit,
      offset,
      with_payload: ['keywords', 'year', 'repository'],
      with_vector: false,
    });
    const url = new URL('/collections/publications/points/scroll', QDRANT_URL);
    const req = https.request({
      hostname: url.hostname, port: url.port || 443, path: url.pathname, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'api-key': QDRANT_API_KEY },
      timeout: 30000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          resolve(data.result || { points: [], next_page_offset: null });
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Qdrant timeout')); });
    req.write(body);
    req.end();
  });
}

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Publication Trends Pipeline — Weekly');
  console.log('═══════════════════════════════════════════════════\n');

  const startTime = Date.now();
  const currentYear = new Date().getFullYear();

  // Ensure trends table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS publication_trends (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      keyword TEXT NOT NULL,
      recent_count INTEGER NOT NULL DEFAULT 0,
      prior_count INTEGER NOT NULL DEFAULT 0,
      acceleration FLOAT NOT NULL DEFAULT 0,
      direction TEXT NOT NULL DEFAULT 'flat',
      total_papers INTEGER NOT NULL DEFAULT 0,
      top_repositories TEXT[] DEFAULT '{}',
      computed_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(keyword)
    )
  `);

  // Scroll through publications and aggregate by keyword + year
  // Use sampling — full scan of 2M vectors is too slow for weekly cron
  console.log('  Sampling publications for trend analysis...');

  const keywordYearCounts = {}; // keyword → { recent: N, prior: N, repos: Set }
  let totalScanned = 0;
  let offset = null;
  const SAMPLE_LIMIT = 50000; // Sample 50K papers

  while (totalScanned < SAMPLE_LIMIT) {
    try {
      const result = await qdrantScroll(offset, 1000);
      const points = result.points || [];
      if (points.length === 0) break;

      for (const point of points) {
        const p = point.payload || {};
        const year = p.year;
        const keywordsRaw = p.keywords || '';
        const keywords = typeof keywordsRaw === 'string'
          ? keywordsRaw.split(/[,;]\s*/).map(k => k.trim().toLowerCase()).filter(k => k.length > 2 && k.length < 50)
          : Array.isArray(keywordsRaw) ? keywordsRaw.map(k => k.toLowerCase()) : [];

        const isRecent = year && year >= currentYear - 1; // last 2 years
        const isPrior = year && year >= currentYear - 4 && year < currentYear - 1; // 2-4 years ago

        for (const kw of keywords) {
          if (!keywordYearCounts[kw]) {
            keywordYearCounts[kw] = { recent: 0, prior: 0, total: 0, repos: new Set() };
          }
          keywordYearCounts[kw].total++;
          if (isRecent) keywordYearCounts[kw].recent++;
          if (isPrior) keywordYearCounts[kw].prior++;
          if (p.repository) keywordYearCounts[kw].repos.add(p.repository);
        }
      }

      totalScanned += points.length;
      offset = result.next_page_offset;
      if (!offset) break;

      if (totalScanned % 10000 === 0) {
        console.log(`  Scanned: ${totalScanned.toLocaleString()} papers, ${Object.keys(keywordYearCounts).length} keywords`);
      }

      await sleep(200);
    } catch (e) {
      console.error('  Scroll error:', e.message);
      break;
    }
  }

  console.log(`\n  Total scanned: ${totalScanned.toLocaleString()}`);
  console.log(`  Unique keywords: ${Object.keys(keywordYearCounts).length}`);

  // Compute acceleration for each keyword
  const trends = [];
  for (const [keyword, counts] of Object.entries(keywordYearCounts)) {
    if (counts.total < 5) continue; // Skip rare keywords

    let acceleration = 0;
    let direction = 'flat';

    if (counts.prior > 0) {
      acceleration = ((counts.recent - counts.prior) / counts.prior) * 100;
    } else if (counts.recent > 0) {
      acceleration = 100; // New topic — all recent
      direction = 'emerging';
    }

    if (acceleration > 40) direction = 'accelerating';
    else if (acceleration > 10) direction = 'growing';
    else if (acceleration > -10) direction = 'flat';
    else direction = 'declining';

    trends.push({
      keyword,
      recent: counts.recent,
      prior: counts.prior,
      acceleration: Math.round(acceleration * 10) / 10,
      direction,
      total: counts.total,
      repos: [...counts.repos].slice(0, 5),
    });
  }

  // Sort by absolute acceleration (most changing topics first)
  trends.sort((a, b) => Math.abs(b.acceleration) - Math.abs(a.acceleration));

  // Insert top 500 trends
  const topTrends = trends.slice(0, 500);
  console.log(`\n  Top accelerating topics:`);
  topTrends.slice(0, 10).forEach(t => {
    console.log(`    ${t.direction.padEnd(14)} ${t.keyword.padEnd(40)} recent=${t.recent} prior=${t.prior} (${t.acceleration > 0 ? '+' : ''}${t.acceleration}%)`);
  });

  let inserted = 0;
  for (const t of topTrends) {
    try {
      await pool.query(`
        INSERT INTO publication_trends (keyword, recent_count, prior_count, acceleration, direction, total_papers, top_repositories, computed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (keyword) DO UPDATE SET
          recent_count = EXCLUDED.recent_count, prior_count = EXCLUDED.prior_count,
          acceleration = EXCLUDED.acceleration, direction = EXCLUDED.direction,
          total_papers = EXCLUDED.total_papers, top_repositories = EXCLUDED.top_repositories,
          computed_at = NOW()
      `, [t.keyword, t.recent, t.prior, t.acceleration, t.direction, t.total, t.repos]);
      inserted++;
    } catch (e) { /* dupe or error */ }
  }

  const duration = Date.now() - startTime;

  // Log to pipeline_runs
  try {
    await pool.query(`
      INSERT INTO pipeline_runs (pipeline_key, pipeline_name, status, started_at, completed_at,
        duration_ms, items_processed, triggered_by)
      VALUES ('publication_trends', 'Publication Trends', 'completed', $1, NOW(), $2, $3, 'cron')
    `, [new Date(startTime), duration, inserted]);
  } catch (e) { /* non-fatal */ }

  console.log(`\n═══════════════════════════════════════════════════`);
  console.log(`  Papers sampled: ${totalScanned.toLocaleString()}`);
  console.log(`  Keywords analysed: ${Object.keys(keywordYearCounts).length}`);
  console.log(`  Trends stored: ${inserted}`);
  console.log(`  Accelerating: ${trends.filter(t => t.direction === 'accelerating').length}`);
  console.log(`  Duration: ${Math.round(duration / 1000)}s`);
  console.log('═══════════════════════════════════════════════════');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
