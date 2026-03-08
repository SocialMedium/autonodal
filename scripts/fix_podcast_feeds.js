#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// fix_podcast_feeds.js — Resolve correct RSS URLs via iTunes Lookup API
// ═══════════════════════════════════════════════════════════════════════════════
//
// The iTunes Lookup API returns the real RSS feed URL for any podcast:
//   https://itunes.apple.com/lookup?id=APPLE_PODCAST_ID&entity=podcast
//
// Usage:
//   node scripts/fix_podcast_feeds.js              Fix all broken feeds
//   node scripts/fix_podcast_feeds.js --dry-run    Show fixes without applying
//   node scripts/fix_podcast_feeds.js --all        Re-check ALL podcast feeds
//
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────────────────────────────────────────
// Apple Podcast IDs for our target shows
// Find these from the Apple Podcasts URL: podcasts.apple.com/us/podcast/TITLE/id{THIS_NUMBER}
// ─────────────────────────────────────────────────────────────────────────────

const PODCAST_APPLE_IDS = {
  '20VC - Twenty Minute VC':                958230465,
  'Equity - TechCrunch':                    1215439780,
  'All-In Podcast':                         1502871393,
  'This Week in Startups':                  315114957,
  'Acquired':                               1050462261,
  "Lenny's Podcast":                        1627920305,
  'Invest Like the Best':                   1154105909,
  'Venture Unlocked':                       1520207736,
  'My First Million':                       1469759170,
  'Village Global Venture Stories':         1316789476,
  'StrictlyVC Download':                    1448114630,
  'Impulso Podcast (SEA VC)':               1535112566,
  'Asia Tech Podcast':                      1518865602,
  'Analyse Asia':                           914382944,
  'Startup Grind AU':                       950272982,
  'Scale Investors Podcast':                1578660107,
  'The Overnight Success (AU VC)':          1575879207,
  'Sifted Talks':                           1507805965,
  'The Twenty Minute VC Europe':            1528549899,
  'EU-Startups Podcast':                    1470840749,
  'Seed to Scale (Accel)':                  1501299299,
  'Unicorn Bakery (DACH/EU)':               1505744555,
};

// ─────────────────────────────────────────────────────────────────────────────
// iTunes Lookup API
// ─────────────────────────────────────────────────────────────────────────────

function lookupPodcast(appleId) {
  return new Promise((resolve, reject) => {
    const url = `https://itunes.apple.com/lookup?id=${appleId}&entity=podcast`;
    https.get(url, { timeout: 15000 }, (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
          if (data.resultCount > 0) {
            const result = data.results[0];
            resolve({
              feedUrl: result.feedUrl,
              trackName: result.trackName || result.collectionName,
              artistName: result.artistName,
            });
          } else {
            reject(new Error('No results'));
          }
        } catch (e) { reject(e); }
      });
      res.on('error', reject);
    }).on('error', reject).on('timeout', function() { this.destroy(); reject(new Error('Timeout')); });
  });
}

// Quick test if a feed URL actually works
function testFeedUrl(url) {
  return new Promise((resolve) => {
    const client = url.startsWith('https') ? https : require('http');
    const req = client.get(url, {
      timeout: 10000,
      headers: {
        'User-Agent': 'MitchelLake-SignalBot/1.0',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
      },
    }, (res) => {
      // Follow one redirect
      if ([301, 302, 307, 308].includes(res.statusCode) && res.headers.location) {
        resolve({ ok: true, status: `${res.statusCode}→redirect`, finalUrl: res.headers.location });
        return;
      }
      resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode });
      res.resume(); // Drain the response
    });
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, status: 'timeout' }); });
    req.on('error', (e) => resolve({ ok: false, status: e.message }));
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const checkAll = args.includes('--all');

  console.log('═══════════════════════════════════════════════════');
  console.log('  Fix Podcast Feed URLs via iTunes Lookup');
  console.log('═══════════════════════════════════════════════════\n');

  await pool.query('SELECT 1');
  console.log('✅ Database connected\n');

  // Get podcast sources from DB
  let query = `SELECT id, name, url, consecutive_errors, enabled
               FROM rss_sources WHERE source_type = 'podcast'`;
  if (!checkAll) query += ` AND consecutive_errors > 0`;
  query += ` ORDER BY name`;

  const { rows: sources } = await pool.query(query);
  console.log(`Found ${sources.length} podcast source(s) to check\n`);

  let fixed = 0, failed = 0, noAppleId = 0, alreadyCorrect = 0;

  for (const source of sources) {
    const appleId = PODCAST_APPLE_IDS[source.name];

    if (!appleId) {
      console.log(`  ⏭️  ${source.name} — no Apple Podcast ID mapped`);
      noAppleId++;
      continue;
    }

    try {
      const lookup = await lookupPodcast(appleId);
      const newUrl = lookup.feedUrl;

      if (!newUrl) {
        console.log(`  ❓ ${source.name} — no feedUrl in iTunes response`);
        failed++;
        continue;
      }

      if (newUrl === source.url) {
        // URL hasn't changed — test it
        const test = await testFeedUrl(newUrl);
        if (test.ok) {
          console.log(`  ✅ ${source.name} — URL correct and working`);
          alreadyCorrect++;
        } else {
          console.log(`  ⚠️  ${source.name} — URL matches but returns ${test.status}`);
          failed++;
        }
        continue;
      }

      // New URL found — test it
      const test = await testFeedUrl(newUrl);
      const status = test.ok ? '🟢' : '🟡';

      console.log(`  ${status} ${source.name}`);
      console.log(`     Old: ${source.url}`);
      console.log(`     New: ${newUrl} (${test.ok ? 'accessible' : test.status})`);

      if (!dryRun && test.ok) {
        await pool.query(
          `UPDATE rss_sources SET url = $1, consecutive_errors = 0, last_error = NULL, enabled = true WHERE id = $2`,
          [newUrl, source.id]
        );
        console.log(`     ✅ Updated + re-enabled`);
        fixed++;
      } else if (!dryRun && !test.ok) {
        // Update URL even if not accessible — it's the canonical one
        await pool.query(
          `UPDATE rss_sources SET url = $1, last_error = $2 WHERE id = $3`,
          [newUrl, `Feed test: ${test.status}`, source.id]
        );
        console.log(`     ⚠️  Updated URL but feed test failed: ${test.status}`);
        fixed++;
      } else {
        console.log(`     [DRY RUN — no changes]`);
        fixed++;
      }

    } catch (err) {
      console.log(`  ❌ ${source.name} — lookup failed: ${err.message}`);
      failed++;
    }

    await sleep(300); // Rate limit iTunes API
  }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`📊 RESULTS${dryRun ? ' [DRY RUN]' : ''}`);
  console.log(`   Fixed:          ${fixed}`);
  console.log(`   Already correct: ${alreadyCorrect}`);
  console.log(`   Failed:         ${failed}`);
  console.log(`   No Apple ID:    ${noAppleId}`);
  console.log('═══════════════════════════════════════════════════\n');

  if (fixed > 0 && !dryRun) {
    console.log('🎯 Now run the harvester to test:');
    console.log('   node scripts/seed_harvest_podcasts.js\n');
  }

  await pool.end();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });