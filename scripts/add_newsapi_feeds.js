#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Add high-signal feeds discovered via NewsAPI.ai calibration
// All feeds verified accessible with valid RSS/Atom XML
// Run: DATABASE_URL="..." node scripts/add_newsapi_feeds.js
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const feeds = [
  // Business Wire already in catalog (10 sector feeds)

  // ── Major Business Press ──
  ['Forbes Business', 'https://www.forbes.com/business/feed/', 'news'],
  ['Forbes Innovation', 'https://www.forbes.com/innovation/feed/', 'news'],
  ['Benzinga', 'https://www.benzinga.com/feed', 'news'],
  ['Yahoo Finance', 'https://finance.yahoo.com/rss/', 'news'],
  ['Seeking Alpha', 'https://seekingalpha.com/feed.xml', 'news'],
  ['Motley Fool', 'https://www.fool.com/feeds/index.aspx', 'news'],
  ['ZDNet', 'https://www.zdnet.com/rss.xml', 'news'],
  ['Nature', 'https://www.nature.com/nature.rss', 'research'],

  // ── Australian Press ──
  ['PerthNow', 'https://www.perthnow.com.au/rss', 'news'],
  ['Business Insider AU', 'https://feeds.feedburner.com/businessinsideraustralia', 'news'],

  // ── Africa & MENA ──
  ['AllAfrica', 'https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf', 'news'],
];

async function run() {
  let added = 0, skipped = 0;
  for (const [name, url, sourceType] of feeds) {
    const { rows } = await pool.query('SELECT id FROM rss_sources WHERE url = $1', [url]);
    if (rows.length > 0) {
      console.log('  ○ ' + name + ' (already exists)');
      skipped++;
      continue;
    }
    await pool.query(
      'INSERT INTO rss_sources (name, url, source_type, enabled) VALUES ($1, $2, $3, true)',
      [name, url, sourceType]
    );
    console.log('  + ' + name);
    added++;
  }

  console.log(`\nDone: ${added} added, ${skipped} already existed`);

  // Summary of sources NOT available via RSS
  console.log('\n── Sources with high signal yield but NO public RSS ──');
  console.log('  AFR (afr.com) — 531 signals, 43 clients — paywall, no RSS');
  console.log('  OpenPR (openpr.com) — 1,167 signals — PR aggregator, no RSS');
  console.log('  MarketScreener — 822 signals — no public RSS');
  console.log('  DailyMail Business — 416 signals — timeout/blocked');
  console.log('  Straits Times — 83 signals — no public RSS');
  console.log('  Mumbrella — 120 signals — 403 blocked');
  console.log('  These would need custom scrapers or API access.');

  await pool.end();
}

run().catch(e => { console.error(e.message); pool.end(); process.exit(1); });
