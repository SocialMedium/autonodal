#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// seed_vc_blogs.js — Seed VC Blogs & Newsletter RSS Sources
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/seed_vc_blogs.js              Seed all VC/newsletter sources
//   node scripts/seed_vc_blogs.js --region us  Seed only US sources
//   node scripts/seed_vc_blogs.js --region au  Seed only Australia/ANZ
//   node scripts/seed_vc_blogs.js --region uk  Seed only UK sources
//   node scripts/seed_vc_blogs.js --region eu  Seed only Europe sources
//   node scripts/seed_vc_blogs.js --region asia Seed only Asia/SEA
//   node scripts/seed_vc_blogs.js --stats      Show current source counts
//   node scripts/seed_vc_blogs.js --test       Test feed accessibility
//
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const https = require('https');
const http = require('http');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ─────────────────────────────────────────────────────────────────────────────
// US — Top 20 VC Blogs & Newsletters
// ─────────────────────────────────────────────────────────────────────────────

const US_SOURCES = [
  // === Tier 1: Top VC Firm Blogs ===
  {
    name: 'a16z Blog',
    url: 'https://a16z.com/feed/',
    credibility_score: 0.95,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Sequoia Capital',
    url: 'https://www.sequoiacap.com/feed/',
    credibility_score: 0.95,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  {
    name: 'First Round Review',
    url: 'https://review.firstround.com/feed.xml',
    credibility_score: 0.90,
    signal_types: ['leadership_change', 'strategic_hiring'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Y Combinator Blog',
    url: 'https://www.ycombinator.com/blog/rss/',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Bessemer Venture Partners',
    url: 'https://www.bvp.com/atlas/rss.xml',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 120,
  },

  // === Tier 2: Influential VC Bloggers ===
  {
    name: 'Fred Wilson (AVC)',
    url: 'https://avc.com/feed/',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Tomasz Tunguz',
    url: 'https://tomtunguz.com/index.xml',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Brad Feld',
    url: 'https://feld.com/feed',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Both Sides of the Table (Mark Suster)',
    url: 'https://bothsidesofthetable.com/feed',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'strategic_hiring'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Christoph Janz (Point Nine)',
    url: 'https://christophjanz.blogspot.com/feeds/posts/default/-/SaaS?alt=rss',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },

  // === Tier 3: Industry News & Newsletters ===
  {
    name: 'Crunchbase News',
    url: 'https://news.crunchbase.com/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'layoffs'],
    poll_interval_minutes: 30,
  },
  {
    name: 'VentureBeat',
    url: 'https://venturebeat.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 30,
  },
  {
    name: 'The Information',
    url: 'https://www.theinformation.com/feed',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 60,
  },
  {
    name: 'PitchBook News',
    url: 'https://pitchbook.com/news/feed',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 60,
  },
  {
    name: 'CB Insights',
    url: 'https://www.cbinsights.com/research/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'product_launch'],
    poll_interval_minutes: 60,
  },
  {
    name: 'StrictlyVC',
    url: 'https://www.strictlyvc.com/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Hacker News (Top)',
    url: 'https://hnrss.org/frontpage',
    credibility_score: 0.70,
    signal_types: ['product_launch', 'capital_raising'],
    poll_interval_minutes: 30,
  },
  {
    name: 'NVCA Blog',
    url: 'https://nvca.org/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Steve Blank',
    url: 'https://steveblank.com/feed/',
    credibility_score: 0.80,
    signal_types: ['product_launch', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Stratechery (Ben Thompson)',
    url: 'https://stratechery.com/feed/',
    credibility_score: 0.90,
    signal_types: ['ma_activity', 'product_launch', 'partnership'],
    poll_interval_minutes: 120,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// ASIA / SEA — Top 10
// ─────────────────────────────────────────────────────────────────────────────

const ASIA_SOURCES = [
  {
    name: 'DealStreetAsia',
    url: 'https://www.dealstreetasia.com/feed/',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 30,
  },
  {
    name: 'KrASIA',
    url: 'https://kr-asia.com/feed',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'geographic_expansion'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Vulcan Post (SEA Startups)',
    url: 'https://vulcanpost.com/feed/',
    credibility_score: 0.70,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 60,
  },
  {
    name: 'NEXEA (Malaysia/SEA VC)',
    url: 'https://nexea.co/feed/',
    credibility_score: 0.70,
    signal_types: ['capital_raising', 'partnership'],
    poll_interval_minutes: 120,
  },
  {
    name: 'AVCJ (Asia VC Journal)',
    url: 'https://avcj.com/feeds/rss',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Golden Gate Ventures Blog',
    url: 'https://www.goldengatevcs.com/blog-feed.xml',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Jungle Ventures Blog',
    url: 'https://www.jungleventures.com/feed',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Nikkei Asia (Business)',
    url: 'https://asia.nikkei.com/rss/feed/nar',
    credibility_score: 0.90,
    signal_types: ['ma_activity', 'capital_raising', 'leadership_change'],
    poll_interval_minutes: 30,
  },
  {
    name: 'TechNode (China/Asia)',
    url: 'https://technode.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Inc42 (India/SEA)',
    url: 'https://inc42.com/feed/',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'ma_activity'],
    poll_interval_minutes: 60,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// AUSTRALIA / ANZ — Top 10
// ─────────────────────────────────────────────────────────────────────────────

const AU_SOURCES = [
  {
    name: 'AirTree Ventures Blog',
    url: 'https://www.airtree.vc/open-source-vc/rss.xml',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'product_launch', 'strategic_hiring'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Blackbird Ventures Blog',
    url: 'https://www.blackbird.vc/blog/rss.xml',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Square Peg Capital Blog',
    url: 'https://www.squarepegcap.com/feed',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Overnight Success (AU VC Newsletter)',
    url: 'https://newsletter.overnightsuccess.vc/feed',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Startup Daily AU',
    url: 'https://www.startupdaily.net/topic/venture-capital/feed/',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 60,
  },
  {
    name: 'AFR Technology',
    url: 'https://www.afr.com/technology/rss',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change', 'restructuring'],
    poll_interval_minutes: 30,
  },
  {
    name: 'Investible Blog',
    url: 'https://www.investible.com/blog/rss.xml',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'strategic_hiring'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Innovation Aus',
    url: 'https://www.innovationaus.com/feed/',
    credibility_score: 0.75,
    signal_types: ['product_launch', 'capital_raising', 'partnership'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Giant Leap (Impact VC AU)',
    url: 'https://www.giantleap.com.au/blog/rss.xml',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Cut Through Venture (AU Data)',
    url: 'https://www.cutthrough.com/insights/rss.xml',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 120,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// UK — Top 10
// ─────────────────────────────────────────────────────────────────────────────

const UK_SOURCES = [
  {
    name: 'UKTN (UK Tech News)',
    url: 'https://www.uktech.news/feed',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'leadership_change'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Tech.eu',
    url: 'https://tech.eu/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'geographic_expansion'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Beauhurst Blog (UK Startups Data)',
    url: 'https://www.beauhurst.com/blog/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Startup Rise UK/EU',
    url: 'https://startuprise.co.uk/feed/',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Molten Ventures Blog',
    url: 'https://www.moltenventures.com/insights/rss',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Balderton Capital Blog',
    url: 'https://www.balderton.com/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Index Ventures Blog',
    url: 'https://www.indexventures.com/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'LocalGlobe Blog',
    url: 'https://localglobe.vc/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Seedcamp Blog',
    url: 'https://seedcamp.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'strategic_hiring'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Playfair Capital Blog',
    url: 'https://medium.com/feed/playfair-capital',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// EUROPE — Top 10
// ─────────────────────────────────────────────────────────────────────────────

const EU_SOURCES = [
  {
    name: 'Sifted EU',
    url: 'https://sifted.eu/feed',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'geographic_expansion'],
    poll_interval_minutes: 30,
  },
  {
    name: 'EU-Startups',
    url: 'https://www.eu-startups.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 60,
  },
  {
    name: 'Atomico Blog',
    url: 'https://www.atomico.com/feed/',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Northzone Blog',
    url: 'https://northzone.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'EQT Ventures Blog',
    url: 'https://eqtventures.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Speedinvest Blog',
    url: 'https://speedinvest.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Point Nine Land',
    url: 'https://medium.com/feed/point-nine-news',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Cherry Ventures Blog',
    url: 'https://www.cherry.vc/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'The Generalist (Mario Gabriele)',
    url: 'https://www.generalist.com/feed',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'product_launch'],
    poll_interval_minutes: 120,
  },
  {
    name: 'PE Insights (EU PE/VC)',
    url: 'https://pe-insights.com/feed/',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 120,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// ALL SOURCES MAP
// ─────────────────────────────────────────────────────────────────────────────

const REGION_MAP = {
  us: { label: 'US (20)', sources: US_SOURCES },
  asia: { label: 'Asia/SEA (10)', sources: ASIA_SOURCES },
  au: { label: 'Australia/ANZ (10)', sources: AU_SOURCES },
  uk: { label: 'UK (10)', sources: UK_SOURCES },
  eu: { label: 'Europe (10)', sources: EU_SOURCES },
};

// ─────────────────────────────────────────────────────────────────────────────
// SEED FUNCTION
// ─────────────────────────────────────────────────────────────────────────────

async function seedRegion(regionKey) {
  const region = REGION_MAP[regionKey];
  if (!region) {
    console.log(`  ❌ Unknown region: ${regionKey}`);
    return { inserted: 0, skipped: 0 };
  }

  console.log(`\n── ${region.label} ──\n`);

  let inserted = 0;
  let skipped = 0;

  for (const src of region.sources) {
    // Safe check — no assumption about UNIQUE constraint on url
    const exists = await pool.query(
      `SELECT id FROM rss_sources WHERE url = $1 LIMIT 1`,
      [src.url]
    );

    if (exists.rows.length > 0) {
      console.log(`  ⏭️  ${src.name}`);
      skipped++;
      continue;
    }

    // Also check by name to avoid near-dupes
    const nameExists = await pool.query(
      `SELECT id FROM rss_sources WHERE LOWER(name) = LOWER($1) LIMIT 1`,
      [src.name]
    );

    if (nameExists.rows.length > 0) {
      console.log(`  ⏭️  ${src.name} (name match)`);
      skipped++;
      continue;
    }

    try {
      await pool.query(
        `INSERT INTO rss_sources (name, source_type, url, poll_interval_minutes, enabled, credibility_score, signal_types, consecutive_errors, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 0, NOW())`,
        [
          src.name,
          'vc_blog',
          src.url,
          src.poll_interval_minutes,
          true,
          src.credibility_score,
          src.signal_types,
        ]
      );
      console.log(`  ✅ ${src.name}`);
      inserted++;
    } catch (err) {
      console.log(`  ❌ ${src.name}: ${err.message}`);
    }
  }

  return { inserted, skipped };
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST FEED ACCESSIBILITY
// ─────────────────────────────────────────────────────────────────────────────

function testUrl(url) {
  return new Promise((resolve) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, {
      headers: { 'User-Agent': 'MitchelLake-SignalBot/1.0' },
      timeout: 10000,
    }, (res) => {
      resolve({ status: res.statusCode, ok: res.statusCode >= 200 && res.statusCode < 400 });
    });
    req.on('timeout', () => { req.destroy(); resolve({ status: 'timeout', ok: false }); });
    req.on('error', (err) => resolve({ status: err.code || err.message, ok: false }));
  });
}

async function testFeeds(regionKey) {
  const regions = regionKey ? [regionKey] : Object.keys(REGION_MAP);

  for (const key of regions) {
    const region = REGION_MAP[key];
    console.log(`\n── Testing: ${region.label} ──\n`);

    for (const src of region.sources) {
      const result = await testUrl(src.url);
      const icon = result.ok ? '✅' : '❌';
      console.log(`  ${icon} [${String(result.status).padEnd(7)}] ${src.name}`);
      if (!result.ok) console.log(`       └─ ${src.url}`);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS
// ─────────────────────────────────────────────────────────────────────────────

async function showStats() {
  console.log('\n📊 RSS Source Breakdown\n');

  const result = await pool.query(`
    SELECT source_type, 
           COUNT(*) AS total,
           COUNT(*) FILTER (WHERE enabled = true) AS enabled,
           COUNT(*) FILTER (WHERE last_fetched_at IS NOT NULL) AS fetched,
           COUNT(*) FILTER (WHERE consecutive_errors > 0) AS errors
    FROM rss_sources
    GROUP BY source_type
    ORDER BY total DESC
  `);

  let grandTotal = 0;
  for (const row of result.rows) {
    console.log(`  ${row.source_type.padEnd(15)} total: ${String(row.total).padStart(3)}  enabled: ${String(row.enabled).padStart(3)}  fetched: ${String(row.fetched).padStart(3)}  errors: ${String(row.errors).padStart(3)}`);
    grandTotal += parseInt(row.total);
  }
  console.log(`  ${'─'.repeat(65)}`);
  console.log(`  ${'TOTAL'.padEnd(15)} ${String(grandTotal).padStart(3)} sources\n`);

  // Show VC blog sources specifically
  const vcResult = await pool.query(`
    SELECT name, enabled, last_fetched_at, consecutive_errors
    FROM rss_sources
    WHERE source_type = 'vc_blog'
    ORDER BY name
  `);

  if (vcResult.rows.length > 0) {
    console.log(`VC Blog sources (${vcResult.rows.length}):`);
    for (const row of vcResult.rows) {
      const fetched = row.last_fetched_at ? new Date(row.last_fetched_at).toISOString().slice(0, 16) : 'never';
      const status = !row.enabled ? '⏸️' : row.consecutive_errors > 0 ? '❌' : '✅';
      console.log(`  ${status} ${row.name.padEnd(40)} last: ${fetched}`);
    }
  }
  console.log('');
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);

  console.log('═══════════════════════════════════════════════════');
  console.log('  MitchelLake Signal Intelligence');
  console.log('  VC Blogs & Newsletters — RSS Source Seeder');
  console.log('═══════════════════════════════════════════════════');

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');

    if (args.includes('--stats')) {
      await showStats();
      return;
    }

    if (args.includes('--test')) {
      const regionIdx = args.indexOf('--region');
      const regionKey = regionIdx >= 0 ? args[regionIdx + 1] : null;
      await testFeeds(regionKey);
      return;
    }

    // Determine which regions to seed
    const regionIdx = args.indexOf('--region');
    const regionFilter = regionIdx >= 0 ? args[regionIdx + 1] : null;
    const regions = regionFilter ? [regionFilter] : Object.keys(REGION_MAP);

    let totalInserted = 0;
    let totalSkipped = 0;

    for (const key of regions) {
      const { inserted, skipped } = await seedRegion(key);
      totalInserted += inserted;
      totalSkipped += skipped;
    }

    console.log('\n═══════════════════════════════════════════════════');
    console.log(`📊 SEED COMPLETE`);
    console.log(`   Inserted: ${totalInserted}`);
    console.log(`   Skipped:  ${totalSkipped} (already existed)`);
    console.log(`   Total:    ${totalInserted + totalSkipped}`);
    console.log('═══════════════════════════════════════════════════');

    if (totalInserted > 0) {
      console.log(`\n💡 Next: Update harvest_news_pr.js to also harvest 'vc_blog' sources,`);
      console.log(`   or run a harvest with: node scripts/harvest_vc_blogs.js`);
      console.log(`   (We'll use the same harvest_news_pr.js logic — just change the source_type filter)\n`);
    }

  } catch (err) {
    console.error('\n❌ Fatal error:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
