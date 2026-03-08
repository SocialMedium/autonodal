#!/usr/bin/env node
/**
 * MitchelLake Signal Intelligence — Podcast Harvester
 * 
 * Monitors VC, tech, and executive podcasts for:
 *   - Executive mentions (potential candidates)
 *   - Company hiring signals
 *   - Market movement signals
 *   - Speaking engagements (visibility signals)
 * 
 * Writes to: person_content_sources, person_content, signal_events
 * 
 * Consolidated from ResearchMedium: harvest_podcasts.js, backfill_podcasts.js,
 *   extract_podcast_meta.js, add_*_podcasts.js
 * 
 * Usage:
 *   node scripts/harvest_podcasts.js --seed           # Seed podcast sources
 *   node scripts/harvest_podcasts.js --poll           # Poll all active feeds
 *   node scripts/harvest_podcasts.js --poll --source "Equity"  # Poll one
 *   node scripts/harvest_podcasts.js --backfill       # Full backfill (24mo)
 *   node scripts/harvest_podcasts.js --detect         # Run person detection on new episodes
 *   node scripts/harvest_podcasts.js --stats          # Show statistics
 */

require('dotenv').config();
const { Pool } = require('pg');
const Parser = require('rss-parser');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

const parser = new Parser({
  timeout: 20000,
  headers: { 'User-Agent': 'MitchelLakeSignals/1.0 (podcast-harvester)' },
  maxRedirects: 5,
});

const DELAY_MS = 1500;
const MAX_EPISODES_PER_POLL = 15;
const BACKFILL_MONTHS = 24;

// ══════════════════════════════════════════════════════════════
// PODCAST SOURCES — exec search / VC / tech relevant
// ══════════════════════════════════════════════════════════════

const PODCAST_SOURCES = [
  // ── VC / Startups ──
  { name: 'Equity (TechCrunch)', slug: 'equity', rss: 'https://feeds.megaphone.fm/equitypod', category: 'vc', region: 'us' },
  { name: 'This Week in Startups', slug: 'twist', rss: 'https://feeds.megaphone.fm/thisweekinstartups', category: 'vc', region: 'us' },
  { name: 'BG2Pod', slug: 'bg2pod', rss: 'https://feeds.megaphone.fm/bg2pod', category: 'vc', region: 'us' },
  { name: 'Invest Like the Best', slug: 'invest-like-best', rss: 'https://feeds.megaphone.fm/investlikethebest', category: 'vc', region: 'us' },
  { name: 'The Prof G Pod', slug: 'profg', rss: 'https://feeds.megaphone.fm/WWO3519750118', category: 'markets', region: 'us' },
  { name: 'Odd Lots (Bloomberg)', slug: 'odd-lots', rss: 'https://feeds.megaphone.fm/GLT1412515089', category: 'markets', region: 'us' },
  { name: 'The Logan Bartlett Show', slug: 'logan-bartlett', rss: 'https://feeds.megaphone.fm/loganbartlett', category: 'vc', region: 'us' },
  { name: 'How I Built This', slug: 'how-i-built-this', rss: 'https://feeds.simplecast.com/EEl4bSAb', category: 'vc', region: 'us' },
  { name: 'Capital Allocators', slug: 'capital-allocators', rss: 'https://feeds.simplecast.com/tOjNXec5', category: 'markets', region: 'us' },

  // ── Tech / AI ──
  { name: 'Techmeme Ride Home', slug: 'techmeme', rss: 'https://feeds.megaphone.fm/techmemeridehome', category: 'tech', region: 'us' },
  { name: 'No Priors', slug: 'no-priors', rss: 'https://feeds.megaphone.fm/nopriors', category: 'tech', region: 'us' },
  { name: 'Eye on AI', slug: 'eye-on-ai', rss: 'https://rss.buzzsprout.com/2225935.rss', category: 'tech', region: 'us' },
  { name: 'Practical AI', slug: 'practical-ai', rss: 'https://feeds.transistor.fm/practical-ai-machine-learning-data-science-llm', category: 'tech', region: 'us' },

  // ── Fintech ──
  { name: 'Fintech Takes', slug: 'fintech-takes', rss: 'https://feeds.simplecast.com/4MvgQ73R', category: 'fintech', region: 'us' },
  { name: 'Bankless', slug: 'bankless', rss: 'https://feeds.simplecast.com/l2i9YnTd', category: 'fintech', region: 'us' },

  // ── Biotech / Healthcare ──
  { name: 'The Long Run', slug: 'the-long-run', rss: 'https://feeds.simplecast.com/dHoohVNH', category: 'biotech', region: 'us' },

  // ── Climate ──
  { name: 'My Climate Journey', slug: 'my-climate-journey', rss: 'https://feeds.simplecast.com/pMsmqdGq', category: 'climate', region: 'us' },
  { name: 'Catalyst (Shayle Kann)', slug: 'catalyst', rss: 'https://feeds.simplecast.com/BG9MbNjf', category: 'climate', region: 'us' },

  // ── Cybersecurity ──
  { name: 'Risky Business', slug: 'risky-business', rss: 'https://risky.biz/feeds/risky-business/', category: 'cybersecurity', region: 'us' },

  // ── UK / EU ──
  { name: 'Riding Unicorns', slug: 'riding-unicorns', rss: 'https://rss.buzzsprout.com/1620178.rss', category: 'vc', region: 'uk' },
  { name: 'Sifted Podcast', slug: 'sifted-pod', rss: 'https://rss.buzzsprout.com/1877446.rss', category: 'tech', region: 'eu' },
  { name: 'EU Startups Podcast', slug: 'eu-startups', rss: 'https://eu-startups.podigee.io/feed/mp3', category: 'startup', region: 'eu' },
  { name: 'Seedcamp Sessions', slug: 'seedcamp', rss: 'https://feeds.soundcloud.com/users/soundcloud:users:126198189/sounds.rss', category: 'vc', region: 'eu' },

  // ── ANZ (Australia / NZ) ──
  { name: 'Wild Hearts (Blackbird)', slug: 'wild-hearts', rss: 'https://feeds.captivate.fm/wild-hearts-blackbird/', category: 'vc', region: 'anz' },
  { name: 'AirTree Podcast', slug: 'airtree', rss: 'https://feeds.buzzsprout.com/2307739.rss', category: 'vc', region: 'anz' },
  { name: 'Startup Daily AU Pod', slug: 'startup-daily-pod', rss: 'https://feeds.megaphone.fm/startup-360', category: 'startup', region: 'anz' },
  { name: 'Cut Through Venture', slug: 'cut-through-venture', rss: 'https://feed.podbean.com/tribeglobalventures/feed.xml', category: 'vc', region: 'anz' },

  // ── Asia ──
  { name: 'BRAVE Southeast Asia Tech', slug: 'brave-sea', rss: 'https://feed.pod.co/bravedynamics', category: 'vc', region: 'asia' },
  { name: 'Analyse Asia', slug: 'analyse-asia', rss: 'https://anchor.fm/s/10808ffd0/podcast/rss', category: 'tech', region: 'asia' },
  { name: 'Hard Truths by Vertex', slug: 'hard-truths-vertex', rss: 'https://feeds.transistor.fm/hard-truths-by-vertex', category: 'vc', region: 'asia' },

  // ── Web3 ──
  { name: 'Unchained (Laura Shin)', slug: 'unchained', rss: 'https://feeds.megaphone.fm/LSHML4761942757', category: 'web3', region: 'us' },
  { name: 'Empire (Blockworks)', slug: 'empire-blockworks', rss: 'https://feeds.megaphone.fm/empire', category: 'web3', region: 'us' },
];

// ══════════════════════════════════════════════════════════════
// PERSON / SIGNAL DETECTION IN EPISODE METADATA
// ══════════════════════════════════════════════════════════════

const EXEC_TITLE_PATTERNS = /\b(CEO|CTO|CFO|COO|CRO|CPO|CMO|CISO|VP|SVP|EVP|founder|co-?founder|partner|director|head of|managing director|president|chairman|chief)\b/i;

const SIGNAL_KEYWORDS = {
  capital_raising: /\b(series [A-Z]|raise[ds]?\s+\$|funding|IPO|going public|unicorn|valuation)\b/i,
  ma_activity: /\b(acqui|merger|acquisition|takeover|buyout|exit)\b/i,
  leadership: /\b(CEO|CTO|CFO|new hire|appointed|stepped down|resign|founder)\b/i,
  expansion: /\b(expand|new market|international|launch|scale|growth)\b/i,
  layoffs: /\b(layoff|restructur|downsiz|cut.*jobs)\b/i,
};

const THEME_KEYWORDS = {
  ai: /\b(artificial intelligence|machine learning|AI|LLM|GPT|generative|foundation model)\b/i,
  fintech: /\b(fintech|crypto|blockchain|DeFi|payments|banking|web3)\b/i,
  healthcare: /\b(healthcare|biotech|pharma|clinical|drug|FDA|medical)\b/i,
  cybersecurity: /\b(cybersecurity|security|hacking|threat|zero trust)\b/i,
  cleantech: /\b(climate|energy|solar|EV|battery|sustainability|carbon)\b/i,
  semiconductor: /\b(semiconductor|chip|GPU|NVIDIA|AMD|Intel)\b/i,
};

function extractEpisodeSignals(title, description) {
  const text = `${title} ${description || ''}`;
  const signals = Object.entries(SIGNAL_KEYWORDS).filter(([, p]) => p.test(text)).map(([s]) => s);
  const themes = Object.entries(THEME_KEYWORDS).filter(([, p]) => p.test(text)).map(([t]) => t);
  const hasExec = EXEC_TITLE_PATTERNS.test(text);
  return { signals, themes, hasExec };
}

// ══════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════

const sleep = ms => new Promise(r => setTimeout(r, ms));

function parseDuration(d) {
  if (typeof d === 'number') return d;
  if (!d) return null;
  const parts = String(d).split(':').map(Number);
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  return parseInt(d) || null;
}

// ══════════════════════════════════════════════════════════════
// SEED SOURCES
// ══════════════════════════════════════════════════════════════

async function seedSources() {
  console.log('\n🎙️  Seeding podcast sources into person_content_sources...\n');
  let added = 0;

  for (const src of PODCAST_SOURCES) {
    try {
      const result = await pool.query(`
        INSERT INTO person_content_sources (
          source_type, source_url, source_name, polling_frequency, active, metadata, created_at
        ) VALUES ('podcast_feed', $1, $2, 'daily', true, $3, NOW())
        ON CONFLICT (source_url) DO NOTHING RETURNING id
      `, [src.rss, src.name, JSON.stringify({ slug: src.slug, category: src.category, region: src.region })]);
      
      if (result.rowCount > 0) { console.log(`  ✅ ${src.name} [${src.region}/${src.category}]`); added++; }
      else console.log(`  ⏭️  ${src.name} (exists)`);
    } catch (err) { console.log(`  ❌ ${src.name}: ${err.message}`); }
  }

  const total = await pool.query(`SELECT COUNT(*) as c FROM person_content_sources WHERE source_type = 'podcast_feed' AND active = true`);
  console.log(`\n📊 Added ${added} new | Total active podcast feeds: ${total.rows[0].c}`);
}

// ══════════════════════════════════════════════════════════════
// POLL / BACKFILL
// ══════════════════════════════════════════════════════════════

async function pollFeeds(sourceFilter, backfill = false) {
  const cutoff = backfill ? new Date(Date.now() - BACKFILL_MONTHS * 30 * 86400000) : null;
  const maxEpisodes = backfill ? 500 : MAX_EPISODES_PER_POLL;
  const mode = backfill ? 'BACKFILL' : 'POLL';
  
  let query = `SELECT id, source_url, source_name, metadata FROM person_content_sources WHERE source_type = 'podcast_feed' AND active = true`;
  const params = [];
  if (sourceFilter) { query += ` AND source_name ILIKE $1`; params.push(`%${sourceFilter}%`); }
  query += ` ORDER BY source_name`;

  const { rows: sources } = await pool.query(query, params);
  
  console.log('═'.repeat(60));
  console.log(`  🎙️  PODCAST ${mode} — ${sources.length} feeds`);
  if (backfill) console.log(`  Cutoff: ${cutoff.toISOString().split('T')[0]}`);
  console.log('═'.repeat(60) + '\n');

  let totalNew = 0, totalSkipped = 0, totalSignals = 0, errors = 0;

  for (const source of sources) {
    const meta = typeof source.metadata === 'string' ? JSON.parse(source.metadata) : source.metadata || {};
    console.log(`📡 ${source.source_name} [${meta.region || '?'}/${meta.category || '?'}]`);

    try {
      const feed = await parser.parseURL(source.source_url);
      const allItems = feed.items || [];
      const items = allItems.slice(0, maxEpisodes);
      console.log(`   Feed: ${allItems.length} items, processing ${items.length}`);

      let newCount = 0, skipCount = 0, sigCount = 0;

      for (const item of items) {
        // Date filter for backfill
        if (cutoff && item.pubDate) {
          const d = new Date(item.pubDate);
          if (!isNaN(d.getTime()) && d < cutoff) continue;
        }

        const guid = item.guid || item.id || item.link || item.title;
        if (!guid) continue;
        const contentHash = crypto.createHash('md5').update(guid + source.id).digest('hex');

        // Check dupe
        const existing = await pool.query(`SELECT id FROM person_content WHERE content_hash = $1`, [contentHash]);
        if (existing.rows.length > 0) { skipCount++; continue; }

        let publishedAt = null;
        if (item.pubDate || item.isoDate) {
          const d = new Date(item.pubDate || item.isoDate);
          if (!isNaN(d.getTime())) publishedAt = d.toISOString();
        }

        const duration = parseDuration(item.itunes?.duration);
        const audioUrl = item.enclosure?.url || null;
        const episodeUrl = item.link || null;
        const description = item.contentSnippet || item.content || item.description || '';

        // Detect signals
        const { signals, themes, hasExec } = extractEpisodeSignals(item.title || '', description);

        // Insert episode into person_content
        try {
          const result = await pool.query(`
            INSERT INTO person_content (
              source_id, content_type, title, content, url, content_hash,
              published_at, metadata, created_at
            ) VALUES ($1, 'podcast_episode', $2, $3, $4, $5, $6, $7, NOW())
            RETURNING id
          `, [
            source.id,
            (item.title || 'Untitled').substring(0, 500),
            description.substring(0, 5000),
            episodeUrl || audioUrl,
            contentHash,
            publishedAt,
            JSON.stringify({
              audio_url: audioUrl, duration_seconds: duration,
              signals, themes, has_exec_mention: hasExec,
              source_slug: meta.slug, region: meta.region, category: meta.category,
            }),
          ]);
          newCount++;

          // Generate signal_events for high-value episodes
          if (signals.length > 0 && hasExec) {
            for (const sig of signals) {
              try {
                await pool.query(`
                  INSERT INTO signal_events (
                    signal_type, signal_category, title, evidence,
                    source_url, source_type, confidence, detected_at, metadata
                  ) VALUES ($1, 'podcast', $2, $3, $4, 'podcast', 0.5, NOW(), $5)
                `, [
                  sig, (item.title || '').substring(0, 300), description.substring(0, 500),
                  episodeUrl,
                  JSON.stringify({ themes, podcast: source.source_name, region: meta.region }),
                ]);
                sigCount++;
              } catch (e) { /* dup */ }
            }
          }
        } catch (err) { skipCount++; }
      }

      // Update last polled
      await pool.query(`UPDATE person_content_sources SET last_polled_at = NOW() WHERE id = $1`, [source.id]);

      console.log(`   ✅ ${newCount} new, ${skipCount} skipped, ${sigCount} signals\n`);
      totalNew += newCount; totalSkipped += skipCount; totalSignals += sigCount;
    } catch (err) {
      console.log(`   ❌ ${err.message}\n`);
      errors++;
    }
    await sleep(DELAY_MS);
  }

  console.log('═'.repeat(60));
  console.log(`  ${mode} DONE: ${totalNew} new episodes, ${totalSignals} signals, ${errors} errors`);
  console.log('═'.repeat(60));
}

// ══════════════════════════════════════════════════════════════
// PERSON DETECTION — match episode guests to MLX people db
// ══════════════════════════════════════════════════════════════

async function detectPersonMentions() {
  console.log('\n🔍 Running person detection on unprocessed podcast episodes...\n');

  // Get unprocessed episodes with exec mentions
  const { rows: episodes } = await pool.query(`
    SELECT id, title, content, metadata FROM person_content 
    WHERE content_type = 'podcast_episode' 
      AND (metadata->>'has_exec_mention')::boolean = true
      AND person_id IS NULL
    ORDER BY published_at DESC
    LIMIT 200
  `);

  console.log(`  Found ${episodes.length} episodes with exec mentions to process`);

  // Load people name index (first + last name combos)
  const { rows: people } = await pool.query(`
    SELECT id, full_name, normalized_name, current_company_name, current_title
    FROM people WHERE full_name IS NOT NULL
  `);
  
  const nameIndex = new Map();
  for (const p of people) {
    const key = p.full_name.toLowerCase().trim();
    if (key.length > 4) nameIndex.set(key, p);
  }
  console.log(`  Name index: ${nameIndex.size} people\n`);

  let matched = 0;
  for (const ep of episodes) {
    const text = `${ep.title} ${ep.content || ''}`.toLowerCase();
    
    // Check each person name against episode text
    for (const [name, person] of nameIndex) {
      if (text.includes(name)) {
        // Create a person_signal for the mention
        try {
          await pool.query(`
            INSERT INTO person_signals (
              person_id, signal_type, signal_category, value, evidence,
              source_type, confidence, detected_at, metadata
            ) VALUES ($1, 'podcast_mention', 'visibility', $2, $3, 'podcast', 0.7, NOW(), $4)
          `, [
            person.id,
            ep.title,
            `Mentioned in podcast episode: ${ep.title}`,
            JSON.stringify({ content_id: ep.id, ...(typeof ep.metadata === 'string' ? JSON.parse(ep.metadata) : ep.metadata) }),
          ]);
          matched++;
        } catch (e) { /* dup */ }
      }
    }
  }

  console.log(`  ✅ ${matched} person-podcast matches found`);
}

// ══════════════════════════════════════════════════════════════
// STATS
// ══════════════════════════════════════════════════════════════

async function showStats() {
  console.log('═'.repeat(60));
  console.log('  📊 PODCAST HARVESTER — STATS');
  console.log('═'.repeat(60));

  const sources = await pool.query(`SELECT COUNT(*) as c FROM person_content_sources WHERE source_type = 'podcast_feed' AND active = true`);
  console.log(`\n  Active feeds: ${sources.rows[0].c}`);

  const episodes = await pool.query(`SELECT COUNT(*) as c FROM person_content WHERE content_type = 'podcast_episode'`);
  console.log(`  Total episodes: ${episodes.rows[0].c}`);

  const signals = await pool.query(`SELECT COUNT(*) as c FROM signal_events WHERE source_type = 'podcast'`);
  console.log(`  Podcast signals: ${signals.rows[0].c}`);

  const bySource = await pool.query(`
    SELECT pcs.source_name, COUNT(pc.id) as eps, pcs.last_polled_at,
           pcs.metadata->>'region' as region, pcs.metadata->>'category' as category
    FROM person_content_sources pcs
    LEFT JOIN person_content pc ON pcs.id = pc.source_id AND pc.content_type = 'podcast_episode'
    WHERE pcs.source_type = 'podcast_feed' AND pcs.active = true
    GROUP BY pcs.id ORDER BY eps DESC
  `);
  
  if (bySource.rows.length > 0) {
    console.log('\n  Source                          | Reg  | Cat       | Episodes | Last Poll');
    console.log('  ' + '─'.repeat(80));
    for (const r of bySource.rows) {
      const nm = r.source_name.substring(0, 33).padEnd(33);
      const reg = (r.region || '?').padEnd(4);
      const cat = (r.category || '?').padEnd(9);
      const dt = r.last_polled_at ? new Date(r.last_polled_at).toISOString().substring(0, 10) : 'Never';
      console.log(`  ${nm}| ${reg} | ${cat} | ${String(r.eps).padStart(8)} | ${dt}`);
    }
  }

  const byRegion = await pool.query(`
    SELECT pcs.metadata->>'region' as region, COUNT(pc.id) as eps
    FROM person_content_sources pcs
    LEFT JOIN person_content pc ON pcs.id = pc.source_id
    WHERE pcs.source_type = 'podcast_feed'
    GROUP BY region ORDER BY eps DESC
  `);
  if (byRegion.rows.length > 0) {
    console.log('\n  Episodes by region:');
    byRegion.rows.forEach(r => console.log(`    ${(r.region || 'unknown').padEnd(10)} ${String(r.eps).padStart(6)} episodes`));
  }

  console.log('\n' + '═'.repeat(60));
}

// ══════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  try {
    if (args.includes('--seed')) await seedSources();
    else if (args.includes('--poll')) {
      const i = args.indexOf('--source');
      await pollFeeds(i >= 0 ? args[i + 1] : null, false);
    }
    else if (args.includes('--backfill')) {
      const i = args.indexOf('--source');
      await pollFeeds(i >= 0 ? args[i + 1] : null, true);
    }
    else if (args.includes('--detect')) await detectPersonMentions();
    else if (args.includes('--stats')) await showStats();
    else {
      console.log(`
MitchelLake — Podcast Harvester

Usage:
  node scripts/harvest_podcasts.js --seed             Seed 33 podcast sources
  node scripts/harvest_podcasts.js --poll             Poll all active feeds
  node scripts/harvest_podcasts.js --poll --source X  Poll single feed
  node scripts/harvest_podcasts.js --backfill         Full backfill (24 months)
  node scripts/harvest_podcasts.js --detect           Match episodes to MLX people
  node scripts/harvest_podcasts.js --stats            Show statistics
      `);
    }
  } finally { await pool.end(); }
}

main().catch(err => { console.error('Fatal:', err); pool.end(); process.exit(1); });
