#!/usr/bin/env node
/**
 * MitchelLake Signal Intelligence — Enhanced News & PR Harvester
 * 
 * Harvests press releases, deal announcements, and tech news from RSS feeds.
 * Detects hiring signals, funding, M&A, executive movements.
 * Writes to: external_documents, signal_events, document_companies
 * 
 * Adapted from ResearchMedium harvest_news.js → PostgreSQL + MLX schema
 * 
 * Usage:
 *   node scripts/harvest_news_pr.js                  # Harvest all sources
 *   node scripts/harvest_news_pr.js --source gnw-ma  # Harvest one source
 *   node scripts/harvest_news_pr.js --stats          # Show statistics
 *   node scripts/harvest_news_pr.js --seed           # Seed new PR/news sources
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

const DELAY_MS = 2000;
const USER_AGENT = 'MitchelLakeSignals/1.0 (Executive Search Intelligence)';

// ══════════════════════════════════════════════════════════════
// SIGNAL DETECTION PATTERNS (exec search focused)
// ══════════════════════════════════════════════════════════════

const SIGNAL_PATTERNS = {
  capital_raising:       /\b(series [A-Z]|seed round|raise[ds]?\s+\$|funding round|venture capital|secured \$|raised \$|funding of \$|pre-IPO|growth equity|private placement)\b/i,
  geographic_expansion:  /\b(expands? (?:to|into|in)|opens? (?:new|office|headquarters)|enters? (?:the|new)|new (?:office|headquarters|hub|center)|relocat|establishes? (?:presence|operations))\b/i,
  strategic_hiring:      /\b(appoints?|names?|hires?|recruits?|brings? on)\s+(?:new\s+)?(?:chief|CEO|CTO|CFO|COO|CRO|CPO|CISO|CMO|VP|SVP|EVP|president|director|head of|managing director|partner|general manager|country manager)\b/i,
  ma_activity:           /\b(acquir|merger|acquisition|takeover|buyout|divest|spin.?off|carve.?out|definitive agreement|combines? with|strategic review)\b/i,
  partnership:           /\b(partner(?:ship|ed|ing)|collaborat(?:ion|e|ing)|joint venture|strategic alliance|teaming agreement)\b/i,
  product_launch:        /\b(launch(?:es|ed|ing)?|unveil(?:s|ed)?|introduce[ds]?|announce[ds]?.*(?:product|platform|solution)|generally available)\b/i,
  layoffs:               /\b(layoff|laid off|restructur|downsiz|cut.*(?:jobs|staff|workforce)|workforce reduction|redundanc)\b/i,
  leadership_departure:  /\b(step(?:s|ped)? down|resign(?:s|ed)?|depart(?:s|ed|ure)|leaves?|left|exit(?:s|ed)?)\s+(?:as\s+)?(?:chief|CEO|CTO|CFO|COO|president|chairman)\b/i,
  board_appointment:     /\b(board (?:of directors|appointment|member)|independent director|non-executive director|advisory board)\b/i,
  ipo_listing:           /\b(IPO|initial public offering|public listing|SPAC|direct listing|NYSE|NASDAQ.*listing|files? (?:for|to) (?:go )?public)\b/i,
  pe_buyout:             /\b(private equity|PE (?:firm|fund|backed)|leveraged buyout|LBO|management buyout|MBO|growth equity|bought by)\b/i,
};

const THEME_PATTERNS = {
  technology:     /\b(technology|software|SaaS|cloud|AI|artificial intelligence|machine learning|digital transformation|platform)\b/i,
  fintech:        /\b(fintech|financial technology|payment|neobank|blockchain|DeFi|digital banking|insurtech|regtech|lending)\b/i,
  healthcare:     /\b(healthcare|health ?tech|medical|clinical|telemedicine|diagnostics|pharma|biotech|life science)\b/i,
  cybersecurity:  /\b(cybersecurity|cyber security|infosec|zero trust|threat detection|endpoint security)\b/i,
  cleantech:      /\b(clean ?tech|renewable|solar|wind|hydrogen|carbon|EV|electric vehicle|battery|sustainability|climate)\b/i,
  ecommerce:      /\b(e.?commerce|marketplace|retail tech|D2C|direct.?to.?consumer)\b/i,
  proptech:       /\b(prop ?tech|real estate tech|construction tech|smart building)\b/i,
  gaming:         /\b(gaming|game studio|esports|metaverse)\b/i,
  media:          /\b(media|content|streaming|creator economy|digital media|publishing)\b/i,
  logistics:      /\b(supply chain|logistics|freight|warehouse|last.?mile|shipping)\b/i,
  semiconductor:  /\b(semiconductor|chip|wafer|fab|GPU|processor)\b/i,
};

const GEO_PATTERNS = {
  australia:       /\b(Australia|Sydney|Melbourne|Brisbane|Perth|Adelaide|Canberra|ASX)\b/i,
  new_zealand:     /\b(New Zealand|Auckland|Wellington|NZX)\b/i,
  singapore:       /\b(Singapore|SGX|Temasek|GIC)\b/i,
  southeast_asia:  /\b(Southeast Asia|SEA|Indonesia|Thailand|Vietnam|Philippines|Malaysia|Jakarta|Bangkok)\b/i,
  uk:              /\b(United Kingdom|UK|London|LSE|Manchester|Edinburgh|FTSE)\b/i,
  us:              /\b(United States|US|USA|Silicon Valley|San Francisco|New York|NYSE|NASDAQ)\b/i,
  europe:          /\b(Europe|EU|Germany|France|Netherlands|Berlin|Amsterdam|Paris|Stockholm)\b/i,
};

// ══════════════════════════════════════════════════════════════
// NEWS/PR RSS SOURCES (to seed into rss_sources table)
// ══════════════════════════════════════════════════════════════

const NEWS_SOURCES = [
  { name: 'GlobeNewswire — M&A', slug: 'gnw-ma', category: 'deals', region: 'global',
    url: 'https://www.globenewswire.com/RssFeed/subjectcode/27-Mergers%20and%20Acquisitions/feedTitle/GlobeNewswire%20-%20Mergers%20and%20Acquisitions' },
  { name: 'GlobeNewswire — Business Contracts', slug: 'gnw-contracts', category: 'deals', region: 'global',
    url: 'https://www.globenewswire.com/RssFeed/subjectcode/7-Business%20Contracts/feedTitle/GlobeNewswire%20-%20Business%20Contracts' },
  { name: 'GlobeNewswire — Public Companies', slug: 'gnw-public', category: 'corporate', region: 'global',
    url: 'https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies' },
  { name: 'PR Newswire — Technology', slug: 'prn-tech', category: 'tech', region: 'global',
    url: 'https://www.prnewswire.com/rss/technology-latest-news/technology-latest-news-list.rss' },
  { name: 'PR Newswire — Financial Services', slug: 'prn-finance', category: 'fintech', region: 'global',
    url: 'https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss' },
  { name: 'Business Wire — Technology', slug: 'bw-tech', category: 'tech', region: 'global',
    url: 'https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpTXA==' },
  { name: 'TechCrunch', slug: 'tc-main', category: 'tech', region: 'global',
    url: 'https://techcrunch.com/feed/' },
  { name: 'Startup Daily AU', slug: 'startup-daily-au', category: 'startup', region: 'australia',
    url: 'https://www.startupdaily.net/feed/' },
  { name: 'SmartCompany AU', slug: 'smartcompany-au', category: 'business', region: 'australia',
    url: 'https://www.smartcompany.com.au/feed/' },
  { name: 'Tech in Asia', slug: 'tech-in-asia', category: 'tech', region: 'sea',
    url: 'https://www.techinasia.com/feed' },
  { name: 'e27', slug: 'e27-sea', category: 'startup', region: 'sea',
    url: 'https://e27.co/feed/' },
  { name: 'Sifted EU', slug: 'sifted-eu', category: 'tech', region: 'europe',
    url: 'https://sifted.eu/feed' },
  { name: 'UKTN', slug: 'uktn', category: 'tech', region: 'uk',
    url: 'https://www.uktech.news/feed' },
];

// ══════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════

const sleep = ms => new Promise(r => setTimeout(r, ms));

function stripHTML(str) {
  if (!str) return '';
  return str.replace(/<!\[CDATA\[(.*?)\]\]>/gs, '$1').replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#x27;/g, "'").replace(/&[a-z]+;/gi, ' ')
    .replace(/\s+/g, ' ').trim();
}

function md5(str) { return crypto.createHash('md5').update(str || '').digest('hex'); }

function detectSignals(text) {
  return Object.entries(SIGNAL_PATTERNS).filter(([, p]) => p.test(text)).map(([s]) => s);
}
function detectThemes(text) {
  return Object.entries(THEME_PATTERNS).filter(([, p]) => p.test(text)).map(([t]) => t);
}
function detectGeographies(text) {
  return Object.entries(GEO_PATTERNS).filter(([, p]) => p.test(text)).map(([g]) => g);
}

function extractCompanyHint(text) {
  const m = text.match(/\b([A-Z][A-Za-z&. ]{2,30}(?:Inc|Corp|Ltd|LLC|Co|SA|AG|PLC|Limited|Group|Holdings|Ventures|Capital|Partners)\.?)\b/)
    || text.match(/\b([A-Z][A-Za-z]{2,20})\s+(?:today announced|announced today|reports|launched|unveiled|appoints?|names?|hires?)/);
  return m ? m[1].trim() : null;
}

function extractAmount(text) {
  const m = text.match(/\$\s*([\d,.]+)\s*(billion|B)\b/i) || text.match(/\$\s*([\d,.]+)\s*(million|M)\b/i);
  if (!m) return null;
  const num = parseFloat(m[1].replace(/,/g, ''));
  return m[2].toLowerCase().startsWith('b') ? num * 1000 : num;
}

function isEnglish(text) {
  if (!text) return true;
  return (text.match(/\b(the|and|of|to|in|for|is|that|with|this|from|has|was|are|will|said)\b/gi) || []).length >= 2;
}

function scoreConfidence(signals, themes, geos) {
  let score = 0.3;
  if (signals.includes('strategic_hiring')) score += 0.3;
  if (signals.includes('capital_raising')) score += 0.2;
  if (signals.includes('geographic_expansion')) score += 0.15;
  if (signals.includes('leadership_departure')) score += 0.25;
  if (signals.includes('pe_buyout')) score += 0.15;
  if (signals.includes('layoffs')) score += 0.1;
  if (geos.some(g => ['australia', 'new_zealand', 'singapore', 'southeast_asia', 'uk'].includes(g))) score += 0.1;
  return Math.min(score, 1.0);
}

// ══════════════════════════════════════════════════════════════
// RSS PARSING
// ══════════════════════════════════════════════════════════════

async function fetchAndParseRSS(url) {
  const { parseString } = require('xml2js');
  const response = await fetch(url, {
    headers: { 'User-Agent': USER_AGENT, 'Accept': 'application/rss+xml, application/xml, text/xml, */*' },
    signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const xml = await response.text();

  return new Promise((resolve, reject) => {
    parseString(xml, { explicitArray: false, trim: true }, (err, result) => {
      if (err) return reject(err);
      // RSS 2.0
      const channel = result?.rss?.channel;
      if (channel) {
        const items = Array.isArray(channel.item) ? channel.item : channel.item ? [channel.item] : [];
        return resolve(items.map(item => ({
          title: stripHTML(typeof item.title === 'object' ? item.title._ || '' : item.title || ''),
          description: stripHTML(typeof item.description === 'object' ? item.description._ || '' : item.description || ''),
          url: typeof item.link === 'object' ? item.link._ || '' : item.link || '',
          guid: item.guid?._ || item.guid || item.link || '',
          pubDate: item.pubDate || item['dc:date'] || null,
          contributor: item['dc:creator'] || item.author || null,
        })));
      }
      // Atom
      const feed = result?.feed;
      if (feed) {
        const entries = Array.isArray(feed.entry) ? feed.entry : feed.entry ? [feed.entry] : [];
        return resolve(entries.map(e => ({
          title: stripHTML(typeof e.title === 'object' ? e.title._ || '' : e.title || ''),
          description: stripHTML(typeof e.summary === 'object' ? e.summary._ || '' : e.summary || e.content?._ || ''),
          url: e.link?.$?.href || (typeof e.link === 'string' ? e.link : '') || e.id || '',
          guid: e.id || e.link?.$?.href || '',
          pubDate: e.published || e.updated || null,
          contributor: e.author?.name || null,
        })));
      }
      resolve([]);
    });
  });
}

// ══════════════════════════════════════════════════════════════
// DATABASE OPERATIONS
// ══════════════════════════════════════════════════════════════

async function seedSources() {
  console.log('\n🌱 Seeding news/PR sources into rss_sources...\n');
  let added = 0;
  for (const src of NEWS_SOURCES) {
    try {
      const result = await pool.query(`
        INSERT INTO rss_sources (name, url, category, region, source_type, enabled, created_at)
        VALUES ($1, $2, $3, $4, 'news_pr', true, NOW())
        ON CONFLICT (url) DO NOTHING RETURNING id
      `, [src.name, src.url, src.category, src.region || 'global']);
      if (result.rowCount > 0) { console.log(`  ✅ ${src.name}`); added++; }
      else console.log(`  ⏭️  ${src.name} (exists)`);
    } catch (err) { console.log(`  ❌ ${src.name}: ${err.message}`); }
  }
  console.log(`\n📊 Added ${added} new sources`);
}

async function harvestSource(source) {
  console.log(`\n  📰 ${source.name}`);
  console.log(`     ${source.url.substring(0, 70)}...`);

  let items;
  try { items = await fetchAndParseRSS(source.url); }
  catch (err) {
    console.log(`     ❌ ${err.message}`);
    await pool.query(`UPDATE rss_sources SET error_count = COALESCE(error_count, 0) + 1, last_error = $1 WHERE id = $2`, [err.message, source.id]);
    return { success: 0, skipped: 0, signals: 0 };
  }
  console.log(`     Found ${items.length} items`);

  let success = 0, skipped = 0, signalsCreated = 0;

  for (const item of items) {
    if (!item.title) { skipped++; continue; }
    const fullText = `${item.title} ${item.description}`;
    if (!isEnglish(fullText)) { skipped++; continue; }

    const urlHash = md5(item.url || item.guid || item.title);
    const signals = detectSignals(fullText);
    const themes = detectThemes(fullText);
    const geos = detectGeographies(fullText);
    const companyHint = extractCompanyHint(fullText);
    const amount = extractAmount(fullText);
    const confidence = scoreConfidence(signals, themes, geos);

    try {
      const result = await pool.query(`
        INSERT INTO external_documents (source_id, title, content, url, source_url_hash, published_at, document_type, metadata, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'news_pr', $7, NOW())
        ON CONFLICT (source_url_hash, tenant_id) DO NOTHING RETURNING id
      `, [
        source.id, item.title.substring(0, 500), item.description.substring(0, 5000),
        (item.url || '').substring(0, 1000), urlHash,
        item.pubDate ? new Date(item.pubDate).toISOString() : null,
        JSON.stringify({ signals, themes, geographies: geos, company_hint: companyHint, amount_millions: amount, confidence }),
      ]);

      if (result.rowCount === 0) { skipped++; continue; }
      success++;
      const docId = result.rows[0].id;

      // Create signal_events for high-value hiring/capital signals
      if (signals.length > 0 && confidence >= 0.4) {
        for (const signalType of signals) {
          try {
            await pool.query(`
              INSERT INTO signal_events (company_name, signal_type, signal_category, title, evidence,
                source_url, source_type, confidence, detected_at, metadata)
              VALUES ($1, $2, $3, $4, $5, $6, 'news_pr', $7, NOW(), $8)
            `, [
              companyHint || 'Unknown', signalType,
              signalType.match(/hiring|departure|board/) ? 'talent' :
                signalType.match(/capital|ipo|pe_/) ? 'capital' :
                signalType.match(/ma_/) ? 'ma' : 'market',
              item.title.substring(0, 300), item.description.substring(0, 500),
              item.url, confidence,
              JSON.stringify({ themes, geographies: geos, amount_millions: amount, doc_id: docId }),
            ]);
            signalsCreated++;
          } catch (e) { /* dup or constraint */ }
        }
      }
    } catch (err) { skipped++; }
  }

  await pool.query(`UPDATE rss_sources SET last_fetched_at = NOW(), error_count = 0, last_error = NULL WHERE id = $1`, [source.id]);
  console.log(`     ✅ ${success} new, ${skipped} skipped, ${signalsCreated} signals`);
  return { success, skipped, signals: signalsCreated };
}

async function harvest(slugFilter) {
  let query = `SELECT id, name, url, category FROM rss_sources WHERE enabled = true`;
  const params = [];
  if (slugFilter) { query += ` AND name ILIKE $1`; params.push(`%${slugFilter}%`); }
  query += ` ORDER BY name`;

  const { rows: sources } = await pool.query(query, params);
  console.log('═'.repeat(60));
  console.log('  📰 MITCHELLAKE NEWS & PR HARVESTER');
  console.log('═'.repeat(60));
  console.log(`  Sources: ${sources.length} active\n`);

  let totalNew = 0, totalSkipped = 0, totalSignals = 0;
  for (const src of sources) {
    try {
      const r = await harvestSource(src);
      totalNew += r.success; totalSkipped += r.skipped; totalSignals += r.signals;
    } catch (err) { console.log(`     ❌ ${err.message}`); }
    await sleep(DELAY_MS);
  }
  console.log('\n' + '═'.repeat(60));
  console.log(`  DONE: ${totalNew} new docs, ${totalSignals} signals, ${totalSkipped} skipped`);
  console.log('═'.repeat(60));
}

async function showStats() {
  console.log('═'.repeat(60));
  console.log('  📊 NEWS HARVESTER — STATS');
  console.log('═'.repeat(60));
  const docs = await pool.query(`SELECT COUNT(*) as total FROM external_documents`);
  console.log(`\n  Total documents: ${docs.rows[0].total}`);
  const signals = await pool.query(`SELECT signal_type, COUNT(*) as cnt FROM signal_events WHERE source_type = 'news_pr' GROUP BY signal_type ORDER BY cnt DESC`);
  if (signals.rows.length > 0) {
    console.log('\n  Signals by type:');
    signals.rows.forEach(r => console.log(`    ${String(r.cnt).padStart(5)} — ${r.signal_type}`));
  }
  console.log('═'.repeat(60));
}

async function main() {
  const args = process.argv.slice(2);
  try {
    if (args.includes('--seed')) await seedSources();
    else if (args.includes('--stats')) await showStats();
    else {
      const i = args.indexOf('--source');
      await harvest(i >= 0 ? args[i + 1] : null);
    }
  } finally { await pool.end(); }
}

main().catch(err => { console.error('Fatal:', err); pool.end(); process.exit(1); });
