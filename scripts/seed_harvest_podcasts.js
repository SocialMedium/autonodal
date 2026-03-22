#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// seed_harvest_podcasts.js — Podcast RSS Seeder & Harvester
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/seed_harvest_podcasts.js --seed          Seed podcast sources
//   node scripts/seed_harvest_podcasts.js --seed --test   Seed + test feed accessibility
//   node scripts/seed_harvest_podcasts.js                 Harvest all enabled podcast sources
//   node scripts/seed_harvest_podcasts.js --source "20VC" Harvest matching source
//   node scripts/seed_harvest_podcasts.js --stats         Show podcast stats
//   node scripts/seed_harvest_podcasts.js --dry-run       Parse feeds without inserting
//
// Dependencies: dotenv, pg, xml2js
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { parseString } = require('xml2js');
const https = require('https');
const http = require('http');
const crypto = require('crypto');

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE
// ─────────────────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ─────────────────────────────────────────────────────────────────────────────
// PODCAST SOURCES (~30 feeds)
// ─────────────────────────────────────────────────────────────────────────────
// Focus: VC, startup, executive leadership, tech — signals for exec search
// Each episode often mentions funding rounds, exec appointments, company strategy

const PODCAST_SOURCES = [
  // ══════════ US / GLOBAL ══════════
  {
    name: '20VC - Twenty Minute VC',
    url: 'https://feeds.megaphone.fm/20minutevc',
    region: 'us',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'leadership_change', 'ma_activity'],
    poll_interval_minutes: 120,
  },
  // Equity TechCrunch removed — Megaphone feed redirects to our own site
  // Masters of Scale removed — only Art19 feed exists, broken redirects
  {
    name: 'Decoder with Nilay Patel',
    url: 'https://feeds.megaphone.fm/recodedecode',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['leadership_change', 'product_launch', 'ma_activity', 'restructuring'],
    poll_interval_minutes: 120,
  },
  {
    name: 'All-In Podcast',
    url: 'https://feeds.megaphone.fm/all-in-with-chamath-jason-sacks-and-friedberg',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'restructuring'],
    poll_interval_minutes: 120,
  },
  {
    name: 'This Week in Startups',
    url: 'https://feeds.megaphone.fm/thisweekinstartups',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  // Masters of Scale removed — Art19 feed redirects back to our own site

  {
    name: 'How I Built This',
    url: 'https://feeds.npr.org/510313/podcast.xml',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['leadership_change', 'capital_raising', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'a16z Podcast',
    url: 'https://feeds.simplecast.com/JGE3yC0V',
    region: 'us',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Invest Like the Best',
    url: 'https://feeds.megaphone.fm/investlikethebest',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Acquired',
    url: 'https://feeds.megaphone.fm/acquired',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['ma_activity', 'capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: "Lenny's Podcast",
    url: 'https://feeds.megaphone.fm/lennys-podcast',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['product_launch', 'leadership_change', 'strategic_hiring'],
    poll_interval_minutes: 180,
  },
  {
    name: 'The Knowledge Project',
    url: 'https://theknowledgeproject.libsyn.com/rss',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['leadership_change', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Venture Unlocked',
    url: 'https://feeds.simplecast.com/hnGaXQ2g',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change', 'partnership'],
    poll_interval_minutes: 180,
  },
  {
    name: 'The Full Ratchet',
    url: 'https://fullratchet.libsyn.com/rss',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 180,
  },
  // My First Million removed — feed not loading, redirects to own site
  {
    name: 'Village Global Venture Stories',
    url: 'https://feeds.megaphone.fm/village-global',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'StrictlyVC Download',
    url: 'https://feeds.megaphone.fm/strictlyvc',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 180,
  },

  // ══════════ ASIA / SEA ══════════
  {
    name: 'Impulso Podcast (SEA VC)',
    url: 'https://anchor.fm/s/3afcfcf8/podcast/rss',
    region: 'asia',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Asia Tech Podcast',
    url: 'https://feeds.buzzsprout.com/258898.rss',
    region: 'asia',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Analyse Asia',
    url: 'https://feeds.simplecast.com/K2vy7R0B',
    region: 'asia',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'geographic_expansion', 'ma_activity'],
    poll_interval_minutes: 180,
  },

  // ══════════ AUSTRALIA ══════════
  {
    name: 'Startup Grind AU',
    url: 'https://feeds.simplecast.com/dMT4hKVX',
    region: 'au',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'leadership_change', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Scale Investors Podcast',
    url: 'https://feeds.buzzsprout.com/1926839.rss',
    region: 'au',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'The Overnight Success (AU VC)',
    url: 'https://feeds.acast.com/public/shows/the-overnight-success',
    region: 'au',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },

  // ══════════ UK / EUROPE ══════════
  {
    name: 'Sifted Talks',
    url: 'https://feeds.acast.com/public/shows/sifted-talks',
    region: 'uk',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'geographic_expansion', 'ma_activity'],
    poll_interval_minutes: 180,
  },
  {
    name: 'The Twenty Minute VC Europe',
    url: 'https://feeds.megaphone.fm/20vceurope',
    region: 'eu',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
  },
  {
    name: 'EU-Startups Podcast',
    url: 'https://feeds.buzzsprout.com/1127040.rss',
    region: 'eu',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Seed to Scale (Accel)',
    url: 'https://feeds.simplecast.com/MjBMnJMR',
    region: 'eu',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change', 'product_launch'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Unicorn Bakery (DACH/EU)',
    url: 'https://feeds.redcircle.com/c04ab51a-3a0c-4e38-aef1-658a1eb6c606',
    region: 'eu',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'geographic_expansion'],
    poll_interval_minutes: 180,
  },
  {
    name: 'In Machines We Trust (MIT)',
    url: 'https://feeds.megaphone.fm/inmachineswetrust',
    region: 'us',
    credibility_score: 0.80,
    signal_types: ['product_launch', 'partnership'],
    poll_interval_minutes: 180,
  },
  {
    name: 'Prof G Markets',
    url: 'https://feeds.megaphone.fm/PPY4661100530',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'product_launch', 'leadership_change'],
    poll_interval_minutes: 120,
  },
  {
    name: 'Pivot with Kara Swisher and Scott Galloway',
    url: 'https://feeds.megaphone.fm/pivot',
    region: 'us',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change', 'restructuring', 'product_launch'],
    poll_interval_minutes: 120,
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// SIGNAL DETECTION (same patterns as harvest_news_pr)
// ─────────────────────────────────────────────────────────────────────────────

const SIGNAL_PATTERNS = {
  capital_raising: {
    keywords: [
      'raised', 'raises', 'funding', 'series a', 'series b', 'series c', 'series d',
      'seed round', 'pre-seed', 'venture capital', 'investment round',
      'ipo', 'initial public offering', 'goes public', 'debt financing',
      'funding round', 'led by', 'valuation', 'oversubscribed',
      'secures funding', 'capital raise', 'growth equity',
    ],
    phrases: [
      /raises?\s+\$[\d.,]+\s*(million|billion|m|b|mn|bn)/i,
      /\$[\d.,]+\s*(million|billion|m|b|mn|bn)\s+(series|seed|round|funding|raise)/i,
      /series\s+[a-f]\s+(round|funding|raise)/i,
      /secured?\s+\$[\d.,]+/i,
      /funding\s+round\s+(?:led|co-led)\s+by/i,
    ],
    weight: 0.9,
  },
  geographic_expansion: {
    keywords: [
      'expands to', 'expansion into', 'opens office', 'new market',
      'enters market', 'launches in', 'apac expansion',
      'european expansion', 'us expansion', 'global expansion',
    ],
    phrases: [
      /expands?\s+(to|into|in)\s+\w+/i,
      /opens?\s+(new\s+)?office\s+in/i,
      /launches?\s+in\s+\w+/i,
      /enters?\s+(the\s+)?\w+\s+market/i,
    ],
    weight: 0.7,
  },
  strategic_hiring: {
    keywords: [
      'hiring spree', 'plans to hire', 'recruiting', 'talent acquisition',
      'headcount', 'new positions', 'hiring push', 'workforce expansion',
    ],
    phrases: [
      /plans?\s+to\s+hire\s+[\d,]+/i,
      /hiring\s+[\d,]+\s+(new\s+)?(employees|people|engineers)/i,
    ],
    weight: 0.7,
  },
  ma_activity: {
    keywords: [
      'acquires', 'acquired', 'acquisition', 'merger', 'merges',
      'takeover', 'buyout', 'divestiture', 'strategic review',
    ],
    phrases: [
      /acquires?\s+\w+/i,
      /acquired\s+by\s+\w+/i,
      /merger\s+(with|between|of)/i,
      /\$[\d.,]+\s*(million|billion|m|b)\s+(acquisition|deal|buyout)/i,
    ],
    weight: 0.9,
  },
  partnership: {
    keywords: [
      'partners with', 'partnership', 'collaboration', 'alliance',
      'joint venture', 'teams up', 'integration with',
    ],
    phrases: [
      /partners?\s+with\s+\w+/i,
      /strategic\s+(partnership|alliance)\s+with/i,
      /joint\s+venture\s+(with|between)/i,
    ],
    weight: 0.6,
  },
  product_launch: {
    keywords: [
      'launches', 'launch', 'unveiled', 'unveils', 'introduces',
      'new product', 'new platform', 'general availability',
      'rolls out', 'debuts',
    ],
    phrases: [
      /launches?\s+(new\s+)?\w+\s+(platform|product|service|tool)/i,
      /announces?\s+(the\s+)?(launch|release|availability)/i,
    ],
    weight: 0.6,
  },
  leadership_change: {
    keywords: [
      'appoints', 'appointed', 'names', 'named', 'promotes',
      'steps down', 'resigns', 'resignation', 'departure',
      'new ceo', 'new cfo', 'new cto', 'new coo',
      'chief executive', 'board appointment',
    ],
    phrases: [
      /appoints?\s+\w+\s+\w+\s+as\s+(ceo|cfo|cto|coo|president|chairman|head|director)/i,
      /(ceo|cfo|cto|coo|president|chairman)\s+(steps?\s+down|resigns?|departs?)/i,
    ],
    weight: 0.85,
  },
  layoffs: {
    keywords: [
      'layoffs', 'laid off', 'lays off', 'job cuts', 'workforce reduction',
      'downsizing', 'redundancies',
    ],
    phrases: [
      /lays?\s+off\s+[\d,]+/i,
      /cuts?\s+[\d,]+\s+(jobs|positions|roles|employees)/i,
    ],
    weight: 0.85,
  },
  restructuring: {
    keywords: [
      'restructuring', 'reorganization', 'strategic review',
      'transformation', 'turnaround', 'pivot', 'bankruptcy',
    ],
    phrases: [
      /announces?\s+(a\s+)?(restructuring|reorganization|strategic\s+review)/i,
    ],
    weight: 0.8,
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// UTILITY FUNCTIONS
// ─────────────────────────────────────────────────────────────────────────────

function md5(str) { return crypto.createHash('md5').update(str).digest('hex'); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function truncate(str, maxLen = 500) { return !str ? '' : str.length > maxLen ? str.slice(0, maxLen) + '...' : str; }

function cleanHtml(html) {
  if (!html) return '';
  return html
    .replace(/<[^>]*>/g, ' ')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ').trim();
}

function extractText(item) {
  const candidates = [
    item?.['content:encoded']?.[0],
    item?.content?.[0]?._,
    item?.content?.[0],
    item?.description?.[0],
    item?.summary?.[0]?._,
    item?.summary?.[0],
    item?.['itunes:summary']?.[0],
  ];
  for (const c of candidates) {
    if (typeof c === 'string' && c.length > 20) return cleanHtml(c);
  }
  return '';
}

function extractTitle(item) {
  const t = item?.title?.[0];
  if (typeof t === 'string') return cleanHtml(t);
  if (typeof t === 'object' && t?._) return cleanHtml(t._);
  return 'Untitled';
}

function extractUrl(item) {
  if (item?.link?.[0] && typeof item.link[0] === 'string') return item.link[0].trim();
  if (item?.link?.[0]?.$ && item.link[0].$.href) return item.link[0].$.href.trim();
  // Podcast enclosure URL (actual audio file)
  if (item?.enclosure?.[0]?.$ && item.enclosure[0].$.url) return item.enclosure[0].$.url.trim();
  if (item?.guid?.[0]?._) return item.guid[0]._.trim();
  if (item?.guid?.[0] && typeof item.guid[0] === 'string') return item.guid[0].trim();
  if (item?.id?.[0]) return item.id[0].trim();
  return null;
}

function extractDate(item) {
  const candidates = [
    item?.pubDate?.[0], item?.published?.[0],
    item?.updated?.[0], item?.['dc:date']?.[0],
  ];
  for (const d of candidates) {
    if (d) { const parsed = new Date(d); if (!isNaN(parsed.getTime())) return parsed; }
  }
  return new Date();
}

function extractAuthor(item) {
  const candidates = [
    item?.['itunes:author']?.[0],
    item?.['dc:creator']?.[0],
    item?.author?.[0]?.name?.[0],
    item?.author?.[0],
  ];
  for (const a of candidates) {
    if (typeof a === 'string' && a.length > 0) return a.trim().slice(0, 255);
  }
  return null;
}

function extractDuration(item) {
  const d = item?.['itunes:duration']?.[0];
  if (!d) return null;
  // Could be "HH:MM:SS", "MM:SS", or seconds
  if (typeof d === 'string' && d.includes(':')) {
    const parts = d.split(':').map(Number);
    if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
    if (parts.length === 2) return parts[0] * 60 + parts[1];
  }
  return parseInt(d) || null;
}

function extractEpisodeLink(item) {
  // For podcasts, prefer the webpage link over the enclosure (audio file)
  if (item?.link?.[0] && typeof item.link[0] === 'string') return item.link[0].trim();
  if (item?.link?.[0]?.$ && item.link[0].$.href) return item.link[0].$.href.trim();
  if (item?.guid?.[0]?._) return item.guid[0]._.trim();
  if (item?.guid?.[0] && typeof item.guid[0] === 'string') {
    const g = item.guid[0].trim();
    if (g.startsWith('http')) return g;
  }
  // Fallback to enclosure
  if (item?.enclosure?.[0]?.$ && item.enclosure[0].$.url) return item.enclosure[0].$.url.trim();
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP FETCH
// ─────────────────────────────────────────────────────────────────────────────

function fetchUrl(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const options = {
      headers: {
        'User-Agent': 'MitchelLake-SignalBot/1.0 (+https://mitchellake.com)',
        'Accept': 'application/rss+xml, application/xml, application/atom+xml, text/xml, */*',
      },
      timeout: 20000,
    };
    const req = client.get(url, options, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        if (maxRedirects <= 0) { reject(new Error(`Too many redirects`)); return; }
        const redir = res.headers.location.startsWith('http')
          ? res.headers.location : new URL(res.headers.location, url).href;
        resolve(fetchUrl(redir, maxRedirects - 1));
        return;
      }
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// RSS PARSER
// ─────────────────────────────────────────────────────────────────────────────

function parseRss(xml) {
  return new Promise((resolve, reject) => {
    parseString(xml, { trim: true, normalize: true, normalizeTags: false, explicitArray: true },
      (err, result) => {
        if (err) { reject(new Error(`XML parse: ${err.message}`)); return; }
        let items = [];
        if (result?.rss?.channel?.[0]?.item) items = result.rss.channel[0].item;
        else if (result?.feed?.entry) items = result.feed.entry;
        else if (result?.['rdf:RDF']?.item) items = result['rdf:RDF'].item;
        else { reject(new Error(`Unknown feed format`)); return; }
        resolve(items);
      });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SIGNAL DETECTION
// ─────────────────────────────────────────────────────────────────────────────

function detectSignals(title, content) {
  const signals = [];
  const text = `${title} ${content}`.toLowerCase();

  for (const [signalType, config] of Object.entries(SIGNAL_PATTERNS)) {
    let score = 0;
    let matchedKeywords = [];
    let matchedPhrases = [];

    for (const kw of config.keywords) {
      if (text.includes(kw.toLowerCase())) { score += 0.15; matchedKeywords.push(kw); }
    }
    for (const pattern of config.phrases) {
      const match = text.match(pattern);
      if (match) { score += 0.3; matchedPhrases.push(match[0]); }
    }

    score = Math.min(score * config.weight, 1.0);
    const titleLower = title.toLowerCase();
    if (config.keywords.some(kw => titleLower.includes(kw.toLowerCase()))) score = Math.min(score + 0.15, 1.0);

    if (score >= 0.4) {
      signals.push({
        signal_type: signalType,
        confidence_score: parseFloat(score.toFixed(3)),
        evidence_keywords: matchedKeywords.slice(0, 10),
        evidence_phrases: matchedPhrases.slice(0, 5),
        evidence_snippet: truncate(matchedPhrases[0] || matchedKeywords.slice(0, 3).join(', '), 500),
      });
    }
  }
  return signals;
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPANY EXTRACTION & UPSERT
// ─────────────────────────────────────────────────────────────────────────────

const STOP_WORDS = new Set([
  'the', 'and', 'for', 'new', 'its', 'has', 'was', 'are', 'with', 'that',
  'this', 'from', 'will', 'have', 'been', 'they', 'more', 'also', 'said',
  'their', 'year', 'over', 'could', 'would', 'about', 'after', 'which',
  'report', 'global', 'market', 'world', 'today', 'says', 'episode',
  'podcast', 'listen', 'subscribe', 'show', 'notes', 'host',
]);

function extractCompanyNames(title, content) {
  const names = new Set();
  const text = `${title}. ${content}`;
  const patterns = [
    /([A-Z][A-Za-z0-9&.\- ]{2,30})\s+(?:raises?|raised|acquires?|acquired|launches?|appointed|announces?|partners?|expands?|hires?)/g,
    /(?:acquired\s+by|partnership\s+with|backed\s+by|led\s+by|funded\s+by)\s+([A-Z][A-Za-z0-9&.\- ]{2,30})/g,
  ];
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const name = match[1].trim().replace(/[.\-\s]+$/, '');
      if (name.length >= 3 && !STOP_WORDS.has(name.toLowerCase())) names.add(name);
    }
  }
  return [...names].slice(0, 5);
}

async function findOrCreateCompany(companyName) {
  if (!companyName || companyName.length < 2) return null;
  const existing = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [companyName]);
  if (existing.rows.length > 0) return existing.rows[0].id;
  try {
    const result = await pool.query(
      `INSERT INTO companies (name, created_at, updated_at) VALUES ($1, NOW(), NOW()) ON CONFLICT DO NOTHING RETURNING id`,
      [companyName]
    );
    if (result.rows.length > 0) return result.rows[0].id;
    const refetch = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [companyName]);
    return refetch.rows?.[0]?.id || null;
  } catch (err) { return null; }
}

// ─────────────────────────────────────────────────────────────────────────────
// SEED SOURCES
// ─────────────────────────────────────────────────────────────────────────────

async function seedSources(testFeeds = false) {
  const regionFilter = process.argv.find(a => ['--us', '--asia', '--au', '--uk', '--eu'].includes(a));
  let sources = PODCAST_SOURCES;
  if (regionFilter) {
    const region = regionFilter.replace('--', '');
    sources = sources.filter(s => s.region === region);
  }

  console.log(`\n📥 Seeding ${sources.length} podcast sources...\n`);

  let inserted = 0, skipped = 0, tested = 0, accessible = 0;

  for (const src of sources) {
    const exists = await pool.query(`SELECT id FROM rss_sources WHERE url = $1 LIMIT 1`, [src.url]);
    if (exists.rows.length > 0) {
      console.log(`  ⏭️  Already exists: ${src.name}`);
      skipped++;
    } else {
      await pool.query(
        `INSERT INTO rss_sources (name, source_type, url, poll_interval_minutes, enabled, credibility_score, signal_types, consecutive_errors, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 0, NOW())`,
        [src.name, 'podcast', src.url, src.poll_interval_minutes, true, src.credibility_score, src.signal_types]
      );
      console.log(`  ✅ Inserted: ${src.name} [${src.region}]`);
      inserted++;
    }

    if (testFeeds) {
      tested++;
      try {
        const xml = await fetchUrl(src.url);
        const items = await parseRss(xml);
        console.log(`     🟢 Accessible — ${items.length} episodes`);
        accessible++;
      } catch (err) {
        console.log(`     🔴 Error: ${err.message}`);
      }
      await sleep(500);
    }
  }

  console.log(`\n📊 Seed complete: ${inserted} inserted, ${skipped} skipped`);
  if (testFeeds) console.log(`   Feed test: ${accessible}/${tested} accessible`);
  console.log('');
}

// ─────────────────────────────────────────────────────────────────────────────
// HARVEST FEEDS
// ─────────────────────────────────────────────────────────────────────────────

async function harvestFeeds(sourceFilter = null, dryRun = false) {
  let query = `SELECT * FROM rss_sources WHERE enabled = true AND source_type = 'podcast'`;
  const params = [];
  if (sourceFilter) { query += ` AND LOWER(name) LIKE $1`; params.push(`%${sourceFilter.toLowerCase()}%`); }
  query += ` ORDER BY last_fetched_at ASC NULLS FIRST`;

  const { rows: sources } = await pool.query(query, params);
  if (sources.length === 0) { console.log('\n⚠️  No enabled podcast sources. Run --seed first.\n'); return; }

  console.log(`\n📡 Harvesting ${sources.length} podcast source(s)...${dryRun ? ' [DRY RUN]' : ''}\n`);

  let totalDocs = 0, totalSignals = 0, totalErrors = 0;

  for (const source of sources) {
    console.log(`\n─── ${source.name} ───`);

    try {
      const xml = await fetchUrl(source.url);
      const items = await parseRss(xml);
      console.log(`  📄 ${items.length} episodes in feed`);

      let sourceDocs = 0, sourceSignals = 0;

      for (const item of items) {
        const title = extractTitle(item);
        const url = extractEpisodeLink(item);
        const content = extractText(item);
        const publishedAt = extractDate(item);
        const author = extractAuthor(item);
        const duration = extractDuration(item);

        if (!url) continue;
        const sourceUrlHash = md5(url);

        if (dryRun) {
          const signals = detectSignals(title, content);
          if (signals.length > 0) console.log(`  📝 [DRY] "${truncate(title, 60)}" → ${signals.length} signal(s)`);
          sourceDocs++; sourceSignals += signals.length;
          continue;
        }

        // Insert document
        const docResult = await pool.query(
          `INSERT INTO external_documents
             (source_type, source_name, source_url, source_url_hash, title, content,
              author, published_at, fetched_at, processing_status, source_id, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), 'pending', $9, NOW())
           ON CONFLICT (source_url_hash) DO NOTHING
           RETURNING id`,
          [source.source_type || 'podcast', source.name, url, sourceUrlHash,
           title.slice(0, 255), content, author, publishedAt, source.id]
        );

        if (docResult.rows.length === 0) continue;
        const docId = docResult.rows[0].id;
        sourceDocs++;

        // Signal detection
        const signals = detectSignals(title, content);
        for (const sig of signals) {
          const companyNames = extractCompanyNames(title, content);
          let companyId = null, companyName = null;
          if (companyNames.length > 0) {
            companyName = companyNames[0];
            companyId = await findOrCreateCompany(companyName);
          }
          try {
            await pool.query(
              `INSERT INTO signal_events
                 (signal_type, company_id, company_name, confidence_score,
                  evidence_summary, evidence_snippet, evidence_snippets,
                  evidence_doc_ids, source_document_id, source_url,
                  triage_status, signal_category, detected_at, signal_date,
                  scoring_breakdown, hiring_implications, created_at, updated_at)
               VALUES
                 ($1::signal_type, $2, $3, $4, $5, $6, $7,
                  $8, $9, $10, 'new', $11, NOW(), $12,
                  $13, $14, NOW(), NOW())`,
              [
                sig.signal_type, companyId, companyName, sig.confidence_score,
                `${sig.signal_type}: ${truncate(title, 200)}`, sig.evidence_snippet,
                JSON.stringify(sig.evidence_phrases), [docId], docId, url,
                sig.signal_type.split('_')[0], publishedAt,
                JSON.stringify({ keywords: sig.evidence_keywords, phrases: sig.evidence_phrases }),
                JSON.stringify({ signal_type: sig.signal_type, company: companyName, source: source.name }),
              ]
            );
            sourceSignals++;
          } catch (sigErr) {
            console.warn(`  ⚠️  Signal error: ${sigErr.message}`);
          }
        }

        if (signals.length > 0) {
          await pool.query(
            `UPDATE external_documents SET signals_computed_at = NOW(), processing_status = 'processed' WHERE id = $1`, [docId]
          );
        }
      }

      // Update source
      await pool.query(`UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = 0, last_error = NULL WHERE id = $1`, [source.id]);
      console.log(`  ✅ ${sourceDocs} new episode(s), ${sourceSignals} signal(s)`);
      totalDocs += sourceDocs; totalSignals += sourceSignals;

    } catch (err) {
      totalErrors++;
      console.error(`  ❌ ${err.message}`);
      await pool.query(
        `UPDATE rss_sources SET consecutive_errors = COALESCE(consecutive_errors, 0) + 1, last_error = $2, last_fetched_at = NOW() WHERE id = $1`,
        [source.id, err.message.slice(0, 500)]
      );
    }

    await sleep(1500 + Math.random() * 500);
  }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`📊 PODCAST HARVEST COMPLETE${dryRun ? ' [DRY RUN]' : ''}`);
  console.log(`   Sources:   ${sources.length}`);
  console.log(`   Episodes:  ${totalDocs} new`);
  console.log(`   Signals:   ${totalSignals}`);
  console.log(`   Errors:    ${totalErrors}`);
  console.log('═══════════════════════════════════════════════════\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS
// ─────────────────────────────────────────────────────────────────────────────

async function showStats() {
  console.log('\n📊 Podcast Harvester Statistics\n');

  const src = await pool.query(`
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE enabled = true) AS enabled,
           COUNT(*) FILTER (WHERE last_fetched_at IS NOT NULL) AS fetched,
           COUNT(*) FILTER (WHERE consecutive_errors > 0) AS with_errors
    FROM rss_sources WHERE source_type = 'podcast'
  `);
  const s = src.rows[0];
  console.log(`Sources: ${s.total} total | ${s.enabled} enabled | ${s.fetched} fetched | ${s.with_errors} with errors`);

  const docs = await pool.query(`
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE processing_status = 'processed') AS processed,
           COUNT(*) FILTER (WHERE embedded_at IS NOT NULL) AS embedded,
           MIN(published_at) AS oldest, MAX(published_at) AS newest
    FROM external_documents WHERE source_type = 'podcast'
  `);
  const d = docs.rows[0];
  console.log(`\nEpisodes: ${d.total} total | ${d.processed} processed | ${d.embedded} embedded`);
  if (d.oldest) console.log(`  Range: ${new Date(d.oldest).toISOString().slice(0, 10)} → ${new Date(d.newest).toISOString().slice(0, 10)}`);

  const sigs = await pool.query(`
    SELECT signal_type, COUNT(*) AS cnt, ROUND(AVG(confidence_score), 2) AS avg_conf
    FROM signal_events
    WHERE source_document_id IN (SELECT id FROM external_documents WHERE source_type = 'podcast')
    GROUP BY signal_type ORDER BY cnt DESC
  `);
  console.log('\nSignals from podcasts:');
  if (sigs.rows.length === 0) console.log('  (none yet)');
  else sigs.rows.forEach(r => console.log(`  ${r.signal_type.padEnd(25)} ${String(r.cnt).padStart(5)}  (avg: ${r.avg_conf})`));

  const health = await pool.query(`
    SELECT name, last_fetched_at, consecutive_errors, last_error
    FROM rss_sources WHERE source_type = 'podcast' AND enabled = true
    ORDER BY consecutive_errors DESC, last_fetched_at ASC
  `);
  console.log('\nSource health:');
  for (const r of health.rows) {
    const fetched = r.last_fetched_at ? new Date(r.last_fetched_at).toISOString().slice(0, 16) : 'never';
    const status = r.consecutive_errors > 0 ? `❌ ${r.consecutive_errors}err` : '✅';
    console.log(`  ${status} ${r.name.padEnd(35)} last: ${fetched}`);
    if (r.last_error) console.log(`       └─ ${truncate(r.last_error, 80)}`);
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
  console.log('  Podcast RSS Harvester');
  console.log('═══════════════════════════════════════════════════');

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');

    if (args.includes('--seed')) {
      await seedSources(args.includes('--test'));
    } else if (args.includes('--stats')) {
      await showStats();
    } else {
      const srcIdx = args.indexOf('--source');
      const sourceFilter = srcIdx >= 0 ? args[srcIdx + 1] : null;
      await harvestFeeds(sourceFilter, args.includes('--dry-run'));
    }
  } catch (err) {
    console.error('\n❌ Fatal:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();