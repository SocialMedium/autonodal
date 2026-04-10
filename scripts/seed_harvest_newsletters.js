#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// seed_harvest_newsletters.js — Newsletter RSS Seeder & Harvester
// ═══════════════════════════════════════════════════════════════════════════════
//
// Most newsletters (especially Substack) have public RSS feeds.
// This seeds them as source_type='newsletter' and harvests via RSS.
//
// Usage:
//   node scripts/seed_harvest_newsletters.js --seed          Seed sources
//   node scripts/seed_harvest_newsletters.js --seed --test   Seed + test accessibility
//   node scripts/seed_harvest_newsletters.js                 Harvest all
//   node scripts/seed_harvest_newsletters.js --source "prof" Filter by name
//   node scripts/seed_harvest_newsletters.js --stats         Stats dashboard
//   node scripts/seed_harvest_newsletters.js --dry-run       Parse without inserting
//
// Dependencies: dotenv, pg, xml2js
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { parseString } = require('xml2js');
const https = require('https');
const http = require('http');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ─────────────────────────────────────────────────────────────────────────────
// NEWSLETTER SOURCES (~50 feeds)
// ─────────────────────────────────────────────────────────────────────────────
//
// Categories:
//   vc       — Venture Capital / Startup investing
//   tech     — Technology industry analysis
//   markets  — Markets, finance, macro
//   exec     — Executive / leadership / management
//   regional — Region-specific (AU, SEA, UK/EU)
//
// Most Substack newsletters: https://SLUG.substack.com/feed
// Custom domains still serve Substack RSS at /feed

const NEWSLETTER_SOURCES = [

  // ══════════ VC / STARTUP INVESTING ══════════

  {
    name: 'Newcomer (Eric Newcomer)',
    url: 'https://www.newcomer.co/feed',
    category: 'vc',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 120,
    description: 'Deep-dive VC reporting. Funding rounds, firm dynamics, startup inside stories.',
  },
  {
    name: 'Not Boring (Packy McCormick)',
    url: 'https://www.notboring.co/feed',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 180,
    description: 'Optimistic deep dives on ambitious startups, strategy, and tech trends.',
  },
  {
    name: 'StrictlyVC (Connie Loizos)',
    url: 'https://www.strictlyvc.com/feed',
    category: 'vc',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 120,
    description: 'Daily VC deal flow and startup funding news.',
  },
  {
    name: 'The Diff (Byrne Hobart)',
    url: 'https://www.thediff.co/feed',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'restructuring'],
    poll_interval_minutes: 180,
    description: 'Finance, tech, and inflection points. Deep analytical essays.',
  },
  {
    name: 'Digital Native (Rex Woodbury)',
    url: 'https://digitalnative.substack.com/feed',
    category: 'vc',
    credibility_score: 0.80,
    signal_types: ['product_launch', 'capital_raising'],
    poll_interval_minutes: 180,
    description: 'How people and technology intersect. Consumer tech trends.',
  },
  {
    name: 'The VC Corner (Ruben D.)',
    url: 'https://thevccorner.substack.com/feed',
    category: 'vc',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'Weekly startup and VC roundup with curated deals and insights.',
  },
  {
    name: 'Venture Unlocked Newsletter',
    url: 'https://ventureunlocked.substack.com/feed',
    category: 'vc',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'Interviews and insights from top VCs and LPs.',
  },
  {
    name: 'Axios Pro Rata (Dan Primack)',
    url: 'https://www.axios.com/pro/deals/feed',
    category: 'vc',
    credibility_score: 0.90,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 120,
    description: 'Daily dealmaking news. Funding, M&A, PE, VC.',
  },
  {
    name: 'Term Sheet (Fortune)',
    url: 'https://fortune.com/tag/term-sheet/feed/',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 120,
    description: 'Fortune\'s daily VC/PE deal sheet.',
  },
  {
    name: 'Sourcery (Molly O\'Shea)',
    url: 'https://sourcery.substack.com/feed',
    category: 'vc',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'VC deals and trends. Tuesday roundups + Friday interviews.',
  },
  {
    name: 'Upstart (Alex Konrad)',
    url: 'https://upstart.substack.com/feed',
    category: 'vc',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'leadership_change', 'product_launch'],
    poll_interval_minutes: 180,
    description: 'Ex-Forbes journalist covering emerging startups and rising trends.',
  },

  // ══════════ SAAS / FINTECH / CFO ══════════

  {
    name: 'OnlyCFO',
    url: 'https://www.onlycfo.io/feed',
    category: 'tech',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 180,
    description: 'SaaS metrics, go-to-market strategy, capital markets for CFOs.',
  },
  {
    name: 'Clouded Judgement (Jamin Ball)',
    url: 'https://cloudedjudgement.substack.com/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 180,
    description: 'Weekly SaaS public market analysis with metrics tables.',
  },
  {
    name: 'SaaStr',
    url: 'https://www.saastr.com/feed/',
    category: 'tech',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'strategic_hiring', 'product_launch'],
    poll_interval_minutes: 180,
    description: 'SaaS community, scaling advice, and industry analysis.',
  },
  {
    name: 'Meritech Capital SaaS Index',
    url: 'https://www.meritechcapital.com/blog/feed',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 240,
    description: 'Growth equity SaaS market data and IPO analysis.',
  },

  // ══════════ TECH INDUSTRY ANALYSIS ══════════

  {
    name: 'Platformer (Casey Newton)',
    url: 'https://www.platformer.news/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['leadership_change', 'restructuring', 'product_launch'],
    poll_interval_minutes: 120,
    description: 'Big tech platform industry coverage. Meta, X, Google, Apple.',
  },
  {
    name: 'Benedict Evans',
    url: 'https://www.ben-evans.com/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['product_launch', 'ma_activity', 'geographic_expansion'],
    poll_interval_minutes: 240,
    description: 'Tech macro analysis, mobile, AI, big picture trends.',
  },
  {
    name: 'Pragmatic Engineer (Gergely Orosz)',
    url: 'https://newsletter.pragmaticengineer.com/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['strategic_hiring', 'layoffs', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'Engineering management, big tech hiring, compensation, culture.',
  },
  {
    name: 'Big Technology (Alex Kantrowitz)',
    url: 'https://bigtechnology.substack.com/feed',
    category: 'tech',
    credibility_score: 0.80,
    signal_types: ['leadership_change', 'product_launch', 'ma_activity'],
    poll_interval_minutes: 180,
    description: 'Critical analysis of Big Tech companies.',
  },
  {
    name: 'Elad Gil Newsletter',
    url: 'https://blog.eladgil.com/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'leadership_change', 'product_launch'],
    poll_interval_minutes: 240,
    description: 'Startup scaling, high-growth company advice from serial founder/investor.',
  },

  // ══════════ AI / FRONTIER TECH ══════════

  {
    name: 'Import AI (Jack Clark)',
    url: 'https://importai.substack.com/feed',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['product_launch', 'capital_raising', 'partnership'],
    poll_interval_minutes: 180,
    description: 'AI research and industry analysis from Anthropic co-founder.',
  },
  {
    name: 'The Batch (Andrew Ng / DeepLearning.AI)',
    url: 'https://www.deeplearning.ai/the-batch/feed/',
    category: 'tech',
    credibility_score: 0.85,
    signal_types: ['product_launch', 'capital_raising', 'partnership'],
    poll_interval_minutes: 180,
    description: 'Weekly AI news roundup from Andrew Ng.',
  },
  {
    name: 'AI Supremacy',
    url: 'https://aisupremacy.substack.com/feed',
    category: 'tech',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'ma_activity'],
    poll_interval_minutes: 180,
    description: 'AI industry trends, funding, and company analysis.',
  },
  {
    name: 'The Neuron (AI Daily)',
    url: 'https://www.theneurondaily.com/feed',
    category: 'tech',
    credibility_score: 0.75,
    signal_types: ['product_launch', 'capital_raising'],
    poll_interval_minutes: 120,
    description: 'Daily AI news digest.',
  },

  // ══════════ MARKETS / BUSINESS / MACRO ══════════

  {
    name: 'Prof G (Scott Galloway)',
    url: 'https://www.profgmedia.com/feed',
    category: 'markets',
    credibility_score: 0.85,
    signal_types: ['ma_activity', 'leadership_change', 'restructuring'],
    poll_interval_minutes: 180,
    description: 'No Mercy, No Malice. Business strategy, Big Tech, markets.',
  },
  {
    name: 'Kyla Scanlon',
    url: 'https://kylascanlon.substack.com/feed',
    category: 'markets',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'restructuring'],
    poll_interval_minutes: 180,
    description: 'Markets, economy, and finance explained accessibly.',
  },
  {
    name: 'Net Interest (Marc Rubinstein)',
    url: 'https://www.netinterest.co/feed',
    category: 'markets',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'restructuring'],
    poll_interval_minutes: 240,
    description: 'Financial services industry deep dives.',
  },
  {
    name: 'The Generalist (Mario Gabriele)',
    url: 'https://www.readthegeneralist.com/feed',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'product_launch', 'ma_activity'],
    poll_interval_minutes: 240,
    description: 'Meticulously researched company and industry deep dives.',
  },
  {
    name: 'Crunchbase News',
    url: 'https://news.crunchbase.com/feed/',
    category: 'vc',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'ma_activity', 'layoffs'],
    poll_interval_minutes: 120,
    description: 'Startup funding data, trends, and analysis.',
  },

  // ══════════ EXECUTIVE / LEADERSHIP ══════════

  {
    name: 'Lenny\'s Newsletter',
    url: 'https://www.lennysnewsletter.com/feed',
    category: 'exec',
    credibility_score: 0.85,
    signal_types: ['strategic_hiring', 'product_launch'],
    poll_interval_minutes: 180,
    description: 'Product management, growth, leadership for tech executives.',
  },
  {
    name: 'First Round Review',
    url: 'https://review.firstround.com/feed/',
    category: 'exec',
    credibility_score: 0.85,
    signal_types: ['strategic_hiring', 'leadership_change', 'product_launch'],
    poll_interval_minutes: 240,
    description: 'In-depth startup advice from First Round portfolio and network.',
  },
  {
    name: 'Harvard Business Review',
    url: 'https://hbr.org/feed',
    category: 'exec',
    credibility_score: 0.85,
    signal_types: ['leadership_change', 'restructuring'],
    poll_interval_minutes: 240,
    description: 'Management, leadership, strategy research and practice.',
  },

  // ══════════ AUSTRALIA / APAC ══════════

  {
    name: 'Overnight Success (AU VC)',
    url: 'https://overnightsuccess.substack.com/feed',
    category: 'regional',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'Australian startup and VC ecosystem coverage.',
  },
  {
    name: 'Aussie FinTech',
    url: 'https://aussiefintech.substack.com/feed',
    category: 'regional',
    credibility_score: 0.70,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 240,
    description: 'Australian fintech news and analysis.',
  },
  {
    name: 'The Aussie Tech Roundup',
    url: 'https://aussietechroundup.substack.com/feed',
    category: 'regional',
    credibility_score: 0.70,
    signal_types: ['capital_raising', 'product_launch'],
    poll_interval_minutes: 240,
    description: 'Weekly Australian tech industry roundup.',
  },
  {
    name: 'Tech Buzz China (Rui Ma)',
    url: 'https://techbuzzchina.substack.com/feed',
    category: 'regional',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'ma_activity'],
    poll_interval_minutes: 240,
    description: 'China tech ecosystem analysis for global audience.',
  },
  {
    name: 'The Ken (India/SEA)',
    url: 'https://the-ken.com/feed/',
    category: 'regional',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'ma_activity', 'leadership_change'],
    poll_interval_minutes: 180,
    description: 'India and Southeast Asia tech business journalism.',
  },

  // ══════════ UK / EUROPE ══════════

  {
    name: 'Sifted Newsletter (EU)',
    url: 'https://sifted.eu/feed',
    category: 'regional',
    credibility_score: 0.85,
    signal_types: ['capital_raising', 'geographic_expansion', 'leadership_change'],
    poll_interval_minutes: 120,
    description: 'European startup and VC coverage from FT-backed outlet.',
  },
  {
    name: 'Tech.eu',
    url: 'https://tech.eu/feed/',
    category: 'regional',
    credibility_score: 0.80,
    signal_types: ['capital_raising', 'product_launch', 'geographic_expansion'],
    poll_interval_minutes: 180,
    description: 'European technology news and funding.',
  },
  {
    name: '0100 Conferences VC Brief',
    url: 'https://0100conferences.substack.com/feed',
    category: 'regional',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'ma_activity'],
    poll_interval_minutes: 240,
    description: 'European VC weekly brief with data-driven analysis.',
  },

  // ══════════ RECRUITING / TALENT ══════════

  {
    name: 'Recruiting Brainfood',
    url: 'https://brainfood.substack.com/feed',
    category: 'exec',
    credibility_score: 0.75,
    signal_types: ['strategic_hiring', 'leadership_change'],
    poll_interval_minutes: 240,
    description: 'Weekly recruiting industry curation. Talent trends and tools.',
  },
  {
    name: 'Hung Lee - Recruiting Brainfood',
    url: 'https://www.intrepidrecruitment.com/feed',
    category: 'exec',
    credibility_score: 0.70,
    signal_types: ['strategic_hiring'],
    poll_interval_minutes: 240,
    description: 'Recruitment industry news and trends.',
  },

  // ══════════ CRYPTO / WEB3 (select) ══════════

  {
    name: 'Bankless',
    url: 'https://www.bankless.com/feed',
    category: 'tech',
    credibility_score: 0.75,
    signal_types: ['capital_raising', 'product_launch', 'partnership'],
    poll_interval_minutes: 240,
    description: 'Crypto/web3 industry with strong crossover to VC/tech.',
  },

  // ══════════ PE / M&A ══════════

  {
    name: 'Mergers & Inquisitions',
    url: 'https://www.mergersandinquisitions.com/feed/',
    category: 'markets',
    credibility_score: 0.75,
    signal_types: ['ma_activity', 'leadership_change'],
    poll_interval_minutes: 240,
    description: 'Investment banking, PE, M&A career and deal analysis.',
  },
  {
    name: 'PE Insights',
    url: 'https://pe-insights.com/feed/',
    category: 'markets',
    credibility_score: 0.80,
    signal_types: ['ma_activity', 'capital_raising', 'leadership_change'],
    poll_interval_minutes: 240,
    description: 'European PE/VC deals and insights.',
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// SIGNAL DETECTION (same as other harvesters)
// ─────────────────────────────────────────────────────────────────────────────

const SIGNAL_PATTERNS = {
  capital_raising: {
    keywords: ['raised', 'raises', 'funding', 'series a', 'series b', 'series c', 'series d',
      'seed round', 'pre-seed', 'venture capital', 'investment round', 'ipo',
      'funding round', 'led by', 'valuation', 'oversubscribed', 'secures funding',
      'capital raise', 'growth equity', 'goes public'],
    phrases: [
      /raises?\s+\$[\d.,]+\s*(million|billion|m|b|mn|bn)/i,
      /\$[\d.,]+\s*(million|billion|m|b|mn|bn)\s+(series|seed|round|funding|raise)/i,
      /series\s+[a-f]\s+(round|funding|raise)/i,
      /secured?\s+\$[\d.,]+/i,
    ],
    weight: 0.9,
  },
  geographic_expansion: {
    keywords: ['expands to', 'expansion into', 'opens office', 'new market', 'enters market',
      'launches in', 'global expansion'],
    phrases: [/expands?\s+(to|into|in)\s+\w+/i, /opens?\s+(new\s+)?office\s+in/i],
    weight: 0.7,
  },
  strategic_hiring: {
    keywords: ['hiring spree', 'plans to hire', 'recruiting', 'headcount', 'new positions',
      'hiring push', 'workforce expansion'],
    phrases: [/plans?\s+to\s+hire\s+[\d,]+/i, /hiring\s+[\d,]+\s+(new\s+)?(employees|people|engineers)/i],
    weight: 0.7,
  },
  ma_activity: {
    keywords: ['acquires', 'acquired', 'acquisition', 'merger', 'merges', 'takeover',
      'buyout', 'divestiture', 'strategic review'],
    phrases: [/acquires?\s+\w+/i, /acquired\s+by\s+\w+/i, /merger\s+(with|between|of)/i],
    weight: 0.9,
  },
  partnership: {
    keywords: ['partners with', 'partnership', 'collaboration', 'alliance', 'joint venture',
      'teams up', 'integration with'],
    phrases: [/partners?\s+with\s+\w+/i, /strategic\s+(partnership|alliance)\s+with/i],
    weight: 0.6,
  },
  product_launch: {
    keywords: ['launches', 'launch', 'unveiled', 'unveils', 'introduces', 'new product',
      'new platform', 'general availability', 'rolls out', 'debuts'],
    phrases: [/launches?\s+(new\s+)?\w+\s+(platform|product|service|tool)/i],
    weight: 0.6,
  },
  leadership_change: {
    keywords: ['appoints', 'appointed', 'names', 'named', 'promotes', 'steps down',
      'resigns', 'resignation', 'departure', 'new ceo', 'new cfo', 'new cto',
      'board appointment'],
    phrases: [
      /appoints?\s+\w+\s+\w+\s+as\s+(ceo|cfo|cto|coo|president|chairman|head|director)/i,
      /(ceo|cfo|cto|coo|president|chairman)\s+(steps?\s+down|resigns?|departs?)/i,
    ],
    weight: 0.85,
  },
  layoffs: {
    keywords: ['layoffs', 'laid off', 'lays off', 'job cuts', 'workforce reduction',
      'downsizing', 'redundancies'],
    phrases: [/lays?\s+off\s+[\d,]+/i, /cuts?\s+[\d,]+\s+(jobs|positions|roles)/i],
    weight: 0.85,
  },
  restructuring: {
    keywords: ['restructuring', 'reorganization', 'strategic review', 'transformation',
      'turnaround', 'pivot', 'bankruptcy'],
    phrases: [/announces?\s+(a\s+)?(restructuring|reorganization|strategic\s+review)/i],
    weight: 0.8,
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────────────────────────────────────

function md5(str) { return crypto.createHash('md5').update(str).digest('hex'); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function truncate(str, maxLen = 500) { return !str ? '' : str.length > maxLen ? str.slice(0, maxLen) + '...' : str; }

function cleanHtml(html) {
  if (!html) return '';
  return html.replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'").replace(/\s+/g, ' ').trim();
}

function extractText(item) {
  const candidates = [item?.['content:encoded']?.[0], item?.content?.[0]?._, item?.content?.[0],
    item?.description?.[0], item?.summary?.[0]?._, item?.summary?.[0]];
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
  if (item?.guid?.[0]?._) return item.guid[0]._.trim();
  if (item?.guid?.[0] && typeof item.guid[0] === 'string') return item.guid[0].trim();
  if (item?.id?.[0]) return item.id[0].trim();
  return null;
}

function extractDate(item) {
  const candidates = [item?.pubDate?.[0], item?.published?.[0], item?.updated?.[0], item?.['dc:date']?.[0]];
  for (const d of candidates) {
    if (d) { const parsed = new Date(d); if (!isNaN(parsed.getTime())) return parsed; }
  }
  return new Date();
}

function extractAuthor(item) {
  const candidates = [item?.['dc:creator']?.[0], item?.author?.[0]?.name?.[0], item?.author?.[0]];
  for (const a of candidates) {
    if (typeof a === 'string' && a.length > 0) return a.trim().slice(0, 255);
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP FETCH
// ─────────────────────────────────────────────────────────────────────────────

function fetchUrl(url, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, {
      headers: {
        'User-Agent': 'MitchelLake-SignalBot/1.0 (+https://mitchellake.com)',
        'Accept': 'application/rss+xml, application/xml, application/atom+xml, text/xml, */*',
      },
      timeout: 20000,
    }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        if (maxRedirects <= 0) { reject(new Error('Too many redirects')); return; }
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
        else { reject(new Error('Unrecognized feed format')); return; }
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
    let matchedKeywords = [], matchedPhrases = [];

    for (const kw of config.keywords) {
      if (text.includes(kw.toLowerCase())) { score += 0.15; matchedKeywords.push(kw); }
    }
    for (const pattern of config.phrases) {
      const match = text.match(pattern);
      if (match) { score += 0.3; matchedPhrases.push(match[0]); }
    }

    score = Math.min(score * config.weight, 1.0);
    if (config.keywords.some(kw => title.toLowerCase().includes(kw.toLowerCase()))) score = Math.min(score + 0.15, 1.0);

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
// COMPANY EXTRACTION
// ─────────────────────────────────────────────────────────────────────────────

const STOP_WORDS = new Set(['the', 'and', 'for', 'new', 'its', 'has', 'was', 'are', 'with', 'that',
  'this', 'from', 'will', 'have', 'been', 'they', 'more', 'also', 'said', 'their',
  'report', 'global', 'market', 'world', 'today', 'says', 'newsletter', 'subscribe']);

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

async function findOrCreateCompany(name) {
  if (!name || name.length < 2) return null;
  const existing = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);
  if (existing.rows.length > 0) return existing.rows[0].id;
  try {
    const result = await pool.query(
      `INSERT INTO companies (name, created_at, updated_at) VALUES ($1, NOW(), NOW()) ON CONFLICT DO NOTHING RETURNING id`, [name]);
    if (result.rows.length > 0) return result.rows[0].id;
    const refetch = await pool.query(`SELECT id FROM companies WHERE LOWER(name) = LOWER($1) LIMIT 1`, [name]);
    return refetch.rows?.[0]?.id || null;
  } catch { return null; }
}

// ─────────────────────────────────────────────────────────────────────────────
// ENSURE source_type ENUM includes 'newsletter'
// ─────────────────────────────────────────────────────────────────────────────

async function ensureSourceType() {
  try {
    // Check if 'newsletter' exists in the source_type enum or column
    const check = await pool.query(
      `SELECT COUNT(*) FROM rss_sources WHERE source_type = 'newsletter' LIMIT 1`
    );
    // If the query doesn't error, the value is accepted
    return true;
  } catch (err) {
    if (err.message.includes('invalid input value for enum')) {
      console.log('  🔧 Adding "newsletter" to source_type enum...');
      try {
        await pool.query(`ALTER TYPE source_type ADD VALUE IF NOT EXISTS 'newsletter'`);
        console.log('  ✅ Added');
        return true;
      } catch (e2) {
        console.error('  ❌ Could not add newsletter enum:', e2.message);
        console.log('  Try: ALTER TYPE source_type ADD VALUE \'newsletter\';');
        return false;
      }
    }
    // It might be a text column, which accepts anything
    return true;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SEED SOURCES
// ─────────────────────────────────────────────────────────────────────────────

async function seedSources(testFeeds = false) {
  const catFilter = process.argv.find(a => ['--vc', '--tech', '--markets', '--exec', '--regional'].includes(a));
  let sources = NEWSLETTER_SOURCES;
  if (catFilter) sources = sources.filter(s => s.category === catFilter.replace('--', ''));

  console.log(`\n📥 Seeding ${sources.length} newsletter sources...\n`);
  let inserted = 0, skipped = 0, accessible = 0, tested = 0;

  for (const src of sources) {
    const exists = await pool.query(`SELECT id FROM rss_sources WHERE url = $1 LIMIT 1`, [src.url]);
    if (exists.rows.length > 0) {
      console.log(`  ⏭️  Already exists: ${src.name}`);
      skipped++;
    } else {
      try {
        await pool.query(
          `INSERT INTO rss_sources (name, source_type, url, poll_interval_minutes, enabled, credibility_score, signal_types, consecutive_errors, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 0, NOW())`,
          [src.name, 'newsletter', src.url, src.poll_interval_minutes, true, src.credibility_score, src.signal_types]
        );
        console.log(`  ✅ Inserted: ${src.name} [${src.category}]`);
        inserted++;
      } catch (err) {
        console.log(`  ❌ Insert failed: ${src.name} — ${err.message}`);
      }
    }

    if (testFeeds) {
      tested++;
      try {
        const xml = await fetchUrl(src.url);
        const items = await parseRss(xml);
        console.log(`     🟢 Accessible — ${items.length} items`);
        accessible++;
      } catch (err) {
        console.log(`     🔴 Error: ${err.message}`);
      }
      await sleep(400);
    }
  }

  console.log(`\n📊 Seed complete: ${inserted} inserted, ${skipped} skipped`);
  if (testFeeds) console.log(`   Feed test: ${accessible}/${tested} accessible`);
  console.log('');
}

// ─────────────────────────────────────────────────────────────────────────────
// HARVEST
// ─────────────────────────────────────────────────────────────────────────────

async function harvestFeeds(sourceFilter = null, dryRun = false) {
  let query = `SELECT * FROM rss_sources WHERE enabled = true AND source_type = 'newsletter'`;
  const params = [];
  if (sourceFilter) { query += ` AND LOWER(name) LIKE $1`; params.push(`%${sourceFilter.toLowerCase()}%`); }
  query += ` ORDER BY last_fetched_at ASC NULLS FIRST`;

  const { rows: sources } = await pool.query(query, params);
  if (sources.length === 0) { console.log('\n⚠️  No enabled newsletter sources. Run --seed first.\n'); return; }

  console.log(`\n📡 Harvesting ${sources.length} newsletter source(s)...${dryRun ? ' [DRY RUN]' : ''}\n`);
  let totalDocs = 0, totalSignals = 0, totalErrors = 0;

  for (const source of sources) {
    console.log(`\n─── ${source.name} ───`);
    try {
      const xml = await fetchUrl(source.url);
      const items = await parseRss(xml);
      console.log(`  📄 ${items.length} items in feed`);

      let sourceDocs = 0, sourceSignals = 0;

      for (const item of items) {
        const title = extractTitle(item);
        const url = extractUrl(item);
        const content = extractText(item);
        const publishedAt = extractDate(item);
        const author = extractAuthor(item);
        if (!url) continue;
        const sourceUrlHash = md5(url);

        if (dryRun) {
          const signals = detectSignals(title, content);
          if (signals.length > 0) console.log(`  📝 [DRY] "${truncate(title, 60)}" → ${signals.length} signal(s)`);
          sourceDocs++; sourceSignals += signals.length;
          continue;
        }

        const docResult = await pool.query(
          `INSERT INTO external_documents
             (source_type, source_name, source_url, source_url_hash, title, content,
              author, published_at, fetched_at, processing_status, source_id, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), 'pending', $9, NOW())
           ON CONFLICT (source_url_hash, tenant_id) DO NOTHING RETURNING id`,
          [source.source_type || 'newsletter', source.name, url, sourceUrlHash,
           title.slice(0, 255), content, author, publishedAt, source.id]
        );
        if (docResult.rows.length === 0) continue;
        const docId = docResult.rows[0].id;
        sourceDocs++;

        const signals = detectSignals(title, content);
        for (const sig of signals) {
          const companyNames = extractCompanyNames(title, content);
          let companyId = null, companyName = null;
          if (companyNames.length > 0) { companyName = companyNames[0]; companyId = await findOrCreateCompany(companyName); }
          try {
            await pool.query(
              `INSERT INTO signal_events
                 (signal_type, company_id, company_name, confidence_score,
                  evidence_summary, evidence_snippet, evidence_snippets,
                  evidence_doc_ids, source_document_id, source_url,
                  triage_status, signal_category, detected_at, signal_date,
                  scoring_breakdown, hiring_implications, created_at, updated_at)
               VALUES ($1::signal_type, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                       'new', $11, NOW(), $12, $13, $14, NOW(), NOW())`,
              [sig.signal_type, companyId, companyName, sig.confidence_score,
               `${sig.signal_type}: ${truncate(title, 200)}`, sig.evidence_snippet,
               JSON.stringify(sig.evidence_phrases), [docId], docId, url,
               sig.signal_type.split('_')[0], publishedAt,
               JSON.stringify({ keywords: sig.evidence_keywords, phrases: sig.evidence_phrases }),
               JSON.stringify({ signal_type: sig.signal_type, company: companyName, source: source.name })]
            );
            sourceSignals++;
          } catch (sigErr) { console.warn(`  ⚠️  Signal error: ${sigErr.message}`); }
        }

        if (signals.length > 0) {
          await pool.query(`UPDATE external_documents SET signals_computed_at = NOW(), processing_status = 'processed' WHERE id = $1`, [docId]);
        }
      }

      await pool.query(`UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = 0, last_error = NULL WHERE id = $1`, [source.id]);
      console.log(`  ✅ ${sourceDocs} new article(s), ${sourceSignals} signal(s)`);
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
  console.log(`📊 NEWSLETTER HARVEST COMPLETE${dryRun ? ' [DRY RUN]' : ''}`);
  console.log(`   Sources:   ${sources.length}`);
  console.log(`   Articles:  ${totalDocs} new`);
  console.log(`   Signals:   ${totalSignals}`);
  console.log(`   Errors:    ${totalErrors}`);
  console.log('═══════════════════════════════════════════════════\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS
// ─────────────────────────────────────────────────────────────────────────────

async function showStats() {
  console.log('\n📊 Newsletter Harvester Statistics\n');

  const src = await pool.query(`
    SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE enabled) AS enabled,
           COUNT(*) FILTER (WHERE last_fetched_at IS NOT NULL) AS fetched,
           COUNT(*) FILTER (WHERE consecutive_errors > 0) AS errors
    FROM rss_sources WHERE source_type = 'newsletter'
  `);
  const s = src.rows[0];
  console.log(`Sources: ${s.total} total | ${s.enabled} enabled | ${s.fetched} fetched | ${s.errors} with errors`);

  const docs = await pool.query(`
    SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE processing_status = 'processed') AS processed,
           MIN(published_at) AS oldest, MAX(published_at) AS newest
    FROM external_documents WHERE source_type = 'newsletter'
  `);
  const d = docs.rows[0];
  console.log(`\nArticles: ${d.total} total | ${d.processed} processed`);
  if (d.oldest) console.log(`  Range: ${new Date(d.oldest).toISOString().slice(0, 10)} → ${new Date(d.newest).toISOString().slice(0, 10)}`);

  const sigs = await pool.query(`
    SELECT signal_type, COUNT(*) AS cnt, ROUND(AVG(confidence_score), 2) AS avg_conf
    FROM signal_events WHERE source_document_id IN (SELECT id FROM external_documents WHERE source_type = 'newsletter')
    GROUP BY signal_type ORDER BY cnt DESC
  `);
  console.log('\nSignals:');
  if (sigs.rows.length === 0) console.log('  (none yet)');
  else sigs.rows.forEach(r => console.log(`  ${r.signal_type.padEnd(25)} ${String(r.cnt).padStart(5)}  (avg: ${r.avg_conf})`));

  const health = await pool.query(`
    SELECT name, last_fetched_at, consecutive_errors, last_error
    FROM rss_sources WHERE source_type = 'newsletter' AND enabled = true
    ORDER BY consecutive_errors DESC, last_fetched_at ASC
  `);
  console.log('\nSource health:');
  for (const r of health.rows) {
    const fetched = r.last_fetched_at ? new Date(r.last_fetched_at).toISOString().slice(0, 16) : 'never';
    const status = r.consecutive_errors > 0 ? `❌ ${r.consecutive_errors}err` : '✅';
    console.log(`  ${status} ${r.name.padEnd(40)} last: ${fetched}`);
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
  console.log('  Newsletter RSS Harvester');
  console.log('═══════════════════════════════════════════════════');

  try {
    await pool.query('SELECT 1');
    console.log('✅ Database connected');

    await ensureSourceType();

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
