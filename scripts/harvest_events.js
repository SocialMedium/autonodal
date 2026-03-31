#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/harvest_events.js - EventMedium Event Feed Ingestion
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const { parseString } = require('xml2js');
const { promisify } = require('util');

const parseXML = promisify(parseString);

const db = require('../lib/db');
const { ML_TENANT_ID } = require('../lib/tenant');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const REQUEST_TIMEOUT = 30000;
const REQUEST_DELAY = 1000;
const MAX_ITEMS_PER_FEED = 50;

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL RELEVANCE MAPPING
// ═══════════════════════════════════════════════════════════════════════════════

const SIGNAL_RELEVANCE_MAP = {
  'AI':            ['product_launch', 'strategic_hiring', 'partnership'],
  'FinTech':       ['capital_raising', 'partnership', 'product_launch'],
  'Climate Tech':  ['capital_raising', 'partnership', 'geographic_expansion'],
  'Cybersecurity': ['strategic_hiring', 'product_launch', 'partnership'],
  'default':       ['partnership', 'product_launch']
};

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function hashUrl(url) {
  return crypto.createHash('md5').update(url).digest('hex');
}

function cleanText(text) {
  if (!text) return '';
  return text
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  try {
    const date = new Date(dateStr);
    return isNaN(date.getTime()) ? null : date.toISOString();
  } catch {
    return null;
  }
}

/**
 * Extract event date from description text using common patterns
 */
function extractEventDate(text) {
  if (!text) return null;

  // DD Month YYYY
  const dmyMatch = text.match(/(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})/i);
  if (dmyMatch) {
    const d = new Date(`${dmyMatch[2]} ${dmyMatch[1]}, ${dmyMatch[3]}`);
    if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }

  // Month DD, YYYY
  const mdyMatch = text.match(/(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})/i);
  if (mdyMatch) {
    const d = new Date(`${mdyMatch[1]} ${mdyMatch[2]}, ${mdyMatch[3]}`);
    if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }

  // ISO date YYYY-MM-DD
  const isoMatch = text.match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) {
    const d = new Date(isoMatch[1]);
    if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }

  return null;
}

/**
 * Extract city from description text
 */
function extractCity(text) {
  if (!text) return null;
  const patterns = [
    /\bin\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b/,
    /\bat\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b/,
    /([A-Z][a-z]+(?:\s[A-Z][a-z]+)?),\s+(?:UK|US|USA|Australia|Singapore|Germany|France|Canada|India)/
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) return match[1];
  }
  return null;
}

/**
 * Detect if event is virtual
 */
function detectVirtual(text) {
  if (!text) return false;
  return /\b(virtual|online|webinar|remote|digital\s+event)\b/i.test(text);
}

/**
 * Extract speaker names from description
 */
function extractSpeakers(text) {
  if (!text) return [];
  const speakers = [];
  const patterns = [
    /(?:featuring|speakers?\s+include|keynote(?:\s+by)?:?)\s+([^.]+)/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      const names = match[1].split(/,\s*|\s+and\s+/).map(n => n.trim()).filter(n => n.length > 2 && n.length < 60);
      speakers.push(...names);
    }
  }
  return [...new Set(speakers)].slice(0, 10);
}

/**
 * Detect event format from title and description
 */
function detectFormat(text) {
  if (!text) return 'other';
  const lower = text.toLowerCase();
  if (/\bsummit\b/.test(lower)) return 'summit';
  if (/\bmeet-?up\b/.test(lower)) return 'meetup';
  if (/\bworkshop\b/.test(lower)) return 'workshop';
  if (/\bwebinar\b/.test(lower)) return 'webinar';
  if (/\broundtable\b/.test(lower)) return 'roundtable';
  if (/\bdemo\s*day\b/.test(lower)) return 'demo_day';
  if (/\bpitch\b/.test(lower)) return 'pitch_event';
  if (/\bawards?\b/.test(lower)) return 'awards';
  if (/\bconference\b/.test(lower)) return 'conference';
  if (/\bpanel\b/.test(lower)) return 'panel';
  if (/\bnetworking\b/.test(lower)) return 'networking';
  return 'other';
}

/**
 * Extract external_id from EventMedium URL (?id=N)
 */
function extractExternalId(url) {
  if (!url) return null;
  try {
    const u = new URL(url);
    return u.searchParams.get('id') || null;
  } catch {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// RSS PARSING
// ═══════════════════════════════════════════════════════════════════════════════

function parseRSS(data, source) {
  const items = [];
  const channel = data.rss?.channel?.[0];
  if (!channel?.item) return items;

  for (const item of channel.item.slice(0, MAX_ITEMS_PER_FEED)) {
    const title = cleanText(Array.isArray(item.title) ? item.title[0] : item.title);
    const link = Array.isArray(item.link) ? item.link[0] : item.link;
    const description = cleanText(Array.isArray(item.description) ? item.description[0] : item.description);
    const pubDate = Array.isArray(item.pubDate) ? item.pubDate[0] : item.pubDate;

    if (!link) continue;

    const combined = `${title} ${description}`;
    items.push({
      title,
      event_url: link,
      description,
      published_at: parseDate(pubDate),
      external_id: extractExternalId(link),
      theme: source.theme,
      region: source.region,
      event_date: extractEventDate(combined),
      city: extractCity(combined),
      is_virtual: detectVirtual(combined),
      speaker_names: extractSpeakers(description),
      format: detectFormat(combined),
      signal_relevance: SIGNAL_RELEVANCE_MAP[source.theme] || SIGNAL_RELEVANCE_MAP['default'],
      raw_feed_data: item,
    });
  }
  return items;
}

function parseAtom(data, source) {
  const items = [];
  const feed = data.feed;
  if (!feed?.entry) return items;

  for (const entry of feed.entry.slice(0, MAX_ITEMS_PER_FEED)) {
    const title = cleanText((() => {
      const t = Array.isArray(entry.title) ? entry.title[0] : entry.title;
      return typeof t === 'object' ? t._ || t['#text'] : t;
    })());

    let link = '';
    if (entry.link) {
      const links = Array.isArray(entry.link) ? entry.link : [entry.link];
      const altLink = links.find(l => l.$?.rel === 'alternate' || !l.$?.rel);
      link = altLink?.$?.href || altLink?.href || links[0]?.$?.href || '';
    }
    if (!link) continue;

    const summary = Array.isArray(entry.summary) ? entry.summary[0] : entry.summary;
    const content = Array.isArray(entry.content) ? entry.content[0] : entry.content;
    const bodyText = cleanText(typeof content === 'object' ? content._ : (content || summary));
    const published = Array.isArray(entry.published) ? entry.published[0] : (Array.isArray(entry.updated) ? entry.updated[0] : entry.updated);

    const combined = `${title} ${bodyText}`;
    items.push({
      title,
      event_url: link,
      description: bodyText,
      published_at: parseDate(published),
      external_id: extractExternalId(link),
      theme: source.theme,
      region: source.region,
      event_date: extractEventDate(combined),
      city: extractCity(combined),
      is_virtual: detectVirtual(combined),
      speaker_names: extractSpeakers(bodyText),
      format: detectFormat(combined),
      signal_relevance: SIGNAL_RELEVANCE_MAP[source.theme] || SIGNAL_RELEVANCE_MAP['default'],
      raw_feed_data: entry,
    });
  }
  return items;
}

async function fetchFeed(source) {
  try {
    const response = await axios.get(source.feed_url, {
      timeout: REQUEST_TIMEOUT,
      headers: {
        'User-Agent': 'MitchelLake Signal Intelligence/1.0',
        'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml'
      }
    });

    const data = await parseXML(response.data);

    if (data.rss) return parseRSS(data, source);
    if (data.feed) return parseAtom(data, source);

    console.log(`   ⚠️  Unknown feed format for ${source.name}`);
    return [];
  } catch (error) {
    throw new Error(`Failed to fetch: ${error.message}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN HARVEST FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════

async function harvestEvents() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - EVENT HARVESTING');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();

  const startTime = Date.now();

  const sources = await db.queryAll(
    'SELECT * FROM event_sources WHERE is_active = true ORDER BY name'
  );

  console.log(`📡 Found ${sources.length} active event sources`);
  console.log();

  let totalFetched = 0;
  let totalNew = 0;
  let totalUpdated = 0;
  let totalErrors = 0;

  for (const source of sources) {
    console.log(`🔄 Processing: ${source.name}`);
    console.log(`   URL: ${source.feed_url.substring(0, 80)}...`);

    try {
      const items = await fetchFeed(source);
      console.log(`   📄 Fetched ${items.length} items`);

      let newItems = 0;
      let updatedItems = 0;

      for (const item of items) {
        const urlHash = hashUrl(item.event_url);
        const tenantId = source.tenant_id || ML_TENANT_ID;

        try {
          const result = await db.query(
            `INSERT INTO events (
              tenant_id, external_id, source_id, title, description,
              event_url, url_hash, theme, region, city,
              format, event_date, event_time, is_virtual,
              speaker_names, signal_relevance, raw_feed_data,
              published_at, fetched_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NOW())
            ON CONFLICT (url_hash) DO UPDATE SET
              title = EXCLUDED.title,
              description = EXCLUDED.description,
              published_at = EXCLUDED.published_at,
              raw_feed_data = EXCLUDED.raw_feed_data,
              updated_at = NOW()
            RETURNING (xmax = 0) AS is_new`,
            [
              tenantId,
              item.external_id,
              source.id,
              item.title,
              item.description,
              item.event_url,
              urlHash,
              item.theme,
              item.region,
              item.city,
              item.format,
              item.event_date,
              null,
              item.is_virtual,
              item.speaker_names.length > 0 ? item.speaker_names : null,
              item.signal_relevance,
              JSON.stringify(item.raw_feed_data),
              item.published_at,
            ]
          );

          if (result.rows[0]?.is_new) {
            newItems++;
          } else {
            updatedItems++;
          }
        } catch (itemErr) {
          console.log(`   ⚠️  Item error: ${itemErr.message}`);
        }
      }

      console.log(`   ✅ ${newItems} new, ${updatedItems} updated`);

      await db.query(
        `UPDATE event_sources SET
          last_fetched_at = NOW(),
          last_error = NULL,
          fetch_count = fetch_count + 1
        WHERE id = $1`,
        [source.id]
      );

      totalFetched += items.length;
      totalNew += newItems;
      totalUpdated += updatedItems;

    } catch (error) {
      console.log(`   ❌ Error: ${error.message}`);

      await db.query(
        `UPDATE event_sources SET
          last_fetched_at = NOW(),
          last_error = $2
        WHERE id = $1`,
        [source.id, error.message]
      );

      totalErrors++;
    }

    console.log();
    await sleep(REQUEST_DELAY);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  EVENT HARVESTING COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  console.log(`   📊 Summary:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   Sources processed: ${sources.length}`);
  console.log(`   Items fetched:     ${totalFetched}`);
  console.log(`   New events:        ${totalNew}`);
  console.log(`   Updated events:    ${totalUpdated}`);
  console.log(`   Errors:            ${totalErrors}`);
  console.log(`   Duration:          ${duration}s`);
  console.log();

  return { sources: sources.length, fetched: totalFetched, new: totalNew, updated: totalUpdated, errors: totalErrors };
}

// Run if called directly
if (require.main === module) {
  harvestEvents()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { harvestEvents };
