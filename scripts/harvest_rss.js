#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/harvest_rss.js - RSS Feed Ingestion
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');
const { parseString } = require('xml2js');
const { promisify } = require('util');

const parseXML = promisify(parseString);

const db = require('../lib/db');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const REQUEST_TIMEOUT = 30000; // 30 seconds
const REQUEST_DELAY = 1000;    // 1 second between requests
const MAX_ITEMS_PER_FEED = 50;

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function hashUrl(url) {
  return crypto.createHash('sha256').update(url).digest('hex');
}

function cleanText(text) {
  if (!text) return '';
  
  return text
    .replace(/<[^>]*>/g, '')  // Remove HTML tags
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

// ═══════════════════════════════════════════════════════════════════════════════
// RSS PARSING
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parse RSS 2.0 feed
 */
function parseRSS(data, sourceName) {
  const items = [];
  const channel = data.rss?.channel?.[0];
  
  if (!channel?.item) return items;
  
  for (const item of channel.item.slice(0, MAX_ITEMS_PER_FEED)) {
    const title = Array.isArray(item.title) ? item.title[0] : item.title;
    let link = Array.isArray(item.link) ? item.link[0] : item.link;
    const description = Array.isArray(item.description) ? item.description[0] : item.description;
    const pubDate = Array.isArray(item.pubDate) ? item.pubDate[0] : item.pubDate;
    const author = Array.isArray(item.author) ? item.author[0] : (item['dc:creator']?.[0] || null);

    // Podcast: extract audio URL from enclosure, image from itunes:image
    let audioUrl = null;
    let imageUrl = null;
    if (item.enclosure) {
      const enc = Array.isArray(item.enclosure) ? item.enclosure[0] : item.enclosure;
      audioUrl = enc.$?.url || enc.url || null;
    }
    if (item['itunes:image']) {
      const img = Array.isArray(item['itunes:image']) ? item['itunes:image'][0] : item['itunes:image'];
      imageUrl = img.$?.href || img.href || null;
    }
    // If no link but we have audio URL, use it as the link
    if (!link && audioUrl) link = audioUrl;
    // If still no link, try guid
    if (!link) {
      const guid = Array.isArray(item.guid) ? item.guid[0] : item.guid;
      const guidText = typeof guid === 'object' ? guid._ : guid;
      if (guidText && guidText.startsWith('http')) link = guidText;
    }

    if (link) {
      items.push({
        title: cleanText(title),
        url: link,
        content: cleanText(description),
        author: cleanText(author),
        published_at: parseDate(pubDate),
        source_name: sourceName,
        image_url: imageUrl,
        audio_url: audioUrl,
      });
    }
  }
  
  return items;
}

/**
 * Parse Atom feed
 */
function parseAtom(data, sourceName) {
  const items = [];
  const feed = data.feed;
  
  if (!feed?.entry) return items;
  
  for (const entry of feed.entry.slice(0, MAX_ITEMS_PER_FEED)) {
    const title = Array.isArray(entry.title) ? entry.title[0] : entry.title;
    const titleText = typeof title === 'object' ? title._ || title['#text'] : title;
    
    // Get link (prefer alternate)
    let link = '';
    if (entry.link) {
      const links = Array.isArray(entry.link) ? entry.link : [entry.link];
      const altLink = links.find(l => l.$?.rel === 'alternate' || !l.$?.rel);
      link = altLink?.$?.href || altLink?.href || links[0]?.$?.href || '';
    }
    
    const summary = Array.isArray(entry.summary) ? entry.summary[0] : entry.summary;
    const content = Array.isArray(entry.content) ? entry.content[0] : entry.content;
    const updated = Array.isArray(entry.updated) ? entry.updated[0] : entry.updated;
    const published = Array.isArray(entry.published) ? entry.published[0] : entry.published;
    const author = entry.author?.[0]?.name?.[0] || null;
    
    const bodyText = typeof content === 'object' ? content._ : (content || summary);

    // YouTube-specific: extract video ID and thumbnail
    let videoId = null;
    let imageUrl = null;
    if (entry['yt:videoId']) {
      videoId = Array.isArray(entry['yt:videoId']) ? entry['yt:videoId'][0] : entry['yt:videoId'];
      imageUrl = 'https://img.youtube.com/vi/' + videoId + '/hqdefault.jpg';
      if (!link) link = 'https://www.youtube.com/watch?v=' + videoId;
    }
    // Also try media:group > media:thumbnail
    if (!imageUrl && entry['media:group']) {
      const mg = Array.isArray(entry['media:group']) ? entry['media:group'][0] : entry['media:group'];
      if (mg['media:thumbnail']) {
        const thumb = Array.isArray(mg['media:thumbnail']) ? mg['media:thumbnail'][0] : mg['media:thumbnail'];
        imageUrl = thumb?.$?.url || thumb?.url || null;
      }
    }

    if (link) {
      items.push({
        title: cleanText(titleText),
        url: link,
        content: cleanText(bodyText),
        author: cleanText(author),
        published_at: parseDate(published || updated),
        source_name: sourceName,
        image_url: imageUrl,
        video_id: videoId,
      });
    }
  }
  
  return items;
}

/**
 * Fetch and parse a feed
 */
async function fetchFeed(source) {
  try {
    const response = await axios.get(source.url, {
      timeout: REQUEST_TIMEOUT,
      headers: {
        'User-Agent': 'MitchelLake Signal Intelligence/1.0',
        'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml'
      }
    });
    
    const data = await parseXML(response.data);
    
    // Determine feed type and parse
    if (data.rss) {
      return parseRSS(data, source.name);
    } else if (data.feed) {
      return parseAtom(data, source.name);
    } else {
      console.log(`   ⚠️  Unknown feed format for ${source.name}`);
      return [];
    }
    
  } catch (error) {
    throw new Error(`Failed to fetch: ${error.message}`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN HARVEST FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════

async function harvestRSS() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - RSS HARVESTING');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  
  const startTime = Date.now();
  
  // Get enabled sources
  const sources = await db.queryAll(
    'SELECT * FROM rss_sources WHERE enabled = true ORDER BY name'
  );
  
  console.log(`📡 Found ${sources.length} enabled RSS sources`);
  console.log();
  
  let totalFetched = 0;
  let totalNew = 0;
  let totalErrors = 0;
  
  for (const source of sources) {
    console.log(`🔄 Processing: ${source.name}`);
    console.log(`   URL: ${source.url.substring(0, 60)}...`);
    
    try {
      // Fetch and parse feed
      const items = await fetchFeed(source);
      console.log(`   📄 Fetched ${items.length} items`);
      
      let newItems = 0;
      
      // Store each item
      for (const item of items) {
        const urlHash = hashUrl(item.url);
        
        // Check if already exists
        const existing = await db.queryOne(
          'SELECT id FROM external_documents WHERE source_url_hash = $1',
          [urlHash]
        );
        
        if (!existing) {
          await db.insert('external_documents', {
            source_type: item.video_id ? 'youtube' : (item.audio_url ? 'podcast' : 'rss'),
            source_name: item.source_name,
            source_url: item.url,
            source_url_hash: urlHash,
            title: item.title,
            content: item.content,
            author: item.author,
            published_at: item.published_at,
            image_url: item.image_url || null,
            audio_url: item.audio_url || null,
            processing_status: 'pending'
          });
          newItems++;
        }
      }
      
      console.log(`   ✅ Added ${newItems} new documents`);
      
      // Update source status
      await db.query(
        `UPDATE rss_sources SET 
          last_fetched_at = NOW(),
          consecutive_errors = 0,
          last_error = NULL
        WHERE id = $1`,
        [source.id]
      );
      
      totalFetched += items.length;
      totalNew += newItems;
      
    } catch (error) {
      console.log(`   ❌ Error: ${error.message}`);
      
      // Update error status
      await db.query(
        `UPDATE rss_sources SET 
          last_fetched_at = NOW(),
          consecutive_errors = consecutive_errors + 1,
          last_error = $2
        WHERE id = $1`,
        [source.id, error.message]
      );
      
      totalErrors++;
    }
    
    console.log();
    
    // Rate limiting delay
    await sleep(REQUEST_DELAY);
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  RSS HARVESTING COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  console.log(`   📊 Summary:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   Sources processed: ${sources.length}`);
  console.log(`   Items fetched:     ${totalFetched}`);
  console.log(`   New documents:     ${totalNew}`);
  console.log(`   Errors:            ${totalErrors}`);
  console.log(`   Duration:          ${duration}s`);
  console.log();
}

// Run if called directly
if (require.main === module) {
  harvestRSS()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { harvestRSS };
