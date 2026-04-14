#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/harvest_jobs.js — Job Feed Harvester
// Fetches ATS feeds, upserts postings, detects removals
// ═══════════════════════════════════════════════════════════════════════════════
//
// Usage:
//   node scripts/harvest_jobs.js                    -- harvest all job feeds
//   node scripts/harvest_jobs.js --company-id <id>  -- harvest single company

require('dotenv').config();

const axios = require('axios');
const crypto = require('crypto');
const { parseString } = require('xml2js');
const { promisify } = require('util');

const parseXML = promisify(parseString);

const db = require('../lib/db');
const { ML_TENANT_ID } = require('../lib/tenant');
const { classifyJobPosting } = require('../lib/job_classifier');

const TENANT_ID = ML_TENANT_ID;
const REQUEST_TIMEOUT = 30000;
const REQUEST_DELAY = 1000;
const USER_AGENT = 'MitchelLake Signal Intelligence/1.0';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function computePostingHash(companyId, title, location) {
  const raw = `${companyId}|${(title || '').toLowerCase().trim()}|${(location || '').toLowerCase().trim()}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
}

function cleanText(text) {
  if (!text) return '';
  return text
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim();
}

// ═══════════════════════════════════════════════════════════════════════════════
// ATS-SPECIFIC PARSERS
// ═══════════════════════════════════════════════════════════════════════════════

function parseGreenhouseJobs(data, companyName) {
  const jobs = Array.isArray(data?.jobs) ? data.jobs : (Array.isArray(data) ? data : []);
  return jobs.map(j => ({
    title: j.title,
    department: j.departments?.[0]?.name || null,
    location: j.location?.name || null,
    employment_type: null,
    description_text: cleanText(j.content || j.description || ''),
    apply_url: j.absolute_url || null,
    external_id: String(j.id || ''),
    company_name: companyName,
  }));
}

function parseLeverJobs(items, companyName) {
  // Lever feed is RSS — items already parsed
  return items.map(item => ({
    title: item.title,
    department: null,
    location: extractLeverLocation(item.title, item.content),
    employment_type: null,
    description_text: cleanText(item.content || ''),
    apply_url: item.url || null,
    external_id: null,
    company_name: companyName,
  }));
}

function extractLeverLocation(title, content) {
  // Lever often puts location in title like "Software Engineer — San Francisco"
  const m = (title || '').match(/[–—-]\s*(.+)$/);
  return m ? m[1].trim() : null;
}

function parseAshbyJobs(data, companyName) {
  const jobs = Array.isArray(data?.jobs) ? data.jobs : [];
  return jobs.map(j => ({
    title: j.title,
    department: j.departmentName || j.team || null,
    location: j.locationName || j.location || null,
    employment_type: j.employmentType || null,
    description_text: cleanText(j.descriptionHtml || j.description || ''),
    apply_url: j.jobUrl || null,
    external_id: j.id || null,
    company_name: companyName,
  }));
}

function parseWorkableJobs(data, companyName) {
  const jobs = Array.isArray(data?.jobs) ? data.jobs : [];
  return jobs.map(j => ({
    title: j.title,
    department: j.department || null,
    location: [j.city, j.state, j.country].filter(Boolean).join(', ') || j.location || null,
    employment_type: j.employment_type || null,
    description_text: cleanText(j.description || ''),
    apply_url: j.url || j.application_url || null,
    external_id: j.shortcode || j.id || null,
    company_name: companyName,
  }));
}

function parseSmartRecruitersJobs(data, companyName) {
  const jobs = Array.isArray(data?.content) ? data.content : (Array.isArray(data) ? data : []);
  return jobs.map(j => ({
    title: j.name || j.title,
    department: j.department?.label || j.department || null,
    location: j.location?.city ? `${j.location.city}, ${j.location.country}` : null,
    employment_type: j.typeOfEmployment?.label || null,
    description_text: cleanText(j.jobAd?.sections?.jobDescription?.text || ''),
    apply_url: j.ref || null,
    external_id: j.id || null,
    company_name: companyName,
  }));
}

function parseRSSJobs(items, companyName) {
  // Generic RSS/XML parser for BambooHR, TeamTailor, Recruitee etc.
  return items.map(item => ({
    title: item.title,
    department: null,
    location: item.location || null,
    employment_type: null,
    description_text: cleanText(item.content || item.description || ''),
    apply_url: item.url || item.link || null,
    external_id: null,
    company_name: companyName,
  }));
}

// ═══════════════════════════════════════════════════════════════════════════════
// FEED FETCHING
// ═══════════════════════════════════════════════════════════════════════════════

async function fetchJobFeed(url, atsType) {
  const resp = await axios.get(url, {
    timeout: REQUEST_TIMEOUT,
    headers: {
      'User-Agent': USER_AGENT,
      'Accept': 'application/json, application/xml, text/xml, application/rss+xml',
    },
    maxRedirects: 3,
  });

  const contentType = resp.headers['content-type'] || '';

  // JSON APIs (Greenhouse, Ashby, Workable, SmartRecruiters, Recruitee)
  if (contentType.includes('json') || (typeof resp.data === 'object' && resp.data !== null)) {
    return { type: 'json', data: resp.data };
  }

  // XML/RSS feeds (Lever, BambooHR, TeamTailor)
  if (typeof resp.data === 'string') {
    const parsed = await parseXML(resp.data);
    const items = [];
    const channel = parsed.rss?.channel?.[0];
    if (channel?.item) {
      for (const item of channel.item) {
        const title = Array.isArray(item.title) ? item.title[0] : item.title;
        const link = Array.isArray(item.link) ? item.link[0] : item.link;
        const desc = Array.isArray(item.description) ? item.description[0] : item.description;
        const location = item['jobLocation']?.[0] || item['location']?.[0] || null;
        if (title) items.push({ title: cleanText(title), url: link, content: desc, location: cleanText(location) });
      }
    }
    // Atom
    if (parsed.feed?.entry) {
      for (const entry of parsed.feed.entry) {
        const title = Array.isArray(entry.title) ? entry.title[0] : entry.title;
        const titleText = typeof title === 'object' ? (title._ || title['#text']) : title;
        const link = entry.link?.[0]?.$?.href || '';
        const content = Array.isArray(entry.content) ? entry.content[0] : entry.content;
        if (titleText) items.push({ title: cleanText(titleText), url: link, content: typeof content === 'object' ? content._ : content });
      }
    }
    return { type: 'rss', items };
  }

  return { type: 'unknown', data: null };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HARVEST SINGLE COMPANY
// ═══════════════════════════════════════════════════════════════════════════════

async function harvestCompanyJobs(rssSource) {
  const companyId = rssSource.notes?.match(/company_id:([a-f0-9-]+)/)?.[1];
  const atsType = rssSource.notes?.match(/ats:([a-z]+)/)?.[1] || 'unknown';
  if (!companyId) return { added: 0, updated: 0, removed: 0 };

  const company = await db.queryOne('SELECT name FROM companies WHERE id = $1', [companyId]);
  const companyName = company?.name || '';

  let feed;
  try {
    feed = await fetchJobFeed(rssSource.url, atsType);
  } catch (e) {
    await db.query(`
      UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = consecutive_errors + 1, last_error = $2
      WHERE id = $1
    `, [rssSource.id, (e.message || 'fetch_error').substring(0, 200)]);
    return { added: 0, updated: 0, removed: 0, error: e.message };
  }

  // Parse into normalized postings
  let postings = [];
  if (feed.type === 'json') {
    if (atsType === 'greenhouse') postings = parseGreenhouseJobs(feed.data, companyName);
    else if (atsType === 'ashby') postings = parseAshbyJobs(feed.data, companyName);
    else if (atsType === 'workable') postings = parseWorkableJobs(feed.data, companyName);
    else if (atsType === 'smartrecruiters') postings = parseSmartRecruitersJobs(feed.data, companyName);
    else postings = parseGreenhouseJobs(feed.data, companyName); // Fallback JSON
  } else if (feed.type === 'rss') {
    if (atsType === 'lever') postings = parseLeverJobs(feed.items, companyName);
    else postings = parseRSSJobs(feed.items, companyName);
  }

  const currentHashes = new Set();
  const newGeoPostings = [];
  let added = 0, updated = 0;

  for (const posting of postings) {
    if (!posting.title) continue;

    const classified = classifyJobPosting(posting.title, posting.location);
    const hash = computePostingHash(companyId, posting.title, posting.location);
    currentHashes.add(hash);

    const result = await db.query(`
      INSERT INTO job_postings (
        tenant_id, company_id, company_name, title, department,
        location, employment_type, description_text, apply_url,
        seniority_level, function_area, posting_hash, external_id,
        source_url, ats_type, status, first_seen_at, last_seen_at,
        is_geo_expansion_role, geo_role_class, target_geography, target_geo_tier
      ) VALUES (
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9,
        $10, $11, $12, $13,
        $14, $15, 'active', NOW(), NOW(),
        $16, $17, $18, $19
      )
      ON CONFLICT (tenant_id, posting_hash)
      DO UPDATE SET
        last_seen_at = NOW(),
        status = 'active',
        removed_at = NULL,
        description_text = EXCLUDED.description_text,
        is_geo_expansion_role = EXCLUDED.is_geo_expansion_role,
        geo_role_class = EXCLUDED.geo_role_class,
        target_geography = EXCLUDED.target_geography,
        target_geo_tier = EXCLUDED.target_geo_tier
      RETURNING (xmax = 0) AS is_new
    `, [
      TENANT_ID, companyId, posting.company_name, posting.title, posting.department,
      posting.location, posting.employment_type, (posting.description_text || '').substring(0, 10000), posting.apply_url,
      classified.seniority_level, classified.function_area, hash, posting.external_id,
      posting.apply_url, atsType,
      classified.is_geo_expansion_role, classified.geo_role_class,
      classified.target_geography, classified.target_geo_tier,
    ]);

    if (result.rows[0]?.is_new) {
      added++;
      // Track new geo expansion roles for immediate signal firing
      if (classified.is_geo_expansion_role) {
        newGeoPostings.push({
          company_id: companyId,
          title: posting.title,
          geo_role_class: classified.geo_role_class,
          target_geography: classified.target_geography,
          target_geo_tier: classified.target_geo_tier,
          is_geo_expansion_role: true,
        });
      }
    } else {
      updated++;
    }
  }

  // Detect removals: active postings not in current feed
  const hashArray = Array.from(currentHashes);
  let removedRows = [];
  if (hashArray.length > 0) {
    const removeResult = await db.query(`
      UPDATE job_postings SET
        status = 'removed',
        removed_at = NOW()
      WHERE tenant_id = $1
        AND company_id = $2
        AND status = 'active'
        AND last_seen_at < NOW() - INTERVAL '6 hours'
        AND posting_hash != ALL($3::text[])
      RETURNING id, title, seniority_level, first_seen_at
    `, [TENANT_ID, companyId, hashArray]);
    removedRows = removeResult.rows;
  } else {
    // Feed returned 0 items — don't mark all as removed (could be transient error)
  }

  // Update rss_source
  await db.query(`
    UPDATE rss_sources SET last_fetched_at = NOW(), consecutive_errors = 0, last_error = NULL
    WHERE id = $1
  `, [rssSource.id]);

  return { added, updated, removed: removedRows.length, removedRows, companyId, newGeoPostings };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HARVEST ALL
// ═══════════════════════════════════════════════════════════════════════════════

async function harvestAllJobFeeds() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  JOB FEED HARVESTER');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();

  const startTime = Date.now();

  // Optionally filter to a single company
  const companyIdArg = process.argv.find((a, i) => process.argv[i - 1] === '--company-id');
  let whereExtra = '';
  const params = [];
  if (companyIdArg) {
    whereExtra = ` AND notes LIKE $1`;
    params.push(`%company_id:${companyIdArg}%`);
  }

  const sources = await db.queryAll(`
    SELECT * FROM rss_sources
    WHERE source_type = 'jobs' AND enabled = true ${whereExtra}
    ORDER BY last_fetched_at ASC NULLS FIRST
  `, params);

  console.log(`Found ${sources.length} job feeds to harvest\n`);

  let totalAdded = 0, totalUpdated = 0, totalRemoved = 0, errors = 0;
  const signalQueue = [];

  for (const source of sources) {
    console.log(`  ${source.name}`);
    try {
      const result = await harvestCompanyJobs(source);
      totalAdded += result.added;
      totalUpdated += result.updated;
      totalRemoved += result.removed;

      if (result.added > 0 || result.removed > 0) {
        signalQueue.push({
          companyId: result.companyId,
          added: result.added,
          removed: result.removedRows || [],
          newGeoPostings: result.newGeoPostings || [],
        });
      }

      console.log(`    +${result.added} new, ~${result.updated} updated, -${result.removed} removed`);
    } catch (e) {
      console.log(`    ❌ ${e.message}`);
      errors++;
    }

    await sleep(REQUEST_DELAY);
  }

  // Evaluate job signals for companies with changes
  try {
    const { evaluateJobSignals, evaluateGeoRoleSignal, evaluateGeoLeadershipWave } = require('../lib/job_signal_evaluator');
    for (const item of signalQueue) {
      await evaluateJobSignals(item.companyId, { added: item.added, removed: item.removed });
      // Fire immediate signals for geographic leadership roles
      for (const geoPosting of (item.newGeoPostings || [])) {
        await evaluateGeoRoleSignal(geoPosting);
      }
    }
    // Market-level wave detection (runs once after all companies processed)
    await evaluateGeoLeadershipWave();
  } catch (e) {
    console.warn('Signal evaluation skipped:', e.message);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log();
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log(`  Complete: +${totalAdded} new, ~${totalUpdated} updated, -${totalRemoved} removed, ${errors} errors (${duration}s)`);
  console.log('═══════════════════════════════════════════════════════════════════');

  return { added: totalAdded, updated: totalUpdated, removed: totalRemoved, errors, feeds: sources.length };
}

if (require.main === module) {
  harvestAllJobFeeds()
    .then(() => process.exit(0))
    .catch(err => { console.error('Fatal:', err); process.exit(1); });
}

module.exports = { harvestAllJobFeeds, harvestCompanyJobs };
