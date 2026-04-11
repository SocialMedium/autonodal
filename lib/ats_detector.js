// ═══════════════════════════════════════════════════════════════════════════════
// lib/ats_detector.js — ATS Detection Service
// Crawls company careers pages, identifies ATS provider, constructs feed URLs
// ═══════════════════════════════════════════════════════════════════════════════

const axios = require('axios');

const FETCH_TIMEOUT = 6000;
const USER_AGENT = 'MitchelLake Signal Intelligence/1.0';

// ═══════════════════════════════════════════════════════════════════════════════
// ATS PATTERNS — feed URL constructors
// ═══════════════════════════════════════════════════════════════════════════════

const ATS_PATTERNS = [
  {
    name: 'lever',
    detect: (html, url) =>
      html.includes('jobs.lever.co') || url.includes('jobs.lever.co'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/jobs\.lever\.co\/([a-zA-Z0-9_-]+)/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://jobs.lever.co/${slug}/feed`,
    confidence: 0.95,
  },
  {
    name: 'greenhouse',
    detect: (html, url) =>
      html.includes('boards.greenhouse.io') ||
      html.includes('greenhouse.io/embed') ||
      url.includes('boards.greenhouse.io'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/boards\.greenhouse\.io\/([a-zA-Z0-9_-]+)/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) =>
      `https://boards-api.greenhouse.io/v1/boards/${slug}/jobs?content=true`,
    confidence: 0.95,
  },
  {
    name: 'workable',
    detect: (html, url) =>
      html.includes('apply.workable.com') || html.includes('workable.com/widget'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/apply\.workable\.com\/([a-zA-Z0-9_-]+)/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://apply.workable.com/api/v1/widget/${slug}`,
    confidence: 0.90,
  },
  {
    name: 'ashby',
    detect: (html, url) =>
      html.includes('jobs.ashbyhq.com') || url.includes('ashbyhq.com'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/jobs\.ashbyhq\.com\/([a-zA-Z0-9_-]+)/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://api.ashbyhq.com/posting-api/job-board/${slug}`,
    confidence: 0.95,
  },
  {
    name: 'smartrecruiters',
    detect: (html, url) =>
      html.includes('careers.smartrecruiters.com') || html.includes('smartrecruiters.com/o/'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/smartrecruiters\.com\/([a-zA-Z0-9_-]+)/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://api.smartrecruiters.com/v1/companies/${slug}/postings`,
    confidence: 0.88,
  },
  {
    name: 'bamboohr',
    detect: (html, url) =>
      html.includes('bamboohr.com/jobs') || html.includes('bamboohr.com/careers'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/([a-zA-Z0-9_-]+)\.bamboohr\.com/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://${slug}.bamboohr.com/jobs/feed.php`,
    confidence: 0.88,
  },
  {
    name: 'teamtailor',
    detect: (html) =>
      html.includes('teamtailor.com') || html.includes('teamtailor-cdn'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/([a-zA-Z0-9_-]+)\.teamtailor\.com/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://${slug}.teamtailor.com/jobs.xml`,
    confidence: 0.85,
  },
  {
    name: 'recruitee',
    detect: (html) => html.includes('recruitee.com'),
    extractSlug: (html, url) => {
      const m = (html + ' ' + url).match(/([a-zA-Z0-9_-]+)\.recruitee\.com/);
      return m ? m[1] : null;
    },
    feedUrl: (slug) => `https://${slug}.recruitee.com/api/offers`,
    confidence: 0.85,
  },
];

// ═══════════════════════════════════════════════════════════════════════════════
// CAREERS URL PATTERNS
// ═══════════════════════════════════════════════════════════════════════════════

const CAREERS_URL_PATTERNS = [
  (domain) => `https://${domain}/careers`,
  (domain) => `https://${domain}/jobs`,
  (domain) => `https://${domain}/about/careers`,
  (domain) => `https://careers.${domain}`,
  (domain) => `https://jobs.${domain}`,
  (domain) => `https://${domain}/work-with-us`,
];

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function extractDomain(url) {
  if (!url) return null;
  try {
    // Handle LinkedIn URLs — extract company domain from other fields
    if (url.includes('linkedin.com')) return null;
    const parsed = new URL(url.startsWith('http') ? url : `https://${url}`);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return null;
  }
}

async function fetchWithTimeout(url, timeoutMs) {
  try {
    const resp = await axios.get(url, {
      timeout: timeoutMs || FETCH_TIMEOUT,
      headers: { 'User-Agent': USER_AGENT },
      maxRedirects: 3,
      validateStatus: (s) => s < 400,
    });
    return typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
  } catch {
    return null;
  }
}

async function validateFeedUrl(url) {
  try {
    const resp = await axios.get(url, {
      timeout: FETCH_TIMEOUT,
      headers: { 'User-Agent': USER_AGENT },
      maxRedirects: 3,
      validateStatus: (s) => s < 400,
    });
    // Check for XML/JSON/HTML content that looks like a job listing
    const body = typeof resp.data === 'string' ? resp.data : JSON.stringify(resp.data);
    if (!body || body.length < 50) return false;
    // Greenhouse/Ashby/SmartRecruiters return JSON
    if (resp.headers['content-type']?.includes('json')) return true;
    // RSS/XML feeds
    if (body.includes('<rss') || body.includes('<feed') || body.includes('<item')) return true;
    // Workable widget / Recruitee API returns JSON objects
    if (body.startsWith('{') || body.startsWith('[')) return true;
    return false;
  } catch {
    return false;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN DETECTION
// ═══════════════════════════════════════════════════════════════════════════════

async function detectATS(company) {
  const domain = extractDomain(company.website_url) || extractDomain(company.domain);
  if (!domain) return null;

  const urlsToTry = company.careers_url
    ? [company.careers_url]
    : CAREERS_URL_PATTERNS.map((fn) => fn(domain));

  for (const url of urlsToTry) {
    try {
      const html = await fetchWithTimeout(url, FETCH_TIMEOUT);
      if (!html) continue;

      for (const ats of ATS_PATTERNS) {
        if (ats.detect(html, url)) {
          const slug = ats.extractSlug(html, url);
          if (!slug) continue;
          const feedUrl = ats.feedUrl(slug);
          const valid = await validateFeedUrl(feedUrl);
          if (valid) {
            return {
              ats_type: ats.name,
              ats_feed_url: feedUrl,
              careers_url: url,
              confidence: ats.confidence,
            };
          }
        }
      }
    } catch {
      // Continue to next URL pattern
    }
  }
  return null;
}

module.exports = { detectATS, ATS_PATTERNS, extractDomain, validateFeedUrl };
