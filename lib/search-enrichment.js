// ═══════════════════════════════════════════════════════════════════════════════
// lib/search-enrichment.js — On-demand Google News career enrichment via Serper
//
// Searches Google News for a person, fetches top articles, extracts career data
// via Claude, and updates the person record.
//
// Usage:
//   const { enrichPersonFromSearch } = require('./search-enrichment');
//   const result = await enrichPersonFromSearch(db, personId, tenantId);
// ═══════════════════════════════════════════════════════════════════════════════

const https = require('https');
const http = require('http');

const SERPER_API_KEY = process.env.SERPER_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const MAX_ARTICLES = 5;
const MAX_ARTICLE_LENGTH = 6000;

const CAREER_PROMPT = `You are a career intelligence extraction engine. Given multiple news articles about a person, extract a comprehensive career profile.

PERSON: {name}
CURRENT KNOWN: {current_title} at {current_company}

ARTICLES:
{articles}

Extract a complete career profile from ALL articles combined. Merge information across sources — one article may have the new role, another may have the bio, another may have previous roles.

Return ONLY valid JSON:
{
  "name": "Full Name (corrected if needed)",
  "current_title": "Most current job title",
  "current_company": "Most current company",
  "bio_summary": "2-4 sentence professional biography synthesized from articles",
  "career_history": [
    {
      "title": "Job Title",
      "company": "Company Name",
      "current": true/false,
      "start_date": "YYYY or YYYY-MM or null",
      "end_date": "YYYY or YYYY-MM or null",
      "description": "Brief role description if available"
    }
  ],
  "expertise": ["area1", "area2"],
  "sectors": ["sector1"],
  "education": [
    {"institution": "University", "degree": "MBA", "field": "Finance"}
  ],
  "years_experience": null,
  "seniority": "c_suite|vp|director|senior|manager|null",
  "signals": [
    {
      "type": "new_role|promotion|departure|board_appointment|company_exit",
      "summary": "What happened",
      "date": "YYYY-MM-DD or null",
      "source": "Reuters/WSJ/etc"
    }
  ]
}

Rules:
- Sort career_history most recent first
- Mark the most current role as current: true
- Merge duplicate info across articles (don't repeat the same role twice)
- For bio_summary, synthesize across all sources — don't copy verbatim
- years_experience: only if explicitly stated, otherwise null
- signals: capture any career moves, appointments, departures mentioned`;

async function enrichPersonFromSearch(db, personId, tenantId) {
  // 1. Get person details
  const person = await db.queryOne(
    'SELECT id, full_name, current_title, current_company_name, career_history, bio, expertise_tags FROM people WHERE id = $1 AND tenant_id = $2',
    [personId, tenantId]
  );
  if (!person) return { error: 'Person not found' };

  if (!SERPER_API_KEY) return { error: 'SERPER_API_KEY not configured' };

  // 2. Search Google News
  const query = buildSearchQuery(person);
  const newsResults = await serperSearch(query);

  if (!newsResults.length) {
    // Try broader search
    const broader = await serperSearch(`"${person.full_name}"`);
    if (!broader.length) return { enriched: false, reason: 'No news articles found' };
    newsResults.push(...broader);
  }

  // 3. Fetch article content (parallel, max 5)
  const articlesToFetch = newsResults.slice(0, MAX_ARTICLES);
  const articles = await Promise.all(
    articlesToFetch.map(async (result) => {
      try {
        const content = await fetchArticleContent(result.link);
        return {
          title: result.title,
          source: result.source,
          date: result.date,
          url: result.link,
          snippet: result.snippet,
          content: content ? content.substring(0, MAX_ARTICLE_LENGTH) : result.snippet,
        };
      } catch (e) {
        return {
          title: result.title,
          source: result.source,
          date: result.date,
          url: result.link,
          snippet: result.snippet,
          content: result.snippet,
        };
      }
    })
  );

  // 4. Claude extraction
  const articlesText = articles.map((a, i) =>
    `--- Article ${i + 1}: ${a.title} (${a.source}, ${a.date || 'recent'}) ---\n${a.content || a.snippet}`
  ).join('\n\n');

  const prompt = CAREER_PROMPT
    .replace('{name}', person.full_name)
    .replace('{current_title}', person.current_title || 'Unknown')
    .replace('{current_company}', person.current_company_name || 'Unknown')
    .replace('{articles}', articlesText);

  const extraction = await callClaude(prompt);
  if (!extraction) return { enriched: false, reason: 'Claude extraction failed' };

  // 5. Update person record
  const updates = [];
  const params = [personId, tenantId];
  let idx = 2;

  // Career history — merge with existing
  if (extraction.career_history && extraction.career_history.length) {
    const existing = Array.isArray(person.career_history) ? person.career_history : [];
    const merged = mergeCareerHistory(existing, extraction.career_history);
    idx++; updates.push(`career_history = $${idx}`);
    params.push(JSON.stringify(merged));
  }

  // Current title + company — always update from search (freshest)
  if (extraction.current_title) {
    idx++; updates.push(`current_title = $${idx}`);
    params.push(extraction.current_title);
  }
  if (extraction.current_company) {
    idx++; updates.push(`current_company_name = $${idx}`);
    params.push(extraction.current_company);
  }

  // Bio
  if (extraction.bio_summary && extraction.bio_summary.length > 20) {
    idx++; updates.push(`bio = $${idx}`);
    params.push(extraction.bio_summary);
  }

  // Seniority
  if (extraction.seniority) {
    idx++; updates.push(`seniority_level = $${idx}`);
    params.push(extraction.seniority);
  }

  // Expertise
  if (extraction.expertise && extraction.expertise.length) {
    const existing = person.expertise_tags || [];
    const merged = [...new Set([...existing, ...extraction.expertise])].slice(0, 30);
    idx++; updates.push(`expertise_tags = $${idx}`);
    params.push(merged);
  }

  // Education
  if (extraction.education && extraction.education.length) {
    idx++; updates.push(`education = $${idx}`);
    params.push(JSON.stringify(extraction.education));
  }

  // Headline from bio
  if (extraction.bio_summary) {
    idx++; updates.push(`headline = $${idx}`);
    params.push(extraction.bio_summary.substring(0, 250));
  }

  if (updates.length) {
    await db.query(
      `UPDATE people SET ${updates.join(', ')}, enriched_at = NOW(), updated_at = NOW() WHERE id = $1 AND tenant_id = $2`,
      params
    );
  }

  // 6. Store signals as person_signals
  let signalsStored = 0;
  if (extraction.signals && extraction.signals.length) {
    for (const sig of extraction.signals) {
      try {
        // Map to person_signal_type enum
        const typeMap = {
          new_role: 'new_role', promotion: 'promotion', departure: 'company_exit',
          board_appointment: 'board_appointment', company_exit: 'company_exit',
        };
        const sigType = typeMap[sig.type] || 'news_mention';
        await db.query(`
          INSERT INTO person_signals (person_id, signal_type, title, description, source, source_url, confidence_score, detected_at, tenant_id)
          VALUES ($1, $2, $3, $4, 'google_news', $5, 0.8, NOW(), $6)
          ON CONFLICT DO NOTHING
        `, [personId, sigType, sig.summary, sig.summary, articles[0]?.url || null, tenantId]);
        signalsStored++;
      } catch (e) {
        // Skip enum errors etc
      }
    }
  }

  return {
    enriched: true,
    articles_searched: newsResults.length,
    articles_fetched: articles.length,
    career_roles: (extraction.career_history || []).length,
    signals_stored: signalsStored,
    extraction,
  };
}

function buildSearchQuery(person) {
  const parts = [`"${person.full_name}"`];
  if (person.current_company_name) parts.push(`"${person.current_company_name}"`);
  return parts.join(' ');
}

async function serperSearch(query) {
  const res = await fetch('https://google.serper.dev/news', {
    method: 'POST',
    headers: {
      'X-API-KEY': SERPER_API_KEY,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ q: query, num: 10 }),
  });

  if (!res.ok) throw new Error(`Serper API error: ${res.status}`);
  const data = await res.json();
  return data.news || [];
}

function fetchArticleContent(url) {
  return new Promise(function(resolve) {
    const timeout = setTimeout(function() { resolve(null); }, 8000);
    const mod = url.startsWith('https') ? https : http;

    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 Autonodal/1.0' } }, function(res) {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        clearTimeout(timeout);
        return fetchArticleContent(res.headers.location).then(resolve);
      }
      if (res.statusCode !== 200) { clearTimeout(timeout); resolve(null); return; }

      let data = '';
      res.on('data', function(chunk) { data += chunk; if (data.length > 200000) res.destroy(); });
      res.on('end', function() {
        clearTimeout(timeout);
        const text = data
          .replace(/<script[\s\S]*?<\/script>/gi, '')
          .replace(/<style[\s\S]*?<\/style>/gi, '')
          .replace(/<[^>]+>/g, ' ')
          .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
          .replace(/\s+/g, ' ').trim();
        resolve(text.substring(0, 50000));
      });
    });
    req.on('error', function() { clearTimeout(timeout); resolve(null); });
  });
}

function mergeCareerHistory(existing, incoming) {
  const key = (r) => ((r.title || '') + '|' + (r.company || '')).toLowerCase();
  const seen = {};
  const merged = [];

  (Array.isArray(existing) ? existing : []).forEach(function(r) {
    const k = key(r);
    if (!seen[k]) { merged.push(r); seen[k] = true; }
  });

  incoming.forEach(function(r) {
    const k = key(r);
    if (!seen[k]) {
      r.source = 'google_news';
      merged.push(r);
      seen[k] = true;
    } else {
      const idx = merged.findIndex(m => key(m) === k);
      if (idx >= 0) {
        if (r.start_date && !merged[idx].start_date) merged[idx].start_date = r.start_date;
        if (r.end_date && !merged[idx].end_date) merged[idx].end_date = r.end_date;
        if (r.description && !merged[idx].description) merged[idx].description = r.description;
        if (r.current) merged[idx].current = true;
      }
    }
  });

  return merged;
}

function callClaude(prompt) {
  return new Promise(function(resolve, reject) {
    const body = JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    });

    const req = https.request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
    }, function(res) {
      let data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        try {
          const parsed = JSON.parse(data);
          const text = parsed.content && parsed.content[0] ? parsed.content[0].text : '';
          const jsonMatch = text.match(/\{[\s\S]*\}/);
          if (jsonMatch) resolve(JSON.parse(jsonMatch[0]));
          else resolve(null);
        } catch (e) { resolve(null); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

module.exports = { enrichPersonFromSearch };
