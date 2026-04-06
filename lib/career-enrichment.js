// ═══════════════════════════════════════════════════════════════════════════════
// lib/career-enrichment.js — Extract career data from signal documents
//
// Post-processor for the signal pipeline. When a document mentions people
// (appointments, departures, promotions), extract structured career data
// and write it back to the people table.
//
// Usage:
//   const { enrichCareersFromDocument } = require('./career-enrichment');
//   await enrichCareersFromDocument(db, documentId, tenantId);
// ═══════════════════════════════════════════════════════════════════════════════

const https = require('https');
const http = require('http');

const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const MIN_CONTENT_LENGTH = 200; // Fetch full article if RSS content is shorter

const CAREER_EXTRACTION_PROMPT = `You are a career data extraction engine for a professional intelligence platform.

Given this article about executive moves, appointments, or organisational changes, extract structured career data for every person mentioned.

ARTICLE:
Title: {title}
Source: {source}
Content: {content}

Extract ALL people mentioned with their career details. For each person, capture:
- Their current/new role and company (from this announcement)
- Any previous roles mentioned in their bio/background
- Expertise areas, sectors, education if mentioned
- Career summary/bio if provided

Return ONLY valid JSON:
{
  "people": [
    {
      "name": "Full Name",
      "new_role": "Title at Company (the appointment/move)",
      "new_title": "Job Title",
      "new_company": "Company Name",
      "previous_roles": [
        {"title": "Previous Title", "company": "Previous Company", "approximate_dates": "2020-2024 or null"}
      ],
      "expertise": ["area1", "area2"],
      "sectors": ["sector1"],
      "education": [{"institution": "University", "degree": "MBA", "field": "Finance"}],
      "years_experience": 20,
      "bio_summary": "One paragraph career summary extracted from the article",
      "seniority": "c_suite|vp|director|senior|manager|null"
    }
  ]
}

Rules:
- Extract EVERY person mentioned, not just the main subject
- Include previous roles even if dates are approximate or missing
- For bio_summary, use the article's own language — don't invent
- If someone "previously served as X at Y", that's a previous_role
- If someone "brings 20 years of experience", capture years_experience
- If no people are clearly mentioned with career context, return {"people": []}`;

async function enrichCareersFromDocument(db, docId, tenantId) {
  // 1. Get the document
  const doc = await db.queryOne(
    'SELECT id, title, content, source_url, source_name FROM external_documents WHERE id = $1',
    [docId]
  );
  if (!doc) return { enriched: 0, error: 'Document not found' };

  // 2. Get content — fetch full article if RSS truncated
  let content = doc.content || '';
  if (content.length < MIN_CONTENT_LENGTH && doc.source_url) {
    try {
      const fetched = await fetchArticle(doc.source_url);
      if (fetched && fetched.length > content.length) {
        content = fetched;
        // Save full content back to document
        await db.query(
          'UPDATE external_documents SET content = $1 WHERE id = $2',
          [content.substring(0, 50000), docId]
        );
      }
    } catch (e) {
      // Use what we have
    }
  }

  if (content.length < 50) return { enriched: 0, error: 'No content' };

  // 3. Extract career data via Claude
  const prompt = CAREER_EXTRACTION_PROMPT
    .replace('{title}', doc.title || '')
    .replace('{source}', doc.source_name || '')
    .replace('{content}', content.substring(0, 8000));

  const extraction = await callClaude(prompt);
  if (!extraction || !extraction.people || !extraction.people.length) {
    return { enriched: 0, skipped: 'No people extracted' };
  }

  // 4. Match and update people
  let enriched = 0, created = 0, skipped = 0;

  for (const person of extraction.people) {
    if (!person.name || person.name.length < 3) { skipped++; continue; }

    try {
      // Find person in DB — try exact name, then fuzzy
      let match = await db.queryOne(
        `SELECT id, full_name, career_history, bio, seniority_level, expertise_tags
         FROM people WHERE LOWER(full_name) = LOWER($1) AND tenant_id = $2`,
        [person.name.trim(), tenantId]
      );

      // Fuzzy: try first + last name split
      if (!match) {
        const parts = person.name.trim().split(/\s+/);
        if (parts.length >= 2) {
          match = await db.queryOne(
            `SELECT id, full_name, career_history, bio, seniority_level, expertise_tags
             FROM people WHERE LOWER(first_name) = LOWER($1) AND LOWER(last_name) = LOWER($2) AND tenant_id = $3`,
            [parts[0], parts[parts.length - 1], tenantId]
          );
        }
      }

      if (!match) {
        // Create new person if we have enough data
        if (person.new_title && person.new_company) {
          const careerHistory = buildCareerHistory(person);
          const result = await db.queryOne(
            `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name,
              career_history, bio, seniority_level, expertise_tags, source, tenant_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'signal_enrichment', $10, NOW())
             ON CONFLICT DO NOTHING RETURNING id`,
            [
              person.name.trim(),
              person.name.trim().split(/\s+/)[0],
              person.name.trim().split(/\s+/).slice(-1)[0],
              person.new_title,
              person.new_company,
              JSON.stringify(careerHistory),
              person.bio_summary || null,
              person.seniority || null,
              (person.expertise || []).slice(0, 20),
              tenantId,
            ]
          );
          if (result) created++;
        }
        continue;
      }

      // Update existing person
      const updates = [];
      const params = [match.id, tenantId];
      let idx = 2;

      // Build/merge career history
      const newCareer = buildCareerHistory(person);
      if (newCareer.length) {
        const existingCareer = match.career_history || [];
        const merged = mergeCareerHistory(existingCareer, newCareer);
        idx++; updates.push(`career_history = $${idx}`);
        params.push(JSON.stringify(merged));
      }

      // Update current title + company if this is an appointment
      if (person.new_title) {
        idx++; updates.push(`current_title = $${idx}`);
        params.push(person.new_title);
      }
      if (person.new_company) {
        idx++; updates.push(`current_company_name = $${idx}`);
        params.push(person.new_company);
      }

      // Bio — only if we got a substantial one and existing is short/empty
      if (person.bio_summary && person.bio_summary.length > 30) {
        const existingBio = match.bio || '';
        if (existingBio.length < person.bio_summary.length) {
          idx++; updates.push(`bio = $${idx}`);
          params.push(person.bio_summary);
        }
      }

      // Seniority
      if (person.seniority) {
        idx++; updates.push(`seniority_level = $${idx}`);
        params.push(person.seniority);
      }

      // Merge expertise tags
      if (person.expertise && person.expertise.length) {
        const existing = match.expertise_tags || [];
        const merged = [...new Set([...existing, ...person.expertise])].slice(0, 30);
        idx++; updates.push(`expertise_tags = $${idx}`);
        params.push(merged);
      }

      // Education
      if (person.education && person.education.length) {
        idx++; updates.push(`education = COALESCE(education, '[]'::jsonb) || $${idx}::jsonb`);
        params.push(JSON.stringify(person.education));
      }

      if (updates.length) {
        await db.query(
          `UPDATE people SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $2`,
          params
        );
        enriched++;
      }
    } catch (err) {
      console.error(`  Career enrichment error for ${person.name}:`, err.message);
      skipped++;
    }
  }

  // Mark document as career-processed
  await db.query(
    `UPDATE external_documents SET processing_status = 'career_enriched' WHERE id = $1`,
    [docId]
  );

  return { enriched, created, skipped, people_found: extraction.people.length };
}

function buildCareerHistory(person) {
  const history = [];

  // Current/new role
  if (person.new_title && person.new_company) {
    history.push({
      title: person.new_title,
      company: person.new_company,
      current: true,
      start_date: new Date().toISOString().split('T')[0],
      source: 'signal_enrichment',
    });
  }

  // Previous roles
  if (person.previous_roles && person.previous_roles.length) {
    person.previous_roles.forEach(function(r) {
      if (!r.title && !r.company) return;
      const dates = parseApproximateDates(r.approximate_dates);
      history.push({
        title: r.title,
        company: r.company,
        current: false,
        start_date: dates.start,
        end_date: dates.end,
        source: 'signal_enrichment',
      });
    });
  }

  return history;
}

function mergeCareerHistory(existing, incoming) {
  // Merge by deduplicating on title+company combo
  const key = (r) => ((r.title || '') + '|' + (r.company || '')).toLowerCase();
  const seen = {};
  const merged = [];

  // Existing roles first (preserve)
  (Array.isArray(existing) ? existing : []).forEach(function(r) {
    const k = key(r);
    if (!seen[k]) { merged.push(r); seen[k] = true; }
  });

  // Add incoming roles that don't exist
  incoming.forEach(function(r) {
    const k = key(r);
    if (!seen[k]) {
      merged.push(r);
      seen[k] = true;
    } else {
      // Update existing entry if incoming has more data (e.g. dates)
      const idx = merged.findIndex(m => key(m) === k);
      if (idx >= 0) {
        if (r.start_date && !merged[idx].start_date) merged[idx].start_date = r.start_date;
        if (r.end_date && !merged[idx].end_date) merged[idx].end_date = r.end_date;
        if (r.current) merged[idx].current = true;
      }
    }
  });

  return merged;
}

function parseApproximateDates(dateStr) {
  if (!dateStr) return { start: null, end: null };
  // Handle: "2020-2024", "2020 - 2024", "2020-present", "since 2020"
  const match = dateStr.match(/(\d{4})\s*[-–—to]\s*(\d{4}|present|current|now)/i);
  if (match) {
    return {
      start: match[1] + '-01-01',
      end: /present|current|now/i.test(match[2]) ? null : match[2] + '-01-01',
    };
  }
  const single = dateStr.match(/(\d{4})/);
  if (single) return { start: single[1] + '-01-01', end: null };
  return { start: null, end: null };
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
          // Extract JSON from response
          const jsonMatch = text.match(/\{[\s\S]*\}/);
          if (jsonMatch) {
            resolve(JSON.parse(jsonMatch[0]));
          } else {
            resolve(null);
          }
        } catch (e) {
          resolve(null);
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function fetchArticle(url) {
  return new Promise(function(resolve, reject) {
    const timeout = setTimeout(function() { resolve(null); }, 10000);
    const mod = url.startsWith('https') ? https : http;

    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0 Autonodal/1.0' } }, function(res) {
      // Follow redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        clearTimeout(timeout);
        return fetchArticle(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) { clearTimeout(timeout); resolve(null); return; }

      let data = '';
      res.on('data', function(chunk) { data += chunk; if (data.length > 100000) res.destroy(); });
      res.on('end', function() {
        clearTimeout(timeout);
        // Strip HTML tags, get text content
        const text = data
          .replace(/<script[\s\S]*?<\/script>/gi, '')
          .replace(/<style[\s\S]*?<\/style>/gi, '')
          .replace(/<[^>]+>/g, ' ')
          .replace(/&nbsp;/g, ' ')
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/\s+/g, ' ')
          .trim();
        resolve(text.substring(0, 50000));
      });
    });
    req.on('error', function() { clearTimeout(timeout); resolve(null); });
  });
}

module.exports = { enrichCareersFromDocument, buildCareerHistory, mergeCareerHistory };
