// ═══════════════════════════════════════════════════════════════════════════════
// lib/company-search-enrichment.js — On-demand Google News enrichment for companies
//
// Searches for a company, extracts signals, leadership, strategy, and updates
// the company record + creates signal_events.
// ═══════════════════════════════════════════════════════════════════════════════

const https = require('https');
const http = require('http');

const SERPER_API_KEY = process.env.SERPER_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const MAX_ARTICLES = 5;

const COMPANY_PROMPT = `You are a company intelligence extraction engine for an executive search firm.

Given news articles about a company, extract structured intelligence.

COMPANY: {name}
KNOWN SECTOR: {sector}
KNOWN GEOGRAPHY: {geography}

ARTICLES:
{articles}

Extract intelligence from ALL articles combined:

Return ONLY valid JSON:
{
  "company_name": "Canonical company name",
  "description": "2-3 sentence company description synthesized from articles",
  "sector": "Primary sector (technology/financial_services/healthcare/energy/consumer/industrial/professional_services/other)",
  "sub_sector": "More specific (e.g. fintech, biotech, SaaS)",
  "geography": "HQ location",
  "employee_count_band": "1-10/11-50/51-200/201-500/501-1000/1001-5000/5000+/null",
  "signals": [
    {
      "type": "capital_raising|geographic_expansion|ma_activity|leadership_change|restructuring|product_launch|strategic_hiring|partnership",
      "summary": "What happened",
      "confidence": 0.0-1.0,
      "date": "YYYY-MM-DD or null",
      "source": "Reuters/WSJ/etc",
      "people_involved": [
        {"name": "Full Name", "title": "Their Role", "action": "appointed/departed/promoted"}
      ],
      "hiring_implications": {
        "likely_roles": ["CFO", "VP Engineering"],
        "timeline": "immediate/3-6 months/6-12 months",
        "seniority": "c_suite/vp/director"
      }
    }
  ],
  "leadership": [
    {"name": "Full Name", "title": "Current Title", "source": "mentioned in article"}
  ],
  "competitors": ["Competitor 1", "Competitor 2"],
  "recent_funding": {"amount": "$50M", "round": "Series C", "date": "2026-01"},
  "key_themes": ["AI", "cloud", "expansion"]
}

Rules:
- Extract ALL signals, not just the primary one
- People involved in signals should include full name + title
- hiring_implications: what roles might they need to fill?
- If articles mention funding amounts, capture in recent_funding
- competitors: only if explicitly mentioned in articles
- key_themes: 3-5 strategic themes from the coverage`;

async function enrichCompanyFromSearch(db, companyId, tenantId) {
  const company = await db.queryOne(
    'SELECT id, name, sector, geography, description, domain FROM companies WHERE id = $1 AND tenant_id = $2',
    [companyId, tenantId]
  );
  if (!company) return { error: 'Company not found' };
  if (!SERPER_API_KEY) return { error: 'SERPER_API_KEY not configured' };

  // Search Google News
  const query = `"${company.name}"`;
  const newsResults = await serperSearch(query);
  if (!newsResults.length) return { enriched: false, reason: 'No news articles found' };

  // Fetch articles
  const toFetch = newsResults.slice(0, MAX_ARTICLES);
  const articles = await Promise.all(
    toFetch.map(async (result) => {
      try {
        const content = await fetchArticleContent(result.link);
        return {
          title: result.title, source: result.source, date: result.date,
          url: result.link, content: (content || result.snippet || '').substring(0, 6000),
        };
      } catch (e) {
        return { title: result.title, source: result.source, date: result.date, url: result.link, content: result.snippet };
      }
    })
  );

  // Claude extraction
  const articlesText = articles.map((a, i) =>
    `--- Article ${i + 1}: ${a.title} (${a.source}, ${a.date || 'recent'}) ---\n${a.content}`
  ).join('\n\n');

  const prompt = COMPANY_PROMPT
    .replace('{name}', company.name)
    .replace('{sector}', company.sector || 'Unknown')
    .replace('{geography}', company.geography || 'Unknown')
    .replace('{articles}', articlesText);

  const extraction = await callClaude(prompt);
  if (!extraction) return { enriched: false, reason: 'Claude extraction failed' };

  // Update company record
  const updates = [];
  const params = [companyId, tenantId];
  let idx = 2;

  if (extraction.description && extraction.description.length > 20) {
    idx++; updates.push(`description = $${idx}`);
    params.push(extraction.description);
  }
  if (extraction.sector) {
    idx++; updates.push(`sector = $${idx}`);
    params.push(extraction.sector);
  }
  if (extraction.sub_sector) {
    idx++; updates.push(`sub_sector = $${idx}`);
    params.push(extraction.sub_sector);
  }
  if (extraction.geography) {
    idx++; updates.push(`geography = COALESCE(geography, $${idx})`);
    params.push(extraction.geography);
  }
  if (extraction.employee_count_band) {
    idx++; updates.push(`employee_count_band = $${idx}`);
    params.push(extraction.employee_count_band);
  }

  if (updates.length) {
    await db.query(
      `UPDATE companies SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $2`,
      params
    );
  }

  // Store signals
  let signalsStored = 0;
  for (const sig of (extraction.signals || [])) {
    try {
      const sigTypeMap = {
        capital_raising: 'capital_raising', geographic_expansion: 'geographic_expansion',
        ma_activity: 'ma_activity', leadership_change: 'leadership_change',
        restructuring: 'restructuring', product_launch: 'product_launch',
        strategic_hiring: 'strategic_hiring', partnership: 'partnership',
      };
      const sigType = sigTypeMap[sig.type];
      if (!sigType) continue;

      await db.query(`
        INSERT INTO signal_events (company_id, company_name, signal_type, confidence_score,
          evidence_summary, detected_at, triage_status, tenant_id)
        VALUES ($1, $2, $3, $4, $5, NOW(), 'new', $6)
      `, [companyId, company.name, sigType, sig.confidence || 0.7, sig.summary, tenantId]);
      signalsStored++;

      // Create/update people mentioned in signals
      for (const person of (sig.people_involved || [])) {
        if (!person.name) continue;
        try {
          const existing = await db.queryOne(
            "SELECT id FROM people WHERE LOWER(full_name) = LOWER($1) AND tenant_id = $2",
            [person.name, tenantId]
          );
          if (existing) {
            // Update title + company
            if (person.title) {
              await db.query(
                'UPDATE people SET current_title = $1, current_company_name = $2, current_company_id = $3, updated_at = NOW() WHERE id = $4 AND tenant_id = $5',
                [person.title, company.name, companyId, existing.id, tenantId]
              );
            }
          } else if (person.title) {
            // Create new person
            const parts = person.name.trim().split(/\s+/);
            await db.query(
              `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, current_company_id,
                source, tenant_id, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, 'signal_enrichment', $7, NOW()) ON CONFLICT DO NOTHING`,
              [person.name, parts[0], parts[parts.length - 1], person.title, company.name, companyId, tenantId]
            );
          }
        } catch (e) { /* skip */ }
      }
    } catch (e) { /* skip dupes */ }
  }

  // Store leadership
  let leadershipUpdated = 0;
  for (const leader of (extraction.leadership || [])) {
    if (!leader.name || !leader.title) continue;
    try {
      const existing = await db.queryOne(
        "SELECT id FROM people WHERE LOWER(full_name) = LOWER($1) AND tenant_id = $2",
        [leader.name, tenantId]
      );
      if (existing) {
        await db.query(
          'UPDATE people SET current_title = $1, current_company_name = $2, current_company_id = $3, updated_at = NOW() WHERE id = $4 AND tenant_id = $5',
          [leader.title, company.name, companyId, existing.id, tenantId]
        );
        leadershipUpdated++;
      } else {
        const parts = leader.name.trim().split(/\s+/);
        await db.query(
          `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, current_company_id,
            source, tenant_id, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, 'signal_enrichment', $7, NOW()) ON CONFLICT DO NOTHING`,
          [leader.name, parts[0], parts[parts.length - 1], leader.title, company.name, companyId, tenantId]
        );
        leadershipUpdated++;
      }
    } catch (e) { /* skip */ }
  }

  return {
    enriched: true,
    articles_searched: newsResults.length,
    articles_fetched: articles.length,
    signals_stored: signalsStored,
    leadership_updated: leadershipUpdated,
    extraction,
  };
}

async function serperSearch(query) {
  const res = await fetch('https://google.serper.dev/news', {
    method: 'POST',
    headers: { 'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({ q: query, num: 10 }),
  });
  if (!res.ok) throw new Error('Serper error: ' + res.status);
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
        const text = data.replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '')
          .replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
          .replace(/\s+/g, ' ').trim();
        resolve(text.substring(0, 50000));
      });
    });
    req.on('error', function() { clearTimeout(timeout); resolve(null); });
  });
}

function callClaude(prompt) {
  return new Promise(function(resolve, reject) {
    const body = JSON.stringify({ model: CLAUDE_MODEL, max_tokens: 4096, messages: [{ role: 'user', content: prompt }] });
    const req = https.request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
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

module.exports = { enrichCompanyFromSearch };
