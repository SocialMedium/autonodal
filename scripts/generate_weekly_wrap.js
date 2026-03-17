#!/usr/bin/env node
/**
 * Weekly Wrap — Regional intelligence summary with key numbers
 * Runs Sunday 6 AM. Produces a structured weekly digest per region.
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const ML_TENANT = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);

const REGIONS = [
  { code: 'AU', name: 'Australia & New Zealand', patterns: ['Australia', 'New Zealand', 'Sydney', 'Melbourne', 'Brisbane', 'Perth'] },
  { code: 'SG', name: 'Singapore & SEA', patterns: ['Singapore', 'Southeast Asia', 'ASEAN', 'Jakarta', 'Bangkok', 'Vietnam', 'Malaysia', 'Indonesia', 'Thailand', 'Philippines'] },
  { code: 'UK', name: 'United Kingdom & Europe', patterns: ['United Kingdom', 'London', 'England', 'Britain', 'Ireland', 'Europe', 'Germany', 'France', 'Netherlands'] },
  { code: 'US', name: 'United States & Americas', patterns: ['United States', 'Silicon Valley', 'New York', 'San Francisco', 'California', 'Texas', 'Boston', 'Seattle', 'Canada'] },
];

async function callClaude(system, user) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 1500, system, messages: [{ role: 'user', content: user }] });
    const req = https.request({ hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) }, timeout: 60000 },
      (res) => { const chunks = []; res.on('data', c => chunks.push(c)); res.on('end', () => {
        try { const d = JSON.parse(Buffer.concat(chunks).toString()); if (d.error) return reject(new Error(d.error.message)); resolve(d.content?.[0]?.text || ''); } catch (e) { reject(e); }
      }); });
    req.on('error', reject); req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body); req.end();
  });
}

async function getRegionData(region) {
  const geoConditions = region.patterns.map((_, i) => `c.geography ILIKE $${i + 2}`).join(' OR ');
  const params = [ML_TENANT, ...region.patterns.map(p => '%' + p + '%')];
  const nextIdx = params.length + 1;

  // Signal counts by type
  const { rows: byType } = await pool.query(`
    SELECT se.signal_type::text, COUNT(*) as cnt, COUNT(DISTINCT se.company_id) as companies
    FROM signal_events se LEFT JOIN companies c ON c.id = se.company_id
    WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false AND (${geoConditions})
    GROUP BY se.signal_type ORDER BY cnt DESC
  `, params);

  // Top companies by signal count
  const { rows: topCompanies } = await pool.query(`
    SELECT se.company_name, COUNT(*) as signal_count, c.is_client,
           array_agg(DISTINCT se.signal_type::text) as types
    FROM signal_events se LEFT JOIN companies c ON c.id = se.company_id
    WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false AND se.company_name IS NOT NULL AND (${geoConditions})
    GROUP BY se.company_name, c.is_client ORDER BY COUNT(*) DESC LIMIT 5
  `, params);

  // Big news — highest confidence signals with evidence
  const { rows: bigNews } = await pool.query(`
    SELECT se.company_name, se.signal_type::text, se.evidence_summary, se.confidence_score,
           ed.source_name, ed.source_url, ed.title as doc_title
    FROM signal_events se LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false AND (${geoConditions})
    ORDER BY se.confidence_score DESC, se.detected_at DESC LIMIT 5
  `, params);

  // Network contacts at signalling companies
  const { rows: [networkStats] } = await pool.query(`
    SELECT COUNT(DISTINCT p.id) as contacts_at_signalling
    FROM people p JOIN companies c ON c.id = p.current_company_id
    JOIN signal_events se ON se.company_id = c.id
    WHERE se.detected_at > NOW() - INTERVAL '7 days' AND p.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false AND (${geoConditions})
  `, params);

  const totalSignals = byType.reduce((s, r) => s + parseInt(r.cnt), 0);
  const totalCompanies = byType.reduce((s, r) => s + parseInt(r.companies), 0);

  return { region, totalSignals, totalCompanies, byType, topCompanies, bigNews, contactsAtSignalling: parseInt(networkStats?.contacts_at_signalling || 0) };
}

async function generateRegionSummary(data) {
  if (data.totalSignals === 0) return null;

  const system = `You are writing a weekly intelligence wrap for executive search consultants. Be concise, data-driven, commercially sharp. No filler. Australian English.

Return ONLY valid JSON:
{
  "headline": "One sharp sentence summarising the week in this region (max 15 words)",
  "key_numbers": [{"label": "...", "value": "...", "change": "up/down/flat"}],
  "big_moves": ["One sentence per major development (max 3)"],
  "watch_list": "One sentence on what to watch next week"
}`;

  const topCos = data.topCompanies.map(c => `${c.company_name} (${c.signal_count} signals: ${c.types.join(', ')}${c.is_client ? ' — CLIENT' : ''})`).join('\n');
  const news = data.bigNews.map(n => `${n.company_name}: ${n.evidence_summary?.slice(0, 100)} [${n.source_name}]`).join('\n');
  const types = data.byType.map(t => `${t.signal_type.replace(/_/g, ' ')}: ${t.cnt}`).join(', ');

  const user = `Weekly wrap for ${data.region.name}:

NUMBERS: ${data.totalSignals} signals across ${data.totalCompanies} companies. ${data.contactsAtSignalling} network contacts at signalling companies.

BY TYPE: ${types}

TOP COMPANIES:
${topCos}

BIGGEST NEWS:
${news}

Generate the weekly wrap JSON.`;

  const raw = await callClaude(system, user);
  try {
    const match = raw.match(/\{[\s\S]*\}/);
    return match ? JSON.parse(match[0]) : null;
  } catch (e) { return null; }
}

async function main() {
  LOG('📰', '═══ Weekly Wrap — Regional Intelligence Summary ═══');

  const weekOf = new Date().toISOString().slice(0, 10);

  // Global stats
  const { rows: [global] } = await pool.query(`
    SELECT COUNT(*) as signals, COUNT(DISTINCT company_id) as companies
    FROM signal_events WHERE detected_at > NOW() - INTERVAL '7 days' AND tenant_id = $1
      AND COALESCE(is_megacap, false) = false
  `, [ML_TENANT]);

  const wrap = {
    week_of: weekOf,
    global: { signals: parseInt(global.signals), companies: parseInt(global.companies) },
    regions: []
  };

  for (const region of REGIONS) {
    LOG('🌐', `Processing ${region.name}...`);
    const data = await getRegionData(region);
    if (data.totalSignals === 0) { LOG('⏭️', `  No signals — skipping`); continue; }

    LOG('📊', `  ${data.totalSignals} signals, ${data.totalCompanies} companies, ${data.contactsAtSignalling} contacts`);

    const summary = await generateRegionSummary(data);
    if (summary) {
      wrap.regions.push({
        code: region.code,
        name: region.name,
        signals: data.totalSignals,
        companies: data.totalCompanies,
        contacts_at_signalling: data.contactsAtSignalling,
        by_type: data.byType.map(t => ({ type: t.signal_type, count: parseInt(t.cnt) })),
        top_companies: data.topCompanies.map(c => ({ name: c.company_name, signals: parseInt(c.signal_count), is_client: c.is_client })),
        ...summary
      });
      LOG('📝', `  "${summary.headline}"`);
    }
    await new Promise(r => setTimeout(r, 1000));
  }

  // Save to signal_grabs as a special 'weekly_wrap' type
  await pool.query(`
    INSERT INTO signal_grabs (tenant_id, storyline, cluster_type, headline, observation, evidence, why_it_matters, so_what, watch_next, grab_score, status, digest_week, created_at)
    VALUES ($1, $2, 'weekly_wrap', $3, $4, $5, $6, $6, $7, 1.0, 'published', $8, NOW())
  `, [
    ML_TENANT,
    'Weekly Wrap — ' + weekOf,
    'Weekly Intelligence Wrap — ' + weekOf,
    JSON.stringify(wrap),
    JSON.stringify(wrap.regions.map(r => ({ source_name: r.name, source_url: '', context_note: r.headline }))),
    wrap.regions.map(r => r.watch_list).filter(Boolean).join(' '),
    wrap.regions[0]?.watch_list || '',
    weekOf
  ]);

  LOG('📰', `═══ Weekly Wrap Complete — ${wrap.regions.length} regions ═══`);
  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
