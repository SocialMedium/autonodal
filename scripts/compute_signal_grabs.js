#!/usr/bin/env node
/**
 * Signal Grabs — Editorial Intelligence Layer
 *
 * Clusters recent signals by theme/geography/company, scores them,
 * and generates short, sharp intelligence observations (Signal Grabs).
 *
 * Each grab: 90-160 words, 2+ source citations, evidence-backed.
 * Runs daily at 5 AM. Produces 3-5 grabs per run.
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const ML_TENANT = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1: CLUSTER DETECTION
// ═══════════════════════════════════════════════════════════════════════════════

async function detectClusters() {
  LOG('🔍', 'Detecting signal clusters from last 7 days...');

  // Geographic + thematic clusters (signals of same type in same region)
  const { rows: geoClusters } = await pool.query(`
    SELECT
      COALESCE(c.geography, 'Global') as geography,
      se.signal_type::text as signal_type,
      array_agg(DISTINCT se.id) as signal_ids,
      array_agg(DISTINCT se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as companies,
      array_agg(DISTINCT c.id) FILTER (WHERE c.id IS NOT NULL) as company_ids,
      array_agg(DISTINCT ed.source_name) FILTER (WHERE ed.source_name IS NOT NULL) as sources,
      array_agg(DISTINCT ed.source_url) FILTER (WHERE ed.source_url IS NOT NULL) as source_urls,
      array_agg(DISTINCT ed.title) FILTER (WHERE ed.title IS NOT NULL) as source_titles,
      array_agg(DISTINCT ed.id) FILTER (WHERE ed.id IS NOT NULL) as doc_ids,
      COUNT(DISTINCT ed.source_name) as source_count,
      COUNT(DISTINCT se.company_id) as company_count,
      COUNT(*) as signal_count,
      AVG(se.confidence_score) as avg_confidence,
      MAX(se.evidence_summary) as sample_evidence
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE se.detected_at > NOW() - INTERVAL '7 days'
      AND se.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false
      AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      AND se.company_name IS NOT NULL
    GROUP BY COALESCE(c.geography, 'Global'), se.signal_type::text
    HAVING COUNT(DISTINCT ed.source_name) >= 2
    ORDER BY COUNT(DISTINCT ed.source_name) DESC, AVG(se.confidence_score) DESC
    LIMIT 20
  `, [ML_TENANT]);

  // Cross-company thematic clusters (same signal type, different companies, any geo)
  const { rows: themeClusters } = await pool.query(`
    SELECT
      se.signal_type::text as signal_type,
      'Global' as geography,
      array_agg(DISTINCT se.id) as signal_ids,
      array_agg(DISTINCT se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as companies,
      array_agg(DISTINCT c.id) FILTER (WHERE c.id IS NOT NULL) as company_ids,
      array_agg(DISTINCT ed.source_name) FILTER (WHERE ed.source_name IS NOT NULL) as sources,
      array_agg(DISTINCT ed.source_url) FILTER (WHERE ed.source_url IS NOT NULL) as source_urls,
      array_agg(DISTINCT ed.title) FILTER (WHERE ed.title IS NOT NULL) as source_titles,
      array_agg(DISTINCT ed.id) FILTER (WHERE ed.id IS NOT NULL) as doc_ids,
      COUNT(DISTINCT ed.source_name) as source_count,
      COUNT(DISTINCT se.company_id) as company_count,
      COUNT(*) as signal_count,
      AVG(se.confidence_score) as avg_confidence,
      MAX(se.evidence_summary) as sample_evidence
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE se.detected_at > NOW() - INTERVAL '7 days'
      AND se.tenant_id = $1
      AND COALESCE(se.is_megacap, false) = false
      AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      AND se.company_name IS NOT NULL
    GROUP BY se.signal_type::text
    HAVING COUNT(DISTINCT se.company_id) >= 3 AND COUNT(DISTINCT ed.source_name) >= 2
    ORDER BY COUNT(DISTINCT se.company_id) DESC
    LIMIT 10
  `, [ML_TENANT]);

  // Merge and deduplicate
  const allClusters = [];
  const seen = new Set();

  for (const c of [...geoClusters, ...themeClusters]) {
    const key = `${c.geography}:${c.signal_type}`;
    if (seen.has(key)) continue;
    seen.add(key);
    allClusters.push({
      ...c,
      cluster_type: c.geography !== 'Global' ? 'regional' : (
        ['restructuring', 'layoffs'].includes(c.signal_type) ? 'talent' :
        ['capital_raising', 'geographic_expansion'].includes(c.signal_type) ? 'sector' : 'macro'
      )
    });
  }

  LOG('📊', `Found ${allClusters.length} clusters (${geoClusters.length} geo, ${themeClusters.length} theme)`);
  return allClusters;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2: SCORING
// ═══════════════════════════════════════════════════════════════════════════════

async function scoreClusters(clusters) {
  LOG('📈', 'Scoring clusters...');

  // Get tenant focus for strategic relevance
  const { rows: [tenant] } = await pool.query('SELECT focus_geographies, focus_sectors FROM tenants WHERE id = $1', [ML_TENANT]);
  const focusGeos = (tenant?.focus_geographies || []).map(g => g.toLowerCase());
  const focusSectors = (tenant?.focus_sectors || []).map(s => s.toLowerCase());

  // Get recent grabs for novelty check
  const { rows: recentGrabs } = await pool.query(
    "SELECT storyline, signal_types FROM signal_grabs WHERE tenant_id = $1 AND created_at > NOW() - INTERVAL '7 days'",
    [ML_TENANT]
  );

  // Get network data for overlap scoring
  const { rows: adjacency } = await pool.query(
    'SELECT company_id, adjacency_score, is_client FROM company_adjacency_scores WHERE adjacency_score > 10 LIMIT 500'
  );
  const adjMap = new Map(adjacency.map(a => [a.company_id, a]));

  return clusters.map(cluster => {
    // Convergence: how many independent sources
    const convergence = Math.min(1, parseInt(cluster.source_count) / 5);

    // Strategic relevance: geo + sector alignment
    const geoMatch = focusGeos.some(g => (cluster.geography || '').toLowerCase().includes(g)) ? 0.8 : 0.3;
    const strategic = geoMatch;

    // Network overlap: do we know people at these companies
    const companyIds = cluster.company_ids || [];
    const networkHits = companyIds.filter(id => adjMap.has(id)).length;
    const clientHits = companyIds.filter(id => adjMap.get(id)?.is_client).length;
    const networkScore = Math.min(1, (networkHits * 0.3 + clientHits * 0.5));

    // Novelty: is this different from recent grabs
    const isDuplicate = recentGrabs.some(rg =>
      rg.signal_types?.includes(cluster.signal_type) &&
      rg.storyline?.toLowerCase().includes(cluster.signal_type.replace(/_/g, ' '))
    );
    const noveltyScore = isDuplicate ? 0.2 : 0.8;

    // Source quality: average confidence as proxy
    const quality = parseFloat(cluster.avg_confidence) || 0.5;

    const grabScore = (convergence * 0.30) + (strategic * 0.25) + (networkScore * 0.20) + (noveltyScore * 0.15) + (quality * 0.10);

    return {
      ...cluster,
      grab_score: Math.round(grabScore * 100) / 100,
      convergence_score: convergence,
      strategic_relevance: strategic,
      network_overlap: networkScore,
      novelty: noveltyScore,
      source_quality: quality
    };
  }).sort((a, b) => b.grab_score - a.grab_score);
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3: EDITORIAL GENERATION
// ═══════════════════════════════════════════════════════════════════════════════

async function callClaude(system, user) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-sonnet-4-20250514', max_tokens: 1024,
      system, messages: [{ role: 'user', content: user }]
    });
    const req = https.request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) },
      timeout: 60000
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const d = JSON.parse(Buffer.concat(chunks).toString());
          if (d.error) return reject(new Error(d.error.message));
          resolve(d.content?.[0]?.text || '');
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body); req.end();
  });
}

async function generateGrab(cluster) {
  // Build source evidence for the prompt
  const sources = [];
  const srcNames = cluster.sources || [];
  const srcUrls = cluster.source_urls || [];
  const srcTitles = cluster.source_titles || [];
  for (let i = 0; i < Math.min(srcNames.length, 6); i++) {
    sources.push({
      name: srcNames[i] || 'Source',
      url: srcUrls[i] || '',
      title: srcTitles[i] || ''
    });
  }

  const companies = (cluster.companies || []).filter(Boolean).slice(0, 8);
  const signalLabel = (cluster.signal_type || '').replace(/_/g, ' ');
  const geo = cluster.geography || 'Global';

  const system = `You are an editorial intelligence analyst producing Signal Grabs — short, sharp, evidence-backed intelligence observations.

RULES:
- Total length: 90-160 words MAXIMUM. Count carefully.
- Do NOT write a blog post or thought leadership.
- Do NOT restate article headlines.
- Do NOT summarize a single article.
- ONLY produce insights when multiple sources converge on a pattern.
- Every source citation must use exact format: <a href="URL">Source Name</a>
- Be analytical, commercially intelligent, opinionated but credible.
- Prefer interpretation over summarization.

Return ONLY valid JSON with these exact keys:
{
  "headline": "Sharp 5-10 word observation",
  "observation": "1-2 sentences describing the emerging pattern",
  "evidence": [{"source_name": "...", "source_url": "...", "context_note": "why this source matters"}],
  "why_it_matters": "Short interpretation of commercial implications (2-3 sentences max)",
  "watch_next": "What would confirm or invalidate this trend (1 sentence, optional)"
}`;

  const user = `Generate a Signal Grab for this cluster:

SIGNAL TYPE: ${signalLabel}
GEOGRAPHY: ${geo}
COMPANIES: ${companies.join(', ')}
COMPANY COUNT: ${cluster.company_count} companies showing this signal
SOURCE COUNT: ${cluster.source_count} independent sources

SOURCES:
${sources.map(s => `- ${s.name}: "${s.title}" (${s.url})`).join('\n')}

SAMPLE EVIDENCE:
${(cluster.sample_evidence || '').slice(0, 500)}

CONTEXT: This cluster represents ${cluster.signal_count} signals detected in the last 7 days across ${cluster.company_count} companies, reported by ${cluster.source_count} independent sources in the ${geo} region.

Generate the Signal Grab JSON. Stay under 160 words total.`;

  const raw = await callClaude(system, user);

  try {
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON');
    return JSON.parse(jsonMatch[0]);
  } catch (e) {
    LOG('⚠️', `  JSON parse failed: ${e.message}`);
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 4: SAVE & ORCHESTRATE
// ═══════════════════════════════════════════════════════════════════════════════

async function saveGrab(cluster, grab) {
  const { rows: [saved] } = await pool.query(`
    INSERT INTO signal_grabs (
      tenant_id, storyline, cluster_type,
      headline, observation, evidence, why_it_matters, watch_next,
      grab_score, convergence_score, strategic_relevance, network_overlap, novelty, source_quality,
      signal_ids, document_ids, company_ids, geographies, themes, signal_types,
      status
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 'draft')
    RETURNING id
  `, [
    ML_TENANT,
    `${(cluster.signal_type || '').replace(/_/g, ' ')} in ${cluster.geography || 'global markets'}`,
    cluster.cluster_type,
    grab.headline,
    grab.observation,
    JSON.stringify(grab.evidence || []),
    grab.why_it_matters,
    grab.watch_next || null,
    cluster.grab_score, cluster.convergence_score, cluster.strategic_relevance,
    cluster.network_overlap, cluster.novelty, cluster.source_quality,
    cluster.signal_ids || [],
    cluster.doc_ids || [],
    cluster.company_ids || [],
    [cluster.geography].filter(Boolean),
    [(cluster.signal_type || '').replace(/_/g, ' ')],
    [cluster.signal_type]
  ]);
  return saved.id;
}

async function run() {
  LOG('🔺', '═══ Signal Grabs — Editorial Intelligence ═══');

  // Detect clusters
  const clusters = await detectClusters();
  if (!clusters.length) {
    LOG('ℹ️', 'No qualifying clusters found. Need ≥2 independent sources per cluster.');
    await pool.end();
    return;
  }

  // Score clusters
  const scored = await scoreClusters(clusters);
  LOG('📊', `Scored ${scored.length} clusters. Top: ${scored[0]?.signal_type} in ${scored[0]?.geography} (score: ${scored[0]?.grab_score})`);

  // Generate top 5 grabs
  const TARGET_GRABS = 5;
  const MIN_SCORE = 0.25;
  const qualifying = scored.filter(c => c.grab_score >= MIN_SCORE);
  LOG('✅', `${qualifying.length} clusters qualify (score >= ${MIN_SCORE})`);

  let generated = 0;
  for (const cluster of qualifying.slice(0, TARGET_GRABS)) {
    try {
      LOG('✍️', `  Generating: ${cluster.signal_type} in ${cluster.geography} (${cluster.company_count} companies, ${cluster.source_count} sources, score: ${cluster.grab_score})`);

      const grab = await generateGrab(cluster);
      if (!grab || !grab.headline) {
        LOG('⚠️', `  Generation failed — skipping`);
        continue;
      }

      const id = await saveGrab(cluster, grab);
      generated++;
      LOG('📝', `  "${grab.headline}" → ${id}`);

      await new Promise(r => setTimeout(r, 1000)); // Rate limit
    } catch (e) {
      LOG('❌', `  Error: ${e.message}`);
    }
  }

  LOG('🔺', `═══ Complete: ${generated} Signal Grabs generated ═══`);
  await pool.end();
}

run().catch(e => { console.error('Fatal:', e); process.exit(1); });
