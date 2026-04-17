// ═══════════════════════════════════════════════════════════════════════════════
// routes/public.js — Public (unauthenticated) API routes
// 9 routes: /api/public/*, /api/waitlist, /api/health*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const router = express.Router();

module.exports = function({ platformPool }) {

  // ─── Public embed constants & middleware ───────────────────────────────────

  const PUBLIC_EMBED_TENANT = '00000000-0000-0000-0000-000000000001';
  const PUBLIC_EMBED_ALLOWED_ORIGINS = ['https://mitchellake.com', 'https://www.mitchellake.com', 'http://localhost:3000'];
  const PUBLIC_EMBED_STRIP_FIELDS = ['proximity_map', 'confidence_score', 'triage_status', 'claimed_by', 'send_to', 'best_connector_name', 'prox_connection_count'];

  const RESEARCH_SEARCH_ENABLED = process.env.RESEARCH_SEARCH_ENABLED !== 'false';

  // Simple in-memory rate limiter: 60 requests/min per IP
  const _publicEmbedRateMap = new Map();
  function publicEmbedRateLimit(req, res, next) {
    const ip = req.ip;
    const now = Date.now();
    const window = 60 * 1000;
    const max = 60;

    let entry = _publicEmbedRateMap.get(ip);
    if (!entry || now - entry.start > window) {
      entry = { start: now, count: 1 };
      _publicEmbedRateMap.set(ip, entry);
    } else {
      entry.count++;
    }

    if (entry.count > max) {
      return res.status(429).json({ error: 'Rate limit exceeded' });
    }
    next();
  }

  // CORS + Cache-Control middleware for public embed routes
  function publicEmbedCors(req, res, next) {
    const origin = req.headers.origin;
    if (PUBLIC_EMBED_ALLOWED_ORIGINS.includes(origin)) {
      res.header('Access-Control-Allow-Origin', origin);
    }
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.header('Cache-Control', 'public, max-age=300');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
  }

  // Strip sensitive fields from response objects
  function stripEmbedFields(obj) {
    if (Array.isArray(obj)) return obj.map(stripEmbedFields);
    if (obj && typeof obj === 'object') {
      const cleaned = { ...obj };
      for (const field of PUBLIC_EMBED_STRIP_FIELDS) delete cleaned[field];
      return cleaned;
    }
    return obj;
  }

  const publicEmbed = [publicEmbedRateLimit, publicEmbedCors];

  // ─── Platform stats cache ─────────────────────────────────────────────────

  let _platformStatsCache = null;
  let _platformStatsCacheTime = 0;

  // ═══════════════════════════════════════════════════════════════════════════════
  // 1. GET /api/public/stats
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/stats', async (req, res) => {
    try {
      // Cache for 5 minutes to avoid hammering DB on every page view
      if (_platformStatsCache && Date.now() - _platformStatsCacheTime < 5 * 60 * 1000) {
        return res.json(_platformStatsCache);
      }

      const tid = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
      const { rows: [s] } = await platformPool.query(`
        SELECT
          (SELECT COUNT(*) FROM people WHERE tenant_id = $1) as people,
          (SELECT COUNT(*) FROM companies WHERE tenant_id = $1 AND (sector IS NOT NULL OR is_client = true OR domain IS NOT NULL)) as companies,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '7 days') as signals_7d,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1)) as signals_total,
          (SELECT COUNT(*) FROM opportunities WHERE tenant_id = $1 AND status IN ('interviewing','sourcing','offer')) as active_searches,
          (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1) as placements,
          (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1) as documents,
          (SELECT COUNT(*) FROM rss_sources WHERE enabled = true) as sources,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '24 hours') as signals_24h,
          (SELECT COUNT(DISTINCT company_id) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '7 days') as companies_signalling,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) as interactions,
          (SELECT COUNT(*) FROM signal_grabs WHERE created_at > NOW() - INTERVAL '7 days') as grabs_7d,
          (SELECT COUNT(*) FROM tenants) as tenants,
          (SELECT COUNT(*) FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 7) as events_this_week,
          (SELECT COUNT(*) FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 30) as upcoming_events_30d,
          (SELECT COUNT(*) FROM event_sources WHERE tenant_id = $1 AND is_active = true) as active_event_sources
      `, [tid]);

      const stats = {
        people: parseInt(s.people),
        companies: parseInt(s.companies),
        signals_7d: parseInt(s.signals_7d),
        signals_total: parseInt(s.signals_total),
        signals_24h: parseInt(s.signals_24h),
        active_searches: parseInt(s.active_searches),
        placements: parseInt(s.placements),
        documents: parseInt(s.documents),
        sources: parseInt(s.sources),
        companies_signalling: parseInt(s.companies_signalling),
        interactions: parseInt(s.interactions),
        grabs_7d: parseInt(s.grabs_7d),
        tenants: parseInt(s.tenants),
        events_this_week: parseInt(s.events_this_week || 0),
        upcoming_events_30d: parseInt(s.upcoming_events_30d || 0),
        active_event_sources: parseInt(s.active_event_sources || 0),
        regions: ['AMER', 'EUR', 'MENA', 'ASIA', 'OCE'],
        research: RESEARCH_SEARCH_ENABLED ? {
          publications: 2067759,
          researchers: 939804,
          repositories: 58,
          year_range: '1898-2026',
          embedded_pct: 99.5,
        } : null,
        updated_at: new Date().toISOString()
      };

      _platformStatsCache = stats;
      _platformStatsCacheTime = Date.now();

      res.json(stats);
    } catch (err) {
      res.status(500).json({ error: 'Stats unavailable' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 2. POST /api/waitlist
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/waitlist', async (req, res) => {
    try {
      const { name, email, company } = req.body;
      if (!email) return res.status(400).json({ error: 'Email is required' });

      await platformPool.query(
        `INSERT INTO waitlist (name, email, company, created_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name, company = EXCLUDED.company, updated_at = NOW()`,
        [name || null, email.trim().toLowerCase(), company || null]
      );

      console.log(`[waitlist] ${email} registered interest${company ? ' (' + company + ')' : ''}`);
      res.json({ ok: true });
    } catch (err) {
      console.error('[waitlist] error:', err.message);
      res.status(500).json({ error: 'Could not register' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 3. GET /api/public/grabs
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/grabs', ...publicEmbed, async (req, res) => {
    try {
      const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 50);
      let where = `WHERE sg.tenant_id = $1 AND sg.status = 'published'`;
      const params = [PUBLIC_EMBED_TENANT];
      let idx = 1;

      if (req.query.cluster_type) { idx++; where += ` AND sg.cluster_type = $${idx}`; params.push(req.query.cluster_type); }
      if (req.query.geography) { idx++; where += ` AND $${idx} = ANY(sg.geographies)`; params.push(req.query.geography.toUpperCase()); }

      idx++; params.push(limit);

      const { rows } = await platformPool.query(`
        SELECT sg.id, sg.headline, sg.observation, sg.so_what, sg.watch_next,
               sg.evidence, sg.geographies, sg.themes, sg.signal_types,
               sg.cluster_type, sg.published_at, sg.grab_score,
               ed.image_url AS image_url
        FROM signal_grabs sg
        LEFT JOIN external_documents ed ON ed.id = (sg.document_ids[1])::uuid
        ${where}
        ORDER BY sg.published_at DESC NULLS LAST, sg.created_at DESC
        LIMIT $${idx}
      `, params);

      const countRes = await platformPool.query(`SELECT COUNT(*) FROM signal_grabs sg ${where}`, params.slice(0, -1));

      res.json({
        grabs: rows.map(r => stripEmbedFields({ ...r, image_url: r.image_url || null })),
        total: parseInt(countRes.rows[0].count),
        generated_at: new Date().toISOString()
      });
    } catch (err) { res.status(500).json({ error: 'Grabs unavailable' }); }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 4. GET /api/public/weekly
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/weekly', ...publicEmbed, async (req, res) => {
    try {
      const region = req.query.region ? req.query.region.toUpperCase() : null;
      const week = req.query.week || null;

      let where = `WHERE sg.tenant_id = $1 AND sg.cluster_type = 'weekly_wrap'`;
      const params = [PUBLIC_EMBED_TENANT];
      let idx = 1;

      if (week) { idx++; where += ` AND sg.digest_week = $${idx}`; params.push(week); }
      if (region) { idx++; where += ` AND $${idx} = ANY(sg.geographies)`; params.push(region); }

      const { rows } = await platformPool.query(`
        SELECT sg.id, sg.geographies, sg.headline, sg.observation,
               sg.so_what, sg.watch_next, sg.digest_week, sg.published_at, sg.created_at,
               ed.image_url AS image_url
        FROM signal_grabs sg
        LEFT JOIN external_documents ed ON ed.id = (sg.document_ids[1])::uuid
        ${where}
        ORDER BY sg.created_at DESC
        LIMIT 4
      `, params);

      // Parse observation JSON into structured weekly wrap fields
      const weekly = rows.map(r => {
        let parsed = {};
        try { parsed = JSON.parse(r.observation); } catch(e) {}
        return stripEmbedFields({
          id: r.id,
          region: (r.geographies || [])[0] || null,
          headline: parsed.headline || r.headline,
          key_numbers: parsed.key_numbers || [],
          big_moves: parsed.big_moves || [],
          watch_list: parsed.watch_list || r.watch_next || '',
          digest_week: r.digest_week || null,
          published_at: r.published_at || r.created_at,
          image_url: r.image_url || null
        });
      });

      res.json({ weekly, generated_at: new Date().toISOString() });
    } catch (err) { res.status(500).json({ error: 'Weekly wraps unavailable' }); }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 5. GET /api/public/hero
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/hero', ...publicEmbed, async (req, res) => {
    try {
      const { rows } = await platformPool.query(`
        SELECT se.id, se.signal_type, se.company_name, se.evidence_summary,
               se.detected_at, se.image_url, se.source_url,
               c.sector, c.geography,
               ed.image_url AS doc_image_url,
               ed.source_name AS doc_source_name, ed.source_url AS doc_source_url
        FROM signal_events se
        LEFT JOIN companies c ON c.id = se.company_id
        LEFT JOIN external_documents ed ON ed.id = se.source_document_id
        WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
          AND se.detected_at > NOW() - INTERVAL '7 days'
          AND COALESCE(se.is_megacap, false) = false
          AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
          AND se.company_name IS NOT NULL
        ORDER BY
          CASE WHEN c.is_client = true THEN 100 ELSE 0 END +
          (se.confidence_score * 30) +
          CASE WHEN se.image_url IS NOT NULL OR ed.image_url IS NOT NULL THEN 20 ELSE 0 END
          DESC
        LIMIT 3
      `, [PUBLIC_EMBED_TENANT]);

      const hero = rows.map(r => {
        const sources = [];
        if (r.doc_source_name || r.doc_source_url) {
          sources.push({ source_name: r.doc_source_name || null, source_url: r.doc_source_url || null });
        }
        return stripEmbedFields({
          id: r.id,
          company_name: r.company_name,
          signal_type: r.signal_type,
          headline: r.evidence_summary ? r.evidence_summary.slice(0, 120) : r.company_name + ' — ' + (r.signal_type || '').replace(/_/g, ' '),
          observation: r.evidence_summary || '',
          so_what: '',
          geography: r.geography || '',
          sector: r.sector || '',
          image_url: r.image_url || r.doc_image_url || null,
          sources,
          signal_date: r.detected_at,
          detected_at: r.detected_at
        });
      });

      res.json({ hero, generated_at: new Date().toISOString() });
    } catch (err) { res.status(500).json({ error: 'Hero signals unavailable' }); }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 6. GET /api/public/market-temperature
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/market-temperature', ...publicEmbed, async (req, res) => {
    try {
      const tid = PUBLIC_EMBED_TENANT;

      // Signal types by count
      const { rows: byType } = await platformPool.query(`
        SELECT se.signal_type, COUNT(*) as cnt
        FROM signal_events se
        WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
        GROUP BY se.signal_type ORDER BY cnt DESC
      `, [tid]);

      // Regional breakdown
      const { rows: byRegion } = await platformPool.query(`
        SELECT
          CASE
            WHEN c.country_code IN ('AU','NZ') OR c.geography ILIKE '%australia%' OR c.geography ILIKE '%oceania%' OR c.geography ILIKE '%new zealand%' THEN 'OCE'
            WHEN c.country_code IN ('SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN','TW') OR c.geography ILIKE '%singapore%' OR c.geography ILIKE '%asia%' OR c.geography ILIKE '%india%' THEN 'ASIA'
            WHEN c.country_code IN ('GB','UK','IE','DE','FR','NL','SE','DK','NO','FI','ES','IT') OR c.geography ILIKE '%united kingdom%' OR c.geography ILIKE '%europe%' OR c.geography ILIKE '%london%' THEN 'EUR'
            WHEN c.country_code IN ('US','CA','BR','MX') OR c.geography ILIKE '%united states%' OR c.geography ILIKE '%america%' OR c.geography ILIKE '%canada%' THEN 'AMER'
            ELSE 'OTHER'
          END AS region,
          se.signal_type, COUNT(*) as cnt
        FROM signal_events se
        LEFT JOIN companies c ON c.id = se.company_id
        WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
        GROUP BY region, se.signal_type
      `, [tid]);

      // Temperature calculation
      const growthTypes = ['capital_raising', 'product_launch', 'strategic_hiring', 'geographic_expansion', 'partnership'];
      const contractionTypes = ['restructuring', 'layoffs', 'ma_activity'];

      function calcTemp(types) {
        const growth = types.filter(t => growthTypes.includes(t.signal_type)).reduce((s, t) => s + parseInt(t.cnt), 0);
        const contraction = types.filter(t => contractionTypes.includes(t.signal_type)).reduce((s, t) => s + parseInt(t.cnt), 0);
        const total = growth + contraction;
        if (total === 0) return { temperature: 'neutral', signal_count: 0 };
        const ratio = growth / total;
        let temperature = 'neutral';
        if (ratio > 0.7) temperature = 'hot';
        else if (ratio > 0.55) temperature = 'warm';
        else if (ratio < 0.3) temperature = 'cold';
        else if (ratio < 0.45) temperature = 'cold';
        return { temperature, signal_count: total };
      }

      const overall = calcTemp(byType);
      const totalSignals = byType.reduce((s, t) => s + parseInt(t.cnt), 0);
      const dominant = byType.length > 0 ? byType[0].signal_type.replace(/_/g, ' ') : 'none';

      // Build region map
      const regions = {};
      for (const r of ['AMER', 'EUR', 'MENA', 'ASIA', 'OCE']) {
        const regionTypes = byRegion.filter(x => x.region === r);
        regions[r] = calcTemp(regionTypes);
        regions[r].signal_count = regionTypes.reduce((s, t) => s + parseInt(t.cnt), 0);
      }

      const labels = { hot: 'Expansion signals dominate — market is running hot', warm: 'Growth signals outpace contraction — cautiously positive', neutral: 'Balanced mix of growth and contraction signals', cold: 'Contraction signals dominate — defensive posture' };

      res.json(stripEmbedFields({
        temperature: overall.temperature,
        label: labels[overall.temperature] || labels.neutral,
        signal_count: totalSignals,
        dominant_type: dominant,
        regions,
        generated_at: new Date().toISOString()
      }));
    } catch (err) { res.status(500).json({ error: 'Market temperature unavailable' }); }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 7. GET /api/public/events
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/public/events', ...publicEmbed, async (req, res) => {
    res.set('Cache-Control', 'public, max-age=3600');
    try {
      const regions = ['AMER','EUR','MENA','ASIA','OCE'];
      const result = {};

      for (const region of regions) {
        const { rows } = await platformPool.query(`
          SELECT
            id, title AS name, description, event_date, city, country,
            region, theme, event_url AS external_url, relevance_score,
            signal_relevance, format, is_virtual
          FROM events
          WHERE tenant_id = $1
            AND region = $2
            AND event_date >= CURRENT_DATE
          ORDER BY relevance_score DESC, event_date ASC
          LIMIT 3
        `, [PUBLIC_EMBED_TENANT, region]);
        result[region] = rows.map(r => ({ ...r, image_url: null }));
      }

      // Also get events without region set
      const { rows: globalRows } = await platformPool.query(`
        SELECT id, title AS name, description, event_date, city, country,
               region, theme, event_url AS external_url, relevance_score
        FROM events
        WHERE tenant_id = $1
          AND region IS NULL
          AND event_date >= CURRENT_DATE
        ORDER BY relevance_score DESC, event_date ASC
        LIMIT 3
      `, [PUBLIC_EMBED_TENANT]);
      result['Global'] = globalRows.map(r => ({ ...r, image_url: null }));

      res.json({
        events: result,
        generated_at: new Date().toISOString()
      });
    } catch (err) {
      console.error('/api/public/events error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 8. GET /api/health
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 9. GET /api/health/pipelines
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/health/pipelines', async (req, res) => {
    try {
      const { rows } = await platformPool.query(`
        SELECT pipeline_name,
          COUNT(*) FILTER (WHERE started_at > NOW() - INTERVAL '24 hours') AS runs_24h,
          COUNT(*) FILTER (WHERE status = 'completed' AND started_at > NOW() - INTERVAL '24 hours') AS success_24h,
          COUNT(*) FILTER (WHERE status IN ('failed','partial') AND started_at > NOW() - INTERVAL '24 hours') AS failed_24h,
          MAX(completed_at) AS last_completed,
          AVG(duration_ms)::int FILTER (WHERE status = 'completed' AND started_at > NOW() - INTERVAL '7 days') AS avg_duration_ms,
          SUM(records_processed) FILTER (WHERE started_at > NOW() - INTERVAL '24 hours') AS records_24h
        FROM pipeline_runs WHERE started_at > NOW() - INTERVAL '7 days'
        GROUP BY pipeline_name ORDER BY pipeline_name
      `);
      var allHealthy = rows.every(function(r) { return parseInt(r.failed_24h) === 0; });
      res.status(allHealthy ? 200 : 206).json({ status: allHealthy ? 'healthy' : 'degraded', pipelines: rows, checked_at: new Date().toISOString() });
    } catch (err) {
      res.status(500).json({ status: 'error', error: err.message });
    }
  });

  return router;
};
