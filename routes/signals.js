// ═══════════════════════════════════════════════════════════════════════════════
// routes/signals.js — Signal intelligence API routes
// 17 routes: /api/signals/*, /api/signal-index/*, /api/market-temperature,
//            /api/talent-in-motion, /api/converging-themes, /api/top-podcasts,
//            /api/reengage-windows
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const https = require('https');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, cachedResponse, setCachedResponse, generateQueryEmbedding, qdrantSearch, REGION_MAP, REGION_CODES, verifyHuddleMember }) {

// Cached transition matrix from signal_pattern_weights (loaded once, refreshed on null)
var _transitionCache = null;

router.get('/api/signals/gdelt', authenticateToken, async (req, res) => {
  try {
    var db = new TenantDB(req.tenant_id);
    var signalType = req.query.signal_type;
    var language = req.query.language;
    var days = parseInt(req.query.days) || 1;
    var nonEnglish = req.query.non_english === 'true';
    var limit = Math.min(parseInt(req.query.limit) || 50, 200);

    var where = "se.detected_at > NOW() - INTERVAL '" + days + " days' AND ed.source_type = 'gdelt'";
    var params = [];
    var idx = 0;

    if (signalType) { idx++; where += ' AND se.signal_type = $' + idx; params.push(signalType); }
    if (language) { idx++; where += ' AND ed.source_language = $' + idx; params.push(language); }
    if (nonEnglish) { where += " AND ed.source_language != 'en' AND ed.source_language != 'English'"; }

    idx++; params.push(limit);

    var { rows } = await db.query(`
      SELECT se.signal_type, se.company_name, se.confidence_score,
             se.evidence_summary, se.source_url, se.detected_at,
             ed.title, ed.source_name AS source_domain,
             ed.source_language, ed.gdelt_tone
      FROM signal_events se
      LEFT JOIN external_documents ed ON ed.source_url = se.source_url
      WHERE ${where}
      ORDER BY se.detected_at DESC
      LIMIT $${idx}
    `, params);

    res.json({ signals: rows, count: rows.length, days: days });
  } catch (err) {
    console.error('GDELT signals error:', err.message);
    res.status(500).json({ error: 'Failed to fetch GDELT signals' });
  }
});

router.get('/api/signals/gdelt/languages', authenticateToken, async (req, res) => {
  try {
    var db = new TenantDB(req.tenant_id);
    var days = parseInt(req.query.days) || 7;
    var { rows } = await db.query(`
      SELECT ed.source_language AS language,
             COUNT(DISTINCT ed.id) AS doc_count,
             COUNT(DISTINCT se.id) AS signal_count,
             ARRAY_AGG(DISTINCT se.signal_type) FILTER (WHERE se.signal_type IS NOT NULL) AS signal_types
      FROM external_documents ed
      LEFT JOIN signal_events se ON se.source_url = ed.source_url
      WHERE ed.source_type = 'gdelt'
        AND ed.created_at > NOW() - INTERVAL '${days} days'
      GROUP BY ed.source_language
      ORDER BY doc_count DESC
    `);
    res.json({ languages: rows, days: days });
  } catch (err) {
    console.error('GDELT languages error:', err.message);
    res.status(500).json({ error: 'Failed to fetch language breakdown' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// HERO SIGNALS — top 3 signals ranked by client proximity + network + confidence
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/signals/hero', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const typeFilter = req.query.type && req.query.type !== 'all' ? req.query.type : null;
    const userId = req.user.user_id;
    const params = [req.tenant_id, userId];
    let typeClause = '';
    if (typeFilter) { params.push(typeFilter); typeClause = ` AND se.signal_type = $${params.length}`; }
    // Signal brief — personalised hero ranking
    // Score weights:
    //   client (100) — billing relationship, highest priority
    //   warmth (60)  — recent interaction depth: 10+ interactions in 90d = max
    //   user proximity (80) — user's own strongest connection at the company
    //   team proximity (40) — team's connections scaled by count
    //   contacts (25) — raw headcount at company (diminishing)
    //   confidence (30) — signal quality
    //   image (20) — visual weight for hero display
    //
    // This means Mark having 10 interactions with a founder at a company
    // scores higher than Sophie having 15 sourced candidates with no interactions.
    const { rows } = await db.query(`
      SELECT * FROM (
        SELECT DISTINCT ON (LOWER(se.company_name))
          se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
          se.evidence_summary, se.detected_at, se.source_url, se.image_url,
          c.sector, c.geography,
          COALESCE(c.is_client, false) AS is_client,
          c.domain,
          ed.title AS doc_title, ed.source_name, ed.image_url AS doc_image_url, ed.audio_url AS doc_audio_url, ed.source_type AS doc_source_type,
          COALESCE(pc.cnt, 0) AS contact_count,
          COALESCE(px.cnt, 0) AS prox_count,
          COALESCE(my_px.strength, 0) AS my_proximity,
          COALESCE(warmth.interaction_count, 0) AS warmth_interactions,
          COALESCE(warmth.recency_days, 999) AS warmth_recency_days,
          CASE WHEN c.is_client = true THEN 100 ELSE 0 END +
          LEAST(COALESCE(warmth.warmth_score, 0), 60) +
          LEAST(COALESCE(my_px.strength, 0) * 100, 80) +
          LEAST(COALESCE(px.cnt, 0) * 10, 40) +
          LEAST(COALESCE(pc.cnt, 0) * 5, 25) +
          (se.confidence_score * 30) +
          CASE WHEN se.image_url IS NOT NULL OR ed.image_url IS NOT NULL THEN 20 ELSE 0 END
          AS hero_score
        FROM signal_events se
        LEFT JOIN companies c ON c.id = se.company_id
        LEFT JOIN external_documents ed ON ed.id = se.source_document_id
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM people p WHERE p.current_company_id = se.company_id AND p.tenant_id = $1
        ) pc ON true
        LEFT JOIN LATERAL (
          SELECT COUNT(DISTINCT tp2.person_id) AS cnt FROM team_proximity tp2
          JOIN people p2 ON p2.id = tp2.person_id AND p2.tenant_id = $1
          WHERE tp2.tenant_id = $1 AND tp2.relationship_strength >= 0.25 AND p2.current_company_id = se.company_id
        ) px ON true
        LEFT JOIN LATERAL (
          SELECT MAX(tp3.relationship_strength) AS strength FROM team_proximity tp3
          JOIN people p3 ON p3.id = tp3.person_id AND p3.tenant_id = $1
          WHERE tp3.team_member_id = $2 AND tp3.tenant_id = $1
            AND tp3.relationship_strength >= 0.2 AND p3.current_company_id = se.company_id
        ) my_px ON true
        LEFT JOIN LATERAL (
          SELECT
            COUNT(*) AS interaction_count,
            EXTRACT(DAY FROM NOW() - MAX(i.interaction_at)) AS recency_days,
            LEAST(COUNT(*) * 6, 40) +
            CASE
              WHEN MAX(i.interaction_at) > NOW() - INTERVAL '7 days' THEN 20
              WHEN MAX(i.interaction_at) > NOW() - INTERVAL '30 days' THEN 12
              WHEN MAX(i.interaction_at) > NOW() - INTERVAL '90 days' THEN 5
              ELSE 0
            END AS warmth_score
          FROM interactions i
          JOIN people p4 ON p4.id = i.person_id AND p4.current_company_id = se.company_id
          WHERE i.user_id = $2 AND i.tenant_id = $1
            AND i.interaction_at > NOW() - INTERVAL '180 days'
        ) warmth ON true
        WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
          AND se.detected_at > NOW() - INTERVAL '7 days'
          AND se.signal_date IS NOT NULL AND se.signal_date > NOW() - INTERVAL '30 days'
          AND COALESCE(se.is_megacap, false) = false
          AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
          AND se.company_name IS NOT NULL
          AND se.company_name NOT ILIKE '%mitchellake%' AND se.company_name NOT ILIKE '%mitchel lake%'
          ${typeClause}
        ORDER BY LOWER(se.company_name), se.confidence_score DESC, se.detected_at DESC
      ) deduped
      ORDER BY hero_score DESC
      LIMIT 5
    `, params);

    // Use doc_image_url as fallback
    rows.forEach(r => { if (!r.image_url && r.doc_image_url) r.image_url = r.doc_image_url; });

    res.json({ heroes: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// MARKET TEMPERATURE — macro summary from megacap/public company signals
// ═══════════════════════════════════════════════════════════════════════════════

// ── Signal Index — Market Health Ticker ──────────────────────────────
router.get('/api/signal-index', authenticateToken, async (req, res) => {
  try {
    const horizon = req.query.horizon || '7d';
    const cacheKey = '/api/signal-index?h=' + horizon;
    const cached = cachedResponse(req.tenant_id, cacheKey);
    if (cached) return res.json(cached);
    const db = new TenantDB(req.tenant_id);
    const tid = req.tenant_id;

    // Market health is platform-wide (derived from market signals, not tenant data)
    // Fall back to ML tenant data if no platform-wide data exists
    const mlTid = '00000000-0000-0000-0000-000000000001';
    const [mh, stocks, stats] = await Promise.all([
      platformPool.query(`SELECT * FROM market_health_index WHERE (tenant_id IS NULL OR tenant_id = $1 OR tenant_id = $2) AND horizon = $3 ORDER BY computed_at DESC LIMIT 1`, [tid, mlTid, horizon]).catch(() => ({ rows: [] })),
      platformPool.query(`SELECT * FROM signal_stocks WHERE (tenant_id IS NULL OR tenant_id = $1 OR tenant_id = $2) AND horizon = $3 ORDER BY weight DESC`, [tid, mlTid, horizon]).catch(() => ({ rows: [] })),
      platformPool.query(`SELECT * FROM signal_index_stats WHERE (tenant_id IS NULL OR tenant_id = $1 OR tenant_id = $2) ORDER BY computed_at DESC LIMIT 1`, [tid, mlTid]).catch(() => ({ rows: [] })),
    ]);

    const signalStocks = {};
    for (const s of stocks.rows) {
      signalStocks[s.stock_name] = {
        sentiment: s.sentiment, weight: s.weight, delta: s.delta,
        direction: s.direction, score: s.score,
        current_count: s.current_count, prior_count: s.prior_count
      };
    }

    const response = {
      horizon,
      market_health: mh.rows[0] || { score: 50, delta: 0, direction: 'flat' },
      signal_stocks: signalStocks,
      stats: stats.rows[0] || { people_tracked: 0, companies_tracked: 0, signals_7d: 0, signals_30d: 0 },
      computed_at: mh.rows[0]?.computed_at || null
    };
    setCachedResponse(req.tenant_id, cacheKey, response);
    res.json(response);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/signal-index/sectors', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const horizon = req.query.horizon || '7d';
    const { rows } = await db.query(
      `SELECT * FROM sector_indices WHERE tenant_id = $1 AND horizon = $2 ORDER BY score DESC`,
      [req.tenant_id, horizon]
    ).catch(() => ({ rows: [] }));

    const sectors = {};
    for (const r of rows) {
      sectors[r.sector] = { score: r.score, delta: r.delta, direction: r.direction, signal_count: r.signal_count, company_count: r.company_count };
    }
    res.json({ horizon, sectors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/signal-index/history', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const horizon = req.query.horizon || '7d';
    const limit = Math.min(parseInt(req.query.limit) || 90, 365);
    const { rows } = await db.query(
      `SELECT score, delta, snapshot_at FROM market_health_history WHERE tenant_id = $1 AND horizon = $2 ORDER BY snapshot_at DESC LIMIT $3`,
      [req.tenant_id, horizon, limit]
    ).catch(() => ({ rows: [] }));
    res.json({ horizon, history: rows.reverse() });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Media Sentiment breakdown
router.get('/api/signal-index/sentiment', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const days = parseInt(req.query.days) || 7;
    const { rows } = await db.query(`
      SELECT ds.sentiment, ds.confidence, ds.themes, ds.summary,
             ed.title, ed.source_name, ed.source_type, ed.published_at
      FROM document_sentiment ds
      JOIN external_documents ed ON ed.id = ds.document_id
      WHERE ds.computed_at > NOW() - ($1 || ' days')::INTERVAL
      ORDER BY ed.published_at DESC
      LIMIT 50
    `, [days]);

    const totals = { bullish: 0, bearish: 0, neutral: 0 };
    const bySource = {};
    for (const r of rows) {
      totals[r.sentiment] = (totals[r.sentiment] || 0) + 1;
      const src = r.source_type || 'other';
      if (!bySource[src]) bySource[src] = { bullish: 0, bearish: 0, neutral: 0 };
      bySource[src][r.sentiment]++;
    }

    res.json({
      days,
      total: rows.length,
      totals,
      by_source: bySource,
      recent: rows.slice(0, 20).map(r => ({
        title: r.title, source: r.source_name, type: r.source_type,
        sentiment: r.sentiment, confidence: r.confidence,
        themes: r.themes, summary: r.summary, published_at: r.published_at
      }))
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/market-temperature', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Aggregate megacap signals by type for the last 7 days
    const { rows: byType } = await db.query(`
      SELECT se.signal_type, COUNT(*) as cnt,
             array_agg(DISTINCT se.company_name ORDER BY se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as companies
      FROM signal_events se
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      GROUP BY se.signal_type ORDER BY cnt DESC
    `, [req.tenant_id]);

    // Headline signals — the most notable recent megacap moves
    const { rows: headlines } = await db.query(`
      SELECT se.company_name, se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score
      FROM signal_events se
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      ORDER BY se.confidence_score DESC, se.detected_at DESC
      LIMIT 8
    `, [req.tenant_id]);

    // Sentiment indicators
    const growth = byType.filter(t => ['capital_raising', 'product_launch', 'strategic_hiring', 'geographic_expansion', 'partnership'].includes(t.signal_type)).reduce((sum, t) => sum + parseInt(t.cnt), 0);
    const contraction = byType.filter(t => ['restructuring', 'layoffs', 'ma_activity'].includes(t.signal_type)).reduce((sum, t) => sum + parseInt(t.cnt), 0);
    const total = growth + contraction;

    let temperature = 'neutral';
    let emoji = '';
    if (total > 0) {
      const ratio = growth / total;
      if (ratio > 0.7) { temperature = 'hot'; }
      else if (ratio > 0.55) { temperature = 'warm'; }
      else if (ratio < 0.3) { temperature = 'cold'; }
      else if (ratio < 0.45) { temperature = 'cooling'; }
    }

    // Build narrative summary via simple template
    const typeLabels = { capital_raising: 'raising capital', product_launch: 'launching products', strategic_hiring: 'hiring aggressively', restructuring: 'restructuring', layoffs: 'cutting headcount', ma_activity: 'doing deals', geographic_expansion: 'expanding geographically', partnership: 'forming partnerships', leadership_change: 'changing leadership' };
    const topMoves = byType.slice(0, 3).map(t => {
      const cos = (t.companies || []).slice(0, 3).join(', ');
      return `${t.cnt} ${typeLabels[t.signal_type] || t.signal_type.replace(/_/g, ' ')} signals (${cos})`;
    });

    const summary = total === 0
      ? 'No significant macro signals this week.'
      : `Market is ${temperature}. ${total} signals from major public companies this week: ${topMoves.join('; ')}.${contraction > 0 ? ' ' + contraction + ' contraction signals may release senior talent downstream.' : ''}`;

    res.json({
      temperature,
      emoji,
      growth_signals: growth,
      contraction_signals: contraction,
      total_signals: total,
      summary,
      by_type: byType,
      headlines: headlines.map(h => ({
        company: h.company_name,
        type: h.signal_type,
        summary: (h.evidence_summary || '').slice(0, 150),
        date: h.detected_at,
        confidence: h.confidence_score
      }))
    });
  } catch (err) {
    console.error('Market temperature error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TALENT IN MOTION — flight risk, activity spikes, re-engagement windows
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/talent-in-motion', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 10, 30);

    // 1. People at companies with restructuring/layoff signals (flight risk)
    const { rows: flightRisk } = await db.query(`
      SELECT DISTINCT ON (p.id)
        p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
        p.seniority_level, p.linkedin_url,
        se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score,
        ps.flight_risk_score, ps.timing_score,
        (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id AND p2.tenant_id = $2) as colleagues_affected,
        (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id AND p2.tenant_id = $2
         AND p2.seniority_level IN ('c_suite','vp','director')) as senior_affected,
        (SELECT COUNT(*) FROM pipeline_contacts sc JOIN opportunities s ON s.id = sc.search_id AND s.status IN ('sourcing','interviewing') AND s.tenant_id = $2
         WHERE sc.person_id = p.id AND sc.tenant_id = $2) as active_search_matches
      FROM people p
      JOIN companies c ON c.id = p.current_company_id AND c.tenant_id = $2
      JOIN signal_events se ON se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $2)
        AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change', 'strategic_hiring')
        AND se.detected_at > NOW() - INTERVAL '30 days'
        AND COALESCE(se.is_megacap, false) = false
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE p.current_title IS NOT NULL
        AND p.tenant_id = $2
      ORDER BY p.id, se.detected_at DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    // 2. People with high activity / timing scores (activity spikes & re-engage)
    const { rows: activeProfiles } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
             p.seniority_level, p.linkedin_url,
             ps.activity_score, ps.timing_score, ps.receptivity_score, ps.flight_risk_score,
             ps.engagement_score, ps.activity_trend, ps.engagement_trend,
             ps.last_interaction_at, ps.interaction_count_30d, ps.external_signals_30d,
             (SELECT COUNT(*) FROM pipeline_contacts sc JOIN opportunities s ON s.id = sc.search_id AND s.status IN ('sourcing','interviewing') AND s.tenant_id = $2
              WHERE sc.person_id = p.id AND sc.tenant_id = $2) as active_search_matches
      FROM people p
      JOIN person_scores ps ON ps.person_id = p.id
      WHERE (ps.timing_score > 0.4 OR ps.activity_score > 0.4 OR ps.receptivity_score > 0.5 OR ps.flight_risk_score > 0.4)
        AND p.current_title IS NOT NULL
        AND p.tenant_id = $2
      ORDER BY (COALESCE(ps.timing_score,0) + COALESCE(ps.activity_score,0) + COALESCE(ps.receptivity_score,0)) DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    // 3. Recent person signals (flight_risk_alert, activity_spike, timing_opportunity)
    const { rows: personSignals } = await db.query(`
      SELECT psg.id, psg.signal_type, psg.title, psg.description, psg.confidence_score, psg.detected_at,
             p.id as person_id, p.full_name, p.current_title, p.current_company_name, p.seniority_level
      FROM person_signals psg
      JOIN people p ON p.id = psg.person_id
      WHERE psg.signal_type IN ('flight_risk_alert', 'activity_spike', 'timing_opportunity', 'new_role', 'company_exit')
        AND psg.detected_at > NOW() - INTERVAL '14 days'
        AND psg.tenant_id = $2
      ORDER BY psg.detected_at DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    res.json({ flight_risk: flightRisk, active_profiles: activeProfiles, person_signals: personSignals });
  } catch (err) {
    console.error('Talent in motion error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERGING THEMES — triangulated signal patterns
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/converging-themes', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Find signal_type clusters with high activity, cross-reference with clients and candidates
    const { rows: themes } = await db.query(`
      WITH candidate_counts AS (
        SELECT se2.signal_type, COUNT(DISTINCT p.id) as cnt
        FROM people p
        JOIN companies c2 ON c2.id = p.current_company_id
        JOIN signal_events se2 ON se2.company_id = c2.id AND se2.detected_at > NOW() - INTERVAL '30 days'
        WHERE p.current_title IS NOT NULL AND p.tenant_id = $1
        GROUP BY se2.signal_type
      )
      SELECT
        se.signal_type,
        COUNT(DISTINCT se.company_id) as company_count,
        COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) as client_count,
        COUNT(*) as signal_count,
        ROUND(AVG(se.confidence_score)::numeric, 2) as avg_confidence,
        COALESCE(cc.cnt, 0) as candidate_count,
        array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names,
        array_agg(DISTINCT se.company_name ORDER BY se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as company_names
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      LEFT JOIN candidate_counts cc ON cc.signal_type = se.signal_type
      WHERE se.detected_at > NOW() - INTERVAL '30 days'
        AND se.signal_type IS NOT NULL
        AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      GROUP BY se.signal_type, cc.cnt
      HAVING COUNT(DISTINCT se.company_id) >= 3
      ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) DESC,
               COUNT(DISTINCT se.company_id) DESC
      LIMIT 5
    `, [req.tenant_id]);

    // Find sector-based convergences
    const { rows: sectorThemes } = await db.query(`
      SELECT
        c.sector,
        COUNT(DISTINCT se.company_id) as company_count,
        COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) as client_count,
        COUNT(*) as signal_count,
        array_agg(DISTINCT se.signal_type) as signal_types,
        (SELECT COUNT(DISTINCT p.id) FROM people p WHERE p.tenant_id = $1 AND p.current_company_id IN (
          SELECT DISTINCT se2.company_id FROM signal_events se2
          JOIN companies c2 ON c2.id = se2.company_id AND c2.sector = c.sector AND c2.tenant_id = $1
          WHERE se2.detected_at > NOW() - INTERVAL '30 days' AND se2.tenant_id = $1
        )) as candidate_count,
        array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names
      FROM signal_events se
      JOIN companies c ON c.id = se.company_id AND c.sector IS NOT NULL AND c.tenant_id = $1
      WHERE se.detected_at > NOW() - INTERVAL '30 days'
        AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      GROUP BY c.sector
      HAVING COUNT(DISTINCT se.company_id) >= 3 AND COUNT(*) >= 5
      ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) DESC,
               COUNT(*) DESC
      LIMIT 5
    `, [req.tenant_id]);

    // Placement pipeline potential — searches with matching signals
    let pipeline = [];
    try {
      const { rows } = await db.query(`
        SELECT s.title as search_title, s.status, a.name as client_name,
               COUNT(DISTINCT se.id) as matching_signals,
               COUNT(DISTINCT se.company_id) as signalling_companies
        FROM searches s
        JOIN search_candidates sc ON sc.search_id = s.id
        JOIN people p ON p.id = sc.person_id
        JOIN signal_events se ON se.company_id = p.current_company_id AND se.detected_at > NOW() - INTERVAL '30 days'
        LEFT JOIN accounts a ON a.id = s.project_id
        WHERE s.status IN ('sourcing', 'interviewing')
          AND s.tenant_id = $1
        GROUP BY s.id, s.title, s.status, a.name
        HAVING COUNT(DISTINCT se.id) >= 2
        ORDER BY COUNT(DISTINCT se.id) DESC
        LIMIT 5
      `, [req.tenant_id]);
      pipeline = rows;
    } catch (e) {
      console.warn('Converging themes pipeline query failed:', e.message);
    }

    res.json({ signal_themes: themes, sector_themes: sectorThemes, pipeline });
  } catch (err) {
    console.error('Converging themes error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TOP PODCASTS — matched to trending signal themes via semantic search
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/top-podcasts', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // 1. Get trending signal themes for deep-dive matching
    const { rows: trending } = await db.query(`
      SELECT signal_type, COUNT(*) as cnt
      FROM signal_events
      WHERE detected_at > NOW() - INTERVAL '7 days' AND signal_type IS NOT NULL
      GROUP BY signal_type ORDER BY cnt DESC LIMIT 3
    `);

    const themeLabels = {
      capital_raising: 'fundraising venture capital IPO Series funding round',
      ma_activity: 'acquisition merger deal M&A takeover corporate development',
      product_launch: 'product launch innovation new release go to market',
      strategic_hiring: 'hiring talent recruitment executive search team building',
      geographic_expansion: 'expansion international new market global growth',
      restructuring: 'restructuring transformation turnaround change management',
      leadership_change: 'CEO appointment executive leadership transition succession',
      partnership: 'partnership alliance collaboration strategic deal ecosystem',
      layoffs: 'layoffs downsizing workforce reduction cost cutting'
    };
    const themeNames = trending.map(t => (t.signal_type || '').replace(/_/g, ' '));

    // ── LATEST: most recent podcast episodes (last 7 days), one per source ──
    // Use platformPool — podcasts are platform content (many have tenant_id NULL)
    // Filter: must have either audio_url OR a valid http(s) source_url — no dead cards
    const { rows: latest } = await platformPool.query(`
      SELECT DISTINCT ON (source_name)
        id, title, source_name, source_url, published_at, image_url, audio_url
      FROM external_documents
      WHERE source_type = 'podcast'
        AND published_at > NOW() - INTERVAL '7 days'
        AND title IS NOT NULL
        AND (
          audio_url IS NOT NULL
          OR (source_url IS NOT NULL AND (source_url LIKE 'http://%' OR source_url LIKE 'https://%'))
        )
      ORDER BY source_name, published_at DESC
    `);
    // Sort by recency after dedup
    const latestSorted = latest.sort((a, b) => new Date(b.published_at) - new Date(a.published_at)).slice(0, 5);

    // Resolve missing audio URLs — check source_url for direct audio links
    for (const ep of latestSorted) {
      if (ep.audio_url) continue;
      if (ep.source_url && /\.(mp3|m4a|ogg|wav)(\?|$)/i.test(ep.source_url)) {
        ep.audio_url = ep.source_url;
        platformPool.query('UPDATE external_documents SET audio_url = $1 WHERE id = $2', [ep.audio_url, ep.id]).catch(() => {});
      }
    }

    // ── DEEP DIVES: articles/blogs — semantic match from full archive via Qdrant ──
    let deepDives = [];
    const searchTerms = trending.map(t => themeLabels[t.signal_type] || t.signal_type.replace(/_/g, ' ')).join(' ');

    if (process.env.OPENAI_API_KEY && process.env.QDRANT_URL) {
      try {
        const vector = await generateQueryEmbedding(`executive search talent leadership: ${searchTerms}`);
        // Tenant filter: match tenant or platform-wide (null tenant) documents
        const docTenantFilter = req.tenant_id ? { should: [{ key: 'tenant_id', match: { value: req.tenant_id } }, { is_empty: { key: 'tenant_id' } }] } : null;
        const qdrantResults = await qdrantSearch('documents', vector, 50, docTenantFilter);

        if (qdrantResults.length > 0) {
          const uuidRx = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const docIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRx.test(id));

          if (docIds.length > 0) {
            const { rows } = await db.query(`
              SELECT id, title, source_name, source_url, published_at, image_url, source_type
              FROM external_documents
              WHERE id = ANY($1::uuid[]) AND source_type IN ('rss', 'vc_blog', 'newsletter', 'news_pr')
              ORDER BY published_at DESC
            `, [docIds]);

            // Re-attach scores, deduplicate by source_name, pick best per source
            const scoreMap = new Map(qdrantResults.map(r => [String(r.id), r.score]));
            const scored = rows.map(r => ({ ...r, match_score: scoreMap.get(r.id) || 0 }));

            // One per source to avoid 4x same show
            const seenSources = new Set(latestSorted.map(l => l.source_name));
            const bySource = new Map();
            scored.sort((a, b) => b.match_score - a.match_score).forEach(r => {
              if (!bySource.has(r.source_name) && !seenSources.has(r.source_name)) {
                bySource.set(r.source_name, r);
              }
            });
            deepDives = [...bySource.values()].slice(0, 5);
          }
        }
      } catch (e) {
        console.warn('Podcast Qdrant search failed:', e.message);
      }
    }

    // Fallback for deep dives if Qdrant empty — articles/blogs, not podcasts
    if (deepDives.length < 3) {
      const latestIds = latestSorted.map(l => l.id);
      const deepIds = deepDives.map(d => d.id);
      const exclude = [...latestIds, ...deepIds];
      const { rows: fallback } = await db.query(`
        SELECT DISTINCT ON (source_name)
          id, title, source_name, source_url, published_at, image_url, source_type
        FROM external_documents
        WHERE source_type IN ('rss', 'vc_blog', 'newsletter', 'news_pr') AND title IS NOT NULL
          AND source_url IS NOT NULL AND source_url LIKE 'http%'
          AND id != ALL($1::uuid[])
          AND published_at > NOW() - INTERVAL '14 days'
        ORDER BY source_name, published_at DESC
      `, [exclude]);
      const fb = fallback.sort((a, b) => new Date(b.published_at) - new Date(a.published_at)).slice(0, 5 - deepDives.length);
      deepDives = [...deepDives, ...fb].slice(0, 5);
    }

    res.json({ latest: latestSorted, deep_dives: deepDives, themes: themeNames });
  } catch (err) {
    console.error('Top podcasts error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// RE-ENGAGE WINDOWS — dormant contacts at companies with recent signals
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/reengage-windows', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT DISTINCT ON (p.id)
        p.id, p.full_name, p.current_title, p.current_company_name,
        se.signal_type, se.company_name AS signal_company, se.confidence_score,
        se.detected_at AS signal_date,
        i.interaction_at AS last_contact,
        i.interaction_type AS last_channel,
        EXTRACT(DAY FROM NOW() - i.interaction_at) AS days_since_contact,
        ps.engagement_score, ps.timing_score
      FROM people p
      JOIN companies c ON c.id = p.current_company_id
      JOIN signal_events se ON se.company_id = c.id
        AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change')
        AND se.detected_at > NOW() - INTERVAL '30 days'
        AND COALESCE(se.is_megacap, false) = false
      LEFT JOIN LATERAL (
        SELECT interaction_at, interaction_type FROM interactions
        WHERE person_id = p.id AND tenant_id = $1
        ORDER BY interaction_at DESC LIMIT 1
      ) i ON true
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE p.tenant_id = $1
        AND p.current_title IS NOT NULL
        AND p.seniority_level IN ('c_suite', 'C-Suite', 'C-level', 'vp', 'VP', 'director', 'Director', 'Head')
        AND i.interaction_at IS NOT NULL
        AND i.interaction_at < NOW() - INTERVAL '60 days'
      ORDER BY p.id, se.confidence_score DESC
    `, [req.tenant_id]);

    // Rank by signal strength + dormancy
    const ranked = rows
      .map(r => ({
        ...r,
        score: (r.confidence_score || 0) * 0.4 + Math.min((r.days_since_contact || 0) / 365, 1) * 0.3 + (r.timing_score || 0) * 0.3
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 6);

    res.json(ranked);
  } catch (err) {
    console.error('Re-engage windows error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNALS
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/signals/brief', authenticateToken, async (req, res) => {
  try {
    // Use platformPool to bypass RLS — query explicitly filters by tenant_id
    const db = { query: (text, params) => platformPool.query(text, params) };
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const type = req.query.type;
    const status = req.query.status;
    const category = req.query.category;
    const region = req.query.region; // AU, SG, UK, US, APAC, EMEA, AMER, or 'all'
    const minConf = parseFloat(req.query.min_confidence) || 0;
    const networkOnly = req.query.network === 'true'; // only signals where we have contacts
    const huddleId = req.query.huddle_id || null; // optional huddle context for cross-tenant ranking

    // If huddle context, get all member tenant IDs
    let huddleTenantIds = null;
    let huddleTenantNames = {};
    let huddleConfig = null;
    if (huddleId) {
      try {
        const membership = await verifyHuddleMember(huddleId, req.tenant_id);
        if (membership) {
          const { rows: members } = await platformPool.query(
            `SELECT hm.tenant_id, t.name FROM huddle_members hm JOIN tenants t ON t.id = hm.tenant_id WHERE hm.huddle_id = $1 AND hm.status = 'active'`,
            [huddleId]
          );
          huddleTenantIds = members.map(m => m.tenant_id);
          members.forEach(m => { huddleTenantNames[m.tenant_id] = m.name; });
          // Load huddle signal config for mission-based filtering
          const { rows: [huddle] } = await platformPool.query('SELECT signal_config, purpose FROM huddles WHERE id = $1', [huddleId]);
          huddleConfig = huddle?.signal_config || {};
        }
      } catch (e) {}
    }

    let where = 'WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter
    paramIdx++;
    where += ` AND (se.visibility IS NULL OR se.visibility != 'private' OR se.owner_user_id = $${paramIdx})`;
    params.push(req.user.user_id);

    // Exclude megacaps, tenant company, and self-referential signals from feed
    where += ` AND COALESCE(se.is_megacap, false) = false AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')`;
    // Hard date filter: only signals with a confirmed signal_date within 90 days
    // Signals without signal_date are excluded from ranked feeds (shown as "other" on dossiers)
    where += ` AND se.signal_date IS NOT NULL AND se.signal_date > NOW() - INTERVAL '90 days'`;
    // Also exclude signals whose company_name matches the tenant name (catches un-linked records)
    if (req.user.tenant_name) {
      paramIdx++;
      where += ` AND (se.company_name IS NULL OR se.company_name NOT ILIKE $${paramIdx})`;
      params.push(`%${req.user.tenant_name}%`);
    }

    if (type) {
      paramIdx++;
      where += ` AND se.signal_type = $${paramIdx}::signal_type`;
      params.push(type);
    }
    if (status) {
      paramIdx++;
      where += ` AND se.triage_status = $${paramIdx}::triage_status`;
      params.push(status);
    }
    if (category) {
      paramIdx++;
      where += ` AND se.signal_category = $${paramIdx}`;
      params.push(category);
    }
    if (minConf > 0) {
      paramIdx++;
      where += ` AND se.confidence_score >= $${paramIdx}`;
      params.push(minConf);
    }

    // Region filter — uses shared REGION_MAP/REGION_CODES constants
    if (region && region !== 'all' && REGION_MAP[region]) {
      const geos = REGION_MAP[region];
      const codes = REGION_CODES[region] || [];

      // Build OR conditions across multiple fields
      const orParts = [];

      // Company geography/country_code
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`c.geography ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });
      codes.forEach(code => {
        paramIdx++;
        orParts.push(`c.country_code = $${paramIdx}`);
        params.push(code);
      });

      // Evidence summary text
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`se.evidence_summary ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      // Company name (catches "Department of Health and Aged Care" etc.)
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`se.company_name ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      // Source document title
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`ed.title ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      where += ` AND (${orParts.join(' OR ')})`;
    }

    // Huddle mission filter — AND between categories, OR within each
    // signal_type AND (sector OR geography) — not everything OR'd together
    if (huddleConfig && !type) {
      // Signal types — restrict to configured types
      if (huddleConfig.signal_types?.length) {
        paramIdx++;
        where += ` AND se.signal_type = ANY($${paramIdx})`;
        params.push(huddleConfig.signal_types);
      }

      // Sector OR geography — at least one must match
      var contextOr = [];
      if (huddleConfig.sectors?.length) {
        huddleConfig.sectors.forEach(function(s) {
          paramIdx++;
          contextOr.push(`c.sector ILIKE $${paramIdx}`);
          params.push('%' + s + '%');
        });
      }
      if (huddleConfig.geography?.length) {
        huddleConfig.geography.forEach(function(g) {
          if (REGION_CODES[g]) {
            REGION_CODES[g].forEach(function(code) {
              paramIdx++;
              contextOr.push(`c.country_code = $${paramIdx}`);
              params.push(code);
            });
          } else {
            paramIdx++;
            contextOr.push(`c.geography ILIKE $${paramIdx}`);
            params.push('%' + g + '%');
          }
        });
      }
      var missionOr = contextOr; // for the closing logic below
      if (missionOr.length > 0) {
        where += ' AND (' + missionOr.join(' OR ') + ')';
      }
    }

    // Network filter — only signals where we have contacts at the company
    if (networkOnly) {
      if (huddleTenantIds && huddleTenantIds.length > 0) {
        // Huddle context: contacts or clients from ANY member tenant
        paramIdx++;
        where += ` AND (
          EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = se.company_id AND p.tenant_id = ANY($${paramIdx}))
          OR c.is_client = true
          OR EXISTS (SELECT 1 FROM companies c2 WHERE c2.is_client = true AND LOWER(c2.name) = LOWER(c.name) AND c2.tenant_id = ANY($${paramIdx}))
        )`;
        params.push(huddleTenantIds);
      } else {
        where += ` AND (
          EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = se.company_id AND p.tenant_id = $1)
          OR EXISTS (SELECT 1 FROM companies c_cl WHERE c_cl.is_client = true AND LOWER(c_cl.name) = LOWER(c.name) AND c_cl.tenant_id = $1)
        )`;
      }
    }

    // User's geographic focus — for relevance boosting in ORDER BY
    const userRegion = req.user.region || '';
    const userRegionCodes = [];
    userRegion.split(',').forEach(r => {
      const codes = REGION_CODES[r.trim()];
      if (codes) userRegionCodes.push(...codes);
    });

    // Snapshot params for count query BEFORE adding boost/limit/offset
    const countParams = params.slice();

    paramIdx++;
    const geoBoostParam = paramIdx;
    params.push(userRegionCodes.length > 0 ? userRegionCodes : ['__none__']);

    paramIdx++;
    const limitParam = paramIdx;
    params.push(limit);
    paramIdx++;
    const offsetParam = paramIdx;
    params.push(offset);
    // Huddle tenant array for cross-tenant queries
    let huddleTenantParam = null;
    if (huddleTenantIds && huddleTenantIds.length > 0) {
      paramIdx++;
      huddleTenantParam = paramIdx;
      params.push(huddleTenantIds);
    }

    const [signalsResult, countResult] = await Promise.all([
      db.query(`
        WITH ranked_signals AS (
          SELECT se.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY se.company_id, date_trunc('day', se.detected_at)
                   ORDER BY se.confidence_score DESC, se.detected_at DESC
                 ) AS rn,
                 COUNT(*) OVER (
                   PARTITION BY se.company_id, date_trunc('day', se.detected_at)
                 ) AS signals_in_cluster
          FROM signal_events se
          LEFT JOIN companies c ON se.company_id = c.id
          LEFT JOIN external_documents ed ON se.source_document_id = ed.id
          ${where}
        )
        SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
               se.evidence_summary, se.evidence_snippet, se.triage_status,
               se.detected_at, se.signal_date, se.source_url, se.signal_category,
               se.hiring_implications, se.is_megacap, se.image_url,
               se.signals_in_cluster,
               c.sector, c.geography,
               COALESCE(c.is_client, false) AS is_client,
               c.country_code, c.company_tier,
               ed.source_name, ed.source_type AS doc_source_type,
               ed.title AS doc_title, ed.summary AS doc_summary,
               ed.image_url AS doc_image_url, ed.audio_url AS doc_audio_url,
               COALESCE(pc.cnt, 0) AS contact_count,
               COALESCE(px.cnt, 0) AS prox_connection_count,
               px.best_name AS best_connector_name,
               COALESCE(plc.cnt, 0) AS placement_count,
               sd.id AS dispatch_id, sd.status AS dispatch_status,
               sd.claimed_by, sd.claimed_by_name, sd.blog_title AS dispatch_blog_title,
               CASE WHEN c.country_code = ANY($${geoBoostParam}) THEN true ELSE false END AS geo_relevant
        FROM ranked_signals se
        LEFT JOIN companies c ON se.company_id = c.id
        LEFT JOIN external_documents ed ON se.source_document_id = ed.id
        LEFT JOIN LATERAL (
          SELECT sd2.id, sd2.status, sd2.claimed_by,
                 u2.name AS claimed_by_name, sd2.blog_title
          FROM signal_dispatches sd2
          LEFT JOIN users u2 ON u2.id = sd2.claimed_by
          WHERE sd2.signal_event_id = se.id
          ORDER BY sd2.generated_at DESC LIMIT 1
        ) sd ON true
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM people p
          WHERE p.current_company_id = se.company_id AND p.tenant_id = $1
        ) pc ON true
        LEFT JOIN LATERAL (
          SELECT COUNT(DISTINCT tp2.person_id) AS cnt,
                 (SELECT u3.name FROM team_proximity tp3
                  JOIN people p3 ON p3.id = tp3.person_id
                  JOIN users u3 ON u3.id = tp3.team_member_id
                  WHERE tp3.tenant_id = $1 AND p3.current_company_id = se.company_id AND tp3.relationship_strength >= 0.25
                  ORDER BY tp3.relationship_strength DESC LIMIT 1) AS best_name
          FROM team_proximity tp2
          JOIN people p2 ON p2.id = tp2.person_id AND p2.tenant_id = $1
          WHERE tp2.tenant_id = $1 AND tp2.relationship_strength >= 0.25 AND p2.current_company_id = se.company_id
        ) px ON true
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM conversions pl
          JOIN accounts cl ON cl.id = pl.client_id
          WHERE pl.tenant_id = $1 AND cl.company_id = se.company_id
        ) plc ON true
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS cnt FROM people p
          WHERE p.current_company_id = se.company_id
            AND p.tenant_id ${huddleTenantParam ? `= ANY($${huddleTenantParam})` : '= $1'}
        ) net_density ON true
        WHERE se.rn = 1
        ORDER BY
          -- 0. Exclude megacaps and tenant's own company
          CASE WHEN c.company_tier = 'tenant_company' THEN 2 WHEN se.is_megacap = true THEN 1 ELSE 0 END,
          -- 1. CLIENT PRIORITY (own tenant only, or huddle partner if in huddle context)
          CASE WHEN EXISTS (SELECT 1 FROM companies c_own WHERE c_own.is_client = true AND LOWER(c_own.name) = LOWER(c.name) AND c_own.tenant_id = $1) THEN 0
            ${huddleTenantParam ? `WHEN EXISTS (SELECT 1 FROM companies c2 WHERE c2.is_client = true AND LOWER(c2.name) = LOWER(c.name) AND c2.tenant_id = ANY($${huddleTenantParam})) THEN 0` : ''}
            ELSE 1 END,
          -- 2. NETWORK DENSITY (single subquery, not 3x)
          CASE
            WHEN COALESCE(net_density.cnt, 0) >= 5 THEN 0
            WHEN COALESCE(net_density.cnt, 0) >= 2 THEN 1
            WHEN COALESCE(net_density.cnt, 0) >= 1 THEN 2
            ELSE 3 END,
          -- 3. GEOGRAPHIC RELEVANCE (user's focus countries)
          CASE WHEN c.country_code = ANY($${geoBoostParam}) THEN 0 ELSE 1 END,
          -- 4. SIGNAL TYPE HIERARCHY (data-driven: closing signals = nearest to mandate)
          -- Source: signal_pattern_weights — closing_signal analysis of 229 clients
          -- restructuring/layoffs/leadership_change = conversion indicators
          -- strategic_hiring/capital_raising = early indicators (opening signals)
          CASE se.signal_type
            WHEN 'restructuring' THEN 0 WHEN 'layoffs' THEN 0
            WHEN 'leadership_change' THEN 1
            WHEN 'strategic_hiring' THEN 2 WHEN 'capital_raising' THEN 2
            WHEN 'geographic_expansion' THEN 2 WHEN 'partnership' THEN 3
            WHEN 'product_launch' THEN 3
            ELSE 4 END,
          -- 5. RECENCY & CONFIDENCE (tiebreaker)
          se.confidence_score DESC NULLS LAST,
          se.detected_at DESC NULLS LAST
        LIMIT $${limitParam} OFFSET $${offsetParam}
      `, params),
      db.query(`SELECT COUNT(*) AS cnt FROM signal_events se LEFT JOIN companies c ON se.company_id = c.id LEFT JOIN external_documents ed ON se.source_document_id = ed.id ${where}`, countParams),
    ]);

    // Compute region stats — cached per tenant (expensive ILIKE scan)
    const regionCacheKey = req.tenant_id + ':region_stats';
    let regionStats = cachedResponse(req.tenant_id, 'region_stats');
    if (!regionStats) {
      try {
        const { rows: rStats } = await db.query(`
          SELECT
            COUNT(*) FILTER (WHERE c.country_code IN ('AU','NZ','FJ','PG')
              OR c.geography ILIKE '%Australia%' OR c.geography ILIKE '%New Zealand%' OR c.geography ILIKE '%Oceania%'
              OR se.evidence_summary ILIKE '%Australia%' OR se.evidence_summary ILIKE '%Sydney%' OR se.evidence_summary ILIKE '%Melbourne%'
              OR se.company_name ILIKE '%Australian%'
              OR ed.title ILIKE '%Australia%' OR ed.title ILIKE '%Australian%'
            ) AS oce,
            COUNT(*) FILTER (WHERE c.country_code IN ('US','CA','BR','MX','AR','CL','CO','PE')
              OR c.geography ILIKE '%United States%' OR c.geography ILIKE '%America%' OR c.geography ILIKE '%Canada%' OR c.geography ILIKE '%Brazil%'
              OR se.evidence_summary ILIKE '%United States%' OR se.evidence_summary ILIKE '%Silicon Valley%' OR se.evidence_summary ILIKE '%New York%'
              OR ed.title ILIKE '%America%' OR ed.title ILIKE '%US %' OR ed.title ILIKE '%Wall Street%'
            ) AS amer,
            COUNT(*) FILTER (WHERE c.country_code IN ('GB','UK','IE','DE','FR','NL','SE','DK','NO','FI','ES','IT','PT','AT','CH','BE','PL','CZ')
              OR c.geography ILIKE '%United Kingdom%' OR c.geography ILIKE '%Europe%' OR c.geography ILIKE '%London%' OR c.geography ILIKE '%Germany%' OR c.geography ILIKE '%France%'
              OR se.evidence_summary ILIKE '%United Kingdom%' OR se.evidence_summary ILIKE '%London%' OR se.evidence_summary ILIKE '%Europe%'
              OR ed.title ILIKE '%UK %' OR ed.title ILIKE '%London%' OR ed.title ILIKE '%Europe%'
            ) AS eur,
            COUNT(*) FILTER (WHERE c.country_code IN ('AE','SA','QA','BH','KW','OM','IL','TR','EG','MA')
              OR c.geography ILIKE '%Dubai%' OR c.geography ILIKE '%Middle East%' OR c.geography ILIKE '%Saudi%' OR c.geography ILIKE '%Israel%'
              OR se.evidence_summary ILIKE '%Middle East%' OR se.evidence_summary ILIKE '%Dubai%' OR se.evidence_summary ILIKE '%Saudi%'
              OR ed.title ILIKE '%Middle East%' OR ed.title ILIKE '%Gulf%'
            ) AS mena,
            COUNT(*) FILTER (WHERE c.country_code IN ('SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN','TW','BD','PK')
              OR c.geography ILIKE '%Singapore%' OR c.geography ILIKE '%Asia%' OR c.geography ILIKE '%India%' OR c.geography ILIKE '%Japan%' OR c.geography ILIKE '%China%' OR c.geography ILIKE '%Hong Kong%'
              OR se.evidence_summary ILIKE '%Singapore%' OR se.evidence_summary ILIKE '%Asia%' OR se.evidence_summary ILIKE '%India%'
              OR ed.title ILIKE '%Singapore%' OR ed.title ILIKE '%Asia%' OR ed.title ILIKE '%India%'
            ) AS asia,
            COUNT(*) FILTER (WHERE c.is_client = true) AS client_signals,
            COUNT(*) AS total
          FROM signal_events se
          LEFT JOIN companies c ON se.company_id = c.id
          LEFT JOIN external_documents ed ON se.source_document_id = ed.id
          WHERE se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
            AND COALESCE(se.is_megacap, false) = false
            AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
        `, [req.tenant_id]);
        regionStats = rStats[0];
        setCachedResponse(req.tenant_id, 'region_stats', regionStats);
      } catch (e) { /* ignore */ }
    }

    // Add huddle client attribution if in huddle context
    let signals = signalsResult.rows;
    if (huddleTenantIds && huddleTenantIds.length > 0) {
      // Batch check which companies are clients for huddle partners
      const companyNames = [...new Set(signals.map(s => s.company_name).filter(Boolean))];
      if (companyNames.length > 0) {
        const { rows: clientMatches } = await platformPool.query(
          `SELECT LOWER(c.name) AS name, t.name AS tenant_name FROM companies c JOIN tenants t ON t.id = c.tenant_id WHERE c.is_client = true AND c.tenant_id = ANY($1) AND LOWER(c.name) = ANY($2)`,
          [huddleTenantIds, companyNames.map(n => n.toLowerCase())]
        );
        const clientMap = {};
        clientMatches.forEach(m => { clientMap[m.name] = m.tenant_name; });
        signals = signals.map(s => ({
          ...s,
          huddle_client: !!(s.is_client || clientMap[(s.company_name || '').toLowerCase()]),
          client_via: clientMap[(s.company_name || '').toLowerCase()] || null,
        }));
      }
    }

    // Annotate signals with transition predictions from pattern analysis
    // Loaded once from signal_pattern_weights, cached in module scope
    if (!_transitionCache) {
      try {
        var { rows: tw } = await platformPool.query(
          "SELECT key, value FROM signal_pattern_weights WHERE pattern_type = 'transition'"
        );
        _transitionCache = {};
        tw.forEach(function(r) {
          var parts = r.key.split(':');
          if (!_transitionCache[parts[0]]) _transitionCache[parts[0]] = [];
          _transitionCache[parts[0]].push(r.value);
        });
      } catch (e) { _transitionCache = {}; }
    }

    signals = signals.map(function(s) {
      var predictions = (_transitionCache[s.signal_type] || [])
        .sort(function(a, b) { return (b.probability || 0) - (a.probability || 0); })
        .slice(0, 2)
        .filter(function(p) { return p.probability >= 0.15; })
        .map(function(p) { return { type: p.to, probability: p.probability }; });
      return { ...s, predicted_next: predictions.length > 0 ? predictions : null };
    });

    res.json({
      signals,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
      region_stats: regionStats,
      huddle_id: huddleId || null,
    });
  } catch (err) {
    console.error('Signals brief error:', err.message);
    res.status(500).json({ error: 'Failed to fetch signals' });
  }
});


router.get('/api/signals/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT se.*, c.name AS company_name_full, c.sector, c.geography,
             c.description AS company_description, c.is_client,
             ed.title AS doc_title, ed.source_name, ed.source_url AS doc_url,
             ed.content AS doc_content
      FROM signal_events se
      LEFT JOIN companies c ON se.company_id = c.id
      LEFT JOIN external_documents ed ON se.source_document_id = ed.id
      WHERE se.id = $1 AND (se.tenant_id IS NULL OR se.tenant_id = $2)
    `, [req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    const signal = rows[0];

    // Auto-bundle relevant case studies
    let relevant_case_studies = [];
    try {
      const scoreTerms = [];
      const csParams = [req.tenant_id];
      let csIdx = 1;

      if (signal.sector) {
        csIdx++; csParams.push(`%${signal.sector}%`);
        scoreTerms.push(`CASE WHEN cs.sector ILIKE $${csIdx} THEN 0.3 ELSE 0 END`);
      }
      if (signal.geography) {
        csIdx++; csParams.push(`%${signal.geography}%`);
        scoreTerms.push(`CASE WHEN cs.geography ILIKE $${csIdx} THEN 0.25 ELSE 0 END`);
      }
      if (signal.signal_type) {
        const sigThemes = {
          capital_raising: ['high-growth','scaling'], geographic_expansion: ['cross-border','expansion'],
          strategic_hiring: ['leadership','team-build'], ma_activity: ['post-acquisition','integration'],
          leadership_change: ['succession','transition'], restructuring: ['turnaround','transformation'],
        };
        const themes = sigThemes[signal.signal_type] || [];
        if (themes.length) {
          csIdx++; csParams.push(themes);
          scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${csIdx}::text[]))::float * 0.25`);
        }
      }
      if (signal.company_id) {
        csIdx++; csParams.push(signal.company_id);
        scoreTerms.push(`CASE WHEN cs.client_id = $${csIdx}::uuid THEN 0.5 ELSE 0 END`);
      }

      if (scoreTerms.length > 0) {
        const scoreExpr = scoreTerms.join(' + ');
        const { rows: csRows } = await db.query(`
          SELECT cs.id, cs.title, cs.sector, cs.geography, cs.engagement_type, cs.year,
                 cs.themes, cs.capabilities, cs.public_approved, cs.visibility,
                 cs.public_title, cs.public_summary,
                 (${scoreExpr}) AS relevance
          FROM case_studies cs
          WHERE cs.tenant_id = $1 AND (${scoreExpr}) > 0
          ORDER BY (${scoreExpr}) DESC LIMIT 5
        `, csParams);
        relevant_case_studies = csRows;
      }
    } catch (e) { /* case_studies table may not exist */ }

    res.json({ ...signal, relevant_case_studies });
  } catch (err) {
    console.error('Signal detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch signal' });
  }
});

router.patch('/api/signals/:id/triage', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status, notes } = req.body;
    const validStatuses = ['new', 'reviewing', 'qualified', 'irrelevant', 'actioned'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: `Invalid status. Must be one of: ${validStatuses.join(', ')}` });
    }

    const { rows } = await db.query(`
      UPDATE signal_events
      SET triage_status = $1::triage_status,
          triage_notes = COALESCE($2, triage_notes),
          triaged_by = $3,
          triaged_at = NOW(),
          updated_at = NOW()
      WHERE id = $4 AND tenant_id = $5
      RETURNING id, triage_status, triaged_at
    `, [status, notes, req.user.user_id, req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Triage error:', err.message);
    res.status(500).json({ error: 'Failed to update triage' });
  }
});

// ─── Signal Proximity Graph (for popup mini-graph) ───
router.get('/api/signals/:id/proximity-graph', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenantId = req.tenant_id;
    const signalId = req.params.id;

    // 1. Get the signal
    const { rows: [sig] } = await db.query(
      'SELECT * FROM signal_events WHERE id = $1 AND (tenant_id IS NULL OR tenant_id = $2)',
      [signalId, tenantId]
    );
    if (!sig) return res.status(404).json({ error: 'Signal not found' });
    if (!sig.company_id) return res.json({ signal: { company: sig.company_name || 'Unknown', type: sig.signal_type, headline: sig.headline, confidence: sig.confidence_score }, account: null, graph: { nodes: [], links: [] } });

    // 2. Get team members (include all roles — viewers with proximity data should appear in graph)
    const { rows: team } = await db.query(
      `SELECT id, name FROM users WHERE tenant_id = $1`,
      [tenantId]
    );

    // 3. Get contacts with proximity to signal company + their scores
    const { rows: contacts } = await db.query(`
      SELECT
        p.id, p.full_name, p.current_title, p.current_company_name,
        ps.timing_score, ps.receptivity_score,
        json_object_agg(
          tp.team_member_id::text,
          json_build_object(
            'strength', tp.relationship_strength,
            'type', tp.relationship_type,
            'last_interaction', tp.last_interaction_date
          )
        ) AS proximity_by_user,
        MAX(tp.relationship_strength) AS best_strength,
        (SELECT json_agg(json_build_object('type', psg.signal_type::text, 'date', psg.detected_at))
         FROM person_signals psg
         WHERE psg.person_id = p.id AND psg.tenant_id = $1
           AND psg.detected_at >= NOW() - INTERVAL '90 days'
         LIMIT 3
        ) AS recent_signals
      FROM people p
      JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = $1
      LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
      WHERE p.tenant_id = $1
        AND (p.current_company_id = $2 OR (p.current_company_id IS NULL AND LENGTH(TRIM($3)) > 3 AND LOWER(TRIM(p.current_company_name)) = LOWER(TRIM($3))))
        AND tp.relationship_strength >= 0.15
      GROUP BY p.id, p.full_name, p.current_title, p.current_company_name,
               ps.timing_score, ps.receptivity_score
      ORDER BY MAX(tp.relationship_strength) DESC
      LIMIT 15
    `, [tenantId, sig.company_id, sig.company_name || '']);

    // 4. Check if signal company is an account/client
    const { rows: [account] } = await db.query(`
      SELECT a.id, a.name, a.relationship_tier
      FROM accounts a
      WHERE a.tenant_id = $1
        AND (a.company_id = $2 OR LOWER(a.name) = LOWER((SELECT name FROM companies WHERE id = $2)))
      LIMIT 1
    `, [tenantId, sig.company_id]);

    // 5. Build graph nodes and links
    const nodes = [];
    const links = [];

    // Company node (focal point)
    nodes.push({
      id: `company-${sig.company_id}`,
      type: 'company',
      label: sig.company_name || 'Unknown',
      companyId: sig.company_id,
      isClient: !!account,
      clientTier: account?.relationship_tier,
      signalType: sig.signal_type,
      signalConfidence: sig.confidence_score
    });

    // Team nodes — only those connected via contacts
    const connectedUserIds = new Set(
      contacts.flatMap(c => Object.keys(c.proximity_by_user || {}))
    );
    team.filter(u => connectedUserIds.has(u.id)).forEach(u => {
      const initials = (u.name || '').split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
      nodes.push({ id: `user-${u.id}`, type: 'team', label: initials, fullName: u.name, userId: u.id });
    });

    // Contact nodes
    contacts.forEach(c => {
      const bestStrength = parseFloat(c.best_strength) || 0;
      nodes.push({
        id: `contact-${c.id}`,
        type: 'contact',
        label: c.full_name,
        personId: c.id,
        role: c.current_title,
        bestStrength,
        proximityByUser: c.proximity_by_user || {},
        recentSignals: c.recent_signals || [],
        timingScore: c.timing_score,
        receptivityScore: c.receptivity_score
      });

      // Contact → company links
      links.push({
        source: `contact-${c.id}`,
        target: `company-${sig.company_id}`,
        strength: bestStrength * 0.7,
        type: 'works_at'
      });

      // Team → contact links
      Object.entries(c.proximity_by_user || {}).forEach(([userId, prox]) => {
        if (prox.strength >= 0.20) {
          links.push({
            source: `user-${userId}`,
            target: `contact-${c.id}`,
            strength: prox.strength,
            type: prox.type || 'connection'
          });
        }
      });
    });

    res.json({
      signal: {
        id: sig.id,
        type: sig.signal_type,
        confidence: sig.confidence_score,
        headline: sig.evidence_summary,
        company: sig.company_name,
        detectedAt: sig.detected_at
      },
      graph: { nodes, links },
      account: account ? { id: account.id, name: account.name, tier: account.relationship_tier } : null,
      connectionCount: contacts.length
    });
  } catch (err) {
    console.error('Proximity graph error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


router.patch('/api/signals/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { visibility } = req.body;
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) return res.status(400).json({ error: 'visibility must be "company" or "private"' });

    const { rows: [sig] } = await db.query('SELECT id, visibility, owner_user_id FROM signal_events WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!sig) return res.status(404).json({ error: 'Signal not found' });
    if (sig.visibility === 'private' && sig.owner_user_id && sig.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change private signals' });
    }

    await db.query(
      `UPDATE signal_events SET visibility = $1, owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END WHERE id = $3 AND tenant_id = $4`,
      [visibility, req.user.user_id, req.params.id, req.tenant_id]
    );
    res.json({ id: req.params.id, visibility });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// LEAD CLAIM LIFECYCLE — claim / release / pipeline / outcome
// ═══════════════════════════════════════════════════════════════════════════════

// POST /api/signals/:id/claim — claim signal for current user
router.post('/api/signals/:id/claim', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Verify signal is visible to tenant
    const { rows: [sig] } = await db.query(
      `SELECT id, polarity FROM signal_events WHERE id = $1 AND (tenant_id IS NULL OR tenant_id = $2)`,
      [req.params.id, req.tenant_id]
    );
    if (!sig) return res.status(404).json({ error: 'Signal not found' });

    const { rows: [claim] } = await db.query(
      `INSERT INTO lead_claims (tenant_id, signal_id, user_id, pipeline_stage)
       VALUES ($1, $2, $3, 'claimed')
       ON CONFLICT (signal_id) DO UPDATE SET
         user_id = EXCLUDED.user_id,
         claimed_at = NOW(),
         released_at = NULL,
         released_reason = NULL,
         pipeline_stage = 'claimed',
         stage_changed_at = NOW()
       RETURNING id, signal_id, user_id, pipeline_stage, claimed_at`,
      [req.tenant_id, req.params.id, req.user.user_id]
    );
    res.status(201).json(claim);
  } catch (err) {
    console.error('Claim error:', err.message);
    res.status(500).json({ error: 'Claim failed: ' + err.message });
  }
});

// POST /api/signals/:id/release — release claim
router.post('/api/signals/:id/release', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const reason = req.body?.reason || null;
    const { rows: [released] } = await db.query(
      `UPDATE lead_claims SET released_at = NOW(), released_reason = $1
       WHERE signal_id = $2 AND tenant_id = $3 AND user_id = $4 AND released_at IS NULL
       RETURNING id, signal_id, released_at, released_reason`,
      [reason, req.params.id, req.tenant_id, req.user.user_id]
    );
    if (!released) return res.status(404).json({ error: 'No active claim to release' });
    res.json(released);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/signals/:id/pipeline — advance pipeline stage
router.patch('/api/signals/:id/pipeline', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { stage, notes } = req.body || {};
    const valid = ['claimed', 'contacted', 'meeting', 'proposal', 'mandate', 'lost'];
    if (!valid.includes(stage)) return res.status(400).json({ error: 'Invalid stage' });

    const { rows: [updated] } = await db.query(
      `UPDATE lead_claims SET pipeline_stage = $1, stage_changed_at = NOW(), notes = COALESCE($2, notes)
       WHERE signal_id = $3 AND tenant_id = $4 AND released_at IS NULL
       RETURNING id, signal_id, pipeline_stage, stage_changed_at`,
      [stage, notes || null, req.params.id, req.tenant_id]
    );
    if (!updated) return res.status(404).json({ error: 'No active claim for this signal' });
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/signals/:id/outcome — log final outcome (feeds forward calibration)
router.post('/api/signals/:id/outcome', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { outcome, converted_at, lead_time_days, mandate_id, revenue_local, revenue_currency, notes } = req.body || {};
    const valid = ['converted_mandate', 'contact_only', 'no_response', 'wrong_moment', 'window_expired', 'declined'];
    if (!valid.includes(outcome)) return res.status(400).json({ error: 'Invalid outcome' });

    // Get claim context if available
    const { rows: [claim] } = await db.query(
      `SELECT claimed_at FROM lead_claims WHERE signal_id = $1 AND tenant_id = $2 ORDER BY claimed_at DESC LIMIT 1`,
      [req.params.id, req.tenant_id]
    );

    const { rows: [row] } = await db.query(
      `INSERT INTO signal_outcomes
        (tenant_id, signal_id, outcome, claimed_at, converted_at, lead_time_days,
         mandate_id, revenue_local, revenue_currency, resolved_by, notes)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING id, signal_id, outcome, resolved_at`,
      [
        req.tenant_id, req.params.id, outcome,
        claim?.claimed_at || null,
        converted_at || null,
        lead_time_days || null,
        mandate_id || null,
        revenue_local || null,
        revenue_currency || null,
        req.user.user_id,
        notes || null,
      ]
    );

    // Mark signal as closed if converted or window expired
    if (outcome === 'converted_mandate' || outcome === 'window_expired') {
      await db.query(
        `UPDATE signal_events SET phase = 'closed' WHERE id = $1`,
        [req.params.id]
      );
    }

    res.status(201).json(row);
  } catch (err) {
    console.error('Outcome error:', err.message);
    res.status(500).json({ error: 'Outcome failed: ' + err.message });
  }
});

  return router;
};
