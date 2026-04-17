// ═══════════════════════════════════════════════════════════════════════════════
// routes/platform.js — All remaining platform API routes
// Covers: feeds, searches, huddles, dispatches, network, documents,
//         integrations, billing, events, pipelines, chat, profile, and more
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const https = require('https');
const crypto = require('crypto');
const path = require('path');
const router = express.Router();

module.exports = function(deps) {
  const {
    platformPool, TenantDB, authenticateToken, requireAdmin, optionalAuth,
    auditLog, generateQueryEmbedding, qdrantSearch,
    cachedResponse, setCachedResponse, endpointLimit, safeError,
    REGION_MAP, REGION_CODES, NICKNAMES, RESEARCH_SEARCH_ENABLED,
    searchPublications, computeResearchMomentum,
    getGoogleToken, sendEmail,
    verifyHuddleMember,
    rootDir,
  } = deps;


  // ═══════════════════════════════════════════════════════════════════════════
  // FEEDS
  // ═══════════════════════════════════════════════════════════════════════════

router.get('/api/feeds', authenticateToken, async (req, res) => {
  try {
    const { rows: byType } = await platformPool.query(
      "SELECT source_type, COUNT(*) AS count FROM rss_sources WHERE enabled = true GROUP BY source_type ORDER BY count DESC"
    );
    const { rows: byRegion } = await platformPool.query(
      "SELECT UNNEST(regions) AS region, COUNT(*) AS count FROM rss_sources WHERE enabled = true AND regions IS NOT NULL GROUP BY 1 ORDER BY count DESC LIMIT 15"
    ).catch(() => ({ rows: [] }));
    const { rows: [totals] } = await platformPool.query(
      "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE enabled) AS active FROM rss_sources"
    );

    res.json({
      total_feeds: parseInt(totals.total),
      active_feeds: parseInt(totals.active),
      by_type: byType.map(r => ({ type: r.source_type, count: parseInt(r.count) })),
      regions: byRegion.map(r => r.region),
      coverage: {
        wire_services: 'PR Newswire, Business Wire, GlobeNewswire',
        research: 'Macro economic, labour market, industry verticals',
        news: 'Technology, financial services, venture capital',
        podcasts: '46 shows across business and technology',
        global: '100+ language coverage via GDELT'
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Feed inventory — admin only
router.get('/api/feeds/inventory', authenticateToken, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  try {
    const db = new TenantDB(req.tenant_id);
    const vertical = req.user.vertical || 'talent';
    const { rows } = await db.query(`
      SELECT fi.*,
        EXISTS(
          SELECT 1 FROM tenant_feeds tf
          WHERE tf.feed_id = fi.id AND tf.tenant_id = $1 AND tf.active = TRUE
        ) AS is_active
      FROM feed_inventory fi
      WHERE fi.status = 'active'
        AND ($2 = ANY(fi.verticals) OR 'all' = ANY(fi.verticals) OR fi.verticals = '{}')
      ORDER BY fi.quality_score DESC, fi.avg_signals_per_week DESC NULLS LAST
    `, [req.tenant_id, vertical]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Activate a feed for tenant
router.post('/api/feeds/:id/activate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(`
      INSERT INTO tenant_feeds (tenant_id, feed_id, selection_method)
      VALUES ($1, $2, 'manual')
      ON CONFLICT (tenant_id, feed_id) DO UPDATE SET active = TRUE, activated_at = NOW()
    `, [req.tenant_id, req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Deactivate a feed for tenant
router.delete('/api/feeds/:id/deactivate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(
      'UPDATE tenant_feeds SET active = FALSE WHERE tenant_id = $1 AND feed_id = $2',
      [req.tenant_id, req.params.id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Rate a feed
router.post('/api/feeds/:id/rate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rating } = req.body;
    if (!rating || rating < 1 || rating > 5) return res.status(400).json({ error: 'Rating must be 1-5' });

    await db.query(
      'UPDATE tenant_feeds SET tenant_rating = $1, last_rated_at = NOW() WHERE tenant_id = $2 AND feed_id = $3',
      [rating, req.tenant_id, req.params.id]
    );
    // Update platform aggregate
    await db.query(`
      UPDATE feed_inventory SET
        total_ratings = total_ratings + 1,
        avg_rating = (SELECT AVG(tenant_rating)::NUMERIC(3,2) FROM tenant_feeds WHERE feed_id = $1 AND tenant_rating IS NOT NULL)
      WHERE id = $1
    `, [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Propose a new feed
router.post('/api/feeds/propose', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale } = req.body;
    if (!proposed_url) return res.status(400).json({ error: 'URL required' });

    // Check if already in the harvester (rss_sources or feed_inventory)
    var { rows: existingRss } = await platformPool.query('SELECT id, name FROM rss_sources WHERE url = $1', [proposed_url]);
    if (existingRss.length) return res.status(409).json({ error: 'already_tracked', message: 'This feed is already in our signal pipeline.', feed_name: existingRss[0].name });
    var { rows: existingFi } = await platformPool.query('SELECT id FROM feed_inventory WHERE url = $1', [proposed_url]).catch(() => ({ rows: [] }));
    if (existingFi.length) return res.status(409).json({ error: 'already_tracked', message: 'This feed is already in our signal pipeline.' });

    const { rows } = await db.query(`
      INSERT INTO feed_proposals (tenant_id, proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id
    `, [req.tenant_id, proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale]);
    res.json({ success: true, proposal_id: rows[0].id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// My feed proposals with status
router.get('/api/feeds/proposals', authenticateToken, async (req, res) => {
  try {
    const { rows } = await platformPool.query(
      `SELECT id, proposed_url, proposed_name, status, reviewer_notes, created_at
       FROM feed_proposals WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 20`,
      [req.tenant_id]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// DASHBOARD STATS
// ═══════════════════════════════════════════════════════════════════════════════



router.get('/api/feeds/bundles', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { type, sector, geo, search, featured, slug, limit } = req.query;
    let where = ['fb.is_active = true'];
    const params = [req.tenant_id]; let idx = 2;
    if (type) { where.push(`fb.bundle_type = $${idx++}`); params.push(type); }
    if (sector) { where.push(`$${idx++} = ANY(fb.sectors)`); params.push(sector); }
    if (geo) { where.push(`$${idx++} = ANY(fb.geographies)`); params.push(geo); }
    if (featured === 'true') where.push('fb.is_featured = true');
    if (search) { where.push(`(fb.name ILIKE $${idx} OR fb.description ILIKE $${idx})`); params.push(`%${search}%`); idx++; }
    const slugs = Array.isArray(slug) ? slug : slug ? [slug] : [];
    if (slugs.length) { where.push(`fb.slug = ANY($${idx++})`); params.push(slugs); }
    const { rows } = await db.query(`
      SELECT fb.*,
        EXISTS(SELECT 1 FROM tenant_feed_subscriptions tfs WHERE tfs.bundle_id = fb.id AND tfs.tenant_id = $1 AND tfs.is_enabled = true) AS is_subscribed,
        ROUND(AVG(fc.quality_score), 2) AS avg_quality_score
      FROM feed_bundles fb
      LEFT JOIN feed_bundle_sources fbs ON fbs.bundle_id = fb.id
      LEFT JOIN feed_catalog fc ON fc.id = fbs.source_id AND fc.is_active = true
      WHERE ${where.join(' AND ')}
      GROUP BY fb.id ORDER BY fb.is_featured DESC, fb.display_order ASC
      LIMIT $${idx}
    `, [...params, parseInt(limit) || 100]);
    res.json({ bundles: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/feeds/bundles/:slug', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [bundle] } = await db.query('SELECT * FROM feed_bundles WHERE slug = $1', [req.params.slug]);
    if (!bundle) return res.status(404).json({ error: 'Bundle not found' });
    const { rows: sources } = await db.query(`
      SELECT fc.*, fqm.signal_yield, fqm.articles_fetched, fqm.high_conf_signals
      FROM feed_bundle_sources fbs JOIN feed_catalog fc ON fc.id = fbs.source_id
      LEFT JOIN LATERAL (SELECT signal_yield, articles_fetched, high_conf_signals FROM feed_quality_metrics WHERE source_id = fc.id ORDER BY measured_at DESC LIMIT 1) fqm ON true
      WHERE fbs.bundle_id = $1 ORDER BY fc.quality_score DESC NULLS LAST
    `, [bundle.id]);
    const isSub = (await db.query('SELECT 1 FROM tenant_feed_subscriptions WHERE bundle_id = $1 AND tenant_id = $2 AND is_enabled = true', [bundle.id, req.tenant_id])).rows.length > 0;
    res.json({ ...bundle, sources, is_subscribed: isSub });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/feeds/catalog', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { search, sector, geo, category } = req.query;
    const params = []; let where = ['fc.is_active = true', 'fc.is_deprecated = false']; let idx = 1;
    if (search) { where.push(`(fc.name ILIKE $${idx} OR fc.description ILIKE $${idx})`); params.push(`%${search}%`); idx++; }
    if (sector) { where.push(`$${idx} = ANY(fc.sectors)`); params.push(sector); idx++; }
    if (geo) { where.push(`$${idx} = ANY(fc.geographies)`); params.push(geo); idx++; }
    if (category) { where.push(`fc.primary_category = $${idx}`); params.push(category); idx++; }
    const { rows } = await db.query(`
      SELECT fc.*, fqm.signal_yield, fqm.articles_fetched
      FROM feed_catalog fc
      LEFT JOIN LATERAL (SELECT signal_yield, articles_fetched FROM feed_quality_metrics WHERE source_id = fc.id ORDER BY measured_at DESC LIMIT 1) fqm ON true
      WHERE ${where.join(' AND ')} ORDER BY fc.quality_score DESC NULLS LAST
    `, params);
    res.json({ sources: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/feeds/my', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT tfs.*, fb.name AS bundle_name, fb.icon, fb.bundle_type, fb.source_count,
             fc.name AS source_name, fc.primary_category AS source_category
      FROM tenant_feed_subscriptions tfs
      LEFT JOIN feed_bundles fb ON fb.id = tfs.bundle_id
      LEFT JOIN feed_catalog fc ON fc.id = tfs.source_id
      WHERE tfs.tenant_id = $1 AND tfs.is_enabled = true ORDER BY tfs.subscribed_at DESC
    `, [req.tenant_id]);
    res.json({ subscriptions: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/api/feeds/subscribe', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { bundle_ids, source_id } = req.body;
    if (bundle_ids && bundle_ids.length) {
      for (const bid of bundle_ids) {
        await db.query(`INSERT INTO tenant_feed_subscriptions (tenant_id, bundle_id, is_enabled) VALUES ($1, $2, true)
          ON CONFLICT (tenant_id, bundle_id) DO UPDATE SET is_enabled = true, disabled_at = NULL`, [req.tenant_id, bid]);
        // Materialise sources into rss_sources
        const { rows: sources } = await db.query(`
          SELECT fc.id, fc.name, fc.url, fc.fetch_interval_min FROM feed_bundle_sources fbs
          JOIN feed_catalog fc ON fc.id = fbs.source_id AND fc.is_active = true WHERE fbs.bundle_id = $1
        `, [bid]);
        for (const src of sources) {
          await db.query(`INSERT INTO rss_sources (name, url, source_type, enabled, poll_interval_minutes, catalog_source_id, tenant_id)
            VALUES ($1, $2, 'rss', true, $3, $4, $5)
            ON CONFLICT (tenant_id, url) DO UPDATE SET enabled = true`,
            [src.name, src.url, src.fetch_interval_min, src.id, req.tenant_id]).catch(() => {});
        }
      }
      res.json({ status: 'subscribed', bundle_ids });
    } else if (source_id) {
      await db.query(`INSERT INTO tenant_feed_subscriptions (tenant_id, source_id, is_enabled) VALUES ($1, $2, true)
        ON CONFLICT (tenant_id, source_id) DO UPDATE SET is_enabled = true, disabled_at = NULL`, [req.tenant_id, source_id]);
      const { rows: [src] } = await db.query('SELECT * FROM feed_catalog WHERE id = $1', [source_id]);
      if (src) {
        await db.query(`INSERT INTO rss_sources (name, url, source_type, enabled, poll_interval_minutes, catalog_source_id, tenant_id)
          VALUES ($1, $2, 'rss', true, $3, $4, $5)
          ON CONFLICT (tenant_id, url) DO UPDATE SET enabled = true`,
          [src.name, src.url, src.fetch_interval_min, src.id, req.tenant_id]).catch(() => {});
      }
      res.json({ status: 'subscribed', source_id });
    } else { res.status(400).json({ error: 'Provide bundle_ids or source_id' }); }
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.delete('/api/feeds/subscribe/:bundleId', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(`UPDATE tenant_feed_subscriptions SET is_enabled = false, disabled_at = NOW() WHERE tenant_id = $1 AND bundle_id = $2`, [req.tenant_id, req.params.bundleId]);
    await db.query(`
      UPDATE rss_sources rs SET enabled = false FROM feed_bundle_sources fbs
      WHERE fbs.bundle_id = $1 AND rs.catalog_source_id = fbs.source_id AND rs.tenant_id = $2
        AND NOT EXISTS (SELECT 1 FROM tenant_feed_subscriptions tfs2 JOIN feed_bundle_sources fbs2 ON fbs2.bundle_id = tfs2.bundle_id
          WHERE tfs2.tenant_id = $2 AND tfs2.is_enabled = true AND fbs2.source_id = rs.catalog_source_id AND tfs2.bundle_id != $1)
    `, [req.params.bundleId, req.tenant_id]);
    res.json({ status: 'unsubscribed' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/feeds/stats', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT rs.name, rs.catalog_source_id,
        COUNT(ed.id) FILTER (WHERE ed.created_at > NOW() - INTERVAL '7 days') AS articles_7d,
        COUNT(DISTINCT se.id) FILTER (WHERE se.detected_at > NOW() - INTERVAL '7 days') AS signals_7d
      FROM rss_sources rs
      LEFT JOIN external_documents ed ON ed.source_name = rs.name AND (ed.tenant_id IS NULL OR ed.tenant_id = $1)
      LEFT JOIN signal_events se ON se.source_document_id = ed.id AND (se.tenant_id IS NULL OR se.tenant_id = $1)
      WHERE rs.tenant_id = $1 AND rs.enabled = true
      GROUP BY rs.id, rs.name, rs.catalog_source_id
      ORDER BY COUNT(DISTINCT se.id) FILTER (WHERE se.detected_at > NOW() - INTERVAL '7 days') DESC LIMIT 50
    `, [req.tenant_id]);
    res.json({ stats: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/api/admin/feeds/catalog', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { name, slug, url, tier, primary_category, tags, sectors, geographies, description, fetch_interval_min, quality_score } = req.body;
    const { rows } = await db.query(`
      INSERT INTO feed_catalog (name, slug, url, tier, primary_category, tags, sectors, geographies, description, fetch_interval_min, quality_score)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [name, slug, url, tier || 'curated', primary_category, tags || [], sectors || [], geographies || [], description, fetch_interval_min || 60, quality_score]);
    res.json(rows[0]);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// EVENTS — EventMedium event feed intelligence
// ═══════════════════════════════════════════════════════════════════════════════


  // ═══════════════════════════════════════════════════════════════════════════
  // SEARCHES / OPPORTUNITIES
  // ═══════════════════════════════════════════════════════════════════════════

// ─── Shared handlers (used by both /api/opportunities and /api/searches) ─────

async function _handleListOpportunities(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status } = req.query;
    let where = 'o.tenant_id = $1';
    const params = [req.tenant_id];
    if (status && status !== 'all') {
      if (status === 'active') {
        where += ` AND o.status IN ('sourcing', 'interviewing', 'offer')`;
      } else {
        params.push(status);
        where += ` AND o.status = $${params.length}`;
      }
    }
    const { rows } = await db.query(`
      SELECT o.id, o.title, o.status, o.location, o.seniority_level,
             o.priority, o.kick_off_date, o.target_shortlist_date,
             o.brief_summary, o.created_at, o.updated_at,
             o.target_industries, o.target_geography,
             p.name AS project_name, c.name AS client_name,
             (SELECT COUNT(*) FROM pipeline_contacts pc WHERE pc.search_id = o.id) AS candidate_count
      FROM opportunities o
      LEFT JOIN projects p ON p.id = o.project_id
      LEFT JOIN clients cl ON cl.id = p.client_id
      LEFT JOIN companies c ON c.id = cl.company_id
      WHERE ${where}
      ORDER BY
        CASE o.status WHEN 'sourcing' THEN 1 WHEN 'interviewing' THEN 2 WHEN 'offer' THEN 3 ELSE 4 END,
        o.updated_at DESC
    `, params);
    res.json(rows);
  } catch (err) {
    console.error('Opportunities list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch opportunities' });
  }
}

async function _handleGetOpportunity(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT o.*,
             p.name AS project_name, c.name AS client_name
      FROM opportunities o
      LEFT JOIN projects p ON p.id = o.project_id
      LEFT JOIN clients cl ON cl.id = p.client_id
      LEFT JOIN companies c ON c.id = cl.company_id
      WHERE o.id = $1 AND o.tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

async function _handleListCandidates(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT pc.id, pc.status, pc.created_at,
             p.id AS person_id, p.full_name AS person_name,
             p.current_title AS person_title, p.current_company_name,
             p.email, p.linkedin_url
      FROM pipeline_contacts pc
      JOIN people p ON p.id = pc.person_id
      WHERE pc.search_id = $1 AND pc.tenant_id = $2
      ORDER BY pc.status, pc.created_at DESC
    `, [req.params.id, req.tenant_id]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

async function _handleListMatches(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { limit = 20, status = 'all' } = req.query;
    let where = 'sm.search_id = $1 AND sm.tenant_id = $2';
    const params = [req.params.id, req.tenant_id];
    if (status !== 'all') {
      params.push(status);
      where += ` AND sm.status = $${params.length}`;
    }
    const { rows } = await db.query(`
      SELECT sm.id, sm.overall_match_score AS match_score, sm.match_reasons,
             sm.status, sm.created_at AS matched_at,
             p.id AS person_id, p.full_name AS person_name,
             p.current_title AS person_title, p.current_company_name,
             p.location, p.email, p.linkedin_url,
             ps.engagement_score, ps.receptivity_score, ps.timing_score
      FROM search_matches sm
      JOIN people p ON p.id = sm.person_id
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE ${where}
      ORDER BY sm.overall_match_score DESC
      LIMIT $${params.length + 1}
    `, [...params, Math.min(parseInt(limit) || 20, 100)]);
    res.json(rows);
  } catch (err) {
    console.error('Matches error:', err.message);
    res.status(500).json({ error: 'Failed to fetch matches' });
  }
}

async function _handlePatchMatch(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status } = req.body;
    if (!['suggested', 'accepted', 'rejected', 'shortlisted'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }
    await db.query(
      `UPDATE search_matches SET status = $1, reviewed_by = $2, reviewed_at = NOW()
       WHERE id = $3 AND tenant_id = $4`,
      [status, req.user.user_id, req.params.matchId, req.tenant_id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

async function _handleOpportunityActivities(req, res) {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT a.*, u.name AS actor_name FROM activities a
      LEFT JOIN users u ON u.id = a.user_id
      WHERE a.tenant_id = $1 AND a.metadata->>'opportunity_id' = $2
      ORDER BY a.created_at DESC LIMIT 50
    `, [req.tenant_id, req.params.id]);
    res.json({ activities: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

// ─── Primary routes: /api/opportunities ──────────────────────────────────────

router.get('/api/opportunities', authenticateToken, _handleListOpportunities);
router.get('/api/opportunities/:id', authenticateToken, _handleGetOpportunity);
router.get('/api/opportunities/:id/candidates', authenticateToken, _handleListCandidates);
router.get('/api/opportunities/:id/contacts', authenticateToken, _handleListCandidates);
router.get('/api/opportunities/:id/matches', authenticateToken, _handleListMatches);
router.patch('/api/opportunities/:id/matches/:matchId', authenticateToken, _handlePatchMatch);
router.get('/api/opportunities/:id/activities', authenticateToken, _handleOpportunityActivities);

// ─── Legacy aliases: /api/searches → same handlers ───────────────────────────

router.get('/api/searches', authenticateToken, _handleListOpportunities);
router.get('/api/searches/:id', authenticateToken, _handleGetOpportunity);
router.post('/api/searches', authenticateToken, (req, res) => res.status(404).json({ error: 'Use POST /api/opportunities' }));
router.patch('/api/searches/:id', authenticateToken, (req, res) => res.status(404).json({ error: 'Use PATCH /api/opportunities/:id' }));
router.get('/api/searches/:id/candidates', authenticateToken, _handleListCandidates);
router.get('/api/searches/:id/contacts', authenticateToken, _handleListCandidates);
router.get('/api/searches/:id/matches', authenticateToken, _handleListMatches);
router.patch('/api/searches/:id/matches/:matchId', authenticateToken, _handlePatchMatch);
router.get('/api/searches/:id/activities', authenticateToken, _handleOpportunityActivities);

// ─── 301 Redirects: browser clients hitting old search URLs ──────────────────
router.use('/api/searches', (req, res, next) => {
  // Only redirect non-API consumers (browsers) via Accept header check
  if (req.headers.accept && req.headers.accept.includes('text/html')) {
    const newPath = req.originalUrl.replace('/api/searches', '/api/opportunities')
      .replace('/candidates', '/contacts');
    return res.redirect(301, newPath);
  }
  next();
});


// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE BOARD (Sales Kanban)
// ═══════════════════════════════════════════════════════════════════════════════


  // ═══════════════════════════════════════════════════════════════════════════
  // HUDDLES
  // ═══════════════════════════════════════════════════════════════════════════

var { HuddleEngine } = require('../lib/platform/HuddleEngine');
var huddleEngine = new HuddleEngine();

async function embedHuddleMission(huddleId, huddle) {
  if (!process.env.OPENAI_API_KEY || !process.env.QDRANT_URL) return;
  try {
    var parts = [huddle.name, huddle.purpose || huddle.description || ''];
    var cfg = huddle.signal_config || {};
    if (cfg.sectors?.length) parts.push('Sectors: ' + cfg.sectors.join(', '));
    if (cfg.geography?.length) parts.push('Geography: ' + cfg.geography.join(', '));
    if (cfg.mission_keywords) parts.push(cfg.mission_keywords);
    var text = parts.filter(Boolean).join('\n').slice(0, 8000);
    if (text.length < 5) return;

    var vector = await generateQueryEmbedding(text);
    var body = JSON.stringify({ points: [{ id: Date.now(), vector: vector, payload: { huddle_id: huddleId, name: huddle.name, type: 'huddle_mission' } }] });
    var url = new URL('/collections/searches/points', process.env.QDRANT_URL);
    await new Promise(function(resolve, reject) {
      var req = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
        function(res) { var c = []; res.on('data', function(d) { c.push(d); }); res.on('end', function() { resolve(); }); });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
    console.log('[Huddle] Mission embedded for ' + huddle.name);
  } catch (e) { console.warn('[Huddle] Embed failed:', e.message); }
}

router.post('/api/huddles', authenticateToken, async function(req, res) {
  try {
    var { name, description, purpose, visibility, phase_label, target_date, geography, sectors, signal_types } = req.body;
    if (!name) return res.status(400).json({ error: 'name is required' });

    var slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    slug = slug + '-' + Date.now().toString(36);

    // Build signal_config from mission parameters
    var signalConfig = {};
    if (geography?.length) signalConfig.geography = geography;
    if (sectors?.length) signalConfig.sectors = sectors;
    if (signal_types?.length) signalConfig.signal_types = signal_types;
    if (purpose) signalConfig.mission_keywords = purpose.replace(/[^\w\s]/g, ' ').split(/\s+/).filter(function(w) { return w.length > 3; }).slice(0, 20).join(' ');

    var { rows } = await platformPool.query(
      `INSERT INTO huddles (name, slug, description, purpose, creator_tenant_id, visibility, phase_label, target_date, signal_config)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [name, slug, description || null, purpose || null, req.tenant_id,
       visibility || 'private', phase_label || null, target_date || null,
       JSON.stringify(signalConfig)]
    );
    var huddle = rows[0];

    // Creator auto-joins as admin
    await platformPool.query(
      `INSERT INTO huddle_members (huddle_id, tenant_id, role, status, invited_by, joined_at)
       VALUES ($1, $2, 'admin', 'active', $2, NOW())`,
      [huddle.id, req.tenant_id]
    );

    // Embed mission for semantic signal matching
    await embedHuddleMission(huddle.id, { name, purpose, signal_config: signalConfig });

    res.status(201).json(huddle);
  } catch (err) {
    console.error('Create huddle error:', err.message);
    res.status(500).json({ error: 'Failed to create huddle' });
  }
});

// PATCH /api/huddles/:id — update huddle mission, config, metadata
router.patch('/api/huddles/:id', authenticateToken, async function(req, res) {
  try {
    // Verify membership
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var { name, description, purpose, geography, sectors, signal_types, phase_label, target_date, visibility } = req.body;

    // Load current huddle
    var { rows: [current] } = await platformPool.query('SELECT * FROM huddles WHERE id = $1', [req.params.id]);
    if (!current) return res.status(404).json({ error: 'Huddle not found' });

    // Build updated signal_config
    var cfg = current.signal_config || {};
    var missionChanged = false;
    if (geography !== undefined) { cfg.geography = geography; missionChanged = true; }
    if (sectors !== undefined) { cfg.sectors = sectors; missionChanged = true; }
    if (signal_types !== undefined) { cfg.signal_types = signal_types; missionChanged = true; }
    if (purpose !== undefined && purpose !== current.purpose) {
      cfg.mission_keywords = purpose.replace(/[^\w\s]/g, ' ').split(/\s+/).filter(function(w) { return w.length > 3; }).slice(0, 20).join(' ');
      missionChanged = true;
    }

    // Update
    var { rows: [updated] } = await platformPool.query(`
      UPDATE huddles SET
        name = COALESCE($2, name),
        description = COALESCE($3, description),
        purpose = COALESCE($4, purpose),
        signal_config = $5,
        phase_label = COALESCE($6, phase_label),
        target_date = COALESCE($7, target_date),
        visibility = COALESCE($8, visibility),
        updated_at = NOW()
      WHERE id = $1 RETURNING *
    `, [req.params.id, name || null, description || null, purpose || null,
        JSON.stringify(cfg), phase_label || null, target_date || null, visibility || null]);

    // Re-embed mission if config changed
    if (missionChanged) {
      await embedHuddleMission(updated.id, { name: updated.name, purpose: updated.purpose, signal_config: cfg });
    }

    res.json(updated);
  } catch (err) {
    console.error('Update huddle error:', err.message);
    res.status(500).json({ error: 'Failed to update huddle' });
  }
});

// POST /api/huddles/:id/chat — AI-powered mission builder conversation
var huddlePlaybook = {};
try { huddlePlaybook = JSON.parse(require('fs').readFileSync(require('path').join(__dirname, 'lib', 'huddle_playbook.json'), 'utf8')); } catch(e) {}

// In-memory chat histories per huddle (cleared on restart — not persistent)
var huddleChatHistories = new Map();

router.post('/api/huddles/:id/chat', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member' });

    var { message } = req.body;
    if (!message) return res.status(400).json({ error: 'message required' });

    // Load current huddle state
    var { rows: [huddle] } = await platformPool.query('SELECT * FROM huddles WHERE id = $1', [req.params.id]);
    if (!huddle) return res.status(404).json({ error: 'Huddle not found' });
    var cfg = huddle.signal_config || {};

    // Build system prompt (Nev pattern)
    var gaps = [];
    if (!huddle.purpose || huddle.purpose.length < 20) gaps.push('mission purpose — what is this huddle trying to achieve?');
    if (!cfg.sectors || !cfg.sectors.length) gaps.push('sector focus — which industries/verticals matter?');
    if (!cfg.geography || !cfg.geography.length) gaps.push('geography — which markets/regions?');
    if (!cfg.signal_types || !cfg.signal_types.length) gaps.push('signal types — what kind of market activity matters? (funding, hiring, M&A, expansion, etc.)');

    var stateSection = 'CURRENT HUDDLE STATE:\n' +
      '- Name: ' + (huddle.name || 'not set') + '\n' +
      '- Purpose: ' + (huddle.purpose || 'not set') + '\n' +
      '- Sectors: ' + (cfg.sectors?.length ? cfg.sectors.join(', ') : 'not set') + '\n' +
      '- Geography: ' + (cfg.geography?.length ? cfg.geography.join(', ') : 'not set') + '\n' +
      '- Signal types: ' + (cfg.signal_types?.length ? cfg.signal_types.map(function(t) { return t.replace(/_/g, ' '); }).join(', ') : 'not set') + '\n' +
      '- Mission keywords: ' + (cfg.mission_keywords || 'not set');

    var gapSection = gaps.length > 0
      ? 'GAPS TO FILL — address the highest priority one first, one question per response:\n' + gaps.map(function(g) { return '- ' + g; }).join('\n')
      : 'CONFIGURATION IS COMPLETE. Confirm the setup with the user and let them know their signal feed is now curated. Keep it to 2-3 sentences.';

    var signalDefs = Object.entries(huddlePlaybook.signal_type_definitions || {}).map(function(e) { return e[0].replace(/_/g, ' ') + ': ' + e[1]; }).join('\n');
    var sectorList = (huddlePlaybook.sector_taxonomy || []).join(', ');
    var geoList = Object.keys(huddlePlaybook.geography_codes || {}).join(', ');

    var systemPrompt = huddlePlaybook.global_instructions + '\n\n---\n\n' +
      stateSection + '\n\n---\n\n' +
      gapSection + '\n\n---\n\n' +
      'SIGNAL TYPES AVAILABLE:\n' + signalDefs + '\n\n' +
      'SECTOR TAXONOMY: ' + sectorList + '\n\n' +
      'GEOGRAPHY CODES: ' + geoList + '\n\n---\n\n' +
      'EXTRACTION — CRITICAL:\n\nAfter EVERY response, append a [HUDDLE_CONFIG] block with the cumulative configuration. Include ALL fields, even unchanged ones.\n\n' +
      'Format:\n[HUDDLE_CONFIG]\n{"purpose":"...","sectors":["ai","fintech"],"geography":["US","UK"],"signal_types":["capital_raising","strategic_hiring"],"mission_keywords":"..."}\n[/HUDDLE_CONFIG]\n\n' +
      'Rules:\n- sectors MUST use values from: ' + sectorList + '\n' +
      '- geography MUST use codes from: ' + geoList + '\n' +
      '- signal_types MUST use: capital_raising, strategic_hiring, geographic_expansion, ma_activity, partnership, product_launch, leadership_change, restructuring, layoffs\n' +
      '- mission_keywords: 5-15 space-separated keywords extracted from the conversation\n' +
      '- This block is stripped from the visible reply\n\n---\n\n' +
      'CONSTRAINTS:\n- One question per response. Three sentences max.\n- Never re-ask something already captured.\n- Confirm when you capture something: "Got it — added [X] to your signal config."\n- Warm, precise tone. Not a form. A conversation.';

    // Get or create chat history
    var historyKey = req.params.id;
    if (!huddleChatHistories.has(historyKey)) huddleChatHistories.set(historyKey, []);
    var history = huddleChatHistories.get(historyKey);
    history.push({ role: 'user', content: message });

    // Keep last 20 messages
    if (history.length > 20) history.splice(0, history.length - 20);

    // Call Claude
    var claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 1024, system: systemPrompt, messages: history })
    });

    if (!claudeRes.ok) {
      var errText = await claudeRes.text();
      return res.status(500).json({ error: 'AI error: ' + claudeRes.status });
    }

    var claudeData = await claudeRes.json();
    var fullReply = claudeData.content?.[0]?.text || '';

    // Extract [HUDDLE_CONFIG] block
    var configMatch = fullReply.match(/\[HUDDLE_CONFIG\]([\s\S]*?)\[\/HUDDLE_CONFIG\]/);
    var configUpdated = false;
    if (configMatch) {
      try {
        var newConfig = JSON.parse(configMatch[1].trim());
        // Merge into existing config
        var updated = { ...cfg };
        if (newConfig.sectors?.length) updated.sectors = newConfig.sectors;
        if (newConfig.geography?.length) updated.geography = newConfig.geography;
        if (newConfig.signal_types?.length) updated.signal_types = newConfig.signal_types;
        if (newConfig.mission_keywords) updated.mission_keywords = newConfig.mission_keywords;

        var newPurpose = newConfig.purpose || huddle.purpose;

        await platformPool.query(
          'UPDATE huddles SET signal_config = $1, purpose = COALESCE($2, purpose), updated_at = NOW() WHERE id = $3',
          [JSON.stringify(updated), newPurpose, req.params.id]
        );

        // Re-embed mission
        await embedHuddleMission(req.params.id, { name: huddle.name, purpose: newPurpose, signal_config: updated });
        configUpdated = true;
      } catch (e) { console.warn('Huddle config parse error:', e.message); }
    }

    // Strip config block from visible reply
    var visibleReply = fullReply.replace(/\[HUDDLE_CONFIG\][\s\S]*?\[\/HUDDLE_CONFIG\]/, '').trim();

    history.push({ role: 'assistant', content: fullReply });

    res.json({
      reply: visibleReply,
      config_updated: configUpdated,
      current_config: configUpdated ? JSON.parse((await platformPool.query('SELECT signal_config FROM huddles WHERE id = $1', [req.params.id])).rows[0].signal_config || '{}') : cfg
    });
  } catch (err) {
    console.error('Huddle chat error:', err.message);
    res.status(500).json({ error: 'Chat failed' });
  }
});

// GET /api/huddles — list my huddles
router.get('/api/huddles', authenticateToken, async function(req, res) {
  try {
    var { rows } = await platformPool.query(
      `SELECT h.*, hm.role, hm.status as member_status,
              (SELECT COUNT(*) FROM huddle_members WHERE huddle_id = h.id AND status = 'active') as member_count,
              (SELECT COUNT(*) FROM huddle_people WHERE huddle_id = h.id) as people_count
       FROM huddles h
       JOIN huddle_members hm ON hm.huddle_id = h.id
       WHERE hm.tenant_id = $1 AND hm.status IN ('active', 'invited')
         AND h.status = 'active'
       ORDER BY h.updated_at DESC`,
      [req.tenant_id]
    );
    res.json({ huddles: rows });
  } catch (err) {
    console.error('List huddles error:', err.message);
    res.status(500).json({ error: 'Failed to list huddles' });
  }
});

// GET /api/huddles/:id — huddle detail with members
router.get('/api/huddles/:id', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var { rows: huddles } = await platformPool.query(
      'SELECT * FROM huddles WHERE id = $1', [req.params.id]
    );
    if (!huddles.length) return res.status(404).json({ error: 'Huddle not found' });

    var { rows: members } = await platformPool.query(
      `SELECT hm.tenant_id, hm.role, hm.status, hm.joined_at,
              hm.contributed_people_count, hm.net_new_people_count,
              t.name as display_name, t.name as name, t.slug as tenant_slug,
              (SELECT COUNT(*) FROM people p WHERE p.tenant_id = hm.tenant_id) AS people_count,
              (SELECT COUNT(*) FROM interactions i WHERE i.tenant_id = hm.tenant_id) AS interaction_count,
              (SELECT COUNT(DISTINCT tp.person_id) FROM team_proximity tp WHERE tp.tenant_id = hm.tenant_id) AS proximity_count
       FROM huddle_members hm
       JOIN tenants t ON t.id = hm.tenant_id
       WHERE hm.huddle_id = $1 AND hm.status IN ('active', 'invited')
       ORDER BY hm.joined_at ASC`,
      [req.params.id]
    );

    var { rows: stats } = await platformPool.query(
      `SELECT COUNT(*) as people_count,
              ROUND(AVG(best_strength_score)::numeric, 3) as avg_strength
       FROM huddle_people WHERE huddle_id = $1`,
      [req.params.id]
    );

    var huddle = huddles[0];
    huddle.members = members;
    huddle.people_count = parseInt(stats[0].people_count) || 0;
    huddle.avg_strength = parseFloat(stats[0].avg_strength) || 0;
    huddle.my_role = membership.role;

    res.json(huddle);
  } catch (err) {
    console.error('Huddle detail error:', err.message);
    res.status(500).json({ error: 'Failed to load huddle' });
  }
});

// POST /api/huddles/:id/invite — invite existing user or send email to new user
router.post('/api/huddles/:id/invite', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var { email, user_id, tenant_id: inviteTenantId, role } = req.body || {};
    if (!email && !user_id && !inviteTenantId) return res.status(400).json({ error: 'email, user_id, or tenant_id required' });

    // Get huddle name for emails
    var { rows: [huddle] } = await platformPool.query('SELECT name, purpose FROM huddles WHERE id = $1', [req.params.id]);

    // Path 1: Invite by user_id — find their tenant and add as member
    if (user_id) {
      var { rows: [user] } = await platformPool.query('SELECT id, name, email, tenant_id FROM users WHERE id = $1', [user_id]);
      if (!user) return res.status(404).json({ error: 'User not found' });
      inviteTenantId = user.tenant_id;
      email = user.email;
    }

    // Path 2: Invite by email — check if they're an existing user
    if (email && !inviteTenantId) {
      var { rows: [existingUser] } = await platformPool.query('SELECT id, name, tenant_id FROM users WHERE email = $1', [email]);
      if (existingUser) {
        inviteTenantId = existingUser.tenant_id;
      }
    }

    // If we have a tenant_id, add them as a member directly
    if (inviteTenantId) {
      // Check if already a member
      var { rows: existing } = await platformPool.query(
        'SELECT status FROM huddle_members WHERE huddle_id = $1 AND tenant_id = $2', [req.params.id, inviteTenantId]
      );
      if (existing.length && existing[0].status === 'active') {
        return res.status(409).json({ error: 'Already a member', status: 'active' });
      }
      if (existing.length) {
        // Reactivate
        await platformPool.query(
          "UPDATE huddle_members SET status = 'active', detached_at = NULL, joined_at = NOW() WHERE huddle_id = $1 AND tenant_id = $2",
          [req.params.id, inviteTenantId]
        );
      } else {
        await platformPool.query(
          "INSERT INTO huddle_members (huddle_id, tenant_id, role, status, invited_by, invited_at, joined_at) VALUES ($1, $2, $3, 'active', $4, NOW(), NOW())",
          [req.params.id, inviteTenantId, role || 'member', req.tenant_id]
        );
      }
      return res.status(201).json({ status: 'added', tenant_id: inviteTenantId, email: email });
    }

    // Path 3: Email not in system — create invite token and send onboarding email
    var { rows: [invite] } = await platformPool.query(
      `INSERT INTO huddle_invites (huddle_id, invited_by, email, role)
       VALUES ($1, $2, $3, $4) RETURNING id, token, email, role, expires_at`,
      [req.params.id, req.tenant_id, email, role || 'member']
    );

    // Send invite email via Resend
    if (process.env.RESEND_API_KEY && email) {
      try {
        var baseUrl = process.env.APP_URL || 'https://' + req.get('host');
        var joinUrl = baseUrl + '/onboarding.html?invite=' + invite.token + '&huddle=' + req.params.id;
        var inviterName = req.user.name || req.user.email;
        var emailFrom = process.env.EMAIL_FROM || 'Autonodal <notifications@autonodal.com>';

        var { Resend } = require('resend');
        var resend = new Resend(process.env.RESEND_API_KEY);
        await resend.emails.send({
          from: emailFrom,
          to: email,
          subject: inviterName + ' invited you to ' + (huddle?.name || 'a huddle') + ' on Autonodal',
          html: '<div style="font-family:system-ui,sans-serif;max-width:520px;margin:0 auto;padding:32px">' +
            '<h2 style="margin:0 0 8px">' + inviterName + ' invited you to join a Huddle</h2>' +
            '<p style="color:#4a4a4a;margin:0 0 20px"><strong>' + (huddle?.name || 'Huddle') + '</strong>' +
            (huddle?.purpose ? '<br><span style="color:#7a7a7a">' + huddle.purpose + '</span>' : '') + '</p>' +
            '<a href="' + joinUrl + '" style="display:inline-block;background:#2563eb;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:600;font-size:15px">Accept & Join</a>' +
            '<p style="color:#aaa;font-size:12px;margin-top:24px">This invite expires in 7 days.</p>' +
            '</div>'
        });
        invite.email_sent = true;
      } catch (e) {
        invite.email_sent = false;
        console.warn('Huddle invite email failed:', e.message);
      }
    }

    res.status(201).json({ status: 'invited', ...invite });
  } catch (err) {
    console.error('Huddle invite error:', err.message);
    res.status(500).json({ error: 'Failed to create invite' });
  }
});

// GET /api/huddles/:id/members/search — search users to invite
router.get('/api/huddles/:id/members/search', authenticateToken, async function(req, res) {
  try {
    var q = req.query.q;
    if (!q || q.length < 2) return res.json([]);
    var { rows } = await platformPool.query(
      `SELECT u.id, u.name, u.email, u.tenant_id, t.name AS tenant_name
       FROM users u JOIN tenants t ON t.id = u.tenant_id
       WHERE (u.name ILIKE $1 OR u.email ILIKE $1)
         AND u.tenant_id != $2
       ORDER BY u.name LIMIT 10`,
      ['%' + q + '%', req.tenant_id]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: 'Search failed' });
  }
});

// POST /api/huddles/:id/join — join with preview/confirm flow
router.post('/api/huddles/:id/join', authenticateToken, async function(req, res) {
  try {
    var huddleId = req.params.id;
    var mode = req.body.mode || 'preview'; // 'preview' or 'confirm'
    var inviteToken = req.body.invite_token;

    // Verify huddle exists
    var { rows: huddles } = await platformPool.query(
      'SELECT id, status FROM huddles WHERE id = $1', [huddleId]
    );
    if (!huddles.length || huddles[0].status !== 'active') {
      return res.status(404).json({ error: 'Huddle not found or inactive' });
    }

    // Check if already a member
    var { rows: existing } = await platformPool.query(
      `SELECT status FROM huddle_members WHERE huddle_id = $1 AND tenant_id = $2`,
      [huddleId, req.tenant_id]
    );
    if (existing.length && existing[0].status === 'active') {
      return res.status(409).json({ error: 'Already an active member' });
    }

    if (mode === 'preview') {
      var preview = await huddleEngine.previewJoin(huddleId, req.tenant_id);
      return res.json({ mode: 'preview', ...preview });
    }

    // Confirm mode — validate invite if not already invited
    if (!existing.length) {
      if (!inviteToken) {
        return res.status(400).json({ error: 'invite_token required to join' });
      }
      var { rows: invites } = await platformPool.query(
        `SELECT id, role FROM huddle_invites
         WHERE huddle_id = $1 AND token = $2 AND status = 'pending' AND expires_at > NOW()`,
        [huddleId, inviteToken]
      );
      if (!invites.length) return res.status(403).json({ error: 'Invalid or expired invite' });

      // Create membership row
      await platformPool.query(
        `INSERT INTO huddle_members (huddle_id, tenant_id, role, status, invited_by)
         VALUES ($1, $2, $3, 'invited', (SELECT invited_by FROM huddle_invites WHERE id = $4))`,
        [huddleId, req.tenant_id, invites[0].role, invites[0].id]
      );

      // Mark invite accepted
      await platformPool.query(
        `UPDATE huddle_invites SET status = 'accepted', accepted_at = NOW() WHERE id = $1`,
        [invites[0].id]
      );
    }

    var role = req.body.role || 'member';
    var result = await huddleEngine.join(huddleId, req.tenant_id, role);
    res.json({ mode: 'confirmed', ...result });
  } catch (err) {
    console.error('Huddle join error:', err.message);
    res.status(500).json({ error: 'Failed to join huddle' });
  }
});

// POST /api/huddles/:id/exit — clean exit
router.post('/api/huddles/:id/exit', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    await huddleEngine.exit(req.params.id, req.tenant_id);
    res.json({ status: 'detached' });
  } catch (err) {
    console.error('Huddle exit error:', err.message);
    res.status(500).json({ error: 'Failed to exit huddle' });
  }
});

// GET /api/huddles/:id/network — merged proximity graph with search/filter/paginate
router.get('/api/huddles/:id/network', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var page = Math.max(1, parseInt(req.query.page) || 1);
    var limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 50));
    var offset = (page - 1) * limit;
    var search = req.query.search || null;
    var minStrength = parseFloat(req.query.min_strength) || 0;
    var depthType = req.query.depth_type || null;
    var sortBy = req.query.sort === 'connections' ? 'member_connection_count' : 'best_strength_score';

    var conditions = ['hp.huddle_id = $1'];
    var params = [req.params.id];
    var paramIdx = 2;

    if (minStrength > 0) {
      conditions.push('hp.best_strength_score >= $' + paramIdx);
      params.push(minStrength);
      paramIdx++;
    }
    if (depthType) {
      conditions.push('hp.best_depth_type = $' + paramIdx);
      params.push(depthType);
      paramIdx++;
    }
    if (search) {
      conditions.push('(p.full_name ILIKE $' + paramIdx + ' OR p.current_company ILIKE $' + paramIdx + ' OR p.current_title ILIKE $' + paramIdx + ')');
      params.push('%' + search + '%');
      paramIdx++;
    }

    var where = conditions.join(' AND ');

    // Count total
    var countQuery = `SELECT COUNT(*) FROM huddle_people hp LEFT JOIN people p ON p.id = hp.person_id WHERE ${where}`;
    var { rows: countRows } = await platformPool.query(countQuery, params);
    var total = parseInt(countRows[0].count) || 0;

    // Fetch page — source_platform intentionally excluded
    var dataQuery = `
      SELECT hp.person_id, hp.best_member_tenant_id, hp.best_strength_score,
             hp.best_depth_type, hp.best_entry_label, hp.best_entry_reason,
             hp.member_connection_count, hp.total_team_interactions, hp.contributor_count,
             p.full_name, p.current_title, p.current_company, p.linkedin_url, p.location
      FROM huddle_people hp
      LEFT JOIN people p ON p.id = hp.person_id
      WHERE ${where}
      ORDER BY ${sortBy} DESC NULLS LAST
      LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`;
    params.push(limit, offset);

    var { rows } = await platformPool.query(dataQuery, params);

    res.json({
      people: rows,
      pagination: { page: page, limit: limit, total: total, pages: Math.ceil(total / limit) },
    });
  } catch (err) {
    console.error('Huddle network error:', err.message);
    res.status(500).json({ error: 'Failed to load network' });
  }
});

// GET /api/huddles/:id/network/:personId — single person entry point detail
router.get('/api/huddles/:id/network/:personId', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    // Person summary
    var { rows: personRows } = await platformPool.query(
      `SELECT hp.person_id, hp.best_member_tenant_id, hp.best_strength_score,
              hp.best_depth_type, hp.best_entry_label, hp.best_entry_reason,
              hp.member_connection_count, hp.total_team_interactions, hp.contributor_count,
              p.full_name, p.current_title, p.current_company, p.linkedin_url, p.location
       FROM huddle_people hp
       LEFT JOIN people p ON p.id = hp.person_id
       WHERE hp.huddle_id = $1 AND hp.person_id = $2`,
      [req.params.id, req.params.personId]
    );
    if (!personRows.length) return res.status(404).json({ error: 'Person not in this huddle' });

    // All member paths — source_platform intentionally excluded
    var { rows: paths } = await platformPool.query(
      `SELECT prox.member_tenant_id, prox.strength_score, prox.depth_type,
              prox.currency_label, prox.entry_recommendation, prox.entry_action,
              prox.last_contact, prox.interaction_count,
              t.name as member_name, t.slug as member_slug
       FROM huddle_proximity prox
       JOIN tenants t ON t.id = prox.member_tenant_id
       WHERE prox.huddle_id = $1 AND prox.person_id = $2
       ORDER BY prox.strength_score DESC`,
      [req.params.id, req.params.personId]
    );

    res.json({ person: personRows[0], paths: paths });
  } catch (err) {
    console.error('Person entry detail error:', err.message);
    res.status(500).json({ error: 'Failed to load person detail' });
  }
});

// GET /api/huddles/:id/signals — cross-tenant signal feed for a huddle
router.get('/api/huddles/:id/signals', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 20));
    var offset = parseInt(req.query.offset) || 0;
    var typeFilter = req.query.type || null;
    var regionFilter = req.query.region || null;

    // Load huddle mission config for filtering
    var { rows: [huddleData] } = await platformPool.query('SELECT signal_config FROM huddles WHERE id = $1', [req.params.id]);
    var cfg = huddleData?.signal_config || {};

    var conditions = ['(se.tenant_id IS NULL OR se.tenant_id = ANY(SELECT tenant_id FROM huddle_members WHERE huddle_id = $1 AND status = \'active\'))'];
    var params = [req.params.id];
    var paramIdx = 1;

    // Only recent signals with confirmed dates
    conditions.push("se.signal_date IS NOT NULL AND se.signal_date > NOW() - INTERVAL '90 days'");

    if (typeFilter) {
      paramIdx++;
      conditions.push('se.signal_type = $' + paramIdx + '::signal_type');
      params.push(typeFilter);
    }
    if (regionFilter && regionFilter !== 'all') {
      paramIdx++;
      conditions.push('(c.geography ILIKE $' + paramIdx + ' OR c.country_code = $' + paramIdx + ')');
      params.push('%' + regionFilter + '%');
    }

    // Mission filter — AND between signal_type and sector/geography
    if (!typeFilter) {
      if (cfg.signal_types?.length) { paramIdx++; conditions.push('se.signal_type = ANY($' + paramIdx + ')'); params.push(cfg.signal_types); }
      var contextOr = [];
      if (cfg.sectors?.length) { cfg.sectors.forEach(function(s) { paramIdx++; contextOr.push('c.sector ILIKE $' + paramIdx); params.push('%' + s + '%'); }); }
      if (cfg.geography?.length) { cfg.geography.forEach(function(g) { paramIdx++; contextOr.push('(c.geography ILIKE $' + paramIdx + ' OR c.country_code = $' + paramIdx + ')'); params.push(g); }); }
      if (contextOr.length > 0) conditions.push('(' + contextOr.join(' OR ') + ')');
    }

    var where = conditions.join(' AND ');

    paramIdx++;
    var limitParam = paramIdx;
    params.push(limit);
    paramIdx++;
    var offsetParam = paramIdx;
    params.push(offset);

    var query = `
      WITH member_tenants AS (
        SELECT hm.tenant_id, t.name AS tenant_name
        FROM huddle_members hm
        JOIN tenants t ON t.id = hm.tenant_id
        WHERE hm.huddle_id = $1 AND hm.status = 'active'
      ),
      client_matches AS (
        SELECT DISTINCT LOWER(co.name) AS company_lower, mt.tenant_name
        FROM companies co
        JOIN member_tenants mt ON co.tenant_id = mt.tenant_id
        WHERE co.is_client = true
      ),
      contact_counts AS (
        SELECT p.current_company_id, COUNT(*) AS cnt
        FROM people p
        WHERE p.tenant_id IN (SELECT tenant_id FROM member_tenants)
          AND p.current_company_id IS NOT NULL
        GROUP BY p.current_company_id
      )
      SELECT se.id, se.signal_type, se.company_name, se.company_id,
             se.confidence_score, se.evidence_summary, se.evidence_snippet,
             se.triage_status, se.detected_at, se.signal_date, se.source_url,
             se.signal_category, se.hiring_implications, se.image_url,
             c.sector, c.geography, c.country_code,
             COALESCE(cc.cnt, 0)::int AS combined_contact_count,
             cm.tenant_name AS client_via,
             CASE WHEN cm.tenant_name IS NOT NULL THEN true ELSE false END AS huddle_client
      FROM signal_events se
      LEFT JOIN companies c ON se.company_id = c.id AND c.tenant_id IS NULL
      LEFT JOIN client_matches cm ON LOWER(se.company_name) = cm.company_lower
      LEFT JOIN contact_counts cc ON cc.current_company_id = se.company_id
      WHERE ${where}
      ORDER BY
        CASE WHEN cm.tenant_name IS NOT NULL THEN 0 ELSE 1 END,
        COALESCE(cc.cnt, 0) DESC,
        CASE se.signal_type
          WHEN 'strategic_hiring' THEN 1
          WHEN 'geographic_expansion' THEN 2
          WHEN 'capital_raising' THEN 3
          WHEN 'product_launch' THEN 4
          WHEN 'partnership' THEN 5
          ELSE 6
        END,
        se.confidence_score DESC,
        se.signal_date DESC
      LIMIT $${limitParam} OFFSET $${offsetParam}`;

    var countQuery = `
      WITH member_tenants AS (
        SELECT hm.tenant_id, t.name AS tenant_name
        FROM huddle_members hm
        JOIN tenants t ON t.id = hm.tenant_id
        WHERE hm.huddle_id = $1 AND hm.status = 'active'
      )
      SELECT COUNT(*) FROM signal_events se
      LEFT JOIN companies c ON se.company_id = c.id AND c.tenant_id IS NULL
      WHERE ${where}`;

    var countParams = params.slice(0, params.length - 2); // exclude limit/offset

    var [signalsResult, countResult] = await Promise.all([
      platformPool.query(query, params),
      platformPool.query(countQuery, countParams)
    ]);

    var total = parseInt(countResult.rows[0].count) || 0;

    res.json({ signals: signalsResult.rows, total: total });
  } catch (err) {
    console.error('Huddle signals error:', err.message);
    res.status(500).json({ error: 'Failed to load huddle signals' });
  }
});

// GET /api/huddles/:id/companies — combined company graph for a huddle
router.get('/api/huddles/:id/companies', authenticateToken, async function(req, res) {
  try {
    var membership = await verifyHuddleMember(req.params.id, req.tenant_id);
    if (!membership) return res.status(403).json({ error: 'Not a member of this huddle' });

    var limit = Math.min(parseInt(req.query.limit) || 50, 200);
    var offset = parseInt(req.query.offset) || 0;
    var search = req.query.q || '';

    var { rows: members } = await platformPool.query(
      `SELECT hm.tenant_id, t.name as tenant_name FROM huddle_members hm JOIN tenants t ON t.id = hm.tenant_id WHERE hm.huddle_id = $1 AND hm.status = 'active'`,
      [req.params.id]
    );
    var memberTenantIds = members.map(function(m) { return m.tenant_id; });
    if (!memberTenantIds.length) return res.json({ companies: [], total: 0 });

    var searchWhere = search ? `AND c.name ILIKE '%' || $4 || '%'` : '';
    var params = [memberTenantIds, req.params.id, limit, offset];
    if (search) params.push(search);

    var { rows } = await platformPool.query(`
      WITH member_contacts AS (
        SELECT p.current_company_id AS company_id, p.tenant_id,
          COUNT(*) AS contact_count
        FROM people p
        WHERE p.tenant_id = ANY($1) AND p.current_company_id IS NOT NULL
        GROUP BY p.current_company_id, p.tenant_id
      ),
      company_agg AS (
        SELECT mc.company_id,
          SUM(mc.contact_count) AS combined_contacts,
          COUNT(DISTINCT mc.tenant_id) AS member_reach,
          array_agg(DISTINCT mc.tenant_id) AS contributing_tenants
        FROM member_contacts mc
        GROUP BY mc.company_id
      )
      SELECT c.id, c.name, c.sector, c.geography, c.domain,
        ca.combined_contacts, ca.member_reach,
        COALESCE(c.is_client, false) AS is_client,
        (SELECT array_agg(DISTINCT t.name) FROM companies c2
          JOIN tenants t ON t.id = c2.tenant_id
          WHERE c2.is_client = true AND LOWER(c2.name) = LOWER(c.name)
            AND c2.tenant_id = ANY($1)) AS client_via,
        (SELECT COUNT(*) FROM signal_events se
          WHERE se.company_id = c.id AND se.tenant_id IS NULL
          AND se.signal_date IS NOT NULL AND se.signal_date > NOW() - INTERVAL '90 days') AS signal_count
      FROM company_agg ca
      JOIN companies c ON c.id = ca.company_id
      WHERE ca.combined_contacts >= 2 ${searchWhere}
      ORDER BY
        (SELECT COUNT(*) FROM companies c2 WHERE c2.is_client = true AND LOWER(c2.name) = LOWER(c.name) AND c2.tenant_id = ANY($1)) > 0 DESC,
        ca.combined_contacts DESC,
        ca.member_reach DESC
      LIMIT $3 OFFSET $4
    `, search ? [memberTenantIds, req.params.id, limit, offset, search] : [memberTenantIds, req.params.id, limit, offset]);

    var { rows: [countRow] } = await platformPool.query(`
      SELECT COUNT(DISTINCT p.current_company_id) AS total
      FROM people p
      WHERE p.tenant_id = ANY($1) AND p.current_company_id IS NOT NULL
    `, [memberTenantIds]);

    res.json({ companies: rows, total: parseInt(countRow.total) });
  } catch (err) {
    console.error('Huddle companies error:', err.message);
    res.status(500).json({ error: 'Failed to load huddle companies' });
  }
});

// GET /api/me/influence — individual influence dashboard
router.get('/api/me/influence', authenticateToken, async function(req, res) {
  try {
    // Core influence stats
    var { rows: influence } = await platformPool.query(
      'SELECT * FROM individual_influence WHERE tenant_id = $1',
      [req.tenant_id]
    );

    // Active huddle memberships
    var { rows: huddles } = await platformPool.query(
      `SELECT h.id, h.name, h.slug, hm.role,
              hm.contributed_people_count, hm.net_new_people_count,
              (SELECT COUNT(*) FROM huddle_people WHERE huddle_id = h.id) as huddle_people_count
       FROM huddle_members hm
       JOIN huddles h ON h.id = hm.huddle_id
       WHERE hm.tenant_id = $1 AND hm.status = 'active' AND h.status = 'active'
       ORDER BY h.updated_at DESC`,
      [req.tenant_id]
    );

    // Proximity summary from sandbox
    var db = new TenantDB(req.tenant_id);
    var { rows: proxStats } = await db.query(
      `SELECT COUNT(*) as total_people,
              ROUND(AVG(strength_score)::numeric, 3) as avg_strength,
              COUNT(*) FILTER (WHERE strength_score >= 0.7) as strong_count,
              COUNT(*) FILTER (WHERE strength_score >= 0.45 AND strength_score < 0.7) as warm_count,
              COUNT(*) FILTER (WHERE strength_score >= 0.2 AND strength_score < 0.45) as cool_count,
              COUNT(*) FILTER (WHERE strength_score < 0.2) as cold_count
       FROM person_proximity`
    );

    res.json({
      influence: influence[0] || null,
      huddles: huddles,
      proximity_summary: proxStats[0] || null,
    });
  } catch (err) {
    console.error('Influence dashboard error:', err.message);
    res.status(500).json({ error: 'Failed to load influence data' });
  }
});

// GET /api/me/export — GDPR data export (download all user's tenant data as JSON)
router.get('/api/me/export', authenticateToken, async function(req, res) {
  try {
    var db = new TenantDB(req.tenant_id);

    var [people, companies, interactions, signalEvents, proximity, feedSubs, tenantProfile] = await Promise.all([
      db.query('SELECT * FROM people'),
      db.query('SELECT * FROM companies'),
      db.query('SELECT * FROM interactions'),
      db.query("SELECT * FROM signal_events WHERE created_at >= NOW() - INTERVAL '90 days' ORDER BY created_at DESC"),
      db.query('SELECT * FROM person_proximity'),
      db.query('SELECT * FROM feed_subscriptions'),
      platformPool.query('SELECT id, name, slug, domain, created_at FROM tenants WHERE id = $1', [req.tenant_id]),
    ]);

    var exportData = {
      exported_at: new Date().toISOString(),
      tenant_id: req.tenant_id,
      tenant_profile: tenantProfile.rows[0] || null,
      people: people.rows,
      companies: companies.rows,
      interactions: interactions.rows,
      signal_events_last_90_days: signalEvents.rows,
      team_proximity: proximity.rows,
      feed_subscriptions: feedSubs.rows,
    };

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', 'attachment; filename="data-export-' + req.tenant_id + '-' + Date.now() + '.json"');
    res.json(exportData);
  } catch (err) {
    console.error('GDPR export error:', err.message);
    res.status(500).json({ error: 'Failed to export data' });
  }
});

// Public embed routes moved to routes/public.js


  // ═══════════════════════════════════════════════════════════════════════════
  // DISPATCHES + PLACEMENTS
  // ═══════════════════════════════════════════════════════════════════════════

router.post('/api/dispatches/generate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { generateDispatches } = require('../scripts/generate_dispatches');
    res.json({ status: 'started', message: 'Dispatch generation triggered' });
    generateDispatches().then(r => console.log('Dispatch generation complete:', r)).catch(e => console.error('Dispatch generation failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger dispatch generation: ' + err.message });
  }
});

// Generate dispatch for a specific signal (full pipeline — proximity, blog, distribution)
router.post('/api/dispatches/generate-for-signal', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { signal_id } = req.body;
    if (!signal_id) return res.status(400).json({ error: 'signal_id required' });

    // Fetch full signal with company context
    const { rows: [signal] } = await db.query(
      `SELECT se.id, se.company_id, se.company_name, se.signal_type,
              se.evidence_summary, se.confidence_score, se.source_url,
              c.sector, c.geography, c.employee_count_band
       FROM signal_events se
       LEFT JOIN companies c ON c.id = se.company_id
       WHERE se.id = $1 AND se.tenant_id = $2`,
      [signal_id, req.tenant_id]
    );
    if (!signal) return res.status(404).json({ error: 'Signal not found' });

    // Run the full generation pipeline (async — respond immediately)
    res.json({ message: 'Dispatch generation started', signal_id: signal.id });

    // Generate in background (don't block the response)
    const { generateForSignal } = require('../scripts/generate_dispatches');
    generateForSignal(signal).catch(err => {
      console.error(`Dispatch generation failed for ${signal.company_name}:`, err.message);
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Claim a dispatch
router.post('/api/dispatches/:id/claim', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Check if already claimed
    const { rows: [dispatch] } = await db.query(
      'SELECT id, claimed_by, status FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]
    );
    if (!dispatch) return res.status(404).json({ error: 'Dispatch not found' });

    if (dispatch.claimed_by && dispatch.claimed_by !== req.user?.user_id) {
      // Already claimed by someone else
      const { rows: [claimer] } = await db.query('SELECT name FROM users WHERE id = $1', [dispatch.claimed_by]);
      return res.status(409).json({
        error: 'Already claimed',
        claimed_by: claimer?.name || 'another user',
        message: `This dispatch has been claimed by ${claimer?.name || 'another team member'}`
      });
    }

    const { rows: [updated] } = await db.query(`
      UPDATE signal_dispatches
      SET claimed_by = $2, claimed_at = NOW(), status = CASE WHEN status = 'draft' THEN 'claimed' ELSE status END, updated_at = NOW()
      WHERE id = $1 AND tenant_id = $3
      RETURNING *
    `, [req.params.id, req.user?.user_id, req.tenant_id]);

    res.json({ dispatch: updated, claimed_by: req.user?.name });
  } catch (err) {
    console.error('Claim error:', err.message);
    res.status(500).json({ error: 'Failed to claim dispatch' });
  }
});

// Unclaim a dispatch
router.post('/api/dispatches/:id/unclaim', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [updated] } = await db.query(`
      UPDATE signal_dispatches
      SET claimed_by = NULL, claimed_at = NULL, status = 'draft', updated_at = NOW()
      WHERE id = $1 AND (claimed_by = $2 OR claimed_by IS NULL) AND tenant_id = $3
      RETURNING *
    `, [req.params.id, req.user?.user_id, req.tenant_id]);

    if (!updated) return res.status(403).json({ error: 'Can only unclaim your own dispatches' });
    res.json({ dispatch: updated });
  } catch (err) {
    console.error('Unclaim error:', err.message);
    res.status(500).json({ error: 'Failed to unclaim dispatch' });
  }
});

// Rescan proximity maps for existing dispatches
router.post('/api/dispatches/rescan', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rescanProximity } = require('../scripts/generate_dispatches');
    res.json({ status: 'started', message: 'Proximity rescan triggered' });
    rescanProximity().then(r => console.log('Proximity rescan complete:', r)).catch(e => console.error('Proximity rescan failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger rescan: ' + err.message });
  }
});

// List dispatches
router.get('/api/dispatches', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const status = req.query.status;
    const region = req.query.region;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const offset = parseInt(req.query.offset) || 0;

    let where = 'WHERE sd.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (status) {
      idx++; where += ` AND sd.status = $${idx}`; params.push(status);
    }

    // Region filter — match on company geography + country_code, with fallback to text search
    if (region && region !== 'all') {
      const regionCodes = REGION_CODES[region] || [];
      const geoNames = REGION_MAP[region] || [];
      const orParts = [];
      // Primary: match company geography or country_code
      regionCodes.forEach(code => { idx++; orParts.push(`c.geography ILIKE $${idx}`); params.push(`%${code}%`); });
      geoNames.slice(0, 5).forEach(g => { idx++; orParts.push(`c.geography ILIKE $${idx}`); params.push(`%${g}%`); });
      // Fallback: match signal summary text
      geoNames.slice(0, 3).forEach(g => { idx++; orParts.push(`sd.signal_summary ILIKE $${idx}`); params.push(`%${g}%`); });
      if (orParts.length > 0) where += ` AND (${orParts.join(' OR ')})`;
    }

    idx++; params.push(limit);
    idx++; params.push(offset);

    const [result, countResult] = await Promise.all([
      db.query(`
        SELECT sd.id, sd.signal_event_id, sd.company_id, sd.company_name,
               sd.signal_type, sd.signal_summary,
               sd.opportunity_angle, sd.blog_title, sd.blog_theme,
               sd.status, sd.generated_at, sd.reviewed_at, sd.sent_at,
               sd.best_entry_point, sd.proximity_map, sd.approach_rationale,
               sd.claimed_by, sd.claimed_at, u_claim.name AS claimed_by_name,
               jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) AS connection_count,
               jsonb_array_length(COALESCE(sd.send_to, '[]'::jsonb)) AS recipient_count,
               c.sector, c.geography, c.is_client,
               (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = sd.company_id AND p2.tenant_id = $1) AS people_at_company,
               (SELECT COUNT(*) FROM conversions pl JOIN accounts cl ON cl.id = pl.client_id AND cl.tenant_id = $1
                WHERE cl.company_id = sd.company_id AND pl.tenant_id = $1) AS placement_count
        FROM signal_dispatches sd
        LEFT JOIN companies c ON c.id = sd.company_id
        LEFT JOIN users u_claim ON u_claim.id = sd.claimed_by
        ${where}
        ORDER BY
          CASE WHEN c.company_tier = 'megacap_indicator' THEN 1 ELSE 0 END,
          CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
          CASE WHEN jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) > 0 THEN 0 ELSE 1 END,
          sd.generated_at DESC
        LIMIT $${idx - 1} OFFSET $${idx}
      `, params),
      db.query(`SELECT COUNT(*) AS cnt FROM signal_dispatches sd LEFT JOIN companies c ON c.id = sd.company_id ${where}`, params.slice(0, -2))
    ]);

    res.json({
      dispatches: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit, offset
    });
  } catch (err) {
    console.error('Dispatches list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch dispatches' });
  }
});

// Get single dispatch
router.get('/api/dispatches/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT sd.*,
             c.sector, c.geography, c.is_client, c.employee_count_band, c.domain,
             se.confidence_score AS signal_confidence,
             se.evidence_snippets, se.hiring_implications,
             se.detected_at AS signal_detected_at, se.signal_date,
             u_claim.name AS claimed_by_name
      FROM signal_dispatches sd
      LEFT JOIN companies c ON c.id = sd.company_id
      LEFT JOIN signal_events se ON se.id = sd.signal_event_id
      LEFT JOIN users u_claim ON u_claim.id = sd.claimed_by
      WHERE sd.id = $1 AND sd.tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });
    const dispatch = rows[0];

    // Auto-bundle relevant case studies
    let relevant_case_studies = [];
    try {
      const scoreTerms = [];
      const csParams = [req.tenant_id];
      let csIdx = 1;

      if (dispatch.sector) {
        csIdx++; csParams.push(`%${dispatch.sector}%`);
        scoreTerms.push(`CASE WHEN cs.sector ILIKE $${csIdx} THEN 0.3 ELSE 0 END`);
      }
      if (dispatch.geography) {
        csIdx++; csParams.push(`%${dispatch.geography}%`);
        scoreTerms.push(`CASE WHEN cs.geography ILIKE $${csIdx} THEN 0.25 ELSE 0 END`);
      }
      if (dispatch.signal_type) {
        const sigThemes = {
          capital_raising: ['high-growth','scaling'], geographic_expansion: ['cross-border','expansion'],
          strategic_hiring: ['leadership','team-build'], ma_activity: ['post-acquisition','integration'],
          leadership_change: ['succession','transition'], restructuring: ['turnaround','transformation'],
        };
        const themes = sigThemes[dispatch.signal_type] || [];
        if (themes.length) {
          csIdx++; csParams.push(themes);
          scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${csIdx}::text[]))::float * 0.25`);
        }
      }
      if (dispatch.company_id) {
        csIdx++; csParams.push(dispatch.company_id);
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

    res.json({ ...dispatch, relevant_case_studies });
  } catch (err) {
    console.error('Dispatch detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch dispatch' });
  }
});

// Update dispatch status
router.patch('/api/dispatches/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status, send_to, blog_body, blog_title } = req.body;
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id];
    let idx = 1;

    if (status) {
      idx++; updates.push(`status = $${idx}`); params.push(status);
      if (status === 'reviewed') { updates.push('reviewed_at = NOW()'); }
      if (status === 'sent') { updates.push('sent_at = NOW()'); }
    }
    if (send_to !== undefined) {
      idx++; updates.push(`send_to = $${idx}`); params.push(JSON.stringify(send_to));
    }
    if (blog_body) {
      idx++; updates.push(`blog_body = $${idx}`); params.push(blog_body);
    }
    if (blog_title) {
      idx++; updates.push(`blog_title = $${idx}`); params.push(blog_title);
    }

    idx++;
    params.push(req.tenant_id);
    const { rows } = await db.query(
      `UPDATE signal_dispatches SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $${idx} RETURNING *`,
      params
    );

    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Dispatch update error:', err.message);
    res.status(500).json({ error: 'Failed to update dispatch' });
  }
});

// Regenerate blog post for a dispatch
router.post('/api/dispatches/:id/regenerate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query('SELECT * FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });

    const dispatch = rows[0];
    const themeOverride = req.body.theme;

    // Get company info
    let company = {};
    if (dispatch.company_id) {
      const { rows: [co] } = await db.query(
        'SELECT sector, geography, employee_count_band FROM companies WHERE id = $1 AND tenant_id = $2',
        [dispatch.company_id, req.tenant_id]
      );
      if (co) company = co;
    }

    // Regenerate via Claude
    const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
    if (!ANTHROPIC_API_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });

    const signalType = dispatch.signal_type || 'market_signal';
    const theme = themeOverride || dispatch.blog_theme || 'executive talent strategy';

    const systemPrompt = `You are a senior executive search consultant writing a thought leadership piece for an executive audience.
Write with authority and insight, not sales language.
The piece should be genuinely useful to a senior leader at a company that has just experienced a ${signalType.replace(/_/g, ' ')} event.
It should feel like advice from a trusted advisor, not a pitch from a recruiter.
Length: 550-700 words.
Format: Return ONLY valid JSON with keys: "title", "body", "keywords"
  - title: Compelling headline
  - body: 4-5 paragraphs of flowing prose. No subheadings, no bullet points. Use \\n\\n between paragraphs.
  - keywords: Array of 4-6 relevant keywords/phrases
Tone: Warm, direct, intelligent. First person plural ("we've seen").
Do not mention the company by name or the specific event.
Do not use the word "landscape" or "navigate".`;

    const userPrompt = `Write a thought leadership article for a senior leader at a ${company.sector || 'technology'} company (${company.employee_count_band || 'growth-stage'}, ${company.geography || 'global'} market) that has just experienced a ${signalType.replace(/_/g, ' ')} event.

The article should explore the theme: "${theme}"

Signal context: ${dispatch.signal_summary || signalType.replace(/_/g, ' ')}
Approach angle: ${dispatch.approach_rationale || 'Market intelligence and talent advisory'}

The article should leave the reader thinking about talent, leadership, and organisational design.

Return valid JSON only.`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
        system: systemPrompt,
        messages: [{ role: 'user', content: userPrompt }]
      })
    });

    if (!response.ok) {
      const err = await response.text();
      return res.status(500).json({ error: `Claude API failed: ${err.slice(0, 200)}` });
    }

    const data = await response.json();
    const raw = data.content[0]?.text || '';

    let blog;
    try {
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      blog = JSON.parse(jsonMatch[0]);
    } catch (e) {
      blog = { title: theme, body: raw, keywords: [] };
    }

    // Update dispatch
    await db.query(`
      UPDATE signal_dispatches
      SET blog_theme = $2, blog_title = $3, blog_body = $4, blog_keywords = $5, updated_at = NOW()
      WHERE id = $1 AND tenant_id = $6
    `, [dispatch.id, theme, blog.title, blog.body, blog.keywords || [], req.tenant_id]);

    res.json({ title: blog.title, body: blog.body, keywords: blog.keywords });
  } catch (err) {
    console.error('Blog regeneration error:', err.message);
    res.status(500).json({ error: 'Failed to regenerate blog' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERSIONS / REVENUE
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/placements', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    // Sorting
    const sortMap = {
      date: 'pl.start_date', fee: 'pl.placement_fee',
      client: 'COALESCE(cl.name, pl.client_name_raw)',
      candidate: 'COALESCE(pe.full_name, pl.consultant_name)',
      role: 'pl.role_title',
    };
    const sortCol = sortMap[req.query.sort] || 'pl.start_date';
    const sortDir = req.query.order === 'asc' ? 'ASC' : 'DESC';

    let where = 'WHERE pl.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    if (req.query.source) {
      const validSources = ['xero', 'xero_export', 'wip_workbook', 'manual', 'myob_import'];
      const sources = req.query.source.split(',').filter(s => validSources.includes(s));
      if (sources.length) {
        paramIdx++;
        where += ` AND pl.source = ANY($${paramIdx})`;
        params.push(sources);
      }
    }
    if (req.query.currency) {
      paramIdx++;
      where += ` AND UPPER(COALESCE(pl.currency, 'AUD')) = $${paramIdx}`;
      params.push(req.query.currency.toUpperCase());
    }
    if (q) {
      paramIdx++;
      where += ` AND (pe.full_name ILIKE $${paramIdx} OR pl.role_title ILIKE $${paramIdx} OR cl.name ILIKE $${paramIdx} OR pl.client_name_raw ILIKE $${paramIdx})`;
      params.push(`%${q}%`);
    }
    if (req.query.company_id) {
      paramIdx++;
      where += ` AND (cl.company_id = $${paramIdx} OR cl.id = $${paramIdx})`;
      params.push(req.query.company_id);
    }
    if (req.query.year) {
      paramIdx++;
      where += ` AND EXTRACT(YEAR FROM pl.start_date) = $${paramIdx}`;
      params.push(parseInt(req.query.year));
    }

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const groupByProject = req.query.group !== 'invoices';

    let placementsResult, statsResult;

    if (groupByProject) {
      // Project view: roll up invoices by client + role into single project rows
      const projectSortMap = {
        date: 'last_invoice_date', fee: 'project_fee',
        client: 'company_name', candidate: 'candidate_name', role: 'role_title',
      };
      const pSortCol = projectSortMap[req.query.sort] || 'last_invoice_date';

      [placementsResult, statsResult] = await Promise.all([
        db.query(`
          WITH project_groups AS (
            SELECT
              COALESCE(pl.client_id, pl.company_id) AS group_client_id,
              pl.role_title,
              (array_agg(pl.id ORDER BY pl.start_date DESC NULLS LAST))[1] AS id,
              MAX(pe.full_name) AS candidate_name,
              (array_agg(pl.person_id ORDER BY pl.start_date DESC NULLS LAST) FILTER (WHERE pl.person_id IS NOT NULL))[1] AS person_id,
              SUM(pl.placement_fee) AS project_fee,
              MAX(pl.start_date) AS last_invoice_date,
              COUNT(*) AS invoice_count,
              array_agg(pl.invoice_number ORDER BY pl.start_date) AS invoice_numbers,
              COALESCE((array_agg(cl.id ORDER BY pl.start_date DESC NULLS LAST) FILTER (WHERE cl.id IS NOT NULL))[1], (array_agg(pl.company_id) FILTER (WHERE pl.company_id IS NOT NULL))[1]) AS company_id,
              COALESCE(MAX(cl.name), MAX(pl.client_name_raw)) AS company_name,
              MAX(co.sector) AS company_sector,
              MAX(pl.source) AS source,
              MAX(pl.payment_status) AS payment_status,
              UPPER(COALESCE(MAX(pl.currency), 'AUD')) AS currency
            FROM conversions pl
            LEFT JOIN accounts cl ON pl.client_id = cl.id
            LEFT JOIN companies co ON cl.company_id = co.id
            LEFT JOIN people pe ON pl.person_id = pe.id
            ${where}
            GROUP BY COALESCE(pl.client_id, pl.company_id), pl.role_title
          )
          SELECT *, project_fee AS placement_fee, last_invoice_date AS start_date
          FROM project_groups
          ORDER BY ${pSortCol} ${sortDir} NULLS LAST
          LIMIT $${limitIdx} OFFSET $${offsetIdx}
        `, params),
        db.query(`
          WITH project_groups AS (
            SELECT COALESCE(pl.client_id, pl.company_id) AS group_client_id,
                   pl.role_title, SUM(pl.placement_fee) AS project_fee
            FROM conversions pl
            LEFT JOIN accounts cl ON pl.client_id = cl.id
            LEFT JOIN people pe ON pl.person_id = pe.id
            ${where}
            GROUP BY COALESCE(pl.client_id, pl.company_id), pl.role_title
          )
          SELECT COUNT(*) AS total_count,
                 COALESCE(SUM(project_fee), 0) AS total_revenue,
                 COUNT(DISTINCT group_client_id) AS client_count
          FROM project_groups
        `, params.slice(0, -2)),
      ]);

      // Add date range from raw data
      const { rows: [dr] } = await db.query(`
        SELECT MIN(pl.start_date) AS earliest, MAX(pl.start_date) AS latest
        FROM conversions pl LEFT JOIN accounts cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id ${where}
      `, params.slice(0, -2));
      statsResult.rows[0].earliest = dr.earliest;
      statsResult.rows[0].latest = dr.latest;
    } else {
      // Invoice view: one row per invoice (original behaviour)
      [placementsResult, statsResult] = await Promise.all([
        db.query(`
          SELECT pl.id, COALESCE(pe.full_name, pl.consultant_name) AS candidate_name, pl.person_id,
                 pl.role_title, pl.start_date,
                 pl.placement_fee, pl.fee_category, pl.fee_type, pl.invoice_number,
                 COALESCE(cl.id, pl.company_id) AS company_id,
                 COALESCE(cl.name, pl.client_name_raw) AS company_name,
                 co.sector AS company_sector,
                 pl.source, pl.payment_status, 1 AS invoice_count,
                 UPPER(COALESCE(pl.currency, 'AUD')) AS currency
          FROM conversions pl
          LEFT JOIN accounts cl ON pl.client_id = cl.id
          LEFT JOIN companies co ON cl.company_id = co.id
          LEFT JOIN people pe ON pl.person_id = pe.id
          ${where}
          ORDER BY ${sortCol} ${sortDir} NULLS LAST
          LIMIT $${limitIdx} OFFSET $${offsetIdx}
        `, params),
        db.query(`
          SELECT COUNT(*) AS total_count,
                 COALESCE(SUM(pl.placement_fee), 0) AS total_revenue,
                 COUNT(DISTINCT COALESCE(pl.client_id::text, pl.client_name_raw)) AS client_count,
                 MIN(pl.start_date) AS earliest,
                 MAX(pl.start_date) AS latest
          FROM conversions pl
          LEFT JOIN accounts cl ON pl.client_id = cl.id
          LEFT JOIN people pe ON pl.person_id = pe.id
          ${where}
        `, params.slice(0, -2)),
      ]);
    }

    // Build source + currency filter for sidebar queries
    const sideParams = [req.tenant_id];
    let sourceWhere = '';
    let sideIdx = 1;
    if (req.query.source) {
      const validSources = ['xero', 'xero_export', 'wip_workbook', 'manual', 'myob_import'];
      const sources = req.query.source.split(',').filter(s => validSources.includes(s));
      if (sources.length) { sideIdx++; sourceWhere += ` AND source = ANY($${sideIdx})`; sideParams.push(sources); }
    }
    if (req.query.currency) {
      sideIdx++;
      sourceWhere += ` AND UPPER(COALESCE(currency, 'AUD')) = $${sideIdx}`;
      sideParams.push(req.query.currency.toUpperCase());
    }

    // Currency breakdown (always unfiltered by currency, but respects source filter)
    const currSideParams = [req.tenant_id];
    let currSourceWhere = '';
    if (req.query.source) {
      const validSources = ['xero', 'xero_export', 'wip_workbook', 'manual', 'myob_import'];
      const sources = req.query.source.split(',').filter(s => validSources.includes(s));
      if (sources.length) { currSourceWhere = ' AND source = ANY($2)'; currSideParams.push(sources); }
    }
    const { rows: currencyBreakdown } = await db.query(`
      SELECT UPPER(COALESCE(currency, 'AUD')) AS currency, COUNT(*) AS count,
             COALESCE(SUM(placement_fee), 0) AS revenue
      FROM conversions WHERE tenant_id = $1 AND placement_fee IS NOT NULL${currSourceWhere}
      GROUP BY UPPER(COALESCE(currency, 'AUD')) ORDER BY revenue DESC
    `, currSideParams);

    // FX conversion for mixed-currency aggregation (to AUD)
    const fxCase = req.query.currency ? 'placement_fee' :
      `placement_fee * CASE UPPER(COALESCE(currency, 'AUD'))
        WHEN 'AUD' THEN 1 WHEN 'USD' THEN 1.55 WHEN 'GBP' THEN 2.05
        WHEN 'EUR' THEN 1.72 WHEN 'SGD' THEN 1.18 WHEN 'NZD' THEN 0.92 ELSE 1 END`;
    const fxCasePl = fxCase.replace(/placement_fee/g, 'pl.placement_fee').replace(/currency/g, 'pl.currency');

    // Revenue by year
    const { rows: byYear } = await db.query(`
      SELECT EXTRACT(YEAR FROM start_date)::int AS year,
             COUNT(*) AS count,
             COALESCE(SUM(${fxCase}), 0) AS revenue
      FROM conversions
      WHERE start_date IS NOT NULL AND tenant_id = $1${sourceWhere}
      GROUP BY year ORDER BY year DESC
    `, sideParams);

    // Top clients by revenue
    const { rows: topClients } = await db.query(`
      SELECT COALESCE(cl.id, pl.company_id) AS id,
             COALESCE(cl.name, pl.client_name_raw) AS name,
             COUNT(*) AS placement_count,
             COALESCE(SUM(${fxCasePl}), 0) AS total_revenue
      FROM conversions pl
      LEFT JOIN accounts cl ON pl.client_id = cl.id
      WHERE pl.tenant_id = $1${sourceWhere.replace(/source/g, 'pl.source').replace(/currency/g, 'pl.currency')}
      GROUP BY COALESCE(cl.id, pl.company_id), COALESCE(cl.name, pl.client_name_raw)
      HAVING COALESCE(cl.name, pl.client_name_raw) IS NOT NULL
      ORDER BY total_revenue DESC LIMIT 20
    `, sideParams);

    const stats = statsResult.rows[0];
    res.json({
      placements: placementsResult.rows,
      total: parseInt(stats.total_count),
      total_revenue: parseFloat(stats.total_revenue),
      client_count: parseInt(stats.client_count || 0),
      date_range: { earliest: stats.earliest, latest: stats.latest },
      by_year: byYear,
      top_clients: topClients,
      currency_breakdown: currencyBreakdown,
      limit, offset,
    });
  } catch (err) {
    console.error('Placements error:', err.message);
    res.status(500).json({ error: 'Failed to fetch placements' });
  }
});

router.patch('/api/placements/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { person_id, role_title, start_date, placement_fee, payment_status } = req.body;
    const sets = [];
    const vals = [];
    let idx = 1;

    if (person_id !== undefined) { sets.push(`person_id = $${idx++}`); vals.push(person_id || null); }
    if (role_title !== undefined) { sets.push(`role_title = $${idx++}`); vals.push(role_title); }
    if (start_date !== undefined) { sets.push(`start_date = $${idx++}`); vals.push(start_date || null); }
    if (placement_fee !== undefined) { sets.push(`placement_fee = $${idx++}`); vals.push(placement_fee); }
    if (payment_status !== undefined) { sets.push(`payment_status = $${idx++}`); vals.push(payment_status); }

    if (sets.length === 0) return res.status(400).json({ error: 'No fields to update' });

    sets.push(`updated_at = NOW()`);
    vals.push(req.params.id, req.tenant_id);

    const { rows } = await db.query(
      `UPDATE conversions SET ${sets.join(', ')} WHERE id = $${idx++} AND tenant_id = $${idx} RETURNING id`,
      vals
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });

    await auditLog(req.user.user_id, 'placement_update', 'conversions', req.params.id,
      req.body, req.ip);
    res.json({ ok: true, id: rows[0].id });
  } catch (err) {
    console.error('Placement update error:', err.message);
    res.status(500).json({ error: 'Failed to update placement' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// AI CHAT CONCIERGE
// ═══════════════════════════════════════════════════════════════════════════════

const multer = require('multer');
const fsChat = require('fs');
const chatUpload = multer({ dest: '/tmp/ml-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });

const chatHistories = new Map();
const MAX_HISTORY = 40;
function getChatHistory(userId) {
  if (!chatHistories.has(userId)) chatHistories.set(userId, []);
  return chatHistories.get(userId);
}

async function callClaude(messages, tools, systemPrompt, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await new Promise((resolve, reject) => {
        const body = JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 4096, system: systemPrompt, messages, tools });
        const req = https.request({
          hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) },
          timeout: 90000,
        }, (res) => {
          const chunks = [];
          res.on('data', c => chunks.push(c));
          res.on('end', () => {
            try {
              const raw = Buffer.concat(chunks).toString();
              const d = JSON.parse(raw);
              if (d.error) {
                // Retry on overloaded (529)
                if (d.error.type === 'overloaded_error' || d.error.message?.includes('overloaded')) {
                  return reject(new Error('RETRY:overloaded'));
                }
                return reject(new Error(d.error.message));
              }
              resolve(d);
            } catch (e) { reject(e); }
          });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Claude timeout')); });
        req.write(body);
        req.end();
      });
      return result;
    } catch (e) {
      if (e.message.startsWith('RETRY:') && attempt < retries) {
        console.log(`Claude overloaded, retrying in ${(attempt + 1) * 3}s...`);
        await new Promise(r => setTimeout(r, (attempt + 1) * 3000));
        continue;
      }
      throw new Error(e.message.replace('RETRY:', ''));
    }
  }
}


  // ═══════════════════════════════════════════════════════════════════════════
  // NETWORK TOPOLOGY
  // ═══════════════════════════════════════════════════════════════════════════

router.get('/api/network/opportunities', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const region = req.query.region;
    const minScore = parseFloat(req.query.min_score) || 0;
    const status = req.query.status || 'active';

    let where = 'WHERE ro.status = $1';
    const params = [status];
    let idx = 1;

    if (region && region !== 'all') {
      idx++; where += ` AND ro.region_code = $${idx}`; params.push(region);
    }
    if (minScore > 0) {
      idx++; where += ` AND ro.composite_score >= $${idx}`; params.push(minScore);
    }

    idx++; params.push(limit);
    idx++; params.push(offset);

    const [result, countResult] = await Promise.all([
      db.query(`
        SELECT ro.*,
               cas.contact_count, cas.senior_contact_count, cas.active_contact_count,
               cas.adjacency_score,
               gp.region_name, gp.weight_boost, gp.is_home_market
        FROM ranked_opportunities ro
        LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
        LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
        ${where}
        ORDER BY ro.composite_score DESC
        LIMIT $${idx - 1} OFFSET $${idx}
      `, params),
      db.query(`SELECT COUNT(*) AS cnt FROM ranked_opportunities ro ${where}`, params.slice(0, -2))
    ]);

    res.json({
      opportunities: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit, offset
    });
  } catch (err) {
    console.error('Opportunities error:', err.message);
    res.status(500).json({ error: 'Failed to fetch opportunities' });
  }
});

// Top opportunities by region
router.get('/api/network/opportunities/by-region', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const perRegion = Math.min(parseInt(req.query.per_region) || 5, 20);

    const { rows } = await db.query(`
      SELECT ro.*,
             cas.contact_count, cas.senior_contact_count, cas.active_contact_count,
             gp.region_name, gp.weight_boost, gp.is_home_market
      FROM ranked_opportunities ro
      LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
      LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
      WHERE ro.status = 'active' AND ro.rank_in_region <= $1
        AND ro.region_code IS NOT NULL AND ro.region_code != 'UNKNOWN'
      ORDER BY gp.weight_boost DESC NULLS LAST, ro.rank_in_region ASC
    `, [perRegion]);

    // Group by region
    const grouped = {};
    for (const row of rows) {
      const rc = row.region_code;
      if (!grouped[rc]) {
        grouped[rc] = {
          region_code: rc,
          region_name: row.region_name,
          weight_boost: row.weight_boost,
          is_home_market: row.is_home_market,
          opportunities: []
        };
      }
      grouped[rc].opportunities.push(row);
    }

    res.json(grouped);
  } catch (err) {
    console.error('Opportunities by-region error:', err.message);
    res.status(500).json({ error: 'Failed to fetch opportunities by region' });
  }
});

// Network density scores
router.get('/api/network/density', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT nds.*, gp.region_name, gp.weight_boost, gp.is_home_market
      FROM network_density_scores nds
      LEFT JOIN geo_priorities gp ON gp.region_code = nds.region_code
      WHERE nds.sector IS NULL
      ORDER BY nds.density_score DESC
    `);
    res.json(rows);
  } catch (err) {
    console.error('Network density error:', err.message);
    res.status(500).json({ error: 'Failed to fetch density scores' });
  }
});

// Full network graph — team nodes, top contacts, client companies, sector clusters
router.get('/api/network/graph', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenantId = req.tenant_id;
    const mode = req.query.mode || 'firm'; // 'firm' or 'signal'
    const signalId = req.query.signal_id;

    if (mode === 'signal' && signalId) {
      // Delegate to proximity-graph endpoint logic
      return res.redirect(`/api/signals/${signalId}/proximity-graph`);
    }

    // Firm-wide network graph
    const [teamResult, contactsResult, clientsResult, sectorsResult, signalsResult] = await Promise.all([
      // Team members
      db.query(`SELECT id, name, email, role FROM users WHERE tenant_id = $1 ORDER BY name`, [tenantId]),

      // Top contacts by proximity strength (limit to strongest connections)
      db.query(`
        SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
               p.seniority_level, p.location,
               tp.team_member_id, tp.relationship_strength, tp.relationship_type,
               u.name as connector_name,
               ps.timing_score, ps.receptivity_score, ps.flight_risk_score
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
        JOIN users u ON u.id = tp.team_member_id
        LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
        WHERE tp.tenant_id = $1 AND tp.relationship_strength >= 0.3
        ORDER BY tp.relationship_strength DESC
        LIMIT 150
      `, [tenantId]),

      // Client companies with signal + people counts
      db.query(`
        SELECT a.id as account_id, a.name, a.relationship_tier, a.company_id,
               c.sector, c.geography,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = a.company_id AND p.tenant_id = $1) as people_count,
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = a.company_id AND (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '30 days') as signal_count,
               (SELECT COALESCE(SUM(cv.placement_fee), 0) FROM conversions cv WHERE cv.client_id = a.id AND cv.tenant_id = $1) as total_revenue
        FROM accounts a
        LEFT JOIN companies c ON c.id = a.company_id
        WHERE a.tenant_id = $1 AND a.relationship_status = 'active'
        ORDER BY a.relationship_tier DESC NULLS LAST, a.name
        LIMIT 50
      `, [tenantId]),

      // Sector clusters (from network density)
      db.query(`
        SELECT region_code, sector, total_contacts, active_contacts, senior_contacts, density_score
        FROM network_density_scores
        WHERE sector IS NOT NULL AND density_score > 5
        ORDER BY density_score DESC
        LIMIT 20
      `),

      // Recent high-confidence signals at client companies
      db.query(`
        SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score, se.detected_at
        FROM signal_events se
        JOIN companies c ON c.id = se.company_id AND c.is_client = true
        WHERE (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '14 days'
          AND se.confidence_score >= 0.7
        ORDER BY se.confidence_score DESC
        LIMIT 30
      `, [tenantId])
    ]);

    const team = teamResult.rows;
    const contacts = contactsResult.rows;
    const clients = clientsResult.rows;
    const sectors = sectorsResult.rows;
    const signals = signalsResult.rows;

    // Build graph
    const nodes = [];
    const links = [];
    const addedNodes = new Set();

    // Team nodes
    team.forEach((u, i) => {
      const initials = (u.name || '').split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
      nodes.push({ id: 'user-' + u.id, type: 'team', label: initials, fullName: u.name, role: u.role, colorIndex: i });
      addedNodes.add('user-' + u.id);
    });

    // Client company nodes
    clients.forEach(cl => {
      const nid = 'client-' + (cl.company_id || cl.account_id);
      if (!addedNodes.has(nid)) {
        // Normalize geography to region code
        let region = null;
        const geo = (cl.geography || '').toLowerCase();
        if (/australia|oceania|nz|new zealand/i.test(geo)) region = 'AU';
        else if (/singapore|sea|asia|asean|hong kong|japan|india/i.test(geo)) region = 'SG';
        else if (/uk|united kingdom|europe|london|eu|ireland/i.test(geo)) region = 'UK';
        else if (/us|usa|america|canada|north america/i.test(geo)) region = 'US';

        nodes.push({
          id: nid, type: 'client', label: cl.name,
          tier: cl.relationship_tier, sector: cl.sector, geography: cl.geography,
          region: region,
          peopleCount: parseInt(cl.people_count) || 0,
          signalCount: parseInt(cl.signal_count) || 0,
          totalRevenue: parseFloat(cl.total_revenue) || 0,
          companyId: cl.company_id, accountId: cl.account_id
        });
        addedNodes.add(nid);
      }
    });

    // Contact nodes + links to team + links to companies
    const contactsByPerson = new Map();
    contacts.forEach(c => {
      if (!contactsByPerson.has(c.id)) contactsByPerson.set(c.id, { ...c, teamLinks: [] });
      contactsByPerson.get(c.id).teamLinks.push({
        userId: c.team_member_id, strength: c.relationship_strength, type: c.relationship_type
      });
    });

    // Build company→geography lookup from clients
    const companyGeoMap = new Map();
    clients.forEach(cl => { if (cl.company_id && cl.geography) companyGeoMap.set(cl.company_id, cl.geography); });

    contactsByPerson.forEach((c, personId) => {
      const nid = 'contact-' + personId;
      // Derive region from company geography or person location
      let region = companyGeoMap.get(c.current_company_id) || null;
      if (!region && c.location) {
        const loc = c.location.toLowerCase();
        if (/australia|sydney|melbourne|brisbane|perth|auckland|nz/i.test(loc)) region = 'AU';
        else if (/singapore|jakarta|bangkok|kuala lumpur|manila|vietnam|sea/i.test(loc)) region = 'SG';
        else if (/london|uk|united kingdom|dublin|amsterdam|paris|berlin|europe/i.test(loc)) region = 'UK';
        else if (/us|usa|new york|san francisco|chicago|boston|los angeles|america|canada|toronto/i.test(loc)) region = 'US';
      }
      if (!addedNodes.has(nid)) {
        nodes.push({
          id: nid, type: 'contact', label: c.full_name, personId,
          role: c.current_title, company: c.current_company_name,
          companyId: c.current_company_id,
          seniority: c.seniority_level,
          bestStrength: Math.max(...c.teamLinks.map(l => l.strength)),
          timingScore: c.timing_score, receptivityScore: c.receptivity_score,
          region: region
        });
        addedNodes.add(nid);
      }

      // Team → contact links
      c.teamLinks.forEach(l => {
        links.push({ source: 'user-' + l.userId, target: nid, strength: l.strength, type: l.type });
      });

      // Contact → client company links
      if (c.current_company_id) {
        const clientNid = 'client-' + c.current_company_id;
        if (addedNodes.has(clientNid)) {
          links.push({ source: nid, target: clientNid, strength: 0.4, type: 'works_at' });
        }
      }
    });

    // Sector cluster nodes
    sectors.forEach(s => {
      if (s.sector) {
        const nid = 'sector-' + s.sector.toLowerCase().replace(/\W+/g, '_');
        if (!addedNodes.has(nid)) {
          nodes.push({
            id: nid, type: 'sector', label: s.sector,
            density: s.density_score, contacts: s.total_contacts,
            region: s.region_code
          });
          addedNodes.add(nid);
        }
      }
    });

    // Signal pulse nodes on client companies
    const signalPulses = signals.map(s => ({
      companyNodeId: 'client-' + s.company_id,
      signalType: s.signal_type, confidence: s.confidence_score
    })).filter(s => addedNodes.has(s.companyNodeId));

    res.json({
      mode: 'firm',
      nodes, links,
      signalPulses,
      stats: {
        teamMembers: team.length,
        contacts: contactsByPerson.size,
        clients: clients.length,
        sectors: sectors.length,
        activeSignals: signals.length
      }
    });
  } catch (err) {
    console.error('Network graph error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Manual trigger for topology recompute
router.post('/api/network/recompute', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { computeNetworkTopology } = require('../scripts/compute_network_topology');
    const { computeTriangulation } = require('../scripts/compute_triangulation');
    res.json({ status: 'started', message: 'Network topology + triangulation recompute triggered' });
    computeNetworkTopology()
      .then(() => computeTriangulation())
      .then(r => console.log('Network recompute complete:', r))
      .catch(e => console.error('Network recompute failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger recompute: ' + err.message });
  }
});

router.get('/api/network/my-analysis', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tid = req.tenant_id;
    const uid = req.user.user_id;

    // Geographic distribution of contacts
    const { rows: geoDist } = await db.query(`
      SELECT
        CASE
          WHEN p.country_code IN ('AU','NZ') OR p.location ILIKE '%australia%' OR p.location ILIKE '%new zealand%' THEN 'OCE'
          WHEN p.country_code IN ('SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN','TW') OR p.location ILIKE '%singapore%' OR p.location ILIKE '%india%' OR p.location ILIKE '%japan%' OR p.location ILIKE '%hong kong%' THEN 'ASIA'
          WHEN p.country_code IN ('GB','UK','IE','DE','FR','NL','SE','DK','NO','FI','ES','IT') OR p.location ILIKE '%london%' OR p.location ILIKE '%united kingdom%' OR p.location ILIKE '%europe%' THEN 'EUR'
          WHEN p.country_code IN ('AE','SA','QA','IL','TR','EG') OR p.location ILIKE '%dubai%' OR p.location ILIKE '%saudi%' OR p.location ILIKE '%israel%' THEN 'MENA'
          WHEN p.country_code IN ('US','CA','BR','MX') OR p.location ILIKE '%united states%' OR p.location ILIKE '%new york%' OR p.location ILIKE '%san francisco%' OR p.location ILIKE '%canada%' THEN 'AMER'
          ELSE 'OTHER'
        END AS region,
        COUNT(DISTINCT tp.person_id) AS contacts
      FROM team_proximity tp
      JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
      WHERE tp.tenant_id = $1
        AND ($2::uuid IS NULL OR tp.team_member_id = $2)
      GROUP BY region
      ORDER BY contacts DESC
    `, [tid, uid]);

    // Sector/industry distribution from companies
    const { rows: sectorDist } = await db.query(`
      SELECT c.sector, COUNT(DISTINCT tp.person_id) AS contacts
      FROM team_proximity tp
      JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
      JOIN companies c ON c.id = p.current_company_id AND c.sector IS NOT NULL
      WHERE tp.tenant_id = $1
        AND ($2::uuid IS NULL OR tp.team_member_id = $2)
      GROUP BY c.sector
      ORDER BY contacts DESC
      LIMIT 10
    `, [tid, uid]);

    // Top contacts by interaction density + recency
    const { rows: topContacts } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location,
             tp.relationship_strength, tp.last_interaction_date,
             tp.interaction_count,
             c.sector AS company_sector
      FROM team_proximity tp
      JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
      LEFT JOIN companies c ON c.id = p.current_company_id
      WHERE tp.tenant_id = $1
        AND ($2::uuid IS NULL OR tp.team_member_id = $2)
        AND tp.relationship_strength >= 0.2
      ORDER BY tp.relationship_strength DESC, tp.last_interaction_date DESC NULLS LAST
      LIMIT 15
    `, [tid, uid]);

    // Network totals
    const { rows: [totals] } = await db.query(`
      SELECT
        COUNT(DISTINCT tp.person_id) AS total_contacts,
        COUNT(DISTINCT CASE WHEN tp.relationship_strength >= 0.5 THEN tp.person_id END) AS strong_contacts,
        COUNT(DISTINCT CASE WHEN tp.last_interaction_date > NOW() - INTERVAL '30 days' THEN tp.person_id END) AS active_30d,
        COUNT(DISTINCT p.current_company_id) AS companies_reached,
        AVG(tp.relationship_strength) AS avg_strength
      FROM team_proximity tp
      JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
      WHERE tp.tenant_id = $1
        AND ($2::uuid IS NULL OR tp.team_member_id = $2)
    `, [tid, uid]);

    // Recency heat — how fresh is the network
    const { rows: recencyHeat } = await db.query(`
      SELECT
        COUNT(*) FILTER (WHERE tp.last_interaction_date > NOW() - INTERVAL '7 days') AS last_7d,
        COUNT(*) FILTER (WHERE tp.last_interaction_date > NOW() - INTERVAL '30 days') AS last_30d,
        COUNT(*) FILTER (WHERE tp.last_interaction_date > NOW() - INTERVAL '90 days') AS last_90d,
        COUNT(*) AS total
      FROM team_proximity tp
      WHERE tp.tenant_id = $1
        AND ($2::uuid IS NULL OR tp.team_member_id = $2)
    `, [tid, uid]);

    const totalContacts = parseInt(totals?.total_contacts) || 0;
    const geoWithPct = geoDist.map(g => ({
      region: g.region,
      contacts: parseInt(g.contacts),
      pct: totalContacts > 0 ? Math.round(parseInt(g.contacts) / totalContacts * 100) : 0
    }));

    res.json({
      totals: {
        total_contacts: totalContacts,
        strong_contacts: parseInt(totals?.strong_contacts) || 0,
        active_30d: parseInt(totals?.active_30d) || 0,
        companies_reached: parseInt(totals?.companies_reached) || 0,
        avg_strength: parseFloat(totals?.avg_strength || 0).toFixed(2),
      },
      geography: geoWithPct,
      sectors: sectorDist.map(s => ({ sector: s.sector, contacts: parseInt(s.contacts) })),
      top_contacts: topContacts,
      recency: recencyHeat.rows?.[0] || recencyHeat[0] || {},
    });
  } catch (err) {
    console.error('Network analysis error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Daily insights — get latest for current user

  // ═══════════════════════════════════════════════════════════════════════════
  // REMAINING PLATFORM ROUTES
  // ═══════════════════════════════════════════════════════════════════════════

router.post('/api/tenant/invite', authenticateToken, async (req, res) => {
  try {
    var { email, role } = req.body;
    if (!email) return res.status(400).json({ error: 'email required' });

    // Check if already a user on this tenant
    var { rows: existing } = await platformPool.query('SELECT id FROM users WHERE email = $1 AND tenant_id = $2', [email, req.tenant_id]);
    if (existing.length) return res.status(409).json({ error: 'Already a member of this tenant' });

    // Create invite
    var { rows: [invite] } = await platformPool.query(
      `INSERT INTO tenant_invites (tenant_id, email, role, invited_by) VALUES ($1, $2, $3, $4) RETURNING id, token, email, role, expires_at`,
      [req.tenant_id, email, role || 'viewer', req.user.user_id]
    );

    // Send email
    if (process.env.RESEND_API_KEY) {
      try {
        var baseUrl = process.env.APP_URL || 'https://' + req.get('host');
        var joinUrl = baseUrl + '/api/auth/google?return_to=/index.html';
        var inviterName = req.user.name || req.user.email;
        var { rows: [tenant] } = await platformPool.query('SELECT name FROM tenants WHERE id = $1', [req.tenant_id]);

        var { Resend } = require('resend');
        var resend = new Resend(process.env.RESEND_API_KEY);
        await resend.emails.send({
          from: process.env.EMAIL_FROM || 'Autonodal <notifications@autonodal.com>',
          to: email,
          subject: inviterName + ' invited you to ' + (tenant?.name || 'their team') + ' on Autonodal',
          html: '<div style="font-family:system-ui,sans-serif;max-width:520px;margin:0 auto;padding:32px">' +
            '<h2 style="margin:0 0 8px">You\'ve been invited to join ' + (tenant?.name || 'a team') + '</h2>' +
            '<p style="color:#4a4a4a;margin:0 0 20px">' + inviterName + ' wants you to join their team on Autonodal — market intelligence mapped to your relationships.</p>' +
            '<a href="' + joinUrl + '" style="display:inline-block;background:#2563eb;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:600;font-size:15px">Join with Google</a>' +
            '<p style="color:#aaa;font-size:12px;margin-top:24px">This invite expires in 14 days. Sign in with <strong>' + email + '</strong> to join automatically.</p>' +
            '</div>'
        });
        invite.email_sent = true;
      } catch (e) {
        invite.email_sent = false;
      }
    }

    res.status(201).json(invite);
  } catch (err) {
    console.error('Tenant invite error:', err.message);
    res.status(500).json({ error: 'Failed to send invite' });
  }
});

router.get('/api/tenant/invites', authenticateToken, async (req, res) => {
  try {
    var { rows } = await platformPool.query(
      `SELECT ti.id, ti.email, ti.role, ti.status, ti.created_at, ti.accepted_at, u.name AS invited_by_name
       FROM tenant_invites ti LEFT JOIN users u ON u.id = ti.invited_by
       WHERE ti.tenant_id = $1 ORDER BY ti.created_at DESC LIMIT 50`,
      [req.tenant_id]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: 'Failed to load invites' });
  }
});

/* REMOVED: app.get/patch('/api/auth/me') — moved to routes/auth.js */

// ─── Personalized Morning Brief ───
router.get('/api/brief/personal', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const userId = req.user.user_id;

    // Get user's region
    const { rows: [userRow] } = await db.query('SELECT region FROM users WHERE id = $1', [userId]);
    const userRegion = userRow?.region || 'APAC';

    // Run all 4 queries in parallel
    const geos = REGION_MAP[userRegion] || REGION_MAP['APAC'];
    const geoConditions = geos.map((_, i) => `c.geography ILIKE $${i + 2} OR sd.signal_summary ILIKE $${i + 2}`).join(' OR ');
    const geoParams = [req.tenant_id, ...geos.map(g => `%${g}%`)];

    const [contactResult, clientResult, dispatchResult, statsResult] = await Promise.all([
      // 1. My contacts in recent signals
      db.query(`
        SELECT DISTINCT ON (p.id)
          p.id as person_id, p.full_name, p.current_title, p.current_company_name,
          se.signal_type, se.company_name as signal_company, se.confidence_score,
          se.evidence_summary, se.detected_at,
          tp.proximity_strength, tp.proximity_type,
          i.interaction_at as last_contact
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $2
        JOIN companies c ON c.id = p.current_company_id
        JOIN signal_events se ON se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '7 days'
        LEFT JOIN LATERAL (
          SELECT interaction_at FROM interactions
          WHERE person_id = p.id AND user_id = $1
          ORDER BY interaction_at DESC LIMIT 1
        ) i ON true
        WHERE tp.user_id = $1 AND tp.tenant_id = $2
        ORDER BY p.id, se.confidence_score DESC, se.detected_at DESC
        LIMIT 5
      `, [userId, req.tenant_id]),
      // 2. Client signals
      db.query(`
        SELECT DISTINCT ON (se.company_id)
          se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
          se.evidence_summary, se.detected_at,
          cl.relationship_status, cl.relationship_tier,
          (SELECT COUNT(*) FROM people p
            JOIN companies c_t ON c_t.id = p.current_company_id AND c_t.tenant_id = $1
            WHERE p.tenant_id = $1 AND LOWER(c_t.name) = LOWER(se.company_name)) as contact_count
        FROM signal_events se
        JOIN companies c ON LOWER(c.name) = LOWER(se.company_name) AND c.is_client = true AND c.tenant_id = $1
        JOIN accounts cl ON cl.company_id = c.id AND cl.tenant_id = $1
        WHERE se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)
        ORDER BY se.company_id, se.confidence_score DESC
        LIMIT 5
      `, [req.tenant_id]),
      // 3. Top dispatches for user's region
      db.query(`
        SELECT sd.id, sd.company_name, sd.signal_type, sd.signal_summary,
               sd.opportunity_angle, sd.blog_title, sd.status, sd.claimed_by,
               c.geography, c.is_client,
               jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) as connection_count
        FROM signal_dispatches sd
        LEFT JOIN companies c ON c.id = sd.company_id
        WHERE sd.status = 'draft' AND sd.claimed_by IS NULL
          AND sd.tenant_id = $1
          AND (${geoConditions})
        ORDER BY
          CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
          jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) DESC,
          sd.generated_at DESC
        LIMIT 3
      `, geoParams),
      // 4. Quick stats
      db.query(`
        SELECT
          (SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours' AND (tenant_id IS NULL OR tenant_id = $1)) as signals_24h,
          (SELECT COUNT(*) FROM signal_dispatches WHERE status = 'draft' AND claimed_by IS NULL AND tenant_id = $1) as unclaimed_dispatches,
          (SELECT COUNT(*) FROM signal_events se JOIN companies c ON c.id = se.company_id AND c.is_client = true WHERE se.detected_at > NOW() - INTERVAL '7 days' AND (se.tenant_id IS NULL OR se.tenant_id = $1)) as client_signals_7d
      `, [req.tenant_id])
    ]);

    const contactSignals = contactResult.rows;
    const clientSignals = clientResult.rows;
    const regionDispatches = dispatchResult.rows;
    const briefStats = statsResult.rows[0];

    res.json({
      region: userRegion,
      stats: briefStats,
      contact_signals: contactSignals,
      client_signals: clientSignals,
      region_dispatches: regionDispatches
    });
  } catch (err) {
    console.error('Personal brief error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

/* REMOVED: app.post('/api/auth/logout'), gmail/connect — moved to routes/auth.js */

/* REMOVED: gmail/callback, gmail/status — moved to routes/auth.js */

// ═══════════════════════════════════════════════════════════════════════════════
// GOOGLE DRIVE — list, ingest, embed
// ═══════════════════════════════════════════════════════════════════════════════

// Helper: get a fresh Google access token for the current user

router.get('/api/drive/files', authenticateToken, async (req, res) => {
  try {
    const token = await getGoogleToken(req.user.user_id);
    if (!token) return res.status(401).json({ error: 'Google account not connected. Visit /api/auth/gmail/connect to connect.' });

    const folderId = req.query.folder || 'root';
    const pageToken = req.query.pageToken || '';
    const q = req.query.q || '';

    // Search for Docs, Sheets, Slides, PDFs
    const mimeTypes = [
      'application/vnd.google-apps.document',
      'application/vnd.google-apps.spreadsheet',
      'application/vnd.google-apps.presentation',
      'application/vnd.google-apps.folder',
      'application/pdf',
    ];
    let query = `trashed = false`;
    if (folderId && folderId !== 'root' && !q) query += ` and '${folderId}' in parents`;
    if (q) query += ` and fullText contains '${q.replace(/'/g, "\\'")}'`;
    if (!q && folderId === 'root') query += ` and (${mimeTypes.map(m => `mimeType = '${m}'`).join(' or ')})`;

    const params = new URLSearchParams({
      q: query,
      fields: 'nextPageToken,files(id,name,mimeType,modifiedTime,size,iconLink,webViewLink,owners,shared)',
      pageSize: '50',
      orderBy: 'modifiedTime desc',
    });
    if (pageToken) params.set('pageToken', pageToken);

    const driveRes = await fetch('https://www.googleapis.com/drive/v3/files?' + params, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!driveRes.ok) {
      const err = await driveRes.text();
      console.error('Drive API error:', err);
      return res.status(driveRes.status).json({ error: 'Drive API error', details: err });
    }
    const data = await driveRes.json();

    // Tag which files are already ingested
    const fileHashes = (data.files || []).map(f => require('crypto').createHash('md5').update('gdrive:' + f.id).digest('hex'));
    const db = new TenantDB(req.tenant_id);
    const { rows: ingested } = fileHashes.length > 0
      ? await db.query(
          `SELECT source_url_hash FROM external_documents WHERE source_url_hash = ANY($1) AND tenant_id = $2`,
          [fileHashes, req.tenant_id]
        )
      : { rows: [] };
    const ingestedSet = new Set(ingested.map(r => r.source_url_hash));

    res.json({
      files: (data.files || []).map(f => ({
        id: f.id,
        name: f.name,
        mimeType: f.mimeType,
        type: f.mimeType.includes('document') ? 'doc' :
              f.mimeType.includes('spreadsheet') ? 'sheet' :
              f.mimeType.includes('presentation') ? 'slides' :
              f.mimeType.includes('folder') ? 'folder' : 'pdf',
        modifiedTime: f.modifiedTime,
        size: f.size,
        webViewLink: f.webViewLink,
        ingested: ingestedSet.has(require('crypto').createHash('md5').update('gdrive:' + f.id).digest('hex')),
      })),
      nextPageToken: data.nextPageToken || null,
    });
  } catch (err) {
    console.error('Drive files error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Ingest a single Drive file — extract text, store in external_documents, embed in Qdrant
router.post('/api/drive/ingest/:fileId', authenticateToken, async (req, res) => {
  try {
    const token = await getGoogleToken(req.user.user_id);
    if (!token) return res.status(401).json({ error: 'Google account not connected' });

    const fileId = req.params.fileId;
    const tenantId = req.tenant_id;
    const sourceUrlHash = require('crypto').createHash('md5').update('gdrive:' + fileId).digest('hex');

    // Check if already ingested
    const db = new TenantDB(req.tenant_id);
    const { rows: existing } = await db.query(
      `SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2`,
      [sourceUrlHash, tenantId]
    );

    // Get file metadata
    const metaRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType,modifiedTime,webViewLink`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!metaRes.ok) return res.status(404).json({ error: 'File not found in Drive' });
    const meta = await metaRes.json();

    // Extract text content based on type
    let content = '';
    let title = meta.name;

    if (meta.mimeType === 'application/vnd.google-apps.document') {
      // Google Docs → export as plain text
      const textRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=text/plain`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (textRes.ok) content = await textRes.text();

    } else if (meta.mimeType === 'application/vnd.google-apps.spreadsheet') {
      // Google Sheets → get all sheet values as text
      const sheetsRes = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${fileId}?fields=sheets.properties.title`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (sheetsRes.ok) {
        const sheetsData = await sheetsRes.json();
        const sheetNames = (sheetsData.sheets || []).map(s => s.properties.title);
        const parts = [];
        for (const sheetName of sheetNames.slice(0, 10)) {
          const valRes = await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${fileId}/values/${encodeURIComponent(sheetName)}`,
            { headers: { Authorization: `Bearer ${token}` } }
          );
          if (valRes.ok) {
            const valData = await valRes.json();
            const rows = (valData.values || []).map(r => r.join('\t')).join('\n');
            parts.push(`--- Sheet: ${sheetName} ---\n${rows}`);
          }
        }
        content = parts.join('\n\n');
      }

    } else if (meta.mimeType === 'application/vnd.google-apps.presentation') {
      // Google Slides → export as plain text
      const textRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=text/plain`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (textRes.ok) content = await textRes.text();

    } else if (meta.mimeType === 'application/pdf') {
      // PDFs — can't easily extract text without OCR, store metadata only
      content = `[PDF document: ${meta.name}]`;
    }

    if (!content || content.length < 10) {
      return res.json({ ingested: false, message: 'No extractable content', fileId });
    }

    // Truncate very large documents
    const maxLen = 50000;
    if (content.length > maxLen) content = content.substring(0, maxLen) + '\n\n[... truncated at ' + maxLen + ' chars]';

    // Store in external_documents
    let docId;
    if (existing.length > 0) {
      docId = existing[0].id;
      await db.query(
        `UPDATE external_documents SET title = $1, content = $2, source_url = $3, updated_at = NOW() WHERE id = $4`,
        [title, content, meta.webViewLink, docId]
      );
    } else {
      // ALL Drive docs are context_only — they're internal knowledge, not market signal sources
      // They're still embedded and searchable, just never fed to the signal extraction pipeline
      const isOld = true; // Always context_only for Drive
      const { rows: [newDoc] } = await db.query(
        `INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash, tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
         VALUES ($1, $2, 'Google Drive', $3, $4, $5, $6, $7, $8, $9, NOW())
         RETURNING id`,
        [title, content,
         meta.mimeType.includes('document') ? 'google_doc' :
         meta.mimeType.includes('spreadsheet') ? 'google_sheet' :
         meta.mimeType.includes('presentation') ? 'google_slides' : 'pdf',
         meta.webViewLink, sourceUrlHash, tenantId, req.user.user_id, meta.modifiedTime,
         isOld ? 'context_only' : 'pending']
      );
      docId = newDoc.id;
    }

    // Embed in Qdrant
    let embedded = false;
    if (process.env.OPENAI_API_KEY && process.env.QDRANT_URL) {
      try {
        const embeddingText = `${title}\n\n${content.substring(0, 8000)}`;
        const embedding = await generateQueryEmbedding(embeddingText);

        const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({
            points: [{
              id: docId,
              vector: embedding,
              payload: {
                tenant_id: tenantId,
                title: title,
                source: 'google_drive',
                source_type: meta.mimeType.includes('document') ? 'google_doc' : meta.mimeType.includes('spreadsheet') ? 'google_sheet' : 'google_slides',
                file_id: fileId,
              }
            }]
          });
          const qReq = https.request({
            hostname: url.hostname, port: url.port || 443,
            path: url.pathname + '?wait=true', method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY },
            timeout: 15000
          }, (qRes) => { const c = []; qRes.on('data', d => c.push(d)); qRes.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        embedded = true;
      } catch (e) {
        console.error('Drive embed error:', e.message);
      }
    }

    res.json({
      ingested: true,
      docId,
      title,
      contentLength: content.length,
      embedded,
      type: meta.mimeType,
      fileId,
    });
  } catch (err) {
    console.error('Drive ingest error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bulk ingest multiple Drive files
router.post('/api/drive/ingest-bulk', authenticateToken, async (req, res) => {
  try {
    const { fileIds } = req.body;
    if (!fileIds || !Array.isArray(fileIds)) return res.status(400).json({ error: 'fileIds array required' });

    const results = [];
    for (const fileId of fileIds.slice(0, 20)) {
      try {
        // Call the single ingest internally
        const token = await getGoogleToken(req.user.user_id);
        if (!token) { results.push({ fileId, error: 'No token' }); continue; }

        // Simplified inline — reuse the logic
        const metaRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        if (!metaRes.ok) { results.push({ fileId, error: 'Not found' }); continue; }
        const meta = await metaRes.json();
        results.push({ fileId, name: meta.name, queued: true });
      } catch (e) {
        results.push({ fileId, error: e.message });
      }
    }

    // Process each file sequentially in background (don't block response)
    res.json({ queued: results.length, files: results });

    // Background processing
    for (const r of results.filter(x => x.queued)) {
      try {
        const fakeReq = { params: { fileId: r.fileId }, user: req.user, tenant_id: req.tenant_id };
        const fakeRes = { json: () => {}, status: () => ({ json: () => {} }) };
        // Trigger single ingest endpoint logic — in production, use a job queue
      } catch (e) { /* skip */ }
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TENANT CONFIG & TERMINOLOGY
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/config/tenant', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenantId = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    const { rows } = await db.query(
      'SELECT id, name, slug, vertical, logo_url, primary_color, plan, onboarding_complete, focus_geographies, focus_sectors FROM tenants WHERE id = $1',
      [tenantId]
    );
    res.json(rows[0] || null);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/config/terminology', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenantId = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    const { rows } = await db.query('SELECT vertical FROM tenants WHERE id = $1', [tenantId]);
    if (!rows.length) return res.status(404).json({ error: 'Tenant not found' });

    const { getTerminology, SIGNAL_LABELS } = require('../lib/terminology');
    const vertical = rows[0].vertical;
    const t = getTerminology(vertical);

    res.json({ vertical, terminology: t, signal_labels: SIGNAL_LABELS[vertical] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// TERMINOLOGY — tenant-specific label overrides from tenant_terminology table
// ─────────────────────────────────────────────────────────────────────────────

router.get('/api/terminology', authenticateToken, async (req, res) => {
  try {
    const vertical = req.query.vertical || 'all';
    const tid = req.tenant_id;
    const { rows } = await platformPool.query(`
      SELECT term_key, display_label FROM tenant_terminology
      WHERE (tenant_id = $1 OR tenant_id IS NULL)
        AND (vertical = $2 OR vertical = 'all')
      ORDER BY tenant_id NULLS LAST, vertical = 'all' ASC
    `, [tid, vertical]);
    // Build map — tenant-specific overrides win over defaults
    const labels = {};
    rows.reverse().forEach(r => { labels[r.term_key] = r.display_label; });
    res.json({ vertical, labels });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// FEED MANAGEMENT
router.get('/api/stats', authenticateToken, async (req, res) => {
  try {
    // Use platformPool to bypass RLS — queries explicitly filter by tenant_id
    const tid = req.tenant_id;
    const [
      people, signals24h, signalsTotal, companies,
      documents, placements, activeSources,
      peopleWithNotes, signalsByType, docsByType,
      eventsThisWeek, events30d, activeEventSources
    ] = await Promise.all([
      platformPool.query('SELECT COUNT(*) AS cnt FROM people WHERE tenant_id = $1', [tid]),
      platformPool.query(`SELECT COUNT(*) AS cnt FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours' AND (tenant_id IS NULL OR tenant_id = $1)`, [tid]),
      platformPool.query('SELECT COUNT(*) AS cnt FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1)', [tid]),
      platformPool.query('SELECT COUNT(*) AS cnt FROM companies WHERE tenant_id = $1', [tid]),
      platformPool.query('SELECT COUNT(*) AS cnt FROM external_documents WHERE (tenant_id IS NULL OR tenant_id = $1)', [tid]),
      platformPool.query('SELECT COUNT(*) AS cnt, COALESCE(SUM(placement_fee), 0) AS total_fees FROM conversions WHERE tenant_id = $1 AND source IN (\'xero_export\', \'xero\', \'manual\') AND placement_fee IS NOT NULL', [tid]),
      platformPool.query('SELECT COUNT(*) AS cnt FROM rss_sources WHERE enabled = true'),
      platformPool.query(`SELECT COUNT(DISTINCT person_id) AS cnt FROM interactions WHERE interaction_type = 'research_note' AND tenant_id = $1`, [tid]),
      platformPool.query(`SELECT signal_type, COUNT(*) AS cnt FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) GROUP BY signal_type ORDER BY cnt DESC`, [tid]),
      platformPool.query(`SELECT source_type, COUNT(*) AS cnt FROM external_documents WHERE (tenant_id IS NULL OR tenant_id = $1) GROUP BY source_type ORDER BY cnt DESC`, [tid]),
      platformPool.query(`SELECT COUNT(*) AS cnt FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 7`, [tid]).catch(() => ({ rows: [{ cnt: 0 }] })),
      platformPool.query(`SELECT COUNT(*) AS cnt FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 30`, [tid]).catch(() => ({ rows: [{ cnt: 0 }] })),
      platformPool.query(`SELECT COUNT(*) AS cnt FROM event_sources WHERE (tenant_id IS NULL OR tenant_id = $1) AND is_active = true`, [tid]).catch(() => ({ rows: [{ cnt: 0 }] })),
    ]);

    res.json({
      people_count: parseInt(people.rows[0].cnt),
      signals_24h: parseInt(signals24h.rows[0].cnt),
      signals_total: parseInt(signalsTotal.rows[0].cnt),
      companies_count: parseInt(companies.rows[0].cnt),
      documents_count: parseInt(documents.rows[0].cnt),
      placements_count: parseInt(placements.rows[0].cnt),
      placements_total_fees: parseFloat(placements.rows[0].total_fees),
      sources_active: parseInt(activeSources.rows[0].cnt),
      people_with_notes: parseInt(peopleWithNotes.rows[0].cnt),
      signals_by_type: signalsByType.rows.map(r => ({ type: r.signal_type, count: parseInt(r.cnt) })),
      documents_by_type: docsByType.rows.map(r => ({ type: r.source_type, count: parseInt(r.cnt) })),
      events_this_week: parseInt(eventsThisWeek.rows[0].cnt),
      upcoming_events_30d: parseInt(events30d.rows[0].cnt),
      active_event_sources: parseInt(activeEventSources.rows[0].cnt),
    });
  } catch (err) {
    console.error('Stats error:', err.message);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
router.post('/api/clients/:id/reconcile', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [client] } = await db.query('SELECT * FROM accounts WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });

    // Already linked?
    if (client.company_id) {
      const { rows: [co] } = await db.query('SELECT id, name FROM companies WHERE id = $1 AND tenant_id = $2', [client.company_id, req.tenant_id]);
      if (co) return res.json({ company_id: co.id, message: 'Already linked', company_name: co.name });
    }

    // Check if a companies record already exists by name
    let { rows: [existing] } = await db.query(
      'SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [client.name, req.tenant_id]
    );

    let companyId;
    if (existing) {
      companyId = existing.id;
    } else {
      // Create a new companies record from the client
      const { rows: [newCo] } = await db.query(`
        INSERT INTO companies (name, is_client, created_at, updated_at, tenant_id)
        VALUES ($1, true, NOW(), NOW(), $2)
        RETURNING id
      `, [client.name, req.tenant_id]);
      companyId = newCo.id;
    }

    // Link client to company
    await db.query('UPDATE accounts SET company_id = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3', [companyId, client.id, req.tenant_id]);

    // Also link any people whose current_company_name matches
    const { rowCount: linkedPeople } = await db.query(`
      UPDATE people SET current_company_id = $1, updated_at = NOW()
      WHERE current_company_name ILIKE $2 AND (current_company_id IS NULL OR current_company_id != $1) AND tenant_id = $3
    `, [companyId, client.name, req.tenant_id]);

    res.json({ company_id: companyId, client_id: client.id, people_linked: linkedPeople, message: existing ? 'Linked to existing company' : 'Created new company record' });
  } catch (err) {
    console.error('Reconcile error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Bulk reconcile all unlinked accounts ───
router.post('/api/clients/reconcile-all', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: unlinked } = await db.query(`
      SELECT cl.id, cl.name FROM accounts cl
      WHERE cl.company_id IS NULL AND cl.tenant_id = $1
      ORDER BY cl.name
    `, [req.tenant_id]);

    let created = 0, linked = 0, errors = 0;
    for (const client of unlinked) {
      try {
        let { rows: [existing] } = await db.query(
          'SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [client.name, req.tenant_id]
        );

        let companyId;
        if (existing) {
          companyId = existing.id;
          linked++;
        } else {
          const { rows: [newCo] } = await db.query(
            `INSERT INTO companies (name, is_client, created_at, updated_at, tenant_id) VALUES ($1, false, NOW(), NOW(), $2) RETURNING id`,
            [client.name, req.tenant_id]
          );
          companyId = newCo.id;
          created++;
        }

        await db.query('UPDATE accounts SET company_id = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3', [companyId, client.id, req.tenant_id]);
        await db.query(`
          UPDATE people SET current_company_id = $1, updated_at = NOW()
          WHERE current_company_name ILIKE $2 AND (current_company_id IS NULL OR current_company_id != $1) AND tenant_id = $3
        `, [companyId, client.name, req.tenant_id]);
      } catch (e) { errors++; }
    }

    res.json({ total_unlinked: unlinked.length, companies_created: created, companies_linked: linked, errors });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/search', authenticateToken, endpointLimit(30), async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const q = req.query.q;
    const collection = req.query.collection || 'all';
    const minScore = parseFloat(req.query.min_score) || 0.25; // relevance threshold
    const qdrantLimit = 100; // max vectors to request from Qdrant per collection

    if (!q || q.trim().length < 2) {
      return res.status(400).json({ error: 'Search query too short' });
    }

    // Generate embedding (cached — repeat searches skip OpenAI)
    const vector = await generateQueryEmbedding(q);

    const results = { people: [], companies: [], documents: [] };

    // Fire all Qdrant searches in parallel — biggest speed win
    const collectionsToSearch = [];
    if (collection === 'people' || collection === 'all') collectionsToSearch.push('people');
    if (collection === 'companies' || collection === 'all') collectionsToSearch.push('companies');
    if (collection === 'documents' || collection === 'all') collectionsToSearch.push('documents');
    if (collection === 'signals' || collection === 'all') collectionsToSearch.push('signal_events');
    if (collection === 'case_studies' || collection === 'all') collectionsToSearch.push('case_studies');
    if (collection === 'interactions' || collection === 'all') collectionsToSearch.push('interactions');
    // Publications only searched when explicitly requested — never in default fan-out
    const searchPublicationsEnabled = RESEARCH_SEARCH_ENABLED && (collection === 'publications' || req.query.include_publications === 'true');

    const qdrantStartTime = Date.now();
    const qdrantResultsMap = {};
    // Build tenant filter for Qdrant — match current tenant OR platform-wide (null tenant)
    const tenantFilter = {
      should: [
        { key: 'tenant_id', match: { value: req.tenant_id } },
        { is_empty: { key: 'tenant_id' } },
      ]
    };
    const qdrantPromises = collectionsToSearch.map(async (coll) => {
      try {
        const raw = await qdrantSearch(coll, vector, qdrantLimit, tenantFilter);
        qdrantResultsMap[coll] = raw.filter(r => r.score >= minScore);
      } catch (e) { qdrantResultsMap[coll] = []; }
    });
    await Promise.all(qdrantPromises);

    // Search people
    if (collection === 'people' || collection === 'all') {
      const qdrantResults = qdrantResultsMap['people'] || [];

      if (qdrantResults.length > 0) {
        // IDs may be UUIDs or numeric — person_id is in payload for numeric IDs
        const personIds = qdrantResults.map(r => {
          const pid = r.payload?.person_id || r.payload?.id;
          if (pid) return pid;
          const sid = String(r.id);
          return /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(sid) ? sid : null;
        }).filter(Boolean);

        if (personIds.length > 0) {
        const { rows: people } = await db.query(`
          SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.headline,
                 p.location, p.seniority_level, p.expertise_tags, p.industries, p.source,
                 p.email, p.linkedin_url, p.current_company_id,
                 ps.timing_score, ps.flight_risk_score, ps.engagement_score, ps.receptivity_score,
                 (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS note_count,
                 (SELECT COUNT(*) FROM team_proximity tp WHERE tp.person_id = p.id AND tp.tenant_id = $2 AND tp.relationship_strength >= 0.3) AS team_connections,
                 (SELECT u.name FROM users u WHERE u.id = (
                   SELECT tp2.team_member_id FROM team_proximity tp2 WHERE tp2.person_id = p.id AND tp2.tenant_id = $2
                   ORDER BY tp2.relationship_strength DESC LIMIT 1
                 )) AS best_connector,
                 (SELECT se.signal_type FROM signal_events se WHERE se.company_id = p.current_company_id
                   AND se.detected_at > NOW() - INTERVAL '30 days' AND (se.tenant_id IS NULL OR se.tenant_id = $2)
                   ORDER BY se.confidence_score DESC LIMIT 1) AS company_signal_type,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = p.current_company_id
                   AND se.detected_at > NOW() - INTERVAL '30 days' AND (se.tenant_id IS NULL OR se.tenant_id = $2)) AS company_signal_count,
                 c.is_client AS at_client_company
          FROM people p
          LEFT JOIN person_scores ps ON ps.person_id = p.id
          LEFT JOIN companies c ON c.id = p.current_company_id
          WHERE p.id = ANY($1::uuid[]) AND p.tenant_id = $2
        `, [personIds, req.tenant_id]);

        const peopleMap = new Map(people.map(p => [p.id, p]));

        const seenPeople = new Set();
        results.people = qdrantResults
          .map(r => {
            const pid = r.payload?.person_id || r.payload?.id || String(r.id);
            if (seenPeople.has(pid)) return null; // dedup multiple vectors per person
            seenPeople.add(pid);
            const person = peopleMap.get(pid);
            if (!person) return null;
            return {
              ...person,
              match_score: Math.round(r.score * 100),
              has_research_notes: r.payload?.has_research_notes || parseInt(person.note_count) > 0,
            };
          })
          .filter(Boolean);
        }
      }

      // SQL fallback: exact name match (catches people that Qdrant missed or ranked low)
      const existingIds = new Set(results.people.map(p => p.id));
      const { rows: nameFallback } = await db.query(`
        SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.headline,
               p.location, p.seniority_level, p.source, p.email, p.linkedin_url, p.current_company_id,
               c.is_client AS at_client_company
        FROM people p
        LEFT JOIN companies c ON c.id = p.current_company_id
        WHERE p.tenant_id = $1 AND p.full_name ILIKE $2
          AND (p.current_title IS NOT NULL OR p.headline IS NOT NULL OR p.source = 'ezekia')
        LIMIT 10
      `, [req.tenant_id, `%${q}%`]).catch(() => ({ rows: [] }));

      for (const p of nameFallback) {
        if (!existingIds.has(p.id)) {
          results.people.push({ ...p, match_score: 95, match_type: 'name' });
          existingIds.add(p.id);
        }
      }
    }

    // Search companies
    if (collection === 'companies' || collection === 'all') {
      const qdrantResults = qdrantResultsMap['companies'] || [];

      if (qdrantResults.length > 0) {
        const uuidRx = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const compIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRx.test(id));

        if (compIds.length === 0) {
          // No valid UUIDs — skip
        } else {
        const { rows: companies } = await db.query(`
          SELECT c.id, c.name, c.sector, c.geography, c.domain, c.is_client,
                 c.employee_count_band, c.description,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $2)) AS signal_count,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $2) AND se.detected_at > NOW() - INTERVAL '30 days') AS recent_signal_count,
                 (SELECT se.signal_type FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $2) AND se.detected_at > NOW() - INTERVAL '30 days'
                   ORDER BY se.confidence_score DESC LIMIT 1) AS top_signal_type,
                 (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $2) AS people_count,
                 (SELECT COUNT(DISTINCT tp.person_id) FROM team_proximity tp
                   JOIN people p2 ON p2.id = tp.person_id AND p2.current_company_id = c.id AND p2.tenant_id = $2
                   WHERE tp.tenant_id = $2 AND tp.relationship_strength >= 0.3) AS network_connections,
                 (SELECT a.relationship_tier FROM accounts a WHERE (a.company_id = c.id OR LOWER(a.name) = LOWER(c.name)) AND a.tenant_id = $2 LIMIT 1) AS client_tier,
                 cas.adjacency_score, cas.warmest_contact_name
          FROM companies c
          LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(c.name))
          WHERE c.id = ANY($1::uuid[]) AND c.tenant_id = $2
        `, [compIds, req.tenant_id]);

        const compMap = new Map(companies.map(c => [c.id, c]));

        results.companies = qdrantResults
          .map(r => {
            const company = compMap.get(r.id);
            if (!company) return null;
            return {
              ...company,
              match_score: Math.round(r.score * 100),
            };
          })
          .filter(Boolean);
        }
      }
    }

    // Search documents
    if (collection === 'documents' || collection === 'all') {
      const qdrantResults = qdrantResultsMap['documents'] || [];

      if (qdrantResults.length > 0) {
        // IDs may be UUIDs or numeric — document_id is in payload for numeric IDs
        const docIds = qdrantResults.map(r => {
          const did = r.payload?.document_id;
          if (did) return did;
          const sid = String(r.id);
          return /^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(sid) ? sid : null;
        }).filter(Boolean);

        if (docIds.length > 0) {
        const { rows: docs } = await db.query(`
          SELECT id, title, source_type, source_name, source_url, author, published_at
          FROM external_documents WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        `, [docIds, req.tenant_id]);

        const docMap = new Map(docs.map(d => [d.id, d]));

        results.documents = qdrantResults
          .map(r => {
            const did = r.payload?.document_id || String(r.id);
            const doc = docMap.get(did);
            if (!doc) return null;
            return {
              ...doc,
              match_score: Math.round(r.score * 100),
            };
          })
          .filter(Boolean);
        }
      }
    }

    // Search signals (direct)
    if (collection === 'signals' || collection === 'all') {
      try {
        const qdrantResults = qdrantResultsMap['signal_events'] || [];
        if (qdrantResults.length > 0) {
          const sigIds = qdrantResults.map(r => r.payload?.signal_id).filter(Boolean);
          if (sigIds.length > 0) {
            const { rows: signals } = await db.query(`
              SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
                     se.evidence_summary, se.detected_at, c.sector, c.geography, c.is_client,
                     COALESCE(pc.cnt, 0) AS network_connections,
                     pc.best_name AS best_connector
              FROM signal_events se
              LEFT JOIN companies c ON c.id = se.company_id
              LEFT JOIN LATERAL (
                SELECT COUNT(DISTINCT tp.person_id) AS cnt,
                       (SELECT u.name FROM team_proximity tp2
                        JOIN people p2 ON p2.id = tp2.person_id AND p2.tenant_id = $2
                        JOIN users u ON u.id = tp2.team_member_id
                        WHERE tp2.tenant_id = $2 AND p2.current_company_id = se.company_id AND tp2.relationship_strength >= 0.3
                        ORDER BY tp2.relationship_strength DESC LIMIT 1) AS best_name
                FROM team_proximity tp
                JOIN people p ON p.id = tp.person_id AND p.tenant_id = $2
                WHERE tp.tenant_id = $2 AND tp.relationship_strength >= 0.3 AND p.current_company_id = se.company_id
              ) pc ON true
              WHERE se.id = ANY($1::uuid[]) AND (se.tenant_id IS NULL OR se.tenant_id = $2)
            `, [sigIds, req.tenant_id]);
            const sigMap = new Map(signals.map(s => [s.id, s]));
            results.signals = qdrantResults.map(r => {
              const sig = sigMap.get(r.payload?.signal_id);
              if (!sig) return null;
              return { ...sig, match_score: Math.round(r.score * 100), score: r.score };
            }).filter(Boolean);
          }
        }
      } catch (e) { console.error('Search collection error:', e.message?.substring(0, 80)); }
    }

    // Search case studies (replaces placements in search — more useful, no duplicate retainer stages)
    if (collection === 'case_studies' || collection === 'all') {
      try {

        // Try Qdrant first
        let csResults = [];
        try {
          const qdrantResults = qdrantResultsMap['case_studies'] || [];
          if (qdrantResults.length > 0) {
            // IDs are numeric timestamps — case_study_id is in the payload
            const csIds = qdrantResults.map(r => r.payload?.case_study_id).filter(Boolean);
            if (csIds.length > 0) {
              const { rows } = await db.query(`
                SELECT id, title, client_name, role_title, sector, geography, year,
                       challenge, engagement_type, themes, capabilities
                FROM case_studies WHERE id = ANY($1::uuid[]) AND tenant_id = $2
              `, [csIds, req.tenant_id]);
              const csMap = new Map(rows.map(r => [r.id, r]));
              csResults = qdrantResults.map(r => {
                const cs = csMap.get(r.payload?.case_study_id);
                if (!cs) return null;
                return { ...cs, match_score: Math.round(r.score * 100), score: r.score };
              }).filter(Boolean);
            }
          }
        } catch (e) { /* collection may not exist */ }

        // Fallback to SQL text search
        if (csResults.length < 3) {
          const { rows } = await db.query(`
            SELECT id, title, client_name, role_title, sector, geography, year,
                   challenge, engagement_type, themes, capabilities
            FROM case_studies
            WHERE tenant_id = $1 AND (title ILIKE $2 OR client_name ILIKE $2 OR role_title ILIKE $2 OR challenge ILIKE $2)
            ORDER BY year DESC NULLS LAST LIMIT $3
          `, [req.tenant_id, `%${q}%`, csLimit]);
          const existing = new Set(csResults.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) csResults.push({ ...r, match_score: 60, score: 0.6 }); });
        }
        results.case_studies = csResults.slice(0, csLimit);
      } catch (e) { /* table may not exist */ }
    }

    // Search interactions
    if (collection === 'interactions' || collection === 'all') {
      try {
        const qdrantResults = qdrantResultsMap['interactions'] || [];
        if (qdrantResults.length > 0) {
          const intIds = qdrantResults.map(r => r.payload?.interaction_id).filter(Boolean);
          if (intIds.length > 0) {
            const { rows: interactions } = await db.query(`
              SELECT i.id, i.interaction_type, i.subject, i.summary, i.interaction_at, i.direction,
                     p.full_name as person_name, p.current_title
              FROM interactions i
              LEFT JOIN people p ON p.id = i.person_id
              WHERE i.id = ANY($1::uuid[]) AND i.tenant_id = $2
            `, [intIds, req.tenant_id]);
            const intMap = new Map(interactions.map(i => [i.id, i]));
            results.interactions = qdrantResults.map(r => {
              const int = intMap.get(r.payload?.interaction_id);
              if (!int) return null;
              return { ...int, match_score: Math.round(r.score * 100), score: r.score };
            }).filter(Boolean);
          }
        }
      } catch (e) { console.error('Search collection error:', e.message?.substring(0, 80)); }
    }

    // Search publications (ResearchMedium — separate collection, no tenant filter)
    if (searchPublicationsEnabled) {
      try {
        const pubs = await searchPublications(vector, { limit: 30, scoreThreshold: minScore });
        if (pubs.length > 0) {
          results.publications = pubs;
          results._research_momentum = computeResearchMomentum(pubs);
        }
      } catch (e) { /* RM unavailable — graceful degradation */ }
    }

    // Add score field to existing results
    results.people = (results.people || []).map(p => ({ ...p, score: (p.match_score || 50) / 100 }));
    results.companies = (results.companies || []).map(c => ({ ...c, score: (c.match_score || 50) / 100 }));
    results.documents = (results.documents || []).map(d => ({ ...d, score: (d.match_score || 50) / 100 }));

    // Sort all result arrays by match_score descending
    if (results.people) results.people.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    if (results.companies) results.companies.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    if (results.documents) results.documents.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    if (results.signals) results.signals.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));
    if (results.case_studies) results.case_studies.sort((a, b) => (b.match_score || 0) - (a.match_score || 0));

    res.json({
      query: q,
      collection,
      results,
      total: (results.people?.length || 0) + (results.companies?.length || 0) +
             (results.documents?.length || 0) + (results.signals?.length || 0) +
             (results.case_studies?.length || 0) + (results.interactions?.length || 0) +
             (results.publications?.length || 0),
    });
  } catch (err) {
    console.error('Search error:', err.message);
    res.status(500).json({ error: 'Search failed: ' + err.message });
  }
});

// Search index status
router.get('/api/search/index-status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [counts] } = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM people WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS people,
        (SELECT COUNT(*) FROM companies WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS companies,
        (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS documents,
        (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND embedded_at IS NOT NULL) AS signals,
        (SELECT COUNT(*) FROM case_studies WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS case_studies,
        (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS conversions,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS interactions
    `, [req.tenant_id]);
    res.json({
      people: Number(counts.people), companies: Number(counts.companies),
      documents: Number(counts.documents), signals: Number(counts.signals),
      case_studies: Number(counts.case_studies),
      conversions: Number(counts.conversions), interactions: Number(counts.interactions),
      total: Number(counts.people) + Number(counts.companies) + Number(counts.documents) +
             Number(counts.signals) + Number(counts.case_studies) + Number(counts.conversions) + Number(counts.interactions)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// DOCUMENTS
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/documents', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const sourceType = req.query.source_type;

    let where = 'WHERE tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter — hide private docs from non-owners
    if (req.user) {
      paramIdx++;
      where += ` AND (visibility IS NULL OR visibility != 'private' OR owner_user_id = $${paramIdx})`;
      params.push(req.user.user_id);
    } else {
      where += ` AND (visibility IS NULL OR visibility != 'private')`;
    }

    if (sourceType) {
      paramIdx++;
      where += ` AND source_type = $${paramIdx}`;
      params.push(sourceType);
    }

    paramIdx++;
    params.push(limit);
    paramIdx++;
    params.push(offset);

    const { rows } = await db.query(`
      SELECT id, title, source_type, source_name, source_url, author,
             published_at, processing_status, embedded_at IS NOT NULL AS is_embedded,
             visibility, owner_user_id, uploaded_by_user_id
      FROM external_documents
      ${where}
      ORDER BY published_at DESC NULLS LAST
      LIMIT $${paramIdx - 1} OFFSET $${paramIdx}
    `, params);

    const countParams = params.slice(0, -2); // everything except limit/offset
    const { rows: [{ cnt }] } = await db.query(
      `SELECT COUNT(*) AS cnt FROM external_documents ${where}`,
      countParams
    );

    res.json({ documents: rows, total: parseInt(cnt), limit, offset });
  } catch (err) {
    console.error('Documents error:', err.message);
    res.status(500).json({ error: 'Failed to fetch documents' });
  }
});

router.get('/api/documents/sources', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT rs.id, rs.name, rs.source_type, rs.url, rs.enabled,
             rs.last_fetched_at, rs.last_error, rs.consecutive_errors,
             (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_id = rs.id AND (ed.tenant_id IS NULL OR ed.tenant_id = $1)) AS doc_count
      FROM rss_sources rs
      ORDER BY rs.source_type, rs.name
    `, [req.tenant_id]);
    res.json({ sources: rows });
  } catch (err) {
    console.error('Sources error:', err.message);
    res.status(500).json({ error: 'Failed to fetch sources' });
  }
});

// Document privacy toggle
router.patch('/api/documents/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { visibility } = req.body; // 'company' or 'private'
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) {
      return res.status(400).json({ error: 'visibility must be "company" or "private"' });
    }

    const { rows: [doc] } = await db.query(
      'SELECT id, visibility, owner_user_id FROM external_documents WHERE id = $1 AND tenant_id = $2',
      [req.params.id, req.tenant_id]
    );
    if (!doc) return res.status(404).json({ error: 'Document not found' });

    // Only owner or admin can change private docs back to company
    if (doc.visibility === 'private' && doc.owner_user_id && doc.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change visibility of private documents' });
    }

    await db.query(`
      UPDATE external_documents
      SET visibility = $1,
          owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END
      WHERE id = $3 AND tenant_id = $4
    `, [visibility, req.user.user_id, req.params.id, req.tenant_id]);

    res.json({ id: req.params.id, visibility });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Also update Drive ingest to set uploaded_by
router.patch('/api/documents/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const allowed = ['title', 'visibility', 'summary'];
    const updates = [];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;
    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        idx++;
        updates.push(`${key} = $${idx}`);
        params.push(req.body[key]);
      }
    }
    if (req.body.visibility === 'private') {
      updates.push(`owner_user_id = $${++idx}`);
      params.push(req.user.user_id);
    }
    if (updates.length === 0) return res.json({ ok: true });
    await db.query(`UPDATE external_documents SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL GRABS — Editorial Intelligence
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/grabs', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const type = req.query.type; // macro, regional, sector, talent, contrarian
    const status = req.query.status || 'draft';

    let where = 'WHERE sg.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (status !== 'all') { idx++; where += ` AND sg.status = $${idx}`; params.push(status); }
    if (type) { idx++; where += ` AND sg.cluster_type = $${idx}`; params.push(type); }
    if (req.query.exclude_weekly === 'true') { where += ` AND sg.cluster_type != 'weekly_wrap'`; }

    // Region filter — match against geographies array or storyline text
    const region = req.query.region;
    if (region && region !== 'all' && region !== '') {
      const geoNames = REGION_MAP[region] || [];
      const regionCodes = REGION_CODES[region] || [];
      const allTerms = [...regionCodes, ...geoNames.slice(0, 5)];
      if (allTerms.length > 0) {
        const orParts = [];
        allTerms.forEach(g => { idx++; orParts.push(`$${idx} = ANY(sg.geographies)`); params.push(g); });
        allTerms.slice(0, 3).forEach(g => { idx++; orParts.push(`sg.storyline ILIKE $${idx}`); params.push(`%${g}%`); });
        where += ` AND (${orParts.join(' OR ')})`;
      }
    }

    idx++; params.push(limit);

    const { rows } = await db.query(`
      SELECT sg.* FROM signal_grabs sg ${where}
      ORDER BY sg.created_at DESC LIMIT $${idx}
    `, params);

    res.json({ grabs: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/grabs/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [grab] } = await db.query(
      'SELECT * FROM signal_grabs WHERE id = $1 AND tenant_id = $2',
      [req.params.id, req.tenant_id]
    );
    if (!grab) return res.status(404).json({ error: 'Grab not found' });
    res.json(grab);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/api/grabs/generate', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { execSync } = require('child_process');
    execSync('node scripts/compute_signal_grabs.js', { timeout: 120000, stdio: 'pipe' });
    const { rows } = await db.query(
      "SELECT * FROM signal_grabs WHERE tenant_id = $1 AND created_at > NOW() - INTERVAL '5 minutes' ORDER BY created_at DESC",
      [req.tenant_id]
    );
    res.json({ generated: rows.length, grabs: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.patch('/api/grabs/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status } = req.body;
    if (status) {
      await db.query(
        'UPDATE signal_grabs SET status = $1, published_at = CASE WHEN $1 = \'published\' THEN NOW() ELSE published_at END WHERE id = $2 AND tenant_id = $3',
        [status, req.params.id, req.tenant_id]
      );
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/grabs/weekly', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Get the most recent weekly wrap
    const { rows: [wrap] } = await db.query(`
      SELECT * FROM signal_grabs
      WHERE tenant_id = $1 AND cluster_type = 'weekly_wrap'
      ORDER BY created_at DESC LIMIT 1
    `, [req.tenant_id]);

    // Also get the top 5 daily grabs from the week
    const { rows: topGrabs } = await db.query(`
      SELECT * FROM signal_grabs
      WHERE tenant_id = $1 AND cluster_type != 'weekly_wrap' AND created_at > NOW() - INTERVAL '7 days'
      ORDER BY grab_score DESC LIMIT 5
    `, [req.tenant_id]);

    let wrapData = null;
    if (wrap) {
      try { wrapData = JSON.parse(wrap.observation); } catch(e) {}
    }

    res.json({ wrap: wrapData, wrap_id: wrap?.id, wrap_produced_at: wrap?.created_at, top_grabs: topGrabs, week_of: new Date().toISOString().slice(0, 10) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

/* REMOVED: waitlist admin routes (3) — moved to routes/admin.js */

// Admin: delete a signal
router.delete('/api/admin/signals/:id', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rowCount } = await db.query(
      'DELETE FROM signal_events WHERE id = $1 AND (tenant_id IS NULL OR tenant_id = $2)',
      [req.params.id, req.tenant_id]
    );
    if (!rowCount) return res.status(404).json({ error: 'Signal not found' });
    res.json({ ok: true });
  } catch (err) {
    console.error('Delete signal error:', err.message);
    res.status(500).json({ error: 'Failed to delete signal' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// LEMON SQUEEZY BILLING
// ═══════════════════════════════════════════════════════════════════════════════
//
// Env vars needed:
//   LEMONSQUEEZY_API_KEY     — from app.lemonsqueezy.com → Settings → API Keys
//   LEMONSQUEEZY_STORE_ID    — from Store → Settings → Store ID
//   LEMONSQUEEZY_VARIANT_ID  — from Products → your €10/mo plan → Variant ID
//   LEMONSQUEEZY_WEBHOOK_SECRET — from Settings → Webhooks → Signing Secret
//

const lsEnabled = !!process.env.LEMONSQUEEZY_API_KEY;
const LS_API = 'https://api.lemonsqueezy.com/v1';
const lsHeaders = lsEnabled ? {
  'Authorization': 'Bearer ' + process.env.LEMONSQUEEZY_API_KEY,
  'Accept': 'application/vnd.api+json',
  'Content-Type': 'application/vnd.api+json',
} : {};

// POST /api/billing/checkout — create Lemon Squeezy checkout URL
router.post('/api/billing/checkout', authenticateToken, async (req, res) => {
  if (!lsEnabled) return res.status(503).json({ error: 'Billing not configured' });
  try {
    const db = new TenantDB(req.tenant_id);
    const user = await db.queryOne('SELECT email, name FROM users WHERE id = $1', [req.user.user_id]);
    const tenant = await db.queryOne('SELECT id, name FROM tenants WHERE id = $1', [req.tenant_id]);

    var storeId = process.env.LEMONSQUEEZY_STORE_ID;
    var variantId = req.body.variant_id || process.env.LEMONSQUEEZY_VARIANT_ID;
    if (!storeId || !variantId) return res.status(400).json({ error: 'Billing product not configured' });

    var response = await fetch(LS_API + '/checkouts', {
      method: 'POST',
      headers: lsHeaders,
      body: JSON.stringify({
        data: {
          type: 'checkouts',
          attributes: {
            checkout_data: {
              email: user.email,
              name: user.name || tenant.name,
              custom: { tenant_id: req.tenant_id },
            },
            product_options: {
              redirect_url: (process.env.BASE_URL || 'https://www.autonodal.com') + '/index.html?billing=success',
              enabled_variants: [parseInt(variantId)],
            },
          },
          relationships: {
            store: { data: { type: 'stores', id: storeId } },
            variant: { data: { type: 'variants', id: variantId } },
          },
        },
      }),
    });

    var result = await response.json();
    if (!response.ok) {
      console.error('[Billing] Checkout creation failed:', JSON.stringify(result));
      return res.status(500).json({ error: 'Failed to create checkout' });
    }

    var checkoutUrl = result.data?.attributes?.url;
    res.json({ url: checkoutUrl });
  } catch (err) {
    console.error('Billing checkout error:', err.message);
    res.status(500).json({ error: 'Failed to create checkout session' });
  }
});

// POST /api/billing/portal — customer portal URL
router.post('/api/billing/portal', authenticateToken, async (req, res) => {
  if (!lsEnabled) return res.status(503).json({ error: 'Billing not configured' });
  try {
    const db = new TenantDB(req.tenant_id);
    const tenant = await db.queryOne('SELECT stripe_customer_id, subscription_id FROM tenants WHERE id = $1', [req.tenant_id]);
    if (!tenant || !tenant.stripe_customer_id) return res.status(400).json({ error: 'No billing account' });

    // Lemon Squeezy customer portal URL
    var response = await fetch(LS_API + '/customers/' + tenant.stripe_customer_id, {
      headers: lsHeaders,
    });
    var result = await response.json();
    var portalUrl = result.data?.attributes?.urls?.customer_portal;

    if (!portalUrl) return res.status(400).json({ error: 'Portal not available' });
    res.json({ url: portalUrl });
  } catch (err) {
    console.error('Billing portal error:', err.message);
    res.status(500).json({ error: 'Failed to open billing portal' });
  }
});

// GET /api/billing/status — current plan info
router.get('/api/billing/status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenant = await db.queryOne(
      'SELECT subscription_status, subscription_plan, subscription_ends_at, stripe_customer_id FROM tenants WHERE id = $1',
      [req.tenant_id]
    );
    res.json({
      status: tenant?.subscription_status || 'free',
      plan: tenant?.subscription_plan || 'free',
      ends_at: tenant?.subscription_ends_at,
      has_billing: !!tenant?.stripe_customer_id,
      billing_enabled: lsEnabled,
      provider: 'lemonsqueezy',
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to load billing status' });
  }
});

// POST /api/billing/webhook — Lemon Squeezy webhook handler
router.post('/api/billing/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  var webhookSecret = process.env.LEMONSQUEEZY_WEBHOOK_SECRET;

  // Verify signature if secret is set
  if (webhookSecret) {
    var crypto = require('crypto');
    var sig = req.headers['x-signature'];
    var expected = crypto.createHmac('sha256', webhookSecret).update(req.body).digest('hex');
    if (sig !== expected) {
      console.error('[Billing] Webhook signature mismatch');
      return res.status(400).send('Invalid signature');
    }
  }

  var event;
  try {
    event = JSON.parse(req.body);
  } catch (err) {
    return res.status(400).send('Invalid JSON');
  }

  var eventName = event.meta?.event_name;
  var customData = event.meta?.custom_data || {};
  var tenantId = customData.tenant_id;
  var attrs = event.data?.attributes || {};
  var subscriptionId = String(event.data?.id || '');
  var customerId = String(attrs.customer_id || '');

  console.log('[Billing] Webhook:', eventName, '| tenant:', tenantId, '| sub:', subscriptionId);

  try {
    switch (eventName) {
      case 'subscription_created':
        if (tenantId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = 'active',
              subscription_plan = 'pro',
              subscription_id = $1,
              stripe_customer_id = $2,
              updated_at = NOW()
            WHERE id = $3
          `, [subscriptionId, customerId, tenantId]);
          console.log('[Billing] Subscription created for tenant', tenantId);
        }
        break;

      case 'subscription_updated':
        var lsStatus = attrs.status; // active, past_due, unpaid, cancelled, expired, paused
        var endsAt = attrs.ends_at || attrs.renews_at;
        if (subscriptionId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = $1,
              subscription_ends_at = $2,
              updated_at = NOW()
            WHERE subscription_id = $3
          `, [lsStatus, endsAt, subscriptionId]);
        }
        break;

      case 'subscription_cancelled':
        if (subscriptionId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = 'cancelled',
              subscription_ends_at = $1,
              updated_at = NOW()
            WHERE subscription_id = $2
          `, [attrs.ends_at || new Date().toISOString(), subscriptionId]);
          console.log('[Billing] Subscription cancelled:', subscriptionId);
        }
        break;

      case 'subscription_expired':
        if (subscriptionId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = 'expired',
              updated_at = NOW()
            WHERE subscription_id = $1
          `, [subscriptionId]);
        }
        break;

      case 'subscription_payment_success':
        if (subscriptionId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = 'active',
              updated_at = NOW()
            WHERE subscription_id = $1
          `, [subscriptionId]);
        }
        break;

      case 'subscription_payment_failed':
        if (subscriptionId) {
          await platformPool.query(`
            UPDATE tenants SET
              subscription_status = 'past_due',
              updated_at = NOW()
            WHERE subscription_id = $1
          `, [subscriptionId]);
          console.log('[Billing] Payment failed:', subscriptionId);
        }
        break;
    }

    res.json({ received: true });
  } catch (err) {
    console.error('[Billing] Webhook handler error:', err.message);
    res.status(500).send('Webhook handler error');
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// EMAIL NOTIFICATIONS (Resend)
// ═══════════════════════════════════════════════════════════════════════════════

const resendEnabled = !!process.env.RESEND_API_KEY;
const resend = resendEnabled ? new (require('resend').Resend)(process.env.RESEND_API_KEY) : null;
const EMAIL_FROM = process.env.EMAIL_FROM || 'Autonodal <notifications@autonodal.com>';


router.get('/api/db-test', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const result = await platformPool.query('SELECT NOW() as time, current_database() as db');
    res.json({ ok: true, ...result.rows[0] });
  } catch(e) {
    res.json({ ok: false, error: e.message });
  }
});

router.get('/api/health/watchdog', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { runWatchdog } = require('./scripts/watchdog');
    const report = await runWatchdog();
    var code = report.status === 'CRITICAL' ? 503 : report.status === 'DEGRADED' ? 206 : 200;
    res.status(code).json(report);
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
const CHAT_SYSTEM = `You are Lorac — the AI intelligence agent for the Signal Intelligence Platform, currently serving MitchelLake.

CRITICAL BEHAVIOUR:
- DO NOT explain what you are going to do. Just DO it.
- DO NOT narrate your tool calls. Execute tools silently and present the results.
- Present results as a clean formatted table or list. Not a wall of text.

TOOL SELECTION (use the most specific tool available):
1. Market patterns, trends, convergence, "what are we seeing" → get_converging_themes
2. Priorities, best opportunities, where to focus, pipeline → get_ranked_opportunities
3. Talent movement, flight risk, re-engage, who to reach out to → get_talent_in_motion
4. "Who do we know at X", network into a company, connections → get_signal_proximity (pass company_name or company_id)
5. Dispatch actions: claim, review, generate, regenerate → dispatch_action
6. Person lookup by name, title, skills → search_people
7. Company lookup → search_companies
8. Person deep-dive → get_person_detail (needs person_id)
9. Company deep-dive → get_company_detail (needs company_id)
10. Signal search by type/confidence/time → search_signals
11. Placement history → search_placements
12. Research notes → search_research_notes
13. Save intel → log_intelligence
14. Create person → create_person
15. Import placements ("we placed X as Y at Z") → import_placements
16. Import case studies ("we did a CTO search for fintech in SG") → import_case_studies
17. Run a pipeline ("harvest podcasts", "sync gmail", "classify documents", etc.) → run_pipeline
18. Search case studies ("what work have we done in fintech", "case studies in APAC") → search_case_studies
19. Complex cross-referencing queries not covered above → run_sql_query with JOINs

CASE STUDY + PLACEMENT IMPORT RULES:
- Store EXACTLY what the user provides. Do NOT invent, embellish, or infer any data.
- If the user gives you "CTO, fintech, Singapore" — store those 3 fields only. Leave challenge, approach, outcome, themes as null.
- Do NOT generate narrative descriptions, challenges, approaches, or outcomes unless the user explicitly states them.
- Do NOT infer engagement_type, seniority_level, or themes — only set them if the user says them.
- Case studies are INTERNAL DRAFTS. Remind the user they need sanitisation before external use.
- Placements are ALWAYS internal — fees and candidate details never go external.

IMPORTANT: Prefer dedicated tools (#1-#5) over run_sql_query. They return pre-computed, pre-ranked results and are faster and more reliable. Only fall back to run_sql_query for questions that no dedicated tool covers.
When using run_sql_query, replace <TENANT> with the actual tenant_id from context.

CONTEXT:
- MitchelLake is a retained executive search firm (APAC, UK, global)
- Database: ~77K people, ~11K companies, ~22K documents, ~9K signals, ~500 placements
- Table names: people, companies, accounts, opportunities, conversions, engagements, pipeline_contacts, signal_events, interactions, team_proximity, external_documents, signal_dispatches, person_scores, person_signals, case_studies, receivables
- Signal types: capital_raising, ma_activity, geographic_expansion, strategic_hiring, leadership_change, partnership, product_launch, layoffs, restructuring
- Key columns: people.current_company_id → companies.id, signal_events.company_id → companies.id, conversions.client_id → accounts.id, accounts.company_id → companies.id
- team_proximity links people to users (team members) via team_member_id with relationship_strength (0-1)

PLACEMENTS TABLE — key columns for billing/WIP queries:
id, person_id, client_id, company_id, search_id, placed_by_user_id,
role_title, role_level, start_date, placement_fee (DECIMAL), currency (AUD|GBP|SGD|USD),
fee_stage (retainer_stage1|retainer_stage2|placement|project), fee_estimate,
invoice_number, invoice_date, payment_status (pending|invoiced|paid|overdue),
opportunity_type (WIP - Placed|WIP - Active|Proposal - Won|Proposal - Lost|Proposal - Draft|Proposal - Sent),
consultant_name, client_name_raw, source (wip_workbook|xero_export|manual),
source_sheet, notes, raw_monthly_data (JSONB monthly invoice amounts), created_at

KEY JOINS for billing queries:
SELECT p.*, c.name as client, u.name as consultant, pe.full_name as candidate
FROM placements p
LEFT JOIN companies c ON c.id = p.company_id
LEFT JOIN users u ON u.id = p.placed_by_user_id
LEFT JOIN people pe ON pe.id = p.person_id

CONSULTANT NAMES in data (match to users table):
Matt, JT (Jonathan Tanner), Illona, Mark Sparrow, Jamie Gripton, Michael Solomon (Solly),
Priyanka Haribhai, Conny Lim, Lexi Lazenby, Richard Farmer, Yoko Senga, Timo Kugler,
Rachel, Jimmy Grice, Claire Yellowlees, David Gumley, Rob, Sam, James,
Ananya Amin, Megan Burke, Sophie Cohen, Andrew

RECEIVABLES TABLE (outstanding invoices):
id, invoice_number, client_name, company_id, invoice_date, due_date,
invoice_total, currency, status, days_overdue, notes, action

STYLE:
- Concise. No preamble. Execute then present results.
- Format: [Name](/person.html?id=X) | Title | Company | Signal/Score
- Australian English
- When saving intel, confirm what was extracted
- For file imports, preview before committing
- LinkedIn CSV: auto-detect type from [LinkedIn Export Type] tag, use import_linkedin_connections or import_linkedin_messages

RULES:
- NEVER say "let me search" then show empty results then say "let me try another approach". Use SQL with JOINs from the start.
- Never fabricate data
- For UPDATE/DELETE, confirm with user first
- SQL: SELECT, UPDATE, INSERT, DELETE allowed. DROP/ALTER/TRUNCATE blocked.
- Prioritise recency — sort by most recent first
- Flag stale intel (>6 months)`;

const CHAT_TOOLS = [
  { name: 'search_people', description: 'Semantic + SQL search for people/candidates by name, title, company, location, skills.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'Search query' }, filters: { type: 'object', properties: { seniority: { type: 'string' }, has_notes: { type: 'boolean' }, company: { type: 'string' } } }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'search_companies', description: 'Search companies by name, sector, geography.', input_schema: { type: 'object', properties: { query: { type: 'string' }, filters: { type: 'object', properties: { is_client: { type: 'boolean' }, sector: { type: 'string' }, geography: { type: 'string' } } }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'get_person_detail', description: 'Full dossier for a person: notes, signals, interactions, colleagues.', input_schema: { type: 'object', properties: { person_id: { type: 'string' } }, required: ['person_id'] } },
  { name: 'get_company_detail', description: 'Full company dossier: signals, people, placements.', input_schema: { type: 'object', properties: { company_id: { type: 'string' } }, required: ['company_id'] } },
  { name: 'search_signals', description: 'Search market signals by type, category, company, confidence, time range.', input_schema: { type: 'object', properties: { signal_type: { type: 'string' }, category: { type: 'string' }, company_name: { type: 'string' }, min_confidence: { type: 'number' }, days_back: { type: 'integer', default: 30 }, limit: { type: 'integer', default: 15 } } } },
  { name: 'search_placements', description: 'Search placement history by company, role, candidate.', input_schema: { type: 'object', properties: { query: { type: 'string' }, limit: { type: 'integer', default: 20 } } } },
  { name: 'search_research_notes', description: 'Search internal research notes — comp expectations, timing, preferences.', input_schema: { type: 'object', properties: { query: { type: 'string' }, person_name: { type: 'string' }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'log_intelligence', description: 'Save intelligence as a research note. Extracts structured data.', input_schema: { type: 'object', properties: { person_name: { type: 'string' }, company_name: { type: 'string' }, intelligence: { type: 'string' }, subject: { type: 'string' }, extracted: { type: 'object', properties: { timing: { type: 'string' }, compensation: { type: 'string' }, location_preference: { type: 'string' }, role_interests: { type: 'string' }, constraints: { type: 'string' }, warm_intros: { type: 'string' }, sentiment: { type: 'string' } } } }, required: ['person_name', 'intelligence', 'subject'] } },
  { name: 'create_person', description: 'Create a new person record.', input_schema: { type: 'object', properties: { full_name: { type: 'string' }, current_title: { type: 'string' }, current_company_name: { type: 'string' }, email: { type: 'string' }, phone: { type: 'string' }, location: { type: 'string' }, linkedin_url: { type: 'string' }, seniority_level: { type: 'string' } }, required: ['full_name'] } },
  { name: 'process_uploaded_file', description: 'Process uploaded CSV/PDF/XLSX. Actions: preview, import_people, import_companies, extract_text, import_linkedin_connections, import_linkedin_messages, import_workbook (XLSX multi-tab: stores each sheet as searchable document, embeds in Qdrant). For XLSX files with multiple tabs, use import_workbook to ingest all sheets.', input_schema: { type: 'object', properties: { file_id: { type: 'string' }, action: { type: 'string', enum: ['preview', 'import_people', 'import_companies', 'extract_text', 'import_linkedin_connections', 'import_linkedin_messages', 'import_workbook'] }, column_mapping: { type: 'object' } }, required: ['file_id', 'action'] } },
  { name: 'run_sql_query', description: 'PRIMARY TOOL — Run SQL (SELECT, UPDATE, INSERT, DELETE) against the database. Use this FIRST for any cross-referencing query. JOINs are fast. Always include tenant_id filter. Key tables: people, companies, signal_events, interactions, conversions, accounts, opportunities, team_proximity, person_scores.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'SQL query. Must include AND tenant_id = \'<tenant_id>\' for data tables.' }, explanation: { type: 'string', description: 'Brief one-line explanation of what this query does' } }, required: ['query', 'explanation'] } },
  { name: 'get_platform_stats', description: 'Current platform statistics.', input_schema: { type: 'object', properties: {} } },

  // ── MCP-style intelligence tools ──────────────────────────────────────────
  {
    name: 'get_converging_themes',
    description: 'Returns signal clusters by type and sector showing where multiple companies exhibit the same signal pattern. Includes client overlap, candidate counts, and active search pipeline matches. Use when asked about market patterns, trends, sector activity, convergence, or "what are we seeing". Much faster and more reliable than composing SQL for these questions.',
    input_schema: {
      type: 'object',
      properties: {
        lookback_days: { type: 'integer', default: 30, description: 'Days to look back for signal activity (default 30)' },
        min_companies: { type: 'integer', default: 3, description: 'Minimum companies per cluster (default 3)' }
      }
    }
  },
  {
    name: 'get_ranked_opportunities',
    description: 'Returns companies ranked by composite opportunity score combining signal strength, network overlap, geographic relevance, and placement adjacency. Use when asked about priorities, where to focus, best opportunities, pipeline, or "what should we be working on". Supports region filtering and score thresholds.',
    input_schema: {
      type: 'object',
      properties: {
        region: { type: 'string', description: 'Region code: AU, SG, UK, US, APAC, EMEA, or omit for all' },
        min_score: { type: 'number', default: 0, description: 'Minimum composite score (0-1)' },
        limit: { type: 'integer', default: 15, description: 'Max results to return' },
        by_region: { type: 'boolean', default: false, description: 'If true, returns top opportunities grouped by region instead of a flat list' }
      }
    }
  },
  {
    name: 'get_talent_in_motion',
    description: 'Returns people showing movement signals: flight risk (at companies with restructuring/layoff/M&A signals), activity spikes (high engagement/timing scores), re-engagement windows (senior contacts at signal companies dormant 60+ days), and recent person-level signals. Use when asked about talent movement, who to reach out to, re-engagement opportunities, flight risk, or market talent activity.',
    input_schema: {
      type: 'object',
      properties: {
        focus: { type: 'string', enum: ['all', 'flight_risk', 'active_profiles', 'reengage', 'person_signals'], default: 'all', description: 'Which talent motion category to return' },
        limit: { type: 'integer', default: 10, description: 'Max results per category' }
      }
    }
  },
  {
    name: 'get_signal_proximity',
    description: 'For a given signal or company, returns the network proximity map: who we know there, team member connections, relationship strengths, contact scores (timing, receptivity), and whether the company is a client. Use when asked "who do we know at X", "what is our connection to X", "how do we get into X", or "show me the network for X". Returns a structured graph of team→contact→company relationships.',
    input_schema: {
      type: 'object',
      properties: {
        signal_id: { type: 'string', description: 'Signal event UUID — use this when the question is about a specific signal' },
        company_id: { type: 'string', description: 'Company UUID — use this when the question is about a company (alternative to signal_id)' },
        company_name: { type: 'string', description: 'Company name — will be resolved to company_id if company_id not provided' }
      }
    }
  },
  {
    name: 'dispatch_action',
    description: 'Perform actions on signal dispatches: claim for review, unclaim, update status, trigger generation for new signals, or regenerate content with a theme override. Use when asked to "claim that dispatch", "mark as reviewed", "generate dispatches", "send that dispatch", or "rewrite the blog post about X".',
    input_schema: {
      type: 'object',
      properties: {
        action: { type: 'string', enum: ['claim', 'unclaim', 'update_status', 'generate_all', 'rescan_proximity', 'regenerate_content'], description: 'The action to perform' },
        dispatch_id: { type: 'string', description: 'Required for claim, unclaim, update_status, regenerate_content' },
        status: { type: 'string', enum: ['draft', 'claimed', 'reviewed', 'sent', 'archived'], description: 'New status — for update_status action' },
        theme: { type: 'string', description: 'Override theme for regenerate_content action' }
      },
      required: ['action']
    }
  },
  {
    name: 'import_placements',
    description: 'Import placement records. Use when the user pastes or describes recent placements — e.g., "we placed Jane Smith as CTO at Acme Corp". Resolves people and companies against existing records, creates conversions entries. Each placement needs at minimum: candidate name, role title, and client/company name. Optional: start date, fee, currency.',
    input_schema: {
      type: 'object',
      properties: {
        placements: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              candidate_name: { type: 'string', description: 'Full name of placed candidate' },
              role_title: { type: 'string', description: 'Role they were placed into' },
              company_name: { type: 'string', description: 'Client company name' },
              start_date: { type: 'string', description: 'Start date (YYYY-MM-DD or approximate)' },
              placement_fee: { type: 'number', description: 'Fee amount if known' },
              currency: { type: 'string', default: 'AUD', description: 'Currency code' },
              notes: { type: 'string', description: 'Any additional context' }
            },
            required: ['candidate_name', 'role_title', 'company_name']
          },
          description: 'Array of placement records to import'
        }
      },
      required: ['placements']
    }
  },
  {
    name: 'import_case_studies',
    description: 'Import case study records from a list. Store EXACTLY what the user provides — do NOT invent, embellish, or infer fields the user did not state. If the user says "CTO search, fintech, Singapore, 2024" then store only those fields and leave everything else null. Case studies are created as internal drafts requiring sanitisation before external use.',
    input_schema: {
      type: 'object',
      properties: {
        case_studies: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              client_name: { type: 'string', description: 'Client company name (INTERNAL — will not be published externally without sanitisation)' },
              role_title: { type: 'string', description: 'Role searched for' },
              engagement_type: { type: 'string', enum: ['executive_search', 'board_advisory', 'leadership_assessment', 'team_build', 'succession', 'market_mapping'], description: 'Type of engagement' },
              seniority_level: { type: 'string', enum: ['c_suite', 'vp', 'director', 'head', 'senior'], description: 'Seniority of role' },
              sector: { type: 'string', description: 'Industry sector' },
              geography: { type: 'string', description: 'Region or country' },
              year: { type: 'integer', description: 'Year of engagement' },
              challenge: { type: 'string', description: 'What the client needed' },
              approach: { type: 'string', description: 'How MitchelLake approached it' },
              outcome: { type: 'string', description: 'Result achieved' },
              themes: { type: 'array', items: { type: 'string' }, description: 'Thematic tags e.g. cross-border, founder-transition' },
              capabilities: { type: 'array', items: { type: 'string' }, description: 'Capabilities demonstrated e.g. post-acquisition, turnaround' }
            },
            required: ['role_title']
          },
          description: 'Array of case study records to import'
        }
      },
      required: ['case_studies']
    }
  },
  {
    name: 'search_case_studies',
    description: 'Search the case study library by keyword, sector, geography, client, or role. Uses semantic vector search when available, falls back to SQL text search. Use when asked about past work, relevant experience, case studies, or "what have we done in X".',
    input_schema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query — client name, sector, role type, geography, or general topic' },
        limit: { type: 'integer', default: 10 }
      },
      required: ['query']
    }
  },
  {
    name: 'run_pipeline',
    description: 'Trigger a platform pipeline manually. Use when the user asks to run, trigger, or execute a pipeline such as: harvest_podcasts, sync_gmail, gmail_match, sync_drive, classify_documents, cleanup_broken_podcasts, import_case_studies_bulk, ingest_signals, compute_scores, match_searches, enrich_content, signal_dispatch, compute_network_topology, compute_triangulation, compute_signal_grabs. Say "run the X pipeline" or "harvest podcasts" or "sync gmail".',
    input_schema: {
      type: 'object',
      properties: {
        pipeline_key: {
          type: 'string',
          description: 'Pipeline key to run. Common ones: harvest_podcasts, sync_gmail, gmail_match, sync_drive, classify_documents, cleanup_broken_podcasts, ingest_signals, compute_scores, compute_signal_grabs, signal_dispatch, compute_network_topology, compute_triangulation, embed_intelligence, migrate_wip_schema, ingest_wip_invoices, ingest_wip_consultants, ingest_receivables'
        }
      },
      required: ['pipeline_key']
    }
  },
];

async function executeTool(name, input, userId, tenantId) {
  try {
    switch (name) {
      case 'search_people': {
        const { query, filters = {}, limit = 10 } = input;
        let results = [];
        try {
          const vector = await generateQueryEmbedding(query);
          const peopleTenantFilter = tenantId ? { should: [{ key: 'tenant_id', match: { value: tenantId } }, { is_empty: { key: 'tenant_id' } }] } : null;
          const qr = await qdrantSearch('people', vector, limit * 2, peopleTenantFilter);
          if (qr.length) {
            const { rows } = await db.query(`SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location, p.seniority_level, p.email, p.linkedin_url, p.headline, p.expertise_tags,
              (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS note_count,
              (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS latest_note_date,
              (SELECT i.subject FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id ORDER BY i.interaction_at DESC NULLS LAST LIMIT 1) AS latest_note_subject
              FROM people p WHERE p.id = ANY($1::uuid[]) AND p.tenant_id = $2`, [qr.map(r => r.id), tenantId]);
            const map = new Map(rows.map(r => [r.id, r]));
            results = qr.map(r => ({ ...map.get(r.id), score: r.score })).filter(r => r.full_name);
          }
        } catch (e) {}
        if (results.length < 3) {
          const { rows } = await db.query(`SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location, p.seniority_level, p.email, p.headline,
            (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS note_count,
            (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS latest_note_date
            FROM people p WHERE (p.full_name ILIKE $1 OR p.current_title ILIKE $1 OR p.current_company_name ILIKE $1 OR p.headline ILIKE $1) AND p.tenant_id = $3 ORDER BY p.full_name LIMIT $2`, [`%${query}%`, limit, tenantId]);
          const existing = new Set(results.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) results.push(r); });
        }
        // Sort by recency — most recent notes first
        results.sort((a, b) => {
          const da = a.latest_note_date ? new Date(a.latest_note_date) : new Date(0);
          const db = b.latest_note_date ? new Date(b.latest_note_date) : new Date(0);
          return db - da;
        });
        return JSON.stringify(results.slice(0, limit));
      }
      case 'search_companies': {
        const { query, filters = {}, limit = 10 } = input;
        const { rows } = await db.query(`SELECT c.id, c.name, c.sector, c.geography, c.domain, c.employee_count_band, c.is_client, c.description, (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $3) AS people_count, (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $3)) AS signal_count FROM companies c WHERE (c.name ILIKE $1 OR c.sector ILIKE $1 OR c.geography ILIKE $1) AND c.tenant_id = $3 ${filters.is_client ? 'AND c.is_client = true' : ''} ORDER BY c.is_client DESC, c.name LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'get_person_detail': {
        const { rows: [p] } = await db.query(`SELECT p.*, c.name AS company_name_linked, c.id AS company_id_linked FROM people p LEFT JOIN companies c ON p.current_company_id = c.id WHERE p.id = $1 AND p.tenant_id = $2`, [input.person_id, tenantId]);
        if (!p) return JSON.stringify({ error: 'Not found' });
        const { rows: notes } = await db.query(`SELECT subject, summary, interaction_at, note_quality, extracted_intelligence FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2 ORDER BY interaction_at DESC NULLS LAST LIMIT 10`, [input.person_id, tenantId]);
        const { rows: sigs } = await db.query(`SELECT signal_type, title, description, confidence_score FROM person_signals WHERE person_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 10`, [input.person_id, tenantId]);
        return JSON.stringify({ ...p, research_notes: notes, person_signals: sigs });
      }
      case 'get_company_detail': {
        const { rows: [co] } = await db.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [input.company_id, tenantId]);
        if (!co) return JSON.stringify({ error: 'Not found' });
        const { rows: sigs } = await db.query(`SELECT signal_type, evidence_summary, confidence_score, detected_at FROM signal_events WHERE company_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 15`, [input.company_id, tenantId]);
        const { rows: ppl } = await db.query(`SELECT id, full_name, current_title, seniority_level FROM people WHERE current_company_id = $1 AND tenant_id = $2 ORDER BY full_name LIMIT 30`, [input.company_id, tenantId]);
        let pls = []; try { const { rows } = await db.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee FROM conversions pl LEFT JOIN accounts cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE (cl.company_id = $1 OR cl.name ILIKE (SELECT name FROM companies WHERE id = $1)) AND pl.tenant_id = $2 ORDER BY pl.start_date DESC`, [input.company_id, tenantId]); pls = rows; } catch (e) {}
        return JSON.stringify({ ...co, signals: sigs, people: ppl, placements: pls });
      }
      case 'search_signals': {
        const { signal_type, category, company_name, min_confidence = 0.5, days_back = 30, limit = 15 } = input;
        const w = [`se.confidence_score >= ${min_confidence}`, `se.detected_at >= NOW() - INTERVAL '${days_back} days'`, `se.tenant_id = $1`];
        if (signal_type) w.push(`se.signal_type = '${signal_type}'`);
        if (category) w.push(`se.signal_category = '${category}'`);
        if (company_name) w.push(`c.name ILIKE '%${company_name}%'`);
        const { rows } = await db.query(`SELECT se.signal_type, se.signal_category, se.evidence_summary, se.confidence_score, se.detected_at, se.source_url, c.name AS company_name, c.id AS company_id FROM signal_events se LEFT JOIN companies c ON se.company_id = c.id WHERE ${w.join(' AND ')} ORDER BY se.confidence_score DESC LIMIT ${limit}`, [tenantId]);
        return JSON.stringify(rows);
      }
      case 'search_placements': {
        const { query = '', limit = 20 } = input;
        const { rows } = await db.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee, cl.name AS company_name, cl.id AS company_id FROM conversions pl LEFT JOIN accounts cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE (pe.full_name ILIKE $1 OR pl.role_title ILIKE $1 OR cl.name ILIKE $1) AND pl.tenant_id = $3 ORDER BY pl.start_date DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'search_research_notes': {
        const { query, person_name, limit = 10 } = input;
        let extra = person_name ? ` AND p.full_name ILIKE '%${person_name}%'` : '';
        const { rows } = await db.query(`SELECT i.subject, i.summary, i.interaction_at, i.note_quality, i.extracted_intelligence, p.full_name, p.id AS person_id, p.current_title, p.current_company_name FROM interactions i JOIN people p ON i.person_id = p.id WHERE i.interaction_type = 'research_note' AND (i.summary ILIKE $1 OR i.subject ILIKE $1) AND i.tenant_id = $3${extra} ORDER BY i.interaction_at DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'log_intelligence': {
        const { person_name, company_name, intelligence, subject, extracted = {} } = input;
        let personId;
        const { rows: ex } = await db.query(`SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [person_name, tenantId]);
        if (ex.length) { personId = ex[0].id; }
        else {
          const { rows: [np] } = await db.query(`INSERT INTO people (full_name, current_company_name, source, created_by, tenant_id) VALUES ($1, $2, 'chat_intel', $3, $4) RETURNING id`, [person_name, company_name || null, userId, tenantId]);
          personId = np.id;
        }
        const { rows: [note] } = await db.query(`INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, extracted_intelligence, source, interaction_at, tenant_id) VALUES ($1, $2, $2, 'research_note', $3, $4, $5, 'chat_concierge', NOW(), $6) RETURNING id`, [personId, userId, subject, intelligence, JSON.stringify(extracted), tenantId]);
        auditLog(userId, 'log_intelligence', 'person', personId, { person_name, subject, source: 'chat_concierge' });

        // Write-back to Ezekia CRM if person has a source_id
        let ezekiaPushed = false;
        if (process.env.EZEKIA_API_TOKEN) {
          try {
            const { rows: [p] } = await db.query('SELECT source_id FROM people WHERE id = $1 AND source = $2', [personId, 'ezekia']);
            if (p?.source_id) {
              const ezekia = require('../lib/ezekia');
              const baseUrl = process.env.APP_URL || 'https://www.autonodal.com';
              const personUrl = `${baseUrl}/person.html?id=${personId}`;
              const noteHtml = `<p><strong>${subject || 'Intelligence Note'}</strong></p>` +
                `<p>${intelligence.replace(/\n/g, '<br>')}</p>` +
                (Object.keys(extracted || {}).length > 0 ? `<p><em>Extracted: ${Object.entries(extracted).filter(([,v]) => v).map(([k,v]) => `${k}: ${v}`).join(' | ')}</em></p>` : '') +
                `<p><a href="${personUrl}">View full dossier in Autonodal</a></p>` +
                `<p style="font-size:11px;color:#888;">Logged via Autonodal by ${req.user?.name || 'team member'} — ${new Date().toLocaleDateString()}</p>`;
              await ezekia.addPersonNote(parseInt(p.source_id), noteHtml, { subject: subject || 'Intelligence Note' });
              ezekiaPushed = true;
            }
          } catch (e) { /* non-fatal */ }
        }

        return JSON.stringify({ success: true, person_id: personId, note_id: note.id, person_name, subject, extracted, ezekia_synced: ezekiaPushed, message: `Saved on ${person_name}'s record${ezekiaPushed ? ' (also pushed to Ezekia CRM)' : ''}` });
      }
      case 'create_person': {
        const { full_name, current_title, current_company_name, email, phone, location, linkedin_url, seniority_level } = input;
        const { rows: dupes } = await db.query(`SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 3`, [full_name, tenantId]);
        if (dupes.length) return JSON.stringify({ existing_matches: dupes, message: 'Possible duplicates found' });
        let coId = null;
        if (current_company_name) { const { rows } = await db.query(`SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [current_company_name, tenantId]); if (rows.length) coId = rows[0].id; }
        const { rows: [p] } = await db.query(`INSERT INTO people (full_name, current_title, current_company_name, current_company_id, email, phone, location, linkedin_url, seniority_level, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'chat_concierge',$10,$11) RETURNING id, full_name`, [full_name, current_title||null, current_company_name||null, coId, email||null, phone||null, location||null, linkedin_url||null, seniority_level||null, userId, tenantId]);
        auditLog(userId, 'create_person', 'person', p.id, { full_name, source: 'chat_concierge' });
        return JSON.stringify({ ...p, message: `Created ${full_name}` });
      }
      case 'process_uploaded_file': {
        const { file_id, action, column_mapping } = input;
        const fm = uploadedFiles.get(file_id);
        if (!fm) return JSON.stringify({ error: 'File not found or expired' });
        if (action === 'preview' || action === 'extract_text') {
          return JSON.stringify({ filename: fm.originalname, type: fm.mimetype, rows: fm.preview?.length||0, columns: fm.columns||[], preview: (fm.preview||[]).slice(0,5), text_excerpt: fm.text ? fm.text.slice(0,2000) : null });
        }
        if (action === 'import_people' && fm.preview) {
          const m = column_mapping || fm.suggestedMapping || {};
          let imported = 0, skipped = 0;
          for (const row of fm.preview) {
            const name = row[m.full_name||'Name']||row['Full Name']||row['name'];
            if (!name || name.trim().length < 2) { skipped++; continue; }
            const { rows: d } = await db.query(`SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [name.trim(), tenantId]);
            if (d.length) { skipped++; continue; }
            await db.query(`INSERT INTO people (full_name, current_title, current_company_name, email, location, linkedin_url, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,'csv_import',$7,$8)`,
              [name.trim(), row[m.current_title||'Title']||row['Job Title']||null, row[m.current_company_name||'Company']||row['Organization']||null, row[m.email||'Email']||null, row[m.location||'Location']||null, row[m.linkedin_url||'LinkedIn']||null, userId, tenantId]);
            imported++;
          }
          auditLog(userId, 'csv_import', 'people', null, { imported, skipped, total: fm.preview.length, filename: fm.originalname });
          return JSON.stringify({ imported, skipped, total: fm.preview.length });
        }
        if (action === 'import_linkedin_connections' && fm.preview) {
          // Load people for matching
          const { rows: dbPeople } = await db.query(`SELECT id, full_name, first_name, last_name, linkedin_url, current_company_name, email FROM people WHERE full_name IS NOT NULL AND full_name != '' AND tenant_id = $1`, [tenantId]);
          const linkedinIndex = new Map(), nameIndex = new Map(), emailIndex = new Map();
          for (const p of dbPeople) {
            if (p.linkedin_url) { const slug = p.linkedin_url.toLowerCase().replace(/\/+$/, '').split('?')[0].match(/linkedin\.com\/in\/([^\/]+)/); if (slug) linkedinIndex.set(slug[1], p); }
            const norm = `${(p.first_name || p.full_name?.split(' ')[0] || '').toLowerCase()} ${(p.last_name || p.full_name?.split(' ').slice(1).join(' ') || '').toLowerCase()}`.trim();
            if (norm.length > 1) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
            if (p.email) emailIndex.set(p.email.toLowerCase(), p);
          }

          // Ensure tables exist
          await db.query(`CREATE TABLE IF NOT EXISTS team_proximity (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), person_id UUID REFERENCES people(id) ON DELETE CASCADE, team_member_id UUID REFERENCES users(id), proximity_type VARCHAR(50) NOT NULL, source VARCHAR(50) NOT NULL, strength NUMERIC(3,2) DEFAULT 0.5, context TEXT, connected_at TIMESTAMPTZ, metadata JSONB DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(person_id, team_member_id, proximity_type, source))`);
          await db.query(`CREATE TABLE IF NOT EXISTS linkedin_connections (id SERIAL PRIMARY KEY, team_member_id UUID REFERENCES users(id), first_name VARCHAR(255), last_name VARCHAR(255), full_name VARCHAR(255), linkedin_url TEXT, linkedin_slug VARCHAR(255), email VARCHAR(255), company VARCHAR(255), position VARCHAR(255), connected_at TIMESTAMPTZ, matched_person_id UUID REFERENCES people(id), match_method VARCHAR(50), match_confidence NUMERIC(3,2), imported_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(linkedin_slug))`);

          const stats = { total: 0, matched: 0, unmatched: 0, proximity_created: 0, new_people: 0, errors: 0 };
          for (const row of fm.preview) {
            stats.total++;
            const firstName = row['First Name'] || '';
            const lastName = row['Last Name'] || '';
            const fullName = `${firstName} ${lastName}`.trim();
            const linkedinUrl = row['URL'] || '';
            const email = row['Email Address'] || '';
            const company = row['Company'] || '';
            const position = row['Position'] || '';
            const connectedOn = row['Connected On'] ? (new Date(row['Connected On']).toISOString().slice(0, 10) || null) : null;
            if (!fullName || fullName.length < 2) continue;

            const slug = linkedinUrl ? (linkedinUrl.toLowerCase().replace(/\/+$/, '').split('?')[0].match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
            let matchedPerson = null, matchMethod = null, matchConfidence = 0;

            // Match: LinkedIn URL > Email > Name+Company > Name
            if (slug && linkedinIndex.has(slug)) { matchedPerson = linkedinIndex.get(slug); matchMethod = 'linkedin_url'; matchConfidence = 0.99; }
            if (!matchedPerson && email) { const m = emailIndex.get(email.toLowerCase()); if (m) { matchedPerson = m; matchMethod = 'email'; matchConfidence = 0.95; } }
            if (!matchedPerson) {
              const norm = `${firstName.toLowerCase()} ${lastName.toLowerCase()}`.trim();
              const cands = nameIndex.get(norm) || [];
              if (cands.length === 1) { matchedPerson = cands[0]; matchMethod = 'name_unique'; matchConfidence = 0.80; }
              else if (cands.length > 1 && company) { const cm = cands.find(p => p.current_company_name && p.current_company_name.toLowerCase().includes(company.toLowerCase())); if (cm) { matchedPerson = cm; matchMethod = 'name_company'; matchConfidence = 0.90; } }
            }

            if (matchedPerson) {
              stats.matched++;
              // Create team_proximity
              if (userId) {
                try {
                  let strength = 0.5;
                  if (connectedOn) { const yrs = (Date.now() - new Date(connectedOn).getTime()) / (365.25*24*60*60*1000); if (yrs > 5) strength = 0.8; else if (yrs > 2) strength = 0.7; else if (yrs > 1) strength = 0.6; }
                  strength = Math.min(1.0, strength + (matchConfidence - 0.5) * 0.2);
                  await db.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at, metadata, tenant_id) VALUES ($1,$2,'linkedin_connection','linkedin_import',$3,$4,$5,$6,$7) ON CONFLICT (person_id, team_member_id, proximity_type, source) DO UPDATE SET strength = GREATEST(team_proximity.strength, EXCLUDED.strength), context = EXCLUDED.context, updated_at = NOW()`, [matchedPerson.id, userId, strength.toFixed(2), `${position} @ ${company}`, connectedOn, JSON.stringify({ linkedin_url: linkedinUrl, match_method: matchMethod, match_confidence: matchConfidence }), tenantId]);
                  stats.proximity_created++;
                } catch (e) { if (!e.message.includes('duplicate')) stats.errors++; }
              }
              // Update LinkedIn URL if missing
              if (linkedinUrl && !matchedPerson.linkedin_url) { try { await db.query('UPDATE people SET linkedin_url = $1, updated_at = NOW() WHERE id = $2 AND linkedin_url IS NULL AND tenant_id = $3', [linkedinUrl, matchedPerson.id, tenantId]); } catch (e) {} }
            } else {
              stats.unmatched++;
              // Create new person record for unmatched connections
              try {
                const { rows: dupes } = await db.query('SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1', [fullName, tenantId]);
                if (!dupes.length) {
                  const { rows: [np] } = await db.query(`INSERT INTO people (full_name, current_title, current_company_name, linkedin_url, email, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,'linkedin_import',$6,$7) RETURNING id`, [fullName, position || null, company || null, linkedinUrl || null, email || null, userId, tenantId]);
                  stats.new_people++;
                  // Also create proximity for new person
                  if (userId && np) {
                    try { await db.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at, tenant_id) VALUES ($1,$2,'linkedin_connection','linkedin_import',0.5,$3,$4,$5) ON CONFLICT DO NOTHING`, [np.id, userId, `${position} @ ${company}`, connectedOn, tenantId]); stats.proximity_created++; } catch (e) {}
                  }
                }
              } catch (e) { stats.errors++; }
            }

            // Store in linkedin_connections table
            if (slug) {
              try { await db.query(`INSERT INTO linkedin_connections (team_member_id, first_name, last_name, full_name, linkedin_url, linkedin_slug, email, company, position, connected_at, matched_person_id, match_method, match_confidence) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (linkedin_slug) DO UPDATE SET company = EXCLUDED.company, position = EXCLUDED.position, matched_person_id = COALESCE(EXCLUDED.matched_person_id, linkedin_connections.matched_person_id), imported_at = NOW()`, [userId, firstName, lastName, fullName, linkedinUrl, slug, email||null, company||null, position||null, connectedOn, matchedPerson?.id||null, matchMethod, matchConfidence||null]); } catch (e) {}
            }
          }
          auditLog(userId, 'linkedin_import', 'people', null, { total: stats.total, matched: stats.matched, new_people: stats.new_people, proximity_created: stats.proximity_created, filename: fm.originalname });
          return JSON.stringify({ ...stats, match_rate: stats.total > 0 ? `${(stats.matched / stats.total * 100).toFixed(1)}%` : '0%', message: `Imported ${stats.total} LinkedIn connections: ${stats.matched} matched to existing people, ${stats.new_people} new people created, ${stats.proximity_created} team proximity links` });
        }

        if (action === 'import_linkedin_messages' && fm.preview) {
          const stats = { total: 0, matched: 0, interactions_created: 0, unmatched_senders: new Set(), errors: 0 };

          // Load people for matching by name
          const { rows: dbPeople } = await db.query(`SELECT id, full_name FROM people WHERE full_name IS NOT NULL AND tenant_id = $1`, [tenantId]);
          const nameMap = new Map();
          for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

          // Group messages by conversation/sender
          const conversations = new Map();
          for (const row of fm.preview) {
            const from = row['FROM'] || row['From'] || row['from'] || '';
            const to = row['TO'] || row['To'] || row['to'] || '';
            const content = row['CONTENT'] || row['Content'] || row['content'] || row['BODY'] || row['Body'] || row['body'] || '';
            const date = row['DATE'] || row['Date'] || row['date'] || '';
            const convId = row['CONVERSATION ID'] || row['Conversation ID'] || row['conversation id'] || `${from}-${to}`;
            if (!content.trim()) continue;
            stats.total++;

            if (!conversations.has(convId)) conversations.set(convId, []);
            conversations.get(convId).push({ from, to, content, date });
          }

          // Process each conversation as an interaction
          for (const [convId, messages] of conversations) {
            // Find the other person (not the current user) in the conversation
            const participants = new Set();
            messages.forEach(m => { if (m.from) participants.add(m.from.trim()); if (m.to) participants.add(m.to.trim()); });

            for (const name of participants) {
              const match = nameMap.get(name.toLowerCase().trim());
              if (match) {
                stats.matched++;
                // Create a condensed interaction from all messages in this conversation
                const sorted = messages.sort((a, b) => new Date(a.date) - new Date(b.date));
                const summary = sorted.map(m => `[${m.date}] ${m.from}: ${m.content}`).join('\n').slice(0, 5000);
                const latestDate = sorted[sorted.length - 1]?.date;

                try {
                  await db.query(`INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, source, interaction_at, tenant_id) VALUES ($1, $2, $2, 'linkedin_message', $3, $4, 'linkedin_import', $5, $6) ON CONFLICT DO NOTHING`, [match.id, userId, `LinkedIn conversation (${messages.length} messages)`, summary, latestDate ? new Date(latestDate).toISOString() : new Date().toISOString(), tenantId]);
                  stats.interactions_created++;
                } catch (e) { stats.errors++; }
              } else {
                stats.unmatched_senders.add(name);
              }
            }
          }

          auditLog(userId, 'linkedin_messages_import', 'interactions', null, { total_messages: stats.total, conversations: conversations.size, matched: stats.matched, interactions_created: stats.interactions_created, filename: fm.originalname });
          return JSON.stringify({ total_messages: stats.total, conversations: conversations.size, matched_people: stats.matched, interactions_created: stats.interactions_created, unmatched_senders: [...stats.unmatched_senders].slice(0, 20), errors: stats.errors, message: `Processed ${stats.total} LinkedIn messages across ${conversations.size} conversations. Created ${stats.interactions_created} interaction records.` });
        }

        // Import XLSX workbook — stores each sheet as a document, embeds for search
        if (action === 'import_workbook' && fm.sheets) {
          const stats = { sheets_imported: 0, total_rows: 0, documents_created: 0, errors: [] };

          for (const sheetName of (fm.sheetNames || Object.keys(fm.sheets))) {
            const sheet = fm.sheets[sheetName];
            if (!sheet || !sheet.row_count) continue;

            try {
              // Build text content from all rows
              const headerLine = (sheet.headers || []).join(' | ');
              const rowLines = (sheet.rows || sheet.preview || []).map(r => Object.values(r).join(' | ')).join('\n');
              const content = `${headerLine}\n${rowLines}`;
              const title = `${fm.originalname} — ${sheetName}`;
              const hash = require('crypto').createHash('md5').update(title + content.slice(0, 500)).digest('hex');

              // Check if already exists
              const { rows: existing } = await db.query(
                'SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2', [hash, tenantId]
              );
              if (existing.length) { stats.sheets_imported++; continue; }

              // Store as external document
              const { rows: [doc] } = await db.query(`
                INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
                  tenant_id, uploaded_by_user_id, processing_status, created_at)
                VALUES ($1, $2, $3, 'xlsx_workbook', $4, $5, $6, $7, 'processed', NOW())
                RETURNING id
              `, [title, content.slice(0, 50000), fm.originalname, `xlsx://${fm.originalname}/${sheetName}`, hash, tenantId, userId]);

              // Embed in Qdrant
              try {
                const embedText = `Workbook: ${fm.originalname}\nSheet: ${sheetName}\nColumns: ${headerLine}\n\n${content.slice(0, 8000)}`;
                const emb = await generateQueryEmbedding(embedText);
                const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
                await new Promise((resolve, reject) => {
                  const body = JSON.stringify({ points: [{ id: hash, vector: emb, payload: { tenant_id: tenantId, title, source_type: 'xlsx_workbook', sheet_name: sheetName } }] });
                  const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
                    headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 15000 },
                    (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
                  qReq.on('error', reject); qReq.write(body); qReq.end();
                });
                await db.query('UPDATE external_documents SET embedded_at = NOW() WHERE id = $1', [doc.id]);
              } catch (e) { /* embed error non-fatal */ }

              stats.documents_created++;
              stats.total_rows += sheet.row_count;
              stats.sheets_imported++;
            } catch (e) {
              stats.errors.push({ sheet: sheetName, error: e.message });
            }
          }

          auditLog(userId, 'workbook_import', 'external_documents', null, { ...stats, filename: fm.originalname });
          return JSON.stringify({
            ...stats,
            filename: fm.originalname,
            sheet_names: fm.sheetNames,
            message: `Imported ${stats.sheets_imported} sheets (${stats.total_rows} total rows) from "${fm.originalname}". Each sheet stored as a searchable document and embedded for semantic search.`
          });
        }

        return JSON.stringify({ error: 'Unsupported action' });
      }
      case 'run_sql_query': {
        const sql = input.query.trim();
        const isWrite = /^(UPDATE|INSERT|DELETE)/i.test(sql);
        const isDangerous = /DROP|ALTER|TRUNCATE|CREATE/i.test(sql);
        if (isDangerous) return JSON.stringify({ error: 'DROP/ALTER/TRUNCATE/CREATE not allowed via chat. Use migrations.' });
        if (isWrite) {
          // Write operations allowed — execute and return affected rows
          const result = await db.query(sql + (sql.toUpperCase().includes('RETURNING') ? '' : ' RETURNING *'));
          return JSON.stringify({ explanation: input.explanation, operation: sql.split(' ')[0].toUpperCase(), rows_affected: result.rowCount, results: result.rows?.slice(0, 20) });
        }
        // SELECT queries
        const { rows } = await db.query(sql + (sql.includes('LIMIT') ? '' : ' LIMIT 50'));
        return JSON.stringify({ explanation: input.explanation, row_count: rows.length, results: rows });
      }
      case 'get_platform_stats': {
        const { rows: [s] } = await db.query(`SELECT (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1) AS signals, (SELECT COUNT(*) FROM companies WHERE (sector IS NOT NULL OR is_client = true) AND tenant_id = $1) AS companies, (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS people, (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1) AS documents, (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1) AS placements, (SELECT COALESCE(SUM(placement_fee),0) FROM conversions WHERE tenant_id = $1) AS revenue`, [tenantId]);
        return JSON.stringify(s);
      }
      // ── MCP-style intelligence tools ──────────────────────────────────────
      case 'get_converging_themes': {
        const lookbackDays = input.lookback_days || 30;
        const minCompanies = input.min_companies || 3;

        // Signal type clusters
        const { rows: signalThemes } = await db.query(`
          WITH candidate_counts AS (
            SELECT se2.signal_type, COUNT(DISTINCT p.id) as cnt
            FROM people p
            JOIN companies c2 ON c2.id = p.current_company_id
            JOIN signal_events se2 ON se2.company_id = c2.id AND se2.detected_at > NOW() - INTERVAL '${lookbackDays} days'
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
          WHERE se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            AND se.signal_type IS NOT NULL
            AND (se.tenant_id IS NULL OR se.tenant_id = $1)
          GROUP BY se.signal_type, cc.cnt
          HAVING COUNT(DISTINCT se.company_id) >= ${minCompanies}
          ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) DESC,
                   COUNT(DISTINCT se.company_id) DESC
          LIMIT 8
        `, [tenantId]);

        // Sector convergences
        const { rows: sectorThemes } = await db.query(`
          SELECT
            c.sector,
            COUNT(DISTINCT se.company_id) as company_count,
            COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) as client_count,
            COUNT(*) as signal_count,
            array_agg(DISTINCT se.signal_type::text) as signal_types,
            array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names
          FROM signal_events se
          JOIN companies c ON c.id = se.company_id AND c.sector IS NOT NULL
          WHERE se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            AND (se.tenant_id IS NULL OR se.tenant_id = $1)
          GROUP BY c.sector
          HAVING COUNT(DISTINCT se.company_id) >= ${minCompanies} AND COUNT(*) >= 5
          ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) DESC, COUNT(*) DESC
          LIMIT 5
        `, [tenantId]);

        // Pipeline matches
        let pipeline = [];
        try {
          const { rows } = await db.query(`
            SELECT s.title as search_title, s.status, a.name as client_name,
                   COUNT(DISTINCT se.id) as matching_signals,
                   COUNT(DISTINCT se.company_id) as signalling_companies
            FROM opportunities s
            JOIN pipeline_contacts sc ON sc.search_id = s.id
            JOIN people p ON p.id = sc.person_id
            JOIN signal_events se ON se.company_id = p.current_company_id AND se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            LEFT JOIN accounts a ON a.id = s.project_id
            WHERE s.status IN ('sourcing', 'interviewing')
              AND s.tenant_id = $1
            GROUP BY s.id, s.title, s.status, a.name
            HAVING COUNT(DISTINCT se.id) >= 2
            ORDER BY COUNT(DISTINCT se.id) DESC
            LIMIT 5
          `, [tenantId]);
          pipeline = rows;
        } catch (e) { /* pipeline query may fail if tables don't exist */ }

        return JSON.stringify({
          lookback_days: lookbackDays,
          signal_themes: signalThemes,
          sector_themes: sectorThemes,
          pipeline_matches: pipeline,
          summary: `${signalThemes.length} signal type clusters, ${sectorThemes.length} sector convergences, ${pipeline.length} active search overlaps`
        });
      }

      case 'get_ranked_opportunities': {
        const { region, min_score = 0, limit = 15, by_region = false } = input;

        if (by_region) {
          const perRegion = Math.min(limit, 10);
          const { rows } = await db.query(`
            SELECT ro.company_name, ro.sector, ro.region_code, ro.composite_score, ro.rank_in_region,
                   ro.signal_importance, ro.network_overlap, ro.geo_relevance,
                   ro.signal_summary, ro.recommended_action, ro.signal_count, ro.signal_types,
                   ro.warmest_contact_name, ro.best_connection_user_name,
                   cas.contact_count, cas.senior_contact_count,
                   gp.region_name, gp.is_home_market
            FROM ranked_opportunities ro
            LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
            LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
            WHERE ro.status = 'active' AND ro.rank_in_region <= $1
              AND ro.region_code IS NOT NULL AND ro.region_code != 'UNKNOWN'
            ORDER BY gp.weight_boost DESC NULLS LAST, ro.rank_in_region ASC
          `, [perRegion]);

          const grouped = {};
          for (const row of rows) {
            const rc = row.region_code;
            if (!grouped[rc]) grouped[rc] = { region_code: rc, region_name: row.region_name, is_home_market: row.is_home_market, opportunities: [] };
            grouped[rc].opportunities.push(row);
          }
          return JSON.stringify({ by_region: true, regions: grouped });
        }

        // Flat list
        let where = `WHERE ro.status = 'active'`;
        const params = [];
        let idx = 0;
        if (region && region !== 'all') { idx++; where += ` AND ro.region_code = $${idx}`; params.push(region); }
        if (min_score > 0) { idx++; where += ` AND ro.composite_score >= $${idx}`; params.push(min_score); }
        idx++; params.push(Math.min(limit, 50));

        const { rows } = await db.query(`
          SELECT ro.company_name, ro.sector, ro.region_code, ro.composite_score, ro.rank_in_region,
                 ro.signal_importance, ro.network_overlap, ro.geo_relevance,
                 ro.signal_summary, ro.recommended_action, ro.signal_count, ro.signal_types,
                 ro.warmest_contact_name, ro.best_connection_user_name,
                 cas.contact_count, cas.senior_contact_count,
                 gp.region_name, gp.is_home_market
          FROM ranked_opportunities ro
          LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
          LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
          ${where}
          ORDER BY ro.composite_score DESC
          LIMIT $${idx}
        `, params);

        return JSON.stringify({ by_region: false, opportunities: rows, count: rows.length });
      }

      case 'get_talent_in_motion': {
        const { focus = 'all', limit: maxResults = 10 } = input;
        const lim = Math.min(maxResults, 30);
        const result = {};

        // Flight risk
        if (focus === 'all' || focus === 'flight_risk') {
          const { rows } = await db.query(`
            SELECT DISTINCT ON (p.id)
              p.id, p.full_name, p.current_title, p.current_company_name,
              p.seniority_level, p.linkedin_url,
              se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score,
              ps.flight_risk_score, ps.timing_score,
              (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id AND p2.tenant_id = $2) as colleagues_affected,
              (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id AND p2.tenant_id = $2
               AND p2.seniority_level IN ('c_suite','vp','director')) as senior_affected
            FROM people p
            JOIN companies c ON c.id = p.current_company_id AND c.tenant_id = $2
            JOIN signal_events se ON se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $2)
              AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change', 'strategic_hiring')
              AND se.detected_at > NOW() - INTERVAL '30 days'
              AND COALESCE(se.is_megacap, false) = false
            LEFT JOIN person_scores ps ON ps.person_id = p.id
            WHERE p.current_title IS NOT NULL AND p.tenant_id = $2
            ORDER BY p.id, se.detected_at DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.flight_risk = rows;
        }

        // Active profiles
        if (focus === 'all' || focus === 'active_profiles') {
          const { rows } = await db.query(`
            SELECT p.id, p.full_name, p.current_title, p.current_company_name,
                   p.seniority_level, p.linkedin_url,
                   ps.activity_score, ps.timing_score, ps.receptivity_score, ps.flight_risk_score,
                   ps.engagement_score, ps.activity_trend, ps.engagement_trend,
                   ps.last_interaction_at, ps.interaction_count_30d, ps.external_signals_30d
            FROM people p
            JOIN person_scores ps ON ps.person_id = p.id
            WHERE (ps.timing_score > 0.4 OR ps.activity_score > 0.4 OR ps.receptivity_score > 0.5 OR ps.flight_risk_score > 0.4)
              AND p.current_title IS NOT NULL AND p.tenant_id = $2
            ORDER BY (COALESCE(ps.timing_score,0) + COALESCE(ps.activity_score,0) + COALESCE(ps.receptivity_score,0)) DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.active_profiles = rows;
        }

        // Re-engage windows
        if (focus === 'all' || focus === 'reengage') {
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
              WHERE person_id = p.id AND tenant_id = $2
              ORDER BY interaction_at DESC LIMIT 1
            ) i ON true
            LEFT JOIN person_scores ps ON ps.person_id = p.id
            WHERE p.tenant_id = $2
              AND p.current_title IS NOT NULL
              AND p.seniority_level IN ('c_suite', 'C-Suite', 'C-level', 'vp', 'VP', 'director', 'Director', 'Head')
              AND i.interaction_at IS NOT NULL
              AND i.interaction_at < NOW() - INTERVAL '60 days'
            ORDER BY p.id, se.confidence_score DESC
          `, [tenantId]);

          const ranked = rows
            .map(r => ({ ...r, reengage_score: (r.confidence_score || 0) * 0.4 + Math.min((r.days_since_contact || 0) / 365, 1) * 0.3 + (r.timing_score || 0) * 0.3 }))
            .sort((a, b) => b.reengage_score - a.reengage_score)
            .slice(0, lim);
          result.reengage_windows = ranked;
        }

        // Person signals
        if (focus === 'all' || focus === 'person_signals') {
          const { rows } = await db.query(`
            SELECT psg.id, psg.signal_type, psg.title, psg.description, psg.confidence_score, psg.detected_at,
                   p.id as person_id, p.full_name, p.current_title, p.current_company_name, p.seniority_level
            FROM person_signals psg
            JOIN people p ON p.id = psg.person_id
            WHERE psg.signal_type IN ('flight_risk_alert', 'activity_spike', 'timing_opportunity', 'new_role', 'company_exit')
              AND psg.detected_at > NOW() - INTERVAL '14 days'
              AND psg.tenant_id = $2
            ORDER BY psg.detected_at DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.person_signals = rows;
        }

        return JSON.stringify(result);
      }

      case 'get_signal_proximity': {
        let companyId = input.company_id;
        let signalContext = null;

        // Resolve from signal_id
        if (input.signal_id) {
          const { rows: [sig] } = await db.query('SELECT * FROM signal_events WHERE id = $1 AND tenant_id = $2', [input.signal_id, tenantId]);
          if (!sig) return JSON.stringify({ error: 'Signal not found' });
          companyId = sig.company_id;
          signalContext = { id: sig.id, type: sig.signal_type, confidence: sig.confidence_score, headline: sig.evidence_summary, company: sig.company_name, detected_at: sig.detected_at };
        }

        // Resolve from company_name
        if (!companyId && input.company_name) {
          const { rows } = await db.query('SELECT id, name FROM companies WHERE name ILIKE $1 AND tenant_id = $2 ORDER BY is_client DESC LIMIT 1', [`%${input.company_name}%`, tenantId]);
          if (rows.length) companyId = rows[0].id;
          else return JSON.stringify({ error: `No company found matching "${input.company_name}"` });
        }

        if (!companyId) return JSON.stringify({ error: 'Provide signal_id, company_id, or company_name' });

        // Get company info
        const { rows: [company] } = await db.query('SELECT id, name, sector, geography, is_client, domain FROM companies WHERE id = $1 AND tenant_id = $2', [companyId, tenantId]);
        if (!company) return JSON.stringify({ error: 'Company not found' });

        // Check client status
        let account = null;
        try {
          const { rows: [acct] } = await db.query(`
            SELECT a.id, a.name, a.relationship_tier FROM accounts a
            WHERE a.tenant_id = $1 AND (a.company_id = $2 OR LOWER(a.name) = LOWER($3)) LIMIT 1
          `, [tenantId, companyId, company.name]);
          account = acct || null;
        } catch (e) { /* accounts table may not exist */ }

        // Get contacts with team proximity
        const { rows: contacts } = await db.query(`
          SELECT
            p.id, p.full_name, p.current_title, p.current_company_name, p.seniority_level,
            ps.timing_score, ps.receptivity_score, ps.engagement_score,
            json_object_agg(
              tp.team_member_id::text,
              json_build_object('strength', tp.relationship_strength, 'type', tp.relationship_type)
            ) AS connections_by_team_member,
            MAX(tp.relationship_strength) AS best_strength,
            (SELECT u.name FROM users u WHERE u.id = (
              SELECT tp2.team_member_id FROM team_proximity tp2 WHERE tp2.person_id = p.id AND tp2.tenant_id = $1
              ORDER BY tp2.relationship_strength DESC LIMIT 1
            )) AS best_connector_name
          FROM people p
          JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = $1
          LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
          WHERE p.tenant_id = $1
            AND p.current_company_id = $2
            AND tp.relationship_strength >= 0.20
          GROUP BY p.id, p.full_name, p.current_title, p.current_company_name, p.seniority_level,
                   ps.timing_score, ps.receptivity_score, ps.engagement_score
          ORDER BY MAX(tp.relationship_strength) DESC
          LIMIT 15
        `, [tenantId, companyId]);

        // Get recent signals for context
        const { rows: signals } = await db.query(`
          SELECT signal_type, evidence_summary, confidence_score, detected_at
          FROM signal_events WHERE company_id = $1 AND tenant_id = $2 AND detected_at > NOW() - INTERVAL '90 days'
          ORDER BY detected_at DESC LIMIT 5
        `, [companyId, tenantId]);

        return JSON.stringify({
          company: { ...company, is_client: !!account, client_tier: account?.relationship_tier },
          signal: signalContext,
          contacts: contacts.map(c => ({
            id: c.id,
            name: c.full_name,
            title: c.current_title,
            seniority: c.seniority_level,
            best_strength: parseFloat(c.best_strength) || 0,
            best_connector: c.best_connector_name,
            connections: c.connections_by_team_member,
            timing_score: c.timing_score,
            receptivity_score: c.receptivity_score,
            engagement_score: c.engagement_score
          })),
          recent_signals: signals,
          connection_count: contacts.length,
          summary: `${contacts.length} contacts at ${company.name}${account ? ` (client, tier: ${account.relationship_tier})` : ''}, ${signals.length} recent signals`
        });
      }

      case 'dispatch_action': {
        const { action, dispatch_id, status, theme } = input;

        switch (action) {
          case 'generate_all': {
            try {
              const { generateDispatches } = require('../scripts/generate_dispatches');
              generateDispatches().then(r => console.log('Dispatch generation complete:', r)).catch(e => console.error('Dispatch generation failed:', e.message));
              return JSON.stringify({ success: true, message: 'Dispatch generation triggered — runs in background' });
            } catch (e) { return JSON.stringify({ error: 'Failed to trigger generation: ' + e.message }); }
          }
          case 'rescan_proximity': {
            try {
              const { rescanProximity } = require('../scripts/generate_dispatches');
              rescanProximity().then(r => console.log('Rescan complete:', r)).catch(e => console.error('Rescan failed:', e.message));
              return JSON.stringify({ success: true, message: 'Proximity rescan triggered — runs in background' });
            } catch (e) { return JSON.stringify({ error: 'Failed to trigger rescan: ' + e.message }); }
          }
          case 'claim': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for claim action' });
            const { rows: [d] } = await db.query('SELECT id, claimed_by, status FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [dispatch_id, tenantId]);
            if (!d) return JSON.stringify({ error: 'Dispatch not found' });
            if (d.claimed_by && d.claimed_by !== userId) {
              const { rows: [claimer] } = await db.query('SELECT name FROM users WHERE id = $1', [d.claimed_by]);
              return JSON.stringify({ error: `Already claimed by ${claimer?.name || 'another user'}` });
            }
            const { rows: [updated] } = await db.query(`
              UPDATE signal_dispatches SET claimed_by = $2, claimed_at = NOW(), status = CASE WHEN status = 'draft' THEN 'claimed' ELSE status END, updated_at = NOW()
              WHERE id = $1 AND tenant_id = $3 RETURNING id, company_name, signal_type, status
            `, [dispatch_id, userId, tenantId]);
            auditLog(userId, 'dispatch_claim', 'dispatch', dispatch_id, { company: updated.company_name });
            return JSON.stringify({ success: true, dispatch: updated, message: 'Dispatch claimed' });
          }
          case 'unclaim': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for unclaim action' });
            const { rows: [updated] } = await db.query(`
              UPDATE signal_dispatches SET claimed_by = NULL, claimed_at = NULL, status = 'draft', updated_at = NOW()
              WHERE id = $1 AND (claimed_by = $2 OR claimed_by IS NULL) AND tenant_id = $3 RETURNING id, company_name, status
            `, [dispatch_id, userId, tenantId]);
            if (!updated) return JSON.stringify({ error: 'Cannot unclaim — not your dispatch or not found' });
            return JSON.stringify({ success: true, dispatch: updated, message: 'Dispatch unclaimed' });
          }
          case 'update_status': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for update_status action' });
            if (!status) return JSON.stringify({ error: 'status required for update_status action' });
            const updates = [`status = $3`, `updated_at = NOW()`];
            if (status === 'reviewed') updates.push(`reviewed_at = NOW(), reviewed_by = $4`);
            if (status === 'sent') updates.push(`sent_at = NOW()`);
            const params = status === 'reviewed'
              ? [dispatch_id, tenantId, status, userId]
              : [dispatch_id, tenantId, status];
            const { rows: [updated] } = await db.query(`
              UPDATE signal_dispatches SET ${updates.join(', ')}
              WHERE id = $1 AND tenant_id = $2 RETURNING id, company_name, signal_type, status
            `, params);
            if (!updated) return JSON.stringify({ error: 'Dispatch not found' });
            return JSON.stringify({ success: true, dispatch: updated, message: `Status updated to ${status}` });
          }
          case 'regenerate_content': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for regenerate_content action' });
            const { rows: [d] } = await db.query(`
              SELECT sd.*, se.evidence_summary, se.signal_type, se.confidence_score,
                     c.sector, c.geography
              FROM signal_dispatches sd
              LEFT JOIN signal_events se ON se.id = sd.signal_event_id
              LEFT JOIN companies c ON c.id = sd.company_id
              WHERE sd.id = $1 AND sd.tenant_id = $2
            `, [dispatch_id, tenantId]);
            if (!d) return JSON.stringify({ error: 'Dispatch not found' });

            const blogTheme = theme || d.blog_theme || d.opportunity_angle || 'market intelligence';
            const prompt = `Write a 550-700 word executive thought leadership piece about: ${blogTheme}\n\nContext: ${d.signal_type} signal for a ${d.sector || 'technology'} company in ${d.geography || 'APAC'}.\nEvidence: ${d.evidence_summary || d.signal_summary || 'Recent market signal'}\n\nWrite with authority, not sales language. Advisor tone. First person plural. No company name. No generic business clichés. Australian English.`;

            try {
              const regen = await callClaude([{ role: 'user', content: prompt }], [], 'You are a market intelligence writer for an executive search firm. Output JSON: {"title":"...","body":"...","keywords":["..."]}');
              const text = regen.content.find(c => c.type === 'text')?.text || '';
              const parsed = JSON.parse(text.replace(/```json\n?|\n?```/g, ''));

              await db.query(`UPDATE signal_dispatches SET blog_theme = $2, blog_title = $3, blog_body = $4, blog_keywords = $5, updated_at = NOW() WHERE id = $1 AND tenant_id = $6`,
                [dispatch_id, blogTheme, parsed.title, JSON.stringify(parsed.body || parsed), parsed.keywords || [], tenantId]);

              return JSON.stringify({ success: true, title: parsed.title, keywords: parsed.keywords, message: 'Content regenerated' });
            } catch (e) { return JSON.stringify({ error: 'Regeneration failed: ' + e.message }); }
          }
          default: return JSON.stringify({ error: `Unknown dispatch action: ${action}` });
        }
      }

      case 'import_placements': {
        const { placements = [] } = input;
        if (!placements.length) return JSON.stringify({ error: 'No placements provided' });

        const results = { imported: 0, skipped: 0, errors: [], details: [] };

        for (const pl of placements) {
          try {
            // Resolve candidate
            let personId = null;
            const { rows: personMatches } = await db.query(
              `SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 3`,
              [pl.candidate_name.trim(), tenantId]
            );
            if (personMatches.length === 1) {
              personId = personMatches[0].id;
            } else if (personMatches.length > 1) {
              // Try exact match first
              const exact = personMatches.find(p => p.full_name.toLowerCase() === pl.candidate_name.trim().toLowerCase());
              personId = exact ? exact.id : personMatches[0].id;
            } else {
              // Create person
              const { rows: [newPerson] } = await db.query(
                `INSERT INTO people (full_name, current_title, source, created_by, tenant_id) VALUES ($1, $2, 'placement_import', $3, $4) RETURNING id`,
                [pl.candidate_name.trim(), pl.role_title || null, userId, tenantId]
              );
              personId = newPerson.id;
            }

            // Resolve client company → account
            let clientId = null;
            const { rows: acctMatches } = await db.query(
              `SELECT a.id FROM accounts a WHERE a.name ILIKE $1 AND a.tenant_id = $2 LIMIT 1`,
              [`%${pl.company_name.trim()}%`, tenantId]
            );
            if (acctMatches.length) {
              clientId = acctMatches[0].id;
            } else {
              // Check companies table, create account if company exists
              const { rows: coMatches } = await db.query(
                `SELECT id, name FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
                [`%${pl.company_name.trim()}%`, tenantId]
              );
              if (coMatches.length) {
                const { rows: [newAcct] } = await db.query(
                  `INSERT INTO accounts (name, company_id, relationship_status, tenant_id, created_at, updated_at)
                   VALUES ($1, $2, 'active', $3, NOW(), NOW()) RETURNING id`,
                  [coMatches[0].name, coMatches[0].id, tenantId]
                );
                clientId = newAcct.id;
              } else {
                // Create both company and account
                const { rows: [newCo] } = await db.query(
                  `INSERT INTO companies (name, is_client, created_by, tenant_id, created_at, updated_at)
                   VALUES ($1, true, $2, $3, NOW(), NOW()) RETURNING id`,
                  [pl.company_name.trim(), userId, tenantId]
                );
                const { rows: [newAcct] } = await db.query(
                  `INSERT INTO accounts (name, company_id, relationship_status, tenant_id, created_at, updated_at)
                   VALUES ($1, $2, 'active', $3, NOW(), NOW()) RETURNING id`,
                  [pl.company_name.trim(), newCo.id, tenantId]
                );
                clientId = newAcct.id;
              }
            }

            // Check for duplicate placement
            const { rows: dupes } = await db.query(
              `SELECT id FROM conversions WHERE person_id = $1 AND client_id = $2 AND role_title = $3 AND tenant_id = $4 LIMIT 1`,
              [personId, clientId, pl.role_title, tenantId]
            );
            if (dupes.length) {
              results.skipped++;
              results.details.push({ candidate: pl.candidate_name, company: pl.company_name, status: 'duplicate' });
              continue;
            }

            // Insert placement
            await db.query(
              `INSERT INTO conversions (person_id, client_id, role_title, start_date, placement_fee, currency, notes, placed_by_user_id, tenant_id, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())`,
              [personId, clientId, pl.role_title, pl.start_date || null,
               pl.placement_fee || null, pl.currency || 'AUD', pl.notes || null,
               userId, tenantId]
            );

            // Update person's current company
            const { rows: [co] } = await db.query('SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [`%${pl.company_name.trim()}%`, tenantId]);
            if (co) {
              await db.query('UPDATE people SET current_company_name = $1, current_company_id = $2, current_title = $3, updated_at = NOW() WHERE id = $4',
                [pl.company_name.trim(), co.id, pl.role_title, personId]);
            }

            results.imported++;
            results.details.push({ candidate: pl.candidate_name, company: pl.company_name, role: pl.role_title, status: 'imported' });

          } catch (e) {
            results.errors.push({ candidate: pl.candidate_name, error: e.message });
          }
        }

        auditLog(userId, 'import_placements', 'conversions', null, { imported: results.imported, skipped: results.skipped, total: placements.length });
        return JSON.stringify({ ...results, message: `Imported ${results.imported} placements (${results.skipped} duplicates skipped)` });
      }

      case 'import_case_studies': {
        const { case_studies = [] } = input;
        if (!case_studies.length) return JSON.stringify({ error: 'No case studies provided' });

        // Ensure table exists
        try {
          const fs = require('fs');
          const migPath = require('path').join(__dirname, 'sql', 'migration_case_studies.sql');
          if (fs.existsSync(migPath)) await db.query(fs.readFileSync(migPath, 'utf8'));
        } catch (e) { /* table may already exist */ }

        const results = { imported: 0, skipped: 0, details: [] };

        for (const cs of case_studies) {
          try {
            // Resolve client company
            let clientId = null;
            if (cs.client_name) {
              const { rows } = await db.query(
                `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
                [`%${cs.client_name.trim()}%`, tenantId]
              );
              if (rows.length) clientId = rows[0].id;
            }

            // Build title
            const title = [cs.role_title, cs.client_name].filter(Boolean).join(' — ') || 'Case Study';

            // Check for duplicate
            const { rows: dupes } = await db.query(
              `SELECT id FROM case_studies WHERE title ILIKE $1 AND tenant_id = $2 LIMIT 1`,
              [title, tenantId]
            );
            if (dupes.length) {
              results.skipped++;
              results.details.push({ title, status: 'duplicate' });
              continue;
            }

            // Compute completeness
            const fields = [cs.client_name, cs.engagement_type, cs.role_title, cs.sector,
                            cs.geography, cs.challenge, cs.approach, cs.outcome];
            const completeness = fields.filter(Boolean).length / fields.length;

            const { rows: [inserted] } = await db.query(`
              INSERT INTO case_studies (
                tenant_id, title, client_name, client_id, engagement_type,
                role_title, seniority_level, sector, geography, year,
                challenge, approach, outcome,
                themes, capabilities, change_vectors,
                completeness, extracted_by, status, visibility
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'chat_import','draft','internal_only')
              RETURNING id
            `, [
              tenantId, title, cs.client_name || null, clientId, cs.engagement_type || null,
              cs.role_title || null, cs.seniority_level || null, cs.sector || null, cs.geography || null, cs.year || null,
              cs.challenge || null, cs.approach || null, cs.outcome || null,
              cs.themes || [], cs.capabilities || [], cs.change_vectors || [],
              completeness
            ]);

            results.imported++;
            results.details.push({ title, id: inserted.id, status: 'imported', completeness: (completeness * 100).toFixed(0) + '%' });
          } catch (e) {
            results.details.push({ title: cs.role_title || 'unknown', status: 'error', error: e.message });
          }
        }

        auditLog(userId, 'import_case_studies', 'case_studies', null, { imported: results.imported, skipped: results.skipped, total: case_studies.length });
        return JSON.stringify({
          ...results,
          message: `Imported ${results.imported} case studies as internal drafts (${results.skipped} duplicates skipped). These require sanitisation before external use — use /api/case-studies/:id/sanitise to approve public fields.`
        });
      }

      case 'search_case_studies': {
        const { query, limit = 10 } = input;
        let results = [];

        // Try Qdrant semantic search first
        try {
          const vector = await generateQueryEmbedding(query);
          const csTenantFilter = tenantId ? { should: [{ key: 'tenant_id', match: { value: tenantId } }, { is_empty: { key: 'tenant_id' } }] } : null;
          const qdrantResults = await qdrantSearch('case_studies', vector, limit, csTenantFilter);
          if (qdrantResults.length > 0) {
            const csIds = qdrantResults.map(r => String(r.id)).filter(id => /^[0-9a-f-]{36}$/i.test(id));
            if (csIds.length > 0) {
              const { rows } = await db.query(
                `SELECT id, title, client_name, role_title, sector, geography, year, challenge, themes, capabilities
                 FROM case_studies WHERE id = ANY($1::uuid[]) AND tenant_id = $2`,
                [csIds, tenantId]
              );
              const csMap = new Map(rows.map(r => [r.id, r]));
              results = qdrantResults.map(r => {
                const cs = csMap.get(r.id);
                if (!cs) return null;
                return { ...cs, match_score: Math.round(r.score * 100) };
              }).filter(Boolean);
            }
          }
        } catch (e) { /* Qdrant collection may not exist */ }

        // Fallback to SQL text search
        if (results.length < 3) {
          const { rows } = await db.query(
            `SELECT id, title, client_name, role_title, sector, geography, year, challenge, themes, capabilities
             FROM case_studies
             WHERE tenant_id = $1 AND (
               title ILIKE $2 OR client_name ILIKE $2 OR role_title ILIKE $2 OR
               challenge ILIKE $2 OR sector ILIKE $2 OR geography ILIKE $2
             )
             ORDER BY year DESC NULLS LAST LIMIT $3`,
            [tenantId, `%${query}%`, limit]
          );
          const existing = new Set(results.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) results.push(r); });
        }

        return JSON.stringify({ case_studies: results.slice(0, limit), count: results.length });
      }

      case 'run_pipeline': {
        const { pipeline_key } = input;
        if (!pipeline_key) return JSON.stringify({ error: 'pipeline_key required' });

        try {
          const scheduler = require('../scripts/scheduler.js');
          const pipelines = scheduler.PIPELINES;
          if (!pipelines[pipeline_key]) {
            const available = Object.keys(pipelines).join(', ');
            return JSON.stringify({ error: `Unknown pipeline "${pipeline_key}". Available: ${available}` });
          }
          const pipeline = pipelines[pipeline_key];
          // Trigger async — don't wait for completion
          scheduler.runPipeline(pipeline_key, 'chat').catch(e => console.error(`Pipeline ${pipeline_key} error:`, e.message));
          auditLog(userId, 'run_pipeline', 'pipeline', null, { pipeline_key });
          return JSON.stringify({ success: true, message: `${pipeline.name} triggered — running in background. Check /api/pipelines/runs for status.` });
        } catch (e) {
          return JSON.stringify({ error: 'Failed to trigger pipeline: ' + e.message });
        }
      }

      default: return JSON.stringify({ error: `Unknown tool: ${name}` });
    }
  } catch (err) {
    console.error(`Tool ${name} error:`, err.message);
    return JSON.stringify({ error: err.message });
  }
}

const uploadedFiles = new Map();

// File upload
router.post('/api/chat/upload', authenticateToken, chatUpload.single('file'), async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file' });
    const fileId = crypto.randomUUID();
    const meta = { path: file.path, mimetype: file.mimetype, originalname: file.originalname, size: file.size };

    if (file.originalname.endsWith('.csv') || file.mimetype === 'text/csv') {
      const raw = fsChat.readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines = raw.split('\n');
      // Find header line — skip LinkedIn preamble/notes at top
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
        if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
          headerIdx = i; break;
        }
      }
      if (headerIdx === 0) {
        for (let i = 0; i < Math.min(allLines.length, 20); i++) {
          const trimmed = allLines[i].trim();
          if (trimmed && trimmed.split(',').length >= 3 && trimmed.split(',')[0].trim().length < 30) { headerIdx = i; break; }
        }
      }
      const lines = allLines.slice(headerIdx).filter(l => l.trim());
      if (lines.length) {
        function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
        const headers = parseCSV(lines[0]);
        meta.columns = headers;
        meta.preview = [];
        for (let i=1; i<Math.min(lines.length,1001); i++) {
          const vals = parseCSV(lines[i]);
          const row = {}; headers.forEach((h,idx) => { row[h] = vals[idx]||''; }); meta.preview.push(row);
        }

        // Detect LinkedIn CSV type
        const lh = headers.map(h => h.toLowerCase().trim());
        const hasFirstName = lh.includes('first name');
        const hasLastName = lh.includes('last name');
        const hasURL = lh.includes('url');
        const hasConnectedOn = lh.includes('connected on');
        const hasPosition = lh.includes('position');
        const hasFrom = lh.some(h => h === 'from');
        const hasTo = lh.some(h => h === 'to');
        const hasContent = lh.some(h => h === 'content' || h === 'body');
        const hasConversationId = lh.some(h => h.includes('conversation'));
        const hasPhoneNumbers = lh.some(h => h.includes('phone'));

        if (hasFirstName && hasLastName && hasConnectedOn && hasURL) {
          meta.linkedinType = 'connections';
          meta.suggestedMapping = { full_name: 'First Name+Last Name', linkedin_url: 'URL', email: 'Email Address', company: 'Company', position: 'Position', connected_on: 'Connected On' };
        } else if ((hasFrom || hasTo) && (hasContent || hasConversationId)) {
          meta.linkedinType = 'messages';
          meta.suggestedMapping = { from: headers[lh.findIndex(h => h === 'from')], to: headers[lh.findIndex(h => h === 'to')], content: headers[lh.findIndex(h => h === 'content' || h === 'body')], date: headers[lh.findIndex(h => h.includes('date'))] };
        } else if (hasFirstName && hasLastName && hasPhoneNumbers) {
          meta.linkedinType = 'contacts';
          meta.suggestedMapping = { full_name: 'First Name+Last Name', email: headers[lh.findIndex(h => h.includes('email'))], phone: headers[lh.findIndex(h => h.includes('phone'))], company: headers[lh.findIndex(h => h.includes('company') || h.includes('org'))] };
        } else {
          // Generic CSV mapping
          meta.suggestedMapping = {};
          if (lh.some(h=>h.includes('name'))) meta.suggestedMapping.full_name = headers[lh.findIndex(h=>h.includes('name'))];
          if (lh.some(h=>h.includes('title')||h.includes('role'))) meta.suggestedMapping.current_title = headers[lh.findIndex(h=>h.includes('title')||h.includes('role'))];
          if (lh.some(h=>h.includes('company')||h.includes('org'))) meta.suggestedMapping.current_company_name = headers[lh.findIndex(h=>h.includes('company')||h.includes('org'))];
          if (lh.some(h=>h.includes('email'))) meta.suggestedMapping.email = headers[lh.findIndex(h=>h.includes('email'))];
          if (lh.some(h=>h.includes('location')||h.includes('city'))) meta.suggestedMapping.location = headers[lh.findIndex(h=>h.includes('location')||h.includes('city'))];
        }
      }
    }
    // XLSX / XLS workbook support — parse all tabs
    if (file.originalname.match(/\.xlsx?$/i) || file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' || file.mimetype === 'application/vnd.ms-excel') {
      try {
        const XLSX = require('xlsx');
        const workbook = XLSX.readFile(file.path);
        meta.sheets = {};
        meta.sheetNames = workbook.SheetNames;
        meta.preview = []; // Combined preview for Claude
        meta.columns = [];

        for (const sheetName of workbook.SheetNames) {
          const sheet = workbook.Sheets[sheetName];
          const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
          const headers = rows.length > 0 ? Object.keys(rows[0]) : [];

          meta.sheets[sheetName] = {
            headers,
            row_count: rows.length,
            preview: rows.slice(0, 5)
          };

          // Store all rows (up to 2000 per sheet) for processing
          if (rows.length > 0) {
            meta.sheets[sheetName].rows = rows.slice(0, 2000);
          }
        }

        // Set top-level columns/preview from first sheet for compatibility
        const firstSheet = workbook.SheetNames[0];
        if (meta.sheets[firstSheet]) {
          meta.columns = meta.sheets[firstSheet].headers;
          meta.preview = meta.sheets[firstSheet].rows || meta.sheets[firstSheet].preview;
        }

        // Build full text for embedding
        meta.text = workbook.SheetNames.map(name => {
          const s = meta.sheets[name];
          const headerLine = s.headers.join(' | ');
          const sampleRows = (s.preview || []).slice(0, 10).map(r => Object.values(r).join(' | ')).join('\n');
          return `=== Sheet: ${name} (${s.row_count} rows) ===\n${headerLine}\n${sampleRows}`;
        }).join('\n\n');
      } catch (e) {
        meta.text = '[XLSX parse error: ' + e.message + ']';
      }
    }

    if (file.originalname.endsWith('.pdf') || file.mimetype === 'application/pdf') {
      try {
        const pdfParse = require('pdf-parse');
        const buf = fsChat.readFileSync(file.path);
        const d = await pdfParse(buf);
        meta.text = d.text;
        meta.pages = d.numpages;
      } catch (e) {
        meta.text = '[PDF parse error: ' + e.message + ']';
      }
    }
    if (file.originalname.endsWith('.txt') || file.mimetype === 'text/plain') { meta.text = fsChat.readFileSync(file.path, 'utf8'); }

    uploadedFiles.set(fileId, meta);
    setTimeout(() => { uploadedFiles.delete(fileId); try { fsChat.unlinkSync(file.path); } catch(e){} }, 30*60*1000);

    const response = { file_id: fileId, filename: file.originalname, size: file.size, type: file.mimetype, columns: meta.columns||null, row_count: meta.preview?.length||null, pages: meta.pages||null, suggested_mapping: meta.suggestedMapping||null, linkedin_type: meta.linkedinType||null };
    // Include sheet info for XLSX workbooks
    if (meta.sheetNames) {
      response.workbook = true;
      response.sheet_names = meta.sheetNames;
      response.sheets = {};
      for (const name of meta.sheetNames) {
        response.sheets[name] = { headers: meta.sheets[name].headers, row_count: meta.sheets[name].row_count, preview: (meta.sheets[name].preview || []).slice(0, 3) };
      }
    }
    res.json(response);
  } catch (err) { console.error('Upload error:', err); res.status(500).json({ error: 'Upload failed' }); }
});

// Chat endpoint
router.post('/api/chat', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { message, file_id } = req.body;
    if (!message?.trim()) return res.status(400).json({ error: 'Message required' });
    if (!process.env.ANTHROPIC_API_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not set' });

    const history = getChatHistory(req.user.id);
    let userContent = message;
    if (file_id) {
      const fm = uploadedFiles.get(file_id);
      if (fm) {
        userContent += `\n\n[File: ${fm.originalname} (${fm.mimetype}, ${fm.size}b)]`;
        if (fm.linkedinType) userContent += `\n[LinkedIn Export Type: ${fm.linkedinType}]`;
        if (fm.columns) userContent += `\n[Columns: ${fm.columns.join(', ')}]`;
        if (fm.preview) userContent += `\n[${fm.preview.length} rows]`;
        if (fm.suggestedMapping) userContent += `\n[Mapping: ${JSON.stringify(fm.suggestedMapping)}]`;
        if (fm.pages) userContent += `\n[PDF: ${fm.pages} pages]`;
        if (fm.text) userContent += `\n[Text: ${fm.text.slice(0,1500)}]`;
        userContent += `\n[file_id: ${file_id}]`;
      }
    }

    history.push({ role: 'user', content: userContent });
    while (history.length > MAX_HISTORY) history.shift();

    // Inject tenant context into system prompt so SQL queries can use the right tenant_id
    const systemWithContext = CHAT_SYSTEM + `\n\nSESSION CONTEXT:\n- tenant_id: '${req.tenant_id}'\n- user: ${req.user.name} (${req.user.email})\n- user_id: '${req.user.user_id}'`;

    let response = await callClaude(history, CHAT_TOOLS, systemWithContext);
    let finalText = '';
    let toolsUsed = [];
    let rounds = 0;

    while (response.stop_reason === 'tool_use' && rounds < 5) {
      rounds++;
      const toolCalls = response.content.filter(c => c.type === 'tool_use');
      const textParts = response.content.filter(c => c.type === 'text').map(c => c.text);
      if (textParts.length) finalText += textParts.join('');

      history.push({ role: 'assistant', content: response.content });
      const toolResultContent = [];
      for (const tc of toolCalls) {
        console.log(`  🔧 ${tc.name}`, JSON.stringify(tc.input).slice(0, 150));
        const result = await executeTool(tc.name, tc.input, req.user.id, req.tenant_id);
        toolResultContent.push({ type: 'tool_result', tool_use_id: tc.id, content: result });
        toolsUsed.push(tc.name);
      }
      history.push({ role: 'user', content: toolResultContent });
      response = await callClaude(history, CHAT_TOOLS, systemWithContext);
    }

    finalText += response.content.filter(c => c.type === 'text').map(c => c.text).join('');
    history.push({ role: 'assistant', content: finalText });
    while (history.length > MAX_HISTORY) history.shift();

    res.json({ response: finalText, tools_used: [...new Set(toolsUsed)] });
  } catch (err) {
    console.error('Chat error:', err.message);
    res.status(500).json({ error: 'Chat failed: ' + err.message });
  }
});

router.delete('/api/chat/history', authenticateToken, (req, res) => {
  chatHistories.delete(req.user.id);
  res.json({ success: true });
});

// ═══════════════════════════════════════════════════════════════════════════════
// CASE STUDY LIBRARY
// ═══════════════════════════════════════════════════════════════════════════════
//
// GOVERNANCE MODEL:
// - Placements (conversions table) are ALWAYS internal. They contain fees,
//   candidate PII, contact details. They NEVER appear in external output.
// - Case studies have two representations:
//   1. INTERNAL: full data (client name, role, people, source doc) — team only
//   2. EXTERNAL-SAFE: public_* fields only, no candidate names, no fees,
//      no contact info. Requires public_approved = true.
// - Only case studies with public_approved = true AND visibility = 'dispatch_ready'
//   or 'published' can be bundled with dispatches or served externally.
//
// ═══════════════════════════════════════════════════════════════════════════════

// Internal: full case study list (authenticated, team only)
router.get('/api/case-studies', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { sector, geography, theme, status, limit: lim = 50, offset = 0 } = req.query;
    let where = 'WHERE cs.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (sector) { idx++; where += ` AND cs.sector ILIKE $${idx}`; params.push(`%${sector}%`); }
    if (geography) { idx++; where += ` AND cs.geography ILIKE $${idx}`; params.push(`%${geography}%`); }
    if (theme) { idx++; where += ` AND $${idx} = ANY(cs.themes)`; params.push(theme); }
    if (status) { idx++; where += ` AND cs.status = $${idx}`; params.push(status); }

    idx++; params.push(Math.min(parseInt(lim) || 50, 100));
    idx++; params.push(parseInt(offset) || 0);

    const { rows } = await db.query(`
      SELECT cs.*, c.name AS client_company_name, c.is_client,
             ed.title AS source_document_title, ed.source_url
      FROM case_studies cs
      LEFT JOIN companies c ON c.id = cs.client_id
      LEFT JOIN external_documents ed ON ed.id = cs.document_id
      ${where}
      ORDER BY cs.year DESC NULLS LAST, cs.created_at DESC
      LIMIT $${idx - 1} OFFSET $${idx}
    `, params);

    const { rows: [{ count }] } = await db.query(
      `SELECT COUNT(*) FROM case_studies cs ${where}`, params.slice(0, -2)
    );

    res.json({ case_studies: rows, total: parseInt(count) });
  } catch (err) {
    console.error('Case studies error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Internal: full case study detail (authenticated, team only)
router.get('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [cs] } = await db.query(`
      SELECT cs.*, c.name AS client_company_name, c.sector AS client_sector,
             ed.title AS source_document_title, ed.source_url, ed.content_summary
      FROM case_studies cs
      LEFT JOIN companies c ON c.id = cs.client_id
      LEFT JOIN external_documents ed ON ed.id = cs.document_id
      WHERE cs.id = $1 AND cs.tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    if (!cs) return res.status(404).json({ error: 'Not found' });

    // People from the source document — INTERNAL ONLY
    let people = [];
    if (cs.document_id) {
      const { rows } = await db.query(`
        SELECT dp.person_name, dp.person_title, dp.person_company, dp.mention_role, dp.context_note,
               dp.person_id, p.current_title AS current_title_now, p.current_company_name AS current_company_now
        FROM document_people dp
        LEFT JOIN people p ON p.id = dp.person_id
        WHERE dp.document_id = $1
        ORDER BY dp.mention_role, dp.person_name
      `, [cs.document_id]);
      people = rows;
    }

    res.json({ case_study: cs, people });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sanitise a case study for external use (admin only)
router.patch('/api/case-studies/:id/sanitise', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { public_title, public_summary, public_sector, public_geography, public_capability, public_approved } = req.body;
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;

    if (public_title !== undefined) { idx++; updates.push(`public_title = $${idx}`); params.push(public_title); }
    if (public_summary !== undefined) { idx++; updates.push(`public_summary = $${idx}`); params.push(public_summary); }
    if (public_sector !== undefined) { idx++; updates.push(`public_sector = $${idx}`); params.push(public_sector); }
    if (public_geography !== undefined) { idx++; updates.push(`public_geography = $${idx}`); params.push(public_geography); }
    if (public_capability !== undefined) { idx++; updates.push(`public_capability = $${idx}`); params.push(public_capability); }
    if (public_approved !== undefined) {
      idx++; updates.push(`public_approved = $${idx}`); params.push(public_approved);
      if (public_approved) {
        updates.push(`sanitised_by = $${idx + 1}`, `sanitised_at = NOW()`);
        idx++; params.push(req.user.user_id);
        updates.push(`visibility = CASE WHEN visibility = 'internal_only' THEN 'dispatch_ready' ELSE visibility END`);
        updates.push(`status = CASE WHEN status = 'draft' THEN 'sanitised' ELSE status END`);
      }
    }

    const { rows: [updated] } = await db.query(
      `UPDATE case_studies SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2 RETURNING id, public_title, public_approved, visibility, status`,
      params
    );
    if (!updated) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'sanitise_case_study', 'case_study', updated.id, { public_approved: updated.public_approved, visibility: updated.visibility });
    res.json({ case_study: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Edit case study (any field)
router.patch('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const allowed = ['title', 'client_name', 'role_title', 'engagement_type', 'seniority_level',
      'sector', 'geography', 'year', 'challenge', 'approach', 'outcome', 'impact_note',
      'themes', 'change_vectors', 'capabilities', 'status', 'visibility'];
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;

    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        idx++;
        if (['themes', 'change_vectors', 'capabilities'].includes(key)) {
          updates.push(`${key} = $${idx}::text[]`);
          params.push(Array.isArray(req.body[key]) ? req.body[key] : [req.body[key]]);
        } else {
          updates.push(`${key} = $${idx}`);
          params.push(req.body[key]);
        }
      }
    }

    if (updates.length <= 1) return res.status(400).json({ error: 'No valid fields to update' });

    const { rows: [updated] } = await db.query(
      `UPDATE case_studies SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2 RETURNING *`,
      params
    );
    if (!updated) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'edit_case_study', 'case_study', updated.id, { fields: Object.keys(req.body) });
    res.json({ case_study: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete case study
router.delete('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [deleted] } = await db.query(
      'DELETE FROM case_studies WHERE id = $1 AND tenant_id = $2 RETURNING id, title',
      [req.params.id, req.tenant_id]
    );
    if (!deleted) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'delete_case_study', 'case_study', deleted.id, { title: deleted.title });
    res.json({ success: true, deleted: deleted.title });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUBLIC / DISPATCH-SAFE: case studies for external consumption
// HARD GATE: only returns public_approved = true, visibility IN ('dispatch_ready', 'published')
// NEVER returns: client_name, candidate names, fees, contact details, source documents
router.get('/api/public/case-studies', async (req, res) => {
  try {
    const { sector, geography, theme, capability, limit: lim = 20 } = req.query;
    const tenantId = '00000000-0000-0000-0000-000000000001';
    let where = `WHERE cs.tenant_id = $1 AND cs.public_approved = true AND cs.visibility IN ('dispatch_ready', 'published')`;
    const params = [tenantId];
    let idx = 1;

    if (sector) { idx++; where += ` AND cs.public_sector ILIKE $${idx}`; params.push(`%${sector}%`); }
    if (geography) { idx++; where += ` AND cs.public_geography ILIKE $${idx}`; params.push(`%${geography}%`); }
    if (theme) { idx++; where += ` AND $${idx} = ANY(cs.themes)`; params.push(theme); }
    if (capability) { idx++; where += ` AND $${idx} = ANY(cs.capabilities)`; params.push(capability); }
    idx++; params.push(Math.min(parseInt(lim) || 20, 50));

    const { rows } = await platformPool.query(`
      SELECT
        cs.id, cs.slug,
        cs.public_title AS title,
        cs.public_summary AS summary,
        cs.public_sector AS sector,
        cs.public_geography AS geography,
        cs.public_capability AS capability,
        cs.engagement_type,
        cs.seniority_level,
        cs.year,
        cs.themes,
        cs.change_vectors,
        cs.capabilities
      FROM case_studies cs
      ${where}
      ORDER BY cs.year DESC NULLS LAST, cs.relevance_score DESC
      LIMIT $${idx}
    `, params);

    res.json({ case_studies: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Internal match: flag relevant case studies for team users on signals/dispatches
// Returns INTERNAL fields — for team use, not external publishing
router.get('/api/case-studies/relevant', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { signal_type, sector, geography, company_name, company_id, limit: lim = 5 } = req.query;
    const tenantId = req.tenant_id;

    let where = 'WHERE cs.tenant_id = $1';
    const params = [tenantId];
    let idx = 1;

    // Build scoring from available dimensions
    const scoreTerms = [];

    if (sector) {
      idx++; params.push(`%${sector}%`);
      scoreTerms.push(`CASE WHEN cs.sector ILIKE $${idx} OR cs.public_sector ILIKE $${idx} THEN 0.3 ELSE 0 END`);
    }
    if (geography) {
      idx++; params.push(`%${geography}%`);
      scoreTerms.push(`CASE WHEN cs.geography ILIKE $${idx} OR cs.public_geography ILIKE $${idx} THEN 0.25 ELSE 0 END`);
    }
    if (signal_type) {
      // Map signal types to likely engagement types and themes
      const sigMap = {
        capital_raising: { themes: ['high-growth', 'scaling', 'fundraising'], eng: 'executive_search' },
        geographic_expansion: { themes: ['cross-border', 'market-entry', 'expansion'], eng: 'executive_search' },
        strategic_hiring: { themes: ['leadership', 'team-build', 'scaling'], eng: 'executive_search' },
        ma_activity: { themes: ['post-acquisition', 'integration', 'merger'], eng: 'executive_search' },
        leadership_change: { themes: ['succession', 'leadership-transition', 'turnaround'], eng: 'succession' },
        restructuring: { themes: ['turnaround', 'restructuring', 'transformation'], eng: 'executive_search' },
        layoffs: { themes: ['restructuring', 'talent-market'], eng: 'executive_search' },
        product_launch: { themes: ['product', 'innovation', 'go-to-market'], eng: 'executive_search' },
        partnership: { themes: ['partnership', 'alliance', 'ecosystem'], eng: 'executive_search' },
      };
      const mapping = sigMap[signal_type] || { themes: [], eng: null };
      if (mapping.themes.length) {
        idx++; params.push(mapping.themes);
        scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${idx}::text[]))::float * 0.2`);
      }
      if (mapping.eng) {
        idx++; params.push(mapping.eng);
        scoreTerms.push(`CASE WHEN cs.engagement_type = $${idx} THEN 0.1 ELSE 0 END`);
      }
    }
    if (company_id) {
      idx++; params.push(company_id);
      scoreTerms.push(`CASE WHEN cs.client_id = $${idx}::uuid THEN 0.5 ELSE 0 END`);
    } else if (company_name) {
      idx++; params.push(`%${company_name}%`);
      scoreTerms.push(`CASE WHEN cs.client_name ILIKE $${idx} THEN 0.4 ELSE 0 END`);
    }

    const scoreExpr = scoreTerms.length > 0 ? scoreTerms.join(' + ') : '0';
    idx++; params.push(Math.min(parseInt(lim) || 5, 20));

    const { rows } = await db.query(`
      SELECT
        cs.id, cs.title, cs.client_name, cs.role_title, cs.engagement_type,
        cs.sector, cs.geography, cs.seniority_level, cs.year,
        cs.themes, cs.capabilities, cs.change_vectors,
        cs.challenge, cs.outcome,
        cs.public_approved, cs.visibility, cs.status,
        cs.public_title, cs.public_summary,
        (${scoreExpr}) AS relevance_score
      FROM case_studies cs
      ${where}
      AND (${scoreExpr}) > 0
      ORDER BY (${scoreExpr}) DESC, cs.year DESC NULLS LAST
      LIMIT $${idx}
    `, params);

    res.json({ relevant_case_studies: rows });
  } catch (err) {
    console.error('Case studies relevant error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Match case studies to a dispatch by theme/sector/geography overlap
// Used by the dispatch rendering pipeline — returns ONLY public-safe fields
router.get('/api/case-studies/match', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { themes, sectors, geographies, change_vectors, limit: lim = 3 } = req.query;
    const tenantId = req.tenant_id;

    // Only return approved case studies
    let where = `WHERE cs.tenant_id = $1 AND cs.public_approved = true`;
    const params = [tenantId];
    let idx = 1;

    // Build relevance scoring
    const scoreTerms = [];
    if (themes) {
      const themeArr = themes.split(',').map(t => t.trim());
      idx++; params.push(themeArr);
      scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${idx}::text[]))::float * 0.4`);
    }
    if (sectors) {
      idx++; params.push(`%${sectors}%`);
      scoreTerms.push(`CASE WHEN cs.public_sector ILIKE $${idx} THEN 0.25 ELSE 0 END`);
    }
    if (geographies) {
      idx++; params.push(`%${geographies}%`);
      scoreTerms.push(`CASE WHEN cs.public_geography ILIKE $${idx} THEN 0.2 ELSE 0 END`);
    }
    if (change_vectors) {
      const cvArr = change_vectors.split(',').map(v => v.trim());
      idx++; params.push(cvArr);
      scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.change_vectors) v WHERE v = ANY($${idx}::text[]))::float * 0.15`);
    }

    const scoreExpr = scoreTerms.length > 0 ? scoreTerms.join(' + ') : '0';
    idx++; params.push(Math.min(parseInt(lim) || 3, 10));

    const { rows } = await db.query(`
      SELECT
        cs.id, cs.slug,
        cs.public_title AS title,
        cs.public_summary AS summary,
        cs.public_sector AS sector,
        cs.public_geography AS geography,
        cs.public_capability AS capability,
        cs.engagement_type,
        cs.themes,
        cs.capabilities,
        (${scoreExpr}) AS match_score
      FROM case_studies cs
      ${where}
      ORDER BY (${scoreExpr}) DESC, cs.relevance_score DESC
      LIMIT $${idx}
    `, params);

    res.json({ matched_case_studies: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Classified documents overview
router.get('/api/documents/classified', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { document_type, limit: lim = 30 } = req.query;
    let where = 'WHERE (ed.tenant_id IS NULL OR ed.tenant_id = $1) AND ed.classified_at IS NOT NULL';
    const params = [req.tenant_id];
    let idx = 1;
    if (document_type) { idx++; where += ` AND ed.document_type = $${idx}`; params.push(document_type); }
    idx++; params.push(Math.min(parseInt(lim) || 30, 100));

    const { rows } = await db.query(`
      SELECT ed.id, ed.title, ed.document_type, ed.content_summary, ed.relevance_tags,
             ed.source_url, ed.classified_at, ed.uploaded_by_user_id,
             u.name AS uploaded_by_name,
             (SELECT COUNT(*) FROM document_people dp WHERE dp.document_id = ed.id) AS people_count
      FROM external_documents ed
      LEFT JOIN users u ON u.id = ed.uploaded_by_user_id
      ${where}
      ORDER BY ed.classified_at DESC
      LIMIT $${idx}
    `, params);

    // Type summary
    const { rows: typeSummary } = await db.query(`
      SELECT document_type, COUNT(*) AS count
      FROM external_documents
      WHERE tenant_id = $1 AND classified_at IS NOT NULL AND document_type IS NOT NULL
      GROUP BY document_type ORDER BY COUNT(*) DESC
    `, [req.tenant_id]);

    res.json({ documents: rows, type_summary: typeSummary });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// USER PROFILE: Self-serve data operations
// ═══════════════════════════════════════════════════════════════════════════════

// Profile stats
router.get('/api/profile/stats', authenticateToken, async (req, res) => {
  try {
    const uid = req.user.user_id;
    const tid = req.tenant_id;
    // Use platformPool to bypass RLS — team_proximity/interactions may have mismatched tenant_id
    // These tables are user-scoped so user_id filter is sufficient
    const { rows: [s] } = await platformPool.query(`
      SELECT
        (SELECT COUNT(*) FROM team_proximity WHERE team_member_id = $1) AS connections,
        (SELECT COUNT(*) FROM interactions WHERE user_id = $1 OR created_by = $1) AS interactions,
        (SELECT COUNT(*) FROM signal_dispatches WHERE claimed_by = $1 AND tenant_id = $2) AS dispatches,
        (SELECT COUNT(*) FROM rss_sources WHERE enabled = true) AS feeds
    `, [uid, tid]);

    // Import history from audit log
    const { rows: imports } = await platformPool.query(`
      SELECT action, details->>'filename' AS filename, details->>'total' AS total, created_at
      FROM audit_logs WHERE user_id = $1 AND action IN ('csv_import','linkedin_connections_import','linkedin_messages_import','workbook_import','admin_linkedin_import','document_upload')
      ORDER BY created_at DESC LIMIT 20
    `, [uid]).catch(() => ({ rows: [] }));

    res.json({
      connections: s?.connections || 0,
      interactions: s?.interactions || 0,
      dispatches: s?.dispatches || 0,
      imports: imports.length,
      feeds: s?.feeds || 0,
      import_history: imports
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User feeds — list
router.get('/api/profile/feeds', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const uid = req.user.user_id;
    const tid = req.tenant_id;

    // Platform feeds (rss_sources)
    const { rows: platformFeeds } = await platformPool.query(`
      SELECT rs.id, rs.name, rs.url, rs.source_type, rs.enabled,
        rs.last_fetched_at,
        (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_name = rs.name) AS doc_count
      FROM rss_sources rs
      ORDER BY rs.enabled DESC, rs.name
    `).catch(() => ({ rows: [] }));

    res.json({
      platform_feeds: platformFeeds,
      user_feeds: [], // user feeds now go directly into rss_sources
      feeds: [], // backward compat
    });
  } catch (err) { res.json({ platform_feeds: [], user_feeds: [], feeds: [] }); }
});

// Toggle feed on/off
router.post('/api/profile/feeds/:id/toggle', authenticateToken, async (req, res) => {
  try {
    const { disabled } = req.body;
    // Update the feed's enabled state directly
    await platformPool.query(
      `UPDATE rss_sources SET enabled = $1 WHERE id = $2`,
      [!disabled, req.params.id]
    );
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User feeds — add (writes directly to rss_sources so harvester picks it up)
router.post('/api/profile/feeds', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { url, name } = req.body;
    if (!url || !url.startsWith('http')) return res.status(400).json({ error: 'Valid URL required' });

    // Try to detect if it's an RSS feed
    let isRss = false;
    try {
      const probe = await fetch(url, { headers: { 'User-Agent': 'MLX-Intelligence/1.0' }, signal: AbortSignal.timeout(5000) });
      const text = await probe.text();
      isRss = text.includes('<rss') || text.includes('<feed') || text.includes('<channel');
    } catch (e) { /* probe failed, not critical */ }

    // Check if feed already exists
    const { rows: existing } = await db.query(
      `SELECT id, name, url FROM rss_sources WHERE url = $1 LIMIT 1`, [url.trim()]
    );

    let feed;
    if (existing.length) {
      // Re-enable if disabled
      await db.query(`UPDATE rss_sources SET enabled = true, name = COALESCE($2, name) WHERE id = $1`, [existing[0].id, name || null]);
      feed = { ...existing[0], active: true };
    } else {
      const { rows: [newFeed] } = await db.query(`
        INSERT INTO rss_sources (name, url, source_type, enabled, tenant_id)
        VALUES ($1, $2, $3, true, $4)
        RETURNING id, name, url, source_type, enabled AS active
      `, [name || url, url.trim(), isRss ? 'rss' : 'page', req.tenant_id]);
      feed = newFeed;
    }

    auditLog(req.user.user_id, 'add_feed', 'rss_sources', feed?.id, { url, name, is_rss: isRss });
    res.json({ ...feed, is_rss: isRss });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User feeds — remove
router.delete('/api/profile/feeds/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Disable rather than delete (preserves history)
    await db.query(`UPDATE rss_sources SET enabled = false WHERE id = $1`, [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User import — handles LinkedIn CSV, contacts, documents
const profileUpload = require('multer')({ dest: '/tmp/ml-profile-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });
// Import preview — dry run analysis without writing
router.post('/api/profile/import/preview', authenticateToken, profileUpload.single('file'), async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const file = req.file;
    const importType = req.body.import_type;
    const tenantId = req.tenant_id;
    if (!file) return res.status(400).json({ error: 'No file' });

    const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');

    // ── WhatsApp chat export preview (.txt) ──
    if (importType === 'whatsapp_chat') {
      const msgRegex = /^\[?(\d{1,2}\/\d{1,2}\/\d{2,4}),?\s*(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[ap]m)?)\]?\s*[-–]\s*([^:]+):\s*(.+)/i;
      const lines = raw.split('\n');
      const senders = new Map();
      let totalMsgs = 0;
      for (const line of lines) {
        const m = line.match(msgRegex);
        if (!m) continue;
        const sender = m[3].trim();
        const content = m[4].trim();
        if (content === '<Media omitted>' || content === 'This message was deleted') continue;
        totalMsgs++;
        senders.set(sender, (senders.get(sender) || 0) + 1);
      }
      const { rows: dbPeople } = await db.query(`SELECT id, full_name FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL`, [tenantId]);
      const nameMap = new Map();
      for (const p of dbPeople) nameMap.set(p.full_name.toLowerCase().trim(), p);
      let matched = 0;
      for (const name of senders.keys()) { if (nameMap.has(name.toLowerCase().trim())) matched++; }

      const fileId = require('crypto').randomUUID();
      require('fs').copyFileSync(file.path, `/tmp/ml-preview-${fileId}`);
      try { require('fs').unlinkSync(file.path); } catch(e) {}
      setTimeout(() => { try { require('fs').unlinkSync(`/tmp/ml-preview-${fileId}`); } catch(e) {} }, 30 * 60 * 1000);

      return res.json({ total: totalMsgs, valid: totalMsgs, new_records: 0, matched, ambiguous: 0, skipped: 0,
        detected_type: 'whatsapp_chat', headers: [], issues: [],
        conversations: senders.size, unique_senders: senders.size, matched_senders: matched,
        file_id: fileId, can_import: totalMsgs > 0 });
    }

    // ── Telegram chat export preview (.json) ──
    if (importType === 'telegram_chat') {
      let chatData;
      try { chatData = JSON.parse(raw); } catch (e) {
        try { require('fs').unlinkSync(file.path); } catch(e2) {}
        return res.status(400).json({ error: 'Invalid JSON — export from Telegram Desktop as JSON' });
      }
      const messages = (chatData.messages || []).filter(m => m.type === 'message');
      const senders = new Map();
      for (const msg of messages) {
        const sender = msg.from || msg.actor || chatData.name || 'Unknown';
        const text = typeof msg.text === 'string' ? msg.text : '';
        if (!text.trim()) continue;
        senders.set(sender, (senders.get(sender) || 0) + 1);
      }
      const { rows: dbPeople } = await db.query(`SELECT id, full_name FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL`, [tenantId]);
      const nameMap = new Map();
      for (const p of dbPeople) nameMap.set(p.full_name.toLowerCase().trim(), p);
      let matched = 0;
      for (const name of senders.keys()) { if (nameMap.has(name.toLowerCase().trim())) matched++; }

      const fileId = require('crypto').randomUUID();
      require('fs').copyFileSync(file.path, `/tmp/ml-preview-${fileId}`);
      try { require('fs').unlinkSync(file.path); } catch(e) {}
      setTimeout(() => { try { require('fs').unlinkSync(`/tmp/ml-preview-${fileId}`); } catch(e) {} }, 30 * 60 * 1000);

      return res.json({ total: messages.length, valid: messages.length, new_records: 0, matched, ambiguous: 0, skipped: 0,
        detected_type: 'telegram_chat', headers: [],  issues: [],
        conversations: senders.size, unique_senders: senders.size, matched_senders: matched,
        file_id: fileId, can_import: messages.length > 0 });
    }

    // Parse CSV
    function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }

    // Find header row (skip LinkedIn preamble)
    const allLines = raw.split('\n');
    let headerIdx = 0;
    for (let i = 0; i < Math.min(allLines.length, 20); i++) {
      const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
      if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) { headerIdx = i; break; }
    }
    if (headerIdx === 0) {
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const parts = allLines[i].split(',');
        if (parts.length >= 3 && parts[0].trim().length > 0 && parts[0].trim().length < 30) { headerIdx = i; break; }
      }
    }

    const lines = allLines.slice(headerIdx).filter(l => l.trim());
    const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSV(lines[i]);
      const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
    }

    // Detect columns
    const colKeys = Object.keys(rows[0] || {});
    const findCol = (...patterns) => colKeys.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
    const firstNameCol = findCol('first name', 'firstname');
    const lastNameCol = findCol('last name', 'lastname');
    const nameCol = findCol('name', 'full name');
    const urlCol = findCol('url', 'profile', 'linkedin');
    const companyCol = findCol('company', 'organisation', 'organization');
    const titleCol = findCol('position', 'title', 'role');
    const emailCol = findCol('email');

    // Determine file type
    let detectedType = importType || 'unknown';
    const fromCol = findCol('from');
    const contentCol = findCol('content', 'body');
    const conversationCol = findCol('conversation');

    if (fromCol && contentCol) {
      detectedType = 'messages';
    } else if (firstNameCol && lastNameCol && urlCol) {
      detectedType = 'linkedin_connections';
    } else if (nameCol || firstNameCol) {
      detectedType = 'contacts';
    }

    // ── Messages preview — different flow, no name column needed ──
    if (detectedType === 'messages') {
      const conversations = new Map();
      const senders = new Set();
      for (const row of rows) {
        const from = row['FROM'] || row['From'] || row['from'] || '';
        const content = row['CONTENT'] || row['Content'] || row['content'] || row['BODY'] || row['Body'] || '';
        const convId = row['CONVERSATION ID'] || row['Conversation ID'] || row['conversation id'] || from;
        if (from) senders.add(from.trim());
        if (!conversations.has(convId)) conversations.set(convId, 0);
        conversations.set(convId, conversations.get(convId) + 1);
      }

      // Match senders against people DB
      const { rows: dbPeople } = await db.query(
        `SELECT id, full_name FROM people WHERE full_name IS NOT NULL AND tenant_id = $1`, [tenantId]
      );
      const nameMap = new Map();
      for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

      let matched = 0;
      for (const name of senders) {
        if (nameMap.has(name.toLowerCase().trim())) matched++;
      }

      const fileId = require('crypto').randomUUID();
      const savedPath = `/tmp/ml-preview-${fileId}`;
      require('fs').copyFileSync(file.path, savedPath);
      try { require('fs').unlinkSync(file.path); } catch(e) {}
      setTimeout(() => { try { require('fs').unlinkSync(savedPath); } catch(e) {} }, 30 * 60 * 1000);

      return res.json({
        total: rows.length,
        valid: rows.length,
        new_records: 0,
        matched,
        ambiguous: 0,
        skipped: 0,
        detected_type: 'messages',
        headers,
        columns: { from: fromCol, content: contentCol, conversation: conversationCol },
        sample_rows: rows.slice(0, 5),
        issues: [],
        conversations: conversations.size,
        unique_senders: senders.size,
        matched_senders: matched,
        file_id: fileId,
        can_import: true,
      });
    }

    // Load existing people for matching (multi-strategy)
    const { rows: dbPeople } = await db.query(
      `SELECT id, full_name, first_name, last_name, linkedin_url, email, current_company_name FROM people WHERE tenant_id = $1`, [tenantId]
    );
    const linkedinIndex = new Map(), nameIndex = new Map(), emailIndex = new Map();
    const lastNameIndex = new Map(); // last_name → [{person, firstName}]
    for (const p of dbPeople) {
      if (p.linkedin_url) { const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1]; if (slug) linkedinIndex.set(slug, p); }
      const norm = (p.full_name || '').toLowerCase().trim();
      if (norm) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
      if (p.email) emailIndex.set(p.email.toLowerCase(), p);
      // Build last name index for fuzzy matching
      const ln = (p.last_name || (p.full_name || '').split(' ').pop() || '').toLowerCase().trim();
      const fn = (p.first_name || (p.full_name || '').split(' ')[0] || '').toLowerCase().trim();
      if (ln && ln.length >= 2) {
        if (!lastNameIndex.has(ln)) lastNameIndex.set(ln, []);
        lastNameIndex.get(ln).push({ ...p, _fn: fn, _ln: ln });
      }
    }

    // Fuzzy name match: same last name + first name starts with same letter, or first name is a common nickname
    function fuzzyNameMatch(inputFirst, inputLast, company) {
      const ln = inputLast.toLowerCase().trim();
      const fn = inputFirst.toLowerCase().trim();
      const candidates = lastNameIndex.get(ln) || [];
      if (!candidates.length) return null;
      // Exact first name
      const exact = candidates.find(c => c._fn === fn);
      if (exact) return { person: exact, matchType: 'name_exact' };
      // Nickname match
      const altName = NICKNAMES[fn];
      if (altName) { const nick = candidates.find(c => c._fn === altName); if (nick) return { person: nick, matchType: 'name_nickname' }; }
      // First initial match + same company
      if (company && fn.length >= 1) {
        const initialMatch = candidates.find(c => c._fn.startsWith(fn[0]) && c.current_company_name && c.current_company_name.toLowerCase().includes(company.toLowerCase().slice(0, 5)));
        if (initialMatch) return { person: initialMatch, matchType: 'name_initial_company' };
      }
      // Single candidate with same last name + first initial
      const initialCands = candidates.filter(c => c._fn.startsWith(fn[0]));
      if (initialCands.length === 1) return { person: initialCands[0], matchType: 'name_initial_unique' };
      return null;
    }

    // Dry-run analysis
    const preview = { total: rows.length, valid: 0, new_records: 0, matched: 0, ambiguous: 0, skipped: 0,
      detected_type: detectedType,
      columns: { firstName: firstNameCol, lastName: lastNameCol, name: nameCol, url: urlCol, company: companyCol, title: titleCol, email: emailCol },
      headers: headers,
      sample_rows: rows.slice(0, 5),
      issues: [],
      matches_preview: [],
      new_preview: []
    };

    for (const row of rows) {
      const firstName = (firstNameCol ? row[firstNameCol] : '') || '';
      const lastName = (lastNameCol ? row[lastNameCol] : '') || '';
      const fullName = (nameCol ? row[nameCol] : `${firstName} ${lastName}`).trim();
      const linkedinUrl = (urlCol ? row[urlCol] : '') || '';
      const company = (companyCol ? row[companyCol] : '') || '';
      const title = (titleCol ? row[titleCol] : '') || '';
      const email = (emailCol ? row[emailCol] : '') || '';

      if (!fullName || fullName.length < 2) { preview.skipped++; continue; }
      preview.valid++;

      // Match check — multi-strategy: linkedin URL → email → exact name → fuzzy name
      let matched = false, matchType = null;
      const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
      if (slug && linkedinIndex.has(slug)) { matched = true; matchType = 'linkedin_url'; }
      else if (email && emailIndex.has(email.toLowerCase())) { matched = true; matchType = 'email'; }
      else {
        const cands = nameIndex.get(fullName.toLowerCase().trim()) || [];
        if (cands.length === 1) { matched = true; matchType = 'name_unique'; }
        else if (cands.length > 1) { preview.ambiguous++; matchType = 'name_ambiguous'; }
        // Fuzzy: nickname / initial + company matching
        if (!matched && matchType !== 'name_ambiguous') {
          const inputFirst = firstName || fullName.split(' ')[0] || '';
          const inputLast = lastName || fullName.split(' ').pop() || '';
          if (inputLast) {
            const fuzzy = fuzzyNameMatch(inputFirst, inputLast, company);
            if (fuzzy) { matched = true; matchType = fuzzy.matchType; }
          }
        }
      }

      if (matched) {
        preview.matched++;
        if (preview.matches_preview.length < 5) preview.matches_preview.push({ name: fullName, company, title, match_type: matchType });
      } else if (matchType !== 'name_ambiguous') {
        preview.new_records++;
        if (preview.new_preview.length < 5) preview.new_preview.push({ name: fullName, company, title });
      }
    }

    // Issues
    if (preview.skipped > 0) preview.issues.push({ type: 'warning', message: preview.skipped + ' rows have no name — will be skipped' });
    if (preview.ambiguous > 0) preview.issues.push({ type: 'warning', message: preview.ambiguous + ' rows match multiple existing people — will create new records' });
    if (!firstNameCol && !lastNameCol && !nameCol) preview.issues.push({ type: 'error', message: 'No name column detected — cannot import' });
    if (preview.total === 0) preview.issues.push({ type: 'error', message: 'File appears empty' });
    if (preview.matched > preview.total * 0.95) preview.issues.push({ type: 'info', message: 'Most records already exist — this may be a re-import' });

    // Save file for confirmed import
    const fileId = require('crypto').randomUUID();
    const savedPath = `/tmp/ml-preview-${fileId}`;
    require('fs').copyFileSync(file.path, savedPath);
    try { require('fs').unlinkSync(file.path); } catch(e) {}

    // Clean up after 30 minutes
    setTimeout(() => { try { require('fs').unlinkSync(savedPath); } catch(e) {} }, 30 * 60 * 1000);

    preview.file_id = fileId;
    preview.can_import = !preview.issues.some(i => i.type === 'error');

    res.json(preview);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Import confirmed — write to DB (accepts file upload OR file_id from preview)
router.post('/api/profile/import', authenticateToken, profileUpload.single('file'), async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Support both: new file upload OR confirmed import from preview
    const fileId = req.query.file_id || req.body?.file_id;
    let file = req.file;
    let importType = req.body?.import_type || req.query.import_type;

    if (!file && fileId) {
      // Load from preview save
      const savedPath = `/tmp/ml-preview-${fileId}`;
      if (require('fs').existsSync(savedPath)) {
        file = { path: savedPath, originalname: 'preview-import' };
        // Parse import_type from body if JSON
        if (req.body && typeof req.body === 'object') importType = req.body.import_type || importType;
      }
    }

    const userId = req.user.user_id;
    const tenantId = req.tenant_id;
    if (!file) return res.status(400).json({ error: 'No file' });

    // LinkedIn connections
    if (importType === 'linkedin_connections') {
      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines = raw.split('\n');
      // Find real header row — skip LinkedIn preamble
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
        if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
          headerIdx = i; break;
        }
      }
      if (headerIdx === 0) {
        for (let i = 0; i < Math.min(allLines.length, 20); i++) {
          const parts = allLines[i].split(',');
          if (parts.length >= 3 && parts[0].trim().length > 0 && parts[0].trim().length < 30) { headerIdx = i; break; }
        }
      }
      const lines = allLines.slice(headerIdx).filter(l => l.trim());
      function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const vals = parseCSV(lines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
      }

      // Load people for matching
      const { rows: dbPeople } = await db.query(
        `SELECT id, full_name, linkedin_url FROM people WHERE tenant_id = $1`, [tenantId]
      );
      const linkedinIndex = new Map(), nameIndex = new Map();
      for (const p of dbPeople) {
        if (p.linkedin_url) { const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1]; if (slug) linkedinIndex.set(slug, p); }
        const norm = (p.full_name || '').toLowerCase().trim();
        if (norm) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
      }

      // Flexible header detection — LinkedIn exports vary
      const sampleRow = rows[0] || {};
      const colKeys = Object.keys(sampleRow);
      const findCol = (...patterns) => colKeys.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
      const firstNameCol = findCol('first name', 'firstname');
      const lastNameCol = findCol('last name', 'lastname');
      const urlCol = findCol('url', 'profile');
      const companyCol = findCol('company', 'organisation', 'organization');
      const positionCol = findCol('position', 'title', 'role');
      const emailCol = findCol('email');

      console.log(`LinkedIn import: detected columns — name: "${firstNameCol}"+"${lastNameCol}", url: "${urlCol}", company: "${companyCol}"`);

      const stats = { total: rows.length, matched: 0, created: 0, proximity_created: 0, skipped: 0 };
      for (const row of rows) {
        const firstName = (firstNameCol ? row[firstNameCol] : '') || '';
        const lastName = (lastNameCol ? row[lastNameCol] : '') || '';
        const fullName = `${firstName} ${lastName}`.trim();
        const linkedinUrl = (urlCol ? row[urlCol] : '') || '';
        const company = (companyCol ? row[companyCol] : '') || '';
        const position = (positionCol ? row[positionCol] : '') || '';
        const email = (emailCol ? row[emailCol] : '') || '';
        if (!fullName || fullName.length < 2) { stats.skipped++; continue; }

        let personId = null;
        const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
        if (slug && linkedinIndex.has(slug)) personId = linkedinIndex.get(slug).id;
        if (!personId) { const cands = nameIndex.get(fullName.toLowerCase().trim()) || []; if (cands.length === 1) personId = cands[0].id; }

        if (personId) {
          stats.matched++;
          // Fill blank company/title on existing records
          if (company || position) {
            await db.query(
              `UPDATE people SET
                 current_company_name = COALESCE(NULLIF(current_company_name, ''), $2),
                 current_title = COALESCE(NULLIF(current_title, ''), $3),
                 linkedin_url = COALESCE(NULLIF(linkedin_url, ''), $4),
                 updated_at = NOW()
               WHERE id = $1`,
              [personId, company || null, position || null, linkedinUrl || null]
            );
          }
        } else {
          try {
            const { rows: [newP] } = await db.query(
              `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, linkedin_url, source, created_by, tenant_id)
               VALUES ($1,$2,$3,$4,$5,$6,'linkedin_import',$7,$8) RETURNING id`,
              [fullName, firstName, lastName, position || null, company || null, linkedinUrl || null, userId, tenantId]);
            personId = newP.id;
            stats.created++;
          } catch (e) { stats.skipped++; continue; }
        }

        if (personId) {
          // Link to company record if company name provided
          if (company) {
            try {
              const { rows: [co] } = await db.query(
                `SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
                [company.trim(), tenantId]
              );
              if (co) {
                await db.query(
                  `UPDATE people SET current_company_id = $1, updated_at = NOW()
                   WHERE id = $2 AND (current_company_id IS NULL OR current_company_id != $1)`,
                  [co.id, personId]
                );
              } else {
                // Create company on the fly
                const { rows: [newCo] } = await db.query(
                  `INSERT INTO companies (name, source, tenant_id, created_at, updated_at)
                   VALUES ($1, 'linkedin_import', $2, NOW(), NOW())
                   ON CONFLICT DO NOTHING RETURNING id`,
                  [company.trim(), tenantId]
                );
                if (newCo) {
                  await db.query(
                    `UPDATE people SET current_company_id = $1, updated_at = NOW() WHERE id = $2`,
                    [newCo.id, personId]
                  );
                }
              }
            } catch (e) {}
          }

          try {
            await db.query(
              `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
               VALUES ($1, $2, 'linkedin_connection', 0.5, 'linkedin_import', $3)
               ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
                 relationship_strength = GREATEST(team_proximity.relationship_strength, 0.5),
                 tenant_id = COALESCE(team_proximity.tenant_id, EXCLUDED.tenant_id)`,
              [personId, userId, tenantId]);
            stats.proximity_created++;
          } catch (e) {}
        }
      }

      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'linkedin_connections_import', 'people', null, { ...stats, filename: file.originalname });
      return res.json(stats);
    }

    // Contacts CSV
    if (importType === 'contacts') {
      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines2 = raw.split('\n').filter(l => l.trim());
      // Find header row
      let hIdx2 = 0;
      for (let i = 0; i < Math.min(allLines2.length, 20); i++) {
        const line = allLines2[i].toLowerCase();
        if (line.includes('name') || line.includes('email')) { hIdx2 = i; break; }
      }
      const lines = allLines2.slice(hIdx2);
      function parseCSV2(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const headers = parseCSV2(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
      const rows2 = [];
      for (let i = 1; i < lines.length; i++) {
        const vals = parseCSV2(lines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows2.push(row);
      }

      const colKeys2 = Object.keys(rows2[0] || {});
      const findCol2 = (...patterns) => colKeys2.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
      const firstNameCol2 = findCol2('first name', 'firstname', 'given');
      const lastNameCol2 = findCol2('last name', 'lastname', 'family', 'surname');
      const nameCol2 = findCol2('name', 'full name');
      const titleCol2 = findCol2('position', 'title', 'role', 'job');
      const companyCol2 = findCol2('company', 'organisation', 'organization', 'org');
      const emailCol2 = findCol2('email', 'e-mail');

      // Load people for multi-strategy matching
      const { rows: dbPeople2 } = await db.query(
        `SELECT id, full_name, first_name, last_name, linkedin_url, email, email_alt, current_company_name, source FROM people WHERE tenant_id = $1`, [tenantId]
      );
      const emailIdx2 = new Map(), nameIdx2 = new Map(), lastNameIdx2 = new Map();
      for (const p of dbPeople2) {
        if (p.email) emailIdx2.set(p.email.toLowerCase(), p);
        if (p.email_alt) emailIdx2.set(p.email_alt.toLowerCase(), p);
        const norm = (p.full_name || '').toLowerCase().trim();
        if (norm) { if (!nameIdx2.has(norm)) nameIdx2.set(norm, []); nameIdx2.get(norm).push(p); }
        const ln = (p.last_name || (p.full_name || '').split(' ').pop() || '').toLowerCase().trim();
        const fn = (p.first_name || (p.full_name || '').split(' ')[0] || '').toLowerCase().trim();
        if (ln && ln.length >= 2) {
          if (!lastNameIdx2.has(ln)) lastNameIdx2.set(ln, []);
          lastNameIdx2.get(ln).push({ ...p, _fn: fn, _ln: ln });
        }
      }

      // Same fuzzy match as preview
      function fuzzyMatch2(inputFirst, inputLast, company) {
        const ln = inputLast.toLowerCase().trim();
        const fn = inputFirst.toLowerCase().trim();
        const candidates = lastNameIdx2.get(ln) || [];
        if (!candidates.length) return null;
        const exact = candidates.find(c => c._fn === fn);
        if (exact) return exact;
        const altName = NICKNAMES[fn];
        if (altName) { const nick = candidates.find(c => c._fn === altName); if (nick) return nick; }
        if (company && fn.length >= 1) {
          const m = candidates.find(c => c._fn.startsWith(fn[0]) && c.current_company_name && c.current_company_name.toLowerCase().includes(company.toLowerCase().slice(0, 5)));
          if (m) return m;
        }
        const ic = candidates.filter(c => c._fn.startsWith(fn[0]));
        if (ic.length === 1) return ic[0];
        return null;
      }

      const stats = { total: 0, created: 0, matched: 0, enriched: 0, skipped: 0 };
      for (const row of rows2) {
        const firstName = (firstNameCol2 ? row[firstNameCol2] : '') || '';
        const lastName = (lastNameCol2 ? row[lastNameCol2] : '') || '';
        const fullName = (nameCol2 ? row[nameCol2] : `${firstName} ${lastName}`).trim();
        const company = (companyCol2 ? row[companyCol2] : '') || '';
        const title = (titleCol2 ? row[titleCol2] : '') || '';
        const email = (emailCol2 ? row[emailCol2] : '') || '';
        if (!fullName || fullName.length < 2) continue;
        stats.total++;

        // Multi-strategy match: email → exact name → fuzzy name
        let existingPerson = null;
        if (email && emailIdx2.has(email.toLowerCase())) {
          existingPerson = emailIdx2.get(email.toLowerCase());
        }
        if (!existingPerson) {
          const cands = nameIdx2.get(fullName.toLowerCase().trim()) || [];
          if (cands.length === 1) existingPerson = cands[0];
        }
        if (!existingPerson) {
          const fn = firstName || fullName.split(' ')[0] || '';
          const ln = lastName || fullName.split(' ').pop() || '';
          existingPerson = fuzzyMatch2(fn, ln, company);
        }

        if (existingPerson) {
          stats.matched++;
          // Enrich — fill blanks ONLY, never overwrite LinkedIn data
          const isLinkedinSource = (existingPerson.source || '').includes('linkedin');
          const updates = [];
          const vals = [];
          let idx = 1;

          // Email: always fill if blank (contacts are the best source for email)
          if (email && !existingPerson.email) {
            updates.push(`email = $${idx++}`); vals.push(email);
          } else if (email && existingPerson.email && email.toLowerCase() !== existingPerson.email.toLowerCase() && !existingPerson.email_alt) {
            updates.push(`email_alt = $${idx++}`); vals.push(email);
          }
          // Company/title: only fill if blank OR source is NOT linkedin
          if (company && (!existingPerson.current_company_name || (!isLinkedinSource && existingPerson.current_company_name !== company))) {
            if (!existingPerson.current_company_name) { updates.push(`current_company_name = $${idx++}`); vals.push(company); }
          }
          if (title && !existingPerson.current_title) {
            updates.push(`current_title = $${idx++}`); vals.push(title);
          }

          if (updates.length) {
            updates.push(`updated_at = NOW()`);
            await db.query(`UPDATE people SET ${updates.join(', ')} WHERE id = $${idx}`, [...vals, existingPerson.id]);
            stats.enriched++;
          }
        } else {
          // Create new person
          try {
            await db.query(
              `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, email, source, created_by, tenant_id)
               VALUES ($1,$2,$3,$4,$5,$6,'contact_import',$7,$8)`,
              [fullName, firstName || fullName.split(' ')[0], lastName || fullName.split(' ').pop(),
               title || null, company || null, email || null, userId, tenantId]);
            stats.created++;
          } catch (e) { stats.skipped++; }
        }
      }
      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'csv_import', 'people', null, { ...stats, filename: file.originalname });
      return res.json(stats);
    }

    // Document upload (PDF, XLSX, TXT)
    if (importType === 'document') {
      const hash = require('crypto').createHash('md5').update(file.originalname + file.size).digest('hex');
      const { rows: exists } = await db.query(`SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2`, [hash, tenantId]);
      if (exists.length) { try { require('fs').unlinkSync(file.path); } catch(e){} return res.json({ documents_created: 0, message: 'File already imported' }); }

      let content = file.originalname;
      if (file.originalname.endsWith('.pdf')) {
        try { const pdfParse = require('pdf-parse'); const d = await pdfParse(require('fs').readFileSync(file.path)); content = d.text; } catch(e) {}
      } else if (file.originalname.match(/\.xlsx?$/i)) {
        try { const XLSX = require('xlsx'); const wb = XLSX.readFile(file.path); content = wb.SheetNames.map(n => { const r = XLSX.utils.sheet_to_json(wb.Sheets[n], {header:1,defval:''}).slice(0,100).map(r=>Object.values(r).join(' | ')).join('\n'); return `=== ${n} ===\n${r}`; }).join('\n\n'); } catch(e) {}
      } else {
        try { content = require('fs').readFileSync(file.path, 'utf8'); } catch(e) {}
      }

      await db.query(`
        INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
          tenant_id, uploaded_by_user_id, processing_status, created_at)
        VALUES ($1, $2, $3, 'user_upload', $4, $5, $6, $7, 'processed', NOW())
      `, [file.originalname, content.slice(0, 50000), file.originalname, `upload://${file.originalname}`, hash, tenantId, userId]);

      // Embed
      try {
        const emb = await generateQueryEmbedding((file.originalname + '\n\n' + content).slice(0, 8000));
        const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: hash, vector: emb, payload: { tenant_id: tenantId, title: file.originalname, source_type: 'user_upload' } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 15000 },
            (r) => { const c = []; r.on('data', d => c.push(d)); r.on('end', () => resolve()); });
          qReq.on('error', reject); qReq.write(body); qReq.end();
        });
      } catch(e) {}

      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'document_upload', 'external_documents', null, { filename: file.originalname, size: file.size });
      return res.json({ documents_created: 1, filename: file.originalname });
    }

    // LinkedIn messages
    if (importType === 'messages') {
      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      try { require('fs').unlinkSync(file.path); } catch (e) {}

      function parseMsgCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const allLines = raw.split('\n').filter(l => l.trim());
      const headers = parseMsgCSV(allLines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
      const msgRows = [];
      for (let i = 1; i < allLines.length; i++) {
        const vals = parseMsgCSV(allLines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); msgRows.push(row);
      }

      // Load people for name matching
      const { rows: dbPeople } = await db.query(
        `SELECT id, full_name FROM people WHERE full_name IS NOT NULL AND tenant_id = $1`, [tenantId]
      );
      const nameMap = new Map();
      for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

      // Group by conversation
      const conversations = new Map();
      const stats = { total: 0, matched: 0, interactions_created: 0, errors: 0 };
      for (const row of msgRows) {
        const from = row['FROM'] || row['From'] || row['from'] || '';
        const to = row['TO'] || row['To'] || row['to'] || '';
        const content = row['CONTENT'] || row['Content'] || row['content'] || row['BODY'] || row['Body'] || '';
        const date = row['DATE'] || row['Date'] || row['date'] || '';
        const convId = row['CONVERSATION ID'] || row['Conversation ID'] || row['conversation id'] || `${from}-${to}`;
        if (!content.trim()) continue;
        stats.total++;
        if (!conversations.has(convId)) conversations.set(convId, []);
        conversations.get(convId).push({ from, to, content, date });
      }

      for (const [convId, messages] of conversations) {
        const participants = new Set();
        messages.forEach(m => { if (m.from) participants.add(m.from.trim()); if (m.to) participants.add(m.to.trim()); });

        for (const name of participants) {
          const match = nameMap.get(name.toLowerCase().trim());
          if (match) {
            stats.matched++;
            const sorted = messages.sort((a, b) => new Date(a.date) - new Date(b.date));
            const summary = sorted.map(m => `[${m.date}] ${m.from}: ${m.content}`).join('\n').slice(0, 5000);
            const latestDate = sorted[sorted.length - 1]?.date;
            try {
              await db.query(
                `INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, source, interaction_at, tenant_id)
                 VALUES ($1, $2, $2, 'linkedin_message', $3, $4, 'linkedin_import', $5, $6)
                 ON CONFLICT DO NOTHING`,
                [match.id, userId, `LinkedIn conversation (${messages.length} messages)`, summary,
                 latestDate ? new Date(latestDate).toISOString() : new Date().toISOString(), tenantId]
              );
              stats.interactions_created++;
            } catch (e) { stats.errors++; }
          }
        }
      }

      auditLog(userId, 'linkedin_messages_import', 'interactions', null, {
        total_messages: stats.total, conversations: conversations.size,
        matched: stats.matched, interactions_created: stats.interactions_created
      });

      return res.json({
        total: stats.total,
        conversations: conversations.size,
        matched: stats.matched,
        created: stats.interactions_created,
        errors: stats.errors,
        message: `Processed ${stats.total} messages across ${conversations.size} conversations. Created ${stats.interactions_created} interaction records.`
      });
    }

    // WhatsApp chat export (.txt)
    if (importType === 'whatsapp_chat') {
      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '');
      try { require('fs').unlinkSync(file.path); } catch (e) {}

      // WhatsApp format: [DD/MM/YYYY, HH:MM:SS] Name: Message
      // or: DD/MM/YYYY, HH:MM - Name: Message
      const msgRegex = /^\[?(\d{1,2}\/\d{1,2}\/\d{2,4}),?\s*(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[ap]m)?)\]?\s*[-–]\s*([^:]+):\s*(.+)/i;
      const lines = raw.split('\n');
      const conversations = new Map();
      const senders = new Set();
      let totalMsgs = 0;

      for (const line of lines) {
        const m = line.match(msgRegex);
        if (!m) continue;
        const dateStr = m[1] + ' ' + m[2];
        const sender = m[3].trim();
        const content = m[4].trim();
        if (content === '<Media omitted>' || content === 'This message was deleted') continue;
        senders.add(sender);
        totalMsgs++;
        if (!conversations.has(sender)) conversations.set(sender, []);
        conversations.get(sender).push({ date: dateStr, content });
      }

      // Match senders to people
      const { rows: dbPeople } = await db.query(
        `SELECT id, full_name, phone FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL`, [tenantId]
      );
      const nameMap = new Map();
      for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

      const stats = { total: totalMsgs, matched: 0, interactions_created: 0, errors: 0 };

      for (const [sender, messages] of conversations) {
        const match = nameMap.get(sender.toLowerCase().trim());
        if (!match) continue;
        stats.matched++;

        const sorted = messages;
        const summary = sorted.map(m => `[${m.date}] ${sender}: ${m.content}`).join('\n').slice(0, 5000);
        const lastMsg = sorted[sorted.length - 1];

        try {
          await db.query(
            `INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, channel, source, external_id, interaction_at, tenant_id)
             VALUES ($1, $2, $2, 'whatsapp_message', $3, $4, 'whatsapp', 'whatsapp_import', $5, $6, $7)
             ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
            [match.id, userId, `WhatsApp (${messages.length} messages)`, summary,
             'wa:' + match.id + ':' + Date.now(), new Date().toISOString(), tenantId]
          );
          stats.interactions_created++;
        } catch (e) { stats.errors++; }

        // Update proximity
        const msgCount = messages.length;
        const strength = msgCount >= 20 ? 0.85 : msgCount >= 5 ? 0.65 : 0.35;
        const relType = msgCount >= 20 ? 'whatsapp_frequent' : msgCount >= 5 ? 'whatsapp_moderate' : 'whatsapp_minimal';
        await db.query(
          `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, notes, source, interaction_count)
           VALUES ($1, $2, $3, $4, $5, 'whatsapp_import', $6)
           ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
             relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
             interaction_count = EXCLUDED.interaction_count,
             notes = EXCLUDED.notes, updated_at = NOW()`,
          [match.id, userId, relType, strength, `${msgCount} WhatsApp messages`, msgCount]
        );
      }

      auditLog(userId, 'whatsapp_import', 'interactions', null, { total: stats.total, matched: stats.matched, created: stats.interactions_created });
      return res.json({ total: stats.total, matched: stats.matched, created: stats.interactions_created, senders: senders.size, errors: stats.errors,
        message: `Processed ${stats.total} WhatsApp messages from ${senders.size} contacts. Created ${stats.interactions_created} interactions.` });
    }

    // Telegram chat export (.json from Telegram Desktop)
    if (importType === 'telegram_chat') {
      const rawJson = require('fs').readFileSync(file.path, 'utf8');
      try { require('fs').unlinkSync(file.path); } catch (e) {}

      let chatData;
      try { chatData = JSON.parse(rawJson); } catch (e) { return res.status(400).json({ error: 'Invalid JSON — export from Telegram Desktop as JSON' }); }

      const messages = chatData.messages || [];
      const chatName = chatData.name || 'Unknown Chat';
      const senders = new Map(); // name → messages[]

      for (const msg of messages) {
        if (msg.type !== 'message') continue;
        const sender = msg.from || msg.actor || chatName;
        const text = typeof msg.text === 'string' ? msg.text : (Array.isArray(msg.text) ? msg.text.map(t => typeof t === 'string' ? t : t.text || '').join('') : '');
        if (!text.trim()) continue;
        if (!senders.has(sender)) senders.set(sender, []);
        senders.get(sender).push({ date: msg.date, text });
      }

      // Match senders to people
      const { rows: dbPeople } = await db.query(
        `SELECT id, full_name FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL`, [tenantId]
      );
      const nameMap = new Map();
      for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

      const stats = { total: messages.length, matched: 0, interactions_created: 0, errors: 0 };

      for (const [sender, msgs] of senders) {
        const match = nameMap.get(sender.toLowerCase().trim());
        if (!match) continue;
        stats.matched++;

        const summary = msgs.map(m => `[${m.date}] ${sender}: ${m.text}`).join('\n').slice(0, 5000);
        try {
          await db.query(
            `INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, channel, source, external_id, interaction_at, tenant_id)
             VALUES ($1, $2, $2, 'telegram_message', $3, $4, 'telegram', 'telegram_import', $5, $6, $7)
             ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
            [match.id, userId, `Telegram (${msgs.length} messages)`, summary,
             'tg:' + match.id + ':' + Date.now(), new Date().toISOString(), tenantId]
          );
          stats.interactions_created++;
        } catch (e) { stats.errors++; }

        // Update proximity
        const msgCount = msgs.length;
        const strength = msgCount >= 20 ? 0.85 : msgCount >= 5 ? 0.65 : 0.35;
        const relType = msgCount >= 20 ? 'telegram_frequent' : msgCount >= 5 ? 'telegram_moderate' : 'telegram_minimal';
        await db.query(
          `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, notes, source, interaction_count)
           VALUES ($1, $2, $3, $4, $5, 'telegram_import', $6)
           ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
             relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
             interaction_count = EXCLUDED.interaction_count,
             notes = EXCLUDED.notes, updated_at = NOW()`,
          [match.id, userId, relType, strength, `${msgCount} Telegram messages`, msgCount]
        );
      }

      auditLog(userId, 'telegram_import', 'interactions', null, { total: stats.total, matched: stats.matched, created: stats.interactions_created });
      return res.json({ total: stats.total, matched: stats.matched, created: stats.interactions_created, senders: senders.size, errors: stats.errors,
        message: `Processed ${stats.total} Telegram messages from ${senders.size} contacts. Created ${stats.interactions_created} interactions.` });
    }

    try { require('fs').unlinkSync(file.path); } catch (e) {}
    res.json({ error: 'Unknown import type: ' + importType });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger sync for current user
router.post('/api/profile/trigger-sync', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT google_email FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true`, [req.user.user_id]
    );
    if (!rows.length) return res.json({ message: 'No Google account connected. Connect from this page first.' });
    res.json({ message: `Sync triggered for ${rows[0].google_email}. Gmail and Drive will sync on the next cycle (every 15 minutes).` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGING INTEGRATIONS — Telegram + WhatsApp
// ═══════════════════════════════════════════════════════════════════════════════

// Messaging status
router.get('/api/profile/messaging-status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT telegram_chat_id, whatsapp_phone, whatsapp_verified FROM users WHERE id = $1`, [req.user.user_id]
    );
    const u = rows[0] || {};

    // Check for MTProto session
    const { rows: tgRows } = await db.query(
      `SELECT phone, sync_enabled, last_sync_at FROM user_telegram_accounts WHERE user_id = $1 LIMIT 1`, [req.user.user_id]
    ).catch(() => ({ rows: [] }));
    const tgAccount = tgRows[0];

    res.json({
      telegram: {
        connected: !!(u.telegram_chat_id || tgAccount),
        mode: tgAccount ? 'mtproto' : u.telegram_chat_id ? 'bot' : null,
        phone: tgAccount?.phone || null,
        sync_enabled: tgAccount?.sync_enabled || false,
        last_sync: tgAccount?.last_sync_at || null,
      },
      whatsapp: { connected: !!u.whatsapp_verified, phone: u.whatsapp_phone || null },
    });
  } catch (err) { res.json({ telegram: { connected: false }, whatsapp: { connected: false } }); }
});

// Telegram MTProto — Step 1: Send code
router.post('/api/profile/telegram/send-code', authenticateToken, async (req, res) => {
  const phone = (req.body.phone || '').replace(/[\s\-()]/g, '');
  if (!phone || phone.length < 8) return res.status(400).json({ error: 'Valid phone number with country code required' });
  if (!process.env.TELEGRAM_API_ID || !process.env.TELEGRAM_API_HASH) {
    return res.status(500).json({ error: 'Telegram API credentials not configured (TELEGRAM_API_ID + TELEGRAM_API_HASH)' });
  }

  try {
    const { TelegramClient } = require('telegram');
    const { StringSession } = require('telegram/sessions');
    const client = new TelegramClient(
      new StringSession(''),
      parseInt(process.env.TELEGRAM_API_ID),
      process.env.TELEGRAM_API_HASH,
      { connectionRetries: 3 }
    );
    await client.connect();

    const result = await client.sendCode(
      { apiId: parseInt(process.env.TELEGRAM_API_ID), apiHash: process.env.TELEGRAM_API_HASH },
      phone
    );

    // Store pending auth state (temporary, expires in 10 min)
    const pendingKey = `tg_auth_${req.user.user_id}`;
    global._tgPendingAuth = global._tgPendingAuth || {};
    global._tgPendingAuth[pendingKey] = {
      client,
      phone,
      phoneCodeHash: result.phoneCodeHash,
      expiresAt: Date.now() + 600000,
    };

    res.json({ ok: true, phone });
  } catch (err) {
    console.error('Telegram send-code error:', err.message);
    res.status(400).json({ error: err.message.includes('PHONE_NUMBER_INVALID') ? 'Invalid phone number — include country code (e.g. +61...)' : err.message });
  }
});

// Telegram MTProto — Step 2: Verify code
router.post('/api/profile/telegram/verify-code', authenticateToken, async (req, res) => {
  const code = (req.body.code || '').trim();
  const password = req.body.password || null; // 2FA password if needed
  if (!code) return res.status(400).json({ error: 'Enter the code from Telegram' });

  const pendingKey = `tg_auth_${req.user.user_id}`;
  const pending = (global._tgPendingAuth || {})[pendingKey];
  if (!pending || pending.expiresAt < Date.now()) {
    return res.status(400).json({ error: 'Session expired — request a new code' });
  }

  try {
    const client = pending.client;

    try {
      await client.invoke(
        new (require('telegram/tl').Api.auth.SignIn)({
          phoneNumber: pending.phone,
          phoneCodeHash: pending.phoneCodeHash,
          phoneCode: code,
        })
      );
    } catch (err) {
      if (err.errorMessage === 'SESSION_PASSWORD_NEEDED') {
        if (!password) {
          return res.json({ needs_2fa: true, message: 'Two-factor authentication enabled — enter your Telegram password' });
        }
        const { computeCheck } = require('telegram/Password');
        const srpResult = await client.invoke(new (require('telegram/tl').Api.account.GetPassword)());
        const srpCheck = await computeCheck(srpResult, password);
        await client.invoke(new (require('telegram/tl').Api.auth.CheckPassword)({ password: srpCheck }));
      } else {
        throw err;
      }
    }

    // Success — save session string
    const sessionString = client.session.save();

    // Get Telegram user info
    const me = await client.getMe();

    await platformPool.query(`
      INSERT INTO user_telegram_accounts (user_id, phone, telegram_user_id, session_string, sync_enabled, first_name, username, created_at)
      VALUES ($1, $2, $3, $4, true, $5, $6, NOW())
      ON CONFLICT (user_id) DO UPDATE SET
        phone = EXCLUDED.phone, telegram_user_id = EXCLUDED.telegram_user_id,
        session_string = EXCLUDED.session_string, sync_enabled = true,
        first_name = EXCLUDED.first_name, username = EXCLUDED.username, updated_at = NOW()
    `, [req.user.user_id, pending.phone, String(me.id), sessionString, me.firstName || '', me.username || '']);

    // Also store chat_id for bot notifications
    await platformPool.query(
      `UPDATE users SET telegram_chat_id = $1 WHERE id = $2`,
      [String(me.id), req.user.user_id]
    );

    // Cleanup
    await client.disconnect();
    delete global._tgPendingAuth[pendingKey];

    res.json({ ok: true, username: me.username, first_name: me.firstName });
  } catch (err) {
    console.error('Telegram verify error:', err.message);
    res.status(400).json({ error: err.errorMessage || err.message });
  }
});

// Telegram — generate bot link with user token (fallback for bot-only mode)
router.get('/api/profile/telegram/link', authenticateToken, (req, res) => {
  const botUsername = process.env.TELEGRAM_BOT_USERNAME;
  if (!botUsername) return res.json({ bot_url: null, error: 'Telegram bot not configured' });
  const linkToken = Buffer.from(JSON.stringify({ userId: req.user.user_id, ts: Date.now() })).toString('base64url');
  res.json({ bot_url: `https://t.me/${botUsername}?start=${linkToken}` });
});

// Telegram webhook — receives updates from Telegram Bot API
router.post('/api/webhooks/telegram', async (req, res) => {
  res.sendStatus(200); // Always ACK fast
  try {
    const update = req.body;
    if (!update.message) return;
    const chatId = String(update.message.chat.id);
    const text = update.message.text || '';

    // Handle /start command — link account
    if (text.startsWith('/start ')) {
      const token = text.replace('/start ', '').trim();
      try {
        const decoded = JSON.parse(Buffer.from(token, 'base64url').toString('utf8'));
        if (decoded.userId && (Date.now() - decoded.ts) < 600000) { // 10 min expiry
          await platformPool.query(
            `UPDATE users SET telegram_chat_id = $1 WHERE id = $2`,
            [chatId, decoded.userId]
          );
          // Send confirmation
          if (process.env.TELEGRAM_BOT_TOKEN) {
            await fetch(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, text: '✅ Connected to MitchelLake Signals! You will receive signal alerts and daily digests here.' })
            });
          }
        }
      } catch (e) { console.error('Telegram link error:', e.message); }
      return;
    }

    // Incoming messages from connected users → log as interactions
    const { rows: [user] } = await platformPool.query(
      `SELECT id, tenant_id FROM users WHERE telegram_chat_id = $1`, [chatId]
    );
    if (!user) return;

    // Forward messages can contain contact intelligence — store as interaction
    const senderName = [update.message.from?.first_name, update.message.from?.last_name].filter(Boolean).join(' ');
    if (text && text.length > 5) {
      // Check if user is forwarding a message from a contact
      const forwardFrom = update.message.forward_from
        ? [update.message.forward_from.first_name, update.message.forward_from.last_name].filter(Boolean).join(' ')
        : null;

      if (forwardFrom) {
        // Try to match forwarded sender to a person
        const { rows: matches } = await platformPool.query(
          `SELECT id FROM people WHERE tenant_id = $1 AND LOWER(full_name) = LOWER($2) LIMIT 1`,
          [user.tenant_id, forwardFrom]
        );
        if (matches.length) {
          await platformPool.query(
            `INSERT INTO interactions (person_id, user_id, interaction_type, direction, subject, summary, channel, source, external_id, interaction_at)
             VALUES ($1, $2, 'telegram_forward', 'inbound', $3, $4, 'telegram', 'telegram_live', $5, NOW())
             ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
            [matches[0].id, user.id, `Forwarded from ${forwardFrom}`, text.slice(0, 2000), 'tg:live:' + update.message.message_id]
          );
        }
      }
    }
  } catch (err) { console.error('Telegram webhook error:', err.message); }
});

// WhatsApp — send verification code
router.post('/api/profile/whatsapp/verify', authenticateToken, async (req, res) => {
  const phone = (req.body.phone || '').replace(/[\s\-()]/g, '');
  if (!phone || phone.length < 8) return res.status(400).json({ error: 'Valid phone number required' });

  // Generate 6-digit code
  const code = String(Math.floor(100000 + Math.random() * 900000));

  // Store pending verification
  await platformPool.query(
    `UPDATE users SET whatsapp_phone = $1, whatsapp_verify_code = $2, whatsapp_verify_expires = NOW() + INTERVAL '10 minutes'
     WHERE id = $3`,
    [phone, code, req.user.user_id]
  );

  // Send via Twilio WhatsApp (or fall back to SMS)
  if (process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_WHATSAPP_PHONE) {
    try {
      const twilio = require('twilio')(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
      await twilio.messages.create({
        from: `whatsapp:${process.env.TWILIO_WHATSAPP_PHONE}`,
        to: `whatsapp:${phone}`,
        body: `Your MitchelLake Signals verification code is: ${code}`
      });
      return res.json({ ok: true });
    } catch (err) {
      console.error('Twilio WhatsApp error:', err.message);
      // Fall through — code is stored, user can be told verbally in dev
    }
  }

  // Dev mode — log the code (no Twilio configured)
  console.log(`📱 WhatsApp verification for ${phone}: ${code}`);
  res.json({ ok: true, dev_note: 'Code logged to console (Twilio not configured)' });
});

// WhatsApp — confirm verification code
router.post('/api/profile/whatsapp/confirm', authenticateToken, async (req, res) => {
  const code = (req.body.code || '').trim();
  const { rows: [user] } = await platformPool.query(
    `SELECT whatsapp_phone, whatsapp_verify_code, whatsapp_verify_expires FROM users WHERE id = $1`,
    [req.user.user_id]
  );
  if (!user || !user.whatsapp_verify_code) return res.status(400).json({ error: 'No pending verification' });
  if (new Date(user.whatsapp_verify_expires) < new Date()) return res.status(400).json({ error: 'Code expired — request a new one' });
  if (user.whatsapp_verify_code !== code) return res.status(400).json({ error: 'Invalid code' });

  await platformPool.query(
    `UPDATE users SET whatsapp_verified = true, whatsapp_verify_code = NULL WHERE id = $1`,
    [req.user.user_id]
  );
  res.json({ ok: true });
});

// Disconnect messaging channel
router.post('/api/profile/messaging/:channel/disconnect', authenticateToken, async (req, res) => {
  const { channel } = req.params;
  if (channel === 'telegram') {
    await platformPool.query(`UPDATE users SET telegram_chat_id = NULL WHERE id = $1`, [req.user.user_id]);
    await platformPool.query(`DELETE FROM user_telegram_accounts WHERE user_id = $1`, [req.user.user_id]);
  } else if (channel === 'whatsapp') {
    await platformPool.query(`UPDATE users SET whatsapp_phone = NULL, whatsapp_verified = false WHERE id = $1`, [req.user.user_id]);
  }
  res.json({ ok: true });
});

// Twilio WhatsApp webhook — incoming messages
router.post('/api/webhooks/whatsapp', async (req, res) => {
  res.sendStatus(200);
  try {
    const from = (req.body.From || '').replace('whatsapp:', '');
    const body = req.body.Body || '';
    if (!from || !body) return;

    // Find user by WhatsApp phone
    const { rows: [user] } = await platformPool.query(
      `SELECT id, tenant_id FROM users WHERE whatsapp_phone = $1 AND whatsapp_verified = true`, [from]
    );
    if (!user) return;

    // Store as note/intelligence — user sending WhatsApp messages to the bot
    console.log(`📱 WhatsApp from ${from}: ${body.slice(0, 100)}`);
  } catch (err) { console.error('WhatsApp webhook error:', err.message); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ECOSYSTEM MAP DATA
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/ecosystem', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tid = req.tenant_id;

    // 1. Regional signal density
    const { rows: signalsByRegion } = await db.query(`
      SELECT
        CASE
          WHEN c.country_code IN ('AU','NZ') OR c.geography ILIKE '%australia%' THEN 'AU'
          WHEN c.country_code IN ('SG','MY','ID','TH','VN','PH') OR c.geography ILIKE '%singapore%' OR c.geography ILIKE '%southeast%' THEN 'SG'
          WHEN c.country_code IN ('GB','UK','IE','DE','FR','NL') OR c.geography ILIKE '%united kingdom%' OR c.geography ILIKE '%london%' OR c.geography ILIKE '%europe%' THEN 'UK'
          WHEN c.country_code IN ('US','CA') OR c.geography ILIKE '%united states%' OR c.geography ILIKE '%america%' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        se.signal_type,
        COUNT(*) AS signal_count,
        COUNT(*) FILTER (WHERE se.detected_at > NOW() - INTERVAL '7 days') AS signals_7d,
        COUNT(*) FILTER (WHERE se.detected_at > NOW() - INTERVAL '30 days') AS signals_30d,
        AVG(se.confidence_score) AS avg_confidence
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '90 days'
      GROUP BY region, se.signal_type
      ORDER BY region, signal_count DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 2. Network density per region
    const { rows: density } = await db.query(`
      SELECT nds.region_code, gp.region_name, gp.weight_boost, gp.is_home_market,
             nds.total_contacts, nds.active_contacts, nds.senior_contacts,
             nds.placement_count, nds.client_count,
             nds.density_score, nds.depth_score, nds.recency_score
      FROM network_density_scores nds
      JOIN geo_priorities gp ON gp.region_code = nds.region_code
      WHERE nds.tenant_id = $1
      ORDER BY nds.density_score DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 3. Revenue by region (from Xero data only)
    const { rows: revenue } = await db.query(`
      SELECT
        CASE
          WHEN cv.currency = 'AUD' THEN 'AU'
          WHEN cv.currency = 'SGD' THEN 'SG'
          WHEN cv.currency = 'GBP' THEN 'UK'
          WHEN cv.currency = 'USD' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        COUNT(*) AS placement_count,
        COALESCE(SUM(cv.placement_fee), 0) AS total_revenue,
        COALESCE(SUM(cv.placement_fee) FILTER (WHERE cv.start_date > NOW() - INTERVAL '12 months'), 0) AS revenue_12m,
        COALESCE(SUM(cv.placement_fee) FILTER (WHERE cv.start_date > NOW() - INTERVAL '6 months'), 0) AS revenue_6m
      FROM conversions cv
      WHERE cv.tenant_id = $1 AND cv.source IN ('xero_export', 'xero', 'manual', 'myob_import') AND cv.placement_fee IS NOT NULL
      GROUP BY region
    `, [tid]).catch(() => ({ rows: [] }));

    // 4. Top companies per region with signal activity
    const { rows: topCompanies } = await db.query(`
      SELECT
        CASE
          WHEN c.country_code IN ('AU','NZ') OR c.geography ILIKE '%australia%' THEN 'AU'
          WHEN c.country_code IN ('SG','MY','ID','TH','VN','PH') OR c.geography ILIKE '%singapore%' THEN 'SG'
          WHEN c.country_code IN ('GB','UK','IE','DE','FR','NL') OR c.geography ILIKE '%united kingdom%' OR c.geography ILIKE '%london%' THEN 'UK'
          WHEN c.country_code IN ('US','CA') OR c.geography ILIKE '%united states%' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        c.id, c.name, c.is_client, c.sector,
        COUNT(se.id) AS signal_count,
        (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) AS contact_count,
        (SELECT COUNT(*) FROM team_proximity tp JOIN people p2 ON p2.id = tp.person_id AND p2.tenant_id = $1 WHERE tp.tenant_id = $1 AND p2.current_company_id = c.id) AS proximity_count
      FROM companies c
      JOIN signal_events se ON se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '90 days'
      WHERE c.tenant_id = $1
      GROUP BY region, c.id, c.name, c.is_client, c.sector
      ORDER BY region, signal_count DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 5. Converging themes (top 5 globally)
    const { rows: themes } = await db.query(`
      SELECT se.signal_type, COUNT(*) AS count, COUNT(DISTINCT se.company_id) AS companies,
             COUNT(DISTINCT se.company_id) FILTER (WHERE c.is_client = true) AS client_companies
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '30 days'
      GROUP BY se.signal_type
      ORDER BY count DESC LIMIT 8
    `, [tid]).catch(() => ({ rows: [] }));

    // 6. Case study coverage by geography
    const { rows: caseGeo } = await db.query(`
      SELECT geography, COUNT(*) AS count
      FROM case_studies
      WHERE tenant_id = $1 AND status != 'deleted' AND geography IS NOT NULL
      GROUP BY geography ORDER BY count DESC LIMIT 10
    `, [tid]).catch(() => ({ rows: [] }));

    // Structure signals by region
    const regionSignals = {};
    for (const r of signalsByRegion) {
      if (!regionSignals[r.region]) regionSignals[r.region] = { total: 0, signals_7d: 0, signals_30d: 0, types: {} };
      regionSignals[r.region].total += parseInt(r.signal_count);
      regionSignals[r.region].signals_7d += parseInt(r.signals_7d);
      regionSignals[r.region].signals_30d += parseInt(r.signals_30d);
      regionSignals[r.region].types[r.signal_type] = parseInt(r.signal_count);
    }

    // Structure companies by region (top 5 per region)
    const regionCompanies = {};
    for (const c of topCompanies) {
      if (!regionCompanies[c.region]) regionCompanies[c.region] = [];
      if (regionCompanies[c.region].length < 5) regionCompanies[c.region].push(c);
    }

    res.json({
      regions: {
        AMER: { lat: 37.77, lng: -95.0, name: 'Americas', color: '#B45309', flag: 'AMER' },
        EUR:  { lat: 50.85, lng: 4.35, name: 'Europe', color: '#2563EB', flag: 'EUR' },
        MENA: { lat: 25.20, lng: 55.27, name: 'Middle East & Africa', color: '#D4537E', flag: 'MENA' },
        ASIA: { lat: 13.75, lng: 100.52, name: 'Asia', color: '#6D28D9', flag: 'ASIA' },
        OCE:  { lat: -33.87, lng: 151.21, name: 'Oceania', color: '#0D7A50', flag: 'OCE' },
      },
      signals: regionSignals,
      density: density,
      revenue: revenue,
      companies: regionCompanies,
      themes: themes,
      case_studies_geo: caseGeo
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN: Per-User Data Operations
// ═══════════════════════════════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════════════════════════════
// XERO OAUTH 2.0
// ═══════════════════════════════════════════════════════════════════════════════

// Initiate OAuth flow — visit this in browser
// In-memory OAuth state store — maps state token to tenant_id (5 min TTL)
const xeroOAuthStates = new Map();
setInterval(() => { var now = Date.now(); for (var [k, v] of xeroOAuthStates) { if (now - v.at > 300000) xeroOAuthStates.delete(k); } }, 60000);

router.get('/api/xero/connect', authenticateToken, (req, res) => {
  if (!process.env.XERO_CLIENT_ID) return res.status(500).json({ error: 'XERO_CLIENT_ID not configured' });
  const state = crypto.randomBytes(16).toString('hex');
  // Store state → tenant mapping for callback validation
  xeroOAuthStates.set(state, { tenant_id: req.tenant_id, user_id: req.user.user_id, at: Date.now() });
  const authUrl = 'https://login.xero.com/identity/connect/authorize?' + new URLSearchParams({
    response_type: 'code',
    client_id: process.env.XERO_CLIENT_ID,
    redirect_uri: process.env.XERO_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/xero/callback`,
    scope: process.env.XERO_SCOPES || 'openid profile email accounting.transactions.read accounting.contacts.read offline_access',
    state
  });
  res.redirect(authUrl);
});

// OAuth callback — validates state parameter, then exchanges code for tokens
router.get('/api/xero/callback', async (req, res) => {
  // Validate OAuth state — prevents CSRF and tenant hijacking
  const stateData = xeroOAuthStates.get(req.query.state);
  if (!stateData) return res.status(403).send('Invalid or expired OAuth state. Please retry from Settings.');
  xeroOAuthStates.delete(req.query.state);
  const callbackTenantId = stateData.tenant_id;
  const { code } = req.query;
  if (!code) return res.status(400).send('Missing authorization code');

  try {
    const credentials = Buffer.from(
      `${process.env.XERO_CLIENT_ID}:${process.env.XERO_CLIENT_SECRET}`
    ).toString('base64');

    // Exchange code for tokens
    const tokenRes = await fetch('https://identity.xero.com/connect/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${credentials}`
      },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: process.env.XERO_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/xero/callback`
      })
    });

    if (!tokenRes.ok) {
      const err = await tokenRes.text();
      return res.status(400).send(`Token exchange failed: ${err}`);
    }

    const tokenData = await tokenRes.json();

    // Get connected tenants
    const tenantsRes = await fetch('https://api.xero.com/connections', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const tenants = await tenantsRes.json();

    if (!tenants.length) return res.status(400).send('No Xero organisations found');

    // Save tokens for each tenant (usually just one)
    const { saveTokens } = require('../scripts/sync_xero');
    for (const tenant of tenants) {
      await saveTokens(tokenData, tenant.tenantId, tenant.tenantName);
      console.log(`✅ Xero connected: ${tenant.tenantName} (${tenant.tenantId})`);
    }

    res.send(`
      <html><body style="font-family:system-ui;text-align:center;padding:60px">
        <h2>Xero Connected</h2>
        <p>Organisation: <strong>${tenants[0].tenantName}</strong></p>
        <p>You can close this window. Invoice sync will run automatically.</p>
        <p><a href="/">Return to dashboard</a></p>
      </body></html>
    `);
  } catch (err) {
    console.error('Xero OAuth error:', err);
    res.status(500).send('Xero connection failed: ' + err.message);
  }
});

// Check Xero connection status
router.get('/api/xero/status', authenticateToken, async (req, res) => {
  const db = new TenantDB(req.tenant_id);
  const tokens = await db.query('SELECT tenant_id, tenant_name, expires_at, updated_at FROM xero_tokens').catch(() => ({ rows: [] }));
  const sync = await db.query('SELECT * FROM xero_sync_state').catch(() => ({ rows: [] }));
  res.json({ connected: tokens.rows.length > 0, tenants: tokens.rows, sync: sync.rows });
});

// Manual sync trigger
router.post('/api/xero/sync', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { pipelineSyncXero } = require('../scripts/sync_xero');
    res.json({ message: 'Xero sync triggered' });
    pipelineSyncXero().catch(e => console.error('Xero sync error:', e.message));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CATCH-ALL — Serve static HTML pages (MUST be LAST)
// ═══════════════════════════════════════════════════════════════════════════════


// ─────────────────────────────────────────────────────────────────────────────

// PIPELINE SCHEDULER — DISABLED in web process
// Background jobs run in separate worker process (orchestrator.js)
// Pipeline status API still available via scheduler routes
try {
  const scheduler = require('../scripts/scheduler.js');
  scheduler.registerRoutes(router, authenticateToken);
  console.log('  ✅ Pipeline routes registered (scheduler runs in worker process)');
} catch(e) {
  console.log('  ⚠️  Pipeline routes skipped:', e.message);
}
// MCP ENDPOINT — Claude.ai remote MCP integration at POST /mcp
// ─────────────────────────────────────────────────────────────────────────────
try {
  const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
  const { createMcpServer } = require('../scripts/mcp_server.js');
  router.post('/mcp', async (req, res) => {
    const mcpServer = createMcpServer();
    const t = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    res.on('close', () => t.close());
    await mcpServer.connect(t);
    await t.handleRequest(req, res, req.body);
  });
  router.get('/mcp', (_req, res) => res.json({ service: 'mitchellake-mcp', tools: 11, status: 'ok' }));
} catch(e) {
  console.log('  ⚠️  MCP endpoint skipped:', e.message);
}
// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// OPPORTUNITIES (Search Briefs / Pipeline)
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/pipeline/board', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { owner, region, signal_type, sector } = req.query;
    let where = "d.tenant_id = $1 AND COALESCE(d.pipeline_stage, 'new') != 'archived'";
    const params = [req.tenant_id];
    let idx = 2;
    if (owner) { where += ` AND d.claimed_by = $${idx++}`; params.push(owner); }
    if (region) { where += ` AND c.geography ILIKE $${idx++}`; params.push(`%${region}%`); }
    if (signal_type) { where += ` AND d.signal_type = $${idx++}`; params.push(signal_type); }
    if (sector) { where += ` AND c.sector ILIKE $${idx++}`; params.push(`%${sector}%`); }

    // Group dispatches by company — one card per company with aggregated signals
    const { rows } = await db.query(`
      SELECT
        -- Use the "best" dispatch per company (highest confidence) as the card ID
        (array_agg(d.id ORDER BY COALESCE(se.confidence_score, 0.5) DESC))[1] AS id,
        d.company_name, d.company_id,
        -- Aggregate the pipeline stage: use the most advanced stage for this company
        (array_agg(d.pipeline_stage ORDER BY
          CASE d.pipeline_stage WHEN 'won' THEN 5 WHEN 'converted' THEN 4 WHEN 'actioned' THEN 3 WHEN 'claimed' THEN 2 ELSE 1 END DESC
        ))[1] AS pipeline_stage,
        -- Sum pipeline values across all dispatches for this company
        COALESCE(SUM(d.pipeline_value), 0) AS pipeline_value,
        -- Ownership: first claimer
        (array_agg(d.claimed_by ORDER BY d.claimed_at ASC NULLS LAST))[1] AS claimed_by,
        MIN(d.claimed_at) AS claimed_at,
        MIN(d.actioned_at) AS actioned_at,
        MIN(d.converted_at) AS converted_at,
        MIN(d.won_at) AS won_at,
        -- Best opportunity angle
        (array_agg(d.opportunity_angle ORDER BY COALESCE(se.confidence_score, 0.5) DESC) FILTER (WHERE d.opportunity_angle IS NOT NULL))[1] AS opportunity_angle,
        MAX(d.updated_at) AS updated_at,
        MIN(d.created_at) AS created_at,
        (array_agg(d.opportunity_id) FILTER (WHERE d.opportunity_id IS NOT NULL))[1] AS opportunity_id,
        -- Signal aggregation
        array_agg(DISTINCT d.signal_type) FILTER (WHERE d.signal_type IS NOT NULL) AS signal_types,
        COUNT(*) AS signal_count,
        MAX(se.confidence_score) AS confidence_score,
        -- Company context
        c.sector, c.geography, c.is_client,
        (SELECT COUNT(*) FROM people p WHERE p.current_company_id = d.company_id AND p.tenant_id = d.tenant_id) AS contact_count,
        (SELECT COUNT(DISTINCT tp.person_id) FROM team_proximity tp
         JOIN people p2 ON p2.id = tp.person_id AND p2.current_company_id = d.company_id AND p2.tenant_id = d.tenant_id
         WHERE tp.tenant_id = d.tenant_id AND tp.relationship_strength >= 0.25) AS prox_count,
        (SELECT json_agg(sub) FROM (
          SELECT u3.name, MAX(tp3.relationship_strength) AS strength
          FROM team_proximity tp3
          JOIN people p3 ON p3.id = tp3.person_id AND p3.current_company_id = d.company_id AND p3.tenant_id = d.tenant_id
          JOIN users u3 ON u3.id = tp3.team_member_id
          WHERE tp3.tenant_id = d.tenant_id AND tp3.relationship_strength >= 0.25
          GROUP BY u3.name ORDER BY MAX(tp3.relationship_strength) DESC LIMIT 3
        ) sub) AS top_connectors,
        -- Lead score: best signal type + client + network + confidence
        MAX(
          CASE d.signal_type
            WHEN 'strategic_hiring' THEN 40
            WHEN 'geographic_expansion' THEN 35
            WHEN 'capital_raising' THEN 35
            WHEN 'product_launch' THEN 25
            WHEN 'partnership' THEN 20
            WHEN 'leadership_change' THEN 15
            WHEN 'ma_activity' THEN 15
            WHEN 'restructuring' THEN 10
            WHEN 'layoffs' THEN 5
            ELSE 10
          END
        ) +
        CASE WHEN c.is_client = true THEN 100 ELSE 0 END +
        CASE WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = d.company_id AND p.tenant_id = d.tenant_id) > 0 THEN 50 ELSE 0 END +
        MAX(COALESCE(se.confidence_score, 0.5)) * 30
        AS lead_score
      FROM signal_dispatches d
      LEFT JOIN signal_events se ON se.id = d.signal_event_id
      LEFT JOIN companies c ON c.id = d.company_id
      WHERE ${where}
      GROUP BY d.company_id, d.company_name, c.sector, c.geography, c.is_client, d.tenant_id
      ORDER BY
        MAX(CASE d.pipeline_stage WHEN 'won' THEN 5 WHEN 'converted' THEN 4 WHEN 'actioned' THEN 3 WHEN 'claimed' THEN 2 ELSE 1 END) DESC,
        lead_score DESC,
        SUM(d.pipeline_value) DESC NULLS LAST,
        MAX(d.updated_at) DESC
    `, params);

    // Resolve owner names
    const ownerIds = [...new Set(rows.filter(r => r.claimed_by).map(r => r.claimed_by))];
    const ownerMap = new Map();
    if (ownerIds.length) {
      const { rows: owners } = await db.query(`SELECT id, name FROM users WHERE id = ANY($1)`, [ownerIds]);
      owners.forEach(o => ownerMap.set(o.id, o.name));
    }
    rows.forEach(r => { r.owner_name = ownerMap.get(r.claimed_by) || null; });

    const columns = {};
    const totals = {};
    for (const row of rows) {
      const stage = row.pipeline_stage || 'new';
      if (!columns[stage]) { columns[stage] = []; totals[stage] = 0; }
      columns[stage].push(row);
      totals[stage] += parseFloat(row.pipeline_value) || 0;
    }

    // Facets for filters (always unfiltered to show all options)
    const [facetRegions, facetTypes, facetSectors] = await Promise.all([
      db.query(`SELECT DISTINCT c.geography FROM signal_dispatches d JOIN companies c ON c.id = d.company_id WHERE d.tenant_id = $1 AND c.geography IS NOT NULL AND COALESCE(d.pipeline_stage,'new') != 'archived' ORDER BY c.geography`, [req.tenant_id]),
      db.query(`SELECT DISTINCT d.signal_type FROM signal_dispatches d WHERE d.tenant_id = $1 AND d.signal_type IS NOT NULL AND COALESCE(d.pipeline_stage,'new') != 'archived' ORDER BY d.signal_type`, [req.tenant_id]),
      db.query(`SELECT DISTINCT c.sector FROM signal_dispatches d JOIN companies c ON c.id = d.company_id WHERE d.tenant_id = $1 AND c.sector IS NOT NULL AND COALESCE(d.pipeline_stage,'new') != 'archived' ORDER BY c.sector`, [req.tenant_id]),
    ]);

    res.json({
      columns, totals, total: rows.length,
      facets: {
        regions: facetRegions.rows.map(r => r.geography),
        signal_types: facetTypes.rows.map(r => r.signal_type),
        sectors: facetSectors.rows.map(r => r.sector),
      }
    });
  } catch (err) {
    console.error('Pipeline board error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/pipeline/:id/stage', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { stage, pipeline_value } = req.body;
    const updates = ['pipeline_stage = $1', 'updated_at = NOW()'];
    const params = [stage];
    let idx = 2;

    // Set timestamp for the stage
    if (stage === 'claimed' && !req.body.skip_timestamp) { updates.push(`claimed_at = COALESCE(claimed_at, NOW()), claimed_by = COALESCE(claimed_by, $${idx})`); params.push(req.user.user_id); idx++; }
    if (stage === 'actioned') { updates.push('actioned_at = COALESCE(actioned_at, NOW())'); }
    if (stage === 'converted') { updates.push('converted_at = COALESCE(converted_at, NOW())'); }
    if (stage === 'won') { updates.push('won_at = COALESCE(won_at, NOW())'); }

    if (pipeline_value !== undefined) { updates.push(`pipeline_value = $${idx}`); params.push(pipeline_value); idx++; }

    params.push(req.params.id, req.tenant_id);
    const { rows } = await db.query(
      `UPDATE signal_dispatches SET ${updates.join(', ')} WHERE id = $${idx} AND tenant_id = $${idx + 1} RETURNING id, company_name, pipeline_stage, pipeline_value`,
      params
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });

    // Log activity
    await db.query(
      `INSERT INTO activities (tenant_id, user_id, user_name, activity_type, subject, dispatch_id, company_id, metadata, source)
       VALUES ($1, $2, $3, 'status_change', $4, $5, (SELECT company_id FROM signal_dispatches WHERE id = $5), $6, 'manual')`,
      [req.tenant_id, req.user.user_id, req.user.name,
       `${rows[0].company_name} → ${stage}` + (pipeline_value ? ` ($${Number(pipeline_value).toLocaleString()})` : ''),
       req.params.id, JSON.stringify({ stage, pipeline_value })]
    );

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/api/pipeline/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `DELETE FROM signal_dispatches WHERE id = $1 AND tenant_id = $2 RETURNING id`,
      [req.params.id, req.tenant_id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/pipeline/:id/value', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { pipeline_value } = req.body;
    const { rows } = await db.query(
      `UPDATE signal_dispatches SET pipeline_value = $1, updated_at = NOW()
       WHERE id = $2 AND tenant_id = $3 RETURNING id, company_name, pipeline_value`,
      [pipeline_value, req.params.id, req.tenant_id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// DELIVERY BOARD (Project Kanban)
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/delivery/board', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { user_id, location, q } = req.query;
    let extraWhere = '';
    const extraParams = [];
    let pIdx = 2;
    if (user_id) {
      extraWhere += ` AND (e.lead_partner_id = $${pIdx} OR EXISTS (SELECT 1 FROM project_members pm WHERE pm.engagement_id = e.id AND pm.user_id = $${pIdx}))`;
      extraParams.push(user_id); pIdx++;
    }
    if (location) {
      extraWhere += ` AND (e.name ILIKE $${pIdx} OR co.name ILIKE $${pIdx} OR ac.name ILIKE $${pIdx})`;
      extraParams.push(`%${location}%`); pIdx++;
    }
    if (q) {
      extraWhere += ` AND (e.name ILIKE $${pIdx} OR e.code ILIKE $${pIdx} OR ac.name ILIKE $${pIdx} OR co.name ILIKE $${pIdx} OR e.client_context ILIKE $${pIdx})`;
      extraParams.push(`%${q}%`); pIdx++;
    }

    const { rows } = await db.query(`
      SELECT e.id, e.name, e.code, e.status, e.priority,
             e.fee_amount, e.fee_type, e.currency,
             e.kick_off_date, e.target_completion_date,
             e.lead_partner_id, e.updated_at, e.created_at,
             ac.name AS client_name, co.name AS company_name, co.id AS company_id,
             u.name AS lead_name,
             (SELECT json_agg(json_build_object('user_id', pm.user_id, 'role', pm.role, 'name', pu.name))
              FROM project_members pm JOIN users pu ON pu.id = pm.user_id
              WHERE pm.engagement_id = e.id) AS team,
             (SELECT COUNT(*) FROM opportunities o WHERE o.project_id = e.id) AS opportunity_count,
             (SELECT json_agg(json_build_object('id', o.id, 'title', o.title, 'status', o.status,
               'candidates', (SELECT COUNT(*) FROM pipeline_contacts pc WHERE pc.search_id = o.id)))
              FROM opportunities o WHERE o.project_id = e.id) AS opportunities,
             (SELECT COUNT(*) FROM conversions cv WHERE cv.search_id IN (SELECT o2.id FROM opportunities o2 WHERE o2.project_id = e.id)) AS placements
      FROM engagements e
      LEFT JOIN accounts ac ON ac.id = e.client_id
      LEFT JOIN companies co ON co.id = ac.company_id
      LEFT JOIN users u ON u.id = e.lead_partner_id
      WHERE e.tenant_id = $1 ${extraWhere}
      ORDER BY e.updated_at DESC
    `, [req.tenant_id, ...extraParams]);

    const columns = {};
    for (const row of rows) {
      const status = row.status || 'active';
      if (!columns[status]) columns[status] = [];
      columns[status].push(row);
    }

    // Facets
    const { rows: users } = await db.query(`SELECT id, name FROM users WHERE tenant_id = $1 ORDER BY name`, [req.tenant_id]);

    res.json({ columns, total: rows.length, users });
  } catch (err) {
    console.error('Delivery board error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/delivery/:id/status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { status } = req.body;
    const { rows } = await db.query(
      `UPDATE engagements SET status = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3 RETURNING id, name, status`,
      [status, req.params.id, req.tenant_id]
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });

    await db.query(
      `INSERT INTO activities (tenant_id, user_id, user_name, activity_type, subject, engagement_id, source)
       VALUES ($1, $2, $3, 'status_change', $4, $5, 'manual')`,
      [req.tenant_id, req.user.user_id, req.user.name, `Project status → ${status}`, req.params.id]
    );

    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/delivery/:id/members', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { user_id, role = 'member' } = req.body;
    const { rows } = await db.query(
      `INSERT INTO project_members (engagement_id, user_id, role, tenant_id)
       VALUES ($1, $2, $3, $4) ON CONFLICT (engagement_id, user_id) DO UPDATE SET role = $3
       RETURNING *`,
      [req.params.id, user_id, role, req.tenant_id]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/api/delivery/:id/members/:userId', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(
      `DELETE FROM project_members WHERE engagement_id = $1 AND user_id = $2 AND tenant_id = $3`,
      [req.params.id, req.params.userId, req.tenant_id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ACTIVITIES (unified activity log with entity cascading)
// ═══════════════════════════════════════════════════════════════════════════════

router.post('/api/activities', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { activity_type, subject, description, opportunity_id, engagement_id, person_id, company_id, metadata } = req.body;
    const { rows } = await db.query(
      `INSERT INTO activities (tenant_id, user_id, user_name, activity_type, subject, description,
         opportunity_id, engagement_id, person_id, company_id, metadata, source)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'manual') RETURNING *`,
      [req.tenant_id, req.user.user_id, req.user.name, activity_type, subject, description,
       opportunity_id || null, engagement_id || null, person_id || null, company_id || null,
       JSON.stringify(metadata || {})]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/activities', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { opportunity_id, engagement_id, person_id, company_id, limit = 50 } = req.query;
    let where = 'a.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 2;
    if (opportunity_id) { where += ` AND a.opportunity_id = $${idx++}`; params.push(opportunity_id); }
    if (engagement_id) { where += ` AND a.engagement_id = $${idx++}`; params.push(engagement_id); }
    if (person_id) { where += ` AND a.person_id = $${idx++}`; params.push(person_id); }
    if (company_id) { where += ` AND a.company_id = $${idx++}`; params.push(company_id); }
    params.push(Math.min(parseInt(limit) || 50, 200));

    const { rows } = await db.query(`
      SELECT a.*, u.name AS actor_name
      FROM activities a LEFT JOIN users u ON u.id = a.user_id
      WHERE ${where} ORDER BY a.activity_at DESC LIMIT $${idx}
    `, params);
    res.json({ activities: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


router.get('/api/crm/connections', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT id, provider, display_name, sync_enabled, sync_direction, sync_interval_minutes,
              last_sync_at, last_sync_status, last_sync_stats, last_error, created_at
       FROM crm_connections WHERE tenant_id = $1 ORDER BY provider`,
      [req.tenant_id]
    );
    res.json({ connections: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/crm/connections', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { provider, display_name, auth_type, credentials, sync_direction, field_mappings } = req.body;
    const { rows } = await db.query(
      `INSERT INTO crm_connections (tenant_id, provider, display_name, auth_type, credentials_encrypted, sync_direction, field_mappings)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, provider, display_name`,
      [req.tenant_id, provider, display_name, auth_type || 'api_key',
       JSON.stringify(credentials || {}), sync_direction || 'bidirectional',
       JSON.stringify(field_mappings || {})]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/crm/connections/:id', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { sync_enabled, sync_direction, field_mappings, display_name } = req.body;
    const fields = []; const params = []; let idx = 1;
    if (sync_enabled !== undefined) { fields.push(`sync_enabled = $${idx++}`); params.push(sync_enabled); }
    if (sync_direction) { fields.push(`sync_direction = $${idx++}`); params.push(sync_direction); }
    if (field_mappings) { fields.push(`field_mappings = $${idx++}`); params.push(JSON.stringify(field_mappings)); }
    if (display_name) { fields.push(`display_name = $${idx++}`); params.push(display_name); }
    if (!fields.length) return res.status(400).json({ error: 'No fields' });
    fields.push('updated_at = NOW()');
    params.push(req.params.id, req.tenant_id);
    const { rows } = await db.query(
      `UPDATE crm_connections SET ${fields.join(', ')} WHERE id = $${idx} AND tenant_id = $${idx + 1} RETURNING *`,
      params
    );
    if (!rows.length) return res.status(404).json({ error: 'Not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/api/crm/connections/:id', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(`DELETE FROM crm_connections WHERE id = $1 AND tenant_id = $2`, [req.params.id, req.tenant_id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/crm/connections/:id/log', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT * FROM crm_sync_log WHERE connection_id = $1 AND tenant_id = $2 ORDER BY synced_at DESC LIMIT 100`,
      [req.params.id, req.tenant_id]
    );
    res.json({ log: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Webhook receiver (no auth — uses HMAC validation)
router.post('/api/webhooks/crm/:connectionId', async (req, res) => {
  try {
    const { rows: [conn] } = await platformPool.query(
      `SELECT * FROM crm_connections WHERE id = $1`, [req.params.connectionId]
    );
    if (!conn) return res.status(404).json({ error: 'Unknown connection' });

    // TODO: HMAC validation using conn.webhook_secret
    // For now, log the webhook payload
    await platformPool.query(
      `INSERT INTO crm_sync_log (connection_id, tenant_id, direction, entity_type, action, changes)
       VALUES ($1, $2, 'inbound', 'webhook', 'received', $3)`,
      [conn.id, conn.tenant_id, JSON.stringify(req.body)]
    );

    res.json({ received: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// NETWORK ANALYSIS & DAILY INSIGHTS
// ═══════════════════════════════════════════════════════════════════════════════

// Instant network topology — runs after Gmail connect or LinkedIn import
router.get('/api/insights/today', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT * FROM daily_insights
      WHERE tenant_id = $1 AND (user_id = $2 OR user_id IS NULL)
      AND insight_date >= CURRENT_DATE - 3
      ORDER BY insight_date DESC, generated_at DESC
      LIMIT 3
    `, [req.tenant_id, req.user.user_id]);

    // Mark as read
    if (rows.length && !rows[0].read_at) {
      await db.query('UPDATE daily_insights SET read_at = NOW() WHERE id = $1', [rows[0].id]).catch(() => {});
    }

    res.json({ insights: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// All insights history
router.get('/api/insights', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT id, insight_date, insight_type, headline, body, generated_at, read_at
      FROM daily_insights
      WHERE tenant_id = $1 AND (user_id = $2 OR user_id IS NULL)
      ORDER BY insight_date DESC
      LIMIT 30
    `, [req.tenant_id, req.user.user_id]);
    res.json({ insights: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// AUDIT LOG API
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/audit/my', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const { rows } = await db.query(`
      SELECT event_type, resource_type, resource_id, action, outcome, ip_address, created_at,
        CASE WHEN event_type IN ('signal_triaged') THEN jsonb_build_object('triage_action', metadata->>'triage_action')
             WHEN event_type IN ('bundle_subscribed') THEN jsonb_build_object('bundle_slug', metadata->>'bundle_slug')
             ELSE '{}'::jsonb END AS safe_metadata
      FROM audit_logs WHERE tenant_id = $1 AND user_id = $2
      ORDER BY created_at DESC LIMIT $3
    `, [req.tenant_id, req.user.user_id, limit]);
    res.json({ events: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/audit/tenant', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT al.event_type, al.resource_type, al.action, al.outcome,
             al.ip_address, al.created_at, al.user_email
      FROM audit_logs al WHERE al.tenant_id = $1
      ORDER BY al.created_at DESC LIMIT 500
    `, [req.tenant_id]);
    res.json({ events: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/audit/security-summary', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: summary } = await db.query(`
      SELECT event_type, outcome, COUNT(*) AS count, COUNT(DISTINCT ip_address) AS unique_ips, MAX(created_at) AS last_seen
      FROM audit_logs WHERE created_at > NOW() - INTERVAL '24 hours' AND outcome IN ('blocked','failed')
      AND ($1::uuid IS NULL OR tenant_id = $1)
      GROUP BY event_type, outcome ORDER BY count DESC
    `, [req.tenant_id === process.env.ML_TENANT_ID ? null : req.tenant_id]);
    const { rows: recent } = await db.query(`
      SELECT event_type, ip_address, user_email, created_at, failure_reason
      FROM audit_logs WHERE created_at > NOW() - INTERVAL '24 hours' AND outcome IN ('blocked','failed')
      AND ($1::uuid IS NULL OR tenant_id = $1)
      ORDER BY created_at DESC LIMIT 20
    `, [req.tenant_id === process.env.ML_TENANT_ID ? null : req.tenant_id]);
    res.json({ summary: summary, recent_failures: recent });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// COMPANY RELATIONSHIPS
// ═══════════════════════════════════════════════════════════════════════════════

router.post('/api/admin/compute-company-relationships', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { compute } = require('../scripts/compute_company_relationships');
    res.json({ message: 'Company relationship scoring triggered' });
    compute().catch(function(e) { console.error('Company rel compute error:', e.message); });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
router.get('/api/jobs/signals', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { signal_type, days, company_id, limit } = req.query;
    let where = ['se.tenant_id = $1', "se.source_url = 'jobs'"];
    const params = [req.tenant_id];
    let idx = 2;
    if (signal_type) { where.push(`se.signal_type = $${idx++}::signal_type`); params.push(signal_type); }
    if (company_id) { where.push(`se.company_id = $${idx++}`); params.push(company_id); }
    const dayWindow = parseInt(days) || 30;
    where.push(`se.detected_at > NOW() - ($${idx++} || ' days')::INTERVAL`);
    params.push(String(dayWindow));
    const lim = Math.min(parseInt(limit) || 50, 200);
    const { rows } = await db.query(`
      SELECT se.id, se.signal_type, se.company_id, se.company_name, se.confidence_score,
             se.evidence_summary, se.scoring_breakdown, se.detected_at, se.triage_status
      FROM signal_events se
      WHERE ${where.join(' AND ')}
      ORDER BY se.detected_at DESC
      LIMIT ${lim}
    `, params);
    res.json({ signals: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
router.get('/api/jobs/stats', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const stats = await db.queryOne(`
      SELECT
        COUNT(*) FILTER (WHERE status = 'active') AS total_active_postings,
        COUNT(DISTINCT company_id) FILTER (WHERE status = 'active') AS companies_with_active_postings
      FROM job_postings WHERE tenant_id = $1
    `, [req.tenant_id]);
    const atsCount = await db.queryOne(`
      SELECT COUNT(*) AS companies_with_ats FROM companies
      WHERE tenant_id = $1 AND ats_type IS NOT NULL
    `, [req.tenant_id]);
    const bySeniority = await db.queryAll(`
      SELECT seniority_level, COUNT(*) AS count
      FROM job_postings WHERE tenant_id = $1 AND status = 'active'
      GROUP BY seniority_level ORDER BY count DESC
    `, [req.tenant_id]);
    const signals7d = await db.queryOne(`
      SELECT COUNT(*) AS count FROM signal_events
      WHERE tenant_id = $1 AND source_url = 'jobs' AND detected_at > NOW() - INTERVAL '7 days'
    `, [req.tenant_id]);
    const topHiring = await db.queryAll(`
      SELECT company_name, COUNT(*) AS count
      FROM job_postings WHERE tenant_id = $1 AND status = 'active' AND company_name IS NOT NULL
      GROUP BY company_name ORDER BY count DESC LIMIT 10
    `, [req.tenant_id]);
    res.json({
      total_active_postings: parseInt(stats?.total_active_postings || 0),
      companies_with_active_postings: parseInt(stats?.companies_with_active_postings || 0),
      companies_with_ats: parseInt(atsCount?.companies_with_ats || 0),
      postings_by_seniority: bySeniority,
      signals_generated_7d: parseInt(signals7d?.count || 0),
      top_hiring_companies: topHiring,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/jobs/geo-signals — Active geographic expansion signals from job postings
router.get('/api/jobs/geo-signals', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { geography, geo_tier, days, limit } = req.query;
    let where = ['jp.tenant_id = $1', 'jp.is_geo_expansion_role = true', "jp.status = 'active'"];
    const params = [req.tenant_id];
    let idx = 2;
    if (geography) { where.push(`jp.target_geography = $${idx++}`); params.push(geography); }
    if (geo_tier) { where.push(`jp.target_geo_tier = $${idx++}`); params.push(geo_tier); }
    const dayWindow = parseInt(days) || 30;
    where.push(`jp.first_seen_at > NOW() - ($${idx++} || ' days')::INTERVAL`);
    params.push(String(dayWindow));
    const lim = Math.min(parseInt(limit) || 50, 200);
    const { rows } = await db.query(`
      SELECT jp.id, jp.title, jp.company_id, jp.company_name, jp.location,
             jp.seniority_level, jp.geo_role_class, jp.target_geography,
             jp.target_geo_tier, jp.apply_url, jp.first_seen_at, jp.days_open,
             c.sector AS company_sector
      FROM job_postings jp
      LEFT JOIN companies c ON c.id = jp.company_id
      WHERE ${where.join(' AND ')}
      ORDER BY jp.first_seen_at DESC
      LIMIT ${lim}
    `, params);
    res.json({ signals: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/jobs/geo-waves — Market-level geographic entry waves
router.get('/api/jobs/geo-waves', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { geography, min_companies } = req.query;
    const minCo = parseInt(min_companies) || 3;
    let havingExtra = '';
    const params = [req.tenant_id];
    let idx = 2;
    if (geography) { havingExtra = ` AND jp.target_geography = $${idx++}`; params.push(geography); }
    const { rows } = await db.query(`
      SELECT
        jp.target_geography,
        jp.target_geo_tier,
        COUNT(DISTINCT jp.company_id)   AS company_count,
        COUNT(*)                        AS posting_count,
        ARRAY_AGG(DISTINCT jp.geo_role_class) AS role_classes,
        ARRAY_AGG(DISTINCT c.sector) FILTER (WHERE c.sector IS NOT NULL) AS sectors,
        ARRAY_AGG(DISTINCT c.name) AS company_names,
        MIN(jp.first_seen_at) AS earliest_posting
      FROM job_postings jp
      JOIN companies c ON c.id = jp.company_id
      WHERE jp.tenant_id = $1
        AND jp.is_geo_expansion_role = true
        AND jp.status = 'active'
        AND jp.first_seen_at > NOW() - INTERVAL '30 days'
        AND jp.target_geography IS NOT NULL
        ${havingExtra}
      GROUP BY jp.target_geography, jp.target_geo_tier
      HAVING COUNT(DISTINCT jp.company_id) >= ${minCo}
      ORDER BY COUNT(DISTINCT jp.company_id) DESC
    `, params);

    const waves = rows.map(w => ({
      ...w,
      company_count: parseInt(w.company_count),
      posting_count: parseInt(w.posting_count),
      confidence: Math.min(0.70 + (parseInt(w.company_count) * 0.05), 0.97),
    }));
    res.json({ waves, total: waves.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/official-sources/stats — Official API source stats
router.get('/api/official-sources/stats', authenticateToken, async (req, res) => {
  try {
    const { rows: sources } = await platformPool.query(`
      SELECT source_key, name, region, category, enabled,
             last_fetched_at, total_fetched, total_signals,
             consecutive_errors, last_error, fetch_interval_minutes
      FROM official_api_sources ORDER BY category, region
    `);
    const totals = sources.reduce((acc, s) => ({
      total_fetched: acc.total_fetched + (s.total_fetched || 0),
      total_signals: acc.total_signals + (s.total_signals || 0),
      enabled: acc.enabled + (s.enabled ? 1 : 0),
    }), { total_fetched: 0, total_signals: 0, enabled: 0 });
    res.json({ sources, totals });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// INVESTOR ENRICHMENT
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/enrichment/investors — People tagged as investors, by fit score
router.get('/api/enrichment/investors', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { min_score, context, limit } = req.query;
    let where = ['p.tenant_id = $1', 'p.is_investor = true'];
    const params = [req.tenant_id];
    let idx = 2;
    if (min_score) { where.push(`p.investor_fit_score >= $${idx++}`); params.push(parseFloat(min_score)); }
    if (context) { where.push(`p.investor_fit_context ILIKE $${idx++}`); params.push(`%${context}%`); }
    const lim = Math.min(parseInt(limit) || 50, 200);
    const { rows } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name,
             p.linkedin_url, p.city, p.country,
             p.investor_fit_score, p.investor_fit_rationale, p.investor_fit_context,
             p.investor_type, p.enrichment_source
      FROM people p
      WHERE ${where.join(' AND ')}
      ORDER BY p.investor_fit_score DESC NULLS LAST
      LIMIT ${lim}
    `, params);
    res.json({ investors: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/enrichment/log — Enrichment log summary by action
router.get('/api/enrichment/log', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: summary } = await db.query(`
      SELECT action, COUNT(*) AS count, COUNT(DISTINCT person_id) AS people,
             MAX(created_at) AS last_at
      FROM enrichment_log WHERE tenant_id = $1
      GROUP BY action ORDER BY count DESC
    `, [req.tenant_id]);
    const { rows: docs } = await db.query(`
      SELECT id, filename, document_type, row_count, matched_count,
             enriched_count, skipped_count, status, processed_at
      FROM enrichment_documents WHERE tenant_id = $1
      ORDER BY created_at DESC LIMIT 20
    `, [req.tenant_id]);
    res.json({ summary, documents: docs });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/enrichment/template — Investor scoring template
router.get('/api/enrichment/template', authenticateToken, async (req, res) => {
  try {
    const { rows } = await platformPool.query(`
      SELECT id, name, version, source, criteria, notes, created_at
      FROM investor_scoring_templates ORDER BY created_at DESC LIMIT 1
    `);
    res.json(rows[0] || null);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// FEED CATALOG & BUNDLES (Platform-level curation + tenant subscriptions)
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/events', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { theme, region, format, from, to, search, limit = 20, offset = 0 } = req.query;
    const params = [req.tenant_id];
    const conditions = ['(e.tenant_id IS NULL OR e.tenant_id = $1)'];
    let idx = 2;

    if (theme) {
      const themes = theme.split(',').map(t => t.trim());
      conditions.push(`e.theme = ANY($${idx})`);
      params.push(themes);
      idx++;
    }
    if (region) {
      conditions.push(`e.region = $${idx}`);
      params.push(region);
      idx++;
    }
    if (format) {
      conditions.push(`e.format = $${idx}`);
      params.push(format);
      idx++;
    }
    if (from || !to) {
      conditions.push(`e.event_date >= $${idx}`);
      params.push(from || new Date().toISOString().slice(0, 10));
      idx++;
    }
    if (to) {
      conditions.push(`e.event_date <= $${idx}`);
      params.push(to);
      idx++;
    }
    if (search) {
      conditions.push(`(e.title ILIKE $${idx} OR e.description ILIKE $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }

    const lim = Math.min(parseInt(limit) || 20, 100);
    const off = parseInt(offset) || 0;

    const where = conditions.join(' AND ');

    const [eventsResult, countResult, themesResult, regionsResult] = await Promise.all([
      db.query(`
        SELECT e.id, e.title, e.theme, e.region, e.city, e.country, e.event_date,
               e.event_end_date, e.format, e.is_virtual, e.event_url, e.relevance_score,
               e.signal_relevance, e.speaker_names, e.description, e.external_id,
               e.published_at, e.organiser,
               COALESCE(
                 (SELECT json_agg(json_build_object('id', c.id, 'name', c.name, 'link_type', ecl.link_type))
                  FROM event_company_links ecl JOIN companies c ON c.id = ecl.company_id
                  WHERE ecl.event_id = e.id), '[]'
               ) AS company_links
        FROM events e
        WHERE ${where}
        ORDER BY e.event_date ASC NULLS LAST, e.relevance_score DESC
        LIMIT $${idx} OFFSET $${idx + 1}
      `, [...params, lim, off]),
      db.query(`SELECT COUNT(*) AS cnt FROM events e WHERE ${where}`, params),
      db.query(`SELECT DISTINCT theme FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND theme IS NOT NULL ORDER BY theme`, [req.tenant_id]),
      db.query(`SELECT DISTINCT region FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND region IS NOT NULL ORDER BY region`, [req.tenant_id]),
    ]);

    res.json({
      events: eventsResult.rows.map(e => ({
        ...e,
        description_excerpt: e.description ? e.description.slice(0, 200) + (e.description.length > 200 ? '...' : '') : null,
      })),
      total: parseInt(countResult.rows[0].cnt),
      themes: themesResult.rows.map(r => r.theme),
      regions: regionsResult.rows.map(r => r.region),
    });
  } catch (err) {
    console.error('Events list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch events' });
  }
});

router.get('/api/events/trends', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const [byTheme, byRegion, thisWeek] = await Promise.all([
      db.query(`
        SELECT theme, COUNT(*) AS count,
               COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 30) AS upcoming_30d
        FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND theme IS NOT NULL
        GROUP BY theme ORDER BY count DESC
      `, [req.tenant_id]),
      db.query(`
        SELECT region, COUNT(*) AS count,
               COUNT(*) FILTER (WHERE event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 30) AS upcoming_30d
        FROM events WHERE (tenant_id IS NULL OR tenant_id = $1) AND region IS NOT NULL
        GROUP BY region ORDER BY count DESC
      `, [req.tenant_id]),
      db.query(`
        SELECT id, title, theme, region, city, event_date, format, event_url, is_virtual
        FROM events
        WHERE tenant_id = $1 AND event_date >= CURRENT_DATE AND event_date <= CURRENT_DATE + 7
        ORDER BY event_date ASC LIMIT 10
      `, [req.tenant_id]),
    ]);

    const hotTheme = byTheme.rows.reduce((best, r) => parseInt(r.upcoming_30d) > (best.upcoming_30d || 0) ? { theme: r.theme, upcoming_30d: parseInt(r.upcoming_30d) } : best, {});
    const hotRegion = byRegion.rows.reduce((best, r) => parseInt(r.upcoming_30d) > (best.upcoming_30d || 0) ? { region: r.region, upcoming_30d: parseInt(r.upcoming_30d) } : best, {});

    res.json({
      by_theme: byTheme.rows.map(r => ({ theme: r.theme, count: parseInt(r.count), upcoming_30d: parseInt(r.upcoming_30d) })),
      by_region: byRegion.rows.map(r => ({ region: r.region, count: parseInt(r.count), upcoming_30d: parseInt(r.upcoming_30d) })),
      upcoming_this_week: thisWeek.rows,
      hottest_theme: hotTheme.theme || null,
      hottest_region: hotRegion.region || null,
    });
  } catch (err) {
    console.error('Events trends error:', err.message);
    res.status(500).json({ error: 'Failed to fetch event trends' });
  }
});

router.get('/api/events/for-search/:searchId', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const search = await db.query(
      `SELECT s.title, p.name AS project_name, s.target_industries, s.target_geography,
              s.must_have_keywords, s.brief_summary
       FROM searches s
       LEFT JOIN projects p ON p.id = s.project_id
       WHERE s.id = $1`,
      [req.params.searchId]
    );

    if (search.rows.length === 0) return res.status(404).json({ error: 'Search not found' });

    const s = search.rows[0];
    const industries = s.target_industries || [];
    const geographies = s.target_geography || [];

    // Map industries to event themes
    const themeMap = {
      fintech: 'FinTech', ai: 'AI', 'artificial intelligence': 'AI',
      cybersecurity: 'Cybersecurity', 'climate tech': 'Climate Tech',
      'clean tech': 'Climate Tech', cleantech: 'Climate Tech',
    };
    const themes = industries.map(i => themeMap[i.toLowerCase()] || null).filter(Boolean);

    // Build query
    const conditions = ['(e.tenant_id IS NULL OR e.tenant_id = $1)', 'e.event_date >= CURRENT_DATE'];
    const params = [req.tenant_id];
    let idx = 2;

    if (themes.length > 0) {
      conditions.push(`e.theme = ANY($${idx})`);
      params.push(themes);
      idx++;
    }
    if (geographies.length > 0) {
      const regionClauses = geographies.map((_, gi) => `e.region ILIKE $${idx + gi}`);
      conditions.push(`(${regionClauses.join(' OR ')})`);
      geographies.forEach(g => params.push(`%${g}%`));
      idx += geographies.length;
    }

    const { rows } = await db.query(`
      SELECT id, title, theme, region, city, event_date, format, event_url, is_virtual
      FROM events e
      WHERE ${conditions.join(' AND ')}
      ORDER BY event_date ASC
      LIMIT 5
    `, params);

    res.json({ events: rows, search_title: s.title });
  } catch (err) {
    console.error('Events for search error:', err.message);
    res.status(500).json({ error: 'Failed to fetch events for search' });
  }
});

router.get('/api/events/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT e.*,
        COALESCE(
          (SELECT json_agg(json_build_object('id', c.id, 'name', c.name, 'link_type', ecl.link_type))
           FROM event_company_links ecl JOIN companies c ON c.id = ecl.company_id
           WHERE ecl.event_id = e.id), '[]'
        ) AS company_links,
        COALESCE(
          (SELECT json_agg(json_build_object('id', p.id, 'name', p.full_name, 'role', epl.role))
           FROM event_person_links epl JOIN people p ON p.id = epl.person_id
           WHERE epl.event_id = e.id), '[]'
        ) AS person_links
      FROM events e
      WHERE e.id = $1 AND (e.tenant_id IS NULL OR e.tenant_id = $2)
    `, [req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Event not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Event detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch event' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN: EVENT SOURCES
// ═══════════════════════════════════════════════════════════════════════════════

router.get('/api/admin/event-sources', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT es.*,
        (SELECT COUNT(*) FROM events e WHERE e.source_id = es.id) AS event_count
       FROM event_sources es
       WHERE es.tenant_id = $1
       ORDER BY es.name`,
      [req.tenant_id]
    );
    res.json({ sources: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/admin/event-sources', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { name, feed_url, theme, region } = req.body;
    if (!name || !feed_url) return res.status(400).json({ error: 'name and feed_url required' });
    const { rows } = await db.query(
      `INSERT INTO event_sources (tenant_id, name, feed_url, theme, region)
       VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [req.tenant_id, name, feed_url, theme || null, region || null]
    );
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/admin/event-sources/:id', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { is_active, name, feed_url, theme, region } = req.body;
    const fields = [];
    const params = [];
    let idx = 1;
    if (is_active !== undefined) { fields.push(`is_active = $${idx++}`); params.push(is_active); }
    if (name !== undefined) { fields.push(`name = $${idx++}`); params.push(name); }
    if (feed_url !== undefined) { fields.push(`feed_url = $${idx++}`); params.push(feed_url); }
    if (theme !== undefined) { fields.push(`theme = $${idx++}`); params.push(theme); }
    if (region !== undefined) { fields.push(`region = $${idx++}`); params.push(region); }
    if (fields.length === 0) return res.status(400).json({ error: 'No fields to update' });
    fields.push(`updated_at = NOW()`);
    params.push(req.params.id, req.tenant_id);
    const { rows } = await db.query(
      `UPDATE event_sources SET ${fields.join(', ')} WHERE id = $${idx} AND tenant_id = $${idx + 1} RETURNING *`,
      params
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Source not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/admin/events/fetch/:sourceId', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(
      `SELECT * FROM event_sources WHERE id = $1 AND tenant_id = $2`,
      [req.params.sourceId, req.tenant_id]
    );
    if (rows.length === 0) return res.status(404).json({ error: 'Source not found' });

    // Trigger async harvest for this source
    const { harvestEvents } = require('../scripts/harvest_events');
    // Run in background
    harvestEvents().catch(err => console.error('On-demand event harvest error:', err.message));
    res.json({ message: 'Event harvest triggered', source: rows[0].name });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/dedup-companies — find and merge duplicate companies by normalised name
router.post('/api/admin/dedup-companies', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);

    // Fetch all companies
    const { rows: allCompanies } = await db.query('SELECT * FROM companies ORDER BY id');

    // Normalise company name for grouping
    function normaliseName(name) {
      if (!name) return '';
      return name
        .toLowerCase()
        .replace(/\b(pty\.?\s*ltd\.?|ltd\.?|inc\.?|llc\.?|australia|group|holdings?|corp\.?|corporation)\b/gi, '')
        .replace(/^the\s+/i, '')
        .replace(/[.,\-_()\[\]{}!@#$%^&*+=~`'"]+/g, '')
        .replace(/\s+/g, ' ')
        .trim();
    }

    // Group by normalised name
    var groups = {};
    for (var c of allCompanies) {
      var key = normaliseName(c.name);
      if (!key) continue;
      if (!groups[key]) groups[key] = [];
      groups[key].push(c);
    }

    var totalMerged = 0;
    var mergeDetails = [];

    for (var key in groups) {
      var group = groups[key];
      if (group.length < 2) continue;

      // Score each company: count of linked people + signal events
      var scored = [];
      for (var company of group) {
        var [peopleRes, signalsRes] = await Promise.all([
          db.query('SELECT COUNT(*)::int as cnt FROM people WHERE current_company_id = $1', [company.id]),
          db.query('SELECT COUNT(*)::int as cnt FROM signal_events WHERE company_id = $1', [company.id]),
        ]);
        scored.push({
          company: company,
          score: (peopleRes.rows[0].cnt || 0) + (signalsRes.rows[0].cnt || 0),
        });
      }

      // Keep the one with highest score
      scored.sort(function(a, b) { return b.score - a.score; });
      var keeper = scored[0].company;
      var dupes = scored.slice(1).map(function(s) { return s.company; });

      for (var dupe of dupes) {
        // Update foreign keys to point to keeper
        await db.query('UPDATE people SET current_company_id = $1 WHERE current_company_id = $2', [keeper.id, dupe.id]);
        await db.query('UPDATE signal_events SET company_id = $1 WHERE company_id = $2', [keeper.id, dupe.id]);
        await db.query('UPDATE accounts SET company_id = $1 WHERE company_id = $2', [keeper.id, dupe.id]);
        // Delete the duplicate
        await db.query('DELETE FROM companies WHERE id = $1', [dupe.id]);
        totalMerged++;
      }

      mergeDetails.push({
        kept: keeper.name,
        kept_id: keeper.id,
        merged: dupes.map(function(d) { return d.name; }),
      });
    }

    res.json({
      merged_count: totalMerged,
      groups_processed: mergeDetails.length,
      details: mergeDetails,
    });
  } catch (err) {
    console.error('Company dedup error:', err.message);
    res.status(500).json({ error: 'Failed to deduplicate companies' });
  }
});

router.post('/api/email/invite', authenticateToken, async (req, res) => {
  try {
    var { email, huddle_name, invite_url, inviter_name } = req.body;
    if (!email) return res.status(400).json({ error: 'email required' });

    await sendEmail(email,
      (inviter_name || 'Someone') + ' invited you to ' + (huddle_name || 'a huddle') + ' on Autonodal',
      '<div style="font-family:sans-serif;max-width:500px;margin:0 auto;padding:24px">' +
        '<h2 style="font-size:20px;margin-bottom:8px">You\'ve been invited to a huddle</h2>' +
        '<p style="color:#4a4a4a;line-height:1.6">' +
          '<strong>' + (inviter_name || 'A colleague') + '</strong> wants you to join ' +
          '<strong>' + (huddle_name || 'their huddle') + '</strong> on Autonodal — ' +
          'collaborative signal intelligence powered by your combined networks.</p>' +
        '<p style="margin:24px 0"><a href="' + (invite_url || 'https://www.autonodal.com') + '" ' +
          'style="background:#1a1a1a;color:#fff;padding:12px 28px;text-decoration:none;border-radius:6px;font-size:14px;font-weight:500">' +
          'Join Huddle</a></p>' +
        '<p style="color:#aaa;font-size:12px">Autonodal — Act when it matters</p>' +
      '</div>'
    );

    res.json({ sent: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to send invite' });
  }
});

// POST /api/email/share-signal — send signal intelligence to a team member
router.post('/api/email/share-signal', authenticateToken, async (req, res) => {
  try {
    var { to_email, company_name, signal_type, geography, proximity, evidence, url } = req.body;
    if (!to_email) return res.status(400).json({ error: 'to_email required' });
    var senderName = req.user.name || req.user.email;
    var typePretty = (signal_type || 'market signal').replace(/_/g, ' ');
    var subject = senderName + ' shared a signal: ' + (company_name || 'Company') + ' — ' + typePretty;

    var detailRows = '';
    if (signal_type) detailRows += '<tr><td style="color:#888;padding:4px 12px 4px 0;font-size:13px">Signal</td><td style="font-size:13px;font-weight:500">' + typePretty + '</td></tr>';
    if (geography) detailRows += '<tr><td style="color:#888;padding:4px 12px 4px 0;font-size:13px">Region</td><td style="font-size:13px">' + geography + '</td></tr>';
    if (proximity) detailRows += '<tr><td style="color:#888;padding:4px 12px 4px 0;font-size:13px">Proximity</td><td style="font-size:13px">' + proximity + '</td></tr>';

    var html = '<div style="font-family:system-ui,sans-serif;max-width:520px;margin:0 auto;padding:28px">' +
      '<div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#888;margin-bottom:16px">Signal Intelligence</div>' +
      '<h2 style="font-size:22px;margin:0 0 6px;font-weight:700">' + (company_name || 'Company') + '</h2>' +
      (evidence ? '<p style="color:#4a4a4a;font-size:14px;line-height:1.55;margin:0 0 16px">' + evidence + '</p>' : '') +
      (detailRows ? '<table style="border-collapse:collapse;margin-bottom:16px">' + detailRows + '</table>' : '') +
      '<a href="' + (url || 'https://www.autonodal.com') + '" style="display:inline-block;background:#1a1a1a;color:#fff;padding:10px 24px;text-decoration:none;border-radius:6px;font-size:13px;font-weight:600">View on Autonodal →</a>' +
      '<p style="color:#aaa;font-size:11px;margin-top:20px">Shared by ' + senderName + ' via Autonodal Signal Intelligence</p>' +
    '</div>';

    await sendEmail(to_email, subject, html);
    res.json({ sent: true });
  } catch (err) {
    res.status(500).json({ error: 'Failed to send: ' + err.message });
  }
});

// POST /api/email/test — send test email (admin only)
router.post('/api/email/test', authenticateToken, async (req, res) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  var target = req.body.email || req.user.email;
  var result = await sendEmail(target, 'Autonodal test email', '<p>Email delivery is working.</p>');
  res.json({ sent: !!result, to: target, resend_enabled: resendEnabled });
});

// ═══════════════════════════════════════════════════════════════════════════════
// RESEARCH — Publications Search (isolated from default search)
// ═══════════════════════════════════════════════════════════════════════════════

// GET /api/research/publications — dedicated publications search
// PLATFORM-CONTEXT: publications collection is global, not tenant-isolated
router.get('/api/research/publications', authenticateToken, async (req, res) => {
  try {
    const { q, limit = 20, min_score = 0.35 } = req.query;

    if (!q || q.trim().length < 3) {
      return res.status(400).json({ error: 'Query must be at least 3 characters' });
    }

    const cap = Math.min(parseInt(limit) || 20, 100);
    const embedding = await generateQueryEmbedding(q);

    // Use the research_search module (handles Qdrant directly for publications)
    const papers = await searchPublications(embedding, {
      limit: cap,
      scoreThreshold: parseFloat(min_score) || 0.35,
    });

    // Check if any authors match people in the tenant's sandbox
    if (papers.length > 0) {
      const allAuthors = papers.map(p => p.authors_full || p.authors).filter(Boolean);
      if (allAuthors.length > 0) {
        try {
          const db = new TenantDB(req.tenant_id);
          // Use last names for matching — more reliable than full name
          const lastNames = allAuthors.map(a => {
            const parts = a.split(/[,;]\s*/)[0].split(' ');
            return parts[parts.length - 1];
          }).filter(n => n && n.length > 2);

          if (lastNames.length > 0) {
            const { rows: matches } = await db.query(
              `SELECT full_name FROM people WHERE tenant_id = $1
               AND LOWER(SPLIT_PART(full_name, ' ', -1)) = ANY($2)
               LIMIT 50`,
              [req.tenant_id, lastNames.map(n => n.toLowerCase())]
            );
            if (matches.length > 0) {
              const matchedNames = new Set(matches.map(m => m.full_name.toLowerCase().split(' ').pop()));
              papers.forEach(paper => {
                const authorLast = (paper.authors_full || paper.authors || '').toLowerCase().split(/[,;]\s*/)[0].split(' ').pop();
                paper.has_network_match = matchedNames.has(authorLast);
              });
            }
          }
        } catch (e) { /* non-fatal — network matching is a bonus */ }
      }
    }

    res.json({
      query: q,
      total: papers.length,
      papers,
      network_matches: papers.filter(p => p.has_network_match).length,
      momentum: computeResearchMomentum(papers),
    });
  } catch (err) {
    console.error('Research search error:', err.message);
    res.status(500).json({ error: 'Research search failed' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ONBOARDING WIZARD API
// ═══════════════════════════════════════════════════════════════════════════════

  return router;
};
