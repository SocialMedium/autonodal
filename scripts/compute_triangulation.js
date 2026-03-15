#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// COMPUTE TRIANGULATION
// Bi-hourly pipeline that scores signal clusters against network topology:
//   signal_importance × network_overlap × geo_relevance ×
//   placement_adjacency × thematic_relevance × convergence_bonus
// Outputs ranked_opportunities with explainable scores
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const { Pool } = require('pg');
const { GeoMapper } = require('../lib/geo_mapper');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);

// Signal type importance weights
const TYPE_WEIGHTS = {
  geographic_expansion: 0.90,
  strategic_hiring: 0.85,
  leadership_change: 0.80,
  capital_raising: 0.75,
  ma_activity: 0.70,
  restructuring: 0.65,
  layoffs: 0.60,
  partnership: 0.55,
  product_launch: 0.50,
};

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1: GROUP SIGNALS BY COMPANY
// ═══════════════════════════════════════════════════════════════════════════════

async function getSignalClusters() {
  const { rows } = await pool.query(`
    SELECT
      COALESCE(se.company_name, c.name, 'Unknown') AS company_name,
      se.company_id,
      array_agg(se.id) AS signal_ids,
      array_agg(DISTINCT se.signal_type::text) AS signal_types,
      MAX(se.confidence_score) AS max_confidence,
      AVG(se.confidence_score) AS avg_confidence,
      MAX(se.detected_at) AS latest_signal,
      COUNT(*) AS signal_count,
      MAX(se.evidence_summary) AS latest_summary,
      c.sector, c.geography, c.country_code, c.is_client
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    WHERE se.detected_at > NOW() - INTERVAL '90 days'
      AND (se.triage_status IS NULL OR se.triage_status NOT IN ('ignore','irrelevant'))
      AND se.company_name IS NOT NULL
    GROUP BY COALESCE(se.company_name, c.name, 'Unknown'), se.company_id,
             c.sector, c.geography, c.country_code, c.is_client
    ORDER BY MAX(se.confidence_score) DESC
    LIMIT 1000
  `);
  return rows;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2: SCORE EACH CLUSTER
// ═══════════════════════════════════════════════════════════════════════════════

async function scoreCluster(cluster, densityMap, geoMapper) {
  const coKey = cluster.company_name.toLowerCase().trim();
  const reasons = [];

  // ── Signal Importance (0-100) ──
  const bestTypeWeight = Math.max(...cluster.signal_types.map(t => TYPE_WEIGHTS[t] || 0.3));
  const signalImportance = Math.round(cluster.max_confidence * bestTypeWeight * 100);
  const bestType = cluster.signal_types.reduce((best, t) =>
    (TYPE_WEIGHTS[t] || 0) > (TYPE_WEIGHTS[best] || 0) ? t : best, cluster.signal_types[0]);
  reasons.push(`${bestType.replace(/_/g, ' ')} signal (${Math.round(cluster.max_confidence * 100)}% confidence)`);

  // ── Network Overlap (0-100) ──
  const { rows: [adj] } = await pool.query(
    `SELECT * FROM company_adjacency_scores WHERE LOWER(TRIM(company_name)) = $1`, [coKey]
  ).catch(() => ({ rows: [null] }));

  const networkOverlap = adj?.adjacency_score ? parseFloat(adj.adjacency_score) : 0;
  if (adj?.is_client) reasons.push('Existing client');
  if (adj?.contact_count > 0) reasons.push(`${adj.contact_count} contacts (${adj.senior_contact_count} senior, ${adj.active_contact_count} active)`);
  if (adj?.placement_count > 0) reasons.push(`${adj.placement_count} past placement${adj.placement_count > 1 ? 's' : ''}`);

  // ── Geo Relevance (0-100) ──
  let regionCode = adj?.derived_region_code || 'UNKNOWN';
  if (regionCode === 'UNKNOWN') {
    // Try from company geography/country_code
    const resolved = geoMapper.resolveCompanyGeo(cluster.geography, cluster.country_code);
    if (resolved.region_code !== 'UNKNOWN') regionCode = resolved.region_code;
    // Try from evidence text
    if (regionCode === 'UNKNOWN' && cluster.latest_summary) {
      const fromText = geoMapper.resolveFromText(cluster.latest_summary);
      if (fromText.region_code !== 'UNKNOWN') regionCode = fromText.region_code;
    }
  }

  const density = densityMap.get(regionCode);
  const priority = geoMapper.getPriority(regionCode);
  const baseDensity = density ? parseFloat(density.density_score) : 0;
  const boost = priority?.weight_boost || 0;
  const geoRelevance = Math.min(100, baseDensity + boost);

  if (priority?.is_home_market) reasons.push(`${priority.region_name} (home market, +${boost} boost)`);
  else if (boost > 0) reasons.push(`${priority?.region_name || regionCode} (+${boost} geo boost)`);

  // ── Placement Adjacency (0-100) ──
  const placementAdj = Math.min(100,
    (adj?.placement_count || 0) * 20 +
    (adj?.is_client ? 30 : 0) +
    (adj?.client_tier === 'platinum' ? 10 : adj?.client_tier === 'gold' ? 5 : 0)
  );

  // ── Thematic Relevance (0-100) ──
  const thematic = adj?.active_search_count > 0 ? 80 :
    (density ? parseFloat(density.density_score) * 0.5 : 0);
  if (adj?.active_search_count > 0) reasons.push(`${adj.active_search_count} active search${adj.active_search_count > 1 ? 'es' : ''} targeting this company`);

  // ── Convergence Bonus (0-100) ──
  const uniqueTypes = new Set(cluster.signal_types).size;
  const signalCount = parseInt(cluster.signal_count);
  let convergence = 0;
  if (uniqueTypes >= 3) convergence = 100;
  else if (uniqueTypes === 2) convergence = 50;
  convergence = convergence * Math.min(signalCount / 5, 1);
  if (uniqueTypes >= 2) reasons.push(`${signalCount} signals converging (${cluster.signal_types.join(', ').replace(/_/g, ' ')})`);

  // ── Composite Score ──
  const raw = (
    signalImportance * 0.25 +
    networkOverlap * 0.25 +
    geoRelevance * 0.15 +
    placementAdj * 0.15 +
    thematic * 0.10 +
    convergence * 0.10
  );

  // Recency decay: 14-day half-life
  const daysOld = (Date.now() - new Date(cluster.latest_signal)) / (1000 * 60 * 60 * 24);
  const decay = Math.pow(0.5, daysOld / 14);
  const composite = Math.round(raw * decay * 100) / 100;

  // Build recommended action
  let action = '';
  if (adj?.warmest_contact_name && adj?.best_connection_user_name) {
    action = `Warm intro via ${adj.best_connection_user_name} to ${adj.warmest_contact_name}`;
  } else if (adj?.warmest_contact_name) {
    action = `Reach out to ${adj.warmest_contact_name}`;
  } else if (adj?.is_client) {
    action = 'Engage existing client relationship';
  } else {
    action = 'Research and identify entry point';
  }

  // Build summary
  const summary = reasons.length > 0 ? reasons.join('. ') + '.' : 'Signal detected.';

  return {
    company_name: cluster.company_name,
    company_id: cluster.company_id,
    region_code: regionCode,
    sector: cluster.sector || adj?.derived_sector || null,
    signal_importance: signalImportance,
    network_overlap: networkOverlap,
    geo_relevance: geoRelevance,
    placement_adjacency: placementAdj,
    thematic_relevance: Math.round(thematic * 100) / 100,
    convergence_bonus: Math.round(convergence * 100) / 100,
    composite_score: composite,
    decay_factor: Math.round(decay * 1000) / 1000,
    score_explanation: {
      summary,
      components: {
        signal_importance: { score: signalImportance, reason: `${bestType.replace(/_/g, ' ')} (${Math.round(cluster.max_confidence * 100)}% conf)` },
        network_overlap: { score: networkOverlap, reason: adj ? `${adj.contact_count} contacts, ${adj.senior_contact_count} senior` : 'No contacts in network' },
        geo_relevance: { score: geoRelevance, reason: `${priority?.region_name || regionCode} (density ${baseDensity.toFixed(0)} + boost ${boost})` },
        placement_adjacency: { score: placementAdj, reason: `${adj?.placement_count || 0} placements, ${adj?.is_client ? 'client' : 'non-client'}` },
        thematic_relevance: { score: thematic, reason: adj?.active_search_count > 0 ? `${adj.active_search_count} active searches` : 'No direct search match' },
        convergence_bonus: { score: convergence, reason: `${uniqueTypes} signal type${uniqueTypes > 1 ? 's' : ''}, ${signalCount} total` }
      },
      decay: { days_old: Math.round(daysOld), factor: Math.round(decay * 100) / 100 },
      recommended_action: action,
      reasons
    },
    signal_summary: summary,
    recommended_action: action,
    signal_event_ids: cluster.signal_ids,
    signal_count: signalCount,
    signal_types: cluster.signal_types,
    strongest_signal_type: bestType,
    warmest_contact_id: adj?.warmest_contact_id || null,
    warmest_contact_name: adj?.warmest_contact_name || null,
    best_connection_user_id: adj?.best_connection_user_id || null,
    best_connection_user_name: adj?.best_connection_user_name || null,
    latest_signal_date: cluster.latest_signal,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3: ANTI-VOLUME-BIAS QUOTAS
// ═══════════════════════════════════════════════════════════════════════════════

function assignRegionRanks(opportunities, densityMap) {
  // Group by region
  const byRegion = new Map();
  for (const opp of opportunities) {
    const r = opp.region_code || 'UNKNOWN';
    if (!byRegion.has(r)) byRegion.set(r, []);
    byRegion.get(r).push(opp);
  }

  // Sort within each region by composite_score DESC
  for (const [region, opps] of byRegion) {
    opps.sort((a, b) => b.composite_score - a.composite_score);
    opps.forEach((opp, i) => { opp.rank_in_region = i + 1; });
  }

  // Log distribution
  for (const [region, opps] of byRegion) {
    const density = densityMap.get(region);
    LOG('📍', `  ${region}: ${opps.length} opportunities (density: ${density?.density_score || 0})`);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 4: PERSIST
// ═══════════════════════════════════════════════════════════════════════════════

async function persistOpportunities(opportunities) {
  let upserted = 0;
  for (const opp of opportunities) {
    await pool.query(`
      INSERT INTO ranked_opportunities (
        company_id, company_name, signal_importance, network_overlap,
        geo_relevance, placement_adjacency, thematic_relevance, convergence_bonus,
        composite_score, rank_in_region, region_code, sector,
        score_explanation, signal_summary, recommended_action,
        signal_event_ids, signal_count, signal_types, strongest_signal_type,
        warmest_contact_id, warmest_contact_name,
        best_connection_user_id, best_connection_user_name,
        latest_signal_date, decay_factor, status, computed_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,'active',NOW())
      ON CONFLICT (LOWER(TRIM(company_name))) DO UPDATE SET
        company_id = COALESCE(EXCLUDED.company_id, ranked_opportunities.company_id),
        signal_importance = EXCLUDED.signal_importance,
        network_overlap = EXCLUDED.network_overlap,
        geo_relevance = EXCLUDED.geo_relevance,
        placement_adjacency = EXCLUDED.placement_adjacency,
        thematic_relevance = EXCLUDED.thematic_relevance,
        convergence_bonus = EXCLUDED.convergence_bonus,
        composite_score = EXCLUDED.composite_score,
        rank_in_region = EXCLUDED.rank_in_region,
        region_code = EXCLUDED.region_code,
        sector = EXCLUDED.sector,
        score_explanation = EXCLUDED.score_explanation,
        signal_summary = EXCLUDED.signal_summary,
        recommended_action = EXCLUDED.recommended_action,
        signal_event_ids = EXCLUDED.signal_event_ids,
        signal_count = EXCLUDED.signal_count,
        signal_types = EXCLUDED.signal_types,
        strongest_signal_type = EXCLUDED.strongest_signal_type,
        warmest_contact_id = EXCLUDED.warmest_contact_id,
        warmest_contact_name = EXCLUDED.warmest_contact_name,
        best_connection_user_id = EXCLUDED.best_connection_user_id,
        best_connection_user_name = EXCLUDED.best_connection_user_name,
        latest_signal_date = EXCLUDED.latest_signal_date,
        decay_factor = EXCLUDED.decay_factor,
        status = 'active',
        computed_at = NOW()
    `, [
      opp.company_id, opp.company_name,
      opp.signal_importance, opp.network_overlap,
      opp.geo_relevance, opp.placement_adjacency,
      opp.thematic_relevance, opp.convergence_bonus,
      opp.composite_score, opp.rank_in_region,
      opp.region_code, opp.sector,
      JSON.stringify(opp.score_explanation), opp.signal_summary, opp.recommended_action,
      opp.signal_event_ids, opp.signal_count, opp.signal_types, opp.strongest_signal_type,
      opp.warmest_contact_id, opp.warmest_contact_name,
      opp.best_connection_user_id, opp.best_connection_user_name,
      opp.latest_signal_date, opp.decay_factor
    ]);
    upserted++;
  }
  return upserted;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function computeTriangulation() {
  LOG('🔺', '═══ Triangulation Engine ═══');
  const start = Date.now();

  const geoMapper = new GeoMapper();
  await geoMapper.load(pool);

  // Load density map
  const { rows: densityRows } = await pool.query(
    `SELECT region_code, density_score FROM network_density_scores WHERE sector IS NULL`
  );
  const densityMap = new Map(densityRows.map(r => [r.region_code, r]));

  // Step 1: Get signal clusters
  const clusters = await getSignalClusters();
  LOG('📡', `Found ${clusters.length} company-signal clusters (90-day window)`);

  if (clusters.length === 0) {
    LOG('✅', 'No signals to triangulate');
    return { clusters: 0, opportunities: 0 };
  }

  // Step 2: Score each cluster
  const opportunities = [];
  for (const cluster of clusters) {
    try {
      const scored = await scoreCluster(cluster, densityMap, geoMapper);
      opportunities.push(scored);
    } catch (e) {
      LOG('⚠️', `  Failed to score ${cluster.company_name}: ${e.message}`);
    }
  }

  LOG('📊', `Scored ${opportunities.length} opportunities`);

  // Step 3: Assign region ranks
  assignRegionRanks(opportunities, densityMap);

  // Step 4: Persist
  const upserted = await persistOpportunities(opportunities);

  // Expire old opportunities
  const { rowCount: expired } = await pool.query(`
    UPDATE ranked_opportunities SET status = 'expired'
    WHERE status = 'active' AND computed_at < NOW() - INTERVAL '1 hour'
  `);

  const duration = ((Date.now() - start) / 1000).toFixed(1);
  LOG('🔺', `═══ Complete in ${duration}s: ${upserted} opportunities ranked, ${expired} expired ═══`);

  // Log top 10
  const top = opportunities.sort((a, b) => b.composite_score - a.composite_score).slice(0, 10);
  LOG('🏆', 'Top 10 opportunities:');
  top.forEach((o, i) => {
    LOG('  ', `${i + 1}. ${o.company_name} (${o.region_code || '??'}) — score: ${o.composite_score.toFixed(1)} | ${o.score_explanation.summary.slice(0, 100)}`);
  });

  return { clusters: clusters.length, opportunities: upserted, expired, duration_s: parseFloat(duration) };
}

if (require.main === module) {
  computeTriangulation()
    .then(r => { console.log('\nResult:', JSON.stringify(r, null, 2)); process.exit(0); })
    .catch(e => { console.error('Fatal:', e); process.exit(1); });
}

module.exports = { computeTriangulation };
