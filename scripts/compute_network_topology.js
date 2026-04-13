#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// COMPUTE NETWORK TOPOLOGY
// Nightly pipeline that pre-computes:
//   A. Company adjacency scores (who we know at each company)
//   B. Region derivation for companies (from people's locations)
//   C. Network density scores by region × sector
//   + Backfills company_id and geography where missing
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

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE A: COMPANY ADJACENCY SCORES
// ═══════════════════════════════════════════════════════════════════════════════

async function computeCompanyAdjacency() {
  LOG('🏢', 'Phase A: Computing company adjacency scores...');

  // Pre-compute active people (interacted in last 180 days) in one pass
  const { rows: activePeople } = await pool.query(`
    SELECT DISTINCT person_id FROM interactions WHERE interaction_at > NOW() - INTERVAL '180 days'
  `);
  const activeSet = new Set(activePeople.map(r => r.person_id));

  // Pre-compute warmest contact and best user per company in one pass
  const { rows: warmestData } = await pool.query(`
    SELECT DISTINCT ON (LOWER(TRIM(p.current_company_name)))
      LOWER(TRIM(p.current_company_name)) AS co_key,
      p.id AS warmest_id, p.full_name AS warmest_name
    FROM people p
    JOIN interactions i ON i.person_id = p.id
    WHERE p.current_company_name IS NOT NULL AND TRIM(p.current_company_name) != ''
    ORDER BY LOWER(TRIM(p.current_company_name)), i.interaction_at DESC
  `);
  const warmestMap = new Map(warmestData.map(r => [r.co_key, r]));

  const { rows: bestUserData } = await pool.query(`
    SELECT DISTINCT ON (co_key) co_key, user_id AS best_user_id FROM (
      SELECT LOWER(TRIM(p.current_company_name)) AS co_key, i.user_id, COUNT(*) AS cnt
      FROM interactions i JOIN people p ON p.id = i.person_id
      WHERE p.current_company_name IS NOT NULL AND i.user_id IS NOT NULL
      GROUP BY LOWER(TRIM(p.current_company_name)), i.user_id
    ) sub ORDER BY co_key, cnt DESC
  `);
  const bestUserMap = new Map(bestUserData.map(r => [r.co_key, r.best_user_id]));

  // Main company aggregation — no correlated subqueries
  const { rows: companies } = await pool.query(`
    SELECT
      LOWER(TRIM(p.current_company_name)) AS co_key,
      MIN(p.current_company_name) AS co_name,
      COUNT(DISTINCT p.id) AS contact_count,
      COUNT(DISTINCT p.id) FILTER (
        WHERE p.seniority_level IN ('c_suite','vp','director')
      ) AS senior_count,
      ARRAY_AGG(DISTINCT p.id) AS person_ids
    FROM people p
    WHERE p.current_company_name IS NOT NULL
      AND TRIM(p.current_company_name) != ''
    GROUP BY LOWER(TRIM(p.current_company_name))
    HAVING COUNT(DISTINCT p.id) >= 1
  `);

  // Compute active_count from pre-built set
  for (const co of companies) {
    co.active_count = (co.person_ids || []).filter(id => activeSet.has(id)).length;
    const w = warmestMap.get(co.co_key);
    co.warmest_id = w?.warmest_id || null;
    co.warmest_name = w?.warmest_name || null;
    co.best_user_id = bestUserMap.get(co.co_key) || null;
  }

  LOG('📊', `  Found ${companies.length} companies with contacts`);

  // Get placement counts per client name
  const { rows: placementData } = await pool.query(`
    SELECT LOWER(TRIM(cl.name)) AS co_key, COUNT(*) AS cnt
    FROM conversions pl JOIN accounts cl ON cl.id = pl.client_id
    GROUP BY LOWER(TRIM(cl.name))
  `);
  const placementMap = new Map(placementData.map(r => [r.co_key, parseInt(r.cnt)]));

  // Get client status per company name
  const { rows: clientData } = await pool.query(`
    SELECT LOWER(TRIM(cl.name)) AS co_key, cl.relationship_tier,
           cl.relationship_status, cl.company_id
    FROM accounts cl
  `);
  const clientMap = new Map(clientData.map(r => [r.co_key, r]));

  // Get active searches targeting companies
  const { rows: searchData } = await pool.query(`
    SELECT LOWER(TRIM(UNNEST(target_companies))) AS co_key, COUNT(*) AS cnt
    FROM opportunities
    WHERE status NOT IN ('placed','cancelled','on_hold')
      AND target_companies IS NOT NULL
    GROUP BY LOWER(TRIM(UNNEST(target_companies)))
  `);
  const searchMap = new Map(searchData.map(r => [r.co_key, parseInt(r.cnt)]));

  // Get company IDs for linking
  const { rows: companyIds } = await pool.query(`
    SELECT id, LOWER(TRIM(name)) AS co_key FROM companies
  `);
  const companyIdMap = new Map(companyIds.map(r => [r.co_key, r.id]));

  // Get best connection user names
  const { rows: userNames } = await pool.query(`SELECT id, name FROM users`);
  const userNameMap = new Map(userNames.map(r => [r.id, r.name]));

  let upserted = 0;
  for (const co of companies) {
    const placements = placementMap.get(co.co_key) || 0;
    const client = clientMap.get(co.co_key);
    const searches = searchMap.get(co.co_key) || 0;
    const companyId = companyIdMap.get(co.co_key) || null;
    const isClient = !!client && client.relationship_status !== 'inactive';
    const clientTier = client?.relationship_tier || null;
    const contacts = parseInt(co.contact_count);
    const senior = parseInt(co.senior_count);
    const active = parseInt(co.active_count);

    // Adjacency score formula (0-100)
    const score = Math.min(100,
      Math.min(contacts / 20, 1) * 25 +
      Math.min(senior / 5, 1) * 20 +
      Math.min(active / 10, 1) * 20 +
      (isClient ? 15 : 0) +
      Math.min(placements / 3, 1) * 15 +
      Math.min(searches, 1) * 5
    );

    const breakdown = {
      contacts: { count: contacts, contribution: Math.min(contacts / 20, 1) * 25 },
      senior: { count: senior, contribution: Math.min(senior / 5, 1) * 20 },
      active: { count: active, contribution: Math.min(active / 10, 1) * 20 },
      client: { is_client: isClient, tier: clientTier, contribution: isClient ? 15 : 0 },
      placements: { count: placements, contribution: Math.min(placements / 3, 1) * 15 },
      searches: { count: searches, contribution: Math.min(searches, 1) * 5 }
    };

    const bestUserName = co.best_user_id ? userNameMap.get(co.best_user_id) || null : null;

    await pool.query(`
      INSERT INTO company_adjacency_scores (
        company_id, company_name, contact_count, senior_contact_count, active_contact_count,
        placement_count, active_search_count, is_client, client_tier,
        warmest_contact_id, warmest_contact_name,
        best_connection_user_id, best_connection_user_name,
        adjacency_score, score_breakdown, computed_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW())
      ON CONFLICT (LOWER(TRIM(company_name))) DO UPDATE SET
        company_id = COALESCE(EXCLUDED.company_id, company_adjacency_scores.company_id),
        contact_count = EXCLUDED.contact_count,
        senior_contact_count = EXCLUDED.senior_contact_count,
        active_contact_count = EXCLUDED.active_contact_count,
        placement_count = EXCLUDED.placement_count,
        active_search_count = EXCLUDED.active_search_count,
        is_client = EXCLUDED.is_client,
        client_tier = EXCLUDED.client_tier,
        warmest_contact_id = EXCLUDED.warmest_contact_id,
        warmest_contact_name = EXCLUDED.warmest_contact_name,
        best_connection_user_id = EXCLUDED.best_connection_user_id,
        best_connection_user_name = EXCLUDED.best_connection_user_name,
        adjacency_score = EXCLUDED.adjacency_score,
        score_breakdown = EXCLUDED.score_breakdown,
        computed_at = NOW()
    `, [
      companyId, co.co_name, contacts, senior, active,
      placements, searches, isClient, clientTier,
      co.warmest_id, co.warmest_name,
      co.best_user_id, bestUserName,
      Math.round(score * 100) / 100, JSON.stringify(breakdown)
    ]);
    upserted++;
  }

  LOG('✅', `  Upserted ${upserted} company adjacency scores`);
  return upserted;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE B: DERIVE REGIONS FOR COMPANIES
// ═══════════════════════════════════════════════════════════════════════════════

async function deriveCompanyRegions(geoMapper) {
  LOG('🌐', 'Phase B: Deriving company regions from people locations...');

  // Get all people with location data, grouped by company
  const { rows: people } = await pool.query(`
    SELECT current_company_name, country_code, location, country
    FROM people
    WHERE current_company_name IS NOT NULL AND TRIM(current_company_name) != ''
      AND (country_code IS NOT NULL OR location IS NOT NULL OR country IS NOT NULL)
  `);

  // Count region votes per company
  const companyRegions = new Map(); // co_key → { region → count }

  for (const p of people) {
    const coKey = p.current_company_name.toLowerCase().trim();
    const resolved = geoMapper.resolve(p.country_code, p.location || p.country);
    if (resolved.region_code === 'UNKNOWN') continue;

    if (!companyRegions.has(coKey)) companyRegions.set(coKey, new Map());
    const votes = companyRegions.get(coKey);
    votes.set(resolved.region_code, (votes.get(resolved.region_code) || 0) + 1);
  }

  // Pick majority region per company and update
  let updated = 0;
  for (const [coKey, votes] of companyRegions) {
    let bestRegion = 'UNKNOWN', bestCount = 0;
    for (const [region, count] of votes) {
      if (count > bestCount) { bestRegion = region; bestCount = count; }
    }

    // Also derive sector from most common industry among people at this company
    const { rows: sectorRows } = await pool.query(`
      SELECT industries[1] AS sector, COUNT(*) AS cnt
      FROM people
      WHERE LOWER(TRIM(current_company_name)) = $1
        AND industries IS NOT NULL AND array_length(industries, 1) > 0
      GROUP BY industries[1]
      ORDER BY cnt DESC LIMIT 1
    `, [coKey]);

    const sector = sectorRows[0]?.sector || null;

    await pool.query(`
      UPDATE company_adjacency_scores
      SET derived_region_code = $1, derived_sector = $2
      WHERE LOWER(TRIM(company_name)) = $3
    `, [bestRegion, sector, coKey]);
    updated++;
  }

  LOG('✅', `  Derived regions for ${updated} companies`);
  return updated;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE C: NETWORK DENSITY SCORES
// ═══════════════════════════════════════════════════════════════════════════════

async function computeNetworkDensity(geoMapper) {
  LOG('📊', 'Phase C: Computing network density by region...');

  // Get all people with enough data to resolve region
  const { rows: people } = await pool.query(`
    SELECT p.id, p.country_code, p.location, p.country,
           p.seniority_level, p.industries,
           p.current_company_name
    FROM people p
    WHERE (p.current_title IS NOT NULL OR p.source = 'ezekia')
  `);

  LOG('👥', `  Processing ${people.length} people...`);

  // Aggregate by region (and region × sector)
  const regionAgg = new Map(); // region → { total, active_ids, senior, sectors: { sector → count } }
  const personIds = [];

  for (const p of people) {
    const resolved = geoMapper.resolve(p.country_code, p.location || p.country);
    const region = resolved.region_code;

    if (!regionAgg.has(region)) {
      regionAgg.set(region, { total: 0, personIds: [], senior: 0, sectors: new Map() });
    }
    const agg = regionAgg.get(region);
    agg.total++;
    agg.personIds.push(p.id);
    if (['C-Suite','VP','Director','Partner','Managing Director','SVP','EVP'].includes(p.seniority_level)) {
      agg.senior++;
    }

    // Track sector
    const sector = p.industries?.[0] || null;
    if (sector) {
      agg.sectors.set(sector, (agg.sectors.get(sector) || 0) + 1);
    }
  }

  // Now compute active counts (batch query for efficiency)
  for (const [region, agg] of regionAgg) {
    if (agg.personIds.length === 0) continue;

    // Count active contacts in batches
    const batchSize = 5000;
    let activeCount = 0;
    for (let i = 0; i < agg.personIds.length; i += batchSize) {
      const batch = agg.personIds.slice(i, i + batchSize);
      const { rows: [{ cnt }] } = await pool.query(`
        SELECT COUNT(DISTINCT person_id) AS cnt
        FROM interactions
        WHERE person_id = ANY($1::uuid[])
          AND interaction_at > NOW() - INTERVAL '180 days'
      `, [batch]);
      activeCount += parseInt(cnt);
    }
    agg.activeCount = activeCount;
  }

  // Get placement and client counts per region from company_adjacency_scores
  const { rows: adjByRegion } = await pool.query(`
    SELECT derived_region_code AS region,
           SUM(placement_count) AS placements,
           COUNT(*) FILTER (WHERE is_client) AS clients
    FROM company_adjacency_scores
    WHERE derived_region_code IS NOT NULL
    GROUP BY derived_region_code
  `);
  const adjMap = new Map(adjByRegion.map(r => [r.region, r]));

  // Upsert region-level density scores
  let upserted = 0;
  for (const [region, agg] of regionAgg) {
    if (region === 'UNKNOWN') continue;

    const adj = adjMap.get(region) || {};
    const placements = parseInt(adj.placements) || 0;
    const clients = parseInt(adj.clients) || 0;
    const active = agg.activeCount || 0;

    const density = Math.min(100,
      Math.min(agg.total / 500, 1) * 30 +
      Math.min(active / 100, 1) * 30 +
      Math.min(agg.senior / 50, 1) * 20 +
      Math.min(clients / 5, 1) * 10 +
      Math.min(placements / 5, 1) * 10
    );

    const breakdown = {
      total_contacts: agg.total,
      active_contacts: active,
      senior_contacts: agg.senior,
      placements, clients,
      top_sectors: [...agg.sectors.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([s, c]) => ({ sector: s, count: c }))
    };

    await pool.query(`
      INSERT INTO network_density_scores (
        region_code, sector, total_contacts, active_contacts, senior_contacts,
        placement_count, client_count, density_score, score_breakdown, computed_at
      ) VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, NOW())
      ON CONFLICT (region_code, COALESCE(sector, '__ALL__')) DO UPDATE SET
        total_contacts = EXCLUDED.total_contacts,
        active_contacts = EXCLUDED.active_contacts,
        senior_contacts = EXCLUDED.senior_contacts,
        placement_count = EXCLUDED.placement_count,
        client_count = EXCLUDED.client_count,
        density_score = EXCLUDED.density_score,
        score_breakdown = EXCLUDED.score_breakdown,
        computed_at = NOW()
    `, [region, agg.total, active, agg.senior, placements, clients,
        Math.round(density * 100) / 100, JSON.stringify(breakdown)]);
    upserted++;

    LOG('📍', `  ${region}: ${agg.total} contacts, ${active} active, ${agg.senior} senior → density ${density.toFixed(1)}`);
  }

  LOG('✅', `  Upserted ${upserted} density scores`);
  return upserted;
}

// ═══════════════════════════════════════════════════════════════════════════════
// BACKFILL: Fix missing company_id and geography
// ═══════════════════════════════════════════════════════════════════════════════

async function backfillCompanyData() {
  LOG('🔧', 'Backfilling missing company_id and geography...');

  // Backfill people.current_company_id where NULL
  const { rowCount: peopleFixed } = await pool.query(`
    UPDATE people p
    SET current_company_id = c.id
    FROM companies c
    WHERE p.current_company_id IS NULL
      AND p.current_company_name IS NOT NULL
      AND LOWER(TRIM(c.name)) = LOWER(TRIM(p.current_company_name))
  `);
  if (peopleFixed > 0) LOG('✅', `  Linked ${peopleFixed} people to company records`);

  // Backfill companies.geography from adjacency derived_region_code
  const { rowCount: geoFixed } = await pool.query(`
    UPDATE companies c
    SET geography = gp.region_name
    FROM company_adjacency_scores cas
    JOIN geo_priorities gp ON gp.region_code = cas.derived_region_code
    WHERE cas.company_id = c.id
      AND (c.geography IS NULL OR c.geography = '')
      AND cas.derived_region_code IS NOT NULL
      AND cas.derived_region_code != 'UNKNOWN'
  `);
  if (geoFixed > 0) LOG('✅', `  Set geography on ${geoFixed} companies`);

  // Backfill companies.country_code from majority people
  const { rowCount: ccFixed } = await pool.query(`
    UPDATE companies c
    SET country_code = sub.cc
    FROM (
      SELECT p.current_company_id AS cid, MODE() WITHIN GROUP (ORDER BY p.country_code) AS cc
      FROM people p
      WHERE p.current_company_id IS NOT NULL AND p.country_code IS NOT NULL
      GROUP BY p.current_company_id
    ) sub
    WHERE c.id = sub.cid
      AND (c.country_code IS NULL OR c.country_code = '')
      AND sub.cc IS NOT NULL
  `);
  if (ccFixed > 0) LOG('✅', `  Set country_code on ${ccFixed} companies`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function computeNetworkTopology() {
  LOG('🌐', '═══ Network Topology Computation ═══');
  const start = Date.now();

  // Ensure tables exist
  const fs = require('fs');
  const path = require('path');
  const migrationPath = path.join(__dirname, '..', 'sql', 'migration_network_topology.sql');
  if (fs.existsSync(migrationPath)) {
    try {
      await pool.query(fs.readFileSync(migrationPath, 'utf8'));
    } catch (e) { /* tables may already exist */ }
  }

  const geoMapper = new GeoMapper();
  await geoMapper.load(pool);

  const adjCount = await computeCompanyAdjacency();
  const regionCount = await deriveCompanyRegions(geoMapper);
  const densityCount = await computeNetworkDensity(geoMapper);
  await backfillCompanyData();

  const duration = ((Date.now() - start) / 1000).toFixed(1);
  LOG('🌐', `═══ Complete in ${duration}s: ${adjCount} companies, ${regionCount} regions derived, ${densityCount} density scores ═══`);

  return { companies: adjCount, regions: regionCount, density: densityCount, duration_s: parseFloat(duration) };
}

if (require.main === module) {
  computeNetworkTopology()
    .then(r => { console.log('\nResult:', JSON.stringify(r, null, 2)); process.exit(0); })
    .catch(e => { console.error('Fatal:', e); process.exit(1); });
}

module.exports = { computeNetworkTopology };
