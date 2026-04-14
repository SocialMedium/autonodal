// ═══════════════════════════════════════════════════════════════════════════════
// lib/job_signal_evaluator.js — Job Signal Rule Evaluator
// Evaluates job_signal_rules against posting state, generates signal_events
// ═══════════════════════════════════════════════════════════════════════════════

const db = require('./db');
const { ML_TENANT_ID } = require('./tenant');

const TENANT_ID = ML_TENANT_ID;

async function evaluateJobSignals(companyId, context = {}) {
  const { added, removed } = context;

  const rules = await db.queryAll(`
    SELECT * FROM job_signal_rules
    WHERE (tenant_id IS NULL OR tenant_id = $1)
      AND is_enabled = true
    ORDER BY tenant_id NULLS LAST
  `, [TENANT_ID]);

  for (const rule of rules) {
    try {
      await evaluateRule(companyId, rule, { added, removed });
    } catch (e) {
      console.warn(`  Rule ${rule.rule_name} error for ${companyId}: ${e.message}`);
    }
  }
}

async function evaluateRule(companyId, rule, context) {
  const { removed } = context;

  const isRemovalRule = rule.rule_name.includes('removed') || rule.rule_name.includes('removal');

  // Skip removal rules if no removals in this harvest
  if (isRemovalRule && (!removed || removed.length === 0)) return;

  // Count matching postings within time window
  const status = isRemovalRule ? 'removed' : 'active';
  const dateCol = isRemovalRule ? 'removed_at' : 'first_seen_at';

  let query = `
    SELECT COUNT(*) AS count,
           ARRAY_AGG(DISTINCT location) FILTER (WHERE location IS NOT NULL) AS locations,
           ARRAY_AGG(title ORDER BY first_seen_at DESC) AS titles
    FROM job_postings
    WHERE tenant_id = $1
      AND company_id = $2
      AND status = $3
      AND ${dateCol} > NOW() - ($4 || ' days')::INTERVAL
  `;
  const params = [TENANT_ID, companyId, status, String(rule.time_window_days)];

  if (rule.seniority_levels && rule.seniority_levels.length > 0) {
    params.push(rule.seniority_levels);
    query += ` AND seniority_level = ANY($${params.length})`;
  }
  if (rule.function_areas && rule.function_areas.length > 0) {
    params.push(rule.function_areas);
    query += ` AND function_area = ANY($${params.length})`;
  }

  const result = await db.queryOne(query, params);
  const count = parseInt(result.count);
  const locations = result.locations || [];
  const titles = result.titles || [];

  if (count < rule.min_postings) return;

  // Get company details for signal
  const company = await db.queryOne(
    `SELECT name, sector, geography FROM companies WHERE id = $1`,
    [companyId]
  );
  if (!company) return;

  // Check for geographic expansion: new locations vs historical
  if (rule.requires_new_geo) {
    const isNewGeo = await checkNewGeography(companyId, locations);
    if (!isNewGeo) return;
  }

  // Dedup: check if this signal was already generated recently
  const existing = await db.queryOne(`
    SELECT id FROM signal_events
    WHERE tenant_id = $1
      AND company_id = $2
      AND signal_type = $3::signal_type
      AND source_url = 'jobs'
      AND detected_at > NOW() - INTERVAL '7 days'
    LIMIT 1
  `, [TENANT_ID, companyId, rule.signal_type]);

  if (existing) return;

  // Build evidence text
  const topTitles = titles.slice(0, 3).join(', ');
  const evidence = buildJobSignalEvidence(rule, count, topTitles, locations, company);

  // Insert signal event
  await db.query(`
    INSERT INTO signal_events (
      tenant_id, company_id, company_name, signal_type,
      confidence_score, evidence_summary, source_url,
      scoring_breakdown, detected_at
    ) VALUES ($1, $2, $3, $4::signal_type, $5, $6, 'jobs', $7, NOW())
  `, [
    TENANT_ID,
    companyId,
    company.name,
    rule.signal_type,
    rule.confidence,
    evidence,
    JSON.stringify({
      rule_name: rule.rule_name,
      posting_count: count,
      titles: titles.slice(0, 5),
      locations: locations.filter(Boolean).slice(0, 5),
      time_window_days: rule.time_window_days,
    }),
  ]);

  console.log(`  Signal: ${rule.signal_type} for ${company.name} (${rule.rule_name})`);
}

async function checkNewGeography(companyId, currentLocations) {
  if (!currentLocations || currentLocations.length === 0) return false;

  // Get historical locations (postings older than 60 days or removed)
  const historical = await db.queryAll(`
    SELECT DISTINCT location FROM job_postings
    WHERE company_id = $1 AND tenant_id = $2
      AND first_seen_at < NOW() - INTERVAL '60 days'
      AND location IS NOT NULL
  `, [companyId, TENANT_ID]);

  const historicalSet = new Set(historical.map(r => (r.location || '').toLowerCase()));
  const newLocations = currentLocations.filter(l =>
    l && !historicalSet.has(l.toLowerCase())
  );

  return newLocations.length > 0;
}

function buildJobSignalEvidence(rule, count, topTitles, locations, company) {
  const locStr = locations.filter(Boolean).slice(0, 3).join(', ') || 'various locations';
  switch (rule.rule_name) {
    case 'c_suite_post':
      return `${company.name} posted ${topTitles} — C-suite or VP-level hiring signal`;
    case 'director_batch':
      return `${company.name} posted ${count} director-level roles in ${rule.time_window_days} days — leadership build-out signal`;
    case 'volume_spike':
      return `${company.name} has posted ${count} roles in the past ${rule.time_window_days} days — growth-phase hiring signal`;
    case 'geo_expansion_jobs':
      return `${company.name} posting ${count} roles in ${locStr} — geographic expansion signal`;
    case 'c_suite_removed':
      return `${company.name} ${topTitles} role removed — hire likely made or search cancelled`;
    case 'mass_removal':
      return `${company.name} removed ${count} active roles in ${rule.time_window_days} days — headcount freeze or restructuring signal`;
    case 'finance_hiring':
      return `${company.name} posted ${topTitles} — finance leadership hire signals post-raise build-out`;
    case 'board_advisory':
      return `${company.name} posted ${topTitles} — board/advisory role signals governance build-out`;
    default:
      return `${company.name}: ${count} job postings match ${rule.rule_name} pattern`;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// GEO ROLE SIGNAL — fires immediately when a geographic leadership role detected
// ═══════════════════════════════════════════════════════════════════════════════

const GEO_ROLE_CONFIDENCE = {
  country_manager:   0.95,
  regional_csuite:   0.95,
  regional_md:       0.92,
  regional_vp:       0.90,
  head_of_region:    0.88,
  regional_director: 0.85,
  market_entry:      0.80,
};

const GEO_ROLE_LABELS = {
  country_manager:   'Country Manager',
  regional_csuite:   'Regional C-suite executive',
  regional_md:       'Regional Managing Director',
  regional_vp:       'Regional VP',
  head_of_region:    'Regional Head',
  regional_director: 'Regional Director',
  market_entry:      'Market Entry lead',
};

async function evaluateGeoRoleSignal(posting) {
  if (!posting.is_geo_expansion_role || !posting.company_id) return;

  const company = await db.queryOne(
    'SELECT id, name, sector, geography FROM companies WHERE id = $1',
    [posting.company_id]
  );
  if (!company) return;

  const confidence = GEO_ROLE_CONFIDENCE[posting.geo_role_class] || 0.80;
  const geoStr = posting.target_geography ? ` in ${posting.target_geography}` : '';
  const tierStr = posting.target_geo_tier === 'country'
    ? 'country-level market entry'
    : posting.target_geo_tier === 'subregion'
    ? 'sub-regional expansion'
    : 'regional expansion';
  const roleLabel = GEO_ROLE_LABELS[posting.geo_role_class] || 'Geographic leadership role';

  const evidence = `${company.name} posted "${posting.title}"${geoStr} — ` +
    `${roleLabel} hire signals committed ${tierStr}. ` +
    `This is a definitive market entry signal, not an exploratory hire.`;

  // Dedup — don't re-fire for same company + geo in 14 days
  const existing = await db.queryOne(`
    SELECT id FROM signal_events
    WHERE tenant_id = $1
      AND company_id = $2
      AND signal_type = 'geographic_expansion'::signal_type
      AND source_url = 'jobs'
      AND scoring_breakdown->>'target_geography' = $3
      AND detected_at > NOW() - INTERVAL '14 days'
    LIMIT 1
  `, [TENANT_ID, posting.company_id, posting.target_geography || '']);

  if (existing) return;

  await db.query(`
    INSERT INTO signal_events (
      tenant_id, company_id, company_name, signal_type,
      confidence_score, evidence_summary, source_url,
      scoring_breakdown, detected_at
    ) VALUES ($1, $2, $3, 'geographic_expansion'::signal_type, $4, $5, 'jobs', $6, NOW())
  `, [
    TENANT_ID,
    posting.company_id,
    company.name,
    confidence,
    evidence,
    JSON.stringify({
      rule_name: posting.geo_role_class,
      job_title: posting.title,
      target_geography: posting.target_geography,
      target_geo_tier: posting.target_geo_tier,
      geo_role_class: posting.geo_role_class,
    }),
  ]);

  console.log(`  GEO SIGNAL: ${company.name} → ${posting.geo_role_class}${geoStr} [${confidence}]`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// GEO LEADERSHIP WAVE — market-level signal (multiple companies → same region)
// ═══════════════════════════════════════════════════════════════════════════════

async function evaluateGeoLeadershipWave() {
  const waves = await db.queryAll(`
    SELECT
      jp.target_geography,
      jp.target_geo_tier,
      COUNT(DISTINCT jp.company_id)   AS company_count,
      COUNT(*)                        AS posting_count,
      ARRAY_AGG(DISTINCT jp.geo_role_class) AS role_classes,
      ARRAY_AGG(DISTINCT c.sector) FILTER (WHERE c.sector IS NOT NULL) AS sectors,
      MIN(jp.first_seen_at)           AS earliest_posting
    FROM job_postings jp
    JOIN companies c ON c.id = jp.company_id
    WHERE jp.tenant_id = $1
      AND jp.is_geo_expansion_role = true
      AND jp.status = 'active'
      AND jp.first_seen_at > NOW() - INTERVAL '30 days'
      AND jp.target_geography IS NOT NULL
    GROUP BY jp.target_geography, jp.target_geo_tier
    HAVING COUNT(DISTINCT jp.company_id) >= 3
    ORDER BY company_count DESC
  `, [TENANT_ID]);

  for (const wave of waves) {
    const confidence = Math.min(0.70 + (parseInt(wave.company_count) * 0.05), 0.97);
    const sectorStr = (wave.sectors || []).filter(Boolean).slice(0, 3).join(', ') || 'mixed';

    const evidence =
      `${wave.company_count} companies are simultaneously hiring geographic ` +
      `leadership roles in ${wave.target_geography} — sector-level market ` +
      `entry signal. Sectors: ${sectorStr}. ` +
      `${wave.posting_count} total geo leadership postings in 30 days.`;

    // Dedup — one wave signal per geography per 7 days
    const existing = await db.queryOne(`
      SELECT id FROM signal_events
      WHERE tenant_id = $1
        AND company_id IS NULL
        AND signal_type = 'geographic_expansion'::signal_type
        AND source_url = 'market_aggregate'
        AND scoring_breakdown->>'target_geography' = $2
        AND detected_at > NOW() - INTERVAL '7 days'
      LIMIT 1
    `, [TENANT_ID, wave.target_geography]);

    if (existing) continue;

    await db.query(`
      INSERT INTO signal_events (
        tenant_id, company_id, signal_type,
        confidence_score, evidence_summary, source_url,
        scoring_breakdown, detected_at
      ) VALUES ($1, NULL, 'geographic_expansion'::signal_type, $2, $3, 'market_aggregate', $4, NOW())
    `, [
      TENANT_ID,
      confidence,
      evidence,
      JSON.stringify({
        rule_name: 'geo_leadership_wave',
        target_geography: wave.target_geography,
        target_geo_tier: wave.target_geo_tier,
        company_count: parseInt(wave.company_count),
        posting_count: parseInt(wave.posting_count),
        role_classes: wave.role_classes,
        sectors: wave.sectors,
        earliest_posting: wave.earliest_posting,
      }),
    ]);

    console.log(`  MARKET WAVE: ${wave.company_count} companies entering ${wave.target_geography} [${confidence.toFixed(2)}]`);
  }
}

module.exports = { evaluateJobSignals, evaluateGeoRoleSignal, evaluateGeoLeadershipWave };
