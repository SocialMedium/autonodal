// ═══════════════════════════════════════════════════════════════════════════
// lib/onboarding/networkAnalyser.js — Tenant Network Analysis for Feed Config
// ═══════════════════════════════════════════════════════════════════════════
//
// CROSS-TENANT: Uses platformPool intentionally for recommendBundles() which
// queries the platform-level feed_bundles table (not tenant-scoped).
// Individual tenant analysis uses TenantDB for people/companies queries.
//
// Analyses a tenant's ingested people and companies to derive:
//   - Sector weights (what industries dominate the network)
//   - Geography weights (where the contacts are)
//   - Stage distribution (early/growth/enterprise)
//   - Bundle recommendations (auto-enable the most relevant feeds)

const { TenantDB, platformPool } = require('../TenantDB');

// Country name/code → ISO3 mapping (partial, covering common values)
const COUNTRY_TO_ISO3 = {
  'australia': 'AUS', 'au': 'AUS', 'aud': 'AUS',
  'new zealand': 'NZL', 'nz': 'NZL',
  'united states': 'USA', 'us': 'USA', 'usa': 'USA', 'united states of america': 'USA',
  'canada': 'CAN', 'ca': 'CAN',
  'united kingdom': 'GBR', 'uk': 'GBR', 'gb': 'GBR', 'britain': 'GBR', 'england': 'GBR',
  'ireland': 'IRL', 'ie': 'IRL',
  'germany': 'DEU', 'de': 'DEU',
  'france': 'FRA', 'fr': 'FRA',
  'austria': 'AUT', 'at': 'AUT',
  'switzerland': 'CHE', 'ch': 'CHE',
  'singapore': 'SGP', 'sg': 'SGP',
  'hong kong': 'HKG', 'hk': 'HKG',
  'japan': 'JPN', 'jp': 'JPN',
  'india': 'IND', 'in': 'IND',
  'indonesia': 'IDN', 'id': 'IDN',
  'malaysia': 'MYS', 'my': 'MYS',
  'thailand': 'THA', 'th': 'THA',
  'vietnam': 'VNM', 'vn': 'VNM',
  'brazil': 'BRA', 'br': 'BRA',
  'mexico': 'MEX', 'mx': 'MEX',
  'colombia': 'COL', 'co': 'COL',
  'uae': 'ARE', 'ae': 'ARE', 'united arab emirates': 'ARE',
  'saudi arabia': 'SAU', 'sa': 'SAU',
  'nigeria': 'NGA', 'ng': 'NGA',
  'kenya': 'KEN', 'ke': 'KEN',
  'south africa': 'ZAF', 'za': 'ZAF',
  'netherlands': 'NLD', 'nl': 'NLD',
  'spain': 'ESP', 'es': 'ESP',
  'italy': 'ITA', 'it': 'ITA',
  'sweden': 'SWE', 'se': 'SWE',
  'norway': 'NOR', 'no': 'NOR',
  'denmark': 'DNK', 'dk': 'DNK',
  'finland': 'FIN', 'fi': 'FIN',
  'china': 'CHN', 'cn': 'CHN',
  'south korea': 'KOR', 'kr': 'KOR',
  'taiwan': 'TWN', 'tw': 'TWN',
  'israel': 'ISR', 'il': 'ISR',
};

// Sector normalisation — maps raw sector values to canonical labels
const SECTOR_MAP = {
  'technology': 'Technology', 'tech': 'Technology', 'it': 'Technology', 'software': 'Technology',
  'fintech': 'FinTech', 'financial technology': 'FinTech',
  'banking/financial services': 'Financial Services', 'financial services': 'Financial Services', 'financial_services': 'Financial Services', 'banking': 'Financial Services',
  'healthcare': 'HealthTech', 'healthtech': 'HealthTech', 'biotech': 'HealthTech', 'health': 'HealthTech',
  'consulting': 'Professional Services', 'professional_services': 'Professional Services', 'professional services': 'Professional Services',
  'saas': 'SaaS/AI', 'ai': 'SaaS/AI', 'artificial intelligence': 'SaaS/AI', 'machine learning': 'SaaS/AI',
  'web3': 'Web3', 'blockchain': 'Web3', 'crypto': 'Web3',
  'cleantech': 'CleanTech', 'clean energy': 'CleanTech', 'sustainability': 'CleanTech', 'renewable': 'CleanTech',
  'consumer': 'Consumer', 'retail': 'Consumer', 'ecommerce': 'Consumer', 'e-commerce': 'Consumer',
  'real estate': 'PropTech', 'proptech': 'PropTech', 'property': 'PropTech',
  'marketing': 'AdTech/Marketing', 'adtech': 'AdTech/Marketing', 'advertising': 'AdTech/Marketing',
  'sales': 'Sales/Revenue', 'revenue': 'Sales/Revenue',
  'human resources': 'HR/People', 'hr': 'HR/People', 'recruiting': 'HR/People', 'talent': 'HR/People',
  'deeptech': 'DeepTech', 'hardware': 'DeepTech', 'robotics': 'DeepTech', 'space': 'DeepTech',
  'industrial': 'Industrial', 'manufacturing': 'Industrial',
};

// Sector → bundle slug mapping
const SECTOR_TO_BUNDLE = {
  'FinTech': 'sector-fintech',
  'HealthTech': 'sector-healthtech',
  'Web3': 'sector-web3',
  'CleanTech': 'sector-cleantech',
  'SaaS/AI': 'sector-saas-ai',
  'AdTech/Marketing': 'sector-adtech',
  'DeepTech': 'sector-deeptech',
  'Consumer': 'sector-consumer',
  'PropTech': 'sector-proptech',
  'Industrial': 'sector-industrial',
  'Professional Services': 'sector-prof-services',
};

// ISO3 → region bundle slug mapping (check which region bundles contain this code)
function geoToBundleSlugs(iso3) {
  const regionMap = {
    'region-north-america': ['USA', 'CAN'],
    'region-emea': ['EUR', 'GBR', 'DEU', 'FRA', 'NLD', 'ESP', 'ITA', 'SWE', 'NOR', 'DNK', 'FIN', 'IRL', 'AUT', 'CHE', 'ISR'],
    'region-apac': ['APAC', 'SGP', 'HKG', 'JPN', 'AUS', 'IND', 'CHN', 'KOR', 'TWN', 'NZL'],
    'region-sea': ['SGP', 'IDN', 'MYS', 'THA', 'VNM'],
    'region-uk': ['GBR', 'IRL'],
    'region-dach': ['DEU', 'AUT', 'CHE'],
    'region-anz': ['AUS', 'NZL'],
    'region-india': ['IND'],
    'region-mea': ['ARE', 'SAU', 'NGA', 'KEN', 'ZAF'],
    'region-latam': ['BRA', 'MEX', 'COL'],
  };
  const matches = [];
  for (const [slug, codes] of Object.entries(regionMap)) {
    if (codes.includes(iso3)) matches.push(slug);
  }
  return matches;
}

function normaliseCountry(raw) {
  if (!raw) return null;
  const lower = raw.toLowerCase().trim();
  return COUNTRY_TO_ISO3[lower] || null;
}

function normaliseSector(raw) {
  if (!raw) return null;
  const lower = raw.toLowerCase().trim();
  return SECTOR_MAP[lower] || raw;
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════

async function analyseNetwork(tenantId) {
  const db = new TenantDB(tenantId);

  // 1. Network size
  const { rows: [sizes] } = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS people_count,
      (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS company_count
  `, [tenantId]);
  const networkSize = {
    people: parseInt(sizes.people_count) || 0,
    companies: parseInt(sizes.company_count) || 0,
  };

  // 2. Sector distribution from companies
  const { rows: rawSectors } = await db.query(`
    SELECT sector, COUNT(*) AS cnt
    FROM companies WHERE tenant_id = $1 AND sector IS NOT NULL AND sector != ''
    GROUP BY sector ORDER BY cnt DESC LIMIT 20
  `, [tenantId]);

  // Normalise and merge sectors
  const sectorCounts = {};
  for (const r of rawSectors) {
    const normalised = normaliseSector(r.sector);
    sectorCounts[normalised] = (sectorCounts[normalised] || 0) + parseInt(r.cnt);
  }
  const totalSectored = Object.values(sectorCounts).reduce((a, b) => a + b, 0);
  const sectorWeights = Object.entries(sectorCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([sector, count]) => ({
      sector,
      count,
      pct: totalSectored > 0 ? Math.round((count / totalSectored) * 100) / 100 : 0,
    }));

  // 3. Geography distribution from people + companies
  const { rows: rawGeos } = await db.query(`
    SELECT geo, SUM(cnt)::int AS cnt FROM (
      SELECT COALESCE(country, country_code, SPLIT_PART(location, ',', -1)) AS geo, COUNT(*) AS cnt
      FROM people WHERE tenant_id = $1 AND (country IS NOT NULL OR country_code IS NOT NULL OR location IS NOT NULL)
      GROUP BY geo
      UNION ALL
      SELECT COALESCE(country_code, SPLIT_PART(geography, ',', 1)) AS geo, COUNT(*) AS cnt
      FROM companies WHERE tenant_id = $1 AND (country_code IS NOT NULL OR geography IS NOT NULL)
      GROUP BY geo
    ) combined
    WHERE geo IS NOT NULL AND TRIM(geo) != ''
    GROUP BY geo ORDER BY cnt DESC LIMIT 30
  `, [tenantId]);

  // Normalise to ISO3 and merge
  const geoCounts = {};
  for (const r of rawGeos) {
    const iso3 = normaliseCountry(r.geo);
    if (iso3) {
      geoCounts[iso3] = (geoCounts[iso3] || 0) + parseInt(r.cnt);
    }
  }
  const totalGeoed = Object.values(geoCounts).reduce((a, b) => a + b, 0);
  const geoWeights = Object.entries(geoCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([country, count]) => ({
      country,
      count,
      pct: totalGeoed > 0 ? Math.round((count / totalGeoed) * 100) / 100 : 0,
    }));

  // 4. Top companies by contact count
  const { rows: topCompanies } = await db.query(`
    SELECT c.name, COUNT(p.id) AS contact_count
    FROM companies c
    LEFT JOIN people p ON p.current_company_id = c.id AND p.tenant_id = $1
    WHERE c.tenant_id = $1 AND c.name IS NOT NULL
    GROUP BY c.name ORDER BY COUNT(p.id) DESC LIMIT 10
  `, [tenantId]);

  // 5. Data quality coverage stats
  const { rows: [coverage] } = await db.query(`
    SELECT
      (SELECT COUNT(*) FILTER (WHERE sector IS NOT NULL AND sector != '') FROM companies WHERE tenant_id = $1) AS companies_with_sector,
      (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS total_companies,
      (SELECT COUNT(*) FILTER (WHERE country IS NOT NULL OR country_code IS NOT NULL OR location IS NOT NULL) FROM people WHERE tenant_id = $1) AS people_with_geo,
      (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS total_people
  `, [tenantId]);

  const dataQuality = {
    sector_coverage: parseInt(coverage.total_companies) > 0
      ? Math.round((parseInt(coverage.companies_with_sector) / parseInt(coverage.total_companies)) * 100)
      : 0,
    geo_coverage: parseInt(coverage.total_people) > 0
      ? Math.round((parseInt(coverage.people_with_geo) / parseInt(coverage.total_people)) * 100)
      : 0,
    companies_with_sector: parseInt(coverage.companies_with_sector),
    people_with_geo: parseInt(coverage.people_with_geo),
  };

  // 6. Bundle recommendations
  const recommendedBundles = await recommendBundles(tenantId, {
    sectorWeights,
    geoWeights,
    networkSize,
  });

  return {
    sector_weights: sectorWeights,
    geo_weights: geoWeights,
    top_companies: topCompanies.map(c => ({ name: c.name, contacts: parseInt(c.contact_count) })),
    network_size: networkSize,
    data_quality: dataQuality,
    recommended_bundles: recommendedBundles,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// BUNDLE RECOMMENDATIONS
// ═══════════════════════════════════════════════════════════════════════════

async function recommendBundles(tenantId, analysis) {
  // Fetch all active bundles
  let bundles;
  try {
    const { rows } = await platformPool.query(
      "SELECT id, name, slug, description, bundle_type, sectors, geographies, is_featured FROM feed_bundles WHERE is_active = true ORDER BY display_order"
    );
    bundles = rows;
  } catch (e) {
    bundles = [];
  }

  const recommendations = [];

  // Global macro — always on, even if no bundles in DB
  const globalMacro = bundles.find(b => b.slug === 'global-macro');
  recommendations.push({
    bundle_id: globalMacro?.id || null,
    bundle_slug: 'global-macro',
    bundle_name: globalMacro?.name || 'Global Macro Intelligence',
    reason: 'Always-on macro signals — regulatory changes, market shifts, and global trends',
    confidence: 1.0,
    auto_enable: true,
    bundle_type: 'core',
  });

  // Score each bundle by relevance to the tenant's network
  for (const bundle of bundles) {
    if (bundle.slug === 'global-macro') continue;

    let score = 0;
    let reason = '';

    if (bundle.bundle_type === 'sector' && bundle.sectors?.length > 0) {
      // Check if any of the tenant's top sectors match this bundle's sectors
      for (const sw of analysis.sectorWeights) {
        const bundleSectorSlug = SECTOR_TO_BUNDLE[sw.sector];
        if (bundleSectorSlug === bundle.slug) {
          score = Math.min(1.0, 0.5 + sw.pct);
          reason = `${Math.round(sw.pct * 100)}% of your network is in ${sw.sector}`;
          break;
        }
        // Also check if raw sector label matches bundle sectors array
        if (bundle.sectors.some(bs => sw.sector.toLowerCase().includes(bs.toLowerCase()))) {
          score = Math.max(score, 0.4 + sw.pct * 0.5);
          reason = reason || `${sw.count} companies in ${sw.sector}`;
        }
      }
    }

    if (bundle.bundle_type === 'region' && bundle.geographies?.length > 0) {
      for (const gw of analysis.geoWeights) {
        if (bundle.geographies.includes(gw.country)) {
          score = Math.max(score, 0.5 + gw.pct);
          reason = reason || `${Math.round(gw.pct * 100)}% of your contacts are in this region`;
        }
        // Check region containment
        const matchedSlugs = geoToBundleSlugs(gw.country);
        if (matchedSlugs.includes(bundle.slug)) {
          score = Math.max(score, 0.4 + gw.pct * 0.6);
          reason = reason || `${gw.count} contacts in ${gw.country}`;
        }
      }
    }

    if (bundle.bundle_type === 'signal_type') {
      // Signal type bundles get a base relevance score
      score = 0.3;
      reason = 'Relevant signal category for your network';
      if (['signal-funding', 'signal-exec-moves'].includes(bundle.slug)) {
        score = 0.5;
        reason = 'Core signal type for network intelligence';
      }
    }

    if (bundle.bundle_type === 'investor_type') {
      // Check if tenant has investors in their network
      score = 0.25;
      reason = 'Investor intelligence feed';
    }

    if (score > 0.2) {
      recommendations.push({
        bundle_id: bundle.id,
        bundle_slug: bundle.slug,
        bundle_name: bundle.name,
        reason,
        confidence: Math.round(score * 100) / 100,
        auto_enable: score >= 0.75,
        bundle_type: bundle.bundle_type,
      });
    }
  }

  // Sort: auto_enable first, then by confidence descending
  recommendations.sort((a, b) => {
    if (a.auto_enable && !b.auto_enable) return -1;
    if (!a.auto_enable && b.auto_enable) return 1;
    return b.confidence - a.confidence;
  });

  return recommendations;
}

module.exports = { analyseNetwork, recommendBundles };
