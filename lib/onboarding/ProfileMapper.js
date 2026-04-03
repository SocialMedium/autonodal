// ═══════════════════════════════════════════════════════════════════════════════
// lib/onboarding/ProfileMapper.js — Maps AI-extracted profile → signal config
// ═══════════════════════════════════════════════════════════════════════════════

var { INTENT_TAXONOMY } = require('./intentTaxonomy');

var GEO_NORMALISE = {
  'singapore': 'SGP', 'sg': 'SGP', 'sgp': 'SGP',
  'australia': 'AUS', 'sydney': 'AUS', 'melbourne': 'AUS', 'aus': 'AUS',
  'new zealand': 'NZL', 'nz': 'NZL', 'nzl': 'NZL',
  'hong kong': 'HKG', 'hk': 'HKG', 'hkg': 'HKG',
  'india': 'IND', 'mumbai': 'IND', 'bangalore': 'IND', 'ind': 'IND',
  'uk': 'GBR', 'london': 'GBR', 'britain': 'GBR', 'gbr': 'GBR', 'united kingdom': 'GBR',
  'usa': 'USA', 'us': 'USA', 'new york': 'USA', 'san francisco': 'USA',
  'japan': 'JPN', 'tokyo': 'JPN', 'jpn': 'JPN',
  'indonesia': 'IDN', 'jakarta': 'IDN', 'idn': 'IDN',
  'malaysia': 'MYS', 'kuala lumpur': 'MYS', 'mys': 'MYS',
  'thailand': 'THA', 'bangkok': 'THA', 'tha': 'THA',
  'vietnam': 'VNM', 'vnm': 'VNM',
  'philippines': 'PHL', 'phl': 'PHL',
  'germany': 'DEU', 'deu': 'DEU',
  'france': 'FRA', 'fra': 'FRA',
  'netherlands': 'NLD', 'nld': 'NLD',
  'uae': 'ARE', 'dubai': 'ARE', 'are': 'ARE',
  'sea': 'SEA', 'southeast asia': 'SEA', 'asean': 'SEA',
  'apac': 'APAC', 'asia pacific': 'APAC', 'asia': 'APAC',
  'anz': 'ANZ',
  'europe': 'EUR', 'eu': 'EUR', 'eur': 'EUR',
  'dach': 'DACH',
  'mena': 'MEA', 'middle east': 'MEA', 'mea': 'MEA',
  'latam': 'LATAM', 'latin america': 'LATAM',
  'global': 'GLOBAL',
};

var GEO_TO_BUNDLE = {
  AUS: 'region-anz', ANZ: 'region-anz', NZL: 'region-anz',
  SGP: 'region-sea', SEA: 'region-sea', IDN: 'region-sea', MYS: 'region-sea',
  THA: 'region-sea', VNM: 'region-sea', PHL: 'region-sea',
  APAC: 'region-apac', HKG: 'region-apac', JPN: 'region-apac',
  IND: 'region-india',
  GBR: 'region-uk', IRL: 'region-uk',
  EUR: 'region-emea', DEU: 'region-dach', DACH: 'region-dach', FRA: 'region-emea', NLD: 'region-emea',
  USA: 'region-north-america', CAN: 'region-north-america',
  MEA: 'region-mea', ARE: 'region-mea',
  LATAM: 'region-latam',
};

var SECTOR_TO_BUNDLE = {
  fintech: 'sector-fintech', saas_ai: 'sector-saas-ai',
  web3: 'theme-web3', healthtech: 'theme-health',
  cleantech: 'theme-climate', adtech: 'theme-programmatic',
  edtech: 'sector-edtech', proptech: 'theme-proptech',
  vc: 'vc-global', pe: 'pe-global',
  ma: 'signal-ma', professional_services: 'sector-prof-services',
};

var INTENT_TO_BUNDLES = {
  investing:        ['vc-global', 'signal-funding', 'signal-ma'],
  raising_capital:  ['signal-funding', 'vc-global'],
  talent_sourcing:  ['signal-exec-moves'],
  job_seeking:      ['signal-exec-moves', 'signal-funding'],
  mandate_hunting:  ['signal-ma', 'signal-distress', 'signal-exec-moves'],
  sales_growth:     ['signal-funding', 'signal-ma'],
  partnerships:     ['signal-funding', 'signal-ma'],
  market_intel:     ['signal-ma', 'signal-funding'],
};

var SECTOR_KEYWORDS = {
  fintech:    ['fintech', 'payments', 'neobank', 'banking', 'insurtech'],
  saas_ai:    ['artificial intelligence', 'saas', 'enterprise software', 'llm', 'ai'],
  web3:       ['web3', 'defi', 'blockchain', 'crypto', 'protocol'],
  healthtech: ['healthtech', 'digital health', 'biotech', 'medtech'],
  cleantech:  ['climate', 'clean energy', 'renewables', 'carbon', 'esg'],
  adtech:     ['advertising', 'martech', 'programmatic', 'brand', 'media'],
  edtech:     ['edtech', 'education', 'higher education', 'learning'],
  vc:         ['venture capital', 'series a', 'series b', 'seed round'],
  pe:         ['private equity', 'buyout', 'portfolio company', 'lbo'],
  ma:         ['acquisition', 'merger', 'transaction', 'strategic review'],
};

function mapProfileToConfig(profile) {
  // Normalise geographies
  var geos = (profile.geographies || []).map(function(g) {
    var norm = GEO_NORMALISE[g.toLowerCase()];
    return norm || g.toUpperCase();
  });

  // Derive bundles
  var geoBundles = [];
  geos.forEach(function(g) { if (GEO_TO_BUNDLE[g]) geoBundles.push(GEO_TO_BUNDLE[g]); });

  var sectorBundles = [];
  (profile.sectors || []).forEach(function(s) { if (SECTOR_TO_BUNDLE[s]) sectorBundles.push(SECTOR_TO_BUNDLE[s]); });

  var intentBundles = [];
  (profile.intents || []).forEach(function(i) {
    (INTENT_TO_BUNDLES[i] || []).forEach(function(b) { intentBundles.push(b); });
  });

  // Deduplicate
  var allBundles = [];
  var seen = {};
  [].concat(geoBundles, sectorBundles, intentBundles).forEach(function(b) {
    if (!seen[b]) { allBundles.push(b); seen[b] = true; }
  });

  // Build signal dial keywords
  var includeKeywords = [];
  (profile.sectors || []).forEach(function(s) {
    (SECTOR_KEYWORDS[s] || []).forEach(function(k) { includeKeywords.push(k); });
  });
  (profile.intents || []).forEach(function(i) {
    var mapped = INTENT_TAXONOMY[i];
    if (mapped && mapped.signal_priorities) {
      mapped.signal_priorities.forEach(function(sp) { includeKeywords.push(sp.replace(/_/g, ' ')); });
    }
  });
  geos.forEach(function(g) { includeKeywords.push(g.toLowerCase()); });

  // Deduplicate keywords
  var kwSeen = {};
  var uniqueKeywords = [];
  includeKeywords.forEach(function(k) {
    if (!kwSeen[k]) { uniqueKeywords.push(k); kwSeen[k] = true; }
  });

  var signalDial = {
    themes: [],
    keywords: {
      include: uniqueKeywords,
      exclude: ['crypto price', 'bitcoin price', 'nft floor price'],
      boost: ['raises', 'acquires', 'appoints', 'launches', 'expands', 'partnership'],
    },
    confidence_threshold: 0.65,
    signal_types: deriveSignalTypes(profile.intents),
  };

  // Populate themes from sectors + intents
  var themes = {};
  (profile.sectors || []).forEach(function(s) { themes[s] = true; });
  (profile.intents || []).forEach(function(i) { themes[i] = true; });
  signalDial.themes = Object.keys(themes);

  // EventMedium canister seed
  var canisterSeed = {
    intent_type: profile.intents && profile.intents[0] ? profile.intents[0] : 'market_intel',
    seeking: (profile.intents || []).map(function(i) { return INTENT_TAXONOMY[i] ? INTENT_TAXONOMY[i].nev_canister_type : null; }).filter(Boolean),
    sectors: profile.sectors,
    geographies: geos,
    stage_focus: profile.stage_focus || null,
    summary: profile.summary,
  };

  return {
    vertical: profile.vertical || 'revenue',
    bundles: allBundles,
    signal_dial: signalDial,
    canister_seed: canisterSeed,
    profile: {
      display_name: profile.display_name,
      role: profile.role,
      firm: profile.firm,
      intents: profile.intents,
      sectors: profile.sectors,
      focus_geographies: geos,
      summary: profile.summary,
    },
  };
}

function deriveSignalTypes(intents) {
  var typeMap = {
    investing:        ['capital_raising', 'company_founded', 'product_launch'],
    raising_capital:  ['capital_raising', 'strategic_hiring', 'partnership'],
    talent_sourcing:  ['leadership_change', 'company_exit', 'strategic_hiring'],
    mandate_hunting:  ['restructuring', 'ma_activity', 'leadership_change'],
    sales_growth:     ['capital_raising', 'geographic_expansion', 'leadership_change'],
    market_intel:     ['capital_raising', 'ma_activity', 'leadership_change', 'product_launch'],
    job_seeking:      ['capital_raising', 'strategic_hiring', 'geographic_expansion'],
    partnerships:     ['partnership', 'product_launch', 'geographic_expansion'],
    co_founding:      ['company_founded', 'company_exit', 'capital_raising'],
    advisory:         ['leadership_change', 'capital_raising', 'restructuring'],
  };
  var result = {};
  (intents || []).forEach(function(i) {
    (typeMap[i] || []).forEach(function(t) { result[t] = true; });
  });
  return Object.keys(result);
}

module.exports = { mapProfileToConfig };
