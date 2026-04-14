// ═══════════════════════════════════════════════════════════════════════════════
// lib/job_classifier.js — Title → seniority + function area + geo role classifier
// ═══════════════════════════════════════════════════════════════════════════════

// ── SENIORITY CLASSIFICATION ────────────────────────────────────────────────
// Order matters — match most specific first

const SENIORITY_PATTERNS = [
  { level: 'c_suite', patterns: [
    /\b(chief|ceo|cto|coo|cfo|cmo|cpo|ciso|chro|cdo)\b/i,
    /\bco-?founder\b/i,
    /\bpresident\b/i,
    /\bmanaging (director|partner)\b/i,
    /\bgeneral counsel\b/i,
    /\bboard (member|director|advisor)\b/i,
  ]},
  { level: 'vp', patterns: [
    /\b(vice president|vp)\b/i,
    /\bhead of\b/i,
    /\bglobal (head|director)\b/i,
  ]},
  { level: 'director', patterns: [
    /\bdirector\b/i,
    /\b(gm|general manager)\b/i,
    /\bsenior director\b/i,
  ]},
  { level: 'manager', patterns: [
    /\bmanager\b/i,
    /\b(team )?lead\b/i,
  ]},
  { level: 'senior', patterns: [
    /\bsenior\b/i,
    /\bstaff\b/i,
    /\bsr\.?\s/i,
  ]},
  { level: 'mid', patterns: [
    /\bengineer\b/i,
    /\banalyst\b/i,
    /\bdesigner\b/i,
    /\bspecialist\b/i,
  ]},
  { level: 'junior', patterns: [
    /\bjunior\b/i,
    /\bgraduate\b/i,
    /\bintern\b/i,
    /\bentry.level\b/i,
    /\bassociate\b/i,
  ]},
];

// ── FUNCTION AREA CLASSIFICATION ────────────────────────────────────────────

const FUNCTION_PATTERNS = [
  { area: 'engineering',  patterns: [/engineer|developer|devops|platform|infrastructure|security|backend|frontend|fullstack|full.stack|sre|data engineer|ml engineer|ai engineer/i] },
  { area: 'data_science', patterns: [/data scien|machine learning|ml |ai research|deep learning/i] },
  { area: 'sales',        patterns: [/sales|account executive|\bae\b|\bbdr\b|\bsdr\b|business development|revenue/i] },
  { area: 'marketing',    patterns: [/market|growth|demand gen|brand|content|seo|performance market/i] },
  { area: 'finance',      patterns: [/financ|cfo|controller|accounting|treasury|fp&a|investor relation/i] },
  { area: 'people',       patterns: [/people|hr\b|human resources|talent|recruit|chro/i] },
  { area: 'operations',   patterns: [/operat|ops\b|supply chain|logistics|procurement/i] },
  { area: 'product',      patterns: [/product manager|product owner|\bpm\b|cpo|product lead/i] },
  { area: 'legal',        patterns: [/legal|counsel|compliance|regulatory|privacy officer/i] },
  { area: 'design',       patterns: [/design|ux|ui|creative director/i] },
  { area: 'executive',    patterns: [/\bceo\b|\bcoo\b|\bcto\b|\bcmo\b|\bcfo\b|president|founder/i] },
  { area: 'customer_success', patterns: [/customer success|customer support|client partner|account manag/i] },
];

function classifySeniority(title) {
  if (!title) return { seniority_level: 'unknown', function_area: 'other' };

  const seniority = SENIORITY_PATTERNS.find(p =>
    p.patterns.some(rx => rx.test(title))
  )?.level || 'unknown';

  const functionArea = FUNCTION_PATTERNS.find(p =>
    p.patterns.some(rx => rx.test(title))
  )?.area || 'other';

  return { seniority_level: seniority, function_area: functionArea };
}

// ═══════════════════════════════════════════════════════════════════════════════
// GEOGRAPHY EXTRACTION
// ═══════════════════════════════════════════════════════════════════════════════

const GEO_REGIONS = {
  // Countries — most specific first
  'singapore':       { tier: 'country',   canonical: 'Singapore' },
  'australia':       { tier: 'country',   canonical: 'Australia' },
  'new zealand':     { tier: 'country',   canonical: 'New Zealand' },
  'hong kong':       { tier: 'country',   canonical: 'Hong Kong' },
  'japan':           { tier: 'country',   canonical: 'Japan' },
  'korea':           { tier: 'country',   canonical: 'South Korea' },
  'china':           { tier: 'country',   canonical: 'China' },
  'india':           { tier: 'country',   canonical: 'India' },
  'indonesia':       { tier: 'country',   canonical: 'Indonesia' },
  'malaysia':        { tier: 'country',   canonical: 'Malaysia' },
  'thailand':        { tier: 'country',   canonical: 'Thailand' },
  'vietnam':         { tier: 'country',   canonical: 'Vietnam' },
  'philippines':     { tier: 'country',   canonical: 'Philippines' },
  'uk':              { tier: 'country',   canonical: 'United Kingdom' },
  'germany':         { tier: 'country',   canonical: 'Germany' },
  'france':          { tier: 'country',   canonical: 'France' },
  'canada':          { tier: 'country',   canonical: 'Canada' },
  'brazil':          { tier: 'country',   canonical: 'Brazil' },
  'mexico':          { tier: 'country',   canonical: 'Mexico' },
  'uae':             { tier: 'country',   canonical: 'UAE' },
  'saudi':           { tier: 'country',   canonical: 'Saudi Arabia' },
  'taiwan':          { tier: 'country',   canonical: 'Taiwan' },
  'netherlands':     { tier: 'country',   canonical: 'Netherlands' },
  'sweden':          { tier: 'country',   canonical: 'Sweden' },
  'israel':          { tier: 'country',   canonical: 'Israel' },

  // Sub-regions
  'southeast asia':  { tier: 'subregion', canonical: 'Southeast Asia' },
  'south asia':      { tier: 'subregion', canonical: 'South Asia' },
  'greater china':   { tier: 'subregion', canonical: 'Greater China' },
  'nordics':         { tier: 'subregion', canonical: 'Nordics' },
  'benelux':         { tier: 'subregion', canonical: 'Benelux' },
  'dach':            { tier: 'subregion', canonical: 'DACH' },
  'iberia':          { tier: 'subregion', canonical: 'Iberia' },
  'cee':             { tier: 'subregion', canonical: 'CEE' },
  'latam':           { tier: 'subregion', canonical: 'LatAm' },
  'latin america':   { tier: 'subregion', canonical: 'LatAm' },
  'mena':            { tier: 'subregion', canonical: 'MENA' },
  'anz':             { tier: 'subregion', canonical: 'ANZ' },
  'gcc':             { tier: 'subregion', canonical: 'GCC' },
  'sea':             { tier: 'subregion', canonical: 'Southeast Asia' },

  // Major regions
  'apac':            { tier: 'region',    canonical: 'APAC' },
  'asia pacific':    { tier: 'region',    canonical: 'APAC' },
  'asia-pacific':    { tier: 'region',    canonical: 'APAC' },
  'emea':            { tier: 'region',    canonical: 'EMEA' },
  'amer':            { tier: 'region',    canonical: 'Americas' },
  'americas':        { tier: 'region',    canonical: 'Americas' },
  'europe':          { tier: 'region',    canonical: 'Europe' },
  'asia':            { tier: 'region',    canonical: 'Asia' },
  'africa':          { tier: 'region',    canonical: 'Africa' },

  // Global
  'global':          { tier: 'global',    canonical: 'Global' },
  'worldwide':       { tier: 'global',    canonical: 'Global' },
  'international':   { tier: 'global',    canonical: 'Global' },
};

function extractTargetGeography(text) {
  const lower = text.toLowerCase();
  // Try most specific tier first (country → subregion → region → global)
  const tierOrder = ['country', 'subregion', 'region', 'global'];

  for (const tier of tierOrder) {
    for (const [key, value] of Object.entries(GEO_REGIONS)) {
      if (value.tier === tier && lower.includes(key)) {
        return { canonical: value.canonical, tier: value.tier };
      }
    }
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// GEO ROLE CLASS DETECTION
// ═══════════════════════════════════════════════════════════════════════════════

const GEO_ROLE_PATTERNS = [
  {
    class: 'country_manager',
    confidence: 0.95,
    patterns: [
      /\bcountry (manager|head|director|lead|gm|general manager)\b/i,
      /\b(head|director|gm) of (country|market)\b/i,
      /\bmarket (head|manager|director)\b/i,
    ],
  },
  {
    class: 'regional_csuite',
    confidence: 0.95,
    patterns: [
      /\bregional (ceo|chief executive|coo|chief operating|cfo|chief financial|cto|chief technology|cmo|chief marketing|cpo|chief people)\b/i,
      /\b(ceo|coo|cfo|cto|cmo)\b.{0,30}\b(apac|emea|sea|amer|latam|mena|asia|europe|americas)\b/i,
      /\b(apac|emea|sea|amer|latam|mena|asia|europe)\b.{0,30}\b(ceo|coo|cfo|cto|cmo)\b/i,
    ],
  },
  {
    class: 'regional_md',
    confidence: 0.92,
    patterns: [
      /\b(regional|area) managing director\b/i,
      /\bmanaging director.{0,40}\b(apac|emea|sea|region|asia|europe|latam|mena|anz|nordics|dach|southeast asia|greater china)\b/i,
      /\b(apac|emea|sea|asia|europe|latam|mena|anz|nordics|dach)\b.{0,40}\bmanaging director\b/i,
      /\bmd\b.{0,30}\b(apac|emea|sea|asia|europe)\b/i,
    ],
  },
  {
    class: 'regional_vp',
    confidence: 0.90,
    patterns: [
      /\b(regional|area) (vice president|vp)\b/i,
      /\bvp\b.{0,40}\b(apac|emea|sea|amer|latam|mena|asia|europe|americas|global|nordics|dach|anz)\b/i,
      /\b(apac|emea|sea|amer|latam|mena|asia|europe|nordics|dach|anz)\b.{0,40}\bvp\b/i,
      /\bvice president\b.{0,40}\b(apac|emea|sea|region|asia|europe|latam|mena)\b/i,
      /\b(apac|emea|sea|region|asia|europe|latam|mena)\b.{0,40}\bvice president\b/i,
    ],
  },
  {
    class: 'regional_director',
    confidence: 0.85,
    patterns: [
      /\b(regional|area|territory) director\b/i,
      /\bdirector\b.{0,40}\b(apac|emea|sea|amer|latam|mena|asia|europe|nordics|dach|anz|southeast asia|greater china)\b/i,
      /\b(apac|emea|sea|amer|latam|mena|asia|europe|nordics|dach|anz)\b.{0,40}\bdirector\b/i,
    ],
  },
  {
    class: 'head_of_region',
    confidence: 0.88,
    patterns: [
      /\bhead of (apac|emea|sea|amer|latam|mena|asia|asia.?pacific|europe|the americas|southeast asia|greater china|nordics|dach|anz|africa|latin america)\b/i,
      /\b(apac|emea|sea|amer|latam|mena|nordics|dach|anz)\b head\b/i,
      /\bregional head\b/i,
    ],
  },
  {
    class: 'market_entry',
    confidence: 0.80,
    patterns: [
      /\b(market entry|market expansion|expansion|launch) (manager|lead|director|head)\b/i,
      /\b(manager|lead|director|head).{0,20}(market entry|market expansion|new markets)\b/i,
      /\bfirst.hire\b/i,
    ],
  },
];

function detectGeoRole(title, location) {
  const combined = `${title} ${location || ''}`;

  for (const roleType of GEO_ROLE_PATTERNS) {
    for (const pattern of roleType.patterns) {
      if (pattern.test(combined)) {
        const geo = extractTargetGeography(combined);
        return {
          is_geo_expansion_role: true,
          geo_role_class: roleType.class,
          geo_confidence: roleType.confidence,
          target_geography: geo?.canonical || null,
          target_geo_tier: geo?.tier || null,
        };
      }
    }
  }

  return {
    is_geo_expansion_role: false,
    geo_role_class: null,
    geo_confidence: null,
    target_geography: null,
    target_geo_tier: null,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// COMBINED CLASSIFIER
// ═══════════════════════════════════════════════════════════════════════════════

function classifyJobPosting(title, location) {
  if (!title) return {
    seniority_level: 'unknown', function_area: 'other',
    is_geo_expansion_role: false, geo_role_class: null,
    target_geography: null, target_geo_tier: null,
  };

  const { seniority_level, function_area } = classifySeniority(title);
  const geoRole = detectGeoRole(title, location);

  // Geo roles override seniority when detection missed it —
  // a "Country Manager" is always leadership regardless of seniority match
  const effectiveSeniority = geoRole.is_geo_expansion_role
    ? (seniority_level === 'unknown' ? 'director' : seniority_level)
    : seniority_level;

  return {
    seniority_level: effectiveSeniority,
    function_area,
    is_geo_expansion_role: geoRole.is_geo_expansion_role,
    geo_role_class: geoRole.geo_role_class,
    target_geography: geoRole.target_geography,
    target_geo_tier: geoRole.target_geo_tier,
  };
}

module.exports = {
  classifyJobPosting, classifySeniority, detectGeoRole, extractTargetGeography,
  SENIORITY_PATTERNS, FUNCTION_PATTERNS, GEO_ROLE_PATTERNS, GEO_REGIONS,
};
