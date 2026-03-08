// ═══════════════════════════════════════════════════════════════════════════════
// lib/signal_keywords.js - Signal Detection Patterns
// ═══════════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL PATTERNS
// ═══════════════════════════════════════════════════════════════════════════════

const SIGNAL_PATTERNS = {
  // Capital Raising
  capital_raising: {
    keywords: [
      'raised', 'raises', 'raising', 'funding', 'funded',
      'series a', 'series b', 'series c', 'series d', 'series e',
      'seed round', 'pre-seed', 'seed funding',
      'investment', 'invested', 'investor', 'investors',
      'venture', 'vc', 'venture capital',
      'million', 'billion', 'valuation',
      'led by', 'participated',
      'growth equity', 'private equity', 'pe investment',
      'capital injection', 'financing round'
    ],
    phrases: [
      'raised \\$[\\d,.]+ million',
      'raised \\$[\\d,.]+ billion',
      'series [a-e] funding',
      'secures \\$[\\d,.]+ million',
      'closes \\$[\\d,.]+ round',
      'funding round',
      'investment round',
      'announces \\$[\\d,.]+ funding'
    ],
    requiredContext: ['million', 'billion', 'funding', 'round', 'investment', 'series'],
    baseConfidence: 0.7
  },

  // Geographic Expansion
  geographic_expansion: {
    keywords: [
      'expands', 'expanding', 'expansion',
      'enters', 'entering', 'entry',
      'opens', 'opening', 'opened',
      'launches in', 'launched in',
      'new market', 'new region', 'new territory',
      'headquarters', 'hq', 'office',
      'international', 'global', 'worldwide',
      'apac', 'emea', 'latam', 'americas'
    ],
    phrases: [
      'expands (into|to) [A-Z][a-z]+',
      'opens (new )?office in',
      'enters [A-Z][a-z]+ market',
      'launches in [A-Z][a-z]+',
      'expanding (into|to|across)',
      'international expansion',
      'global expansion'
    ],
    geographies: [
      'europe', 'asia', 'america', 'africa', 'middle east',
      'uk', 'germany', 'france', 'spain', 'italy', 'netherlands',
      'china', 'japan', 'india', 'singapore', 'australia',
      'usa', 'us', 'canada', 'brazil', 'mexico'
    ],
    baseConfidence: 0.6
  },

  // Strategic Hiring
  strategic_hiring: {
    keywords: [
      'appoints', 'appointed', 'appointment',
      'names', 'named', 'naming',
      'hires', 'hired', 'hiring',
      'joins', 'joined', 'joining',
      'welcomes', 'welcomed',
      'promotes', 'promoted', 'promotion'
    ],
    titles: [
      'ceo', 'cfo', 'cto', 'coo', 'cmo', 'cro', 'cpo', 'ciso',
      'chief executive', 'chief financial', 'chief technology',
      'chief operating', 'chief marketing', 'chief revenue',
      'chief product', 'chief people', 'chief information',
      'president', 'vice president', 'vp', 'svp', 'evp',
      'managing director', 'general manager', 'gm',
      'head of', 'director of', 'senior director',
      'partner', 'general partner', 'managing partner',
      'board', 'board member', 'board of directors'
    ],
    phrases: [
      'appoints .+ as (CEO|CFO|CTO|COO|CMO|CRO)',
      'names .+ (CEO|CFO|CTO|president)',
      'hired .+ to lead',
      'joins as (CEO|CFO|CTO|COO|CMO)',
      'new (CEO|CFO|CTO|president)',
      'promoted to (CEO|CFO|CTO|VP)'
    ],
    baseConfidence: 0.75
  },

  // M&A Activity
  ma_activity: {
    keywords: [
      'acquires', 'acquired', 'acquisition',
      'merger', 'merges', 'merged',
      'buys', 'bought', 'purchase', 'purchased',
      'takeover', 'takes over',
      'deal', 'transaction',
      'divests', 'divestiture', 'divested',
      'spin-off', 'spinoff', 'spins off',
      'carve-out', 'carveout'
    ],
    phrases: [
      'acquires .+ for \\$[\\d,.]+ (million|billion)',
      'to acquire',
      'acquisition of',
      'merger with',
      'agrees to buy',
      'completes acquisition',
      'announces acquisition',
      'strategic acquisition'
    ],
    baseConfidence: 0.8
  },

  // Partnership
  partnership: {
    keywords: [
      'partnership', 'partners', 'partnered', 'partnering',
      'collaboration', 'collaborates', 'collaborating',
      'alliance', 'allied',
      'joint venture', 'jv',
      'strategic agreement',
      'integration', 'integrates', 'integrated',
      'teams up', 'teaming up'
    ],
    phrases: [
      'partners with',
      'announces partnership',
      'strategic partnership',
      'enters partnership',
      'collaboration with',
      'joint venture with',
      'integrates with'
    ],
    baseConfidence: 0.6
  },

  // Product Launch
  product_launch: {
    keywords: [
      'launches', 'launched', 'launching', 'launch',
      'introduces', 'introduced', 'introducing',
      'unveils', 'unveiled', 'unveiling',
      'announces', 'announced',
      'releases', 'released', 'release',
      'rolls out', 'rollout',
      'debuts', 'debuted',
      'new product', 'new platform', 'new solution',
      'new feature', 'new service'
    ],
    phrases: [
      'launches (new|its)',
      'introduces (new|its)',
      'unveils (new|its)',
      'announces (new|its)',
      'product launch',
      'platform launch',
      'general availability',
      'now available'
    ],
    baseConfidence: 0.5
  },

  // Leadership Change
  leadership_change: {
    keywords: [
      'steps down', 'stepping down',
      'departs', 'departed', 'departure',
      'leaves', 'left', 'leaving',
      'resigns', 'resigned', 'resignation',
      'retires', 'retired', 'retirement',
      'exits', 'exit',
      'transition', 'transitions', 'transitioning',
      'succeeds', 'succeeded', 'succession'
    ],
    phrases: [
      '(CEO|CFO|CTO|president) (steps down|departs|leaves|resigns)',
      'announces (departure|retirement|resignation)',
      'leadership transition',
      'executive transition',
      'steps down as (CEO|CFO|CTO)'
    ],
    baseConfidence: 0.7
  },

  // Layoffs / Restructuring
  layoffs: {
    keywords: [
      'layoffs', 'layoff', 'laid off', 'laying off',
      'cuts', 'cutting', 'cut jobs',
      'reduces', 'reducing', 'reduction',
      'downsizes', 'downsizing', 'downsize',
      'eliminates', 'eliminating', 'elimination',
      'workforce reduction',
      'headcount reduction',
      'job cuts', 'staff cuts'
    ],
    phrases: [
      'lays off \\d+',
      'cuts \\d+ (jobs|employees|staff|positions)',
      'reduces (workforce|headcount) by',
      'eliminates \\d+ (jobs|positions|roles)',
      'workforce reduction',
      'announces layoffs'
    ],
    baseConfidence: 0.75
  },

  // Restructuring
  restructuring: {
    keywords: [
      'restructures', 'restructured', 'restructuring',
      'reorganizes', 'reorganized', 'reorganization',
      'transforms', 'transformation',
      'overhauls', 'overhauled', 'overhaul',
      'pivots', 'pivoted', 'pivot',
      'turnaround',
      'cost cutting', 'cost reduction'
    ],
    phrases: [
      'announces restructuring',
      'organizational restructuring',
      'strategic restructuring',
      'business transformation',
      'operational restructuring'
    ],
    baseConfidence: 0.65
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
// KNOWN ENTITIES
// ═══════════════════════════════════════════════════════════════════════════════

const KNOWN_INVESTORS = [
  'sequoia', 'sequoia capital',
  'andreessen horowitz', 'a16z',
  'accel', 'accel partners',
  'benchmark', 'benchmark capital',
  'greylock', 'greylock partners',
  'kleiner perkins', 'kpcb',
  'lightspeed', 'lightspeed venture',
  'index ventures',
  'general catalyst',
  'bessemer', 'bessemer venture',
  'insight partners',
  'tiger global',
  'coatue',
  'softbank', 'softbank vision',
  'y combinator', 'yc',
  'founders fund',
  'kkr', 'blackstone', 'carlyle',
  'warburg pincus', 'tpg', 'advent',
  'general atlantic', 'silver lake'
];

const TITLE_PATTERNS = [
  /\b(CEO|CFO|CTO|COO|CMO|CRO|CPO|CISO)\b/i,
  /\bChief\s+\w+\s+Officer\b/i,
  /\b(President|Vice\s+President|VP|SVP|EVP)\b/i,
  /\b(Managing\s+Director|General\s+Manager)\b/i,
  /\b(Head\s+of|Director\s+of)\s+\w+/i,
  /\b(Partner|General\s+Partner|Managing\s+Partner)\b/i,
  /\bBoard\s+(Member|Director)\b/i
];

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL DETECTION FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Detect signals in text
 * @param {string} text - Text to analyze
 * @returns {Array} Detected signals with scores
 */
function detectSignals(text) {
  if (!text) return [];
  
  const normalizedText = text.toLowerCase();
  const signals = [];
  
  for (const [signalType, pattern] of Object.entries(SIGNAL_PATTERNS)) {
    const result = detectSignalType(text, normalizedText, signalType, pattern);
    if (result) {
      signals.push(result);
    }
  }
  
  return signals.sort((a, b) => b.confidence - a.confidence);
}

/**
 * Detect a specific signal type
 */
function detectSignalType(originalText, normalizedText, signalType, pattern) {
  let score = 0;
  const evidence = [];
  const matches = [];
  
  // Check keywords
  for (const keyword of pattern.keywords) {
    const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
    const keywordMatches = originalText.match(regex);
    if (keywordMatches) {
      score += 0.1 * keywordMatches.length;
      matches.push(...keywordMatches);
    }
  }
  
  // Check phrases (higher weight)
  if (pattern.phrases) {
    for (const phrase of pattern.phrases) {
      const regex = new RegExp(phrase, 'gi');
      const phraseMatches = originalText.match(regex);
      if (phraseMatches) {
        score += 0.3 * phraseMatches.length;
        evidence.push(...phraseMatches);
      }
    }
  }
  
  // Check required context
  if (pattern.requiredContext) {
    const hasContext = pattern.requiredContext.some(ctx => 
      normalizedText.includes(ctx.toLowerCase())
    );
    if (!hasContext) {
      score *= 0.3; // Reduce score if no context
    }
  }
  
  // Check title patterns for hiring signals
  if (signalType === 'strategic_hiring' && pattern.titles) {
    for (const title of pattern.titles) {
      if (normalizedText.includes(title.toLowerCase())) {
        score += 0.2;
        evidence.push(title);
      }
    }
    
    // Check regex patterns
    for (const titlePattern of TITLE_PATTERNS) {
      const titleMatches = originalText.match(titlePattern);
      if (titleMatches) {
        score += 0.25;
        evidence.push(...titleMatches);
      }
    }
  }
  
  // Check for investors in capital raising
  if (signalType === 'capital_raising') {
    for (const investor of KNOWN_INVESTORS) {
      if (normalizedText.includes(investor.toLowerCase())) {
        score += 0.15;
        evidence.push(`Investor: ${investor}`);
      }
    }
  }
  
  // Calculate final confidence
  const confidence = Math.min(
    pattern.baseConfidence + (score * 0.1),
    0.95
  );
  
  // Return null if below threshold
  if (confidence < 0.4 || matches.length === 0) {
    return null;
  }
  
  // Extract evidence snippet
  const evidenceSnippet = extractEvidenceSnippet(originalText, matches[0]);
  
  return {
    signal_type: signalType,
    confidence: Math.round(confidence * 100) / 100,
    matches: [...new Set(matches)],
    evidence: [...new Set(evidence)],
    snippet: evidenceSnippet
  };
}

/**
 * Extract a snippet around the evidence
 */
function extractEvidenceSnippet(text, match, contextLength = 150) {
  const index = text.toLowerCase().indexOf(match.toLowerCase());
  if (index === -1) return text.substring(0, 300);
  
  const start = Math.max(0, index - contextLength);
  const end = Math.min(text.length, index + match.length + contextLength);
  
  let snippet = text.substring(start, end);
  
  if (start > 0) snippet = '...' + snippet;
  if (end < text.length) snippet = snippet + '...';
  
  return snippet;
}

/**
 * Extract company names from text
 * @param {string} text - Text to analyze
 * @returns {Array} Potential company names
 */
function extractCompanyNames(text) {
  const companies = new Set();
  
  // Pattern: "CompanyName, Inc." or "CompanyName Inc"
  const incPattern = /([A-Z][a-zA-Z0-9\s&.]+?)(?:,?\s*(?:Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Company|Co\.?))/g;
  let match;
  while ((match = incPattern.exec(text)) !== null) {
    companies.add(match[1].trim());
  }
  
  // Pattern: "announced by CompanyName" or "CompanyName announced"
  const actionPattern = /(?:^|\.\s+)([A-Z][a-zA-Z0-9\s&]+?)\s+(?:announces?d?|raises?d?|launches?d?|acquires?d?|partners?)/g;
  while ((match = actionPattern.exec(text)) !== null) {
    const name = match[1].trim();
    if (name.split(' ').length <= 4) {
      companies.add(name);
    }
  }
  
  return Array.from(companies);
}

/**
 * Extract monetary amounts
 */
function extractAmounts(text) {
  const amounts = [];
  
  // Pattern: $X million/billion
  const amountPattern = /\$([0-9,]+(?:\.[0-9]+)?)\s*(million|billion|M|B)/gi;
  let match;
  while ((match = amountPattern.exec(text)) !== null) {
    const value = parseFloat(match[1].replace(/,/g, ''));
    const unit = match[2].toLowerCase();
    const multiplier = (unit === 'billion' || unit === 'b') ? 1000000000 : 1000000;
    amounts.push({
      raw: match[0],
      value: value * multiplier,
      formatted: match[0]
    });
  }
  
  return amounts;
}

/**
 * Score a document for signal relevance
 */
function scoreDocument(document) {
  const text = `${document.title || ''} ${document.content || ''}`;
  const signals = detectSignals(text);
  
  if (signals.length === 0) {
    return { score: 0, signals: [] };
  }
  
  // Calculate composite score
  const maxConfidence = Math.max(...signals.map(s => s.confidence));
  const signalCount = signals.length;
  const score = maxConfidence * (1 + (signalCount - 1) * 0.1);
  
  return {
    score: Math.min(score, 1),
    signals,
    companies: extractCompanyNames(text),
    amounts: extractAmounts(text)
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PERSON SIGNAL DETECTION
// ═══════════════════════════════════════════════════════════════════════════════

const PERSON_SIGNAL_PATTERNS = {
  new_role: {
    phrases: [
      'joins .+ as',
      'appointed .+ at',
      'named .+ at',
      'hired as',
      'new role as',
      'started as'
    ],
    baseConfidence: 0.7
  },
  promotion: {
    phrases: [
      'promoted to',
      'elevated to',
      'now .+ at',
      'takes on .+ role'
    ],
    baseConfidence: 0.65
  },
  speaking_engagement: {
    keywords: ['keynote', 'speaker', 'panelist', 'presenter', 'moderator', 'summit', 'conference'],
    baseConfidence: 0.6
  },
  publication: {
    keywords: ['published', 'authored', 'wrote', 'article', 'paper', 'book'],
    baseConfidence: 0.55
  }
};

/**
 * Detect person-related signals in text
 */
function detectPersonSignals(text, personName) {
  if (!text || !personName) return [];
  
  const signals = [];
  const normalizedText = text.toLowerCase();
  const normalizedName = personName.toLowerCase();
  
  // Check if person is mentioned
  if (!normalizedText.includes(normalizedName)) {
    return [];
  }
  
  for (const [signalType, pattern] of Object.entries(PERSON_SIGNAL_PATTERNS)) {
    let score = 0;
    const evidence = [];
    
    if (pattern.keywords) {
      for (const keyword of pattern.keywords) {
        if (normalizedText.includes(keyword)) {
          score += 0.2;
          evidence.push(keyword);
        }
      }
    }
    
    if (pattern.phrases) {
      for (const phrase of pattern.phrases) {
        const regex = new RegExp(phrase, 'gi');
        if (regex.test(text)) {
          score += 0.3;
          evidence.push(phrase);
        }
      }
    }
    
    if (score > 0) {
      const confidence = Math.min(pattern.baseConfidence + score, 0.95);
      signals.push({
        signal_type: signalType,
        confidence,
        evidence
      });
    }
  }
  
  return signals;
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  SIGNAL_PATTERNS,
  KNOWN_INVESTORS,
  TITLE_PATTERNS,
  detectSignals,
  detectSignalType,
  extractCompanyNames,
  extractAmounts,
  scoreDocument,
  detectPersonSignals,
  extractEvidenceSnippet
};
