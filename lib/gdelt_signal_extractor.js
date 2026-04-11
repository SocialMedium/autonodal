// ═══════════════════════════════════════════════════════════════════════════════
// lib/gdelt_signal_extractor.js — Signal extraction from GDELT articles
//
// GDELT articles already have theme codes that map to our signal taxonomy.
// This extractor uses those codes first, then falls back to title keyword matching.
// ═══════════════════════════════════════════════════════════════════════════════

const THEME_TO_SIGNAL = {
  // M&A
  'BUS_ACQUISITION':        { signal: 'ma_activity',          confidence: 0.88 },
  'BUS_MERGER':             { signal: 'ma_activity',          confidence: 0.88 },
  'BUS_MERGER_ACQUISITION': { signal: 'ma_activity',          confidence: 0.90 },
  'ECON_MA':                { signal: 'ma_activity',          confidence: 0.85 },

  // Capital raising
  'BUS_FUNDING_ROUND':      { signal: 'capital_raising',      confidence: 0.88 },
  'BUS_VENTURE_CAPITAL':    { signal: 'capital_raising',      confidence: 0.85 },
  'ECON_IPO':               { signal: 'capital_raising',      confidence: 0.90 },
  'ECON_FUNDING':           { signal: 'capital_raising',      confidence: 0.82 },
  'BUS_INVESTMENT':         { signal: 'capital_raising',      confidence: 0.80 },

  // Leadership
  'BUS_CEO_CHANGE':         { signal: 'leadership_change',    confidence: 0.90 },
  'BUS_APPOINTMENT':        { signal: 'leadership_change',    confidence: 0.85 },
  'BUS_RESIGNATION':        { signal: 'leadership_change',    confidence: 0.88 },
  'BUS_EXECUTIVE_CHANGE':   { signal: 'leadership_change',    confidence: 0.85 },

  // Distress
  'BUS_LAYOFFS':            { signal: 'restructuring',        confidence: 0.88 },
  'ECON_LAYOFFS':           { signal: 'restructuring',        confidence: 0.85 },
  'BUS_RESTRUCTURING':      { signal: 'restructuring',        confidence: 0.85 },
  'BUS_BANKRUPTCY':         { signal: 'restructuring',        confidence: 0.92 },
  'BUS_DOWNSIZING':         { signal: 'restructuring',        confidence: 0.82 },

  // Expansion
  'BUS_MARKET_EXPANSION':   { signal: 'geographic_expansion', confidence: 0.82 },
  'BUS_OPENING':            { signal: 'geographic_expansion', confidence: 0.80 },
  'ECON_GEOGRAPHIC_EXPANSION': { signal: 'geographic_expansion', confidence: 0.85 },
  'BUS_NEW_OFFICE':         { signal: 'geographic_expansion', confidence: 0.82 },

  // Partnership
  'BUS_PARTNERSHIP':        { signal: 'partnership',          confidence: 0.80 },
  'BUS_JOINT_VENTURE':      { signal: 'partnership',          confidence: 0.82 },
  'BUS_ALLIANCE':           { signal: 'partnership',          confidence: 0.78 },

  // Product
  'BUS_PRODUCT_LAUNCH':     { signal: 'product_launch',       confidence: 0.82 },
  'BUS_NEW_PRODUCT':        { signal: 'product_launch',       confidence: 0.80 },
  'TECH_LAUNCH':            { signal: 'product_launch',       confidence: 0.78 },

  // Hiring
  'BUS_HIRING':             { signal: 'strategic_hiring',     confidence: 0.75 },
  'ECON_JOB_CREATION':      { signal: 'strategic_hiring',     confidence: 0.72 },
  'BUS_EMPLOYMENT':         { signal: 'strategic_hiring',     confidence: 0.70 },
};

// Title keyword patterns as fallback
const TITLE_PATTERNS = [
  { pattern: /\bacquir|merger|acquisition\b/i,                           signal: 'ma_activity',          confidence: 0.75 },
  { pattern: /\braises?\s+\$|funding round|series [A-Z]\b/i,            signal: 'capital_raising',      confidence: 0.80 },
  { pattern: /\bappointed|names?\s+new|joins?\s+as\b/i,                 signal: 'leadership_change',    confidence: 0.75 },
  { pattern: /\blayoffs?|redundanc|job cuts|workforce reduction\b/i,     signal: 'restructuring',        confidence: 0.78 },
  { pattern: /\bexpands? to|opens? (?:office|hub)|enters? market\b/i,   signal: 'geographic_expansion', confidence: 0.75 },
  { pattern: /\bcountry manager|regional director|head of [A-Z]+\b/i,   signal: 'geographic_expansion', confidence: 0.85 },
  { pattern: /\blaunches?|introduces?|unveils?\b/i,                      signal: 'product_launch',       confidence: 0.70 },
  { pattern: /\bpartners? with|partnership|alliance\b/i,                 signal: 'partnership',          confidence: 0.72 },
  { pattern: /\brestructur|downsiz|cost.?cutting\b/i,                    signal: 'restructuring',        confidence: 0.75 },
];

/**
 * Extract signal type and confidence from a GDELT document.
 * @param {object} doc — external_documents row with gdelt fields
 * @returns {object|null} { signal_type, confidence, detection_method, theme_code }
 */
function extractSignalFromGDELT(doc) {
  // 1. Theme code matching (most reliable)
  if (doc.gdelt_themes && doc.gdelt_themes.length > 0) {
    for (const theme of doc.gdelt_themes) {
      const mapped = THEME_TO_SIGNAL[theme];
      if (mapped) {
        var toneMultiplier = (doc.gdelt_tone || 0) < -3 ? 0.9 : 1.0;
        return {
          signal_type: mapped.signal,
          confidence: parseFloat((mapped.confidence * toneMultiplier).toFixed(3)),
          detection_method: 'gdelt_theme',
          theme_code: theme,
        };
      }
    }
  }

  // 2. Title pattern fallback
  var title = String(doc.title || '');
  for (var i = 0; i < TITLE_PATTERNS.length; i++) {
    if (TITLE_PATTERNS[i].pattern.test(title)) {
      return {
        signal_type: TITLE_PATTERNS[i].signal,
        confidence: TITLE_PATTERNS[i].confidence,
        detection_method: 'title_pattern',
        theme_code: null,
      };
    }
  }

  return null;
}

function interpretTone(tone) {
  if (tone > 5) return 'positive coverage';
  if (tone < -5) return 'negative coverage';
  return 'neutral coverage';
}

function buildGDELTEvidence(doc, signal) {
  var lang = doc.source_language !== 'en' ? ' [' + (doc.source_language || '').toUpperCase() + ' source]' : '';
  var tone = interpretTone(doc.gdelt_tone || 0);
  var method = signal.detection_method === 'gdelt_theme'
    ? 'GDELT theme: ' + signal.theme_code
    : 'keyword pattern match';
  return String(doc.title || '') + lang + ' — ' + tone + '. Detected via ' + method + '. Source: ' + (doc.source || 'unknown') + '.';
}

module.exports = { extractSignalFromGDELT, buildGDELTEvidence, THEME_TO_SIGNAL, TITLE_PATTERNS };
