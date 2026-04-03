// ═══════════════════════════════════════════════════════════════════════════════
// lib/events.js — EventMedium helpers: region bucketing + theme scoring
// ═══════════════════════════════════════════════════════════════════════════════

function bucketRegion(country) {
  const map = {
    ANZ:    ['Australia','New Zealand'],
    Asia:   ['Singapore','Japan','Hong Kong','India','Indonesia','Malaysia','Thailand','Vietnam','Philippines'],
    Europe: ['United Kingdom','Germany','France','Netherlands','Sweden','Ireland','Spain','Italy','Denmark','Norway','Finland','Belgium','Switzerland'],
    US:     ['United States','Canada'],
    Africa: ['South Africa','Nigeria','Kenya','Ghana','Egypt','Rwanda','Ethiopia']
  };
  for (const [region, countries] of Object.entries(map)) {
    if (countries.includes(country)) return region;
  }
  return 'Global';
}

const ML_THEMES = [
  'AI','Fintech','SaaS','Enterprise SaaS','Web3',
  'Health','Cybersecurity','Climate','Quantum','Robotics','Defence'
];

function scoreThemeRelevance(eventThemes = []) {
  if (!eventThemes.length) return 0;
  const matches = eventThemes.filter(t => ML_THEMES.includes(t)).length;
  return parseFloat((matches / ML_THEMES.length).toFixed(3));
}

module.exports = { bucketRegion, scoreThemeRelevance, ML_THEMES };
