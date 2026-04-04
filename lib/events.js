// ═══════════════════════════════════════════════════════════════════════════════
// lib/events.js — EventMedium helpers: region bucketing + theme scoring
// ═══════════════════════════════════════════════════════════════════════════════

function bucketRegion(country) {
  const map = {
    AMER:   ['United States','Canada','Brazil','Mexico','Colombia','Argentina','Chile','Peru'],
    EUR:    ['United Kingdom','Germany','France','Netherlands','Sweden','Ireland','Spain','Italy','Denmark','Norway','Finland','Belgium','Switzerland','Portugal','Austria','Poland','Czech Republic','Greece'],
    MENA:   ['UAE','United Arab Emirates','Saudi Arabia','Qatar','Bahrain','Kuwait','Oman','Israel','Turkey','Egypt','Morocco','Tunisia','Jordan','Lebanon'],
    ASIA:   ['Singapore','Japan','Hong Kong','India','Indonesia','Malaysia','Thailand','Vietnam','Philippines','China','South Korea','Taiwan','Bangladesh','Pakistan','Sri Lanka','Myanmar','Cambodia','Nepal'],
    OCE:    ['Australia','New Zealand','Fiji','Papua New Guinea'],
    Africa: ['South Africa','Nigeria','Kenya','Ghana','Rwanda','Ethiopia'],
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
