// lib/proximity-graph.js
// Utility functions for building proximity graph payloads

const TEAM_COLORS = [
  { fill: '#5DCAA5', stroke: '#1D9E75', text: '#04342C' },
  { fill: '#F0997B', stroke: '#D85A30', text: '#4A1B0C' },
  { fill: '#AFA9EC', stroke: '#7F77DD', text: '#26215C' },
  { fill: '#FAC775', stroke: '#EF9F27', text: '#412402' },
  { fill: '#85B7EB', stroke: '#378ADD', text: '#042C53' },
];

const SIGNAL_COLORS = {
  capital_raising: '#EF9F27', strategic_hiring: '#5DCAA5',
  geographic_expansion: '#378ADD', leadership_change: '#D4537E',
  ma_activity: '#AFA9EC', product_launch: '#97C459',
  layoffs: '#E24B4A', restructuring: '#E24B4A', partnership: '#85B7EB',
};

const SIGNAL_LABELS = {
  capital_raising: 'Funding', strategic_hiring: 'Hiring',
  geographic_expansion: 'Expansion', leadership_change: 'Leadership change',
  ma_activity: 'M&A', product_launch: 'Product launch',
  layoffs: 'Layoffs', restructuring: 'Restructuring', partnership: 'Partnership',
};

function getTeamColor(index) { return TEAM_COLORS[index % TEAM_COLORS.length]; }
function getSignalColor(type) { return SIGNAL_COLORS[type] || '#888888'; }
function getSignalLabel(type) { return SIGNAL_LABELS[type] || type.replace(/_/g, ' '); }

module.exports = { getTeamColor, getSignalColor, getSignalLabel, TEAM_COLORS, SIGNAL_COLORS, SIGNAL_LABELS };
