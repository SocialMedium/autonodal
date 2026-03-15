// lib/terminology.js
// Terminology map — vertical drives all UI labels.
// Never use hardcoded strings in UI components or API responses.
// Always reference t.opportunity, t.person, t.win etc.

const TERMINOLOGY = {
  talent: {
    person: 'Candidate', persons: 'Candidates',
    opportunity: 'Search', opportunities: 'Searches',
    account: 'Client', accounts: 'Clients',
    engagement: 'Project', engagements: 'Projects',
    win: 'Placement', wins: 'Placements', win_rate: 'Fill Rate',
    conversion: 'Placement', conversions: 'Placements',
    owner: 'Lead Consultant', owners: 'Consultants',
    brief: 'Search Brief', pipeline: 'Search Pipeline',
    stage: 'Search Stage', stages: 'Search Stages',
    shortlist: 'Shortlist', qualified: 'Longlisted',
    identified: 'Identified', approached: 'Approached',
    interested: 'Interested', presented: 'Presented',
    proximity: 'Team Proximity', warm_path: 'Warm Introduction',
    going_cold: 'Disengaged', revival: 'Re-engagement',
    in_market: 'Open to Opportunities',
    signal_triage: 'Candidate Relevance', outreach: 'Approach', touchpoint: 'Touchpoint',
    receptivity: 'Candidate Receptivity', flight_risk: 'Flight Risk',
    timing: 'Timing Score', engagement: 'Engagement Score',
    activity: 'Activity Score', market_heat: 'Market Heat',
    relationship_strength: 'Relationship Strength', ml_alignment: 'ML Alignment',
    add_to_pipeline: 'Add to Search', contact: 'Approach', qualify: 'Longlist', convert: 'Place',
    no_opportunities: 'No active searches', no_persons: 'No candidates yet', no_conversions: 'No placements yet',
  },
  revenue: {
    person: 'Prospect', persons: 'Prospects',
    opportunity: 'Deal', opportunities: 'Deals',
    account: 'Account', accounts: 'Accounts',
    engagement: 'Campaign', engagements: 'Campaigns',
    win: 'Closed-Won', wins: 'Closed Deals', win_rate: 'Win Rate',
    conversion: 'Closed-Won', conversions: 'Closed Deals',
    owner: 'Account Executive', owners: 'Account Executives',
    brief: 'Deal Memo', pipeline: 'Deal Pipeline',
    stage: 'Deal Stage', stages: 'Deal Stages',
    shortlist: 'Qualified', qualified: 'MQL',
    identified: 'Identified', approached: 'Contacted',
    interested: 'Engaged', presented: 'Proposal Sent',
    proximity: 'Relationship Score', warm_path: 'Warm Intro',
    going_cold: 'Churning', revival: 'Pipeline Revival',
    in_market: 'In-Market',
    signal_triage: 'Lead Qualification', outreach: 'Outreach', touchpoint: 'Touch',
    receptivity: 'Buyer Intent', flight_risk: 'Churn Signal',
    timing: 'Intent Score', engagement: 'Engagement Score',
    activity: 'Activity Score', market_heat: 'Market Activity',
    relationship_strength: 'Relationship Strength', ml_alignment: 'ICP Alignment',
    add_to_pipeline: 'Add to Pipeline', contact: 'Reach Out', qualify: 'Qualify', convert: 'Close',
    no_opportunities: 'No active deals', no_persons: 'No prospects yet', no_conversions: 'No closed deals yet',
  },
  mandate: {
    person: 'Contact', persons: 'Contacts',
    opportunity: 'Mandate', opportunities: 'Mandates',
    account: 'Client', accounts: 'Clients',
    engagement: 'Engagement', engagements: 'Engagements',
    win: 'Signed Mandate', wins: 'Signed Mandates', win_rate: 'Conversion Rate',
    conversion: 'Signed Mandate', conversions: 'Signed Mandates',
    owner: 'Partner', owners: 'Partners',
    brief: 'Scope of Work', pipeline: 'Mandate Pipeline',
    stage: 'Mandate Stage', stages: 'Mandate Stages',
    shortlist: 'Shortlist', qualified: 'Assessed',
    identified: 'Identified', approached: 'Approached',
    interested: 'Scoped', presented: 'Proposal Submitted',
    proximity: 'Network Proximity', warm_path: 'Referral Path',
    going_cold: 'Going Dark', revival: 'Mandate Reactivation',
    in_market: 'Actively Seeking',
    signal_triage: 'Opportunity Assessment', outreach: 'Business Development', touchpoint: 'Touchpoint',
    receptivity: 'Engagement Readiness', flight_risk: 'Relationship Risk',
    timing: 'Readiness Score', engagement: 'Engagement Score',
    activity: 'Visibility Score', market_heat: 'Market Activity',
    relationship_strength: 'Relationship Depth', ml_alignment: 'Practice Alignment',
    add_to_pipeline: 'Add to Mandate', contact: 'Initiate Contact', qualify: 'Assess', convert: 'Sign Mandate',
    no_opportunities: 'No active mandates', no_persons: 'No contacts yet', no_conversions: 'No signed mandates yet',
  }
};

const SIGNAL_LABELS = {
  talent: {
    capital_raising: 'Funding Signal — likely to grow headcount',
    geographic_expansion: 'Expansion — likely to hire locally',
    strategic_hiring: 'Hiring Signal — active talent acquisition',
    ma_activity: 'M&A Activity — leadership change likely',
    partnership: 'Partnership — potential headcount growth',
    product_launch: 'Product Launch — may need specialist talent',
    leadership_change: 'Leadership Change — backfill or restructure likely',
    layoffs: 'Distress Signal — talent may be available or receptive',
    restructuring: 'Restructuring — role changes likely',
  },
  revenue: {
    capital_raising: 'Funding Signal — new budget likely available',
    geographic_expansion: 'Expansion — new territory, potential new account',
    strategic_hiring: 'Hiring Signal — company is scaling, buying intent up',
    ma_activity: 'M&A Activity — champion may change, re-qualify',
    partnership: 'Partnership Signal — ecosystem opportunity',
    product_launch: 'Product Launch — new use case or budget unlocked',
    leadership_change: 'Champion Change — re-engage with new stakeholder',
    layoffs: 'Distress Signal — cost pressure, price sensitivity',
    restructuring: 'Restructuring — re-evaluate account status',
  },
  mandate: {
    capital_raising: 'Funding Event — M&A or advisory mandate likely',
    geographic_expansion: 'Expansion — entry strategy or local advisory needed',
    strategic_hiring: 'Talent Advisory Signal — executive hire mandate possible',
    ma_activity: 'M&A Activity — transaction advisory mandate likely',
    partnership: 'Partnership Signal — advisory or facilitation mandate',
    product_launch: 'Launch Signal — go-to-market advisory opportunity',
    leadership_change: 'Leadership Change — advisory or transition mandate',
    layoffs: 'Restructuring — potential advisory engagement',
    restructuring: 'Restructuring — advisory mandate likely',
  }
};

function getTerminology(vertical) {
  return TERMINOLOGY[vertical] || TERMINOLOGY.talent;
}

function getSignalLabel(signalType, vertical) {
  const labels = SIGNAL_LABELS[vertical] || SIGNAL_LABELS.talent;
  return labels[signalType] || signalType.replace(/_/g, ' ');
}

module.exports = { getTerminology, getSignalLabel, TERMINOLOGY, SIGNAL_LABELS };
