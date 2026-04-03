// ═══════════════════════════════════════════════════════════════════════════════
// lib/onboarding/intentTaxonomy.js — Intent Classification for Onboarding
// ═══════════════════════════════════════════════════════════════════════════════

var INTENT_TAXONOMY = {

  talent_sourcing: {
    keywords: ['hire', 'recruit', 'talent', 'find people', 'team',
               'headhunt', 'search', 'candidates', 'staffing'],
    vertical: 'talent',
    signal_priorities: ['leadership_change', 'company_exit', 'strategic_hiring', 'company_founded'],
    nev_canister_type: 'talent_sourcing',
    label: 'Finding & hiring talent',
  },

  job_seeking: {
    keywords: ['job', 'role', 'position', 'opportunity', 'career',
               'looking for work', 'next move', 'open to'],
    vertical: 'talent',
    signal_priorities: ['capital_raising', 'strategic_hiring', 'geographic_expansion', 'product_launch'],
    nev_canister_type: 'job_seeking',
    label: 'Finding a new role',
  },

  raising_capital: {
    keywords: ['raise', 'raising', 'fundraise', 'seed', 'series',
               'investors', 'capital', 'funding round', 'pitch'],
    vertical: 'revenue',
    signal_priorities: ['capital_raising', 'partnership', 'geographic_expansion'],
    nev_canister_type: 'raising_capital',
    label: 'Raising capital',
  },

  investing: {
    keywords: ['invest', 'vc', 'venture', 'fund', 'portfolio',
               'deal flow', 'angel', 'scout', 'thesis',
               'private equity', 'pe', 'backing'],
    vertical: 'revenue',
    signal_priorities: ['capital_raising', 'product_launch', 'leadership_change',
                        'geographic_expansion', 'company_founded'],
    nev_canister_type: 'deal_flow',
    label: 'Deal flow & investment',
  },

  sales_growth: {
    keywords: ['customers', 'clients', 'sales', 'revenue', 'sell',
               'bd', 'business development', 'pipeline', 'leads',
               'accounts', 'enterprise'],
    vertical: 'revenue',
    signal_priorities: ['capital_raising', 'geographic_expansion', 'leadership_change',
                        'product_launch', 'strategic_hiring'],
    nev_canister_type: 'customer_acquisition',
    label: 'Finding customers',
  },

  partnerships: {
    keywords: ['partner', 'partnership', 'collaborate', 'alliance',
               'strategic', 'joint venture', 'distribution',
               'channel', 'integration'],
    vertical: 'revenue',
    signal_priorities: ['partnership', 'product_launch', 'geographic_expansion', 'capital_raising'],
    nev_canister_type: 'partnership_seeking',
    label: 'Building partnerships',
  },

  co_founding: {
    keywords: ['co-founder', 'cofounder', 'founding team',
               'technical co-founder', 'building a startup',
               'starting a company', 'looking for a partner to build'],
    vertical: 'revenue',
    signal_priorities: ['company_founded', 'leadership_change', 'company_exit', 'capital_raising'],
    nev_canister_type: 'co_founder_seeking',
    label: 'Finding co-founders',
  },

  advisory: {
    keywords: ['advisor', 'mentor', 'board', 'guidance', 'expertise',
               'network', 'introductions', 'connect me with'],
    vertical: 'mandate',
    signal_priorities: ['leadership_change', 'capital_raising', 'geographic_expansion'],
    nev_canister_type: 'advisory_seeking',
    label: 'Finding advisors',
  },

  mandate_hunting: {
    keywords: ['mandate', 'engagement', 'consulting', 'advisory work',
               'clients', 'projects', 'assignments', 'law firm',
               'accountant', 'restructuring', 'tax'],
    vertical: 'mandate',
    signal_priorities: ['restructuring', 'leadership_change', 'ma_activity',
                        'geographic_expansion'],
    nev_canister_type: 'mandate_seeking',
    label: 'Finding client engagements',
  },

  market_intel: {
    keywords: ['monitor', 'track', 'intelligence', 'signals',
               'keep tabs', 'competitive', 'landscape', 'sector',
               'market', 'watch'],
    vertical: 'revenue',
    signal_priorities: ['capital_raising', 'ma_activity', 'leadership_change', 'product_launch'],
    nev_canister_type: 'market_intelligence',
    label: 'Market intelligence',
  },

  research_talent: {
    keywords: ['researcher', 'scientist', 'academic', 'expert',
               'phd', 'lab', 'r&d', 'innovation', 'ip', 'patents'],
    vertical: 'talent',
    signal_priorities: ['product_launch', 'partnership', 'capital_raising', 'company_founded'],
    nev_canister_type: 'research_talent',
    label: 'Finding researchers & experts',
  },
};

module.exports = { INTENT_TAXONOMY };
