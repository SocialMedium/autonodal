-- ============================================================================
-- NETWORK TOPOLOGY ENGINE — Phase 1
-- Tables for network density, company adjacency, geo priorities,
-- and triangulated ranked opportunities
-- ============================================================================

-- ─── GEO PRIORITIES (configurable region weights + matching patterns) ───

CREATE TABLE IF NOT EXISTS geo_priorities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  region_code VARCHAR(10) NOT NULL UNIQUE,
  region_name VARCHAR(100) NOT NULL,
  country_codes CHAR(2)[] NOT NULL DEFAULT '{}',
  location_keywords TEXT[] NOT NULL DEFAULT '{}',
  weight_boost INTEGER NOT NULL DEFAULT 0,
  is_home_market BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO geo_priorities (region_code, region_name, country_codes, location_keywords, weight_boost, is_home_market) VALUES
  ('AU', 'Australia & New Zealand', ARRAY['AU','NZ'],
   ARRAY['australia','australian','sydney','melbourne','brisbane','perth','adelaide','canberra','auckland','new zealand','asx','csiro'],
   20, TRUE),
  ('SG', 'Singapore & SEA', ARRAY['SG','MY','ID','TH','VN','PH'],
   ARRAY['singapore','southeast asia','asean','jakarta','kuala lumpur','bangkok','vietnam','philippines','indonesia','malaysia','thailand'],
   20, TRUE),
  ('UK', 'United Kingdom & Ireland', ARRAY['GB','UK','IE'],
   ARRAY['united kingdom','london','england','britain','british','manchester','edinburgh','ireland','dublin','ftse'],
   10, FALSE),
  ('US', 'United States & Canada', ARRAY['US','CA'],
   ARRAY['united states','silicon valley','new york','san francisco','california','texas','boston','seattle','canada','toronto','nasdaq','nyse'],
   5, FALSE),
  ('APAC_OTHER', 'APAC (ex AU/SG)', ARRAY['JP','KR','IN','HK','CN','TW'],
   ARRAY['japan','japanese','korea','korean','india','indian','hong kong','china','chinese','taiwan'],
   15, FALSE),
  ('EMEA_OTHER', 'EMEA (ex UK)', ARRAY['DE','FR','NL','SE','DK','NO','FI','ES','IT','CH','AT','BE'],
   ARRAY['germany','german','france','french','netherlands','europe','european','nordics','sweden','denmark','switzerland'],
   0, FALSE),
  ('LATAM', 'Latin America', ARRAY['BR','MX','AR','CL','CO'],
   ARRAY['brazil','brazilian','mexico','latin america','argentina','chile','colombia'],
   0, FALSE)
ON CONFLICT (region_code) DO NOTHING;

-- ─── NETWORK DENSITY SCORES (pre-computed by region × sector) ───

CREATE TABLE IF NOT EXISTS network_density_scores (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  region_code VARCHAR(10) NOT NULL,
  sector VARCHAR(100),
  total_contacts INTEGER NOT NULL DEFAULT 0,
  active_contacts INTEGER NOT NULL DEFAULT 0,
  senior_contacts INTEGER NOT NULL DEFAULT 0,
  placement_count INTEGER NOT NULL DEFAULT 0,
  client_count INTEGER NOT NULL DEFAULT 0,
  active_search_count INTEGER NOT NULL DEFAULT 0,
  density_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  depth_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  recency_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  score_breakdown JSONB NOT NULL DEFAULT '{}',
  computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_density_unique ON network_density_scores(region_code, COALESCE(sector, '__ALL__'));
CREATE INDEX IF NOT EXISTS idx_density_region ON network_density_scores(region_code);
CREATE INDEX IF NOT EXISTS idx_density_score ON network_density_scores(density_score DESC);

-- ─── COMPANY ADJACENCY SCORES (pre-computed per company) ───

CREATE TABLE IF NOT EXISTS company_adjacency_scores (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID,
  company_name VARCHAR(500) NOT NULL,
  contact_count INTEGER NOT NULL DEFAULT 0,
  senior_contact_count INTEGER NOT NULL DEFAULT 0,
  active_contact_count INTEGER NOT NULL DEFAULT 0,
  placement_count INTEGER NOT NULL DEFAULT 0,
  active_search_count INTEGER NOT NULL DEFAULT 0,
  is_client BOOLEAN DEFAULT FALSE,
  client_tier VARCHAR(50),
  warmest_contact_id UUID,
  warmest_contact_name VARCHAR(255),
  best_connection_user_id UUID,
  best_connection_user_name VARCHAR(255),
  adjacency_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  score_breakdown JSONB NOT NULL DEFAULT '{}',
  derived_region_code VARCHAR(10),
  derived_sector VARCHAR(100),
  computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_adjacency_name ON company_adjacency_scores(LOWER(TRIM(company_name)));
CREATE INDEX IF NOT EXISTS idx_adjacency_company_id ON company_adjacency_scores(company_id);
CREATE INDEX IF NOT EXISTS idx_adjacency_score ON company_adjacency_scores(adjacency_score DESC);
CREATE INDEX IF NOT EXISTS idx_adjacency_region ON company_adjacency_scores(derived_region_code);

-- ─── RANKED OPPORTUNITIES (triangulated output) ───

CREATE TABLE IF NOT EXISTS ranked_opportunities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID,
  company_name VARCHAR(500) NOT NULL,
  signal_importance DECIMAL(5,2) NOT NULL DEFAULT 0,
  network_overlap DECIMAL(5,2) NOT NULL DEFAULT 0,
  geo_relevance DECIMAL(5,2) NOT NULL DEFAULT 0,
  placement_adjacency DECIMAL(5,2) NOT NULL DEFAULT 0,
  thematic_relevance DECIMAL(5,2) NOT NULL DEFAULT 0,
  convergence_bonus DECIMAL(5,2) NOT NULL DEFAULT 0,
  composite_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  rank_in_region INTEGER,
  region_code VARCHAR(10),
  sector VARCHAR(100),
  score_explanation JSONB NOT NULL DEFAULT '{}',
  signal_summary TEXT,
  recommended_action TEXT,
  signal_event_ids UUID[] DEFAULT '{}',
  signal_count INTEGER DEFAULT 0,
  signal_types TEXT[] DEFAULT '{}',
  strongest_signal_type VARCHAR(50),
  warmest_contact_id UUID,
  warmest_contact_name VARCHAR(255),
  best_connection_user_id UUID,
  best_connection_user_name VARCHAR(255),
  latest_signal_date TIMESTAMPTZ,
  decay_factor DECIMAL(4,3) DEFAULT 1.0,
  status VARCHAR(20) DEFAULT 'active',
  computed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_opportunities_name ON ranked_opportunities(LOWER(TRIM(company_name)));
CREATE INDEX IF NOT EXISTS idx_opportunities_score ON ranked_opportunities(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_opportunities_region ON ranked_opportunities(region_code, composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_opportunities_status ON ranked_opportunities(status);

-- ─── PERFORMANCE INDEXES ───

CREATE INDEX IF NOT EXISTS idx_people_company_name_lower ON people (LOWER(TRIM(current_company_name)));
CREATE INDEX IF NOT EXISTS idx_interactions_person_recent ON interactions(person_id, interaction_at DESC);

DO $$ BEGIN RAISE NOTICE 'Network topology tables created successfully'; END $$;
