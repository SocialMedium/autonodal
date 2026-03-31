-- ═══════════════════════════════════════════════════════════════════════════════
-- MITCHELLAKE SIGNAL INTELLIGENCE PLATFORM
-- PostgreSQL Schema v1.0
-- ═══════════════════════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ═══════════════════════════════════════════════════════════════════════════════
-- ENUM TYPES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TYPE signal_type AS ENUM (
  'capital_raising', 'geographic_expansion', 'strategic_hiring', 'ma_activity',
  'partnership', 'product_launch', 'leadership_change', 'layoffs', 'restructuring'
);

CREATE TYPE triage_status AS ENUM (
  'new', 'investigating', 'qualified', 'ignore', 'contacted'
);

CREATE TYPE person_signal_type AS ENUM (
  'new_role', 'promotion', 'company_exit', 'board_appointment', 'company_founded',
  'speaking_engagement', 'publication', 'podcast_appearance', 'award_recognition',
  'patent_filed', 'news_mention', 'company_raised', 'company_acquired',
  'company_layoffs', 'company_trouble', 'email_sent', 'email_received',
  'call_completed', 'meeting_held', 'linkedin_outreach', 'intro_made',
  'pipeline_added', 'pipeline_stage_change', 'engagement_change', 'activity_spike',
  'flight_risk_alert', 'timing_opportunity'
);

CREATE TYPE content_source_type AS ENUM (
  'newsletter', 'blog', 'podcast_host', 'podcast_guest', 'youtube',
  'twitter', 'linkedin', 'github', 'academic', 'book', 'patent'
);

CREATE TYPE project_status AS ENUM (
  'scoping', 'proposal', 'active', 'on_hold', 'completed', 'cancelled'
);

CREATE TYPE search_status AS ENUM (
  'briefing', 'research', 'sourcing', 'outreach', 'interviewing',
  'shortlist', 'client_interviews', 'offer', 'negotiation', 'placed', 'on_hold', 'cancelled'
);

CREATE TYPE candidate_status AS ENUM (
  'identified', 'researching', 'to_contact', 'contacted', 'in_dialogue',
  'interested', 'screening', 'interviewing', 'shortlisted', 'presented',
  'client_interview', 'offer', 'placed', 'declined', 'rejected', 'withdrawn', 'on_hold'
);

CREATE TYPE proximity_type AS ENUM (
  'direct', 'indirect', 'inferred', 'requested'
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- COMPANIES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS companies (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(500) NOT NULL,
  domain VARCHAR(255) UNIQUE,
  aliases TEXT[],
  sector VARCHAR(100),
  sub_sector VARCHAR(100),
  geography VARCHAR(100),
  country_code CHAR(2),
  employee_count_band VARCHAR(50),
  description TEXT,
  website_url TEXT,
  linkedin_url TEXT,
  is_client BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_companies_name ON companies USING gin (name gin_trgm_ops);
CREATE INDEX idx_companies_domain ON companies (domain);
CREATE INDEX idx_companies_sector ON companies (sector);

-- ═══════════════════════════════════════════════════════════════════════════════
-- EXTERNAL DOCUMENTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS external_documents (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_type VARCHAR(50) NOT NULL,
  source_name VARCHAR(100),
  source_url TEXT NOT NULL,
  source_url_hash VARCHAR(64) NOT NULL UNIQUE,
  title VARCHAR(1000),
  content TEXT,
  summary TEXT,
  author VARCHAR(255),
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ DEFAULT NOW(),
  extracted_entities JSONB,
  extracted_signals JSONB,
  processing_status VARCHAR(50) DEFAULT 'pending',
  embedded_at TIMESTAMPTZ,
  signals_computed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_docs_source ON external_documents (source_type, source_name);
CREATE INDEX idx_docs_published ON external_documents (published_at DESC);
CREATE INDEX idx_docs_status ON external_documents (processing_status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SIGNAL EVENTS (Company Signals)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS signal_events (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  signal_type signal_type NOT NULL,
  company_id UUID REFERENCES companies(id),
  company_name VARCHAR(500),
  confidence_score DECIMAL(3,2) NOT NULL,
  scoring_breakdown JSONB,
  evidence_doc_ids UUID[],
  evidence_summary TEXT,
  evidence_snippets JSONB,
  triage_status triage_status DEFAULT 'new',
  triaged_by UUID,
  triaged_at TIMESTAMPTZ,
  triage_notes TEXT,
  detected_at TIMESTAMPTZ DEFAULT NOW(),
  signal_date TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_signals_company ON signal_events (company_id);
CREATE INDEX idx_signals_type ON signal_events (signal_type);
CREATE INDEX idx_signals_status ON signal_events (triage_status);
CREATE INDEX idx_signals_detected ON signal_events (detected_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- DOCUMENT-COMPANY LINKS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS document_companies (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  document_id UUID NOT NULL REFERENCES external_documents(id) ON DELETE CASCADE,
  company_id UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
  mention_context TEXT,
  mention_role VARCHAR(100),
  confidence DECIMAL(3,2),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(document_id, company_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PEOPLE (Candidates)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS people (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  full_name VARCHAR(255) NOT NULL,
  normalized_name VARCHAR(255),
  email VARCHAR(255),
  linkedin_url TEXT,
  current_title VARCHAR(255),
  current_company_id UUID REFERENCES companies(id),
  current_company_name VARCHAR(255),
  headline TEXT,
  bio TEXT,
  location VARCHAR(255),
  country_code CHAR(2),
  years_experience INTEGER,
  seniority_level VARCHAR(50),
  functional_area VARCHAR(100),
  expertise_tags TEXT[],
  industries TEXT[],
  education JSONB,
  career_history JSONB,
  source VARCHAR(50),
  source_id TEXT,
  status VARCHAR(50) DEFAULT 'active',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_people_name ON people USING gin (full_name gin_trgm_ops);
CREATE INDEX idx_people_company ON people (current_company_id);
CREATE INDEX idx_people_seniority ON people (seniority_level);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PERSON SIGNALS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS person_signals (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  signal_type person_signal_type NOT NULL,
  signal_category VARCHAR(50),
  title VARCHAR(500),
  description TEXT,
  source VARCHAR(50),
  source_url TEXT,
  document_id UUID REFERENCES external_documents(id),
  company_id UUID REFERENCES companies(id),
  user_id UUID,
  confidence_score DECIMAL(3,2),
  impact_score DECIMAL(3,2),
  metadata JSONB,
  signal_date TIMESTAMPTZ,
  detected_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_person_signals_person ON person_signals (person_id);
CREATE INDEX idx_person_signals_type ON person_signals (signal_type);
CREATE INDEX idx_person_signals_date ON person_signals (signal_date DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PERSON SCORES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS person_scores (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE UNIQUE,
  engagement_score DECIMAL(3,2),
  engagement_trend VARCHAR(20),
  last_interaction_at TIMESTAMPTZ,
  interaction_count_30d INTEGER,
  activity_score DECIMAL(3,2),
  activity_trend VARCHAR(20),
  external_signals_30d INTEGER,
  receptivity_score DECIMAL(3,2),
  tenure_months INTEGER,
  flight_risk_score DECIMAL(3,2),
  timing_score DECIMAL(3,2),
  market_heat_score DECIMAL(3,2),
  relationship_strength DECIMAL(3,2),
  best_connection_user_id UUID,
  composite_score DECIMAL(3,2),
  score_factors JSONB,
  computed_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_person_scores_composite ON person_scores (composite_score DESC);
CREATE INDEX idx_person_scores_timing ON person_scores (timing_score DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PERSON CONTENT SOURCES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS person_content_sources (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  source_type content_source_type NOT NULL,
  source_name VARCHAR(255),
  source_url TEXT NOT NULL,
  feed_url TEXT,
  enabled BOOLEAN DEFAULT TRUE,
  verified BOOLEAN DEFAULT FALSE,
  poll_frequency_hours INTEGER DEFAULT 24,
  last_polled_at TIMESTAMPTZ,
  last_content_at TIMESTAMPTZ,
  error_count INTEGER DEFAULT 0,
  last_error TEXT,
  total_items INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(person_id, source_url)
);

CREATE INDEX idx_content_sources_person ON person_content_sources (person_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PERSON CONTENT
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS person_content (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  source_id UUID REFERENCES person_content_sources(id) ON DELETE CASCADE,
  content_type content_source_type NOT NULL,
  title VARCHAR(1000),
  url TEXT NOT NULL,
  url_hash VARCHAR(64) NOT NULL UNIQUE,
  content TEXT,
  summary TEXT,
  key_topics TEXT[],
  key_quotes TEXT[],
  mentioned_companies TEXT[],
  sentiment VARCHAR(20),
  word_count INTEGER,
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ DEFAULT NOW(),
  embedded BOOLEAN DEFAULT FALSE,
  embedded_at TIMESTAMPTZ,
  analyzed BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_person_content_person ON person_content (person_id);
CREATE INDEX idx_person_content_published ON person_content (published_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- INTERACTIONS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS interactions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  user_id UUID NOT NULL,
  interaction_type VARCHAR(50) NOT NULL,
  direction VARCHAR(20),
  subject VARCHAR(500),
  summary TEXT,
  sentiment VARCHAR(20),
  duration_minutes INTEGER,
  channel VARCHAR(50),
  requires_response BOOLEAN DEFAULT FALSE,
  response_received BOOLEAN,
  response_time_hours DECIMAL(6,2),
  source VARCHAR(50),
  external_id VARCHAR(255),
  interaction_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_person ON interactions (person_id);
CREATE INDEX idx_interactions_date ON interactions (interaction_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TEAM PROXIMITY
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS team_proximity (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  user_id UUID NOT NULL,
  proximity_type proximity_type NOT NULL,
  proximity_strength DECIMAL(3,2),
  relationship_context TEXT,
  last_contact_date DATE,
  source VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(person_id, user_id)
);

CREATE INDEX idx_proximity_person ON team_proximity (person_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- CLIENTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS clients (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id UUID REFERENCES companies(id),
  name VARCHAR(255) NOT NULL,
  relationship_status VARCHAR(50) DEFAULT 'prospect',
  relationship_tier VARCHAR(50),
  relationship_owner_id UUID,
  contract_type VARCHAR(50),
  annual_value DECIMAL(12,2),
  first_engagement_date DATE,
  total_placements INTEGER DEFAULT 0,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_clients_status ON clients (relationship_status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PROJECTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS projects (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  code VARCHAR(50) UNIQUE,
  project_type VARCHAR(50),
  description TEXT,
  status project_status DEFAULT 'scoping',
  priority INTEGER DEFAULT 5,
  lead_partner_id UUID,
  team_member_ids UUID[],
  kick_off_date DATE,
  target_completion_date DATE,
  fee_type VARCHAR(50),
  fee_amount DECIMAL(12,2),
  currency VARCHAR(3) DEFAULT 'USD',
  client_context TEXT,
  success_criteria TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_projects_client ON projects (client_id);
CREATE INDEX idx_projects_status ON projects (status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEARCHES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS searches (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  title VARCHAR(255) NOT NULL,
  code VARCHAR(50),
  location VARCHAR(255),
  location_flexibility VARCHAR(100),
  target_geography TEXT[],
  seniority_level VARCHAR(50),
  reports_to VARCHAR(255),
  team_size VARCHAR(100),
  comp_range_low DECIMAL(12,2),
  comp_range_high DECIMAL(12,2),
  comp_currency VARCHAR(3) DEFAULT 'USD',
  equity_offered BOOLEAN,
  status search_status DEFAULT 'briefing',
  priority VARCHAR(20) DEFAULT 'medium',
  lead_consultant_id UUID,
  researcher_id UUID,
  kick_off_date DATE,
  target_shortlist_date DATE,
  target_placement_date DATE,
  brief_summary TEXT,
  role_overview TEXT,
  key_responsibilities TEXT,
  required_experience TEXT,
  preferred_experience TEXT,
  ideal_background TEXT,
  target_companies TEXT[],
  off_limits_companies TEXT[],
  must_have_keywords TEXT[],
  nice_to_have_keywords TEXT[],
  target_industries TEXT[],
  embedded BOOLEAN DEFAULT FALSE,
  embedded_at TIMESTAMPTZ,
  total_candidates INTEGER DEFAULT 0,
  placed_person_id UUID REFERENCES people(id),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_searches_project ON searches (project_id);
CREATE INDEX idx_searches_status ON searches (status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEARCH CANDIDATES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS search_candidates (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  search_id UUID NOT NULL REFERENCES searches(id) ON DELETE CASCADE,
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  status candidate_status DEFAULT 'identified',
  status_changed_at TIMESTAMPTZ DEFAULT NOW(),
  source VARCHAR(50),
  source_details TEXT,
  assigned_to_id UUID,
  overall_fit_score DECIMAL(4,3),
  experience_fit DECIMAL(4,3),
  skills_fit DECIMAL(4,3),
  cultural_fit DECIMAL(4,3),
  match_reasons JSONB,
  strengths TEXT[],
  gaps TEXT[],
  questions_to_explore TEXT[],
  interest_level VARCHAR(50),
  interest_notes TEXT,
  assessment_notes TEXT,
  client_feedback TEXT,
  outcome VARCHAR(50),
  outcome_reason TEXT,
  first_contact_at TIMESTAMPTZ,
  last_contact_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(search_id, person_id)
);

CREATE INDEX idx_search_cand_search ON search_candidates (search_id);
CREATE INDEX idx_search_cand_person ON search_candidates (person_id);
CREATE INDEX idx_search_cand_status ON search_candidates (status);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEARCH ACTIVITIES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS search_activities (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  search_id UUID NOT NULL REFERENCES searches(id) ON DELETE CASCADE,
  search_candidate_id UUID REFERENCES search_candidates(id) ON DELETE CASCADE,
  activity_type VARCHAR(50) NOT NULL,
  subject VARCHAR(500),
  description TEXT,
  performed_by_id UUID,
  activity_at TIMESTAMPTZ DEFAULT NOW(),
  duration_minutes INTEGER,
  follow_up_required BOOLEAN DEFAULT FALSE,
  follow_up_date DATE,
  attachments JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_search_act_search ON search_activities (search_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEARCH MATCHES (AI-generated)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS search_matches (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  search_id UUID NOT NULL REFERENCES searches(id) ON DELETE CASCADE,
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  overall_match_score DECIMAL(4,3),
  experience_match DECIMAL(4,3),
  skills_match DECIMAL(4,3),
  industry_match DECIMAL(4,3),
  content_alignment DECIMAL(4,3),
  match_reasons JSONB,
  strengths TEXT[],
  gaps TEXT[],
  status VARCHAR(50) DEFAULT 'suggested',
  reviewed_by UUID,
  reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(search_id, person_id)
);

CREATE INDEX idx_search_matches_score ON search_matches (overall_match_score DESC);

-- ═══════════════════════════════════════════════════════════════════════════════
-- CLIENT CONTACTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS client_contacts (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  person_id UUID REFERENCES people(id),
  name VARCHAR(255) NOT NULL,
  title VARCHAR(255),
  email VARCHAR(255),
  phone VARCHAR(50),
  linkedin_url TEXT,
  is_primary BOOLEAN DEFAULT FALSE,
  is_decision_maker BOOLEAN DEFAULT FALSE,
  relationship_owner_id UUID,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- USERS & AUTH
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  role VARCHAR(50) DEFAULT 'analyst',
  email_verified BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users (email);

CREATE TABLE IF NOT EXISTS sessions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token VARCHAR(255) UNIQUE NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_token ON sessions (token);

-- ═══════════════════════════════════════════════════════════════════════════════
-- WATCHLISTS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS watchlists (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  filter_criteria JSONB,
  alert_enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watchlist_items (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  watchlist_id UUID NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
  entity_type VARCHAR(50) NOT NULL,
  entity_id UUID,
  entity_value VARCHAR(255),
  note TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- AUDIT & SEARCH LOGS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES users(id),
  action VARCHAR(100) NOT NULL,
  target_type VARCHAR(50),
  target_id UUID,
  details JSONB,
  ip_address INET,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_created ON audit_logs (created_at DESC);

CREATE TABLE IF NOT EXISTS search_logs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES users(id),
  query TEXT NOT NULL,
  filters JSONB,
  results_count INTEGER,
  search_mode VARCHAR(50),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- CONFIGURATION TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rss_sources (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(100) NOT NULL,
  source_type VARCHAR(50) NOT NULL,
  url TEXT NOT NULL,
  poll_interval_minutes INTEGER DEFAULT 30,
  enabled BOOLEAN DEFAULT TRUE,
  credibility_score DECIMAL(3,2) DEFAULT 0.7,
  signal_types signal_type[],
  last_fetched_at TIMESTAMPTZ,
  last_error TEXT,
  consecutive_errors INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_rules (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(255) NOT NULL,
  description TEXT,
  signal_type VARCHAR(50) NOT NULL,
  keywords TEXT[],
  patterns JSONB,
  base_confidence DECIMAL(3,2),
  enabled BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_stages (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(100) NOT NULL,
  sort_order INTEGER,
  color VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS ml_interests (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  interest_type VARCHAR(50) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  keywords TEXT[],
  priority INTEGER DEFAULT 5,
  active BOOLEAN DEFAULT TRUE,
  embedded BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- EMBEDDINGS & VECTOR REFERENCES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS embeddings (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  entity_type VARCHAR(50) NOT NULL,
  entity_id UUID NOT NULL,
  embedding_type VARCHAR(50) NOT NULL,
  qdrant_collection VARCHAR(100) NOT NULL,
  qdrant_point_id UUID NOT NULL,
  source_text_hash VARCHAR(64),
  model VARCHAR(100) DEFAULT 'text-embedding-3-small',
  version INTEGER DEFAULT 1,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(entity_type, entity_id, embedding_type)
);

CREATE TABLE IF NOT EXISTS vector_relationships (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  from_entity_type VARCHAR(50) NOT NULL,
  from_entity_id UUID NOT NULL,
  to_entity_type VARCHAR(50) NOT NULL,
  to_entity_id UUID NOT NULL,
  relationship_type VARCHAR(100) NOT NULL,
  similarity_score DECIMAL(4,3),
  match_reasons JSONB,
  computed_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(from_entity_type, from_entity_id, to_entity_type, to_entity_id, relationship_type)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED DATA
-- ═══════════════════════════════════════════════════════════════════════════════

-- RSS Sources
INSERT INTO rss_sources (name, source_type, url, credibility_score, signal_types) VALUES
  ('PR Newswire', 'rss', 'https://www.prnewswire.com/rss/news-releases-list.rss', 0.85, 
   ARRAY['capital_raising', 'partnership', 'product_launch', 'leadership_change']::signal_type[]),
  ('Business Wire', 'rss', 'https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ==', 0.85,
   ARRAY['capital_raising', 'ma_activity', 'partnership']::signal_type[]),
  ('TechCrunch', 'rss', 'https://techcrunch.com/feed/', 0.75,
   ARRAY['capital_raising', 'product_launch']::signal_type[]),
  ('Sifted', 'rss', 'https://sifted.eu/feed', 0.75,
   ARRAY['capital_raising', 'geographic_expansion']::signal_type[]),
  ('GlobeNewswire', 'rss', 'https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire', 0.80,
   ARRAY['capital_raising', 'partnership']::signal_type[])
ON CONFLICT DO NOTHING;

-- Pipeline Stages
INSERT INTO pipeline_stages (name, sort_order, color) VALUES
  ('Identified', 1, 'gray'),
  ('Researching', 2, 'blue'),
  ('Reaching Out', 3, 'yellow'),
  ('In Conversation', 4, 'orange'),
  ('Interviewing', 5, 'purple'),
  ('Shortlisted', 6, 'indigo'),
  ('Presented', 7, 'cyan'),
  ('Client Interview', 8, 'teal'),
  ('Offer', 9, 'green'),
  ('Placed', 10, 'emerald'),
  ('Declined', 11, 'red')
ON CONFLICT DO NOTHING;

-- MitchelLake Interests
INSERT INTO ml_interests (interest_type, name, description, keywords, priority) VALUES
  ('sector', 'Fintech', 'Financial technology', ARRAY['fintech', 'payments', 'banking', 'lending', 'insurtech'], 9),
  ('sector', 'Enterprise SaaS', 'B2B software', ARRAY['saas', 'enterprise', 'b2b', 'cloud', 'software'], 9),
  ('sector', 'AI/ML', 'Artificial intelligence', ARRAY['ai', 'ml', 'machine learning', 'deep learning', 'llm'], 10),
  ('sector', 'Healthtech', 'Healthcare technology', ARRAY['healthtech', 'digital health', 'medtech', 'biotech'], 8),
  ('theme', 'Go-to-market', 'Revenue leadership', ARRAY['sales', 'revenue', 'gtm', 'growth', 'expansion'], 8),
  ('theme', 'Product-led growth', 'PLG expertise', ARRAY['plg', 'product-led', 'self-serve', 'freemium'], 7),
  ('capability', 'Scaling', 'Hypergrowth experience', ARRAY['scaling', 'hypergrowth', 'series b', 'series c'], 9),
  ('capability', 'Turnaround', 'Transformation', ARRAY['turnaround', 'transformation', 'restructuring'], 7)
ON CONFLICT DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- EVENT INTELLIGENCE (EventMedium integration)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TYPE event_format AS ENUM (
  'conference', 'meetup', 'summit', 'workshop', 'webinar', 'roundtable',
  'demo_day', 'pitch_event', 'awards', 'networking', 'panel', 'other'
);

CREATE TABLE IF NOT EXISTS event_sources (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  tenant_id UUID NOT NULL,
  name TEXT NOT NULL,
  feed_url TEXT NOT NULL UNIQUE,
  theme TEXT,
  region TEXT,
  is_active BOOLEAN DEFAULT true,
  last_fetched_at TIMESTAMPTZ,
  last_error TEXT,
  fetch_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  tenant_id UUID NOT NULL,
  external_id TEXT,
  source_id UUID REFERENCES event_sources(id),
  title TEXT NOT NULL,
  description TEXT,
  event_url TEXT,
  source_url TEXT,
  url_hash TEXT UNIQUE,
  theme TEXT,
  region TEXT,
  city TEXT,
  country TEXT,
  format event_format DEFAULT 'other',
  event_date DATE,
  event_end_date DATE,
  event_time TEXT,
  is_virtual BOOLEAN DEFAULT false,
  organiser TEXT,
  speaker_names TEXT[],
  company_mentions TEXT[],
  signal_relevance TEXT[],
  relevance_score FLOAT DEFAULT 0,
  is_featured BOOLEAN DEFAULT false,
  raw_feed_data JSONB,
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ DEFAULT NOW(),
  embedded_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_events_tenant ON events(tenant_id);
CREATE INDEX idx_events_theme ON events(theme);
CREATE INDEX idx_events_region ON events(region);
CREATE INDEX idx_events_event_date ON events(event_date);
CREATE INDEX idx_events_relevance ON events(relevance_score DESC);
CREATE INDEX idx_events_source ON events(source_id);

CREATE TABLE IF NOT EXISTS event_company_links (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_id UUID REFERENCES events(id) ON DELETE CASCADE,
  company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
  link_type TEXT DEFAULT 'mentioned',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(event_id, company_id)
);

CREATE TABLE IF NOT EXISTS event_person_links (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_id UUID REFERENCES events(id) ON DELETE CASCADE,
  person_id UUID REFERENCES people(id) ON DELETE CASCADE,
  role TEXT DEFAULT 'mentioned',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(event_id, person_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- TRIGGERS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER companies_updated_at BEFORE UPDATE ON companies FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER signal_events_updated_at BEFORE UPDATE ON signal_events FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER people_updated_at BEFORE UPDATE ON people FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER person_scores_updated_at BEFORE UPDATE ON person_scores FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER projects_updated_at BEFORE UPDATE ON projects FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER searches_updated_at BEFORE UPDATE ON searches FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER search_candidates_updated_at BEFORE UPDATE ON search_candidates FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER events_updated_at BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER event_sources_updated_at BEFORE UPDATE ON event_sources FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ═══════════════════════════════════════════════════════════════════════════════
-- VIEWS
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_active_searches AS
SELECT 
  s.*,
  p.name as project_name,
  p.code as project_code,
  c.name as client_name,
  (SELECT COUNT(*) FROM search_candidates sc WHERE sc.search_id = s.id) as total_candidates,
  (SELECT COUNT(*) FROM search_candidates sc WHERE sc.search_id = s.id AND sc.status IN ('shortlisted', 'presented', 'client_interview')) as shortlisted
FROM searches s
JOIN projects p ON s.project_id = p.id
JOIN clients c ON p.client_id = c.id
WHERE s.status NOT IN ('placed', 'cancelled', 'on_hold');

-- Event Sources (EventMedium feeds)
INSERT INTO event_sources (tenant_id, name, feed_url, theme, region) VALUES
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — All Events',              'https://eventmedium.ai/api/events/rss',                                     NULL,            NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — AI',                      'https://eventmedium.ai/api/events/rss?theme=AI',                            'AI',            NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — FinTech',                 'https://eventmedium.ai/api/events/rss?theme=FinTech',                       'FinTech',       NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Climate Tech',            'https://eventmedium.ai/api/events/rss?theme=Climate%20Tech',                'Climate Tech',  NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Cybersecurity',           'https://eventmedium.ai/api/events/rss?theme=Cybersecurity',                 'Cybersecurity', NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — UK',                      'https://eventmedium.ai/api/events/rss?region=UK',                           NULL,            'UK'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Australia',               'https://eventmedium.ai/api/events/rss?region=Australia',                    NULL,            'Australia'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Singapore',               'https://eventmedium.ai/api/events/rss?region=Singapore',                    NULL,            'Singapore'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — US',                      'https://eventmedium.ai/api/events/rss?region=US',                           NULL,            'US'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Cybersecurity Singapore', 'https://eventmedium.ai/api/events/rss?theme=Cybersecurity&region=Singapore','Cybersecurity', 'Singapore')
ON CONFLICT (feed_url) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DONE
-- ═══════════════════════════════════════════════════════════════════════════════
