-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Investor Enrichment & Document Ingestion
-- Investor profile columns on people, enrichment tracking tables,
-- SocialMedium AI scoring template
-- Run: node scripts/run_migration.js sql/migration_investor_enrichment.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── INVESTOR PROFILE ON PEOPLE ──────────────────────────────────────────────

ALTER TABLE people ADD COLUMN IF NOT EXISTS is_investor            BOOLEAN DEFAULT FALSE;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_type          TEXT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_stage_focus   TEXT[];
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_sector_focus  TEXT[];
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_geo_focus     TEXT[];
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_check_min     INTEGER;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_check_max     INTEGER;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_fit_score     FLOAT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_fit_rationale TEXT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_fit_criteria  JSONB;
ALTER TABLE people ADD COLUMN IF NOT EXISTS investor_fit_context   TEXT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS enrichment_source      TEXT;
ALTER TABLE people ADD COLUMN IF NOT EXISTS enrichment_notes       JSONB DEFAULT '[]'::JSONB;

-- ── ENRICHMENT DOCUMENTS REGISTRY ───────────────────────────────────────────

CREATE TABLE IF NOT EXISTS enrichment_documents (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       UUID,
  filename        TEXT NOT NULL,
  document_type   TEXT NOT NULL
    CHECK (document_type IN (
      'investor_list','linkedin_export','crm_export',
      'contact_list','event_attendees','board_data','portfolio_data'
    )),
  row_count       INTEGER,
  matched_count   INTEGER DEFAULT 0,
  enriched_count  INTEGER DEFAULT 0,
  skipped_count   INTEGER DEFAULT 0,
  rejected_count  INTEGER DEFAULT 0,
  column_audit    JSONB,
  status          TEXT DEFAULT 'pending'
    CHECK (status IN ('pending','auditing','ready','processing','complete','failed')),
  processed_at    TIMESTAMPTZ,
  notes           TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── PER-ROW AUDIT TRAIL ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS enrichment_log (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id      UUID REFERENCES enrichment_documents(id),
  person_id        UUID REFERENCES people(id),
  tenant_id        UUID,
  source_row       INTEGER,
  source_name      TEXT,
  source_linkedin  TEXT,
  match_method     TEXT,
  match_confidence FLOAT,
  had_interaction  BOOLEAN,
  action           TEXT CHECK (action IN (
    'enriched','skipped_no_interaction','skipped_no_match',
    'skipped_no_changes','rejected_new_person_attempt'
  )),
  fields_updated   TEXT[],
  previous_values  JSONB,
  new_values       JSONB,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_document ON enrichment_log(document_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_person ON enrichment_log(person_id);

-- ── INVESTOR SCORING TEMPLATES ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS investor_scoring_templates (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  version     TEXT DEFAULT '1.0',
  source      TEXT,
  criteria    JSONB NOT NULL,
  notes       TEXT,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO investor_scoring_templates (name, version, source, criteria, notes)
VALUES (
  'SocialMedium AI Investor Fit Framework', '1.0', 'socialmedium_ai_matching_engine',
  '{
    "scoring_scale": "1-5 (5=perfect fit, 1=no fit)",
    "hard_gate": "Constraints & Exclusions must score >= 4 or investor excluded",
    "context": "Built for SocialMedium AI seed raise, Barcelona, AI/agentic, 2025",
    "criteria": [
      {"name": "Stage & Check-Size Fit", "weight": "high"},
      {"name": "Domain / Investment Thesis Fit", "weight": "high"},
      {"name": "Geography & Market Fit", "weight": "high"},
      {"name": "Value-Add Potential", "weight": "medium"},
      {"name": "Mission, Motivation & Timing Alignment", "weight": "medium"},
      {"name": "Constraints & Exclusions (Hard Gate)", "weight": "critical",
       "gate_logic": "Score < 4 = investor excluded regardless of other scores"},
      {"name": "Professional Expertise Alignment", "weight": "low"},
      {"name": "Mission & Purpose Alignment", "weight": "low"},
      {"name": "Geographic Practicality", "weight": "low"},
      {"name": "Communication & Cultural Fit", "weight": "low"},
      {"name": "Professional Standing & Credibility", "weight": "medium"},
      {"name": "Motivational Compatibility", "weight": "low"}
    ]
  }'::JSONB,
  'SocialMedium AI matching engine criteria. JT + Omid network, December 2025. File: Copy_of_JT_Matches__Omid_JT_networks_in_SM_.xlsx'
) ON CONFLICT DO NOTHING;
