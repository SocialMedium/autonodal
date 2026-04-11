-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Jobs Monitoring Layer v1
-- ATS detection columns, job_postings table, job_signal_rules
-- Run: node scripts/run_migration.js sql/migration_jobs_v1.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── COMPANY ATS COLUMNS ─────────────────────────────────────────────────────

ALTER TABLE companies ADD COLUMN IF NOT EXISTS ats_type        TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS ats_feed_url    TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS ats_detected_at TIMESTAMPTZ;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS ats_error       TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS careers_url     TEXT;

CREATE INDEX IF NOT EXISTS idx_companies_ats_type
  ON companies(ats_type) WHERE ats_type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_companies_ats_feed
  ON companies(ats_feed_url) WHERE ats_feed_url IS NOT NULL;

-- ── JOB POSTINGS TABLE ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_postings (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id         UUID REFERENCES tenants(id),
  company_id        UUID REFERENCES companies(id),
  company_name      TEXT,

  -- Content
  title             TEXT NOT NULL,
  department        TEXT,
  location          TEXT,
  employment_type   TEXT,
  description_text  TEXT,
  apply_url         TEXT,

  -- Classification
  seniority_level   TEXT CHECK (seniority_level IN (
                      'c_suite','vp','director','manager',
                      'senior','mid','junior','unknown')),
  function_area     TEXT,
  is_leadership     BOOLEAN GENERATED ALWAYS AS (
                      seniority_level IN ('c_suite','vp','director')
                    ) STORED,

  -- Deduplication
  posting_hash      TEXT NOT NULL,
  external_id       TEXT,
  source_url        TEXT,
  ats_type          TEXT,

  -- Lifecycle
  status            TEXT DEFAULT 'active'
                      CHECK (status IN ('active','filled','removed','unknown')),
  first_seen_at     TIMESTAMPTZ DEFAULT NOW(),
  last_seen_at      TIMESTAMPTZ DEFAULT NOW(),
  removed_at        TIMESTAMPTZ,
  days_open         INTEGER GENERATED ALWAYS AS (
                      EXTRACT(DAY FROM
                        COALESCE(removed_at, NOW()) - first_seen_at
                      )::INTEGER
                    ) STORED,

  -- Signal linkage
  signal_event_id   UUID REFERENCES signal_events(id),
  created_at        TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(tenant_id, posting_hash)
);

CREATE INDEX IF NOT EXISTS idx_job_postings_company
  ON job_postings(tenant_id, company_id);
CREATE INDEX IF NOT EXISTS idx_job_postings_status
  ON job_postings(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_job_postings_seniority
  ON job_postings(tenant_id, seniority_level, status);
CREATE INDEX IF NOT EXISTS idx_job_postings_first_seen
  ON job_postings(tenant_id, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_postings_removed
  ON job_postings(tenant_id, removed_at DESC)
  WHERE removed_at IS NOT NULL;

-- ── JOB SIGNAL RULES ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_signal_rules (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id        UUID REFERENCES tenants(id),
  rule_name        TEXT NOT NULL,
  signal_type      TEXT NOT NULL,
  description      TEXT,

  min_postings     INTEGER DEFAULT 1,
  seniority_levels TEXT[],
  function_areas   TEXT[],
  time_window_days INTEGER DEFAULT 30,
  requires_new_geo BOOLEAN DEFAULT FALSE,

  confidence       FLOAT DEFAULT 0.75,
  is_enabled       BOOLEAN DEFAULT TRUE,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Seed platform-default rules (tenant_id = NULL)
INSERT INTO job_signal_rules
  (tenant_id, rule_name, signal_type, description,
   min_postings, seniority_levels, time_window_days, confidence)
VALUES
  (NULL, 'c_suite_post',      'strategic_hiring',
   'C-suite or VP role posted',
   1, ARRAY['c_suite','vp'], 30, 0.88),

  (NULL, 'director_batch',    'strategic_hiring',
   '3+ director-level roles in 30 days',
   3, ARRAY['director'], 30, 0.80),

  (NULL, 'volume_spike',      'strategic_hiring',
   '10+ roles posted in 30 days (growth mode)',
   10, NULL, 30, 0.75),

  (NULL, 'geo_expansion_jobs','geographic_expansion',
   '5+ roles in a geography not previously hired in',
   5, NULL, 60, 0.82),

  (NULL, 'c_suite_removed',   'leadership_change',
   'C-suite role removed within 90 days (hire made or cancelled)',
   1, ARRAY['c_suite','vp'], 90, 0.70),

  (NULL, 'mass_removal',      'restructuring',
   '10+ active roles removed in 7 days (freeze or RIF)',
   10, NULL, 7, 0.85),

  (NULL, 'finance_hiring',    'capital_raising',
   'CFO/VP Finance/Controller role posted (post-raise build-out signal)',
   1, ARRAY['c_suite','vp'], 30, 0.72),

  (NULL, 'board_advisory',    'ma_activity',
   'Board member or advisory role posted (governance build-out)',
   1, ARRAY['c_suite'], 30, 0.60)
ON CONFLICT DO NOTHING;
