-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Jobs v1b — Geographic Leadership Role Detection
-- Adds geo expansion classification columns to job_postings
-- + 7 new signal rules for geographic leadership roles
-- Run: node scripts/run_migration.js sql/migration_jobs_v1b.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── GEO COLUMNS ON JOB_POSTINGS ─────────────────────────────────────────────

ALTER TABLE job_postings
  ADD COLUMN IF NOT EXISTS is_geo_expansion_role BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS geo_role_class        TEXT,
  ADD COLUMN IF NOT EXISTS target_geography      TEXT,
  ADD COLUMN IF NOT EXISTS target_geo_tier       TEXT CHECK (target_geo_tier IN (
    'country', 'subregion', 'region', 'global'
  ));

CREATE INDEX IF NOT EXISTS idx_job_postings_geo_role
  ON job_postings(tenant_id, is_geo_expansion_role, status)
  WHERE is_geo_expansion_role = TRUE;

CREATE INDEX IF NOT EXISTS idx_job_postings_target_geo
  ON job_postings(tenant_id, target_geography, status)
  WHERE target_geography IS NOT NULL;

-- ── GEO LEADERSHIP SIGNAL RULES ─────────────────────────────────────────────

INSERT INTO job_signal_rules
  (tenant_id, rule_name, signal_type, description,
   min_postings, seniority_levels, time_window_days, confidence)
VALUES
  (NULL, 'country_manager_posted',  'geographic_expansion',
   'Country Manager or Country Head role posted — committed single-market entry',
   1, NULL, 30, 0.95),

  (NULL, 'regional_csuite_posted',  'geographic_expansion',
   'Regional CEO/COO/CFO/CTO posted — major regional build-out signal',
   1, NULL, 30, 0.95),

  (NULL, 'regional_vp_posted',      'geographic_expansion',
   'Regional VP or VP [Geography] posted — geographic scaling signal',
   1, NULL, 30, 0.90),

  (NULL, 'regional_md_posted',      'geographic_expansion',
   'Regional MD or Managing Director [Geography] posted',
   1, NULL, 30, 0.92),

  (NULL, 'head_of_region_posted',   'geographic_expansion',
   'Head of [Region] posted — dedicated regional leadership signal',
   1, NULL, 30, 0.88),

  (NULL, 'market_entry_cluster',    'geographic_expansion',
   '3+ geo leadership roles for same region in 60 days — major market commitment',
   3, NULL, 60, 0.95),

  (NULL, 'geo_leadership_wave',     'geographic_expansion',
   '5+ companies posting geo leadership roles for same region in 30 days — sector entering market',
   5, NULL, 30, 0.90)
ON CONFLICT DO NOTHING;
