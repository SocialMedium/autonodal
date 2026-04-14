-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Official API Sources — structured government/institutional data
-- Adds tracking table for API source state + watermarks
-- Run: node scripts/run_migration.js sql/migration_official_apis.sql
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS official_api_sources (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_key      TEXT NOT NULL UNIQUE,       -- e.g. 'uk_find_a_tender', 'edgar_8k'
  name            TEXT NOT NULL,
  description     TEXT,
  base_url        TEXT NOT NULL,
  api_type        TEXT NOT NULL,              -- 'ocds', 'sdmx', 'rest_json', 'efts', 'bulk'
  region          TEXT,                       -- 'uk', 'au', 'sg', 'ca', 'us', 'global'
  category        TEXT,                       -- 'procurement', 'insolvency', 'patents', 'statistics', 'filings'
  requires_auth   BOOLEAN DEFAULT FALSE,
  auth_config     JSONB,                      -- { type: 'api_key', header: 'Authorization', env_var: 'COMPANIES_HOUSE_KEY' }

  -- Watermarking for incremental harvest
  last_fetched_at TIMESTAMPTZ,
  watermark       JSONB,                      -- { last_offset: 500, last_published: '2026-04-10', cursor: 'abc' }
  fetch_interval_minutes INTEGER DEFAULT 360,

  -- Stats
  total_fetched   INTEGER DEFAULT 0,
  total_signals   INTEGER DEFAULT 0,
  last_error      TEXT,
  consecutive_errors INTEGER DEFAULT 0,
  enabled         BOOLEAN DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Seed the sources
INSERT INTO official_api_sources (source_key, name, description, base_url, api_type, region, category, requires_auth, auth_config, fetch_interval_minutes)
VALUES
  -- ── PROCUREMENT (OCDS) ────────────────────────────────────────────────────
  ('uk_find_a_tender', 'Find a Tender (UK)', 'UK government procurement notices and contract awards via OCDS API',
   'https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages', 'ocds', 'uk', 'procurement',
   false, NULL, 360),

  ('au_austender', 'AusTender', 'Australian government contract notices and awards via OCDS API',
   'https://api.tenders.gov.au/ocds/findByDates/contractPublished', 'ocds', 'au', 'procurement',
   false, NULL, 360),

  -- ── INSOLVENCY / COMPANY FILINGS ──────────────────────────────────────────
  ('uk_companies_house', 'Companies House (UK)', 'UK company filings: insolvency, director changes, charges, accounts',
   'https://api.company-information.service.gov.uk', 'rest_json', 'uk', 'insolvency',
   true, '{"type":"basic_auth","env_var":"COMPANIES_HOUSE_KEY"}', 240),

  -- ── SEC FILINGS ───────────────────────────────────────────────────────────
  ('us_edgar_8k', 'SEC EDGAR 8-K', 'US material event filings via EDGAR EFTS full-text search',
   'https://efts.sec.gov/LATEST/search-index', 'efts', 'us', 'filings',
   false, NULL, 120),

  -- ── PATENTS ───────────────────────────────────────────────────────────────
  ('us_patentsview', 'USPTO PatentsView', 'US patent grants and applications — assignee, inventor, classification data',
   'https://search.patentsview.org/api/v1/patent', 'rest_json', 'us', 'patents',
   false, NULL, 1440),

  -- ── STATISTICS ────────────────────────────────────────────────────────────
  ('uk_ons', 'UK ONS', 'Office for National Statistics — economic releases, labour market, GDP',
   'https://api.beta.ons.gov.uk/v1', 'rest_json', 'uk', 'statistics',
   false, NULL, 720),

  ('au_abs', 'Australian Bureau of Statistics', 'ABS SDMX data API — labour force, CPI, GDP, trade',
   'https://data.api.abs.gov.au/rest', 'sdmx', 'au', 'statistics',
   false, NULL, 720),

  ('sg_singstat', 'SingStat', 'Singapore Department of Statistics — economic indicators, trade, employment',
   'https://tablebuilder.singstat.gov.sg/api/table/tabledata', 'rest_json', 'sg', 'statistics',
   false, NULL, 720),

  ('ca_statcan', 'Statistics Canada', 'StatCan Web Data Service — labour force, GDP, trade, CPI',
   'https://www150.statcan.gc.ca/t1/tbl1/en/dtl!downloadTbl/csvDownload.action', 'rest_json', 'ca', 'statistics',
   false, NULL, 720)

ON CONFLICT (source_key) DO NOTHING;
