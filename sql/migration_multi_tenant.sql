-- ═══════════════════════════════════════════════════════════════════════════════
-- MULTI-TENANT MIGRATION — Deploy 1
-- Adds tenants table and tenant_id to all data tables
-- MitchelLake = Tenant Zero (deterministic UUID)
-- All existing data assigned to MitchelLake via column DEFAULT
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── TENANTS TABLE ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  vertical TEXT NOT NULL CHECK (vertical IN ('talent', 'revenue', 'mandate')),

  -- Branding
  logo_url TEXT,
  primary_color TEXT DEFAULT '#2563eb',

  -- Network focus
  focus_geographies TEXT[] DEFAULT '{}',
  focus_sectors TEXT[] DEFAULT '{}',

  -- Subscription
  plan TEXT DEFAULT 'trial' CHECK (plan IN ('trial', 'starter', 'growth', 'enterprise')),
  trial_ends_at TIMESTAMPTZ,

  -- Onboarding state
  onboarding_complete BOOLEAN DEFAULT FALSE,
  network_analysed_at TIMESTAMPTZ,
  feeds_configured_at TIMESTAMPTZ,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed MitchelLake as Tenant Zero with deterministic UUID
INSERT INTO tenants (
  id, name, slug, vertical,
  focus_geographies, focus_sectors,
  plan, onboarding_complete
) VALUES (
  '00000000-0000-0000-0000-000000000001',
  'MitchelLake',
  'mitchellake',
  'talent',
  ARRAY['AU', 'SG', 'UK', 'US'],
  ARRAY['Technology', 'FinTech', 'SaaS', 'Web3', 'HealthTech', 'DeepTech'],
  'enterprise',
  TRUE
) ON CONFLICT (slug) DO NOTHING;

-- ─── ADD tenant_id TO ALL DATA TABLES ────────────────────────────────────────
-- Using DEFAULT so all existing rows are automatically assigned to MitchelLake
-- and existing INSERT statements continue to work without changes

ALTER TABLE users ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE people ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE companies ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE interactions ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id);
ALTER TABLE signal_events ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_signals ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_scores ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_content ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_content_sources ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE team_proximity ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
-- Use new table names (old names are VIEWs after rename, can't ALTER VIEWs)
DO $$ BEGIN ALTER TABLE accounts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE clients ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE engagements ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE projects ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE searches ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE pipeline_contacts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE search_candidates ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE pipeline_activities ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE search_activities ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
ALTER TABLE search_matches ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
DO $$ BEGIN ALTER TABLE conversions ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE placements ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE account_contacts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE client_contacts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
DO $$ BEGIN ALTER TABLE account_financials ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; EXCEPTION WHEN undefined_table THEN ALTER TABLE client_financials ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001'; END $$;
ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE user_google_accounts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';

-- ─── BACKFILL: set tenant_id on any NULLs (safety net) ──────────────────────

UPDATE users SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE people SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE companies SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE interactions SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE signal_events SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE external_documents SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE person_signals SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE person_scores SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE team_proximity SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE accounts SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE engagements SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE opportunities SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE pipeline_contacts SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE conversions SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE signal_dispatches SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE sessions SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;

-- ─── INDEXES FOR TENANT-SCOPED QUERIES ───────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_people_tenant ON people(tenant_id);
CREATE INDEX IF NOT EXISTS idx_companies_tenant ON companies(tenant_id);
CREATE INDEX IF NOT EXISTS idx_interactions_tenant ON interactions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_events_tenant ON signal_events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_person_signals_tenant ON person_signals(tenant_id);
CREATE INDEX IF NOT EXISTS idx_person_scores_tenant ON person_scores(tenant_id);
CREATE INDEX IF NOT EXISTS idx_accounts_tenant ON accounts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_tenant ON opportunities(tenant_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_contacts_tenant ON pipeline_contacts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_conversions_tenant ON conversions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_dispatches_tenant ON signal_dispatches(tenant_id);
CREATE INDEX IF NOT EXISTS idx_team_proximity_tenant ON team_proximity(tenant_id);
CREATE INDEX IF NOT EXISTS idx_external_documents_tenant ON external_documents(tenant_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- DEPLOY 2: TABLE RENAMES WITH BACKWARD-COMPATIBLE VIEWS
-- VIEWs ensure any code still using old names continues to work
-- ═══════════════════════════════════════════════════════════════════════════════

-- Helper: only rename if the old table exists and new table doesn't
-- Each rename creates a VIEW with the old name for backward compatibility

-- clients → accounts
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='clients')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='accounts') THEN
    ALTER TABLE clients RENAME TO accounts;
    CREATE OR REPLACE VIEW clients AS SELECT * FROM accounts;
  END IF;
END $$;

-- projects → engagements
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='projects')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='engagements') THEN
    ALTER TABLE projects RENAME TO engagements;
    CREATE OR REPLACE VIEW projects AS SELECT * FROM engagements;
  END IF;
END $$;

-- searches → opportunities
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='searches')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunities') THEN
    ALTER TABLE searches RENAME TO opportunities;
    CREATE OR REPLACE VIEW searches AS SELECT * FROM opportunities;
  END IF;
END $$;

-- search_candidates → pipeline_contacts
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='search_candidates')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='pipeline_contacts') THEN
    ALTER TABLE search_candidates RENAME TO pipeline_contacts;
    CREATE OR REPLACE VIEW search_candidates AS SELECT * FROM pipeline_contacts;
  END IF;
END $$;

-- search_activities → pipeline_activities
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='search_activities')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='pipeline_activities') THEN
    ALTER TABLE search_activities RENAME TO pipeline_activities;
    CREATE OR REPLACE VIEW search_activities AS SELECT * FROM pipeline_activities;
  END IF;
END $$;

-- client_contacts → account_contacts
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='client_contacts')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='account_contacts') THEN
    ALTER TABLE client_contacts RENAME TO account_contacts;
    CREATE OR REPLACE VIEW client_contacts AS SELECT * FROM account_contacts;
  END IF;
END $$;

-- placements → conversions
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='placements')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='conversions') THEN
    ALTER TABLE placements RENAME TO conversions;
    CREATE OR REPLACE VIEW placements AS SELECT * FROM conversions;
  END IF;
END $$;

-- client_financials → account_financials
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='client_financials')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='account_financials') THEN
    ALTER TABLE client_financials RENAME TO account_financials;
    CREATE OR REPLACE VIEW client_financials AS SELECT * FROM account_financials;
  END IF;
END $$;

-- Add flexible metadata columns to conversions (if not already there)
DO $$ BEGIN
  ALTER TABLE conversions ADD COLUMN IF NOT EXISTS value NUMERIC(12,2);
  ALTER TABLE conversions ADD COLUMN IF NOT EXISTS converted_at TIMESTAMPTZ;
EXCEPTION WHEN undefined_table THEN NULL;
END $$;

-- ═══════════════════════════════════════════════════════════════════════════════
-- DEPLOY 5: COMMUNITY FEED LAYER
-- feed_inventory = platform infrastructure (no tenant_id)
-- tenant_feeds = activation join per tenant
-- feed_proposals = community submission queue
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS feed_inventory (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  url TEXT NOT NULL UNIQUE,
  feed_type TEXT DEFAULT 'rss' CHECK (feed_type IN ('rss', 'atom', 'api', 'scrape')),

  geographies TEXT[] DEFAULT '{}',
  sectors TEXT[] DEFAULT '{}',
  signal_types TEXT[] DEFAULT '{}',
  company_tiers TEXT[] DEFAULT '{}',
  verticals TEXT[] DEFAULT '{}',
  language TEXT DEFAULT 'en',

  quality_score NUMERIC(3,2) DEFAULT 0,
  velocity TEXT CHECK (velocity IN ('realtime', 'daily', 'weekly')),
  avg_signals_per_week NUMERIC(6,2),

  status TEXT DEFAULT 'active' CHECK (status IN ('active', 'pending_review', 'disabled')),

  submitted_by_tenant_id UUID,
  approved_at TIMESTAMPTZ,

  total_ratings INTEGER DEFAULT 0,
  avg_rating NUMERIC(3,2),

  legacy_rss_source_id UUID,

  last_fetched_at TIMESTAMPTZ,
  last_error TEXT,
  error_count INTEGER DEFAULT 0,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Migrate existing rss_sources into feed_inventory
INSERT INTO feed_inventory (
  name, url, feed_type, signal_types, status,
  last_fetched_at, last_error, legacy_rss_source_id, quality_score
)
SELECT
  name, url, 'rss', signal_types,
  CASE WHEN enabled = TRUE THEN 'active' ELSE 'disabled' END,
  last_fetched_at, last_error, id,
  COALESCE(credibility_score, 0)
FROM rss_sources
ON CONFLICT (url) DO NOTHING;

CREATE TABLE IF NOT EXISTS tenant_feeds (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  feed_id UUID NOT NULL REFERENCES feed_inventory(id) ON DELETE CASCADE,

  selection_method TEXT DEFAULT 'manual'
    CHECK (selection_method IN ('recommended', 'manual', 'network_matched')),
  match_reason JSONB,

  local_signal_yield INTEGER DEFAULT 0,
  last_rated_at TIMESTAMPTZ,
  tenant_rating INTEGER CHECK (tenant_rating BETWEEN 1 AND 5),

  active BOOLEAN DEFAULT TRUE,
  activated_at TIMESTAMPTZ DEFAULT NOW(),

  UNIQUE(tenant_id, feed_id)
);

-- Activate all existing feeds for MitchelLake
INSERT INTO tenant_feeds (tenant_id, feed_id, selection_method, active)
SELECT
  '00000000-0000-0000-0000-000000000001',
  fi.id,
  'manual',
  TRUE
FROM feed_inventory fi
WHERE fi.legacy_rss_source_id IS NOT NULL
ON CONFLICT (tenant_id, feed_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS feed_proposals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  proposed_url TEXT NOT NULL,
  proposed_name TEXT,
  proposed_geographies TEXT[],
  proposed_sectors TEXT[],
  proposed_signal_types TEXT[],
  rationale TEXT,
  status TEXT DEFAULT 'pending'
    CHECK (status IN ('pending', 'approved', 'rejected', 'duplicate')),
  reviewed_at TIMESTAMPTZ,
  reviewer_notes TEXT,
  feed_id UUID REFERENCES feed_inventory(id),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_feeds_tenant ON tenant_feeds(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_feeds_feed ON tenant_feeds(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_inventory_status ON feed_inventory(status);
CREATE INDEX IF NOT EXISTS idx_feed_proposals_tenant ON feed_proposals(tenant_id);
