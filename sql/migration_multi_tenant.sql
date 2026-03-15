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
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_signals ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_scores ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_content ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE person_content_sources ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE team_proximity ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE clients ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE projects ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE searches ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE search_candidates ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE search_activities ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE search_matches ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE placements ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE client_contacts ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE client_financials ADD COLUMN IF NOT EXISTS tenant_id UUID REFERENCES tenants(id) DEFAULT '00000000-0000-0000-0000-000000000001';
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
UPDATE clients SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE projects SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE searches SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE search_candidates SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE placements SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE signal_dispatches SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;
UPDATE sessions SET tenant_id = '00000000-0000-0000-0000-000000000001' WHERE tenant_id IS NULL;

-- ─── INDEXES FOR TENANT-SCOPED QUERIES ───────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_people_tenant ON people(tenant_id);
CREATE INDEX IF NOT EXISTS idx_companies_tenant ON companies(tenant_id);
CREATE INDEX IF NOT EXISTS idx_interactions_tenant ON interactions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_events_tenant ON signal_events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_person_signals_tenant ON person_signals(tenant_id);
CREATE INDEX IF NOT EXISTS idx_person_scores_tenant ON person_scores(tenant_id);
CREATE INDEX IF NOT EXISTS idx_clients_tenant ON clients(tenant_id);
CREATE INDEX IF NOT EXISTS idx_searches_tenant ON searches(tenant_id);
CREATE INDEX IF NOT EXISTS idx_search_candidates_tenant ON search_candidates(tenant_id);
CREATE INDEX IF NOT EXISTS idx_placements_tenant ON placements(tenant_id);
CREATE INDEX IF NOT EXISTS idx_signal_dispatches_tenant ON signal_dispatches(tenant_id);
CREATE INDEX IF NOT EXISTS idx_team_proximity_tenant ON team_proximity(tenant_id);
CREATE INDEX IF NOT EXISTS idx_external_documents_tenant ON external_documents(tenant_id);
