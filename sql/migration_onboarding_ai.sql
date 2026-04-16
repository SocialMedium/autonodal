-- ═══════════════════════════════════════════════════════════════════════════
-- Migration: Onboarding AI — Field Mapping + Health Monitor
-- ═══════════════════════════════════════════════════════════════════════════

-- Stores AI-inferred field mappings per integration connection
CREATE TABLE IF NOT EXISTS field_mapping_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  connection_type TEXT NOT NULL,           -- 'google', 'hubspot', 'airtable', 'csv'
  connection_ref TEXT,                     -- user_google_accounts.id or other ref
  entity_type TEXT NOT NULL,              -- 'people', 'companies'
  sample_size INTEGER NOT NULL DEFAULT 0,
  mappings JSONB NOT NULL DEFAULT '[]',   -- array of mapping objects
  auto_applied INTEGER DEFAULT 0,
  review_required INTEGER DEFAULT 0,
  skipped INTEGER DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'pending', -- pending, reviewing, approved, applied
  reviewed_at TIMESTAMPTZ,
  applied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Onboarding AI sessions — tracks the magic onboarding flow per tenant
CREATE TABLE IF NOT EXISTS onboarding_ai_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  current_phase TEXT NOT NULL DEFAULT 'connect',
  -- phases: connect → field_mapping → feed_config → health_check → complete
  phase_data JSONB NOT NULL DEFAULT '{}',
  completed_phases TEXT[] DEFAULT '{}',
  started_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id)
);

-- Health issues per tenant — one active issue per type
CREATE TABLE IF NOT EXISTS tenant_health_issues (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  issue_type TEXT NOT NULL,
  severity TEXT NOT NULL,           -- info | warning | error
  title TEXT NOT NULL,
  explanation TEXT NOT NULL,
  impact TEXT,
  actions JSONB NOT NULL DEFAULT '[]',
  affected_count INTEGER,
  context JSONB DEFAULT '{}',
  resolved BOOLEAN DEFAULT false,
  resolved_at TIMESTAMPTZ,
  resolution_action TEXT,
  first_detected_at TIMESTAMPTZ DEFAULT NOW(),
  last_checked_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id, issue_type)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_field_mapping_sessions_tenant
  ON field_mapping_sessions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_field_mapping_sessions_status
  ON field_mapping_sessions(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_onboarding_ai_sessions_tenant
  ON onboarding_ai_sessions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_health_issues_tenant
  ON tenant_health_issues(tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_health_issues_unresolved
  ON tenant_health_issues(tenant_id, resolved)
  WHERE resolved = false;
