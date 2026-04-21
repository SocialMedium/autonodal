-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: lead_claims + signal_outcomes
-- ═══════════════════════════════════════════════════════════════════════════════

-- Lead claims — one consultant owns a signal at a time
CREATE TABLE IF NOT EXISTS lead_claims (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  signal_id UUID NOT NULL REFERENCES signal_events(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id),
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  pipeline_stage TEXT NOT NULL DEFAULT 'claimed'
    CHECK (pipeline_stage IN ('claimed','contacted','meeting','proposal','mandate','lost')),
  stage_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  notes TEXT,
  released_at TIMESTAMPTZ,
  released_reason TEXT,
  UNIQUE (signal_id)
);

CREATE INDEX IF NOT EXISTS idx_lead_claims_tenant_user ON lead_claims(tenant_id, user_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_lead_claims_stage ON lead_claims(tenant_id, pipeline_stage) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_lead_claims_signal ON lead_claims(signal_id);

-- Signal outcomes — final resolution, drives forward calibration
CREATE TABLE IF NOT EXISTS signal_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  signal_id UUID NOT NULL REFERENCES signal_events(id) ON DELETE CASCADE,
  outcome TEXT NOT NULL
    CHECK (outcome IN (
      'converted_mandate', 'contact_only', 'no_response',
      'wrong_moment', 'window_expired', 'declined'
    )),
  claimed_at TIMESTAMPTZ,
  converted_at TIMESTAMPTZ,
  lead_time_days INTEGER,
  mandate_id UUID,
  revenue_local NUMERIC,
  revenue_currency TEXT,
  resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_by UUID REFERENCES users(id),
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_signal_outcomes_tenant ON signal_outcomes(tenant_id, resolved_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_signal ON signal_outcomes(signal_id);
CREATE INDEX IF NOT EXISTS idx_signal_outcomes_outcome ON signal_outcomes(tenant_id, outcome);

-- RLS
ALTER TABLE lead_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE lead_claims FORCE ROW LEVEL SECURITY;
DO $$ BEGIN
  CREATE POLICY tenant_isolation_lead_claims ON lead_claims
    USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE signal_outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_outcomes FORCE ROW LEVEL SECURITY;
DO $$ BEGIN
  CREATE POLICY tenant_isolation_signal_outcomes ON signal_outcomes
    USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Grants
DO $$ BEGIN
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON lead_claims TO autonodal_app';
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON signal_outcomes TO autonodal_app';
EXCEPTION WHEN undefined_object THEN NULL;
END $$;
