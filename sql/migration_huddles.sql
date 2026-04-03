-- ═══════════════════════════════════════════════════════════════════════════════
-- Individual · Huddle · Company Architecture
-- ═══════════════════════════════════════════════════════════════════════════════

-- 1. Extend tenants with type
ALTER TABLE tenants
  ADD COLUMN IF NOT EXISTS tenant_type TEXT NOT NULL DEFAULT 'company',
  ADD COLUMN IF NOT EXISTS parent_tenant_id UUID REFERENCES tenants(id),
  ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'private';

UPDATE tenants SET tenant_type = 'company'
WHERE slug IN ('mitchellake', 'themiracel', 'am-asia-tax');

-- 2. Huddles
CREATE TABLE IF NOT EXISTS huddles (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name              TEXT NOT NULL,
  slug              TEXT NOT NULL UNIQUE,
  description       TEXT,
  purpose           TEXT,
  creator_tenant_id UUID NOT NULL REFERENCES tenants(id),
  status            TEXT NOT NULL DEFAULT 'active',
  visibility        TEXT NOT NULL DEFAULT 'private',
  signal_config     JSONB DEFAULT '{}',
  phase_label       TEXT,
  target_date       DATE,
  rolled_up_to      UUID REFERENCES tenants(id),
  rolled_up_at      TIMESTAMPTZ,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_huddles_creator ON huddles(creator_tenant_id);
CREATE INDEX IF NOT EXISTS idx_huddles_status ON huddles(status);

-- 3. Huddle membership
CREATE TABLE IF NOT EXISTS huddle_members (
  huddle_id           UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  tenant_id           UUID NOT NULL REFERENCES tenants(id),
  role                TEXT NOT NULL DEFAULT 'member',
  status              TEXT NOT NULL DEFAULT 'invited',
  invited_by          UUID REFERENCES tenants(id),
  invited_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  joined_at           TIMESTAMPTZ,
  detached_at         TIMESTAMPTZ,
  contributed_people_count    INTEGER DEFAULT 0,
  contributed_companies_count INTEGER DEFAULT 0,
  net_new_people_count        INTEGER DEFAULT 0,
  net_new_companies_count     INTEGER DEFAULT 0,
  contributed_bundle_slugs    TEXT[] DEFAULT '{}',
  PRIMARY KEY (huddle_id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_huddle_members_tenant ON huddle_members(tenant_id, status);

-- 4. Huddle person graph (reference layer, not data copy)
CREATE TABLE IF NOT EXISTS huddle_people (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  huddle_id             UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  person_id             UUID NOT NULL,
  first_contributed_by  UUID NOT NULL REFERENCES tenants(id),
  contributor_count     INTEGER NOT NULL DEFAULT 1,
  best_member_tenant_id UUID REFERENCES tenants(id),
  best_strength_score   NUMERIC(4,3),
  best_depth_type       TEXT,
  best_entry_label      TEXT,
  best_entry_reason     TEXT,
  member_connection_count INTEGER DEFAULT 1,
  total_team_interactions INTEGER DEFAULT 0,
  added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (huddle_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_huddle_people_huddle ON huddle_people(huddle_id);
CREATE INDEX IF NOT EXISTS idx_huddle_people_best ON huddle_people(huddle_id, best_strength_score DESC);

-- 5. Huddle proximity edges (member-owned, fully detachable)
CREATE TABLE IF NOT EXISTS huddle_proximity (
  huddle_id           UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  member_tenant_id    UUID NOT NULL REFERENCES tenants(id),
  person_id           UUID NOT NULL,
  strength_score      NUMERIC(4,3) NOT NULL DEFAULT 0,
  currency_score      NUMERIC(4,3),
  history_score       NUMERIC(4,3),
  depth_score         NUMERIC(4,3),
  reciprocity_score   NUMERIC(4,3),
  depth_type          TEXT,
  currency_label      TEXT,
  entry_recommendation TEXT,
  entry_action        TEXT,
  source_platform     TEXT,
  last_contact        DATE,
  interaction_count   INTEGER DEFAULT 0,
  contributed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  score_computed_at   TIMESTAMPTZ,
  PRIMARY KEY (huddle_id, member_tenant_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_huddle_proximity_huddle ON huddle_proximity(huddle_id, person_id);
CREATE INDEX IF NOT EXISTS idx_huddle_proximity_member ON huddle_proximity(member_tenant_id);

-- 6. Huddle company graph
CREATE TABLE IF NOT EXISTS huddle_companies (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  huddle_id             UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  company_id            UUID NOT NULL,
  first_contributed_by  UUID NOT NULL REFERENCES tenants(id),
  contributor_count     INTEGER NOT NULL DEFAULT 1,
  combined_signal_count INTEGER DEFAULT 0,
  added_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (huddle_id, company_id)
);

-- 7. Huddle signal pool
CREATE TABLE IF NOT EXISTS huddle_signal_pool (
  huddle_id           UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  signal_event_id     UUID NOT NULL,
  contributed_by      UUID NOT NULL REFERENCES tenants(id),
  contributor_count   INTEGER NOT NULL DEFAULT 1,
  added_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (huddle_id, signal_event_id)
);

-- 8. Huddle invite tokens
CREATE TABLE IF NOT EXISTS huddle_invites (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  huddle_id   UUID NOT NULL REFERENCES huddles(id) ON DELETE CASCADE,
  token       TEXT NOT NULL UNIQUE DEFAULT encode(digest(gen_random_uuid()::text || now()::text, 'sha256'), 'hex'),
  invited_by  UUID NOT NULL REFERENCES tenants(id),
  email       TEXT,
  role        TEXT NOT NULL DEFAULT 'member',
  status      TEXT NOT NULL DEFAULT 'pending',
  expires_at  TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days',
  accepted_at TIMESTAMPTZ,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 9. Company membership (individual <-> company)
CREATE TABLE IF NOT EXISTS company_members (
  company_tenant_id    UUID NOT NULL REFERENCES tenants(id),
  individual_tenant_id UUID NOT NULL REFERENCES tenants(id),
  role                 TEXT NOT NULL DEFAULT 'member',
  status               TEXT NOT NULL DEFAULT 'invited',
  contribution_mode    TEXT NOT NULL DEFAULT 'proximity',
  invited_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  joined_at            TIMESTAMPTZ,
  contributed_people_count    INTEGER DEFAULT 0,
  contributed_companies_count INTEGER DEFAULT 0,
  PRIMARY KEY (company_tenant_id, individual_tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_company_members_individual ON company_members(individual_tenant_id, status);

-- 10. Canonical person proximity (lives in member's sandbox)
CREATE TABLE IF NOT EXISTS person_proximity (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id           UUID NOT NULL,
  person_id           UUID NOT NULL,
  strength_score      NUMERIC(4,3) NOT NULL DEFAULT 0,
  currency_score      NUMERIC(4,3),
  history_score       NUMERIC(4,3),
  depth_score         NUMERIC(4,3),
  reciprocity_score   NUMERIC(4,3),
  first_contact_date  DATE,
  last_contact_date   DATE,
  interaction_count   INTEGER DEFAULT 0,
  inbound_count       INTEGER DEFAULT 0,
  deep_interaction_count INTEGER DEFAULT 0,
  primary_platform    TEXT,
  depth_type          TEXT,
  entry_recommendation TEXT,
  entry_action        TEXT,
  currency_label      TEXT,
  computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, person_id)
);

ALTER TABLE person_proximity ENABLE ROW LEVEL SECURITY;
ALTER TABLE person_proximity FORCE ROW LEVEL SECURITY;
DO $$ BEGIN
  CREATE POLICY tenant_isolation_person_proximity ON person_proximity
    USING (tenant_id = current_tenant_id())
    WITH CHECK (tenant_id = current_tenant_id());
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_person_proximity_strength ON person_proximity(tenant_id, strength_score DESC);

-- 11. Individual influence dashboard
CREATE TABLE IF NOT EXISTS individual_influence (
  tenant_id             UUID PRIMARY KEY REFERENCES tenants(id),
  total_people          INTEGER DEFAULT 0,
  total_companies       INTEGER DEFAULT 0,
  lent_people_unique    INTEGER DEFAULT 0,
  lent_companies_unique INTEGER DEFAULT 0,
  active_huddle_count   INTEGER DEFAULT 0,
  company_member_count  INTEGER DEFAULT 0,
  intros_surfaced       INTEGER DEFAULT 0,
  intros_made           INTEGER DEFAULT 0,
  signals_contributed   INTEGER DEFAULT 0,
  context_breakdown     JSONB DEFAULT '[]',
  computed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 12. Views
CREATE OR REPLACE VIEW v_huddle_network AS
SELECT
  hp.huddle_id, hp.person_id,
  hp.best_member_tenant_id, hp.best_strength_score,
  hp.best_depth_type, hp.best_entry_label, hp.best_entry_reason,
  hp.member_connection_count, hp.total_team_interactions
FROM huddle_people hp;

CREATE OR REPLACE VIEW v_individual_contexts AS
SELECT
  t.id as tenant_id, t.name, t.tenant_type,
  (SELECT COUNT(*) FROM huddle_members hm WHERE hm.tenant_id = t.id AND hm.status = 'active') as active_huddles,
  (SELECT COUNT(*) FROM company_members cm WHERE cm.individual_tenant_id = t.id AND cm.status = 'active') as company_memberships,
  (SELECT COALESCE(SUM(net_new_people_count),0) FROM huddle_members WHERE tenant_id = t.id AND status = 'active') as total_people_lent
FROM tenants t WHERE t.tenant_type = 'individual';
