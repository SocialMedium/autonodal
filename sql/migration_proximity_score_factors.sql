-- ═══════════════════════════════════════════════════════════════════════════════
-- migration_proximity_score_factors.sql
-- Adds 4-factor score breakdown to team_proximity:
--   currency · history · weight · reciprocity
-- See docs/audits/proximity_audit_2026-04-22.md for context.
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE team_proximity
  ADD COLUMN IF NOT EXISTS score_factors JSONB DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS currency_score NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS history_score NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS weight_score NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS reciprocity_score NUMERIC(4,3),
  ADD COLUMN IF NOT EXISTS last_interaction_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS first_interaction_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_interaction_channel TEXT,
  ADD COLUMN IF NOT EXISTS interaction_count_inbound INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS interaction_count_outbound INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_computed_at TIMESTAMPTZ;

-- Composite "best path per person" index
CREATE INDEX IF NOT EXISTS idx_team_proximity_tenant_person_strength
  ON team_proximity (tenant_id, person_id, relationship_strength DESC);

-- Support the nightly recompute lookup by member + freshness
CREATE INDEX IF NOT EXISTS idx_team_proximity_tenant_member_computed
  ON team_proximity (tenant_id, team_member_id, last_computed_at);
