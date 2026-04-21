-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: companies.relationship_state + computed_at
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- relationship_state reflects COMMERCIAL lifecycle state (has mandate history?
-- active client? warm without history?), distinct from company_relationships.
-- relationship_tier which reflects ENGAGEMENT health (critical/active/monitor/gap/quiet).

ALTER TABLE companies ADD COLUMN IF NOT EXISTS relationship_state TEXT
  CHECK (relationship_state IN (
    'active_client', 'ex_client', 'warm_non_client', 'cool_non_client', 'cold_non_client'
  ));

ALTER TABLE companies ADD COLUMN IF NOT EXISTS relationship_state_computed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_companies_relationship_state
  ON companies (tenant_id, relationship_state)
  WHERE relationship_state IS NOT NULL;
