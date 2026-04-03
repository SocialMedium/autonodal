-- ═══════════════════════════════════════════════════════════════════════════════
-- RLS STRICT MODE — Raises if app.current_tenant is not set
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- WARNING: Only activate this AFTER all application code uses TenantDB.
-- The permissive version (NULL = allow) must remain active until migration is complete.
--
-- To activate: psql $DATABASE_URL -f sql/rls_strict_mode.sql
-- To revert:   psql $DATABASE_URL -f sql/migration_rls.sql
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION current_tenant_id()
RETURNS UUID AS $$
DECLARE
  raw_tenant TEXT;
BEGIN
  BEGIN
    raw_tenant := current_setting('app.current_tenant');
  EXCEPTION WHEN undefined_object THEN
    RAISE EXCEPTION
      'TENANT ISOLATION VIOLATION: app.current_tenant is not set. '
      'All queries must run within a tenant context. '
      'Use TenantDB client or SET LOCAL app.current_tenant before querying.';
  END;

  IF raw_tenant IS NULL OR raw_tenant = '' THEN
    RAISE EXCEPTION
      'TENANT ISOLATION VIOLATION: app.current_tenant is empty. '
      'Tenant ID must be a valid UUID.';
  END IF;

  RETURN raw_tenant::UUID;
EXCEPTION WHEN invalid_text_representation THEN
  RAISE EXCEPTION
    'TENANT ISOLATION VIOLATION: app.current_tenant value "%" is not a valid UUID.',
    raw_tenant;
END;
$$ LANGUAGE plpgsql STABLE;

-- Also FORCE RLS on all tables (applies even to table owners)
ALTER TABLE people FORCE ROW LEVEL SECURITY;
ALTER TABLE companies FORCE ROW LEVEL SECURITY;
ALTER TABLE signal_events FORCE ROW LEVEL SECURITY;
ALTER TABLE interactions FORCE ROW LEVEL SECURITY;
ALTER TABLE external_documents FORCE ROW LEVEL SECURITY;
ALTER TABLE person_signals FORCE ROW LEVEL SECURITY;
ALTER TABLE person_scores FORCE ROW LEVEL SECURITY;
ALTER TABLE team_proximity FORCE ROW LEVEL SECURITY;
ALTER TABLE accounts FORCE ROW LEVEL SECURITY;
ALTER TABLE conversions FORCE ROW LEVEL SECURITY;
ALTER TABLE account_financials FORCE ROW LEVEL SECURITY;
ALTER TABLE opportunities FORCE ROW LEVEL SECURITY;
ALTER TABLE pipeline_contacts FORCE ROW LEVEL SECURITY;
ALTER TABLE signal_dispatches FORCE ROW LEVEL SECURITY;
ALTER TABLE search_matches FORCE ROW LEVEL SECURITY;
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
