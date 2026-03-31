-- ═══════════════════════════════════════════════════════════════════════════════
-- ROW-LEVEL SECURITY (RLS) — immutable tenant isolation safety net
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- This provides database-level enforcement of tenant isolation as a second
-- layer of defence behind application-level filtering. Even if a query
-- accidentally omits a tenant_id WHERE clause, RLS prevents cross-tenant
-- data leakage at the Postgres level.
--
-- How it works:
--   1. Application sets current_setting('app.current_tenant') per connection
--   2. RLS policies restrict SELECT/INSERT/UPDATE/DELETE to matching tenant_id
--   3. Superuser/owner bypasses RLS (for migrations, admin scripts)
--
-- Run: psql $DATABASE_URL -f sql/migration_rls.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- Helper function to get current tenant (returns NULL if not set, allowing superuser bypass)
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS UUID AS $$
BEGIN
  RETURN NULLIF(current_setting('app.current_tenant', true), '')::UUID;
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- ENABLE RLS ON ALL TENANT-SCOPED TABLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Core data tables
ALTER TABLE people ENABLE ROW LEVEL SECURITY;
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE external_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE person_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE person_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE team_proximity ENABLE ROW LEVEL SECURITY;

-- CRM / Pipeline tables
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversions ENABLE ROW LEVEL SECURITY;
ALTER TABLE account_financials ENABLE ROW LEVEL SECURITY;
ALTER TABLE opportunities ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_contacts ENABLE ROW LEVEL SECURITY;

-- Dispatch & content tables
ALTER TABLE signal_dispatches ENABLE ROW LEVEL SECURITY;
ALTER TABLE search_matches ENABLE ROW LEVEL SECURITY;

-- User & session tables
ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;

-- ═══════════════════════════════════════════════════════════════════════════════
-- CREATE POLICIES — one policy per table
-- Pattern: allow access when tenant_id matches OR no tenant is set (superuser/migration)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Core data
CREATE POLICY tenant_isolation_people ON people
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_companies ON companies
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_signal_events ON signal_events
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_interactions ON interactions
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_external_documents ON external_documents
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_person_signals ON person_signals
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_person_scores ON person_scores
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_team_proximity ON team_proximity
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- CRM / Pipeline
CREATE POLICY tenant_isolation_accounts ON accounts
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_conversions ON conversions
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_account_financials ON account_financials
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_opportunities ON opportunities
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_pipeline_contacts ON pipeline_contacts
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- Dispatch & content
CREATE POLICY tenant_isolation_signal_dispatches ON signal_dispatches
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

CREATE POLICY tenant_isolation_search_matches ON search_matches
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- Events
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_events ON events
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

ALTER TABLE event_sources ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_event_sources ON event_sources
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- Sessions
CREATE POLICY tenant_isolation_sessions ON sessions
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());
