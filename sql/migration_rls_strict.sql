-- ═══════════════════════════════════════════════════════════════════════════
-- Migration: RLS Strict Enforcement
-- ═══════════════════════════════════════════════════════════════════════════
--
-- CRITICAL: This changes current_tenant_id() from permissive (NULL = allow all)
-- to strict (NULL = match nothing). After this migration:
--   - Queries via TenantDB (app role with SET LOCAL) → see tenant's data only
--   - Queries via platformPool (postgres superuser) → bypass RLS entirely
--   - Queries without tenant context (app role, no SET LOCAL) → see 0 rows
--
-- Rollback: restore the old function body that returns NULL instead of sentinel

-- Step 1: Update current_tenant_id() to strict mode
CREATE OR REPLACE FUNCTION current_tenant_id()
RETURNS UUID AS $$
BEGIN
  -- Return the tenant ID set by TenantDB before each query.
  -- If not set (empty string or NULL), return a nil UUID that matches no row.
  -- This replaces the NULL-returns-all behaviour with NULL-returns-nothing.
  RETURN COALESCE(
    NULLIF(current_setting('app.current_tenant', true), '')::UUID,
    '00000000-0000-0000-0000-000000000000'::UUID
  );
EXCEPTION
  WHEN invalid_text_representation THEN
    -- Malformed setting value → deny access
    RETURN '00000000-0000-0000-0000-000000000000'::UUID;
  WHEN OTHERS THEN
    -- Any other error → deny access
    RETURN '00000000-0000-0000-0000-000000000000'::UUID;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

-- Note: The existing RLS policies all use:
--   (current_tenant_id() IS NULL) OR (tenant_id IS NULL) OR (tenant_id = current_tenant_id())
--
-- With the old function, current_tenant_id() returned NULL → first clause was TRUE → all rows visible.
-- With the new function, current_tenant_id() returns '0000...0000' → first clause is FALSE →
--   only rows where tenant_id IS NULL (platform data) or tenant_id = '0000...0000' (no match) are visible.
--
-- Effective result:
--   - Without tenant context: only platform-wide data (tenant_id IS NULL) is visible
--   - With tenant context: own data + platform-wide data
--   - platformPool (superuser): bypasses RLS entirely
