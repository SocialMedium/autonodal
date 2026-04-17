// lib/tenant.js
// PIPELINE-CONTEXT: Uses pool.query intentionally — resolves tenant context
// for scripts running outside HTTP request context. Queries tenants table.
// Helpers for tenant context resolution

const ML_TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

/**
 * Get tenant context for a script run outside of an HTTP request.
 * Used by scheduled scripts (harvest, score, embed) that run as Tenant Zero.
 */
async function getTenantContext(pool, tenantId = ML_TENANT_ID) {
  const { rows } = await pool.query(
    'SELECT * FROM tenants WHERE id = $1',
    [tenantId]
  );
  if (!rows.length) throw new Error(`Tenant not found: ${tenantId}`);
  return rows[0];
}

/**
 * Get all active tenant IDs (for scripts that need to run across all tenants).
 * Phase 1 will only return MitchelLake. Phase 2+ returns all tenants.
 */
async function getAllTenantIds(pool) {
  const { rows } = await pool.query(
    "SELECT id FROM tenants WHERE plan != 'trial' OR trial_ends_at > NOW()"
  );
  return rows.map(r => r.id);
}

module.exports = { getTenantContext, getAllTenantIds, ML_TENANT_ID };
