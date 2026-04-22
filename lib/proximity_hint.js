// ═══════════════════════════════════════════════════════════════════════════════
// lib/proximity_hint.js — Canonical ProximityHint shape + SQL helpers.
// Every endpoint that returns a person MUST attach a `proximity` field following
// this contract. See docs/api/proximity.md.
// ═══════════════════════════════════════════════════════════════════════════════

const { band } = require('./scoring/proximity');

/**
 * ProximityHint:
 * {
 *   best_path: {
 *     member_user_id, member_name, score, band,
 *     last_contact_at, last_contact_channel,
 *     factors?: { currency, history, weight, reciprocity }   // only on expanded
 *   } | null,
 *   backup_paths_count: number,     // paths >= 0.2, excluding best_path
 *   pooled: boolean,                // true when resolved via huddle lens
 *   paths?: Array<...>              // only on dedicated /proximity endpoint
 * }
 */

const SCORE_THRESHOLD = 0.2;

// Build a single CTE fragment that computes best-path-per-person + backup count.
// Caller substitutes $tenant placeholder(s) appropriately.
//
// Usage pattern:
//   const {cte, select} = buildProximityCTE({ alias: 'prox', scoreMin: 0.2 });
//   const sql = `WITH ${cte} SELECT p.*, ${select} FROM people p LEFT JOIN prox ON prox.person_id = p.id WHERE ...`;
//
// For huddle-pooled queries, pass { tenantIds: [...] } instead of a single tenantId.
function buildBestPathSQL({ tenantParam = '$1', scoreMin = SCORE_THRESHOLD, huddleParam = null } = {}) {
  const tenantClause = huddleParam
    ? `tp.tenant_id = ANY(${huddleParam}::uuid[])`
    : `tp.tenant_id = ${tenantParam}`;
  return `
    WITH ranked_prox AS (
      SELECT
        tp.person_id,
        tp.team_member_id AS member_user_id,
        u.name AS member_name,
        tp.relationship_strength AS score,
        tp.last_interaction_at,
        tp.last_interaction_channel,
        tp.score_factors,
        tp.tenant_id AS edge_tenant_id,
        ROW_NUMBER() OVER (PARTITION BY tp.person_id ORDER BY tp.relationship_strength DESC) AS rn,
        COUNT(*) OVER (PARTITION BY tp.person_id) AS total_paths
      FROM team_proximity tp
      JOIN users u ON u.id = tp.team_member_id
      WHERE tp.relationship_type = 'composite'
        AND tp.relationship_strength >= ${scoreMin}
        AND ${tenantClause}
    ),
    best_prox AS (
      SELECT person_id, member_user_id, member_name, score,
             last_interaction_at, last_interaction_channel, score_factors,
             edge_tenant_id,
             GREATEST(total_paths - 1, 0) AS backup_count
      FROM ranked_prox WHERE rn = 1
    )`;
}

// Convert a joined row (produced by best_prox above) into the ProximityHint shape.
// Expects r.member_user_id, r.member_name, r.score, r.last_interaction_at,
//         r.last_interaction_channel, r.score_factors, r.backup_count, [r.edge_tenant_id]
function rowToHint(r, { expanded = false, callerTenantId = null } = {}) {
  if (!r || !r.member_user_id) {
    return { best_path: null, backup_paths_count: 0, pooled: false };
  }
  const score = parseFloat(r.score);
  const best = {
    member_user_id: r.member_user_id,
    member_name: r.member_name,
    score: Math.round(score * 10000) / 10000,
    band: band(score),
    last_contact_at: r.last_interaction_at,
    last_contact_channel: r.last_interaction_channel,
  };
  if (expanded && r.score_factors) {
    best.factors = r.score_factors;
  }
  const pooled = callerTenantId != null && r.edge_tenant_id != null
    ? r.edge_tenant_id !== callerTenantId
    : false;
  return {
    best_path: best,
    backup_paths_count: parseInt(r.backup_count) || 0,
    pooled,
  };
}

// For the dedicated /api/people/:id/proximity endpoint — returns all paths.
async function loadAllPaths(pool, personId, { tenantIds = null, tenantId = null, scoreMin = SCORE_THRESHOLD } = {}) {
  const sql = `
    SELECT tp.team_member_id AS member_user_id, u.name AS member_name,
           tp.relationship_strength AS score, tp.last_interaction_at,
           tp.last_interaction_channel, tp.score_factors, tp.tenant_id AS edge_tenant_id
    FROM team_proximity tp
    JOIN users u ON u.id = tp.team_member_id
    WHERE tp.person_id = $1
      AND tp.relationship_type = 'composite'
      AND tp.relationship_strength >= $2
      AND ${tenantIds ? 'tp.tenant_id = ANY($3::uuid[])' : 'tp.tenant_id = $3'}
    ORDER BY tp.relationship_strength DESC`;
  const params = [personId, scoreMin, tenantIds || tenantId];
  const { rows } = await pool.query(sql, params);
  const callerTenant = tenantId || (tenantIds && tenantIds[0]);
  return rows.map(r => ({
    member_user_id: r.member_user_id,
    member_name: r.member_name,
    score: Math.round(parseFloat(r.score) * 10000) / 10000,
    band: band(parseFloat(r.score)),
    last_contact_at: r.last_interaction_at,
    last_contact_channel: r.last_interaction_channel,
    factors: r.score_factors,
    pooled: tenantIds ? (r.edge_tenant_id !== callerTenant) : false,
  }));
}

module.exports = {
  buildBestPathSQL,
  rowToHint,
  loadAllPaths,
  SCORE_THRESHOLD,
};
