// ═══════════════════════════════════════════════════════════════════════════════
// lib/relationship_state.js — Commercial relationship state classifier
// ═══════════════════════════════════════════════════════════════════════════════
//
// State reflects commercial temperature, not contractual history:
//   active_client    — open mandate OR closed mandate within 18 months
//   ex_client        — had a mandate, gone cold for 18+ months
//   warm_non_client  — no mandate history, max proximity > 0.6
//   cool_non_client  — no mandate history, max proximity 0.3 - 0.6
//   cold_non_client  — no mandate history, max proximity < 0.3

const ACTIVE_MONTHS = 18;
const WARM_THRESHOLD = 0.6;
const COOL_THRESHOLD = 0.3;

// opportunities.status values treated as open
const OPEN_STATUSES = [
  'briefing', 'research', 'sourcing', 'outreach', 'shortlist',
  'interviewing', 'presenting', 'placement', 'live',
];

/**
 * Compute relationship state for a single company.
 * @param {object} db — pool-like { query(sql, params) }
 * @param {string} companyId
 * @param {string} tenantId
 * @returns {Promise<string>} one of the five state values
 */
async function computeRelationshipState(db, companyId, tenantId) {
  // 1. Open mandate check — via opportunities linked through accounts
  const { rows: [openMandate] } = await db.query(`
    SELECT 1 FROM opportunities o
    JOIN engagements e ON e.id = o.project_id AND e.tenant_id = o.tenant_id
    JOIN accounts a ON a.id = e.client_id AND a.tenant_id = e.tenant_id
    WHERE (a.company_id = $1 OR LOWER(TRIM(a.name)) = LOWER(TRIM((SELECT name FROM companies WHERE id = $1))))
      AND o.tenant_id = $2
      AND o.status = ANY($3)
    LIMIT 1
  `, [companyId, tenantId, OPEN_STATUSES]).catch(() => ({ rows: [] }));

  if (openMandate) return 'active_client';

  // 2. Recent mandate (closed or invoiced) — check conversions
  const { rows: [recent] } = await db.query(`
    SELECT MAX(start_date) AS last_mandate
    FROM conversions conv
    LEFT JOIN accounts a ON a.id = conv.client_id
    WHERE conv.tenant_id = $2
      AND (a.company_id = $1 OR LOWER(TRIM(conv.client_name_raw)) = LOWER(TRIM((SELECT name FROM companies WHERE id = $1))))
      AND conv.placement_fee > 0
      AND conv.start_date IS NOT NULL
  `, [companyId, tenantId]).catch(() => ({ rows: [{}] }));

  if (recent?.last_mandate) {
    const monthsSince = (Date.now() - new Date(recent.last_mandate).getTime()) / (1000 * 60 * 60 * 24 * 30);
    if (monthsSince <= ACTIVE_MONTHS) return 'active_client';
    return 'ex_client';
  }

  // 3. No mandate history — classify by proximity
  const { rows: [prox] } = await db.query(`
    SELECT MAX(tp.relationship_strength) AS max_strength
    FROM team_proximity tp
    JOIN people p ON p.id = tp.person_id AND p.current_company_id = $1
    WHERE tp.tenant_id = $2
  `, [companyId, tenantId]).catch(() => ({ rows: [{}] }));

  const maxStrength = parseFloat(prox?.max_strength) || 0;
  if (maxStrength >= WARM_THRESHOLD) return 'warm_non_client';
  if (maxStrength >= COOL_THRESHOLD) return 'cool_non_client';
  return 'cold_non_client';
}

module.exports = { computeRelationshipState, ACTIVE_MONTHS, WARM_THRESHOLD, COOL_THRESHOLD, OPEN_STATUSES };
