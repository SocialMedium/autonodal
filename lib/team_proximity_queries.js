/**
 * TEAM PROXIMITY & FINANCIAL INTELLIGENCE QUERIES
 */

const { Pool } = require('pg');

// ============================================================================
// TEAM PROXIMITY QUERIES
// ============================================================================

async function whoKnowsPerson(pool, personId) {
  const result = await pool.query(`
    SELECT 
      tp.id as proximity_id,
      u.id as consultant_id,
      u.full_name as consultant_name,
      u.email as consultant_email,
      tp.relationship_type,
      tp.relationship_strength,
      tp.warmth_score,
      tp.recency_score,
      tp.connected_date,
      tp.last_interaction_date,
      tp.interaction_count,
      tp.notes,
      tp.metadata
    FROM team_proximity tp
    JOIN users u ON tp.team_member_id = u.id
    WHERE tp.person_id = $1
    ORDER BY tp.warmth_score DESC, tp.relationship_strength DESC, tp.last_interaction_date DESC
  `, [personId]);
  
  return result.rows;
}

async function findWarmIntroPath(pool, personId, maxDegrees = 4, limit = 5) {
  const result = await pool.query(`
    WITH RECURSIVE intro_path AS (
      SELECT 
        tp.team_member_id as start_member,
        tp.person_id as current_person,
        1 as degree,
        ARRAY[tp.team_member_id::text] as path_ids,
        ARRAY[u.full_name] as path_names,
        ARRAY[tp.relationship_type] as path_types,
        tp.relationship_strength as path_strength,
        tp.relationship_strength as min_strength
      FROM team_proximity tp
      JOIN users u ON tp.team_member_id = u.id
      WHERE tp.relationship_type IN (
        'email_frequent', 'email_moderate',
        'past_placement', 'shared_project'
      )
      
      UNION ALL
      
      SELECT
        ip.start_member,
        tp2.person_id,
        ip.degree + 1,
        ip.path_ids || tp2.team_member_id::text,
        ip.path_names || u2.full_name,
        ip.path_types || tp2.relationship_type,
        ip.path_strength * tp2.relationship_strength,
        LEAST(ip.min_strength, tp2.relationship_strength)
      FROM intro_path ip
      JOIN team_proximity tp2 ON ip.current_person::uuid = tp2.team_member_id
      JOIN users u2 ON tp2.team_member_id = u2.id
      WHERE ip.degree < $2
        AND NOT (tp2.person_id::text = ANY(ip.path_ids))
        AND tp2.relationship_type IN (
          'email_frequent', 'email_moderate',
          'past_placement'
        )
    )
    SELECT 
      u.id as team_member_id,
      u.full_name as team_member_name,
      u.email as team_member_email,
      ip.degree,
      ROUND(ip.path_strength::numeric, 3) as path_strength,
      ROUND(ip.min_strength::numeric, 3) as weakest_link,
      ip.path_names,
      ip.path_types,
      CASE 
        WHEN ip.degree = 1 THEN 
          'Direct connection via ' || ip.path_types[1]
        WHEN ip.degree = 2 THEN 
          'Can intro via ' || ip.path_names[2]
        ELSE 
          'Multi-hop intro through ' || array_to_string(ip.path_names[2:], ' → ')
      END as intro_suggestion
    FROM intro_path ip
    JOIN users u ON ip.start_member = u.id
    WHERE ip.current_person = $1
    ORDER BY ip.degree ASC, ip.path_strength DESC, ip.min_strength DESC
    LIMIT $3
  `, [personId, maxDegrees, limit]);
  
  return result.rows;
}

async function getBestConsultantContact(pool, personId) {
  const result = await pool.query(`
    SELECT 
      u.id as consultant_id,
      u.full_name as consultant_name,
      u.email as consultant_email,
      tp.relationship_type,
      tp.relationship_strength,
      tp.warmth_score,
      tp.last_interaction_date,
      tp.interaction_count,
      CASE 
        WHEN tp.relationship_type = 'past_placement' THEN 'Placed this person previously'
        WHEN tp.relationship_type = 'email_frequent' THEN 'Frequent email contact'
        WHEN tp.relationship_type = 'shared_project' THEN 'Worked together on project'
        ELSE 'Has relationship'
      END as contact_reason
    FROM team_proximity tp
    JOIN users u ON tp.team_member_id = u.id
    WHERE tp.person_id = $1
    ORDER BY tp.warmth_score DESC, tp.relationship_strength DESC
    LIMIT 1
  `, [personId]);
  
  return result.rows[0] || null;
}

async function getConsultantNetworkStats(pool, userId) {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total_connections,
      SUM(CASE WHEN relationship_strength >= 0.8 THEN 1 ELSE 0 END) as strong_connections,
      SUM(CASE WHEN warmth_score >= 0.7 THEN 1 ELSE 0 END) as warm_connections,
      SUM(CASE WHEN relationship_type = 'past_placement' THEN 1 ELSE 0 END) as past_placements,
      SUM(CASE WHEN relationship_type LIKE 'email_%' THEN 1 ELSE 0 END) as email_connections,
      ROUND(AVG(relationship_strength)::numeric, 2) as avg_strength,
      ROUND(AVG(warmth_score)::numeric, 2) as avg_warmth
    FROM team_proximity
    WHERE team_member_id = $1
  `, [userId]);
  
  return result.rows[0];
}

// ============================================================================
// PLACEMENT & FINANCIAL QUERIES
// ============================================================================

async function getClientPlacements(pool, clientId) {
  const result = await pool.query(`
    SELECT 
      pl.*,
      p.full_name as candidate_name,
      p.email as candidate_email,
      u.name as placed_by_name,
      u.email as placed_by_email
    FROM placements pl
    LEFT JOIN people p ON pl.person_id = p.id
    JOIN users u ON pl.placed_by_user_id = u.id
    WHERE pl.client_id = $1
    ORDER BY pl.start_date DESC
  `, [clientId]);
  
  return result.rows;
}

async function getConsultantPlacementHistory(pool, userId, limitMonths = null) {
  const whereClause = limitMonths 
    ? `AND pl.start_date > NOW() - INTERVAL '${limitMonths} months'`
    : '';
  
  const result = await pool.query(`
    SELECT 
      pl.id,
      pl.start_date,
      pl.invoice_date,
      p.full_name as candidate_name,
      c.name as client_name,
      pl.role_title,
      pl.role_level,
      pl.placement_fee,
      pl.payment_status,
      pl.still_employed,
      pl.client_satisfaction_score,
      EXTRACT(EPOCH FROM (NOW() - pl.start_date)) / 86400 / 30 as months_ago
    FROM placements pl
    JOIN people p ON pl.person_id = p.id
    JOIN clients c ON pl.client_id = c.id
    WHERE pl.placed_by_user_id = $1 ${whereClause}
    ORDER BY pl.start_date DESC
  `, [userId]);
  
  return result.rows;
}

async function getConsultantRevenueSummary(pool, userId, periodMonths = 12) {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total_placements,
      SUM(pl.placement_fee) as total_revenue,
      SUM(CASE WHEN pl.payment_status = 'paid' THEN pl.placement_fee ELSE 0 END) as paid_revenue,
      SUM(CASE WHEN pl.payment_status IN ('pending', 'overdue') THEN pl.placement_fee ELSE 0 END) as outstanding_revenue,
      ROUND(AVG(pl.placement_fee)::numeric, 2) as avg_fee,
      MAX(pl.placement_fee) as highest_fee,
      MIN(pl.placement_fee) as lowest_fee,
      COUNT(DISTINCT pl.client_id) as unique_clients,
      SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END) as active_placements,
      ROUND(100.0 * SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as retention_rate,
      ROUND(AVG(pl.client_satisfaction_score)::numeric, 2) as avg_satisfaction
    FROM placements pl
    WHERE pl.placed_by_user_id = $1
      AND pl.start_date > NOW() - INTERVAL '${periodMonths} months'
  `, [userId]);
  
  return result.rows[0];
}

async function getConsultantLeaderboard(pool, periodMonths = 12, limit = 10) {
  const result = await pool.query(`
    SELECT 
      u.id as consultant_id,
      u.full_name as consultant_name,
      COUNT(pl.id) as total_placements,
      SUM(pl.placement_fee) as total_revenue,
      ROUND(AVG(pl.placement_fee)::numeric, 2) as avg_fee,
      MAX(pl.placement_fee) as highest_fee,
      SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END) as active_placements,
      ROUND(100.0 * SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END) / NULLIF(COUNT(pl.id), 0), 2) as retention_rate,
      MAX(pl.start_date) as most_recent_placement
    FROM users u
    LEFT JOIN placements pl ON u.id = pl.placed_by_user_id
      AND pl.start_date > NOW() - INTERVAL '${periodMonths} months'
      AND pl.payment_status = 'paid'
    WHERE u.role IN ('admin', 'consultant')
    GROUP BY u.id, u.full_name
    HAVING COUNT(pl.id) > 0
    ORDER BY total_revenue DESC
    LIMIT $1
  `, [limit]);
  
  return result.rows;
}

async function getClientFinancialHealth(pool, clientId) {
  const result = await pool.query(`
    SELECT 
      cf.*,
      c.name as client_name,
      c.type as client_type,
      CASE 
        WHEN cf.last_placement_date > NOW() - INTERVAL '6 months' THEN 'active'
        WHEN cf.last_placement_date > NOW() - INTERVAL '12 months' THEN 'warm'
        WHEN cf.last_placement_date > NOW() - INTERVAL '24 months' THEN 'dormant'
        ELSE 'inactive'
      END as computed_status,
      CASE 
        WHEN cf.payment_reliability > 0.9 AND cf.total_placements >= 3 THEN 'platinum'
        WHEN cf.payment_reliability > 0.8 AND cf.total_placements >= 2 THEN 'gold'
        WHEN cf.payment_reliability > 0.7 THEN 'silver'
        WHEN cf.payment_reliability > 0.5 THEN 'bronze'
        ELSE 'at_risk'
      END as client_tier
    FROM client_financials cf
    JOIN clients c ON cf.client_id = c.id
    WHERE cf.client_id = $1
  `, [clientId]);
  
  return result.rows[0] || null;
}

async function getTopClients(pool, limit = 20) {
  const result = await pool.query(`
    SELECT 
      c.id as client_id,
      c.name as client_name,
      cf.total_placements,
      cf.total_paid as total_revenue,
      cf.average_placement_fee as avg_fee,
      cf.last_placement_date,
      cf.payment_reliability,
      cf.average_client_satisfaction,
      CASE 
        WHEN cf.payment_reliability > 0.9 AND cf.total_placements >= 3 THEN 'platinum'
        WHEN cf.payment_reliability > 0.8 AND cf.total_placements >= 2 THEN 'gold'
        WHEN cf.payment_reliability > 0.7 THEN 'silver'
        ELSE 'bronze'
      END as tier
    FROM clients c
    JOIN client_financials cf ON c.id = cf.client_id
    WHERE cf.total_paid > 0
    ORDER BY cf.total_paid DESC
    LIMIT $1
  `, [limit]);
  
  return result.rows;
}

async function getPersonIntelligenceProfile(pool, personId) {
  const personResult = await pool.query(`
    SELECT * FROM people WHERE id = $1
  `, [personId]);
  
  if (personResult.rows.length === 0) {
    return null;
  }
  
  const person = personResult.rows[0];
  const proximity = await whoKnowsPerson(pool, personId);
  const introPaths = await findWarmIntroPath(pool, personId, 3, 3);
  const bestContact = await getBestConsultantContact(pool, personId);
  
  const placementHistory = await pool.query(`
    SELECT 
      pl.*,
      c.name as client_name,
      u.full_name as placed_by_name
    FROM placements pl
    JOIN clients c ON pl.client_id = c.id
    JOIN users u ON pl.placed_by_user_id = u.id
    WHERE pl.person_id = $1
    ORDER BY pl.start_date DESC
  `, [personId]);
  
  return {
    person,
    team_relationships: proximity,
    warm_intro_paths: introPaths,
    best_contact: bestContact,
    placement_history: placementHistory.rows
  };
}

module.exports = {
  whoKnowsPerson,
  findWarmIntroPath,
  getBestConsultantContact,
  getConsultantNetworkStats,
  getClientPlacements,
  getConsultantPlacementHistory,
  getConsultantRevenueSummary,
  getConsultantLeaderboard,
  getClientFinancialHealth,
  getTopClients,
  getPersonIntelligenceProfile
};