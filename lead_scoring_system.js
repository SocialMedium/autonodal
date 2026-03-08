// ═══════════════════════════════════════════════════════════════════════════════
// LEAD SCORE CALCULATION (enhances existing confidence_score)
// ═══════════════════════════════════════════════════════════════════════════════

async function calculateLeadScore(signal) {
  const breakdown = {
    base_confidence: parseFloat(signal.confidence_score || 0) * 20, // Max 20 points
    client_proximity: 0,   // Max 40 points
    search_relevance: 0,   // Max 30 points  
    timing_urgency: 0,     // Max 10 points
    total: 0
  };
  
  // 1. CLIENT PROXIMITY (40 points max)
  if (signal.company_id) {
    try {
      // Check if it's an active client
      const clientCheck = await pool.query(`
        SELECT 
          c.id,
          c.relationship_tier,
          cf.total_placements,
          cf.total_invoiced
        FROM clients c
        LEFT JOIN client_financials cf ON c.id = cf.client_id
        WHERE c.id = $1
      `, [signal.company_id]);
      
      if (clientCheck.rows.length > 0) {
        const client = clientCheck.rows[0];
        breakdown.client_proximity = 40;
        breakdown.client_type = 'active_client';
        breakdown.client_tier = client.relationship_tier;
        breakdown.client_revenue = parseFloat(client.total_invoiced || 0);
      } else {
        // Check people in network
        const peopleCheck = await pool.query(`
          SELECT COUNT(*) as count
          FROM people p
          JOIN companies c ON c.id = $1
          WHERE p.current_company_name ILIKE c.name
        `, [signal.company_id]);
        
        const peopleCount = parseInt(peopleCheck.rows[0]?.count || 0);
        
        if (peopleCount >= 5) {
          breakdown.client_proximity = 30;
          breakdown.client_type = 'strong_network';
          breakdown.people_count = peopleCount;
        } else if (peopleCount > 0) {
          breakdown.client_proximity = 20;
          breakdown.client_type = 'network_contact';
          breakdown.people_count = peopleCount;
        } else {
          breakdown.client_proximity = 0;
          breakdown.client_type = 'no_relationship';
        }
      }
    } catch (error) {
      console.error('Error calculating client proximity:', error);
      breakdown.client_proximity = 0;
    }
  }
  
  // 2. SEARCH RELEVANCE (30 points max)
  try {
    // Check if signal matches active search criteria
    const searchMatches = await pool.query(`
      SELECT 
        s.id,
        s.title,
        s.seniority_level,
        s.industry
      FROM searches s
      WHERE s.status = 'active'
      AND (
        -- Signal type matches search need
        (s.hiring_stage = 'active' AND $1 IN ('strategic_hiring', 'leadership_change', 'geographic_expansion'))
        OR
        -- Company sector matches search industry
        (s.industry IS NOT NULL AND $2 ILIKE '%' || s.industry || '%')
      )
      LIMIT 5
    `, [signal.signal_type, signal.sector]);
    
    if (searchMatches.rows.length > 0) {
      breakdown.search_relevance = 30;
      breakdown.matching_searches = searchMatches.rows.map(s => ({ id: s.id, title: s.title }));
    } else {
      // Check ML interests alignment
      const interestCheck = await pool.query(`
        SELECT COUNT(*) as count
        FROM ml_interests mi
        WHERE mi.focus_area ILIKE '%' || $1 || '%'
        OR $1 ILIKE '%' || mi.focus_area || '%'
      `, [signal.sector || signal.signal_category]);
      
      if (interestCheck.rows[0]?.count > 0) {
        breakdown.search_relevance = 15;
        breakdown.relevance_type = 'ml_interest';
      } else {
        breakdown.search_relevance = 0;
        breakdown.relevance_type = 'no_match';
      }
    }
  } catch (error) {
    console.error('Error calculating search relevance:', error);
    breakdown.search_relevance = 0;
  }
  
  // 3. TIMING/URGENCY (10 points max)
  const detectedAt = new Date(signal.detected_at);
  const hoursSince = (Date.now() - detectedAt) / (1000 * 60 * 60);
  
  if (hoursSince < 24) {
    breakdown.timing_urgency = 10;
    breakdown.urgency_level = 'immediate';
  } else if (hoursSince < 168) { // 7 days
    breakdown.timing_urgency = 7;
    breakdown.urgency_level = 'this_week';
  } else if (hoursSince < 720) { // 30 days
    breakdown.timing_urgency = 5;
    breakdown.urgency_level = 'this_month';
  } else {
    breakdown.timing_urgency = 2;
    breakdown.urgency_level = 'older';
  }
  
  // TOTAL SCORE (max 100)
  breakdown.total = Math.round(
    breakdown.base_confidence +
    breakdown.client_proximity +
    breakdown.search_relevance +
    breakdown.timing_urgency
  );
  
  return breakdown;
}

// ═══════════════════════════════════════════════════════════════════════════════
// UPDATE SIGNAL BRIEF ENDPOINT TO INCLUDE LEAD SCORES
// ═══════════════════════════════════════════════════════════════════════════════

// REPLACE the existing /api/signals/brief endpoint (lines ~384-443) with this enhanced version:

app.get('/api/signals/brief', optionalAuth, async (req, res) => {
  try {
    const { 
      status = 'new', 
      type, 
      days = 365,
      limit = 50,
      page = 1 
    } = req.query;
    
    let whereClause = 'WHERE 1=1';
    const params = [];
    let paramIndex = 1;
    
    // Optional date filter
    if (days) {
      whereClause += ` AND detected_at > NOW() - $${paramIndex}::interval`;
      params.push(`${days} days`);
      paramIndex++;
    }
    
    if (status && status !== 'all') {
      whereClause += ` AND triage_status = $${paramIndex}`;
      params.push(status);
      paramIndex++;
    }
    
    if (type) {
      whereClause += ` AND signal_type = $${paramIndex}`;
      params.push(type);
      paramIndex++;
    }
    
    const offset = (parseInt(page) - 1) * parseInt(limit);
    
    const signals = await db.queryAll(`
      SELECT 
        s.*,
        COALESCE(c.name, s.company_name) as company_name,
        c.sector,
        c.geography
      FROM signal_events s
      LEFT JOIN companies c ON s.company_id = c.id
      ${whereClause}
      ORDER BY s.confidence_score DESC, s.detected_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `, [...params, parseInt(limit), offset]);
    
    // Calculate lead scores for all signals
    const scoredSignals = await Promise.all(
      signals.map(async (signal) => {
        const leadScore = await calculateLeadScore(signal);
        return {
          ...signal,
          lead_score: leadScore.total,
          lead_score_breakdown: leadScore
        };
      })
    );
    
    // Re-sort by lead_score (highest first)
    scoredSignals.sort((a, b) => b.lead_score - a.lead_score);
    
    // Get total count
    const countResult = await db.queryOne(`
      SELECT COUNT(*) FROM signal_events ${whereClause}
    `, params);
    
    res.json({
      signals: scoredSignals,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: parseInt(countResult.count),
        totalPages: Math.ceil(countResult.count / parseInt(limit))
      }
    });
  } catch (error) {
    console.error('Signals error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// USAGE IN FRONTEND
// ═══════════════════════════════════════════════════════════════════════════════

/*
Each signal will now have:

{
  ...existing fields...,
  lead_score: 85,  // 0-100 score showing lead quality
  lead_score_breakdown: {
    base_confidence: 18,        // From original signal detection (max 20)
    client_proximity: 40,       // Active client (max 40)
    search_relevance: 15,       // Matches ML interest (max 30)
    timing_urgency: 10,         // Detected <24h (max 10)
    total: 83,
    
    // Additional context:
    client_type: 'active_client',
    client_tier: 'platinum',
    client_revenue: 539633.60,
    matching_searches: [{id: '...', title: 'VP Engineering'}],
    urgency_level: 'immediate'
  }
}

Display in UI:
- Use lead_score for sorting (highest = best leads)
- Show lead_score as primary metric (not confidence_score)
- Add visual indicators:
  - 80-100: 🔥 Hot Lead (green)
  - 60-79: ⭐ Good Lead (blue)
  - 40-59: 📊 Qualified (yellow)
  - 0-39: 📝 Research (gray)
  
- Show breakdown on hover/click:
  "85 Lead Score
   • Active Client (40pts) - Platinum tier
   • Base Signal (18pts) - 90% confidence
   • ML Interest Match (15pts)
   • Immediate (10pts) - detected 3h ago"
*/