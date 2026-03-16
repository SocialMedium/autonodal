#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL DISPATCH GENERATOR
// Processes unhandled signal events and generates intelligence briefs with:
//   1. Proximity mapping (who we know at/near the target company)
//   2. Approach angle (optimal engagement strategy per signal type)
//   3. Thought leadership content (blog post relevant to the signal)
//   4. Distribution plan (who to send it to, via which channel)
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-sonnet-4-20250514';
const MAX_SIGNALS_PER_RUN = parseInt(process.env.DISPATCH_BATCH_SIZE) || 20;
const MIN_CONFIDENCE = parseFloat(process.env.DISPATCH_MIN_CONFIDENCE) || 0.65;
const MAX_SIGNAL_AGE_HOURS = 72;

const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ═══════════════════════════════════════════════════════════════════════════════
// CLAUDE API
// ═══════════════════════════════════════════════════════════════════════════════

async function callClaude(systemPrompt, userMessage, maxTokens = 2048) {
  if (!ANTHROPIC_API_KEY) throw new Error('ANTHROPIC_API_KEY not set');

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: maxTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }]
    })
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Claude API ${response.status}: ${err.slice(0, 200)}`);
  }

  const data = await response.json();
  return data.content[0]?.text || '';
}

// ═══════════════════════════════════════════════════════════════════════════════
// APPROACH ANGLE MAPPING
// ═══════════════════════════════════════════════════════════════════════════════

const APPROACH_ANGLES = {
  capital_raising: {
    angle: 'Talent to deploy capital',
    rationale: "They've just raised and will be building fast. Our angle: help them deploy capital into talent before the market moves. Funding rounds create 3-6 month hiring surges — leadership, product, and go-to-market roles open up simultaneously.",
    themes: ['scaling leadership after funding', 'building executive teams in growth mode', 'talent strategy for Series B+ companies']
  },
  leadership_change: {
    angle: 'Backfill and team reconfiguration',
    rationale: "A senior departure creates both a backfill need and a team reconfiguration opportunity. New leaders reshape teams — this is when mandates emerge for fresh talent at multiple levels.",
    themes: ['leadership transitions and organisational design', 'building teams under new leadership', 'the first 100 days of a new executive']
  },
  geographic_expansion: {
    angle: 'Local market knowledge and leadership',
    rationale: "New market entry requires local market knowledge and leadership. We have both. Expansion into new geographies is one of the hardest talent challenges — cultural fit, regulatory knowledge, and network access are critical.",
    themes: ['building leadership teams in new markets', 'cross-border executive search', 'cultural intelligence in market expansion']
  },
  strategic_hiring: {
    angle: 'Pipeline acceleration',
    rationale: "Active hiring signal — they're building and need to move fast. We can accelerate their pipeline with pre-qualified candidates and market intelligence they can't get from job boards.",
    themes: ['executive hiring velocity', 'competitive talent markets', 'building high-performance teams']
  },
  ma_activity: {
    angle: 'Integration leadership',
    rationale: "M&A creates integration leadership needs, cultural bridging, and new functional heads. Post-merger, companies need leaders who can unify cultures and accelerate integration — a window that lasts 6-12 months.",
    themes: ['post-merger leadership integration', 'building culture after acquisition', 'the talent dimension of M&A']
  },
  product_launch: {
    angle: 'Specialist talent and leadership restructure',
    rationale: "New product line requires specialist talent and often signals a leadership restructure. Product launches create demand for go-to-market leaders, technical specialists, and sometimes entirely new business units.",
    themes: ['talent for product-led growth', 'building product leadership teams', 'scaling from launch to market']
  },
  restructuring: {
    angle: 'Talent market opportunity',
    rationale: "Restructuring releases experienced talent into the market and creates new leadership configurations. Companies restructuring need advisors who understand both the talent flowing out and the leadership gaps forming.",
    themes: ['organisational redesign and talent strategy', 'leadership in transition', 'restructuring as a growth catalyst']
  },
  layoffs: {
    angle: 'Displaced talent pipeline',
    rationale: "Layoffs release experienced professionals who are immediately available. We can connect displaced talent with companies actively hiring — and approach the restructuring company about their new leadership needs.",
    themes: ['talent redeployment after restructuring', 'building resilient leadership teams', 'the opportunity in market disruption']
  },
  partnership: {
    angle: 'Joint venture leadership',
    rationale: "Strategic partnerships create new leadership roles — joint venture heads, integration managers, and cross-functional leaders who can bridge two organisations.",
    themes: ['leadership for strategic partnerships', 'cross-organisational talent', 'building teams for collaborative ventures']
  }
};

const DEFAULT_ANGLE = {
  angle: 'Market intelligence and talent advisory',
  rationale: "A significant market signal presents an opportunity to position ourselves as trusted advisors with deep network access and talent intelligence.",
  themes: ['executive talent in dynamic markets', 'leadership for market inflection points', 'building teams for what comes next']
};

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1: PROXIMITY MAPPING
// ═══════════════════════════════════════════════════════════════════════════════

async function buildProximityMap(signal) {
  const companyName = signal.company_name;
  let companyId = signal.company_id;
  if (!companyName) return [];

  LOG('🔍', `  Proximity search: "${companyName}" (company_id: ${companyId || 'NULL'})`);

  // If no company_id, try to find matching company record
  if (!companyId) {
    try {
      const { rows } = await pool.query(
        `SELECT id FROM companies WHERE name ILIKE $1 OR name ILIKE $2 LIMIT 1`,
        [`%${companyName}%`, companyName]
      );
      if (rows.length > 0) {
        companyId = rows[0].id;
        LOG('🔗', `  Linked to company record: ${companyId}`);
      }
    } catch (e) { /* ignore */ }
  }

  // Build multiple search variants for fuzzy company matching
  // e.g. "Google LLC" -> search for "Google", "Alphabet Inc" -> also check aliases
  const nameVariants = [companyName];
  // Strip common suffixes for broader matching
  const stripped = companyName.replace(/\s+(Inc\.?|LLC|Ltd\.?|Pty\.?|Corp\.?|Corporation|Limited|Group|Holdings|International)$/i, '').trim();
  if (stripped !== companyName && stripped.length > 2) nameVariants.push(stripped);
  // Also try first word if multi-word (catches "Google Cloud" -> "Google")
  const firstWord = companyName.split(/\s+/)[0];
  if (firstWord.length > 3 && firstWord !== stripped) nameVariants.push(firstWord);

  LOG('🔍', `  Search variants: ${nameVariants.join(', ')}`);

  const connections = [];
  const seen = new Set();

  const addConnection = (person, type, strength, extra = {}) => {
    if (!person.id || seen.has(person.id)) return;
    seen.add(person.id);
    connections.push({
      person_id: person.id,
      name: person.full_name || person.name,
      title: person.current_title || person.title || '',
      company: person.current_company_name || companyName,
      relationship_type: type,
      strength,
      last_contact: person.last_interaction_at || person.interaction_at || person.last_contact || null,
      team_member: person.team_member || person.contacted_by || null,
      team_member_id: person.team_member_id || person.user_id || null,
      engagement_score: person.engagement_score || null,
      ...extra
    });
  };

  // Build ILIKE conditions for all name variants
  const ilikeConds = nameVariants.map((_, i) => `p.current_company_name ILIKE $${i + 1}`).join(' OR ');
  const ilikeParams = nameVariants.map(n => `%${n}%`);

  // a) Current employees
  try {
    const { rows } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.email,
             p.linkedin_url, p.seniority_level,
             ps.engagement_score, ps.last_interaction_at
      FROM people p
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE (${ilikeConds})
         OR p.current_company_id = $${nameVariants.length + 1}
      ORDER BY ps.engagement_score DESC NULLS LAST
      LIMIT 20
    `, [...ilikeParams, companyId]);

    LOG('👥', `  Current employees found: ${rows.length}`);
    for (const r of rows) {
      const hasInteraction = r.last_interaction_at != null;
      addConnection(r, 'current_employee', hasInteraction ? 'direct' : 'warm', {
        engagement_score: r.engagement_score
      });
    }
  } catch (e) { LOG('⚠️', `Current employees query failed: ${e.message}`); }

  // b) Past employees (career_history JSONB)
  try {
    const careerConds = nameVariants.map((_, i) => `elem->>'company' ILIKE $${i + 1}`).join(' OR ');
    const { rows } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name,
             elem->>'company' AS past_company,
             elem->>'title' AS past_title,
             elem->>'end_date' AS left_date
      FROM people p,
           jsonb_array_elements(p.career_history) elem
      WHERE p.career_history IS NOT NULL
        AND p.career_history != 'null'::jsonb
        AND jsonb_typeof(p.career_history) = 'array'
        AND (${careerConds})
      LIMIT 10
    `, ilikeParams);

    for (const r of rows) {
      addConnection(r, 'past_employee', 'warm', {
        past_company: r.past_company,
        past_title: r.past_title,
        left_date: r.left_date
      });
    }
    LOG('👤', `  Past employees found: ${rows.length}`);
  } catch (e) { LOG('⚠️', `Past employees query failed: ${e.message}`); }

  // c) Placed candidates
  try {
    const placeConds = nameVariants.map((_, i) => `cl.name ILIKE $${i + 1}`).join(' OR ');
    const { rows } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title,
             pl.role_title AS placed_role, pl.start_date AS placed_at,
             cl.name AS client_name,
             pl.placed_by_user_id AS user_id,
             u.name AS team_member
      FROM conversions pl
      JOIN people p ON p.id = pl.person_id
      JOIN accounts cl ON cl.id = pl.client_id
      LEFT JOIN users u ON u.id = pl.placed_by_user_id
      WHERE (${placeConds})
         OR (cl.company_id IS NOT NULL AND cl.company_id = $${nameVariants.length + 1})
      ORDER BY pl.start_date DESC NULLS LAST
      LIMIT 10
    `, [...ilikeParams, companyId]);

    for (const r of rows) {
      addConnection(r, 'placed', 'direct', {
        placed_role: r.placed_role,
        placed_at: r.placed_at,
        team_member: r.team_member
      });
    }
    LOG('📋', `  Placements found: ${rows.length}`);
  } catch (e) { LOG('⚠️', `Placements query failed: ${e.message}`); }

  // d) Recent interactions
  try {
    const ixConds = nameVariants.map((_, i) => `p.current_company_name ILIKE $${i + 1}`).join(' OR ');
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (p.id)
             p.id, p.full_name, p.current_title, p.current_company_name,
             i.interaction_type, i.interaction_at, i.summary,
             u.name AS team_member, i.user_id AS team_member_id
      FROM interactions i
      JOIN people p ON p.id = i.person_id
      LEFT JOIN users u ON u.id = i.user_id
      WHERE (${ixConds} OR p.current_company_id = $${nameVariants.length + 1})
        AND i.interaction_at > NOW() - INTERVAL '18 months'
      ORDER BY p.id, i.interaction_at DESC
      LIMIT 10
    `, [...ilikeParams, companyId]);

    for (const r of rows) {
      addConnection(r, 'interacted', 'direct', {
        last_interaction_type: r.interaction_type,
        last_contact: r.interaction_at,
        team_member: r.team_member,
        team_member_id: r.team_member_id
      });
    }
    LOG('💬', `  Recent interactions found: ${rows.length}`);
  } catch (e) { LOG('⚠️', `Interactions query failed: ${e.message}`); }

  // e) Team proximity
  try {
    const tpConds = nameVariants.map((_, i) => `p.current_company_name ILIKE $${i + 1}`).join(' OR ');
    const { rows } = await pool.query(`
      SELECT tp.person_id AS id, p.full_name, p.current_title, p.current_company_name,
             tp.relationship_strength, tp.relationship_type,
             tp.last_interaction_date AS last_contact,
             u.name AS team_member, tp.team_member_id
      FROM team_proximity tp
      JOIN people p ON p.id = tp.person_id
      LEFT JOIN users u ON u.id = tp.team_member_id
      WHERE (${tpConds} OR p.current_company_id = $${nameVariants.length + 1})
      ORDER BY tp.relationship_strength DESC
      LIMIT 10
    `, [...ilikeParams, companyId]);

    for (const r of rows) {
      const strength = r.relationship_strength > 0.7 ? 'direct' :
                       r.relationship_strength > 0.3 ? 'warm' : 'cold';
      addConnection(r, 'network_connection', strength, {
        relationship_strength: r.relationship_strength,
        team_member: r.team_member,
        team_member_id: r.team_member_id
      });
    }
    LOG('🤝', `  Team proximity found: ${rows.length}`);
  } catch (e) { LOG('⚠️', `Team proximity query failed: ${e.message}`); }

  return connections;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2: SCORE AND RANK CONNECTIONS
// ═══════════════════════════════════════════════════════════════════════════════

function scoreConnections(connections) {
  for (const c of connections) {
    let score = 0;

    // Base score by relationship type
    switch (c.relationship_type) {
      case 'current_employee':
        score = c.strength === 'direct' ? 90 : 70;
        break;
      case 'placed':
        score = 80;
        break;
      case 'past_employee':
        score = 60;
        break;
      case 'interacted':
        score = 75;
        break;
      case 'network_connection':
        score = c.strength === 'direct' ? 50 : c.strength === 'warm' ? 40 : 30;
        break;
      default:
        score = 30;
    }

    // Boost for recent contact
    if (c.last_contact) {
      const daysSince = (Date.now() - new Date(c.last_contact)) / (1000 * 60 * 60 * 24);
      if (daysSince < 30) score += 10;
      else if (daysSince < 90) score += 5;
      else if (daysSince > 365) score -= 10;
    }

    // Boost for engagement
    if (c.engagement_score) {
      score += Math.round(parseFloat(c.engagement_score) * 10);
    }

    // Boost for having a team member connection
    if (c.team_member) score += 5;

    c.score = Math.max(0, Math.min(100, score));
  }

  // Sort by score descending
  connections.sort((a, b) => b.score - a.score);

  // Return top 5
  return connections.slice(0, 5);
}

function selectBestEntryPoint(rankedConnections) {
  if (rankedConnections.length === 0) return null;

  const best = rankedConnections[0];
  let reason = '';
  switch (best.relationship_type) {
    case 'current_employee':
      reason = `Currently at the company${best.team_member ? `, known by ${best.team_member}` : ''}`;
      break;
    case 'placed':
      reason = `We placed them here${best.placed_role ? ` as ${best.placed_role}` : ''}`;
      break;
    case 'past_employee':
      reason = `Former employee with insider knowledge`;
      break;
    case 'interacted':
      reason = `Recent interaction${best.team_member ? ` with ${best.team_member}` : ''}`;
      break;
    default:
      reason = `Network connection${best.team_member ? ` via ${best.team_member}` : ''}`;
  }

  return {
    person_id: best.person_id,
    name: best.name,
    title: best.title,
    approach_via: best.team_member || 'Direct outreach',
    reason
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3: DETERMINE APPROACH ANGLE
// ═══════════════════════════════════════════════════════════════════════════════

function getApproachAngle(signalType) {
  return APPROACH_ANGLES[signalType] || DEFAULT_ANGLE;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 4: GENERATE BLOG POST
// ═══════════════════════════════════════════════════════════════════════════════

async function generateBlogPost(signal, company, approach) {
  const theme = approach.themes[Math.floor(Math.random() * approach.themes.length)];
  const sector = company?.sector || 'technology';
  const geography = company?.geography || 'global';
  const size = company?.employee_count_band || 'growth-stage';

  // Get related signals for trend data
  let trendContext = '';
  try {
    const { rows: related } = await pool.query(`
      SELECT signal_type, company_name, confidence_score, detected_at, evidence_summary
      FROM signal_events
      WHERE signal_type = $1 AND detected_at > NOW() - INTERVAL '30 days'
        AND company_name IS NOT NULL
      ORDER BY confidence_score DESC LIMIT 10
    `, [signal.signal_type]);
    if (related.length > 1) {
      trendContext = `\n\nTREND DATA (${related.length} similar signals in 30 days):\n` +
        related.map(r => `- ${r.company_name}: ${(r.evidence_summary || '').slice(0, 100)}`).join('\n');
    }
  } catch (e) { /* ignore */ }

  const systemPrompt = `You are a market intelligence analyst producing a data-driven signal brief for executive search consultants.

Output TWO formats in a single JSON response:

1. "linkedin_post" — A punchy LinkedIn post (120-180 words max). Data-first, insight-driven. Include 1-2 specific data points or stats. End with a question or observation that invites engagement. Use line breaks for readability. No hashtags.

2. "email_brief" — A direct email body (200-300 words) to send to a specific contact at or near the signalling company. Reference the signal event specifically. Include what the data shows about the trend. End with a soft ask ("Would be good to catch up on how this is landing for your team" style, not salesy).

3. "data_points" — Array of 3-5 hard data points extracted from the signal and trend context. Each: { "metric": "what", "value": "number or fact", "context": "why it matters" }

4. "trend_summary" — One sentence summary of the broader trend this signal is part of.

5. "title" — Short headline (max 10 words) for the dispatch card.

6. "keywords" — Array of 3-5 keywords.

Format: Return ONLY valid JSON. No markdown wrapping.
Tone: Direct, data-driven, no jargon. Australian English. Not salesy.`;

  const userPrompt = `Signal: ${signal.signal_type.replace(/_/g, ' ')} at ${signal.company_name || 'a company'}
Sector: ${sector} | Geography: ${geography} | Size: ${size}

Evidence: ${signal.evidence_summary || signal.signal_type.replace(/_/g, ' ')}

Approach angle: ${approach.rationale}
Theme: ${theme}
${trendContext}

Generate the signal brief JSON.`;

  const raw = await callClaude(systemPrompt, userPrompt, 2048);

  try {
    const jsonMatch = raw.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON found');
    const parsed = JSON.parse(jsonMatch[0]);
    return {
      theme,
      title: parsed.title || theme,
      body: JSON.stringify({
        linkedin_post: parsed.linkedin_post || '',
        email_brief: parsed.email_brief || '',
        data_points: parsed.data_points || [],
        trend_summary: parsed.trend_summary || ''
      }),
      keywords: parsed.keywords || []
    };
  } catch (e) {
    LOG('⚠️', `Brief JSON parse failed, using raw text`);
    return { theme, title: theme, body: raw, keywords: [] };
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 5: GENERATE DISTRIBUTION PLAN
// ═══════════════════════════════════════════════════════════════════════════════

function generateDistributionPlan(rankedConnections, signal, blogTitle) {
  const recipients = [];
  const signalRef = signal.signal_type.replace(/_/g, ' ');
  const companyRef = signal.company_name;

  for (const conn of rankedConnections.slice(0, 3)) {
    // Channel selection based on recency
    let channel = 'linkedin';
    if (conn.last_contact) {
      const daysSince = (Date.now() - new Date(conn.last_contact)) / (1000 * 60 * 60 * 24);
      if (daysSince < 180) channel = 'email';
    }

    // Personal note draft
    const firstName = (conn.name || '').split(' ')[0];
    let note = '';
    if (conn.relationship_type === 'placed') {
      note = `Hi ${firstName}, hope you're well since we placed you at ${companyRef}. Saw some movement there recently and wrote something on the theme that felt relevant — thought of you. [Link]. Would love to hear how things are going.`;
    } else if (conn.relationship_type === 'current_employee') {
      note = `Hi ${firstName}, saw ${companyRef} in the news recently. Wrote a piece on ${blogTitle.toLowerCase()} that felt relevant given where you are right now — thought of you when I finished it. [Link]. Would love to hear your take.`;
    } else if (conn.relationship_type === 'past_employee') {
      note = `Hi ${firstName}, given your time at ${companyRef}, thought you'd find this interesting — wrote something on ${blogTitle.toLowerCase()} that connects to what's happening there. [Link]. Would value your perspective.`;
    } else {
      note = `Hi ${firstName}, wrote a piece on ${blogTitle.toLowerCase()} — given your work in the space, thought you'd find it relevant. [Link]. Would love to hear your take.`;
    }

    recipients.push({
      person_id: conn.person_id,
      name: conn.name,
      title: conn.title,
      channel,
      personal_note: note
    });
  }

  return recipients;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN PIPELINE
// ═══════════════════════════════════════════════════════════════════════════════

async function generateDispatches() {
  LOG('🎯', '═══ Signal Dispatch Generator ═══');

  // Ensure table exists
  await pool.query(`
    CREATE TABLE IF NOT EXISTS signal_dispatches (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      signal_event_id UUID, company_id UUID, company_name TEXT,
      signal_type TEXT, signal_summary TEXT,
      proximity_map JSONB DEFAULT '[]'::jsonb,
      best_entry_point JSONB,
      opportunity_angle TEXT, approach_rationale TEXT,
      blog_theme TEXT, blog_title TEXT, blog_body TEXT, blog_keywords TEXT[],
      send_to JSONB DEFAULT '[]'::jsonb,
      status TEXT DEFAULT 'draft',
      generated_at TIMESTAMPTZ DEFAULT NOW(),
      reviewed_at TIMESTAMPTZ, reviewed_by UUID,
      sent_at TIMESTAMPTZ, created_by UUID,
      created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // Find unprocessed signal events — prioritise clients, then companies with contacts
  const { rows: signals } = await pool.query(`
    SELECT se.id, se.signal_type, se.company_name, se.company_id,
           se.confidence_score, se.evidence_summary, se.detected_at,
           se.signal_date, se.hiring_implications,
           c.name AS co_name, c.sector, c.geography, c.employee_count_band,
           c.is_client, c.domain,
           (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS contact_count,
           (SELECT COUNT(*) FROM conversions pl JOIN accounts cl ON cl.id = pl.client_id
            WHERE cl.company_id = c.id) AS placement_count
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    WHERE se.confidence_score >= $1
      AND se.detected_at > NOW() - INTERVAL '${MAX_SIGNAL_AGE_HOURS} hours'
      AND COALESCE(se.is_megacap, false) = false
      AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      AND NOT EXISTS (
        SELECT 1 FROM signal_dispatches sd WHERE sd.signal_event_id = se.id
      )
    ORDER BY
      CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
      CASE WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) > 0 THEN 0 ELSE 1 END,
      se.confidence_score DESC,
      se.detected_at DESC
    LIMIT $2
  `, [MIN_CONFIDENCE, MAX_SIGNALS_PER_RUN]);

  LOG('📡', `Found ${signals.length} unprocessed signals (confidence >= ${MIN_CONFIDENCE}, age <= ${MAX_SIGNAL_AGE_HOURS}h)`);

  if (signals.length === 0) {
    LOG('✅', 'No signals to process');
    return { processed: 0, dispatches: 0 };
  }

  let dispatched = 0;
  let skipped = 0;

  for (const signal of signals) {
    try {
      LOG('🔍', `Processing: ${signal.signal_type} at ${signal.company_name} (${(signal.confidence_score * 100).toFixed(0)}% confidence)`);

      // GATE: Must have a company name
      if (!signal.company_name || signal.company_name === 'Unknown') {
        LOG('⏭️', `  No company name — SKIPPING`);
        skipped++;
        continue;
      }

      // STEP 1: Build proximity map
      const rawConnections = await buildProximityMap(signal);
      LOG('📊', `  Found ${rawConnections.length} raw connections`);

      // STEP 2: Score and rank
      const ranked = scoreConnections(rawConnections);
      const bestEntry = selectBestEntryPoint(ranked);

      if (ranked.length === 0) {
        LOG('⏭️', `  No connections found — SKIPPING (dispatches require at least 1 contact)`);
        skipped++;
        continue;
      }
      LOG('🎯', `  Best entry: ${bestEntry.name} (${bestEntry.reason})`);

      // STEP 3: Approach angle
      const approach = getApproachAngle(signal.signal_type);
      LOG('💡', `  Angle: ${approach.angle}`);

      // STEP 4: Generate blog
      LOG('✍️', `  Generating thought leadership article...`);
      const blog = await generateBlogPost(signal, {
        sector: signal.sector,
        geography: signal.geography,
        employee_count_band: signal.employee_count_band
      }, approach);
      LOG('📝', `  Blog: "${blog.title}" (${blog.body.length} chars)`);

      // STEP 5: Distribution plan
      const sendTo = generateDistributionPlan(ranked, signal, blog.title);
      LOG('📮', `  Distribution: ${sendTo.length} recipients`);

      // STEP 6: Save dispatch
      await pool.query(`
        INSERT INTO signal_dispatches (
          signal_event_id, company_id, company_name, signal_type, signal_summary,
          proximity_map, best_entry_point,
          opportunity_angle, approach_rationale,
          blog_theme, blog_title, blog_body, blog_keywords,
          send_to, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'draft')
      `, [
        signal.id,
        signal.company_id,
        signal.company_name,
        signal.signal_type,
        signal.evidence_summary || `${signal.signal_type.replace(/_/g, ' ')} detected`,
        JSON.stringify(ranked),
        JSON.stringify(bestEntry),
        approach.angle,
        approach.rationale,
        blog.theme,
        blog.title,
        blog.body,
        blog.keywords,
        JSON.stringify(sendTo)
      ]);

      dispatched++;
      LOG('✅', `  Dispatch saved (${dispatched}/${signals.length})`);

      // Rate limit Claude API
      if (dispatched < signals.length) {
        await sleep(2000);
      }

    } catch (err) {
      LOG('❌', `  Failed: ${err.message}`);
      skipped++;
    }
  }

  LOG('🎯', `═══ Complete: ${dispatched} dispatches generated, ${skipped} skipped ═══`);
  return { processed: signals.length, dispatches: dispatched, skipped };
}

// ═══════════════════════════════════════════════════════════════════════════════
// RESCAN — Re-run proximity mapping for existing dispatches with empty maps
// ═══════════════════════════════════════════════════════════════════════════════

async function rescanProximity() {
  LOG('🔄', '═══ Rescan Proximity Maps ═══');

  const { rows: dispatches } = await pool.query(`
    SELECT sd.id, sd.company_name, sd.company_id, sd.signal_type,
           sd.blog_title, sd.opportunity_angle
    FROM signal_dispatches sd
    WHERE jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) = 0
       OR sd.proximity_map IS NULL
    ORDER BY sd.generated_at DESC
    LIMIT 50
  `);

  LOG('📋', `Found ${dispatches.length} dispatches with empty proximity maps`);

  let updated = 0;
  for (const d of dispatches) {
    try {
      LOG('🔍', `Rescanning: ${d.company_name}`);
      const rawConnections = await buildProximityMap(d);
      const ranked = scoreConnections(rawConnections);
      const bestEntry = selectBestEntryPoint(ranked);

      if (ranked.length > 0) {
        const sendTo = generateDistributionPlan(ranked, d, d.blog_title || 'our latest article');
        await pool.query(`
          UPDATE signal_dispatches
          SET proximity_map = $2, best_entry_point = $3, send_to = $4, updated_at = NOW()
          WHERE id = $1
        `, [d.id, JSON.stringify(ranked), JSON.stringify(bestEntry), JSON.stringify(sendTo)]);
        updated++;
        LOG('✅', `  Updated: ${ranked.length} connections, best entry: ${bestEntry?.name || 'none'}`);
      } else {
        LOG('⏭️', `  No connections found`);
      }
    } catch (e) {
      LOG('❌', `  Failed: ${e.message}`);
    }
  }

  LOG('🔄', `═══ Rescan complete: ${updated}/${dispatches.length} updated ═══`);
  return { total: dispatches.length, updated };
}

// ═══════════════════════════════════════════════════════════════════════════════
// RUN
// ═══════════════════════════════════════════════════════════════════════════════

if (require.main === module) {
  generateDispatches()
    .then(result => {
      console.log('\nResult:', JSON.stringify(result, null, 2));
      process.exit(0);
    })
    .catch(err => {
      console.error('Fatal error:', err);
      process.exit(1);
    });
}

module.exports = { generateDispatches, rescanProximity };
