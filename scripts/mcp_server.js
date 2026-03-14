#!/usr/bin/env node
/**
 * MitchelLake Signal Intelligence — MCP Server
 *
 * Exposes the full Signal Intelligence Platform to Claude via MCP.
 * Run with stdio transport for Claude Desktop / Claude.ai integration.
 *
 * Usage:
 *   node scripts/mcp_server.js
 *
 * Claude Desktop config (~/.config/claude/claude_desktop_config.json):
 *   {
 *     "mcpServers": {
 *       "mitchellake": {
 *         "command": "node",
 *         "args": ["/path/to/mitchellake-signals/scripts/mcp_server.js"],
 *         "env": { "DATABASE_URL": "...", "QDRANT_URL": "...", "QDRANT_API_KEY": "...", "OPENAI_API_KEY": "..." }
 *       }
 *     }
 *   }
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { z } = require('zod');
const { Pool } = require('pg');
const { QdrantClient } = require('@qdrant/js-client-rest');
const OpenAI = require('openai');

// ─────────────────────────────────────────────────────────────────────────────
// CLIENTS
// ─────────────────────────────────────────────────────────────────────────────

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// Pre-warm the pool at startup
pool.query('SELECT 1').then(() => process.stderr.write('MCP DB pool ready\n')).catch(e => process.stderr.write('MCP DB pool error: ' + e.message + '\n'));

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

async function dbQuery(sql, params = []) {
  const client = await pool.connect();
  try {
    return await client.query(sql, params);
  } finally {
    client.release();
  }
}

async function embedText(text) {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text.slice(0, 8000)
  });
  return response.data[0].embedding;
}

async function vectorSearch(collection, vector, limit = 20, filter = null) {
  const params = { vector, limit, with_payload: true };
  if (filter) params.filter = filter;
  const result = await qdrant.search(collection, params);
  return result;
}

function formatScore(score) {
  if (score === null || score === undefined) return 'n/a';
  return Math.round(score * 100) + '%';
}

function formatPerson(p, scores = null) {
  const lines = [
    `**${p.full_name}** (ID: ${p.id})`,
    `${p.current_title || 'Unknown title'} @ ${p.current_company_name || 'Unknown company'}`,
    p.location ? `📍 ${p.location}` : null,
    p.email ? `✉️  ${p.email}` : null,
    p.linkedin_url ? `🔗 ${p.linkedin_url}` : null,
    scores ? `Scores — Engagement: ${formatScore(scores.engagement_score)} | Receptivity: ${formatScore(scores.receptivity_score)} | Timing: ${formatScore(scores.timing_score)} | Flight Risk: ${formatScore(scores.flight_risk_score)}` : null
  ];
  return lines.filter(Boolean).join('\n');
}

function okText(text) {
  return { content: [{ type: 'text', text }] };
}

// ─────────────────────────────────────────────────────────────────────────────
// SERVER
// ─────────────────────────────────────────────────────────────────────────────

function createMcpServer() {
const server = new McpServer({
  name: 'mitchellake-mcp-server',
  version: '1.0.0'
});

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_search_people
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_search_people',
  {
    title: 'Search People',
    description: `Search the MitchelLake network of 28,500+ people using semantic vector search or keyword filters.
    
Use this to find candidates by role, expertise, company, location, or any combination.
Also supports finding people similar to a description, e.g. "CFO with fintech background in Singapore".

Returns name, title, company, location, email, scores (engagement/receptivity/timing/flight risk).

Args:
  - query (string): Natural language search, name, title, or keywords
  - semantic (boolean): Use vector similarity search (default: true). Set false for exact keyword matching.
  - company (string, optional): Filter by current company name
  - location (string, optional): Filter by city or country
  - seniority (string, optional): One of: c_suite, vp, director, manager, senior_ic, mid_level, junior
  - limit (number): Max results (default: 20, max: 50)

Examples:
  - "Head of Product fintech Sydney" → finds product leaders in fintech in Sydney
  - "CFO raised series B" → finds CFOs at companies that recently raised
  - "candidates for a CPO role" → vectors match against product leadership profiles`,
    inputSchema: z.object({
      query: z.string().min(2).describe('Search query — name, title, expertise, or natural language description'),
      semantic: z.boolean().default(true).describe('Use vector similarity (true) or keyword search (false)'),
      company: z.string().optional().describe('Filter by current company name'),
      location: z.string().optional().describe('Filter by city or country'),
      seniority: z.enum(['c_suite', 'vp', 'director', 'manager', 'senior_ic', 'mid_level', 'junior']).optional(),
      limit: z.number().int().min(1).max(50).default(20)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ query, semantic, company, location, seniority, limit }) => {
    try {
      let people = [];

      if (semantic) {
        // Vector search in Qdrant
        const vector = await embedText(query);
        const hits = await vectorSearch('people', vector, limit);
        const ids = hits.map(h => h.id).filter(Boolean);

        if (ids.length > 0) {
          const conditions = ['p.id = ANY($1)'];
          const params = [ids];
          let pi = 2;
          if (company) { conditions.push(`p.current_company_name ILIKE $${pi++}`); params.push(`%${company}%`); }
          if (location) { conditions.push(`(p.location ILIKE $${pi++} OR p.city ILIKE $${pi - 1} OR p.country ILIKE $${pi - 1})`); params.push(`%${location}%`); }
          if (seniority) { conditions.push(`p.seniority_level = $${pi++}`); params.push(seniority); }

          const result = await dbQuery(`
            SELECT p.*, ps.engagement_score, ps.receptivity_score, ps.timing_score, ps.flight_risk_score
            FROM people p
            LEFT JOIN person_scores ps ON p.id = ps.person_id
            WHERE ${conditions.join(' AND ')}
            LIMIT $${pi}
          `, [...params, limit]);
          people = result.rows;
        }
      } else {
        // Keyword search
        const conditions = [`(p.full_name ILIKE $1 OR p.current_title ILIKE $1 OR p.current_company_name ILIKE $1 OR p.headline ILIKE $1)`];
        const params = [`%${query}%`];
        let pi = 2;
        if (company) { conditions.push(`p.current_company_name ILIKE $${pi++}`); params.push(`%${company}%`); }
        if (location) { conditions.push(`(p.location ILIKE $${pi++} OR p.city ILIKE $${pi - 1} OR p.country ILIKE $${pi - 1})`); params.push(`%${location}%`); }
        if (seniority) { conditions.push(`p.seniority_level = $${pi++}`); params.push(seniority); }

        const result = await dbQuery(`
          SELECT p.*, ps.engagement_score, ps.receptivity_score, ps.timing_score, ps.flight_risk_score
          FROM people p
          LEFT JOIN person_scores ps ON p.id = ps.person_id
          WHERE ${conditions.join(' AND ')}
          ORDER BY p.full_name
          LIMIT $${pi}
        `, [...params, limit]);
        people = result.rows;
      }

      if (people.length === 0) return okText(`No people found matching "${query}"`);

      const lines = [`**${people.length} people found for "${query}"**\n`];
      for (const p of people) {
        lines.push(formatPerson(p, p.engagement_score !== undefined ? p : null));
        lines.push('');
      }
      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error searching people: ${(err.message || err.code || String(err))}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_person_dossier
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_person_dossier',
  {
    title: 'Get Person Dossier',
    description: `Retrieve the full 360° intelligence dossier for a person by their ID or name.

Returns: full profile, career history, scores (engagement/receptivity/timing/flight risk),
recent signals, interaction history, team proximity (who knows them), and active search matches.

Use ml_search_people first to find the person's ID, then call this for full detail.

Args:
  - person_id (string, optional): UUID of the person
  - name (string, optional): Full or partial name to look up (used if no person_id)
  - include_interactions (boolean): Include interaction history (default: true)
  - include_signals (boolean): Include signal history (default: true)
  - include_proximity (boolean): Include team proximity / who knows them (default: true)`,
    inputSchema: z.object({
      person_id: z.string().uuid().optional().describe('UUID of the person'),
      name: z.string().optional().describe('Name to look up if no ID'),
      include_interactions: z.boolean().default(true),
      include_signals: z.boolean().default(true),
      include_proximity: z.boolean().default(true)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ person_id, name, include_interactions, include_signals, include_proximity }) => {
    try {
      let personRow;
      if (person_id) {
        const r = await dbQuery('SELECT * FROM people WHERE id = $1', [person_id]);
        personRow = r.rows[0];
      } else if (name) {
        const r = await dbQuery(`SELECT * FROM people WHERE full_name ILIKE $1 ORDER BY full_name LIMIT 1`, [`%${name}%`]);
        personRow = r.rows[0];
      }

      if (!personRow) return okText(`Person not found: ${person_id || name}`);

      const sections = [];

      // Core profile
      sections.push(`# ${personRow.full_name}`);
      sections.push(`**${personRow.current_title || 'Title unknown'}** @ **${personRow.current_company_name || 'Company unknown'}**`);
      if (personRow.location) sections.push(`📍 ${personRow.location}`);
      if (personRow.email) sections.push(`✉️  ${personRow.email}`);
      if (personRow.phone) sections.push(`📞 ${personRow.phone}`);
      if (personRow.linkedin_url) sections.push(`🔗 ${personRow.linkedin_url}`);
      if (personRow.bio) sections.push(`\n${personRow.bio}`);

      // Scores
      const scoresR = await dbQuery('SELECT * FROM person_scores WHERE person_id = $1', [personRow.id]);
      if (scoresR.rows[0]) {
        const s = scoresR.rows[0];
        sections.push(`\n## Scores`);
        sections.push(`- Engagement:  ${formatScore(s.engagement_score)}`);
        sections.push(`- Activity:    ${formatScore(s.activity_score)}`);
        sections.push(`- Receptivity: ${formatScore(s.receptivity_score)}`);
        sections.push(`- Timing:      ${formatScore(s.timing_score)}`);
        sections.push(`- Flight Risk: ${formatScore(s.flight_risk_score)}`);
        if (s.tenure_months) sections.push(`- Tenure:      ${s.tenure_months} months in current role`);
      }

      // Career history
      if (personRow.career_history) {
        try {
          const history = typeof personRow.career_history === 'string'
            ? JSON.parse(personRow.career_history) : personRow.career_history;
          if (history.length > 0) {
            sections.push(`\n## Career History`);
            for (const role of history.slice(0, 8)) {
              const period = [role.start_date?.slice(0, 7), role.end_date ? role.end_date.slice(0, 7) : 'present'].filter(Boolean).join(' → ');
              sections.push(`- ${role.title || '?'} @ ${role.company || '?'} (${period})`);
            }
          }
        } catch (e) {}
      }

      // Signals
      if (include_signals) {
        const sigsR = await dbQuery(`
          SELECT signal_type, detected_at, title, description, source
          FROM person_signals
          WHERE person_id = $1
          ORDER BY detected_at DESC LIMIT 10
        `, [personRow.id]);
        if (sigsR.rows.length > 0) {
          sections.push(`\n## Recent Signals`);
          for (const sig of sigsR.rows) {
            const date = sig.detected_at ? new Date(sig.detected_at).toLocaleDateString() : '?';
            sections.push(`- **${sig.signal_type}** (${date}): ${sig.title || sig.description || ''}`);
          }
        }
      }

      // Interactions
      if (include_interactions) {
        const intR = await dbQuery(`
          SELECT interaction_type, direction, subject, summary, interaction_at, u.full_name as user_name
          FROM interactions i
          LEFT JOIN users u ON i.user_id = u.id
          WHERE i.person_id = $1
          ORDER BY interaction_at DESC LIMIT 10
        `, [personRow.id]);
        if (intR.rows.length > 0) {
          sections.push(`\n## Interaction History`);
          for (const int of intR.rows) {
            const date = int.interaction_at ? new Date(int.interaction_at).toLocaleDateString() : '?';
            const dir = int.direction === 'inbound' ? '←' : '→';
            sections.push(`- ${dir} **${int.interaction_type}** (${date}) ${int.user_name ? 'by ' + int.user_name : ''}: ${int.subject || int.summary || ''}`);
          }
        }
      }

      // Team proximity
      if (include_proximity) {
        const proxR = await dbQuery(`
          SELECT tp.relationship_type, tp.relationship_strength, tp.connected_date, u.full_name as team_member
          FROM team_proximity tp
          LEFT JOIN users u ON tp.team_member_id = u.id
          WHERE tp.person_id = $1
          ORDER BY tp.relationship_strength DESC LIMIT 5
        `, [personRow.id]);
        if (proxR.rows.length > 0) {
          sections.push(`\n## Team Proximity (Who Knows Them)`);
          for (const p of proxR.rows) {
            const strength = formatScore(p.relationship_strength);
            sections.push(`- **${p.team_member || '?'}** — ${p.relationship_type || 'connection'} (${strength})`);
          }
        }
      }

      // Active search matches
      const matchR = await dbQuery(`
        SELECT s.title, sc.match_score, sc.stage
        FROM search_candidates sc
        JOIN searches s ON sc.search_id = s.id
        WHERE sc.person_id = $1
        ORDER BY sc.match_score DESC NULLS LAST LIMIT 5
      `, [personRow.id]);
      if (matchR.rows.length > 0) {
        sections.push(`\n## Active Search Matches`);
        for (const m of matchR.rows) {
          sections.push(`- ${m.title} — Stage: ${m.stage || 'identified'} (Match: ${formatScore(m.match_score)})`);
        }
      }

      return okText(sections.join('\n'));
    } catch (err) {
      return okText(`Error fetching dossier: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_search_companies
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_search_companies',
  {
    title: 'Search Companies',
    description: `Search for companies in the MitchelLake intelligence database.

Returns company profiles including sector, size, location, recent signals, and whether MitchelLake has worked with them.

Args:
  - query (string): Company name or description
  - is_client (boolean, optional): Filter to only companies MitchelLake has worked with
  - sector (string, optional): Filter by industry/sector
  - limit (number): Max results (default: 15)`,
    inputSchema: z.object({
      query: z.string().min(2),
      is_client: z.boolean().optional().describe('Only show companies MitchelLake has worked with'),
      sector: z.string().optional(),
      limit: z.number().int().min(1).max(50).default(15)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ query, is_client, sector, limit }) => {
    try {
      const conditions = [`c.name ILIKE $1`];
      const params = [`%${query}%`];
      let pi = 2;

      if (is_client) {
        conditions.push(`EXISTS (SELECT 1 FROM clients cl WHERE cl.company_id = c.id OR cl.name ILIKE c.name)`);
      }
      if (sector) {
        conditions.push(`c.industry ILIKE $${pi++}`);
        params.push(`%${sector}%`);
      }

      const result = await dbQuery(`
        SELECT c.*,
          (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) as employee_count,
          (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '90 days') as recent_signals
        FROM companies c
        WHERE ${conditions.join(' AND ')}
        ORDER BY c.name
        LIMIT $${pi}
      `, [...params, limit]);

      if (result.rows.length === 0) return okText(`No companies found matching "${query}"`);

      const lines = [`**${result.rows.length} companies found**\n`];
      for (const co of result.rows) {
        lines.push(`**${co.name}** (ID: ${co.id})`);
        if (co.industry) lines.push(`  Industry: ${co.industry}`);
        if (co.location) lines.push(`  Location: ${co.location}`);
        if (co.employee_count > 0) lines.push(`  People tracked: ${co.employee_count}`);
        if (co.recent_signals > 0) lines.push(`  Recent signals (90d): ${co.recent_signals}`);
        lines.push('');
      }

      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error searching companies: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_company_intel
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_company_intel',
  {
    title: 'Get Company Intelligence',
    description: `Get full intelligence report for a company: signals, key people, MitchelLake history, and hiring indicators.

Args:
  - company_id (string, optional): UUID
  - name (string, optional): Company name to look up`,
    inputSchema: z.object({
      company_id: z.string().optional(),
      name: z.string().optional()
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ company_id, name }) => {
    try {
      let co;
      if (company_id) {
        const r = await dbQuery('SELECT * FROM companies WHERE id = $1', [company_id]);
        co = r.rows[0];
      } else if (name) {
        const r = await dbQuery('SELECT * FROM companies WHERE name ILIKE $1 LIMIT 1', [`%${name}%`]);
        co = r.rows[0];
      }
      if (!co) return okText(`Company not found: ${company_id || name}`);

      const sections = [`# ${co.name}`];
      if (co.industry) sections.push(`**Industry**: ${co.industry}`);
      if (co.location) sections.push(`**Location**: ${co.location}`);
      if (co.website) sections.push(`**Website**: ${co.website}`);
      if (co.description) sections.push(`\n${co.description}`);

      // MitchelLake relationship
      const clientR = await dbQuery(`SELECT * FROM clients WHERE company_id = $1 OR name ILIKE $2`, [co.id, co.name]);
      if (clientR.rows.length > 0) {
        const cl = clientR.rows[0];
        sections.push(`\n## MitchelLake Relationship`);
        sections.push(`- Client since: ${cl.first_engagement_date ? new Date(cl.first_engagement_date).toLocaleDateString() : 'unknown'}`);
        sections.push(`- Tier: ${cl.relationship_tier || 'standard'}`);
        sections.push(`- Status: ${cl.relationship_status || 'active'}`);
        if (cl.total_placements) sections.push(`- Total placements: ${cl.total_placements}`);
      }

      // Past searches
      const searchR = await dbQuery(`
        SELECT s.title, s.status, s.kick_off_date
        FROM searches s
        JOIN projects pr ON s.project_id = pr.id
        JOIN clients cl ON pr.client_id = cl.id
        WHERE cl.company_id = $1 OR cl.name ILIKE $2
        ORDER BY s.kick_off_date DESC LIMIT 10
      `, [co.id, `%${co.name}%`]);
      if (searchR.rows.length > 0) {
        sections.push(`\n## Search History (${searchR.rows.length} searches)`);
        for (const s of searchR.rows) {
          const date = s.kick_off_date ? new Date(s.kick_off_date).getFullYear() : '?';
          sections.push(`- ${s.title} (${s.status}, ${date})`);
        }
      }

      // Key people
      const peopleR = await dbQuery(`
        SELECT p.full_name, p.current_title, ps.engagement_score, ps.timing_score
        FROM people p
        LEFT JOIN person_scores ps ON p.id = ps.person_id
        WHERE p.current_company_id = $1 OR p.current_company_name ILIKE $2
        ORDER BY ps.timing_score DESC NULLS LAST LIMIT 10
      `, [co.id, `%${co.name}%`]);
      if (peopleR.rows.length > 0) {
        sections.push(`\n## Key People (${peopleR.rows.length} tracked)`);
        for (const p of peopleR.rows) {
          sections.push(`- **${p.full_name}** — ${p.current_title || '?'} (Timing: ${formatScore(p.timing_score)})`);
        }
      }

      // Recent signals
      const sigR = await dbQuery(`
        SELECT signal_type, confidence_score, evidence_summary, hiring_implications, detected_at
        FROM signal_events
        WHERE company_id = $1
        ORDER BY detected_at DESC LIMIT 8
      `, [co.id]);
      if (sigR.rows.length > 0) {
        sections.push(`\n## Recent Signals`);
        for (const s of sigR.rows) {
          const date = s.detected_at ? new Date(s.detected_at).toLocaleDateString() : '?';
          sections.push(`- **${s.signal_type}** (${date}, ${formatScore(s.confidence_score)}): ${s.evidence_summary || ''}`);
          if (s.hiring_implications) sections.push(`  _Hiring: ${s.hiring_implications}_`);
        }
      }

      return okText(sections.join('\n'));
    } catch (err) {
      return okText(`Error fetching company intel: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_signals
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_signals',
  {
    title: 'Get Signals Feed',
    description: `Get the latest market signals detected by the MitchelLake signal intelligence engine.

Signals include: funding rounds, leadership changes, expansions, layoffs, M&A activity, product launches.

Args:
  - signal_type (string, optional): Filter by type (capital_raising, geographic_expansion, strategic_hiring, ma_activity, layoffs, product_launch, partnership)
  - min_confidence (number): Minimum confidence threshold 0-1 (default: 0.6)
  - days (number): How many days back to look (default: 7)
  - limit (number): Max results (default: 20)`,
    inputSchema: z.object({
      signal_type: z.string().optional(),
      min_confidence: z.number().min(0).max(1).default(0.6),
      days: z.number().int().min(1).max(90).default(7),
      limit: z.number().int().min(1).max(50).default(20)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: false, openWorldHint: false }
  },
  async ({ signal_type, min_confidence, days, limit }) => {
    try {
      const conditions = [
        `se.confidence_score >= $1`,
        `se.detected_at > NOW() - INTERVAL '${days} days'`
      ];
      const params = [min_confidence];
      let pi = 2;

      if (signal_type) {
        conditions.push(`se.signal_type = $${pi++}`);
        params.push(signal_type);
      }

      const result = await dbQuery(`
        SELECT se.*, c.name as company_name_resolved
        FROM signal_events se
        LEFT JOIN companies c ON se.company_id = c.id
        WHERE ${conditions.join(' AND ')}
        ORDER BY se.detected_at DESC
        LIMIT $${pi}
      `, [...params, limit]);

      if (result.rows.length === 0) return okText(`No signals found in the last ${days} days`);

      const lines = [`**${result.rows.length} signals in the last ${days} days**\n`];
      for (const sig of result.rows) {
        const date = new Date(sig.detected_at).toLocaleDateString();
        const co = sig.company_name_resolved || sig.company_name || 'Unknown';
        lines.push(`### ${co} — ${sig.signal_type} (${formatScore(sig.confidence_score)})`);
        lines.push(`📅 ${date}`);
        if (sig.evidence_summary) lines.push(`> ${sig.evidence_summary}`);
        if (sig.hiring_implications) lines.push(`_Hiring: ${sig.hiring_implications}_`);
        lines.push('');
      }

      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error fetching signals: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_search_searches
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_search_searches',
  {
    title: 'Search Past & Active Searches',
    description: `Search MitchelLake's full history of 300+ executive searches (active and historical).

Every closed search is a signal — use this to find:
- Active searches needing candidates now
- Past searches for similar roles (pattern intelligence)
- Whether we've worked with a company before
- What candidates were shortlisted for similar roles

Args:
  - query (string): Role title, company, or thematic description
  - status (string, optional): Filter by status — active statuses: interviewing, sourcing, outreach, research, shortlist. Historical: placed, cancelled, on_hold
  - active_only (boolean): Only return active searches (default: false)
  - limit (number): Max results (default: 20)`,
    inputSchema: z.object({
      query: z.string().min(1),
      status: z.string().optional(),
      active_only: z.boolean().default(false),
      limit: z.number().int().min(1).max(50).default(20)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ query, status, active_only, limit }) => {
    try {
      const conditions = [`(s.title ILIKE $1 OR s.brief_summary ILIKE $1 OR cl.name ILIKE $1)`];
      const params = [`%${query}%`];
      let pi = 2;

      if (status) {
        conditions.push(`s.status = $${pi++}`);
        params.push(status);
      } else if (active_only) {
        conditions.push(`s.status IN ('interviewing', 'sourcing', 'outreach', 'research', 'shortlist', 'briefing')`);
      }

      const result = await dbQuery(`
        SELECT s.id, s.title, s.status, s.seniority_level, s.kick_off_date, s.location,
               cl.name as client_name, 
               (SELECT COUNT(*) FROM search_candidates sc WHERE sc.search_id = s.id) as candidate_count
        FROM searches s
        JOIN projects pr ON s.project_id = pr.id
        JOIN clients cl ON pr.client_id = cl.id
        WHERE ${conditions.join(' AND ')}
        ORDER BY s.kick_off_date DESC NULLS LAST
        LIMIT $${pi}
      `, [...params, limit]);

      if (result.rows.length === 0) return okText(`No searches found matching "${query}"`);

      const lines = [`**${result.rows.length} searches found for "${query}"**\n`];
      for (const s of result.rows) {
        const date = s.kick_off_date ? new Date(s.kick_off_date).getFullYear() : '?';
        const active = ['interviewing', 'sourcing', 'outreach', 'research', 'shortlist', 'briefing'].includes(s.status);
        lines.push(`${active ? '🟢' : '⚪'} **${s.title}** @ ${s.client_name}`);
        lines.push(`   ID: ${s.id} | Status: ${s.status} | ${date}${s.location ? ' | ' + s.location : ''} | ${s.candidate_count} candidates`);
        lines.push('');
      }

      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error searching searches: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_search_pipeline
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_search_pipeline',
  {
    title: 'Get Search Pipeline',
    description: `Get the full candidate pipeline for a specific search.

Returns all candidates in the pipeline with their stage, match score, and profile summary.
Also returns the search brief and key requirements.

Args:
  - search_id (string): UUID of the search (get from ml_search_searches)`,
    inputSchema: z.object({
      search_id: z.string().uuid()
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ search_id }) => {
    try {
      const searchR = await dbQuery(`
        SELECT s.*, cl.name as client_name
        FROM searches s
        JOIN projects pr ON s.project_id = pr.id
        JOIN clients cl ON pr.client_id = cl.id
        WHERE s.id = $1
      `, [search_id]);

      if (searchR.rows.length === 0) return okText(`Search not found: ${search_id}`);
      const search = searchR.rows[0];

      const sections = [
        `# ${search.title}`,
        `**Client**: ${search.client_name}`,
        `**Status**: ${search.status}`,
        search.location ? `**Location**: ${search.location}` : null,
        search.seniority_level ? `**Seniority**: ${search.seniority_level}` : null,
        search.brief_summary ? `\n## Brief\n${search.brief_summary}` : null
      ].filter(Boolean);

      const candR = await dbQuery(`
        SELECT sc.stage, sc.match_score, sc.notes,
               p.full_name, p.current_title, p.current_company_name, p.location,
               ps.engagement_score, ps.receptivity_score, ps.timing_score
        FROM search_candidates sc
        JOIN people p ON sc.person_id = p.id
        LEFT JOIN person_scores ps ON p.id = ps.person_id
        WHERE sc.search_id = $1
        ORDER BY sc.match_score DESC NULLS LAST, sc.stage
      `, [search_id]);

      if (candR.rows.length === 0) {
        sections.push('\n## Pipeline\nNo candidates in pipeline yet.');
      } else {
        sections.push(`\n## Pipeline (${candR.rows.length} candidates)`);

        // Group by stage
        const byStage = {};
        for (const c of candR.rows) {
          const stage = c.stage || 'identified';
          if (!byStage[stage]) byStage[stage] = [];
          byStage[stage].push(c);
        }

        const stageOrder = ['identified', 'contacted', 'interested', 'shortlist', 'presented', 'interviewing', 'offer', 'placed'];
        for (const stage of [...stageOrder, ...Object.keys(byStage).filter(s => !stageOrder.includes(s))]) {
          if (!byStage[stage]) continue;
          sections.push(`\n### ${stage.toUpperCase()} (${byStage[stage].length})`);
          for (const c of byStage[stage]) {
            sections.push(`- **${c.full_name}** — ${c.current_title || '?'} @ ${c.current_company_name || '?'} (Match: ${formatScore(c.match_score)}, Timing: ${formatScore(c.timing_score)})`);
          }
        }
      }

      return okText(sections.join('\n'));
    } catch (err) {
      return okText(`Error fetching pipeline: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_log_interaction
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_log_interaction',
  {
    title: 'Log Interaction',
    description: `Log a touchpoint/interaction with a person in the MitchelLake platform.

Use this to record calls, emails, meetings, LinkedIn messages, etc.
This is a write operation — it permanently records the interaction.

Args:
  - person_id (string): UUID of the person
  - interaction_type (string): One of: email, call, meeting, linkedin_message, note
  - direction (string): inbound or outbound
  - summary (string): What was discussed
  - subject (string, optional): Subject line or topic
  - user_id (string, optional): UUID of the MitchelLake team member (defaults to Jon's ID if not provided)`,
    inputSchema: z.object({
      person_id: z.string().uuid(),
      interaction_type: z.enum(['email', 'call', 'meeting', 'linkedin_message', 'note']),
      direction: z.enum(['inbound', 'outbound']),
      summary: z.string().min(5).describe('What was discussed or the content of the interaction'),
      subject: z.string().optional(),
      user_id: z.string().uuid().optional()
    }).strict(),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false }
  },
  async ({ person_id, interaction_type, direction, summary, subject, user_id }) => {
    try {
      const defaultUserId = '13ab009a-62b1-4023-80e3-6241cbcda25d'; // Jon's user ID
      const uid = user_id || defaultUserId;

      const result = await dbQuery(`
        INSERT INTO interactions (person_id, user_id, interaction_type, direction, summary, subject, interaction_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        RETURNING id
      `, [person_id, uid, interaction_type, direction, summary, subject || null]);

      const id = result.rows[0]?.id;

      // Fetch person name for confirmation
      const personR = await dbQuery('SELECT full_name FROM people WHERE id = $1', [person_id]);
      const personName = personR.rows[0]?.full_name || person_id;

      return okText(`✅ Interaction logged for **${personName}**\n- Type: ${interaction_type} (${direction})\n- Summary: ${summary}\n- ID: ${id}`);
    } catch (err) {
      return okText(`Error logging interaction: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_find_similar_people
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_find_similar_people',
  {
    title: 'Find Similar People',
    description: `Find people similar to a given person using vector similarity.

Use this to:
- Find candidates similar to a placed person ("find more like her")
- Identify people with similar backgrounds to a target
- Surface comparable candidates across the network

Args:
  - person_id (string): UUID of the reference person
  - limit (number): Max results (default: 10)
  - exclude_same_company (boolean): Exclude people at the same company (default: true)`,
    inputSchema: z.object({
      person_id: z.string().uuid(),
      limit: z.number().int().min(1).max(30).default(10),
      exclude_same_company: z.boolean().default(true)
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ person_id, limit, exclude_same_company }) => {
    try {
      // Find person's vector in Qdrant
      const results = await qdrant.retrieve('people', {
        ids: [person_id],
        with_vector: true
      });

      if (!results.length || !results[0].vector) {
        return okText(`Person ${person_id} has no embedding. Run embed_all.js first.`);
      }

      const vector = results[0].vector;
      const personR = await dbQuery('SELECT * FROM people WHERE id = $1', [person_id]);
      const person = personR.rows[0];
      if (!person) return okText(`Person not found: ${person_id}`);

      // Search for similar
      const hits = await vectorSearch('people', vector, limit + 5);
      const similarIds = hits
        .filter(h => h.id !== person_id)
        .map(h => ({ id: h.id, score: h.score }))
        .slice(0, limit + 2);

      if (similarIds.length === 0) return okText('No similar people found');

      const ids = similarIds.map(s => s.id);
      let sql = `
        SELECT p.*, ps.engagement_score, ps.timing_score, ps.receptivity_score
        FROM people p
        LEFT JOIN person_scores ps ON p.id = ps.person_id
        WHERE p.id = ANY($1)
      `;
      const params = [ids];
      if (exclude_same_company && person.current_company_name) {
        sql += ` AND p.current_company_name NOT ILIKE $2`;
        params.push(`%${person.current_company_name}%`);
      }

      const similar = await dbQuery(sql, params);
      const scoreMap = Object.fromEntries(similarIds.map(s => [s.id, s.score]));

      const sorted = similar.rows.sort((a, b) => (scoreMap[b.id] || 0) - (scoreMap[a.id] || 0)).slice(0, limit);

      const lines = [`**People similar to ${person.full_name}** (${person.current_title || '?'} @ ${person.current_company_name || '?'})\n`];
      for (const p of sorted) {
        const sim = Math.round((scoreMap[p.id] || 0) * 100);
        lines.push(`- **${p.full_name}** — ${p.current_title || '?'} @ ${p.current_company_name || '?'} (Similarity: ${sim}%, Timing: ${formatScore(p.timing_score)})`);
      }
      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error finding similar people: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_network_path
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_network_path',
  {
    title: 'Get Network Path',
    description: `Find how the MitchelLake team is connected to a person — who knows them and how well.

Use this to identify warm introduction paths before outreach.

Args:
  - person_id (string, optional): UUID of the person
  - name (string, optional): Person's name to look up`,
    inputSchema: z.object({
      person_id: z.string().uuid().optional(),
      name: z.string().optional()
    }).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false }
  },
  async ({ person_id, name }) => {
    try {
      let pid = person_id;
      if (!pid && name) {
        const r = await dbQuery(`SELECT id, full_name FROM people WHERE full_name ILIKE $1 LIMIT 1`, [`%${name}%`]);
        if (!r.rows[0]) return okText(`Person not found: ${name}`);
        pid = r.rows[0].id;
      }
      if (!pid) return okText('Provide person_id or name');

      const personR = await dbQuery('SELECT full_name, current_title, current_company_name FROM people WHERE id = $1', [pid]);
      const person = personR.rows[0];
      if (!person) return okText(`Person not found: ${pid}`);

      const proxR = await dbQuery(`
        SELECT tp.relationship_type, tp.relationship_strength, tp.connected_date,
               tp.interaction_count, tp.last_interaction_date, tp.notes,
               u.full_name as team_member, u.email as team_email
        FROM team_proximity tp
        LEFT JOIN users u ON tp.team_member_id = u.id
        WHERE tp.person_id = $1
        ORDER BY tp.relationship_strength DESC
      `, [pid]);

      if (proxR.rows.length === 0) {
        return okText(`No direct connections found to **${person.full_name}** in the MitchelLake network.`);
      }

      const lines = [
        `## Network Paths to ${person.full_name}`,
        `${person.current_title || '?'} @ ${person.current_company_name || '?'}\n`
      ];

      for (const p of proxR.rows) {
        const strength = formatScore(p.relationship_strength);
        const since = p.connected_date ? `connected ${new Date(p.connected_date).getFullYear()}` : '';
        const last = p.last_interaction_date ? `last contact ${new Date(p.last_interaction_date).toLocaleDateString()}` : '';
        const details = [since, last, p.interaction_count ? `${p.interaction_count} interactions` : null].filter(Boolean).join(', ');
        lines.push(`### ${p.team_member || '?'} (${strength} strength)`);
        lines.push(`Type: ${p.relationship_type || 'connection'}${details ? ' | ' + details : ''}`);
        if (p.notes) lines.push(`Note: ${p.notes}`);
        lines.push('');
      }

      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error fetching network path: ${err.message}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TOOL: ml_get_platform_stats
// ─────────────────────────────────────────────────────────────────────────────

server.registerTool(
  'ml_get_platform_stats',
  {
    title: 'Get Platform Stats',
    description: `Get a high-level summary of the MitchelLake Signal Intelligence Platform — data volumes, active searches, recent signal activity.`,
    inputSchema: z.object({}).strict(),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: false, openWorldHint: false }
  },
  async () => {
    try {
      const [people, companies, searches, signals, interactions, scores] = await Promise.all([
        dbQuery('SELECT COUNT(*) FROM people'),
        dbQuery('SELECT COUNT(*) FROM companies'),
        dbQuery(`SELECT status, COUNT(*) FROM searches GROUP BY status ORDER BY count DESC`),
        dbQuery(`SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '7 days'`),
        dbQuery(`SELECT COUNT(*) FROM interactions WHERE interaction_at > NOW() - INTERVAL '30 days'`),
        dbQuery('SELECT COUNT(*) FROM person_scores')
      ]);

      const searchBreakdown = searches.rows.map(r => `  ${r.status}: ${r.count}`).join('\n');

      const lines = [
        '# MitchelLake Signal Intelligence Platform',
        `\n## Data`,
        `- People tracked: ${Number(people.rows[0].count).toLocaleString()}`,
        `- Companies: ${Number(companies.rows[0].count).toLocaleString()}`,
        `- People scored: ${Number(scores.rows[0].count).toLocaleString()}`,
        `\n## Searches`,
        searchBreakdown,
        `\n## Activity`,
        `- Signals detected (7 days): ${signals.rows[0].count}`,
        `- Interactions logged (30 days): ${interactions.rows[0].count}`
      ];

      return okText(lines.join('\n'));
    } catch (err) {
      return okText(`Error fetching stats: ${(err.message || err.code || String(err))}`);
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// TRANSPORT — stdio (local) or HTTP (Railway/remote)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// TRANSPORT — only runs when executed directly (not when require()'d)
// ─────────────────────────────────────────────────────────────────────────────

  return server;
}

if (require.main === module) {
  const mcpInstance = createMcpServer();
  const server = mcpInstance;
  const transport = process.env.MCP_TRANSPORT || 'stdio';
  if (transport === 'http') {
    const express = require('express');
    const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
    const app = express();
    app.use(express.json());
    app.get('/health', (_req, res) => res.json({ status: 'ok' }));
    app.post('/mcp', async (req, res) => {
      const t = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
      res.on('close', () => t.close());
      await mcpInstance.connect(t);
      await t.handleRequest(req, res, req.body);
    });
    const port = parseInt(process.env.MCP_PORT || process.env.PORT || 3001);
    app.listen(port, () => process.stderr.write(`MitchelLake MCP (HTTP) on port ${port}/mcp\n`));
  } else {
    const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
    (async () => {
      const t = new StdioServerTransport();
      await mcpInstance.connect(t);
      process.stderr.write('MitchelLake MCP Server running (stdio)\n');
    })().catch(err => { process.stderr.write(`Fatal: ${err.message}\n`); process.exit(1); });
  }
} else {
}

module.exports = { createMcpServer };
// Sun Mar  8 17:57:35 CET 2026
