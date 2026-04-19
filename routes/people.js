// ═══════════════════════════════════════════════════════════════════════════════
// routes/people.js — People API routes
// 12 routes: /api/people/*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, verifyHuddleMember, generateQueryEmbedding, searchPublications }) {

router.get('/api/people', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    let where = 'WHERE p.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter — hide private contacts from non-owners
    const userId = req.user?.user_id;
    if (userId) {
      paramIdx++;
      where += ` AND (p.visibility IS NULL OR p.visibility != 'private' OR p.owner_user_id = $${paramIdx})`;
      params.push(userId);
    } else {
      where += ` AND (p.visibility IS NULL OR p.visibility != 'private')`;
    }

    // By default, only show people with actual profile data
    if (req.query.show_all !== 'true') {
      where += ` AND (p.current_title IS NOT NULL OR p.headline IS NOT NULL OR p.source = 'ezekia')`;
    }

    if (q) {
      paramIdx++;
      where += ` AND (p.full_name ILIKE $${paramIdx} OR p.current_title ILIKE $${paramIdx} OR p.current_company_name ILIKE $${paramIdx} OR p.headline ILIKE $${paramIdx} OR p.location ILIKE $${paramIdx})`;
      params.push(`%${q}%`);
    }
    if (req.query.source) {
      paramIdx++;
      where += ` AND p.source = $${paramIdx}`;
      params.push(req.query.source);
    }
    if (req.query.has_notes === 'true') {
      where += ` AND p.id IN (SELECT DISTINCT person_id FROM interactions WHERE interaction_type = 'research_note' AND tenant_id = $1)`;
    }
    if (req.query.seniority) {
      paramIdx++;
      where += ` AND p.seniority_level = $${paramIdx}`;
      params.push(req.query.seniority);
    }
    if (req.query.industry) {
      paramIdx++;
      where += ` AND $${paramIdx} = ANY(p.industries)`;
      params.push(req.query.industry);
    }
    if (req.query.company) {
      paramIdx++;
      where += ` AND p.current_company_name ILIKE $${paramIdx}`;
      params.push(`%${req.query.company}%`);
    }
    if (req.query.skill) {
      const skills = Array.isArray(req.query.skill) ? req.query.skill : [req.query.skill];
      skills.forEach(s => {
        paramIdx++;
        where += ` AND $${paramIdx} = ANY(p.expertise_tags)`;
        params.push(s);
      });
    }

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const [peopleResult, countResult] = await Promise.all([
      db.query(`
        SELECT p.id, p.full_name, p.current_title, p.current_company_name,
               p.headline, p.location, p.source, p.seniority_level,
               p.expertise_tags, p.industries, p.email, p.linkedin_url,
               p.functional_area, p.embedded_at IS NOT NULL AS is_embedded,
               (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' AND i.tenant_id = p.tenant_id) AS note_count
        FROM people p
        ${where}
        ORDER BY
          CASE WHEN p.current_title IS NOT NULL THEN 0 ELSE 1 END,
          note_count DESC,
          p.full_name
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      db.query(`SELECT COUNT(*) AS cnt FROM people p ${where}`, params.slice(0, -2)),
    ]);

    res.json({
      people: peopleResult.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
    });
  } catch (err) {
    console.error('People list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch people' });
  }
});

// Recent interactions stream — who MitchelLake team contacted recently
router.get('/api/people/stream/recent-contacts', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const { rows } = await db.query(`
      SELECT DISTINCT ON (i.person_id)
        i.person_id, i.interaction_type, i.subject, i.summary,
        i.interaction_at, i.direction, i.channel, i.source,
        p.full_name, p.current_title, p.current_company_name, p.location,
        p.seniority_level, p.linkedin_url,
        u.name AS contacted_by
      FROM interactions i
      JOIN people p ON p.id = i.person_id
      LEFT JOIN users u ON u.id = i.user_id
      WHERE i.interaction_at IS NOT NULL AND i.tenant_id = $1
      ORDER BY i.person_id, i.interaction_at DESC
    `, [req.tenant_id]);
    // Sort by most recent interaction across all people
    rows.sort((a, b) => new Date(b.interaction_at) - new Date(a.interaction_at));
    res.json({ contacts: rows.slice(0, limit) });
  } catch (err) {
    console.error('Recent contacts error:', err.message);
    res.status(500).json({ error: 'Failed to fetch recent contacts' });
  }
});

// Signal-connected candidates — people at companies with recent signals
router.get('/api/people/stream/signal-connected', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const { rows } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name,
             p.location, p.seniority_level, p.linkedin_url,
             se.signal_type, se.evidence_summary, se.confidence_score,
             se.detected_at AS signal_detected_at, se.company_name AS signal_company,
             (SELECT COUNT(*) FROM interactions ix WHERE ix.person_id = p.id AND ix.interaction_type = 'research_note' AND ix.tenant_id = p.tenant_id) AS note_count
      FROM people p
      JOIN companies c ON c.id = p.current_company_id
      JOIN signal_events se ON se.company_id = c.id
      WHERE se.detected_at > NOW() - INTERVAL '30 days'
        AND (p.current_title IS NOT NULL OR p.headline IS NOT NULL)
        AND p.tenant_id = $2
      ORDER BY se.detected_at DESC, se.confidence_score DESC
      LIMIT $1
    `, [limit, req.tenant_id]);
    res.json({ people: rows });
  } catch (err) {
    console.error('Signal-connected error:', err.message);
    res.status(500).json({ error: 'Failed to fetch signal-connected people' });
  }
});

// Skill/industry facets for filter UI
router.get('/api/people/facets', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const [skillsResult, industriesResult, senioritiesResult] = await Promise.all([
      db.query(`
        SELECT unnest(expertise_tags) AS val, COUNT(*) AS cnt
        FROM people WHERE tenant_id = $1 AND expertise_tags IS NOT NULL
        GROUP BY val ORDER BY cnt DESC LIMIT 30
      `, [req.tenant_id]),
      db.query(`
        SELECT unnest(industries) AS val, COUNT(*) AS cnt
        FROM people WHERE tenant_id = $1 AND industries IS NOT NULL
        GROUP BY val ORDER BY cnt DESC LIMIT 20
      `, [req.tenant_id]),
      db.query(`
        SELECT seniority_level AS val, COUNT(*) AS cnt
        FROM people WHERE tenant_id = $1 AND seniority_level IS NOT NULL
        GROUP BY seniority_level ORDER BY cnt DESC
      `, [req.tenant_id]),
    ]);
    res.json({
      skills: skillsResult.rows.map(r => ({ name: r.val, count: parseInt(r.cnt) })),
      industries: industriesResult.rows.map(r => ({ name: r.val, count: parseInt(r.cnt) })),
      seniorities: senioritiesResult.rows.map(r => ({ name: r.val, count: parseInt(r.cnt) })),
    });
  } catch (err) {
    console.error('People facets error:', err.message);
    res.status(500).json({ error: 'Failed to fetch facets' });
  }
});

router.get('/api/people/:id', authenticateToken, async (req, res) => {
  try {
    // Guard against invalid UUIDs (e.g. "null", "undefined", empty)
    const id = req.params.id;
    if (!id || id === 'null' || id === 'undefined' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
      return res.status(400).json({ error: 'Invalid person ID' });
    }
    const db = new TenantDB(req.tenant_id);
    const { rows: [person] } = await db.query(`
      SELECT p.*, c.name AS company_name_full, c.sector AS company_sector,
             c.geography AS company_geography, c.id AS company_id_linked,
             c.domain AS company_domain, c.is_client AS company_is_client
      FROM people p
      LEFT JOIN companies c ON p.current_company_id = c.id
      WHERE p.id = $1 AND p.tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    if (!person) return res.status(404).json({ error: 'Person not found' });

    // Privacy check — block access to private contacts unless owner
    if (person.visibility === 'private' && person.owner_user_id && person.owner_user_id !== req.user?.user_id) {
      return res.status(403).json({ error: 'This contact is private' });
    }

    // Research notes
    const { rows: notes } = await db.query(`
      SELECT id, summary, subject, email_snippet, interaction_at, created_at,
             note_quality, extracted_intelligence, source, interaction_type
      FROM interactions
      WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2
      ORDER BY interaction_at DESC NULLS LAST
      LIMIT 50
    `, [req.params.id, req.tenant_id]);

    // All other interactions (emails, calls, meetings)
    const { rows: interactions } = await db.query(`
      SELECT id, interaction_type, summary, subject, email_snippet,
             interaction_at, created_at, channel, direction, source,
             visibility, is_internal, sensitivity,
             email_from, email_to
      FROM interactions
      WHERE person_id = $1 AND interaction_type != 'research_note'
        AND (visibility IS NULL OR visibility != 'private' OR owner_user_id = $2)
        AND tenant_id = $3
      ORDER BY interaction_at DESC NULLS LAST
      LIMIT 30
    `, [req.params.id, req.user?.user_id, req.tenant_id]);

    // Person signals
    const { rows: signals } = await db.query(`
      SELECT id, signal_type, signal_category, title, description,
             confidence_score, signal_date, detected_at
      FROM person_signals
      WHERE person_id = $1 AND tenant_id = $2
      ORDER BY detected_at DESC
    `, [req.params.id, req.tenant_id]);

    // Company signals (if person has a linked company)
    let companySignals = [];
    let companyJobs = [];
    if (person.current_company_id) {
      const { rows } = await db.query(`
        SELECT id, signal_type, confidence_score, evidence_summary, detected_at, triage_status
        FROM signal_events WHERE company_id = $1 AND (tenant_id IS NULL OR tenant_id = $2)
        ORDER BY detected_at DESC LIMIT 10
      `, [person.current_company_id, req.tenant_id]);
      companySignals = rows;

      // Active senior job postings at their employer
      try {
        const { rows: jobs } = await db.query(`
          SELECT jp.title, jp.location, jp.seniority_level, jp.function_area,
                 jp.first_seen_at, jp.apply_url, jp.days_open
          FROM job_postings jp
          WHERE jp.company_id = $1 AND jp.tenant_id = $2
            AND jp.status = 'active'
            AND jp.seniority_level IN ('c_suite', 'vp', 'director')
          ORDER BY jp.first_seen_at DESC
          LIMIT 10
        `, [person.current_company_id, req.tenant_id]);
        companyJobs = jobs;
      } catch (e) {}
    }

    // Interaction stats — count all types
    const { rows: [stats] } = await db.query(`
      SELECT COUNT(*) AS total,
             COUNT(*) FILTER (WHERE interaction_type = 'research_note') AS notes,
             COUNT(*) FILTER (WHERE interaction_type IN ('email', 'gmail', 'enrich_gmail')) AS emails,
             COUNT(*) FILTER (WHERE interaction_type = 'call') AS calls,
             COUNT(*) FILTER (WHERE interaction_type = 'meeting') AS meetings,
             COUNT(*) FILTER (WHERE interaction_type IN ('linkedin_message', 'linkedin')) AS linkedin,
             COUNT(*) FILTER (WHERE interaction_type NOT IN ('research_note','email','gmail','enrich_gmail','call','meeting','linkedin_message','linkedin')) AS other,
             MIN(interaction_at) AS first_interaction,
             MAX(interaction_at) AS last_interaction
      FROM interactions WHERE person_id = $1 AND tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    // Also get type breakdown for debugging
    const { rows: typeCounts } = await db.query(
      `SELECT interaction_type, COUNT(*) AS cnt FROM interactions WHERE person_id = $1 AND tenant_id = $2 GROUP BY interaction_type ORDER BY cnt DESC`,
      [req.params.id, req.tenant_id]
    );
    stats.type_breakdown = typeCounts;

    // Colleagues at same company
    let colleagues = [];
    if (person.current_company_id) {
      const { rows } = await db.query(`
        SELECT id, full_name, current_title, seniority_level
        FROM people
        WHERE current_company_id = $1 AND id != $2 AND tenant_id = $3
        ORDER BY full_name LIMIT 20
      `, [person.current_company_id, req.params.id, req.tenant_id]);
      colleagues = rows;
    }

    // Proximity — which team members are connected to this person (deduplicated)
    let proximity = [];
    try {
      const { rows } = await db.query(`
        SELECT u.name AS team_member, u.id AS team_member_id,
               MAX(tp.relationship_strength) AS relationship_strength,
               string_agg(DISTINCT tp.relationship_type, ', ') AS proximity_type,
               string_agg(DISTINCT tp.source, ', ') AS proximity_source,
               (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = $1 AND i.user_id = u.id) AS last_contact,
               (SELECT COUNT(*) FROM interactions i WHERE i.person_id = $1 AND i.user_id = u.id) AS interaction_count
        FROM team_proximity tp
        JOIN users u ON u.id = tp.team_member_id
        WHERE tp.person_id = $1 AND tp.relationship_strength >= 0.1
        GROUP BY u.name, u.id
        ORDER BY MAX(tp.relationship_strength) DESC
        LIMIT 10
      `, [req.params.id]);
      proximity = rows;
    } catch (e) {}

    // Huddle proximity — if huddle context active, show all members' connections
    var huddle_proximity = null;
    var huddleId = req.query.huddle_id;
    if (huddleId) {
      try {
        var hMembership = await verifyHuddleMember(huddleId, req.tenant_id);
        if (hMembership) {
          var { rows: hMembers } = await platformPool.query(
            `SELECT hm.tenant_id, t.name AS tenant_name FROM huddle_members hm JOIN tenants t ON t.id = hm.tenant_id WHERE hm.huddle_id = $1 AND hm.status = 'active'`,
            [huddleId]
          );
          var hProx = [];
          for (var hm of hMembers) {
            var { rows: tp } = await platformPool.query(
              `SELECT MAX(tp.relationship_strength) AS score,
                      MAX(tp.last_interaction_date) AS last_contact,
                      STRING_AGG(DISTINCT tp.relationship_type, ', ') AS types
               FROM team_proximity tp
               JOIN users u ON u.id = tp.team_member_id
               WHERE tp.person_id = $1 AND u.tenant_id = $2`,
              [req.params.id, hm.tenant_id]
            );
            var score = tp[0]?.score || 0;
            hProx.push({
              member_name: hm.tenant_name,
              tenant_id: hm.tenant_id,
              score: parseFloat(score) || 0,
              classification: score >= 0.7 ? 'Strong' : score >= 0.4 ? 'Warm' : score >= 0.2 ? 'Cool' : 'Cold',
              last_contact: tp[0]?.last_contact || null,
              types: tp[0]?.types || null,
            });
          }
          hProx.sort(function(a, b) { return b.score - a.score; });
          var bestPath = hProx[0];
          huddle_proximity = {
            members: hProx,
            best_entry_point: bestPath?.score > 0 ? {
              member_name: bestPath.member_name,
              score: bestPath.score,
              classification: bestPath.classification,
            } : null,
          };
        }
      } catch (e) {}
    }

    // Work artifacts linked to this person
    let artifacts = [];
    try {
      const { rows } = await db.query(`
        SELECT wa.id, wa.artifact_type, wa.title, wa.summary, wa.key_findings,
               wa.status, wa.created_by_name, wa.created_at, ael.link_type
        FROM work_artifacts wa
        JOIN artifact_entity_links ael ON ael.artifact_id = wa.id AND ael.person_id = $1
        WHERE wa.tenant_id = $2 AND wa.status != 'archived'
        ORDER BY wa.created_at DESC LIMIT 10
      `, [req.params.id, req.tenant_id]);
      artifacts = rows;
    } catch (e) { /* table may not exist yet */ }

    res.json({
      ...person,
      research_notes: notes,
      interactions,
      person_signals: signals,
      company_signals: companySignals,
      company_jobs: companyJobs,
      interaction_stats: stats,
      colleagues,
      proximity,
      huddle_proximity,
      artifacts,
    });
  } catch (err) {
    console.error('Person detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch person' });
  }
});

router.get('/api/people/:id/notes', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT id, summary, subject, interaction_at, created_at, note_quality,
             extracted_intelligence, source
      FROM interactions
      WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2
      ORDER BY interaction_at DESC NULLS LAST
    `, [req.params.id, req.tenant_id]);

    res.json({ notes: rows });
  } catch (err) {
    console.error('Person notes error:', err.message);
    res.status(500).json({ error: 'Failed to fetch notes' });
  }
});

// ─── Edit Person ───
// Parse pasted career history text into structured roles
router.post('/api/people/:id/career/parse', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { text } = req.body;
    if (!text || text.length < 10) return res.status(400).json({ error: 'Paste career history text' });

    // Parse LinkedIn-style career text
    // Formats:
    //   Title\nCompany\nDates · Duration\nLocation\n\n
    //   Title at Company (Date - Date)
    //   Company — Title (Date - Date)
    const roles = [];
    const blocks = text.split(/\n\s*\n/).filter(b => b.trim());

    for (const block of blocks) {
      const lines = block.split('\n').map(l => l.trim()).filter(Boolean);
      if (lines.length === 0) continue;

      let title = null, company = null, startDate = null, endDate = null, current = false, location = null, description = null;

      // Try to parse each line
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // Date patterns: "Jan 2020 - Present · 4 yrs", "2020 - 2023", "Mar 2016 - Dec 2019 · 3 yrs 10 mos"
        const dateMatch = line.match(/^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}|\d{4})\s*[-–]\s*(Present|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}|\d{4})/i);
        if (dateMatch) {
          startDate = dateMatch[1];
          endDate = dateMatch[2];
          if (/present/i.test(endDate)) { current = true; endDate = null; }
          continue;
        }

        // Duration-only line: "4 yrs 3 mos", "2 years"
        if (/^\d+\s*(yr|year|mo|month)/i.test(line)) continue;

        // Location patterns
        if (/,\s*(Australia|United States|United Kingdom|Singapore|London|Sydney|Melbourne|New York|San Francisco)/i.test(line)) {
          location = line;
          continue;
        }

        // "Full-time", "Part-time", "Contract" — skip
        if (/^(Full-time|Part-time|Contract|Self-employed|Freelance|Internship)$/i.test(line)) continue;

        // First meaningful line = title, second = company
        if (!title) { title = line; }
        else if (!company) { company = line; }
        else if (!description) { description = line; }
      }

      // Handle "Title at Company" format
      if (title && !company && title.includes(' at ')) {
        const parts = title.split(' at ');
        title = parts[0].trim();
        company = parts.slice(1).join(' at ').trim();
      }

      // Handle "Company — Title" format
      if (title && !company && (title.includes(' — ') || title.includes(' - '))) {
        const sep = title.includes(' — ') ? ' — ' : ' - ';
        const parts = title.split(sep);
        if (parts.length === 2 && parts[0].length > 2 && parts[1].length > 2) {
          company = parts[0].trim();
          title = parts[1].trim();
        }
      }

      if (title || company) {
        // Normalize dates to ISO
        function parseDate(d) {
          if (!d) return null;
          const m = d.match(/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})/i);
          if (m) {
            const months = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12 };
            return `${m[2]}-${String(months[m[1].toLowerCase()]).padStart(2,'0')}-01`;
          }
          if (/^\d{4}$/.test(d)) return `${d}-01-01`;
          return null;
        }

        roles.push({
          title: title || null,
          company: company || null,
          start_date: parseDate(startDate),
          end_date: current ? null : parseDate(endDate),
          current: current || false,
          location: location || null,
          description: description || null,
        });
      }
    }

    if (!roles.length) return res.json({ error: 'Could not parse any roles from the text', roles: 0 });

    // Save to person record
    await db.query(
      `UPDATE people SET career_history = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3`,
      [JSON.stringify(roles), req.params.id, req.tenant_id]
    );

    // Update current title/company from the most recent role
    const currentRole = roles.find(r => r.current) || roles[0];
    if (currentRole) {
      await db.query(
        `UPDATE people SET
           current_title = COALESCE($1, current_title),
           current_company_name = COALESCE($2, current_company_name),
           updated_at = NOW()
         WHERE id = $3 AND tenant_id = $4`,
        [currentRole.title, currentRole.company, req.params.id, req.tenant_id]
      );
    }

    // Create company records for any new companies mentioned
    for (const role of roles) {
      if (!role.company) continue;
      try {
        const { rows: [existing] } = await db.query(
          `SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
          [role.company.trim(), req.tenant_id]
        );
        if (!existing) {
          await db.query(
            `INSERT INTO companies (name, source, tenant_id, created_at, updated_at)
             VALUES ($1, 'career_paste', $2, NOW(), NOW())`,
            [role.company.trim(), req.tenant_id]
          );
        }
      } catch (e) { /* duplicate — fine */ }
    }

    res.json({ roles: roles.length, parsed: roles });
  } catch (err) {
    console.error('Career parse error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

router.patch('/api/people/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const allowedFields = ['full_name', 'current_title', 'current_company_name', 'email', 'phone',
                           'linkedin_url', 'location', 'headline', 'seniority_level', 'functional_area', 'bio',
                           'visibility', 'email_alt'];
    const updates = [];
    const params = [req.params.id];
    let idx = 1;

    for (const [key, value] of Object.entries(req.body)) {
      if (!allowedFields.includes(key)) continue;
      idx++;
      updates.push(`${key} = $${idx}`);
      params.push(value || null);
    }

    if (updates.length === 0) return res.status(400).json({ error: 'No valid fields to update' });

    // If visibility changed to private, set owner and timestamp
    if (req.body.visibility === 'private') {
      idx++;
      updates.push(`owner_user_id = $${idx}`);
      params.push(req.user?.user_id || null);
      updates.push(`marked_private_at = NOW()`);
    } else if (req.body.visibility === 'company') {
      updates.push(`owner_user_id = NULL`);
      updates.push(`marked_private_at = NULL`);
    }

    // If company name changed, try to link to company record
    if (req.body.current_company_name) {
      const compName = req.body.current_company_name.trim();
      // Try exact match first, then fuzzy (first significant word)
      let match = await db.queryOne(
        `SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
        [compName, req.tenant_id]
      );
      if (!match) {
        // Fuzzy: match on first word that's 4+ chars
        const words = compName.replace(/[()]/g, '').split(/\s+/).filter(function(w) { return w.length >= 4; });
        if (words.length > 0) {
          match = await db.queryOne(
            `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 ORDER BY LENGTH(name) LIMIT 1`,
            ['%' + words[0] + '%', req.tenant_id]
          );
        }
      }
      if (!match) {
        // Create new company record
        const created = await db.queryOne(
          `INSERT INTO companies (name, tenant_id, created_at) VALUES ($1, $2, NOW()) RETURNING id`,
          [compName, req.tenant_id]
        );
        match = created;
      }
      if (match) {
        idx++;
        updates.push(`current_company_id = $${idx}`);
        params.push(match.id);
      }
    }

    idx++;
    params.push(req.tenant_id);
    const { rows } = await db.query(
      `UPDATE people SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $${idx} RETURNING *`,
      params
    );

    if (rows.length === 0) return res.status(404).json({ error: 'Person not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Person update error:', err.message);
    res.status(500).json({ error: 'Failed to update person' });
  }
});

// ─── Person Enrichment ───
router.post('/api/people/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [person] } = await db.query(
      `SELECT id, full_name, email, source_id, source, current_title,
              current_company_name, current_company_id, linkedin_url, location
       FROM people WHERE id = $1 AND tenant_id = $2`, [req.params.id, req.tenant_id]
    );
    if (!person) return res.status(404).json({ error: 'Person not found' });

    const enrichResults = { ezekia_profile: null, ezekia_projects: null, gmail: null, signals: null, web: null, embedding: null };

    // 1a. Ezekia People API — pull latest profile data
    if (process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('../lib/ezekia');

        // If no source_id, try to find them in Ezekia by email (most reliable) or exact name
        let ezekiaId = person.source_id;
        if (!ezekiaId && person.email) {
          const searchRes = await ezekia.searchPeople({ email: person.email });
          const match = searchRes?.data?.[0];
          if (match) {
            ezekiaId = String(match.id);
            await db.query('UPDATE people SET source_id = $1, source = $2 WHERE id = $3 AND source_id IS NULL AND tenant_id = $4',
              [ezekiaId, 'ezekia', req.params.id, req.tenant_id]);
          }
        }
        if (!ezekiaId && person.full_name) {
          const searchRes = await ezekia.searchPeople({ name: person.full_name });
          // Only match if name is exact (not fuzzy)
          const match = searchRes?.data?.find(m => {
            const ezName = (m.fullName || `${m.firstName || ''} ${m.lastName || ''}`).trim().toLowerCase();
            return ezName === person.full_name.toLowerCase();
          });
          if (match) {
            ezekiaId = String(match.id);
            await db.query('UPDATE people SET source_id = $1, source = $2 WHERE id = $3 AND source_id IS NULL AND tenant_id = $4',
              [ezekiaId, 'ezekia', req.params.id, req.tenant_id]);
          }
        }

        if (!ezekiaId) {
          enrichResults.ezekia_profile = { message: 'Not found in Ezekia CRM' };
        } else {
        // Pull full profile with all relationships + notes in parallel
        const [ezRes, notesRes] = await Promise.all([
          ezekia.getPersonFull(ezekiaId),
          ezekia.getPersonNotes(ezekiaId).catch(() => null)
        ]);

        if (ezRes && ezRes.data) {
          const d = ezRes.data;

          // Safety: verify the Ezekia name matches our person (prevent wrong ID contamination)
          const ezName = (d.fullName || `${d.firstName || ''} ${d.lastName || ''}`).trim().toLowerCase();
          const ourName = (person.full_name || '').toLowerCase();
          // Safety: verify the Ezekia name matches our person
          if (ezName && ourName && !ezName.includes(ourName.split(' ')[0]) && !ourName.includes(ezName.split(' ')[0])) {
            console.warn(`Ezekia name mismatch: "${d.fullName}" vs "${person.full_name}" — re-searching`);
            await db.query('UPDATE people SET source_id = NULL WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
            enrichResults.ezekia_profile = { error: `Name mismatch: "${d.fullName}" — source_id cleared, will re-match on next enrich` };
          } else {

          const updates = {};

          // Profile fields — find the CURRENT position
          // Sort by startDate DESC first, then pick the most recent that's active
          // Don't trust primary/tense flags alone — Ezekia data often has stale flags
          const positions = (d.profile?.positions || [])
            .sort((a, b) => (b.startDate || '0000').localeCompare(a.startDate || '0000'));
          const pos = positions.find(p => p.endDate === '9999-12-31' || !p.endDate || p.endDate > new Date().toISOString().slice(0, 10))
            || positions[0]; // fallback to most recent by start date

          // For Ezekia-sourced people: update title/company if Ezekia has a newer primary position
          // For other sources: only fill empty fields
          const isEzekiaSource = person.source === 'ezekia';
          if (d.headline || d.profile?.headline) {
            if (!person.headline || isEzekiaSource) updates.headline = d.headline || d.profile.headline;
          }
          if (pos?.title) {
            if (!person.current_title || (isEzekiaSource && pos.title !== person.current_title)) updates.current_title = pos.title;
          }
          if (pos?.company?.name || pos?.company) {
            const ezCompany = pos.company?.name || pos.company;
            if (!person.current_company_name || (isEzekiaSource && ezCompany !== person.current_company_name)) updates.current_company_name = ezCompany;
          }

          // Contact data from base object (emails[], phones[], links[], addresses[])
          const defaultEmail = d.emails?.find(e => e.isDefault)?.address || d.emails?.[0]?.address;
          const defaultPhone = d.phones?.find(p => p.isDefault)?.number || d.phones?.[0]?.number;
          const linkedinLink = d.links?.find(l => l.type === 'linkedin' || l.url?.includes('linkedin'))?.url;
          const defaultAddr = d.addresses?.find(a => a.isDefault) || d.addresses?.[0];

          if (!person.email && defaultEmail) updates.email = defaultEmail;
          if (!person.phone && defaultPhone) updates.phone = defaultPhone;
          if (!person.linkedin_url && linkedinLink) updates.linkedin_url = linkedinLink;
          if (!person.location && defaultAddr) updates.location = [defaultAddr.city, defaultAddr.state, defaultAddr.country].filter(Boolean).join(', ');
          if (!person.city && defaultAddr?.city) updates.city = defaultAddr.city;
          if (!person.country && defaultAddr?.country) updates.country = defaultAddr.country;

          // Store all emails as alt if we have multiple
          if (d.emails?.length > 1 && !person.email_alt) {
            const altEmail = d.emails.find(e => !e.isDefault)?.address;
            if (altEmail) updates.email_alt = altEmail;
          }

          // Career history from positions — extract everything useful for signal matching
          if (d.profile?.positions?.length > 0) {
            const career = d.profile.positions.map(p => ({
              title: p.title,
              company: p.company?.name || p.company,
              company_id: p.company?.id || null,
              location: p.location?.name || null,
              start_date: p.startDate,
              end_date: p.endDate,
              current: p.tense || p.primary || !p.endDate || p.endDate === '9999-12-31',
              skills: p.skills || [],
              summary: p.summary || null,
              achievements: (p.achievements || []).filter(Boolean),
              seniority_tag: p.career?.name || null,
              department: p.department || null,
              industry: p.industry || null,
            }));
            updates.career_history = JSON.stringify(career);

            // Extract all skills across all positions → expertise_tags
            const allSkills = [...new Set(d.profile.positions.flatMap(p => p.skills || []))].filter(Boolean);
            if (allSkills.length > 0) updates.expertise_tags = allSkills;

            // Derive seniority from Ezekia career tag (more reliable than title parsing)
            const currentCareer = pos?.career?.name;
            if (currentCareer) {
              const ccl = currentCareer.toLowerCase();
              let sen = null;
              if (/\b(ceo|cfo|cto|coo|cmo|cio|chief|founder|co-founder|managing director|president|partner)\b/i.test(ccl)) sen = 'c_suite';
              else if (/\b(vp|vice president|svp|evp)\b/i.test(ccl)) sen = 'vp';
              else if (/\b(director|head of|general manager)\b/i.test(ccl)) sen = 'director';
              else if (/\b(senior|lead|principal|staff)\b/i.test(ccl)) sen = 'senior';
              else if (/\b(manager|supervisor|controller)\b/i.test(ccl)) sen = 'manager';
              if (sen) updates.seniority_level = sen;
            }
          }

          // Profile picture
          if (d.profilePicture && !person.profile_photo_url) {
            updates.profile_photo_url = d.profilePicture;
          }

          // Education
          if (d.profile?.education?.length > 0) {
            updates.education = JSON.stringify(d.profile.education);
          }

          if (Object.keys(updates).length > 0) {
            const setClauses = Object.entries(updates).map(([k, v], i) => `${k} = $${i + 2}`);
            const updateVals = Object.values(updates);
            await db.query(`UPDATE people SET ${setClauses.join(', ')}, synced_at = NOW(), updated_at = NOW() WHERE id = $1 AND tenant_id = $${updateVals.length + 2}`,
              [req.params.id, ...updateVals, req.tenant_id]);
            enrichResults.ezekia_profile = { updated_fields: Object.keys(updates) };
          } else {
            enrichResults.ezekia_profile = { message: 'No new profile data' };
          }

          // Import Ezekia assignments (projects this person was considered for)
          if (d.relationships?.assignments?.length > 0) {
            const assignments = d.relationships.assignments;
            enrichResults.ezekia_assignments = {
              total: assignments.length,
              projects: assignments.slice(0, 10).map(a => ({
                project: a.projectName || a.name,
                status: a.status,
                stage: a.stage
              }))
            };
          }
        }

        // Import Ezekia notes as research notes (interactions)
        if (notesRes?.data) {
          const researchNotes = notesRes.data.researchNotes || [];
          const systemNotes = notesRes.data.systemNotes || [];
          let notesImported = 0;

          for (const note of [...researchNotes, ...systemNotes].slice(0, 50)) {
            const noteText = note.textStripped || note.text || '';
            if (!noteText || noteText.length < 5) continue;

            // Check if already imported (by external_id)
            const { rows: existing } = await db.query(
              `SELECT id FROM interactions WHERE person_id = $1 AND external_id = $2 AND tenant_id = $3`,
              [req.params.id, 'ezekia_note_' + note.id, req.tenant_id]
            );
            if (existing.length > 0) continue;

            await db.query(`
              INSERT INTO interactions (person_id, user_id, interaction_type, direction, subject, summary,
                source, external_id, channel, interaction_at, tenant_id, created_at)
              VALUES ($1, $2, 'research_note', 'inbound', $3, $4, 'ezekia_enrich', $5, 'crm', $6, $7, NOW())
              ON CONFLICT DO NOTHING
            `, [
              req.params.id, req.user.user_id,
              (note.type === 'system' ? 'Ezekia: ' : '') + (note.author || 'Note').slice(0, 100),
              noteText.slice(0, 5000),
              'ezekia_note_' + note.id,
              note.date ? new Date(note.date) : new Date(),
              req.tenant_id
            ]);
            notesImported++;
          }
          enrichResults.ezekia_notes = { research: researchNotes.length, system: systemNotes.length, imported: notesImported };
        }

        } // end name match safety check

        // ── Pull aspirations, status, and documents from Ezekia ──
        try {
          const [aspirations, status, docs] = await Promise.all([
            ezekia.getPersonAspirations(parseInt(ezekiaId)).catch(() => null),
            ezekia.getPersonStatus(parseInt(ezekiaId)).catch(() => null),
            ezekia.getPersonDocuments(parseInt(ezekiaId)).catch(() => null),
          ]);

          if (aspirations?.data) {
            enrichResults.ezekia_aspirations = aspirations.data;
            // Store aspirations as extracted intelligence
            const aspText = JSON.stringify(aspirations.data);
            if (aspText.length > 10) {
              await db.query(`UPDATE people SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('aspirations', $1::jsonb) WHERE id = $2 AND tenant_id = $3`,
                [aspText, req.params.id, req.tenant_id]).catch(() => {});
            }
          }

          if (status?.data) {
            enrichResults.ezekia_status = status.data;
          }

          if (docs?.data && Array.isArray(docs.data)) {
            enrichResults.ezekia_documents = {
              count: docs.data.length,
              files: docs.data.slice(0, 10).map(d => ({ name: d.name || d.filename, type: d.type || d.mimeType, id: d.id }))
            };

            // Extract companies from CV/document filenames and career history
            // Career history companies are already pulled from positions
            // But CV content could have more — flag for manual review
            const cvDocs = docs.data.filter(d => {
              const name = (d.name || d.filename || '').toLowerCase();
              return name.includes('cv') || name.includes('resume') || name.includes('curriculum');
            });
            if (cvDocs.length > 0) {
              enrichResults.ezekia_cv = {
                found: cvDocs.length,
                files: cvDocs.map(d => ({ name: d.name || d.filename, id: d.id })),
                message: 'CV files available — download for company extraction if needed'
              };
            }
          }
        } catch (e) {
          // Non-fatal — aspirations/status/docs may not be available
        }

        // Extract companies from career history positions (already pulled)
        if (enrichResults.ezekia_profile?.updated_fields?.includes('career_history') || person.career_history) {
          try {
            const career = JSON.parse(person.career_history || enrichResults.ezekia_profile?.career_raw || '[]');
            const companyNames = [...new Set(career.map(c => c.company).filter(Boolean))];
            let companiesCreated = 0;
            for (const name of companyNames) {
              if (!name || name.length < 2) continue;
              const { rows: exists } = await db.query('SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [name, req.tenant_id]);
              if (!exists.length) {
                await db.query('INSERT INTO companies (name, source, tenant_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [name, 'ezekia_career', req.tenant_id]);
                companiesCreated++;
              }
            }
            if (companiesCreated > 0) enrichResults.companies_from_career = { created: companiesCreated, total: companyNames.length };
          } catch (e) { /* career history parse error */ }
        }

        // ── Write-back: push Signals intelligence to Ezekia ──
        try {
          // Gather any intelligence we have that Ezekia doesn't
          const { rows: recentSignals } = await db.query(`
            SELECT se.signal_type, se.evidence_summary, se.company_name, se.detected_at, se.source_url
            FROM signal_events se
            JOIN companies c ON c.id = se.company_id
            WHERE c.name ILIKE $1 AND se.detected_at > NOW() - INTERVAL '30 days'
            ORDER BY se.detected_at DESC LIMIT 5
          `, ['%' + (person.current_company_name || 'NONE') + '%']).catch(() => ({ rows: [] }));

          const { rows: recentInteractions } = await db.query(`
            SELECT COUNT(*) AS cnt, MAX(interaction_at) AS last_at
            FROM interactions WHERE person_id = $1 AND tenant_id = $2 AND interaction_at > NOW() - INTERVAL '90 days'
          `, [req.params.id, req.tenant_id]).catch(() => ({ rows: [{}] }));

          // Build intel note with links back to Autonodal dossiers and signal sources
          const baseUrl = process.env.APP_URL || `https://${req.get('host')}`;
          const personUrl = `${baseUrl}/person.html?id=${req.params.id}`;
          const companyUrl = person.current_company_id ? `${baseUrl}/company.html?id=${person.current_company_id}` : null;

          const signalLines = recentSignals.map(s => {
            const typeLabel = s.signal_type.replace(/_/g, ' ');
            const summary = (s.evidence_summary || s.company_name || '').slice(0, 120);
            const date = new Date(s.detected_at).toLocaleDateString();
            const sourceLink = s.source_url ? ` (<a href="${s.source_url}" target="_blank">source</a>)` : '';
            return `<li><strong>${typeLabel}</strong>: ${summary} — ${date}${sourceLink}</li>`;
          });
          const ix = recentInteractions[0];

          if (signalLines.length > 0 || parseInt(ix?.cnt) > 0) {
            let noteBody = `<p><strong>Autonodal Signal Intelligence</strong> — <a href="${personUrl}">View full dossier</a></p>`;
            if (companyUrl) noteBody += `<p>Company: <a href="${companyUrl}">${person.current_company_name}</a></p>`;
            if (signalLines.length) noteBody += `<p>Recent signals:</p><ul>${signalLines.join('')}</ul>`;
            if (parseInt(ix?.cnt) > 0) noteBody += `<p>Interaction activity: <strong>${ix.cnt}</strong> touchpoints in 90 days (last: ${ix.last_at ? new Date(ix.last_at).toLocaleDateString() : 'unknown'})</p>`;
            noteBody += `<p style="font-size:11px;color:#888;">Auto-generated by <a href="${baseUrl}">Autonodal</a> — ${new Date().toLocaleDateString()}</p>`;

            await ezekia.addPersonNote(parseInt(ezekiaId), noteBody, {
              subject: 'Autonodal Intel Update - ' + new Date().toLocaleDateString()
            });
            enrichResults.ezekia_writeback = { note_pushed: true, signals: signalLines.length, interactions: parseInt(ix?.cnt) || 0 };
          } else {
            enrichResults.ezekia_writeback = { note_pushed: false, reason: 'No new intelligence to push' };
          }
        } catch (e) {
          enrichResults.ezekia_writeback = { error: e.message };
        }

        } // end if (ezekiaId)
      } catch (e) {
        console.error('Ezekia enrichment error:', e.message);
        enrichResults.ezekia_profile = { error: e.message };
      }
    } else {
      enrichResults.ezekia_profile = { message: 'EZEKIA_API_TOKEN not configured' };
    }

    // Resolve ezekiaId for projects step (may have been linked above)
    const ezekiaId = person.source_id || (await db.query('SELECT source_id FROM people WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]).then(r => r.rows[0]?.source_id));

    // 1b. Ezekia Projects API — find which projects/searches this person is in
    if (ezekiaId && process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('../lib/ezekia');
        // Search across projects for this candidate
        let projectsFound = 0;
        let searchesLinked = 0;

        // Get projects and check for this person as a candidate
        const projRes = await ezekia.getProjects({ page: 1, per_page: 100 });
        const projects = projRes?.data || [];

        for (const proj of projects.slice(0, 50)) {
          try {
            const candRes = await ezekia.getProjectCandidates(proj.id, { per_page: 200 });
            const candidates = candRes?.data || [];
            const isCandidate = candidates.some(c =>
              String(c.id) === String(ezekiaId) ||
              c.candidate?.id === parseInt(ezekiaId)
            );

            if (isCandidate) {
              projectsFound++;
              // Try to link to our searches table
              const { rows: [existingSearch] } = await db.query(
                `SELECT id FROM opportunities WHERE (code = $1 OR title ILIKE $2) AND tenant_id = $3 LIMIT 1`,
                [`ezekia_${proj.id}`, `%${proj.name}%`, req.tenant_id]
              );
              if (existingSearch) {
                // Link person as search candidate
                await db.query(`
                  INSERT INTO pipeline_contacts (search_id, person_id, status, source, added_at, tenant_id)
                  VALUES ($1, $2, 'sourced', 'ezekia_enrich', NOW(), $3)
                  ON CONFLICT DO NOTHING
                `, [existingSearch.id, req.params.id, req.tenant_id]);
                searchesLinked++;
              }
            }
          } catch (e) { /* skip individual project errors */ }
        }

        enrichResults.ezekia_projects = {
          projects_scanned: Math.min(projects.length, 50),
          found_in: projectsFound,
          searches_linked: searchesLinked,
          message: `Found in ${projectsFound} project${projectsFound !== 1 ? 's' : ''}, linked to ${searchesLinked} search${searchesLinked !== 1 ? 'es' : ''}`
        };
      } catch (e) {
        enrichResults.ezekia_projects = { error: e.message };
      }
    } else {
      enrichResults.ezekia_projects = { message: 'No CRM ID or API key' };
    }

    // 2. Gmail — search via user_google_accounts with proper token refresh
    if (person.email) {
      try {
        const { rows: gmailAccounts } = await db.query(
          `SELECT id, user_id, google_email, access_token, refresh_token, token_expires_at
           FROM user_google_accounts WHERE sync_enabled = true AND tenant_id = $2
           ORDER BY CASE WHEN user_id = $1 THEN 0 ELSE 1 END, google_email`,
          [req.user.user_id, req.tenant_id]
        ).catch(() => ({ rows: [] }));

        let newEmails = 0;

        for (const acct of gmailAccounts) {
          try {
            // Refresh token if expired
            let token = acct.access_token;
            const expires = new Date(acct.token_expires_at);
            if (expires <= new Date(Date.now() + 5 * 60 * 1000)) {
              // Token expired or expiring — refresh it
              if (acct.refresh_token && process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
                try {
                  const refreshRes = await fetch('https://oauth2.googleapis.com/token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({
                      refresh_token: acct.refresh_token,
                      client_id: process.env.GOOGLE_CLIENT_ID,
                      client_secret: process.env.GOOGLE_CLIENT_SECRET,
                      grant_type: 'refresh_token'
                    })
                  });
                  if (refreshRes.ok) {
                    const tokens = await refreshRes.json();
                    token = tokens.access_token;
                    await db.query(
                      `UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2, updated_at = NOW() WHERE id = $3`,
                      [token, new Date(Date.now() + tokens.expires_in * 1000), acct.id]
                    );
                  } else {
                    const errBody = await refreshRes.text().catch(() => '');
                    console.warn(`Gmail token refresh failed for ${acct.google_email}: ${refreshRes.status} ${errBody.slice(0, 200)}`);
                  }
                } catch (e) {
                  console.warn(`Gmail token refresh error for ${acct.google_email}:`, e.message);
                }
              }
            }

            // Search Gmail — quote the email for exact matching, 10y window for individual enrichment
            const emailQ = person.email.replace(/"/g, '');
            const searchQuery = encodeURIComponent(`from:"${emailQ}" OR to:"${emailQ}" OR cc:"${emailQ}" newer_than:10y`);
            const gmailRes = await fetch(
              `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${searchQuery}&maxResults=100`,
              { headers: { 'Authorization': `Bearer ${token}` } }
            );

            if (!gmailRes.ok) {
              const errBody = await gmailRes.text().catch(() => '');
              console.warn(`Gmail search failed for ${acct.google_email}: ${gmailRes.status} ${errBody.slice(0, 200)}`);
              continue;
            }
            const gmailData = await gmailRes.json();

            // Fetch and store new messages as interactions
            if (gmailData.messages && gmailData.messages.length > 0) {
              for (const msg of gmailData.messages.slice(0, 50)) {
                const { rows: existing } = await db.query(
                  `SELECT id FROM interactions WHERE person_id = $1 AND external_id = $2 AND tenant_id = $3`,
                  [req.params.id, msg.id, req.tenant_id]
                );
                if (existing.length > 0) continue;

                try {
                  const msgRes = await fetch(
                    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=Subject&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Cc&metadataHeaders=Date`,
                    { headers: { 'Authorization': `Bearer ${token}` } }
                  );
                  if (!msgRes.ok) continue;
                  const msgDetail = await msgRes.json();

                  const headers = msgDetail.payload?.headers || [];
                  const subject = headers.find(h => h.name === 'Subject')?.value || '';
                  const from = headers.find(h => h.name === 'From')?.value || '';
                  const to = headers.find(h => h.name === 'To')?.value || '';
                  const cc = headers.find(h => h.name === 'Cc')?.value || '';
                  const dateStr = headers.find(h => h.name === 'Date')?.value;

                  // Validate this message actually involves the person — Gmail search can return false positives
                  const allRecipients = `${from} ${to} ${cc}`.toLowerCase();
                  if (!allRecipients.includes(emailQ.toLowerCase())) continue;

                  const direction = from.toLowerCase().includes(emailQ.toLowerCase()) ? 'inbound' : 'outbound';

                  await db.query(`
                    INSERT INTO interactions (person_id, user_id, interaction_type, direction, subject, email_snippet,
                                              email_from, email_to, source, external_id, channel, interaction_at, created_at, tenant_id)
                    VALUES ($1, $2, 'email', $3, $4, $5, $6, $7, 'enrich_gmail', $8, 'email', $9, NOW(), $10)
                    ON CONFLICT DO NOTHING
                  `, [req.params.id, acct.user_id, direction, subject, msgDetail.snippet || '',
                      from, to || person.email, msg.id,
                      dateStr ? new Date(dateStr).toISOString() : new Date().toISOString(), req.tenant_id]);
                  newEmails++;
                } catch (e) { /* skip individual message errors */ }
              }
            }
          } catch (e) { /* skip account errors */ }
        }

        enrichResults.gmail = {
          messages_found: newEmails,
          new_stored: newEmails,
          accounts_checked: gmailAccounts.length,
          searched_email: person.email,
          message: gmailAccounts.length === 0
            ? 'No Gmail accounts connected'
            : `${newEmails} verified emails stored (${gmailAccounts.length} account${gmailAccounts.length > 1 ? 's' : ''} checked)`
        };
        if (newEmails === 0 && gmailAccounts.length > 0) {
          console.log(`Gmail enrich: 0 verified matches for ${person.email} across ${gmailAccounts.length} accounts`);
        }
      } catch (e) {
        enrichResults.gmail = { error: e.message };
      }
    } else {
      enrichResults.gmail = { message: 'No email address on file' };
    }

    // 3. Signal scan — search for recent news/signals about this person
    if (process.env.ANTHROPIC_API_KEY && (person.full_name || person.current_company_name)) {
      try {
        // Check existing external_documents for mentions
        const searchTerms = [person.full_name];
        if (person.current_company_name) searchTerms.push(person.current_company_name);

        const { rows: mentions } = await db.query(`
          SELECT ed.id, ed.title, ed.source_name, ed.published_at, ed.source_url,
                 ts_rank(to_tsvector('english', COALESCE(ed.title,'') || ' ' || COALESCE(ed.summary,'') || ' ' || COALESCE(ed.content,'')),
                         plainto_tsquery('english', $1)) AS relevance
          FROM external_documents ed
          WHERE to_tsvector('english', COALESCE(ed.title,'') || ' ' || COALESCE(ed.summary,'') || ' ' || COALESCE(ed.content,''))
                @@ plainto_tsquery('english', $1)
            AND ed.published_at > NOW() - INTERVAL '90 days'
            AND ed.tenant_id = $2
          ORDER BY relevance DESC
          LIMIT 10
        `, [person.full_name, req.tenant_id]);

        // Generate person signals from mentions via Claude
        let newSignals = 0;
        if (mentions.length > 0 && process.env.ANTHROPIC_API_KEY) {
          const mentionSummaries = mentions.map(m => `- "${m.title}" (${m.source_name}, ${m.published_at ? new Date(m.published_at).toLocaleDateString() : 'recent'})`).join('\n');

          const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
            body: JSON.stringify({
              model: 'claude-sonnet-4-20250514', max_tokens: 1024,
              system: 'Extract career signals from news mentions about a person. Return JSON array of signals: [{signal_type, title, description, confidence}]. signal_type must be one of: new_role, promotion, company_exit, board_appointment, speaking_engagement, publication, award_recognition, news_mention. Only include clear, factual signals. Return [] if no clear signals.',
              messages: [{ role: 'user', content: `Person: ${person.full_name}\nCurrent role: ${person.current_title || 'unknown'} at ${person.current_company_name || 'unknown'}\n\nRecent mentions:\n${mentionSummaries}\n\nExtract career signals. Return JSON array only.` }]
            })
          });

          if (claudeRes.ok) {
            const data = await claudeRes.json();
            const raw = data.content?.[0]?.text || '[]';
            try {
              const jsonMatch = raw.match(/\[[\s\S]*\]/);
              const signals = JSON.parse(jsonMatch ? jsonMatch[0] : '[]');
              for (const sig of signals) {
                // Avoid duplicates
                const { rows: existing } = await db.query(
                  `SELECT id FROM person_signals WHERE person_id = $1 AND signal_type = $2 AND title = $3 AND tenant_id = $4`,
                  [req.params.id, sig.signal_type, sig.title, req.tenant_id]
                );
                if (existing.length > 0) continue;

                await db.query(`
                  INSERT INTO person_signals (person_id, signal_type, title, description, confidence_score, source, detected_at, tenant_id)
                  VALUES ($1, $2, $3, $4, $5, 'enrichment', NOW(), $6)
                `, [req.params.id, sig.signal_type, sig.title, sig.description, sig.confidence || 0.7, req.tenant_id]);
                newSignals++;
              }
            } catch (e) { /* JSON parse failed */ }
          }
        }

        enrichResults.signals = { mentions_found: mentions.length, new_signals: newSignals, message: `${mentions.length} mentions scanned, ${newSignals} new signals detected` };
      } catch (e) {
        enrichResults.signals = { error: e.message };
      }
    } else {
      enrichResults.signals = { message: 'ANTHROPIC_API_KEY not configured' };
    }

    // 4. Web search — search for recent public information
    if (person.full_name && person.current_company_name) {
      try {
        // Use existing documents as a proxy for web signals
        // Also check for company signals that relate to this person's employer
        const { rows: companySignals } = await db.query(`
          SELECT signal_type, evidence_summary, confidence_score, detected_at
          FROM signal_events
          WHERE company_id = $1 AND detected_at > NOW() - INTERVAL '60 days' AND tenant_id = $2
          ORDER BY detected_at DESC LIMIT 5
        `, [person.current_company_id, req.tenant_id]).catch(() => ({ rows: [] }));

        enrichResults.web = {
          company_signals: companySignals.length,
          message: `${companySignals.length} company signals in last 60 days`
        };
      } catch (e) {
        enrichResults.web = { error: e.message };
      }
    } else {
      enrichResults.web = { message: 'Need name and company for web search' };
    }

    // 5. Re-embed the person with all enriched data
    try {
      const { rows: [latest] } = await db.query(`SELECT * FROM people WHERE id = $1 AND tenant_id = $2`, [req.params.id, req.tenant_id]);
      const parts = [latest.full_name, latest.current_title, latest.current_company_name, latest.headline, latest.bio, latest.location].filter(Boolean);
      if (latest.expertise_tags?.length) parts.push('Skills: ' + latest.expertise_tags.join(', '));
      if (latest.industries?.length) parts.push('Industries: ' + latest.industries.join(', '));

      // Get latest notes for embedding context
      const { rows: notes } = await db.query(`SELECT summary FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2 ORDER BY interaction_at DESC NULLS LAST LIMIT 5`, [req.params.id, req.tenant_id]);
      notes.forEach(n => { if (n.summary) parts.push(n.summary.slice(0, 500)); });

      // Get person signals for embedding context
      const { rows: psigs } = await db.query(`SELECT title, description FROM person_signals WHERE person_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 3`, [req.params.id, req.tenant_id]);
      psigs.forEach(s => { if (s.title) parts.push(s.title + (s.description ? ': ' + s.description.slice(0, 200) : '')); });

      if (parts.join(' ').length > 10 && process.env.QDRANT_URL) {
        const embedding = await generateQueryEmbedding(parts.join('\n'));
        const url = new URL('/collections/people/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: req.params.id, vector: embedding, payload: {
            name: latest.full_name, title: latest.current_title, company: latest.current_company_name,
            has_research_notes: notes.length > 0
          } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
            (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        await db.query('UPDATE people SET embedded_at = NOW() WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
        enrichResults.embedding = { message: 'Re-embedded with enriched data' };
      } else {
        enrichResults.embedding = { message: 'Insufficient data for embedding' };
      }
    } catch (e) {
      enrichResults.embedding = { error: e.message };
    }

    // 6. Gmail re-link — match unlinked interactions to this person by email/alt_emails
    try {
      const { rows: [freshPerson] } = await db.query(
        'SELECT email, email_alt FROM people WHERE id = $1 AND tenant_id = $2',
        [req.params.id, req.tenant_id]
      );
      const emailList = [
        freshPerson.email,
        freshPerson.email_alt
      ].filter(Boolean).map(e => e.toLowerCase().trim());

      if (emailList.length > 0) {
        const { rows: unlinked } = await db.query(`
          SELECT id FROM interactions
          WHERE person_id IS NULL
            AND tenant_id = $1
            AND (email_from = ANY($2) OR email_to = ANY($2))
          LIMIT 500
        `, [req.tenant_id, emailList]);

        if (unlinked.length > 0) {
          const ids = unlinked.map(r => r.id);
          await db.query(
            'UPDATE interactions SET person_id = $1 WHERE id = ANY($2) AND tenant_id = $3',
            [req.params.id, ids, req.tenant_id]
          );
          enrichResults.gmail_linked = { count: ids.length, message: `Linked ${ids.length} existing email interactions` };
        } else {
          enrichResults.gmail_linked = { count: 0, message: 'No unlinked email interactions found' };
        }
      } else {
        enrichResults.gmail_linked = { count: 0, message: 'No email addresses on file' };
      }
    } catch (e) {
      enrichResults.gmail_linked = { error: e.message };
    }

    // 7. Update team_proximity for this person — compute from all interaction types
    try {
      var tpCreated = 0;
      var { rows: interactingUsers } = await db.query(
        'SELECT DISTINCT user_id FROM interactions WHERE person_id = $1 AND user_id IS NOT NULL AND tenant_id = $2',
        [req.params.id, req.tenant_id]
      );
      for (var iu of interactingUsers) {
        for (var ixType of [
          { types: ['email', 'email_sent', 'email_received'], rtype: 'email', source: 'enrich_gmail', thresholds: [10, 3] },
          { types: ['research_note', 'note'], rtype: 'research_note', source: 'ezekia', thresholds: [20, 5] },
          { types: ['linkedin_message'], rtype: 'linkedin_message', source: 'linkedin_import', thresholds: [10, 3] },
          { types: ['meeting'], rtype: 'meeting', source: 'gcal_sync', thresholds: [5, 2] },
        ]) {
          var { rows: [ct] } = await db.query(
            'SELECT COUNT(*) AS cnt, MAX(interaction_at) AS latest FROM interactions WHERE person_id = $1 AND user_id = $2 AND interaction_type = ANY($3)',
            [req.params.id, iu.user_id, ixType.types]
          );
          var cnt = parseInt(ct.cnt);
          if (cnt === 0) continue;
          var strength = cnt >= ixType.thresholds[0] ? 0.85 : cnt >= ixType.thresholds[1] ? 0.60 : 0.30;
          if (ixType.rtype === 'research_note') strength = cnt >= 20 ? 0.90 : cnt >= 10 ? 0.80 : cnt >= 5 ? 0.65 : cnt >= 2 ? 0.45 : 0.25;
          await db.query(`
            INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, interaction_count, last_interaction_date, tenant_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
              interaction_count = EXCLUDED.interaction_count,
              relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
              last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
              updated_at = NOW()
          `, [req.params.id, iu.user_id, ixType.rtype, strength, ixType.source, cnt, ct.latest, req.tenant_id]);
          tpCreated++;
        }
      }
      enrichResults.proximity = { updated: tpCreated, users: interactingUsers.length };
    } catch (e) {
      enrichResults.proximity = { error: e.message };
    }

    console.log(`Person enrich ${person.full_name}: ${JSON.stringify(Object.keys(enrichResults).map(k => k + '=' + (enrichResults[k]?.error || enrichResults[k]?.message || enrichResults[k]?.updated_fields?.join(',') || 'ok')))}`);
    res.json({ person_id: req.params.id, person_name: person.full_name, results: enrichResults });
  } catch (err) {
    console.error('Enrich error:', err.message);
    res.status(500).json({ error: 'Enrichment failed: ' + err.message });
  }
});

// ─── Google News Search Enrichment ───

router.post('/api/people/:id/search-enrich', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { enrichPersonFromSearch } = require('../lib/search-enrichment');
    const result = await enrichPersonFromSearch(db, req.params.id, req.tenant_id);
    res.json(result);
  } catch (err) {
    console.error('Search enrich error:', err.message);
    res.status(500).json({ error: 'Search enrichment failed: ' + err.message });
  }
});

router.get('/api/people/:id/matches', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT sm.search_id, sm.overall_match_score AS match_score,
             sm.match_reasons, sm.status,
             o.title AS search_title, o.status AS search_status,
             o.location, o.seniority_level
      FROM search_matches sm
      JOIN opportunities o ON o.id = sm.search_id
      WHERE sm.person_id = $1 AND sm.tenant_id = $2
      ORDER BY sm.overall_match_score DESC LIMIT 10
    `, [req.params.id, req.tenant_id]);
    res.json({ matches: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/people/:id/activities', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT a.*, u.name AS actor_name FROM activities a
      LEFT JOIN users u ON u.id = a.user_id
      WHERE a.tenant_id = $1 AND (
        a.person_id = $2
        OR a.opportunity_id IN (SELECT pc.search_id FROM pipeline_contacts pc WHERE pc.person_id = $2)
        OR a.company_id = (SELECT current_company_id FROM people WHERE id = $2)
      )
      ORDER BY a.activity_at DESC LIMIT 50
    `, [req.tenant_id, req.params.id]);
    res.json({ activities: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/people/:id/publications — check if a person has academic publications
// PLATFORM-CONTEXT: publications collection is global, not tenant-isolated
router.get('/api/people/:id/publications', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [person] } = await db.query(
      'SELECT full_name, linkedin_url, email FROM people WHERE id = $1 AND tenant_id = $2',
      [req.params.id, req.tenant_id]
    );
    if (!person) return res.status(404).json({ error: 'Person not found' });

    if (!searchPublications || !generateQueryEmbedding) {
      return res.json({ person_id: req.params.id, person_name: person.full_name, has_publications: false, papers: [] });
    }

    // Search publications by person name
    const embedding = await generateQueryEmbedding('author researcher: ' + person.full_name);
    const results = await searchPublications(embedding, { limit: 20, scoreThreshold: 0.40 });

    // Filter results where the person's last name appears in the author field
    const lastName = person.full_name.split(' ').pop().toLowerCase();
    const papers = results.filter(r => {
      const authorStr = (r.authors_full || r.authors || '').toLowerCase();
      return authorStr.includes(lastName);
    }).map(r => ({
      id: r.id,
      title: r.title,
      year: r.year,
      source: r.source,
      abstract: r.abstract,
      keywords: r.subjects || [],
      score: r.match_score,
      doi_url: r.doi_url,
      url: r.url,
      co_authors: (r.authors_full || r.authors || '').split(/[,;]\s*/).filter(a =>
        !a.toLowerCase().includes(lastName)
      ).slice(0, 5),
    }));

    res.json({
      person_id: req.params.id,
      person_name: person.full_name,
      publication_count: papers.length,
      has_publications: papers.length > 0,
      papers,
    });
  } catch (err) {
    console.error('Person publications error:', err.message);
    res.json({ person_id: req.params.id, has_publications: false, papers: [] });
  }
});

  return router;
};
