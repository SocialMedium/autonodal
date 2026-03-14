#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// MitchelLake Signal Intelligence Platform
// server.js — Express API Server
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const https = require('https');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE
// ─────────────────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// ─────────────────────────────────────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// CORS for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ─────────────────────────────────────────────────────────────────────────────
// AUTH MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

async function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.replace('Bearer ', '');

  if (!token) return res.status(401).json({ error: 'No token provided' });

  try {
    const { rows } = await pool.query(
      `SELECT s.user_id, u.email, u.name, u.role
       FROM sessions s JOIN users u ON s.user_id = u.id
       WHERE s.token = $1 AND s.expires_at > NOW()`,
      [token]
    );

    if (rows.length === 0) return res.status(401).json({ error: 'Invalid or expired token' });

    req.user = rows[0];
    next();
  } catch (err) {
    console.error('Auth error:', err.message);
    res.status(500).json({ error: 'Auth check failed' });
  }
}

// Optional auth — doesn't block, just attaches user if token valid
async function optionalAuth(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.replace('Bearer ', '');
  if (token) {
    try {
      const { rows } = await pool.query(
        `SELECT s.user_id, u.email, u.name, u.role
         FROM sessions s JOIN users u ON s.user_id = u.id
         WHERE s.token = $1 AND s.expires_at > NOW()`,
        [token]
      );
      if (rows.length > 0) req.user = rows[0];
    } catch (e) { /* ignore */ }
  }
  next();
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUTH ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

// Google OAuth — initiate
app.get('/api/auth/google', (req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });
  const redirectUri = process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
  const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    redirect_uri: redirectUri,
    response_type: 'code',
    scope: 'openid email profile',
    hd: 'mitchellake.com',
    prompt: 'select_account',
    access_type: 'offline'
  });
  res.redirect(authUrl);
});

// Google OAuth — callback
app.get('/api/auth/google/callback', async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.redirect('/?auth_error=' + encodeURIComponent(error));
  if (!code) return res.redirect('/?auth_error=no_code');

  try {
    const redirectUri = process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;

    // Exchange code for tokens
    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        code,
        client_id: process.env.GOOGLE_CLIENT_ID,
        client_secret: process.env.GOOGLE_CLIENT_SECRET,
        redirect_uri: redirectUri,
        grant_type: 'authorization_code'
      })
    });

    if (!tokenRes.ok) {
      const err = await tokenRes.text();
      console.error('Google token exchange failed:', err);
      return res.redirect('/?auth_error=token_exchange_failed');
    }

    const tokenData = await tokenRes.json();

    // Get user info
    const userInfoRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const userInfo = await userInfoRes.json();

    // Enforce mitchellake.com domain
    if (!userInfo.email || !userInfo.email.endsWith('@mitchellake.com')) {
      return res.redirect('/?auth_error=domain_restricted');
    }

    // Find or create user
    let { rows } = await pool.query('SELECT id, email, name, role FROM users WHERE email = $1', [userInfo.email]);
    let user;

    if (rows.length > 0) {
      user = rows[0];
      // Update name/avatar if changed
      await pool.query(
        'UPDATE users SET name = COALESCE(NULLIF($1, \'\'), name), updated_at = NOW() WHERE id = $2',
        [userInfo.name, user.id]
      );
    } else {
      // Auto-create MitchelLake team member
      const { rows: [newUser] } = await pool.query(
        `INSERT INTO users (id, email, name, role, created_at, updated_at)
         VALUES (gen_random_uuid(), $1, $2, 'consultant', NOW(), NOW())
         RETURNING id, email, name, role`,
        [userInfo.email, userInfo.name || userInfo.email.split('@')[0]]
      );
      user = newUser;
      console.log(`✅ New team member created: ${user.name} (${user.email})`);
    }

    // Create session
    const sessionToken = crypto.randomBytes(48).toString('hex');
    await pool.query(
      `INSERT INTO sessions (id, user_id, token, expires_at, created_at)
       VALUES (gen_random_uuid(), $1, $2, NOW() + INTERVAL '30 days', NOW())`,
      [user.id, sessionToken]
    );

    // Clean up expired sessions
    await pool.query('DELETE FROM sessions WHERE expires_at < NOW()');

    // Redirect to app with token (frontend picks it up from URL)
    res.redirect(`/?token=${sessionToken}&user=${encodeURIComponent(JSON.stringify({ id: user.id, email: user.email, name: user.name, role: user.role }))}`);
  } catch (err) {
    console.error('Google auth error:', err);
    res.redirect('/?auth_error=server_error');
  }
});

app.get('/api/auth/me', authenticateToken, (req, res) => {
  res.json({ user: req.user });
});

app.post('/api/auth/logout', authenticateToken, async (req, res) => {
  try {
    const token = req.headers.authorization.replace('Bearer ', '');
    await pool.query('DELETE FROM sessions WHERE token = $1', [token]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: 'Logout failed' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// DASHBOARD STATS
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/stats', authenticateToken, async (req, res) => {
  try {
    const [
      people, signals24h, signalsTotal, companies,
      documents, placements, activeSources,
      peopleWithNotes, signalsByType, docsByType
    ] = await Promise.all([
      pool.query('SELECT COUNT(*) AS cnt FROM people'),
      pool.query(`SELECT COUNT(*) AS cnt FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours'`),
      pool.query('SELECT COUNT(*) AS cnt FROM signal_events'),
      pool.query('SELECT COUNT(*) AS cnt FROM companies'),
      pool.query('SELECT COUNT(*) AS cnt FROM external_documents'),
      pool.query('SELECT COUNT(*) AS cnt, COALESCE(SUM(placement_fee), 0) AS total_fees FROM placements'),
      pool.query('SELECT COUNT(*) AS cnt FROM rss_sources WHERE enabled = true'),
      pool.query(`SELECT COUNT(DISTINCT person_id) AS cnt FROM interactions WHERE interaction_type = 'research_note'`),
      pool.query(`SELECT signal_type, COUNT(*) AS cnt FROM signal_events GROUP BY signal_type ORDER BY cnt DESC`),
      pool.query(`SELECT source_type, COUNT(*) AS cnt FROM external_documents GROUP BY source_type ORDER BY cnt DESC`),
    ]);

    res.json({
      people_count: parseInt(people.rows[0].cnt),
      signals_24h: parseInt(signals24h.rows[0].cnt),
      signals_total: parseInt(signalsTotal.rows[0].cnt),
      companies_count: parseInt(companies.rows[0].cnt),
      documents_count: parseInt(documents.rows[0].cnt),
      placements_count: parseInt(placements.rows[0].cnt),
      placements_total_fees: parseFloat(placements.rows[0].total_fees),
      sources_active: parseInt(activeSources.rows[0].cnt),
      people_with_notes: parseInt(peopleWithNotes.rows[0].cnt),
      signals_by_type: signalsByType.rows.map(r => ({ type: r.signal_type, count: parseInt(r.cnt) })),
      documents_by_type: docsByType.rows.map(r => ({ type: r.source_type, count: parseInt(r.cnt) })),
    });
  } catch (err) {
    console.error('Stats error:', err.message);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNALS
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/signals/brief', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const type = req.query.type;
    const status = req.query.status;
    const category = req.query.category;
    const minConf = parseFloat(req.query.min_confidence) || 0;

    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 0;

    if (type) {
      paramIdx++;
      where += ` AND se.signal_type = $${paramIdx}::signal_type`;
      params.push(type);
    }
    if (status) {
      paramIdx++;
      where += ` AND se.triage_status = $${paramIdx}::triage_status`;
      params.push(status);
    }
    if (category) {
      paramIdx++;
      where += ` AND se.signal_category = $${paramIdx}`;
      params.push(category);
    }
    if (minConf > 0) {
      paramIdx++;
      where += ` AND se.confidence_score >= $${paramIdx}`;
      params.push(minConf);
    }

    paramIdx++;
    const limitParam = paramIdx;
    params.push(limit);
    paramIdx++;
    const offsetParam = paramIdx;
    params.push(offset);

    const [signalsResult, countResult] = await Promise.all([
      pool.query(`
        SELECT se.id, se.signal_type, se.company_name, se.confidence_score,
               se.evidence_summary, se.evidence_snippet, se.triage_status,
               se.detected_at, se.signal_date, se.source_url, se.signal_category,
               se.hiring_implications,
               c.sector, c.geography, c.is_client,
               ed.source_name, ed.source_type AS doc_source_type,
               ed.title AS doc_title, ed.summary AS doc_summary
        FROM signal_events se
        LEFT JOIN companies c ON se.company_id = c.id
        LEFT JOIN external_documents ed ON se.source_document_id = ed.id
        ${where}
        ORDER BY se.confidence_score DESC NULLS LAST, se.detected_at DESC NULLS LAST
        LIMIT $${limitParam} OFFSET $${offsetParam}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM signal_events se ${where}`, params.slice(0, -2)),
    ]);

    res.json({
      signals: signalsResult.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
    });
  } catch (err) {
    console.error('Signals brief error:', err.message);
    res.status(500).json({ error: 'Failed to fetch signals' });
  }
});

app.get('/api/signals/:id', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT se.*, c.name AS company_name_full, c.sector, c.geography,
             c.description AS company_description, c.is_client,
             ed.title AS doc_title, ed.source_name, ed.source_url AS doc_url,
             ed.content AS doc_content
      FROM signal_events se
      LEFT JOIN companies c ON se.company_id = c.id
      LEFT JOIN external_documents ed ON se.source_document_id = ed.id
      WHERE se.id = $1
    `, [req.params.id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Signal detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch signal' });
  }
});

app.patch('/api/signals/:id/triage', authenticateToken, async (req, res) => {
  try {
    const { status, notes } = req.body;
    const validStatuses = ['new', 'reviewing', 'qualified', 'irrelevant', 'actioned'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: `Invalid status. Must be one of: ${validStatuses.join(', ')}` });
    }

    const { rows } = await pool.query(`
      UPDATE signal_events
      SET triage_status = $1::triage_status,
          triage_notes = COALESCE($2, triage_notes),
          triaged_by = $3,
          triaged_at = NOW(),
          updated_at = NOW()
      WHERE id = $4
      RETURNING id, triage_status, triaged_at
    `, [status, notes, req.user.user_id, req.params.id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Triage error:', err.message);
    res.status(500).json({ error: 'Failed to update triage' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// PEOPLE
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/people', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 0;

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
      where += ` AND p.id IN (SELECT DISTINCT person_id FROM interactions WHERE interaction_type = 'research_note')`;
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

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const [peopleResult, countResult] = await Promise.all([
      pool.query(`
        SELECT p.id, p.full_name, p.current_title, p.current_company_name,
               p.headline, p.location, p.source, p.seniority_level,
               p.expertise_tags, p.industries, p.email, p.linkedin_url,
               p.functional_area, p.embedded_at IS NOT NULL AS is_embedded,
               (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count
        FROM people p
        ${where}
        ORDER BY
          CASE WHEN p.current_title IS NOT NULL THEN 0 ELSE 1 END,
          note_count DESC,
          p.full_name
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM people p ${where}`, params.slice(0, -2)),
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

app.get('/api/people/:id', authenticateToken, async (req, res) => {
  try {
    // Guard against invalid UUIDs (e.g. "null", "undefined", empty)
    const id = req.params.id;
    if (!id || id === 'null' || id === 'undefined' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
      return res.status(400).json({ error: 'Invalid person ID' });
    }
    const { rows: [person] } = await pool.query(`
      SELECT p.*, c.name AS company_name_full, c.sector AS company_sector,
             c.geography AS company_geography, c.id AS company_id_linked,
             c.domain AS company_domain, c.is_client AS company_is_client
      FROM people p
      LEFT JOIN companies c ON p.current_company_id = c.id
      WHERE p.id = $1
    `, [req.params.id]);

    if (!person) return res.status(404).json({ error: 'Person not found' });

    // Research notes
    const { rows: notes } = await pool.query(`
      SELECT id, summary, subject, email_snippet, interaction_at, created_at,
             note_quality, extracted_intelligence, source, interaction_type
      FROM interactions
      WHERE person_id = $1 AND interaction_type = 'research_note'
      ORDER BY interaction_at DESC NULLS LAST
      LIMIT 50
    `, [req.params.id]);

    // All other interactions (emails, calls, meetings)
    const { rows: interactions } = await pool.query(`
      SELECT id, interaction_type, summary, subject, email_snippet,
             interaction_at, created_at, channel, direction, source
      FROM interactions
      WHERE person_id = $1 AND interaction_type != 'research_note'
      ORDER BY interaction_at DESC NULLS LAST
      LIMIT 30
    `, [req.params.id]);

    // Person signals
    const { rows: signals } = await pool.query(`
      SELECT id, signal_type, signal_category, title, description,
             confidence_score, signal_date, detected_at
      FROM person_signals
      WHERE person_id = $1
      ORDER BY detected_at DESC
    `, [req.params.id]);

    // Company signals (if person has a linked company)
    let companySignals = [];
    if (person.current_company_id) {
      const { rows } = await pool.query(`
        SELECT id, signal_type, confidence_score, evidence_summary, detected_at, triage_status
        FROM signal_events WHERE company_id = $1
        ORDER BY detected_at DESC LIMIT 10
      `, [person.current_company_id]);
      companySignals = rows;
    }

    // Interaction stats
    const { rows: [stats] } = await pool.query(`
      SELECT COUNT(*) AS total,
             COUNT(*) FILTER (WHERE interaction_type = 'research_note') AS notes,
             COUNT(*) FILTER (WHERE interaction_type = 'email') AS emails,
             COUNT(*) FILTER (WHERE interaction_type = 'call') AS calls,
             COUNT(*) FILTER (WHERE interaction_type = 'meeting') AS meetings,
             MIN(interaction_at) AS first_interaction,
             MAX(interaction_at) AS last_interaction
      FROM interactions WHERE person_id = $1
    `, [req.params.id]);

    // Colleagues at same company
    let colleagues = [];
    if (person.current_company_id) {
      const { rows } = await pool.query(`
        SELECT id, full_name, current_title, seniority_level
        FROM people
        WHERE current_company_id = $1 AND id != $2
        ORDER BY full_name LIMIT 20
      `, [person.current_company_id, req.params.id]);
      colleagues = rows;
    }

    res.json({
      ...person,
      research_notes: notes,
      interactions,
      person_signals: signals,
      company_signals: companySignals,
      interaction_stats: stats,
      colleagues,
    });
  } catch (err) {
    console.error('Person detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch person' });
  }
});

app.get('/api/people/:id/notes', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT id, summary, subject, interaction_at, created_at, note_quality,
             extracted_intelligence, source
      FROM interactions
      WHERE person_id = $1 AND interaction_type = 'research_note'
      ORDER BY interaction_at DESC NULLS LAST
    `, [req.params.id]);

    res.json({ notes: rows });
  } catch (err) {
    console.error('Person notes error:', err.message);
    res.status(500).json({ error: 'Failed to fetch notes' });
  }
});

// ─── Person Enrichment ───
app.post('/api/people/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const { rows: [person] } = await pool.query(
      `SELECT id, full_name, email, external_id, current_company_name, current_company_id FROM people WHERE id = $1`, [req.params.id]
    );
    if (!person) return res.status(404).json({ error: 'Person not found' });

    const enrichResults = { ezekia: null, gmail: null, embedding: null };

    // 1. Try Ezekia if we have external_id
    if (person.external_id && process.env.EZEKIA_API_KEY) {
      try {
        const ezRes = await new Promise((resolve, reject) => {
          const req = https.request({
            hostname: process.env.EZEKIA_HOST || 'app.ezekia.com',
            path: `/api/v1/candidates/${person.external_id}?fields[]=profile.positions&fields[]=relationships.billings`,
            method: 'GET',
            headers: { 'Authorization': `Bearer ${process.env.EZEKIA_API_KEY}`, 'Accept': 'application/json' },
            timeout: 15000,
          }, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch (e) { reject(e); } });
          });
          req.on('error', reject);
          req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
          req.end();
        });

        // Update person with latest Ezekia data
        if (ezRes && ezRes.data) {
          const d = ezRes.data;
          const updates = {};
          if (d.headline) updates.headline = d.headline;
          if (d.current_title || (d.profile?.positions?.[0]?.title)) updates.current_title = d.current_title || d.profile.positions[0].title;
          if (d.current_company || (d.profile?.positions?.[0]?.company)) updates.current_company_name = d.current_company || d.profile.positions[0].company;
          if (d.email) updates.email = d.email;
          if (d.phone) updates.phone = d.phone;
          if (d.linkedin_url) updates.linkedin_url = d.linkedin_url;
          if (d.location) updates.location = d.location;

          if (Object.keys(updates).length > 0) {
            const setClauses = Object.entries(updates).map(([k, v], i) => `${k} = $${i + 2}`);
            const vals = [req.params.id, ...Object.values(updates)];
            await pool.query(`UPDATE people SET ${setClauses.join(', ')}, updated_at = NOW() WHERE id = $1`, vals);
            enrichResults.ezekia = { updated_fields: Object.keys(updates) };
          } else {
            enrichResults.ezekia = { message: 'No new data from Ezekia' };
          }
        }
      } catch (e) {
        enrichResults.ezekia = { error: e.message };
      }
    } else {
      enrichResults.ezekia = { message: person.external_id ? 'EZEKIA_API_KEY not configured' : 'No Ezekia ID linked' };
    }

    // 2. Try Gmail search if configured and person has email
    if (person.email && process.env.GOOGLE_ACCESS_TOKEN) {
      try {
        // Search Gmail for recent emails to/from this person
        const searchQuery = encodeURIComponent(`from:${person.email} OR to:${person.email} newer_than:90d`);
        const gmailRes = await new Promise((resolve, reject) => {
          const req = https.request({
            hostname: 'gmail.googleapis.com',
            path: `/gmail/v1/users/me/messages?q=${searchQuery}&maxResults=5`,
            method: 'GET',
            headers: { 'Authorization': `Bearer ${process.env.GOOGLE_ACCESS_TOKEN}` },
            timeout: 10000,
          }, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch (e) { reject(e); } });
          });
          req.on('error', reject);
          req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
          req.end();
        });

        const msgCount = gmailRes.resultSizeEstimate || 0;
        enrichResults.gmail = { messages_found: msgCount, message: `${msgCount} emails in last 90 days` };
      } catch (e) {
        enrichResults.gmail = { error: e.message };
      }
    } else {
      enrichResults.gmail = { message: person.email ? 'Google OAuth not configured' : 'No email address' };
    }

    // 3. Re-embed the person
    try {
      const { rows: [latest] } = await pool.query(`SELECT * FROM people WHERE id = $1`, [req.params.id]);
      const parts = [latest.full_name, latest.current_title, latest.current_company_name, latest.headline, latest.bio, latest.location].filter(Boolean);
      if (latest.expertise_tags?.length) parts.push('Skills: ' + latest.expertise_tags.join(', '));

      // Get latest notes for embedding
      const { rows: notes } = await pool.query(`SELECT summary FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' ORDER BY interaction_at DESC NULLS LAST LIMIT 3`, [req.params.id]);
      notes.forEach(n => { if (n.summary) parts.push(n.summary.slice(0, 500)); });

      if (parts.join(' ').length > 10) {
        const embedding = await generateQueryEmbedding(parts.join('\n'));
        // Upsert to Qdrant
        const url = new URL('/collections/people/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: req.params.id, vector: embedding, payload: { name: latest.full_name, title: latest.current_title, company: latest.current_company_name } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT', headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
            (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        await pool.query('UPDATE people SET embedded_at = NOW() WHERE id = $1', [req.params.id]);
        enrichResults.embedding = { message: 'Re-embedded successfully' };
      }
    } catch (e) {
      enrichResults.embedding = { error: e.message };
    }

    res.json({ person_id: req.params.id, person_name: person.full_name, results: enrichResults });
  } catch (err) {
    console.error('Enrich error:', err.message);
    res.status(500).json({ error: 'Enrichment failed: ' + err.message });
  }
});

// ─── Company Enrichment ───
app.post('/api/companies/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const { rows: [company] } = await pool.query('SELECT * FROM companies WHERE id = $1', [req.params.id]);
    if (!company) return res.status(404).json({ error: 'Company not found' });

    const enrichResults = {};

    // Re-embed with latest data
    try {
      const parts = [company.name, company.sector, company.geography, company.description, company.domain].filter(Boolean);

      const { rows: signals } = await pool.query(`SELECT evidence_summary FROM signal_events WHERE company_id = $1 AND evidence_summary IS NOT NULL ORDER BY detected_at DESC LIMIT 5`, [req.params.id]);
      signals.forEach(s => parts.push(s.evidence_summary));

      const { rows: people } = await pool.query(`SELECT full_name, current_title FROM people WHERE current_company_id = $1 AND current_title IS NOT NULL LIMIT 10`, [req.params.id]);
      if (people.length) parts.push('Key people: ' + people.map(p => `${p.full_name} — ${p.current_title}`).join(', '));

      if (parts.join(' ').length > 10) {
        const embedding = await generateQueryEmbedding(parts.join('\n'));
        const url = new URL('/collections/companies/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: req.params.id, vector: embedding, payload: { name: company.name, sector: company.sector, is_client: company.is_client } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT', headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
            (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        await pool.query('UPDATE companies SET embedded_at = NOW() WHERE id = $1', [req.params.id]);
        enrichResults.embedding = { message: 'Re-embedded successfully' };
      }
    } catch (e) {
      enrichResults.embedding = { error: e.message };
    }

    res.json({ company_id: req.params.id, company_name: company.name, results: enrichResults });
  } catch (err) {
    console.error('Company enrich error:', err.message);
    res.status(500).json({ error: 'Enrichment failed' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// COMPANIES
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/companies', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    // ── CLIENTS filter: query the clients table directly ──
    if (req.query.is_client === 'true') {
      let clWhere = 'WHERE 1=1';
      const clParams = [];
      let clIdx = 0;
      if (q) {
        clIdx++;
        clWhere += ` AND cl.name ILIKE $${clIdx}`;
        clParams.push(`%${q}%`);
      }
      clIdx++; clParams.push(limit);
      const clLimitIdx = clIdx;
      clIdx++; clParams.push(offset);
      const clOffsetIdx = clIdx;

      const [clientsResult, clientCountResult] = await Promise.all([
        pool.query(`
          SELECT cl.id, cl.name, cl.company_id,
                 cl.relationship_status, cl.relationship_tier,
                 co.sector, co.geography, co.domain, co.employee_count_band, co.description,
                 TRUE AS is_client,
                 COALESCE(cf.total_placements, 0) AS placement_count,
                 COALESCE(cf.total_invoiced, 0) AS total_revenue,
                 (SELECT COUNT(*) FROM people p WHERE p.current_company_id = cl.company_id) AS people_count,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = cl.company_id) AS signal_count
          FROM clients cl
          LEFT JOIN companies co ON cl.company_id = co.id
          LEFT JOIN client_financials cf ON cf.client_id = cl.id
          ${clWhere}
          ORDER BY COALESCE(cf.total_invoiced, 0) DESC, cl.name
          LIMIT $${clLimitIdx} OFFSET $${clOffsetIdx}
        `, clParams),
        pool.query(`SELECT COUNT(*) AS cnt FROM clients cl ${clWhere}`, clParams.slice(0, -2)),
      ]);

      return res.json({
        companies: clientsResult.rows,
        total: parseInt(clientCountResult.rows[0].cnt),
        limit, offset,
      });
    }

    // ── ALL / filtered companies ──
    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 0;

    // Filter out junk companies: require at least one quality signal
    if (req.query.show_all !== 'true') {
      where += ` AND (
        c.domain IS NOT NULL
        OR EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = c.id)
        OR (c.sector IS NOT NULL AND LENGTH(c.name) <= 60 AND c.name !~ '[.!?]')
      )`;
    }

    if (q) {
      paramIdx++;
      where += ` AND (c.name ILIKE $${paramIdx} OR c.sector ILIKE $${paramIdx} OR c.geography ILIKE $${paramIdx} OR c.domain ILIKE $${paramIdx})`;
      params.push(`%${q}%`);
    }
    if (req.query.sector) {
      paramIdx++;
      where += ` AND c.sector ILIKE $${paramIdx}`;
      params.push(`%${req.query.sector}%`);
    }
    if (req.query.geography) {
      paramIdx++;
      where += ` AND c.geography ILIKE $${paramIdx}`;
      params.push(`%${req.query.geography}%`);
    }

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const [companiesResult, countResult] = await Promise.all([
      pool.query(`
        SELECT c.id, c.name, c.sector, c.geography, c.domain, c.is_client,
               c.employee_count_band, c.description,
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id) AS signal_count,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count
        FROM companies c
        ${where}
        ORDER BY
          (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) DESC,
          c.name
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM companies c ${where}`, params.slice(0, -2)),
    ]);

    res.json({
      companies: companiesResult.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
    });
  } catch (err) {
    console.error('Companies error:', err.message);
    res.status(500).json({ error: 'Failed to fetch companies' });
  }
});

app.get('/api/companies/:id', authenticateToken, async (req, res) => {
  try {
    let company = null;
    let companyId = req.params.id;
    let clientRecord = null;

    // Try companies table first
    const { rows: [co] } = await pool.query('SELECT * FROM companies WHERE id = $1', [companyId]);
    if (co) {
      company = co;
    } else {
      // Maybe it's a clients table ID — resolve it
      const { rows: [cl] } = await pool.query(`
        SELECT cl.*, co.id AS resolved_company_id,
               co.sector, co.geography, co.domain, co.employee_count_band,
               co.description AS company_description, co.is_client AS co_is_client
        FROM clients cl
        LEFT JOIN companies co ON cl.company_id = co.id
        WHERE cl.id = $1
      `, [companyId]);
      if (cl && cl.resolved_company_id) {
        // Client has a linked company — use that
        const { rows: [linked] } = await pool.query('SELECT * FROM companies WHERE id = $1', [cl.resolved_company_id]);
        company = linked;
        companyId = cl.resolved_company_id;
        clientRecord = cl;
      } else if (cl) {
        // Client with no linked company — build a synthetic company record
        clientRecord = cl;
        company = {
          id: cl.id,
          name: cl.name,
          sector: cl.sector || null,
          geography: cl.geography || null,
          domain: cl.domain || null,
          is_client: true,
          description: cl.company_description || null,
          employee_count_band: cl.employee_count_band || null,
        };
      }
    }

    if (!company) return res.status(404).json({ error: 'Company not found' });

    // Get client financials if available
    let financials = null;
    try {
      const { rows: [cf] } = await pool.query(`
        SELECT cf.* FROM client_financials cf
        JOIN clients cl ON cf.client_id = cl.id
        WHERE cl.company_id = $1 OR cl.id = $1
      `, [companyId]);
      financials = cf || null;
    } catch (e) {}

    // Signals
    const { rows: signals } = await pool.query(`
      SELECT se.id, se.signal_type, se.confidence_score, se.evidence_summary,
             se.evidence_snippet, se.detected_at, se.triage_status, se.signal_category,
             se.hiring_implications, se.source_url,
             ed.title AS doc_title, ed.source_name AS doc_source
      FROM signal_events se
      LEFT JOIN external_documents ed ON se.source_document_id = ed.id
      WHERE se.company_id = $1
      ORDER BY se.detected_at DESC LIMIT 30
    `, [companyId]);

    // People at this company
    const { rows: people } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.seniority_level, p.location,
             p.expertise_tags, p.linkedin_url, p.email,
             (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count
      FROM people p WHERE p.current_company_id = $1
      ORDER BY
        CASE WHEN p.seniority_level IN ('C-Level','VP','Director') THEN 0 ELSE 1 END,
        p.full_name
      LIMIT 100
    `, [companyId]);

    // Placements at this company
    let placements = [];
    try {
      const { rows } = await pool.query(`
        SELECT pl.id, pe.full_name AS candidate_name, pl.role_title, pl.start_date,
               pl.placement_fee, pl.fee_category
        FROM placements pl
        LEFT JOIN people pe ON pl.person_id = pe.id
        LEFT JOIN clients cl ON pl.client_id = cl.id
        WHERE cl.company_id = $1 OR cl.id = $1
        ORDER BY pl.start_date DESC NULLS LAST
      `, [companyId]);
      placements = rows;
    } catch (e) { /* table may not exist */ }

    // Documents mentioning this company (may not exist yet)
    let documents = [];
    try {
      const { rows } = await pool.query(`
        SELECT ed.id, ed.title, ed.source_name, ed.source_type, ed.source_url,
               ed.published_at
        FROM external_documents ed
        JOIN document_companies dc ON dc.document_id = ed.id
        WHERE dc.company_id = $1
        ORDER BY ed.published_at DESC NULLS LAST
        LIMIT 20
      `, [companyId]);
      documents = rows;
    } catch (e) { /* table may not exist */ }

    res.json({ ...company, signals, people, placements, documents, financials });
  } catch (err) {
    console.error('Company detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch company' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SEMANTIC SEARCH
// ═══════════════════════════════════════════════════════════════════════════════

async function generateQueryEmbedding(text) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'text-embedding-3-small',
      input: text,
    });

    const req = https.request({
      hostname: 'api.openai.com',
      path: '/v1/embeddings',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 15000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          if (data.error) return reject(new Error(data.error.message));
          resolve(data.data[0].embedding);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(body);
    req.end();
  });
}

async function qdrantSearch(collection, vector, limit = 20, filter = null) {
  return new Promise((resolve, reject) => {
    const body = { vector, limit, with_payload: true };
    if (filter) body.filter = filter;

    const url = new URL(`/collections/${collection}/points/search`, process.env.QDRANT_URL);
    const req = https.request({
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'api-key': process.env.QDRANT_API_KEY,
      },
      timeout: 10000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          resolve(data.result || []);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Qdrant timeout')); });
    req.write(JSON.stringify(body));
    req.end();
  });
}

app.get('/api/search', authenticateToken, async (req, res) => {
  try {
    const q = req.query.q;
    const collection = req.query.collection || 'all'; // people, documents, all
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);

    if (!q || q.trim().length < 2) {
      return res.status(400).json({ error: 'Search query too short' });
    }

    // Generate embedding for the query
    const vector = await generateQueryEmbedding(q);

    const results = { people: [], companies: [], documents: [] };

    // Search people
    if (collection === 'people' || collection === 'all') {
      const qdrantResults = await qdrantSearch('people', vector, limit);

      if (qdrantResults.length > 0) {
        const pointIds = qdrantResults.map(r => r.id);

        const { rows: people } = await pool.query(`
          SELECT id, full_name, current_title, current_company_name, headline,
                 location, seniority_level, expertise_tags, industries, source,
                 email, linkedin_url
          FROM people WHERE id = ANY($1::uuid[])
        `, [pointIds]);

        const peopleMap = new Map(people.map(p => [p.id, p]));

        results.people = qdrantResults
          .map(r => {
            const person = peopleMap.get(r.id);
            if (!person) return null;
            return {
              ...person,
              match_score: Math.round(r.score * 100),
              has_research_notes: r.payload?.has_research_notes || false,
            };
          })
          .filter(Boolean);
      }
    }

    // Search companies
    if (collection === 'companies' || collection === 'all') {
      const compLimit = collection === 'all' ? Math.min(limit, 8) : limit;
      const qdrantResults = await qdrantSearch('companies', vector, compLimit);

      if (qdrantResults.length > 0) {
        const compIds = qdrantResults.map(r => r.id);

        const { rows: companies } = await pool.query(`
          SELECT c.id, c.name, c.sector, c.geography, c.domain, c.is_client,
                 c.employee_count_band, c.description,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id) AS signal_count,
                 (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count
          FROM companies c WHERE c.id = ANY($1::uuid[])
        `, [compIds]);

        const compMap = new Map(companies.map(c => [c.id, c]));

        results.companies = qdrantResults
          .map(r => {
            const company = compMap.get(r.id);
            if (!company) return null;
            return {
              ...company,
              match_score: Math.round(r.score * 100),
            };
          })
          .filter(Boolean);
      }
    }

    // Search documents
    if (collection === 'documents' || collection === 'all') {
      const docLimit = collection === 'all' ? Math.min(limit, 10) : limit;
      const qdrantResults = await qdrantSearch('documents', vector, docLimit);

      if (qdrantResults.length > 0) {
        const docIds = qdrantResults.map(r => r.id);

        const { rows: docs } = await pool.query(`
          SELECT id, title, source_type, source_name, source_url, author, published_at
          FROM external_documents WHERE id = ANY($1::uuid[])
        `, [docIds]);

        const docMap = new Map(docs.map(d => [d.id, d]));

        results.documents = qdrantResults
          .map(r => {
            const doc = docMap.get(r.id);
            if (!doc) return null;
            return {
              ...doc,
              match_score: Math.round(r.score * 100),
            };
          })
          .filter(Boolean);
      }
    }

    res.json({
      query: q,
      collection,
      results,
      total: results.people.length + results.companies.length + results.documents.length,
    });
  } catch (err) {
    console.error('Search error:', err.message);
    res.status(500).json({ error: 'Search failed: ' + err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// DOCUMENTS
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/documents', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const sourceType = req.query.source_type;

    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 0;

    if (sourceType) {
      paramIdx++;
      where += ` AND source_type = $${paramIdx}`;
      params.push(sourceType);
    }

    paramIdx++;
    params.push(limit);
    paramIdx++;
    params.push(offset);

    const { rows } = await pool.query(`
      SELECT id, title, source_type, source_name, source_url, author,
             published_at, processing_status, embedded_at IS NOT NULL AS is_embedded
      FROM external_documents
      ${where}
      ORDER BY published_at DESC NULLS LAST
      LIMIT $${paramIdx - 1} OFFSET $${paramIdx}
    `, params);

    const { rows: [{ cnt }] } = await pool.query(
      `SELECT COUNT(*) AS cnt FROM external_documents ${where}`,
      params.slice(0, sourceType ? 1 : 0)
    );

    res.json({ documents: rows, total: parseInt(cnt), limit, offset });
  } catch (err) {
    console.error('Documents error:', err.message);
    res.status(500).json({ error: 'Failed to fetch documents' });
  }
});

app.get('/api/documents/sources', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT rs.id, rs.name, rs.source_type, rs.url, rs.enabled,
             rs.last_fetched_at, rs.last_error, rs.consecutive_errors,
             (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_id = rs.id) AS doc_count
      FROM rss_sources rs
      ORDER BY rs.source_type, rs.name
    `);
    res.json({ sources: rows });
  } catch (err) {
    console.error('Sources error:', err.message);
    res.status(500).json({ error: 'Failed to fetch sources' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// HEALTH
// ═══════════════════════════════════════════════════════════════════════════════


app.get('/api/db-test', async (req, res) => {
  const url = (process.env.DATABASE_URL || '').replace(/:([^@]+)@/, ':***@');
  try {
    const result = await pool.query('SELECT NOW() as time, current_database() as db');
    res.json({ ok: true, url, ...result.rows[0] });
  } catch(e) {
    res.json({ ok: false, url, error: e.message, code: e.code });
  }
});
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ═══════════════════════════════════════════════════════════════════════════════
// PLACEMENTS / REVENUE
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/placements', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    let where = 'WHERE 1=1';
    const params = [];
    let paramIdx = 0;

    if (q) {
      paramIdx++;
      where += ` AND (pe.full_name ILIKE $${paramIdx} OR pl.role_title ILIKE $${paramIdx} OR cl.name ILIKE $${paramIdx})`;
      params.push(`%${q}%`);
    }
    if (req.query.company_id) {
      paramIdx++;
      where += ` AND (cl.company_id = $${paramIdx} OR cl.id = $${paramIdx})`;
      params.push(req.query.company_id);
    }
    if (req.query.year) {
      paramIdx++;
      where += ` AND EXTRACT(YEAR FROM pl.start_date) = $${paramIdx}`;
      params.push(parseInt(req.query.year));
    }

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const [placementsResult, statsResult] = await Promise.all([
      pool.query(`
        SELECT pl.id, pe.full_name AS candidate_name, pl.role_title, pl.start_date,
               pl.placement_fee, pl.fee_category, pl.fee_type, pl.invoice_number,
               cl.id AS company_id, cl.name AS company_name,
               co.sector AS company_sector
        FROM placements pl
        LEFT JOIN clients cl ON pl.client_id = cl.id
        LEFT JOIN companies co ON cl.company_id = co.id
        LEFT JOIN people pe ON pl.person_id = pe.id
        ${where}
        ORDER BY pl.start_date DESC NULLS LAST
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      pool.query(`
        SELECT COUNT(*) AS total_count,
               COALESCE(SUM(pl.placement_fee), 0) AS total_revenue,
               COUNT(DISTINCT pl.client_id) AS client_count,
               MIN(pl.start_date) AS earliest,
               MAX(pl.start_date) AS latest
        FROM placements pl
        LEFT JOIN clients cl ON pl.client_id = cl.id
        LEFT JOIN people pe ON pl.person_id = pe.id
        ${where}
      `, params.slice(0, -2)),
    ]);

    // Revenue by year
    const { rows: byYear } = await pool.query(`
      SELECT EXTRACT(YEAR FROM start_date)::int AS year,
             COUNT(*) AS count,
             COALESCE(SUM(placement_fee), 0) AS revenue
      FROM placements
      WHERE start_date IS NOT NULL
      GROUP BY year ORDER BY year DESC
    `);

    // Top clients by revenue
    const { rows: topClients } = await pool.query(`
      SELECT cl.id, cl.name, COUNT(*) AS placement_count,
             COALESCE(SUM(pl.placement_fee), 0) AS total_revenue
      FROM placements pl
      LEFT JOIN clients cl ON pl.client_id = cl.id
      GROUP BY cl.id, cl.name
      ORDER BY total_revenue DESC LIMIT 20
    `);

    const stats = statsResult.rows[0];
    res.json({
      placements: placementsResult.rows,
      total: parseInt(stats.total_count),
      total_revenue: parseFloat(stats.total_revenue),
      client_count: parseInt(stats.client_count),
      date_range: { earliest: stats.earliest, latest: stats.latest },
      by_year: byYear,
      top_clients: topClients,
      limit, offset,
    });
  } catch (err) {
    console.error('Placements error:', err.message);
    res.status(500).json({ error: 'Failed to fetch placements' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// AI CHAT CONCIERGE
// ═══════════════════════════════════════════════════════════════════════════════

const multer = require('multer');
const fsChat = require('fs');
const chatUpload = multer({ dest: '/tmp/ml-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });

const chatHistories = new Map();
const MAX_HISTORY = 40;
function getChatHistory(userId) {
  if (!chatHistories.has(userId)) chatHistories.set(userId, []);
  return chatHistories.get(userId);
}

async function callClaude(messages, tools, systemPrompt) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 4096, system: systemPrompt, messages, tools });
    const req = https.request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) },
      timeout: 60000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => { try { const d = JSON.parse(Buffer.concat(chunks).toString()); if (d.error) return reject(new Error(d.error.message)); resolve(d); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Claude timeout')); });
    req.write(body);
    req.end();
  });
}

const CHAT_SYSTEM = `You are the MitchelLake Signal Intelligence concierge — an AI assistant embedded in an executive search intelligence platform.

CAPABILITIES:
1. **Interrogate data**: Search people, companies, signals, placements, research notes. Query the live database.
2. **Ingest intelligence**: When consultants share gossip, meeting notes, or insights, extract structured data and save as research notes.
3. **Process files**: Parse uploaded CSVs and PDFs — import candidates, companies, contacts.
4. **LinkedIn imports**: Auto-detect LinkedIn Connections, Contacts, and Messages CSVs. Match against existing people, create team proximity links, store message history as interactions.
5. **Advanced analysis**: Run SQL queries for complex cross-referencing and aggregations.

CONTEXT:
- MitchelLake is a retained executive search firm (APAC, UK, global)
- Database: ~30K people, ~1K companies, ~12K documents, ~2K signals, ~500 placements
- Research notes are goldmine intel: timing, comp expectations, preferences, warm intros
- Signals: capital_raising, ma_activity, geographic_expansion, strategic_hiring, leadership_change, partnership, product_launch, layoffs

STYLE:
- Concise and actionable — consultants are busy
- Format results with names, titles, companies in clear lists
- Link to pages: [Name](/person.html?id=X), [Company](/company.html?id=X)
- Australian English
- When saving intel, confirm what was extracted
- For file imports, preview before committing
- When a LinkedIn CSV is uploaded, auto-detect the type (connections/contacts/messages) from the [LinkedIn Export Type] tag and use the appropriate import action (import_linkedin_connections or import_linkedin_messages). Always tell the user what was detected and confirm before importing.
- For LinkedIn Connections: use import_linkedin_connections — this matches against existing people by LinkedIn URL, email, and name, creates team_proximity records, and adds new people for unmatched connections.
- For LinkedIn Messages: use import_linkedin_messages — this groups messages by conversation and stores them as interaction records linked to matched people.

RULES:
- Always search before saying "I don't know"
- Extract ALL entities from intelligence (people, companies, roles, comp, timing)
- Never fabricate data — only return what the database contains
- For SQL queries, only SELECT is allowed
- ALWAYS prioritise recency — sort results by most recent interaction/note date first
- When showing candidates, include when they were last contacted and how recent the intelligence is
- Flag stale intel: notes older than 6 months should be marked as potentially outdated
- "Open to roles" intelligence from 2+ years ago is likely stale — note this to the user`;

const CHAT_TOOLS = [
  { name: 'search_people', description: 'Semantic + SQL search for people/candidates by name, title, company, location, skills.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'Search query' }, filters: { type: 'object', properties: { seniority: { type: 'string' }, has_notes: { type: 'boolean' }, company: { type: 'string' } } }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'search_companies', description: 'Search companies by name, sector, geography.', input_schema: { type: 'object', properties: { query: { type: 'string' }, filters: { type: 'object', properties: { is_client: { type: 'boolean' }, sector: { type: 'string' }, geography: { type: 'string' } } }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'get_person_detail', description: 'Full dossier for a person: notes, signals, interactions, colleagues.', input_schema: { type: 'object', properties: { person_id: { type: 'string' } }, required: ['person_id'] } },
  { name: 'get_company_detail', description: 'Full company dossier: signals, people, placements.', input_schema: { type: 'object', properties: { company_id: { type: 'string' } }, required: ['company_id'] } },
  { name: 'search_signals', description: 'Search market signals by type, category, company, confidence, time range.', input_schema: { type: 'object', properties: { signal_type: { type: 'string' }, category: { type: 'string' }, company_name: { type: 'string' }, min_confidence: { type: 'number' }, days_back: { type: 'integer', default: 30 }, limit: { type: 'integer', default: 15 } } } },
  { name: 'search_placements', description: 'Search placement history by company, role, candidate.', input_schema: { type: 'object', properties: { query: { type: 'string' }, limit: { type: 'integer', default: 20 } } } },
  { name: 'search_research_notes', description: 'Search internal research notes — comp expectations, timing, preferences.', input_schema: { type: 'object', properties: { query: { type: 'string' }, person_name: { type: 'string' }, limit: { type: 'integer', default: 10 } }, required: ['query'] } },
  { name: 'log_intelligence', description: 'Save intelligence as a research note. Extracts structured data.', input_schema: { type: 'object', properties: { person_name: { type: 'string' }, company_name: { type: 'string' }, intelligence: { type: 'string' }, subject: { type: 'string' }, extracted: { type: 'object', properties: { timing: { type: 'string' }, compensation: { type: 'string' }, location_preference: { type: 'string' }, role_interests: { type: 'string' }, constraints: { type: 'string' }, warm_intros: { type: 'string' }, sentiment: { type: 'string' } } } }, required: ['person_name', 'intelligence', 'subject'] } },
  { name: 'create_person', description: 'Create a new person record.', input_schema: { type: 'object', properties: { full_name: { type: 'string' }, current_title: { type: 'string' }, current_company_name: { type: 'string' }, email: { type: 'string' }, phone: { type: 'string' }, location: { type: 'string' }, linkedin_url: { type: 'string' }, seniority_level: { type: 'string' } }, required: ['full_name'] } },
  { name: 'process_uploaded_file', description: 'Process uploaded CSV/PDF. Actions: preview, import_people, import_companies, extract_text, import_linkedin_connections (match LinkedIn connections against DB, create team_proximity records), import_linkedin_messages (store LinkedIn message history as interactions/intel).', input_schema: { type: 'object', properties: { file_id: { type: 'string' }, action: { type: 'string', enum: ['preview', 'import_people', 'import_companies', 'extract_text', 'import_linkedin_connections', 'import_linkedin_messages'] }, column_mapping: { type: 'object' } }, required: ['file_id', 'action'] } },
  { name: 'run_sql_query', description: 'Run read-only SQL SELECT for advanced analysis.', input_schema: { type: 'object', properties: { query: { type: 'string' }, explanation: { type: 'string' } }, required: ['query', 'explanation'] } },
  { name: 'get_platform_stats', description: 'Current platform statistics.', input_schema: { type: 'object', properties: {} } },
];

async function executeTool(name, input, userId) {
  try {
    switch (name) {
      case 'search_people': {
        const { query, filters = {}, limit = 10 } = input;
        let results = [];
        try {
          const vector = await generateQueryEmbedding(query);
          const qr = await qdrantSearch('people', vector, limit * 2);
          if (qr.length) {
            const { rows } = await pool.query(`SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location, p.seniority_level, p.email, p.linkedin_url, p.headline, p.expertise_tags,
              (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
              (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS latest_note_date,
              (SELECT i.subject FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note' ORDER BY i.interaction_at DESC NULLS LAST LIMIT 1) AS latest_note_subject
              FROM people p WHERE p.id = ANY($1::uuid[])`, [qr.map(r => r.id)]);
            const map = new Map(rows.map(r => [r.id, r]));
            results = qr.map(r => ({ ...map.get(r.id), score: r.score })).filter(r => r.full_name);
          }
        } catch (e) {}
        if (results.length < 3) {
          const { rows } = await pool.query(`SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location, p.seniority_level, p.email, p.headline,
            (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
            (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS latest_note_date
            FROM people p WHERE (p.full_name ILIKE $1 OR p.current_title ILIKE $1 OR p.current_company_name ILIKE $1 OR p.headline ILIKE $1) ORDER BY p.full_name LIMIT $2`, [`%${query}%`, limit]);
          const existing = new Set(results.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) results.push(r); });
        }
        // Sort by recency — most recent notes first
        results.sort((a, b) => {
          const da = a.latest_note_date ? new Date(a.latest_note_date) : new Date(0);
          const db = b.latest_note_date ? new Date(b.latest_note_date) : new Date(0);
          return db - da;
        });
        return JSON.stringify(results.slice(0, limit));
      }
      case 'search_companies': {
        const { query, filters = {}, limit = 10 } = input;
        const { rows } = await pool.query(`SELECT c.id, c.name, c.sector, c.geography, c.domain, c.employee_count_band, c.is_client, c.description, (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count, (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id) AS signal_count FROM companies c WHERE (c.name ILIKE $1 OR c.sector ILIKE $1 OR c.geography ILIKE $1) ${filters.is_client ? 'AND c.is_client = true' : ''} ORDER BY c.is_client DESC, c.name LIMIT $2`, [`%${query}%`, limit]);
        return JSON.stringify(rows);
      }
      case 'get_person_detail': {
        const { rows: [p] } = await pool.query(`SELECT p.*, c.name AS company_name_linked, c.id AS company_id_linked FROM people p LEFT JOIN companies c ON p.current_company_id = c.id WHERE p.id = $1`, [input.person_id]);
        if (!p) return JSON.stringify({ error: 'Not found' });
        const { rows: notes } = await pool.query(`SELECT subject, summary, interaction_at, note_quality, extracted_intelligence FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' ORDER BY interaction_at DESC NULLS LAST LIMIT 10`, [input.person_id]);
        const { rows: sigs } = await pool.query(`SELECT signal_type, title, description, confidence_score FROM person_signals WHERE person_id = $1 ORDER BY detected_at DESC LIMIT 10`, [input.person_id]);
        return JSON.stringify({ ...p, research_notes: notes, person_signals: sigs });
      }
      case 'get_company_detail': {
        const { rows: [co] } = await pool.query('SELECT * FROM companies WHERE id = $1', [input.company_id]);
        if (!co) return JSON.stringify({ error: 'Not found' });
        const { rows: sigs } = await pool.query(`SELECT signal_type, evidence_summary, confidence_score, detected_at FROM signal_events WHERE company_id = $1 ORDER BY detected_at DESC LIMIT 15`, [input.company_id]);
        const { rows: ppl } = await pool.query(`SELECT id, full_name, current_title, seniority_level FROM people WHERE current_company_id = $1 ORDER BY full_name LIMIT 30`, [input.company_id]);
        let pls = []; try { const { rows } = await pool.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee FROM placements pl LEFT JOIN clients cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE cl.company_id = $1 OR cl.name ILIKE (SELECT name FROM companies WHERE id = $1) ORDER BY pl.start_date DESC`, [input.company_id]); pls = rows; } catch (e) {}
        return JSON.stringify({ ...co, signals: sigs, people: ppl, placements: pls });
      }
      case 'search_signals': {
        const { signal_type, category, company_name, min_confidence = 0.5, days_back = 30, limit = 15 } = input;
        const w = [`se.confidence_score >= ${min_confidence}`, `se.detected_at >= NOW() - INTERVAL '${days_back} days'`];
        if (signal_type) w.push(`se.signal_type = '${signal_type}'`);
        if (category) w.push(`se.signal_category = '${category}'`);
        if (company_name) w.push(`c.name ILIKE '%${company_name}%'`);
        const { rows } = await pool.query(`SELECT se.signal_type, se.signal_category, se.evidence_summary, se.confidence_score, se.detected_at, se.source_url, c.name AS company_name, c.id AS company_id FROM signal_events se LEFT JOIN companies c ON se.company_id = c.id WHERE ${w.join(' AND ')} ORDER BY se.confidence_score DESC LIMIT ${limit}`);
        return JSON.stringify(rows);
      }
      case 'search_placements': {
        const { query = '', limit = 20 } = input;
        const { rows } = await pool.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee, cl.name AS company_name, cl.id AS company_id FROM placements pl LEFT JOIN clients cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE pe.full_name ILIKE $1 OR pl.role_title ILIKE $1 OR cl.name ILIKE $1 ORDER BY pl.start_date DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit]);
        return JSON.stringify(rows);
      }
      case 'search_research_notes': {
        const { query, person_name, limit = 10 } = input;
        let extra = person_name ? ` AND p.full_name ILIKE '%${person_name}%'` : '';
        const { rows } = await pool.query(`SELECT i.subject, i.summary, i.interaction_at, i.note_quality, i.extracted_intelligence, p.full_name, p.id AS person_id, p.current_title, p.current_company_name FROM interactions i JOIN people p ON i.person_id = p.id WHERE i.interaction_type = 'research_note' AND (i.summary ILIKE $1 OR i.subject ILIKE $1)${extra} ORDER BY i.interaction_at DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit]);
        return JSON.stringify(rows);
      }
      case 'log_intelligence': {
        const { person_name, company_name, intelligence, subject, extracted = {} } = input;
        let personId;
        const { rows: ex } = await pool.query(`SELECT id FROM people WHERE full_name ILIKE $1 LIMIT 1`, [person_name]);
        if (ex.length) { personId = ex[0].id; }
        else {
          const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_company_name, source) VALUES ($1, $2, 'chat_intel') RETURNING id`, [person_name, company_name || null]);
          personId = np.id;
        }
        const { rows: [note] } = await pool.query(`INSERT INTO interactions (person_id, interaction_type, subject, summary, extracted_intelligence, source, interaction_at) VALUES ($1, 'research_note', $2, $3, $4, 'chat_concierge', NOW()) RETURNING id`, [personId, subject, intelligence, JSON.stringify(extracted)]);
        return JSON.stringify({ success: true, person_id: personId, note_id: note.id, person_name, subject, extracted, message: `Saved on ${person_name}'s record` });
      }
      case 'create_person': {
        const { full_name, current_title, current_company_name, email, phone, location, linkedin_url, seniority_level } = input;
        const { rows: dupes } = await pool.query(`SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 LIMIT 3`, [full_name]);
        if (dupes.length) return JSON.stringify({ existing_matches: dupes, message: 'Possible duplicates found' });
        let coId = null;
        if (current_company_name) { const { rows } = await pool.query(`SELECT id FROM companies WHERE name ILIKE $1 LIMIT 1`, [current_company_name]); if (rows.length) coId = rows[0].id; }
        const { rows: [p] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, current_company_id, email, phone, location, linkedin_url, seniority_level, source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'chat_concierge') RETURNING id, full_name`, [full_name, current_title||null, current_company_name||null, coId, email||null, phone||null, location||null, linkedin_url||null, seniority_level||null]);
        return JSON.stringify({ ...p, message: `Created ${full_name}` });
      }
      case 'process_uploaded_file': {
        const { file_id, action, column_mapping } = input;
        const fm = uploadedFiles.get(file_id);
        if (!fm) return JSON.stringify({ error: 'File not found or expired' });
        if (action === 'preview' || action === 'extract_text') {
          return JSON.stringify({ filename: fm.originalname, type: fm.mimetype, rows: fm.preview?.length||0, columns: fm.columns||[], preview: (fm.preview||[]).slice(0,5), text_excerpt: fm.text ? fm.text.slice(0,2000) : null });
        }
        if (action === 'import_people' && fm.preview) {
          const m = column_mapping || fm.suggestedMapping || {};
          let imported = 0, skipped = 0;
          for (const row of fm.preview) {
            const name = row[m.full_name||'Name']||row['Full Name']||row['name'];
            if (!name || name.trim().length < 2) { skipped++; continue; }
            const { rows: d } = await pool.query(`SELECT id FROM people WHERE full_name ILIKE $1 LIMIT 1`, [name.trim()]);
            if (d.length) { skipped++; continue; }
            await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, email, location, linkedin_url, source) VALUES ($1,$2,$3,$4,$5,$6,'csv_import')`,
              [name.trim(), row[m.current_title||'Title']||row['Job Title']||null, row[m.current_company_name||'Company']||row['Organization']||null, row[m.email||'Email']||null, row[m.location||'Location']||null, row[m.linkedin_url||'LinkedIn']||null]);
            imported++;
          }
          return JSON.stringify({ imported, skipped, total: fm.preview.length });
        }
        if (action === 'import_linkedin_connections' && fm.preview) {
          // Load people for matching
          const { rows: dbPeople } = await pool.query(`SELECT id, full_name, first_name, last_name, linkedin_url, current_company_name, email FROM people WHERE full_name IS NOT NULL AND full_name != ''`);
          const linkedinIndex = new Map(), nameIndex = new Map(), emailIndex = new Map();
          for (const p of dbPeople) {
            if (p.linkedin_url) { const slug = p.linkedin_url.toLowerCase().replace(/\/+$/, '').split('?')[0].match(/linkedin\.com\/in\/([^\/]+)/); if (slug) linkedinIndex.set(slug[1], p); }
            const norm = `${(p.first_name || p.full_name?.split(' ')[0] || '').toLowerCase()} ${(p.last_name || p.full_name?.split(' ').slice(1).join(' ') || '').toLowerCase()}`.trim();
            if (norm.length > 1) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
            if (p.email) emailIndex.set(p.email.toLowerCase(), p);
          }

          // Ensure tables exist
          await pool.query(`CREATE TABLE IF NOT EXISTS team_proximity (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), person_id UUID REFERENCES people(id) ON DELETE CASCADE, team_member_id UUID REFERENCES users(id), proximity_type VARCHAR(50) NOT NULL, source VARCHAR(50) NOT NULL, strength NUMERIC(3,2) DEFAULT 0.5, context TEXT, connected_at TIMESTAMPTZ, metadata JSONB DEFAULT '{}', created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(person_id, team_member_id, proximity_type, source))`);
          await pool.query(`CREATE TABLE IF NOT EXISTS linkedin_connections (id SERIAL PRIMARY KEY, team_member_id UUID REFERENCES users(id), first_name VARCHAR(255), last_name VARCHAR(255), full_name VARCHAR(255), linkedin_url TEXT, linkedin_slug VARCHAR(255), email VARCHAR(255), company VARCHAR(255), position VARCHAR(255), connected_at TIMESTAMPTZ, matched_person_id UUID REFERENCES people(id), match_method VARCHAR(50), match_confidence NUMERIC(3,2), imported_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(linkedin_slug))`);

          const stats = { total: 0, matched: 0, unmatched: 0, proximity_created: 0, new_people: 0, errors: 0 };
          for (const row of fm.preview) {
            stats.total++;
            const firstName = row['First Name'] || '';
            const lastName = row['Last Name'] || '';
            const fullName = `${firstName} ${lastName}`.trim();
            const linkedinUrl = row['URL'] || '';
            const email = row['Email Address'] || '';
            const company = row['Company'] || '';
            const position = row['Position'] || '';
            const connectedOn = row['Connected On'] ? (new Date(row['Connected On']).toISOString().slice(0, 10) || null) : null;
            if (!fullName || fullName.length < 2) continue;

            const slug = linkedinUrl ? (linkedinUrl.toLowerCase().replace(/\/+$/, '').split('?')[0].match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
            let matchedPerson = null, matchMethod = null, matchConfidence = 0;

            // Match: LinkedIn URL > Email > Name+Company > Name
            if (slug && linkedinIndex.has(slug)) { matchedPerson = linkedinIndex.get(slug); matchMethod = 'linkedin_url'; matchConfidence = 0.99; }
            if (!matchedPerson && email) { const m = emailIndex.get(email.toLowerCase()); if (m) { matchedPerson = m; matchMethod = 'email'; matchConfidence = 0.95; } }
            if (!matchedPerson) {
              const norm = `${firstName.toLowerCase()} ${lastName.toLowerCase()}`.trim();
              const cands = nameIndex.get(norm) || [];
              if (cands.length === 1) { matchedPerson = cands[0]; matchMethod = 'name_unique'; matchConfidence = 0.80; }
              else if (cands.length > 1 && company) { const cm = cands.find(p => p.current_company_name && p.current_company_name.toLowerCase().includes(company.toLowerCase())); if (cm) { matchedPerson = cm; matchMethod = 'name_company'; matchConfidence = 0.90; } }
            }

            if (matchedPerson) {
              stats.matched++;
              // Create team_proximity
              if (userId) {
                try {
                  let strength = 0.5;
                  if (connectedOn) { const yrs = (Date.now() - new Date(connectedOn).getTime()) / (365.25*24*60*60*1000); if (yrs > 5) strength = 0.8; else if (yrs > 2) strength = 0.7; else if (yrs > 1) strength = 0.6; }
                  strength = Math.min(1.0, strength + (matchConfidence - 0.5) * 0.2);
                  await pool.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at, metadata) VALUES ($1,$2,'linkedin_connection','linkedin_import',$3,$4,$5,$6) ON CONFLICT (person_id, team_member_id, proximity_type, source) DO UPDATE SET strength = GREATEST(team_proximity.strength, EXCLUDED.strength), context = EXCLUDED.context, updated_at = NOW()`, [matchedPerson.id, userId, strength.toFixed(2), `${position} @ ${company}`, connectedOn, JSON.stringify({ linkedin_url: linkedinUrl, match_method: matchMethod, match_confidence: matchConfidence })]);
                  stats.proximity_created++;
                } catch (e) { if (!e.message.includes('duplicate')) stats.errors++; }
              }
              // Update LinkedIn URL if missing
              if (linkedinUrl && !matchedPerson.linkedin_url) { try { await pool.query('UPDATE people SET linkedin_url = $1, updated_at = NOW() WHERE id = $2 AND linkedin_url IS NULL', [linkedinUrl, matchedPerson.id]); } catch (e) {} }
            } else {
              stats.unmatched++;
              // Create new person record for unmatched connections
              try {
                const { rows: dupes } = await pool.query('SELECT id FROM people WHERE full_name ILIKE $1 LIMIT 1', [fullName]);
                if (!dupes.length) {
                  const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, linkedin_url, email, source) VALUES ($1,$2,$3,$4,$5,'linkedin_import') RETURNING id`, [fullName, position || null, company || null, linkedinUrl || null, email || null]);
                  stats.new_people++;
                  // Also create proximity for new person
                  if (userId && np) {
                    try { await pool.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at) VALUES ($1,$2,'linkedin_connection','linkedin_import',0.5,$3,$4) ON CONFLICT DO NOTHING`, [np.id, userId, `${position} @ ${company}`, connectedOn]); stats.proximity_created++; } catch (e) {}
                  }
                }
              } catch (e) { stats.errors++; }
            }

            // Store in linkedin_connections table
            if (slug) {
              try { await pool.query(`INSERT INTO linkedin_connections (team_member_id, first_name, last_name, full_name, linkedin_url, linkedin_slug, email, company, position, connected_at, matched_person_id, match_method, match_confidence) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (linkedin_slug) DO UPDATE SET company = EXCLUDED.company, position = EXCLUDED.position, matched_person_id = COALESCE(EXCLUDED.matched_person_id, linkedin_connections.matched_person_id), imported_at = NOW()`, [userId, firstName, lastName, fullName, linkedinUrl, slug, email||null, company||null, position||null, connectedOn, matchedPerson?.id||null, matchMethod, matchConfidence||null]); } catch (e) {}
            }
          }
          return JSON.stringify({ ...stats, match_rate: stats.total > 0 ? `${(stats.matched / stats.total * 100).toFixed(1)}%` : '0%', message: `Imported ${stats.total} LinkedIn connections: ${stats.matched} matched to existing people, ${stats.new_people} new people created, ${stats.proximity_created} team proximity links` });
        }

        if (action === 'import_linkedin_messages' && fm.preview) {
          const stats = { total: 0, matched: 0, interactions_created: 0, unmatched_senders: new Set(), errors: 0 };

          // Load people for matching by name
          const { rows: dbPeople } = await pool.query(`SELECT id, full_name FROM people WHERE full_name IS NOT NULL`);
          const nameMap = new Map();
          for (const p of dbPeople) { nameMap.set(p.full_name.toLowerCase().trim(), p); }

          // Group messages by conversation/sender
          const conversations = new Map();
          for (const row of fm.preview) {
            const from = row['FROM'] || row['From'] || row['from'] || '';
            const to = row['TO'] || row['To'] || row['to'] || '';
            const content = row['CONTENT'] || row['Content'] || row['content'] || row['BODY'] || row['Body'] || row['body'] || '';
            const date = row['DATE'] || row['Date'] || row['date'] || '';
            const convId = row['CONVERSATION ID'] || row['Conversation ID'] || row['conversation id'] || `${from}-${to}`;
            if (!content.trim()) continue;
            stats.total++;

            if (!conversations.has(convId)) conversations.set(convId, []);
            conversations.get(convId).push({ from, to, content, date });
          }

          // Process each conversation as an interaction
          for (const [convId, messages] of conversations) {
            // Find the other person (not the current user) in the conversation
            const participants = new Set();
            messages.forEach(m => { if (m.from) participants.add(m.from.trim()); if (m.to) participants.add(m.to.trim()); });

            for (const name of participants) {
              const match = nameMap.get(name.toLowerCase().trim());
              if (match) {
                stats.matched++;
                // Create a condensed interaction from all messages in this conversation
                const sorted = messages.sort((a, b) => new Date(a.date) - new Date(b.date));
                const summary = sorted.map(m => `[${m.date}] ${m.from}: ${m.content}`).join('\n').slice(0, 5000);
                const latestDate = sorted[sorted.length - 1]?.date;

                try {
                  await pool.query(`INSERT INTO interactions (person_id, interaction_type, subject, summary, source, interaction_at) VALUES ($1, 'linkedin_message', $2, $3, 'linkedin_import', $4) ON CONFLICT DO NOTHING`, [match.id, `LinkedIn conversation (${messages.length} messages)`, summary, latestDate ? new Date(latestDate).toISOString() : new Date().toISOString()]);
                  stats.interactions_created++;
                } catch (e) { stats.errors++; }
              } else {
                stats.unmatched_senders.add(name);
              }
            }
          }

          return JSON.stringify({ total_messages: stats.total, conversations: conversations.size, matched_people: stats.matched, interactions_created: stats.interactions_created, unmatched_senders: [...stats.unmatched_senders].slice(0, 20), errors: stats.errors, message: `Processed ${stats.total} LinkedIn messages across ${conversations.size} conversations. Created ${stats.interactions_created} interaction records.` });
        }

        return JSON.stringify({ error: 'Unsupported action' });
      }
      case 'run_sql_query': {
        const sql = input.query.trim();
        if (!sql.toUpperCase().startsWith('SELECT')) return JSON.stringify({ error: 'Only SELECT allowed' });
        if (/DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE/i.test(sql)) return JSON.stringify({ error: 'Modification not allowed' });
        const { rows } = await pool.query(sql + (sql.includes('LIMIT') ? '' : ' LIMIT 50'));
        return JSON.stringify({ explanation: input.explanation, row_count: rows.length, results: rows });
      }
      case 'get_platform_stats': {
        const { rows: [s] } = await pool.query(`SELECT (SELECT COUNT(*) FROM signal_events) AS signals, (SELECT COUNT(*) FROM companies WHERE sector IS NOT NULL OR is_client = true) AS companies, (SELECT COUNT(*) FROM people) AS people, (SELECT COUNT(*) FROM external_documents) AS documents, (SELECT COUNT(*) FROM placements) AS placements, (SELECT COALESCE(SUM(placement_fee),0) FROM placements) AS revenue`);
        return JSON.stringify(s);
      }
      default: return JSON.stringify({ error: `Unknown tool: ${name}` });
    }
  } catch (err) {
    console.error(`Tool ${name} error:`, err.message);
    return JSON.stringify({ error: err.message });
  }
}

const uploadedFiles = new Map();

// File upload
app.post('/api/chat/upload', authenticateToken, chatUpload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    if (!file) return res.status(400).json({ error: 'No file' });
    const fileId = crypto.randomUUID();
    const meta = { path: file.path, mimetype: file.mimetype, originalname: file.originalname, size: file.size };

    if (file.originalname.endsWith('.csv') || file.mimetype === 'text/csv') {
      const raw = fsChat.readFileSync(file.path, 'utf8');
      const allLines = raw.split('\n');
      // Find header line (skip LinkedIn notes/blank lines at top)
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 10); i++) {
        const trimmed = allLines[i].trim();
        if (trimmed && trimmed.includes(',')) { headerIdx = i; break; }
      }
      const lines = allLines.slice(headerIdx).filter(l => l.trim());
      if (lines.length) {
        function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
        const headers = parseCSV(lines[0]);
        meta.columns = headers;
        meta.preview = [];
        for (let i=1; i<Math.min(lines.length,1001); i++) {
          const vals = parseCSV(lines[i]);
          const row = {}; headers.forEach((h,idx) => { row[h] = vals[idx]||''; }); meta.preview.push(row);
        }

        // Detect LinkedIn CSV type
        const lh = headers.map(h => h.toLowerCase().trim());
        const hasFirstName = lh.includes('first name');
        const hasLastName = lh.includes('last name');
        const hasURL = lh.includes('url');
        const hasConnectedOn = lh.includes('connected on');
        const hasPosition = lh.includes('position');
        const hasFrom = lh.some(h => h === 'from');
        const hasTo = lh.some(h => h === 'to');
        const hasContent = lh.some(h => h === 'content' || h === 'body');
        const hasConversationId = lh.some(h => h.includes('conversation'));
        const hasPhoneNumbers = lh.some(h => h.includes('phone'));

        if (hasFirstName && hasLastName && hasConnectedOn && hasURL) {
          meta.linkedinType = 'connections';
          meta.suggestedMapping = { full_name: 'First Name+Last Name', linkedin_url: 'URL', email: 'Email Address', company: 'Company', position: 'Position', connected_on: 'Connected On' };
        } else if ((hasFrom || hasTo) && (hasContent || hasConversationId)) {
          meta.linkedinType = 'messages';
          meta.suggestedMapping = { from: headers[lh.findIndex(h => h === 'from')], to: headers[lh.findIndex(h => h === 'to')], content: headers[lh.findIndex(h => h === 'content' || h === 'body')], date: headers[lh.findIndex(h => h.includes('date'))] };
        } else if (hasFirstName && hasLastName && hasPhoneNumbers) {
          meta.linkedinType = 'contacts';
          meta.suggestedMapping = { full_name: 'First Name+Last Name', email: headers[lh.findIndex(h => h.includes('email'))], phone: headers[lh.findIndex(h => h.includes('phone'))], company: headers[lh.findIndex(h => h.includes('company') || h.includes('org'))] };
        } else {
          // Generic CSV mapping
          meta.suggestedMapping = {};
          if (lh.some(h=>h.includes('name'))) meta.suggestedMapping.full_name = headers[lh.findIndex(h=>h.includes('name'))];
          if (lh.some(h=>h.includes('title')||h.includes('role'))) meta.suggestedMapping.current_title = headers[lh.findIndex(h=>h.includes('title')||h.includes('role'))];
          if (lh.some(h=>h.includes('company')||h.includes('org'))) meta.suggestedMapping.current_company_name = headers[lh.findIndex(h=>h.includes('company')||h.includes('org'))];
          if (lh.some(h=>h.includes('email'))) meta.suggestedMapping.email = headers[lh.findIndex(h=>h.includes('email'))];
          if (lh.some(h=>h.includes('location')||h.includes('city'))) meta.suggestedMapping.location = headers[lh.findIndex(h=>h.includes('location')||h.includes('city'))];
        }
      }
    }
    if (file.originalname.endsWith('.pdf') || file.mimetype === 'application/pdf') {
      try { const pdfParse = require('pdf-parse'); const buf = fsChat.readFileSync(file.path); const d = await pdfParse(buf); meta.text = d.text; meta.pages = d.numpages; } catch (e) { meta.text = '[Install pdf-parse: npm install pdf-parse]'; }
    }
    if (file.originalname.endsWith('.txt') || file.mimetype === 'text/plain') { meta.text = fsChat.readFileSync(file.path, 'utf8'); }

    uploadedFiles.set(fileId, meta);
    setTimeout(() => { uploadedFiles.delete(fileId); try { fsChat.unlinkSync(file.path); } catch(e){} }, 30*60*1000);

    res.json({ file_id: fileId, filename: file.originalname, size: file.size, type: file.mimetype, columns: meta.columns||null, row_count: meta.preview?.length||null, pages: meta.pages||null, suggested_mapping: meta.suggestedMapping||null, linkedin_type: meta.linkedinType||null });
  } catch (err) { console.error('Upload error:', err); res.status(500).json({ error: 'Upload failed' }); }
});

// Chat endpoint
app.post('/api/chat', authenticateToken, async (req, res) => {
  try {
    const { message, file_id } = req.body;
    if (!message?.trim()) return res.status(400).json({ error: 'Message required' });
    if (!process.env.ANTHROPIC_API_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not set' });

    const history = getChatHistory(req.user.id);
    let userContent = message;
    if (file_id) {
      const fm = uploadedFiles.get(file_id);
      if (fm) {
        userContent += `\n\n[File: ${fm.originalname} (${fm.mimetype}, ${fm.size}b)]`;
        if (fm.linkedinType) userContent += `\n[LinkedIn Export Type: ${fm.linkedinType}]`;
        if (fm.columns) userContent += `\n[Columns: ${fm.columns.join(', ')}]`;
        if (fm.preview) userContent += `\n[${fm.preview.length} rows]`;
        if (fm.suggestedMapping) userContent += `\n[Mapping: ${JSON.stringify(fm.suggestedMapping)}]`;
        if (fm.pages) userContent += `\n[PDF: ${fm.pages} pages]`;
        if (fm.text) userContent += `\n[Text: ${fm.text.slice(0,1500)}]`;
        userContent += `\n[file_id: ${file_id}]`;
      }
    }

    history.push({ role: 'user', content: userContent });
    while (history.length > MAX_HISTORY) history.shift();

    let response = await callClaude(history, CHAT_TOOLS, CHAT_SYSTEM);
    let finalText = '';
    let toolsUsed = [];
    let rounds = 0;

    while (response.stop_reason === 'tool_use' && rounds < 5) {
      rounds++;
      const toolCalls = response.content.filter(c => c.type === 'tool_use');
      const textParts = response.content.filter(c => c.type === 'text').map(c => c.text);
      if (textParts.length) finalText += textParts.join('');

      history.push({ role: 'assistant', content: response.content });
      const toolResultContent = [];
      for (const tc of toolCalls) {
        console.log(`  🔧 ${tc.name}`, JSON.stringify(tc.input).slice(0, 150));
        const result = await executeTool(tc.name, tc.input, req.user.id);
        toolResultContent.push({ type: 'tool_result', tool_use_id: tc.id, content: result });
        toolsUsed.push(tc.name);
      }
      history.push({ role: 'user', content: toolResultContent });
      response = await callClaude(history, CHAT_TOOLS, CHAT_SYSTEM);
    }

    finalText += response.content.filter(c => c.type === 'text').map(c => c.text).join('');
    history.push({ role: 'assistant', content: finalText });
    while (history.length > MAX_HISTORY) history.shift();

    res.json({ response: finalText, tools_used: [...new Set(toolsUsed)] });
  } catch (err) {
    console.error('Chat error:', err.message);
    res.status(500).json({ error: 'Chat failed: ' + err.message });
  }
});

app.delete('/api/chat/history', authenticateToken, (req, res) => {
  chatHistories.delete(req.user.id);
  res.json({ success: true });
});

// ═══════════════════════════════════════════════════════════════════════════════
// XERO OAUTH 2.0
// ═══════════════════════════════════════════════════════════════════════════════

// Initiate OAuth flow — visit this in browser
app.get('/api/xero/connect', authenticateToken, (req, res) => {
  if (!process.env.XERO_CLIENT_ID) return res.status(500).json({ error: 'XERO_CLIENT_ID not configured' });
  const state = crypto.randomBytes(16).toString('hex');
  const authUrl = 'https://login.xero.com/identity/connect/authorize?' + new URLSearchParams({
    response_type: 'code',
    client_id: process.env.XERO_CLIENT_ID,
    redirect_uri: process.env.XERO_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/xero/callback`,
    scope: process.env.XERO_SCOPES || 'openid profile email accounting.transactions.read accounting.contacts.read offline_access',
    state
  });
  res.redirect(authUrl);
});

// OAuth callback — exchanges code for tokens
app.get('/api/xero/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).send('Missing authorization code');

  try {
    const credentials = Buffer.from(
      `${process.env.XERO_CLIENT_ID}:${process.env.XERO_CLIENT_SECRET}`
    ).toString('base64');

    // Exchange code for tokens
    const tokenRes = await fetch('https://identity.xero.com/connect/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Authorization: `Basic ${credentials}`
      },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: process.env.XERO_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/xero/callback`
      })
    });

    if (!tokenRes.ok) {
      const err = await tokenRes.text();
      return res.status(400).send(`Token exchange failed: ${err}`);
    }

    const tokenData = await tokenRes.json();

    // Get connected tenants
    const tenantsRes = await fetch('https://api.xero.com/connections', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const tenants = await tenantsRes.json();

    if (!tenants.length) return res.status(400).send('No Xero organisations found');

    // Save tokens for each tenant (usually just one)
    const { saveTokens } = require('./scripts/sync_xero');
    for (const tenant of tenants) {
      await saveTokens(tokenData, tenant.tenantId, tenant.tenantName);
      console.log(`✅ Xero connected: ${tenant.tenantName} (${tenant.tenantId})`);
    }

    res.send(`
      <html><body style="font-family:system-ui;text-align:center;padding:60px">
        <h2>Xero Connected</h2>
        <p>Organisation: <strong>${tenants[0].tenantName}</strong></p>
        <p>You can close this window. Invoice sync will run automatically.</p>
        <p><a href="/">Return to dashboard</a></p>
      </body></html>
    `);
  } catch (err) {
    console.error('Xero OAuth error:', err);
    res.status(500).send('Xero connection failed: ' + err.message);
  }
});

// Check Xero connection status
app.get('/api/xero/status', authenticateToken, async (req, res) => {
  const tokens = await pool.query('SELECT tenant_id, tenant_name, expires_at, updated_at FROM xero_tokens').catch(() => ({ rows: [] }));
  const sync = await pool.query('SELECT * FROM xero_sync_state').catch(() => ({ rows: [] }));
  res.json({ connected: tokens.rows.length > 0, tenants: tokens.rows, sync: sync.rows });
});

// Manual sync trigger
app.post('/api/xero/sync', authenticateToken, async (req, res) => {
  try {
    const { pipelineSyncXero } = require('./scripts/sync_xero');
    res.json({ message: 'Xero sync triggered' });
    pipelineSyncXero().catch(e => console.error('Xero sync error:', e.message));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CATCH-ALL — Serve static HTML pages (MUST be LAST)
// ═══════════════════════════════════════════════════════════════════════════════


// ─────────────────────────────────────────────────────────────────────────────

// PIPELINE SCHEDULER
// ─────────────────────────────────────────────────────────────────────────────
try {
  const scheduler = require('./scripts/scheduler.js');
  scheduler.registerRoutes(app, authenticateToken);
  scheduler.startScheduler().catch(e => console.log('Scheduler error:', e.message));
  console.log('  ✅ Pipeline scheduler started');
} catch(e) {
  console.log('  ⚠️  Scheduler skipped:', e.message);
}
// MCP ENDPOINT — Claude.ai remote MCP integration at POST /mcp
// ─────────────────────────────────────────────────────────────────────────────
try {
  const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
  const { createMcpServer } = require('./scripts/mcp_server.js');
  app.post('/mcp', async (req, res) => {
    const mcpServer = createMcpServer();
    const t = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined, enableJsonResponse: true });
    res.on('close', () => t.close());
    await mcpServer.connect(t);
    await t.handleRequest(req, res, req.body);
  });
  app.get('/mcp', (_req, res) => res.json({ service: 'mitchellake-mcp', tools: 11, status: 'ok' }));
} catch(e) {
  console.log('  ⚠️  MCP endpoint skipped:', e.message);
}
app.get('*', (req, res) => {
  // Serve the requested HTML file or fall back to dashboard
  const page = req.path === '/' ? 'index.html' : req.path;
  const filePath = path.join(__dirname, 'public', page);

  res.sendFile(filePath, (err) => {
    if (err) {
      res.sendFile(path.join(__dirname, 'public', 'index.html'));
    }
  });
});

app.get('/autonodal', (req, res) => res.sendFile(path.join(__dirname, 'public/autonodal.html')));

// ═══════════════════════════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════════════════════════



app.listen(PORT, async () => {
  console.log('═══════════════════════════════════════════════════');
  console.log('  MitchelLake Signal Intelligence Platform');
  console.log(`  Server running on port ${PORT}`);
  console.log('═══════════════════════════════════════════════════');
  console.log(`  Dashboard: http://localhost:${PORT}`);
  console.log(`  API:       http://localhost:${PORT}/api/health`);
  console.log('═══════════════════════════════════════════════════\n');

  // Backfill: mark companies as clients if they have placements
  try {
    // First, try to link clients to companies by name match
    const { rowCount: linked } = await pool.query(`
      UPDATE clients SET company_id = co.id
      FROM companies co
      WHERE clients.company_id IS NULL
        AND LOWER(TRIM(clients.name)) = LOWER(TRIM(co.name))
    `);
    if (linked > 0) console.log(`  ✅ Linked ${linked} clients to companies by name`);

    // Then mark those companies as clients
    const { rowCount } = await pool.query(`
      UPDATE companies SET is_client = true
      WHERE id IN (
        SELECT DISTINCT cl.company_id FROM clients cl
        JOIN placements pl ON pl.client_id = cl.id
        WHERE cl.company_id IS NOT NULL
      ) AND (is_client IS NULL OR is_client = false)
    `);
    if (rowCount > 0) console.log(`  ✅ Backfilled is_client on ${rowCount} companies from placement data`);
  } catch (e) {
    console.log('  ⚠️ Client backfill skipped:', e.message);
  }
});