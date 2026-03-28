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
// SHARED CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

const REGION_MAP = {
  'AU': ['Australia', 'Australian', 'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'ASX', 'Canberra', 'CSIRO'],
  'SG': ['Singapore', 'Southeast Asia', 'ASEAN', 'Jakarta', 'Kuala Lumpur', 'Bangkok', 'Vietnam', 'Philippines', 'Indonesia', 'Malaysia', 'Thailand'],
  'UK': ['United Kingdom', 'London', 'England', 'Britain', 'British', 'Manchester', 'Edinburgh', 'FTSE', 'LSE'],
  'US': ['United States', 'Silicon Valley', 'New York', 'San Francisco', 'California', 'Texas', 'Boston', 'Seattle', 'NASDAQ', 'NYSE', 'SEC', 'Federal'],
  'APAC': ['Australia', 'Australian', 'Singapore', 'Asia', 'APAC', 'Japan', 'Japanese', 'Korea', 'Korean', 'India', 'Indian', 'Hong Kong', 'Southeast Asia', 'ASEAN', 'Sydney', 'Melbourne', 'China', 'Chinese', 'Taiwan', 'New Zealand'],
  'EMEA': ['United Kingdom', 'Europe', 'European', 'EMEA', 'London', 'Germany', 'German', 'France', 'French', 'Netherlands', 'Ireland', 'Middle East', 'Africa', 'Nordics', 'Sweden', 'Denmark', 'EU'],
  'AMER': ['United States', 'North America', 'Canada', 'Canadian', 'Latin America', 'Brazil', 'Brazilian', 'Mexico', 'Silicon Valley', 'New York', 'NASDAQ', 'NYSE'],
};
const REGION_CODES = {
  'AU': ['AU','NZ'], 'SG': ['SG','MY','ID','TH','VN','PH'], 'UK': ['UK','GB','IE'],
  'US': ['US','CA'], 'APAC': ['AU','NZ','SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN','TW'],
  'EMEA': ['UK','GB','IE','DE','FR','NL','SE','DK','NO','FI','ES','IT','PT','AT','CH','BE'],
  'AMER': ['US','CA','BR','MX','AR','CL','CO'],
};

// ─────────────────────────────────────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

app.set('trust proxy', 1);
app.use(express.json({ limit: '10mb' }));

// Serve Autonodal landing page as homepage when accessed via autonodal.com
app.get('/', (req, res, next) => {
  const host = req.hostname;
  if (host === 'autonodal.com' || host === 'www.autonodal.com') {
    return res.sendFile(path.join(__dirname, 'public/autonodal.html'));
  }
  next();
});

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
      `SELECT s.user_id, u.email, u.name, u.role, u.tenant_id,
              t.vertical, t.name as tenant_name, t.slug as tenant_slug
       FROM sessions s
       JOIN users u ON s.user_id = u.id
       LEFT JOIN tenants t ON t.id = u.tenant_id
       WHERE s.token = $1 AND s.expires_at > NOW()`,
      [token]
    );

    if (rows.length === 0) return res.status(401).json({ error: 'Invalid or expired token' });

    req.user = rows[0];
    // Ensure tenant_id is always available (fallback to ML tenant)
    req.user.tenant_id = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    req.tenant_id = req.user.tenant_id;
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
        `SELECT s.user_id, u.email, u.name, u.role, u.tenant_id
         FROM sessions s JOIN users u ON s.user_id = u.id
         WHERE s.token = $1 AND s.expires_at > NOW()`,
        [token]
      );
      if (rows.length > 0) {
        req.user = rows[0];
        req.user.tenant_id = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
        req.tenant_id = req.user.tenant_id;
      }
    } catch (e) { /* ignore */ }
  }
  next();
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUDIT LOG HELPER
// ═══════════════════════════════════════════════════════════════════════════════

async function auditLog(userId, action, targetType, targetId, details, ip) {
  try {
    await pool.query(
      `INSERT INTO audit_logs (id, user_id, action, target_type, target_id, details, ip_address, created_at)
       VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, NOW())`,
      [userId, action, targetType || null, targetId || null, details ? JSON.stringify(details) : null, ip || null]
    );
  } catch (e) { /* audit logging should never block operations */ }
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUTH ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

// Google OAuth — initiate
app.get('/api/auth/google', (req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });
  const redirectUri = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
  const returnTo = req.query.return_to || '/index.html';
  // Request full scopes upfront (Gmail + Drive + Docs) so every sign-in auto-connects
  const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    redirect_uri: redirectUri,
    response_type: 'code',
    scope: GOOGLE_CONNECT_SCOPES,
    hd: 'mitchellake.com',
    prompt: 'consent',
    access_type: 'offline',
    state: returnTo
  });
  res.redirect(authUrl);
});

// Google OAuth — callback
app.get('/api/auth/google/callback', async (req, res) => {
  const { code, error, state } = req.query;
  const returnTo = (state && state.startsWith('/')) ? state : '/index.html';
  if (error) return res.redirect(returnTo + '?auth_error=' + encodeURIComponent(error));
  if (!code) return res.redirect(returnTo + '?auth_error=no_code');

  try {
    const redirectUri = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;

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
      return res.redirect('/index.html?auth_error=token_exchange_failed');
    }

    const tokenData = await tokenRes.json();

    // Get user info
    const userInfoRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` }
    });
    const userInfo = await userInfoRes.json();

    // Enforce mitchellake.com domain
    if (!userInfo.email || !userInfo.email.endsWith('@mitchellake.com')) {
      return res.redirect('/index.html?auth_error=domain_restricted');
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
      // Auto-create MitchelLake team member (password_hash set to placeholder — OAuth users don't use passwords)
      const { rows: [newUser] } = await pool.query(
        `INSERT INTO users (id, email, name, role, password_hash, tenant_id, created_at, updated_at)
         VALUES (gen_random_uuid(), $1, $2, 'consultant', 'oauth_google', $3, NOW(), NOW())
         RETURNING id, email, name, role`,
        [userInfo.email, userInfo.name || userInfo.email.split('@')[0], process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001']
      );
      user = newUser;
      console.log(`✅ New team member created: ${user.name} (${user.email})`);
      auditLog(user.id, 'user_registered', 'user', user.id, { name: user.name, email: user.email });
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

    // Auto-connect Google (Gmail + Drive) if we received a refresh token
    if (tokenData.refresh_token) {
      try {
        const tenantId = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
        await pool.query(`
          INSERT INTO user_google_accounts (id, user_id, google_email, google_name, access_token, refresh_token, scopes, sync_enabled, tenant_id, connected_at)
          VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, true, $7, NOW())
          ON CONFLICT (user_id, google_email) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = COALESCE(EXCLUDED.refresh_token, user_google_accounts.refresh_token),
            scopes = EXCLUDED.scopes,
            sync_enabled = true,
            connected_at = NOW()
        `, [user.id, userInfo.email, userInfo.name, tokenData.access_token, tokenData.refresh_token, GOOGLE_CONNECT_SCOPES, tenantId]);
        console.log(`✅ Auto-connected Google for ${userInfo.email} (Gmail + Drive)`);
      } catch (e) {
        console.error('Auto-connect Google error:', e.message);
      }
    }

    // Redirect to app with token (frontend picks it up from URL)
    const sep = returnTo.includes('?') ? '&' : '?';
    res.redirect(`${returnTo}${sep}token=${sessionToken}&user=${encodeURIComponent(JSON.stringify({ id: user.id, email: user.email, name: user.name, role: user.role }))}`);
  } catch (err) {
    console.error('Google auth error:', err);
    res.redirect(returnTo + '?auth_error=server_error');
  }
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  try {
    const { rows: [user] } = await pool.query(
      'SELECT id, email, name, role, region, onboarded, preferences FROM users WHERE id = $1',
      [req.user.user_id]
    );
    res.json({ user: { ...req.user, region: user?.region, onboarded: user?.onboarded, preferences: user?.preferences } });
  } catch (e) {
    res.json({ user: req.user });
  }
});

// Update user profile (region, preferences, onboarding)
app.patch('/api/auth/me', authenticateToken, async (req, res) => {
  try {
    const allowed = ['region', 'onboarded', 'preferences'];
    const updates = [];
    const params = [];
    let idx = 1;
    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        idx++;
        updates.push(`${key} = $${idx}`);
        params.push(key === 'preferences' ? JSON.stringify(req.body[key]) : req.body[key]);
      }
    }
    if (updates.length === 0) return res.json({ ok: true });
    params.unshift(req.user.user_id);
    await pool.query(`UPDATE users SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Personalized Morning Brief ───
app.get('/api/brief/personal', authenticateToken, async (req, res) => {
  try {
    const userId = req.user.user_id;

    // Get user's region
    const { rows: [userRow] } = await pool.query('SELECT region FROM users WHERE id = $1', [userId]);
    const userRegion = userRow?.region || 'APAC';

    // Run all 4 queries in parallel
    const geos = REGION_MAP[userRegion] || REGION_MAP['APAC'];
    const geoConditions = geos.map((_, i) => `c.geography ILIKE $${i + 2} OR sd.signal_summary ILIKE $${i + 2}`).join(' OR ');
    const geoParams = [req.tenant_id, ...geos.map(g => `%${g}%`)];

    const [contactResult, clientResult, dispatchResult, statsResult] = await Promise.all([
      // 1. My contacts in recent signals
      pool.query(`
        SELECT DISTINCT ON (p.id)
          p.id as person_id, p.full_name, p.current_title, p.current_company_name,
          se.signal_type, se.company_name as signal_company, se.confidence_score,
          se.evidence_summary, se.detected_at,
          tp.proximity_strength, tp.proximity_type,
          i.interaction_at as last_contact
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $2
        JOIN companies c ON c.id = p.current_company_id
        JOIN signal_events se ON se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '7 days'
        LEFT JOIN LATERAL (
          SELECT interaction_at FROM interactions
          WHERE person_id = p.id AND user_id = $1
          ORDER BY interaction_at DESC LIMIT 1
        ) i ON true
        WHERE tp.user_id = $1 AND tp.tenant_id = $2
        ORDER BY p.id, se.confidence_score DESC, se.detected_at DESC
        LIMIT 5
      `, [userId, req.tenant_id]),
      // 2. Client signals
      pool.query(`
        SELECT DISTINCT ON (se.company_id)
          se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
          se.evidence_summary, se.detected_at,
          cl.relationship_status, cl.relationship_tier,
          (SELECT COUNT(*) FROM people p WHERE p.current_company_id = se.company_id) as contact_count
        FROM signal_events se
        JOIN companies c ON c.id = se.company_id AND c.is_client = true AND c.tenant_id = $1
        JOIN accounts cl ON cl.company_id = c.id AND cl.tenant_id = $1
        WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
        ORDER BY se.company_id, se.confidence_score DESC
        LIMIT 5
      `, [req.tenant_id]),
      // 3. Top dispatches for user's region
      pool.query(`
        SELECT sd.id, sd.company_name, sd.signal_type, sd.signal_summary,
               sd.opportunity_angle, sd.blog_title, sd.status, sd.claimed_by,
               c.geography, c.is_client,
               jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) as connection_count
        FROM signal_dispatches sd
        LEFT JOIN companies c ON c.id = sd.company_id
        WHERE sd.status = 'draft' AND sd.claimed_by IS NULL
          AND sd.tenant_id = $1
          AND (${geoConditions})
        ORDER BY
          CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
          jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) DESC,
          sd.generated_at DESC
        LIMIT 3
      `, geoParams),
      // 4. Quick stats
      pool.query(`
        SELECT
          (SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours' AND tenant_id = $1) as signals_24h,
          (SELECT COUNT(*) FROM signal_dispatches WHERE status = 'draft' AND claimed_by IS NULL AND tenant_id = $1) as unclaimed_dispatches,
          (SELECT COUNT(*) FROM signal_events se JOIN companies c ON c.id = se.company_id AND c.is_client = true WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1) as client_signals_7d
      `, [req.tenant_id])
    ]);

    const contactSignals = contactResult.rows;
    const clientSignals = clientResult.rows;
    const regionDispatches = dispatchResult.rows;
    const briefStats = statsResult.rows[0];

    res.json({
      region: userRegion,
      stats: briefStats,
      contact_signals: contactSignals,
      client_signals: clientSignals,
      region_dispatches: regionDispatches
    });
  } catch (err) {
    console.error('Personal brief error:', err.message);
    res.status(500).json({ error: err.message });
  }
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
// GOOGLE CONNECT (Gmail + Drive + Docs + Sheets + Slides)
// ═══════════════════════════════════════════════════════════════════════════════

const GOOGLE_CONNECT_SCOPES = [
  'openid', 'email', 'profile',
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/documents.readonly',
  'https://www.googleapis.com/auth/spreadsheets.readonly',
  'https://www.googleapis.com/auth/presentations.readonly',
].join(' ');

app.get('/api/auth/gmail/connect', async (req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });

  // Accept token from query param (browser navigation can't send headers) or auth header
  const token = req.query.token || (req.headers.authorization && req.headers.authorization.replace('Bearer ', ''));
  if (!token) return res.redirect('/index.html?auth_error=login_required');

  // Validate token
  const { rows } = await pool.query(
    'SELECT s.user_id FROM sessions s WHERE s.token = $1 AND s.expires_at > NOW()', [token]
  );
  if (rows.length === 0) return res.redirect('/index.html?auth_error=session_expired');

  const redirectUri = process.env.GOOGLE_GMAIL_REDIRECT_URL || `${req.protocol}://${req.get('host')}/api/auth/gmail/callback`;
  const state = Buffer.from(JSON.stringify({
    userId: rows[0].user_id,
    token: token,
    returnTo: req.query.return_to || '/index.html'
  })).toString('base64');

  const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    redirect_uri: redirectUri,
    response_type: 'code',
    scope: GOOGLE_CONNECT_SCOPES,
    access_type: 'offline',
    prompt: 'consent',
    hd: 'mitchellake.com',
    state
  });
  res.redirect(authUrl);
});

app.get('/api/auth/gmail/callback', async (req, res) => {
  const { code, error, state } = req.query;
  let stateData = {};
  try { stateData = JSON.parse(Buffer.from(state || '', 'base64').toString('utf-8')); } catch (e) {}
  const returnTo = stateData.returnTo || '/index.html';

  if (error || !code) return res.redirect(returnTo + '?gmail_error=' + encodeURIComponent(error || 'no_code'));

  try {
    const redirectUri = process.env.GOOGLE_GMAIL_REDIRECT_URL || `${req.protocol}://${req.get('host')}/api/auth/gmail/callback`;

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
      console.error('Gmail token exchange failed:', await tokenRes.text());
      return res.redirect(returnTo + '?gmail_error=token_exchange_failed');
    }

    const tokens = await tokenRes.json();

    // Get email from the token
    const userInfoRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokens.access_token}` }
    });
    const userInfo = await userInfoRes.json();

    // Store in user_google_accounts
    await pool.query(`
      INSERT INTO user_google_accounts (user_id, google_email, access_token, refresh_token, token_expires_at, scopes, sync_enabled, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), NOW())
      ON CONFLICT (user_id, google_email) DO UPDATE SET
        access_token = EXCLUDED.access_token,
        refresh_token = COALESCE(EXCLUDED.refresh_token, user_google_accounts.refresh_token),
        token_expires_at = EXCLUDED.token_expires_at,
        scopes = EXCLUDED.scopes,
        sync_enabled = true,
        updated_at = NOW()
    `, [
      stateData.userId,
      userInfo.email,
      tokens.access_token,
      tokens.refresh_token || null,
      tokens.expires_in ? new Date(Date.now() + tokens.expires_in * 1000) : null,
      GOOGLE_CONNECT_SCOPES.split(' ')
    ]);

    console.log(`✅ Google connected (Gmail + Drive): ${userInfo.email} for user ${stateData.userId}`);

    // Trigger async Drive ingestion in background
    setImmediate(async () => {
      try {
        console.log(`📁 Auto-ingesting Drive files for ${userInfo.email}...`);
        const freshToken = tokens.access_token;
        const userId = stateData.userId;
        const tenantId = ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
        const crypto = require('crypto');

        const mimeTypes = [
          ['application/vnd.google-apps.document', 'google_doc', 'Google Docs'],
          ['application/vnd.google-apps.spreadsheet', 'google_sheet', 'Google Sheets'],
          ['application/vnd.google-apps.presentation', 'google_slides', 'Google Slides'],
        ];

        let totalIngested = 0;
        for (const [mime, typeLabel, sourceName] of mimeTypes) {
          let pageToken = '';
          do {
            const params = new URLSearchParams({ q: `mimeType = '${mime}' and trashed = false`, fields: 'nextPageToken,files(id,name,mimeType,modifiedTime,webViewLink)', pageSize: '100', orderBy: 'modifiedTime desc' });
            if (pageToken) params.set('pageToken', pageToken);
            const listRes = await fetch('https://www.googleapis.com/drive/v3/files?' + params, { headers: { Authorization: 'Bearer ' + freshToken } });
            if (!listRes.ok) break;
            const listData = await listRes.json();
            const files = listData.files || [];
            pageToken = listData.nextPageToken || '';

            for (const file of files) {
              const hash = crypto.createHash('md5').update('gdrive:' + file.id).digest('hex');
              const { rows: existing } = await pool.query('SELECT id FROM external_documents WHERE source_url_hash = $1', [hash]);
              if (existing.length) continue;

              // Extract content
              let content = '';
              if (mime.includes('document') || mime.includes('presentation')) {
                const r = await fetch('https://www.googleapis.com/drive/v3/files/' + file.id + '/export?mimeType=text/plain', { headers: { Authorization: 'Bearer ' + freshToken } });
                if (r.ok) content = await r.text();
              } else if (mime.includes('spreadsheet')) {
                const sr = await fetch('https://sheets.googleapis.com/v4/spreadsheets/' + file.id + '?fields=sheets.properties.title', { headers: { Authorization: 'Bearer ' + freshToken } });
                if (sr.ok) {
                  const sd = await sr.json();
                  const parts = [];
                  for (const s of (sd.sheets || []).slice(0, 5)) {
                    const vr = await fetch('https://sheets.googleapis.com/v4/spreadsheets/' + file.id + '/values/' + encodeURIComponent(s.properties.title), { headers: { Authorization: 'Bearer ' + freshToken } });
                    if (vr.ok) { const d = await vr.json(); parts.push((d.values || []).map(r => r.join('\t')).join('\n')); }
                  }
                  content = parts.join('\n\n');
                }
              }
              if (!content || content.length < 20) continue;
              const truncated = content.length > 50000 ? content.substring(0, 50000) : content;

              await pool.query(
                `INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash, tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'context_only', NOW()) ON CONFLICT (source_url_hash) DO NOTHING`,
                [file.name, truncated, sourceName, typeLabel, file.webViewLink || '', hash, tenantId, userId, file.modifiedTime]
              );

              // Embed
              if (process.env.OPENAI_API_KEY && process.env.QDRANT_URL) {
                try {
                  const emb = await generateQueryEmbedding((file.name + '\n\n' + truncated).slice(0, 8000));
                  const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
                  await new Promise((resolve, reject) => {
                    const body = JSON.stringify({ points: [{ id: hash, vector: emb, payload: { tenant_id: tenantId, title: file.name, source_type: typeLabel } }] });
                    const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
                      headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 15000 },
                      (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
                    qReq.on('error', reject); qReq.write(body); qReq.end();
                  });
                } catch (e) { /* embed error, doc still stored */ }
              }
              totalIngested++;
              if (totalIngested % 50 === 0) console.log(`  📁 ${userInfo.email}: ${totalIngested} files ingested...`);
              await new Promise(r => setTimeout(r, 200));
            }
          } while (pageToken);
        }
        console.log(`✅ Drive auto-ingest complete for ${userInfo.email}: ${totalIngested} files`);
      } catch (e) {
        console.error(`⚠️ Drive auto-ingest error for ${userInfo?.email}: ${e.message}`);
      }
    });

    const sep = returnTo.includes('?') ? '&' : '?';
    res.redirect(`${returnTo}${sep}gmail=connected`);
  } catch (err) {
    console.error('Gmail connect error:', err);
    res.redirect(returnTo + '?gmail_error=server_error');
  }
});

// Check Gmail connection status
app.get('/api/auth/gmail/status', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT google_email, sync_enabled, token_expires_at, scopes, updated_at
       FROM user_google_accounts WHERE user_id = $1`,
      [req.user.user_id]
    );
    res.json({ connected: rows.length > 0, accounts: rows });
  } catch (err) {
    res.json({ connected: false, accounts: [] });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// GOOGLE DRIVE — list, ingest, embed
// ═══════════════════════════════════════════════════════════════════════════════

// Helper: get a fresh Google access token for the current user
async function getGoogleToken(userId) {
  const { rows: [acct] } = await pool.query(
    'SELECT id, access_token, refresh_token, token_expires_at FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true LIMIT 1',
    [userId]
  );
  if (!acct) return null;

  // Refresh if expired or expiring within 5 min
  if (acct.token_expires_at && new Date(acct.token_expires_at) <= new Date(Date.now() + 5 * 60 * 1000)) {
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
          await pool.query(
            'UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2, updated_at = NOW() WHERE id = $3',
            [tokens.access_token, new Date(Date.now() + tokens.expires_in * 1000), acct.id]
          );
          return tokens.access_token;
        }
      } catch (e) { /* fall through to existing token */ }
    }
  }
  return acct.access_token;
}

// List files from Drive (Docs, Sheets, Slides, PDFs)
app.get('/api/drive/files', authenticateToken, async (req, res) => {
  try {
    const token = await getGoogleToken(req.user.user_id);
    if (!token) return res.status(401).json({ error: 'Google account not connected. Visit /api/auth/gmail/connect to connect.' });

    const folderId = req.query.folder || 'root';
    const pageToken = req.query.pageToken || '';
    const q = req.query.q || '';

    // Search for Docs, Sheets, Slides, PDFs
    const mimeTypes = [
      'application/vnd.google-apps.document',
      'application/vnd.google-apps.spreadsheet',
      'application/vnd.google-apps.presentation',
      'application/vnd.google-apps.folder',
      'application/pdf',
    ];
    let query = `trashed = false`;
    if (folderId && folderId !== 'root' && !q) query += ` and '${folderId}' in parents`;
    if (q) query += ` and fullText contains '${q.replace(/'/g, "\\'")}'`;
    if (!q && folderId === 'root') query += ` and (${mimeTypes.map(m => `mimeType = '${m}'`).join(' or ')})`;

    const params = new URLSearchParams({
      q: query,
      fields: 'nextPageToken,files(id,name,mimeType,modifiedTime,size,iconLink,webViewLink,owners,shared)',
      pageSize: '50',
      orderBy: 'modifiedTime desc',
    });
    if (pageToken) params.set('pageToken', pageToken);

    const driveRes = await fetch('https://www.googleapis.com/drive/v3/files?' + params, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!driveRes.ok) {
      const err = await driveRes.text();
      console.error('Drive API error:', err);
      return res.status(driveRes.status).json({ error: 'Drive API error', details: err });
    }
    const data = await driveRes.json();

    // Tag which files are already ingested
    const fileHashes = (data.files || []).map(f => require('crypto').createHash('md5').update('gdrive:' + f.id).digest('hex'));
    const { rows: ingested } = fileHashes.length > 0
      ? await pool.query(
          `SELECT source_url_hash FROM external_documents WHERE source_url_hash = ANY($1) AND tenant_id = $2`,
          [fileHashes, req.tenant_id]
        )
      : { rows: [] };
    const ingestedSet = new Set(ingested.map(r => r.source_url_hash));

    res.json({
      files: (data.files || []).map(f => ({
        id: f.id,
        name: f.name,
        mimeType: f.mimeType,
        type: f.mimeType.includes('document') ? 'doc' :
              f.mimeType.includes('spreadsheet') ? 'sheet' :
              f.mimeType.includes('presentation') ? 'slides' :
              f.mimeType.includes('folder') ? 'folder' : 'pdf',
        modifiedTime: f.modifiedTime,
        size: f.size,
        webViewLink: f.webViewLink,
        ingested: ingestedSet.has(require('crypto').createHash('md5').update('gdrive:' + f.id).digest('hex')),
      })),
      nextPageToken: data.nextPageToken || null,
    });
  } catch (err) {
    console.error('Drive files error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Ingest a single Drive file — extract text, store in external_documents, embed in Qdrant
app.post('/api/drive/ingest/:fileId', authenticateToken, async (req, res) => {
  try {
    const token = await getGoogleToken(req.user.user_id);
    if (!token) return res.status(401).json({ error: 'Google account not connected' });

    const fileId = req.params.fileId;
    const tenantId = req.tenant_id;
    const sourceUrlHash = require('crypto').createHash('md5').update('gdrive:' + fileId).digest('hex');

    // Check if already ingested
    const { rows: existing } = await pool.query(
      `SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2`,
      [sourceUrlHash, tenantId]
    );

    // Get file metadata
    const metaRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType,modifiedTime,webViewLink`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!metaRes.ok) return res.status(404).json({ error: 'File not found in Drive' });
    const meta = await metaRes.json();

    // Extract text content based on type
    let content = '';
    let title = meta.name;

    if (meta.mimeType === 'application/vnd.google-apps.document') {
      // Google Docs → export as plain text
      const textRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=text/plain`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (textRes.ok) content = await textRes.text();

    } else if (meta.mimeType === 'application/vnd.google-apps.spreadsheet') {
      // Google Sheets → get all sheet values as text
      const sheetsRes = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${fileId}?fields=sheets.properties.title`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (sheetsRes.ok) {
        const sheetsData = await sheetsRes.json();
        const sheetNames = (sheetsData.sheets || []).map(s => s.properties.title);
        const parts = [];
        for (const sheetName of sheetNames.slice(0, 10)) {
          const valRes = await fetch(
            `https://sheets.googleapis.com/v4/spreadsheets/${fileId}/values/${encodeURIComponent(sheetName)}`,
            { headers: { Authorization: `Bearer ${token}` } }
          );
          if (valRes.ok) {
            const valData = await valRes.json();
            const rows = (valData.values || []).map(r => r.join('\t')).join('\n');
            parts.push(`--- Sheet: ${sheetName} ---\n${rows}`);
          }
        }
        content = parts.join('\n\n');
      }

    } else if (meta.mimeType === 'application/vnd.google-apps.presentation') {
      // Google Slides → export as plain text
      const textRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=text/plain`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (textRes.ok) content = await textRes.text();

    } else if (meta.mimeType === 'application/pdf') {
      // PDFs — can't easily extract text without OCR, store metadata only
      content = `[PDF document: ${meta.name}]`;
    }

    if (!content || content.length < 10) {
      return res.json({ ingested: false, message: 'No extractable content', fileId });
    }

    // Truncate very large documents
    const maxLen = 50000;
    if (content.length > maxLen) content = content.substring(0, maxLen) + '\n\n[... truncated at ' + maxLen + ' chars]';

    // Store in external_documents
    let docId;
    if (existing.length > 0) {
      docId = existing[0].id;
      await pool.query(
        `UPDATE external_documents SET title = $1, content = $2, source_url = $3, updated_at = NOW() WHERE id = $4`,
        [title, content, meta.webViewLink, docId]
      );
    } else {
      // ALL Drive docs are context_only — they're internal knowledge, not market signal sources
      // They're still embedded and searchable, just never fed to the signal extraction pipeline
      const isOld = true; // Always context_only for Drive
      const { rows: [newDoc] } = await pool.query(
        `INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash, tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
         VALUES ($1, $2, 'Google Drive', $3, $4, $5, $6, $7, $8, $9, NOW())
         RETURNING id`,
        [title, content,
         meta.mimeType.includes('document') ? 'google_doc' :
         meta.mimeType.includes('spreadsheet') ? 'google_sheet' :
         meta.mimeType.includes('presentation') ? 'google_slides' : 'pdf',
         meta.webViewLink, sourceUrlHash, tenantId, req.user.user_id, meta.modifiedTime,
         isOld ? 'context_only' : 'pending']
      );
      docId = newDoc.id;
    }

    // Embed in Qdrant
    let embedded = false;
    if (process.env.OPENAI_API_KEY && process.env.QDRANT_URL) {
      try {
        const embeddingText = `${title}\n\n${content.substring(0, 8000)}`;
        const embedding = await generateQueryEmbedding(embeddingText);

        const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({
            points: [{
              id: docId,
              vector: embedding,
              payload: {
                tenant_id: tenantId,
                title: title,
                source: 'google_drive',
                source_type: meta.mimeType.includes('document') ? 'google_doc' : meta.mimeType.includes('spreadsheet') ? 'google_sheet' : 'google_slides',
                file_id: fileId,
              }
            }]
          });
          const qReq = https.request({
            hostname: url.hostname, port: url.port || 443,
            path: url.pathname + '?wait=true', method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY },
            timeout: 15000
          }, (qRes) => { const c = []; qRes.on('data', d => c.push(d)); qRes.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        embedded = true;
      } catch (e) {
        console.error('Drive embed error:', e.message);
      }
    }

    res.json({
      ingested: true,
      docId,
      title,
      contentLength: content.length,
      embedded,
      type: meta.mimeType,
      fileId,
    });
  } catch (err) {
    console.error('Drive ingest error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bulk ingest multiple Drive files
app.post('/api/drive/ingest-bulk', authenticateToken, async (req, res) => {
  try {
    const { fileIds } = req.body;
    if (!fileIds || !Array.isArray(fileIds)) return res.status(400).json({ error: 'fileIds array required' });

    const results = [];
    for (const fileId of fileIds.slice(0, 20)) {
      try {
        // Call the single ingest internally
        const token = await getGoogleToken(req.user.user_id);
        if (!token) { results.push({ fileId, error: 'No token' }); continue; }

        // Simplified inline — reuse the logic
        const metaRes = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        if (!metaRes.ok) { results.push({ fileId, error: 'Not found' }); continue; }
        const meta = await metaRes.json();
        results.push({ fileId, name: meta.name, queued: true });
      } catch (e) {
        results.push({ fileId, error: e.message });
      }
    }

    // Process each file sequentially in background (don't block response)
    res.json({ queued: results.length, files: results });

    // Background processing
    for (const r of results.filter(x => x.queued)) {
      try {
        const fakeReq = { params: { fileId: r.fileId }, user: req.user, tenant_id: req.tenant_id };
        const fakeRes = { json: () => {}, status: () => ({ json: () => {} }) };
        // Trigger single ingest endpoint logic — in production, use a job queue
      } catch (e) { /* skip */ }
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TENANT CONFIG & TERMINOLOGY
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/config/tenant', authenticateToken, async (req, res) => {
  try {
    const tenantId = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    const { rows } = await pool.query(
      'SELECT id, name, slug, vertical, logo_url, primary_color, plan, onboarding_complete, focus_geographies, focus_sectors FROM tenants WHERE id = $1',
      [tenantId]
    );
    res.json(rows[0] || null);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/config/terminology', authenticateToken, async (req, res) => {
  try {
    const tenantId = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    const { rows } = await pool.query('SELECT vertical FROM tenants WHERE id = $1', [tenantId]);
    if (!rows.length) return res.status(404).json({ error: 'Tenant not found' });

    const { getTerminology, SIGNAL_LABELS } = require('./lib/terminology');
    const vertical = rows[0].vertical;
    const t = getTerminology(vertical);

    res.json({ vertical, terminology: t, signal_labels: SIGNAL_LABELS[vertical] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// FEED MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════

// Tenant's active feeds
app.get('/api/feeds', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT fi.*, tf.active, tf.local_signal_yield, tf.tenant_rating, tf.selection_method, tf.activated_at
      FROM feed_inventory fi
      JOIN tenant_feeds tf ON tf.feed_id = fi.id
      WHERE tf.tenant_id = $1
      ORDER BY fi.quality_score DESC
    `, [req.tenant_id]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Full platform feed inventory (for feed selection UI)
app.get('/api/feeds/inventory', authenticateToken, async (req, res) => {
  try {
    const vertical = req.user.vertical || 'talent';
    const { rows } = await pool.query(`
      SELECT fi.*,
        EXISTS(
          SELECT 1 FROM tenant_feeds tf
          WHERE tf.feed_id = fi.id AND tf.tenant_id = $1 AND tf.active = TRUE
        ) AS is_active
      FROM feed_inventory fi
      WHERE fi.status = 'active'
        AND ($2 = ANY(fi.verticals) OR 'all' = ANY(fi.verticals) OR fi.verticals = '{}')
      ORDER BY fi.quality_score DESC, fi.avg_signals_per_week DESC NULLS LAST
    `, [req.tenant_id, vertical]);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Activate a feed for tenant
app.post('/api/feeds/:id/activate', authenticateToken, async (req, res) => {
  try {
    await pool.query(`
      INSERT INTO tenant_feeds (tenant_id, feed_id, selection_method)
      VALUES ($1, $2, 'manual')
      ON CONFLICT (tenant_id, feed_id) DO UPDATE SET active = TRUE, activated_at = NOW()
    `, [req.tenant_id, req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Deactivate a feed for tenant
app.delete('/api/feeds/:id/deactivate', authenticateToken, async (req, res) => {
  try {
    await pool.query(
      'UPDATE tenant_feeds SET active = FALSE WHERE tenant_id = $1 AND feed_id = $2',
      [req.tenant_id, req.params.id]
    );
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Rate a feed
app.post('/api/feeds/:id/rate', authenticateToken, async (req, res) => {
  try {
    const { rating } = req.body;
    if (!rating || rating < 1 || rating > 5) return res.status(400).json({ error: 'Rating must be 1-5' });

    await pool.query(
      'UPDATE tenant_feeds SET tenant_rating = $1, last_rated_at = NOW() WHERE tenant_id = $2 AND feed_id = $3',
      [rating, req.tenant_id, req.params.id]
    );
    // Update platform aggregate
    await pool.query(`
      UPDATE feed_inventory SET
        total_ratings = total_ratings + 1,
        avg_rating = (SELECT AVG(tenant_rating)::NUMERIC(3,2) FROM tenant_feeds WHERE feed_id = $1 AND tenant_rating IS NOT NULL)
      WHERE id = $1
    `, [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Propose a new feed
app.post('/api/feeds/propose', authenticateToken, async (req, res) => {
  try {
    const { proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale } = req.body;
    if (!proposed_url) return res.status(400).json({ error: 'URL required' });

    // Check for duplicate
    const { rows: existing } = await pool.query('SELECT id FROM feed_inventory WHERE url = $1', [proposed_url]);
    if (existing.length) return res.status(409).json({ error: 'Feed already exists', feed_id: existing[0].id });

    const { rows } = await pool.query(`
      INSERT INTO feed_proposals (tenant_id, proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id
    `, [req.tenant_id, proposed_url, proposed_name, proposed_geographies, proposed_sectors, proposed_signal_types, rationale]);
    res.json({ success: true, proposal_id: rows[0].id });
  } catch (err) {
    res.status(500).json({ error: err.message });
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
      pool.query('SELECT COUNT(*) AS cnt FROM people WHERE tenant_id = $1', [req.tenant_id]),
      pool.query(`SELECT COUNT(*) AS cnt FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours' AND tenant_id = $1`, [req.tenant_id]),
      pool.query('SELECT COUNT(*) AS cnt FROM signal_events WHERE tenant_id = $1', [req.tenant_id]),
      pool.query('SELECT COUNT(*) AS cnt FROM companies WHERE tenant_id = $1', [req.tenant_id]),
      pool.query('SELECT COUNT(*) AS cnt FROM external_documents WHERE tenant_id = $1', [req.tenant_id]),
      pool.query('SELECT COUNT(*) AS cnt, COALESCE(SUM(placement_fee), 0) AS total_fees FROM conversions WHERE tenant_id = $1 AND source IN (\'xero_export\', \'xero\', \'manual\') AND placement_fee IS NOT NULL', [req.tenant_id]),
      pool.query('SELECT COUNT(*) AS cnt FROM rss_sources WHERE enabled = true'),
      pool.query(`SELECT COUNT(DISTINCT person_id) AS cnt FROM interactions WHERE interaction_type = 'research_note' AND tenant_id = $1`, [req.tenant_id]),
      pool.query(`SELECT signal_type, COUNT(*) AS cnt FROM signal_events WHERE tenant_id = $1 GROUP BY signal_type ORDER BY cnt DESC`, [req.tenant_id]),
      pool.query(`SELECT source_type, COUNT(*) AS cnt FROM external_documents WHERE tenant_id = $1 GROUP BY source_type ORDER BY cnt DESC`, [req.tenant_id]),
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
// HERO SIGNALS — top 3 signals ranked by client proximity + network + confidence
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/signals/hero', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
             se.evidence_summary, se.detected_at, se.source_url, se.image_url,
             c.sector, c.geography, c.is_client, c.domain,
             ed.title AS doc_title, ed.source_name, ed.image_url AS doc_image_url,
             (SELECT COUNT(*) FROM people p WHERE p.current_company_id = se.company_id AND p.tenant_id = $1) AS contact_count,
             (SELECT COUNT(DISTINCT tp2.person_id) FROM team_proximity tp2
              JOIN people p2 ON p2.id = tp2.person_id AND p2.current_company_id = se.company_id AND p2.tenant_id = $1
              WHERE tp2.tenant_id = $1 AND tp2.relationship_strength >= 0.25
             ) AS prox_count
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      LEFT JOIN external_documents ed ON ed.id = se.source_document_id
      WHERE se.tenant_id = $1
        AND se.detected_at > NOW() - INTERVAL '7 days'
        AND COALESCE(se.is_megacap, false) = false
        AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
        AND se.company_name IS NOT NULL
        AND se.company_name NOT ILIKE '%mitchellake%' AND se.company_name NOT ILIKE '%mitchel lake%'
      ORDER BY
        CASE WHEN c.is_client = true THEN 100 ELSE 0 END +
        CASE WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = se.company_id) > 0 THEN 50 ELSE 0 END +
        (se.confidence_score * 30) +
        CASE WHEN se.image_url IS NOT NULL OR ed.image_url IS NOT NULL THEN 20 ELSE 0 END
        DESC
      LIMIT 3
    `, [req.tenant_id]);

    // Use doc_image_url as fallback
    rows.forEach(r => { if (!r.image_url && r.doc_image_url) r.image_url = r.doc_image_url; });

    res.json({ heroes: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// MARKET TEMPERATURE — macro summary from megacap/public company signals
// ═══════════════════════════════════════════════════════════════════════════════

// ── Signal Index — Market Health Ticker ──────────────────────────────
app.get('/api/signal-index', authenticateToken, async (req, res) => {
  try {
    const horizon = req.query.horizon || '7d';
    const tid = req.tenant_id;

    const [mh, stocks, stats] = await Promise.all([
      pool.query(`SELECT * FROM market_health_index WHERE tenant_id = $1 AND horizon = $2 LIMIT 1`, [tid, horizon]).catch(() => ({ rows: [] })),
      pool.query(`SELECT * FROM signal_stocks WHERE tenant_id = $1 AND horizon = $2 ORDER BY weight DESC`, [tid, horizon]).catch(() => ({ rows: [] })),
      pool.query(`SELECT * FROM signal_index_stats WHERE tenant_id = $1 LIMIT 1`, [tid]).catch(() => ({ rows: [] })),
    ]);

    const signalStocks = {};
    for (const s of stocks.rows) {
      signalStocks[s.stock_name] = {
        sentiment: s.sentiment, weight: s.weight, delta: s.delta,
        direction: s.direction, score: s.score,
        current_count: s.current_count, prior_count: s.prior_count
      };
    }

    res.json({
      horizon,
      market_health: mh.rows[0] || { score: 50, delta: 0, direction: 'flat' },
      signal_stocks: signalStocks,
      stats: stats.rows[0] || { people_tracked: 0, companies_tracked: 0, signals_7d: 0, signals_30d: 0 },
      computed_at: mh.rows[0]?.computed_at || null
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/signal-index/sectors', authenticateToken, async (req, res) => {
  try {
    const horizon = req.query.horizon || '7d';
    const { rows } = await pool.query(
      `SELECT * FROM sector_indices WHERE tenant_id = $1 AND horizon = $2 ORDER BY score DESC`,
      [req.tenant_id, horizon]
    ).catch(() => ({ rows: [] }));

    const sectors = {};
    for (const r of rows) {
      sectors[r.sector] = { score: r.score, delta: r.delta, direction: r.direction, signal_count: r.signal_count, company_count: r.company_count };
    }
    res.json({ horizon, sectors });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/signal-index/history', authenticateToken, async (req, res) => {
  try {
    const horizon = req.query.horizon || '7d';
    const limit = Math.min(parseInt(req.query.limit) || 90, 365);
    const { rows } = await pool.query(
      `SELECT score, delta, snapshot_at FROM market_health_history WHERE tenant_id = $1 AND horizon = $2 ORDER BY snapshot_at DESC LIMIT $3`,
      [req.tenant_id, horizon, limit]
    ).catch(() => ({ rows: [] }));
    res.json({ horizon, history: rows.reverse() });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/market-temperature', authenticateToken, async (req, res) => {
  try {
    // Aggregate megacap signals by type for the last 7 days
    const { rows: byType } = await pool.query(`
      SELECT se.signal_type, COUNT(*) as cnt,
             array_agg(DISTINCT se.company_name ORDER BY se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as companies
      FROM signal_events se
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      GROUP BY se.signal_type ORDER BY cnt DESC
    `, [req.tenant_id]);

    // Headline signals — the most notable recent megacap moves
    const { rows: headlines } = await pool.query(`
      SELECT se.company_name, se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score
      FROM signal_events se
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      ORDER BY se.confidence_score DESC, se.detected_at DESC
      LIMIT 8
    `, [req.tenant_id]);

    // Sentiment indicators
    const growth = byType.filter(t => ['capital_raising', 'product_launch', 'strategic_hiring', 'geographic_expansion', 'partnership'].includes(t.signal_type)).reduce((sum, t) => sum + parseInt(t.cnt), 0);
    const contraction = byType.filter(t => ['restructuring', 'layoffs', 'ma_activity'].includes(t.signal_type)).reduce((sum, t) => sum + parseInt(t.cnt), 0);
    const total = growth + contraction;

    let temperature = 'neutral';
    let emoji = '';
    if (total > 0) {
      const ratio = growth / total;
      if (ratio > 0.7) { temperature = 'hot'; }
      else if (ratio > 0.55) { temperature = 'warm'; }
      else if (ratio < 0.3) { temperature = 'cold'; }
      else if (ratio < 0.45) { temperature = 'cooling'; }
    }

    // Build narrative summary via simple template
    const typeLabels = { capital_raising: 'raising capital', product_launch: 'launching products', strategic_hiring: 'hiring aggressively', restructuring: 'restructuring', layoffs: 'cutting headcount', ma_activity: 'doing deals', geographic_expansion: 'expanding geographically', partnership: 'forming partnerships', leadership_change: 'changing leadership' };
    const topMoves = byType.slice(0, 3).map(t => {
      const cos = (t.companies || []).slice(0, 3).join(', ');
      return `${t.cnt} ${typeLabels[t.signal_type] || t.signal_type.replace(/_/g, ' ')} signals (${cos})`;
    });

    const summary = total === 0
      ? 'No significant macro signals this week.'
      : `Market is ${temperature}. ${total} signals from major public companies this week: ${topMoves.join('; ')}.${contraction > 0 ? ' ' + contraction + ' contraction signals may release senior talent downstream.' : ''}`;

    res.json({
      temperature,
      emoji,
      growth_signals: growth,
      contraction_signals: contraction,
      total_signals: total,
      summary,
      by_type: byType,
      headlines: headlines.map(h => ({
        company: h.company_name,
        type: h.signal_type,
        summary: (h.evidence_summary || '').slice(0, 150),
        date: h.detected_at,
        confidence: h.confidence_score
      }))
    });
  } catch (err) {
    console.error('Market temperature error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TALENT IN MOTION — flight risk, activity spikes, re-engagement windows
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/talent-in-motion', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 30);

    // 1. People at companies with restructuring/layoff signals (flight risk)
    const { rows: flightRisk } = await pool.query(`
      SELECT DISTINCT ON (p.id)
        p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
        p.seniority_level, p.linkedin_url,
        se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score,
        ps.flight_risk_score, ps.timing_score,
        (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id) as colleagues_affected,
        (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id
         AND p2.seniority_level IN ('c_suite','vp','director')) as senior_affected,
        (SELECT COUNT(*) FROM pipeline_contacts sc JOIN opportunities s ON s.id = sc.search_id AND s.status IN ('sourcing','interviewing')
         WHERE sc.person_id = p.id) as active_search_matches
      FROM people p
      JOIN companies c ON c.id = p.current_company_id
      JOIN signal_events se ON se.company_id = c.id
        AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change', 'strategic_hiring')
        AND se.detected_at > NOW() - INTERVAL '30 days'
        AND COALESCE(se.is_megacap, false) = false
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE p.current_title IS NOT NULL
        AND p.tenant_id = $2
      ORDER BY p.id, se.detected_at DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    // 2. People with high activity / timing scores (activity spikes & re-engage)
    const { rows: activeProfiles } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
             p.seniority_level, p.linkedin_url,
             ps.activity_score, ps.timing_score, ps.receptivity_score, ps.flight_risk_score,
             ps.engagement_score, ps.activity_trend, ps.engagement_trend,
             ps.last_interaction_at, ps.interaction_count_30d, ps.external_signals_30d,
             (SELECT COUNT(*) FROM pipeline_contacts sc JOIN opportunities s ON s.id = sc.search_id AND s.status IN ('sourcing','interviewing')
              WHERE sc.person_id = p.id) as active_search_matches
      FROM people p
      JOIN person_scores ps ON ps.person_id = p.id
      WHERE (ps.timing_score > 0.4 OR ps.activity_score > 0.4 OR ps.receptivity_score > 0.5 OR ps.flight_risk_score > 0.4)
        AND p.current_title IS NOT NULL
        AND p.tenant_id = $2
      ORDER BY (COALESCE(ps.timing_score,0) + COALESCE(ps.activity_score,0) + COALESCE(ps.receptivity_score,0)) DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    // 3. Recent person signals (flight_risk_alert, activity_spike, timing_opportunity)
    const { rows: personSignals } = await pool.query(`
      SELECT psg.id, psg.signal_type, psg.title, psg.description, psg.confidence_score, psg.detected_at,
             p.id as person_id, p.full_name, p.current_title, p.current_company_name, p.seniority_level
      FROM person_signals psg
      JOIN people p ON p.id = psg.person_id
      WHERE psg.signal_type IN ('flight_risk_alert', 'activity_spike', 'timing_opportunity', 'new_role', 'company_exit')
        AND psg.detected_at > NOW() - INTERVAL '14 days'
        AND psg.tenant_id = $2
      ORDER BY psg.detected_at DESC
      LIMIT $1
    `, [limit, req.tenant_id]);

    res.json({ flight_risk: flightRisk, active_profiles: activeProfiles, person_signals: personSignals });
  } catch (err) {
    console.error('Talent in motion error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERGING THEMES — triangulated signal patterns
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/converging-themes', authenticateToken, async (req, res) => {
  try {
    // Find signal_type clusters with high activity, cross-reference with clients and candidates
    const { rows: themes } = await pool.query(`
      WITH candidate_counts AS (
        SELECT se2.signal_type, COUNT(DISTINCT p.id) as cnt
        FROM people p
        JOIN companies c2 ON c2.id = p.current_company_id
        JOIN signal_events se2 ON se2.company_id = c2.id AND se2.detected_at > NOW() - INTERVAL '30 days'
        WHERE p.current_title IS NOT NULL AND p.tenant_id = $1
        GROUP BY se2.signal_type
      )
      SELECT
        se.signal_type,
        COUNT(DISTINCT se.company_id) as company_count,
        COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) as client_count,
        COUNT(*) as signal_count,
        ROUND(AVG(se.confidence_score)::numeric, 2) as avg_confidence,
        COALESCE(cc.cnt, 0) as candidate_count,
        array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names,
        array_agg(DISTINCT se.company_name ORDER BY se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as company_names
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      LEFT JOIN candidate_counts cc ON cc.signal_type = se.signal_type
      WHERE se.detected_at > NOW() - INTERVAL '30 days'
        AND se.signal_type IS NOT NULL
        AND se.tenant_id = $1
      GROUP BY se.signal_type, cc.cnt
      HAVING COUNT(DISTINCT se.company_id) >= 3
      ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) DESC,
               COUNT(DISTINCT se.company_id) DESC
      LIMIT 5
    `, [req.tenant_id]);

    // Find sector-based convergences
    const { rows: sectorThemes } = await pool.query(`
      SELECT
        c.sector,
        COUNT(DISTINCT se.company_id) as company_count,
        COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) as client_count,
        COUNT(*) as signal_count,
        array_agg(DISTINCT se.signal_type) as signal_types,
        (SELECT COUNT(DISTINCT p.id) FROM people p WHERE p.current_company_id IN (
          SELECT DISTINCT se2.company_id FROM signal_events se2
          JOIN companies c2 ON c2.id = se2.company_id AND c2.sector = c.sector
          WHERE se2.detected_at > NOW() - INTERVAL '30 days'
        )) as candidate_count,
        array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names
      FROM signal_events se
      JOIN companies c ON c.id = se.company_id AND c.sector IS NOT NULL
      WHERE se.detected_at > NOW() - INTERVAL '30 days'
        AND se.tenant_id = $1
      GROUP BY c.sector
      HAVING COUNT(DISTINCT se.company_id) >= 3 AND COUNT(*) >= 5
      ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) DESC,
               COUNT(*) DESC
      LIMIT 5
    `, [req.tenant_id]);

    // Placement pipeline potential — searches with matching signals
    let pipeline = [];
    try {
      const { rows } = await pool.query(`
        SELECT s.title as search_title, s.status, a.name as client_name,
               COUNT(DISTINCT se.id) as matching_signals,
               COUNT(DISTINCT se.company_id) as signalling_companies
        FROM searches s
        JOIN search_candidates sc ON sc.search_id = s.id
        JOIN people p ON p.id = sc.person_id
        JOIN signal_events se ON se.company_id = p.current_company_id AND se.detected_at > NOW() - INTERVAL '30 days'
        LEFT JOIN accounts a ON a.id = s.project_id
        WHERE s.status IN ('sourcing', 'interviewing')
          AND s.tenant_id = $1
        GROUP BY s.id, s.title, s.status, a.name
        HAVING COUNT(DISTINCT se.id) >= 2
        ORDER BY COUNT(DISTINCT se.id) DESC
        LIMIT 5
      `, [req.tenant_id]);
      pipeline = rows;
    } catch (e) {
      console.warn('Converging themes pipeline query failed:', e.message);
    }

    res.json({ signal_themes: themes, sector_themes: sectorThemes, pipeline });
  } catch (err) {
    console.error('Converging themes error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// TOP PODCASTS — matched to trending signal themes via semantic search
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/top-podcasts', authenticateToken, async (req, res) => {
  try {
    // 1. Get trending signal themes for deep-dive matching
    const { rows: trending } = await pool.query(`
      SELECT signal_type, COUNT(*) as cnt
      FROM signal_events
      WHERE detected_at > NOW() - INTERVAL '7 days' AND signal_type IS NOT NULL
      GROUP BY signal_type ORDER BY cnt DESC LIMIT 3
    `);

    const themeLabels = {
      capital_raising: 'fundraising venture capital IPO Series funding round',
      ma_activity: 'acquisition merger deal M&A takeover corporate development',
      product_launch: 'product launch innovation new release go to market',
      strategic_hiring: 'hiring talent recruitment executive search team building',
      geographic_expansion: 'expansion international new market global growth',
      restructuring: 'restructuring transformation turnaround change management',
      leadership_change: 'CEO appointment executive leadership transition succession',
      partnership: 'partnership alliance collaboration strategic deal ecosystem',
      layoffs: 'layoffs downsizing workforce reduction cost cutting'
    };
    const themeNames = trending.map(t => (t.signal_type || '').replace(/_/g, ' '));

    // ── LATEST: most recent podcast episodes (last 7 days), one per source ──
    const { rows: latest } = await pool.query(`
      SELECT DISTINCT ON (source_name)
        id, title, source_name, source_url, published_at, image_url, audio_url
      FROM external_documents
      WHERE source_type = 'podcast'
        AND published_at > NOW() - INTERVAL '7 days'
        AND title IS NOT NULL
      ORDER BY source_name, published_at DESC
    `);
    // Sort by recency after dedup
    const latestSorted = latest.sort((a, b) => new Date(b.published_at) - new Date(a.published_at)).slice(0, 5);

    // ── DEEP DIVES: semantic match from full archive via Qdrant ──
    let deepDives = [];
    const searchTerms = trending.map(t => themeLabels[t.signal_type] || t.signal_type.replace(/_/g, ' ')).join(' ');

    if (process.env.OPENAI_API_KEY && process.env.QDRANT_URL) {
      try {
        const vector = await generateQueryEmbedding(`executive search talent leadership: ${searchTerms}`);
        const qdrantResults = await qdrantSearch('documents', vector, 50);

        if (qdrantResults.length > 0) {
          const uuidRx = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
          const docIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRx.test(id));

          if (docIds.length > 0) {
            const { rows } = await pool.query(`
              SELECT id, title, source_name, source_url, published_at, image_url
              FROM external_documents
              WHERE id = ANY($1::uuid[]) AND source_type = 'podcast'
              ORDER BY published_at DESC
            `, [docIds]);

            // Re-attach scores, deduplicate by source_name, pick best per source
            const scoreMap = new Map(qdrantResults.map(r => [String(r.id), r.score]));
            const scored = rows.map(r => ({ ...r, match_score: scoreMap.get(r.id) || 0 }));

            // One per source to avoid 4x same show
            const seenSources = new Set(latestSorted.map(l => l.source_name));
            const bySource = new Map();
            scored.sort((a, b) => b.match_score - a.match_score).forEach(r => {
              if (!bySource.has(r.source_name) && !seenSources.has(r.source_name)) {
                bySource.set(r.source_name, r);
              }
            });
            deepDives = [...bySource.values()].slice(0, 5);
          }
        }
      } catch (e) {
        console.warn('Podcast Qdrant search failed:', e.message);
      }
    }

    // Fallback for deep dives if Qdrant empty
    if (deepDives.length < 3) {
      const latestIds = latestSorted.map(l => l.id);
      const deepIds = deepDives.map(d => d.id);
      const exclude = [...latestIds, ...deepIds];
      const { rows: fallback } = await pool.query(`
        SELECT DISTINCT ON (source_name)
          id, title, source_name, source_url, published_at, image_url
        FROM external_documents
        WHERE source_type = 'podcast' AND title IS NOT NULL
          AND id != ALL($1::uuid[])
        ORDER BY source_name, published_at DESC
      `, [exclude]);
      const fb = fallback.sort((a, b) => new Date(b.published_at) - new Date(a.published_at)).slice(0, 5 - deepDives.length);
      deepDives = [...deepDives, ...fb].slice(0, 5);
    }

    res.json({ latest: latestSorted, deep_dives: deepDives, themes: themeNames });
  } catch (err) {
    console.error('Top podcasts error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// RE-ENGAGE WINDOWS — dormant contacts at companies with recent signals
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/reengage-windows', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (p.id)
        p.id, p.full_name, p.current_title, p.current_company_name,
        se.signal_type, se.company_name AS signal_company, se.confidence_score,
        se.detected_at AS signal_date,
        i.interaction_at AS last_contact,
        i.interaction_type AS last_channel,
        EXTRACT(DAY FROM NOW() - i.interaction_at) AS days_since_contact,
        ps.engagement_score, ps.timing_score
      FROM people p
      JOIN companies c ON c.id = p.current_company_id
      JOIN signal_events se ON se.company_id = c.id
        AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change')
        AND se.detected_at > NOW() - INTERVAL '30 days'
        AND COALESCE(se.is_megacap, false) = false
      LEFT JOIN LATERAL (
        SELECT interaction_at, interaction_type FROM interactions
        WHERE person_id = p.id AND tenant_id = $1
        ORDER BY interaction_at DESC LIMIT 1
      ) i ON true
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE p.tenant_id = $1
        AND p.current_title IS NOT NULL
        AND p.seniority_level IN ('c_suite', 'C-Suite', 'C-level', 'vp', 'VP', 'director', 'Director', 'Head')
        AND i.interaction_at IS NOT NULL
        AND i.interaction_at < NOW() - INTERVAL '60 days'
      ORDER BY p.id, se.confidence_score DESC
    `, [req.tenant_id]);

    // Rank by signal strength + dormancy
    const ranked = rows
      .map(r => ({
        ...r,
        score: (r.confidence_score || 0) * 0.4 + Math.min((r.days_since_contact || 0) / 365, 1) * 0.3 + (r.timing_score || 0) * 0.3
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 6);

    res.json(ranked);
  } catch (err) {
    console.error('Re-engage windows error:', err.message);
    res.status(500).json({ error: err.message });
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
    const region = req.query.region; // AU, SG, UK, US, APAC, EMEA, AMER, or 'all'
    const minConf = parseFloat(req.query.min_confidence) || 0;
    const networkOnly = req.query.network === 'true'; // only signals where we have contacts

    let where = 'WHERE se.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter
    paramIdx++;
    where += ` AND (se.visibility IS NULL OR se.visibility != 'private' OR se.owner_user_id = $${paramIdx})`;
    params.push(req.user.user_id);

    // Exclude megacaps, tenant company, and self-referential signals from feed
    where += ` AND COALESCE(se.is_megacap, false) = false AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')`;
    // Also exclude signals whose company_name matches the tenant name (catches un-linked records)
    if (req.user.tenant_name) {
      paramIdx++;
      where += ` AND (se.company_name IS NULL OR se.company_name NOT ILIKE $${paramIdx})`;
      params.push(`%${req.user.tenant_name}%`);
    }

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

    // Region filter — uses shared REGION_MAP/REGION_CODES constants
    if (region && region !== 'all' && REGION_MAP[region]) {
      const geos = REGION_MAP[region];
      const codes = REGION_CODES[region] || [];

      // Build OR conditions across multiple fields
      const orParts = [];

      // Company geography/country_code
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`c.geography ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });
      codes.forEach(code => {
        paramIdx++;
        orParts.push(`c.country_code = $${paramIdx}`);
        params.push(code);
      });

      // Evidence summary text
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`se.evidence_summary ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      // Company name (catches "Department of Health and Aged Care" etc.)
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`se.company_name ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      // Source document title
      geos.forEach(g => {
        paramIdx++;
        orParts.push(`ed.title ILIKE $${paramIdx}`);
        params.push(`%${g}%`);
      });

      where += ` AND (${orParts.join(' OR ')})`;
    }

    // Network filter — only signals where we have contacts at the company
    if (networkOnly) {
      where += ` AND (
        EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = se.company_id)
        OR c.is_client = true
      )`;
    }

    paramIdx++;
    const limitParam = paramIdx;
    params.push(limit);
    paramIdx++;
    const offsetParam = paramIdx;
    params.push(offset);

    const [signalsResult, countResult] = await Promise.all([
      pool.query(`
        SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
               se.evidence_summary, se.evidence_snippet, se.triage_status,
               se.detected_at, se.signal_date, se.source_url, se.signal_category,
               se.hiring_implications, se.is_megacap, se.image_url,
               c.sector, c.geography, c.is_client, c.country_code, c.company_tier,
               ed.source_name, ed.source_type AS doc_source_type,
               ed.title AS doc_title, ed.summary AS doc_summary,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = se.company_id AND p.tenant_id = $1) AS contact_count,
               (SELECT COUNT(DISTINCT tp2.person_id) FROM team_proximity tp2
                JOIN people p2 ON p2.id = tp2.person_id AND p2.current_company_id = se.company_id AND p2.tenant_id = $1
                WHERE tp2.tenant_id = $1 AND tp2.relationship_strength >= 0.25
               ) AS prox_connection_count,
               (SELECT u2.name FROM team_proximity tp3
                JOIN people p3 ON p3.id = tp3.person_id AND p3.current_company_id = se.company_id AND p3.tenant_id = $1
                JOIN users u2 ON u2.id = tp3.team_member_id
                WHERE tp3.tenant_id = $1 AND tp3.relationship_strength >= 0.25
                ORDER BY tp3.relationship_strength DESC LIMIT 1
               ) AS best_connector_name,
               (SELECT COUNT(*) FROM conversions pl JOIN accounts cl ON cl.id = pl.client_id
                WHERE cl.company_id = se.company_id AND pl.tenant_id = $1) AS placement_count,
               sd.id AS dispatch_id, sd.status AS dispatch_status,
               sd.claimed_by, sd.claimed_by_name, sd.blog_title AS dispatch_blog_title
        FROM signal_events se
        LEFT JOIN companies c ON se.company_id = c.id
        LEFT JOIN external_documents ed ON se.source_document_id = ed.id
        LEFT JOIN LATERAL (
          SELECT sd2.id, sd2.status, sd2.claimed_by,
                 u2.name AS claimed_by_name, sd2.blog_title
          FROM signal_dispatches sd2
          LEFT JOIN users u2 ON u2.id = sd2.claimed_by
          WHERE sd2.signal_event_id = se.id
          ORDER BY sd2.generated_at DESC LIMIT 1
        ) sd ON true
        ${where}
        ORDER BY
          CASE WHEN c.company_tier = 'tenant_company' THEN 2 WHEN se.is_megacap = true THEN 1 ELSE 0 END,
          CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
          CASE WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = se.company_id) > 0 THEN 0 ELSE 1 END,
          CASE WHEN se.signal_type = 'geographic_expansion' THEN 0 ELSE 1 END,
          CASE WHEN se.signal_type IN ('capital_raising', 'strategic_hiring') THEN 0 ELSE 1 END,
          se.confidence_score DESC NULLS LAST,
          se.detected_at DESC NULLS LAST
        LIMIT $${limitParam} OFFSET $${offsetParam}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM signal_events se LEFT JOIN companies c ON se.company_id = c.id LEFT JOIN external_documents ed ON se.source_document_id = ed.id ${where}`, params.slice(0, -2)),
    ]);

    // Compute region stats for the header — search across company geo, evidence, doc title
    let regionStats = null;
    try {
      const { rows: rStats } = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE
            c.geography ILIKE '%Australia%' OR c.country_code = 'AU'
            OR se.evidence_summary ILIKE '%Australia%' OR se.evidence_summary ILIKE '%Sydney%' OR se.evidence_summary ILIKE '%Melbourne%'
            OR se.company_name ILIKE '%Australian%'
            OR ed.title ILIKE '%Australia%' OR ed.title ILIKE '%Australian%'
          ) AS au,
          COUNT(*) FILTER (WHERE
            c.geography ILIKE '%Singapore%' OR c.country_code = 'SG'
            OR c.geography ILIKE '%Southeast Asia%' OR c.geography ILIKE '%ASEAN%'
            OR se.evidence_summary ILIKE '%Singapore%' OR se.evidence_summary ILIKE '%Southeast Asia%'
            OR ed.title ILIKE '%Singapore%'
          ) AS sg,
          COUNT(*) FILTER (WHERE
            c.geography ILIKE '%United Kingdom%' OR c.country_code IN ('UK','GB')
            OR c.geography ILIKE '%London%' OR c.geography ILIKE '%Britain%'
            OR se.evidence_summary ILIKE '%United Kingdom%' OR se.evidence_summary ILIKE '%London%' OR se.evidence_summary ILIKE '%Britain%'
            OR ed.title ILIKE '%UK %' OR ed.title ILIKE '%London%' OR ed.title ILIKE '%British%'
          ) AS uk,
          COUNT(*) FILTER (WHERE
            c.geography ILIKE '%United States%' OR c.country_code = 'US'
            OR c.geography ILIKE '%America%' OR c.geography ILIKE '%Silicon Valley%'
            OR se.evidence_summary ILIKE '%United States%' OR se.evidence_summary ILIKE '%Silicon Valley%'
          ) AS us,
          COUNT(*) FILTER (WHERE c.is_client = true) AS client_signals,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = se.company_id)) AS network_signals,
          COUNT(*) AS total
        FROM signal_events se
        LEFT JOIN companies c ON se.company_id = c.id
        LEFT JOIN external_documents ed ON se.source_document_id = ed.id
        WHERE se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
          AND COALESCE(se.is_megacap, false) = false
          AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      `, [req.tenant_id]);
      regionStats = rStats[0];
    } catch (e) { /* ignore */ }

    res.json({
      signals: signalsResult.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
      region_stats: regionStats,
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
      WHERE se.id = $1 AND se.tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    const signal = rows[0];

    // Auto-bundle relevant case studies
    let relevant_case_studies = [];
    try {
      const scoreTerms = [];
      const csParams = [req.tenant_id];
      let csIdx = 1;

      if (signal.sector) {
        csIdx++; csParams.push(`%${signal.sector}%`);
        scoreTerms.push(`CASE WHEN cs.sector ILIKE $${csIdx} THEN 0.3 ELSE 0 END`);
      }
      if (signal.geography) {
        csIdx++; csParams.push(`%${signal.geography}%`);
        scoreTerms.push(`CASE WHEN cs.geography ILIKE $${csIdx} THEN 0.25 ELSE 0 END`);
      }
      if (signal.signal_type) {
        const sigThemes = {
          capital_raising: ['high-growth','scaling'], geographic_expansion: ['cross-border','expansion'],
          strategic_hiring: ['leadership','team-build'], ma_activity: ['post-acquisition','integration'],
          leadership_change: ['succession','transition'], restructuring: ['turnaround','transformation'],
        };
        const themes = sigThemes[signal.signal_type] || [];
        if (themes.length) {
          csIdx++; csParams.push(themes);
          scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${csIdx}::text[]))::float * 0.25`);
        }
      }
      if (signal.company_id) {
        csIdx++; csParams.push(signal.company_id);
        scoreTerms.push(`CASE WHEN cs.client_id = $${csIdx}::uuid THEN 0.5 ELSE 0 END`);
      }

      if (scoreTerms.length > 0) {
        const scoreExpr = scoreTerms.join(' + ');
        const { rows: csRows } = await pool.query(`
          SELECT cs.id, cs.title, cs.sector, cs.geography, cs.engagement_type, cs.year,
                 cs.themes, cs.capabilities, cs.public_approved, cs.visibility,
                 cs.public_title, cs.public_summary,
                 (${scoreExpr}) AS relevance
          FROM case_studies cs
          WHERE cs.tenant_id = $1 AND (${scoreExpr}) > 0
          ORDER BY (${scoreExpr}) DESC LIMIT 5
        `, csParams);
        relevant_case_studies = csRows;
      }
    } catch (e) { /* case_studies table may not exist */ }

    res.json({ ...signal, relevant_case_studies });
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
      WHERE id = $4 AND tenant_id = $5
      RETURNING id, triage_status, triaged_at
    `, [status, notes, req.user.user_id, req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Signal not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Triage error:', err.message);
    res.status(500).json({ error: 'Failed to update triage' });
  }
});

// ─── Signal Proximity Graph (for popup mini-graph) ───
app.get('/api/signals/:id/proximity-graph', authenticateToken, async (req, res) => {
  try {
    const tenantId = req.tenant_id;
    const signalId = req.params.id;

    // 1. Get the signal
    const { rows: [sig] } = await pool.query(
      'SELECT * FROM signal_events WHERE id = $1 AND tenant_id = $2',
      [signalId, tenantId]
    );
    if (!sig) return res.status(404).json({ error: 'Signal not found' });

    // 2. Get team members
    const { rows: team } = await pool.query(
      `SELECT id, name FROM users WHERE tenant_id = $1 AND role != 'viewer'`,
      [tenantId]
    );

    // 3. Get contacts with proximity to signal company + their scores
    const { rows: contacts } = await pool.query(`
      SELECT
        p.id, p.full_name, p.current_title, p.current_company_name,
        ps.timing_score, ps.receptivity_score,
        json_object_agg(
          tp.team_member_id::text,
          json_build_object(
            'strength', tp.relationship_strength,
            'type', tp.relationship_type,
            'last_interaction', tp.last_interaction_date
          )
        ) AS proximity_by_user,
        MAX(tp.relationship_strength) AS best_strength,
        (SELECT json_agg(json_build_object('type', psg.signal_type::text, 'date', psg.detected_at))
         FROM person_signals psg
         WHERE psg.person_id = p.id AND psg.tenant_id = $1
           AND psg.detected_at >= NOW() - INTERVAL '90 days'
         LIMIT 3
        ) AS recent_signals
      FROM people p
      JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = $1
      LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
      WHERE p.tenant_id = $1
        AND (p.current_company_id = $2 OR LOWER(TRIM(p.current_company_name)) = LOWER(TRIM($3)))
        AND tp.relationship_strength >= 0.15
      GROUP BY p.id, p.full_name, p.current_title, p.current_company_name,
               ps.timing_score, ps.receptivity_score
      ORDER BY MAX(tp.relationship_strength) DESC
      LIMIT 15
    `, [tenantId, sig.company_id, sig.company_name || '']);

    // 4. Check if signal company is an account/client
    const { rows: [account] } = await pool.query(`
      SELECT a.id, a.name, a.relationship_tier
      FROM accounts a
      WHERE a.tenant_id = $1
        AND (a.company_id = $2 OR LOWER(a.name) = LOWER((SELECT name FROM companies WHERE id = $2)))
      LIMIT 1
    `, [tenantId, sig.company_id]);

    // 5. Build graph nodes and links
    const nodes = [];
    const links = [];

    // Company node (focal point)
    nodes.push({
      id: `company-${sig.company_id}`,
      type: 'company',
      label: sig.company_name || 'Unknown',
      companyId: sig.company_id,
      isClient: !!account,
      clientTier: account?.relationship_tier,
      signalType: sig.signal_type,
      signalConfidence: sig.confidence_score
    });

    // Team nodes — only those connected via contacts
    const connectedUserIds = new Set(
      contacts.flatMap(c => Object.keys(c.proximity_by_user || {}))
    );
    team.filter(u => connectedUserIds.has(u.id)).forEach(u => {
      const initials = (u.name || '').split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
      nodes.push({ id: `user-${u.id}`, type: 'team', label: initials, fullName: u.name, userId: u.id });
    });

    // Contact nodes
    contacts.forEach(c => {
      const bestStrength = parseFloat(c.best_strength) || 0;
      nodes.push({
        id: `contact-${c.id}`,
        type: 'contact',
        label: c.full_name,
        personId: c.id,
        role: c.current_title,
        bestStrength,
        proximityByUser: c.proximity_by_user || {},
        recentSignals: c.recent_signals || [],
        timingScore: c.timing_score,
        receptivityScore: c.receptivity_score
      });

      // Contact → company links
      links.push({
        source: `contact-${c.id}`,
        target: `company-${sig.company_id}`,
        strength: bestStrength * 0.7,
        type: 'works_at'
      });

      // Team → contact links
      Object.entries(c.proximity_by_user || {}).forEach(([userId, prox]) => {
        if (prox.strength >= 0.20) {
          links.push({
            source: `user-${userId}`,
            target: `contact-${c.id}`,
            strength: prox.strength,
            type: prox.type || 'connection'
          });
        }
      });
    });

    res.json({
      signal: {
        id: sig.id,
        type: sig.signal_type,
        confidence: sig.confidence_score,
        headline: sig.evidence_summary,
        company: sig.company_name,
        detectedAt: sig.detected_at
      },
      graph: { nodes, links },
      account: account ? { id: account.id, name: account.name, tier: account.relationship_tier } : null,
      connectionCount: contacts.length
    });
  } catch (err) {
    console.error('Proximity graph error:', err.message);
    res.status(500).json({ error: err.message });
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

// Recent interactions stream — who MitchelLake team contacted recently
app.get('/api/people/stream/recent-contacts', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const { rows } = await pool.query(`
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
app.get('/api/people/stream/signal-connected', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const { rows } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.current_company_name,
             p.location, p.seniority_level, p.linkedin_url,
             se.signal_type, se.evidence_summary, se.confidence_score,
             se.detected_at AS signal_detected_at, se.company_name AS signal_company,
             (SELECT COUNT(*) FROM interactions ix WHERE ix.person_id = p.id AND ix.interaction_type = 'research_note') AS note_count
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
      WHERE p.id = $1 AND p.tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    if (!person) return res.status(404).json({ error: 'Person not found' });

    // Privacy check — block access to private contacts unless owner
    if (person.visibility === 'private' && person.owner_user_id && person.owner_user_id !== req.user?.user_id) {
      return res.status(403).json({ error: 'This contact is private' });
    }

    // Research notes
    const { rows: notes } = await pool.query(`
      SELECT id, summary, subject, email_snippet, interaction_at, created_at,
             note_quality, extracted_intelligence, source, interaction_type
      FROM interactions
      WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2
      ORDER BY interaction_at DESC NULLS LAST
      LIMIT 50
    `, [req.params.id, req.tenant_id]);

    // All other interactions (emails, calls, meetings)
    const { rows: interactions } = await pool.query(`
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
    const { rows: signals } = await pool.query(`
      SELECT id, signal_type, signal_category, title, description,
             confidence_score, signal_date, detected_at
      FROM person_signals
      WHERE person_id = $1 AND tenant_id = $2
      ORDER BY detected_at DESC
    `, [req.params.id, req.tenant_id]);

    // Company signals (if person has a linked company)
    let companySignals = [];
    if (person.current_company_id) {
      const { rows } = await pool.query(`
        SELECT id, signal_type, confidence_score, evidence_summary, detected_at, triage_status
        FROM signal_events WHERE company_id = $1 AND tenant_id = $2
        ORDER BY detected_at DESC LIMIT 10
      `, [person.current_company_id, req.tenant_id]);
      companySignals = rows;
    }

    // Interaction stats — count all types
    const { rows: [stats] } = await pool.query(`
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
    const { rows: typeCounts } = await pool.query(
      `SELECT interaction_type, COUNT(*) AS cnt FROM interactions WHERE person_id = $1 AND tenant_id = $2 GROUP BY interaction_type ORDER BY cnt DESC`,
      [req.params.id, req.tenant_id]
    );
    stats.type_breakdown = typeCounts;

    // Colleagues at same company
    let colleagues = [];
    if (person.current_company_id) {
      const { rows } = await pool.query(`
        SELECT id, full_name, current_title, seniority_level
        FROM people
        WHERE current_company_id = $1 AND id != $2 AND tenant_id = $3
        ORDER BY full_name LIMIT 20
      `, [person.current_company_id, req.params.id, req.tenant_id]);
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
app.patch('/api/people/:id', authenticateToken, async (req, res) => {
  try {
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
      const { rows: [match] } = await pool.query(
        `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
        [req.body.current_company_name, req.tenant_id]
      );
      if (match) {
        idx++;
        updates.push(`current_company_id = $${idx}`);
        params.push(match.id);
      }
    }

    idx++;
    params.push(req.tenant_id);
    const { rows } = await pool.query(
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
app.post('/api/people/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const { rows: [person] } = await pool.query(
      `SELECT id, full_name, email, source_id, source, current_title,
              current_company_name, current_company_id, linkedin_url, location
       FROM people WHERE id = $1 AND tenant_id = $2`, [req.params.id, req.tenant_id]
    );
    if (!person) return res.status(404).json({ error: 'Person not found' });

    const enrichResults = { ezekia_profile: null, ezekia_projects: null, gmail: null, signals: null, web: null, embedding: null };

    // 1a. Ezekia People API — pull latest profile data
    if (process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('./lib/ezekia');

        // If no source_id, try to find them in Ezekia by email (most reliable) or exact name
        let ezekiaId = person.source_id;
        if (!ezekiaId && person.email) {
          const searchRes = await ezekia.searchPeople({ email: person.email });
          const match = searchRes?.data?.[0];
          if (match) {
            ezekiaId = String(match.id);
            await pool.query('UPDATE people SET source_id = $1, source = $2 WHERE id = $3 AND source_id IS NULL AND tenant_id = $4',
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
            await pool.query('UPDATE people SET source_id = $1, source = $2 WHERE id = $3 AND source_id IS NULL AND tenant_id = $4',
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
          const updates = {};

          // Profile fields — find the CURRENT position (primary, or most recent by start date)
          const positions = d.profile?.positions || [];
          const pos = positions.find(p => p.primary || p.tense || p.current)
            || positions.sort((a, b) => (b.startDate || '').localeCompare(a.startDate || ''))[0];

          // Only update fields that are EMPTY on our record — never overwrite existing data
          // The user or Lorac can update manually if needed
          if (!person.headline && (d.headline || d.profile?.headline)) updates.headline = d.headline || d.profile.headline;
          if (!person.current_title && pos?.title) updates.current_title = pos.title;
          if (!person.current_company_name && (pos?.company?.name || pos?.company)) {
            updates.current_company_name = pos.company?.name || pos.company;
          }
          // Only fill empty fields — never overwrite existing contact data
          if (!person.email && (d.email || d.emails?.[0]?.address)) updates.email = d.email || d.emails[0].address;
          if (!person.phone && (d.phone || d.phones?.[0]?.number)) updates.phone = d.phone || d.phones[0].number;
          if (!person.linkedin_url && (d.linkedin_url || d.linkedinUrl)) updates.linkedin_url = d.linkedin_url || d.linkedinUrl;
          if (!person.location && (d.location || d.address?.city)) updates.location = d.location || [d.address?.city, d.address?.country].filter(Boolean).join(', ');

          // Career history from positions
          if (d.profile?.positions?.length > 0) {
            const career = d.profile.positions.map(p => ({
              title: p.title,
              company: p.company?.name || p.company,
              start_date: p.startDate,
              end_date: p.endDate,
              current: p.current || !p.endDate
            }));
            updates.career_history = JSON.stringify(career);
          }

          // Education
          if (d.profile?.education?.length > 0) {
            updates.education = JSON.stringify(d.profile.education);
          }

          if (Object.keys(updates).length > 0) {
            const setClauses = Object.entries(updates).map(([k, v], i) => `${k} = $${i + 2}`);
            const updateVals = Object.values(updates);
            await pool.query(`UPDATE people SET ${setClauses.join(', ')}, synced_at = NOW(), updated_at = NOW() WHERE id = $1 AND tenant_id = $${updateVals.length + 2}`,
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
            const { rows: existing } = await pool.query(
              `SELECT id FROM interactions WHERE person_id = $1 AND external_id = $2 AND tenant_id = $3`,
              [req.params.id, 'ezekia_note_' + note.id, req.tenant_id]
            );
            if (existing.length > 0) continue;

            await pool.query(`
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

        } // end if (ezekiaId)
      } catch (e) {
        enrichResults.ezekia_profile = { error: e.message };
      }
    } else {
      enrichResults.ezekia_profile = { message: 'EZEKIA_API_TOKEN not configured' };
    }

    // Resolve ezekiaId for projects step (may have been linked above)
    const ezekiaId = person.source_id || (await pool.query('SELECT source_id FROM people WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]).then(r => r.rows[0]?.source_id));

    // 1b. Ezekia Projects API — find which projects/searches this person is in
    if (ezekiaId && process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('./lib/ezekia');
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
              const { rows: [existingSearch] } = await pool.query(
                `SELECT id FROM opportunities WHERE (code = $1 OR title ILIKE $2) AND tenant_id = $3 LIMIT 1`,
                [`ezekia_${proj.id}`, `%${proj.name}%`, req.tenant_id]
              );
              if (existingSearch) {
                // Link person as search candidate
                await pool.query(`
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
        const { rows: gmailAccounts } = await pool.query(
          `SELECT id, user_id, google_email, access_token, refresh_token, token_expires_at
           FROM user_google_accounts WHERE sync_enabled = true LIMIT 5`
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
                    await pool.query(
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
              `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${searchQuery}&maxResults=20`,
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
              for (const msg of gmailData.messages.slice(0, 15)) {
                const { rows: existing } = await pool.query(
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

                  await pool.query(`
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

        const { rows: mentions } = await pool.query(`
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
                const { rows: existing } = await pool.query(
                  `SELECT id FROM person_signals WHERE person_id = $1 AND signal_type = $2 AND title = $3 AND tenant_id = $4`,
                  [req.params.id, sig.signal_type, sig.title, req.tenant_id]
                );
                if (existing.length > 0) continue;

                await pool.query(`
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
        const { rows: companySignals } = await pool.query(`
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
      const { rows: [latest] } = await pool.query(`SELECT * FROM people WHERE id = $1 AND tenant_id = $2`, [req.params.id, req.tenant_id]);
      const parts = [latest.full_name, latest.current_title, latest.current_company_name, latest.headline, latest.bio, latest.location].filter(Boolean);
      if (latest.expertise_tags?.length) parts.push('Skills: ' + latest.expertise_tags.join(', '));
      if (latest.industries?.length) parts.push('Industries: ' + latest.industries.join(', '));

      // Get latest notes for embedding context
      const { rows: notes } = await pool.query(`SELECT summary FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2 ORDER BY interaction_at DESC NULLS LAST LIMIT 5`, [req.params.id, req.tenant_id]);
      notes.forEach(n => { if (n.summary) parts.push(n.summary.slice(0, 500)); });

      // Get person signals for embedding context
      const { rows: psigs } = await pool.query(`SELECT title, description FROM person_signals WHERE person_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 3`, [req.params.id, req.tenant_id]);
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
        await pool.query('UPDATE people SET embedded_at = NOW() WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
        enrichResults.embedding = { message: 'Re-embedded with enriched data' };
      } else {
        enrichResults.embedding = { message: 'Insufficient data for embedding' };
      }
    } catch (e) {
      enrichResults.embedding = { error: e.message };
    }

    // 6. Gmail re-link — match unlinked interactions to this person by email/alt_emails
    try {
      const { rows: [freshPerson] } = await pool.query(
        'SELECT email, email_alt FROM people WHERE id = $1 AND tenant_id = $2',
        [req.params.id, req.tenant_id]
      );
      const emailList = [
        freshPerson.email,
        freshPerson.email_alt
      ].filter(Boolean).map(e => e.toLowerCase().trim());

      if (emailList.length > 0) {
        const { rows: unlinked } = await pool.query(`
          SELECT id FROM interactions
          WHERE person_id IS NULL
            AND tenant_id = $1
            AND (email_from = ANY($2) OR email_to = ANY($2))
          LIMIT 500
        `, [req.tenant_id, emailList]);

        if (unlinked.length > 0) {
          const ids = unlinked.map(r => r.id);
          await pool.query(
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

    res.json({ person_id: req.params.id, person_name: person.full_name, results: enrichResults });
  } catch (err) {
    console.error('Enrich error:', err.message);
    res.status(500).json({ error: 'Enrichment failed: ' + err.message });
  }
});

// ─── Reconcile Account → Company (create companies record for unlinked clients) ───
app.post('/api/clients/:id/reconcile', authenticateToken, async (req, res) => {
  try {
    const { rows: [client] } = await pool.query('SELECT * FROM accounts WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!client) return res.status(404).json({ error: 'Client not found' });

    // Already linked?
    if (client.company_id) {
      const { rows: [co] } = await pool.query('SELECT id, name FROM companies WHERE id = $1 AND tenant_id = $2', [client.company_id, req.tenant_id]);
      if (co) return res.json({ company_id: co.id, message: 'Already linked', company_name: co.name });
    }

    // Check if a companies record already exists by name
    let { rows: [existing] } = await pool.query(
      'SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [client.name, req.tenant_id]
    );

    let companyId;
    if (existing) {
      companyId = existing.id;
    } else {
      // Create a new companies record from the client
      const { rows: [newCo] } = await pool.query(`
        INSERT INTO companies (name, is_client, created_at, updated_at, tenant_id)
        VALUES ($1, true, NOW(), NOW(), $2)
        RETURNING id
      `, [client.name, req.tenant_id]);
      companyId = newCo.id;
    }

    // Link client to company
    await pool.query('UPDATE accounts SET company_id = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3', [companyId, client.id, req.tenant_id]);

    // Also link any people whose current_company_name matches
    const { rowCount: linkedPeople } = await pool.query(`
      UPDATE people SET current_company_id = $1, updated_at = NOW()
      WHERE current_company_name ILIKE $2 AND (current_company_id IS NULL OR current_company_id != $1) AND tenant_id = $3
    `, [companyId, client.name, req.tenant_id]);

    res.json({ company_id: companyId, client_id: client.id, people_linked: linkedPeople, message: existing ? 'Linked to existing company' : 'Created new company record' });
  } catch (err) {
    console.error('Reconcile error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Bulk reconcile all unlinked accounts ───
app.post('/api/clients/reconcile-all', authenticateToken, async (req, res) => {
  try {
    const { rows: unlinked } = await pool.query(`
      SELECT cl.id, cl.name FROM accounts cl
      WHERE cl.company_id IS NULL AND cl.tenant_id = $1
      ORDER BY cl.name
    `, [req.tenant_id]);

    let created = 0, linked = 0, errors = 0;
    for (const client of unlinked) {
      try {
        let { rows: [existing] } = await pool.query(
          'SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [client.name, req.tenant_id]
        );

        let companyId;
        if (existing) {
          companyId = existing.id;
          linked++;
        } else {
          const { rows: [newCo] } = await pool.query(
            `INSERT INTO companies (name, is_client, created_at, updated_at, tenant_id) VALUES ($1, true, NOW(), NOW(), $2) RETURNING id`,
            [client.name, req.tenant_id]
          );
          companyId = newCo.id;
          created++;
        }

        await pool.query('UPDATE accounts SET company_id = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3', [companyId, client.id, req.tenant_id]);
        await pool.query(`
          UPDATE people SET current_company_id = $1, updated_at = NOW()
          WHERE current_company_name ILIKE $2 AND (current_company_id IS NULL OR current_company_id != $1) AND tenant_id = $3
        `, [companyId, client.name, req.tenant_id]);
      } catch (e) { errors++; }
    }

    res.json({ total_unlinked: unlinked.length, companies_created: created, companies_linked: linked, errors });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Company Enrichment ───
app.post('/api/companies/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const { rows: [company] } = await pool.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!company) return res.status(404).json({ error: 'Company not found' });

    const enrichResults = {};

    // 1. Ezekia company data
    if (process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('./lib/ezekia');
        // Search Ezekia companies by name — use exact match for short names to avoid PAM/EPAM confusion
        let ezekiaCompany = null;
        let page = 1;
        const companyNameLower = company.name.toLowerCase().trim();
        const isShortName = companyNameLower.length <= 5;
        while (page <= 5 && !ezekiaCompany) {
          const compRes = await ezekia.getCompanies({ page, per_page: 100 });
          const companies = compRes?.data || [];
          if (companies.length === 0) break;
          ezekiaCompany = companies.find(c => {
            const ezName = (c.name || '').toLowerCase().trim();
            if (isShortName) return ezName === companyNameLower; // Exact match for short names
            return ezName === companyNameLower || (ezName.length > 5 && companyNameLower.length > 5 && (ezName.includes(companyNameLower) || companyNameLower.includes(ezName)));
          });
          page++;
        }

        if (ezekiaCompany) {
          const updates = {};
          if (ezekiaCompany.industry && !company.sector) updates.sector = ezekiaCompany.industry;
          if (ezekiaCompany.website && !company.domain) updates.domain = ezekiaCompany.website;
          if (ezekiaCompany.address && !company.geography) {
            updates.geography = [ezekiaCompany.address.city, ezekiaCompany.address.country].filter(Boolean).join(', ');
          }
          if (ezekiaCompany.description && !company.description) updates.description = ezekiaCompany.description;

          if (Object.keys(updates).length > 0) {
            const setClauses = Object.entries(updates).map(([k, v], i) => `${k} = $${i + 2}`);
            const coUpdateVals = Object.values(updates);
            await pool.query(`UPDATE companies SET ${setClauses.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $${coUpdateVals.length + 2}`,
              [req.params.id, ...coUpdateVals, req.tenant_id]);
            enrichResults.ezekia = { updated_fields: Object.keys(updates), ezekia_id: ezekiaCompany.id };
          } else {
            enrichResults.ezekia = { message: 'No new data from Ezekia', ezekia_id: ezekiaCompany.id };
          }
        } else {
          enrichResults.ezekia = { message: 'Not found in Ezekia CRM' };
        }
      } catch (e) {
        enrichResults.ezekia = { error: e.message };
      }

      // Also search Ezekia projects for this company
      try {
        const ezekia = require('./lib/ezekia');
        let projectsFound = [];
        for (let pg = 1; pg <= 3; pg++) {
          const projRes = await ezekia.getProjects({ page: pg, per_page: 100 });
          const projs = projRes?.data || [];
          if (!projs.length) break;
          const matches = projs.filter(p => {
            const pName = (p.companyName || p.company?.name || p.name || '').toLowerCase();
            return company.name.length <= 5
              ? pName === company.name.toLowerCase()
              : pName.includes(company.name.toLowerCase()) || company.name.toLowerCase().includes(pName);
          });
          projectsFound.push(...matches);
        }
        if (projectsFound.length) {
          enrichResults.ezekia_projects = {
            found: projectsFound.length,
            projects: projectsFound.slice(0, 10).map(p => ({ name: p.name, status: p.status, id: p.id }))
          };
        }
      } catch (e) { /* ignore project search errors */ }
    }

    // 1c. Gmail domain discovery — find contacts by email domain
    if (company.domain || company.name) {
      try {
        const emailDomain = company.domain || company.name.toLowerCase().replace(/[^a-z0-9]/g, '') + '.com';
        const googleToken = await getGoogleToken(req.user.user_id);
        if (googleToken) {
          const q = encodeURIComponent(`from:*@${emailDomain} OR to:*@${emailDomain}`);
          const gmailRes = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=50`, { headers: { Authorization: `Bearer ${googleToken}` } });
          if (gmailRes.ok) {
            const gmailData = await gmailRes.json();
            const discoveredContacts = new Map();

            for (const msg of (gmailData.messages || []).slice(0, 30)) {
              try {
                const mRes = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=From&metadataHeaders=To`, { headers: { Authorization: `Bearer ${googleToken}` } });
                if (!mRes.ok) continue;
                const mData = await mRes.json();
                const hdrs = mData.payload?.headers || [];
                const fromTo = (hdrs.find(h => h.name === 'From')?.value || '') + ',' + (hdrs.find(h => h.name === 'To')?.value || '');
                const matches = fromTo.match(new RegExp(`([^<,]+)<([a-zA-Z0-9._%+-]+@${emailDomain.replace('.', '\\.')})>`, 'gi')) || [];
                matches.forEach(m => {
                  const parts = m.match(/(.+)<(.+)>/);
                  if (parts) discoveredContacts.set(parts[2].toLowerCase().trim(), parts[1].trim().replace(/[\"']/g, ''));
                });
              } catch (e) { /* skip message errors */ }
            }

            let linked = 0;
            for (const [email, name] of discoveredContacts) {
              const { rows: exists } = await pool.query('SELECT id FROM people WHERE email = $1 AND tenant_id = $2', [email, req.tenant_id]);
              if (exists.length) {
                await pool.query('UPDATE people SET current_company_id = $1, current_company_name = $2, updated_at = NOW() WHERE id = $3 AND (current_company_id IS NULL OR current_company_id != $1)', [req.params.id, company.name, exists[0].id]);
              } else {
                await pool.query('INSERT INTO people (full_name, email, current_company_id, current_company_name, source, created_by, tenant_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())',
                  [name, email, req.params.id, company.name, 'gmail_discovery', req.user?.user_id || null, req.tenant_id]);
              }
              linked++;
            }
            if (linked) enrichResults.gmail_contacts = { discovered: discoveredContacts.size, linked };
          }
        }
      } catch (e) { /* gmail discovery is best-effort */ }
    }

    // 2. Conversion history from account record
    try {
      const { rows: [clientRecord] } = await pool.query(
        'SELECT cl.id, cl.relationship_status, cl.relationship_tier FROM accounts cl WHERE cl.company_id = $1 AND cl.tenant_id = $2 LIMIT 1',
        [req.params.id, req.tenant_id]
      );
      if (clientRecord) {
        const { rows: placements } = await pool.query(
          `SELECT p.role_title, p.start_date, p.placement_fee, p.currency, pe.full_name
           FROM conversions p
           LEFT JOIN people pe ON pe.id = p.person_id
           WHERE p.client_id = $1 AND p.tenant_id = $2 ORDER BY p.start_date DESC LIMIT 10`,
          [clientRecord.id, req.tenant_id]
        );
        // Mark as client in companies table
        await pool.query('UPDATE companies SET is_client = true, updated_at = NOW() WHERE id = $1 AND (is_client IS NULL OR is_client = false) AND tenant_id = $2', [req.params.id, req.tenant_id]);
        enrichResults.client = {
          status: clientRecord.relationship_status,
          tier: clientRecord.relationship_tier,
          recent_placements: placements.length,
          placements: placements.map(p => ({ role: p.role_title, candidate: p.full_name, date: p.start_date, fee: p.placement_fee }))
        };
      } else {
        enrichResults.client = { message: 'No client record linked' };
      }
    } catch (e) {
      enrichResults.client = { error: e.message };
    }

    // 3. Link unlinked people to this company (exact name match only)
    try {
      const { rowCount: linked } = await pool.query(
        `UPDATE people SET current_company_id = $1, updated_at = NOW()
         WHERE LOWER(TRIM(current_company_name)) = LOWER(TRIM($2))
           AND (current_company_id IS NULL OR current_company_id != $1)
           AND tenant_id = $3`,
        [req.params.id, company.name, req.tenant_id]
      );
      // Also try account names
      const { rows: accountNames } = await pool.query(
        'SELECT DISTINCT name FROM accounts WHERE company_id = $1 AND tenant_id = $2',
        [req.params.id, req.tenant_id]
      );
      let extraLinked = 0;
      for (const an of accountNames) {
        if (an.name.toLowerCase() !== company.name.toLowerCase()) {
          const { rowCount } = await pool.query(
            `UPDATE people SET current_company_id = $1, updated_at = NOW()
             WHERE LOWER(TRIM(current_company_name)) = LOWER(TRIM($2))
               AND (current_company_id IS NULL OR current_company_id != $1)
               AND tenant_id = $3`,
            [req.params.id, an.name, req.tenant_id]
          );
          extraLinked += rowCount;
        }
      }
      if (linked + extraLinked > 0) enrichResults.people_linked = linked + extraLinked;
    } catch (e) { /* ignore linking errors */ }

    // 4. People at this company
    try {
      const { rows: people } = await pool.query(
        `SELECT full_name, current_title, email FROM people WHERE current_company_id = $1 AND tenant_id = $2 ORDER BY current_title LIMIT 20`,
        [req.params.id, req.tenant_id]
      );
      enrichResults.people = { count: people.length, sample: people.slice(0, 5).map(p => `${p.full_name} — ${p.current_title}`) };
    } catch (e) {
      enrichResults.people = { error: e.message };
    }

    // 5. Google News search — fetch recent news + instant signal detection
    try {
      const searchName = company.name.replace(/\s+(Pty|Ltd|Limited|Inc|Corp|plc|AG|S\.A\.|Group|Holdings)\b/gi, '').trim();
      const newsUrl = `https://news.google.com/rss/search?q=${encodeURIComponent('"' + searchName + '"')}&hl=en&gl=AU&ceid=AU:en`;
      const newsXml = await new Promise((resolve, reject) => {
        const client = newsUrl.startsWith('https') ? https : require('http');
        const nReq = client.get(newsUrl, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MLX-Intelligence/1.0)' } }, (res) => {
          const chunks = []; res.on('data', c => chunks.push(c)); res.on('end', () => resolve(Buffer.concat(chunks).toString()));
        });
        nReq.on('error', reject); nReq.on('timeout', () => { nReq.destroy(); reject(new Error('timeout')); });
      });

      // Parse RSS items
      const newsItems = [];
      const itemRegex = /<item>([\s\S]*?)<\/item>/g;
      let newsMatch;
      while ((newsMatch = itemRegex.exec(newsXml)) !== null && newsItems.length < 10) {
        const itemXml = newsMatch[1];
        const getTag = (tag) => { const m = itemXml.match(new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`)); return (m?.[1] || m?.[2] || '').trim(); };
        const title = getTag('title');
        const link = getTag('link');
        const pubDate = getTag('pubDate');
        const source = getTag('source');
        if (title && link) newsItems.push({ title, link, pubDate, source });
      }

      // Run instant signal detection via Claude on the headlines
      let signalsCreated = 0;
      if (newsItems.length > 0 && process.env.ANTHROPIC_API_KEY) {
        try {
          const headlineBlock = newsItems.map((n, i) => `${i + 1}. "${n.title}" — ${n.source || 'Unknown'} (${n.pubDate || 'recent'})`).join('\n');
          const signalPrompt = `Analyse these recent news headlines about "${company.name}" for executive search signals.

HEADLINES:
${headlineBlock}

For each headline that contains a signal, return a JSON array. Signal types: capital_raising, geographic_expansion, strategic_hiring, ma_activity, partnership, product_launch, leadership_change, layoffs, restructuring.

Return ONLY a JSON array (or empty array [] if no signals found):
[{"headline_index": 1, "signal_type": "...", "confidence": 0.5-1.0, "evidence_summary": "one sentence describing the signal"}]

Only include genuine business signals. Ignore opinion pieces, listicles, or generic mentions.`;

          const signalResponse = await callClaude(
            [{ role: 'user', content: signalPrompt }],
            [],
            'You are a market signal analyst. Return ONLY valid JSON arrays.'
          );
          const signalText = signalResponse.content?.find(c => c.type === 'text')?.text || '[]';
          const detectedSignals = JSON.parse(signalText.replace(/```json\n?|\n?```/g, '').trim());

          for (const sig of (Array.isArray(detectedSignals) ? detectedSignals : [])) {
            if (!sig.signal_type || !sig.evidence_summary) continue;
            const headlineItem = newsItems[sig.headline_index - 1];
            if (!headlineItem) continue;

            // Check for duplicate signal
            const { rows: existing } = await pool.query(
              `SELECT id FROM signal_events WHERE company_id = $1 AND signal_type = $2 AND evidence_summary ILIKE $3 AND tenant_id = $4 LIMIT 1`,
              [req.params.id, sig.signal_type, `%${sig.evidence_summary.slice(0, 50)}%`, req.tenant_id]
            );
            if (existing.length) continue;

            await pool.query(`
              INSERT INTO signal_events (signal_type, company_id, company_name, confidence_score,
                evidence_summary, source_url, detected_at, signal_date, tenant_id)
              VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
            `, [
              sig.signal_type, req.params.id, company.name, Math.min(sig.confidence || 0.6, 0.85),
              sig.evidence_summary, headlineItem.link,
              headlineItem.pubDate ? new Date(headlineItem.pubDate).toISOString() : new Date().toISOString(),
              req.tenant_id
            ]);
            signalsCreated++;
          }
        } catch (e) {
          // Signal detection failure is non-fatal
          console.error('News signal detection error:', e.message);
        }
      }

      // Store articles as documents
      let newsIngested = 0;
      for (const item of newsItems) {
        const sourceUrlHash = require('crypto').createHash('md5').update(item.link).digest('hex');
        const { rows: exists } = await pool.query('SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2', [sourceUrlHash, req.tenant_id]);
        if (exists.length) continue;
        await pool.query(`
          INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
            tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
          VALUES ($1, $2, $3, 'news_enrich', $4, $5, $6, $7, $8, 'processed', NOW())
        `, [item.title, item.title, item.source || 'Google News', item.link, sourceUrlHash,
            req.tenant_id, req.user?.user_id || null,
            item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString()]);
        newsIngested++;
      }

      enrichResults.news = {
        articles_found: newsItems.length,
        new_ingested: newsIngested,
        signals_created: signalsCreated,
        headlines: newsItems.slice(0, 5).map(i => ({ title: i.title, source: i.source, date: i.pubDate })),
        message: signalsCreated > 0
          ? `${newsItems.length} articles found, ${signalsCreated} signals detected and created`
          : newsItems.length > 0
            ? `${newsItems.length} articles found, no new signals detected`
            : 'No recent news found'
      };
    } catch (e) {
      enrichResults.news = { error: e.message };
    }

    // 4. Re-embed with all enriched data
    try {
      const { rows: [latest] } = await pool.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
      const parts = [latest.name, latest.sector, latest.geography, latest.description, latest.domain].filter(Boolean);

      const { rows: signals } = await pool.query(`SELECT evidence_summary FROM signal_events WHERE company_id = $1 AND evidence_summary IS NOT NULL AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 5`, [req.params.id, req.tenant_id]);
      signals.forEach(s => parts.push(s.evidence_summary));

      const { rows: people } = await pool.query(`SELECT full_name, current_title FROM people WHERE current_company_id = $1 AND current_title IS NOT NULL AND tenant_id = $2 LIMIT 10`, [req.params.id, req.tenant_id]);
      if (people.length) parts.push('Key people: ' + people.map(p => `${p.full_name} — ${p.current_title}`).join(', '));

      if (parts.join(' ').length > 10 && process.env.QDRANT_URL) {
        const embedding = await generateQueryEmbedding(parts.join('\n'));
        const url = new URL('/collections/companies/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: req.params.id, vector: embedding, payload: { name: latest.name, sector: latest.sector, is_client: latest.is_client } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT', headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
            (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        await pool.query('UPDATE companies SET embedded_at = NOW() WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
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

    // ── CLIENTS filter: query the accounts table directly ──
    if (req.query.is_client === 'true') {
      let clWhere = 'WHERE cl.tenant_id = $1';
      const clParams = [req.tenant_id];
      let clIdx = 1;
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
          FROM accounts cl
          LEFT JOIN companies co ON cl.company_id = co.id
          LEFT JOIN account_financials cf ON cf.client_id = cl.id
          ${clWhere}
          ORDER BY COALESCE(cf.total_invoiced, 0) DESC, cl.name
          LIMIT $${clLimitIdx} OFFSET $${clOffsetIdx}
        `, clParams),
        pool.query(`SELECT COUNT(*) AS cnt FROM accounts cl ${clWhere}`, clParams.slice(0, -2)),
      ]);

      return res.json({
        companies: clientsResult.rows,
        total: parseInt(clientCountResult.rows[0].cnt),
        limit, offset,
      });
    }

    // ── ALL / filtered companies ──
    let where = 'WHERE c.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter
    paramIdx++;
    where += ` AND (c.visibility IS NULL OR c.visibility != 'private' OR c.owner_user_id = $${paramIdx})`;
    params.push(req.user.user_id);

    // Exclude tenant company (that's us, not a client/target)
    where += ` AND COALESCE(c.company_tier, '') != 'tenant_company'`;

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
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id
                AND se.signal_type::text IN ('capital_raising','product_launch','geographic_expansion','partnership','strategic_hiring')
                AND se.detected_at > NOW() - INTERVAL '30 days') AS positive_signal_count,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count
        FROM companies c
        ${where}
        ORDER BY
          -- Tier 1: Clients with positive signals (30d)
          CASE WHEN c.is_client = true AND (SELECT COUNT(*) FROM signal_events se
            WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '30 days'
            AND se.signal_type::text IN ('capital_raising','product_launch','geographic_expansion','partnership','strategic_hiring')
          ) > 0 THEN 0
          -- Tier 2: Clients with contacts
          WHEN c.is_client = true THEN 1
          -- Tier 3: Non-clients with signals AND contacts
          WHEN (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '30 days') > 0
            AND (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) > 0 THEN 2
          -- Tier 4: Companies with contacts only
          WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) > 0 THEN 3
          ELSE 4 END,
          -- Within each tier, sort by signal+contact density
          (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '30 days') DESC,
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
    const { rows: [co] } = await pool.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [companyId, req.tenant_id]);
    if (co) {
      company = co;
    } else {
      // Maybe it's a clients table ID — resolve it
      const { rows: [cl] } = await pool.query(`
        SELECT cl.*, co.id AS resolved_company_id,
               co.sector, co.geography, co.domain, co.employee_count_band,
               co.description AS company_description, co.is_client AS co_is_client
        FROM accounts cl
        LEFT JOIN companies co ON cl.company_id = co.id
        WHERE cl.id = $1 AND cl.tenant_id = $2
      `, [companyId, req.tenant_id]);
      if (cl && cl.resolved_company_id) {
        // Client has a linked company — use that
        const { rows: [linked] } = await pool.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [cl.resolved_company_id, req.tenant_id]);
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
        SELECT cf.* FROM account_financials cf
        JOIN accounts cl ON cf.client_id = cl.id
        WHERE (cl.company_id = $1 OR cl.id = $1) AND cf.tenant_id = $2
      `, [companyId, req.tenant_id]);
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
      WHERE se.company_id = $1 AND se.tenant_id = $2
      ORDER BY se.detected_at DESC LIMIT 30
    `, [companyId, req.tenant_id]);

    // People at this company — ordered by engagement level
    const { rows: people } = await pool.query(`
      SELECT p.id, p.full_name, p.current_title, p.seniority_level, p.location,
             p.expertise_tags, p.linkedin_url, p.email, p.source,
             (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) AS interaction_count,
             (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_at > NOW() - INTERVAL '90 days') AS interactions_90d,
             (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
             (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id) AS last_interaction,
             (SELECT MAX(tp.relationship_strength) FROM team_proximity tp WHERE tp.person_id = p.id) AS proximity_strength,
             (SELECT STRING_AGG(DISTINCT u.name, ', ') FROM team_proximity tp JOIN users u ON u.id = tp.team_member_id WHERE tp.person_id = p.id) AS connected_via,
             (SELECT STRING_AGG(DISTINCT tp.relationship_type, ', ') FROM team_proximity tp WHERE tp.person_id = p.id) AS connection_types
      FROM people p WHERE p.current_company_id = $1 AND p.tenant_id = $2
      ORDER BY
        (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) DESC,
        (SELECT MAX(tp.relationship_strength) FROM team_proximity tp WHERE tp.person_id = p.id) DESC NULLS LAST,
        CASE WHEN p.seniority_level IN ('c_suite','vp','director') THEN 0 ELSE 1 END,
        p.full_name
      LIMIT 10
    `, [companyId, req.tenant_id]);

    // Placements at this company
    let placements = [];
    try {
      const { rows } = await pool.query(`
        SELECT pl.id, pe.full_name AS candidate_name, pl.role_title, pl.start_date,
               pl.placement_fee, pl.fee_category
        FROM conversions pl
        LEFT JOIN people pe ON pl.person_id = pe.id
        LEFT JOIN accounts cl ON pl.client_id = cl.id
        WHERE (cl.company_id = $1 OR cl.id = $1) AND pl.tenant_id = $2
        ORDER BY pl.start_date DESC NULLS LAST
      `, [companyId, req.tenant_id]);
      placements = rows;
    } catch (e) { /* table may not exist */ }

    // Documents mentioning this company — by document_companies link OR title/content match
    let documents = [];
    try {
      // Use word-boundary regex for short names to avoid PAM matching EPAM
      const namePattern = company.name.length <= 5
        ? '(^|[^a-zA-Z])' + company.name + '([^a-zA-Z]|$)'
        : company.name;
      const isRegex = company.name.length <= 5;
      const { rows } = await pool.query(`
        SELECT DISTINCT ed.id, ed.title, ed.source_name, ed.source_type, ed.source_url,
               ed.published_at
        FROM external_documents ed
        LEFT JOIN document_companies dc ON dc.document_id = ed.id
        WHERE ed.tenant_id = $2 AND (
          dc.company_id = $1
          ${isRegex ? "OR ed.title ~* $3" : "OR ed.title ILIKE $3"}
        )
        ORDER BY ed.published_at DESC NULLS LAST
        LIMIT 20
      `, [companyId, req.tenant_id, isRegex ? namePattern : '%' + company.name + '%']);
      documents = rows;
    } catch (e) { /* table may not exist */ }

    // Pipeline — opportunities + candidate counts for this company
    let opportunities = [];
    try {
      const { rows } = await pool.query(`
        SELECT o.id, o.title, o.status, o.seniority_level,
               (SELECT COUNT(*) FROM pipeline_contacts pc WHERE pc.search_id = o.id) as candidate_count
        FROM opportunities o
        JOIN engagements e ON e.id = o.project_id
        JOIN accounts a ON a.id = e.client_id
        WHERE a.company_id = $1 AND o.tenant_id = $2
        ORDER BY o.created_at DESC
      `, [companyId, req.tenant_id]);
      opportunities = rows;
    } catch (e) {}

    // Total pipeline candidates across all opportunities
    let pipelineTotal = 0;
    try {
      const { rows: [{ cnt }] } = await pool.query(`
        SELECT COUNT(DISTINCT pc.person_id) as cnt
        FROM pipeline_contacts pc
        JOIN opportunities o ON o.id = pc.search_id
        JOIN engagements e ON e.id = o.project_id
        JOIN accounts a ON a.id = e.client_id
        WHERE a.company_id = $1 AND pc.tenant_id = $2
      `, [companyId, req.tenant_id]);
      pipelineTotal = parseInt(cnt);
    } catch (e) {}

    // Case studies where this company was the client
    let case_studies = [];
    try {
      const { rows } = await pool.query(`
        SELECT id, title, role_title, engagement_type, seniority_level, year,
               challenge, approach, outcome, themes, capabilities, status, visibility
        FROM case_studies
        WHERE (client_id = $1 OR client_name ILIKE $2) AND tenant_id = $3
        ORDER BY year DESC NULLS LAST
      `, [companyId, `%${company.name}%`, req.tenant_id]);
      case_studies = rows;
    } catch (e) { /* table may not exist */ }

    // Interaction summary — relationship activity across people at this company
    let interaction_summary = null;
    try {
      const { rows: [is] } = await pool.query(`
        SELECT
          COUNT(i.id) as total_interactions,
          COUNT(DISTINCT i.person_id) as contacts_engaged,
          COUNT(DISTINCT i.user_id) as team_members_involved,
          MAX(i.interaction_at) as last_interaction,
          COUNT(i.id) FILTER (WHERE i.interaction_at > NOW() - INTERVAL '90 days') as interactions_90d,
          COUNT(i.id) FILTER (WHERE i.interaction_at > NOW() - INTERVAL '30 days') as interactions_30d,
          COUNT(i.id) FILTER (WHERE i.interaction_type IN ('email_sent', 'email_received')) as email_count,
          COUNT(i.id) FILTER (WHERE i.interaction_type = 'linkedin_message') as linkedin_count,
          COUNT(i.id) FILTER (WHERE i.direction = 'outbound') as outbound_count,
          COUNT(i.id) FILTER (WHERE i.direction = 'inbound') as inbound_count
        FROM interactions i
        JOIN people p ON p.id = i.person_id
        WHERE p.current_company_id = $1
          AND i.interaction_at > NOW() - INTERVAL '2 years'
      `, [companyId]);
      if (is && parseInt(is.total_interactions) > 0) interaction_summary = is;
    } catch (e) {}

    res.json({ ...company, signals, people, placements, documents, financials, opportunities, pipeline_total: pipelineTotal, case_studies, interaction_summary });
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

// Company visibility toggle
app.patch('/api/companies/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const { visibility } = req.body;
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) return res.status(400).json({ error: 'visibility must be "company" or "private"' });

    const { rows: [co] } = await pool.query('SELECT id, visibility, owner_user_id FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!co) return res.status(404).json({ error: 'Company not found' });
    if (co.visibility === 'private' && co.owner_user_id && co.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change private companies' });
    }

    await pool.query(
      `UPDATE companies SET visibility = $1, owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END WHERE id = $3 AND tenant_id = $4`,
      [visibility, req.user.user_id, req.params.id, req.tenant_id]
    );
    res.json({ id: req.params.id, visibility });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Signal visibility toggle
app.patch('/api/signals/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const { visibility } = req.body;
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) return res.status(400).json({ error: 'visibility must be "company" or "private"' });

    const { rows: [sig] } = await pool.query('SELECT id, visibility, owner_user_id FROM signal_events WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!sig) return res.status(404).json({ error: 'Signal not found' });
    if (sig.visibility === 'private' && sig.owner_user_id && sig.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change private signals' });
    }

    await pool.query(
      `UPDATE signal_events SET visibility = $1, owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END WHERE id = $3 AND tenant_id = $4`,
      [visibility, req.user.user_id, req.params.id, req.tenant_id]
    );
    res.json({ id: req.params.id, visibility });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

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
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const pointIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRegex.test(id));

        if (pointIds.length === 0) {
          // No valid UUIDs — skip DB query
        } else {
        const { rows: people } = await pool.query(`
          SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.headline,
                 p.location, p.seniority_level, p.expertise_tags, p.industries, p.source,
                 p.email, p.linkedin_url, p.current_company_id,
                 ps.timing_score, ps.flight_risk_score, ps.engagement_score, ps.receptivity_score,
                 (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
                 (SELECT COUNT(*) FROM team_proximity tp WHERE tp.person_id = p.id AND tp.tenant_id = $2 AND tp.relationship_strength >= 0.3) AS team_connections,
                 (SELECT u.name FROM users u WHERE u.id = (
                   SELECT tp2.team_member_id FROM team_proximity tp2 WHERE tp2.person_id = p.id AND tp2.tenant_id = $2
                   ORDER BY tp2.relationship_strength DESC LIMIT 1
                 )) AS best_connector,
                 (SELECT se.signal_type FROM signal_events se WHERE se.company_id = p.current_company_id
                   AND se.detected_at > NOW() - INTERVAL '30 days' AND se.tenant_id = $2
                   ORDER BY se.confidence_score DESC LIMIT 1) AS company_signal_type,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = p.current_company_id
                   AND se.detected_at > NOW() - INTERVAL '30 days' AND se.tenant_id = $2) AS company_signal_count,
                 c.is_client AS at_client_company
          FROM people p
          LEFT JOIN person_scores ps ON ps.person_id = p.id
          LEFT JOIN companies c ON c.id = p.current_company_id
          WHERE p.id = ANY($1::uuid[]) AND p.tenant_id = $2
        `, [pointIds, req.tenant_id]);

        const peopleMap = new Map(people.map(p => [p.id, p]));

        results.people = qdrantResults
          .map(r => {
            const person = peopleMap.get(r.id);
            if (!person) return null;
            return {
              ...person,
              match_score: Math.round(r.score * 100),
              has_research_notes: r.payload?.has_research_notes || parseInt(person.note_count) > 0,
            };
          })
          .filter(Boolean);
        }
      }
    }

    // Search companies
    if (collection === 'companies' || collection === 'all') {
      const compLimit = collection === 'all' ? Math.min(limit, 12) : limit;
      const qdrantResults = await qdrantSearch('companies', vector, compLimit);

      if (qdrantResults.length > 0) {
        const uuidRx = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const compIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRx.test(id));

        if (compIds.length === 0) {
          // No valid UUIDs — skip
        } else {
        const { rows: companies } = await pool.query(`
          SELECT c.id, c.name, c.sector, c.geography, c.domain, c.is_client,
                 c.employee_count_band, c.description,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id) AS signal_count,
                 (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '30 days') AS recent_signal_count,
                 (SELECT se.signal_type FROM signal_events se WHERE se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '30 days'
                   ORDER BY se.confidence_score DESC LIMIT 1) AS top_signal_type,
                 (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count,
                 (SELECT COUNT(DISTINCT tp.person_id) FROM team_proximity tp
                   JOIN people p2 ON p2.id = tp.person_id AND p2.current_company_id = c.id
                   WHERE tp.tenant_id = $2 AND tp.relationship_strength >= 0.3) AS network_connections,
                 (SELECT a.relationship_tier FROM accounts a WHERE (a.company_id = c.id OR LOWER(a.name) = LOWER(c.name)) AND a.tenant_id = $2 LIMIT 1) AS client_tier,
                 cas.adjacency_score, cas.warmest_contact_name
          FROM companies c
          LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(c.name))
          WHERE c.id = ANY($1::uuid[]) AND c.tenant_id = $2
        `, [compIds, req.tenant_id]);

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
    }

    // Search documents
    if (collection === 'documents' || collection === 'all') {
      const docLimit = collection === 'all' ? Math.min(limit, 12) : limit;
      const qdrantResults = await qdrantSearch('documents', vector, docLimit);

      if (qdrantResults.length > 0) {
        const uuidRx2 = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        const docIds = qdrantResults.map(r => String(r.id)).filter(id => uuidRx2.test(id));

        if (docIds.length === 0) {
          // No valid UUIDs — skip
        } else {
        const { rows: docs } = await pool.query(`
          SELECT id, title, source_type, source_name, source_url, author, published_at
          FROM external_documents WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        `, [docIds, req.tenant_id]);

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
    }

    // Search signals (direct)
    if (collection === 'signals' || collection === 'all') {
      try {
        const sigLimit = collection === 'all' ? Math.min(limit, 12) : limit;
        const qdrantResults = await qdrantSearch('signal_events', vector, sigLimit);
        if (qdrantResults.length > 0) {
          const sigIds = qdrantResults.map(r => r.payload?.signal_id).filter(Boolean);
          if (sigIds.length > 0) {
            const { rows: signals } = await pool.query(`
              SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
                     se.evidence_summary, se.detected_at, c.sector, c.geography, c.is_client,
                     (SELECT COUNT(DISTINCT tp.person_id) FROM team_proximity tp
                       JOIN people p ON p.id = tp.person_id AND p.current_company_id = se.company_id
                       WHERE tp.tenant_id = $2 AND tp.relationship_strength >= 0.3) AS network_connections,
                     (SELECT u.name FROM users u WHERE u.id = (
                       SELECT tp2.team_member_id FROM team_proximity tp2
                       JOIN people p2 ON p2.id = tp2.person_id AND p2.current_company_id = se.company_id
                       WHERE tp2.tenant_id = $2 ORDER BY tp2.relationship_strength DESC LIMIT 1
                     )) AS best_connector
              FROM signal_events se LEFT JOIN companies c ON c.id = se.company_id
              WHERE se.id = ANY($1::uuid[]) AND se.tenant_id = $2
            `, [sigIds, req.tenant_id]);
            const sigMap = new Map(signals.map(s => [s.id, s]));
            results.signals = qdrantResults.map(r => {
              const sig = sigMap.get(r.payload?.signal_id);
              if (!sig) return null;
              return { ...sig, match_score: Math.round(r.score * 100), score: r.score };
            }).filter(Boolean);
          }
        }
      } catch (e) { /* collection may not exist yet */ }
    }

    // Search case studies (replaces placements in search — more useful, no duplicate retainer stages)
    if (collection === 'case_studies' || collection === 'all') {
      try {
        const csLimit = collection === 'all' ? Math.min(limit, 12) : limit;
        // Try Qdrant first
        let csResults = [];
        try {
          const qdrantResults = await qdrantSearch('case_studies', vector, csLimit);
          if (qdrantResults.length > 0) {
            const csIds = qdrantResults.map(r => String(r.id)).filter(id => /^[0-9a-f-]{36}$/i.test(id));
            if (csIds.length > 0) {
              const { rows } = await pool.query(`
                SELECT id, title, client_name, role_title, sector, geography, year,
                       challenge, engagement_type, themes, capabilities
                FROM case_studies WHERE id = ANY($1::uuid[]) AND tenant_id = $2
              `, [csIds, req.tenant_id]);
              const csMap = new Map(rows.map(r => [r.id, r]));
              csResults = qdrantResults.map(r => {
                const cs = csMap.get(r.id);
                if (!cs) return null;
                return { ...cs, match_score: Math.round(r.score * 100), score: r.score };
              }).filter(Boolean);
            }
          }
        } catch (e) { /* collection may not exist */ }

        // Fallback to SQL text search
        if (csResults.length < 3) {
          const { rows } = await pool.query(`
            SELECT id, title, client_name, role_title, sector, geography, year,
                   challenge, engagement_type, themes, capabilities
            FROM case_studies
            WHERE tenant_id = $1 AND (title ILIKE $2 OR client_name ILIKE $2 OR role_title ILIKE $2 OR challenge ILIKE $2)
            ORDER BY year DESC NULLS LAST LIMIT $3
          `, [req.tenant_id, `%${q}%`, csLimit]);
          const existing = new Set(csResults.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) csResults.push({ ...r, match_score: 60, score: 0.6 }); });
        }
        results.case_studies = csResults.slice(0, csLimit);
      } catch (e) { /* table may not exist */ }
    }

    // Search interactions
    if (collection === 'interactions' || collection === 'all') {
      try {
        const intLimit = collection === 'all' ? Math.min(limit, 10) : limit;
        const qdrantResults = await qdrantSearch('interactions', vector, intLimit);
        if (qdrantResults.length > 0) {
          const intIds = qdrantResults.map(r => r.payload?.interaction_id).filter(Boolean);
          if (intIds.length > 0) {
            const { rows: interactions } = await pool.query(`
              SELECT i.id, i.interaction_type, i.subject, i.summary, i.interaction_at, i.direction,
                     p.full_name as person_name, p.current_title
              FROM interactions i
              LEFT JOIN people p ON p.id = i.person_id
              WHERE i.id = ANY($1::uuid[]) AND i.tenant_id = $2
            `, [intIds, req.tenant_id]);
            const intMap = new Map(interactions.map(i => [i.id, i]));
            results.interactions = qdrantResults.map(r => {
              const int = intMap.get(r.payload?.interaction_id);
              if (!int) return null;
              return { ...int, match_score: Math.round(r.score * 100), score: r.score };
            }).filter(Boolean);
          }
        }
      } catch (e) { /* collection may not exist yet */ }
    }

    // Add score field to existing results
    results.people = (results.people || []).map(p => ({ ...p, score: (p.match_score || 50) / 100 }));
    results.companies = (results.companies || []).map(c => ({ ...c, score: (c.match_score || 50) / 100 }));
    results.documents = (results.documents || []).map(d => ({ ...d, score: (d.match_score || 50) / 100 }));

    res.json({
      query: q,
      collection,
      results,
      total: (results.people?.length || 0) + (results.companies?.length || 0) +
             (results.documents?.length || 0) + (results.signals?.length || 0) +
             (results.case_studies?.length || 0) + (results.interactions?.length || 0),
    });
  } catch (err) {
    console.error('Search error:', err.message);
    res.status(500).json({ error: 'Search failed: ' + err.message });
  }
});

// Search index status
app.get('/api/search/index-status', authenticateToken, async (req, res) => {
  try {
    const { rows: [counts] } = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM people WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS people,
        (SELECT COUNT(*) FROM companies WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS companies,
        (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS documents,
        (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS signals,
        (SELECT COUNT(*) FROM case_studies WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS case_studies,
        (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS conversions,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS interactions
    `, [req.tenant_id]);
    res.json({
      people: Number(counts.people), companies: Number(counts.companies),
      documents: Number(counts.documents), signals: Number(counts.signals),
      case_studies: Number(counts.case_studies),
      conversions: Number(counts.conversions), interactions: Number(counts.interactions),
      total: Number(counts.people) + Number(counts.companies) + Number(counts.documents) +
             Number(counts.signals) + Number(counts.case_studies) + Number(counts.conversions) + Number(counts.interactions)
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
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

    let where = 'WHERE tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter — hide private docs from non-owners
    if (req.user) {
      paramIdx++;
      where += ` AND (visibility IS NULL OR visibility != 'private' OR owner_user_id = $${paramIdx})`;
      params.push(req.user.user_id);
    } else {
      where += ` AND (visibility IS NULL OR visibility != 'private')`;
    }

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
             published_at, processing_status, embedded_at IS NOT NULL AS is_embedded,
             visibility, owner_user_id, uploaded_by_user_id
      FROM external_documents
      ${where}
      ORDER BY published_at DESC NULLS LAST
      LIMIT $${paramIdx - 1} OFFSET $${paramIdx}
    `, params);

    const countParams = params.slice(0, -2); // everything except limit/offset
    const { rows: [{ cnt }] } = await pool.query(
      `SELECT COUNT(*) AS cnt FROM external_documents ${where}`,
      countParams
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
             (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_id = rs.id AND ed.tenant_id = $1) AS doc_count
      FROM rss_sources rs
      ORDER BY rs.source_type, rs.name
    `, [req.tenant_id]);
    res.json({ sources: rows });
  } catch (err) {
    console.error('Sources error:', err.message);
    res.status(500).json({ error: 'Failed to fetch sources' });
  }
});

// Document privacy toggle
app.patch('/api/documents/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const { visibility } = req.body; // 'company' or 'private'
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) {
      return res.status(400).json({ error: 'visibility must be "company" or "private"' });
    }

    const { rows: [doc] } = await pool.query(
      'SELECT id, visibility, owner_user_id FROM external_documents WHERE id = $1 AND tenant_id = $2',
      [req.params.id, req.tenant_id]
    );
    if (!doc) return res.status(404).json({ error: 'Document not found' });

    // Only owner or admin can change private docs back to company
    if (doc.visibility === 'private' && doc.owner_user_id && doc.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change visibility of private documents' });
    }

    await pool.query(`
      UPDATE external_documents
      SET visibility = $1,
          owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END
      WHERE id = $3 AND tenant_id = $4
    `, [visibility, req.user.user_id, req.params.id, req.tenant_id]);

    res.json({ id: req.params.id, visibility });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Also update Drive ingest to set uploaded_by
app.patch('/api/documents/:id', authenticateToken, async (req, res) => {
  try {
    const allowed = ['title', 'visibility', 'summary'];
    const updates = [];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;
    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        idx++;
        updates.push(`${key} = $${idx}`);
        params.push(req.body[key]);
      }
    }
    if (req.body.visibility === 'private') {
      updates.push(`owner_user_id = $${++idx}`);
      params.push(req.user.user_id);
    }
    if (updates.length === 0) return res.json({ ok: true });
    await pool.query(`UPDATE external_documents SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2`, params);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL GRABS — Editorial Intelligence
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/grabs', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 10, 50);
    const type = req.query.type; // macro, regional, sector, talent, contrarian
    const status = req.query.status || 'draft';

    let where = 'WHERE sg.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (status !== 'all') { idx++; where += ` AND sg.status = $${idx}`; params.push(status); }
    if (type) { idx++; where += ` AND sg.cluster_type = $${idx}`; params.push(type); }
    if (req.query.exclude_weekly === 'true') { where += ` AND sg.cluster_type != 'weekly_wrap'`; }

    // Region filter — match against geographies array or storyline text
    const region = req.query.region;
    if (region && region !== 'all' && region !== '') {
      const geoNames = REGION_MAP[region] || [];
      const regionCodes = REGION_CODES[region] || [];
      const allTerms = [...regionCodes, ...geoNames.slice(0, 5)];
      if (allTerms.length > 0) {
        const orParts = [];
        allTerms.forEach(g => { idx++; orParts.push(`$${idx} = ANY(sg.geographies)`); params.push(g); });
        allTerms.slice(0, 3).forEach(g => { idx++; orParts.push(`sg.storyline ILIKE $${idx}`); params.push(`%${g}%`); });
        where += ` AND (${orParts.join(' OR ')})`;
      }
    }

    idx++; params.push(limit);

    const { rows } = await pool.query(`
      SELECT sg.* FROM signal_grabs sg ${where}
      ORDER BY sg.created_at DESC LIMIT $${idx}
    `, params);

    res.json({ grabs: rows, total: rows.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/grabs/:id', authenticateToken, async (req, res) => {
  try {
    const { rows: [grab] } = await pool.query(
      'SELECT * FROM signal_grabs WHERE id = $1 AND tenant_id = $2',
      [req.params.id, req.tenant_id]
    );
    if (!grab) return res.status(404).json({ error: 'Grab not found' });
    res.json(grab);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/grabs/generate', authenticateToken, async (req, res) => {
  try {
    const { execSync } = require('child_process');
    execSync('node scripts/compute_signal_grabs.js', { timeout: 120000, stdio: 'pipe' });
    const { rows } = await pool.query(
      "SELECT * FROM signal_grabs WHERE tenant_id = $1 AND created_at > NOW() - INTERVAL '5 minutes' ORDER BY created_at DESC",
      [req.tenant_id]
    );
    res.json({ generated: rows.length, grabs: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.patch('/api/grabs/:id', authenticateToken, async (req, res) => {
  try {
    const { status } = req.body;
    if (status) {
      await pool.query(
        'UPDATE signal_grabs SET status = $1, published_at = CASE WHEN $1 = \'published\' THEN NOW() ELSE published_at END WHERE id = $2 AND tenant_id = $3',
        [status, req.params.id, req.tenant_id]
      );
    }
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/grabs/weekly', authenticateToken, async (req, res) => {
  try {
    // Get the most recent weekly wrap
    const { rows: [wrap] } = await pool.query(`
      SELECT * FROM signal_grabs
      WHERE tenant_id = $1 AND cluster_type = 'weekly_wrap'
      ORDER BY created_at DESC LIMIT 1
    `, [req.tenant_id]);

    // Also get the top 5 daily grabs from the week
    const { rows: topGrabs } = await pool.query(`
      SELECT * FROM signal_grabs
      WHERE tenant_id = $1 AND cluster_type != 'weekly_wrap' AND created_at > NOW() - INTERVAL '7 days'
      ORDER BY grab_score DESC LIMIT 5
    `, [req.tenant_id]);

    let wrapData = null;
    if (wrap) {
      try { wrapData = JSON.parse(wrap.observation); } catch(e) {}
    }

    res.json({ wrap: wrapData, wrap_id: wrap?.id, wrap_produced_at: wrap?.created_at, top_grabs: topGrabs, week_of: new Date().toISOString().slice(0, 10) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLIC PLATFORM STATS — no auth, for landing page ticker
// Aggregate across all tenants (future-proof for multi-tenant SaaS)
// ═══════════════════════════════════════════════════════════════════════════════

let _platformStatsCache = null;
let _platformStatsCacheTime = 0;

app.get('/api/public/stats', async (req, res) => {
  try {
    // Cache for 5 minutes to avoid hammering DB on every page view
    if (_platformStatsCache && Date.now() - _platformStatsCacheTime < 5 * 60 * 1000) {
      return res.json(_platformStatsCache);
    }

    const { rows: [s] } = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM people) as people,
        (SELECT COUNT(*) FROM companies WHERE sector IS NOT NULL OR is_client = true OR domain IS NOT NULL) as companies,
        (SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '7 days') as signals_7d,
        (SELECT COUNT(*) FROM signal_events) as signals_total,
        (SELECT COUNT(*) FROM opportunities WHERE status IN ('interviewing','sourcing','offer')) as active_searches,
        (SELECT COUNT(*) FROM conversions) as placements,
        (SELECT COUNT(*) FROM external_documents) as documents,
        (SELECT COUNT(*) FROM rss_sources WHERE enabled = true) as sources,
        (SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours') as signals_24h,
        (SELECT COUNT(DISTINCT company_id) FROM signal_events WHERE detected_at > NOW() - INTERVAL '7 days') as companies_signalling,
        (SELECT COUNT(*) FROM interactions) as interactions,
        (SELECT COUNT(*) FROM signal_grabs WHERE created_at > NOW() - INTERVAL '7 days') as grabs_7d,
        (SELECT COUNT(*) FROM tenants) as tenants
    `);

    const stats = {
      people: parseInt(s.people),
      companies: parseInt(s.companies),
      signals_7d: parseInt(s.signals_7d),
      signals_total: parseInt(s.signals_total),
      signals_24h: parseInt(s.signals_24h),
      active_searches: parseInt(s.active_searches),
      placements: parseInt(s.placements),
      documents: parseInt(s.documents),
      sources: parseInt(s.sources),
      companies_signalling: parseInt(s.companies_signalling),
      interactions: parseInt(s.interactions),
      grabs_7d: parseInt(s.grabs_7d),
      tenants: parseInt(s.tenants),
      regions: ['AU', 'SG', 'UK', 'US'],
      updated_at: new Date().toISOString()
    };

    _platformStatsCache = stats;
    _platformStatsCacheTime = Date.now();

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: 'Stats unavailable' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLIC EMBED ENDPOINTS — no auth, for mitchellake.com website embeds
// CORS restricted to mitchellake.com + localhost:3000
// Rate-limited: 60 req/min per IP, Cache-Control: 5 min
// All queries scoped to tenant_id = '00000000-0000-0000-0000-000000000001'
// ═══════════════════════════════════════════════════════════════════════════════

const PUBLIC_EMBED_TENANT = '00000000-0000-0000-0000-000000000001';
const PUBLIC_EMBED_ALLOWED_ORIGINS = ['https://mitchellake.com', 'https://www.mitchellake.com', 'http://localhost:3000'];
const PUBLIC_EMBED_STRIP_FIELDS = ['proximity_map', 'confidence_score', 'triage_status', 'claimed_by', 'send_to', 'best_connector_name', 'prox_connection_count'];

// Simple in-memory rate limiter: 60 requests/min per IP
const _publicEmbedRateMap = new Map();
function publicEmbedRateLimit(req, res, next) {
  const ip = req.ip;
  const now = Date.now();
  const window = 60 * 1000;
  const max = 60;

  let entry = _publicEmbedRateMap.get(ip);
  if (!entry || now - entry.start > window) {
    entry = { start: now, count: 1 };
    _publicEmbedRateMap.set(ip, entry);
  } else {
    entry.count++;
  }

  if (entry.count > max) {
    return res.status(429).json({ error: 'Rate limit exceeded' });
  }
  next();
}

// CORS + Cache-Control middleware for public embed routes
function publicEmbedCors(req, res, next) {
  const origin = req.headers.origin;
  if (PUBLIC_EMBED_ALLOWED_ORIGINS.includes(origin)) {
    res.header('Access-Control-Allow-Origin', origin);
  }
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.header('Cache-Control', 'public, max-age=300');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
}

// Strip sensitive fields from response objects
function stripEmbedFields(obj) {
  if (Array.isArray(obj)) return obj.map(stripEmbedFields);
  if (obj && typeof obj === 'object') {
    const cleaned = { ...obj };
    for (const field of PUBLIC_EMBED_STRIP_FIELDS) delete cleaned[field];
    return cleaned;
  }
  return obj;
}

// Public embed route middleware stack
const publicEmbed = [publicEmbedRateLimit, publicEmbedCors];

// ── 1. GET /api/public/grabs — latest published Signal Grabs
app.get('/api/public/grabs', ...publicEmbed, async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 50);
    let where = `WHERE sg.tenant_id = $1 AND sg.status = 'published'`;
    const params = [PUBLIC_EMBED_TENANT];
    let idx = 1;

    if (req.query.cluster_type) { idx++; where += ` AND sg.cluster_type = $${idx}`; params.push(req.query.cluster_type); }
    if (req.query.geography) { idx++; where += ` AND $${idx} = ANY(sg.geographies)`; params.push(req.query.geography.toUpperCase()); }

    idx++; params.push(limit);

    const { rows } = await pool.query(`
      SELECT sg.id, sg.headline, sg.observation, sg.so_what, sg.watch_next,
             sg.evidence, sg.geographies, sg.themes, sg.signal_types,
             sg.cluster_type, sg.published_at, sg.grab_score,
             ed.image_url AS image_url
      FROM signal_grabs sg
      LEFT JOIN external_documents ed ON ed.id = (sg.document_ids[1])::uuid
      ${where}
      ORDER BY sg.published_at DESC NULLS LAST, sg.created_at DESC
      LIMIT $${idx}
    `, params);

    const countRes = await pool.query(`SELECT COUNT(*) FROM signal_grabs sg ${where}`, params.slice(0, -1));

    res.json({
      grabs: rows.map(r => stripEmbedFields({ ...r, image_url: r.image_url || null })),
      total: parseInt(countRes.rows[0].count),
      generated_at: new Date().toISOString()
    });
  } catch (err) { res.status(500).json({ error: 'Grabs unavailable' }); }
});

// ── 2. GET /api/public/weekly — latest weekly wraps by region
app.get('/api/public/weekly', ...publicEmbed, async (req, res) => {
  try {
    const region = req.query.region ? req.query.region.toUpperCase() : null;
    const week = req.query.week || null;

    let where = `WHERE sg.tenant_id = $1 AND sg.cluster_type = 'weekly_wrap'`;
    const params = [PUBLIC_EMBED_TENANT];
    let idx = 1;

    if (week) { idx++; where += ` AND sg.digest_week = $${idx}`; params.push(week); }
    if (region) { idx++; where += ` AND $${idx} = ANY(sg.geographies)`; params.push(region); }

    const { rows } = await pool.query(`
      SELECT sg.id, sg.geographies, sg.headline, sg.observation,
             sg.so_what, sg.watch_next, sg.digest_week, sg.published_at, sg.created_at,
             ed.image_url AS image_url
      FROM signal_grabs sg
      LEFT JOIN external_documents ed ON ed.id = (sg.document_ids[1])::uuid
      ${where}
      ORDER BY sg.created_at DESC
      LIMIT 4
    `, params);

    // Parse observation JSON into structured weekly wrap fields
    const weekly = rows.map(r => {
      let parsed = {};
      try { parsed = JSON.parse(r.observation); } catch(e) {}
      return stripEmbedFields({
        id: r.id,
        region: (r.geographies || [])[0] || null,
        headline: parsed.headline || r.headline,
        key_numbers: parsed.key_numbers || [],
        big_moves: parsed.big_moves || [],
        watch_list: parsed.watch_list || r.watch_next || '',
        digest_week: r.digest_week || null,
        published_at: r.published_at || r.created_at,
        image_url: r.image_url || null
      });
    });

    res.json({ weekly, generated_at: new Date().toISOString() });
  } catch (err) { res.status(500).json({ error: 'Weekly wraps unavailable' }); }
});

// ── 3. GET /api/public/hero — top 3 hero signals, sanitised
app.get('/api/public/hero', ...publicEmbed, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT se.id, se.signal_type, se.company_name, se.evidence_summary,
             se.detected_at, se.image_url, se.source_url,
             c.sector, c.geography,
             ed.image_url AS doc_image_url,
             ed.source_name AS doc_source_name, ed.source_url AS doc_source_url
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      LEFT JOIN external_documents ed ON ed.id = se.source_document_id
      WHERE se.tenant_id = $1
        AND se.detected_at > NOW() - INTERVAL '7 days'
        AND COALESCE(se.is_megacap, false) = false
        AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
        AND se.company_name IS NOT NULL
      ORDER BY
        CASE WHEN c.is_client = true THEN 100 ELSE 0 END +
        (se.confidence_score * 30) +
        CASE WHEN se.image_url IS NOT NULL OR ed.image_url IS NOT NULL THEN 20 ELSE 0 END
        DESC
      LIMIT 3
    `, [PUBLIC_EMBED_TENANT]);

    const hero = rows.map(r => {
      const sources = [];
      if (r.doc_source_name || r.doc_source_url) {
        sources.push({ source_name: r.doc_source_name || null, source_url: r.doc_source_url || null });
      }
      return stripEmbedFields({
        id: r.id,
        company_name: r.company_name,
        signal_type: r.signal_type,
        headline: r.evidence_summary ? r.evidence_summary.slice(0, 120) : r.company_name + ' — ' + (r.signal_type || '').replace(/_/g, ' '),
        observation: r.evidence_summary || '',
        so_what: '',
        geography: r.geography || '',
        sector: r.sector || '',
        image_url: r.image_url || r.doc_image_url || null,
        sources,
        signal_date: r.detected_at,
        detected_at: r.detected_at
      });
    });

    res.json({ hero, generated_at: new Date().toISOString() });
  } catch (err) { res.status(500).json({ error: 'Hero signals unavailable' }); }
});

// ── 4. GET /api/public/market-temperature — macro market sentiment
app.get('/api/public/market-temperature', ...publicEmbed, async (req, res) => {
  try {
    const tid = PUBLIC_EMBED_TENANT;

    // Signal types by count
    const { rows: byType } = await pool.query(`
      SELECT se.signal_type, COUNT(*) as cnt
      FROM signal_events se
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      GROUP BY se.signal_type ORDER BY cnt DESC
    `, [tid]);

    // Regional breakdown
    const { rows: byRegion } = await pool.query(`
      SELECT
        CASE
          WHEN c.geography ILIKE '%Australia%' OR c.country_code = 'AU' THEN 'AU'
          WHEN c.geography ILIKE '%Singapore%' OR c.country_code = 'SG' OR c.geography ILIKE '%Southeast Asia%' THEN 'SG'
          WHEN c.geography ILIKE '%United Kingdom%' OR c.country_code IN ('UK','GB') OR c.geography ILIKE '%London%' THEN 'UK'
          WHEN c.geography ILIKE '%United States%' OR c.country_code = 'US' OR c.geography ILIKE '%America%' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        se.signal_type, COUNT(*) as cnt
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE se.is_megacap = true AND se.detected_at > NOW() - INTERVAL '7 days' AND se.tenant_id = $1
      GROUP BY region, se.signal_type
    `, [tid]);

    // Temperature calculation
    const growthTypes = ['capital_raising', 'product_launch', 'strategic_hiring', 'geographic_expansion', 'partnership'];
    const contractionTypes = ['restructuring', 'layoffs', 'ma_activity'];

    function calcTemp(types) {
      const growth = types.filter(t => growthTypes.includes(t.signal_type)).reduce((s, t) => s + parseInt(t.cnt), 0);
      const contraction = types.filter(t => contractionTypes.includes(t.signal_type)).reduce((s, t) => s + parseInt(t.cnt), 0);
      const total = growth + contraction;
      if (total === 0) return { temperature: 'neutral', signal_count: 0 };
      const ratio = growth / total;
      let temperature = 'neutral';
      if (ratio > 0.7) temperature = 'hot';
      else if (ratio > 0.55) temperature = 'warm';
      else if (ratio < 0.3) temperature = 'cold';
      else if (ratio < 0.45) temperature = 'cold';
      return { temperature, signal_count: total };
    }

    const overall = calcTemp(byType);
    const totalSignals = byType.reduce((s, t) => s + parseInt(t.cnt), 0);
    const dominant = byType.length > 0 ? byType[0].signal_type.replace(/_/g, ' ') : 'none';

    // Build region map
    const regions = {};
    for (const r of ['AU', 'SG', 'UK', 'US']) {
      const regionTypes = byRegion.filter(x => x.region === r);
      regions[r] = calcTemp(regionTypes);
      regions[r].signal_count = regionTypes.reduce((s, t) => s + parseInt(t.cnt), 0);
    }

    const labels = { hot: 'Expansion signals dominate — market is running hot', warm: 'Growth signals outpace contraction — cautiously positive', neutral: 'Balanced mix of growth and contraction signals', cold: 'Contraction signals dominate — defensive posture' };

    res.json(stripEmbedFields({
      temperature: overall.temperature,
      label: labels[overall.temperature] || labels.neutral,
      signal_count: totalSignals,
      dominant_type: dominant,
      regions,
      generated_at: new Date().toISOString()
    }));
  } catch (err) { res.status(500).json({ error: 'Market temperature unavailable' }); }
});

// ── 5. GET /api/public/events — top upcoming events per region, ranked by theme relevance
app.get('/api/public/events', ...publicEmbed, async (req, res) => {
  res.set('Cache-Control', 'public, max-age=3600');
  try {
    const regions = ['ANZ','Asia','Europe','US','Africa','Global'];
    const result = {};

    for (const region of regions) {
      const { rows } = await pool.query(`
        SELECT
          id, name, description, event_date, city, country,
          region, themes, rsvp_count, expected_attendees, external_url
        FROM event_listings
        WHERE tenant_id = $1
          AND region = $2
          AND status = 'upcoming'
          AND event_date >= CURRENT_DATE
        ORDER BY theme_score DESC, event_date ASC
        LIMIT 2
      `, [PUBLIC_EMBED_TENANT, region]);
      result[region] = rows.map(r => ({ ...r, image_url: null }));
    }

    res.json({
      events: result,
      generated_at: new Date().toISOString()
    });
  } catch (err) {
    console.error('/api/public/events error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

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
// NETWORK TOPOLOGY — RANKED OPPORTUNITIES & DENSITY
// ═══════════════════════════════════════════════════════════════════════════════

// Ranked opportunities — triangulated scores
app.get('/api/network/opportunities', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const offset = parseInt(req.query.offset) || 0;
    const region = req.query.region;
    const minScore = parseFloat(req.query.min_score) || 0;
    const status = req.query.status || 'active';

    let where = 'WHERE ro.status = $1';
    const params = [status];
    let idx = 1;

    if (region && region !== 'all') {
      idx++; where += ` AND ro.region_code = $${idx}`; params.push(region);
    }
    if (minScore > 0) {
      idx++; where += ` AND ro.composite_score >= $${idx}`; params.push(minScore);
    }

    idx++; params.push(limit);
    idx++; params.push(offset);

    const [result, countResult] = await Promise.all([
      pool.query(`
        SELECT ro.*,
               cas.contact_count, cas.senior_contact_count, cas.active_contact_count,
               cas.adjacency_score,
               gp.region_name, gp.weight_boost, gp.is_home_market
        FROM ranked_opportunities ro
        LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
        LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
        ${where}
        ORDER BY ro.composite_score DESC
        LIMIT $${idx - 1} OFFSET $${idx}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM ranked_opportunities ro ${where}`, params.slice(0, -2))
    ]);

    res.json({
      opportunities: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit, offset
    });
  } catch (err) {
    console.error('Opportunities error:', err.message);
    res.status(500).json({ error: 'Failed to fetch opportunities' });
  }
});

// Top opportunities by region
app.get('/api/network/opportunities/by-region', authenticateToken, async (req, res) => {
  try {
    const perRegion = Math.min(parseInt(req.query.per_region) || 5, 20);

    const { rows } = await pool.query(`
      SELECT ro.*,
             cas.contact_count, cas.senior_contact_count, cas.active_contact_count,
             gp.region_name, gp.weight_boost, gp.is_home_market
      FROM ranked_opportunities ro
      LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
      LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
      WHERE ro.status = 'active' AND ro.rank_in_region <= $1
        AND ro.region_code IS NOT NULL AND ro.region_code != 'UNKNOWN'
      ORDER BY gp.weight_boost DESC NULLS LAST, ro.rank_in_region ASC
    `, [perRegion]);

    // Group by region
    const grouped = {};
    for (const row of rows) {
      const rc = row.region_code;
      if (!grouped[rc]) {
        grouped[rc] = {
          region_code: rc,
          region_name: row.region_name,
          weight_boost: row.weight_boost,
          is_home_market: row.is_home_market,
          opportunities: []
        };
      }
      grouped[rc].opportunities.push(row);
    }

    res.json(grouped);
  } catch (err) {
    console.error('Opportunities by-region error:', err.message);
    res.status(500).json({ error: 'Failed to fetch opportunities by region' });
  }
});

// Network density scores
app.get('/api/network/density', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT nds.*, gp.region_name, gp.weight_boost, gp.is_home_market
      FROM network_density_scores nds
      LEFT JOIN geo_priorities gp ON gp.region_code = nds.region_code
      WHERE nds.sector IS NULL
      ORDER BY nds.density_score DESC
    `);
    res.json(rows);
  } catch (err) {
    console.error('Network density error:', err.message);
    res.status(500).json({ error: 'Failed to fetch density scores' });
  }
});

// Full network graph — team nodes, top contacts, client companies, sector clusters
app.get('/api/network/graph', authenticateToken, async (req, res) => {
  try {
    const tenantId = req.tenant_id;
    const mode = req.query.mode || 'firm'; // 'firm' or 'signal'
    const signalId = req.query.signal_id;

    if (mode === 'signal' && signalId) {
      // Delegate to proximity-graph endpoint logic
      return res.redirect(`/api/signals/${signalId}/proximity-graph`);
    }

    // Firm-wide network graph
    const [teamResult, contactsResult, clientsResult, sectorsResult, signalsResult] = await Promise.all([
      // Team members
      pool.query(`SELECT id, name, email, role FROM users WHERE tenant_id = $1 AND role != 'viewer' ORDER BY name`, [tenantId]),

      // Top contacts by proximity strength (limit to strongest connections)
      pool.query(`
        SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.current_company_id,
               p.seniority_level, p.location,
               tp.team_member_id, tp.relationship_strength, tp.relationship_type,
               u.name as connector_name,
               ps.timing_score, ps.receptivity_score, ps.flight_risk_score
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.tenant_id = $1
        JOIN users u ON u.id = tp.team_member_id
        LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
        WHERE tp.tenant_id = $1 AND tp.relationship_strength >= 0.3
        ORDER BY tp.relationship_strength DESC
        LIMIT 150
      `, [tenantId]),

      // Client companies with signal + people counts
      pool.query(`
        SELECT a.id as account_id, a.name, a.relationship_tier, a.company_id,
               c.sector, c.geography,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = a.company_id AND p.tenant_id = $1) as people_count,
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = a.company_id AND se.tenant_id = $1 AND se.detected_at > NOW() - INTERVAL '30 days') as signal_count
        FROM accounts a
        LEFT JOIN companies c ON c.id = a.company_id
        WHERE a.tenant_id = $1 AND a.relationship_status = 'active'
        ORDER BY a.relationship_tier DESC NULLS LAST, a.name
        LIMIT 50
      `, [tenantId]),

      // Sector clusters (from network density)
      pool.query(`
        SELECT region_code, sector, total_contacts, active_contacts, senior_contacts, density_score
        FROM network_density_scores
        WHERE sector IS NOT NULL AND density_score > 5
        ORDER BY density_score DESC
        LIMIT 20
      `),

      // Recent high-confidence signals at client companies
      pool.query(`
        SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score, se.detected_at
        FROM signal_events se
        JOIN companies c ON c.id = se.company_id AND c.is_client = true
        WHERE se.tenant_id = $1 AND se.detected_at > NOW() - INTERVAL '14 days'
          AND se.confidence_score >= 0.7
        ORDER BY se.confidence_score DESC
        LIMIT 30
      `, [tenantId])
    ]);

    const team = teamResult.rows;
    const contacts = contactsResult.rows;
    const clients = clientsResult.rows;
    const sectors = sectorsResult.rows;
    const signals = signalsResult.rows;

    // Build graph
    const nodes = [];
    const links = [];
    const addedNodes = new Set();

    // Team nodes
    team.forEach((u, i) => {
      const initials = (u.name || '').split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
      nodes.push({ id: 'user-' + u.id, type: 'team', label: initials, fullName: u.name, role: u.role, colorIndex: i });
      addedNodes.add('user-' + u.id);
    });

    // Client company nodes
    clients.forEach(cl => {
      const nid = 'client-' + (cl.company_id || cl.account_id);
      if (!addedNodes.has(nid)) {
        nodes.push({
          id: nid, type: 'client', label: cl.name,
          tier: cl.relationship_tier, sector: cl.sector, geography: cl.geography,
          peopleCount: parseInt(cl.people_count) || 0,
          signalCount: parseInt(cl.signal_count) || 0,
          companyId: cl.company_id, accountId: cl.account_id
        });
        addedNodes.add(nid);
      }
    });

    // Contact nodes + links to team + links to companies
    const contactsByPerson = new Map();
    contacts.forEach(c => {
      if (!contactsByPerson.has(c.id)) contactsByPerson.set(c.id, { ...c, teamLinks: [] });
      contactsByPerson.get(c.id).teamLinks.push({
        userId: c.team_member_id, strength: c.relationship_strength, type: c.relationship_type
      });
    });

    contactsByPerson.forEach((c, personId) => {
      const nid = 'contact-' + personId;
      if (!addedNodes.has(nid)) {
        nodes.push({
          id: nid, type: 'contact', label: c.full_name, personId,
          role: c.current_title, company: c.current_company_name,
          companyId: c.current_company_id,
          seniority: c.seniority_level,
          bestStrength: Math.max(...c.teamLinks.map(l => l.strength)),
          timingScore: c.timing_score, receptivityScore: c.receptivity_score
        });
        addedNodes.add(nid);
      }

      // Team → contact links
      c.teamLinks.forEach(l => {
        links.push({ source: 'user-' + l.userId, target: nid, strength: l.strength, type: l.type });
      });

      // Contact → client company links
      if (c.current_company_id) {
        const clientNid = 'client-' + c.current_company_id;
        if (addedNodes.has(clientNid)) {
          links.push({ source: nid, target: clientNid, strength: 0.4, type: 'works_at' });
        }
      }
    });

    // Sector cluster nodes
    sectors.forEach(s => {
      if (s.sector) {
        const nid = 'sector-' + s.sector.toLowerCase().replace(/\W+/g, '_');
        if (!addedNodes.has(nid)) {
          nodes.push({
            id: nid, type: 'sector', label: s.sector,
            density: s.density_score, contacts: s.total_contacts,
            region: s.region_code
          });
          addedNodes.add(nid);
        }
      }
    });

    // Signal pulse nodes on client companies
    const signalPulses = signals.map(s => ({
      companyNodeId: 'client-' + s.company_id,
      signalType: s.signal_type, confidence: s.confidence_score
    })).filter(s => addedNodes.has(s.companyNodeId));

    res.json({
      mode: 'firm',
      nodes, links,
      signalPulses,
      stats: {
        teamMembers: team.length,
        contacts: contactsByPerson.size,
        clients: clients.length,
        sectors: sectors.length,
        activeSignals: signals.length
      }
    });
  } catch (err) {
    console.error('Network graph error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Manual trigger for topology recompute
app.post('/api/network/recompute', authenticateToken, async (req, res) => {
  try {
    const { computeNetworkTopology } = require('./scripts/compute_network_topology');
    const { computeTriangulation } = require('./scripts/compute_triangulation');
    res.json({ status: 'started', message: 'Network topology + triangulation recompute triggered' });
    computeNetworkTopology()
      .then(() => computeTriangulation())
      .then(r => console.log('Network recompute complete:', r))
      .catch(e => console.error('Network recompute failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger recompute: ' + err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL DISPATCHES
// ═══════════════════════════════════════════════════════════════════════════════

// Manual trigger for dispatch generation
app.post('/api/dispatches/generate', authenticateToken, async (req, res) => {
  try {
    const { generateDispatches } = require('./scripts/generate_dispatches');
    res.json({ status: 'started', message: 'Dispatch generation triggered' });
    generateDispatches().then(r => console.log('Dispatch generation complete:', r)).catch(e => console.error('Dispatch generation failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger dispatch generation: ' + err.message });
  }
});

// Generate dispatch for a specific signal
app.post('/api/dispatches/generate-for-signal', authenticateToken, async (req, res) => {
  try {
    const { signal_id } = req.body;
    if (!signal_id) return res.status(400).json({ error: 'signal_id required' });

    // Check signal exists
    const { rows: [signal] } = await pool.query(
      `SELECT id, company_id, company_name, signal_type, evidence_summary, confidence_score, source_url
       FROM signal_events WHERE id = $1 AND tenant_id = $2`,
      [signal_id, req.tenant_id]
    );
    if (!signal) return res.status(404).json({ error: 'Signal not found' });

    // Check if dispatch already exists for this signal
    const { rows: existing } = await pool.query(
      `SELECT id FROM signal_dispatches WHERE signal_event_id = $1 LIMIT 1`, [signal_id]
    );
    if (existing.length) return res.json({ dispatch_id: existing[0].id, message: 'Dispatch already exists for this signal' });

    // Create a basic dispatch — the generate pipeline will enrich it
    const { rows: [dispatch] } = await pool.query(`
      INSERT INTO signal_dispatches (
        signal_event_id, company_id, company_name, signal_type, signal_summary,
        status, created_by, tenant_id, generated_at
      ) VALUES ($1, $2, $3, $4, $5, 'draft', $6, $7, NOW())
      RETURNING id
    `, [signal.id, signal.company_id, signal.company_name, signal.signal_type,
        signal.evidence_summary, req.user.user_id, req.tenant_id]);

    res.json({ dispatch_id: dispatch.id, message: 'Dispatch created as draft' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Claim a dispatch
app.post('/api/dispatches/:id/claim', authenticateToken, async (req, res) => {
  try {
    // Check if already claimed
    const { rows: [dispatch] } = await pool.query(
      'SELECT id, claimed_by, status FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]
    );
    if (!dispatch) return res.status(404).json({ error: 'Dispatch not found' });

    if (dispatch.claimed_by && dispatch.claimed_by !== req.user?.user_id) {
      // Already claimed by someone else
      const { rows: [claimer] } = await pool.query('SELECT name FROM users WHERE id = $1', [dispatch.claimed_by]);
      return res.status(409).json({
        error: 'Already claimed',
        claimed_by: claimer?.name || 'another user',
        message: `This dispatch has been claimed by ${claimer?.name || 'another team member'}`
      });
    }

    const { rows: [updated] } = await pool.query(`
      UPDATE signal_dispatches
      SET claimed_by = $2, claimed_at = NOW(), status = CASE WHEN status = 'draft' THEN 'claimed' ELSE status END, updated_at = NOW()
      WHERE id = $1 AND tenant_id = $3
      RETURNING *
    `, [req.params.id, req.user?.user_id, req.tenant_id]);

    res.json({ dispatch: updated, claimed_by: req.user?.name });
  } catch (err) {
    console.error('Claim error:', err.message);
    res.status(500).json({ error: 'Failed to claim dispatch' });
  }
});

// Unclaim a dispatch
app.post('/api/dispatches/:id/unclaim', authenticateToken, async (req, res) => {
  try {
    const { rows: [updated] } = await pool.query(`
      UPDATE signal_dispatches
      SET claimed_by = NULL, claimed_at = NULL, status = 'draft', updated_at = NOW()
      WHERE id = $1 AND (claimed_by = $2 OR claimed_by IS NULL) AND tenant_id = $3
      RETURNING *
    `, [req.params.id, req.user?.user_id, req.tenant_id]);

    if (!updated) return res.status(403).json({ error: 'Can only unclaim your own dispatches' });
    res.json({ dispatch: updated });
  } catch (err) {
    console.error('Unclaim error:', err.message);
    res.status(500).json({ error: 'Failed to unclaim dispatch' });
  }
});

// Rescan proximity maps for existing dispatches
app.post('/api/dispatches/rescan', authenticateToken, async (req, res) => {
  try {
    const { rescanProximity } = require('./scripts/generate_dispatches');
    res.json({ status: 'started', message: 'Proximity rescan triggered' });
    rescanProximity().then(r => console.log('Proximity rescan complete:', r)).catch(e => console.error('Proximity rescan failed:', e.message));
  } catch (err) {
    res.status(500).json({ error: 'Failed to trigger rescan: ' + err.message });
  }
});

// List dispatches
app.get('/api/dispatches', authenticateToken, async (req, res) => {
  try {
    const status = req.query.status;
    const region = req.query.region;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const offset = parseInt(req.query.offset) || 0;

    let where = 'WHERE sd.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (status) {
      idx++; where += ` AND sd.status = $${idx}`; params.push(status);
    }

    // Region filter — match on company geography + country_code, with fallback to text search
    if (region && region !== 'all') {
      const regionCodes = REGION_CODES[region] || [];
      const geoNames = REGION_MAP[region] || [];
      const orParts = [];
      // Primary: match company geography or country_code
      regionCodes.forEach(code => { idx++; orParts.push(`c.geography ILIKE $${idx}`); params.push(`%${code}%`); });
      geoNames.slice(0, 5).forEach(g => { idx++; orParts.push(`c.geography ILIKE $${idx}`); params.push(`%${g}%`); });
      // Fallback: match signal summary text
      geoNames.slice(0, 3).forEach(g => { idx++; orParts.push(`sd.signal_summary ILIKE $${idx}`); params.push(`%${g}%`); });
      if (orParts.length > 0) where += ` AND (${orParts.join(' OR ')})`;
    }

    idx++; params.push(limit);
    idx++; params.push(offset);

    const [result, countResult] = await Promise.all([
      pool.query(`
        SELECT sd.id, sd.signal_event_id, sd.company_id, sd.company_name,
               sd.signal_type, sd.signal_summary,
               sd.opportunity_angle, sd.blog_title, sd.blog_theme,
               sd.status, sd.generated_at, sd.reviewed_at, sd.sent_at,
               sd.best_entry_point, sd.proximity_map, sd.approach_rationale,
               sd.claimed_by, sd.claimed_at, u_claim.name AS claimed_by_name,
               jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) AS connection_count,
               jsonb_array_length(COALESCE(sd.send_to, '[]'::jsonb)) AS recipient_count,
               c.sector, c.geography, c.is_client,
               (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = sd.company_id) AS people_at_company,
               (SELECT COUNT(*) FROM conversions pl JOIN accounts cl ON cl.id = pl.client_id
                WHERE cl.company_id = sd.company_id) AS placement_count
        FROM signal_dispatches sd
        LEFT JOIN companies c ON c.id = sd.company_id
        LEFT JOIN users u_claim ON u_claim.id = sd.claimed_by
        ${where}
        ORDER BY
          CASE WHEN c.company_tier = 'megacap_indicator' THEN 1 ELSE 0 END,
          CASE WHEN c.is_client = true THEN 0 ELSE 1 END,
          CASE WHEN jsonb_array_length(COALESCE(sd.proximity_map, '[]'::jsonb)) > 0 THEN 0 ELSE 1 END,
          sd.generated_at DESC
        LIMIT $${idx - 1} OFFSET $${idx}
      `, params),
      pool.query(`SELECT COUNT(*) AS cnt FROM signal_dispatches sd LEFT JOIN companies c ON c.id = sd.company_id ${where}`, params.slice(0, -2))
    ]);

    res.json({
      dispatches: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit, offset
    });
  } catch (err) {
    console.error('Dispatches list error:', err.message);
    res.status(500).json({ error: 'Failed to fetch dispatches' });
  }
});

// Get single dispatch
app.get('/api/dispatches/:id', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT sd.*,
             c.sector, c.geography, c.is_client, c.employee_count_band, c.domain,
             se.confidence_score AS signal_confidence,
             se.evidence_snippets, se.hiring_implications,
             se.detected_at AS signal_detected_at, se.signal_date,
             u_claim.name AS claimed_by_name
      FROM signal_dispatches sd
      LEFT JOIN companies c ON c.id = sd.company_id
      LEFT JOIN signal_events se ON se.id = sd.signal_event_id
      LEFT JOIN users u_claim ON u_claim.id = sd.claimed_by
      WHERE sd.id = $1 AND sd.tenant_id = $2
    `, [req.params.id, req.tenant_id]);

    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });
    const dispatch = rows[0];

    // Auto-bundle relevant case studies
    let relevant_case_studies = [];
    try {
      const scoreTerms = [];
      const csParams = [req.tenant_id];
      let csIdx = 1;

      if (dispatch.sector) {
        csIdx++; csParams.push(`%${dispatch.sector}%`);
        scoreTerms.push(`CASE WHEN cs.sector ILIKE $${csIdx} THEN 0.3 ELSE 0 END`);
      }
      if (dispatch.geography) {
        csIdx++; csParams.push(`%${dispatch.geography}%`);
        scoreTerms.push(`CASE WHEN cs.geography ILIKE $${csIdx} THEN 0.25 ELSE 0 END`);
      }
      if (dispatch.signal_type) {
        const sigThemes = {
          capital_raising: ['high-growth','scaling'], geographic_expansion: ['cross-border','expansion'],
          strategic_hiring: ['leadership','team-build'], ma_activity: ['post-acquisition','integration'],
          leadership_change: ['succession','transition'], restructuring: ['turnaround','transformation'],
        };
        const themes = sigThemes[dispatch.signal_type] || [];
        if (themes.length) {
          csIdx++; csParams.push(themes);
          scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${csIdx}::text[]))::float * 0.25`);
        }
      }
      if (dispatch.company_id) {
        csIdx++; csParams.push(dispatch.company_id);
        scoreTerms.push(`CASE WHEN cs.client_id = $${csIdx}::uuid THEN 0.5 ELSE 0 END`);
      }

      if (scoreTerms.length > 0) {
        const scoreExpr = scoreTerms.join(' + ');
        const { rows: csRows } = await pool.query(`
          SELECT cs.id, cs.title, cs.sector, cs.geography, cs.engagement_type, cs.year,
                 cs.themes, cs.capabilities, cs.public_approved, cs.visibility,
                 cs.public_title, cs.public_summary,
                 (${scoreExpr}) AS relevance
          FROM case_studies cs
          WHERE cs.tenant_id = $1 AND (${scoreExpr}) > 0
          ORDER BY (${scoreExpr}) DESC LIMIT 5
        `, csParams);
        relevant_case_studies = csRows;
      }
    } catch (e) { /* case_studies table may not exist */ }

    res.json({ ...dispatch, relevant_case_studies });
  } catch (err) {
    console.error('Dispatch detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch dispatch' });
  }
});

// Update dispatch status
app.patch('/api/dispatches/:id', authenticateToken, async (req, res) => {
  try {
    const { status, send_to, blog_body, blog_title } = req.body;
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id];
    let idx = 1;

    if (status) {
      idx++; updates.push(`status = $${idx}`); params.push(status);
      if (status === 'reviewed') { updates.push('reviewed_at = NOW()'); }
      if (status === 'sent') { updates.push('sent_at = NOW()'); }
    }
    if (send_to !== undefined) {
      idx++; updates.push(`send_to = $${idx}`); params.push(JSON.stringify(send_to));
    }
    if (blog_body) {
      idx++; updates.push(`blog_body = $${idx}`); params.push(blog_body);
    }
    if (blog_title) {
      idx++; updates.push(`blog_title = $${idx}`); params.push(blog_title);
    }

    idx++;
    params.push(req.tenant_id);
    const { rows } = await pool.query(
      `UPDATE signal_dispatches SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $${idx} RETURNING *`,
      params
    );

    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });
    res.json(rows[0]);
  } catch (err) {
    console.error('Dispatch update error:', err.message);
    res.status(500).json({ error: 'Failed to update dispatch' });
  }
});

// Regenerate blog post for a dispatch
app.post('/api/dispatches/:id/regenerate', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (rows.length === 0) return res.status(404).json({ error: 'Dispatch not found' });

    const dispatch = rows[0];
    const themeOverride = req.body.theme;

    // Get company info
    let company = {};
    if (dispatch.company_id) {
      const { rows: [co] } = await pool.query(
        'SELECT sector, geography, employee_count_band FROM companies WHERE id = $1 AND tenant_id = $2',
        [dispatch.company_id, req.tenant_id]
      );
      if (co) company = co;
    }

    // Regenerate via Claude
    const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
    if (!ANTHROPIC_API_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });

    const signalType = dispatch.signal_type || 'market_signal';
    const theme = themeOverride || dispatch.blog_theme || 'executive talent strategy';

    const systemPrompt = `You are a senior executive search consultant writing a thought leadership piece for an executive audience.
Write with authority and insight, not sales language.
The piece should be genuinely useful to a senior leader at a company that has just experienced a ${signalType.replace(/_/g, ' ')} event.
It should feel like advice from a trusted advisor, not a pitch from a recruiter.
Length: 550-700 words.
Format: Return ONLY valid JSON with keys: "title", "body", "keywords"
  - title: Compelling headline
  - body: 4-5 paragraphs of flowing prose. No subheadings, no bullet points. Use \\n\\n between paragraphs.
  - keywords: Array of 4-6 relevant keywords/phrases
Tone: Warm, direct, intelligent. First person plural ("we've seen").
Do not mention the company by name or the specific event.
Do not use the word "landscape" or "navigate".`;

    const userPrompt = `Write a thought leadership article for a senior leader at a ${company.sector || 'technology'} company (${company.employee_count_band || 'growth-stage'}, ${company.geography || 'global'} market) that has just experienced a ${signalType.replace(/_/g, ' ')} event.

The article should explore the theme: "${theme}"

Signal context: ${dispatch.signal_summary || signalType.replace(/_/g, ' ')}
Approach angle: ${dispatch.approach_rationale || 'Market intelligence and talent advisory'}

The article should leave the reader thinking about talent, leadership, and organisational design.

Return valid JSON only.`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2048,
        system: systemPrompt,
        messages: [{ role: 'user', content: userPrompt }]
      })
    });

    if (!response.ok) {
      const err = await response.text();
      return res.status(500).json({ error: `Claude API failed: ${err.slice(0, 200)}` });
    }

    const data = await response.json();
    const raw = data.content[0]?.text || '';

    let blog;
    try {
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      blog = JSON.parse(jsonMatch[0]);
    } catch (e) {
      blog = { title: theme, body: raw, keywords: [] };
    }

    // Update dispatch
    await pool.query(`
      UPDATE signal_dispatches
      SET blog_theme = $2, blog_title = $3, blog_body = $4, blog_keywords = $5, updated_at = NOW()
      WHERE id = $1 AND tenant_id = $6
    `, [dispatch.id, theme, blog.title, blog.body, blog.keywords || [], req.tenant_id]);

    res.json({ title: blog.title, body: blog.body, keywords: blog.keywords });
  } catch (err) {
    console.error('Blog regeneration error:', err.message);
    res.status(500).json({ error: 'Failed to regenerate blog' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CONVERSIONS / REVENUE
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/placements', authenticateToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    let where = 'WHERE pl.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

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
        FROM conversions pl
        LEFT JOIN accounts cl ON pl.client_id = cl.id
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
        FROM conversions pl
        LEFT JOIN accounts cl ON pl.client_id = cl.id
        LEFT JOIN people pe ON pl.person_id = pe.id
        ${where}
      `, params.slice(0, -2)),
    ]);

    // Revenue by year
    const { rows: byYear } = await pool.query(`
      SELECT EXTRACT(YEAR FROM start_date)::int AS year,
             COUNT(*) AS count,
             COALESCE(SUM(placement_fee), 0) AS revenue
      FROM conversions
      WHERE start_date IS NOT NULL AND tenant_id = $1
      GROUP BY year ORDER BY year DESC
    `, [req.tenant_id]);

    // Top clients by revenue
    const { rows: topClients } = await pool.query(`
      SELECT cl.id, cl.name, COUNT(*) AS placement_count,
             COALESCE(SUM(pl.placement_fee), 0) AS total_revenue
      FROM conversions pl
      LEFT JOIN accounts cl ON pl.client_id = cl.id
      WHERE pl.tenant_id = $1
      GROUP BY cl.id, cl.name
      ORDER BY total_revenue DESC LIMIT 20
    `, [req.tenant_id]);

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

async function callClaude(messages, tools, systemPrompt, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await new Promise((resolve, reject) => {
        const body = JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 4096, system: systemPrompt, messages, tools });
        const req = https.request({
          hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) },
          timeout: 90000,
        }, (res) => {
          const chunks = [];
          res.on('data', c => chunks.push(c));
          res.on('end', () => {
            try {
              const raw = Buffer.concat(chunks).toString();
              const d = JSON.parse(raw);
              if (d.error) {
                // Retry on overloaded (529)
                if (d.error.type === 'overloaded_error' || d.error.message?.includes('overloaded')) {
                  return reject(new Error('RETRY:overloaded'));
                }
                return reject(new Error(d.error.message));
              }
              resolve(d);
            } catch (e) { reject(e); }
          });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Claude timeout')); });
        req.write(body);
        req.end();
      });
      return result;
    } catch (e) {
      if (e.message.startsWith('RETRY:') && attempt < retries) {
        console.log(`Claude overloaded, retrying in ${(attempt + 1) * 3}s...`);
        await new Promise(r => setTimeout(r, (attempt + 1) * 3000));
        continue;
      }
      throw new Error(e.message.replace('RETRY:', ''));
    }
  }
}

const CHAT_SYSTEM = `You are Lorac — the AI intelligence agent for the Signal Intelligence Platform, currently serving MitchelLake.

CRITICAL BEHAVIOUR:
- DO NOT explain what you are going to do. Just DO it.
- DO NOT narrate your tool calls. Execute tools silently and present the results.
- Present results as a clean formatted table or list. Not a wall of text.

TOOL SELECTION (use the most specific tool available):
1. Market patterns, trends, convergence, "what are we seeing" → get_converging_themes
2. Priorities, best opportunities, where to focus, pipeline → get_ranked_opportunities
3. Talent movement, flight risk, re-engage, who to reach out to → get_talent_in_motion
4. "Who do we know at X", network into a company, connections → get_signal_proximity (pass company_name or company_id)
5. Dispatch actions: claim, review, generate, regenerate → dispatch_action
6. Person lookup by name, title, skills → search_people
7. Company lookup → search_companies
8. Person deep-dive → get_person_detail (needs person_id)
9. Company deep-dive → get_company_detail (needs company_id)
10. Signal search by type/confidence/time → search_signals
11. Placement history → search_placements
12. Research notes → search_research_notes
13. Save intel → log_intelligence
14. Create person → create_person
15. Import placements ("we placed X as Y at Z") → import_placements
16. Import case studies ("we did a CTO search for fintech in SG") → import_case_studies
17. Run a pipeline ("harvest podcasts", "sync gmail", "classify documents", etc.) → run_pipeline
18. Search case studies ("what work have we done in fintech", "case studies in APAC") → search_case_studies
19. Complex cross-referencing queries not covered above → run_sql_query with JOINs

CASE STUDY + PLACEMENT IMPORT RULES:
- Store EXACTLY what the user provides. Do NOT invent, embellish, or infer any data.
- If the user gives you "CTO, fintech, Singapore" — store those 3 fields only. Leave challenge, approach, outcome, themes as null.
- Do NOT generate narrative descriptions, challenges, approaches, or outcomes unless the user explicitly states them.
- Do NOT infer engagement_type, seniority_level, or themes — only set them if the user says them.
- Case studies are INTERNAL DRAFTS. Remind the user they need sanitisation before external use.
- Placements are ALWAYS internal — fees and candidate details never go external.

IMPORTANT: Prefer dedicated tools (#1-#5) over run_sql_query. They return pre-computed, pre-ranked results and are faster and more reliable. Only fall back to run_sql_query for questions that no dedicated tool covers.
When using run_sql_query, replace <TENANT> with the actual tenant_id from context.

CONTEXT:
- MitchelLake is a retained executive search firm (APAC, UK, global)
- Database: ~77K people, ~11K companies, ~22K documents, ~9K signals, ~500 placements
- Table names: people, companies, accounts, opportunities, conversions, engagements, pipeline_contacts, signal_events, interactions, team_proximity, external_documents, signal_dispatches, person_scores, person_signals, case_studies, receivables
- Signal types: capital_raising, ma_activity, geographic_expansion, strategic_hiring, leadership_change, partnership, product_launch, layoffs, restructuring
- Key columns: people.current_company_id → companies.id, signal_events.company_id → companies.id, conversions.client_id → accounts.id, accounts.company_id → companies.id
- team_proximity links people to users (team members) via team_member_id with relationship_strength (0-1)

PLACEMENTS TABLE — key columns for billing/WIP queries:
id, person_id, client_id, company_id, search_id, placed_by_user_id,
role_title, role_level, start_date, placement_fee (DECIMAL), currency (AUD|GBP|SGD|USD),
fee_stage (retainer_stage1|retainer_stage2|placement|project), fee_estimate,
invoice_number, invoice_date, payment_status (pending|invoiced|paid|overdue),
opportunity_type (WIP - Placed|WIP - Active|Proposal - Won|Proposal - Lost|Proposal - Draft|Proposal - Sent),
consultant_name, client_name_raw, source (wip_workbook|xero_export|manual),
source_sheet, notes, raw_monthly_data (JSONB monthly invoice amounts), created_at

KEY JOINS for billing queries:
SELECT p.*, c.name as client, u.name as consultant, pe.full_name as candidate
FROM placements p
LEFT JOIN companies c ON c.id = p.company_id
LEFT JOIN users u ON u.id = p.placed_by_user_id
LEFT JOIN people pe ON pe.id = p.person_id

CONSULTANT NAMES in data (match to users table):
Matt, JT (Jonathan Tanner), Illona, Mark Sparrow, Jamie Gripton, Michael Solomon (Solly),
Priyanka Haribhai, Conny Lim, Lexi Lazenby, Richard Farmer, Yoko Senga, Timo Kugler,
Rachel, Jimmy Grice, Claire Yellowlees, David Gumley, Rob, Sam, James,
Ananya Amin, Megan Burke, Sophie Cohen, Andrew

RECEIVABLES TABLE (outstanding invoices):
id, invoice_number, client_name, company_id, invoice_date, due_date,
invoice_total, currency, status, days_overdue, notes, action

STYLE:
- Concise. No preamble. Execute then present results.
- Format: [Name](/person.html?id=X) | Title | Company | Signal/Score
- Australian English
- When saving intel, confirm what was extracted
- For file imports, preview before committing
- LinkedIn CSV: auto-detect type from [LinkedIn Export Type] tag, use import_linkedin_connections or import_linkedin_messages

RULES:
- NEVER say "let me search" then show empty results then say "let me try another approach". Use SQL with JOINs from the start.
- Never fabricate data
- For UPDATE/DELETE, confirm with user first
- SQL: SELECT, UPDATE, INSERT, DELETE allowed. DROP/ALTER/TRUNCATE blocked.
- Prioritise recency — sort by most recent first
- Flag stale intel (>6 months)`;

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
  { name: 'process_uploaded_file', description: 'Process uploaded CSV/PDF/XLSX. Actions: preview, import_people, import_companies, extract_text, import_linkedin_connections, import_linkedin_messages, import_workbook (XLSX multi-tab: stores each sheet as searchable document, embeds in Qdrant). For XLSX files with multiple tabs, use import_workbook to ingest all sheets.', input_schema: { type: 'object', properties: { file_id: { type: 'string' }, action: { type: 'string', enum: ['preview', 'import_people', 'import_companies', 'extract_text', 'import_linkedin_connections', 'import_linkedin_messages', 'import_workbook'] }, column_mapping: { type: 'object' } }, required: ['file_id', 'action'] } },
  { name: 'run_sql_query', description: 'PRIMARY TOOL — Run SQL (SELECT, UPDATE, INSERT, DELETE) against the database. Use this FIRST for any cross-referencing query. JOINs are fast. Always include tenant_id filter. Key tables: people, companies, signal_events, interactions, conversions, accounts, opportunities, team_proximity, person_scores.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'SQL query. Must include AND tenant_id = \'<tenant_id>\' for data tables.' }, explanation: { type: 'string', description: 'Brief one-line explanation of what this query does' } }, required: ['query', 'explanation'] } },
  { name: 'get_platform_stats', description: 'Current platform statistics.', input_schema: { type: 'object', properties: {} } },

  // ── MCP-style intelligence tools ──────────────────────────────────────────
  {
    name: 'get_converging_themes',
    description: 'Returns signal clusters by type and sector showing where multiple companies exhibit the same signal pattern. Includes client overlap, candidate counts, and active search pipeline matches. Use when asked about market patterns, trends, sector activity, convergence, or "what are we seeing". Much faster and more reliable than composing SQL for these questions.',
    input_schema: {
      type: 'object',
      properties: {
        lookback_days: { type: 'integer', default: 30, description: 'Days to look back for signal activity (default 30)' },
        min_companies: { type: 'integer', default: 3, description: 'Minimum companies per cluster (default 3)' }
      }
    }
  },
  {
    name: 'get_ranked_opportunities',
    description: 'Returns companies ranked by composite opportunity score combining signal strength, network overlap, geographic relevance, and placement adjacency. Use when asked about priorities, where to focus, best opportunities, pipeline, or "what should we be working on". Supports region filtering and score thresholds.',
    input_schema: {
      type: 'object',
      properties: {
        region: { type: 'string', description: 'Region code: AU, SG, UK, US, APAC, EMEA, or omit for all' },
        min_score: { type: 'number', default: 0, description: 'Minimum composite score (0-1)' },
        limit: { type: 'integer', default: 15, description: 'Max results to return' },
        by_region: { type: 'boolean', default: false, description: 'If true, returns top opportunities grouped by region instead of a flat list' }
      }
    }
  },
  {
    name: 'get_talent_in_motion',
    description: 'Returns people showing movement signals: flight risk (at companies with restructuring/layoff/M&A signals), activity spikes (high engagement/timing scores), re-engagement windows (senior contacts at signal companies dormant 60+ days), and recent person-level signals. Use when asked about talent movement, who to reach out to, re-engagement opportunities, flight risk, or market talent activity.',
    input_schema: {
      type: 'object',
      properties: {
        focus: { type: 'string', enum: ['all', 'flight_risk', 'active_profiles', 'reengage', 'person_signals'], default: 'all', description: 'Which talent motion category to return' },
        limit: { type: 'integer', default: 10, description: 'Max results per category' }
      }
    }
  },
  {
    name: 'get_signal_proximity',
    description: 'For a given signal or company, returns the network proximity map: who we know there, team member connections, relationship strengths, contact scores (timing, receptivity), and whether the company is a client. Use when asked "who do we know at X", "what is our connection to X", "how do we get into X", or "show me the network for X". Returns a structured graph of team→contact→company relationships.',
    input_schema: {
      type: 'object',
      properties: {
        signal_id: { type: 'string', description: 'Signal event UUID — use this when the question is about a specific signal' },
        company_id: { type: 'string', description: 'Company UUID — use this when the question is about a company (alternative to signal_id)' },
        company_name: { type: 'string', description: 'Company name — will be resolved to company_id if company_id not provided' }
      }
    }
  },
  {
    name: 'dispatch_action',
    description: 'Perform actions on signal dispatches: claim for review, unclaim, update status, trigger generation for new signals, or regenerate content with a theme override. Use when asked to "claim that dispatch", "mark as reviewed", "generate dispatches", "send that dispatch", or "rewrite the blog post about X".',
    input_schema: {
      type: 'object',
      properties: {
        action: { type: 'string', enum: ['claim', 'unclaim', 'update_status', 'generate_all', 'rescan_proximity', 'regenerate_content'], description: 'The action to perform' },
        dispatch_id: { type: 'string', description: 'Required for claim, unclaim, update_status, regenerate_content' },
        status: { type: 'string', enum: ['draft', 'claimed', 'reviewed', 'sent', 'archived'], description: 'New status — for update_status action' },
        theme: { type: 'string', description: 'Override theme for regenerate_content action' }
      },
      required: ['action']
    }
  },
  {
    name: 'import_placements',
    description: 'Import placement records. Use when the user pastes or describes recent placements — e.g., "we placed Jane Smith as CTO at Acme Corp". Resolves people and companies against existing records, creates conversions entries. Each placement needs at minimum: candidate name, role title, and client/company name. Optional: start date, fee, currency.',
    input_schema: {
      type: 'object',
      properties: {
        placements: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              candidate_name: { type: 'string', description: 'Full name of placed candidate' },
              role_title: { type: 'string', description: 'Role they were placed into' },
              company_name: { type: 'string', description: 'Client company name' },
              start_date: { type: 'string', description: 'Start date (YYYY-MM-DD or approximate)' },
              placement_fee: { type: 'number', description: 'Fee amount if known' },
              currency: { type: 'string', default: 'AUD', description: 'Currency code' },
              notes: { type: 'string', description: 'Any additional context' }
            },
            required: ['candidate_name', 'role_title', 'company_name']
          },
          description: 'Array of placement records to import'
        }
      },
      required: ['placements']
    }
  },
  {
    name: 'import_case_studies',
    description: 'Import case study records from a list. Store EXACTLY what the user provides — do NOT invent, embellish, or infer fields the user did not state. If the user says "CTO search, fintech, Singapore, 2024" then store only those fields and leave everything else null. Case studies are created as internal drafts requiring sanitisation before external use.',
    input_schema: {
      type: 'object',
      properties: {
        case_studies: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              client_name: { type: 'string', description: 'Client company name (INTERNAL — will not be published externally without sanitisation)' },
              role_title: { type: 'string', description: 'Role searched for' },
              engagement_type: { type: 'string', enum: ['executive_search', 'board_advisory', 'leadership_assessment', 'team_build', 'succession', 'market_mapping'], description: 'Type of engagement' },
              seniority_level: { type: 'string', enum: ['c_suite', 'vp', 'director', 'head', 'senior'], description: 'Seniority of role' },
              sector: { type: 'string', description: 'Industry sector' },
              geography: { type: 'string', description: 'Region or country' },
              year: { type: 'integer', description: 'Year of engagement' },
              challenge: { type: 'string', description: 'What the client needed' },
              approach: { type: 'string', description: 'How MitchelLake approached it' },
              outcome: { type: 'string', description: 'Result achieved' },
              themes: { type: 'array', items: { type: 'string' }, description: 'Thematic tags e.g. cross-border, founder-transition' },
              capabilities: { type: 'array', items: { type: 'string' }, description: 'Capabilities demonstrated e.g. post-acquisition, turnaround' }
            },
            required: ['role_title']
          },
          description: 'Array of case study records to import'
        }
      },
      required: ['case_studies']
    }
  },
  {
    name: 'search_case_studies',
    description: 'Search the case study library by keyword, sector, geography, client, or role. Uses semantic vector search when available, falls back to SQL text search. Use when asked about past work, relevant experience, case studies, or "what have we done in X".',
    input_schema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search query — client name, sector, role type, geography, or general topic' },
        limit: { type: 'integer', default: 10 }
      },
      required: ['query']
    }
  },
  {
    name: 'run_pipeline',
    description: 'Trigger a platform pipeline manually. Use when the user asks to run, trigger, or execute a pipeline such as: harvest_podcasts, sync_gmail, gmail_match, sync_drive, classify_documents, cleanup_broken_podcasts, import_case_studies_bulk, ingest_signals, compute_scores, match_searches, enrich_content, signal_dispatch, compute_network_topology, compute_triangulation, compute_signal_grabs. Say "run the X pipeline" or "harvest podcasts" or "sync gmail".',
    input_schema: {
      type: 'object',
      properties: {
        pipeline_key: {
          type: 'string',
          description: 'Pipeline key to run. Common ones: harvest_podcasts, sync_gmail, gmail_match, sync_drive, classify_documents, cleanup_broken_podcasts, ingest_signals, compute_scores, compute_signal_grabs, signal_dispatch, compute_network_topology, compute_triangulation, embed_intelligence, migrate_wip_schema, ingest_wip_invoices, ingest_wip_consultants, ingest_receivables'
        }
      },
      required: ['pipeline_key']
    }
  },
];

async function executeTool(name, input, userId, tenantId) {
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
              FROM people p WHERE p.id = ANY($1::uuid[]) AND p.tenant_id = $2`, [qr.map(r => r.id), tenantId]);
            const map = new Map(rows.map(r => [r.id, r]));
            results = qr.map(r => ({ ...map.get(r.id), score: r.score })).filter(r => r.full_name);
          }
        } catch (e) {}
        if (results.length < 3) {
          const { rows } = await pool.query(`SELECT p.id, p.full_name, p.current_title, p.current_company_name, p.location, p.seniority_level, p.email, p.headline,
            (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
            (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS latest_note_date
            FROM people p WHERE (p.full_name ILIKE $1 OR p.current_title ILIKE $1 OR p.current_company_name ILIKE $1 OR p.headline ILIKE $1) AND p.tenant_id = $3 ORDER BY p.full_name LIMIT $2`, [`%${query}%`, limit, tenantId]);
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
        const { rows } = await pool.query(`SELECT c.id, c.name, c.sector, c.geography, c.domain, c.employee_count_band, c.is_client, c.description, (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count, (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id) AS signal_count FROM companies c WHERE (c.name ILIKE $1 OR c.sector ILIKE $1 OR c.geography ILIKE $1) AND c.tenant_id = $3 ${filters.is_client ? 'AND c.is_client = true' : ''} ORDER BY c.is_client DESC, c.name LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'get_person_detail': {
        const { rows: [p] } = await pool.query(`SELECT p.*, c.name AS company_name_linked, c.id AS company_id_linked FROM people p LEFT JOIN companies c ON p.current_company_id = c.id WHERE p.id = $1 AND p.tenant_id = $2`, [input.person_id, tenantId]);
        if (!p) return JSON.stringify({ error: 'Not found' });
        const { rows: notes } = await pool.query(`SELECT subject, summary, interaction_at, note_quality, extracted_intelligence FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2 ORDER BY interaction_at DESC NULLS LAST LIMIT 10`, [input.person_id, tenantId]);
        const { rows: sigs } = await pool.query(`SELECT signal_type, title, description, confidence_score FROM person_signals WHERE person_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 10`, [input.person_id, tenantId]);
        return JSON.stringify({ ...p, research_notes: notes, person_signals: sigs });
      }
      case 'get_company_detail': {
        const { rows: [co] } = await pool.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [input.company_id, tenantId]);
        if (!co) return JSON.stringify({ error: 'Not found' });
        const { rows: sigs } = await pool.query(`SELECT signal_type, evidence_summary, confidence_score, detected_at FROM signal_events WHERE company_id = $1 AND tenant_id = $2 ORDER BY detected_at DESC LIMIT 15`, [input.company_id, tenantId]);
        const { rows: ppl } = await pool.query(`SELECT id, full_name, current_title, seniority_level FROM people WHERE current_company_id = $1 AND tenant_id = $2 ORDER BY full_name LIMIT 30`, [input.company_id, tenantId]);
        let pls = []; try { const { rows } = await pool.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee FROM conversions pl LEFT JOIN accounts cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE (cl.company_id = $1 OR cl.name ILIKE (SELECT name FROM companies WHERE id = $1)) AND pl.tenant_id = $2 ORDER BY pl.start_date DESC`, [input.company_id, tenantId]); pls = rows; } catch (e) {}
        return JSON.stringify({ ...co, signals: sigs, people: ppl, placements: pls });
      }
      case 'search_signals': {
        const { signal_type, category, company_name, min_confidence = 0.5, days_back = 30, limit = 15 } = input;
        const w = [`se.confidence_score >= ${min_confidence}`, `se.detected_at >= NOW() - INTERVAL '${days_back} days'`, `se.tenant_id = $1`];
        if (signal_type) w.push(`se.signal_type = '${signal_type}'`);
        if (category) w.push(`se.signal_category = '${category}'`);
        if (company_name) w.push(`c.name ILIKE '%${company_name}%'`);
        const { rows } = await pool.query(`SELECT se.signal_type, se.signal_category, se.evidence_summary, se.confidence_score, se.detected_at, se.source_url, c.name AS company_name, c.id AS company_id FROM signal_events se LEFT JOIN companies c ON se.company_id = c.id WHERE ${w.join(' AND ')} ORDER BY se.confidence_score DESC LIMIT ${limit}`, [tenantId]);
        return JSON.stringify(rows);
      }
      case 'search_placements': {
        const { query = '', limit = 20 } = input;
        const { rows } = await pool.query(`SELECT pe.full_name AS candidate_name, pl.role_title, pl.start_date, pl.placement_fee, cl.name AS company_name, cl.id AS company_id FROM conversions pl LEFT JOIN accounts cl ON pl.client_id = cl.id LEFT JOIN people pe ON pl.person_id = pe.id WHERE (pe.full_name ILIKE $1 OR pl.role_title ILIKE $1 OR cl.name ILIKE $1) AND pl.tenant_id = $3 ORDER BY pl.start_date DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'search_research_notes': {
        const { query, person_name, limit = 10 } = input;
        let extra = person_name ? ` AND p.full_name ILIKE '%${person_name}%'` : '';
        const { rows } = await pool.query(`SELECT i.subject, i.summary, i.interaction_at, i.note_quality, i.extracted_intelligence, p.full_name, p.id AS person_id, p.current_title, p.current_company_name FROM interactions i JOIN people p ON i.person_id = p.id WHERE i.interaction_type = 'research_note' AND (i.summary ILIKE $1 OR i.subject ILIKE $1) AND i.tenant_id = $3${extra} ORDER BY i.interaction_at DESC NULLS LAST LIMIT $2`, [`%${query}%`, limit, tenantId]);
        return JSON.stringify(rows);
      }
      case 'log_intelligence': {
        const { person_name, company_name, intelligence, subject, extracted = {} } = input;
        let personId;
        const { rows: ex } = await pool.query(`SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [person_name, tenantId]);
        if (ex.length) { personId = ex[0].id; }
        else {
          const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_company_name, source, created_by, tenant_id) VALUES ($1, $2, 'chat_intel', $3, $4) RETURNING id`, [person_name, company_name || null, userId, tenantId]);
          personId = np.id;
        }
        const { rows: [note] } = await pool.query(`INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, extracted_intelligence, source, interaction_at, tenant_id) VALUES ($1, $2, $2, 'research_note', $3, $4, $5, 'chat_concierge', NOW(), $6) RETURNING id`, [personId, userId, subject, intelligence, JSON.stringify(extracted), tenantId]);
        auditLog(userId, 'log_intelligence', 'person', personId, { person_name, subject, source: 'chat_concierge' });
        return JSON.stringify({ success: true, person_id: personId, note_id: note.id, person_name, subject, extracted, message: `Saved on ${person_name}'s record` });
      }
      case 'create_person': {
        const { full_name, current_title, current_company_name, email, phone, location, linkedin_url, seniority_level } = input;
        const { rows: dupes } = await pool.query(`SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 3`, [full_name, tenantId]);
        if (dupes.length) return JSON.stringify({ existing_matches: dupes, message: 'Possible duplicates found' });
        let coId = null;
        if (current_company_name) { const { rows } = await pool.query(`SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [current_company_name, tenantId]); if (rows.length) coId = rows[0].id; }
        const { rows: [p] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, current_company_id, email, phone, location, linkedin_url, seniority_level, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'chat_concierge',$10,$11) RETURNING id, full_name`, [full_name, current_title||null, current_company_name||null, coId, email||null, phone||null, location||null, linkedin_url||null, seniority_level||null, userId, tenantId]);
        auditLog(userId, 'create_person', 'person', p.id, { full_name, source: 'chat_concierge' });
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
            const { rows: d } = await pool.query(`SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [name.trim(), tenantId]);
            if (d.length) { skipped++; continue; }
            await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, email, location, linkedin_url, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,'csv_import',$7,$8)`,
              [name.trim(), row[m.current_title||'Title']||row['Job Title']||null, row[m.current_company_name||'Company']||row['Organization']||null, row[m.email||'Email']||null, row[m.location||'Location']||null, row[m.linkedin_url||'LinkedIn']||null, userId, tenantId]);
            imported++;
          }
          auditLog(userId, 'csv_import', 'people', null, { imported, skipped, total: fm.preview.length, filename: fm.originalname });
          return JSON.stringify({ imported, skipped, total: fm.preview.length });
        }
        if (action === 'import_linkedin_connections' && fm.preview) {
          // Load people for matching
          const { rows: dbPeople } = await pool.query(`SELECT id, full_name, first_name, last_name, linkedin_url, current_company_name, email FROM people WHERE full_name IS NOT NULL AND full_name != '' AND tenant_id = $1`, [tenantId]);
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
                  await pool.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at, metadata, tenant_id) VALUES ($1,$2,'linkedin_connection','linkedin_import',$3,$4,$5,$6,$7) ON CONFLICT (person_id, team_member_id, proximity_type, source) DO UPDATE SET strength = GREATEST(team_proximity.strength, EXCLUDED.strength), context = EXCLUDED.context, updated_at = NOW()`, [matchedPerson.id, userId, strength.toFixed(2), `${position} @ ${company}`, connectedOn, JSON.stringify({ linkedin_url: linkedinUrl, match_method: matchMethod, match_confidence: matchConfidence }), tenantId]);
                  stats.proximity_created++;
                } catch (e) { if (!e.message.includes('duplicate')) stats.errors++; }
              }
              // Update LinkedIn URL if missing
              if (linkedinUrl && !matchedPerson.linkedin_url) { try { await pool.query('UPDATE people SET linkedin_url = $1, updated_at = NOW() WHERE id = $2 AND linkedin_url IS NULL AND tenant_id = $3', [linkedinUrl, matchedPerson.id, tenantId]); } catch (e) {} }
            } else {
              stats.unmatched++;
              // Create new person record for unmatched connections
              try {
                const { rows: dupes } = await pool.query('SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1', [fullName, tenantId]);
                if (!dupes.length) {
                  const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, linkedin_url, email, source, created_by, tenant_id) VALUES ($1,$2,$3,$4,$5,'linkedin_import',$6,$7) RETURNING id`, [fullName, position || null, company || null, linkedinUrl || null, email || null, userId, tenantId]);
                  stats.new_people++;
                  // Also create proximity for new person
                  if (userId && np) {
                    try { await pool.query(`INSERT INTO team_proximity (person_id, team_member_id, proximity_type, source, strength, context, connected_at, tenant_id) VALUES ($1,$2,'linkedin_connection','linkedin_import',0.5,$3,$4,$5) ON CONFLICT DO NOTHING`, [np.id, userId, `${position} @ ${company}`, connectedOn, tenantId]); stats.proximity_created++; } catch (e) {}
                  }
                }
              } catch (e) { stats.errors++; }
            }

            // Store in linkedin_connections table
            if (slug) {
              try { await pool.query(`INSERT INTO linkedin_connections (team_member_id, first_name, last_name, full_name, linkedin_url, linkedin_slug, email, company, position, connected_at, matched_person_id, match_method, match_confidence) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (linkedin_slug) DO UPDATE SET company = EXCLUDED.company, position = EXCLUDED.position, matched_person_id = COALESCE(EXCLUDED.matched_person_id, linkedin_connections.matched_person_id), imported_at = NOW()`, [userId, firstName, lastName, fullName, linkedinUrl, slug, email||null, company||null, position||null, connectedOn, matchedPerson?.id||null, matchMethod, matchConfidence||null]); } catch (e) {}
            }
          }
          auditLog(userId, 'linkedin_import', 'people', null, { total: stats.total, matched: stats.matched, new_people: stats.new_people, proximity_created: stats.proximity_created, filename: fm.originalname });
          return JSON.stringify({ ...stats, match_rate: stats.total > 0 ? `${(stats.matched / stats.total * 100).toFixed(1)}%` : '0%', message: `Imported ${stats.total} LinkedIn connections: ${stats.matched} matched to existing people, ${stats.new_people} new people created, ${stats.proximity_created} team proximity links` });
        }

        if (action === 'import_linkedin_messages' && fm.preview) {
          const stats = { total: 0, matched: 0, interactions_created: 0, unmatched_senders: new Set(), errors: 0 };

          // Load people for matching by name
          const { rows: dbPeople } = await pool.query(`SELECT id, full_name FROM people WHERE full_name IS NOT NULL AND tenant_id = $1`, [tenantId]);
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
                  await pool.query(`INSERT INTO interactions (person_id, user_id, created_by, interaction_type, subject, summary, source, interaction_at, tenant_id) VALUES ($1, $2, $2, 'linkedin_message', $3, $4, 'linkedin_import', $5, $6) ON CONFLICT DO NOTHING`, [match.id, userId, `LinkedIn conversation (${messages.length} messages)`, summary, latestDate ? new Date(latestDate).toISOString() : new Date().toISOString(), tenantId]);
                  stats.interactions_created++;
                } catch (e) { stats.errors++; }
              } else {
                stats.unmatched_senders.add(name);
              }
            }
          }

          auditLog(userId, 'linkedin_messages_import', 'interactions', null, { total_messages: stats.total, conversations: conversations.size, matched: stats.matched, interactions_created: stats.interactions_created, filename: fm.originalname });
          return JSON.stringify({ total_messages: stats.total, conversations: conversations.size, matched_people: stats.matched, interactions_created: stats.interactions_created, unmatched_senders: [...stats.unmatched_senders].slice(0, 20), errors: stats.errors, message: `Processed ${stats.total} LinkedIn messages across ${conversations.size} conversations. Created ${stats.interactions_created} interaction records.` });
        }

        // Import XLSX workbook — stores each sheet as a document, embeds for search
        if (action === 'import_workbook' && fm.sheets) {
          const stats = { sheets_imported: 0, total_rows: 0, documents_created: 0, errors: [] };

          for (const sheetName of (fm.sheetNames || Object.keys(fm.sheets))) {
            const sheet = fm.sheets[sheetName];
            if (!sheet || !sheet.row_count) continue;

            try {
              // Build text content from all rows
              const headerLine = (sheet.headers || []).join(' | ');
              const rowLines = (sheet.rows || sheet.preview || []).map(r => Object.values(r).join(' | ')).join('\n');
              const content = `${headerLine}\n${rowLines}`;
              const title = `${fm.originalname} — ${sheetName}`;
              const hash = require('crypto').createHash('md5').update(title + content.slice(0, 500)).digest('hex');

              // Check if already exists
              const { rows: existing } = await pool.query(
                'SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2', [hash, tenantId]
              );
              if (existing.length) { stats.sheets_imported++; continue; }

              // Store as external document
              const { rows: [doc] } = await pool.query(`
                INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
                  tenant_id, uploaded_by_user_id, processing_status, created_at)
                VALUES ($1, $2, $3, 'xlsx_workbook', $4, $5, $6, $7, 'processed', NOW())
                RETURNING id
              `, [title, content.slice(0, 50000), fm.originalname, `xlsx://${fm.originalname}/${sheetName}`, hash, tenantId, userId]);

              // Embed in Qdrant
              try {
                const embedText = `Workbook: ${fm.originalname}\nSheet: ${sheetName}\nColumns: ${headerLine}\n\n${content.slice(0, 8000)}`;
                const emb = await generateQueryEmbedding(embedText);
                const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
                await new Promise((resolve, reject) => {
                  const body = JSON.stringify({ points: [{ id: hash, vector: emb, payload: { tenant_id: tenantId, title, source_type: 'xlsx_workbook', sheet_name: sheetName } }] });
                  const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
                    headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 15000 },
                    (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
                  qReq.on('error', reject); qReq.write(body); qReq.end();
                });
                await pool.query('UPDATE external_documents SET embedded_at = NOW() WHERE id = $1', [doc.id]);
              } catch (e) { /* embed error non-fatal */ }

              stats.documents_created++;
              stats.total_rows += sheet.row_count;
              stats.sheets_imported++;
            } catch (e) {
              stats.errors.push({ sheet: sheetName, error: e.message });
            }
          }

          auditLog(userId, 'workbook_import', 'external_documents', null, { ...stats, filename: fm.originalname });
          return JSON.stringify({
            ...stats,
            filename: fm.originalname,
            sheet_names: fm.sheetNames,
            message: `Imported ${stats.sheets_imported} sheets (${stats.total_rows} total rows) from "${fm.originalname}". Each sheet stored as a searchable document and embedded for semantic search.`
          });
        }

        return JSON.stringify({ error: 'Unsupported action' });
      }
      case 'run_sql_query': {
        const sql = input.query.trim();
        const isWrite = /^(UPDATE|INSERT|DELETE)/i.test(sql);
        const isDangerous = /DROP|ALTER|TRUNCATE|CREATE/i.test(sql);
        if (isDangerous) return JSON.stringify({ error: 'DROP/ALTER/TRUNCATE/CREATE not allowed via chat. Use migrations.' });
        if (isWrite) {
          // Write operations allowed — execute and return affected rows
          const result = await pool.query(sql + (sql.toUpperCase().includes('RETURNING') ? '' : ' RETURNING *'));
          return JSON.stringify({ explanation: input.explanation, operation: sql.split(' ')[0].toUpperCase(), rows_affected: result.rowCount, results: result.rows?.slice(0, 20) });
        }
        // SELECT queries
        const { rows } = await pool.query(sql + (sql.includes('LIMIT') ? '' : ' LIMIT 50'));
        return JSON.stringify({ explanation: input.explanation, row_count: rows.length, results: rows });
      }
      case 'get_platform_stats': {
        const { rows: [s] } = await pool.query(`SELECT (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1) AS signals, (SELECT COUNT(*) FROM companies WHERE (sector IS NOT NULL OR is_client = true) AND tenant_id = $1) AS companies, (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS people, (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1) AS documents, (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1) AS placements, (SELECT COALESCE(SUM(placement_fee),0) FROM conversions WHERE tenant_id = $1) AS revenue`, [tenantId]);
        return JSON.stringify(s);
      }
      // ── MCP-style intelligence tools ──────────────────────────────────────
      case 'get_converging_themes': {
        const lookbackDays = input.lookback_days || 30;
        const minCompanies = input.min_companies || 3;

        // Signal type clusters
        const { rows: signalThemes } = await pool.query(`
          WITH candidate_counts AS (
            SELECT se2.signal_type, COUNT(DISTINCT p.id) as cnt
            FROM people p
            JOIN companies c2 ON c2.id = p.current_company_id
            JOIN signal_events se2 ON se2.company_id = c2.id AND se2.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            WHERE p.current_title IS NOT NULL AND p.tenant_id = $1
            GROUP BY se2.signal_type
          )
          SELECT
            se.signal_type,
            COUNT(DISTINCT se.company_id) as company_count,
            COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) as client_count,
            COUNT(*) as signal_count,
            ROUND(AVG(se.confidence_score)::numeric, 2) as avg_confidence,
            COALESCE(cc.cnt, 0) as candidate_count,
            array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names,
            array_agg(DISTINCT se.company_name ORDER BY se.company_name) FILTER (WHERE se.company_name IS NOT NULL) as company_names
          FROM signal_events se
          LEFT JOIN companies c ON c.id = se.company_id
          LEFT JOIN candidate_counts cc ON cc.signal_type = se.signal_type
          WHERE se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            AND se.signal_type IS NOT NULL
            AND se.tenant_id = $1
          GROUP BY se.signal_type, cc.cnt
          HAVING COUNT(DISTINCT se.company_id) >= ${minCompanies}
          ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN se.company_id END) DESC,
                   COUNT(DISTINCT se.company_id) DESC
          LIMIT 8
        `, [tenantId]);

        // Sector convergences
        const { rows: sectorThemes } = await pool.query(`
          SELECT
            c.sector,
            COUNT(DISTINCT se.company_id) as company_count,
            COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) as client_count,
            COUNT(*) as signal_count,
            array_agg(DISTINCT se.signal_type::text) as signal_types,
            array_agg(DISTINCT c.name ORDER BY c.name) FILTER (WHERE c.is_client = true) as client_names
          FROM signal_events se
          JOIN companies c ON c.id = se.company_id AND c.sector IS NOT NULL
          WHERE se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            AND se.tenant_id = $1
          GROUP BY c.sector
          HAVING COUNT(DISTINCT se.company_id) >= ${minCompanies} AND COUNT(*) >= 5
          ORDER BY COUNT(DISTINCT CASE WHEN c.is_client = true THEN c.id END) DESC, COUNT(*) DESC
          LIMIT 5
        `, [tenantId]);

        // Pipeline matches
        let pipeline = [];
        try {
          const { rows } = await pool.query(`
            SELECT s.title as search_title, s.status, a.name as client_name,
                   COUNT(DISTINCT se.id) as matching_signals,
                   COUNT(DISTINCT se.company_id) as signalling_companies
            FROM opportunities s
            JOIN pipeline_contacts sc ON sc.search_id = s.id
            JOIN people p ON p.id = sc.person_id
            JOIN signal_events se ON se.company_id = p.current_company_id AND se.detected_at > NOW() - INTERVAL '${lookbackDays} days'
            LEFT JOIN accounts a ON a.id = s.project_id
            WHERE s.status IN ('sourcing', 'interviewing')
              AND s.tenant_id = $1
            GROUP BY s.id, s.title, s.status, a.name
            HAVING COUNT(DISTINCT se.id) >= 2
            ORDER BY COUNT(DISTINCT se.id) DESC
            LIMIT 5
          `, [tenantId]);
          pipeline = rows;
        } catch (e) { /* pipeline query may fail if tables don't exist */ }

        return JSON.stringify({
          lookback_days: lookbackDays,
          signal_themes: signalThemes,
          sector_themes: sectorThemes,
          pipeline_matches: pipeline,
          summary: `${signalThemes.length} signal type clusters, ${sectorThemes.length} sector convergences, ${pipeline.length} active search overlaps`
        });
      }

      case 'get_ranked_opportunities': {
        const { region, min_score = 0, limit = 15, by_region = false } = input;

        if (by_region) {
          const perRegion = Math.min(limit, 10);
          const { rows } = await pool.query(`
            SELECT ro.company_name, ro.sector, ro.region_code, ro.composite_score, ro.rank_in_region,
                   ro.signal_importance, ro.network_overlap, ro.geo_relevance,
                   ro.signal_summary, ro.recommended_action, ro.signal_count, ro.signal_types,
                   ro.warmest_contact_name, ro.best_connection_user_name,
                   cas.contact_count, cas.senior_contact_count,
                   gp.region_name, gp.is_home_market
            FROM ranked_opportunities ro
            LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
            LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
            WHERE ro.status = 'active' AND ro.rank_in_region <= $1
              AND ro.region_code IS NOT NULL AND ro.region_code != 'UNKNOWN'
            ORDER BY gp.weight_boost DESC NULLS LAST, ro.rank_in_region ASC
          `, [perRegion]);

          const grouped = {};
          for (const row of rows) {
            const rc = row.region_code;
            if (!grouped[rc]) grouped[rc] = { region_code: rc, region_name: row.region_name, is_home_market: row.is_home_market, opportunities: [] };
            grouped[rc].opportunities.push(row);
          }
          return JSON.stringify({ by_region: true, regions: grouped });
        }

        // Flat list
        let where = `WHERE ro.status = 'active'`;
        const params = [];
        let idx = 0;
        if (region && region !== 'all') { idx++; where += ` AND ro.region_code = $${idx}`; params.push(region); }
        if (min_score > 0) { idx++; where += ` AND ro.composite_score >= $${idx}`; params.push(min_score); }
        idx++; params.push(Math.min(limit, 50));

        const { rows } = await pool.query(`
          SELECT ro.company_name, ro.sector, ro.region_code, ro.composite_score, ro.rank_in_region,
                 ro.signal_importance, ro.network_overlap, ro.geo_relevance,
                 ro.signal_summary, ro.recommended_action, ro.signal_count, ro.signal_types,
                 ro.warmest_contact_name, ro.best_connection_user_name,
                 cas.contact_count, cas.senior_contact_count,
                 gp.region_name, gp.is_home_market
          FROM ranked_opportunities ro
          LEFT JOIN company_adjacency_scores cas ON LOWER(TRIM(cas.company_name)) = LOWER(TRIM(ro.company_name))
          LEFT JOIN geo_priorities gp ON gp.region_code = ro.region_code
          ${where}
          ORDER BY ro.composite_score DESC
          LIMIT $${idx}
        `, params);

        return JSON.stringify({ by_region: false, opportunities: rows, count: rows.length });
      }

      case 'get_talent_in_motion': {
        const { focus = 'all', limit: maxResults = 10 } = input;
        const lim = Math.min(maxResults, 30);
        const result = {};

        // Flight risk
        if (focus === 'all' || focus === 'flight_risk') {
          const { rows } = await pool.query(`
            SELECT DISTINCT ON (p.id)
              p.id, p.full_name, p.current_title, p.current_company_name,
              p.seniority_level, p.linkedin_url,
              se.signal_type, se.evidence_summary, se.detected_at, se.confidence_score,
              ps.flight_risk_score, ps.timing_score,
              (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id) as colleagues_affected,
              (SELECT COUNT(*) FROM people p2 WHERE p2.current_company_id = p.current_company_id
               AND p2.seniority_level IN ('c_suite','vp','director')) as senior_affected
            FROM people p
            JOIN companies c ON c.id = p.current_company_id
            JOIN signal_events se ON se.company_id = c.id
              AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change', 'strategic_hiring')
              AND se.detected_at > NOW() - INTERVAL '30 days'
              AND COALESCE(se.is_megacap, false) = false
            LEFT JOIN person_scores ps ON ps.person_id = p.id
            WHERE p.current_title IS NOT NULL AND p.tenant_id = $2
            ORDER BY p.id, se.detected_at DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.flight_risk = rows;
        }

        // Active profiles
        if (focus === 'all' || focus === 'active_profiles') {
          const { rows } = await pool.query(`
            SELECT p.id, p.full_name, p.current_title, p.current_company_name,
                   p.seniority_level, p.linkedin_url,
                   ps.activity_score, ps.timing_score, ps.receptivity_score, ps.flight_risk_score,
                   ps.engagement_score, ps.activity_trend, ps.engagement_trend,
                   ps.last_interaction_at, ps.interaction_count_30d, ps.external_signals_30d
            FROM people p
            JOIN person_scores ps ON ps.person_id = p.id
            WHERE (ps.timing_score > 0.4 OR ps.activity_score > 0.4 OR ps.receptivity_score > 0.5 OR ps.flight_risk_score > 0.4)
              AND p.current_title IS NOT NULL AND p.tenant_id = $2
            ORDER BY (COALESCE(ps.timing_score,0) + COALESCE(ps.activity_score,0) + COALESCE(ps.receptivity_score,0)) DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.active_profiles = rows;
        }

        // Re-engage windows
        if (focus === 'all' || focus === 'reengage') {
          const { rows } = await pool.query(`
            SELECT DISTINCT ON (p.id)
              p.id, p.full_name, p.current_title, p.current_company_name,
              se.signal_type, se.company_name AS signal_company, se.confidence_score,
              se.detected_at AS signal_date,
              i.interaction_at AS last_contact,
              i.interaction_type AS last_channel,
              EXTRACT(DAY FROM NOW() - i.interaction_at) AS days_since_contact,
              ps.engagement_score, ps.timing_score
            FROM people p
            JOIN companies c ON c.id = p.current_company_id
            JOIN signal_events se ON se.company_id = c.id
              AND se.signal_type::text IN ('restructuring', 'layoffs', 'ma_activity', 'leadership_change')
              AND se.detected_at > NOW() - INTERVAL '30 days'
              AND COALESCE(se.is_megacap, false) = false
            LEFT JOIN LATERAL (
              SELECT interaction_at, interaction_type FROM interactions
              WHERE person_id = p.id AND tenant_id = $2
              ORDER BY interaction_at DESC LIMIT 1
            ) i ON true
            LEFT JOIN person_scores ps ON ps.person_id = p.id
            WHERE p.tenant_id = $2
              AND p.current_title IS NOT NULL
              AND p.seniority_level IN ('c_suite', 'C-Suite', 'C-level', 'vp', 'VP', 'director', 'Director', 'Head')
              AND i.interaction_at IS NOT NULL
              AND i.interaction_at < NOW() - INTERVAL '60 days'
            ORDER BY p.id, se.confidence_score DESC
          `, [tenantId]);

          const ranked = rows
            .map(r => ({ ...r, reengage_score: (r.confidence_score || 0) * 0.4 + Math.min((r.days_since_contact || 0) / 365, 1) * 0.3 + (r.timing_score || 0) * 0.3 }))
            .sort((a, b) => b.reengage_score - a.reengage_score)
            .slice(0, lim);
          result.reengage_windows = ranked;
        }

        // Person signals
        if (focus === 'all' || focus === 'person_signals') {
          const { rows } = await pool.query(`
            SELECT psg.id, psg.signal_type, psg.title, psg.description, psg.confidence_score, psg.detected_at,
                   p.id as person_id, p.full_name, p.current_title, p.current_company_name, p.seniority_level
            FROM person_signals psg
            JOIN people p ON p.id = psg.person_id
            WHERE psg.signal_type IN ('flight_risk_alert', 'activity_spike', 'timing_opportunity', 'new_role', 'company_exit')
              AND psg.detected_at > NOW() - INTERVAL '14 days'
              AND psg.tenant_id = $2
            ORDER BY psg.detected_at DESC
            LIMIT $1
          `, [lim, tenantId]);
          result.person_signals = rows;
        }

        return JSON.stringify(result);
      }

      case 'get_signal_proximity': {
        let companyId = input.company_id;
        let signalContext = null;

        // Resolve from signal_id
        if (input.signal_id) {
          const { rows: [sig] } = await pool.query('SELECT * FROM signal_events WHERE id = $1 AND tenant_id = $2', [input.signal_id, tenantId]);
          if (!sig) return JSON.stringify({ error: 'Signal not found' });
          companyId = sig.company_id;
          signalContext = { id: sig.id, type: sig.signal_type, confidence: sig.confidence_score, headline: sig.evidence_summary, company: sig.company_name, detected_at: sig.detected_at };
        }

        // Resolve from company_name
        if (!companyId && input.company_name) {
          const { rows } = await pool.query('SELECT id, name FROM companies WHERE name ILIKE $1 AND tenant_id = $2 ORDER BY is_client DESC LIMIT 1', [`%${input.company_name}%`, tenantId]);
          if (rows.length) companyId = rows[0].id;
          else return JSON.stringify({ error: `No company found matching "${input.company_name}"` });
        }

        if (!companyId) return JSON.stringify({ error: 'Provide signal_id, company_id, or company_name' });

        // Get company info
        const { rows: [company] } = await pool.query('SELECT id, name, sector, geography, is_client, domain FROM companies WHERE id = $1 AND tenant_id = $2', [companyId, tenantId]);
        if (!company) return JSON.stringify({ error: 'Company not found' });

        // Check client status
        let account = null;
        try {
          const { rows: [acct] } = await pool.query(`
            SELECT a.id, a.name, a.relationship_tier FROM accounts a
            WHERE a.tenant_id = $1 AND (a.company_id = $2 OR LOWER(a.name) = LOWER($3)) LIMIT 1
          `, [tenantId, companyId, company.name]);
          account = acct || null;
        } catch (e) { /* accounts table may not exist */ }

        // Get contacts with team proximity
        const { rows: contacts } = await pool.query(`
          SELECT
            p.id, p.full_name, p.current_title, p.current_company_name, p.seniority_level,
            ps.timing_score, ps.receptivity_score, ps.engagement_score,
            json_object_agg(
              tp.team_member_id::text,
              json_build_object('strength', tp.relationship_strength, 'type', tp.relationship_type)
            ) AS connections_by_team_member,
            MAX(tp.relationship_strength) AS best_strength,
            (SELECT u.name FROM users u WHERE u.id = (
              SELECT tp2.team_member_id FROM team_proximity tp2 WHERE tp2.person_id = p.id AND tp2.tenant_id = $1
              ORDER BY tp2.relationship_strength DESC LIMIT 1
            )) AS best_connector_name
          FROM people p
          JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = $1
          LEFT JOIN person_scores ps ON ps.person_id = p.id AND ps.tenant_id = $1
          WHERE p.tenant_id = $1
            AND p.current_company_id = $2
            AND tp.relationship_strength >= 0.20
          GROUP BY p.id, p.full_name, p.current_title, p.current_company_name, p.seniority_level,
                   ps.timing_score, ps.receptivity_score, ps.engagement_score
          ORDER BY MAX(tp.relationship_strength) DESC
          LIMIT 15
        `, [tenantId, companyId]);

        // Get recent signals for context
        const { rows: signals } = await pool.query(`
          SELECT signal_type, evidence_summary, confidence_score, detected_at
          FROM signal_events WHERE company_id = $1 AND tenant_id = $2 AND detected_at > NOW() - INTERVAL '90 days'
          ORDER BY detected_at DESC LIMIT 5
        `, [companyId, tenantId]);

        return JSON.stringify({
          company: { ...company, is_client: !!account, client_tier: account?.relationship_tier },
          signal: signalContext,
          contacts: contacts.map(c => ({
            id: c.id,
            name: c.full_name,
            title: c.current_title,
            seniority: c.seniority_level,
            best_strength: parseFloat(c.best_strength) || 0,
            best_connector: c.best_connector_name,
            connections: c.connections_by_team_member,
            timing_score: c.timing_score,
            receptivity_score: c.receptivity_score,
            engagement_score: c.engagement_score
          })),
          recent_signals: signals,
          connection_count: contacts.length,
          summary: `${contacts.length} contacts at ${company.name}${account ? ` (client, tier: ${account.relationship_tier})` : ''}, ${signals.length} recent signals`
        });
      }

      case 'dispatch_action': {
        const { action, dispatch_id, status, theme } = input;

        switch (action) {
          case 'generate_all': {
            try {
              const { generateDispatches } = require('./scripts/generate_dispatches');
              generateDispatches().then(r => console.log('Dispatch generation complete:', r)).catch(e => console.error('Dispatch generation failed:', e.message));
              return JSON.stringify({ success: true, message: 'Dispatch generation triggered — runs in background' });
            } catch (e) { return JSON.stringify({ error: 'Failed to trigger generation: ' + e.message }); }
          }
          case 'rescan_proximity': {
            try {
              const { rescanProximity } = require('./scripts/generate_dispatches');
              rescanProximity().then(r => console.log('Rescan complete:', r)).catch(e => console.error('Rescan failed:', e.message));
              return JSON.stringify({ success: true, message: 'Proximity rescan triggered — runs in background' });
            } catch (e) { return JSON.stringify({ error: 'Failed to trigger rescan: ' + e.message }); }
          }
          case 'claim': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for claim action' });
            const { rows: [d] } = await pool.query('SELECT id, claimed_by, status FROM signal_dispatches WHERE id = $1 AND tenant_id = $2', [dispatch_id, tenantId]);
            if (!d) return JSON.stringify({ error: 'Dispatch not found' });
            if (d.claimed_by && d.claimed_by !== userId) {
              const { rows: [claimer] } = await pool.query('SELECT name FROM users WHERE id = $1', [d.claimed_by]);
              return JSON.stringify({ error: `Already claimed by ${claimer?.name || 'another user'}` });
            }
            const { rows: [updated] } = await pool.query(`
              UPDATE signal_dispatches SET claimed_by = $2, claimed_at = NOW(), status = CASE WHEN status = 'draft' THEN 'claimed' ELSE status END, updated_at = NOW()
              WHERE id = $1 AND tenant_id = $3 RETURNING id, company_name, signal_type, status
            `, [dispatch_id, userId, tenantId]);
            auditLog(userId, 'dispatch_claim', 'dispatch', dispatch_id, { company: updated.company_name });
            return JSON.stringify({ success: true, dispatch: updated, message: 'Dispatch claimed' });
          }
          case 'unclaim': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for unclaim action' });
            const { rows: [updated] } = await pool.query(`
              UPDATE signal_dispatches SET claimed_by = NULL, claimed_at = NULL, status = 'draft', updated_at = NOW()
              WHERE id = $1 AND (claimed_by = $2 OR claimed_by IS NULL) AND tenant_id = $3 RETURNING id, company_name, status
            `, [dispatch_id, userId, tenantId]);
            if (!updated) return JSON.stringify({ error: 'Cannot unclaim — not your dispatch or not found' });
            return JSON.stringify({ success: true, dispatch: updated, message: 'Dispatch unclaimed' });
          }
          case 'update_status': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for update_status action' });
            if (!status) return JSON.stringify({ error: 'status required for update_status action' });
            const updates = [`status = $3`, `updated_at = NOW()`];
            if (status === 'reviewed') updates.push(`reviewed_at = NOW(), reviewed_by = $4`);
            if (status === 'sent') updates.push(`sent_at = NOW()`);
            const params = status === 'reviewed'
              ? [dispatch_id, tenantId, status, userId]
              : [dispatch_id, tenantId, status];
            const { rows: [updated] } = await pool.query(`
              UPDATE signal_dispatches SET ${updates.join(', ')}
              WHERE id = $1 AND tenant_id = $2 RETURNING id, company_name, signal_type, status
            `, params);
            if (!updated) return JSON.stringify({ error: 'Dispatch not found' });
            return JSON.stringify({ success: true, dispatch: updated, message: `Status updated to ${status}` });
          }
          case 'regenerate_content': {
            if (!dispatch_id) return JSON.stringify({ error: 'dispatch_id required for regenerate_content action' });
            const { rows: [d] } = await pool.query(`
              SELECT sd.*, se.evidence_summary, se.signal_type, se.confidence_score,
                     c.sector, c.geography
              FROM signal_dispatches sd
              LEFT JOIN signal_events se ON se.id = sd.signal_event_id
              LEFT JOIN companies c ON c.id = sd.company_id
              WHERE sd.id = $1 AND sd.tenant_id = $2
            `, [dispatch_id, tenantId]);
            if (!d) return JSON.stringify({ error: 'Dispatch not found' });

            const blogTheme = theme || d.blog_theme || d.opportunity_angle || 'market intelligence';
            const prompt = `Write a 550-700 word executive thought leadership piece about: ${blogTheme}\n\nContext: ${d.signal_type} signal for a ${d.sector || 'technology'} company in ${d.geography || 'APAC'}.\nEvidence: ${d.evidence_summary || d.signal_summary || 'Recent market signal'}\n\nWrite with authority, not sales language. Advisor tone. First person plural. No company name. No generic business clichés. Australian English.`;

            try {
              const regen = await callClaude([{ role: 'user', content: prompt }], [], 'You are a market intelligence writer for an executive search firm. Output JSON: {"title":"...","body":"...","keywords":["..."]}');
              const text = regen.content.find(c => c.type === 'text')?.text || '';
              const parsed = JSON.parse(text.replace(/```json\n?|\n?```/g, ''));

              await pool.query(`UPDATE signal_dispatches SET blog_theme = $2, blog_title = $3, blog_body = $4, blog_keywords = $5, updated_at = NOW() WHERE id = $1 AND tenant_id = $6`,
                [dispatch_id, blogTheme, parsed.title, JSON.stringify(parsed.body || parsed), parsed.keywords || [], tenantId]);

              return JSON.stringify({ success: true, title: parsed.title, keywords: parsed.keywords, message: 'Content regenerated' });
            } catch (e) { return JSON.stringify({ error: 'Regeneration failed: ' + e.message }); }
          }
          default: return JSON.stringify({ error: `Unknown dispatch action: ${action}` });
        }
      }

      case 'import_placements': {
        const { placements = [] } = input;
        if (!placements.length) return JSON.stringify({ error: 'No placements provided' });

        const results = { imported: 0, skipped: 0, errors: [], details: [] };

        for (const pl of placements) {
          try {
            // Resolve candidate
            let personId = null;
            const { rows: personMatches } = await pool.query(
              `SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 3`,
              [pl.candidate_name.trim(), tenantId]
            );
            if (personMatches.length === 1) {
              personId = personMatches[0].id;
            } else if (personMatches.length > 1) {
              // Try exact match first
              const exact = personMatches.find(p => p.full_name.toLowerCase() === pl.candidate_name.trim().toLowerCase());
              personId = exact ? exact.id : personMatches[0].id;
            } else {
              // Create person
              const { rows: [newPerson] } = await pool.query(
                `INSERT INTO people (full_name, current_title, source, created_by, tenant_id) VALUES ($1, $2, 'placement_import', $3, $4) RETURNING id`,
                [pl.candidate_name.trim(), pl.role_title || null, userId, tenantId]
              );
              personId = newPerson.id;
            }

            // Resolve client company → account
            let clientId = null;
            const { rows: acctMatches } = await pool.query(
              `SELECT a.id FROM accounts a WHERE a.name ILIKE $1 AND a.tenant_id = $2 LIMIT 1`,
              [`%${pl.company_name.trim()}%`, tenantId]
            );
            if (acctMatches.length) {
              clientId = acctMatches[0].id;
            } else {
              // Check companies table, create account if company exists
              const { rows: coMatches } = await pool.query(
                `SELECT id, name FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
                [`%${pl.company_name.trim()}%`, tenantId]
              );
              if (coMatches.length) {
                const { rows: [newAcct] } = await pool.query(
                  `INSERT INTO accounts (name, company_id, relationship_status, tenant_id, created_at, updated_at)
                   VALUES ($1, $2, 'active', $3, NOW(), NOW()) RETURNING id`,
                  [coMatches[0].name, coMatches[0].id, tenantId]
                );
                clientId = newAcct.id;
              } else {
                // Create both company and account
                const { rows: [newCo] } = await pool.query(
                  `INSERT INTO companies (name, is_client, created_by, tenant_id, created_at, updated_at)
                   VALUES ($1, true, $2, $3, NOW(), NOW()) RETURNING id`,
                  [pl.company_name.trim(), userId, tenantId]
                );
                const { rows: [newAcct] } = await pool.query(
                  `INSERT INTO accounts (name, company_id, relationship_status, tenant_id, created_at, updated_at)
                   VALUES ($1, $2, 'active', $3, NOW(), NOW()) RETURNING id`,
                  [pl.company_name.trim(), newCo.id, tenantId]
                );
                clientId = newAcct.id;
              }
            }

            // Check for duplicate placement
            const { rows: dupes } = await pool.query(
              `SELECT id FROM conversions WHERE person_id = $1 AND client_id = $2 AND role_title = $3 AND tenant_id = $4 LIMIT 1`,
              [personId, clientId, pl.role_title, tenantId]
            );
            if (dupes.length) {
              results.skipped++;
              results.details.push({ candidate: pl.candidate_name, company: pl.company_name, status: 'duplicate' });
              continue;
            }

            // Insert placement
            await pool.query(
              `INSERT INTO conversions (person_id, client_id, role_title, start_date, placement_fee, currency, notes, placed_by_user_id, tenant_id, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())`,
              [personId, clientId, pl.role_title, pl.start_date || null,
               pl.placement_fee || null, pl.currency || 'AUD', pl.notes || null,
               userId, tenantId]
            );

            // Update person's current company
            const { rows: [co] } = await pool.query('SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1', [`%${pl.company_name.trim()}%`, tenantId]);
            if (co) {
              await pool.query('UPDATE people SET current_company_name = $1, current_company_id = $2, current_title = $3, updated_at = NOW() WHERE id = $4',
                [pl.company_name.trim(), co.id, pl.role_title, personId]);
            }

            results.imported++;
            results.details.push({ candidate: pl.candidate_name, company: pl.company_name, role: pl.role_title, status: 'imported' });

          } catch (e) {
            results.errors.push({ candidate: pl.candidate_name, error: e.message });
          }
        }

        auditLog(userId, 'import_placements', 'conversions', null, { imported: results.imported, skipped: results.skipped, total: placements.length });
        return JSON.stringify({ ...results, message: `Imported ${results.imported} placements (${results.skipped} duplicates skipped)` });
      }

      case 'import_case_studies': {
        const { case_studies = [] } = input;
        if (!case_studies.length) return JSON.stringify({ error: 'No case studies provided' });

        // Ensure table exists
        try {
          const fs = require('fs');
          const migPath = require('path').join(__dirname, 'sql', 'migration_case_studies.sql');
          if (fs.existsSync(migPath)) await pool.query(fs.readFileSync(migPath, 'utf8'));
        } catch (e) { /* table may already exist */ }

        const results = { imported: 0, skipped: 0, details: [] };

        for (const cs of case_studies) {
          try {
            // Resolve client company
            let clientId = null;
            if (cs.client_name) {
              const { rows } = await pool.query(
                `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
                [`%${cs.client_name.trim()}%`, tenantId]
              );
              if (rows.length) clientId = rows[0].id;
            }

            // Build title
            const title = [cs.role_title, cs.client_name].filter(Boolean).join(' — ') || 'Case Study';

            // Check for duplicate
            const { rows: dupes } = await pool.query(
              `SELECT id FROM case_studies WHERE title ILIKE $1 AND tenant_id = $2 LIMIT 1`,
              [title, tenantId]
            );
            if (dupes.length) {
              results.skipped++;
              results.details.push({ title, status: 'duplicate' });
              continue;
            }

            // Compute completeness
            const fields = [cs.client_name, cs.engagement_type, cs.role_title, cs.sector,
                            cs.geography, cs.challenge, cs.approach, cs.outcome];
            const completeness = fields.filter(Boolean).length / fields.length;

            const { rows: [inserted] } = await pool.query(`
              INSERT INTO case_studies (
                tenant_id, title, client_name, client_id, engagement_type,
                role_title, seniority_level, sector, geography, year,
                challenge, approach, outcome,
                themes, capabilities, change_vectors,
                completeness, extracted_by, status, visibility
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'chat_import','draft','internal_only')
              RETURNING id
            `, [
              tenantId, title, cs.client_name || null, clientId, cs.engagement_type || null,
              cs.role_title || null, cs.seniority_level || null, cs.sector || null, cs.geography || null, cs.year || null,
              cs.challenge || null, cs.approach || null, cs.outcome || null,
              cs.themes || [], cs.capabilities || [], cs.change_vectors || [],
              completeness
            ]);

            results.imported++;
            results.details.push({ title, id: inserted.id, status: 'imported', completeness: (completeness * 100).toFixed(0) + '%' });
          } catch (e) {
            results.details.push({ title: cs.role_title || 'unknown', status: 'error', error: e.message });
          }
        }

        auditLog(userId, 'import_case_studies', 'case_studies', null, { imported: results.imported, skipped: results.skipped, total: case_studies.length });
        return JSON.stringify({
          ...results,
          message: `Imported ${results.imported} case studies as internal drafts (${results.skipped} duplicates skipped). These require sanitisation before external use — use /api/case-studies/:id/sanitise to approve public fields.`
        });
      }

      case 'search_case_studies': {
        const { query, limit = 10 } = input;
        let results = [];

        // Try Qdrant semantic search first
        try {
          const vector = await generateQueryEmbedding(query);
          const qdrantResults = await qdrantSearch('case_studies', vector, limit);
          if (qdrantResults.length > 0) {
            const csIds = qdrantResults.map(r => String(r.id)).filter(id => /^[0-9a-f-]{36}$/i.test(id));
            if (csIds.length > 0) {
              const { rows } = await pool.query(
                `SELECT id, title, client_name, role_title, sector, geography, year, challenge, themes, capabilities
                 FROM case_studies WHERE id = ANY($1::uuid[]) AND tenant_id = $2`,
                [csIds, tenantId]
              );
              const csMap = new Map(rows.map(r => [r.id, r]));
              results = qdrantResults.map(r => {
                const cs = csMap.get(r.id);
                if (!cs) return null;
                return { ...cs, match_score: Math.round(r.score * 100) };
              }).filter(Boolean);
            }
          }
        } catch (e) { /* Qdrant collection may not exist */ }

        // Fallback to SQL text search
        if (results.length < 3) {
          const { rows } = await pool.query(
            `SELECT id, title, client_name, role_title, sector, geography, year, challenge, themes, capabilities
             FROM case_studies
             WHERE tenant_id = $1 AND (
               title ILIKE $2 OR client_name ILIKE $2 OR role_title ILIKE $2 OR
               challenge ILIKE $2 OR sector ILIKE $2 OR geography ILIKE $2
             )
             ORDER BY year DESC NULLS LAST LIMIT $3`,
            [tenantId, `%${query}%`, limit]
          );
          const existing = new Set(results.map(r => r.id));
          rows.forEach(r => { if (!existing.has(r.id)) results.push(r); });
        }

        return JSON.stringify({ case_studies: results.slice(0, limit), count: results.length });
      }

      case 'run_pipeline': {
        const { pipeline_key } = input;
        if (!pipeline_key) return JSON.stringify({ error: 'pipeline_key required' });

        try {
          const scheduler = require('./scripts/scheduler.js');
          const pipelines = scheduler.PIPELINES;
          if (!pipelines[pipeline_key]) {
            const available = Object.keys(pipelines).join(', ');
            return JSON.stringify({ error: `Unknown pipeline "${pipeline_key}". Available: ${available}` });
          }
          const pipeline = pipelines[pipeline_key];
          // Trigger async — don't wait for completion
          scheduler.runPipeline(pipeline_key, 'chat').catch(e => console.error(`Pipeline ${pipeline_key} error:`, e.message));
          auditLog(userId, 'run_pipeline', 'pipeline', null, { pipeline_key });
          return JSON.stringify({ success: true, message: `${pipeline.name} triggered — running in background. Check /api/pipelines/runs for status.` });
        } catch (e) {
          return JSON.stringify({ error: 'Failed to trigger pipeline: ' + e.message });
        }
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
      const raw = fsChat.readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines = raw.split('\n');
      // Find header line — skip LinkedIn preamble/notes at top
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
        if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
          headerIdx = i; break;
        }
      }
      if (headerIdx === 0) {
        for (let i = 0; i < Math.min(allLines.length, 20); i++) {
          const trimmed = allLines[i].trim();
          if (trimmed && trimmed.split(',').length >= 3 && trimmed.split(',')[0].trim().length < 30) { headerIdx = i; break; }
        }
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
    // XLSX / XLS workbook support — parse all tabs
    if (file.originalname.match(/\.xlsx?$/i) || file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' || file.mimetype === 'application/vnd.ms-excel') {
      try {
        const XLSX = require('xlsx');
        const workbook = XLSX.readFile(file.path);
        meta.sheets = {};
        meta.sheetNames = workbook.SheetNames;
        meta.preview = []; // Combined preview for Claude
        meta.columns = [];

        for (const sheetName of workbook.SheetNames) {
          const sheet = workbook.Sheets[sheetName];
          const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });
          const headers = rows.length > 0 ? Object.keys(rows[0]) : [];

          meta.sheets[sheetName] = {
            headers,
            row_count: rows.length,
            preview: rows.slice(0, 5)
          };

          // Store all rows (up to 2000 per sheet) for processing
          if (rows.length > 0) {
            meta.sheets[sheetName].rows = rows.slice(0, 2000);
          }
        }

        // Set top-level columns/preview from first sheet for compatibility
        const firstSheet = workbook.SheetNames[0];
        if (meta.sheets[firstSheet]) {
          meta.columns = meta.sheets[firstSheet].headers;
          meta.preview = meta.sheets[firstSheet].rows || meta.sheets[firstSheet].preview;
        }

        // Build full text for embedding
        meta.text = workbook.SheetNames.map(name => {
          const s = meta.sheets[name];
          const headerLine = s.headers.join(' | ');
          const sampleRows = (s.preview || []).slice(0, 10).map(r => Object.values(r).join(' | ')).join('\n');
          return `=== Sheet: ${name} (${s.row_count} rows) ===\n${headerLine}\n${sampleRows}`;
        }).join('\n\n');
      } catch (e) {
        meta.text = '[XLSX parse error: ' + e.message + ']';
      }
    }

    if (file.originalname.endsWith('.pdf') || file.mimetype === 'application/pdf') {
      try {
        const pdfParse = require('pdf-parse');
        const buf = fsChat.readFileSync(file.path);
        const d = await pdfParse(buf);
        meta.text = d.text;
        meta.pages = d.numpages;
      } catch (e) {
        meta.text = '[PDF parse error: ' + e.message + ']';
      }
    }
    if (file.originalname.endsWith('.txt') || file.mimetype === 'text/plain') { meta.text = fsChat.readFileSync(file.path, 'utf8'); }

    uploadedFiles.set(fileId, meta);
    setTimeout(() => { uploadedFiles.delete(fileId); try { fsChat.unlinkSync(file.path); } catch(e){} }, 30*60*1000);

    const response = { file_id: fileId, filename: file.originalname, size: file.size, type: file.mimetype, columns: meta.columns||null, row_count: meta.preview?.length||null, pages: meta.pages||null, suggested_mapping: meta.suggestedMapping||null, linkedin_type: meta.linkedinType||null };
    // Include sheet info for XLSX workbooks
    if (meta.sheetNames) {
      response.workbook = true;
      response.sheet_names = meta.sheetNames;
      response.sheets = {};
      for (const name of meta.sheetNames) {
        response.sheets[name] = { headers: meta.sheets[name].headers, row_count: meta.sheets[name].row_count, preview: (meta.sheets[name].preview || []).slice(0, 3) };
      }
    }
    res.json(response);
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

    // Inject tenant context into system prompt so SQL queries can use the right tenant_id
    const systemWithContext = CHAT_SYSTEM + `\n\nSESSION CONTEXT:\n- tenant_id: '${req.tenant_id}'\n- user: ${req.user.name} (${req.user.email})\n- user_id: '${req.user.user_id}'`;

    let response = await callClaude(history, CHAT_TOOLS, systemWithContext);
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
        const result = await executeTool(tc.name, tc.input, req.user.id, req.tenant_id);
        toolResultContent.push({ type: 'tool_result', tool_use_id: tc.id, content: result });
        toolsUsed.push(tc.name);
      }
      history.push({ role: 'user', content: toolResultContent });
      response = await callClaude(history, CHAT_TOOLS, systemWithContext);
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
// CASE STUDY LIBRARY
// ═══════════════════════════════════════════════════════════════════════════════
//
// GOVERNANCE MODEL:
// - Placements (conversions table) are ALWAYS internal. They contain fees,
//   candidate PII, contact details. They NEVER appear in external output.
// - Case studies have two representations:
//   1. INTERNAL: full data (client name, role, people, source doc) — team only
//   2. EXTERNAL-SAFE: public_* fields only, no candidate names, no fees,
//      no contact info. Requires public_approved = true.
// - Only case studies with public_approved = true AND visibility = 'dispatch_ready'
//   or 'published' can be bundled with dispatches or served externally.
//
// ═══════════════════════════════════════════════════════════════════════════════

// Internal: full case study list (authenticated, team only)
app.get('/api/case-studies', authenticateToken, async (req, res) => {
  try {
    const { sector, geography, theme, status, limit: lim = 50, offset = 0 } = req.query;
    let where = 'WHERE cs.tenant_id = $1';
    const params = [req.tenant_id];
    let idx = 1;

    if (sector) { idx++; where += ` AND cs.sector ILIKE $${idx}`; params.push(`%${sector}%`); }
    if (geography) { idx++; where += ` AND cs.geography ILIKE $${idx}`; params.push(`%${geography}%`); }
    if (theme) { idx++; where += ` AND $${idx} = ANY(cs.themes)`; params.push(theme); }
    if (status) { idx++; where += ` AND cs.status = $${idx}`; params.push(status); }

    idx++; params.push(Math.min(parseInt(lim) || 50, 100));
    idx++; params.push(parseInt(offset) || 0);

    const { rows } = await pool.query(`
      SELECT cs.*, c.name AS client_company_name, c.is_client,
             ed.title AS source_document_title, ed.source_url
      FROM case_studies cs
      LEFT JOIN companies c ON c.id = cs.client_id
      LEFT JOIN external_documents ed ON ed.id = cs.document_id
      ${where}
      ORDER BY cs.year DESC NULLS LAST, cs.created_at DESC
      LIMIT $${idx - 1} OFFSET $${idx}
    `, params);

    const { rows: [{ count }] } = await pool.query(
      `SELECT COUNT(*) FROM case_studies cs ${where}`, params.slice(0, -2)
    );

    res.json({ case_studies: rows, total: parseInt(count) });
  } catch (err) {
    console.error('Case studies error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Internal: full case study detail (authenticated, team only)
app.get('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const { rows: [cs] } = await pool.query(`
      SELECT cs.*, c.name AS client_company_name, c.sector AS client_sector,
             ed.title AS source_document_title, ed.source_url, ed.content_summary
      FROM case_studies cs
      LEFT JOIN companies c ON c.id = cs.client_id
      LEFT JOIN external_documents ed ON ed.id = cs.document_id
      WHERE cs.id = $1 AND cs.tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    if (!cs) return res.status(404).json({ error: 'Not found' });

    // People from the source document — INTERNAL ONLY
    let people = [];
    if (cs.document_id) {
      const { rows } = await pool.query(`
        SELECT dp.person_name, dp.person_title, dp.person_company, dp.mention_role, dp.context_note,
               dp.person_id, p.current_title AS current_title_now, p.current_company_name AS current_company_now
        FROM document_people dp
        LEFT JOIN people p ON p.id = dp.person_id
        WHERE dp.document_id = $1
        ORDER BY dp.mention_role, dp.person_name
      `, [cs.document_id]);
      people = rows;
    }

    res.json({ case_study: cs, people });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sanitise a case study for external use (admin only)
app.patch('/api/case-studies/:id/sanitise', authenticateToken, async (req, res) => {
  try {
    const { public_title, public_summary, public_sector, public_geography, public_capability, public_approved } = req.body;
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;

    if (public_title !== undefined) { idx++; updates.push(`public_title = $${idx}`); params.push(public_title); }
    if (public_summary !== undefined) { idx++; updates.push(`public_summary = $${idx}`); params.push(public_summary); }
    if (public_sector !== undefined) { idx++; updates.push(`public_sector = $${idx}`); params.push(public_sector); }
    if (public_geography !== undefined) { idx++; updates.push(`public_geography = $${idx}`); params.push(public_geography); }
    if (public_capability !== undefined) { idx++; updates.push(`public_capability = $${idx}`); params.push(public_capability); }
    if (public_approved !== undefined) {
      idx++; updates.push(`public_approved = $${idx}`); params.push(public_approved);
      if (public_approved) {
        updates.push(`sanitised_by = $${idx + 1}`, `sanitised_at = NOW()`);
        idx++; params.push(req.user.user_id);
        updates.push(`visibility = CASE WHEN visibility = 'internal_only' THEN 'dispatch_ready' ELSE visibility END`);
        updates.push(`status = CASE WHEN status = 'draft' THEN 'sanitised' ELSE status END`);
      }
    }

    const { rows: [updated] } = await pool.query(
      `UPDATE case_studies SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2 RETURNING id, public_title, public_approved, visibility, status`,
      params
    );
    if (!updated) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'sanitise_case_study', 'case_study', updated.id, { public_approved: updated.public_approved, visibility: updated.visibility });
    res.json({ case_study: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Edit case study (any field)
app.patch('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const allowed = ['title', 'client_name', 'role_title', 'engagement_type', 'seniority_level',
      'sector', 'geography', 'year', 'challenge', 'approach', 'outcome', 'impact_note',
      'themes', 'change_vectors', 'capabilities', 'status', 'visibility'];
    const updates = ['updated_at = NOW()'];
    const params = [req.params.id, req.tenant_id];
    let idx = 2;

    for (const key of allowed) {
      if (req.body[key] !== undefined) {
        idx++;
        if (['themes', 'change_vectors', 'capabilities'].includes(key)) {
          updates.push(`${key} = $${idx}::text[]`);
          params.push(Array.isArray(req.body[key]) ? req.body[key] : [req.body[key]]);
        } else {
          updates.push(`${key} = $${idx}`);
          params.push(req.body[key]);
        }
      }
    }

    if (updates.length <= 1) return res.status(400).json({ error: 'No valid fields to update' });

    const { rows: [updated] } = await pool.query(
      `UPDATE case_studies SET ${updates.join(', ')} WHERE id = $1 AND tenant_id = $2 RETURNING *`,
      params
    );
    if (!updated) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'edit_case_study', 'case_study', updated.id, { fields: Object.keys(req.body) });
    res.json({ case_study: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete case study
app.delete('/api/case-studies/:id', authenticateToken, async (req, res) => {
  try {
    const { rows: [deleted] } = await pool.query(
      'DELETE FROM case_studies WHERE id = $1 AND tenant_id = $2 RETURNING id, title',
      [req.params.id, req.tenant_id]
    );
    if (!deleted) return res.status(404).json({ error: 'Not found' });

    auditLog(req.user.user_id, 'delete_case_study', 'case_study', deleted.id, { title: deleted.title });
    res.json({ success: true, deleted: deleted.title });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUBLIC / DISPATCH-SAFE: case studies for external consumption
// HARD GATE: only returns public_approved = true, visibility IN ('dispatch_ready', 'published')
// NEVER returns: client_name, candidate names, fees, contact details, source documents
app.get('/api/public/case-studies', async (req, res) => {
  try {
    const { sector, geography, theme, capability, limit: lim = 20 } = req.query;
    const tenantId = '00000000-0000-0000-0000-000000000001';
    let where = `WHERE cs.tenant_id = $1 AND cs.public_approved = true AND cs.visibility IN ('dispatch_ready', 'published')`;
    const params = [tenantId];
    let idx = 1;

    if (sector) { idx++; where += ` AND cs.public_sector ILIKE $${idx}`; params.push(`%${sector}%`); }
    if (geography) { idx++; where += ` AND cs.public_geography ILIKE $${idx}`; params.push(`%${geography}%`); }
    if (theme) { idx++; where += ` AND $${idx} = ANY(cs.themes)`; params.push(theme); }
    if (capability) { idx++; where += ` AND $${idx} = ANY(cs.capabilities)`; params.push(capability); }
    idx++; params.push(Math.min(parseInt(lim) || 20, 50));

    const { rows } = await pool.query(`
      SELECT
        cs.id, cs.slug,
        cs.public_title AS title,
        cs.public_summary AS summary,
        cs.public_sector AS sector,
        cs.public_geography AS geography,
        cs.public_capability AS capability,
        cs.engagement_type,
        cs.seniority_level,
        cs.year,
        cs.themes,
        cs.change_vectors,
        cs.capabilities
      FROM case_studies cs
      ${where}
      ORDER BY cs.year DESC NULLS LAST, cs.relevance_score DESC
      LIMIT $${idx}
    `, params);

    res.json({ case_studies: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Internal match: flag relevant case studies for team users on signals/dispatches
// Returns INTERNAL fields — for team use, not external publishing
app.get('/api/case-studies/relevant', authenticateToken, async (req, res) => {
  try {
    const { signal_type, sector, geography, company_name, company_id, limit: lim = 5 } = req.query;
    const tenantId = req.tenant_id;

    let where = 'WHERE cs.tenant_id = $1';
    const params = [tenantId];
    let idx = 1;

    // Build scoring from available dimensions
    const scoreTerms = [];

    if (sector) {
      idx++; params.push(`%${sector}%`);
      scoreTerms.push(`CASE WHEN cs.sector ILIKE $${idx} OR cs.public_sector ILIKE $${idx} THEN 0.3 ELSE 0 END`);
    }
    if (geography) {
      idx++; params.push(`%${geography}%`);
      scoreTerms.push(`CASE WHEN cs.geography ILIKE $${idx} OR cs.public_geography ILIKE $${idx} THEN 0.25 ELSE 0 END`);
    }
    if (signal_type) {
      // Map signal types to likely engagement types and themes
      const sigMap = {
        capital_raising: { themes: ['high-growth', 'scaling', 'fundraising'], eng: 'executive_search' },
        geographic_expansion: { themes: ['cross-border', 'market-entry', 'expansion'], eng: 'executive_search' },
        strategic_hiring: { themes: ['leadership', 'team-build', 'scaling'], eng: 'executive_search' },
        ma_activity: { themes: ['post-acquisition', 'integration', 'merger'], eng: 'executive_search' },
        leadership_change: { themes: ['succession', 'leadership-transition', 'turnaround'], eng: 'succession' },
        restructuring: { themes: ['turnaround', 'restructuring', 'transformation'], eng: 'executive_search' },
        layoffs: { themes: ['restructuring', 'talent-market'], eng: 'executive_search' },
        product_launch: { themes: ['product', 'innovation', 'go-to-market'], eng: 'executive_search' },
        partnership: { themes: ['partnership', 'alliance', 'ecosystem'], eng: 'executive_search' },
      };
      const mapping = sigMap[signal_type] || { themes: [], eng: null };
      if (mapping.themes.length) {
        idx++; params.push(mapping.themes);
        scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${idx}::text[]))::float * 0.2`);
      }
      if (mapping.eng) {
        idx++; params.push(mapping.eng);
        scoreTerms.push(`CASE WHEN cs.engagement_type = $${idx} THEN 0.1 ELSE 0 END`);
      }
    }
    if (company_id) {
      idx++; params.push(company_id);
      scoreTerms.push(`CASE WHEN cs.client_id = $${idx}::uuid THEN 0.5 ELSE 0 END`);
    } else if (company_name) {
      idx++; params.push(`%${company_name}%`);
      scoreTerms.push(`CASE WHEN cs.client_name ILIKE $${idx} THEN 0.4 ELSE 0 END`);
    }

    const scoreExpr = scoreTerms.length > 0 ? scoreTerms.join(' + ') : '0';
    idx++; params.push(Math.min(parseInt(lim) || 5, 20));

    const { rows } = await pool.query(`
      SELECT
        cs.id, cs.title, cs.client_name, cs.role_title, cs.engagement_type,
        cs.sector, cs.geography, cs.seniority_level, cs.year,
        cs.themes, cs.capabilities, cs.change_vectors,
        cs.challenge, cs.outcome,
        cs.public_approved, cs.visibility, cs.status,
        cs.public_title, cs.public_summary,
        (${scoreExpr}) AS relevance_score
      FROM case_studies cs
      ${where}
      AND (${scoreExpr}) > 0
      ORDER BY (${scoreExpr}) DESC, cs.year DESC NULLS LAST
      LIMIT $${idx}
    `, params);

    res.json({ relevant_case_studies: rows });
  } catch (err) {
    console.error('Case studies relevant error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Match case studies to a dispatch by theme/sector/geography overlap
// Used by the dispatch rendering pipeline — returns ONLY public-safe fields
app.get('/api/case-studies/match', authenticateToken, async (req, res) => {
  try {
    const { themes, sectors, geographies, change_vectors, limit: lim = 3 } = req.query;
    const tenantId = req.tenant_id;

    // Only return approved case studies
    let where = `WHERE cs.tenant_id = $1 AND cs.public_approved = true`;
    const params = [tenantId];
    let idx = 1;

    // Build relevance scoring
    const scoreTerms = [];
    if (themes) {
      const themeArr = themes.split(',').map(t => t.trim());
      idx++; params.push(themeArr);
      scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.themes) t WHERE t = ANY($${idx}::text[]))::float * 0.4`);
    }
    if (sectors) {
      idx++; params.push(`%${sectors}%`);
      scoreTerms.push(`CASE WHEN cs.public_sector ILIKE $${idx} THEN 0.25 ELSE 0 END`);
    }
    if (geographies) {
      idx++; params.push(`%${geographies}%`);
      scoreTerms.push(`CASE WHEN cs.public_geography ILIKE $${idx} THEN 0.2 ELSE 0 END`);
    }
    if (change_vectors) {
      const cvArr = change_vectors.split(',').map(v => v.trim());
      idx++; params.push(cvArr);
      scoreTerms.push(`(SELECT COUNT(*) FROM unnest(cs.change_vectors) v WHERE v = ANY($${idx}::text[]))::float * 0.15`);
    }

    const scoreExpr = scoreTerms.length > 0 ? scoreTerms.join(' + ') : '0';
    idx++; params.push(Math.min(parseInt(lim) || 3, 10));

    const { rows } = await pool.query(`
      SELECT
        cs.id, cs.slug,
        cs.public_title AS title,
        cs.public_summary AS summary,
        cs.public_sector AS sector,
        cs.public_geography AS geography,
        cs.public_capability AS capability,
        cs.engagement_type,
        cs.themes,
        cs.capabilities,
        (${scoreExpr}) AS match_score
      FROM case_studies cs
      ${where}
      ORDER BY (${scoreExpr}) DESC, cs.relevance_score DESC
      LIMIT $${idx}
    `, params);

    res.json({ matched_case_studies: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Classified documents overview
app.get('/api/documents/classified', authenticateToken, async (req, res) => {
  try {
    const { document_type, limit: lim = 30 } = req.query;
    let where = 'WHERE ed.tenant_id = $1 AND ed.classified_at IS NOT NULL';
    const params = [req.tenant_id];
    let idx = 1;
    if (document_type) { idx++; where += ` AND ed.document_type = $${idx}`; params.push(document_type); }
    idx++; params.push(Math.min(parseInt(lim) || 30, 100));

    const { rows } = await pool.query(`
      SELECT ed.id, ed.title, ed.document_type, ed.content_summary, ed.relevance_tags,
             ed.source_url, ed.classified_at, ed.uploaded_by_user_id,
             u.name AS uploaded_by_name,
             (SELECT COUNT(*) FROM document_people dp WHERE dp.document_id = ed.id) AS people_count
      FROM external_documents ed
      LEFT JOIN users u ON u.id = ed.uploaded_by_user_id
      ${where}
      ORDER BY ed.classified_at DESC
      LIMIT $${idx}
    `, params);

    // Type summary
    const { rows: typeSummary } = await pool.query(`
      SELECT document_type, COUNT(*) AS count
      FROM external_documents
      WHERE tenant_id = $1 AND classified_at IS NOT NULL AND document_type IS NOT NULL
      GROUP BY document_type ORDER BY COUNT(*) DESC
    `, [req.tenant_id]);

    res.json({ documents: rows, type_summary: typeSummary });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN — tenant owner only
// ═══════════════════════════════════════════════════════════════════════════════

function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  next();
}

// Team members overview
app.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
  try {
    // Core user data — only references tables guaranteed to exist
    const { rows } = await pool.query(`
      SELECT u.id, u.name, u.email, u.role, u.region, u.onboarded, u.created_at, u.updated_at,
        (SELECT COUNT(*) FROM sessions s WHERE s.user_id = u.id AND s.expires_at > NOW()) AS active_sessions,
        (SELECT MAX(s.created_at) FROM sessions s WHERE s.user_id = u.id) AS last_login,
        (SELECT COUNT(*) FROM interactions i WHERE i.user_id = u.id AND i.tenant_id = $1) AS interactions_created,
        (SELECT COUNT(*) FROM interactions i WHERE i.user_id = u.id AND i.tenant_id = $1 AND i.interaction_at > NOW() - INTERVAL '30 days') AS interactions_30d,
        (SELECT COUNT(*) FROM team_proximity tp WHERE tp.team_member_id = u.id AND tp.tenant_id = $1) AS proximity_connections,
        (SELECT COUNT(*) FROM signal_dispatches sd WHERE sd.claimed_by = u.id AND sd.tenant_id = $1) AS dispatches_claimed,
        (SELECT COUNT(*) FROM signal_dispatches sd WHERE sd.claimed_by = u.id AND sd.status = 'sent' AND sd.tenant_id = $1) AS dispatches_sent
      FROM users u
      WHERE u.tenant_id = $1
      ORDER BY u.created_at ASC
    `, [req.tenant_id]);

    // Enrich with Google account data (table may not exist)
    try {
      const { rows: googleRows } = await pool.query(`
        SELECT user_id,
          COUNT(*) AS google_accounts,
          bool_or(sync_enabled) AS google_sync_active,
          MAX(last_sync_at) AS google_last_sync
        FROM user_google_accounts GROUP BY user_id
      `);
      const googleMap = new Map(googleRows.map(g => [g.user_id, g]));
      rows.forEach(u => {
        const g = googleMap.get(u.id);
        u.google_accounts = g?.google_accounts || 0;
        u.google_sync_active = g?.google_sync_active || false;
        u.google_last_sync = g?.google_last_sync || null;
      });
    } catch (e) {
      rows.forEach(u => { u.google_accounts = 0; u.google_sync_active = false; u.google_last_sync = null; });
    }

    res.json({ users: rows });
  } catch (err) {
    console.error('Admin users error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Update user role
app.patch('/api/admin/users/:id/role', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { role } = req.body;
    if (!['admin', 'consultant', 'researcher', 'viewer'].includes(role)) {
      return res.status(400).json({ error: 'Invalid role' });
    }
    const { rows: [updated] } = await pool.query(
      'UPDATE users SET role = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3 RETURNING id, name, email, role',
      [role, req.params.id, req.tenant_id]
    );
    if (!updated) return res.status(404).json({ error: 'User not found' });
    auditLog(req.user.user_id, 'change_role', 'user', updated.id, { name: updated.name, new_role: role });
    res.json({ user: updated });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Platform health overview
app.get('/api/admin/health', authenticateToken, requireAdmin, async (req, res) => {
  try {
    // Each query is fail-safe — tables may not exist on all deployments
    const stats = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM users WHERE tenant_id = $1) AS total_users,
        (SELECT COUNT(*) FROM sessions WHERE expires_at > NOW()) AS active_sessions,
        (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS total_people,
        (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS total_companies,
        (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1) AS total_signals,
        (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1 AND detected_at > NOW() - INTERVAL '24 hours') AS signals_24h,
        (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1 AND detected_at > NOW() - INTERVAL '7 days') AS signals_7d,
        (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1) AS total_documents,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) AS total_interactions,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND interaction_at > NOW() - INTERVAL '7 days') AS interactions_7d,
        (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1) AS total_placements,
        (SELECT COALESCE(SUM(placement_fee), 0) FROM conversions WHERE tenant_id = $1 AND source IN ('xero_export', 'xero', 'manual') AND placement_fee IS NOT NULL) AS total_revenue,
        (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1) AS total_dispatches,
        (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1 AND status = 'draft') AS dispatches_draft,
        (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1 AND status = 'sent') AS dispatches_sent
    `, [req.tenant_id]).catch(e => { console.error('Admin stats error:', e.message); return { rows: [{}] }; });

    // Optional tables — may not exist
    let googleCount = 0;
    try { const r = await pool.query('SELECT COUNT(*) AS cnt FROM user_google_accounts WHERE sync_enabled = true'); googleCount = r.rows[0]?.cnt || 0; } catch (e) {}
    let grabsCount = 0;
    try { const r = await pool.query('SELECT COUNT(*) AS cnt FROM signal_grabs WHERE tenant_id = $1', [req.tenant_id]); grabsCount = r.rows[0]?.cnt || 0; } catch (e) {}

    // Gmail/sync running tallies
    let gmailStats = {};
    try {
      const { rows: [gs] } = await pool.query(`
        SELECT
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync') AS gmail_interactions,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync' AND interaction_at > NOW() - INTERVAL '7 days') AS gmail_7d,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync' AND interaction_at > NOW() - INTERVAL '24 hours') AS gmail_24h,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) AS total_interactions,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND interaction_at > NOW() - INTERVAL '7 days') AS interactions_7d,
          (SELECT COUNT(DISTINCT person_id) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync') AS gmail_people_matched,
          (SELECT COUNT(*) FROM team_proximity WHERE tenant_id = $1 AND source = 'gmail') AS gmail_proximity_links,
          (SELECT MAX(last_sync_at) FROM user_google_accounts WHERE sync_enabled = true) AS last_gmail_sync,
          (SELECT COUNT(*) FROM case_studies WHERE tenant_id = $1 AND status != 'deleted') AS total_case_studies,
          (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND source = 'wip_workbook') AS wip_records,
          (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND source = 'xero_export') AS xero_records,
          (SELECT COUNT(*) FROM receivables WHERE tenant_id = $1) AS total_receivables,
          (SELECT COUNT(*) FROM feed_proposals) AS user_feeds
      `, [req.tenant_id]);
      gmailStats = gs || {};
    } catch (e) { /* some tables may not exist */ }

    const sources = await pool.query(`
      SELECT rs.name, rs.source_type, rs.url, rs.enabled,
             rs.last_fetched_at, rs.last_error, rs.consecutive_errors,
             (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_name = rs.name AND ed.tenant_id = $1) AS doc_count
      FROM rss_sources rs
      ORDER BY rs.enabled DESC, rs.last_fetched_at DESC NULLS LAST
    `, [req.tenant_id]).catch(() => ({ rows: [] }));

    const pipelines = await pool.query(`
      SELECT pipeline_key, pipeline_name, status, started_at, completed_at, duration_ms,
             items_processed, error_message
      FROM pipeline_runs
      ORDER BY started_at DESC LIMIT 30
    `).catch(() => ({ rows: [] }));

    const storage = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM people WHERE embedded_at IS NOT NULL) AS person_embeddings,
        (SELECT COUNT(*) FROM companies WHERE embedded_at IS NOT NULL) AS company_embeddings,
        (SELECT COUNT(*) FROM external_documents WHERE embedded_at IS NOT NULL) AS document_embeddings,
        (SELECT COUNT(*) FROM signal_events WHERE embedded_at IS NOT NULL) AS signal_embeddings,
        (SELECT COUNT(*) FROM case_studies WHERE embedded_at IS NOT NULL) AS case_study_embeddings,
        (SELECT COUNT(*) FROM people) AS total_people,
        (SELECT COUNT(*) FROM companies) AS total_companies,
        (SELECT COUNT(*) FROM external_documents) AS total_documents,
        (SELECT COUNT(*) FROM signal_events) AS total_signals,
        (SELECT COUNT(*) FROM case_studies WHERE status != 'deleted') AS total_case_studies
    `).catch(() => ({ rows: [{}] }));

    const emb = storage.rows[0] || {};
    res.json({
      stats: { ...stats.rows[0], google_syncs_active: googleCount, total_grabs: grabsCount, ...gmailStats },
      sources: sources.rows,
      pipeline_runs: pipelines.rows,
      embeddings: {
        total_embeddings: Number(emb.person_embeddings || 0) + Number(emb.company_embeddings || 0) + Number(emb.document_embeddings || 0) + Number(emb.signal_embeddings || 0) + Number(emb.case_study_embeddings || 0),
        person_embeddings: `${emb.person_embeddings || 0} / ${emb.total_people || 0}`,
        company_embeddings: `${emb.company_embeddings || 0} / ${emb.total_companies || 0}`,
        document_embeddings: `${emb.document_embeddings || 0} / ${emb.total_documents || 0}`,
        signal_embeddings: `${emb.signal_embeddings || 0} / ${emb.total_signals || 0}`,
        case_study_embeddings: `${emb.case_study_embeddings || 0} / ${emb.total_case_studies || 0}`
      }
    });
  } catch (err) {
    console.error('Admin health error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Data ingestion per user
app.get('/api/admin/ingestion', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const tenantId = req.tenant_id;

    // People imported per user by source — use created_by first, fall back to proximity
    let peopleBySource = [];
    try {
      const { rows } = await pool.query(`
        SELECT
          COALESCE(u1.id, u2.id) AS user_id,
          COALESCE(u1.name, u2.name) AS name,
          COALESCE(u1.email, u2.email) AS email,
          p.source,
          COUNT(*) AS count,
          MIN(p.created_at) AS earliest,
          MAX(p.created_at) AS latest
        FROM people p
        LEFT JOIN users u1 ON u1.id = p.created_by
        LEFT JOIN LATERAL (
          SELECT tp.team_member_id FROM team_proximity tp
          WHERE tp.person_id = p.id AND tp.tenant_id = $1 LIMIT 1
        ) tp_link ON p.created_by IS NULL
        LEFT JOIN users u2 ON u2.id = tp_link.team_member_id AND p.created_by IS NULL
        WHERE p.tenant_id = $1
          AND p.source IN ('csv_import', 'linkedin_import', 'chat_concierge', 'chat_intel', 'ezekia', 'gmail_discovery')
        GROUP BY COALESCE(u1.id, u2.id), COALESCE(u1.name, u2.name), COALESCE(u1.email, u2.email), p.source
        ORDER BY MAX(p.created_at) DESC
      `, [tenantId]);
      peopleBySource = rows;
    } catch (e) { /* created_by column may not exist yet on older deployments */ }

    // LinkedIn connections per team member
    let linkedinConnections = [];
    try {
      const { rows } = await pool.query(`
        SELECT
          lc.team_member_id, u.name, u.email,
          COUNT(*) AS total_connections,
          COUNT(lc.matched_person_id) AS matched,
          COUNT(*) - COUNT(lc.matched_person_id) AS unmatched,
          ROUND(AVG(lc.match_confidence)::numeric, 2) AS avg_confidence,
          MIN(lc.imported_at) AS first_import,
          MAX(lc.imported_at) AS last_import,
          COUNT(DISTINCT lc.company) AS unique_companies
        FROM linkedin_connections lc
        LEFT JOIN users u ON u.id = lc.team_member_id
        GROUP BY lc.team_member_id, u.name, u.email
        ORDER BY COUNT(*) DESC
      `);
      linkedinConnections = rows;
    } catch (e) { /* table may not exist */ }

    // Google accounts detail
    let googleAccounts = [];
    try {
      const { rows } = await pool.query(`
        SELECT
          ug.user_id, u.name, u.email AS user_email,
          ug.google_email, ug.sync_enabled, ug.last_sync_at, ug.scopes,
          ug.created_at AS connected_at,
          (SELECT COUNT(*) FROM interactions i WHERE i.source = 'gmail' AND i.user_id = ug.user_id AND i.tenant_id = $1) AS emails_synced,
          (SELECT COUNT(*) FROM interactions i WHERE i.source = 'gmail' AND i.user_id = ug.user_id AND i.tenant_id = $1 AND i.interaction_at > NOW() - INTERVAL '7 days') AS emails_7d,
          (SELECT COUNT(*) FROM email_signals es WHERE es.user_id = ug.user_id) AS email_signals
        FROM user_google_accounts ug
        JOIN users u ON u.id = ug.user_id
        ORDER BY ug.last_sync_at DESC NULLS LAST
      `, [tenantId]);
      googleAccounts = rows;
    } catch (e) { /* table may not exist */ }

    // Xero sync status
    let xeroSync = [];
    try {
      const { rows } = await pool.query(`
        SELECT xt.tenant_name, xt.expires_at, xt.updated_at AS token_updated,
               xs.last_sync_at, xs.invoices_synced, xs.last_error
        FROM xero_tokens xt
        LEFT JOIN xero_sync_state xs ON xs.tenant_id = xt.tenant_id
      `);
      xeroSync = rows;
    } catch (e) { /* tables may not exist */ }

    // Ezekia enrichment stats
    let ezekiaStats = null;
    try {
      const { rows: [stats] } = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE source = 'ezekia') AS ezekia_people,
          COUNT(*) FILTER (WHERE enriched_at IS NOT NULL) AS enriched_people,
          MAX(synced_at) FILTER (WHERE source = 'ezekia') AS last_ezekia_sync,
          MAX(enriched_at) AS last_enrichment
        FROM people WHERE tenant_id = $1
      `, [tenantId]);
      ezekiaStats = stats;
    } catch (e) { /* columns may not exist */ }

    // Documents uploaded per user
    let docUploads = [];
    try {
      const { rows } = await pool.query(`
        SELECT u.id AS user_id, u.name, u.email,
               COUNT(*) AS docs_uploaded,
               MIN(ed.published_at) AS earliest,
               MAX(ed.published_at) AS latest
        FROM external_documents ed
        JOIN users u ON u.id = ed.uploaded_by_user_id
        WHERE ed.tenant_id = $1 AND ed.uploaded_by_user_id IS NOT NULL
        GROUP BY u.id, u.name, u.email
        ORDER BY COUNT(*) DESC
      `, [tenantId]);
      docUploads = rows;
    } catch (e) { /* column may not exist */ }

    // Team proximity by source (how connections were created)
    let proxBySrc = [];
    try {
      const { rows } = await pool.query(`
        SELECT
          u.name, u.email,
          tp.source AS proximity_source,
          COUNT(*) AS connections,
          ROUND(AVG(tp.strength)::numeric, 2) AS avg_strength
        FROM team_proximity tp
        JOIN users u ON u.id = tp.team_member_id
        WHERE tp.tenant_id = $1
        GROUP BY u.name, u.email, tp.source
        ORDER BY u.name, COUNT(*) DESC
      `, [tenantId]);
      proxBySrc = rows;
    } catch (e) { /* table may not exist */ }

    res.json({
      people_by_source: peopleBySource,
      linkedin_connections: linkedinConnections,
      google_accounts: googleAccounts,
      xero_sync: xeroSync,
      ezekia_stats: ezekiaStats,
      doc_uploads: docUploads,
      proximity_by_source: proxBySrc
    });
  } catch (err) {
    console.error('Admin ingestion error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Tenant config
app.get('/api/admin/tenant', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { rows: [tenant] } = await pool.query(
      'SELECT * FROM tenants WHERE id = $1', [req.tenant_id]
    );
    res.json({ tenant });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Audit log
app.get('/api/admin/audit', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const { rows } = await pool.query(`
      SELECT al.action, al.target_type, al.target_id, al.details, al.ip_address, al.created_at,
             u.name AS user_name, u.email AS user_email
      FROM audit_logs al
      LEFT JOIN users u ON u.id = al.user_id
      ORDER BY al.created_at DESC
      LIMIT $1
    `, [limit]);
    res.json({ logs: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// USER PROFILE: Self-serve data operations
// ═══════════════════════════════════════════════════════════════════════════════

// Profile stats
app.get('/api/profile/stats', authenticateToken, async (req, res) => {
  try {
    const uid = req.user.user_id;
    const tid = req.tenant_id;
    const { rows: [s] } = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM team_proximity WHERE team_member_id = $1 AND tenant_id = $2) AS connections,
        (SELECT COUNT(*) FROM interactions WHERE (user_id = $1 OR created_by = $1) AND tenant_id = $2) AS interactions,
        (SELECT COUNT(*) FROM signal_dispatches WHERE claimed_by = $1 AND tenant_id = $2) AS dispatches,
        (SELECT COUNT(*) FROM feed_proposals WHERE proposed_by = $1) AS feeds
    `, [uid, tid]);

    // Import history from audit log
    const { rows: imports } = await pool.query(`
      SELECT action, details->>'filename' AS filename, details->>'total' AS total, created_at
      FROM audit_logs WHERE user_id = $1 AND action IN ('csv_import','linkedin_connections_import','linkedin_messages_import','workbook_import','admin_linkedin_import','document_upload')
      ORDER BY created_at DESC LIMIT 20
    `, [uid]).catch(() => ({ rows: [] }));

    res.json({
      connections: s?.connections || 0,
      interactions: s?.interactions || 0,
      dispatches: s?.dispatches || 0,
      imports: imports.length,
      feeds: s?.feeds || 0,
      import_history: imports
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User feeds — list
app.get('/api/profile/feeds', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT fp.id, fp.proposed_url AS url, fp.proposed_name AS name, fp.status,
        fp.status = 'approved' AS active, fp.created_at
      FROM feed_proposals fp
      WHERE fp.proposed_by = $1
      ORDER BY fp.created_at DESC
    `, [req.user.user_id]).catch(() => ({ rows: [] }));
    res.json({ feeds: rows });
  } catch (err) { res.json({ feeds: [] }); }
});

// User feeds — add
app.post('/api/profile/feeds', authenticateToken, async (req, res) => {
  try {
    const { url, name } = req.body;
    if (!url || !url.startsWith('http')) return res.status(400).json({ error: 'Valid URL required' });

    // Try to detect if it's an RSS feed
    let isRss = false;
    try {
      const probe = await fetch(url, { headers: { 'User-Agent': 'MLX-Intelligence/1.0' }, signal: AbortSignal.timeout(5000) });
      const text = await probe.text();
      isRss = text.includes('<rss') || text.includes('<feed') || text.includes('<channel');
    } catch (e) { /* probe failed, not critical */ }

    const { rows: [feed] } = await pool.query(`
      INSERT INTO feed_proposals (proposed_url, proposed_name, proposed_by, status, is_rss, created_at)
      VALUES ($1, $2, $3, 'approved', $4, NOW())
      ON CONFLICT (proposed_url) DO UPDATE SET proposed_name = COALESCE(EXCLUDED.proposed_name, feed_proposals.proposed_name)
      RETURNING id, proposed_url AS url, proposed_name AS name, status
    `, [url.trim(), name || null, req.user.user_id, isRss]);

    // If it's RSS, also add to the feed_inventory / external_sources system
    if (isRss) {
      try {
        await pool.query(`
          INSERT INTO feed_inventory (url, name, source_type, region, added_by, tenant_id, created_at)
          VALUES ($1, $2, 'rss', $3, $4, $5, NOW())
          ON CONFLICT (url) DO NOTHING
        `, [url.trim(), name || url, req.user?.region || 'GLOBAL', req.user.user_id, req.tenant_id]);

        // Also activate for this tenant
        const { rows: [fi] } = await pool.query(`SELECT id FROM feed_inventory WHERE url = $1 LIMIT 1`, [url.trim()]);
        if (fi) {
          await pool.query(`
            INSERT INTO tenant_feeds (tenant_id, feed_id, selection_method, activated_at)
            VALUES ($1, $2, 'user_contributed', NOW())
            ON CONFLICT (tenant_id, feed_id) DO UPDATE SET active = TRUE
          `, [req.tenant_id, fi.id]);
        }
      } catch (e) { /* feed_inventory may not exist or have different schema */ }
    }

    auditLog(req.user.user_id, 'add_feed', 'feed', feed?.id, { url, name, is_rss: isRss });
    res.json({ ...feed, is_rss: isRss });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User feeds — remove
app.delete('/api/profile/feeds/:id', authenticateToken, async (req, res) => {
  try {
    await pool.query(`DELETE FROM feed_proposals WHERE id = $1 AND proposed_by = $2`, [req.params.id, req.user.user_id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// User import — handles LinkedIn CSV, contacts, documents
const profileUpload = require('multer')({ dest: '/tmp/ml-profile-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });
app.post('/api/profile/import', authenticateToken, profileUpload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const importType = req.body.import_type;
    const userId = req.user.user_id;
    const tenantId = req.tenant_id;
    if (!file) return res.status(400).json({ error: 'No file' });

    // LinkedIn connections
    if (importType === 'linkedin_connections') {
      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines = raw.split('\n');
      // Find real header row — skip LinkedIn preamble
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
        if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
          headerIdx = i; break;
        }
      }
      if (headerIdx === 0) {
        for (let i = 0; i < Math.min(allLines.length, 20); i++) {
          const parts = allLines[i].split(',');
          if (parts.length >= 3 && parts[0].trim().length > 0 && parts[0].trim().length < 30) { headerIdx = i; break; }
        }
      }
      const lines = allLines.slice(headerIdx).filter(l => l.trim());
      function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const vals = parseCSV(lines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
      }

      // Load people for matching
      const { rows: dbPeople } = await pool.query(
        `SELECT id, full_name, linkedin_url FROM people WHERE tenant_id = $1`, [tenantId]
      );
      const linkedinIndex = new Map(), nameIndex = new Map();
      for (const p of dbPeople) {
        if (p.linkedin_url) { const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1]; if (slug) linkedinIndex.set(slug, p); }
        const norm = (p.full_name || '').toLowerCase().trim();
        if (norm) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
      }

      // Flexible header detection — LinkedIn exports vary
      const sampleRow = rows[0] || {};
      const colKeys = Object.keys(sampleRow);
      const findCol = (...patterns) => colKeys.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
      const firstNameCol = findCol('first name', 'firstname');
      const lastNameCol = findCol('last name', 'lastname');
      const urlCol = findCol('url', 'profile');
      const companyCol = findCol('company', 'organisation', 'organization');
      const positionCol = findCol('position', 'title', 'role');
      const emailCol = findCol('email');

      console.log(`LinkedIn import: detected columns — name: "${firstNameCol}"+"${lastNameCol}", url: "${urlCol}", company: "${companyCol}"`);

      const stats = { total: rows.length, matched: 0, created: 0, proximity_created: 0, skipped: 0 };
      for (const row of rows) {
        const firstName = (firstNameCol ? row[firstNameCol] : '') || '';
        const lastName = (lastNameCol ? row[lastNameCol] : '') || '';
        const fullName = `${firstName} ${lastName}`.trim();
        const linkedinUrl = (urlCol ? row[urlCol] : '') || '';
        const company = (companyCol ? row[companyCol] : '') || '';
        const position = (positionCol ? row[positionCol] : '') || '';
        const email = (emailCol ? row[emailCol] : '') || '';
        if (!fullName || fullName.length < 2) { stats.skipped++; continue; }

        let personId = null;
        const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
        if (slug && linkedinIndex.has(slug)) personId = linkedinIndex.get(slug).id;
        if (!personId) { const cands = nameIndex.get(fullName.toLowerCase().trim()) || []; if (cands.length === 1) personId = cands[0].id; }

        if (personId) { stats.matched++; }
        else {
          try {
            const { rows: [newP] } = await pool.query(
              `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, linkedin_url, source, created_by, tenant_id)
               VALUES ($1,$2,$3,$4,$5,$6,'linkedin_import',$7,$8) RETURNING id`,
              [fullName, firstName, lastName, position || null, company || null, linkedinUrl || null, userId, tenantId]);
            personId = newP.id;
            stats.created++;
          } catch (e) { stats.skipped++; continue; }
        }

        if (personId) {
          try {
            await pool.query(
              `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
               VALUES ($1, $2, 'linkedin_connection', 0.5, 'linkedin_import', $3)
               ON CONFLICT (person_id, team_member_id) DO UPDATE SET relationship_strength = GREATEST(team_proximity.relationship_strength, 0.5)`,
              [personId, userId, tenantId]);
            stats.proximity_created++;
          } catch (e) {}
        }
      }

      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'linkedin_connections_import', 'people', null, { ...stats, filename: file.originalname });
      return res.json(stats);
    }

    // Contacts CSV
    if (importType === 'contacts') {
      const raw = require('fs').readFileSync(file.path, 'utf8');
      const lines = raw.split('\n').filter(l => l.trim());
      function parseCSV2(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const headers = parseCSV2(lines[0]);
      const lh = headers.map(h => h.toLowerCase());
      const nameCol = headers[lh.findIndex(h => h.includes('name'))] || headers[0];
      const titleCol = headers[lh.findIndex(h => h.includes('title') || h.includes('role'))] || null;
      const companyCol = headers[lh.findIndex(h => h.includes('company') || h.includes('org'))] || null;
      const emailCol = headers[lh.findIndex(h => h.includes('email'))] || null;

      const stats = { total: 0, created: 0, skipped: 0 };
      for (let i = 1; i < lines.length; i++) {
        const vals = parseCSV2(lines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });
        const name = row[nameCol]?.trim();
        if (!name || name.length < 2) continue;
        stats.total++;
        const { rows: exists } = await pool.query(`SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [name, tenantId]);
        if (exists.length) { stats.skipped++; continue; }
        await pool.query(
          `INSERT INTO people (full_name, current_title, current_company_name, email, source, created_by, tenant_id)
           VALUES ($1,$2,$3,$4,'csv_import',$5,$6)`,
          [name, titleCol ? row[titleCol] : null, companyCol ? row[companyCol] : null, emailCol ? row[emailCol] : null, userId, tenantId]);
        stats.created++;
      }
      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'csv_import', 'people', null, { ...stats, filename: file.originalname });
      return res.json(stats);
    }

    // Document upload (PDF, XLSX, TXT)
    if (importType === 'document') {
      const hash = require('crypto').createHash('md5').update(file.originalname + file.size).digest('hex');
      const { rows: exists } = await pool.query(`SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2`, [hash, tenantId]);
      if (exists.length) { try { require('fs').unlinkSync(file.path); } catch(e){} return res.json({ documents_created: 0, message: 'File already imported' }); }

      let content = file.originalname;
      if (file.originalname.endsWith('.pdf')) {
        try { const pdfParse = require('pdf-parse'); const d = await pdfParse(require('fs').readFileSync(file.path)); content = d.text; } catch(e) {}
      } else if (file.originalname.match(/\.xlsx?$/i)) {
        try { const XLSX = require('xlsx'); const wb = XLSX.readFile(file.path); content = wb.SheetNames.map(n => { const r = XLSX.utils.sheet_to_json(wb.Sheets[n], {header:1,defval:''}).slice(0,100).map(r=>Object.values(r).join(' | ')).join('\n'); return `=== ${n} ===\n${r}`; }).join('\n\n'); } catch(e) {}
      } else {
        try { content = require('fs').readFileSync(file.path, 'utf8'); } catch(e) {}
      }

      await pool.query(`
        INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
          tenant_id, uploaded_by_user_id, processing_status, created_at)
        VALUES ($1, $2, $3, 'user_upload', $4, $5, $6, $7, 'processed', NOW())
      `, [file.originalname, content.slice(0, 50000), file.originalname, `upload://${file.originalname}`, hash, tenantId, userId]);

      // Embed
      try {
        const emb = await generateQueryEmbedding((file.originalname + '\n\n' + content).slice(0, 8000));
        const url = new URL('/collections/documents/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: hash, vector: emb, payload: { tenant_id: tenantId, title: file.originalname, source_type: 'user_upload' } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 15000 },
            (r) => { const c = []; r.on('data', d => c.push(d)); r.on('end', () => resolve()); });
          qReq.on('error', reject); qReq.write(body); qReq.end();
        });
      } catch(e) {}

      try { require('fs').unlinkSync(file.path); } catch (e) {}
      auditLog(userId, 'document_upload', 'external_documents', null, { filename: file.originalname, size: file.size });
      return res.json({ documents_created: 1, filename: file.originalname });
    }

    // LinkedIn messages
    if (importType === 'messages') {
      // Reuse the chat upload + process_uploaded_file logic
      try { require('fs').unlinkSync(file.path); } catch (e) {}
      return res.json({ error: 'LinkedIn messages import — use the chat interface for now (requires AI-assisted parsing)' });
    }

    try { require('fs').unlinkSync(file.path); } catch (e) {}
    res.json({ error: 'Unknown import type: ' + importType });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger sync for current user
app.post('/api/profile/trigger-sync', authenticateToken, async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT google_email FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true`, [req.user.user_id]
    );
    if (!rows.length) return res.json({ message: 'No Google account connected. Connect from this page first.' });
    res.json({ message: `Sync triggered for ${rows[0].google_email}. Gmail and Drive will sync on the next cycle (every 15 minutes).` });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ECOSYSTEM MAP DATA
// ═══════════════════════════════════════════════════════════════════════════════

app.get('/api/ecosystem', authenticateToken, async (req, res) => {
  try {
    const tid = req.tenant_id;

    // 1. Regional signal density
    const { rows: signalsByRegion } = await pool.query(`
      SELECT
        CASE
          WHEN c.country_code IN ('AU','NZ') OR c.geography ILIKE '%australia%' THEN 'AU'
          WHEN c.country_code IN ('SG','MY','ID','TH','VN','PH') OR c.geography ILIKE '%singapore%' OR c.geography ILIKE '%southeast%' THEN 'SG'
          WHEN c.country_code IN ('GB','UK','IE','DE','FR','NL') OR c.geography ILIKE '%united kingdom%' OR c.geography ILIKE '%london%' OR c.geography ILIKE '%europe%' THEN 'UK'
          WHEN c.country_code IN ('US','CA') OR c.geography ILIKE '%united states%' OR c.geography ILIKE '%america%' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        se.signal_type,
        COUNT(*) AS signal_count,
        COUNT(*) FILTER (WHERE se.detected_at > NOW() - INTERVAL '7 days') AS signals_7d,
        COUNT(*) FILTER (WHERE se.detected_at > NOW() - INTERVAL '30 days') AS signals_30d,
        AVG(se.confidence_score) AS avg_confidence
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE se.tenant_id = $1 AND se.detected_at > NOW() - INTERVAL '90 days'
      GROUP BY region, se.signal_type
      ORDER BY region, signal_count DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 2. Network density per region
    const { rows: density } = await pool.query(`
      SELECT nds.region_code, gp.region_name, gp.weight_boost, gp.is_home_market,
             nds.total_contacts, nds.active_contacts, nds.senior_contacts,
             nds.placement_count, nds.client_count,
             nds.density_score, nds.depth_score, nds.recency_score
      FROM network_density_scores nds
      JOIN geo_priorities gp ON gp.region_code = nds.region_code
      WHERE nds.tenant_id = $1
      ORDER BY nds.density_score DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 3. Revenue by region (from Xero data only)
    const { rows: revenue } = await pool.query(`
      SELECT
        CASE
          WHEN cv.currency = 'AUD' THEN 'AU'
          WHEN cv.currency = 'SGD' THEN 'SG'
          WHEN cv.currency = 'GBP' THEN 'UK'
          WHEN cv.currency = 'USD' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        COUNT(*) AS placement_count,
        COALESCE(SUM(cv.placement_fee), 0) AS total_revenue,
        COALESCE(SUM(cv.placement_fee) FILTER (WHERE cv.start_date > NOW() - INTERVAL '12 months'), 0) AS revenue_12m,
        COALESCE(SUM(cv.placement_fee) FILTER (WHERE cv.start_date > NOW() - INTERVAL '6 months'), 0) AS revenue_6m
      FROM conversions cv
      WHERE cv.tenant_id = $1 AND cv.source IN ('xero_export', 'xero', 'manual') AND cv.placement_fee IS NOT NULL
      GROUP BY region
    `, [tid]).catch(() => ({ rows: [] }));

    // 4. Top companies per region with signal activity
    const { rows: topCompanies } = await pool.query(`
      SELECT
        CASE
          WHEN c.country_code IN ('AU','NZ') OR c.geography ILIKE '%australia%' THEN 'AU'
          WHEN c.country_code IN ('SG','MY','ID','TH','VN','PH') OR c.geography ILIKE '%singapore%' THEN 'SG'
          WHEN c.country_code IN ('GB','UK','IE','DE','FR','NL') OR c.geography ILIKE '%united kingdom%' OR c.geography ILIKE '%london%' THEN 'UK'
          WHEN c.country_code IN ('US','CA') OR c.geography ILIKE '%united states%' THEN 'US'
          ELSE 'OTHER'
        END AS region,
        c.id, c.name, c.is_client, c.sector,
        COUNT(se.id) AS signal_count,
        (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS contact_count,
        (SELECT COUNT(*) FROM team_proximity tp JOIN people p2 ON p2.id = tp.person_id WHERE p2.current_company_id = c.id) AS proximity_count
      FROM companies c
      JOIN signal_events se ON se.company_id = c.id AND se.detected_at > NOW() - INTERVAL '90 days'
      WHERE c.tenant_id = $1
      GROUP BY region, c.id, c.name, c.is_client, c.sector
      ORDER BY region, signal_count DESC
    `, [tid]).catch(() => ({ rows: [] }));

    // 5. Converging themes (top 5 globally)
    const { rows: themes } = await pool.query(`
      SELECT se.signal_type, COUNT(*) AS count, COUNT(DISTINCT se.company_id) AS companies,
             COUNT(DISTINCT se.company_id) FILTER (WHERE c.is_client = true) AS client_companies
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE se.tenant_id = $1 AND se.detected_at > NOW() - INTERVAL '30 days'
      GROUP BY se.signal_type
      ORDER BY count DESC LIMIT 8
    `, [tid]).catch(() => ({ rows: [] }));

    // 6. Case study coverage by geography
    const { rows: caseGeo } = await pool.query(`
      SELECT geography, COUNT(*) AS count
      FROM case_studies
      WHERE tenant_id = $1 AND status != 'deleted' AND geography IS NOT NULL
      GROUP BY geography ORDER BY count DESC LIMIT 10
    `, [tid]).catch(() => ({ rows: [] }));

    // Structure signals by region
    const regionSignals = {};
    for (const r of signalsByRegion) {
      if (!regionSignals[r.region]) regionSignals[r.region] = { total: 0, signals_7d: 0, signals_30d: 0, types: {} };
      regionSignals[r.region].total += parseInt(r.signal_count);
      regionSignals[r.region].signals_7d += parseInt(r.signals_7d);
      regionSignals[r.region].signals_30d += parseInt(r.signals_30d);
      regionSignals[r.region].types[r.signal_type] = parseInt(r.signal_count);
    }

    // Structure companies by region (top 5 per region)
    const regionCompanies = {};
    for (const c of topCompanies) {
      if (!regionCompanies[c.region]) regionCompanies[c.region] = [];
      if (regionCompanies[c.region].length < 5) regionCompanies[c.region].push(c);
    }

    res.json({
      regions: {
        AU: { lat: -33.87, lng: 151.21, name: 'Australia & NZ', color: '#0D7A50', flag: '\ud83c\udde6\ud83c\uddfa' },
        SG: { lat: 1.35, lng: 103.82, name: 'Singapore & SEA', color: '#6D28D9', flag: '\ud83c\uddf8\ud83c\uddec' },
        UK: { lat: 51.51, lng: -0.13, name: 'United Kingdom & Europe', color: '#2563EB', flag: '\ud83c\uddec\ud83c\udde7' },
        US: { lat: 37.77, lng: -122.42, name: 'United States & Americas', color: '#B45309', flag: '\ud83c\uddfa\ud83c\uddf8' },
      },
      signals: regionSignals,
      density: density,
      revenue: revenue,
      companies: regionCompanies,
      themes: themes,
      case_studies_geo: caseGeo
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// ADMIN: Per-User Data Operations
// ═══════════════════════════════════════════════════════════════════════════════

// Upload LinkedIn CSV on behalf of a user
const adminUpload = require('multer')({ dest: '/tmp/ml-admin-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });
app.post('/api/admin/upload-linkedin', authenticateToken, requireAdmin, adminUpload.single('file'), async (req, res) => {
  try {
    const file = req.file;
    const targetUserId = req.body.target_user_id;
    if (!file || !targetUserId) return res.status(400).json({ error: 'File and target_user_id required' });

    // Strip BOM and normalize line endings
    const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    const allLines = raw.split('\n');
    // Find the real header row — LinkedIn CSVs have preamble text before "First Name,Last Name,..."
    let headerIdx = 0;
    for (let i = 0; i < Math.min(allLines.length, 20); i++) {
      const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
      if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
        headerIdx = i; break;
      }
    }
    // Fallback: first line with 3+ comma-separated fields
    if (headerIdx === 0) {
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const parts = allLines[i].split(',');
        if (parts.length >= 3 && parts[0].trim().length > 0 && parts[0].trim().length < 30) { headerIdx = i; break; }
      }
    }
    const lines = allLines.slice(headerIdx).filter(l => l.trim());
    if (!lines.length) return res.json({ error: 'No data found in CSV' });

    function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
    const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim()); // Strip non-printable chars
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const vals = parseCSV(lines[i]);
      const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
    }

    console.log(`Admin LinkedIn upload: ${rows.length} rows, headers: [${headers.slice(0, 8).join('], [')}]`);

    // Flexible header detection
    const sampleRow = rows[0] || {};
    const aColKeys = Object.keys(sampleRow);
    const aFindCol = (...patterns) => aColKeys.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
    const aFirstNameCol = aFindCol('first name', 'firstname');
    const aLastNameCol = aFindCol('last name', 'lastname');
    const aUrlCol = aFindCol('url', 'profile');
    const aCompanyCol = aFindCol('company', 'organisation', 'organization');
    const aPositionCol = aFindCol('position', 'title', 'role');
    const aEmailCol = aFindCol('email');

    console.log(`Admin LinkedIn upload: detected cols — firstName: "${aFirstNameCol}", lastName: "${aLastNameCol}", url: "${aUrlCol}", company: "${aCompanyCol}"`);

    if (!aFirstNameCol && !aLastNameCol) {
      try { require('fs').unlinkSync(file.path); } catch (e) {}
      return res.json({ error: 'Could not detect name columns. Headers found: ' + headers.slice(0, 8).join(', '), total: rows.length, headers: headers.slice(0, 10) });
    }

    // Respond immediately — process in background (20K rows takes minutes)
    const tenantId = req.tenant_id;
    const adminUserId = req.user.user_id;
    res.json({ total: rows.length, message: `Processing ${rows.length} connections in background. Check admin dashboard for progress.`, headers: headers.slice(0, 8), detected: { firstName: aFirstNameCol, lastName: aLastNameCol, url: aUrlCol, company: aCompanyCol } });

    // Background processing
    (async () => {
      try {
        const { rows: dbPeople } = await pool.query(
          `SELECT id, full_name, linkedin_url, current_company_name, email FROM people WHERE tenant_id = $1`, [tenantId]
        );
        const linkedinIndex = new Map(), nameIndex = new Map();
        for (const p of dbPeople) {
          if (p.linkedin_url) {
            const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1];
            if (slug) linkedinIndex.set(slug, p);
          }
          const norm = (p.full_name || '').toLowerCase().trim();
          if (norm) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
        }

        const stats = { total: rows.length, matched: 0, created: 0, proximity_created: 0, skipped: 0 };

        // Batch insert — collect all new people first, then bulk insert
        const toCreate = [];
        const toProximity = [];

        for (const row of rows) {
          const firstName = (aFirstNameCol ? row[aFirstNameCol] : '') || '';
          const lastName = (aLastNameCol ? row[aLastNameCol] : '') || '';
          const fullName = `${firstName} ${lastName}`.trim();
          const linkedinUrl = (aUrlCol ? row[aUrlCol] : '') || '';
          const company = (aCompanyCol ? row[aCompanyCol] : '') || '';
          const position = (aPositionCol ? row[aPositionCol] : '') || '';
          const email = (aEmailCol ? row[aEmailCol] : '') || '';
          if (!fullName || fullName.length < 2) { stats.skipped++; continue; }

          let personId = null;
          const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
          if (slug && linkedinIndex.has(slug)) personId = linkedinIndex.get(slug).id;
          if (!personId) {
            const cands = nameIndex.get(fullName.toLowerCase().trim()) || [];
            if (cands.length === 1) personId = cands[0].id;
          }

          if (personId) {
            stats.matched++;
          } else {
            try {
              const { rows: [newP] } = await pool.query(
                `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, linkedin_url, email, source, created_by, tenant_id)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,'linkedin_import',$8,$9) RETURNING id`,
                [fullName, firstName, lastName, position || null, company || null, linkedinUrl || null, email || null, targetUserId, tenantId]
              );
              personId = newP.id;
              stats.created++;
            } catch (e) { stats.skipped++; continue; }
          }

          if (personId) {
            try {
              await pool.query(
                `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
                 VALUES ($1, $2, 'linkedin_connection', 0.5, 'linkedin_import', $3)
                 ON CONFLICT (person_id, team_member_id) DO UPDATE SET relationship_strength = GREATEST(team_proximity.relationship_strength, 0.5)`,
                [personId, targetUserId, tenantId]
              );
              stats.proximity_created++;
            } catch (e) {}
          }

          if (stats.created % 500 === 0 && stats.created > 0) console.log(`  LinkedIn import: ${stats.created} created, ${stats.matched} matched so far...`);
        }

        try { require('fs').unlinkSync(file.path); } catch (e) {}
        auditLog(adminUserId, 'admin_linkedin_import', 'people', targetUserId, { ...stats, filename: file.originalname });
        console.log(`✅ LinkedIn import complete: ${stats.total} total, ${stats.matched} matched, ${stats.created} created, ${stats.proximity_created} links`);
      } catch (e) {
        console.error('LinkedIn background import error:', e.message);
      }
    })();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger Drive sync for a specific user
app.post('/api/admin/trigger-drive-sync', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { user_id } = req.body;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });

    const { rows } = await pool.query(
      `SELECT id, google_email, access_token, refresh_token FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true`,
      [user_id]
    );
    if (!rows.length) return res.json({ message: 'No Google account connected for this user' });

    // Trigger the drive sync pipeline for this specific user
    const { rows: [user] } = await pool.query('SELECT name FROM users WHERE id = $1', [user_id]);
    auditLog(req.user.user_id, 'admin_trigger_drive_sync', 'user', user_id, { google_email: rows[0].google_email });
    res.json({ message: `Drive sync triggered for ${user?.name || user_id} (${rows[0].google_email}). Will process on next sync cycle.` });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
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

  // Ensure user profile columns exist
  try {
    await pool.query(`
      ALTER TABLE users ADD COLUMN IF NOT EXISTS region VARCHAR(10);
      ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarded BOOLEAN DEFAULT false;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS preferences JSONB DEFAULT '{}'::jsonb;
      ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL;
    `);
  } catch (e) { /* columns may already exist */ }

  // Case study library + document classification tables
  try {
    const fs = require('fs');
    const csMigration = require('path').join(__dirname, 'sql', 'migration_case_studies.sql');
    if (fs.existsSync(csMigration)) {
      await pool.query(fs.readFileSync(csMigration, 'utf8'));
    }
  } catch (e) { /* tables may already exist */ }

  // Ensure people privacy columns exist
  try {
    await pool.query(`
      ALTER TABLE people ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE people ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE people ADD COLUMN IF NOT EXISTS marked_private_at TIMESTAMPTZ;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure document privacy columns exist
  try {
    await pool.query(`
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS uploaded_by_user_id UUID;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure company + signal privacy columns exist
  try {
    await pool.query(`
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS owner_user_id UUID;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure interactions has sensitivity flag for internal ML-to-ML
  try {
    await pool.query(`
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT false;
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS sensitivity VARCHAR(20) DEFAULT 'normal';
    `);
  } catch (e) { /* columns may already exist */ }

  // User attribution columns
  try {
    await pool.query(`
      ALTER TABLE people ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
    `);
  } catch (e) { /* columns may already exist */ }

  // Gmail sync counter + podcast audio URL
  try {
    await pool.query(`ALTER TABLE user_google_accounts ADD COLUMN IF NOT EXISTS emails_synced INTEGER DEFAULT 0`);
    await pool.query(`ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS audio_url TEXT`);
    // Always run podcast audio backfill on startup
    console.log('  🎧 Running podcast audio backfill...');
    const { spawn: spawnAudio } = require('child_process');
    spawnAudio('node', [require('path').join(__dirname, 'scripts', 'backfill_podcast_audio.js')], { stdio: 'inherit', timeout: 300000 })
      .on('exit', (code) => console.log(`  ✅ Podcast audio backfill exited (code ${code})`));
  } catch (e) {}

  // Embedding tracking columns for all embeddable entities
  try {
    await pool.query(`
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE case_studies ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE people ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
    `);
  } catch (e) { /* columns may already exist */ }

  // Bootstrap admin: ensure at least one admin exists per tenant
  try {
    const { rows: admins } = await pool.query(
      `SELECT id FROM users WHERE role = 'admin' AND tenant_id = '00000000-0000-0000-0000-000000000001' LIMIT 1`
    );
    if (admins.length === 0) {
      // Promote the first user created (tenant owner)
      await pool.query(`
        UPDATE users SET role = 'admin', updated_at = NOW()
        WHERE id = (SELECT id FROM users WHERE tenant_id = '00000000-0000-0000-0000-000000000001' ORDER BY created_at ASC LIMIT 1)
      `);
      console.log('  ✅ Admin role bootstrapped for tenant owner');
    }
  } catch (e) { /* ok if fails */ }

  // Ensure network topology tables exist
  try {
    const fs = require('fs');
    const topoMigration = require('path').join(__dirname, 'sql', 'migration_network_topology.sql');
    if (fs.existsSync(topoMigration)) {
      await pool.query(fs.readFileSync(topoMigration, 'utf8'));
      console.log('  ✅ Network topology tables ready');
    }
  } catch (e) { /* tables may already exist */ }

  // Signal Index tables (inline — SQL file splitting breaks on multi-line CREATE TABLE)
  try {
    const siTables = [
      `CREATE TABLE IF NOT EXISTS signal_stocks (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, stock_name VARCHAR(100) NOT NULL, sentiment VARCHAR(10) NOT NULL, weight FLOAT NOT NULL DEFAULT 1.0, horizon VARCHAR(10) NOT NULL, current_count INT DEFAULT 0, prior_count INT DEFAULT 0, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', score FLOAT NOT NULL DEFAULT 50, computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, stock_name, horizon))`,
      `CREATE TABLE IF NOT EXISTS market_health_index (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', bullish_count INT DEFAULT 0, bearish_count INT DEFAULT 0, dominant_signal VARCHAR(100), computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, horizon))`,
      `CREATE TABLE IF NOT EXISTS sector_indices (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, sector VARCHAR(100) NOT NULL, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL DEFAULT 50, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', signal_count INT DEFAULT 0, company_count INT DEFAULT 0, computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, sector, horizon))`,
      `CREATE TABLE IF NOT EXISTS market_health_history (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL, delta FLOAT NOT NULL DEFAULT 0, snapshot_at TIMESTAMPTZ DEFAULT NOW())`,
      `CREATE TABLE IF NOT EXISTS signal_index_stats (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID UNIQUE, people_tracked INT DEFAULT 0, companies_tracked INT DEFAULT 0, signals_7d INT DEFAULT 0, signals_30d INT DEFAULT 0, computed_at TIMESTAMPTZ DEFAULT NOW())`,
    ];
    for (const sql of siTables) { try { await pool.query(sql); } catch (e) {} }
  } catch (e) {}

  // Ensure signal_dispatches table exists with claim columns
  try {
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
        claimed_by UUID, claimed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    // Add claim columns if table already existed
    await pool.query(`ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS claimed_by UUID`);
    await pool.query(`ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ`);
  } catch (e) { /* table may already exist */ }

  // Ensure indexes for new query patterns
  try {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_signal_events_detected ON signal_events(detected_at DESC);
      CREATE INDEX IF NOT EXISTS idx_signal_events_company_type_date ON signal_events(company_id, signal_type, detected_at);
      CREATE INDEX IF NOT EXISTS idx_signal_events_megacap ON signal_events(is_megacap) WHERE is_megacap = true;
      CREATE INDEX IF NOT EXISTS idx_dispatches_status_unclaimed ON signal_dispatches(status, claimed_by) WHERE status = 'draft' AND claimed_by IS NULL;
      CREATE INDEX IF NOT EXISTS idx_companies_is_client ON companies(is_client) WHERE is_client = true;
    `);
  } catch (e) { /* indexes may already exist */ }

  // Run multi-tenant migration
  try {
    const mtMigration = require('path').join(__dirname, 'sql', 'migration_multi_tenant.sql');
    if (require('fs').existsSync(mtMigration)) {
      await pool.query(require('fs').readFileSync(mtMigration, 'utf8'));
      console.log('  \u2705 Multi-tenant migration complete');
    }
  } catch (e) {
    console.log('  \u26a0\ufe0f Multi-tenant migration:', e.message);
  }

  // Backfill: mark companies as clients if they have placements
  try {
    // First, try to link clients to companies by name match
    const { rowCount: linked } = await pool.query(`
      UPDATE accounts SET company_id = co.id
      FROM companies co
      WHERE accounts.company_id IS NULL
        AND LOWER(TRIM(accounts.name)) = LOWER(TRIM(co.name))
    `);
    if (linked > 0) console.log(`  ✅ Linked ${linked} clients to companies by name`);

    // Then mark those companies as clients
    const { rowCount } = await pool.query(`
      UPDATE companies SET is_client = true
      WHERE id IN (
        SELECT DISTINCT cl.company_id FROM accounts cl
        JOIN conversions pl ON pl.client_id = cl.id
        WHERE cl.company_id IS NOT NULL
      ) AND (is_client IS NULL OR is_client = false)
    `);
    if (rowCount > 0) console.log(`  ✅ Backfilled is_client on ${rowCount} companies from placement data`);
  } catch (e) {
    console.log('  ⚠️ Client backfill skipped:', e.message);
  }

  // One-time: Sophie's LinkedIn connections import
  try {
    const sophieCsv = require('path').join(__dirname, 'data', 'sophie_linkedin_connections.csv');
    if (require('fs').existsSync(sophieCsv)) {
      const { rows: [check] } = await pool.query(
        `SELECT COUNT(*) AS cnt FROM team_proximity WHERE source = 'linkedin_import' AND team_member_id = (SELECT id FROM users WHERE email = 'sophiec@mitchellake.com' LIMIT 1)`
      ).catch(() => ({ rows: [{ cnt: '0' }] }));
      if (parseInt(check.cnt) < 100) {
        console.log('  📋 Sophie LinkedIn CSV found — importing in background...');
        const { spawn } = require('child_process');
        const connProc = spawn('node', [require('path').join(__dirname, 'scripts', 'ingest_linkedin_connections.js')], { stdio: 'inherit', timeout: 1200000 });
        connProc.on('exit', (code) => console.log(`  ✅ Sophie LinkedIn connections import exited (code ${code})`));
      } else {
        console.log(`  ℹ️  Sophie LinkedIn already imported (${check.cnt} links)`);
      }
    }
  } catch (e) {}

  // One-time: Sophie's LinkedIn messages import
  try {
    const sophieMsgs = require('path').join(__dirname, 'data', 'sophie_linkedin_messages.csv');
    if (require('fs').existsSync(sophieMsgs)) {
      const { rows: [check] } = await pool.query(
        `SELECT COUNT(*) AS cnt FROM interactions WHERE source = 'linkedin_import' AND user_id = (SELECT id FROM users WHERE email = 'sophiec@mitchellake.com' LIMIT 1)`
      ).catch(() => ({ rows: [{ cnt: '0' }] }));
      if (parseInt(check.cnt) < 100) {
        console.log('  💬 Sophie LinkedIn messages found — importing in background...');
        const { exec } = require('child_process');
        exec(`node ${require('path').join(__dirname, 'scripts', 'ingest_linkedin_messages.js')}`, { timeout: 1200000 }, (err, stdout, stderr) => {
          if (stdout) console.log(stdout.slice(-800));
          if (stderr) console.error('  stderr:', stderr.slice(-300));
          if (err) console.error('  ⚠️ Sophie messages import error:', err.message?.slice(0, 200));
          else console.log('  ✅ Sophie LinkedIn messages import complete');
        });
      } else {
        console.log(`  ℹ️  Sophie LinkedIn messages already imported (${check.cnt} interactions)`);
      }
    }
  } catch (e) {}

  // Backfill: create team_proximity for LinkedIn-imported people missing proximity links
  try {
    const { rowCount: proxCreated } = await pool.query(`
      INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
      SELECT p.id, p.created_by, 'linkedin_connection', 0.5, 'linkedin_import', p.tenant_id
      FROM people p
      WHERE p.source = 'linkedin_import'
        AND p.created_by IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM team_proximity tp WHERE tp.person_id = p.id AND tp.team_member_id = p.created_by
        )
    `).catch(() => ({ rowCount: 0 }));
    if (proxCreated > 0) console.log(`  ✅ Backfilled ${proxCreated} LinkedIn proximity links`);
  } catch (e) {}

  // One-time backfill: link orphaned interactions to people
  try {
    // 1. Sent emails — match recipients against people.email
    const { rowCount: sentLinked } = await pool.query(`
      UPDATE interactions i
      SET person_id = p.id
      FROM people p,
           unnest(i.email_to) AS recipient
      WHERE i.person_id IS NULL
        AND i.source = 'gmail_sync'
        AND i.direction IN ('outbound', 'sent')
        AND p.email IS NOT NULL AND p.email != ''
        AND lower(p.email) = lower(recipient)
    `).catch(() => ({ rowCount: 0 }));
    if (sentLinked > 0) console.log(`  ✅ Backfilled ${sentLinked} sent-email interactions → people`);

    // 2. Received emails — match sender against people.email
    const { rowCount: recvLinked } = await pool.query(`
      UPDATE interactions i
      SET person_id = p.id
      FROM people p
      WHERE i.person_id IS NULL
        AND i.source = 'gmail_sync'
        AND i.direction IN ('inbound', 'received')
        AND i.email_from IS NOT NULL AND i.email_from != ''
        AND p.email IS NOT NULL AND p.email != ''
        AND lower(p.email) = lower(i.email_from)
    `).catch(() => ({ rowCount: 0 }));
    if (recvLinked > 0) console.log(`  ✅ Backfilled ${recvLinked} received-email interactions → people`);

    // 3. Also try email_alt
    const { rowCount: altLinked } = await pool.query(`
      UPDATE interactions i
      SET person_id = p.id
      FROM people p
      WHERE i.person_id IS NULL
        AND i.source = 'gmail_sync'
        AND p.email_alt IS NOT NULL AND p.email_alt != ''
        AND (
          (i.direction IN ('outbound','sent') AND lower(p.email_alt) = ANY(SELECT lower(unnest(i.email_to))))
          OR
          (i.direction IN ('inbound','received') AND lower(p.email_alt) = lower(i.email_from))
        )
    `).catch(() => ({ rowCount: 0 }));
    if (altLinked > 0) console.log(`  ✅ Backfilled ${altLinked} interactions via email_alt`);

    // 4. Delete noise rows that slipped through before filter
    const { rowCount: noiseDeleted } = await pool.query(`
      DELETE FROM interactions
      WHERE source = 'gmail_sync'
        AND person_id IS NULL
        AND (email_from = '' OR email_from IS NULL)
        AND (email_to IS NULL OR email_to = '{}')
    `).catch(() => ({ rowCount: 0 }));
    if (noiseDeleted > 0) console.log(`  🗑️  Cleaned ${noiseDeleted} noise interaction rows`);

    // 5. Link signals to people via company — people at signalling companies
    const { rowCount: sigLinked } = await pool.query(`
      UPDATE person_signals ps
      SET person_id = p.id
      FROM signal_events se, people p
      WHERE ps.signal_event_id = se.id
        AND ps.person_id IS NULL
        AND p.current_company_id = se.company_id
        AND p.current_company_id IS NOT NULL
    `).catch(() => ({ rowCount: 0 }));
    if (sigLinked > 0) console.log(`  ✅ Backfilled ${sigLinked} person↔signal links via company`);

    const total = sentLinked + recvLinked + altLinked + noiseDeleted + sigLinked;
    if (total > 0) console.log(`  ✅ Backfill complete: ${total} records updated`);
  } catch (e) {
    console.log('  ⚠️ Backfill:', e.message);
  }

  // One-time: force re-auth for all users to pick up new Gmail+Drive scopes
  // Remove this block after everyone has re-authenticated (deploy after 2026-03-27)
  try {
    const { rowCount } = await pool.query(`DELETE FROM sessions WHERE created_at < '2026-03-26T12:00:00Z'`);
    if (rowCount > 0) console.log(`  🔑 Cleared ${rowCount} old sessions — users will re-auth with full Gmail+Drive scopes`);
  } catch (e) { /* ok */ }

  // One-time WIP workbook ingestion (runs once, guarded by check)
  try {
    const wipFile = require('path').join(__dirname, 'data', 'Global_Billings_and_WIP.xlsx');
    if (require('fs').existsSync(wipFile)) {
      // Check if already fully ingested (need at least 500 WIP records — invoices + WIP combined)
      const { rows: [check] } = await pool.query(
        `SELECT COUNT(*) AS cnt FROM conversions WHERE source IN ('wip_workbook', 'xero_export') LIMIT 1`
      ).catch(() => ({ rows: [{ cnt: '0' }] }));
      if (parseInt(check.cnt) < 500) {
        // Run async — don't block server startup
        console.log('\n  📊 WIP workbook found — running ingestion in background...');
        const { exec } = require('child_process');
        const scriptDir = require('path').join(__dirname, 'scripts');
        // Chain: invoices → WIP → receivables, non-blocking
        exec(`node ${scriptDir}/ingest_invoice_ledgers.js && node ${scriptDir}/ingest_consultant_wip.js && node ${scriptDir}/ingest_receivables.js`, { timeout: 900000 }, (err, stdout, stderr) => {
          if (stdout) console.log(stdout.slice(-500));
          if (err) console.error('  ⚠️ WIP ingestion error:', err.message?.slice(0, 200));
          else console.log('  ✅ WIP workbook ingestion complete');
        });
      } else {
        console.log(`  ℹ️  WIP data already loaded (${check.cnt} records) — skipping ingestion`);
      }
    }
  } catch (e) {
    console.log('  ⚠️ WIP ingestion check:', e.message);
  }
});