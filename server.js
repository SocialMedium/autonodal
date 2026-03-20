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
// AUTH ROUTES
// ═══════════════════════════════════════════════════════════════════════════════

// Google OAuth — initiate
app.get('/api/auth/google', (req, res) => {
  if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });
  const redirectUri = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
  const returnTo = req.query.return_to || '/index.html';
  const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
    client_id: process.env.GOOGLE_CLIENT_ID,
    redirect_uri: redirectUri,
    response_type: 'code',
    scope: 'openid email profile',
    hd: 'mitchellake.com',
    prompt: 'select_account',
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
      pool.query('SELECT COUNT(*) AS cnt, COALESCE(SUM(placement_fee), 0) AS total_fees FROM conversions WHERE tenant_id = $1', [req.tenant_id]),
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
    let emoji = '🌤️';
    if (total > 0) {
      const ratio = growth / total;
      if (ratio > 0.7) { temperature = 'hot'; emoji = '🔥'; }
      else if (ratio > 0.55) { temperature = 'warm'; emoji = '☀️'; }
      else if (ratio < 0.3) { temperature = 'cold'; emoji = '❄️'; }
      else if (ratio < 0.45) { temperature = 'cooling'; emoji = '🌧️'; }
    }

    // Build narrative summary via simple template
    const typeLabels = { capital_raising: 'raising capital', product_launch: 'launching products', strategic_hiring: 'hiring aggressively', restructuring: 'restructuring', layoffs: 'cutting headcount', ma_activity: 'doing deals', geographic_expansion: 'expanding geographically', partnership: 'forming partnerships', leadership_change: 'changing leadership' };
    const topMoves = byType.slice(0, 3).map(t => {
      const cos = (t.companies || []).slice(0, 3).join(', ');
      return `${t.cnt} ${typeLabels[t.signal_type] || t.signal_type.replace(/_/g, ' ')} signals (${cos})`;
    });

    const summary = total === 0
      ? 'No significant macro signals this week.'
      : `${emoji} Market is ${temperature}. ${total} signals from major public companies this week: ${topMoves.join('; ')}.${contraction > 0 ? ' ' + contraction + ' contraction signals may release senior talent downstream.' : ''}`;

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
        id, title, source_name, source_url, published_at, image_url
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
        AND p.seniority_level IN ('C-level', 'VP', 'Director', 'Head')
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
        AND p.current_company_id = $2
        AND tp.relationship_strength >= 0.20
      GROUP BY p.id, p.full_name, p.current_title, p.current_company_name,
               ps.timing_score, ps.receptivity_score
      ORDER BY MAX(tp.relationship_strength) DESC
      LIMIT 12
    `, [tenantId, sig.company_id]);

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
                await pool.query('INSERT INTO people (full_name, email, current_company_id, current_company_name, source, tenant_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())',
                  [name, email, req.params.id, company.name, 'gmail_discovery', req.tenant_id]);
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
             (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id AND i.interaction_type = 'research_note') AS note_count,
             (SELECT MAX(i.interaction_at) FROM interactions i WHERE i.person_id = p.id) AS last_interaction,
             (SELECT MAX(tp.relationship_strength) FROM team_proximity tp WHERE tp.person_id = p.id) AS proximity_strength,
             (SELECT STRING_AGG(DISTINCT tp.relationship_type, ', ') FROM team_proximity tp WHERE tp.person_id = p.id) AS connection_types
      FROM people p WHERE p.current_company_id = $1 AND p.tenant_id = $2
      ORDER BY
        (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) DESC,
        (SELECT MAX(tp.relationship_strength) FROM team_proximity tp WHERE tp.person_id = p.id) DESC NULLS LAST,
        CASE WHEN p.seniority_level IN ('c_suite','vp','director') THEN 0 ELSE 1 END,
        p.full_name
      LIMIT 100
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

    res.json({ ...company, signals, people, placements, documents, financials, opportunities, pipeline_total: pipelineTotal });
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
          SELECT id, full_name, current_title, current_company_name, headline,
                 location, seniority_level, expertise_tags, industries, source,
                 email, linkedin_url
          FROM people WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        `, [pointIds, req.tenant_id]);

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
    }

    // Search companies
    if (collection === 'companies' || collection === 'all') {
      const compLimit = collection === 'all' ? Math.min(limit, 8) : limit;
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
                 (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) AS people_count
          FROM companies c WHERE c.id = ANY($1::uuid[]) AND c.tenant_id = $2
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
      const docLimit = collection === 'all' ? Math.min(limit, 10) : limit;
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
        const sigLimit = collection === 'all' ? Math.min(limit, 8) : limit;
        const qdrantResults = await qdrantSearch('signal_events', vector, sigLimit);
        if (qdrantResults.length > 0) {
          const sigIds = qdrantResults.map(r => r.payload?.signal_id).filter(Boolean);
          if (sigIds.length > 0) {
            const { rows: signals } = await pool.query(`
              SELECT se.id, se.signal_type, se.company_name, se.company_id, se.confidence_score,
                     se.evidence_summary, se.detected_at, c.sector, c.geography, c.is_client
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

    // Search conversions (placements)
    if (collection === 'placements' || collection === 'all') {
      try {
        const cvLimit = collection === 'all' ? Math.min(limit, 6) : limit;
        const qdrantResults = await qdrantSearch('conversions', vector, cvLimit);
        if (qdrantResults.length > 0) {
          const cvIds = qdrantResults.map(r => r.payload?.conversion_id).filter(Boolean);
          if (cvIds.length > 0) {
            const { rows: conversions } = await pool.query(`
              SELECT cv.id, cv.role_title, cv.placement_fee, cv.currency, cv.start_date,
                     p.full_name as person_name, a.name as account_name
              FROM conversions cv
              LEFT JOIN people p ON p.id = cv.person_id
              LEFT JOIN accounts a ON a.id = cv.client_id
              WHERE cv.id = ANY($1::uuid[]) AND cv.tenant_id = $2
            `, [cvIds, req.tenant_id]);
            const cvMap = new Map(conversions.map(c => [c.id, c]));
            results.placements = qdrantResults.map(r => {
              const cv = cvMap.get(r.payload?.conversion_id);
              if (!cv) return null;
              return { ...cv, match_score: Math.round(r.score * 100), score: r.score };
            }).filter(Boolean);
          }
        }
      } catch (e) { /* collection may not exist yet */ }
    }

    // Search interactions
    if (collection === 'interactions' || collection === 'all') {
      try {
        const intLimit = collection === 'all' ? Math.min(limit, 6) : limit;
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
             (results.placements?.length || 0) + (results.interactions?.length || 0),
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
        (SELECT COUNT(*) FROM signal_events WHERE tenant_id = $1 AND signals_embedded_at IS NOT NULL) AS signals,
        (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS conversions,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND embedded_at IS NOT NULL) AS interactions
    `, [req.tenant_id]);
    res.json({
      people: Number(counts.people), companies: Number(counts.companies),
      documents: Number(counts.documents), signals: Number(counts.signals),
      conversions: Number(counts.conversions), interactions: Number(counts.interactions),
      total: Number(counts.people) + Number(counts.companies) + Number(counts.documents) +
             Number(counts.signals) + Number(counts.conversions) + Number(counts.interactions)
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

    res.json({ wrap: wrapData, wrap_id: wrap?.id, top_grabs: topGrabs, week_of: new Date().toISOString().slice(0, 10) });
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

    // Region filter — uses shared REGION_MAP constant
    if (region && region !== 'all' && REGION_MAP[region]) {
      const geos = REGION_MAP[region];
      const orParts = [];
      geos.forEach(g => { idx++; orParts.push(`c.geography ILIKE $${idx}`); params.push(`%${g}%`); });
      geos.forEach(g => { idx++; orParts.push(`sd.company_name ILIKE $${idx}`); params.push(`%${g}%`); });
      geos.forEach(g => { idx++; orParts.push(`sd.signal_summary ILIKE $${idx}`); params.push(`%${g}%`); });
      geos.forEach(g => { idx++; orParts.push(`sd.opportunity_angle ILIKE $${idx}`); params.push(`%${g}%`); });
      where += ` AND (${orParts.join(' OR ')})`;
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
    res.json(rows[0]);
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
- When asked a cross-referencing question like "who do we know at X" — use run_sql_query immediately with a JOIN query. Do NOT chain multiple search tools.
- For ANY question involving cross-referencing two entity types (signals + people, companies + contacts, placements + clients) — use run_sql_query with a single JOIN query. This is ALWAYS faster and more accurate than chaining separate search tools.
- Present results as a clean formatted table or list. Not a wall of text.

QUERY PATTERNS (use these SQL patterns directly):
1. "Who do we know at companies with X signals?"
   → run_sql_query: SELECT DISTINCT p.full_name, p.current_title, p.current_company_name, se.signal_type, se.evidence_summary, se.detected_at, p.id FROM people p JOIN companies c ON c.id = p.current_company_id JOIN signal_events se ON se.company_id = c.id WHERE se.signal_type = 'capital_raising' AND se.detected_at > NOW() - INTERVAL '90 days' AND p.tenant_id = '<TENANT>' ORDER BY se.detected_at DESC

2. "What signals do we have for X company?"
   → run_sql_query: SELECT se.signal_type, se.evidence_summary, se.confidence_score, se.detected_at FROM signal_events se JOIN companies c ON c.id = se.company_id WHERE c.name ILIKE '%X%' AND se.tenant_id = '<TENANT>' ORDER BY se.detected_at DESC

3. "Who moved recently / flight risk?"
   → run_sql_query: SELECT p.full_name, p.current_title, p.current_company_name, ps.flight_risk_score, ps.timing_score FROM people p JOIN person_scores ps ON ps.person_id = p.id WHERE ps.flight_risk_score > 0.5 AND p.tenant_id = '<TENANT>' ORDER BY ps.flight_risk_score DESC LIMIT 20

4. "What placements have we done in X sector?"
   → run_sql_query: SELECT p.full_name, cv.role_title, a.name as client, cv.placement_fee, cv.start_date FROM conversions cv JOIN people p ON p.id = cv.person_id JOIN accounts a ON a.id = cv.client_id LEFT JOIN companies c ON c.id = a.company_id WHERE (c.sector ILIKE '%X%' OR cv.role_title ILIKE '%X%') AND cv.tenant_id = '<TENANT>' ORDER BY cv.start_date DESC

IMPORTANT: Replace <TENANT> with the actual tenant_id from context. The user's tenant_id is always provided in the conversation context.

CONTEXT:
- MitchelLake is a retained executive search firm (APAC, UK, global)
- Database: ~77K people, ~11K companies, ~22K documents, ~9K signals, ~500 placements
- Table names: people, companies, accounts, opportunities, conversions, engagements, pipeline_contacts, signal_events, interactions, team_proximity, external_documents, signal_dispatches, person_scores, person_signals
- Signal types: capital_raising, ma_activity, geographic_expansion, strategic_hiring, leadership_change, partnership, product_launch, layoffs, restructuring
- Key columns: people.current_company_id → companies.id, signal_events.company_id → companies.id, conversions.client_id → accounts.id, accounts.company_id → companies.id
- team_proximity links people to users (team members) via team_member_id with relationship_strength (0-1)

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
  { name: 'process_uploaded_file', description: 'Process uploaded CSV/PDF. Actions: preview, import_people, import_companies, extract_text, import_linkedin_connections (match LinkedIn connections against DB, create team_proximity records), import_linkedin_messages (store LinkedIn message history as interactions/intel).', input_schema: { type: 'object', properties: { file_id: { type: 'string' }, action: { type: 'string', enum: ['preview', 'import_people', 'import_companies', 'extract_text', 'import_linkedin_connections', 'import_linkedin_messages'] }, column_mapping: { type: 'object' } }, required: ['file_id', 'action'] } },
  { name: 'run_sql_query', description: 'PRIMARY TOOL — Run SQL (SELECT, UPDATE, INSERT, DELETE) against the database. Use this FIRST for any cross-referencing query. JOINs are fast. Always include tenant_id filter. Key tables: people, companies, signal_events, interactions, conversions, accounts, opportunities, team_proximity, person_scores.', input_schema: { type: 'object', properties: { query: { type: 'string', description: 'SQL query. Must include AND tenant_id = \'<tenant_id>\' for data tables.' }, explanation: { type: 'string', description: 'Brief one-line explanation of what this query does' } }, required: ['query', 'explanation'] } },
  { name: 'get_platform_stats', description: 'Current platform statistics.', input_schema: { type: 'object', properties: {} } },
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
          const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_company_name, source, tenant_id) VALUES ($1, $2, 'chat_intel', $3) RETURNING id`, [person_name, company_name || null, tenantId]);
          personId = np.id;
        }
        const { rows: [note] } = await pool.query(`INSERT INTO interactions (person_id, interaction_type, subject, summary, extracted_intelligence, source, interaction_at, tenant_id) VALUES ($1, 'research_note', $2, $3, $4, 'chat_concierge', NOW(), $5) RETURNING id`, [personId, subject, intelligence, JSON.stringify(extracted), tenantId]);
        return JSON.stringify({ success: true, person_id: personId, note_id: note.id, person_name, subject, extracted, message: `Saved on ${person_name}'s record` });
      }
      case 'create_person': {
        const { full_name, current_title, current_company_name, email, phone, location, linkedin_url, seniority_level } = input;
        const { rows: dupes } = await pool.query(`SELECT id, full_name, current_title FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 3`, [full_name, tenantId]);
        if (dupes.length) return JSON.stringify({ existing_matches: dupes, message: 'Possible duplicates found' });
        let coId = null;
        if (current_company_name) { const { rows } = await pool.query(`SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`, [current_company_name, tenantId]); if (rows.length) coId = rows[0].id; }
        const { rows: [p] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, current_company_id, email, phone, location, linkedin_url, seniority_level, source, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'chat_concierge',$10) RETURNING id, full_name`, [full_name, current_title||null, current_company_name||null, coId, email||null, phone||null, location||null, linkedin_url||null, seniority_level||null, tenantId]);
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
            await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, email, location, linkedin_url, source, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,'csv_import',$7)`,
              [name.trim(), row[m.current_title||'Title']||row['Job Title']||null, row[m.current_company_name||'Company']||row['Organization']||null, row[m.email||'Email']||null, row[m.location||'Location']||null, row[m.linkedin_url||'LinkedIn']||null, tenantId]);
            imported++;
          }
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
                  const { rows: [np] } = await pool.query(`INSERT INTO people (full_name, current_title, current_company_name, linkedin_url, email, source, tenant_id) VALUES ($1,$2,$3,$4,$5,'linkedin_import',$6) RETURNING id`, [fullName, position || null, company || null, linkedinUrl || null, email || null, tenantId]);
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
                  await pool.query(`INSERT INTO interactions (person_id, interaction_type, subject, summary, source, interaction_at, tenant_id) VALUES ($1, 'linkedin_message', $2, $3, 'linkedin_import', $4, $5) ON CONFLICT DO NOTHING`, [match.id, `LinkedIn conversation (${messages.length} messages)`, summary, latestDate ? new Date(latestDate).toISOString() : new Date().toISOString(), tenantId]);
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

  // Ensure network topology tables exist
  try {
    const fs = require('fs');
    const topoMigration = require('path').join(__dirname, 'sql', 'migration_network_topology.sql');
    if (fs.existsSync(topoMigration)) {
      await pool.query(fs.readFileSync(topoMigration, 'utf8'));
      console.log('  ✅ Network topology tables ready');
    }
  } catch (e) { /* tables may already exist */ }

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
});