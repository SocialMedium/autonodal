// ═══════════════════════════════════════════════════════════════════════════════
// routes/auth.js — Authentication & Google OAuth routes
// 8 routes: /api/auth/*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const https = require('https');
const { encryptToken } = require('../lib/crypto');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, auditLog, generateQueryEmbedding }) {

  // ─── Scope constants ──────────────────────────────────────────────────────

  const SIGNIN_SCOPES = 'openid email profile';
  const GMAIL_CONNECT_SCOPES = 'openid email profile https://www.googleapis.com/auth/gmail.readonly';
  // Drive / Docs / Sheets / Slides scopes are intentionally NOT included in the default bundle.
  // They will be re-added via a scope-amendment submission to Google OAuth verification once the
  // Drive ingestion path ships with a per-file picker (`drive.file`) rather than broad readonly.
  const DRIVE_CONNECT_SCOPES = 'openid email profile https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/documents.readonly https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/presentations.readonly';
  const GOOGLE_CONNECT_SCOPES = 'openid email profile https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/contacts.readonly https://www.googleapis.com/auth/calendar.readonly';

  // ═══════════════════════════════════════════════════════════════════════════════
  // 1. GET /api/auth/google — initiate Google OAuth
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/google', (req, res) => {
    if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });
    const redirectUri = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
    const returnTo = req.query.return_to || '/index.html';
    // Drive scope override disabled pending re-verification. Previously allowed via ?scope=drive.
    // To re-enable: amend OAuth consent screen with drive.file (preferred) or drive.readonly, then
    // restore the conditional below after Google approves the scope amendment.
    const scope = GOOGLE_CONNECT_SCOPES;
    const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
      client_id: process.env.GOOGLE_CLIENT_ID,
      redirect_uri: redirectUri,
      response_type: 'code',
      scope: scope,
      access_type: 'offline',
      prompt: 'consent',
      include_granted_scopes: 'true',
      state: returnTo
    });
    res.redirect(authUrl);
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 2. GET /api/auth/google/callback — Google OAuth callback
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/google/callback', async (req, res) => {
    console.log('🔑 OAuth callback hit — code:', req.query.code ? 'yes(' + req.query.code.length + ')' : 'no', 'error:', req.query.error || 'none', 'state:', req.query.state);
    const { code, error, state } = req.query;
    const returnTo = (state && state.startsWith('/')) ? state : '/index.html';
    if (error) return res.redirect(returnTo + '?auth_error=' + encodeURIComponent(error));
    if (!code) return res.redirect(returnTo + '?auth_error=no_code');

    try {
      const redirectUri = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || `${req.protocol}://${req.get('host')}/api/auth/google/callback`;
      console.log('🔑 Step 1: exchanging code, redirectUri:', redirectUri);

      let tokenRes;
      try {
        tokenRes = await fetch('https://oauth2.googleapis.com/token', {
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
      } catch (fetchErr) {
        console.error('🔑 FETCH FAILED:', fetchErr.message);
        return res.redirect('/index.html?auth_error=token_exchange_failed');
      }

      console.log('🔑 Step 2: token response status:', tokenRes.status);

      if (!tokenRes.ok) {
        const err = await tokenRes.text();
        console.error('🔑 Token exchange failed:', err);
        return res.redirect('/index.html?auth_error=token_exchange_failed');
      }

      const tokenData = await tokenRes.json();
      console.log('🔑 Step 3: got tokens, has access_token:', !!tokenData.access_token);

      // Get user info
      const userInfoRes = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
        headers: { Authorization: `Bearer ${tokenData.access_token}` }
      });
      const userInfo = await userInfoRes.json();
      console.log('🔑 Step 4: userInfo email:', userInfo.email);

      if (!userInfo.email) {
        return res.redirect('/index.html?auth_error=no_email');
      }

      // Find or create user
      let { rows } = await platformPool.query('SELECT id, email, name, role FROM users WHERE email = $1', [userInfo.email]);
      console.log('🔑 Step 5: user found:', rows.length > 0, rows[0]?.email);
      let user;

      if (rows.length > 0) {
        user = rows[0];
        // Update name/avatar if changed
        await platformPool.query(
          'UPDATE users SET name = COALESCE(NULLIF($1, \'\'), name), updated_at = NOW() WHERE id = $2',
          [userInfo.name, user.id]
        );
      } else {
        // New user — determine tenant assignment
        const displayName = userInfo.name || userInfo.email.split('@')[0];

        if (userInfo.email.endsWith('@mitchellake.com')) {
          // MitchelLake domain → assign to MitchelLake tenant
          const { rows: [newUser] } = await platformPool.query(
            `INSERT INTO users (id, email, name, role, password_hash, tenant_id, created_at, updated_at)
             VALUES (gen_random_uuid(), $1, $2, 'consultant', 'oauth_google', $3, NOW(), NOW())
             RETURNING id, email, name, role`,
            [userInfo.email, displayName, process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001']
          );
          user = newUser;
          console.log(`✅ New MitchelLake team member: ${user.name} (${user.email})`);
        } else {
          // Check for tenant invite — join existing tenant instead of creating new one
        var tenantInvite = null;
        try {
          var { rows: [inv] } = await platformPool.query(
            "SELECT ti.*, t.name AS tenant_name FROM tenant_invites ti JOIN tenants t ON t.id = ti.tenant_id WHERE ti.email = $1 AND ti.status = 'pending' AND ti.expires_at > NOW() ORDER BY ti.created_at DESC LIMIT 1",
            [userInfo.email]
          );
          if (inv) tenantInvite = inv;
        } catch (e) {}

        if (tenantInvite) {
          // Invited user → join existing tenant
          const { rows: [newUser] } = await platformPool.query(
            `INSERT INTO users (id, email, name, role, password_hash, tenant_id, created_at, updated_at)
             VALUES (gen_random_uuid(), $1, $2, $3, 'oauth_google', $4, NOW(), NOW())
             RETURNING id, email, name, role`,
            [userInfo.email, displayName, tenantInvite.role || 'viewer', tenantInvite.tenant_id]
          );
          user = newUser;
          await platformPool.query("UPDATE tenant_invites SET status = 'accepted', accepted_at = NOW() WHERE id = $1", [tenantInvite.id]);
          console.log('✅ Invited user joined tenant ' + tenantInvite.tenant_name + ': ' + user.name + ' (' + user.email + ')');
        } else {
        // External user → auto-provision a new tenant
          const slug = userInfo.email.split('@')[0]
            .toLowerCase().replace(/[^a-z0-9]/g, '-').slice(0, 30) + '-' + Date.now().toString(36);

          const { rows: [newTenant] } = await platformPool.query(
            `INSERT INTO tenants (id, name, slug, vertical, plan, tenant_type, onboarding_status, subscription_status, profile, created_at)
             VALUES (gen_random_uuid(), $1, $2, 'revenue', 'free', 'individual', 'step_1', 'free', $3, NOW())
             RETURNING id`,
            [displayName, slug, JSON.stringify({ provisioned_from: 'google_oauth', email: userInfo.email })]
          );

          const { rows: [newUser] } = await platformPool.query(
            `INSERT INTO users (id, email, name, role, password_hash, tenant_id, created_at, updated_at)
             VALUES (gen_random_uuid(), $1, $2, 'admin', 'oauth_google', $3, NOW(), NOW())
             RETURNING id, email, name, role`,
            [userInfo.email, displayName, newTenant.id]
          );
          user = newUser;
          console.log(`✅ New tenant provisioned: ${displayName} (${slug}) for ${userInfo.email}`);
        }
        } // close tenantInvite else
        auditLog(user.id, 'user_registered', 'user', user.id, { name: user.name || displayName, email: user.email });
      }

      // Create session
      console.log('🔑 Step 6: creating session for user:', user.id);
      const sessionToken = crypto.randomBytes(48).toString('hex');
      await platformPool.query(
        `INSERT INTO sessions (id, user_id, token, expires_at, created_at)
         VALUES (gen_random_uuid(), $1, $2, NOW() + INTERVAL '30 days', NOW())`,
        [user.id, sessionToken]
      );
      console.log('🔑 Step 7: session created');

      // Audit: login success
      try { const { audit: _audit } = require('../lib/auditLogger'); _audit.loginSuccess(req, user.id, user.email); } catch(e) {}

      // Clean up expired sessions
      await platformPool.query('DELETE FROM sessions WHERE expires_at < NOW()');

      // Auto-connect Google account on every sign-in
      // Store the ACTUAL granted scopes from Google (tokenData.scope) — preserves prior Drive grants
      if (tokenData.access_token) {
        const grantedScopes = (tokenData.scope || GOOGLE_CONNECT_SCOPES).split(' ').filter(Boolean);
        await platformPool.query(`
          INSERT INTO user_google_accounts (user_id, google_email, access_token, refresh_token, token_expires_at, scopes, sync_enabled, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), NOW())
          ON CONFLICT (user_id, google_email) DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = COALESCE(EXCLUDED.refresh_token, user_google_accounts.refresh_token),
            token_expires_at = EXCLUDED.token_expires_at,
            -- Merge granted scopes with any previously-stored scopes (union)
            scopes = (
              SELECT ARRAY(SELECT DISTINCT unnest(COALESCE(user_google_accounts.scopes, ARRAY[]::text[]) || EXCLUDED.scopes))
            ),
            sync_enabled = true,
            updated_at = NOW()
        `, [
          user.id,
          userInfo.email,
          encryptToken(tokenData.access_token),
          tokenData.refresh_token ? encryptToken(tokenData.refresh_token) : null,
          tokenData.expires_in ? new Date(Date.now() + tokenData.expires_in * 1000) : null,
          grantedScopes
        ]).catch((e) => console.error('Google account upsert:', e.message));
      }

      // Check onboarding status — new users need onboarding
      const { rows: [tenantStatus] } = await platformPool.query(
        'SELECT onboarding_status FROM tenants WHERE id = (SELECT tenant_id FROM users WHERE id = $1)',
        [user.id]
      );
      const userJson = encodeURIComponent(JSON.stringify({ id: user.id, email: user.email, name: user.name, role: user.role }));

      console.log('🔑 Step 8: onboarding_status:', tenantStatus?.onboarding_status);

      if (tenantStatus && tenantStatus.onboarding_status && tenantStatus.onboarding_status !== 'complete') {
        const step = tenantStatus.onboarding_status || 'step_1';
        console.log('🔑 Step 9: redirecting to onboarding, step:', step);
        return res.redirect(`/onboarding.html?step=${step}&token=${sessionToken}&user=${userJson}`);
      }

      // Onboarding complete → go to dashboard
      const sep = returnTo.includes('?') ? '&' : '?';
      console.log('🔑 Step 9: redirecting to:', returnTo);
      res.redirect(`${returnTo}${sep}token=${sessionToken}&user=${userJson}`);
    } catch (err) {
      console.error('🔑 CATCH — Google auth error:', err.message, err.stack?.split('\n')[1]);
      res.redirect(returnTo + '?auth_error=server_error');
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 3. GET /api/auth/me
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/me', authenticateToken, async (req, res) => {
    console.log('🔑 /api/auth/me hit for:', req.user?.email);
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows: [user] } = await db.query(
        'SELECT id, email, name, role, region, onboarded, preferences FROM users WHERE id = $1',
        [req.user.user_id]
      );
      const { rows: [tenantData] } = await db.query(
        'SELECT name, slug, signal_dial, profile, logo_url, primary_color FROM tenants WHERE id = $1', [req.tenant_id]
      ).catch(() => ({ rows: [null] }));
      res.json({ user: { ...req.user, region: user?.region, onboarded: user?.onboarded, preferences: user?.preferences }, signal_dial: tenantData?.signal_dial, profile: tenantData?.profile, tenant: { name: tenantData?.name, slug: tenantData?.slug, logo_url: tenantData?.logo_url, primary_color: tenantData?.primary_color } });
    } catch (e) {
      res.json({ user: req.user });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 4. PATCH /api/auth/me — update user profile
  // ═══════════════════════════════════════════════════════════════════════════════

  router.patch('/api/auth/me', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
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
      await db.query(`UPDATE users SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1`, params);
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 5. POST /api/auth/logout
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/auth/logout', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const token = req.headers.authorization.replace('Bearer ', '');
      await db.query('DELETE FROM sessions WHERE token = $1', [token]);
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: 'Logout failed' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 5b. POST /api/auth/google/disconnect — revoke Google, schedule data purge
  // Required for Google Limited Use compliance — user-initiated disconnect triggers
  // token revocation at Google + 30-day data deletion window.
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/auth/google/disconnect', authenticateToken, async (req, res) => {
    try {
      const userId = req.user.user_id;
      const { rows: accounts } = await platformPool.query(
        'SELECT id, google_email, access_token, refresh_token FROM user_google_accounts WHERE user_id = $1',
        [userId]
      );

      // Revoke each token at Google (best-effort — the revoke API is rate-limited but idempotent)
      for (const acct of accounts) {
        const tokenToRevoke = acct.refresh_token
          ? require('../lib/crypto').decryptToken(acct.refresh_token)
          : require('../lib/crypto').decryptToken(acct.access_token);
        if (!tokenToRevoke) continue;
        try {
          await fetch('https://oauth2.googleapis.com/revoke?token=' + encodeURIComponent(tokenToRevoke), { method: 'POST' });
        } catch (e) { /* non-fatal — we still delete locally */ }
      }

      // Enqueue for 30-day purge of all Google-derived data belonging to this user
      for (const acct of accounts) {
        await platformPool.query(
          `INSERT INTO google_disconnect_queue (user_id, google_email, disconnected_at, purge_after)
           VALUES ($1, $2, NOW(), NOW() + INTERVAL '30 days')`,
          [userId, acct.google_email]
        );
      }

      // Immediately delete the token row (access revoked — no further sync)
      await platformPool.query('DELETE FROM user_google_accounts WHERE user_id = $1', [userId]);

      auditLog(userId, 'google_disconnected', 'user', userId, { accounts: accounts.length });
      res.json({ ok: true, disconnected: accounts.length, purge_in_days: 30 });
    } catch (err) {
      console.error('Google disconnect error:', err.message);
      res.status(500).json({ error: 'Disconnect failed' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 6. GET /api/auth/gmail/connect — initiate Gmail/Contacts/Calendar connect
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/gmail/connect', async (req, res) => {
    if (!process.env.GOOGLE_CLIENT_ID) return res.status(500).json({ error: 'Google OAuth not configured' });

    const token = req.query.token || (req.headers.authorization && req.headers.authorization.replace('Bearer ', ''));
    if (!token) return res.redirect('/index.html?auth_error=login_required');

    const { rows } = await platformPool.query(
      'SELECT s.user_id FROM sessions s WHERE s.token = $1 AND s.expires_at > NOW()', [token]
    );
    if (rows.length === 0) return res.redirect('/index.html?auth_error=session_expired');

    const redirectUri = process.env.GOOGLE_GMAIL_REDIRECT_URL || `${req.protocol}://${req.get('host')}/api/auth/gmail/callback`;
    const state = Buffer.from(JSON.stringify({
      userId: rows[0].user_id,
      token: token,
      returnTo: req.query.return_to || '/index.html'
    })).toString('base64');

    const promptMode = req.query.prompt === 'select_account' ? 'select_account consent' : 'consent';
    const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
      client_id: process.env.GOOGLE_CLIENT_ID,
      redirect_uri: redirectUri,
      response_type: 'code',
      scope: GOOGLE_CONNECT_SCOPES,
      access_type: 'offline',
      prompt: promptMode,
      state
    });
    res.redirect(authUrl);
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 7. GET /api/auth/gmail/callback — Gmail OAuth callback + Drive auto-ingest
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/gmail/callback', async (req, res) => {
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

      // Store in user_google_accounts — use actual granted scopes from Google, merge with any existing
      const grantedScopes = (tokens.scope || GOOGLE_CONNECT_SCOPES).split(' ').filter(Boolean);
      await platformPool.query(`
        INSERT INTO user_google_accounts (user_id, google_email, access_token, refresh_token, token_expires_at, scopes, sync_enabled, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), NOW())
        ON CONFLICT (user_id, google_email) DO UPDATE SET
          access_token = EXCLUDED.access_token,
          refresh_token = COALESCE(EXCLUDED.refresh_token, user_google_accounts.refresh_token),
          token_expires_at = EXCLUDED.token_expires_at,
          scopes = (
            SELECT ARRAY(SELECT DISTINCT unnest(COALESCE(user_google_accounts.scopes, ARRAY[]::text[]) || EXCLUDED.scopes))
          ),
          sync_enabled = true,
          updated_at = NOW()
      `, [
        stateData.userId,
        userInfo.email,
        tokens.access_token,
        tokens.refresh_token || null,
        tokens.expires_in ? new Date(Date.now() + tokens.expires_in * 1000) : null,
        grantedScopes
      ]);

      console.log(`✅ Google connected (Gmail + Contacts + Calendar): ${userInfo.email} for user ${stateData.userId}`);

      // Trigger async Drive ingestion in background
      setImmediate(async () => {
        try {
          console.log(`📁 Auto-ingesting Drive files for ${userInfo.email}...`);
          const freshToken = tokens.access_token;
          const userId = stateData.userId;
          // BUG FLAG: original code used bare ML_TENANT_ID (undefined variable), using process.env instead
          const tenantId = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

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
                const { rows: existing } = await platformPool.query('SELECT id FROM external_documents WHERE source_url_hash = $1', [hash]);
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

                await platformPool.query(
                  `INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash, tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'context_only', NOW()) ON CONFLICT (source_url_hash, tenant_id) DO NOTHING`,
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

      // If session token is in state (from login flow redirect), pass it through for frontend auth
      let redirectUrl = returnTo;
      const sep = returnTo.includes('?') ? '&' : '?';
      if (stateData.token) {
        const { rows: [sessionUser] } = await platformPool.query(
          'SELECT u.id, u.email, u.name, u.role FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = $1 AND s.expires_at > NOW()',
          [stateData.token]
        );
        if (sessionUser) {
          redirectUrl = `${returnTo}${sep}token=${stateData.token}&user=${encodeURIComponent(JSON.stringify({ id: sessionUser.id, email: sessionUser.email, name: sessionUser.name, role: sessionUser.role }))}&gmail=connected`;
        } else {
          redirectUrl = `${returnTo}${sep}gmail=connected`;
        }
      } else {
        redirectUrl = `${returnTo}${sep}gmail=connected`;
      }
      res.redirect(redirectUrl);
    } catch (err) {
      console.error('Gmail connect error:', err);
      res.redirect(returnTo + '?gmail_error=server_error');
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 8. GET /api/auth/gmail/status — check Gmail connection status
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/auth/gmail/status', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(
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
  // LINKEDIN OIDC SIGN-IN — shared lib/auth-oidc module
  // ═══════════════════════════════════════════════════════════════════════════════

  const oidc = require('../lib/auth-oidc');

  // Helper: provision a new user + tenant from an OIDC identity.
  // Mirrors the tenant-assignment logic in the existing Google flow.
  async function provisionUserFromIdentity({ email, name, picture, provider }) {
    const displayName = name || email.split('@')[0];
    const lowerEmail = email.toLowerCase();
    const isMlDomain = lowerEmail.endsWith('@mitchellake.com');

    // Check for pending tenant invite
    let tenantInvite = null;
    if (!isMlDomain) {
      try {
        const { rows: [inv] } = await platformPool.query(
          "SELECT ti.*, t.name AS tenant_name FROM tenant_invites ti JOIN tenants t ON t.id = ti.tenant_id WHERE ti.email = $1 AND ti.status = 'pending' AND ti.expires_at > NOW() ORDER BY ti.created_at DESC LIMIT 1",
          [lowerEmail]
        );
        if (inv) tenantInvite = inv;
      } catch (e) { /* non-fatal */ }
    }

    let tenantId;
    let role = 'admin';

    if (isMlDomain) {
      tenantId = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
      role = 'consultant';
    } else if (tenantInvite) {
      tenantId = tenantInvite.tenant_id;
      role = tenantInvite.role || 'viewer';
    } else {
      // Auto-provision new individual tenant
      const slug = lowerEmail.split('@')[0]
        .toLowerCase().replace(/[^a-z0-9]/g, '-').slice(0, 30) + '-' + Date.now().toString(36);
      const { rows: [newTenant] } = await platformPool.query(
        `INSERT INTO tenants (id, name, slug, vertical, plan, tenant_type, onboarding_status, subscription_status, profile, created_at)
         VALUES (gen_random_uuid(), $1, $2, 'revenue', 'free', 'individual', 'step_1', 'free', $3, NOW())
         RETURNING id`,
        [displayName, slug, JSON.stringify({ provisioned_from: 'oidc_' + provider, email: lowerEmail })]
      );
      tenantId = newTenant.id;
    }

    const passwordHashPlaceholder = 'oauth_' + provider;
    const { rows: [newUser] } = await platformPool.query(
      `INSERT INTO users (id, email, name, role, password_hash, tenant_id, email_verified, created_at, updated_at)
       VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, TRUE, NOW(), NOW())
       RETURNING id, email, name, role, tenant_id`,
      [lowerEmail, displayName, role, passwordHashPlaceholder, tenantId]
    );

    if (tenantInvite) {
      await platformPool.query("UPDATE tenant_invites SET status = 'accepted', accepted_at = NOW() WHERE id = $1", [tenantInvite.id]);
    }

    auditLog(newUser.id, 'user_registered', 'user', newUser.id, {
      name: newUser.name, email: newUser.email, provider,
    });
    console.log(`✅ New user provisioned via ${provider}: ${newUser.name} (${newUser.email}) → tenant ${tenantId}`);
    return newUser;
  }

  // GET /api/auth/linkedin — initiate LinkedIn OIDC flow
  router.get('/api/auth/linkedin', (req, res) => {
    if (!process.env.LINKEDIN_CLIENT_ID) {
      return res.status(500).json({ error: 'LinkedIn OAuth not configured' });
    }
    try {
      const provider = oidc.getProvider('linkedin');
      const redirectUri = process.env.LINKEDIN_REDIRECT_URI ||
        `${req.protocol}://${req.get('host')}/api/auth/linkedin/callback`;
      process.env.LINKEDIN_REDIRECT_URI = redirectUri;
      const returnTo = req.query.return_to || '/index.html';
      const { authorizeUrl } = oidc.startFlow(provider, { returnTo });
      res.redirect(authorizeUrl);
    } catch (err) {
      console.error('LinkedIn start error:', err.message);
      res.redirect('/index.html?auth_error=' + encodeURIComponent(err.code || 'linkedin_start_failed'));
    }
  });

  // GET /api/auth/linkedin/callback — handle LinkedIn OIDC callback
  router.get('/api/auth/linkedin/callback', async (req, res) => {
    try {
      if (req.query.error) {
        const code = req.query.error === 'user_cancelled_login' ? 'user_cancelled' : 'linkedin_denied';
        return res.redirect('/index.html?auth_error=' + code);
      }
      if (!req.query.code) return res.redirect('/index.html?auth_error=no_code');

      // Validate state + retrieve PKCE verifier
      const entry = oidc.validateCallback(req.query.state);
      const provider = oidc.getProvider('linkedin');
      const redirectUri = process.env.LINKEDIN_REDIRECT_URI ||
        `${req.protocol}://${req.get('host')}/api/auth/linkedin/callback`;

      // Exchange code for tokens
      const tokens = await oidc.exchangeCode(provider, req.query.code, entry.codeVerifier, redirectUri);

      // Fetch userinfo
      const identity = await oidc.fetchUserinfo(provider, tokens.accessToken);

      // Reconcile with local user DB
      const { user, isNew } = await oidc.reconcileIdentity(platformPool, identity, {
        createUser: provisionUserFromIdentity,
      });

      // Create session (same mechanism as existing Google flow)
      const sessionToken = crypto.randomBytes(48).toString('hex');
      await platformPool.query(
        `INSERT INTO sessions (id, user_id, token, expires_at, created_at)
         VALUES (gen_random_uuid(), $1, $2, NOW() + INTERVAL '30 days', NOW())`,
        [user.id, sessionToken]
      );
      try { require('../lib/auditLogger').audit.loginSuccess(req, user.id, user.email); } catch(e) {}
      await platformPool.query('DELETE FROM sessions WHERE expires_at < NOW()');

      // Route: new LinkedIn sign-ups → continuity screen; returning users → return_to
      const returnTo = entry.returnTo || '/index.html';
      if (isNew) {
        res.redirect(`/onboarding/linkedin-continue?token=${sessionToken}&return_to=${encodeURIComponent(returnTo)}`);
      } else {
        const sep = returnTo.includes('?') ? '&' : '?';
        res.redirect(`${returnTo}${sep}token=${sessionToken}`);
      }
    } catch (err) {
      console.error('LinkedIn callback error:', err.code || 'unknown', err.message);
      const code = err.code || 'oauth_failed';
      res.redirect('/index.html?auth_error=' + encodeURIComponent(code));
    }
  });

  // GET /api/auth/providers — list linked identity providers for current user
  router.get('/api/auth/providers', authenticateToken, async (req, res) => {
    try {
      const providers = await oidc.getLinkedProviders(platformPool, req.user.user_id);
      res.json({ providers });
    } catch (err) {
      res.status(500).json({ error: 'Failed to fetch providers' });
    }
  });

  // POST /api/auth/:provider/unlink — remove a provider link (not the last)
  router.post('/api/auth/:provider/unlink', authenticateToken, async (req, res) => {
    try {
      const result = await oidc.unlinkProvider(platformPool, req.user.user_id, req.params.provider);
      res.json(result);
    } catch (err) {
      res.status(400).json({ error: err.message, code: err.code });
    }
  });

  return router;
};
