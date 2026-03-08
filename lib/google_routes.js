// Google OAuth Routes - Add to server.js
// Copy these routes into your server.js file

const googleLib = require('./lib/google');

// =====================================================
// GOOGLE OAUTH ROUTES
// =====================================================

// Start OAuth flow - redirect user to Google
app.get('/api/auth/google', authenticateToken, (req, res) => {
  const state = Buffer.from(JSON.stringify({ 
    userId: req.user.id,
    returnUrl: req.query.returnUrl || '/my.html'
  })).toString('base64');
  
  const authUrl = googleLib.getAuthUrl(state);
  res.redirect(authUrl);
});

// OAuth callback - Google redirects here
app.get('/api/auth/google/callback', async (req, res) => {
  try {
    const { code, state } = req.query;
    
    if (!code) {
      return res.redirect('/my.html?error=no_code');
    }
    
    // Decode state
    let stateData = {};
    if (state) {
      try {
        stateData = JSON.parse(Buffer.from(state, 'base64').toString('utf-8'));
      } catch (e) {
        console.error('Invalid state:', e);
      }
    }
    
    // Exchange code for tokens
    const tokens = await googleLib.getTokensFromCode(code);
    
    // Get user's Google email
    const googleEmail = await googleLib.getUserEmail(tokens.access_token);
    
    // Store in database
    await pool.query(`
      INSERT INTO user_google_accounts (user_id, google_email, access_token, refresh_token, token_expires_at, scopes)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (user_id, google_email) DO UPDATE SET
        access_token = EXCLUDED.access_token,
        refresh_token = COALESCE(EXCLUDED.refresh_token, user_google_accounts.refresh_token),
        token_expires_at = EXCLUDED.token_expires_at,
        scopes = EXCLUDED.scopes,
        updated_at = NOW()
    `, [
      stateData.userId,
      googleEmail,
      tokens.access_token,
      tokens.refresh_token,
      tokens.expiry_date ? new Date(tokens.expiry_date) : null,
      googleLib.SCOPES
    ]);
    
    console.log(`Google account connected: ${googleEmail} for user ${stateData.userId}`);
    
    // Redirect back
    const returnUrl = stateData.returnUrl || '/my.html';
    res.redirect(`${returnUrl}?google=connected`);
    
  } catch (error) {
    console.error('Google OAuth error:', error);
    res.redirect('/my.html?error=google_auth_failed');
  }
});

// Disconnect Google account
app.delete('/api/auth/google/:accountId', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(
      'DELETE FROM user_google_accounts WHERE id = $1 AND user_id = $2 RETURNING *',
      [req.params.accountId, req.user.id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Account not found' });
    }
    
    res.json({ success: true });
  } catch (error) {
    console.error('Disconnect error:', error);
    res.status(500).json({ error: error.message });
  }
});

// List connected Google accounts
app.get('/api/auth/google/accounts', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, google_email, sync_enabled, last_sync_at, created_at
      FROM user_google_accounts
      WHERE user_id = $1
      ORDER BY created_at DESC
    `, [req.user.id]);
    
    res.json(result.rows);
  } catch (error) {
    console.error('List accounts error:', error);
    res.status(500).json({ error: error.message });
  }
});

// =====================================================
// GMAIL SYNC ROUTES
// =====================================================

// Trigger email sync for current user
app.post('/api/gmail/sync', authenticateToken, async (req, res) => {
  try {
    // Get user's Google account
    const accountResult = await pool.query(
      'SELECT * FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true LIMIT 1',
      [req.user.id]
    );
    
    if (accountResult.rows.length === 0) {
      return res.status(400).json({ error: 'No Google account connected' });
    }
    
    const account = accountResult.rows[0];
    
    // Check if token needs refresh
    if (account.token_expires_at && new Date(account.token_expires_at) < new Date()) {
      const newTokens = await googleLib.refreshAccessToken(account.refresh_token);
      await pool.query(
        'UPDATE user_google_accounts SET access_token = $1, token_expires_at = $2 WHERE id = $3',
        [newTokens.access_token, new Date(newTokens.expiry_date), account.id]
      );
      account.access_token = newTokens.access_token;
    }
    
    // Get all candidate emails from database
    const candidatesResult = await pool.query(
      "SELECT DISTINCT LOWER(email) as email FROM people WHERE email IS NOT NULL AND email != ''"
    );
    const candidateEmails = candidatesResult.rows.map(r => r.email);
    
    if (candidateEmails.length === 0) {
      return res.json({ message: 'No candidate emails to sync', stats: { processed: 0 } });
    }
    
    // Create db helper for sync function
    const db = {
      query: async (sql, params) => {
        const result = await pool.query(sql, params);
        return result.rows;
      },
      queryOne: async (sql, params) => {
        const result = await pool.query(sql, params);
        return result.rows[0];
      }
    };
    
    // Run sync
    const stats = await googleLib.syncCandidateEmails(db, account, candidateEmails);
    
    res.json({ success: true, stats });
    
  } catch (error) {
    console.error('Gmail sync error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get sync status
app.get('/api/gmail/status', authenticateToken, async (req, res) => {
  try {
    const accountResult = await pool.query(`
      SELECT 
        uga.id,
        uga.google_email,
        uga.sync_enabled,
        uga.last_sync_at,
        (SELECT COUNT(*) FROM email_signals WHERE user_id = uga.user_id) as signals_count,
        (SELECT COUNT(*) FROM interactions WHERE owner_user_id = uga.user_id AND type = 'email') as emails_count
      FROM user_google_accounts uga
      WHERE uga.user_id = $1
    `, [req.user.id]);
    
    res.json(accountResult.rows[0] || { connected: false });
    
  } catch (error) {
    console.error('Status error:', error);
    res.status(500).json({ error: error.message });
  }
});

// =====================================================
// INTERACTION VISIBILITY ROUTES
// =====================================================

// Update interaction visibility (mark private/company)
app.patch('/api/interactions/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const { visibility } = req.body; // 'private', 'team', 'company'
    
    if (!['private', 'team', 'company'].includes(visibility)) {
      return res.status(400).json({ error: 'Invalid visibility' });
    }
    
    const result = await pool.query(`
      UPDATE interactions 
      SET visibility = $1,
          marked_private_at = CASE WHEN $1 = 'private' THEN NOW() ELSE NULL END,
          marked_private_by = CASE WHEN $1 = 'private' THEN $2 ELSE NULL END
      WHERE id = $3 AND owner_user_id = $2
      RETURNING *
    `, [visibility, req.user.id, req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Interaction not found or not owned by you' });
    }
    
    res.json(result.rows[0]);
    
  } catch (error) {
    console.error('Visibility update error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Batch update visibility
app.patch('/api/interactions/visibility', authenticateToken, async (req, res) => {
  try {
    const { ids, visibility } = req.body;
    
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'ids array required' });
    }
    
    if (!['private', 'team', 'company'].includes(visibility)) {
      return res.status(400).json({ error: 'Invalid visibility' });
    }
    
    const result = await pool.query(`
      UPDATE interactions 
      SET visibility = $1,
          marked_private_at = CASE WHEN $1 = 'private' THEN NOW() ELSE NULL END,
          marked_private_by = CASE WHEN $1 = 'private' THEN $2 ELSE NULL END
      WHERE id = ANY($3) AND owner_user_id = $2
      RETURNING id
    `, [visibility, req.user.id, ids]);
    
    res.json({ updated: result.rows.length });
    
  } catch (error) {
    console.error('Batch visibility error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get person interactions with visibility filtering
app.get('/api/people/:id/interactions', authenticateToken, async (req, res) => {
  try {
    const personId = req.params.id;
    const userId = req.user.id;
    
    // Get interactions - hide content if private and not owner
    const result = await pool.query(`
      SELECT 
        i.id,
        i.type,
        i.direction,
        i.occurred_at,
        i.visibility,
        i.owner_user_id,
        u.full_name as owner_name,
        CASE 
          WHEN i.visibility = 'private' AND i.owner_user_id != $2 THEN NULL
          ELSE i.content 
        END as content,
        CASE 
          WHEN i.visibility = 'private' AND i.owner_user_id != $2 THEN '[Private]'
          ELSE i.email_subject 
        END as email_subject,
        CASE 
          WHEN i.visibility = 'private' AND i.owner_user_id != $2 THEN NULL
          ELSE i.email_snippet 
        END as email_snippet,
        i.email_from,
        i.email_has_attachments,
        i.created_at
      FROM interactions i
      LEFT JOIN users u ON i.owner_user_id = u.id
      WHERE i.person_id = $1
      ORDER BY i.occurred_at DESC
      LIMIT 100
    `, [personId, userId]);
    
    res.json(result.rows);
    
  } catch (error) {
    console.error('Get interactions error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get person engagement signals (always visible)
app.get('/api/people/:id/engagement', authenticateToken, async (req, res) => {
  try {
    const personId = req.params.id;
    
    // Get aggregated engagement data (no content, just signals)
    const result = await pool.query(`
      SELECT 
        COUNT(DISTINCT es.user_id) as ml_contacts,
        COUNT(CASE WHEN es.direction = 'inbound' THEN 1 END) as emails_received,
        COUNT(CASE WHEN es.direction = 'outbound' THEN 1 END) as emails_sent,
        MAX(CASE WHEN es.direction = 'inbound' THEN es.email_date END) as last_email_from,
        MAX(CASE WHEN es.direction = 'outbound' THEN es.email_date END) as last_email_to,
        AVG(es.response_time_minutes)::INTEGER as avg_response_minutes,
        COUNT(DISTINCT es.thread_id) as thread_count,
        ARRAY_AGG(DISTINCT u.full_name) FILTER (WHERE u.full_name IS NOT NULL) as contacts
      FROM email_signals es
      LEFT JOIN users u ON es.user_id = u.id
      WHERE es.person_id = $1
      GROUP BY es.person_id
    `, [personId]);
    
    const engagement = result.rows[0] || {
      ml_contacts: 0,
      emails_received: 0,
      emails_sent: 0,
      last_email_from: null,
      last_email_to: null,
      avg_response_minutes: null,
      thread_count: 0,
      contacts: []
    };
    
    // Compute response rate
    if (engagement.emails_sent > 0) {
      const threadResult = await pool.query(`
        SELECT 
          COUNT(DISTINCT CASE WHEN direction = 'outbound' THEN thread_id END) as outbound_threads,
          COUNT(DISTINCT CASE 
            WHEN direction = 'inbound' AND thread_id IN (
              SELECT thread_id FROM email_signals WHERE person_id = $1 AND direction = 'outbound'
            ) THEN thread_id 
          END) as replied_threads
        FROM email_signals
        WHERE person_id = $1
      `, [personId]);
      
      const { outbound_threads, replied_threads } = threadResult.rows[0];
      engagement.response_rate = outbound_threads > 0 
        ? Math.round((replied_threads / outbound_threads) * 100) 
        : null;
    }
    
    res.json(engagement);
    
  } catch (error) {
    console.error('Get engagement error:', error);
    res.status(500).json({ error: error.message });
  }
});
