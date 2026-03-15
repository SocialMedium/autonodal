#!/usr/bin/env node
/**
 * MitchelLake Gmail Intelligence Pipeline
 * 
 * 1. OAuth flow to get/refresh tokens
 * 2. Sync emails (headers only — no body content stored)
 * 3. Match emails to people in MLX network
 * 4. Extract engagement signals (response times, frequency, recency)
 * 5. Store as interactions + email_signals
 * 
 * Privacy-by-design:
 * - Only stores: from, to, subject, snippet, thread_id, date, labels
 * - Does NOT store email bodies
 * - Derived signals (response time, frequency) flow to shared pool
 * - Raw email metadata stays attributed to the user who connected
 * 
 * Usage:
 *   node scripts/gmail_sync.js                  # Sync all connected accounts
 *   node scripts/gmail_sync.js --user jt@ml.com # Sync specific user
 *   node scripts/gmail_sync.js --full           # Full historical sync (slow)
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI;

const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/userinfo.email'
];

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══════════════════════════════════════════════════════════════════════════════
// OAUTH HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function getAuthUrl(state = '') {
  const params = new URLSearchParams({
    client_id: GOOGLE_CLIENT_ID,
    redirect_uri: GOOGLE_REDIRECT_URI,
    response_type: 'code',
    scope: SCOPES.join(' '),
    access_type: 'offline',
    prompt: 'consent',
    state
  });
  return `https://accounts.google.com/o/oauth2/v2/auth?${params}`;
}

async function exchangeCode(code) {
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      code,
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      redirect_uri: GOOGLE_REDIRECT_URI,
      grant_type: 'authorization_code'
    })
  });
  
  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Token exchange failed: ${err}`);
  }
  
  return response.json();
}

async function refreshAccessToken(refreshToken) {
  const response = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      refresh_token: refreshToken,
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      grant_type: 'refresh_token'
    })
  });
  
  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Token refresh failed: ${err}`);
  }
  
  return response.json();
}

async function getValidToken(account) {
  const now = new Date();
  const expires = new Date(account.token_expires_at);
  
  // Refresh if expiring within 5 minutes
  if (expires <= new Date(now.getTime() + 5 * 60 * 1000)) {
    console.log('  🔄 Refreshing access token...');
    const tokens = await refreshAccessToken(account.refresh_token);
    
    const newExpires = new Date(Date.now() + tokens.expires_in * 1000);
    await pool.query(`
      UPDATE user_google_accounts 
      SET access_token = $1, token_expires_at = $2, updated_at = NOW()
      WHERE id = $3
    `, [tokens.access_token, newExpires, account.id]);
    
    return tokens.access_token;
  }
  
  return account.access_token;
}

// ═══════════════════════════════════════════════════════════════════════════════
// GMAIL API HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

async function gmailApi(accessToken, endpoint, params = {}) {
  const url = new URL(`https://gmail.googleapis.com/gmail/v1/users/me/${endpoint}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  });
  
  const response = await fetch(url, {
    headers: { 'Authorization': `Bearer ${accessToken}` }
  });
  
  if (response.status === 429) {
    console.log('  ⏳ Rate limited, waiting 10s...');
    await sleep(10000);
    return gmailApi(accessToken, endpoint, params);
  }
  
  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Gmail API ${response.status}: ${err.slice(0, 200)}`);
  }
  
  return response.json();
}

async function listMessages(accessToken, query, maxResults = 100, pageToken = null) {
  return gmailApi(accessToken, 'messages', {
    q: query,
    maxResults,
    pageToken
  });
}

async function getMessage(accessToken, messageId) {
  return gmailApi(accessToken, `messages/${messageId}`, {
    format: 'metadata',
    metadataHeaders: 'From,To,Cc,Subject,Date,Message-ID,In-Reply-To,References'
  });
}

function getHeader(headers, name) {
  const h = headers.find(h => h.name.toLowerCase() === name.toLowerCase());
  return h ? h.value : null;
}

function extractEmail(headerValue) {
  if (!headerValue) return null;
  const match = headerValue.match(/<([^>]+)>/) || headerValue.match(/([^\s<,]+@[^\s>,]+)/);
  return match ? match[1].toLowerCase() : headerValue.toLowerCase();
}

function extractAllEmails(headerValue) {
  if (!headerValue) return [];
  const matches = headerValue.match(/[^\s<,]+@[^\s>,]+/g) || [];
  return matches.map(e => e.toLowerCase());
}

// ═══════════════════════════════════════════════════════════════════════════════
// PERSON MATCHING
// ═══════════════════════════════════════════════════════════════════════════════

let emailIndex = null;

async function loadEmailIndex() {
  if (emailIndex) return emailIndex;
  
  console.log('  Loading email index from people...');
  const result = await pool.query(`
    SELECT id, email, full_name FROM people WHERE email IS NOT NULL AND email != ''
  `);
  
  emailIndex = new Map();
  for (const row of result.rows) {
    const emails = row.email.toLowerCase().split(/[,;\s]+/).filter(e => e.includes('@'));
    for (const email of emails) {
      emailIndex.set(email, { id: row.id, name: row.full_name });
    }
  }
  
  // Also load from interactions that already have email_from matched
  const existing = await pool.query(`
    SELECT DISTINCT email_from, person_id FROM interactions 
    WHERE email_from IS NOT NULL AND person_id IS NOT NULL
  `);
  for (const row of existing.rows) {
    if (!emailIndex.has(row.email_from)) {
      emailIndex.set(row.email_from, { id: row.person_id, name: null });
    }
  }
  
  console.log(`  ✅ Email index: ${emailIndex.size} addresses`);
  return emailIndex;
}

function matchEmailToPerson(emailAddress) {
  if (!emailAddress || !emailIndex) return null;
  return emailIndex.get(emailAddress.toLowerCase()) || null;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SYNC ENGINE
// ═══════════════════════════════════════════════════════════════════════════════

async function syncAccount(account, fullSync = false) {
  const stats = { messages: 0, matched: 0, signals: 0, threads: new Set(), errors: 0 };
  
  console.log(`\n  📧 Syncing: ${account.google_email}`);
  
  const accessToken = await getValidToken(account);
  await loadEmailIndex();
  
  // Determine sync window
  const query = fullSync
    ? 'in:sent OR in:inbox'
    : account.last_sync_at
      ? `after:${Math.floor(new Date(account.last_sync_at).getTime() / 1000 - 86400)}`
      : 'newer_than:90d';
  
  console.log(`  Query: ${query}`);
  
  let pageToken = null;
  let totalFetched = 0;
  const maxPages = fullSync ? 50 : 10; // ~5000 messages full, ~1000 incremental
  
  for (let page = 0; page < maxPages; page++) {
    const list = await listMessages(accessToken, query, 100, pageToken);
    
    if (!list.messages || list.messages.length === 0) break;
    
    // Fetch message details in parallel (batches of 10)
    for (let i = 0; i < list.messages.length; i += 10) {
      const batch = list.messages.slice(i, i + 10);
      
      const messages = await Promise.all(
        batch.map(m => getMessage(accessToken, m.id).catch(e => {
          stats.errors++;
          return null;
        }))
      );
      
      for (const msg of messages) {
        if (!msg) continue;
        
        try {
          await processMessage(msg, account, stats);
        } catch (e) {
          stats.errors++;
        }
      }
      
      totalFetched += batch.length;
      await sleep(100); // Rate limit courtesy
    }
    
    console.log(`  Fetched ${totalFetched} messages | Matched: ${stats.matched} | Signals: ${stats.signals}`);
    
    pageToken = list.nextPageToken;
    if (!pageToken) break;
    
    await sleep(200);
  }
  
  // Update sync timestamp and history ID
  await pool.query(`
    UPDATE user_google_accounts SET 
      last_sync_at = NOW(), updated_at = NOW()
    WHERE id = $1
  `, [account.id]);
  
  // Compute engagement signals from thread data
  const threadSignals = await computeThreadSignals(account.user_id, account.google_email);
  stats.signals += threadSignals;
  
  return stats;
}

async function processMessage(msg, account, stats) {
  const headers = msg.payload?.headers || [];
  const messageId = getHeader(headers, 'Message-ID');
  const from = getHeader(headers, 'From');
  const to = getHeader(headers, 'To');
  const cc = getHeader(headers, 'Cc');
  const subject = getHeader(headers, 'Subject');
  const dateStr = getHeader(headers, 'Date');
  const inReplyTo = getHeader(headers, 'In-Reply-To');
  
  const fromEmail = extractEmail(from);
  const toEmails = extractAllEmails(to);
  const ccEmails = extractAllEmails(cc);
  const snippet = (msg.snippet || '').slice(0, 200);
  const threadId = msg.threadId;
  const gmailId = msg.id;
  const labels = (msg.labelIds || []).join(',');
  const hasAttachments = msg.payload?.parts?.some(p => p.filename && p.filename.length > 0) || false;
  
  const emailDate = dateStr ? new Date(dateStr) : new Date(parseInt(msg.internalDate));
  
  // Determine direction
  const isSent = fromEmail === account.google_email.toLowerCase() || 
                 (msg.labelIds || []).includes('SENT');
  const direction = isSent ? 'outbound' : 'inbound';
  
  // Match the other party to a person
  const otherEmail = isSent ? toEmails[0] : fromEmail;
  const person = matchEmailToPerson(otherEmail);
  
  // Also check all recipients
  const allOtherEmails = isSent 
    ? [...toEmails, ...ccEmails]
    : [fromEmail, ...ccEmails].filter(e => e !== account.google_email.toLowerCase());
  
  // Skip if we can't match anyone and it's not interesting
  const personId = person?.id || null;
  
  // Check for duplicate
  const existing = await pool.query(
    'SELECT id FROM interactions WHERE email_message_id = $1 AND user_id = $2 LIMIT 1',
    [gmailId, account.user_id]
  );
  
  if (existing.rows.length > 0) return; // Already synced
  
  // Detect internal MitchelLake-to-MitchelLake conversations
  const allParties = [fromEmail, ...toEmails, ...ccEmails];
  const isInternal = allParties.length > 0 && allParties.every(e => e && e.endsWith('@mitchellake.com'));

  // Detect potentially sensitive topics
  const lowerSubject = (subject || '').toLowerCase();
  const lowerSnippet = (snippet || '').toLowerCase();
  const sensitiveKeywords = ['salary', 'salaries', 'wage', 'wages', 'compensation', 'comp review',
    'disciplinary', 'termination', 'dismissal', 'performance review', 'pip',
    'confidential', 'redundancy', 'grievance', 'warning', 'misconduct'];
  const isSensitive = sensitiveKeywords.some(kw => lowerSubject.includes(kw) || lowerSnippet.includes(kw));
  const sensitivity = isSensitive ? 'sensitive' : 'normal';
  const visibility = isInternal && isSensitive ? 'private' : 'team';

  // Store as interaction
  await pool.query(`
    INSERT INTO interactions (
      person_id, user_id, interaction_type, direction,
      subject, summary, channel, source,
      email_message_id, email_thread_id, email_subject,
      email_snippet, email_from, email_to,
      email_labels, email_has_attachments,
      interaction_at, created_at,
      visibility, owner_user_id, is_internal, sensitivity
    ) VALUES ($1, $2, 'email', $3, $4, $5, 'email', 'gmail_sync',
              $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(),
              $15, $2, $16, $17)
    ON CONFLICT DO NOTHING
  `, [
    personId,
    account.user_id,
    direction,
    (subject || '').slice(0, 500),
    snippet,
    gmailId,
    threadId,
    (subject || '').slice(0, 500),
    snippet,
    fromEmail,
    allOtherEmails.join(', '),
    labels,
    hasAttachments,
    emailDate,
    visibility,
    isInternal,
    sensitivity
  ]);
  
  stats.messages++;
  stats.threads.add(threadId);
  
  if (personId) {
    stats.matched++;
    
    // Create email_signal for matched person
    const threadPosition = inReplyTo ? 'reply' : 'initial';
    
    await pool.query(`
      INSERT INTO email_signals (
        person_id, user_id, direction, email_date,
        thread_id, thread_position, has_attachment, email_domain
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT DO NOTHING
    `, [
      personId,
      account.user_id,
      direction,
      emailDate,
      threadId,
      inReplyTo ? 2 : 1, // approximate thread position
      hasAttachments,
      otherEmail ? otherEmail.split('@')[1] : null
    ]).catch(() => {}); // Skip constraint violations
    
    stats.signals++;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ENGAGEMENT SIGNAL COMPUTATION
// ═══════════════════════════════════════════════════════════════════════════════

async function computeThreadSignals(userId, userEmail) {
  console.log('  📊 Computing engagement signals from threads...');
  
  let signalCount = 0;
  
  // Find threads where we have both sent and received from same person
  const threads = await pool.query(`
    SELECT 
      i.person_id,
      i.email_thread_id,
      MIN(CASE WHEN i.direction = 'outbound' THEN i.interaction_at END) as first_sent,
      MIN(CASE WHEN i.direction = 'inbound' THEN i.interaction_at END) as first_received,
      COUNT(*) FILTER (WHERE i.direction = 'outbound') as sent_count,
      COUNT(*) FILTER (WHERE i.direction = 'inbound') as received_count,
      COUNT(*) as total_messages
    FROM interactions i
    WHERE i.user_id = $1
    AND i.interaction_type = 'email'
    AND i.person_id IS NOT NULL
    AND i.email_thread_id IS NOT NULL
    GROUP BY i.person_id, i.email_thread_id
    HAVING COUNT(*) > 1
  `, [userId]);
  
  // Compute response times per thread
  for (const thread of threads.rows) {
    if (thread.first_sent && thread.first_received) {
      const sent = new Date(thread.first_sent);
      const received = new Date(thread.first_received);
      
      // If we sent first and they replied
      if (sent < received) {
        const responseMinutes = Math.round((received - sent) / 60000);
        
        if (responseMinutes > 0 && responseMinutes < 43200) { // Under 30 days
          await pool.query(`
            UPDATE email_signals 
            SET response_time_minutes = $1
            WHERE person_id = $2 AND thread_id = $3 AND direction = 'inbound'
          `, [responseMinutes, thread.person_id, thread.email_thread_id]).catch(() => {});
          
          signalCount++;
        }
      }
      
      // If they sent first and we replied
      if (received < sent) {
        const responseMinutes = Math.round((sent - received) / 60000);
        
        if (responseMinutes > 0 && responseMinutes < 43200) {
          await pool.query(`
            UPDATE email_signals 
            SET response_time_minutes = $1
            WHERE person_id = $2 AND thread_id = $3 AND direction = 'outbound'
          `, [responseMinutes, thread.person_id, thread.email_thread_id]).catch(() => {});
        }
      }
    }
  }
  
  // Compute per-person engagement aggregates
  const personStats = await pool.query(`
    SELECT 
      person_id,
      COUNT(*) as total_emails,
      COUNT(*) FILTER (WHERE direction = 'inbound') as received,
      COUNT(*) FILTER (WHERE direction = 'outbound') as sent,
      AVG(response_time_minutes) FILTER (WHERE direction = 'inbound' AND response_time_minutes IS NOT NULL) as avg_response_minutes,
      MIN(response_time_minutes) FILTER (WHERE direction = 'inbound' AND response_time_minutes IS NOT NULL) as fastest_response_minutes,
      MAX(email_date) as last_email,
      MIN(email_date) as first_email,
      COUNT(DISTINCT thread_id) as thread_count
    FROM email_signals
    WHERE user_id = $1
    GROUP BY person_id
  `, [userId]);
  
  for (const ps of personStats.rows) {
    const responseRate = ps.sent > 0 ? ps.received / ps.sent : 0;
    const daysSinceLastEmail = ps.last_email 
      ? Math.floor((Date.now() - new Date(ps.last_email)) / (1000 * 60 * 60 * 24))
      : 999;
    
    // Update person_scores with email engagement data
    await pool.query(`
      UPDATE person_scores SET
        engagement_score = GREATEST(engagement_score, $1),
        last_interaction_at = GREATEST(last_interaction_at, $2),
        interaction_count_30d = GREATEST(interaction_count_30d, $3),
        score_factors = COALESCE(score_factors, '{}'::jsonb) || $4::jsonb,
        computed_at = NOW()
      WHERE person_id = $5
    `, [
      // Engagement score from email patterns
      Math.min(1.0, 
        (responseRate > 0.5 ? 0.3 : responseRate > 0.2 ? 0.15 : 0) +
        (daysSinceLastEmail < 30 ? 0.3 : daysSinceLastEmail < 90 ? 0.15 : 0) +
        (ps.thread_count > 5 ? 0.2 : ps.thread_count > 2 ? 0.1 : 0) +
        (ps.avg_response_minutes && ps.avg_response_minutes < 1440 ? 0.2 : 0.05)
      ),
      ps.last_email,
      parseInt(ps.total_emails) || 0,
      JSON.stringify({
        email_total: parseInt(ps.total_emails),
        email_sent: parseInt(ps.sent),
        email_received: parseInt(ps.received),
        email_threads: parseInt(ps.thread_count),
        avg_response_minutes: ps.avg_response_minutes ? Math.round(ps.avg_response_minutes) : null,
        fastest_response_minutes: ps.fastest_response_minutes ? Math.round(ps.fastest_response_minutes) : null,
        last_email: ps.last_email,
        response_rate: Math.round(responseRate * 100) / 100,
        days_since_last: daysSinceLastEmail
      }),
      ps.person_id
    ]).catch(() => {});
    
    // Generate person_signals for notable patterns
    if (daysSinceLastEmail > 90 && ps.total_emails > 5) {
      await pool.query(`
        INSERT INTO person_signals (person_id, signal_type, signal_category, confidence, detail, source, detected_at)
        VALUES ($1, 'going_cold', 'engagement', 0.7, $2, 'gmail_analysis', NOW())
        ON CONFLICT DO NOTHING
      `, [
        ps.person_id,
        JSON.stringify({ days_silent: daysSinceLastEmail, previous_emails: ps.total_emails })
      ]).catch(() => {});
      signalCount++;
    }
    
    if (ps.avg_response_minutes && ps.avg_response_minutes < 60 && ps.received > 3) {
      await pool.query(`
        INSERT INTO person_signals (person_id, signal_type, signal_category, confidence, detail, source, detected_at)
        VALUES ($1, 'highly_responsive', 'engagement', 0.8, $2, 'gmail_analysis', NOW())
        ON CONFLICT DO NOTHING
      `, [
        ps.person_id,
        JSON.stringify({ avg_response_min: Math.round(ps.avg_response_minutes), total_responses: ps.received })
      ]).catch(() => {});
      signalCount++;
    }
  }
  
  console.log(`  ✅ ${signalCount} engagement signals computed from ${personStats.rows.length} people`);
  return signalCount;
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPRESS ROUTES (add to server.js)
// ═══════════════════════════════════════════════════════════════════════════════

function registerRoutes(app, authenticateToken) {
  
  // Start OAuth flow
  app.get('/api/auth/google', authenticateToken, (req, res) => {
    const state = req.user.id; // Pass user ID through OAuth state
    const url = getAuthUrl(state);
    res.json({ url });
  });
  
  // OAuth callback
  app.get('/api/auth/google/callback', async (req, res) => {
    const { code, state: userId } = req.query;
    
    if (!code) {
      return res.status(400).send('Missing authorization code');
    }
    
    try {
      const tokens = await exchangeCode(code);
      
      // Get user's email from Google
      const userInfo = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
        headers: { 'Authorization': `Bearer ${tokens.access_token}` }
      }).then(r => r.json());
      
      const expiresAt = new Date(Date.now() + tokens.expires_in * 1000);
      
      // Upsert Google account
      await pool.query(`
        INSERT INTO user_google_accounts (
          user_id, google_email, access_token, refresh_token,
          token_expires_at, scopes, sync_enabled
        ) VALUES ($1, $2, $3, $4, $5, $6, true)
        ON CONFLICT (user_id, google_email) DO UPDATE SET
          access_token = $3,
          refresh_token = COALESCE($4, user_google_accounts.refresh_token),
          token_expires_at = $5,
          scopes = $6,
          sync_enabled = true,
          updated_at = NOW()
      `, [
        userId,
        userInfo.email,
        tokens.access_token,
        tokens.refresh_token,
        expiresAt,
        SCOPES.join(' ')
      ]);
      
      // Redirect back to app with success
      res.redirect('/?gmail=connected');
      
    } catch (e) {
      console.error('Google OAuth error:', e);
      res.redirect('/?gmail=error');
    }
  });
  
  // Check Gmail connection status
  app.get('/api/gmail/status', authenticateToken, async (req, res) => {
    const accounts = await pool.query(`
      SELECT google_email, sync_enabled, last_sync_at, created_at
      FROM user_google_accounts WHERE user_id = $1
    `, [req.user.id]);
    
    res.json({
      connected: accounts.rows.length > 0,
      accounts: accounts.rows
    });
  });
  
  // Trigger manual sync
  app.post('/api/gmail/sync', authenticateToken, async (req, res) => {
    const accounts = await pool.query(`
      SELECT * FROM user_google_accounts 
      WHERE user_id = $1 AND sync_enabled = true
    `, [req.user.id]);
    
    if (accounts.rows.length === 0) {
      return res.status(400).json({ error: 'No Gmail account connected' });
    }
    
    res.json({ message: 'Sync started' });
    
    // Run sync in background
    for (const account of accounts.rows) {
      syncAccount(account, req.body.full || false).catch(e => {
        console.error(`Gmail sync error for ${account.google_email}:`, e.message);
      });
    }
  });
  
  // Get email engagement for a person
  app.get('/api/people/:id/email-engagement', authenticateToken, async (req, res) => {
    const { id } = req.params;
    
    const [signals, recent] = await Promise.all([
      pool.query(`
        SELECT 
          COUNT(*) as total,
          COUNT(*) FILTER (WHERE direction = 'inbound') as received,
          COUNT(*) FILTER (WHERE direction = 'outbound') as sent,
          AVG(response_time_minutes) FILTER (WHERE response_time_minutes IS NOT NULL) as avg_response_min,
          MAX(email_date) as last_email,
          COUNT(DISTINCT thread_id) as threads
        FROM email_signals WHERE person_id = $1
      `, [id]),
      pool.query(`
        SELECT direction, email_date, response_time_minutes, thread_id
        FROM email_signals WHERE person_id = $1
        ORDER BY email_date DESC LIMIT 20
      `, [id])
    ]);
    
    res.json({
      summary: signals.rows[0],
      recent: recent.rows
    });
  });
  
  // Disconnect Gmail
  app.delete('/api/gmail/disconnect', authenticateToken, async (req, res) => {
    await pool.query(`
      UPDATE user_google_accounts SET sync_enabled = false WHERE user_id = $1
    `, [req.user.id]);
    res.json({ ok: true });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLI: Run sync for all connected accounts
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const args = process.argv.slice(2);
  const fullSync = args.includes('--full');
  const userFilter = args.includes('--user') ? args[args.indexOf('--user') + 1] : null;
  
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   MitchelLake Gmail Intelligence Sync                     ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');
  
  let query = 'SELECT * FROM user_google_accounts WHERE sync_enabled = true';
  const params = [];
  
  if (userFilter) {
    query += ' AND google_email = $1';
    params.push(userFilter);
  }
  
  const accounts = await pool.query(query, params);
  
  if (accounts.rows.length === 0) {
    console.log('No Gmail accounts connected.');
    console.log('Connect via: GET /api/auth/google');
    await pool.end();
    return;
  }
  
  console.log(`Found ${accounts.rows.length} connected account(s)\n`);
  
  for (const account of accounts.rows) {
    try {
      const stats = await syncAccount(account, fullSync);
      console.log(`\n  ✅ ${account.google_email}: ${stats.messages} messages, ${stats.matched} matched, ${stats.signals} signals`);
    } catch (e) {
      console.error(`\n  ❌ ${account.google_email}: ${e.message}`);
    }
  }
  
  console.log('\n✅ Gmail sync complete');
  await pool.end();
}

// Only run CLI if called directly
if (require.main === module) {
  main().catch(e => {
    console.error('Fatal:', e);
    pool.end();
    process.exit(1);
  });
}

module.exports = { registerRoutes, syncAccount, computeThreadSignals, getAuthUrl };
