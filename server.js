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
const rateLimit = require('express-rate-limit');

// Global rate limit — 200 requests per minute per IP+tenant
app.use('/api/', rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  validate: false,
  keyGenerator: function(req) { return req.ip + ':' + (req.tenant_id || 'anon'); },
  handler: function(req, res) { res.status(429).json({ error: 'Rate limit exceeded. Try again in a minute.' }); },
}));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  if (req.secure || req.headers['x-forwarded-proto'] === 'https') {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  }
  next();
});

// Strict limit on auth endpoints — 20 per minute per IP
app.use('/api/auth/', rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  validate: false,
  keyGenerator: function(req) { return req.ip; },
  handler: function(req, res) { res.status(429).json({ error: 'Too many auth requests.' }); },
}));

// Strict limit on waitlist — 5 per minute per IP
app.use('/api/waitlist', rateLimit({
  windowMs: 60 * 1000,
  max: 5,
  validate: false,
  keyGenerator: function(req) { return req.ip; },
}));

// ─────────────────────────────────────────────────────────────────────────────
// NICKNAME MAP — used for fuzzy people matching across imports
// ─────────────────────────────────────────────────────────────────────────────
const NICKNAMES = { jon: 'jonathan', jonathan: 'jon', mike: 'michael', michael: 'mike', rob: 'robert', robert: 'rob', bob: 'robert', will: 'william', william: 'will', bill: 'william', jim: 'james', james: 'jim', dave: 'david', david: 'dave', dan: 'daniel', daniel: 'dan', chris: 'christopher', christopher: 'chris', matt: 'matthew', matthew: 'matt', tom: 'thomas', thomas: 'tom', tony: 'anthony', anthony: 'tony', nick: 'nicholas', nicholas: 'nick', alex: 'alexander', alexander: 'alex', ben: 'benjamin', benjamin: 'ben', sam: 'samuel', samuel: 'sam', ed: 'edward', edward: 'ed', steve: 'steven', steven: 'steve', rick: 'richard', richard: 'rick', liz: 'elizabeth', elizabeth: 'liz', kate: 'katherine', katherine: 'kate', jen: 'jennifer', jennifer: 'jen', sue: 'susan', susan: 'sue', meg: 'megan', megan: 'meg', becky: 'rebecca', rebecca: 'becky', andy: 'andrew', andrew: 'andy', greg: 'gregory', gregory: 'greg', joe: 'joseph', joseph: 'joe', phil: 'philip', philip: 'phil', tim: 'timothy', timothy: 'tim', pete: 'peter', peter: 'pete', pat: 'patrick', patrick: 'pat' };

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE
// ─────────────────────────────────────────────────────────────────────────────

// Application pool — non-superuser (autonodal_app), RLS enforced
const pool = new Pool({
  connectionString: process.env.DATABASE_URL_APP || process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

// Tenant-isolated DB client — use for all tenant-scoped queries
const { TenantDB, platformPool } = require('./lib/TenantDB');
const { searchPublications, computeResearchMomentum } = require('./lib/research_search');
const RESEARCH_SEARCH_ENABLED = process.env.RESEARCH_SEARCH_ENABLED !== 'false';

// ─────────────────────────────────────────────────────────────────────────────
// SHARED CONSTANTS
// ─────────────────────────────────────────────────────────────────────────────

const REGION_MAP = {
  'AMER': ['United States', 'North America', 'Canada', 'Canadian', 'Latin America', 'Brazil', 'Brazilian', 'Mexico', 'Silicon Valley', 'New York', 'NASDAQ', 'NYSE', 'SEC', 'Federal', 'San Francisco', 'California', 'Texas', 'Boston', 'Seattle', 'Chicago', 'Colombia', 'Argentina'],
  'EUR':  ['United Kingdom', 'Europe', 'European', 'London', 'England', 'Britain', 'British', 'Germany', 'German', 'France', 'French', 'Netherlands', 'Ireland', 'Nordics', 'Sweden', 'Denmark', 'EU', 'FTSE', 'LSE', 'Manchester', 'Edinburgh', 'Spain', 'Italy', 'Switzerland', 'Belgium', 'Portugal', 'Austria', 'Finland', 'Norway'],
  'MENA': ['Middle East', 'Dubai', 'UAE', 'Saudi', 'Riyadh', 'Abu Dhabi', 'Qatar', 'Bahrain', 'Kuwait', 'Oman', 'Israel', 'Tel Aviv', 'Turkey', 'Istanbul', 'Egypt', 'Cairo', 'Morocco', 'North Africa', 'Gulf', 'GCC'],
  'ASIA': ['Singapore', 'Southeast Asia', 'ASEAN', 'Jakarta', 'Kuala Lumpur', 'Bangkok', 'Vietnam', 'Philippines', 'Indonesia', 'Malaysia', 'Thailand', 'Asia', 'APAC', 'Japan', 'Japanese', 'Korea', 'Korean', 'India', 'Indian', 'Hong Kong', 'China', 'Chinese', 'Taiwan', 'Mumbai', 'Delhi', 'Bangalore', 'Shenzhen', 'Shanghai', 'Tokyo', 'Seoul'],
  'OCE':  ['Australia', 'Australian', 'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'ASX', 'Canberra', 'New Zealand', 'Auckland', 'Wellington', 'Oceania', 'Pacific'],
  // Legacy aliases — map old codes to new
  'AU': ['Australia', 'Australian', 'Sydney', 'Melbourne', 'Brisbane', 'Perth', 'New Zealand'],
  'SG': ['Singapore', 'Southeast Asia', 'ASEAN', 'Jakarta', 'Bangkok', 'Vietnam'],
  'UK': ['United Kingdom', 'London', 'England', 'Britain'],
  'US': ['United States', 'Silicon Valley', 'New York', 'San Francisco'],
};
const REGION_CODES = {
  'AMER': ['US','CA','BR','MX','AR','CL','CO','PE'],
  'EUR':  ['UK','GB','IE','DE','FR','NL','SE','DK','NO','FI','ES','IT','PT','AT','CH','BE','PL','CZ','GR','RO','HU'],
  'MENA': ['AE','SA','QA','BH','KW','OM','IL','TR','EG','MA','TN','JO','LB'],
  'ASIA': ['SG','MY','ID','TH','VN','PH','JP','KR','IN','HK','CN','TW','BD','PK','LK','MM','KH','NP'],
  'OCE':  ['AU','NZ','FJ','PG'],
  // Legacy aliases
  'AU': ['AU','NZ'], 'SG': ['SG','MY','ID','TH','VN','PH'], 'UK': ['UK','GB','IE'], 'US': ['US','CA'],
};

// ─────────────────────────────────────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

app.set('trust proxy', 1);

// ─── Performance: gzip compression (40-70% smaller responses) ───
const compression = require('compression');
app.use(compression());

// ─── Security: per-IP rate limiting for expensive endpoints ───
const endpointLimitMap = new Map();
setInterval(() => endpointLimitMap.clear(), 60000);

function endpointLimit(maxPerMinute) {
  return function(req, res, next) {
    var key = (req.ip || 'unknown') + ':' + req.path;
    var count = endpointLimitMap.get(key) || 0;
    if (count >= maxPerMinute) return res.status(429).json({ error: 'Too many requests' });
    endpointLimitMap.set(key, count + 1);
    next();
  };
}

// ─── Security: sanitise error responses — never leak stack traces or schema ───
function safeError(err) {
  if (process.env.NODE_ENV === 'development') return err.message;
  // Strip SQL details, file paths, and stack traces
  var msg = (err.message || 'Internal error').replace(/at \/.*$/gm, '').replace(/relation ".*?"/g, 'relation').replace(/column ".*?"/g, 'column');
  if (msg.length > 200) msg = msg.substring(0, 200);
  return msg;
}

// Skip JSON parsing for Stripe webhook (needs raw body for signature verification)
app.use(function(req, res, next) {
  if (req.path === '/api/billing/webhook') return next();
  express.json({ limit: '10mb' })(req, res, next);
});

// Serve Autonodal landing page as homepage when accessed via autonodal.com
app.get('/', (req, res, next) => {
  const host = req.hostname;
  if (host === 'autonodal.com' || host === 'www.autonodal.com') {
    return res.sendFile(path.join(__dirname, 'public/autonodal.html'));
  }
  next();
});

// ─── Performance: cache static assets (1h for JS/CSS/images, 5min for HTML) ───
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1h',
  setHeaders: function(res, filePath) {
    if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'public, max-age=300');
  }
}));

// CORS for development
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ─────────────────────────────────────────────────────────────────────────────
// RESPONSE CACHE — for heavy endpoints that don't change frequently
// Keyed by tenant_id + path. 2-minute TTL. Saves DB round-trips at 100 users.
// ─────────────────────────────────────────────────────────────────────────────

const responseCache = new Map();
const RESPONSE_CACHE_TTL = 120000; // 2 min

setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of responseCache) {
    if (now - entry.at > RESPONSE_CACHE_TTL) responseCache.delete(key);
  }
}, 60000);

function cachedResponse(tenantId, path) {
  const key = tenantId + ':' + path;
  const entry = responseCache.get(key);
  if (entry && (Date.now() - entry.at) < RESPONSE_CACHE_TTL) return entry.data;
  return null;
}

function setCachedResponse(tenantId, path, data) {
  responseCache.set(tenantId + ':' + path, { data, at: Date.now() });
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTH MIDDLEWARE — with in-memory session cache (60s TTL)
// Eliminates DB query on every request for recently authenticated users
// ─────────────────────────────────────────────────────────────────────────────

const sessionCache = new Map();
const SESSION_CACHE_TTL = 60000; // 60s

// Evict expired entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of sessionCache) {
    if (now - entry.cachedAt > SESSION_CACHE_TTL) sessionCache.delete(key);
  }
}, 300000);

async function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.replace('Bearer ', '');

  if (!token) return res.status(401).json({ error: 'No token provided' });

  // Check session cache first
  const cached = sessionCache.get(token);
  if (cached && (Date.now() - cached.cachedAt) < SESSION_CACHE_TTL) {
    req.user = cached.user;
    req.tenant_id = cached.user.tenant_id;
    return next();
  }

  try {
    const { rows } = await platformPool.query(
      `SELECT s.user_id, u.email, u.name, u.role, u.tenant_id,
              t.vertical, t.name as tenant_name, t.slug as tenant_slug
       FROM sessions s
       JOIN users u ON s.user_id = u.id
       LEFT JOIN tenants t ON t.id = u.tenant_id
       WHERE s.token = $1 AND s.expires_at > NOW()`,
      [token]
    );

    if (rows.length === 0) {
      sessionCache.delete(token);
      try { const { audit } = require('./lib/auditLogger'); audit.invalidToken(req); } catch(e) {}
      return res.status(401).json({ error: 'Invalid or expired token' });
    }

    req.user = rows[0];
    req.user.tenant_id = req.user.tenant_id || process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
    req.tenant_id = req.user.tenant_id;

    // Cache for 60s
    sessionCache.set(token, { user: { ...req.user }, cachedAt: Date.now() });

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
      const { rows } = await platformPool.query(
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
    await platformPool.query(
      `INSERT INTO audit_logs (id, user_id, action, target_type, target_id, details, ip_address, created_at)
       VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, NOW())`,
      [userId, action, targetType || null, targetId || null, details ? JSON.stringify(details) : null, ip || null]
    );
  } catch (e) { /* audit logging should never block operations */ }
}
const embeddingCache = new Map();
const EMBEDDING_CACHE_TTL = 300000; // 5 min
setInterval(() => {
  var now = Date.now();
  for (var [k, v] of embeddingCache) { if (now - v.at > EMBEDDING_CACHE_TTL) embeddingCache.delete(k); }
}, 60000);

async function generateQueryEmbedding(text) {
  var cacheKey = text.trim().toLowerCase().slice(0, 200);
  var cached = embeddingCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) < EMBEDDING_CACHE_TTL) return cached.vector;

  var vector = await _generateQueryEmbeddingRaw(text);
  embeddingCache.set(cacheKey, { vector, at: Date.now() });
  return vector;
}

function _generateQueryEmbeddingRaw(text) {
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

// ═══════════════════════════════════════════════════════════════════════════════
// ROUTE MODULES
// ═══════════════════════════════════════════════════════════════════════════════

app.use(require('./routes/public')({ platformPool }));
// auth.js uses generateQueryEmbedding (hoisted) — safe to mount here
app.use(require('./routes/auth')({ platformPool, TenantDB, authenticateToken, auditLog, generateQueryEmbedding }));
app.use(require('./routes/admin')({ platformPool, TenantDB, authenticateToken, requireAdmin, auditLog, rootDir: __dirname }));
app.use(require('./routes/people')({ platformPool, TenantDB, authenticateToken, verifyHuddleMember, generateQueryEmbedding, searchPublications }));
app.use(require('./routes/companies')({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding, getGoogleToken }));
app.use(require('./routes/signals')({ platformPool, TenantDB, authenticateToken, cachedResponse, setCachedResponse, generateQueryEmbedding, qdrantSearch, REGION_MAP, REGION_CODES, verifyHuddleMember }));
app.use(require('./routes/onboarding')({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding }));
app.use(require('./routes/artifacts')({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding, auditLog }));
app.use(require('./routes/platform')({
  platformPool, TenantDB, authenticateToken, requireAdmin, optionalAuth,
  auditLog, generateQueryEmbedding, qdrantSearch,
  cachedResponse, setCachedResponse, endpointLimit, safeError,
  REGION_MAP, REGION_CODES, NICKNAMES, RESEARCH_SEARCH_ENABLED,
  searchPublications, computeResearchMomentum,
  getGoogleToken, sendEmail,
  verifyHuddleMember,
  rootDir: __dirname,
}));

// ═══════════════════════════════════════════════════════════════════════════════
// AUTH ROUTES (non-extracted: tenant invites, brief — remain in server.js)
// ═══════════════════════════════════════════════════════════════════════════════

/* Google OAuth routes extracted to routes/auth.js */

/* REMOVED: app.get('/api/auth/google') — moved to routes/auth.js */
/* REMOVED: app.get('/api/auth/google/callback') — moved to routes/auth.js */

// ─── Tenant teammate invites ───
// Helper: get a fresh Google access token for the current user
async function getGoogleToken(userId) {
  const { rows: [acct] } = await platformPool.query(
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
          await platformPool.query(
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
const resendEnabled = !!process.env.RESEND_API_KEY;
const resend = resendEnabled ? new (require('resend').Resend)(process.env.RESEND_API_KEY) : null;
const EMAIL_FROM = process.env.EMAIL_FROM || 'Autonodal <notifications@autonodal.com>';

async function sendEmail(to, subject, html) {
  if (!resend) { console.log('[Email] Skipped (not configured):', subject, '→', to); return null; }
  try {
    var result = await resend.emails.send({ from: EMAIL_FROM, to: to, subject: subject, html: html });
    console.log('[Email] Sent:', subject, '→', to);
    return result;
  } catch (err) {
    console.error('[Email] Failed:', subject, '→', to, err.message);
    return null;
  }
}
// Helper: verify caller is an active member of a huddle
async function verifyHuddleMember(huddleId, tenantId) {
  var { rows } = await platformPool.query(
    `SELECT role, status FROM huddle_members
     WHERE huddle_id = $1 AND tenant_id = $2 AND status = 'active'`,
    [huddleId, tenantId]
  );
  return rows[0] || null;
}
function requireAdmin(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  next();
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

// Clean URLs for public compliance pages (no .html required)
app.get('/privacy', (req, res) => res.sendFile(path.join(__dirname, 'public/privacy.html')));
app.get('/terms', (req, res) => res.sendFile(path.join(__dirname, 'public/terms.html')));
app.get('/home', (req, res) => res.sendFile(path.join(__dirname, 'public/home.html')));
app.get('/data-deletion', (req, res) => res.sendFile(path.join(__dirname, 'public/data-deletion.html')));

// ═══════════════════════════════════════════════════════════════════════════════
// ERROR HANDLING MIDDLEWARE
// ═══════════════════════════════════════════════════════════════════════════════

// 404 handler for API routes
app.use('/api', function(req, res) {
  res.status(404).json({ error: 'Endpoint not found' });
});

// Global error handler
app.use(function(err, req, res, next) {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});


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

  // Defer startup migrations — run after healthcheck passes
  setTimeout(async function() {
  try {
  const db = platformPool;

  // Ensure user profile columns exist
  try {
    await db.query(`
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
      await db.query(fs.readFileSync(csMigration, 'utf8'));
    }
  } catch (e) { /* tables may already exist */ }

  // Ensure people privacy columns exist
  try {
    await db.query(`
      ALTER TABLE people ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE people ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE people ADD COLUMN IF NOT EXISTS marked_private_at TIMESTAMPTZ;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure document privacy columns exist
  try {
    await db.query(`
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS uploaded_by_user_id UUID;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure company + signal privacy columns exist
  try {
    await db.query(`
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS owner_user_id UUID;
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company';
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS owner_user_id UUID;
    `);
  } catch (e) { /* columns may already exist */ }

  // Ensure interactions has sensitivity flag for internal ML-to-ML
  try {
    await db.query(`
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT false;
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS sensitivity VARCHAR(20) DEFAULT 'normal';
    `);
  } catch (e) { /* columns may already exist */ }

  // User attribution columns
  try {
    await db.query(`
      ALTER TABLE people ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
      ALTER TABLE interactions ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
    `);
  } catch (e) { /* columns may already exist */ }

  // Gmail sync counter + podcast audio URL
  try {
    await db.query(`ALTER TABLE user_google_accounts ADD COLUMN IF NOT EXISTS emails_synced INTEGER DEFAULT 0`);
    await db.query(`ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS audio_url TEXT`);
    // Podcast backfill + company extraction moved to worker process (scheduler.js)
    // to avoid blocking web process on deploy
  } catch (e) {}

  // Platform-wide migration moved below multi-tenant migration (must run after)

  // Backfill signal_date from linked document published_at
  try {
    const { rowCount: datesFilled } = await platformPool.query(`
      UPDATE signal_events se SET signal_date = ed.published_at
      FROM external_documents ed
      WHERE se.source_document_id = ed.id AND se.signal_date IS NULL AND ed.published_at IS NOT NULL
    `);
    if (datesFilled) console.log(`  📅 Backfilled ${datesFilled} signal dates from documents`);
  } catch (e) {}

  // Backfill signal images from linked documents (skip favicons/logos)
  try {
    const { rowCount: imgFilled } = await platformPool.query(`
      UPDATE signal_events se SET image_url = ed.image_url
      FROM external_documents ed
      WHERE se.source_document_id = ed.id
        AND (se.image_url IS NULL OR se.image_url = '')
        AND ed.image_url IS NOT NULL AND ed.image_url != ''
        AND ed.image_url NOT LIKE '%favicon%' AND ed.image_url NOT LIKE '%logo%'
        AND ed.image_url NOT LIKE '%icon%' AND ed.image_url NOT LIKE '%cropped-%'
        AND ed.image_url NOT LIKE '%32x32%' AND ed.image_url NOT LIKE '%50x50%'
        AND ed.image_url NOT LIKE '%96x96%' AND ed.image_url NOT LIKE '%150x150%'
        AND ed.image_url NOT LIKE '%w=32%'
    `);
    if (imgFilled) console.log(`  🖼️ Backfilled ${imgFilled} signal images from documents`);
  } catch (e) {}

  // Flag companies with revenue as is_client (survives enrichment resets, tenant-scoped)
  try {
    const { rowCount: clientsFlagged } = await platformPool.query(`
      UPDATE companies c SET is_client = true
      WHERE c.is_client = false AND c.id IN (
        SELECT DISTINCT a.company_id FROM accounts a
        JOIN conversions conv ON conv.client_id = a.id
        WHERE a.company_id IS NOT NULL AND a.tenant_id = c.tenant_id
      )
    `);
    if (clientsFlagged) console.log(`  🏷️ Flagged ${clientsFlagged} companies as clients from revenue data`);
  } catch (e) {}

  // Ensure companies.source column exists
  try {
    await db.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS source VARCHAR(50)`);
  } catch (e) {}

  // Ensure conversions table has all needed columns for sales import
  try {
    await db.query(`
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS client_name_raw VARCHAR(500);
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS consultant_name VARCHAR(255);
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS fee_stage VARCHAR(50);
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS payment_status VARCHAR(50);
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS currency VARCHAR(10) DEFAULT 'AUD';
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS notes TEXT;
      ALTER TABLE conversions ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';
    `);
  } catch (e) {}

  // Remove default tenant_id on platform content tables — new rows should be NULL (platform-wide)
  try {
    await db.query(`ALTER TABLE signal_events ALTER COLUMN tenant_id DROP DEFAULT`);
    await db.query(`ALTER TABLE external_documents ALTER COLUMN tenant_id DROP DEFAULT`);
    await db.query(`ALTER TABLE events ALTER COLUMN tenant_id DROP DEFAULT`);
  } catch (e) {}

  // Fix RLS policies — platform content (tenant_id IS NULL) must be visible to all tenants
  // Update ALL policies that use current_tenant_id() to also allow tenant_id IS NULL
  try {
    // Get all tables with RLS policies
    const { rows: policies } = await db.query(`
      SELECT schemaname, tablename, policyname FROM pg_policies
      WHERE schemaname = 'public' AND policyname LIKE 'tenant_isolation_%'
    `);
    for (const p of policies) {
      await db.query(`DROP POLICY IF EXISTS ${p.policyname} ON ${p.tablename}`);
      await db.query(`
        CREATE POLICY ${p.policyname} ON ${p.tablename}
        USING (current_tenant_id() IS NULL OR tenant_id IS NULL OR tenant_id = current_tenant_id())
      `);
    }
    if (policies.length) console.log(`  ✅ Fixed ${policies.length} RLS policies — platform content (tenant_id IS NULL) now visible to all tenants`);
  } catch (e) { console.log('  ⚠️ RLS policy fix:', e.message); }

  // Fix global UNIQUE constraints that break multi-tenant isolation
  try {
    // companies.domain must be unique per tenant, not globally
    await db.query(`ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_domain_key`);
    await db.query(`DROP INDEX IF EXISTS companies_domain_key`);
    await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_domain_tenant ON companies(domain, tenant_id) WHERE domain IS NOT NULL`);

    // external_documents.source_url_hash must be per tenant
    await db.query(`ALTER TABLE external_documents DROP CONSTRAINT IF EXISTS external_documents_source_url_hash_key`);
    await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_url_hash_tenant ON external_documents(source_url_hash, tenant_id) WHERE source_url_hash IS NOT NULL`);

    // events.url_hash must be per tenant (or global for platform events)
    await db.query(`ALTER TABLE events DROP CONSTRAINT IF EXISTS events_url_hash_key`);

    // event_sources.feed_url must be per tenant
    await db.query(`ALTER TABLE event_sources DROP CONSTRAINT IF EXISTS event_sources_feed_url_key`);

    console.log('  ✅ Fixed multi-tenant unique constraints');
  } catch (e) { console.log('  ⚠️ Constraint fix:', e.message); }

  // Backfill tenant_id on team_proximity and interactions from user's tenant
  try {
    const { rowCount: tpFixed } = await db.query(`
      UPDATE team_proximity tp SET tenant_id = u.tenant_id
      FROM users u WHERE u.id = tp.team_member_id
        AND (tp.tenant_id IS NULL OR tp.tenant_id != u.tenant_id)
    `);
    if (tpFixed) console.log(`  🔗 Fixed tenant_id on ${tpFixed} team_proximity records`);

    const { rowCount: ixFixed } = await db.query(`
      UPDATE interactions i SET tenant_id = u.tenant_id
      FROM users u WHERE u.id = i.user_id
        AND (i.tenant_id IS NULL OR i.tenant_id != u.tenant_id)
    `);
    if (ixFixed) console.log(`  🔗 Fixed tenant_id on ${ixFixed} interaction records`);
  } catch (e) {}

  // Interaction → company linkage column + auto-populate trigger
  try {
    await db.query(`ALTER TABLE interactions ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id)`);
    await db.query(`CREATE INDEX IF NOT EXISTS idx_interactions_company ON interactions(company_id)`);

    // Trigger: auto-set company_id from person's current company on INSERT
    await db.query(`
      CREATE OR REPLACE FUNCTION fn_interaction_set_company()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.company_id IS NULL AND NEW.person_id IS NOT NULL THEN
          SELECT current_company_id INTO NEW.company_id
          FROM people WHERE id = NEW.person_id;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);
    await db.query(`
      DROP TRIGGER IF EXISTS trg_interaction_company ON interactions;
      CREATE TRIGGER trg_interaction_company
      BEFORE INSERT ON interactions
      FOR EACH ROW EXECUTE FUNCTION fn_interaction_set_company();
    `);

    // Trigger: when person changes company, update recent interactions
    await db.query(`
      CREATE OR REPLACE FUNCTION fn_person_company_changed()
      RETURNS TRIGGER AS $$
      BEGIN
        IF NEW.current_company_id IS DISTINCT FROM OLD.current_company_id AND NEW.current_company_id IS NOT NULL THEN
          UPDATE interactions SET company_id = NEW.current_company_id
          WHERE person_id = NEW.id AND interaction_at > NOW() - INTERVAL '1 year';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);
    await db.query(`
      DROP TRIGGER IF EXISTS trg_person_company_update ON people;
      CREATE TRIGGER trg_person_company_update
      AFTER UPDATE OF current_company_id ON people
      FOR EACH ROW EXECUTE FUNCTION fn_person_company_changed();
    `);

    // Backfill existing interactions
    const { rowCount: ixLinked } = await db.query(`
      UPDATE interactions i SET company_id = p.current_company_id
      FROM people p
      WHERE i.person_id = p.id AND p.current_company_id IS NOT NULL
        AND i.company_id IS NULL
    `);
    if (ixLinked) console.log(`  🔗 Linked ${ixLinked} interactions to companies`);
  } catch (e) {}

  // Messaging integration columns
  try {
    await db.query(`
      ALTER TABLE users ADD COLUMN IF NOT EXISTS telegram_chat_id VARCHAR(50);
      ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp_phone VARCHAR(50);
      ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp_verified BOOLEAN DEFAULT false;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp_verify_code VARCHAR(10);
      ALTER TABLE users ADD COLUMN IF NOT EXISTS whatsapp_verify_expires TIMESTAMPTZ;
    `);
  } catch (e) {}

  // User feed preferences (per-user toggle of platform feeds)
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS user_feed_prefs (
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        feed_id UUID NOT NULL,
        disabled BOOLEAN DEFAULT false,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (user_id, feed_id)
      );
    `);
  } catch (e) {}

  // Telegram MTProto accounts table
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS user_telegram_accounts (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE UNIQUE,
        phone VARCHAR(50),
        telegram_user_id VARCHAR(50),
        session_string TEXT,
        sync_enabled BOOLEAN DEFAULT true,
        first_name VARCHAR(100),
        username VARCHAR(100),
        last_sync_at TIMESTAMPTZ,
        last_message_id BIGINT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_tg_accounts_user ON user_telegram_accounts(user_id);
    `);
  } catch (e) {}

  // Purge known false-positive signals (e.g. "Pam Bondi" matched via short company name "PAM")
  try {
    const { rowCount } = await db.query(`
      DELETE FROM signal_events
      WHERE (headline ILIKE '%pam bondi%' OR evidence_summary ILIKE '%pam bondi%')
    `);
    if (rowCount) console.log(`  🧹 Purged ${rowCount} false-positive Pam Bondi signal(s)`);
  } catch (e) {}

  // Embedding tracking columns for all embeddable entities
  try {
    await db.query(`
      ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE case_studies ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE people ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE companies ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
      ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS embedded_at TIMESTAMPTZ;
    `);
  } catch (e) { /* columns may already exist */ }

  // Bootstrap admin: ensure at least one admin exists per tenant
  try {
    const { rows: admins } = await db.query(
      `SELECT id FROM users WHERE role = 'admin' AND tenant_id = '00000000-0000-0000-0000-000000000001' LIMIT 1`
    );
    if (admins.length === 0) {
      // Promote the first user created (tenant owner)
      await db.query(`
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
      await db.query(fs.readFileSync(topoMigration, 'utf8'));
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
    for (const sql of siTables) { try { await db.query(sql); } catch (e) {} }
  } catch (e) {}

  // Ensure signal_dispatches table exists with claim columns
  try {
    await db.query(`
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
    await db.query(`ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS claimed_by UUID`);
    await db.query(`ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ`);
  } catch (e) { /* table may already exist */ }

  // Ensure indexes for new query patterns
  try {
    await db.query(`
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
      await db.query(require('fs').readFileSync(mtMigration, 'utf8'));
      console.log('  \u2705 Multi-tenant migration complete');
    }
  } catch (e) {
    console.log('  \u26a0\ufe0f Multi-tenant migration:', e.message);
  }

  // Migrate signals + events to platform-wide (NULL tenant_id) so all tenants can see them
  // MUST run AFTER multi-tenant migration which adds the tenant_id column
  try {
    // Drop any default that multi-tenant migration may have re-added
    await platformPool.query('ALTER TABLE signal_events ALTER COLUMN tenant_id DROP DEFAULT');
    const { rowCount: sigMigrated } = await platformPool.query(`UPDATE signal_events SET tenant_id = NULL WHERE tenant_id IS NOT NULL AND COALESCE(visibility, 'public') != 'private'`);
    if (sigMigrated) console.log(`  ⚡ Migrated ${sigMigrated} signals to platform-wide visibility`);
  } catch (e) { console.log('  ⚠️ Signal migration:', e.message); }
  try {
    const { rowCount } = await platformPool.query(`UPDATE events SET tenant_id = NULL WHERE tenant_id IS NOT NULL`);
    if (rowCount) console.log(`  📅 Migrated ${rowCount} events to platform-wide visibility`);
  } catch (e) {}

  // Backfill: mark companies as clients if they have placements/revenue
  try {
    // First, try to link clients to companies by name match (same tenant only)
    const { rowCount: linked } = await platformPool.query(`
      UPDATE accounts SET company_id = co.id
      FROM companies co
      WHERE accounts.company_id IS NULL
        AND LOWER(TRIM(accounts.name)) = LOWER(TRIM(co.name))
        AND accounts.tenant_id = co.tenant_id
    `);
    if (linked > 0) console.log(`  ✅ Linked ${linked} clients to companies by exact name`);

    // Fuzzy link: match accounts to companies where company name starts with account name or vice versa
    const { rowCount: fuzzyLinked } = await platformPool.query(`
      UPDATE accounts SET company_id = co.id
      FROM (
        SELECT DISTINCT ON (a2.id) a2.id AS account_id, co2.id AS company_id
        FROM accounts a2
        JOIN companies co2 ON co2.tenant_id = a2.tenant_id
          AND (LOWER(co2.name) LIKE LOWER(TRIM(a2.name)) || '%'
               OR LOWER(TRIM(a2.name)) LIKE LOWER(co2.name) || '%')
          AND LENGTH(TRIM(a2.name)) >= 4
        WHERE a2.company_id IS NULL
        ORDER BY a2.id, LENGTH(co2.name)
      ) co
      WHERE accounts.id = co.account_id
    `);
    if (fuzzyLinked > 0) console.log(`  ✅ Linked ${fuzzyLinked} clients to companies by fuzzy name`);

    // Flag companies with revenue as is_client (same tenant only — don't leak across tenants)
    const { rowCount } = await platformPool.query(`
      UPDATE companies c SET is_client = true
      WHERE c.is_client = false AND c.id IN (
        SELECT DISTINCT a.company_id FROM accounts a
        JOIN conversions conv ON conv.client_id = a.id
        WHERE a.company_id IS NOT NULL AND a.tenant_id = c.tenant_id
      )
    `);
    if (rowCount > 0) console.log(`  ✅ ${rowCount} companies marked as clients (invoiced via Xero)`);

    // Direct fallback: flag companies matching conversion client_name_raw
    const { rowCount: directFlagged } = await platformPool.query(`
      UPDATE companies c SET is_client = true
      WHERE c.is_client = false AND c.tenant_id IS NOT NULL
        AND EXISTS (
          SELECT 1 FROM conversions conv
          WHERE conv.tenant_id = c.tenant_id
            AND conv.placement_fee > 0
            AND LOWER(TRIM(conv.client_name_raw)) = LOWER(TRIM(c.name))
        )
    `);
    if (directFlagged > 0) console.log(`  ✅ ${directFlagged} companies marked as clients (direct name match from conversions)`);
  } catch (e) {
    console.log('  ⚠️ Client backfill skipped:', e.message);
  }

  // LinkedIn imports moved to admin UI / worker pipeline — not run on web startup

  // Backfill: create team_proximity for LinkedIn-imported people missing proximity links
  try {
    const { rowCount: proxCreated } = await db.query(`
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

  // Interaction backfills + session purge moved to worker pipeline — not run on web startup

  // WIP ingestion moved to admin pipeline — not run on web startup

  // Work artifacts schema
  try {
    const artifactSql = require('fs').readFileSync(require('path').join(__dirname, 'sql/migration_work_artifacts.sql'), 'utf8');
    await db.query(artifactSql);
    console.log('  ✅ Work artifacts schema ready');
  } catch (e) {
    if (!e.message.includes('already exists')) console.error('  Work artifacts migration:', e.message);
  }

  // Lead engine schema (polarity, lifecycle, claims, outcomes, relationship_state)
  const leadEngineMigrations = [
    'sql/migration_signal_polarity.sql',
    'sql/migration_companies_relationship_state.sql',
    'sql/migration_lead_claims.sql',
  ];
  for (const migPath of leadEngineMigrations) {
    try {
      const sql = require('fs').readFileSync(require('path').join(__dirname, migPath), 'utf8');
      await db.query(sql);
      console.log(`  ✅ ${migPath.split('/').pop()} applied`);
    } catch (e) {
      if (!e.message.includes('already exists')) console.error(`  ${migPath}:`, e.message);
    }
  }

  } catch (e) { console.error('Startup migration error:', e.message); }
  }, 5000); // 5s delay — let healthcheck pass first
});