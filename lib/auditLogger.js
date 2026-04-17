// ═══════════════════════════════════════════════════════════════════════════════
// lib/auditLogger.js — Security audit logging (fire-and-forget)
// PIPELINE-CONTEXT: Uses pool.query intentionally — audit_logs is a platform
// table for security events. Includes tenant_id parameter but not RLS-gated.
// Never blocks requests. Never throws. Errors logged to console only.
// ═══════════════════════════════════════════════════════════════════════════════

const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 3 });

async function auditLog(event) {
  try {
    await pool.query(`
      INSERT INTO audit_logs (tenant_id, user_id, user_email, ip_address, user_agent,
        event_type, resource_type, resource_id, action, metadata, outcome, failure_reason)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)
    `, [
      event.tenantId || null, event.userId || null, event.userEmail || null,
      event.ipAddress || null, event.userAgent || null,
      event.eventType, event.resourceType || null, event.resourceId || null,
      event.action || null, JSON.stringify(event.metadata || {}),
      event.outcome || 'success', event.failureReason || null,
    ]);
  } catch (err) { console.error('[audit] Write failed:', err.message); }
}

function fromRequest(req) {
  return {
    tenantId: req.tenant_id,
    userId: req.user?.user_id,
    userEmail: req.user?.email,
    ipAddress: (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket?.remoteAddress,
    userAgent: (req.headers['user-agent'] || '').slice(0, 200),
  };
}

var audit = {
  loginSuccess: function(req, userId, email) { return auditLog({ ...fromRequest(req), userId: userId, userEmail: email, eventType: 'login_success', action: 'create', resourceType: 'session' }); },
  loginFailed: function(req, email, reason) { return auditLog({ ...fromRequest(req), userEmail: email, eventType: 'login_failed', outcome: 'failed', failureReason: reason, action: 'create', resourceType: 'session' }); },
  logout: function(req) { return auditLog({ ...fromRequest(req), eventType: 'logout', action: 'delete', resourceType: 'session' }); },
  invalidToken: function(req) { return auditLog({ ...fromRequest(req), eventType: 'invalid_token', outcome: 'blocked', action: 'read', resourceType: 'session' }); },
  personViewed: function(req, personId) { return auditLog({ ...fromRequest(req), eventType: 'person_viewed', action: 'read', resourceType: 'person', resourceId: personId }); },
  signalTriaged: function(req, signalId, triageAction) { return auditLog({ ...fromRequest(req), eventType: 'signal_triaged', action: 'update', resourceType: 'signal', resourceId: signalId, metadata: { triage_action: triageAction } }); },
  gmailConnected: function(req, accountEmail) { return auditLog({ ...fromRequest(req), eventType: 'gmail_connected', action: 'create', resourceType: 'oauth_account', metadata: { account_email: accountEmail } }); },
  linkedinImported: function(req, count) { return auditLog({ ...fromRequest(req), eventType: 'linkedin_imported', action: 'create', resourceType: 'network_data', metadata: { connection_count: count } }); },
  bundleSubscribed: function(req, slug) { return auditLog({ ...fromRequest(req), eventType: 'bundle_subscribed', action: 'create', resourceType: 'feed_bundle', metadata: { bundle_slug: slug } }); },
  huddleJoined: function(req, huddleId, count) { return auditLog({ ...fromRequest(req), eventType: 'huddle_joined', action: 'create', resourceType: 'huddle', resourceId: huddleId, metadata: { contributed_contacts: count } }); },
  huddleExited: function(req, huddleId, count) { return auditLog({ ...fromRequest(req), eventType: 'huddle_exited', action: 'delete', resourceType: 'huddle', resourceId: huddleId, metadata: { removed_contacts: count } }); },
  rateLimitHit: function(req, endpoint) { return auditLog({ ...fromRequest(req), eventType: 'rate_limit_hit', outcome: 'blocked', metadata: { endpoint: endpoint } }); },
};

module.exports = { auditLog, audit };
