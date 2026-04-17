// ═══════════════════════════════════════════════════════════════════════════
// lib/onboarding/healthMonitor.js — Proactive Tenant Health Monitoring
// ═══════════════════════════════════════════════════════════════════════════
//
// CROSS-TENANT: Uses platformPool intentionally for runHealthCheckAllTenants()
// which iterates all tenants. Individual tenant checks use TenantDB.
// platformPool also used for pipeline_runs logging (cross-tenant table).
//
// Runs health checks across integrations, signal feeds, network intelligence,
// and data quality. Surfaces issues with one-click resolutions before the
// tenant notices a problem.

const { TenantDB, platformPool } = require('../TenantDB');

// ═══════════════════════════════════════════════════════════════════════════
// HEALTH CHECKS
// ═══════════════════════════════════════════════════════════════════════════

async function runHealthCheck(tenantId) {
  const db = new TenantDB(tenantId);
  const issues = [];

  // ─── Check 1: Token expiry ─────────────────────────────────────────────
  try {
    const { rows: tokens } = await db.query(`
      SELECT id, google_email, token_expires_at, sync_enabled,
        CASE
          WHEN token_expires_at IS NULL THEN 'unknown'
          WHEN token_expires_at < NOW() THEN 'expired'
          WHEN token_expires_at < NOW() + INTERVAL '24 hours' THEN 'expiring_soon'
          ELSE 'ok'
        END AS token_status
      FROM user_google_accounts
      WHERE tenant_id = $1 AND sync_enabled = true
    `, [tenantId]);

    for (const t of tokens) {
      if (t.token_status === 'expired') {
        issues.push({
          type: 'token_expired',
          severity: 'error',
          title: 'Gmail connection expired',
          explanation: `The connection for ${t.google_email} has expired and syncing has stopped.`,
          impact: 'No new emails or contacts are being synced from this account.',
          actions: [
            { id: 'reconnect', label: 'Reconnect now', recommended: true, auto_resolve: true },
          ],
          affected_count: 1,
          context: { google_email: t.google_email, connection_id: t.id },
        });
      } else if (t.token_status === 'expiring_soon') {
        issues.push({
          type: 'token_expiring_soon',
          severity: 'warning',
          title: 'Gmail token expires soon',
          explanation: `The connection for ${t.google_email} will expire within 24 hours.`,
          impact: 'Syncing will stop once the token expires.',
          actions: [
            { id: 'reconnect', label: 'Reconnect now', recommended: true, auto_resolve: true },
            { id: 'dismiss', label: "I'll do this later", recommended: false },
          ],
          affected_count: 1,
          context: { google_email: t.google_email, connection_id: t.id },
        });
      }
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 2: Sync staleness ───────────────────────────────────────────
  try {
    const { rows: stale } = await db.query(`
      SELECT google_email, gmail_last_sync_at,
        EXTRACT(EPOCH FROM (NOW() - gmail_last_sync_at)) / 3600 AS hours_since
      FROM user_google_accounts
      WHERE tenant_id = $1 AND sync_enabled = true
        AND gmail_last_sync_at IS NOT NULL
        AND gmail_last_sync_at < NOW() - INTERVAL '3 days'
    `, [tenantId]);

    if (stale.length > 0) {
      const emails = stale.map(s => s.google_email).join(', ');
      const maxHours = Math.max(...stale.map(s => Math.round(s.hours_since)));
      issues.push({
        type: 'sync_stale',
        severity: 'warning',
        title: 'Gmail sync hasn\'t run recently',
        explanation: `${stale.length} account(s) haven't synced in over 3 days: ${emails}`,
        impact: `Up to ${maxHours} hours of email interactions may be missing from proximity scores.`,
        actions: [
          { id: 'retry', label: 'Trigger sync now', recommended: true, auto_resolve: true },
          { id: 'dismiss', label: 'Dismiss', recommended: false },
        ],
        affected_count: stale.length,
        context: { stale_accounts: stale.map(s => s.google_email) },
      });
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 3: Duplicate accumulation ───────────────────────────────────
  try {
    const { rows: [dupes] } = await db.query(`
      SELECT COUNT(*) AS cnt FROM (
        SELECT LOWER(email) AS em FROM people
        WHERE tenant_id = $1 AND email IS NOT NULL AND email != ''
        GROUP BY LOWER(email) HAVING COUNT(*) > 1
      ) d
    `, [tenantId]);

    const dupeCount = parseInt(dupes.cnt) || 0;
    if (dupeCount > 50) {
      issues.push({
        type: 'duplicates_detected',
        severity: 'warning',
        title: `${dupeCount} possible duplicate contacts`,
        explanation: `${dupeCount} email addresses appear on more than one person record.`,
        impact: 'Duplicate records can split proximity scores and create inconsistent signal routing.',
        actions: [
          { id: 'merge', label: 'Review and merge duplicates', recommended: true },
          { id: 'dismiss', label: 'Ignore for now', recommended: false },
        ],
        affected_count: dupeCount,
        context: { duplicate_emails: dupeCount },
      });
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 4: Signal feed yield ────────────────────────────────────────
  try {
    const { rows: [signalCount] } = await db.query(`
      SELECT COUNT(*) AS cnt
      FROM signal_events
      WHERE (tenant_id IS NULL OR tenant_id = $1)
        AND detected_at > NOW() - INTERVAL '7 days'
    `, [tenantId]);

    const weeklySignals = parseInt(signalCount.cnt) || 0;
    if (weeklySignals === 0) {
      issues.push({
        type: 'feed_dry',
        severity: 'error',
        title: 'No signals in the last 7 days',
        explanation: 'No new signals have been detected this week. The signal pipeline may be stalled.',
        impact: 'Deal intelligence and market health will not update until signals resume.',
        actions: [
          { id: 'check_feeds', label: 'Check feed sources', recommended: true },
          { id: 'contact_support', label: 'Get help', recommended: false },
        ],
        affected_count: 0,
        context: { weekly_signals: weeklySignals },
      });
    } else if (weeklySignals < 50) {
      issues.push({
        type: 'feed_low_yield',
        severity: 'info',
        title: 'Low signal volume this week',
        explanation: `Only ${weeklySignals} signals detected in the last 7 days. This is lower than typical.`,
        impact: 'Deal intelligence may be less comprehensive than usual.',
        actions: [
          { id: 'add_feeds', label: 'Add more signal feeds', recommended: true },
          { id: 'dismiss', label: 'This is expected', recommended: false },
        ],
        affected_count: weeklySignals,
        context: { weekly_signals: weeklySignals },
      });
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 5: Embedding lag ────────────────────────────────────────────
  try {
    const { rows: [embedLag] } = await db.query(`
      SELECT
        COUNT(*) FILTER (WHERE embedded_at IS NULL AND created_at < NOW() - INTERVAL '48 hours') AS stale_people,
        COUNT(*) AS total_people
      FROM people WHERE tenant_id = $1
    `, [tenantId]);

    const stalePeople = parseInt(embedLag.stale_people) || 0;
    if (stalePeople > 100) {
      issues.push({
        type: 'embedding_lag',
        severity: 'warning',
        title: `${stalePeople} people not yet searchable`,
        explanation: `${stalePeople} contacts imported more than 48 hours ago still haven't been embedded for semantic search.`,
        impact: 'These contacts won\'t appear in search results or AI-powered matching.',
        actions: [
          { id: 'trigger_embed', label: 'Run embedding now', recommended: true, auto_resolve: true },
          { id: 'dismiss', label: 'Dismiss', recommended: false },
        ],
        affected_count: stalePeople,
        context: { stale_people: stalePeople, total_people: parseInt(embedLag.total_people) },
      });
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 6: Proximity staleness ──────────────────────────────────────
  try {
    const { rows: [lastProx] } = await platformPool.query(`
      SELECT MAX(completed_at) AS last_run
      FROM pipeline_runs
      WHERE pipeline_key IN ('proximity', 'compute_network_topology')
        AND status = 'completed'
        AND (tenant_id = $1 OR tenant_id IS NULL)
    `, [tenantId]);

    if (lastProx.last_run) {
      const hoursSince = (Date.now() - new Date(lastProx.last_run).getTime()) / (1000 * 60 * 60);
      if (hoursSince > 48) {
        issues.push({
          type: 'proximity_stale',
          severity: 'info',
          title: 'Network proximity needs refresh',
          explanation: `Proximity scores were last computed ${Math.round(hoursSince)} hours ago.`,
          impact: 'Signal ranking by network density may not reflect recent interactions.',
          actions: [
            { id: 'trigger_proximity', label: 'Recompute now', recommended: true, auto_resolve: true },
            { id: 'dismiss', label: 'Dismiss', recommended: false },
          ],
          context: { hours_since: Math.round(hoursSince), last_run: lastProx.last_run },
        });
      }
    }
  } catch (e) { /* non-fatal */ }

  // ─── Check 7: Unmatched interactions ───────────────────────────────────
  try {
    const { rows: [unmatched] } = await db.query(`
      SELECT
        COUNT(*) FILTER (WHERE person_id IS NULL) AS unmatched,
        COUNT(*) AS total
      FROM interactions
      WHERE tenant_id = $1 AND source = 'gmail_sync'
    `, [tenantId]);

    const unmatchedCount = parseInt(unmatched.unmatched) || 0;
    const totalCount = parseInt(unmatched.total) || 0;
    const unmatchedPct = totalCount > 0 ? Math.round((unmatchedCount / totalCount) * 100) : 0;

    if (unmatchedCount > 100 && unmatchedPct > 30) {
      issues.push({
        type: 'unmatched_interactions',
        severity: 'info',
        title: `${unmatchedPct}% of email interactions unmatched`,
        explanation: `${unmatchedCount} of ${totalCount} Gmail interactions couldn't be linked to a contact.`,
        impact: 'These interactions don\'t contribute to proximity scores or signal ranking.',
        actions: [
          { id: 'create_contacts', label: 'Auto-create contacts from emails', recommended: true },
          { id: 'dismiss', label: 'Dismiss', recommended: false },
        ],
        affected_count: unmatchedCount,
        context: { unmatched: unmatchedCount, total: totalCount, pct: unmatchedPct },
      });
    }
  } catch (e) { /* non-fatal */ }

  // Sort by severity: error > warning > info
  const severityOrder = { error: 0, warning: 1, info: 2 };
  issues.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════
// INTEGRATION STATUS SUMMARY
// ═══════════════════════════════════════════════════════════════════════════

async function getIntegrationStatus(tenantId) {
  const db = new TenantDB(tenantId);
  const integrations = [];

  try {
    const { rows } = await db.query(`
      SELECT google_email, sync_enabled, gmail_last_sync_at, contacts_last_sync_at,
        token_expires_at, emails_synced
      FROM user_google_accounts WHERE tenant_id = $1
    `, [tenantId]);

    for (const g of rows) {
      const isExpired = g.token_expires_at && new Date(g.token_expires_at) < new Date();
      const lastSync = g.gmail_last_sync_at;
      const hoursSince = lastSync ? (Date.now() - new Date(lastSync).getTime()) / (1000 * 60 * 60) : null;

      integrations.push({
        type: 'gmail',
        name: 'Gmail',
        account: g.google_email,
        status: isExpired ? 'error' : (hoursSince && hoursSince > 72 ? 'warning' : 'ok'),
        status_label: isExpired ? 'Token expired' : (lastSync ? `Last sync: ${formatAgo(lastSync)}` : 'Never synced'),
        details: { emails_synced: g.emails_synced, sync_enabled: g.sync_enabled },
      });
    }
  } catch (e) { /* non-fatal */ }

  return integrations;
}

function formatAgo(date) {
  const ms = Date.now() - new Date(date).getTime();
  const hours = Math.floor(ms / (1000 * 60 * 60));
  if (hours < 1) return 'just now';
  if (hours < 24) return hours + ' hour' + (hours !== 1 ? 's' : '') + ' ago';
  const days = Math.floor(hours / 24);
  return days + ' day' + (days !== 1 ? 's' : '') + ' ago';
}

// ═══════════════════════════════════════════════════════════════════════════
// ALL-TENANT HEALTH CHECK (CRON)
// ═══════════════════════════════════════════════════════════════════════════

async function runHealthCheckAllTenants() {
  console.log('[HealthMonitor] Running health checks for all tenants');
  const startTime = Date.now();

  try {
    const { rows: tenants } = await platformPool.query(
      'SELECT id, name FROM tenants'
    );

    let totalIssues = 0;
    for (const tenant of tenants) {
      try {
        const issues = await runHealthCheck(tenant.id);
        totalIssues += issues.length;

        // Upsert issues into tenant_health_issues
        for (const issue of issues) {
          await platformPool.query(`
            INSERT INTO tenant_health_issues
              (tenant_id, issue_type, severity, title, explanation, impact, actions,
               affected_count, context, resolved, last_checked_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false, NOW())
            ON CONFLICT (tenant_id, issue_type) DO UPDATE SET
              severity = EXCLUDED.severity, title = EXCLUDED.title,
              explanation = EXCLUDED.explanation, impact = EXCLUDED.impact,
              actions = EXCLUDED.actions, affected_count = EXCLUDED.affected_count,
              context = EXCLUDED.context, last_checked_at = NOW(),
              resolved = false
          `, [
            tenant.id, issue.type, issue.severity, issue.title,
            issue.explanation, issue.impact || null, JSON.stringify(issue.actions),
            issue.affected_count || null, JSON.stringify(issue.context || {}),
          ]);
        }

        // Clear resolved issues that no longer appear
        const activeTypes = issues.map(i => i.type);
        if (activeTypes.length > 0) {
          await platformPool.query(`
            UPDATE tenant_health_issues SET resolved = true, resolved_at = NOW()
            WHERE tenant_id = $1 AND resolved = false AND issue_type != ALL($2)
          `, [tenant.id, activeTypes]);
        } else {
          await platformPool.query(`
            UPDATE tenant_health_issues SET resolved = true, resolved_at = NOW()
            WHERE tenant_id = $1 AND resolved = false
          `, [tenant.id]);
        }

        if (issues.length > 0) {
          console.log(`  [${tenant.name}] ${issues.length} issues: ${issues.map(i => i.type).join(', ')}`);
        }
      } catch (e) {
        console.error(`  [${tenant.name}] Health check failed: ${e.message}`);
      }
    }

    const duration = Date.now() - startTime;
    console.log(`[HealthMonitor] Done: ${tenants.length} tenants, ${totalIssues} issues, ${duration}ms`);

    // Log to pipeline_runs
    try {
      await platformPool.query(`
        INSERT INTO pipeline_runs (pipeline_key, pipeline_name, status, started_at, completed_at,
          duration_ms, items_processed, triggered_by)
        VALUES ('health_check', 'Health Monitor', 'completed', $1, NOW(), $2, $3, 'cron')
      `, [new Date(startTime), duration, totalIssues]);
    } catch (e) { /* non-fatal */ }

    return { tenants: tenants.length, issues: totalIssues, duration_ms: duration };
  } catch (e) {
    console.error('[HealthMonitor] Fatal:', e.message);
    return { error: e.message };
  }
}

module.exports = { runHealthCheck, getIntegrationStatus, runHealthCheckAllTenants };
