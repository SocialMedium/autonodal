#!/usr/bin/env node
/**
 * scripts/watchdog.js — Platform Health Watchdog
 *
 * Monitors all critical platform functions on a schedule:
 *   1. Pipeline freshness — did each pipeline run on time?
 *   2. Data freshness    — are signals, embeddings, scores current?
 *   3. External services — Qdrant, OpenAI, Anthropic reachable?
 *   4. RSS feed health   — any sources with consecutive errors?
 *   5. Embedding coverage — are new records getting embedded?
 *   6. Volume anomalies  — sudden drops in processing volume?
 *
 * Sends email alerts via Resend when issues are detected.
 * Logs results to pipeline_runs as 'watchdog' for observability.
 *
 * Usage:
 *   node scripts/watchdog.js              # Run once, print report
 *   node scripts/watchdog.js --json       # Output JSON
 *   node scripts/watchdog.js --email      # Run + send alert email if issues found
 *
 * Called by scheduler every 2 hours as a registered pipeline.
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3
});

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE SCHEDULE EXPECTATIONS
// How long can each pipeline go without running before we flag it?
// ═══════════════════════════════════════════════════════════════════════════════

const PIPELINE_SLA = {
  // Core intelligence loop — must run frequently
  ingest_signals:        { max_gap_hours: 2,   severity: 'critical', label: 'Signal Ingestion' },
  compute_signal_index:  { max_gap_hours: 2,   severity: 'critical', label: 'Signal Index' },
  compute_scores:        { max_gap_hours: 3,   severity: 'critical', label: 'Score Computation' },

  // Regular processing — every few hours
  embed_intelligence:    { max_gap_hours: 8,   severity: 'high',     label: 'Embed Intelligence' },
  compute_triangulation: { max_gap_hours: 6,   severity: 'high',     label: 'Triangulation' },
  signal_dispatch:       { max_gap_hours: 6,   severity: 'high',     label: 'Signal Dispatch' },
  harvest_events:        { max_gap_hours: 6,   severity: 'medium',   label: 'Event Harvest' },
  gmail_match:           { max_gap_hours: 6,   severity: 'medium',   label: 'Gmail Match' },
  classify_documents:    { max_gap_hours: 6,   severity: 'medium',   label: 'Doc Classification' },

  // Periodic syncs — every 4-6 hours
  match_searches:        { max_gap_hours: 12,  severity: 'medium',   label: 'Search Matching' },
  enrich_content:        { max_gap_hours: 10,  severity: 'medium',   label: 'Content Enrichment' },
  sync_gmail:            { max_gap_hours: 10,  severity: 'medium',   label: 'Gmail Sync' },
  sync_drive:            { max_gap_hours: 8,   severity: 'medium',   label: 'Drive Sync' },
  sync_calendar:         { max_gap_hours: 10,  severity: 'medium',   label: 'Calendar Sync' },
  sync_contacts:         { max_gap_hours: 14,  severity: 'low',      label: 'Contacts Sync' },
  sync_telegram:         { max_gap_hours: 10,  severity: 'low',      label: 'Telegram Sync' },
  ingest_events:         { max_gap_hours: 10,  severity: 'low',      label: 'EventMedium Ingest' },
  detect_rel_changes:    { max_gap_hours: 14,  severity: 'low',      label: 'Relationship Changes' },

  // Daily jobs — 36h max gap (gives buffer for timezone/timing)
  daily_brief:               { max_gap_hours: 36, severity: 'high',   label: 'Daily Brief' },
  network_insights:          { max_gap_hours: 36, severity: 'medium', label: 'Network Insights' },
  company_relationships:     { max_gap_hours: 36, severity: 'low',    label: 'Company Relationships' },
  compute_network_topology:  { max_gap_hours: 36, severity: 'low',    label: 'Network Topology' },
  compute_signal_grabs:      { max_gap_hours: 36, severity: 'medium', label: 'Signal Grabs' },
  harvest_podcasts:          { max_gap_hours: 36, severity: 'low',    label: 'Podcast Harvest' },
  extract_companies:         { max_gap_hours: 36, severity: 'low',    label: 'Company Extraction' },
  waitlist_digest:           { max_gap_hours: 36, severity: 'low',    label: 'Waitlist Digest' },

  // Weekly jobs — 8 day max gap
  weekly_wrap:           { max_gap_hours: 192, severity: 'low',    label: 'Weekly Wrap' },
  daily_digest_email:    { max_gap_hours: 192, severity: 'low',    label: 'Weekly Digest Email' },

  // Xero — twice daily
  sync_xero:             { max_gap_hours: 18,  severity: 'medium', label: 'Xero Sync' },
};

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 1: PIPELINE FRESHNESS
// ═══════════════════════════════════════════════════════════════════════════════

async function checkPipelineFreshness() {
  const issues = [];

  const { rows } = await pool.query(`
    SELECT pipeline_key,
           MAX(started_at) AS last_run,
           MAX(CASE WHEN status = 'complete' THEN started_at END) AS last_success,
           COUNT(*) FILTER (WHERE status IN ('error','failed') AND started_at > NOW() - INTERVAL '24 hours') AS errors_24h,
           COUNT(*) FILTER (WHERE status = 'complete' AND started_at > NOW() - INTERVAL '24 hours') AS success_24h
    FROM pipeline_runs
    WHERE started_at > NOW() - INTERVAL '14 days'
    GROUP BY pipeline_key
  `);

  const pipelineMap = {};
  rows.forEach(r => { pipelineMap[r.pipeline_key] = r; });

  for (const [key, sla] of Object.entries(PIPELINE_SLA)) {
    const run = pipelineMap[key];

    if (!run || !run.last_success) {
      issues.push({
        check: 'pipeline_freshness',
        pipeline: key,
        label: sla.label,
        severity: sla.severity,
        message: `Never completed successfully (or no runs in 14 days)`,
        hours_overdue: null
      });
      continue;
    }

    const hoursSinceSuccess = (Date.now() - new Date(run.last_success).getTime()) / (1000 * 60 * 60);
    if (hoursSinceSuccess > sla.max_gap_hours) {
      issues.push({
        check: 'pipeline_freshness',
        pipeline: key,
        label: sla.label,
        severity: sla.severity,
        message: `Last success ${hoursSinceSuccess.toFixed(1)}h ago (SLA: ${sla.max_gap_hours}h)`,
        hours_overdue: +(hoursSinceSuccess - sla.max_gap_hours).toFixed(1),
        last_success: run.last_success
      });
    }

    // Flag high error rates
    const errors24 = parseInt(run.errors_24h || 0);
    const success24 = parseInt(run.success_24h || 0);
    if (errors24 > 0 && errors24 >= success24) {
      issues.push({
        check: 'pipeline_errors',
        pipeline: key,
        label: sla.label,
        severity: errors24 > 3 ? 'high' : 'medium',
        message: `${errors24} errors vs ${success24} successes in last 24h`,
        errors_24h: errors24,
        success_24h: success24
      });
    }
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 2: DATA FRESHNESS
// ═══════════════════════════════════════════════════════════════════════════════

async function checkDataFreshness() {
  const issues = [];

  // Signal events — should have new ones daily
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '24 hours') AS signals_24h,
        COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '7 days') AS signals_7d,
        MAX(detected_at) AS latest_signal
      FROM signal_events
    `);
    const r = rows[0];
    const signals24 = parseInt(r.signals_24h || 0);
    const signals7d = parseInt(r.signals_7d || 0);

    if (signals24 === 0) {
      issues.push({
        check: 'data_freshness',
        metric: 'signals_24h',
        severity: 'critical',
        message: `No new signals in 24h (last: ${r.latest_signal || 'never'})`,
        value: 0
      });
    } else if (signals24 < 5) {
      issues.push({
        check: 'data_freshness',
        metric: 'signals_24h',
        severity: 'high',
        message: `Only ${signals24} signals in 24h (usually expect 20+)`,
        value: signals24
      });
    }
  } catch (e) {
    issues.push({ check: 'data_freshness', metric: 'signals_24h', severity: 'critical', message: 'Cannot query signal_events: ' + e.message });
  }

  // Documents — RSS harvest should produce new docs
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS docs_24h,
        COUNT(*) FILTER (WHERE embedded_at IS NULL AND processing_status != 'error') AS unembedded,
        MAX(created_at) AS latest_doc
      FROM external_documents
    `);
    const r = rows[0];
    const docs24 = parseInt(r.docs_24h || 0);
    const unembedded = parseInt(r.unembedded || 0);

    if (docs24 === 0) {
      issues.push({
        check: 'data_freshness',
        metric: 'documents_24h',
        severity: 'high',
        message: `No new documents in 24h (last: ${r.latest_doc || 'never'}). RSS harvest may be stalled.`,
        value: 0
      });
    }

    if (unembedded > 100) {
      issues.push({
        check: 'embedding_backlog',
        metric: 'unembedded_docs',
        severity: 'medium',
        message: `${unembedded} documents waiting for embedding`,
        value: unembedded
      });
    }
  } catch (e) {
    issues.push({ check: 'data_freshness', metric: 'documents_24h', severity: 'high', message: 'Cannot query external_documents: ' + e.message });
  }

  // Person scores — should be computed regularly
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) AS total_people,
        COUNT(ps.id) AS scored_people,
        MAX(ps.updated_at) AS latest_score
      FROM people p
      LEFT JOIN person_scores ps ON ps.person_id = p.id
      WHERE p.tenant_id IS NOT NULL
    `);
    const r = rows[0];
    const total = parseInt(r.total_people || 0);
    const scored = parseInt(r.scored_people || 0);

    if (total > 0 && scored < total * 0.5) {
      issues.push({
        check: 'data_freshness',
        metric: 'score_coverage',
        severity: 'medium',
        message: `Only ${scored}/${total} people scored (${(scored/total*100).toFixed(0)}%)`,
        value: scored
      });
    }

    if (r.latest_score) {
      const hoursSinceScore = (Date.now() - new Date(r.latest_score).getTime()) / (1000 * 60 * 60);
      if (hoursSinceScore > 6) {
        issues.push({
          check: 'data_freshness',
          metric: 'scores_stale',
          severity: 'high',
          message: `Scores last updated ${hoursSinceScore.toFixed(1)}h ago`,
          value: hoursSinceScore
        });
      }
    }
  } catch (e) {
    // person_scores may not exist yet for all tenants — warn, don't crit
    issues.push({ check: 'data_freshness', metric: 'score_coverage', severity: 'low', message: 'Cannot query person_scores: ' + e.message });
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 3: EXTERNAL SERVICE HEALTH
// ═══════════════════════════════════════════════════════════════════════════════

async function checkExternalServices() {
  const issues = [];

  // PostgreSQL — if we got here it's alive, but check table access
  try {
    await pool.query('SELECT 1');
  } catch (e) {
    issues.push({ check: 'service_health', service: 'postgresql', severity: 'critical', message: 'Database unreachable: ' + e.message });
    return issues; // No point continuing
  }

  // Qdrant
  if (process.env.QDRANT_URL) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const res = await fetch(process.env.QDRANT_URL + '/collections', {
        headers: { 'api-key': process.env.QDRANT_API_KEY || '' },
        signal: controller.signal
      });
      clearTimeout(timeout);

      if (!res.ok) {
        issues.push({ check: 'service_health', service: 'qdrant', severity: 'critical', message: `Qdrant returned ${res.status}` });
      } else {
        const data = await res.json();
        const collections = data.result?.collections || [];
        const expected = ['people', 'documents', 'signals', 'companies'];
        const missing = expected.filter(c => !collections.find(col => col.name === c));
        if (missing.length > 0) {
          issues.push({ check: 'service_health', service: 'qdrant', severity: 'high', message: `Missing collections: ${missing.join(', ')}` });
        }

        // Check vector counts
        for (const coll of expected.filter(c => !missing.includes(c))) {
          try {
            const cr = await fetch(process.env.QDRANT_URL + '/collections/' + coll, {
              headers: { 'api-key': process.env.QDRANT_API_KEY || '' }
            });
            const cd = await cr.json();
            const count = cd.result?.points_count || 0;
            if (count === 0) {
              issues.push({ check: 'service_health', service: 'qdrant', severity: 'high', message: `Collection '${coll}' has 0 vectors` });
            }
          } catch (e) {}
        }
      }
    } catch (e) {
      issues.push({ check: 'service_health', service: 'qdrant', severity: 'critical', message: 'Qdrant unreachable: ' + e.message });
    }
  } else {
    issues.push({ check: 'service_health', service: 'qdrant', severity: 'critical', message: 'QDRANT_URL not configured' });
  }

  // OpenAI — lightweight check (list models)
  if (process.env.OPENAI_API_KEY) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const res = await fetch('https://api.openai.com/v1/models', {
        headers: { 'Authorization': 'Bearer ' + process.env.OPENAI_API_KEY },
        signal: controller.signal
      });
      clearTimeout(timeout);
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        issues.push({ check: 'service_health', service: 'openai', severity: 'critical', message: `OpenAI API returned ${res.status}: ${body.slice(0, 100)}` });
      }
    } catch (e) {
      issues.push({ check: 'service_health', service: 'openai', severity: 'critical', message: 'OpenAI unreachable: ' + e.message });
    }
  } else {
    issues.push({ check: 'service_health', service: 'openai', severity: 'critical', message: 'OPENAI_API_KEY not configured' });
  }

  // Anthropic — lightweight check
  if (process.env.ANTHROPIC_API_KEY) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 1, messages: [{ role: 'user', content: 'ping' }] }),
        signal: controller.signal
      });
      clearTimeout(timeout);
      // 200 = working, 429 = rate limited but key works, anything else = problem
      if (!res.ok && res.status !== 429) {
        issues.push({ check: 'service_health', service: 'anthropic', severity: 'critical', message: `Anthropic API returned ${res.status}` });
      }
    } catch (e) {
      issues.push({ check: 'service_health', service: 'anthropic', severity: 'high', message: 'Anthropic unreachable: ' + e.message });
    }
  } else {
    issues.push({ check: 'service_health', service: 'anthropic', severity: 'critical', message: 'ANTHROPIC_API_KEY not configured' });
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 4: RSS FEED HEALTH
// ═══════════════════════════════════════════════════════════════════════════════

async function checkRSSFeeds() {
  const issues = [];

  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) AS total_sources,
        COUNT(*) FILTER (WHERE enabled = true) AS enabled_sources,
        COUNT(*) FILTER (WHERE enabled = true AND consecutive_errors >= 3) AS failing_sources,
        COUNT(*) FILTER (WHERE enabled = true AND last_fetched_at < NOW() - INTERVAL '48 hours') AS stale_sources,
        COUNT(*) FILTER (WHERE enabled = true AND last_fetched_at IS NULL) AS never_fetched,
        ARRAY_AGG(DISTINCT source_name ORDER BY source_name) FILTER (WHERE enabled = true AND consecutive_errors >= 5) AS broken_feeds
      FROM rss_sources
    `);
    const r = rows[0];
    const failing = parseInt(r.failing_sources || 0);
    const enabled = parseInt(r.enabled_sources || 0);
    const stale = parseInt(r.stale_sources || 0);

    if (failing > 0) {
      issues.push({
        check: 'rss_health',
        severity: failing > 5 ? 'high' : 'medium',
        message: `${failing}/${enabled} RSS sources have 3+ consecutive errors`,
        broken_feeds: r.broken_feeds || [],
        value: failing
      });
    }

    if (stale > enabled * 0.3 && enabled > 0) {
      issues.push({
        check: 'rss_health',
        severity: 'high',
        message: `${stale}/${enabled} RSS sources not fetched in 48h`,
        value: stale
      });
    }
  } catch (e) {
    issues.push({ check: 'rss_health', severity: 'medium', message: 'Cannot query rss_sources: ' + e.message });
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 5: EMBEDDING COVERAGE
// ═══════════════════════════════════════════════════════════════════════════════

async function checkEmbeddingCoverage() {
  const issues = [];

  // Qdrant collection counts vs DB entity counts
  if (!process.env.QDRANT_URL) return issues;

  const checks = [
    { collection: 'people', query: "SELECT COUNT(*) AS cnt FROM people WHERE tenant_id IS NOT NULL", min_ratio: 0.3 },
    { collection: 'signals', query: "SELECT COUNT(*) AS cnt FROM signal_events WHERE detected_at > NOW() - INTERVAL '90 days'", min_ratio: 0.2 },
    { collection: 'documents', query: "SELECT COUNT(*) AS cnt FROM external_documents WHERE embedded_at IS NOT NULL", min_ratio: 0.8 },
  ];

  for (const check of checks) {
    try {
      const dbResult = await pool.query(check.query);
      const dbCount = parseInt(dbResult.rows[0].cnt || 0);

      const res = await fetch(process.env.QDRANT_URL + '/collections/' + check.collection, {
        headers: { 'api-key': process.env.QDRANT_API_KEY || '' }
      });
      const data = await res.json();
      const vectorCount = data.result?.points_count || 0;

      if (dbCount > 100 && vectorCount < dbCount * check.min_ratio) {
        issues.push({
          check: 'embedding_coverage',
          collection: check.collection,
          severity: 'medium',
          message: `${check.collection}: ${vectorCount} vectors vs ${dbCount} DB records (${(vectorCount/dbCount*100).toFixed(0)}%)`,
          db_count: dbCount,
          vector_count: vectorCount
        });
      }
    } catch (e) {
      // Skip silently — service checks will catch connectivity issues
    }
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CHECK 6: VOLUME ANOMALIES (sudden drops)
// ═══════════════════════════════════════════════════════════════════════════════

async function checkVolumeAnomalies() {
  const issues = [];

  try {
    const { rows } = await pool.query(`
      WITH daily AS (
        SELECT pipeline_key,
               DATE(started_at) AS run_date,
               COUNT(*) FILTER (WHERE status = 'complete') AS completions,
               SUM((items_processed->>'count')::int) FILTER (WHERE status = 'complete' AND items_processed ? 'count') AS items
        FROM pipeline_runs
        WHERE started_at > NOW() - INTERVAL '14 days'
        GROUP BY pipeline_key, DATE(started_at)
      ),
      stats AS (
        SELECT pipeline_key,
               AVG(completions) AS avg_completions,
               AVG(items) AS avg_items
        FROM daily
        WHERE run_date < CURRENT_DATE  -- exclude today (incomplete)
        GROUP BY pipeline_key
      ),
      today AS (
        SELECT pipeline_key, completions, items
        FROM daily WHERE run_date = CURRENT_DATE
      )
      SELECT s.pipeline_key,
             s.avg_completions,
             COALESCE(t.completions, 0) AS today_completions,
             s.avg_items,
             t.items AS today_items
      FROM stats s
      LEFT JOIN today t ON t.pipeline_key = s.pipeline_key
      WHERE s.avg_completions > 2  -- only flag pipelines that normally run multiple times/day
        AND COALESCE(t.completions, 0) < s.avg_completions * 0.3
    `);

    for (const r of rows) {
      const sla = PIPELINE_SLA[r.pipeline_key];
      if (!sla) continue;
      issues.push({
        check: 'volume_anomaly',
        pipeline: r.pipeline_key,
        label: sla.label,
        severity: 'medium',
        message: `${r.today_completions} runs today vs avg ${parseFloat(r.avg_completions).toFixed(1)}/day`,
        today: parseInt(r.today_completions),
        avg: parseFloat(r.avg_completions)
      });
    }
  } catch (e) {
    // items_processed column may not be JSON-parseable — skip silently
  }

  return issues;
}

// ═══════════════════════════════════════════════════════════════════════════════
// REPORT & ALERT
// ═══════════════════════════════════════════════════════════════════════════════

function formatReport(allIssues) {
  const critical = allIssues.filter(i => i.severity === 'critical');
  const high = allIssues.filter(i => i.severity === 'high');
  const medium = allIssues.filter(i => i.severity === 'medium');
  const low = allIssues.filter(i => i.severity === 'low');

  const status = critical.length > 0 ? 'CRITICAL' : high.length > 0 ? 'DEGRADED' : allIssues.length > 0 ? 'WARNINGS' : 'HEALTHY';

  return { status, total: allIssues.length, critical: critical.length, high: high.length, medium: medium.length, low: low.length, issues: allIssues };
}

function printReport(report) {
  const icon = { CRITICAL: '🔴', DEGRADED: '🟠', WARNINGS: '🟡', HEALTHY: '🟢' };
  console.log('\n' + '═'.repeat(60));
  console.log(`${icon[report.status] || '?'} WATCHDOG REPORT — ${report.status}`);
  console.log('═'.repeat(60));
  console.log(`  Checked at: ${new Date().toISOString()}`);
  console.log(`  Issues:     ${report.critical} critical, ${report.high} high, ${report.medium} medium, ${report.low} low`);

  if (report.issues.length === 0) {
    console.log('\n  All systems operational.\n');
    return;
  }

  console.log('');
  const severityOrder = ['critical', 'high', 'medium', 'low'];
  const icons = { critical: '🔴', high: '🟠', medium: '🟡', low: '⚪' };

  for (const sev of severityOrder) {
    const items = report.issues.filter(i => i.severity === sev);
    if (items.length === 0) continue;
    console.log(`  ${icons[sev]} ${sev.toUpperCase()} (${items.length})`);
    for (const item of items) {
      const label = item.label || item.service || item.metric || item.collection || '';
      console.log(`     ${label ? label + ': ' : ''}${item.message}`);
    }
    console.log('');
  }
}

function buildEmailHTML(report) {
  const icon = { CRITICAL: '&#x1F534;', DEGRADED: '&#x1F7E0;', WARNINGS: '&#x1F7E1;', HEALTHY: '&#x1F7E2;' };
  const sevColor = { critical: '#dc2626', high: '#ea580c', medium: '#ca8a04', low: '#6b7280' };

  let html = `
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
  <h2 style="margin:0 0 4px 0;">${icon[report.status]} Platform Watchdog — ${report.status}</h2>
  <p style="color:#6b7280;margin:0 0 20px 0;">${new Date().toISOString()}</p>
  <p style="margin:0 0 16px 0;">${report.critical} critical, ${report.high} high, ${report.medium} medium, ${report.low} low</p>`;

  if (report.issues.length === 0) {
    html += '<p style="color:#16a34a;font-weight:600;">All systems operational.</p>';
  } else {
    const severityOrder = ['critical', 'high', 'medium', 'low'];
    for (const sev of severityOrder) {
      const items = report.issues.filter(i => i.severity === sev);
      if (items.length === 0) continue;
      html += `<h3 style="color:${sevColor[sev]};margin:16px 0 8px 0;text-transform:uppercase;font-size:14px;">${sev} (${items.length})</h3><ul style="margin:0;padding:0 0 0 20px;">`;
      for (const item of items) {
        const label = item.label || item.service || item.metric || item.collection || '';
        html += `<li style="margin:4px 0;"><strong>${label}</strong>: ${item.message}</li>`;
      }
      html += '</ul>';
    }
  }

  html += `
  <hr style="margin:24px 0;border:none;border-top:1px solid #e5e7eb;">
  <p style="color:#9ca3af;font-size:12px;">Autonodal Platform Watchdog &mdash; runs every 2 hours</p>
</div>`;

  return html;
}

async function sendAlert(report) {
  if (!process.env.RESEND_API_KEY) {
    console.log('[Watchdog] Email alert skipped — RESEND_API_KEY not configured');
    return;
  }

  // Only alert on critical/high issues or if explicitly requested
  if (report.critical === 0 && report.high === 0 && !process.argv.includes('--email')) {
    console.log('[Watchdog] No critical/high issues — skipping email');
    return;
  }

  const alertTo = process.env.WATCHDOG_ALERT_EMAIL || process.env.ADMIN_EMAIL || 'jonathan@mitchellake.com';
  const from = process.env.EMAIL_FROM || 'Autonodal <notifications@autonodal.com>';

  try {
    const { Resend } = require('resend');
    const resend = new Resend(process.env.RESEND_API_KEY);
    await resend.emails.send({
      from: from,
      to: alertTo,
      subject: `[${report.status}] Autonodal Watchdog — ${report.critical + report.high} issues`,
      html: buildEmailHTML(report)
    });
    console.log(`[Watchdog] Alert sent to ${alertTo}`);
  } catch (e) {
    console.error('[Watchdog] Email send failed:', e.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function runWatchdog() {
  const allIssues = [];
  const timings = {};

  const checks = [
    { name: 'Pipeline Freshness', fn: checkPipelineFreshness },
    { name: 'Data Freshness', fn: checkDataFreshness },
    { name: 'External Services', fn: checkExternalServices },
    { name: 'RSS Feed Health', fn: checkRSSFeeds },
    { name: 'Embedding Coverage', fn: checkEmbeddingCoverage },
    { name: 'Volume Anomalies', fn: checkVolumeAnomalies },
  ];

  for (const check of checks) {
    const start = Date.now();
    try {
      const issues = await check.fn();
      allIssues.push(...issues);
      timings[check.name] = Date.now() - start;
    } catch (e) {
      console.error(`[Watchdog] ${check.name} check failed:`, e.message);
      allIssues.push({ check: check.name, severity: 'high', message: `Check itself failed: ${e.message}` });
      timings[check.name] = Date.now() - start;
    }
  }

  const report = formatReport(allIssues);
  report.timings = timings;
  report.checked_at = new Date().toISOString();

  return report;
}

// Exported for scheduler integration
async function pipelineWatchdog() {
  const report = await runWatchdog();

  if (!process.argv.includes('--json')) {
    printReport(report);
  }

  // Send email if critical/high issues or --email flag
  if (report.critical > 0 || report.high > 0 || process.argv.includes('--email')) {
    await sendAlert(report);
  }

  return { status: report.status, total: report.total, critical: report.critical, high: report.high };
}

// CLI execution
if (require.main === module) {
  (async () => {
    const report = await runWatchdog();

    if (process.argv.includes('--json')) {
      console.log(JSON.stringify(report, null, 2));
    } else {
      printReport(report);
    }

    if (process.argv.includes('--email')) {
      await sendAlert(report);
    }

    await pool.end();
    process.exit(report.critical > 0 ? 2 : report.high > 0 ? 1 : 0);
  })().catch(e => {
    console.error('Watchdog failed:', e);
    pool.end();
    process.exit(2);
  });
}

module.exports = { pipelineWatchdog, runWatchdog };
