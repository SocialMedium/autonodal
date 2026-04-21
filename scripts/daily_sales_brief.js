#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Daily Sales Intelligence Brief (v2 — matrix-ranked, polarity-filtered)
// Runs weekdays at 07:00 AEST (21:00 UTC day-of).
// Usage: node scripts/daily_sales_brief.js [--dry-run] [--user <uuid>]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { currentPhase } = require('../lib/signal_lifecycle');
const { leadScore, PHASE_CURRENCY } = require('../lib/lead_scoring');
const { isPositive } = require('../lib/signal_polarity');
const { sendEmail } = require('../lib/email');

const DRY_RUN = process.argv.includes('--dry-run');
const userArg = (() => { const i = process.argv.indexOf('--user'); return i !== -1 ? process.argv[i + 1] : null; })();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

const ML_TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

function esc(s) { return (s || '').toString().replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
function fmtDate(d) { return d ? new Date(d).toLocaleDateString('en-AU', { day: 'numeric', month: 'short' }) : ''; }
function daysAgo(d) { return d ? Math.floor((Date.now() - new Date(d).getTime()) / 86400000) : null; }

// ═══════════════════════════════════════════════════════════════════════════════
// Data loaders
// ═══════════════════════════════════════════════════════════════════════════════

async function loadUsers(tenantId) {
  const { rows } = await pool.query(
    `SELECT id, email, name FROM users WHERE tenant_id = $1 AND email IS NOT NULL`,
    [tenantId]
  );
  return rows;
}

// Pull all positive-polarity signals with relevant status, score them, map proximity
async function loadRankedLeadsForTenant(tenantId) {
  const { rows: signals } = await pool.query(`
    SELECT
      se.id AS signal_id, se.signal_type, se.polarity, se.phase,
      se.confidence_score, se.evidence_summary,
      COALESCE(se.first_detected_at, se.detected_at) AS first_detected_at,
      se.detected_at, se.critical_at, se.closing_at,
      se.company_id, se.company_name,
      c.name AS company_name_resolved, c.sector, c.geography,
      c.relationship_state,
      ed.title AS doc_title, ed.source_name
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
      AND se.polarity = 'positive'
      AND (se.phase IS NULL OR se.phase NOT IN ('closed'))
      AND COALESCE(se.is_megacap, false) = false
      AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      AND se.company_name IS NOT NULL
      AND se.detected_at > NOW() - INTERVAL '540 days'
    ORDER BY se.confidence_score DESC
  `, [tenantId]);

  // Compute phase for any signals that have NULL phase (not yet processed)
  for (const s of signals) {
    if (!s.phase) {
      const ph = currentPhase({ signal_type: s.signal_type, first_detected_at: s.first_detected_at });
      s.phase = ph.phase;
      s._days_to_critical = ph.days_to_critical;
      s._days_to_closing = ph.days_to_closing;
    } else {
      const ph = currentPhase({ signal_type: s.signal_type, first_detected_at: s.first_detected_at });
      s._days_to_critical = ph.days_to_critical;
      s._days_to_closing = ph.days_to_closing;
    }
  }

  return signals;
}

async function loadClaimsForTenant(tenantId) {
  const { rows } = await pool.query(`
    SELECT lc.signal_id, lc.user_id, lc.pipeline_stage, lc.claimed_at, lc.stage_changed_at,
           u.name AS user_name
    FROM lead_claims lc
    LEFT JOIN users u ON u.id = lc.user_id
    WHERE lc.tenant_id = $1 AND lc.released_at IS NULL
  `, [tenantId]);
  return rows;
}

// Proximity per signal × user — max across all team members per company, flagged per user
async function loadProximityForCompanies(tenantId, companyIds) {
  if (!companyIds.length) return new Map();
  const { rows } = await pool.query(`
    SELECT p.current_company_id AS company_id,
           tp.team_member_id AS user_id,
           MAX(tp.relationship_strength) AS strength,
           (ARRAY_AGG(p.full_name ORDER BY tp.relationship_strength DESC))[1] AS best_contact_name,
           (ARRAY_AGG(p.current_title ORDER BY tp.relationship_strength DESC))[1] AS best_contact_title,
           MAX(tp.last_interaction_date) AS last_contact
    FROM team_proximity tp
    JOIN people p ON p.id = tp.person_id AND p.current_company_id = ANY($1::uuid[])
    WHERE tp.tenant_id = $2 AND tp.relationship_strength > 0
    GROUP BY p.current_company_id, tp.team_member_id
  `, [companyIds, tenantId]);

  const map = new Map(); // companyId → [{user_id, strength, ...}]
  for (const r of rows) {
    if (!map.has(r.company_id)) map.set(r.company_id, []);
    map.get(r.company_id).push({
      user_id: r.user_id,
      strength: parseFloat(r.strength),
      best_contact_name: r.best_contact_name,
      best_contact_title: r.best_contact_title,
      last_contact: r.last_contact,
    });
  }
  return map;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Brief composition per user
// ═══════════════════════════════════════════════════════════════════════════════

function composeBriefForUser({ user, signals, claims, proximityMap }) {
  const claimedBySignal = new Map(claims.map(c => [c.signal_id, c]));
  const userClaimsSet = new Set(claims.filter(c => c.user_id === user.id).map(c => c.signal_id));

  const critical = [];   // time-perishable — top of email, any category, this user has best path
  const claimedByMe = []; // claimed by this user
  const warmPaths = [];   // unclaimed, active_client / warm_non_client, this user has best path
  const hotNetNew = [];   // unclaimed, warm_non_client, fresh/warming

  for (const sig of signals) {
    if (!sig.company_id) continue;

    const proxEntries = proximityMap.get(sig.company_id) || [];
    const maxEntry = proxEntries.length
      ? proxEntries.reduce((a, b) => (a.strength >= b.strength ? a : b))
      : null;
    const bestUserId = maxEntry?.user_id;
    const userEntry = proxEntries.find(p => p.user_id === user.id);
    const userProximity = userEntry?.strength || 0;

    const company = {
      id: sig.company_id,
      name: sig.company_name_resolved || sig.company_name,
      sector: sig.sector, geography: sig.geography,
      relationship_state: sig.relationship_state,
    };

    const score = leadScore(sig, company, userProximity);
    if (score === null) continue; // non-positive polarity — never appears

    const entry = {
      signal: sig,
      company,
      score,
      user_proximity: userProximity,
      best_contact: userEntry || maxEntry,
      is_best_path: bestUserId === user.id,
    };

    // Claimed by me
    if (userClaimsSet.has(sig.signal_id)) {
      const claim = claimedBySignal.get(sig.signal_id);
      claimedByMe.push({ ...entry, claim });
      continue;
    }

    // Claimed by someone else — skip
    if (claimedBySignal.has(sig.signal_id)) continue;

    // Critical window (perishable — time is the driver, not category)
    if (entry.is_best_path && (sig.phase === 'critical' || sig.phase === 'closing')) {
      critical.push(entry);
      continue;
    }

    // Warm paths — user is best path at active_client or warm_non_client
    if (entry.is_best_path && ['active_client', 'warm_non_client'].includes(company.relationship_state)) {
      warmPaths.push(entry);
      continue;
    }

    // Hot net-new — unclaimed, warm non-client, fresh/warming
    if (company.relationship_state === 'warm_non_client'
        && ['fresh', 'warming'].includes(sig.phase)
        && userProximity > 0.15) {
      hotNetNew.push(entry);
      continue;
    }
  }

  // Sort each section by score desc
  const byScore = (a, b) => b.score - a.score;
  critical.sort(byScore);
  warmPaths.sort(byScore);
  hotNetNew.sort(byScore);
  claimedByMe.sort((a, b) => new Date(b.claim?.stage_changed_at || 0) - new Date(a.claim?.stage_changed_at || 0));

  return {
    critical: critical.slice(0, 6),
    claimed_by_me: claimedByMe.slice(0, 10),
    warm_paths: warmPaths.slice(0, 8),
    hot_net_new: hotNetNew.slice(0, 8),
    summary: {
      critical_count: critical.length,
      claimed_count: claimedByMe.length,
      warm_paths_count: warmPaths.length,
      hot_net_new_count: hotNetNew.length,
    },
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// HTML rendering
// ═══════════════════════════════════════════════════════════════════════════════

const TYPE_COLORS = {
  capital_raising: '#EF9F27', strategic_hiring: '#5DCAA5', geographic_expansion: '#378ADD',
  partnership: '#85B7EB', ma_activity: '#AFA9EC', product_launch: '#97C459',
  leadership_change: '#D4537E',
};

const PHASE_LABELS = {
  fresh: 'Fresh', warming: 'Warming', hot: 'Hot', critical: 'Critical', closing: 'Closing', closed: 'Closed',
};

function countdownSvg(phase, daysToCritical, daysToClosing) {
  // Inline SVG bar with phase markers. Width 280, height 20.
  const w = 280, h = 8, markerSize = 4;
  const positions = {
    fresh: 0.10, warming: 0.30, hot: 0.55, critical: 0.80, closing: 0.95, closed: 1.00,
  };
  const pos = (positions[phase] ?? 0.10) * w;
  const color = phase === 'critical' || phase === 'closing' ? '#ef4444' : phase === 'hot' ? '#f59e0b' : '#10b981';
  return `<svg width="${w}" height="${h + 4}" viewBox="0 0 ${w} ${h + 4}" xmlns="http://www.w3.org/2000/svg">
    <rect x="0" y="2" width="${w}" height="${h}" rx="${h/2}" fill="#e5e7eb"/>
    <rect x="0" y="2" width="${pos}" height="${h}" rx="${h/2}" fill="${color}"/>
    <circle cx="${pos}" cy="${h/2 + 2}" r="${markerSize}" fill="${color}" stroke="#fff" stroke-width="1.5"/>
  </svg>`;
}

function leadRow(entry, opts = {}) {
  const s = entry.signal;
  const tc = TYPE_COLORS[s.signal_type] || '#6b7280';
  const type = (s.signal_type || '').replace(/_/g, ' ');
  const phase = s.phase || 'fresh';
  const phaseLabel = PHASE_LABELS[phase] || phase;
  const age = daysAgo(s.first_detected_at);
  const nextInflection = phase === 'fresh' || phase === 'warming'
    ? (s._days_to_critical > 0 ? `${s._days_to_critical}d to critical` : '')
    : phase === 'hot'
      ? (s._days_to_critical > 0 ? `${s._days_to_critical}d to critical` : `${s._days_to_closing}d to closing`)
      : phase === 'critical'
        ? `${s._days_to_closing}d to closing`
        : '';

  const pathText = entry.best_contact
    ? `${esc(entry.best_contact.best_contact_name || '')}${entry.best_contact.best_contact_title ? ' (' + esc(entry.best_contact.best_contact_title) + ')' : ''} · proximity ${entry.best_contact.strength.toFixed(2)}${entry.best_contact.last_contact ? ' · last ' + fmtDate(entry.best_contact.last_contact) : ''}`
    : 'No warm path';

  const relState = entry.company.relationship_state || 'cold_non_client';
  const relBadge = {
    active_client: '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:#fef3c7;color:#92400e;font-weight:700">CLIENT</span>',
    ex_client: '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:#fee2e2;color:#991b1b;font-weight:700">EX-CLIENT</span>',
    warm_non_client: '<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:#ddd6fe;color:#5b21b6;font-weight:700">WARM</span>',
    cool_non_client: '',
    cold_non_client: '',
  }[relState] || '';

  const claimBtn = opts.showClaimButton
    ? `<a href="https://www.autonodal.com/company.html?id=${s.company_id}&signal=${s.signal_id}" style="font-size:11px;padding:4px 10px;border-radius:4px;background:#1a1a1a;color:#fff;text-decoration:none;font-weight:600">Claim →</a>`
    : '';

  const stageBadge = entry.claim
    ? `<span style="font-size:9px;padding:2px 6px;border-radius:4px;background:#e0e7ff;color:#3730a3;font-weight:700">${esc(entry.claim.pipeline_stage.toUpperCase())}</span>`
    : '';

  return `<tr><td style="padding:10px 0;border-bottom:1px solid #e5e7eb">
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:4px">
      <span style="font-size:9px;padding:2px 6px;border-radius:4px;background:${tc}18;color:${tc};font-weight:700;text-transform:uppercase">${type}</span>
      ${relBadge}
      ${stageBadge}
      <span style="font-size:10px;color:#6b7280">${phaseLabel}${nextInflection ? ' · ' + nextInflection : ''}</span>
      <span style="margin-left:auto;font-size:10px;color:#94a3b8;font-family:monospace">${(entry.score * 100).toFixed(0)}</span>
    </div>
    <div style="font-weight:600;font-size:14px;color:#111827;margin-bottom:2px">${esc(entry.company.name)}</div>
    <div style="font-size:12px;color:#4b5563;line-height:1.45;margin-bottom:4px">${esc((s.evidence_summary || s.doc_title || '').slice(0, 180))}</div>
    ${countdownSvg(phase, s._days_to_critical, s._days_to_closing)}
    <div style="display:flex;gap:8px;align-items:center;margin-top:6px">
      <span style="font-size:11px;color:#2563eb;flex:1">${pathText}</span>
      ${claimBtn}
    </div>
  </td></tr>`;
}

function renderBriefHtml(brief, user, today) {
  const sections = [];

  if (brief.critical.length) {
    sections.push(`<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:12px 16px;margin-bottom:18px">
      <div style="font-size:11px;font-weight:700;color:#991b1b;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">
        🔴 Critical Window · ${brief.critical.length}
      </div>
      <table style="width:100%;border-collapse:collapse">${brief.critical.map(l => leadRow(l, { showClaimButton: true })).join('')}</table>
    </div>`);
  }

  if (brief.claimed_by_me.length) {
    sections.push(`<div style="background:#eef2ff;border:1px solid #c7d2fe;border-radius:8px;padding:12px 16px;margin-bottom:18px">
      <div style="font-size:11px;font-weight:700;color:#3730a3;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">
        📋 Your Claims · ${brief.claimed_by_me.length}
      </div>
      <table style="width:100%;border-collapse:collapse">${brief.claimed_by_me.map(l => leadRow(l, { showClaimButton: false })).join('')}</table>
    </div>`);
  }

  if (brief.warm_paths.length) {
    sections.push(`<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:18px">
      <div style="font-size:11px;font-weight:700;color:#92400e;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">
        🟡 Warm Client Paths · ${brief.warm_paths.length}
      </div>
      <table style="width:100%;border-collapse:collapse">${brief.warm_paths.map(l => leadRow(l, { showClaimButton: true })).join('')}</table>
    </div>`);
  }

  if (brief.hot_net_new.length) {
    sections.push(`<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 16px;margin-bottom:18px">
      <div style="font-size:11px;font-weight:700;color:#1e40af;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">
        🔵 Hot Net-New · ${brief.hot_net_new.length}
      </div>
      <table style="width:100%;border-collapse:collapse">${brief.hot_net_new.map(l => leadRow(l, { showClaimButton: true })).join('')}</table>
    </div>`);
  }

  const body = sections.length ? sections.join('') : '<div style="padding:16px;color:#6b7280;font-size:13px;text-align:center">No qualifying leads today.</div>';

  return `<div style="font-family:system-ui,-apple-system,sans-serif;max-width:680px;margin:0 auto;padding:20px;color:#111827">
    <div style="text-align:center;margin-bottom:20px">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:1.5px;color:#94a3b8;margin-bottom:6px">Daily Intelligence Brief</div>
      <div style="font-size:13px;color:#6b7280">${today} · for ${esc(user.name || user.email)}</div>
    </div>
    ${body}
    <div style="text-align:center;margin-top:24px">
      <a href="https://www.autonodal.com/index.html" style="display:inline-block;background:#1a1a1a;color:#fff;padding:10px 24px;border-radius:6px;text-decoration:none;font-size:13px;font-weight:600">Open dashboard →</a>
    </div>
    <div style="text-align:center;margin-top:16px;font-size:10px;color:#94a3b8">
      Autonodal · Matrix-ranked commercial leads · Positive signal polarity only
    </div>
  </div>`;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════════════

async function run() {
  console.log(`📋 Daily sales brief${DRY_RUN ? ' [DRY RUN]' : ''}...`);
  const today = new Date().toISOString().split('T')[0];

  const users = userArg
    ? await pool.query('SELECT id, email, name FROM users WHERE id = $1', [userArg]).then(r => r.rows)
    : await loadUsers(ML_TENANT_ID);

  if (users.length === 0) {
    console.log('  No users found');
    await pool.end();
    return;
  }

  console.log(`  Tenant: ${ML_TENANT_ID}`);
  console.log(`  Users: ${users.length}`);

  const signals = await loadRankedLeadsForTenant(ML_TENANT_ID);
  const claims = await loadClaimsForTenant(ML_TENANT_ID);
  const companyIds = [...new Set(signals.map(s => s.company_id).filter(Boolean))];
  const proximityMap = await loadProximityForCompanies(ML_TENANT_ID, companyIds);

  console.log(`  Positive signals: ${signals.length}`);
  console.log(`  Active claims:    ${claims.length}`);
  console.log(`  Companies:        ${companyIds.length}`);

  const briefs = {};
  let totalSent = 0;

  for (const user of users) {
    const brief = composeBriefForUser({ user, signals, claims, proximityMap });
    briefs[user.email] = { summary: brief.summary, user: { id: user.id, name: user.name, email: user.email } };

    const totalLeads = brief.critical.length + brief.claimed_by_me.length + brief.warm_paths.length + brief.hot_net_new.length;
    console.log(`  ${(user.name || user.email).padEnd(30)} critical=${brief.critical.length} claimed=${brief.claimed_by_me.length} warm=${brief.warm_paths.length} new=${brief.hot_net_new.length}`);

    if (totalLeads === 0) continue;

    const html = renderBriefHtml(brief, user, today);
    const subj = brief.critical.length
      ? `${brief.critical.length} critical lead${brief.critical.length !== 1 ? 's' : ''} · ${today}`
      : `${totalLeads} lead${totalLeads !== 1 ? 's' : ''} · ${today}`;

    if (!DRY_RUN) {
      try {
        await sendEmail({ to: user.email, subject: subj, html });
        totalSent++;
      } catch (e) {
        console.error(`    send failed: ${e.message}`);
      }
    }
  }

  // Archive
  if (!DRY_RUN) {
    fs.mkdirSync(path.join(__dirname, '..', 'reports', 'daily_briefs'), { recursive: true });
    fs.writeFileSync(
      path.join(__dirname, '..', 'reports', 'daily_briefs', `brief_${today}.json`),
      JSON.stringify({ date: today, tenant_id: ML_TENANT_ID, briefs }, null, 2)
    );
  }

  console.log(`\n  Total sent: ${totalSent}/${users.length}`);
  console.log(`  Archive:    reports/daily_briefs/brief_${today}.json${DRY_RUN ? ' (not written)' : ''}`);

  await pool.end();
}

run().catch(e => { console.error('Daily brief error:', e.message); console.error(e); pool.end(); process.exit(1); });
