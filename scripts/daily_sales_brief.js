#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Daily Sales Intelligence Brief
// Generates and sends a scored, proximity-ranked signal brief to the team.
// Cron: 0 21 * * 0-4 (21:00 UTC = 7:00 AEST weekdays)
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { computeSignalTimingScore, checkForReboot } = require('../lib/signal_timing');
const { sendEmail } = require('../lib/email');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

const ML_TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

function esc(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

async function run() {
  console.log('📋 Generating daily sales intelligence brief...');

  // ── Step 1: Recent signals (48h) ──────────────────────────────────────────

  const { rows: recentSignals } = await pool.query(`
    SELECT se.id, se.signal_type, se.confidence_score, se.evidence_summary,
           se.detected_at, se.company_id, se.company_name,
           EXTRACT(DAY FROM NOW() - se.detected_at)::int AS age_days,
           c.name AS company_name_resolved, c.sector, c.geography, c.country_code,
           COALESCE(c.is_client, false) AS is_client,
           ed.title AS doc_title, ed.source_name
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
      AND se.detected_at > NOW() - INTERVAL '48 hours'
      AND COALESCE(se.is_megacap, false) = false
      AND COALESCE(c.company_tier, '') NOT IN ('megacap_indicator', 'tenant_company')
      AND se.company_name IS NOT NULL
    ORDER BY se.confidence_score DESC
  `, [ML_TENANT_ID]);

  // ── Step 2: Signals in peak/rising window (older) ─────────────────────────

  const { rows: olderSignals } = await pool.query(`
    SELECT se.id, se.signal_type, se.confidence_score, se.evidence_summary,
           se.detected_at, se.company_id, se.company_name,
           EXTRACT(DAY FROM NOW() - se.detected_at)::int AS age_days,
           c.name AS company_name_resolved, c.sector, c.geography,
           COALESCE(c.is_client, false) AS is_client,
           ed.title AS doc_title, ed.source_name
    FROM signal_events se
    LEFT JOIN companies c ON c.id = se.company_id
    LEFT JOIN external_documents ed ON ed.id = se.source_document_id
    WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
      AND se.detected_at BETWEEN NOW() - INTERVAL '540 days' AND NOW() - INTERVAL '48 hours'
      AND COALESCE(se.is_megacap, false) = false
      AND se.company_name IS NOT NULL
      AND se.confidence_score >= 0.5
    ORDER BY se.confidence_score DESC
    LIMIT 2000
  `, [ML_TENANT_ID]);

  const peakOlderSignals = olderSignals.filter(s => {
    const t = computeSignalTimingScore(s.signal_type, s.age_days);
    return t.phase === 'peak_window' || t.phase === 'approaching_peak';
  });

  // ── Step 3: Group by company and score ────────────────────────────────────

  const companies = {};

  function addSignal(signal, category) {
    const cid = signal.company_id;
    if (!cid) return;
    if (!companies[cid]) {
      companies[cid] = {
        company_id: cid,
        company_name: signal.company_name_resolved || signal.company_name,
        sector: signal.sector, geography: signal.geography,
        is_client: signal.is_client,
        signals: [], category, reboot: false, compound: null,
      };
    }
    const timing = computeSignalTimingScore(signal.signal_type, signal.age_days);
    companies[cid].signals.push({ ...signal, ...timing });
    // Promote category: peak > rising > new
    if (timing.phase === 'peak_window') companies[cid].category = 'peak';
    else if (timing.phase === 'approaching_peak' && companies[cid].category !== 'peak') companies[cid].category = 'rising';
  }

  recentSignals.forEach(s => addSignal(s, 'new'));
  peakOlderSignals.forEach(s => addSignal(s, 'rising'));

  // ── Step 4: Compound patterns + reboots ───────────────────────────────────

  for (const co of Object.values(companies)) {
    const types = [...new Set(co.signals.map(s => s.signal_type))];
    if (types.length >= 2) {
      co.compound = types.map(t => t.replace(/_/g, ' ')).join(' + ');
    }
    // Check for reboots
    try {
      const reboots = await checkForReboot(
        { query: (sql, params) => pool.query(sql, params) },
        co.company_id, co.signals[0]?.signal_type, ML_TENANT_ID
      );
      if (reboots?.length > 0) { co.reboot = true; co.category = 'peak'; }
    } catch (e) {}
  }

  // ── Step 5: Composite score ───────────────────────────────────────────────

  for (const co of Object.values(companies)) {
    let score = 0;
    const bestTiming = Math.max(...co.signals.map(s => s.score || 0));
    score += bestTiming * 0.35;
    const avgConf = co.signals.reduce((s, x) => s + (x.confidence_score || 0.5), 0) / co.signals.length;
    score += avgConf * 0.25;
    if (co.is_client) score += 0.15;
    if (co.compound) score += 0.10;
    if (co.reboot) score = Math.min(1.0, score * 1.3);
    co.composite_score = Math.round(Math.min(1.0, score) * 100) / 100;
  }

  // ── Step 6: Team proximity per company ────────────────────────────────────

  for (const co of Object.values(companies)) {
    try {
      const { rows } = await pool.query(`
        SELECT u.name AS team_member, u.email AS team_email,
               p.full_name AS contact_name, p.current_title AS contact_title,
               MAX(tp.proximity_strength) AS strength,
               MAX(tp.last_contact_date) AS last_contact
        FROM team_proximity tp
        JOIN people p ON p.id = tp.person_id AND p.current_company_id = $1
        JOIN users u ON u.id = tp.user_id
        WHERE tp.tenant_id = $2 AND tp.proximity_strength >= 0.15
        GROUP BY u.name, u.email, p.full_name, p.current_title
        ORDER BY MAX(tp.proximity_strength) DESC
        LIMIT 3
      `, [co.company_id, ML_TENANT_ID]);

      co.proximity = rows.map(r => ({
        team_member: r.team_member, team_email: r.team_email,
        contact: r.contact_name, contact_title: r.contact_title,
        strength: parseFloat(r.strength),
        strength_label: r.strength >= 0.6 ? 'Strong' : r.strength >= 0.3 ? 'Warm' : 'Cool',
        last_contact: r.last_contact,
      }));
      co.best_path = co.proximity[0] || null;
    } catch (e) { co.proximity = []; co.best_path = null; }
  }

  // ── Step 7: Categorise and rank ───────────────────────────────────────────

  const all = Object.values(companies).sort((a, b) => b.composite_score - a.composite_score);
  const peak = all.filter(c => c.category === 'peak').slice(0, 10);
  const rising = all.filter(c => c.category === 'rising').slice(0, 10);
  const fresh = all.filter(c => c.category === 'new').slice(0, 10);

  // ── Step 8: Per-team-member leads ─────────────────────────────────────────

  const teamLeads = {};
  for (const co of all) {
    if (!co.best_path) continue;
    const email = co.best_path.team_email;
    if (!teamLeads[email]) teamLeads[email] = { name: co.best_path.team_member, email, leads: [] };
    teamLeads[email].leads.push({
      company: co.company_name, score: co.composite_score, category: co.category,
      contact: co.best_path.contact, signal: co.signals[0]?.signal_type,
    });
  }

  // ── Step 9: Build HTML email ──────────────────────────────────────────────

  const today = new Date().toISOString().split('T')[0];
  const typeColors = {
    capital_raising: '#EF9F27', strategic_hiring: '#5DCAA5', geographic_expansion: '#378ADD',
    leadership_change: '#D4537E', ma_activity: '#AFA9EC', product_launch: '#97C459',
    partnership: '#85B7EB', layoffs: '#E24B4A', restructuring: '#E24B4A',
  };

  function leadCard(co, idx) {
    const sig = co.signals[0];
    const type = (sig?.signal_type || '').replace(/_/g, ' ');
    const tc = typeColors[sig?.signal_type] || '#888';
    const phaseEmoji = co.category === 'peak' ? '🔴' : co.category === 'rising' ? '🟡' : '🔵';
    const path = co.best_path;

    return `<tr><td style="padding:12px 0;border-bottom:1px solid #e2e8f0">
      <div style="display:flex;gap:8px;align-items:baseline">
        <span style="font-size:16px">${phaseEmoji}</span>
        <div style="flex:1">
          <div style="font-weight:600;font-size:15px;color:#1a1a1a">${esc(co.company_name)}${co.is_client ? ' <span style="font-size:10px;padding:2px 6px;border-radius:4px;background:#fef3c7;color:#92400e;font-weight:600">CLIENT</span>' : ''}</div>
          <div style="font-size:13px;color:#4a5568;margin-top:2px"><span style="padding:2px 6px;border-radius:4px;background:${tc}18;color:${tc};font-size:11px;font-weight:600">${type.toUpperCase()}</span> · Score: ${co.composite_score}${co.compound ? ' · <em>' + esc(co.compound) + '</em>' : ''}${co.reboot ? ' · <span style="color:#ef4444;font-weight:600">REBOOT</span>' : ''}</div>
          <div style="font-size:12px;color:#64748b;margin-top:4px">${esc(sig?.evidence_summary || sig?.doc_title || '')}</div>
          <div style="font-size:11px;color:#94a3b8;margin-top:2px">${sig?.description || ''}</div>
          ${path ? `<div style="font-size:12px;color:#2563eb;margin-top:6px;font-weight:500">Best path: ${esc(path.team_member)} via ${esc(path.contact)} (${esc(path.contact_title || '')}) · ${path.strength_label}${path.last_contact ? ' · Last: ' + new Date(path.last_contact).toLocaleDateString() : ''}</div>` : '<div style="font-size:12px;color:#94a3b8;margin-top:6px">No warm path — consider EventMedium</div>'}
        </div>
      </div>
    </td></tr>`;
  }

  const html = `<div style="font-family:system-ui,-apple-system,sans-serif;max-width:640px;margin:0 auto;padding:24px;color:#1a1a1a">
    <div style="text-align:center;margin-bottom:24px">
      <div style="font-size:11px;text-transform:uppercase;letter-spacing:1.5px;color:#94a3b8;margin-bottom:8px">Daily Intelligence Brief</div>
      <div style="font-size:13px;color:#64748b">${today} · ${recentSignals.length} new signals · ${peak.length} in peak window</div>
    </div>

    ${peak.length > 0 ? `
      <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:12px 16px;margin-bottom:20px">
        <div style="font-size:12px;font-weight:700;color:#991b1b;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">🔴 Peak Window — Act This Week</div>
        <table style="width:100%;border-collapse:collapse">${peak.map((c, i) => leadCard(c, i + 1)).join('')}</table>
      </div>
    ` : ''}

    ${rising.length > 0 ? `
      <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;margin-bottom:20px">
        <div style="font-size:12px;font-weight:700;color:#92400e;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">🟡 Rising — Watch & Prepare</div>
        <table style="width:100%;border-collapse:collapse">${rising.map((c, i) => leadCard(c, i + 1)).join('')}</table>
      </div>
    ` : ''}

    ${fresh.length > 0 ? `
      <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:12px 16px;margin-bottom:20px">
        <div style="font-size:12px;font-weight:700;color:#1e40af;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px">🔵 New Signals — Just Detected</div>
        <table style="width:100%;border-collapse:collapse">${fresh.map((c, i) => leadCard(c, i + 1)).join('')}</table>
      </div>
    ` : ''}

    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 16px;margin-bottom:20px;font-size:12px;color:#475569">
      <div style="font-weight:700;margin-bottom:6px">📊 Signal Summary</div>
      <div>Signals (48h): ${recentSignals.length} · Peak window: ${peak.length} · Rising: ${rising.length} · New: ${fresh.length}</div>
      <div>With warm path: ${all.filter(c => c.best_path).length} · No path: ${all.filter(c => !c.best_path).length} · Existing clients: ${all.filter(c => c.is_client).length}</div>
    </div>

    <div style="text-align:center;margin-top:24px">
      <a href="https://www.autonodal.com/index.html" style="display:inline-block;background:#1a1a1a;color:#fff;padding:10px 24px;border-radius:6px;text-decoration:none;font-size:13px;font-weight:600">Open full signal feed →</a>
    </div>

    <div style="text-align:center;margin-top:16px;font-size:10px;color:#94a3b8">
      Autonodal | MitchelLake Signal Intelligence
    </div>
  </div>`;

  // ── Step 10: Send emails ──────────────────────────────────────────────────

  const { rows: teamMembers } = await pool.query(
    `SELECT id, name, email FROM users WHERE tenant_id = $1 AND email IS NOT NULL`,
    [ML_TENANT_ID]
  );

  if (process.env.RESEND_API_KEY) {
    // Team-wide brief
    const teamEmails = teamMembers.map(u => u.email).filter(Boolean);
    if (teamEmails.length > 0) {
      await sendEmail({
        to: teamEmails,
        subject: `Autonodal | Daily Brief — ${today} — ${peak.length} peak leads`,
        html,
      });
      console.log(`  ✉️  Team brief sent to ${teamEmails.length} members`);
    }

    // Personal briefs
    for (const [email, data] of Object.entries(teamLeads)) {
      const peakCount = data.leads.filter(l => l.category === 'peak').length;
      const personalHtml = `<div style="font-family:system-ui,sans-serif;max-width:640px;margin:0 auto;padding:24px">
        <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#94a3b8;margin-bottom:8px">Your Leads Today</div>
        <div style="font-size:15px;font-weight:600;color:#1a1a1a;margin-bottom:16px">${esc(data.name)}, you have the warmest path to ${data.leads.length} signalling companies${peakCount > 0 ? ' — ' + peakCount + ' in peak window' : ''}</div>
        ${data.leads.slice(0, 8).map(l => `<div style="padding:8px 0;border-bottom:1px solid #e2e8f0;font-size:13px">
          <span style="font-weight:600">${esc(l.company)}</span> via ${esc(l.contact)} · <span style="color:${typeColors[l.signal] || '#888'}">${(l.signal || '').replace(/_/g, ' ')}</span> · Score: ${l.score}
        </div>`).join('')}
        <div style="margin-top:16px"><a href="https://www.autonodal.com/index.html" style="color:#2563eb;font-size:12px">Open dashboard →</a></div>
      </div>`;

      await sendEmail({
        to: email,
        subject: `Your leads — ${peakCount} peak, ${data.leads.length - peakCount} rising`,
        html: personalHtml,
      });
    }
    console.log(`  ✉️  Personal briefs sent to ${Object.keys(teamLeads).length} members`);
  }

  // ── Step 11: Archive ──────────────────────────────────────────────────────

  const fs = require('fs');
  fs.mkdirSync('reports/daily_briefs', { recursive: true });
  fs.writeFileSync(`reports/daily_briefs/brief_${today}.json`, JSON.stringify({
    date: today, summary: { total: recentSignals.length, peak: peak.length, rising: rising.length, new: fresh.length },
    peak: peak.map(c => ({ company: c.company_name, score: c.composite_score, signal: c.signals[0]?.signal_type, path: c.best_path?.team_member })),
    rising: rising.map(c => ({ company: c.company_name, score: c.composite_score })),
    team_assignments: teamLeads,
  }, null, 2));

  console.log(`\n  📋 Daily brief complete`);
  console.log(`  Signals (48h):   ${recentSignals.length}`);
  console.log(`  Peak leads:      ${peak.length}`);
  console.log(`  Rising leads:    ${rising.length}`);
  console.log(`  New signals:     ${fresh.length}`);
  console.log(`  With warm path:  ${all.filter(c => c.best_path).length}`);
  console.log(`  Team assigned:   ${Object.keys(teamLeads).length}`);
  console.log(`  Archived:        reports/daily_briefs/brief_${today}.json`);

  await pool.end();
}

run().catch(e => { console.error('Daily brief error:', e.message); pool.end(); process.exit(1); });
