// ═══════════════════════════════════════════════════════════════════════════════
// lib/email.js — Email delivery via Resend
//
// Usage:
//   const { sendWelcome, sendDailyDigest, sendEmail } = require('./email');
//   await sendWelcome({ to: 'user@example.com', name: 'Jane' });
//   await sendDailyDigest({ to: 'user@example.com', name: 'Jane', signals, insight });
// ═══════════════════════════════════════════════════════════════════════════════

const FROM_EMAIL = process.env.EMAIL_FROM || 'Autonodal <signals@autonodal.com>';
const RESEND_API_KEY = process.env.RESEND_API_KEY;

async function sendEmail({ to, subject, html, text }) {
  if (!RESEND_API_KEY) {
    console.log('[email] RESEND_API_KEY not set — skipping:', subject, '→', to);
    return { skipped: true };
  }

  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: FROM_EMAIL, to: Array.isArray(to) ? to : [to], subject, html, text }),
  });

  if (!res.ok) {
    const err = await res.text();
    console.error('[email] Send failed:', res.status, err.slice(0, 200));
    return { error: err };
  }

  const data = await res.json();
  console.log('[email] Sent:', subject, '→', to);
  return data;
}

// ═══════════════════════════════════════════════════════════════════════════════
// TEMPLATES
// ═══════════════════════════════════════════════════════════════════════════════

function esc(s) { return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

function baseLayout(content) {
  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{margin:0;padding:0;background:#0a0e1a;font-family:'Helvetica Neue',Arial,sans-serif;color:#e2e8f0;line-height:1.6}
.container{max-width:560px;margin:0 auto;padding:32px 24px}
.header{margin-bottom:24px}
.mark{color:#f59e0b;font-size:18px;margin-right:6px}
.brand{color:#64748b;font-size:13px;letter-spacing:0.1em;text-transform:uppercase}
h1{font-size:22px;font-weight:700;margin:0 0 8px;color:#f1f5f9}
h2{font-size:16px;font-weight:600;margin:20px 0 8px;color:#f59e0b}
p{font-size:14px;color:#94a3b8;margin:0 0 12px}
.card{background:#111827;border:1px solid #1e2d3d;border-radius:8px;padding:14px 16px;margin:8px 0}
.card-title{font-size:13px;font-weight:600;color:#e2e8f0;margin-bottom:4px}
.card-meta{font-size:11px;color:#64748b}
.badge{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;border-radius:10px;margin-right:4px}
.btn{display:inline-block;background:#f59e0b;color:#0a0e1a;font-size:14px;font-weight:600;padding:12px 24px;border-radius:8px;text-decoration:none;margin:16px 0}
.footer{margin-top:32px;padding-top:16px;border-top:1px solid #1e2d3d;font-size:11px;color:#475569}
.footer a{color:#f59e0b;text-decoration:none}
</style></head>
<body><div class="container">
<div class="header"><span class="mark">&#10022;</span><span class="brand">Autonodal</span></div>
${content}
<div class="footer">
<p><a href="https://www.autonodal.com">autonodal.com</a> &middot; <a href="https://www.autonodal.com/privacy.html">Privacy</a> &middot; <a href="https://www.autonodal.com/terms.html">Terms</a></p>
<p style="color:#334155">You received this because you have an Autonodal account.</p>
</div>
</div></body></html>`;
}

// ── WELCOME EMAIL ────────────────────────────────────────────────────────────

async function sendWelcome({ to, name }) {
  const firstName = (name || '').split(' ')[0] || 'there';
  return sendEmail({
    to,
    subject: 'Your signal feed is live',
    html: baseLayout(`
      <h1>Welcome, ${esc(firstName)}.</h1>
      <p>Your signal feed is now active. We're scanning your sources for signals matching your mission — the first results will appear within minutes.</p>

      <h2>Three things to know</h2>

      <div class="card">
        <div class="card-title">1. Your feed personalises</div>
        <p style="margin:4px 0 0;font-size:12px;color:#94a3b8">Qualify signals that matter, ignore ones that don't. Your feed learns from every action.</p>
      </div>

      <div class="card">
        <div class="card-title">2. Connect Gmail for network proximity</div>
        <p style="margin:4px 0 0;font-size:12px;color:#94a3b8">See who in your network has the warmest path to the companies generating signals. We never read your messages.</p>
      </div>

      <div class="card">
        <div class="card-title">3. Your data is yours</div>
        <p style="margin:4px 0 0;font-size:12px;color:#94a3b8">Everything stays in your private sandbox. Never merged, sold, or used to train AI models.</p>
      </div>

      <a href="https://www.autonodal.com/index.html" class="btn">Open your feed &rarr;</a>
    `),
  });
}

// ── DAILY DIGEST ─────────────────────────────────────────────────────────────

async function sendDailyDigest({ to, name, signals, insight, eventCount }) {
  const firstName = (name || '').split(' ')[0] || 'there';
  const signalCount = signals?.length || 0;

  let signalCards = '';
  (signals || []).slice(0, 5).forEach(s => {
    const typeColors = { capital_raising: '#EF9F27', strategic_hiring: '#5DCAA5', geographic_expansion: '#378ADD', leadership_change: '#D4537E', ma_activity: '#AFA9EC', product_launch: '#97C459' };
    const tc = typeColors[s.signal_type] || '#888';
    signalCards += `<div class="card">
      <div class="card-title">${esc(s.company_name)}</div>
      <div class="card-meta"><span class="badge" style="background:${tc}22;color:${tc}">${esc((s.signal_type || '').replace(/_/g, ' '))}</span> ${Math.round((s.confidence_score || 0) * 100)}% confidence</div>
      <p style="font-size:12px;color:#94a3b8;margin:6px 0 0">${esc((s.evidence_summary || '').slice(0, 120))}</p>
    </div>`;
  });

  let insightHtml = '';
  if (insight) {
    insightHtml = `<h2>Today's Insight</h2>
      <div class="card" style="border-color:rgba(245,158,11,0.3)">
        <div class="card-title" style="color:#f59e0b">${esc(insight.headline || '')}</div>
        <p style="font-size:12px;color:#94a3b8;margin:6px 0 0">${esc(insight.body || '')}</p>
      </div>`;
  }

  return sendEmail({
    to,
    subject: `${signalCount} signals today${insight ? ' — ' + (insight.headline || '').slice(0, 40) : ''}`,
    html: baseLayout(`
      <h1>Good morning, ${esc(firstName)}.</h1>
      <p>${signalCount} new signal${signalCount !== 1 ? 's' : ''} detected in the last 24 hours${eventCount ? ', ' + eventCount + ' upcoming events' : ''}.</p>

      ${insightHtml}

      ${signalCards ? '<h2>Top Signals</h2>' + signalCards : ''}

      <a href="https://www.autonodal.com/index.html" class="btn">Open your feed &rarr;</a>
    `),
  });
}

module.exports = { sendEmail, sendWelcome, sendDailyDigest };
