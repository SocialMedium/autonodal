// ═══════════════════════════════════════════════════════════════════════════════
// proximity-badge.js — Unified proximity surface across list / inline / expanded
// variants. Reads the ProximityHint contract from the API (see docs/api/proximity.md).
//
// Usage:
//   ProximityBadge.render(proximity, 'compact')       → HTML string
//   ProximityBadge.render(proximity, 'inline')        → HTML string with backup count
//   ProximityBadge.renderExpanded(proximity, opts)    → HTML string for dossier sidebar
//   ProximityBadge.mount(rootEl)                      → attach interactive behaviours
//
// proximity shape (nullable): {
//   best_path: { member_user_id, member_name, score, band, last_contact_at,
//                last_contact_channel } | null,
//   backup_paths_count: number,
//   pooled: boolean,
//   factors?: { currency, history, weight, reciprocity }  // on expanded/dossier
// }
// ═══════════════════════════════════════════════════════════════════════════════

const ProximityBadge = (() => {
  const BAND_COLOR = {
    strong: { fg: '#064E3B', bg: '#D1FAE5', ring: '#10B981', label: 'Strong' },
    warm:   { fg: '#1E3A8A', bg: '#DBEAFE', ring: '#2563EB', label: 'Warm' },
    cool:   { fg: '#78350F', bg: '#FEF3C7', ring: '#F59E0B', label: 'Cool' },
    cold:   { fg: '#475569', bg: '#F1F5F9', ring: '#94A3B8', label: 'Cold' },
  };

  const CHANNEL_LABEL = {
    in_person_meeting: 'in person',
    video_call:        'video call',
    phone_call:        'phone call',
    email_reciprocal:  'reciprocal email',
    email_one_way:     'email',
    linkedin_message:  'LinkedIn',
    linkedin_reaction: 'LinkedIn reaction',
    research_note:     'research note',
    unknown:           'contact',
  };

  function handshakeSVG(size = 14, color = 'currentColor') {
    return `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0"><path d="m11 17 2 2a1 1 0 1 0 3-3"/><path d="m14 14 2.5 2.5a1 1 0 1 0 3-3l-3.88-3.88a3 3 0 0 0-4.24 0l-.88.88a1 1 0 1 1-3-3l2.81-2.81a5.79 5.79 0 0 1 7.06-.87l.47.28a2 2 0 0 0 1.42.25L21 4"/><path d="m21 3 1 11h-2"/><path d="M3 3 2 14l6.5 6.5a1 1 0 1 0 3-3"/><path d="M3 4h8"/></svg>`;
  }

  function pooledDot() {
    // Small violet dot marks huddle-pooled paths — i.e. "coalition proximity, not just yours"
    return '<span title="Coalition proximity (huddle lens)" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:#8B5CF6;margin-right:4px;vertical-align:middle"></span>';
  }

  function memberInitials(name) {
    if (!name) return '?';
    return name.split(/\s+/).map(w => w[0]).join('').toUpperCase().slice(0, 2);
  }

  function relativeTime(iso) {
    if (!iso) return null;
    const days = Math.floor((Date.now() - new Date(iso).getTime()) / 86400000);
    if (days < 1) return 'today';
    if (days < 7) return days + 'd ago';
    if (days < 30) return Math.floor(days / 7) + 'w ago';
    if (days < 365) return Math.floor(days / 30) + 'mo ago';
    return Math.floor(days / 365) + 'y ago';
  }

  // ─── COMPACT ───────────────────────────────────────────────────────────────
  // One-line badge for list cards. e.g. "🤝 Warm · JT"
  function renderCompact(prox) {
    if (!prox || !prox.best_path) return '';
    const bp = prox.best_path;
    const c = BAND_COLOR[bp.band] || BAND_COLOR.cool;
    const initials = memberInitials(bp.member_name);
    const dot = prox.pooled ? pooledDot() : '';
    return `<span class="prox-badge prox-compact" data-prox-score="${bp.score}" data-prox-band="${bp.band}"
      style="display:inline-flex;align-items:center;gap:4px;font-size:11px;font-weight:500;color:${c.fg};background:${c.bg};padding:3px 8px;border-radius:999px;line-height:1.4;white-space:nowrap;min-height:22px">
      ${dot}${handshakeSVG(11, c.fg)}<span>${c.label} · ${initials}</span>
    </span>`;
  }

  // ─── INLINE ────────────────────────────────────────────────────────────────
  // Signal cards / search matches / pipeline. Shows best path + backup count.
  function renderInline(prox) {
    if (!prox || !prox.best_path) return '';
    const bp = prox.best_path;
    const c = BAND_COLOR[bp.band] || BAND_COLOR.cool;
    const firstName = (bp.member_name || '').split(/\s+/)[0] || '—';
    const backup = prox.backup_paths_count > 0 ? `<span style="margin-left:6px;font-size:10px;color:${c.fg};opacity:0.75">+${prox.backup_paths_count} more</span>` : '';
    const dot = prox.pooled ? pooledDot() : '';
    const pct = Math.round((bp.score || 0) * 100);
    return `<span class="prox-badge prox-inline" data-prox-score="${bp.score}" data-prox-band="${bp.band}"
      style="display:inline-flex;align-items:center;gap:5px;font-size:11px;font-weight:500;color:${c.fg};background:${c.bg};padding:4px 10px;border-radius:8px;line-height:1.4;border:1px solid ${c.ring}30">
      ${dot}${handshakeSVG(12, c.fg)}<span>${firstName} · ${c.label} <span style="opacity:0.65">(${pct}%)</span></span>${backup}
    </span>`;
  }

  // ─── EXPANDED ──────────────────────────────────────────────────────────────
  // Dossier sidebar — one card per path, factor breakdown, action buttons.
  // Accepts either a single proximity hint + paths array, or just a paths[] array.
  function renderExpanded(data, opts) {
    opts = opts || {};
    const paths = Array.isArray(data) ? data : (data && data.paths) || [];
    if (!paths.length) return '';

    const medals = ['🥇', '🥈', '🥉'];
    const showFactors = opts.showFactors !== false;
    const showActions = opts.showActions !== false;
    const pooled = Array.isArray(data) ? false : !!data.pooled;

    const rows = paths.slice(0, 5).map((p, i) => {
      const c = BAND_COLOR[p.band] || BAND_COLOR.cool;
      const f = p.factors || {};
      const rel = relativeTime(p.last_contact_at);
      const channel = CHANNEL_LABEL[p.last_contact_channel] || 'contact';
      const pct = Math.round((p.score || 0) * 100);

      let factorLine = '';
      if (showFactors && f.history && f.history.tenure_years != null) {
        const tenure = f.history.tenure_years;
        const tenureStr = tenure >= 1 ? tenure.toFixed(1) + ' years' : Math.round(tenure * 12) + ' months';
        const recip = f.reciprocity && f.reciprocity.ratio != null
          ? (f.reciprocity.ratio >= 0.4 ? 'Reciprocal' : 'Mostly outbound')
          : null;
        const parts = [];
        if (rel && channel) parts.push(`Last: ${channel} · ${rel}`);
        if (tenure > 0) parts.push(`History: ${tenureStr}`);
        if (f.weight && f.weight.weighted_interactions_12mo) parts.push(`${f.weight.weighted_interactions_12mo.toFixed(1)} weighted int. 12mo`);
        if (recip) parts.push(recip);
        factorLine = parts.map(s => `<div style="font-size:11px;color:#64748B;line-height:1.55">${s}</div>`).join('');
      } else if (rel) {
        factorLine = `<div style="font-size:11px;color:#64748B">Last: ${channel} · ${rel}</div>`;
      }

      const actions = showActions ? `
        <div style="display:flex;gap:6px;margin-top:8px">
          <button class="prox-action-intro" data-member-id="${p.member_user_id || ''}" style="font-size:11px;font-weight:500;padding:4px 10px;border-radius:6px;background:#fff;border:1px solid #E0DDD8;color:#1A1A1A;cursor:pointer">Request intro</button>
          <button class="prox-action-log" data-member-id="${p.member_user_id || ''}" style="font-size:11px;font-weight:500;padding:4px 10px;border-radius:6px;background:#fff;border:1px solid #E0DDD8;color:#1A1A1A;cursor:pointer">Log interaction</button>
        </div>` : '';

      const medal = i < 3 ? `<span style="font-size:14px;margin-right:6px">${medals[i]}</span>` : '';

      return `
        <div class="prox-path" style="padding:12px 0;border-bottom:1px solid #F1F5F9">
          <div style="display:flex;align-items:baseline;justify-content:space-between;gap:8px;margin-bottom:4px">
            <div style="font-size:13px;font-weight:600;color:#1A1A1A">${medal}${p.member_name || '—'} · <span style="color:${c.ring}">${c.label}</span></div>
            <div style="font-size:12px;color:${c.fg};font-weight:600">${pct}%</div>
          </div>
          ${factorLine}
          ${actions}
        </div>`;
    }).join('');

    const header = pooled
      ? '<div style="display:flex;align-items:center;gap:6px;font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#7F77DD;padding:0 0 8px">'+pooledDot()+'Coalition proximity (huddle)</div>'
      : '<div style="font-size:10px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#64748B;padding:0 0 8px">Team proximity</div>';

    return `<div class="prox-expanded" style="padding:16px;background:#FFFFFF;border:1px solid #E0DDD8;border-radius:10px">${header}${rows}</div>`;
  }

  // ─── MAIN ENTRY ────────────────────────────────────────────────────────────
  function render(prox, variant) {
    variant = variant || 'compact';
    if (variant === 'compact') return renderCompact(prox);
    if (variant === 'inline')  return renderInline(prox);
    if (variant === 'expanded') return renderExpanded(prox);
    return '';
  }

  // Attach interactive behaviours (tooltip-on-hover with paths, bottom sheet on mobile).
  // Idempotent — safe to call on every list re-render.
  function mount(rootEl) {
    rootEl = rootEl || document;
    // Hover tooltip on compact/inline badges showing backup paths.
    rootEl.querySelectorAll('.prox-badge[data-prox-score]').forEach(el => {
      if (el.__proxMounted) return;
      el.__proxMounted = true;
      el.style.cursor = 'pointer';
      el.addEventListener('click', e => {
        e.stopPropagation();
        const card = el.closest('[data-person-id], [data-signal-id]');
        if (card) card.dispatchEvent(new CustomEvent('prox-expand', { bubbles: true, detail: { trigger: el } }));
      });
    });
  }

  return { render, renderCompact, renderInline, renderExpanded, mount, BAND_COLOR, CHANNEL_LABEL };
})();

if (typeof window !== 'undefined') window.ProximityBadge = ProximityBadge;
if (typeof module !== 'undefined' && module.exports) module.exports = ProximityBadge;
