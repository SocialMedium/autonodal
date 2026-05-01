// ═══════════════════════════════════════════════════════════════════════════════
// public/js/signal-lifecycle-chip.js
// Renders the lifecycle phase chip on signal cards (fresh / warming / hot /
// critical / closing / closed) with a countdown to the next transition.
// Reads phase + first_detected_at + critical_at + closing_at + closed_at from
// the signal payload (added in routes/signals.js /api/signals/brief).
// ═══════════════════════════════════════════════════════════════════════════════

(function() {
  'use strict';

  // Inline Lucide icon paths — keeps this self-contained
  const ICONS = {
    sprout: '<path d="M7 20h10"/><path d="M10 20c5.5-2.5.8-6.4 3-10"/><path d="M9.5 9.4c1.1.8 1.8 2.2 2.3 3.7-2 .4-3.5.4-4.8-.3-1.2-.6-2.3-1.9-3-4.2 2.8-.5 4.4 0 5.5.8z"/><path d="M14.1 6a7 7 0 0 0-1.1 4c1.9-.1 3.3-.6 4.3-1.4 1-1 1.6-2.3 1.7-4.6-2.7.1-4 1-4.9 2z"/>',
    flame: '<path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/>',
    'alert-triangle': '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    hourglass: '<path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.172a2 2 0 0 0-.586-1.414L12 12l-4.414 4.414A2 2 0 0 0 7 17.828V22"/><path d="M7 2v4.172a2 2 0 0 0 .586 1.414L12 12l4.414-4.414A2 2 0 0 0 17 6.172V2"/>',
    check: '<path d="M20 6 9 17l-5-5"/>',
  };

  function lucide(name, size, color) {
    const s = size || 11;
    return '<svg xmlns="http://www.w3.org/2000/svg" width="' + s + '" height="' + s + '" viewBox="0 0 24 24" fill="none" stroke="' + (color || 'currentColor') + '" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0;vertical-align:middle">' + (ICONS[name] || ICONS.check) + '</svg>';
  }

  function daysUntil(iso) {
    if (!iso) return null;
    return Math.floor((new Date(iso).getTime() - Date.now()) / 86400000);
  }

  function daysSince(iso) {
    if (!iso) return null;
    return Math.floor((Date.now() - new Date(iso).getTime()) / 86400000);
  }

  // Phase visual + label table. Bands match common warm/cool conventions across the dashboard.
  const PHASE_STYLES = {
    fresh:    { label: 'Fresh',    icon: 'sprout',         bg: '#D1FAE5', fg: '#064E3B', urgent: false },
    warming:  { label: 'Warming',  icon: 'flame',          bg: '#FEF3C7', fg: '#78350F', urgent: false },
    hot:      { label: 'Hot',      icon: 'flame',          bg: '#FED7AA', fg: '#9A3412', urgent: false },
    critical: { label: 'Critical', icon: 'alert-triangle', bg: '#FEE2E2', fg: '#991B1B', urgent: true },
    closing:  { label: 'Closing',  icon: 'hourglass',      bg: '#FECACA', fg: '#7F1D1D', urgent: true },
    closed:   { label: 'Closed',   icon: 'check',          bg: '#F1F5F9', fg: '#475569', urgent: false },
  };

  // Returns the chip HTML for a signal payload. Empty string if no phase set.
  function signalLifecycleChip(s) {
    if (!s || !s.phase || !PHASE_STYLES[s.phase]) return '';
    const phase = s.phase;
    const style = PHASE_STYLES[phase];

    // Tail describing the countdown to the next milestone
    let tail = '';
    if (phase === 'fresh' || phase === 'warming') {
      const age = daysSince(s.first_detected_at || s.detected_at);
      if (age != null && age >= 0) tail = ' · ' + age + 'd old';
    } else if (phase === 'hot' && s.critical_at) {
      const d = daysUntil(s.critical_at);
      if (d != null && d > 0) tail = ' · critical in ' + d + 'd';
      else if (d != null && d <= 0) tail = ' · critical now';
    } else if (phase === 'critical' && s.closing_at) {
      const d = daysUntil(s.closing_at);
      if (d != null && d > 0) tail = ' · closes in ' + d + 'd';
      else if (d != null && d <= 0) tail = ' · closing';
    } else if (phase === 'closing' && s.closed_at) {
      const d = daysUntil(s.closed_at);
      if (d != null && d > 0) tail = ' · ' + d + 'd left';
    }

    return '<span class="lifecycle-chip" data-phase="' + phase + '" '
      + 'style="display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:600;'
      + 'padding:3px 8px;border-radius:999px;background:' + style.bg + ';color:' + style.fg + ';'
      + 'white-space:nowrap;line-height:1.4">'
      + lucide(style.icon, 11, style.fg)
      + style.label + tail
      + '</span>';
  }

  function isPhaseUrgent(phase) {
    return !!(PHASE_STYLES[phase] && PHASE_STYLES[phase].urgent);
  }

  window.signalLifecycleChip = signalLifecycleChip;
  window.PHASE_STYLES = PHASE_STYLES;
  window.isPhaseUrgent = isPhaseUrgent;
})();
