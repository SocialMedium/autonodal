// ═══════════════════════════════════════════════════════════════════════════════
// public/js/tour.js — Guided spotlight tour engine
// No dependencies. Dark editorial design.
// ═══════════════════════════════════════════════════════════════════════════════

// Inline Lucide icon paths — keeps tour.js self-contained on pages without Lucide CDN
const LUCIDE_TOUR_ICONS = {
  sparkles: '<path d="M9.937 15.5A2 2 0 0 0 8.5 14.063l-6.135-1.582a.5.5 0 0 1 0-.962L8.5 9.936A2 2 0 0 0 9.937 8.5l1.582-6.135a.5.5 0 0 1 .963 0L14.063 8.5A2 2 0 0 0 15.5 9.937l6.135 1.582a.5.5 0 0 1 0 .962L15.5 14.063a2 2 0 0 0-1.437 1.437l-1.582 6.135a.5.5 0 0 1-.963 0z"/><path d="M20 3v4"/><path d="M22 5h-4"/><path d="M4 17v2"/><path d="M5 18H3"/>',
  zap: '<path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/>',
  globe: '<circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/>',
  compass: '<path d="m16.24 7.76-1.804 5.411a2 2 0 0 1-1.265 1.265L7.76 16.24l1.804-5.411a2 2 0 0 1 1.265-1.265z"/><circle cx="12" cy="12" r="10"/>',
  link: '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
  'plug-zap': '<path d="M6.3 20.3a2.4 2.4 0 0 0 3.4 0L12 18l-6-6-2.3 2.3a2.4 2.4 0 0 0 0 3.4Z"/><path d="m2 22 3-3"/><path d="M7.5 13.5 10 11"/><path d="M10.5 16.5 13 14"/><path d="m18 3-4 4h6l-4 4"/>',
  target: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
  rocket: '<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
};

function lucideSVG(name, size, strokeWidth) {
  const s = size || 24;
  const sw = strokeWidth || 2;
  const paths = LUCIDE_TOUR_ICONS[name] || LUCIDE_TOUR_ICONS.sparkles;
  return '<svg xmlns="http://www.w3.org/2000/svg" width="' + s + '" height="' + s + '" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="' + sw + '" stroke-linecap="round" stroke-linejoin="round">' + paths + '</svg>';
}

class AufonodaTour {
  constructor(steps, options) {
    this.steps = steps || [];
    this.current = 0;
    this.options = Object.assign({ storageKey: 'autonodal_tour_done', onComplete: function(){}, onSkip: function(){} }, options || {});
    this.overlay = null;
    this.tooltip = null;
    this._scrollH = null;
    this._resizeH = null;
    this._keyH = null;
  }

  static shouldRun(key) { return !localStorage.getItem(key || 'autonodal_tour_done'); }

  start() {
    if (!this.steps.length) return;
    this._buildDOM();
    this._showStep(0);
    var self = this;
    this._keyH = function(e) {
      if (e.key === 'ArrowRight' || e.key === 'Enter') self.next();
      if (e.key === 'ArrowLeft') self.back();
      if (e.key === 'Escape') self.skip();
    };
    document.addEventListener('keydown', this._keyH);
  }

  _buildDOM() {
    this.overlay = document.createElement('div');
    this.overlay.id = 'tour-overlay';
    this.overlay.innerHTML = '<div id="tour-spotlight"></div>';
    document.body.appendChild(this.overlay);

    this.tooltip = document.createElement('div');
    this.tooltip.id = 'tour-tooltip';
    var n = this.steps.length;
    this.tooltip.innerHTML =
      '<div id="tour-header"><span id="tour-step-label"></span><button id="tour-skip">Skip tour</button></div>' +
      '<div id="tour-icon"></div><h3 id="tour-title"></h3><p id="tour-body"></p>' +
      '<div id="tour-action-wrap"></div>' +
      '<div id="tour-footer"><div id="tour-dots"></div><div id="tour-actions"><button id="tour-back">←</button><button id="tour-next">Next →</button></div></div>';
    document.body.appendChild(this.tooltip);

    var self = this;
    this.tooltip.querySelector('#tour-skip').onclick = function() { self.skip(); };
    this.tooltip.querySelector('#tour-back').onclick = function() { self.back(); };
    this.tooltip.querySelector('#tour-next').onclick = function() { self.next(); };
  }

  _showStep(index) {
    if (index >= this.steps.length) { this.complete(); return; }
    this.current = index;
    var step = this.steps[index];
    var isFirst = index === 0;
    var isLast = index === this.steps.length - 1;

    document.getElementById('tour-icon').innerHTML = lucideSVG(step.icon || 'sparkles', 28, 2);
    document.getElementById('tour-title').textContent = step.title;
    document.getElementById('tour-body').innerHTML = step.body;
    document.getElementById('tour-step-label').textContent = (index + 1) + ' of ' + this.steps.length;

    // Inline action button (e.g. "Open My Profile")
    var actionWrap = document.getElementById('tour-action-wrap');
    if (actionWrap) {
      actionWrap.innerHTML = '';
      if (step.action && step.action.href) {
        var a = document.createElement('a');
        a.href = step.action.href;
        a.textContent = step.action.label || 'Open';
        a.style.cssText = 'display:inline-block;margin-top:10px;padding:8px 14px;background:#2563eb;color:#fff;text-decoration:none;border-radius:6px;font-size:13px;font-weight:500';
        actionWrap.appendChild(a);
      }
    }

    document.getElementById('tour-back').style.display = isFirst ? 'none' : 'inline-flex';
    var nextBtn = document.getElementById('tour-next');
    nextBtn.innerHTML = isLast
      ? '<span style="display:inline-flex;align-items:center;gap:6px;">Got it ' + lucideSVG('check', 14, 2.5) + '</span>'
      : 'Next →';
    nextBtn.className = isLast ? 'btn-got-it' : '';

    var dots = '';
    for (var i = 0; i < this.steps.length; i++) dots += '<span class="tour-dot' + (i === index ? ' active' : '') + '"></span>';
    document.getElementById('tour-dots').innerHTML = dots;

    this._spotlight(step.target);
  }

  _spotlight(selector) {
    if (this._scrollH) window.removeEventListener('scroll', this._scrollH);
    if (this._resizeH) window.removeEventListener('resize', this._resizeH);

    if (!selector) {
      document.getElementById('tour-spotlight').style.cssText = 'opacity:0;pointer-events:none';
      this._positionTooltip(null);
      return;
    }

    var el = document.querySelector(selector);
    if (!el) { this._positionTooltip(null); return; }

    el.scrollIntoView({ behavior: 'smooth', block: 'center' });

    var self = this;
    setTimeout(function() {
      self._positionSpotlight(el);
      self._positionTooltip(el);
    }, 300);

    this._scrollH = function() { self._positionSpotlight(el); self._positionTooltip(el); };
    this._resizeH = this._scrollH;
    window.addEventListener('scroll', this._scrollH, { passive: true });
    window.addEventListener('resize', this._resizeH);
  }

  _positionSpotlight(el) {
    if (!el) return;
    var r = el.getBoundingClientRect();
    var pad = 8;
    document.getElementById('tour-spotlight').style.cssText =
      'position:fixed;top:' + (r.top - pad) + 'px;left:' + (r.left - pad) + 'px;width:' + (r.width + pad * 2) +
      'px;height:' + (r.height + pad * 2) + 'px;border-radius:10px;box-shadow:0 0 0 9999px rgba(0,0,0,0.72);pointer-events:none;transition:all 0.25s ease;';
  }

  _positionTooltip(el) {
    var tt = this.tooltip;
    var vw = window.innerWidth, vh = window.innerHeight;
    var tw = Math.min(420, window.innerWidth - 24), th = tt.offsetHeight || 220;

    if (!el) {
      tt.style.cssText = 'position:fixed;top:' + ((vh - th) / 2) + 'px;left:' + ((vw - tw) / 2) + 'px;width:' + tw + 'px;';
      return;
    }
    var r = el.getBoundingClientRect();
    var top, left;
    if (r.bottom + th + 16 < vh) { top = r.bottom + 16; left = Math.max(12, Math.min(r.left, vw - tw - 12)); }
    else if (r.top - th - 16 > 0) { top = r.top - th - 16; left = Math.max(12, Math.min(r.left, vw - tw - 12)); }
    else if (r.right + tw + 16 < vw) { top = Math.max(12, r.top); left = r.right + 16; }
    else { top = Math.max(12, r.top); left = r.left - tw - 16; }
    tt.style.cssText = 'position:fixed;top:' + top + 'px;left:' + Math.max(12, left) + 'px;width:' + tw + 'px;';
  }

  next() { this.current >= this.steps.length - 1 ? this.complete() : this._showStep(this.current + 1); }
  back() { if (this.current > 0) this._showStep(this.current - 1); }
  skip() { this._teardown(); localStorage.setItem(this.options.storageKey, '1'); this.options.onSkip(); }
  complete() { this._teardown(); localStorage.setItem(this.options.storageKey, '1'); this.options.onComplete(); }

  _teardown() {
    if (this.overlay) this.overlay.remove();
    if (this.tooltip) this.tooltip.remove();
    if (this._scrollH) window.removeEventListener('scroll', this._scrollH);
    if (this._resizeH) window.removeEventListener('resize', this._resizeH);
    if (this._keyH) document.removeEventListener('keydown', this._keyH);
  }
}

// Tour steps for the main dashboard
var DASHBOARD_TOUR_STEPS = [
  { target: null, icon: 'sparkles', title: 'Welcome to Autonodal', body: 'Your personal signal intelligence platform. We\'ll walk you through the key features in about 90 seconds.<br><br>Press <code>→</code> to advance or <code>Esc</code> to skip.' },
  { target: '.command-section', icon: 'zap', title: 'Semantic search', body: 'Search across people, companies, signals, and case studies using natural language. Try "VP engineering fintech APAC" or "companies raising capital".' },
  { target: '#regionGrid', icon: 'globe', title: 'Regional deal intelligence', body: 'Signals grouped by geography, ranked by client relationships, network density, and strategic priority. Click a region to filter.' },
  { target: '#insightCard, #eventsDashboard', icon: 'compass', title: 'Insights & events', body: 'Daily intelligence insights compare your network against your mission. Events show forward-looking signals — what\'s about to happen.' },
  { target: '#networkFeed', icon: 'link', title: 'Network signals', body: 'Hover over proximity to see who has the warmest relationship in a huddle or company instance, share or claim the signal, or generate dispatch content from the card.' },
  { target: '.masthead-user', icon: 'plug-zap', title: 'Wire up your network', body: 'Next, head to <strong>My Profile &amp; Data</strong> to connect the data that powers your intelligence:<br><br>• <strong>LinkedIn</strong> — import connections, map your 1st-degree network<br>• <strong>Gmail / Calendar</strong> — auto-detect warm intros and interaction recency<br>• <strong>CRM</strong> (HubSpot, Ezekia) — sync contacts, accounts, and pipeline<br>• <strong>Sales CSV / Xero</strong> — revenue history and client relationships<br><br>Each connection unlocks a layer: the platform triangulates relationship overlaps, market activity, and timing to surface where your network is active right now.', action: { label: 'Open My Profile →', href: '/profile.html' } },
  { target: null, icon: 'target', title: 'Deploy your mission', body: 'From your profile, set your <strong>mission context</strong> — what signals are you seeking and why?<br><br>• <strong>Investment</strong> — capital raises, M&amp;A, founder activity<br>• <strong>Sales &amp; BD</strong> — expansion, buying signals, partnerships<br>• <strong>Talent &amp; People</strong> — leadership moves, team changes, capital raises<br>• <strong>Advisory &amp; Partnerships</strong> — strategic moves, restructuring<br><br>Your mission tunes the feed, ranks opportunities, and shapes the daily brief. You can adjust anytime.' },
  { target: null, icon: 'rocket', title: 'You\'re set up', body: 'Your feed is live. Signals appear as sources are harvested.<br><br>Hover any <span style="color:#f59e0b;display:inline-flex;align-items:center;vertical-align:middle;">' + '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>' + '</span> icon to learn more about any feature.' },
];
