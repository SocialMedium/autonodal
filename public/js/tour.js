// ═══════════════════════════════════════════════════════════════════════════════
// public/js/tour.js — Guided spotlight tour engine
// No dependencies. Dark editorial design.
// ═══════════════════════════════════════════════════════════════════════════════

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

    document.getElementById('tour-icon').textContent = step.icon || '✦';
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
    nextBtn.textContent = isLast ? 'Got it ✓' : 'Next →';
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
  { target: null, icon: '✦', title: 'Welcome to Autonodal', body: 'Your personal signal intelligence platform. We\'ll walk you through the key features in about 90 seconds.<br><br>Press <code>→</code> to advance or <code>Esc</code> to skip.' },
  { target: '.command-section', icon: '⚡', title: 'Semantic search', body: 'Search across people, companies, signals, and case studies using natural language. Try "VP engineering fintech APAC" or "companies raising capital".' },
  { target: '#regionGrid', icon: '🌍', title: 'Regional deal intelligence', body: 'Signals grouped by geography, ranked by client relationships, network density, and strategic priority. Click a region to filter.' },
  { target: '#insightCard, #eventsDashboard', icon: '🧭', title: 'Insights & events', body: 'Daily intelligence insights compare your network against your mission. Events show forward-looking signals — what\'s about to happen.' },
  { target: '#networkFeed', icon: '🔗', title: 'Network signals', body: 'Hover over proximity to see who has the warmest relationship in a huddle or company instance, share or claim the signal, or generate dispatch content from the card.' },
  { target: '.masthead-user', icon: '🔌', title: 'Wire up your network', body: 'Next, head to <strong>My Profile &amp; Data</strong> to connect the data that powers your intelligence:<br><br>• <strong>LinkedIn</strong> — import connections, map your 1st-degree network<br>• <strong>Gmail / Calendar</strong> — auto-detect warm intros and interaction recency<br>• <strong>CRM</strong> (HubSpot, Ezekia) — sync contacts, accounts, and pipeline<br>• <strong>Sales CSV / Xero</strong> — revenue history and client relationships<br><br>Each connection unlocks a layer: the platform triangulates relationship overlaps, market activity, and timing to surface where your network is active right now.', action: { label: 'Open My Profile →', href: '/profile.html' } },
  { target: null, icon: '🎯', title: 'Deploy your mission', body: 'From your profile, set your <strong>mission context</strong> — what signals are you seeking and why?<br><br>• <strong>Investment</strong> — capital raises, M&amp;A, founder activity<br>• <strong>Sales &amp; BD</strong> — expansion, buying signals, partnerships<br>• <strong>Talent &amp; People</strong> — leadership moves, team changes, capital raises<br>• <strong>Advisory &amp; Partnerships</strong> — strategic moves, restructuring<br><br>Your mission tunes the feed, ranks opportunities, and shapes the daily brief. You can adjust anytime.' },
  { target: null, icon: '🚀', title: 'You\'re set up', body: 'Your feed is live. Signals appear as sources are harvested.<br><br>Hover any <span style="color:#f59e0b">ⓘ</span> icon to learn more about any feature.' },
];
