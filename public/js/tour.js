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
      '<div id="tour-footer"><div id="tour-dots"></div><div id="tour-actions"><button id="tour-back">\u2190</button><button id="tour-next">Next \u2192</button></div></div>';
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

    document.getElementById('tour-icon').textContent = step.icon || '\u2726';
    document.getElementById('tour-title').textContent = step.title;
    document.getElementById('tour-body').innerHTML = step.body;
    document.getElementById('tour-step-label').textContent = (index + 1) + ' of ' + this.steps.length;

    document.getElementById('tour-back').style.display = isFirst ? 'none' : 'inline-flex';
    var nextBtn = document.getElementById('tour-next');
    nextBtn.textContent = isLast ? 'Got it \u2713' : 'Next \u2192';
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
  { target: null, icon: '\u2726', title: 'Welcome to Autonodal', body: 'Your personal signal intelligence platform. We\'ll walk you through the key features in about 90 seconds.<br><br>Press <code>\u2192</code> to advance or <code>Esc</code> to skip.' },
  { target: '.command-section', icon: '\u26a1', title: 'Semantic search', body: 'Search across people, companies, signals, and case studies using natural language. Try "VP engineering fintech APAC" or "companies raising capital".' },
  { target: '#regionGrid', icon: '\ud83c\udf0d', title: 'Regional deal intelligence', body: 'Signals grouped by geography, ranked by client relationships, network density, and strategic priority. Click a region to filter.' },
  { target: '#insightCard, #eventsDashboard', icon: '\ud83e\udded', title: 'Insights & events', body: 'Daily intelligence insights compare your network against your mission. Events show forward-looking signals \u2014 what\'s about to happen.' },
  { target: '#networkFeed', icon: '\ud83d\udd17', title: 'Network signals', body: 'Signals at companies where you have contacts, placements, or client relationships. The proximity popup shows who has the warmest path.' },
  { target: null, icon: '\ud83d\ude80', title: 'You\'re set up', body: 'Your feed is live. Signals appear as sources are harvested.<br><br>Hover any <span style="color:#f59e0b">\u24d8</span> icon to learn more about any feature.' },
];
