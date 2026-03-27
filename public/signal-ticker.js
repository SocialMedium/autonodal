/**
 * Signal Index Ticker — Market Health Monitor
 * Self-contained widget. Drops into any page via:
 *   <div id="signal-ticker"></div>
 *   <script src="/signal-ticker.js"></script>
 */
(function() {
  var HORIZON = '7d';
  var LABELS = {
    capital_raising: 'Capital', ma_activity: 'M&A', product_launch: 'Product',
    leadership_change: 'Leadership', strategic_hiring: 'Hiring',
    geographic_expansion: 'Expansion', partnership: 'Partnership',
    layoffs: 'Layoffs', restructuring: 'Restructuring'
  };

  function fmt(n) { n = parseInt(n) || 0; return n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : n.toString(); }
  function fmtD(d) { d = parseFloat(d) || 0; return (d > 0 ? '+' : '') + d.toFixed(1) + '%'; }
  function arrow(d) { return d === 'up' ? '\u2191' : d === 'down' ? '\u2193' : '\u2192'; }
  function dirCls(d, s) {
    if (d === 'flat') return 'si-flat';
    if (s === 'bullish') return d === 'up' ? 'si-bull' : 'si-bear';
    if (s === 'bearish') return d === 'up' ? 'si-bear' : 'si-bull';
    return d === 'up' ? 'si-bull' : d === 'down' ? 'si-bear' : 'si-flat';
  }

  function injectCSS() {
    if (document.getElementById('si-css')) return;
    var s = document.createElement('style'); s.id = 'si-css';
    s.textContent = [
      '#signal-ticker{position:fixed;left:0;right:0;height:34px;background:var(--surface,#fff);display:flex;align-items:stretch;z-index:9999;font-family:var(--sans,system-ui);font-size:11px;overflow:hidden}',
      '#signal-ticker.si-top{top:0;border-bottom:1px solid var(--rule,#e5e7eb);box-shadow:0 1px 4px rgba(0,0,0,.04)}',
      '#signal-ticker.si-bottom{bottom:0;border-top:1px solid var(--rule,#e5e7eb)}',
      'body.si-top-offset{padding-top:34px}',
      'body.si-bottom-offset{padding-bottom:34px}',
      '.si-live{display:flex;align-items:center;gap:5px;padding:0 10px;border-right:1px solid var(--rule,#e5e7eb);flex-shrink:0;white-space:nowrap}',
      '.si-dot{width:5px;height:5px;border-radius:50%;background:#10b981;animation:si-p 2s ease-in-out infinite;flex-shrink:0}',
      '@keyframes si-p{0%,100%{opacity:1}50%{opacity:.3}}',
      '.si-score{font-size:13px;font-weight:600;cursor:pointer;border-bottom:1px dotted var(--rule,#ddd)}',
      '.si-score:hover{border-bottom-style:solid}',
      '.si-bull{color:#059669} .si-bear{color:#dc2626} .si-flat{color:#94a3b8}',
      '.si-stat{display:flex;align-items:center;gap:3px;padding:0 8px;border-right:1px solid var(--rule,#e5e7eb);flex-shrink:0;color:var(--ink-3,#888);white-space:nowrap}',
      '.si-stat b{color:var(--ink,#333);font-weight:500}',
      '.si-scroll{flex:1;overflow:hidden;display:flex;align-items:center;position:relative}',
      '.si-inner{display:flex;align-items:center;animation:si-s 50s linear infinite;white-space:nowrap}',
      '.si-inner:hover{animation-play-state:paused}',
      '@keyframes si-s{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}',
      '.si-stk{display:inline-flex;align-items:center;gap:4px;padding:0 12px;height:34px;border-right:1px solid var(--rule,#e5e7eb);cursor:default}',
      '.si-stk-n{color:var(--ink,#333);font-weight:500}',
      '.si-bar{display:inline-block;width:24px;height:3px;background:var(--rule,#ddd);border-radius:2px;overflow:hidden;vertical-align:middle}',
      '.si-bar i{display:block;height:100%;border-radius:2px;transition:width .5s}',
      '.si-ctrl{display:flex;align-items:center;gap:1px;padding:0 6px;border-left:1px solid var(--rule,#e5e7eb);flex-shrink:0}',
      '.si-btn{padding:3px 6px;border:none;border-radius:4px;background:transparent;font-size:10px;font-family:inherit;color:var(--ink-3,#888);cursor:pointer;transition:background .1s}',
      '.si-btn:hover{background:var(--rule,#e5e7eb)}',
      '.si-btn.on{background:var(--rule,#e5e7eb);color:var(--ink,#333);font-weight:600}',
      '#si-panel{position:fixed;right:0;width:400px;background:var(--surface,#fff);border:1px solid var(--rule,#e5e7eb);padding:14px;z-index:9998;box-shadow:-2px -2px 12px rgba(0,0,0,.06);font-family:var(--sans,system-ui);font-size:11px;display:none}',
      '#si-panel.si-panel-top{top:34px;border-top:none;border-radius:0 0 0 8px}',
      '#si-panel.si-panel-bottom{bottom:34px;border-bottom:none;border-radius:8px 0 0 0}',
      '#si-panel.open{display:block}'
    ].join('\n');
    document.head.appendChild(s);
  }

  function bar(score) {
    var p = Math.round(score || 50);
    var c = p >= 60 ? '#059669' : p >= 40 ? '#94a3b8' : '#dc2626';
    return '<span class="si-bar"><i style="width:'+p+'%;background:'+c+'"></i></span>';
  }

  function render(data) {
    var el = document.getElementById('signal-ticker');
    if (!el) return;
    var mh = data.market_health || {};
    var st = data.stats || {};
    var stocks = data.signal_stocks || {};
    var cls = dirCls(mh.direction, 'bullish');

    var items = Object.entries(stocks).map(function(e) {
      var k = e[0], d = e[1];
      return '<span class="si-stk" title="'+(LABELS[k]||k)+': '+fmt(d.current_count)+' signals, '+fmtD(d.delta)+'">'+
        '<span class="si-stk-n">'+(LABELS[k]||k)+'</span>'+bar(d.score)+
        '<span class="'+dirCls(d.direction,d.sentiment)+'">'+arrow(d.direction)+' '+fmtD(d.delta)+'</span></span>';
    }).join('');

    el.innerHTML =
      '<div class="si-live"><span class="si-dot"></span>'+
        '<span class="si-score '+cls+'" onclick="window.__siChart()" title="Market Health Index">'+(mh.score?.toFixed?.(1)||'--')+'</span>'+
        '<span class="'+cls+'">'+arrow(mh.direction)+' '+fmtD(mh.delta)+'</span></div>'+
      '<div class="si-stat"><b>'+fmt(st.people_tracked)+'</b> people</div>'+
      '<div class="si-stat"><b>'+fmt(st.companies_tracked)+'</b> cos</div>'+
      '<div class="si-stat"><b>'+fmt(st.signals_7d)+'</b> 7d</div>'+
      '<div class="si-scroll"><div class="si-inner">'+items+items+'</div></div>'+
      '<div class="si-ctrl">'+
        ['7d','30d','90d'].map(function(h){return '<button class="si-btn'+(h===HORIZON?' on':'')+'" onclick="window.__siHz(\''+h+'\')">'+h+'</button>';}).join('')+
        '<button class="si-btn" onclick="window.__siSec()">Sectors</button></div>';
  }

  function renderPanel(sectors) {
    var p = document.getElementById('si-panel');
    if (!p) { p = document.createElement('div'); p.id = 'si-panel'; p.classList.add(_isTop ? 'si-panel-top' : 'si-panel-bottom'); document.body.appendChild(p); }
    var rows = Object.entries(sectors?.sectors || {}).sort(function(a,b){return b[1].score-a[1].score;}).map(function(e) {
      var cls = dirCls(e[1].direction, 'bullish');
      return '<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--rule,#eee)">'+
        '<span style="font-weight:500">'+e[0]+'</span>'+
        '<span>'+bar(e[1].score)+' <span class="'+cls+'" style="margin-left:4px">'+arrow(e[1].direction)+' '+fmtD(e[1].delta)+'</span></span></div>';
    }).join('');
    p.innerHTML = '<div style="display:flex;justify-content:space-between;margin-bottom:6px;font-weight:600"><span>Sectors \u2014 '+HORIZON+'</span><button onclick="window.__siSec()" style="border:none;background:none;cursor:pointer;font-size:14px;color:var(--ink-3,#888)">\u00d7</button></div>'+rows;
    return p;
  }

  function renderChart(history) {
    var p = document.getElementById('si-panel');
    if (!p) { p = document.createElement('div'); p.id = 'si-panel'; p.classList.add(_isTop ? 'si-panel-top' : 'si-panel-bottom'); document.body.appendChild(p); }
    var pts = (history?.history || []).slice(-60);
    if (pts.length < 2) { p.innerHTML = '<p style="color:var(--ink-3,#888)">Not enough history yet</p>'; p.classList.add('open'); return; }
    var scores = pts.map(function(p){return p.score;}), mn = Math.min.apply(null,scores)-3, mx = Math.max.apply(null,scores)+3;
    var W = 370, H = 100;
    var line = pts.map(function(pt,i){return (i/(pts.length-1))*W+','+(H-((pt.score-mn)/(mx-mn))*H);}).join(' ');
    var last = pts[pts.length-1], col = last.delta > 0 ? '#059669' : last.delta < 0 ? '#dc2626' : '#94a3b8';
    p.innerHTML = '<div style="display:flex;justify-content:space-between;margin-bottom:8px;font-weight:600"><span>Market Health \u2014 '+HORIZON+'</span><button onclick="window.__siChart()" style="border:none;background:none;cursor:pointer;font-size:14px;color:var(--ink-3,#888)">\u00d7</button></div>'+
      '<svg width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+'" style="overflow:visible">'+
        '<line x1="0" y1="'+(H-((50-mn)/(mx-mn))*H)+'" x2="'+W+'" y2="'+(H-((50-mn)/(mx-mn))*H)+'" stroke="#e2e8f0" stroke-width="1" stroke-dasharray="3 3"/>'+
        '<polygon points="0,'+H+' '+line+' '+W+','+H+'" fill="'+col+'" fill-opacity="0.1"/>'+
        '<polyline points="'+line+'" fill="none" stroke="'+col+'" stroke-width="1.5" stroke-linecap="round"/>'+
        '<circle cx="'+W+'" cy="'+(H-((last.score-mn)/(mx-mn))*H)+'" r="3" fill="'+col+'"/>'+
      '</svg>'+
      '<div style="display:flex;justify-content:space-between;margin-top:4px;font-size:10px;color:var(--ink-3,#888)"><span>'+(pts[0]?.snapshot_at?.slice(0,10)||'')+'</span><span style="color:'+col+';font-weight:600">'+last.score.toFixed(1)+' '+fmtD(last.delta)+'</span><span>'+(last.snapshot_at?.slice(0,10)||'')+'</span></div>';
    p.classList.add('open');
  }

  var _data = null, _sectors = null, _panelMode = null;

  async function api(path) {
    var token = localStorage.getItem('ml_token');
    var res = await fetch(path, { headers: token ? { Authorization: 'Bearer ' + token } : {} });
    return res.ok ? res.json() : null;
  }

  async function load(hz) {
    HORIZON = hz || HORIZON;
    var [d, s] = await Promise.all([
      api('/api/signal-index?horizon=' + HORIZON),
      api('/api/signal-index/sectors?horizon=' + HORIZON)
    ]);
    _data = d || _data; _sectors = s || _sectors;
    if (_data) render(_data);
    if (_panelMode === 'sectors' && _sectors) { renderPanel(_sectors); document.getElementById('si-panel').classList.add('open'); }
    if (_panelMode === 'chart') { var h = await api('/api/signal-index/history?horizon=' + HORIZON + '&limit=90'); if (h) renderChart(h); }
  }

  window.__siHz = function(h) { load(h); };
  window.__siSec = function() {
    var p = document.getElementById('si-panel');
    if (_panelMode === 'sectors') { _panelMode = null; if (p) p.classList.remove('open'); return; }
    _panelMode = 'sectors';
    if (_sectors) { renderPanel(_sectors); document.getElementById('si-panel').classList.add('open'); }
  };
  window.__siChart = function() {
    var p = document.getElementById('si-panel');
    if (_panelMode === 'chart') { _panelMode = null; if (p) p.classList.remove('open'); return; }
    _panelMode = 'chart';
    api('/api/signal-index/history?horizon=' + HORIZON + '&limit=90').then(function(h) { if (h) renderChart(h); });
  };

  var _script = document.currentScript;
  var _position = _script?.dataset?.position || 'bottom';
  var _isTop = _position === 'top';

  injectCSS();

  function applyPosition() {
    var el = document.getElementById('signal-ticker');
    if (el) {
      el.classList.add(_isTop ? 'si-top' : 'si-bottom');
      document.body.classList.add(_isTop ? 'si-top-offset' : 'si-bottom-offset');
    }
  }

  var init = function() { applyPosition(); load(HORIZON); };
  document.readyState === 'loading' ? document.addEventListener('DOMContentLoaded', init) : init();
  setInterval(function() { load(HORIZON); }, 5 * 60 * 1000);
})();
