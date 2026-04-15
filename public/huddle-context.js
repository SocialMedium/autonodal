// ═══════════════════════════════════════════════════════════════════════════════
// huddle-context.js — Huddle context switcher + API context injection
//
// Include on every page after the masthead. Injects context switcher into nav,
// manages huddle state in sessionStorage, and wraps fetch to auto-append
// huddle_id to all API calls.
//
// Usage: <script src="/huddle-context.js"></script>
// ═══════════════════════════════════════════════════════════════════════════════

(function() {
  var TK = localStorage.getItem('ml_token');
  if (!TK) return; // Not logged in

  // Lucide SVG icons — no emoji slop
  var _icDash = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9"/><rect x="14" y="3" width="7" height="5"/><rect x="14" y="12" width="7" height="9"/><rect x="3" y="16" width="7" height="5"/></svg>';
  var _icAtom = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="1"/><path d="M20.2 20.2c2.04-2.03.02-7.36-4.5-11.9-4.54-4.52-9.87-6.54-11.9-4.5-2.04 2.03-.02 7.36 4.5 11.9 4.54 4.52 9.87 6.54 11.9 4.5Z"/><path d="M15.7 15.7c4.52-4.54 6.54-9.87 4.5-11.9-2.03-2.04-7.36-.02-11.9 4.5-4.52 4.54-6.54 9.87-4.5 11.9 2.03 2.04 7.36.02 11.9-4.5Z"/></svg>';
  var _icPlus = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>';

  // ─── State ───
  var activeHuddleId = sessionStorage.getItem('activeHuddleId') || null;
  var activeHuddleName = sessionStorage.getItem('activeHuddleName') || null;
  var huddles = [];

  // ─── Inject context switcher into masthead ───
  function injectSwitcher() {
    var brand = document.querySelector('.masthead-brand');
    if (!brand) return;

    var el = document.createElement('div');
    el.id = 'ctx-switcher';
    el.style.cssText = 'position:relative;margin-left:16px;';
    el.innerHTML =
      '<button id="ctxBtn" onclick="document.getElementById(\'ctxDrop\').classList.toggle(\'hc-show\')" ' +
      'style="display:flex;align-items:center;gap:6px;padding:5px 12px;border-radius:8px;border:1px solid var(--rule);' +
      'background:var(--surface);font-size:13px;font-weight:500;color:var(--ink-1);cursor:pointer;transition:all .15s;white-space:nowrap">' +
      '<span id="ctxIcon">' + (activeHuddleId ? _icAtom : _icDash) + '</span>' +
      '<span id="ctxName">' + esc(activeHuddleName || getTenantName()) + '</span>' +
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 9l6 6 6-6"/></svg>' +
      '</button>' +
      '<div id="ctxDrop" style="display:none;position:absolute;top:100%;left:0;margin-top:4px;width:280px;' +
      'background:#fff;border:1px solid var(--rule);border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,.12);z-index:999;padding:4px 0">' +
      '<div id="ctxList"></div></div>';
    brand.appendChild(el);

    // Close on outside click
    document.addEventListener('click', function(e) {
      if (!el.contains(e.target)) document.getElementById('ctxDrop').classList.remove('hc-show');
    });

    loadHuddles();
  }

  // ─── Load user's huddles ───
  function loadHuddles() {
    fetch('/api/huddles', { headers: { Authorization: 'Bearer ' + TK } })
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(function(data) {
        huddles = Array.isArray(data) ? data : (data.rows || data.huddles || []);
        renderDropdown();
      })
      .catch(function() { huddles = []; renderDropdown(); });
  }

  // ─── Render dropdown ───
  function renderDropdown() {
    var list = document.getElementById('ctxList');
    if (!list) return;

    var html = '';
    // Main tenant
    html += '<button onclick="window._switchCtx(null)" style="' + itemStyle(!activeHuddleId) + '">' +
      '<span style="opacity:.5">' + _icDash + '</span>' +
      '<div style="flex:1;min-width:0"><div style="font-size:13px;font-weight:500">' + esc(getTenantName()) + '</div>' +
      '<div style="font-size:11px;color:var(--ink-4)">Main dashboard</div></div></button>';

    if (huddles.length > 0) {
      html += '<div style="border-top:1px solid var(--rule);margin:4px 0"></div>';
      for (var i = 0; i < huddles.length; i++) {
        var h = huddles[i];
        var isActive = activeHuddleId === h.id;
        html += '<button onclick="window._switchCtx(\'' + h.id + '\',\'' + esc(h.name) + '\')" style="' + itemStyle(isActive) + '">' +
          '<span style="opacity:.5">' + _icAtom + '</span>' +
          '<div style="flex:1;min-width:0"><div style="font-size:13px;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(h.name) + '</div>' +
          '<div style="font-size:11px;color:var(--ink-4);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' +
          esc(h.purpose || h.description || '') + ' &middot; ' + (h.member_count || 1) + ' members</div></div></button>';
      }
    }

    html += '<div style="border-top:1px solid var(--rule);margin:4px 0"></div>';
    html += '<button onclick="window.location=\'/huddles.html\'" style="' + itemStyle(false) + 'color:var(--blue)">' +
      '<span style="opacity:.5">' + _icPlus + '</span>' +
      '<div style="font-size:13px;font-weight:500">Manage Huddles</div></button>';

    list.innerHTML = html;
  }

  function itemStyle(active) {
    return 'display:flex;align-items:center;gap:10px;width:100%;text-align:left;padding:8px 14px;border:none;' +
      'background:' + (active ? 'var(--blue-soft,#eff6ff)' : 'transparent') + ';cursor:pointer;transition:background .1s;';
  }

  // ─── Switch context ───
  window._switchCtx = function(huddleId, huddleName) {
    if (huddleId) {
      sessionStorage.setItem('activeHuddleId', huddleId);
      sessionStorage.setItem('activeHuddleName', huddleName || '');
      activeHuddleId = huddleId;
      activeHuddleName = huddleName || '';
    } else {
      sessionStorage.removeItem('activeHuddleId');
      sessionStorage.removeItem('activeHuddleName');
      activeHuddleId = null;
      activeHuddleName = null;
    }

    // Update button
    var icon = document.getElementById('ctxIcon');
    var name = document.getElementById('ctxName');
    if (icon) icon.innerHTML = huddleId ? _icAtom : _icDash;
    if (name) textContent = huddleId ? huddleName : getTenantName();
    if (name) name.textContent = huddleId ? huddleName : getTenantName();

    // Update banner
    updateBanner();

    // Close dropdown
    var drop = document.getElementById('ctxDrop');
    if (drop) drop.classList.remove('hc-show');

    // Re-render dropdown to show active state
    renderDropdown();

    // Reload page data
    if (typeof refreshPageData === 'function') refreshPageData();
    else window.location.reload();
  };

  // ─── Huddle banner ───
  function updateBanner() {
    var existing = document.getElementById('huddleBanner');
    if (activeHuddleId) {
      if (!existing) {
        existing = document.createElement('div');
        existing.id = 'huddleBanner';
        existing.style.cssText = 'background:linear-gradient(135deg,#eff6ff,#f0f9ff);padding:6px 16px;font-size:13px;' +
          'color:#1e40af;display:flex;align-items:center;gap:8px;border-bottom:1px solid #bfdbfe';
        var masthead = document.querySelector('.masthead');
        if (masthead) masthead.after(existing);
      }
      var h = huddles.find(function(h) { return h.id === activeHuddleId; });
      existing.innerHTML = '<span style="display:inline-flex;vertical-align:middle">' + _icAtom + '</span> Viewing through: <strong>' + esc(activeHuddleName || '') + '</strong>' +
        (h?.purpose ? '<span style="color:#3b82f680;margin:0 6px">&middot;</span><span style="color:#3b82f6aa;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:400px">' + esc(h.purpose) + '</span>' : '') +
        '<button onclick="window._openEditHuddle()" style="margin-left:auto;background:none;border:1px solid #93c5fd;' +
        'padding:3px 10px;border-radius:6px;font-size:12px;color:#2563eb;cursor:pointer;font-weight:500">Edit Mission</button>' +
        '<button onclick="window._switchCtx(null)" style="margin-left:6px;background:none;border:1px solid #93c5fd;' +
        'padding:3px 10px;border-radius:6px;font-size:12px;color:#2563eb;cursor:pointer;font-weight:500">Exit &#x2715;</button>';
      existing.style.display = 'flex';
    } else if (existing) {
      existing.style.display = 'none';
    }
  }

  // ─── Wrap global api() to append huddle_id ───
  if (typeof window.api === 'function') {
    var _origApi = window.api;
    window.api = function(url, opts) {
      var hid = sessionStorage.getItem('activeHuddleId');
      if (hid && url.indexOf('huddle_id=') === -1) {
        url += (url.indexOf('?') >= 0 ? '&' : '?') + 'huddle_id=' + hid;
      }
      return _origApi(url, opts);
    };
  }

  // ─── CSS for dropdown show ───
  var style = document.createElement('style');
  style.textContent = '.hc-show{display:block!important} #ctxBtn:hover{border-color:var(--blue);background:var(--blue-soft,#eff6ff)} ' +
    '#ctxList button:hover{background:var(--blue-soft,#eff6ff)!important}';
  document.head.appendChild(style);

  // ─── Helpers ───
  function esc(s) { return s ? String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/'/g, '&#39;').replace(/"/g, '&quot;') : ''; }
  function getTenantName() {
    try { return JSON.parse(localStorage.getItem('ml_user') || '{}').tenant_name || 'Dashboard'; } catch(e) { return 'Dashboard'; }
  }

  // ─── Edit Huddle Modal ───
  window._openEditHuddle = function() {
    if (!activeHuddleId) return;
    var h = huddles.find(function(x) { return x.id === activeHuddleId; }) || {};
    var cfg = h.signal_config || {};

    var overlay = document.createElement('div');
    overlay.id = 'editHuddleOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:9999;display:flex;align-items:center;justify-content:center';
    overlay.onclick = function(e) { if (e.target === overlay) overlay.remove(); };

    var geos = cfg.geography || [];
    var secs = cfg.sectors || [];
    var geoOpts = ['US','UK','EU','AU','SG','SEA','GLOBAL'];
    var secOpts = ['web3','fintech','saas','ai','health','climate','enterprise','consumer'];

    var geoPills = geoOpts.map(function(g) {
      var active = geos.indexOf(g) >= 0;
      return '<button type="button" data-geo="' + g + '" onclick="this.classList.toggle(\'hc-pill-on\')" class="hc-pill' + (active ? ' hc-pill-on' : '') + '">' + g + '</button>';
    }).join('');

    var secPills = secOpts.map(function(s) {
      var active = secs.indexOf(s) >= 0;
      return '<button type="button" data-sec="' + s + '" onclick="this.classList.toggle(\'hc-pill-on\')" class="hc-pill' + (active ? ' hc-pill-on' : '') + '">' + s.charAt(0).toUpperCase() + s.slice(1) + '</button>';
    }).join('');

    overlay.innerHTML =
      '<div style="background:#fff;border-radius:12px;width:480px;max-width:95vw;max-height:90vh;overflow:auto;padding:28px 32px;box-shadow:0 20px 60px rgba(0,0,0,.2)">' +
      '<div style="font-size:18px;font-weight:600;margin-bottom:16px">Edit Huddle</div>' +
      '<label style="display:block;font-size:13px;font-weight:500;margin-bottom:4px;color:#374151">Name</label>' +
      '<input id="ehName" value="' + esc(h.name || '') + '" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;margin-bottom:12px;box-sizing:border-box">' +
      '<label style="display:block;font-size:13px;font-weight:500;margin-bottom:4px;color:#374151">Mission</label>' +
      '<textarea id="ehMission" rows="3" style="width:100%;padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;margin-bottom:12px;box-sizing:border-box;resize:vertical">' + esc(h.purpose || '') + '</textarea>' +
      '<label style="display:block;font-size:13px;font-weight:500;margin-bottom:6px;color:#374151">Focus geographies</label>' +
      '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px" id="ehGeos">' + geoPills + '</div>' +
      '<label style="display:block;font-size:13px;font-weight:500;margin-bottom:6px;color:#374151">Focus sectors</label>' +
      '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:20px" id="ehSecs">' + secPills + '</div>' +
      '<div style="display:flex;gap:10px;justify-content:flex-end">' +
      '<button onclick="document.getElementById(\'editHuddleOverlay\').remove()" style="padding:8px 18px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;cursor:pointer;background:#fff">Cancel</button>' +
      '<button id="ehSave" onclick="window._saveEditHuddle()" style="padding:8px 24px;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;background:#2563eb;color:#fff">Save</button>' +
      '</div></div>';

    document.body.appendChild(overlay);
  };

  window._saveEditHuddle = function() {
    var btn = document.getElementById('ehSave');
    if (btn) { btn.textContent = 'Saving...'; btn.disabled = true; }

    var geography = [];
    document.querySelectorAll('#ehGeos .hc-pill-on').forEach(function(el) { geography.push(el.dataset.geo); });
    var sectors = [];
    document.querySelectorAll('#ehSecs .hc-pill-on').forEach(function(el) { sectors.push(el.dataset.sec); });

    var body = {
      name: document.getElementById('ehName').value,
      purpose: document.getElementById('ehMission').value,
      geography: geography,
      sectors: sectors,
    };

    fetch('/api/huddles/' + activeHuddleId, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + TK },
      body: JSON.stringify(body)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      document.getElementById('editHuddleOverlay').remove();
      // Update local state
      sessionStorage.setItem('activeHuddleName', body.name);
      activeHuddleName = body.name;
      // Reload huddles and page data
      loadHuddles();
      updateBanner();
      if (typeof refreshPageData === 'function') refreshPageData();
      else window.location.reload();
    })
    .catch(function(e) {
      if (btn) { btn.textContent = 'Save'; btn.disabled = false; }
      alert('Failed to save: ' + e.message);
    });
  };

  // ─── Pill CSS ───
  style.textContent += ' .hc-pill{padding:4px 12px;border-radius:20px;border:1px solid #d1d5db;font-size:12px;background:#fff;cursor:pointer;transition:all .15s}' +
    ' .hc-pill-on{background:#2563eb;color:#fff;border-color:#2563eb}' +
    ' .hc-pill:hover{border-color:#2563eb}';

  // ─── Init on DOM ready ───
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() { injectSwitcher(); updateBanner(); });
  } else {
    injectSwitcher(); updateBanner();
  }
})();
