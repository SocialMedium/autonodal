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
      '<span id="ctxIcon">' + (activeHuddleId ? '&#x1F91D;' : '&#x1F3E2;') + '</span>' +
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
      '<span style="font-size:16px">&#x1F3E2;</span>' +
      '<div style="flex:1;min-width:0"><div style="font-size:13px;font-weight:500">' + esc(getTenantName()) + '</div>' +
      '<div style="font-size:11px;color:var(--ink-4)">Main dashboard</div></div></button>';

    if (huddles.length > 0) {
      html += '<div style="border-top:1px solid var(--rule);margin:4px 0"></div>';
      for (var i = 0; i < huddles.length; i++) {
        var h = huddles[i];
        var isActive = activeHuddleId === h.id;
        html += '<button onclick="window._switchCtx(\'' + h.id + '\',\'' + esc(h.name) + '\')" style="' + itemStyle(isActive) + '">' +
          '<span style="font-size:16px">&#x1F91D;</span>' +
          '<div style="flex:1;min-width:0"><div style="font-size:13px;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(h.name) + '</div>' +
          '<div style="font-size:11px;color:var(--ink-4);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' +
          esc(h.purpose || h.description || '') + ' &middot; ' + (h.member_count || 1) + ' members</div></div></button>';
      }
    }

    html += '<div style="border-top:1px solid var(--rule);margin:4px 0"></div>';
    html += '<button onclick="window.location=\'/huddles.html\'" style="' + itemStyle(false) + 'color:var(--blue)">' +
      '<span style="font-size:16px">+</span>' +
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
    if (icon) icon.innerHTML = huddleId ? '&#x1F91D;' : '&#x1F3E2;';
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
      existing.innerHTML = '<span>&#x1F3AF;</span> Viewing through: <strong>' + esc(activeHuddleName || '') + '</strong>' +
        (h?.purpose ? '<span style="color:#3b82f680;margin:0 6px">&middot;</span><span style="color:#3b82f6aa;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:400px">' + esc(h.purpose) + '</span>' : '') +
        '<button onclick="window._switchCtx(null)" style="margin-left:auto;background:none;border:1px solid #93c5fd;' +
        'padding:3px 10px;border-radius:6px;font-size:12px;color:#2563eb;cursor:pointer;font-weight:500">Exit Huddle &#x2715;</button>';
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

  // ─── Init on DOM ready ───
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() { injectSwitcher(); updateBanner(); });
  } else {
    injectSwitcher(); updateBanner();
  }
})();
