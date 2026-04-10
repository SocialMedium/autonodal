// terminology.js — Vertical-aware label system
(function() {
  var TERM_CACHE = {};
  var _loaded = false;

  // Detect vertical from stored user prefs or default to 'all'
  function detectVertical() {
    try {
      var user = JSON.parse(localStorage.getItem('ml_user') || '{}');
      return user.vertical || 'all';
    } catch(e) { return 'all'; }
  }

  window.loadTerminology = async function(vertical) {
    vertical = vertical || detectVertical();
    try {
      var token = localStorage.getItem('ml_token');
      var res = await fetch('/api/terminology?vertical=' + encodeURIComponent(vertical), {
        headers: token ? { Authorization: 'Bearer ' + token } : {}
      });
      if (res.ok) {
        var data = await res.json();
        Object.assign(TERM_CACHE, data.labels || {});
        _loaded = true;
      }
    } catch(e) {}
    return TERM_CACHE;
  };

  window.t = function(key, fallback) {
    return TERM_CACHE[key] || fallback || key;
  };

  window.applyTerminology = function() {
    document.querySelectorAll('[data-term]').forEach(function(el) {
      var key = el.getAttribute('data-term');
      var label = t(key);
      if (label !== key) el.textContent = label;
    });
  };

  // Auto-load on DOMContentLoaded if not already loaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
      if (!_loaded) loadTerminology().then(applyTerminology);
    });
  }
})();
