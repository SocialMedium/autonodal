// ═══════════════════════════════════════════════════════════════════════════════
// public/js/hints.js — Contextual hover tooltip system
// ═══════════════════════════════════════════════════════════════════════════════

(function() {
  var dismissed = {};
  try { dismissed = JSON.parse(localStorage.getItem('autonodal_hints_dismissed') || '{}'); } catch(e) {}

  var activeHint = null;
  var hideTimer = null;

  function saveDismissed() {
    localStorage.setItem('autonodal_hints_dismissed', JSON.stringify(dismissed));
  }

  function showHint(triggerEl) {
    clearTimeout(hideTimer);
    if (activeHint) activeHint.remove();

    var text = triggerEl.getAttribute('data-hint');
    var title = triggerEl.getAttribute('data-hint-title') || '';
    var hintId = triggerEl.getAttribute('data-hint-id') || text.slice(0, 20);

    var el = document.createElement('div');
    el.className = 'hint-popup';
    el.innerHTML =
      (title ? '<div class="hint-popup-title">' + title + '</div>' : '') +
      '<div class="hint-popup-body">' + text + '</div>' +
      '<div class="hint-popup-footer"><button class="hint-got-it" data-dismiss="' + hintId + '">Got it</button></div>';

    el.addEventListener('mouseenter', function() { clearTimeout(hideTimer); });
    el.addEventListener('mouseleave', function() { scheduleHide(); });
    el.querySelector('.hint-got-it').addEventListener('click', function() {
      dismissed[this.getAttribute('data-dismiss')] = true;
      saveDismissed();
      if (activeHint) activeHint.remove();
      activeHint = null;
      var icons = document.querySelectorAll('[data-hint-id="' + hintId + '"]');
      for (var i = 0; i < icons.length; i++) icons[i].classList.add('hint-dismissed');
    });

    document.body.appendChild(el);
    positionHint(el, triggerEl);
    activeHint = el;
  }

  function positionHint(hintEl, triggerEl) {
    var tr = triggerEl.getBoundingClientRect();
    var hw = 260;
    var hh = hintEl.offsetHeight || 100;
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var top = tr.bottom + 8;
    var left = tr.left;
    if (top + hh > vh - 20) top = tr.top - hh - 8;
    left = Math.max(8, Math.min(left, vw - hw - 8));
    hintEl.style.cssText = 'position:fixed;top:' + top + 'px;left:' + left + 'px;width:' + hw + 'px;z-index:8000;';
  }

  function scheduleHide() {
    hideTimer = setTimeout(function() {
      if (activeHint) activeHint.remove();
      activeHint = null;
    }, 220);
  }

  // Bind to all [data-hint] elements — use event delegation for dynamic content
  document.addEventListener('mouseenter', function(e) {
    if (!e.target || !e.target.closest) return;
    var el = e.target.closest('[data-hint]');
    if (el) showHint(el);
  }, true);

  document.addEventListener('mouseleave', function(e) {
    if (!e.target || !e.target.closest) return;
    var el = e.target.closest('[data-hint]');
    if (el) scheduleHide();
  }, true);

  // Global API
  window._hints = {
    resetAll: function() {
      dismissed = {};
      saveDismissed();
      var icons = document.querySelectorAll('.hint-dismissed');
      for (var i = 0; i < icons.length; i++) icons[i].classList.remove('hint-dismissed');
    }
  };
})();
