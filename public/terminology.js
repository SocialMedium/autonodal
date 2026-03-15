// terminology.js — shared frontend terminology loader
// Include in every HTML page: <script src="/terminology.js"></script>
// Then use window.t.opportunity, window.t.person, etc. in your rendering code
// Or use data-t="key" attributes on elements for auto-replacement

(async function loadTerminology() {
  const token = localStorage.getItem('ml_token');
  if (!token) return;

  try {
    const res = await fetch('/api/config/terminology', {
      headers: { 'Authorization': 'Bearer ' + token }
    });
    if (!res.ok) return;

    const { terminology, signal_labels, vertical } = await res.json();
    window.t = terminology;
    window.signalLabels = signal_labels;
    window.vertical = vertical;

    // Auto-replace elements with data-t attributes
    // Usage: <span data-t="opportunities">Searches</span>
    document.querySelectorAll('[data-t]').forEach(function(el) {
      var key = el.getAttribute('data-t');
      if (window.t[key]) el.textContent = window.t[key];
    });
  } catch (e) {
    // Fallback — use talent vertical defaults
    window.t = window.t || {};
    window.vertical = window.vertical || 'talent';
  }
})();
