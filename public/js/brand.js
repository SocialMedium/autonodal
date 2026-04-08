// Shared masthead setup: tenant brand name, logo link, user avatar + logout
(function() {
  // Find logo element — try id first, then class
  var logo = document.getElementById('mastheadLogo')
    || document.querySelector('.masthead-logo')
    || document.querySelector('.mast-logo');

  // Make logo clickable → dashboard
  if (logo) {
    if (logo.tagName !== 'A') {
      logo.style.cursor = 'pointer';
      logo.onclick = function() { location.href = '/index.html'; };
    } else if (!logo.href || logo.href === '#') {
      logo.href = '/index.html';
    }
  }

  // Set brand name from cache (instant)
  var cached = localStorage.getItem('ml_tenant_name');
  if (cached && logo) logo.innerHTML = cached + ' <em>Signals</em>';

  // User avatar — set initials + logout handler
  var avatar = document.getElementById('userAvatar');
  if (avatar) {
    try {
      var u = JSON.parse(localStorage.getItem('ml_user') || '{}');
      var initials = (u.name || u.email || '?').split(' ').map(function(w) { return w[0]; }).join('').toUpperCase().slice(0, 2);
      avatar.textContent = initials;
    } catch (e) {}

    // Add logout on click if not already wired
    if (!avatar.onclick && !avatar.getAttribute('onclick')) {
      avatar.style.cursor = 'pointer';
      avatar.title = 'Sign out';
      avatar.onclick = function() {
        if (!confirm('Sign out?')) return;
        var token = localStorage.getItem('ml_token');
        if (token) fetch('/api/auth/logout', { method: 'POST', headers: { Authorization: 'Bearer ' + token } }).catch(function() {});
        localStorage.removeItem('ml_token');
        localStorage.removeItem('ml_user');
        localStorage.removeItem('ml_tenant_name');
        location.href = '/index.html';
      };
    }
  }

  // Fetch fresh tenant name (updates cache, fixes stale brand)
  var token = localStorage.getItem('ml_token');
  if (!token) return;
  fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var name = (data.tenant && data.tenant.name) || (data.user && data.user.tenant_name) || '';
      if (name) {
        localStorage.setItem('ml_tenant_name', name);
        if (logo) logo.innerHTML = name + ' <em>Signals</em>';
        document.title = document.title.replace(/^.*?(?=—|$)/, name + ' Signals ');
      }
    })
    .catch(function() {});
})();
