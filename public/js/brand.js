// Set tenant brand name in masthead
(function() {
  var logo = document.getElementById('mastheadLogo');
  if (!logo) return;

  // Try cached tenant name first (instant, no flash)
  var cached = localStorage.getItem('ml_tenant_name');
  if (cached) logo.innerHTML = cached + ' <em>Signals</em>';

  // Fetch fresh from auth/me (updates cache)
  var token = localStorage.getItem('ml_token');
  if (!token) return;
  fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var name = (data.tenant && data.tenant.name) || (data.user && data.user.tenant_name) || '';
      if (name) {
        localStorage.setItem('ml_tenant_name', name);
        logo.innerHTML = name + ' <em>Signals</em>';
        document.title = document.title.replace(/^.*Signals/, name + ' Signals');
      }
    })
    .catch(function() {});
})();
