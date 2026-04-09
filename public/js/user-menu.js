/**
 * Shared user menu dropdown — inject on any page with <div class="masthead-user" id="userAvatar">
 * Include: <script src="/js/user-menu.js"></script>
 */
(function() {
  var avatar = document.getElementById('userAvatar');
  if (!avatar) return;

  // Wrap avatar in menu container if not already wrapped
  var wrap = avatar.parentElement;
  if (!wrap || !wrap.classList.contains('user-menu-wrap')) {
    wrap = document.createElement('div');
    wrap.className = 'user-menu-wrap';
    wrap.id = 'userMenuWrap';
    avatar.parentNode.insertBefore(wrap, avatar);
    wrap.appendChild(avatar);
  } else {
    wrap.id = 'userMenuWrap';
  }

  // Remove any existing onclick on avatar
  avatar.removeAttribute('onclick');
  avatar.style.cursor = 'pointer';
  avatar.addEventListener('click', function(e) {
    e.stopPropagation();
    wrap.classList.toggle('open');
  });

  // Don't duplicate if dropdown already exists (e.g. index.html)
  if (document.getElementById('userDropdown')) return;

  // Inject styles if not present
  if (!document.querySelector('style[data-user-menu]')) {
    var style = document.createElement('style');
    style.setAttribute('data-user-menu', '1');
    style.textContent = [
      '.user-menu-wrap { position: relative; }',
      '.user-menu-wrap .um-dropdown { display: none; }',
      '.user-menu-wrap.open .um-dropdown { display: block; }',
      '.um-dropdown {',
      '  position: absolute; top: calc(100% + 4px); right: 0;',
      '  background: var(--surface, #fff); border: 1px solid var(--rule, #e2e8f0); border-radius: 10px;',
      '  box-shadow: 0 8px 30px rgba(0,0,0,0.12); min-width: 220px; z-index: 200;',
      '  padding: 8px 0; font-size: 13px; font-family: var(--sans, -apple-system, sans-serif);',
      '}',
      '.um-dropdown-header { padding: 12px 16px; border-bottom: 1px solid var(--rule, #e2e8f0); }',
      '.um-dropdown-name { font-weight: 600; font-size: 14px; color: var(--ink, #1e293b); }',
      '.um-dropdown-email { font-size: 11px; color: var(--ink-3, #64748b); margin-top: 1px; }',
      '.um-dropdown-role { font-size: 10px; color: var(--blue, #2563eb); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 3px; font-weight: 600; }',
      '.um-dropdown a, .um-dropdown button {',
      '  display: block; width: 100%; text-align: left; padding: 9px 16px;',
      '  font-size: 13px; color: var(--ink-2, #334155); text-decoration: none;',
      '  border: none; background: none; cursor: pointer; font-family: inherit;',
      '  border-radius: 0; transition: background 0.1s; box-sizing: border-box;',
      '}',
      '.um-dropdown a:hover, .um-dropdown button:hover { background: var(--surface-warm, #f8f9fa); color: var(--ink, #1e293b); }',
      '.um-dropdown .um-sep { border-top: 1px solid var(--rule, #e2e8f0); margin: 4px 0; }',
      '.um-dropdown .um-signout { color: var(--rose, #e24b4a); }',
      '.um-dropdown .um-signout:hover { background: #fef2f2; }',
    ].join('\n');
    document.head.appendChild(style);
  }

  // Build dropdown
  var dd = document.createElement('div');
  dd.className = 'um-dropdown';
  dd.id = 'userDropdown';
  dd.onclick = function(e) { e.stopPropagation(); };

  var user = null;
  try { user = JSON.parse(localStorage.getItem('ml_user') || 'null'); } catch(e) {}
  var tk = localStorage.getItem('ml_token');
  var name = user?.name || '';
  var email = user?.email || '';
  var role = user?.role || '';
  var isAdmin = role === 'admin' || role === 'owner';
  var initials = name ? name.split(' ').map(function(w) { return w[0]; }).join('').substring(0, 2).toUpperCase() : '?';

  // Set avatar initials
  avatar.textContent = initials;

  dd.innerHTML =
    '<div class="um-dropdown-header">' +
      '<div class="um-dropdown-name">' + esc(name) + '</div>' +
      '<div class="um-dropdown-email">' + esc(email) + '</div>' +
      (role ? '<div class="um-dropdown-role">' + esc(role) + '</div>' : '') +
    '</div>' +
    '<a href="/profile.html">My Profile & Data</a>' +
    (isAdmin ? '<a href="/admin.html">Admin Dashboard</a>' : '') +
    '<div class="um-sep"></div>' +
    '<button class="um-signout" id="umSignOut">Sign Out</button>';

  wrap.appendChild(dd);

  // Sign out handler
  document.getElementById('umSignOut').addEventListener('click', function() {
    if (!confirm('Sign out?')) return;
    fetch('/api/auth/logout', { method: 'POST', headers: { Authorization: 'Bearer ' + tk } }).catch(function(){});
    localStorage.removeItem('ml_token');
    localStorage.removeItem('ml_user');
    location.href = '/index.html';
  });

  // Close on click outside
  document.addEventListener('click', function() {
    wrap.classList.remove('open');
  });

  function esc(s) { return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }
})();
