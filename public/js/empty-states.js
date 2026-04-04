// ═══════════════════════════════════════════════════════════════════════════════
// public/js/empty-states.js — Empty state coaching for new users
// ═══════════════════════════════════════════════════════════════════════════════

var EMPTY_STATES = {
  'signal-feed': { icon: '\u26a1', title: 'Your signal feed is warming up', body: 'First signals will appear within a few minutes as your sources are scanned. The more feeds you activate, the richer your intelligence becomes.', action: { label: 'Add feeds', href: '/profile.html' } },
  'network-panel': { icon: '\ud83d\udc65', title: 'Connect to see warm paths', body: 'Connect Gmail to see who in your network has the strongest relationship with the companies generating signals in your market.', action: { label: 'Connect Gmail', href: '/profile.html' } },
  'events-section': { icon: '\ud83d\udcc5', title: 'No upcoming events in your markets', body: 'Events matching your sectors and geographies appear here \u2014 the only forward-looking signal type.', action: { label: 'Browse events', href: '/signals.html?tab=events' } },
  'huddles-list': { icon: '\ud83e\udd1d', title: 'No active huddles', body: 'Create a huddle to temporarily pool network proximity with investors, advisors, or partners. Clean exit removes all shared visibility immediately.', action: { label: 'Create a huddle', onclick: 'createHuddle()' } },
  'signals-page': { icon: '\u26a1', title: 'Signals are on their way', body: 'Your sources are being scanned. High-confidence signals for your market will appear here as they are detected.', action: { label: 'Check your feeds', href: '/profile.html' } },
  'search-results': { icon: '\ud83d\udd0d', title: 'No results yet', body: 'Try broader terms, or add more signal sources to increase coverage.', action: null },
};

function renderEmptyState(sectionId, containerId) {
  var state = EMPTY_STATES[sectionId];
  if (!state) return;
  var el = typeof containerId === 'string' ? document.getElementById(containerId) : containerId;
  if (!el) return;
  var actionHtml = '';
  if (state.action) {
    actionHtml = state.action.href
      ? '<a href="' + state.action.href + '" class="empty-action">' + state.action.label + ' \u2192</a>'
      : '<button onclick="' + state.action.onclick + '" class="empty-action">' + state.action.label + ' \u2192</button>';
  }
  el.innerHTML = '<div class="empty-state"><span class="empty-icon">' + state.icon + '</span><h3 class="empty-title">' + state.title + '</h3><p class="empty-body">' + state.body + '</p>' + actionHtml + '</div>';
}
