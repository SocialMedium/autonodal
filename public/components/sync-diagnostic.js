/**
 * Sync Diagnostic Panel — Embeddable Component
 *
 * Usage:
 *   <div id="sync-diagnostic"></div>
 *   <script src="/components/sync-diagnostic.js"></script>
 *   <script>
 *     SyncDiagnostic.show('sync-diagnostic', {
 *       error: 'Request failed with status 401',
 *       context: { integration: 'HubSpot', count: 847 }
 *     });
 *   </script>
 *
 * Or auto-diagnose from an API call:
 *   SyncDiagnostic.diagnose('sync-diagnostic', errorMessage, context);
 */
(function() {
  'use strict';

  var TOKEN = localStorage.getItem('ml_token');
  var SEVERITY_STYLES = {
    info:    { bg: '#eff6ff', border: '#bfdbfe', icon: '\u2139\ufe0f', accent: '#2563eb' },
    warning: { bg: '#fffbeb', border: '#fde68a', icon: '\u26a0\ufe0f', accent: '#d97706' },
    error:   { bg: '#fef2f2', border: '#fecaca', icon: '\u274c',       accent: '#dc2626' },
  };

  function injectStyles() {
    if (document.getElementById('sd-styles')) return;
    var style = document.createElement('style');
    style.id = 'sd-styles';
    style.textContent = [
      '.sd-panel { border-radius: 12px; padding: 20px; font-family: Inter, system-ui, sans-serif; }',
      '.sd-title { font-size: 15px; font-weight: 700; margin-bottom: 6px; display: flex; align-items: center; gap: 8px; }',
      '.sd-explain { font-size: 13px; line-height: 1.5; color: #475569; margin-bottom: 16px; }',
      '.sd-note { font-size: 12px; color: #64748b; font-style: italic; margin-bottom: 16px; }',
      '.sd-options { display: flex; flex-direction: column; gap: 8px; margin-bottom: 16px; }',
      '.sd-option { display: flex; align-items: center; gap: 10px; padding: 10px 14px; border-radius: 8px; cursor: pointer; border: 1px solid #e2e8f0; transition: all 0.15s; min-height: 44px; font-family: inherit; background: white; text-align: left; width: 100%; font-size: 13px; }',
      '.sd-option:hover { border-color: #2563eb; background: #eff6ff; }',
      '.sd-option.selected { border-color: #2563eb; background: #eff6ff; }',
      '.sd-radio { width: 16px; height: 16px; border-radius: 50%; border: 2px solid #e2e8f0; flex-shrink: 0; display: flex; align-items: center; justify-content: center; }',
      '.sd-option.selected .sd-radio { border-color: #2563eb; }',
      '.sd-option.selected .sd-radio::after { content: ""; width: 8px; height: 8px; border-radius: 50%; background: #2563eb; }',
      '.sd-rec { font-size: 10px; color: #2563eb; font-weight: 600; margin-left: auto; white-space: nowrap; }',
      '.sd-actions { display: flex; gap: 8px; flex-wrap: wrap; }',
      '.sd-btn { padding: 10px 20px; border: none; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; font-family: inherit; transition: all 0.2s; }',
      '.sd-btn-primary { background: #2563eb; color: white; }',
      '.sd-btn-primary:hover { background: #1d4ed8; }',
      '.sd-btn-primary:disabled { background: #e2e8f0; color: #94a3b8; cursor: not-allowed; }',
      '.sd-btn-ghost { background: none; border: 1px solid #e2e8f0; color: #64748b; }',
      '.sd-btn-ghost:hover { border-color: #94a3b8; }',
      '.sd-raw { margin-top: 12px; font-size: 11px; color: #94a3b8; background: #f8fafc; padding: 8px 10px; border-radius: 6px; font-family: monospace; display: none; word-break: break-all; }',
      '.sd-spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #bfdbfe; border-top-color: #2563eb; border-radius: 50%; animation: sd-spin 0.8s linear infinite; vertical-align: middle; margin-right: 6px; }',
      '@keyframes sd-spin { to { transform: rotate(360deg); } }',
      '.sd-resolved { padding: 16px 20px; border-radius: 12px; background: #ecfdf5; border: 1px solid #a7f3d0; }',
      '.sd-resolved-text { font-size: 13px; font-weight: 600; color: #059669; }',
    ].join('\n');
    document.head.appendChild(style);
  }

  function esc(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

  async function apiCall(path, opts) {
    var res = await fetch(path, Object.assign({
      headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    }, opts || {}));
    if (!res.ok) throw new Error('API error ' + res.status);
    return res.json();
  }

  function renderDiagnosis(containerId, diagnosis) {
    injectStyles();
    var el = document.getElementById(containerId);
    if (!el) return;

    var sev = SEVERITY_STYLES[diagnosis.severity] || SEVERITY_STYLES.info;
    var selectedAction = null;

    var html = '<div class="sd-panel" style="background:' + sev.bg + ';border:1px solid ' + sev.border + '">';

    // Title
    html += '<div class="sd-title">' + sev.icon + ' ' + esc(diagnosis.title) + '</div>';

    // Explanation
    html += '<div class="sd-explain">' + esc(diagnosis.explanation) + '</div>';

    // Action options
    html += '<div class="sd-options" id="sd-opts-' + containerId + '">';
    (diagnosis.actions || []).forEach(function(action, idx) {
      var cls = action.recommended ? ' selected' : '';
      if (action.recommended) selectedAction = action.id;
      html += '<button class="sd-option' + cls + '" data-action="' + esc(action.id) + '" onclick="window._sdSelect(\'' + containerId + '\',\'' + esc(action.id) + '\',' + idx + ')">';
      html += '<div class="sd-radio"></div>';
      html += '<span>' + esc(action.label) + '</span>';
      if (action.recommended) html += '<span class="sd-rec">recommended</span>';
      html += '</button>';
    });
    html += '</div>';

    // Action buttons
    html += '<div class="sd-actions">';
    html += '<button class="sd-btn sd-btn-primary" id="sd-apply-' + containerId + '" onclick="window._sdApply(\'' + containerId + '\')">Apply</button>';
    html += '<button class="sd-btn sd-btn-ghost" onclick="var r=document.getElementById(\'sd-raw-' + containerId + '\');r.style.display=r.style.display===\'none\'?\'block\':\'none\'">Tell me more</button>';
    html += '</div>';

    // Raw error (hidden)
    html += '<div class="sd-raw" id="sd-raw-' + containerId + '">' + esc(diagnosis.raw_error || '') + '</div>';
    html += '</div>';

    el.innerHTML = html;
    el._diagnosis = diagnosis;
    el._selectedAction = selectedAction;
  }

  window._sdSelect = function(containerId, actionId, idx) {
    var el = document.getElementById(containerId);
    if (el) el._selectedAction = actionId;
    var opts = document.querySelectorAll('#sd-opts-' + containerId + ' .sd-option');
    opts.forEach(function(o, i) {
      o.classList.toggle('selected', i === idx);
    });
  };

  window._sdApply = async function(containerId) {
    var el = document.getElementById(containerId);
    if (!el || !el._selectedAction) return;

    var btn = document.getElementById('sd-apply-' + containerId);
    btn.innerHTML = '<span class="sd-spinner"></span> Resolving...';
    btn.disabled = true;

    try {
      var result = await apiCall('/api/onboarding/diagnostic/resolve', {
        method: 'POST',
        body: JSON.stringify({
          action_id: el._selectedAction,
          context: el._diagnosis ? { integration: 'sync' } : {},
        }),
      });

      if (result.redirect_url) {
        window.location = result.redirect_url;
        return;
      }

      el.innerHTML = '<div class="sd-resolved"><div class="sd-resolved-text">\u2713 ' + esc(result.message || 'Resolved') + '</div></div>';
    } catch (e) {
      btn.innerHTML = 'Retry';
      btn.disabled = false;
    }
  };

  // Public API
  window.SyncDiagnostic = {
    /**
     * Show a pre-classified diagnosis in a container.
     */
    show: function(containerId, diagnosis) {
      renderDiagnosis(containerId, diagnosis);
    },

    /**
     * Classify an error via the API and show the diagnosis.
     */
    diagnose: async function(containerId, errorMessage, context) {
      injectStyles();
      var el = document.getElementById(containerId);
      if (el) el.innerHTML = '<div style="text-align:center;padding:20px"><span class="sd-spinner"></span> Diagnosing...</div>';

      try {
        var diagnosis = await apiCall('/api/onboarding/diagnostic', {
          method: 'POST',
          body: JSON.stringify({ error_message: errorMessage, context: context || {} }),
        });
        renderDiagnosis(containerId, diagnosis);
      } catch (e) {
        if (el) el.innerHTML = '<div style="padding:16px;color:#dc2626;font-size:13px">Diagnostic failed: ' + esc(e.message) + '</div>';
      }
    },

    /**
     * Hide the diagnostic panel.
     */
    hide: function(containerId) {
      var el = document.getElementById(containerId);
      if (el) el.innerHTML = '';
    },
  };
})();
