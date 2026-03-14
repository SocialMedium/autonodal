// MitchelLake Signal Concierge — self-injecting chat widget
// Usage: <script src="/chat.js"></script>
(function() {
  'use strict';

  // ═══ Inject HTML ═══
  const container = document.createElement('div');
  container.id = 'mlChat';
  container.innerHTML = `
  <button id="chatFab" title="AI Concierge (⌘J)">
    <svg id="chatFabIcon" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path></svg>
    <svg id="chatFabClose" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="display:none"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
  </button>
  <div id="chatDrawer" class="chat-drawer">
    <div class="ch-hdr">
      <div class="ch-hdr-l"><div class="ch-av">ML</div><div><div class="ch-title">Signal Concierge</div><div class="ch-sub">Ask anything · Drop intel · Upload files</div></div></div>
      <button class="ch-clear" id="chatClearBtn" title="Clear"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4h8v2M5 6v14a2 2 0 002 2h10a2 2 0 002-2V6"/></svg></button>
    </div>
    <div class="ch-msgs" id="chatMessages">
      <div class="ch-welcome"><div class="ch-welcome-t">What can I help with?</div>
        <div class="ch-sugg">
          <button data-q="Who do we know at companies that raised funding in the last 90 days?">🔍 Recent funding × our network</button>
          <button data-q="Show me candidates with research notes about being open to new roles">📋 Open-to-move candidates</button>
          <button data-q="What are our top 5 clients by placement revenue?">💰 Top clients by revenue</button>
          <button data-q="Give me a summary of platform stats">📊 Platform overview</button>
        </div>
      </div>
    </div>
    <div class="ch-input-area">
      <div class="ch-file-ind" id="chatFileInd" style="display:none"><span id="chatFileName"></span><button id="chatFileClear">✕</button></div>
      <div class="ch-input-row">
        <label class="ch-upload" title="Upload CSV, PDF, or text"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.44 11.05l-9.19 9.19a6 6 0 01-8.49-8.49l9.19-9.19a4 4 0 015.66 5.66l-9.2 9.19a2 2 0 01-2.83-2.83l8.49-8.48"/></svg><input type="file" id="chatFileInput" accept=".csv,.pdf,.txt" style="display:none"></label>
        <textarea id="chatInput" placeholder="Ask a question, share intel, or upload a file…" rows="1"></textarea>
        <button id="chatSendBtn" class="ch-send" disabled><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"></line><polygon points="22 2 15 22 11 13 2 9 22 2"></polygon></svg></button>
      </div>
    </div>
  </div>`;
  document.body.appendChild(container);

  // ═══ Inject CSS ═══
  const style = document.createElement('style');
  style.textContent = `
#chatFab{position:fixed;bottom:24px;right:24px;width:56px;height:56px;border-radius:50%;background:#1A1A1A;color:white;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 20px rgba(0,0,0,.2);transition:all .2s;z-index:9999}
#chatFab:hover{transform:scale(1.08);box-shadow:0 6px 28px rgba(0,0,0,.25)}
.chat-drawer{position:fixed;bottom:92px;right:24px;width:420px;max-height:calc(100vh - 120px);background:#FFF;border:1px solid #E0DDD8;border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,.12),0 4px 20px rgba(0,0,0,.06);display:none;flex-direction:column;z-index:9998;overflow:hidden;font-family:'DM Sans',system-ui,sans-serif;font-size:14px;color:#1A1A1A}
.chat-drawer.open{display:flex;animation:chSlide .25s ease-out}
@keyframes chSlide{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
.ch-hdr{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #E0DDD8;background:#FAF9F7}
.ch-hdr-l{display:flex;align-items:center;gap:10px}
.ch-av{width:34px;height:34px;border-radius:10px;background:#1A1A1A;color:white;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;letter-spacing:.5px}
.ch-title{font-size:14px;font-weight:600;color:#1A1A1A}
.ch-sub{font-size:10px;color:#7A7A7A}
.ch-clear{background:none;border:none;cursor:pointer;padding:6px;color:#AAA;border-radius:6px}
.ch-clear:hover{background:#F2F4F8;color:#7A7A7A}
.ch-msgs{flex:1;overflow-y:auto;padding:14px 18px;min-height:280px;max-height:calc(100vh - 300px)}
.ch-welcome{text-align:center;padding:16px 0}
.ch-welcome-t{font-family:'Source Serif 4',serif;font-size:17px;font-weight:600;margin-bottom:14px}
.ch-sugg{display:flex;flex-direction:column;gap:5px}
.ch-sugg button{background:#F7F5F2;border:1px solid #E0DDD8;border-radius:8px;padding:9px 12px;font-size:12px;color:#4A4A4A;cursor:pointer;text-align:left;font-family:'DM Sans',system-ui;transition:all .15s}
.ch-sugg button:hover{background:#EBF1FE;border-color:#2563EB;color:#1A1A1A}
.msg{margin-bottom:12px;animation:mFade .2s ease-out}
@keyframes mFade{from{opacity:0;transform:translateY(4px)}to{opacity:1}}
.msg-u{text-align:right}
.msg-u .mb{display:inline-block;max-width:85%;text-align:left;background:#1A1A1A;color:white;border-radius:14px 14px 4px 14px;padding:9px 14px;font-size:13px;line-height:1.5}
.msg-b .mb{max-width:95%;background:#F7F5F2;color:#1A1A1A;border-radius:14px 14px 14px 4px;padding:11px 15px;font-size:13px;line-height:1.6}
.msg-tools{font-size:9px;color:#AAA;margin-bottom:3px;display:flex;gap:5px;flex-wrap:wrap}
.msg-tbadge{background:#F2F4F8;padding:2px 7px;border-radius:3px;font-weight:600;font-family:'IBM Plex Mono',monospace}
.msg-loading .mb{background:#F7F5F2;display:flex;align-items:center;gap:8px;padding:11px 15px}
.tdots{display:flex;gap:3px}
.tdots span{width:6px;height:6px;border-radius:50%;background:#AAA;animation:tBounce 1.2s infinite}
.tdots span:nth-child(2){animation-delay:.15s}
.tdots span:nth-child(3){animation-delay:.3s}
@keyframes tBounce{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-6px)}}
.mb h3{font-size:14px;font-weight:600;margin:8px 0 3px}.mb h4{font-size:13px;font-weight:600;margin:6px 0 2px}.mb p{margin:3px 0}.mb ul,.mb ol{margin:3px 0;padding-left:16px}.mb li{margin:1px 0;font-size:13px}
.mb code{background:rgba(0,0,0,.06);padding:1px 4px;border-radius:3px;font-family:'IBM Plex Mono',monospace;font-size:12px}
.mb pre{background:rgba(0,0,0,.06);padding:8px;border-radius:5px;overflow-x:auto;margin:5px 0;font-size:12px}.mb pre code{background:none;padding:0}
.mb a{color:#2563EB;text-decoration:none;font-weight:500}.mb a:hover{text-decoration:underline}
.mb strong{font-weight:600}
.mb table{font-size:11px;border-collapse:collapse;margin:5px 0;width:100%}.mb th,.mb td{padding:3px 7px;border-bottom:1px solid #E0DDD8;text-align:left}.mb th{font-weight:600;background:rgba(0,0,0,.03)}
.ch-file-ind{display:flex;align-items:center;gap:8px;padding:5px 10px;margin:0 10px;background:#EBF1FE;border-radius:5px;font-size:11px;color:#2563EB;font-weight:500}
.ch-file-ind button{background:none;border:none;cursor:pointer;color:#2563EB;font-size:13px;padding:0 2px}
.ch-input-area{border-top:1px solid #E0DDD8;padding:10px 10px 12px;background:#FAF9F7}
.ch-input-row{display:flex;align-items:flex-end;gap:7px}
.ch-upload{cursor:pointer;padding:8px;color:#7A7A7A;border-radius:8px;flex-shrink:0;display:flex;align-items:center}
.ch-upload:hover{background:#F2F4F8;color:#1A1A1A}
#chatInput{flex:1;border:1px solid #E0DDD8;border-radius:12px;padding:9px 13px;font-size:13px;font-family:'DM Sans',system-ui;resize:none;outline:none;min-height:38px;max-height:120px;background:white;color:#1A1A1A;line-height:1.4}
#chatInput:focus{border-color:#2563EB;box-shadow:0 0 0 2px rgba(37,99,235,.1)}#chatInput::placeholder{color:#AAA}
.ch-send{width:38px;height:38px;border-radius:10px;background:#1A1A1A;color:white;border:none;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:all .15s}
.ch-send:hover:not(:disabled){background:#2563EB}.ch-send:disabled{opacity:.3;cursor:default}
@media(max-width:480px){.chat-drawer{width:calc(100vw - 16px);right:8px;bottom:84px;border-radius:14px}#chatFab{bottom:16px;right:16px;width:50px;height:50px}}
@media(max-width:768px){.chat-drawer{width:calc(100vw - 32px);right:16px}}
  `;
  document.head.appendChild(style);

  // ═══ State ═══
  let isOpen = false, isLoading = false, fileId = null, fileName = null;
  const $ = id => document.getElementById(id);
  const TK = () => localStorage.getItem('ml_token');

  // ═══ Events ═══
  $('chatFab').onclick = () => {
    isOpen = !isOpen;
    $('chatDrawer').classList.toggle('open', isOpen);
    $('chatFabIcon').style.display = isOpen ? 'none' : 'block';
    $('chatFabClose').style.display = isOpen ? 'block' : 'none';
    if (isOpen) $('chatInput').focus();
  };

  $('chatInput').addEventListener('input', function() {
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 120) + 'px';
    $('chatSendBtn').disabled = this.value.trim().length === 0 && !fileId;
  });

  $('chatInput').addEventListener('keydown', function(e) {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
  });

  $('chatSendBtn').onclick = send;
  $('chatClearBtn').onclick = clear;
  $('chatFileClear').onclick = clearFile;

  $('chatFileInput').addEventListener('change', function(e) {
    const file = e.target.files[0];
    if (!file) return;
    const fd = new FormData();
    fd.append('file', file);
    $('chatFileInd').style.display = 'flex';
    $('chatFileName').textContent = '⏳ Uploading ' + file.name + '…';

    fetch('/api/chat/upload', { method: 'POST', headers: { Authorization: 'Bearer ' + TK() }, body: fd })
      .then(r => r.json())
      .then(d => {
        if (d.error) throw new Error(d.error);
        fileId = d.file_id; fileName = d.filename;
        const liLabel = d.linkedin_type ? ` [LinkedIn ${d.linkedin_type.charAt(0).toUpperCase() + d.linkedin_type.slice(1)}]` : '';
        $('chatFileName').textContent = '📎 ' + d.filename + liLabel + (d.row_count ? ` (${d.row_count} rows)` : '') + (d.pages ? ` (${d.pages} pages)` : '');
        $('chatSendBtn').disabled = false;
      })
      .catch(() => { $('chatFileName').textContent = '❌ Failed'; setTimeout(clearFile, 2000); });
    e.target.value = '';
  });

  // Suggestion buttons (delegated)
  $('chatMessages').addEventListener('click', function(e) {
    const btn = e.target.closest('[data-q]');
    if (btn) { $('chatInput').value = btn.dataset.q; send(); }
  });

  // Keyboard shortcuts
  document.addEventListener('keydown', function(e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'j') { e.preventDefault(); $('chatFab').click(); }
    if (e.key === 'Escape' && isOpen) $('chatFab').click();
  });

  function clearFile() {
    fileId = null; fileName = null;
    $('chatFileInd').style.display = 'none';
    $('chatSendBtn').disabled = $('chatInput').value.trim().length === 0;
  }

  // ═══ Messaging ═══
  function addMsg(role, content, tools) {
    const c = $('chatMessages');
    const w = c.querySelector('.ch-welcome'); if (w) w.remove();
    const div = document.createElement('div');
    div.className = 'msg ' + (role === 'user' ? 'msg-u' : 'msg-b');
    let h = '';
    if (tools && tools.length) {
      h += '<div class="msg-tools">' + tools.map(t => '<span class="msg-tbadge">🔧 ' + t.replace(/_/g,' ') + '</span>').join('') + '</div>';
    }
    h += '<div class="mb">' + (role === 'user' ? esc(content) : md(content)) + '</div>';
    div.innerHTML = h;
    c.appendChild(div);
    c.scrollTop = c.scrollHeight;
  }

  function addLoading() {
    const c = $('chatMessages');
    const div = document.createElement('div');
    div.className = 'msg msg-b msg-loading'; div.id = 'chatLoading';
    div.innerHTML = '<div class="mb"><div class="tdots"><span></span><span></span><span></span></div><span style="font-size:11px;color:#AAA">Searching…</span></div>';
    c.appendChild(div); c.scrollTop = c.scrollHeight;
  }

  // ═══ Send ═══
  async function send() {
    const input = $('chatInput');
    const msg = input.value.trim();
    if (!msg && !fileId) return;
    if (isLoading) return;

    addMsg('user', msg + (fileName ? '\n📎 ' + fileName : ''));
    input.value = ''; input.style.height = 'auto';
    $('chatSendBtn').disabled = true;

    const fid = fileId; clearFile();
    isLoading = true; addLoading();

    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + TK() },
        body: JSON.stringify({ message: msg || 'Process the uploaded file', file_id: fid }),
      });
      const data = await res.json();
      const el = $('chatLoading'); if (el) el.remove();
      if (data.error) addMsg('bot', '❌ ' + data.error);
      else addMsg('bot', data.response, data.tools_used);
    } catch (err) {
      const el = $('chatLoading'); if (el) el.remove();
      addMsg('bot', '❌ Connection error.');
    }
    isLoading = false;
  }

  function clear() {
    $('chatMessages').innerHTML = `<div class="ch-welcome"><div class="ch-welcome-t">What can I help with?</div><div class="ch-sugg">
      <button data-q="Who do we know at companies that raised funding in the last 90 days?">🔍 Recent funding × our network</button>
      <button data-q="Show me candidates with research notes about being open to new roles">📋 Open-to-move candidates</button>
      <button data-q="What are our top 5 clients by placement revenue?">💰 Top clients by revenue</button>
      <button data-q="Give me a summary of platform stats">📊 Platform overview</button>
    </div></div>`;
    fetch('/api/chat/history', { method: 'DELETE', headers: { Authorization: 'Bearer ' + TK() } });
  }

  // ═══ Helpers ═══
  function esc(s) { return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }

  function md(text) {
    if (!text) return '';
    let h = esc(text);
    h = h.replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
    h = h.replace(/`([^`]+)`/g, '<code>$1</code>');
    h = h.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    h = h.replace(/\*([^*]+)\*/g, '<em>$1</em>');
    h = h.replace(/\[([^\]]+)\]\((\/(?:person|company)\.html\?id=[^)]+)\)/g, '<a href="$2">$1</a>');
    h = h.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');
    h = h.replace(/^#### (.+)$/gm, '<h4>$1</h4>');
    h = h.replace(/^### (.+)$/gm, '<h3>$1</h3>');
    h = h.replace(/^- (.+)$/gm, '<li>$1</li>');
    h = h.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');
    h = h.replace(/^\d+\. (.+)$/gm, '<li>$1</li>');
    h = h.replace(/^---$/gm, '<hr style="border:none;border-top:1px solid #E0DDD8;margin:6px 0">');
    h = h.replace(/\n\n/g, '</p><p>');
    h = h.replace(/\n/g, '<br>');
    h = '<p>' + h + '</p>';
    h = h.replace(/<p><(h[34]|ul|ol|pre|hr)/g, '<$1');
    h = h.replace(/<\/(h[34]|ul|ol|pre)><\/p>/g, '</$1>');
    return h;
  }

})();