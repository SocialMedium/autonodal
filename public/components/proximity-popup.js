// proximity-popup.js — Singleton D3 mini-graph popup for signal cards
// Usage: ProximityPopup.show(triggerEl, signalId)
//        ProximityPopup.scheduleHide()

const ProximityPopup = (() => {
  let W = 360, H = 210;
  const SHOW_DELAY = 120, HIDE_DELAY = 200, MARGIN = 12;

  const TC = [
    { fill: '#5DCAA5', stroke: '#1D9E75', text: '#04342C' },
    { fill: '#F0997B', stroke: '#D85A30', text: '#4A1B0C' },
    { fill: '#AFA9EC', stroke: '#7F77DD', text: '#26215C' },
    { fill: '#FAC775', stroke: '#EF9F27', text: '#412402' },
    { fill: '#85B7EB', stroke: '#378ADD', text: '#042C53' },
  ];
  const SC = {
    capital_raising: '#EF9F27', strategic_hiring: '#5DCAA5',
    geographic_expansion: '#378ADD', leadership_change: '#D4537E',
    ma_activity: '#AFA9EC', product_launch: '#97C459',
    layoffs: '#E24B4A', restructuring: '#E24B4A', partnership: '#85B7EB',
  };
  const SL = {
    capital_raising: 'Funding', strategic_hiring: 'Hiring',
    geographic_expansion: 'Expansion', leadership_change: 'Leadership',
    ma_activity: 'M&A', product_launch: 'Launch',
    layoffs: 'Layoffs', restructuring: 'Restructuring', partnership: 'Partnership',
  };

  let popEl = null, svgEl = null, sim = null;
  let hideTimer = null, currentId = null, overPopup = false;

  function mount() {
    if (popEl) return;
    popEl = document.createElement('div');
    popEl.id = 'prox-popup';
    popEl.style.cssText = 'position:fixed;z-index:9999;width:' + W + 'px;background:#06091a;border:0.5px solid rgba(255,255,255,0.14);border-radius:14px;box-shadow:0 24px 64px rgba(0,0,0,.72);pointer-events:none;opacity:0;transform:scale(.93) translateY(6px);transition:opacity .16s,transform .16s;overflow:hidden;font-family:var(--sans,system-ui)';
    popEl.innerHTML = '<div id="pp-head" style="padding:11px 14px 9px;border-bottom:0.5px solid rgba(255,255,255,0.08);display:flex;align-items:flex-start;justify-content:space-between">' +
      '<div><div id="pp-tag" style="display:inline-flex;align-items:center;gap:4px;font-size:8px;font-weight:600;letter-spacing:.09em;text-transform:uppercase;padding:2px 8px;border-radius:10px;margin-bottom:6px"></div>' +
      '<div id="pp-company" style="font-size:13px;font-weight:500;color:#fff;line-height:1.25"></div>' +
      '<div id="pp-headline" style="font-size:10px;color:rgba(255,255,255,0.36);margin-top:2px;line-height:1.35;max-height:28px;overflow:hidden"></div></div>' +
      '<div style="flex-shrink:0;margin-left:12px;text-align:right"><div style="font-size:8px;color:rgba(255,255,255,0.25);letter-spacing:.06em;text-transform:uppercase;margin-bottom:3px">confidence</div>' +
      '<div id="pp-conf-bar" style="width:44px;height:2.5px;border-radius:2px;background:rgba(255,255,255,0.1);overflow:hidden;margin-bottom:2px"><div id="pp-conf-fill" style="height:100%;border-radius:2px;transition:width .4s"></div></div>' +
      '<div id="pp-conf-val" style="font-size:9px;color:rgba(255,255,255,0.3)"></div></div></div>' +
      '<svg id="pp-svg" width="' + W + '" height="' + H + '" style="display:block;background:#06091a"></svg>' +
      '<div id="pp-ft" style="padding:8px 14px;border-top:0.5px solid rgba(255,255,255,0.08);display:flex;align-items:center;gap:8px;flex-wrap:wrap"></div>';
    document.body.appendChild(popEl);
    svgEl = popEl.querySelector('#pp-svg');

    // Dot grid
    var ns = 'http://www.w3.org/2000/svg';
    var grd = document.createElementNS(ns, 'g');
    grd.setAttribute('opacity', '0.07');
    for (var x = 20; x < W; x += 30) for (var y = 10; y < H; y += 30) {
      var c = document.createElementNS(ns, 'circle');
      c.setAttribute('cx', x); c.setAttribute('cy', y); c.setAttribute('r', '0.7'); c.setAttribute('fill', '#3366aa');
      grd.appendChild(c);
    }
    svgEl.appendChild(grd);
    popEl.addEventListener('mouseenter', function() { overPopup = true; clearTimeout(hideTimer); });
    popEl.addEventListener('mouseleave', function() { overPopup = false; scheduleHide(); });
  }

  function position(el) {
    if (popEl.classList.contains('pp-expanded')) return; // Don't reposition when expanded
    var tr = el.getBoundingClientRect();
    var pw = popEl.offsetWidth || W;
    var ph = popEl.offsetHeight || 300;
    var vw = window.innerWidth, vh = window.innerHeight;
    var left, top;
    // Prefer right of trigger, then left, then below, then above
    if (tr.right + pw + MARGIN < vw) { left = tr.right + 8; top = tr.top + tr.height / 2 - ph / 2; }
    else if (tr.left - pw - MARGIN > 0) { left = tr.left - pw - 8; top = tr.top + tr.height / 2 - ph / 2; }
    else if (tr.bottom + ph + MARGIN < vh) { left = Math.max(MARGIN, tr.left + tr.width / 2 - pw / 2); top = tr.bottom + 8; }
    else { left = Math.max(MARGIN, tr.left + tr.width / 2 - pw / 2); top = Math.max(MARGIN, tr.top - ph - 8); }
    left = Math.max(MARGIN, Math.min(vw - pw - MARGIN, left));
    top = Math.max(MARGIN, Math.min(vh - ph - MARGIN, top));
    popEl.style.left = left + 'px'; popEl.style.top = top + 'px';
  }

  function renderGraph(data) {
    var graph = data.graph, signal = data.signal;
    if (!graph || !graph.nodes.length) return;

    // Scale popup for larger graphs
    var nodeCount = graph.nodes.length;
    if (nodeCount > 10) { W = 480; H = 300; }
    else if (nodeCount > 6) { W = 420; H = 260; }
    else { W = 360; H = 210; }
    popEl.style.width = W + 'px';
    svgEl.setAttribute('width', W);
    svgEl.setAttribute('height', H);

    while (svgEl.children.length > 1) svgEl.removeChild(svgEl.lastChild);
    if (sim) { sim.stop(); sim = null; }

    var ns = 'http://www.w3.org/2000/svg';
    var teamIdx = 0, teamColorMap = {};
    graph.nodes.filter(function(n) { return n.type === 'team'; }).forEach(function(n) {
      teamColorMap[n.id] = TC[teamIdx % TC.length]; teamIdx++;
    });

    function nodeR(d) {
      if (d.type === 'team') return 18;
      if (d.type === 'company') return 14;
      return 4 + (d.bestStrength || 0) * 9;
    }
    function nodeColor(d) {
      if (d.type === 'company') return d.isClient ? '#EF9F27' : 'rgba(255,255,255,0.55)';
      if (d.type === 'team') return (teamColorMap[d.id] || TC[0]).fill;
      return 'rgba(255,255,255,0.45)';
    }

    var cx = W / 2, cy = H / 2;
    graph.nodes.forEach(function(n, i) {
      if (n.type === 'company') { n.x = cx; n.y = cy; }
      else if (n.type === 'team') { var a = (i / Math.max(1, graph.nodes.filter(function(x) { return x.type === 'team'; }).length)) * Math.PI * 2; n.x = cx + Math.cos(a) * 70; n.y = cy + Math.sin(a) * 55; }
      else { var a2 = (i / graph.nodes.length) * Math.PI * 2; n.x = cx + Math.cos(a2) * 120; n.y = cy + Math.sin(a2) * 70; }
    });

    var links = graph.links.map(function(l) { return Object.assign({}, l); });
    var pulseG = document.createElementNS(ns, 'g'); svgEl.appendChild(pulseG);
    var linkG = document.createElementNS(ns, 'g'); svgEl.appendChild(linkG);
    var haloG = document.createElementNS(ns, 'g'); svgEl.appendChild(haloG);
    var nodeG = document.createElementNS(ns, 'g'); svgEl.appendChild(nodeG);
    var labelG = document.createElementNS(ns, 'g'); svgEl.appendChild(labelG);

    var linkEls = links.map(function(l) {
      var line = document.createElementNS(ns, 'line');
      var sc2 = l.type === 'works_at' ? 'rgba(255,255,255,0.12)' : 'rgba(255,255,255,0.18)';
      line.setAttribute('stroke', sc2);
      line.setAttribute('stroke-width', (0.5 + (l.strength || 0.5) * 1.2).toFixed(2));
      linkG.appendChild(line);
      return line;
    });

    // Signal pulse on company
    var companyNode = graph.nodes.find(function(n) { return n.type === 'company'; });
    var pulseEls = [];
    if (companyNode) {
      var cp = document.createElementNS(ns, 'circle');
      cp.setAttribute('r', nodeR(companyNode) + 9); cp.setAttribute('fill', 'none');
      cp.setAttribute('stroke', SC[signal.type] || '#EF9F27'); cp.setAttribute('stroke-width', '1.8');
      cp.style.animation = 'pp-pk1 2.0s ease-in-out infinite';
      pulseG.appendChild(cp); pulseEls.push({ el: cp, node: companyNode });
    }

    // Nodes
    graph.nodes.forEach(function(n) {
      if (n.type === 'team') {
        var col = (teamColorMap[n.id] || TC[0]).fill;
        [36, 26].forEach(function(r) {
          var h = document.createElementNS(ns, 'circle');
          h.setAttribute('r', r); h.setAttribute('fill', col); h.setAttribute('fill-opacity', r === 36 ? '0.04' : '0.10');
          haloG.appendChild(h); n._halos = n._halos || []; n._halos.push(h);
        });
      }
      var circle = document.createElementNS(ns, 'circle');
      circle.setAttribute('r', nodeR(n)); circle.setAttribute('fill', nodeColor(n));
      circle.setAttribute('fill-opacity', n.type === 'team' ? '0.88' : n.type === 'company' ? '0.82' : (0.42 + (n.bestStrength || 0) * 0.45).toFixed(2));
      circle.setAttribute('stroke', nodeColor(n)); circle.setAttribute('stroke-opacity', '0.28');
      circle.setAttribute('stroke-width', n.type === 'team' ? '1.5' : '0.8');
      circle.style.cursor = 'pointer';
      circle.addEventListener('click', function(e) {
        e.stopPropagation();
        if (!popEl.classList.contains('pp-expanded')) {
          popEl.classList.add('pp-expanded');
          var expW = Math.min(window.innerWidth * 0.85, 900);
          var expH = Math.min(window.innerHeight * 0.8, 650);
          popEl.style.cssText = 'position:fixed;z-index:9999;left:' + ((window.innerWidth - expW) / 2) + 'px;top:' + ((window.innerHeight - expH) / 2) + 'px;width:' + expW + 'px;height:' + expH + 'px;background:#06091a;border:0.5px solid rgba(255,255,255,0.14);border-radius:14px;box-shadow:0 24px 64px rgba(0,0,0,.72);pointer-events:auto;opacity:1;transform:none;overflow:hidden;font-family:var(--sans,system-ui)';
          var svgW = expW;
          var svgH = expH - 120; // room for header + footer
          svgEl.setAttribute('width', svgW);
          svgEl.setAttribute('height', svgH);
          W = svgW; H = svgH;
          // Restart sim with new bounds
          if (sim) {
            sim.force('center', d3.forceCenter(svgW / 2, svgH / 2));
            sim.alpha(0.8).restart();
          }
          // Add close button
          if (!popEl.querySelector('.pp-close-btn')) {
            var closeBtn = document.createElement('button');
            closeBtn.className = 'pp-close-btn';
            closeBtn.textContent = '\u2715 Close';
            closeBtn.style.cssText = 'position:absolute;top:12px;right:14px;font-size:11px;padding:4px 12px;border-radius:6px;border:0.5px solid rgba(255,255,255,0.2);background:transparent;color:rgba(255,255,255,0.5);cursor:pointer;z-index:10;font-family:var(--sans,system-ui)';
            closeBtn.onclick = function(ev) { ev.stopPropagation(); hide(); };
            popEl.appendChild(closeBtn);
          }
        } else {
          window.location.href = '/network.html?signal=' + currentId;
        }
      });
      nodeG.appendChild(circle); n._el = circle;

      // Labels for team and company nodes
      if (n.type === 'team' || n.type === 'company') {
        var t = document.createElementNS(ns, 'text');
        t.textContent = n.type === 'company' ? (n.label.length > 14 ? n.label.substring(0, 13) + '\u2026' : n.label) : n.label;
        t.setAttribute('text-anchor', 'middle'); t.setAttribute('font-size', n.type === 'team' ? '9' : '8');
        t.setAttribute('font-weight', n.type === 'team' ? '600' : '500');
        t.setAttribute('fill', n.type === 'team' ? (teamColorMap[n.id] || TC[0]).text : (n.isClient ? '#EF9F27' : 'rgba(255,255,255,0.7)'));
        if (n.type === 'team') t.setAttribute('dominant-baseline', 'central');
        else t.setAttribute('dy', nodeR(n) + 11);
        t.style.pointerEvents = 'none'; labelG.appendChild(t); n._labelEl = t;
      }
    });

    // D3 force simulation
    var d3Sim = d3.forceSimulation(graph.nodes)
      .force('link', d3.forceLink(links).id(function(d) { return d.id; })
        .distance(function(l) { return l.type === 'works_at' ? 52 + (1 - (l.strength || 0.5)) * 40 : 38 + (1 - (l.strength || 0.5)) * 55; })
        .strength(function(l) { return l.type === 'works_at' ? 0.45 : (l.strength || 0.5) * 0.55; })
      )
      .force('charge', d3.forceManyBody().strength(function(d) {
        var scale = nodeCount > 10 ? 1.4 : nodeCount > 6 ? 1.2 : 1;
        return d.type === 'team' ? -280 * scale : d.type === 'company' ? -180 * scale : -45 * scale;
      }))
      .force('center', d3.forceCenter(W / 2, H / 2))
      .force('collision', d3.forceCollide(function(d) { return nodeR(d) + 6; }))
      .alphaDecay(0.025);

    sim = d3Sim;

    d3Sim.on('tick', function() {
      graph.nodes.forEach(function(n) {
        var r = nodeR(n); n.x = Math.max(r + 4, Math.min(W - r - 4, n.x)); n.y = Math.max(r + 4, Math.min(H - r - 4, n.y));
      });
      links.forEach(function(l, i) {
        var se2 = typeof l.source === 'object' ? l.source : graph.nodes.find(function(n) { return n.id === l.source; });
        var te = typeof l.target === 'object' ? l.target : graph.nodes.find(function(n) { return n.id === l.target; });
        if (!se2 || !te) return;
        linkEls[i].setAttribute('x1', se2.x); linkEls[i].setAttribute('y1', se2.y);
        linkEls[i].setAttribute('x2', te.x); linkEls[i].setAttribute('y2', te.y);
      });
      graph.nodes.forEach(function(n) {
        if (n._el) { n._el.setAttribute('cx', n.x); n._el.setAttribute('cy', n.y); }
        if (n._halos) n._halos.forEach(function(h) { h.setAttribute('cx', n.x); h.setAttribute('cy', n.y); });
        if (n._labelEl) { n._labelEl.setAttribute('x', n.x); if (n.type === 'team') n._labelEl.setAttribute('y', n.y); else n._labelEl.setAttribute('y', n.y); }
      });
      pulseEls.forEach(function(p) { p.el.setAttribute('cx', p.node.x); p.el.setAttribute('cy', p.node.y); });
    });
  }

  function populateHeader(data) {
    var signal = data.signal, account = data.account;
    var sigColor = SC[signal.type] || '#888';
    var tag = popEl.querySelector('#pp-tag');
    tag.style.background = sigColor + '20'; tag.style.color = sigColor; tag.style.border = '0.5px solid ' + sigColor + '55';
    tag.innerHTML = '<span>\u25cf</span>' + (SL[signal.type] || signal.type);
    popEl.querySelector('#pp-company').textContent = account ? signal.company + ' \u00b7 ' + (account.tier || 'client') : signal.company;
    var hl = signal.headline || '';
    popEl.querySelector('#pp-headline').textContent = hl.length > 72 ? hl.substring(0, 71) + '\u2026' : hl;
    var fill = popEl.querySelector('#pp-conf-fill');
    fill.style.width = Math.round((signal.confidence || 0) * 100) + '%'; fill.style.background = sigColor;
    popEl.querySelector('#pp-conf-val').textContent = Math.round((signal.confidence || 0) * 100) + '%';
  }

  function populateFooter(data, signalId) {
    var ft = popEl.querySelector('#pp-ft'); ft.innerHTML = '';
    var contacts = data.graph.nodes.filter(function(n) { return n.type === 'contact'; }).sort(function(a, b) { return (b.bestStrength || 0) - (a.bestStrength || 0); });
    if (contacts.length) {
      var best = contacts[0];
      var btn = document.createElement('button');
      btn.style.cssText = 'font-size:10px;padding:3px 10px;border-radius:16px;border:0.5px solid rgba(255,255,255,0.18);background:transparent;color:rgba(255,255,255,0.55);cursor:pointer;font-family:var(--sans,system-ui);transition:all .14s';
      btn.textContent = 'Approach via ' + best.label.split(' ')[0] + ' \u2197';
      btn.onmouseenter = function() { btn.style.color = '#fff'; btn.style.borderColor = 'rgba(255,255,255,0.45)'; };
      btn.onmouseleave = function() { btn.style.color = 'rgba(255,255,255,0.55)'; btn.style.borderColor = 'rgba(255,255,255,0.18)'; };
      btn.onclick = function() { window.location.href = '/network.html?signal=' + signalId + '&focus=' + best.personId; };
      ft.appendChild(btn);
    }
    var sp = document.createElement('div'); sp.style.flex = '1'; ft.appendChild(sp);
    var link = document.createElement('div');
    link.style.cssText = 'font-size:10px;color:rgba(255,255,255,0.28);cursor:pointer;transition:color .14s';
    link.textContent = 'Open full network \u2197';
    link.onmouseenter = function() { link.style.color = 'rgba(255,255,255,0.65)'; };
    link.onmouseleave = function() { link.style.color = 'rgba(255,255,255,0.28)'; };
    link.onclick = function() { window.location.href = '/network.html?signal=' + signalId; };
    ft.appendChild(link);
  }

  function scheduleHide() {
    clearTimeout(hideTimer);
    hideTimer = setTimeout(function() { if (!overPopup) hide(); }, HIDE_DELAY);
  }

  function hide() {
    if (!popEl) return;
    popEl.style.opacity = '0'; popEl.style.transform = 'scale(.93) translateY(6px)'; popEl.style.pointerEvents = 'none';
    if (sim) { sim.stop(); sim = null; }
    currentId = null;
  }

  async function show(triggerEl, signalId) {
    mount(); clearTimeout(hideTimer);
    if (currentId === signalId && popEl.style.opacity === '1') return;
    currentId = signalId;

    var data;
    var cached = triggerEl.dataset.proxGraph;
    if (cached) {
      data = JSON.parse(cached);
    } else {
      popEl.style.pointerEvents = 'auto'; popEl.style.opacity = '1'; popEl.style.transform = 'scale(1) translateY(0)';
      position(triggerEl);
      while (svgEl.children.length > 1) svgEl.removeChild(svgEl.lastChild);
      var lt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      lt.textContent = 'Loading proximity graph\u2026'; lt.setAttribute('x', W / 2); lt.setAttribute('y', H / 2);
      lt.setAttribute('text-anchor', 'middle'); lt.setAttribute('font-size', '11'); lt.setAttribute('fill', 'rgba(255,255,255,0.2)');
      svgEl.appendChild(lt);
      try {
        var token = localStorage.getItem('ml_token');
        var res = await fetch('/api/signals/' + signalId + '/proximity-graph', { headers: { Authorization: 'Bearer ' + token } });
        if (!res.ok) throw new Error('Failed');
        data = await res.json();
        triggerEl.dataset.proxGraph = JSON.stringify(data);
      } catch (e) { hide(); return; }
    }
    if (currentId !== signalId) return;
    // Cap nodes to prevent D3 from choking — show top contacts by strength
    if (data.graph && data.graph.nodes) {
      var companyNode = data.graph.nodes.find(function(n) { return n.type === 'company'; });
      var contactNodes = data.graph.nodes.filter(function(n) { return n.type === 'contact'; })
        .sort(function(a, b) { return (b.bestStrength || 0) - (a.bestStrength || 0); })
        .slice(0, 15);
      var contactIds = new Set(contactNodes.map(function(n) { return n.id; }));
      // Only include team nodes that connect to remaining contacts
      var teamNodes = data.graph.nodes.filter(function(n) {
        if (n.type !== 'team') return false;
        return data.graph.links.some(function(l) {
          var src = typeof l.source === 'object' ? l.source.id : l.source;
          var tgt = typeof l.target === 'object' ? l.target.id : l.target;
          return (src === n.id && contactIds.has(tgt)) || (tgt === n.id && contactIds.has(src));
        });
      });
      data.graph.nodes = [companyNode].concat(teamNodes).concat(contactNodes).filter(Boolean);
      var nodeIds = new Set(data.graph.nodes.map(function(n) { return n.id; }));
      data.graph.links = data.graph.links.filter(function(l) {
        var src = typeof l.source === 'object' ? l.source.id : l.source;
        var tgt = typeof l.target === 'object' ? l.target.id : l.target;
        return nodeIds.has(src) && nodeIds.has(tgt);
      });
    }
    populateHeader(data); renderGraph(data); populateFooter(data, signalId);
    // Reposition AFTER render (popup size may have changed)
    popEl.classList.remove('pp-expanded');
    position(triggerEl);
    popEl.style.pointerEvents = 'auto'; popEl.style.opacity = '1'; popEl.style.transform = 'scale(1) translateY(0)';
  }

  return { show: show, hide: hide, scheduleHide: scheduleHide };
})();

// Inject keyframe animations
var ppStyle = document.createElement('style');
ppStyle.textContent = '@keyframes pp-pk1{0%,100%{opacity:.72}50%{opacity:.10}}@keyframes pp-pk2{0%,100%{opacity:.38}50%{opacity:.05}}';
document.head.appendChild(ppStyle);
