// ═══════════════════════════════════════════════════════════════════════════════
// routes/companies.js — Companies API routes
// 14 routes: /api/companies/*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const https = require('https');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding, getGoogleToken }) {

router.post('/api/companies/:id/search-enrich', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { enrichCompanyFromSearch } = require('../lib/company-search-enrichment');
    const result = await enrichCompanyFromSearch(db, req.params.id, req.tenant_id);
    res.json(result);
  } catch (err) {
    console.error('Company search enrich error:', err.message);
    res.status(500).json({ error: 'Search enrichment failed: ' + err.message });
  }
});

router.post('/api/companies/:id/enrich', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [company] } = await db.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!company) return res.status(404).json({ error: 'Company not found' });

    const enrichResults = {};

    // 1. Ezekia company data
    if (process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('../lib/ezekia');
        // Search Ezekia companies by name — use exact match for short names to avoid PAM/EPAM confusion
        let ezekiaCompany = null;
        let page = 1;
        const companyNameLower = company.name.toLowerCase().trim();
        const isShortName = companyNameLower.length <= 5;
        while (page <= 5 && !ezekiaCompany) {
          const compRes = await ezekia.getCompanies({ page, per_page: 100 });
          const companies = compRes?.data || [];
          if (companies.length === 0) break;
          ezekiaCompany = companies.find(c => {
            const ezName = (c.name || '').toLowerCase().trim();
            if (isShortName) return ezName === companyNameLower; // Exact match for short names
            return ezName === companyNameLower || (ezName.length > 5 && companyNameLower.length > 5 && (ezName.includes(companyNameLower) || companyNameLower.includes(ezName)));
          });
          page++;
        }

        if (ezekiaCompany) {
          const updates = {};
          if (ezekiaCompany.industry && !company.sector) updates.sector = ezekiaCompany.industry;
          if (ezekiaCompany.website && !company.domain) updates.domain = ezekiaCompany.website;
          if (ezekiaCompany.address && !company.geography) {
            updates.geography = [ezekiaCompany.address.city, ezekiaCompany.address.country].filter(Boolean).join(', ');
          }
          if (ezekiaCompany.description && !company.description) updates.description = ezekiaCompany.description;

          if (Object.keys(updates).length > 0) {
            const setClauses = Object.entries(updates).map(([k, v], i) => `${k} = $${i + 2}`);
            const coUpdateVals = Object.values(updates);
            await db.query(`UPDATE companies SET ${setClauses.join(', ')}, updated_at = NOW() WHERE id = $1 AND tenant_id = $${coUpdateVals.length + 2}`,
              [req.params.id, ...coUpdateVals, req.tenant_id]);
            enrichResults.ezekia = { updated_fields: Object.keys(updates), ezekia_id: ezekiaCompany.id };
          } else {
            enrichResults.ezekia = { message: 'No new data from Ezekia', ezekia_id: ezekiaCompany.id };
          }
        } else {
          enrichResults.ezekia = { message: 'Not found in Ezekia CRM' };
        }
      } catch (e) {
        enrichResults.ezekia = { error: e.message };
      }

      // Also search Ezekia projects for this company
      try {
        const ezekia = require('../lib/ezekia');
        let projectsFound = [];
        for (let pg = 1; pg <= 3; pg++) {
          const projRes = await ezekia.getProjects({ page: pg, per_page: 100 });
          const projs = projRes?.data || [];
          if (!projs.length) break;
          const matches = projs.filter(p => {
            const pName = (p.companyName || p.company?.name || p.name || '').toLowerCase();
            return company.name.length <= 5
              ? pName === company.name.toLowerCase()
              : pName.includes(company.name.toLowerCase()) || company.name.toLowerCase().includes(pName);
          });
          projectsFound.push(...matches);
        }
        if (projectsFound.length) {
          enrichResults.ezekia_projects = {
            found: projectsFound.length,
            projects: projectsFound.slice(0, 10).map(p => ({ name: p.name, status: p.status, id: p.id }))
          };
        }
      } catch (e) { /* ignore project search errors */ }
    }

    // 1c. Gmail domain discovery — find contacts by email domain
    if (company.domain || company.name) {
      try {
        const emailDomain = company.domain || company.name.toLowerCase().replace(/[^a-z0-9]/g, '') + '.com';
        const googleToken = await getGoogleToken(req.user.user_id);
        if (googleToken) {
          const q = encodeURIComponent(`from:*@${emailDomain} OR to:*@${emailDomain}`);
          const gmailRes = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${q}&maxResults=50`, { headers: { Authorization: `Bearer ${googleToken}` } });
          if (gmailRes.ok) {
            const gmailData = await gmailRes.json();
            const discoveredContacts = new Map();

            for (const msg of (gmailData.messages || []).slice(0, 30)) {
              try {
                const mRes = await fetch(`https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=metadata&metadataHeaders=From&metadataHeaders=To`, { headers: { Authorization: `Bearer ${googleToken}` } });
                if (!mRes.ok) continue;
                const mData = await mRes.json();
                const hdrs = mData.payload?.headers || [];
                const fromTo = (hdrs.find(h => h.name === 'From')?.value || '') + ',' + (hdrs.find(h => h.name === 'To')?.value || '');
                const matches = fromTo.match(new RegExp(`([^<,]+)<([a-zA-Z0-9._%+-]+@${emailDomain.replace('.', '\\.')})>`, 'gi')) || [];
                matches.forEach(m => {
                  const parts = m.match(/(.+)<(.+)>/);
                  if (parts) discoveredContacts.set(parts[2].toLowerCase().trim(), parts[1].trim().replace(/[\"']/g, ''));
                });
              } catch (e) { /* skip message errors */ }
            }

            let linked = 0;
            for (const [email, name] of discoveredContacts) {
              const { rows: exists } = await db.query('SELECT id FROM people WHERE email = $1 AND tenant_id = $2', [email, req.tenant_id]);
              if (exists.length) {
                await db.query('UPDATE people SET current_company_id = $1, current_company_name = $2, updated_at = NOW() WHERE id = $3 AND (current_company_id IS NULL OR current_company_id != $1)', [req.params.id, company.name, exists[0].id]);
              } else {
                await db.query('INSERT INTO people (full_name, email, current_company_id, current_company_name, source, created_by, tenant_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NOW())',
                  [name, email, req.params.id, company.name, 'gmail_discovery', req.user?.user_id || null, req.tenant_id]);
              }
              linked++;
            }
            if (linked) enrichResults.gmail_contacts = { discovered: discoveredContacts.size, linked };
          }
        }
      } catch (e) { /* gmail discovery is best-effort */ }
    }

    // 2. Conversion history from account record
    try {
      const { rows: [clientRecord] } = await db.query(
        'SELECT cl.id, cl.relationship_status, cl.relationship_tier FROM accounts cl WHERE cl.company_id = $1 AND cl.tenant_id = $2 LIMIT 1',
        [req.params.id, req.tenant_id]
      );
      if (clientRecord) {
        const { rows: placements } = await db.query(
          `SELECT p.role_title, p.start_date, p.placement_fee, p.currency, pe.full_name
           FROM conversions p
           LEFT JOIN people pe ON pe.id = p.person_id
           WHERE p.client_id = $1 AND p.tenant_id = $2 ORDER BY p.start_date DESC LIMIT 10`,
          [clientRecord.id, req.tenant_id]
        );
        // Mark as client in companies table
        await db.query('UPDATE companies SET is_client = true, updated_at = NOW() WHERE id = $1 AND (is_client IS NULL OR is_client = false) AND tenant_id = $2', [req.params.id, req.tenant_id]);
        enrichResults.client = {
          status: clientRecord.relationship_status,
          tier: clientRecord.relationship_tier,
          recent_placements: placements.length,
          placements: placements.map(p => ({ role: p.role_title, candidate: p.full_name, date: p.start_date, fee: p.placement_fee }))
        };
      } else {
        enrichResults.client = { message: 'No client record linked' };
      }
    } catch (e) {
      enrichResults.client = { error: e.message };
    }

    // 3. Link unlinked people to this company (exact name match only)
    try {
      const { rowCount: linked } = await db.query(
        `UPDATE people SET current_company_id = $1, updated_at = NOW()
         WHERE LOWER(TRIM(current_company_name)) = LOWER(TRIM($2))
           AND (current_company_id IS NULL OR current_company_id != $1)
           AND tenant_id = $3`,
        [req.params.id, company.name, req.tenant_id]
      );
      // Also try account names
      const { rows: accountNames } = await db.query(
        'SELECT DISTINCT name FROM accounts WHERE company_id = $1 AND tenant_id = $2',
        [req.params.id, req.tenant_id]
      );
      let extraLinked = 0;
      for (const an of accountNames) {
        if (an.name.toLowerCase() !== company.name.toLowerCase()) {
          const { rowCount } = await db.query(
            `UPDATE people SET current_company_id = $1, updated_at = NOW()
             WHERE LOWER(TRIM(current_company_name)) = LOWER(TRIM($2))
               AND (current_company_id IS NULL OR current_company_id != $1)
               AND tenant_id = $3`,
            [req.params.id, an.name, req.tenant_id]
          );
          extraLinked += rowCount;
        }
      }
      if (linked + extraLinked > 0) enrichResults.people_linked = linked + extraLinked;
    } catch (e) { /* ignore linking errors */ }

    // 4. People at this company
    try {
      const { rows: people } = await db.query(
        `SELECT full_name, current_title, email FROM people WHERE current_company_id = $1 AND tenant_id = $2 ORDER BY current_title LIMIT 20`,
        [req.params.id, req.tenant_id]
      );
      enrichResults.people = { count: people.length, sample: people.slice(0, 5).map(p => `${p.full_name} — ${p.current_title}`) };
    } catch (e) {
      enrichResults.people = { error: e.message };
    }

    // 5. Google News search — fetch recent news + instant signal detection
    try {
      const searchName = company.name.replace(/\s+(Pty|Ltd|Limited|Inc|Corp|plc|AG|S\.A\.|Group|Holdings)\b/gi, '').trim();
      // Add sector/geography context for short or ambiguous company names
      // SCIENCE: Reduces false positive rate for names like PAM, EY, AWS
      const sectorCtx = company.sector ? ' ' + company.sector : '';
      const geoCtx = company.geography ? ' ' + company.geography.split('&')[0].trim() : '';
      const qualifiedName = searchName.length <= 4
        ? searchName + sectorCtx + geoCtx  // Short names need context
        : '"' + searchName + '"' + (sectorCtx ? ' ' + sectorCtx : '');
      const newsUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(qualifiedName)}&hl=en&gl=AU&ceid=AU:en`;
      const newsXml = await new Promise((resolve, reject) => {
        const client = newsUrl.startsWith('https') ? https : require('http');
        const nReq = client.get(newsUrl, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 (compatible; MLX-Intelligence/1.0)' } }, (res) => {
          const chunks = []; res.on('data', c => chunks.push(c)); res.on('end', () => resolve(Buffer.concat(chunks).toString()));
        });
        nReq.on('error', reject); nReq.on('timeout', () => { nReq.destroy(); reject(new Error('timeout')); });
      });

      // Parse RSS items
      const newsItems = [];
      const itemRegex = /<item>([\s\S]*?)<\/item>/g;
      let newsMatch;
      while ((newsMatch = itemRegex.exec(newsXml)) !== null && newsItems.length < 10) {
        const itemXml = newsMatch[1];
        const getTag = (tag) => { const m = itemXml.match(new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`)); return (m?.[1] || m?.[2] || '').trim(); };
        const title = getTag('title');
        const link = getTag('link');
        const pubDate = getTag('pubDate');
        const source = getTag('source');
        if (title && link) newsItems.push({ title, link, pubDate, source });
      }

      // Run instant signal detection via Claude on the headlines
      let signalsCreated = 0;
      if (newsItems.length > 0 && process.env.ANTHROPIC_API_KEY) {
        try {
          const headlineBlock = newsItems.map((n, i) => `${i + 1}. "${n.title}" — ${n.source || 'Unknown'} (${n.pubDate || 'recent'})`).join('\n');
          const signalPrompt = `Analyse these recent news headlines about "${company.name}" for executive search signals.

HEADLINES:
${headlineBlock}

For each headline that contains a signal, return a JSON array. Signal types: capital_raising, geographic_expansion, strategic_hiring, ma_activity, partnership, product_launch, leadership_change, layoffs, restructuring.

Return ONLY a JSON array (or empty array [] if no signals found):
[{"headline_index": 1, "signal_type": "...", "confidence": 0.5-1.0, "evidence_summary": "one sentence describing the signal"}]

Only include genuine business signals. Ignore opinion pieces, listicles, or generic mentions.`;

          const signalResponse = await callClaude(
            [{ role: 'user', content: signalPrompt }],
            [],
            'You are a market signal analyst. Return ONLY valid JSON arrays.'
          );
          const signalText = signalResponse.content?.find(c => c.type === 'text')?.text || '[]';
          const detectedSignals = JSON.parse(signalText.replace(/```json\n?|\n?```/g, '').trim());

          for (const sig of (Array.isArray(detectedSignals) ? detectedSignals : [])) {
            if (!sig.signal_type || !sig.evidence_summary) continue;
            const headlineItem = newsItems[sig.headline_index - 1];
            if (!headlineItem) continue;

            // Skip old news — only accept articles from the last 90 days
            if (headlineItem.pubDate) {
              const articleAge = Date.now() - new Date(headlineItem.pubDate).getTime();
              if (articleAge > 90 * 24 * 60 * 60 * 1000) continue;
            }

            // Check for duplicate signal
            const { rows: existing } = await db.query(
              `SELECT id FROM signal_events WHERE company_id = $1 AND signal_type = $2 AND evidence_summary ILIKE $3 AND tenant_id = $4 LIMIT 1`,
              [req.params.id, sig.signal_type, `%${sig.evidence_summary.slice(0, 50)}%`, req.tenant_id]
            );
            if (existing.length) continue;

            await db.query(`
              INSERT INTO signal_events (signal_type, company_id, company_name, confidence_score,
                evidence_summary, source_url, detected_at, signal_date, tenant_id)
              VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
            `, [
              sig.signal_type, req.params.id, company.name, Math.min(sig.confidence || 0.6, 0.85),
              sig.evidence_summary, headlineItem.link,
              headlineItem.pubDate ? new Date(headlineItem.pubDate).toISOString() : new Date().toISOString(),
              req.tenant_id
            ]);
            signalsCreated++;
          }
        } catch (e) {
          // Signal detection failure is non-fatal
          console.error('News signal detection error:', e.message);
        }
      }

      // Store articles as documents
      let newsIngested = 0;
      for (const item of newsItems) {
        const sourceUrlHash = require('crypto').createHash('md5').update(item.link).digest('hex');
        const { rows: exists } = await db.query('SELECT id FROM external_documents WHERE source_url_hash = $1 AND tenant_id = $2', [sourceUrlHash, req.tenant_id]);
        if (exists.length) continue;
        await db.query(`
          INSERT INTO external_documents (title, content, source_name, source_type, source_url, source_url_hash,
            tenant_id, uploaded_by_user_id, published_at, processing_status, created_at)
          VALUES ($1, $2, $3, 'news_enrich', $4, $5, $6, $7, $8, 'processed', NOW())
        `, [item.title, item.title, item.source || 'Google News', item.link, sourceUrlHash,
            req.tenant_id, req.user?.user_id || null,
            item.pubDate ? new Date(item.pubDate).toISOString() : new Date().toISOString()]);
        newsIngested++;
      }

      enrichResults.news = {
        articles_found: newsItems.length,
        new_ingested: newsIngested,
        signals_created: signalsCreated,
        headlines: newsItems.slice(0, 5).map(i => ({ title: i.title, source: i.source, date: i.pubDate })),
        message: signalsCreated > 0
          ? `${newsItems.length} articles found, ${signalsCreated} signals detected and created`
          : newsItems.length > 0
            ? `${newsItems.length} articles found, no new signals detected`
            : 'No recent news found'
      };
    } catch (e) {
      enrichResults.news = { error: e.message };
    }

    // 4. Re-embed with all enriched data
    try {
      const { rows: [latest] } = await db.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
      const parts = [latest.name, latest.sector, latest.geography, latest.description, latest.domain].filter(Boolean);

      const { rows: signals } = await db.query(`SELECT evidence_summary FROM signal_events WHERE company_id = $1 AND evidence_summary IS NOT NULL AND (tenant_id IS NULL OR tenant_id = $2) ORDER BY detected_at DESC LIMIT 5`, [req.params.id, req.tenant_id]);
      signals.forEach(s => parts.push(s.evidence_summary));

      const { rows: people } = await db.query(`SELECT full_name, current_title FROM people WHERE current_company_id = $1 AND current_title IS NOT NULL AND tenant_id = $2 LIMIT 10`, [req.params.id, req.tenant_id]);
      if (people.length) parts.push('Key people: ' + people.map(p => `${p.full_name} — ${p.current_title}`).join(', '));

      if (parts.join(' ').length > 10 && process.env.QDRANT_URL) {
        const embedding = await generateQueryEmbedding(parts.join('\n'));
        const url = new URL('/collections/companies/points', process.env.QDRANT_URL);
        await new Promise((resolve, reject) => {
          const body = JSON.stringify({ points: [{ id: req.params.id, vector: embedding, payload: { name: latest.name, sector: latest.sector, is_client: latest.is_client } }] });
          const qReq = https.request({ hostname: url.hostname, port: url.port || 443, path: url.pathname + '?wait=true', method: 'PUT', headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY }, timeout: 10000 },
            (res) => { const c = []; res.on('data', d => c.push(d)); res.on('end', () => resolve()); });
          qReq.on('error', reject);
          qReq.write(body);
          qReq.end();
        });
        await db.query('UPDATE companies SET embedded_at = NOW() WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
        enrichResults.embedding = { message: 'Re-embedded successfully' };
      }
    } catch (e) {
      enrichResults.embedding = { error: e.message };
    }

    // ── Ezekia write-back: push signal intelligence to CRM ──
    if (enrichResults.ezekia?.ezekia_id && process.env.EZEKIA_API_TOKEN) {
      try {
        const ezekia = require('../lib/ezekia');
        const { rows: recentSignals } = await db.query(`
          SELECT signal_type, evidence_summary, detected_at, confidence_score
          FROM signal_events WHERE company_id = $1 AND detected_at > NOW() - INTERVAL '30 days'
          ORDER BY detected_at DESC LIMIT 5
        `, [req.params.id]).catch(() => ({ rows: [] }));

        if (recentSignals.length > 0) {
          const noteBody = '[Autonodal Signal Intelligence]\n\n' +
            recentSignals.map(s =>
              `${s.signal_type.replace(/_/g, ' ')} (${Math.round(s.confidence_score * 100)}%): ${(s.evidence_summary || '').slice(0, 150)} — ${new Date(s.detected_at).toLocaleDateString()}`
            ).join('\n');

          // Write as a note on the company (via any linked person, since Ezekia notes attach to people)
          // For now, log intent — Ezekia doesn't have a company notes endpoint
          enrichResults.ezekia_writeback = { signals_available: recentSignals.length, note: 'Company notes not supported in Ezekia API — signal intel stored locally' };
        }
      } catch (e) {
        enrichResults.ezekia_writeback = { error: e.message };
      }
    }

    // Update team_proximity for people discovered via Gmail domain search
    if (enrichResults.gmail_contacts?.linked > 0) {
      try {
        var { rows: companyPeople } = await db.query(
          'SELECT id FROM people WHERE current_company_id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]
        );
        var tpUpdated = 0;
        for (var cp of companyPeople) {
          var { rows: ixUsers } = await db.query(
            'SELECT DISTINCT user_id FROM interactions WHERE person_id = $1 AND user_id IS NOT NULL AND tenant_id = $2', [cp.id, req.tenant_id]
          );
          for (var iu of ixUsers) {
            var { rows: [ct] } = await db.query(
              "SELECT COUNT(*) AS cnt, MAX(interaction_at) AS latest FROM interactions WHERE person_id = $1 AND user_id = $2 AND interaction_type IN ('email','email_sent','email_received')",
              [cp.id, iu.user_id]
            );
            var cnt = parseInt(ct.cnt);
            if (cnt === 0) continue;
            var strength = cnt >= 10 ? 0.85 : cnt >= 3 ? 0.60 : 0.30;
            await db.query(`
              INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, interaction_count, last_interaction_date, tenant_id)
              VALUES ($1, $2, 'email', $3, 'enrich_gmail', $4, $5, $6)
              ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
                interaction_count = EXCLUDED.interaction_count,
                relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
                last_interaction_date = GREATEST(team_proximity.last_interaction_date, EXCLUDED.last_interaction_date),
                updated_at = NOW()
            `, [cp.id, iu.user_id, strength, cnt, ct.latest, req.tenant_id]);
            tpUpdated++;
          }
        }
        if (tpUpdated > 0) enrichResults.proximity = { updated: tpUpdated };
      } catch (e) { enrichResults.proximity = { error: e.message }; }
    }

    res.json({ company_id: req.params.id, company_name: company.name, results: enrichResults });
  } catch (err) {
    console.error('Company enrich error:', err.message);
    res.status(500).json({ error: 'Enrichment failed' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// COMPANIES
// ═══════════════════════════════════════════════════════════════════════════════


router.get('/api/companies', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;
    const q = req.query.q;

    // ── ALL / filtered companies ──
    let where = 'WHERE c.tenant_id = $1';
    const params = [req.tenant_id];
    let paramIdx = 1;

    // Privacy filter
    paramIdx++;
    where += ` AND (c.visibility IS NULL OR c.visibility != 'private' OR c.owner_user_id = $${paramIdx})`;
    params.push(req.user.user_id);

    // Exclude tenant company (that's us, not a client/target)
    where += ` AND COALESCE(c.company_tier, '') != 'tenant_company'`;

    // Filter out junk companies: require at least one quality signal
    if (req.query.show_all !== 'true') {
      where += ` AND (
        c.domain IS NOT NULL
        OR EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = c.id)
        OR (c.sector IS NOT NULL AND LENGTH(c.name) <= 60 AND c.name !~ '[.!?]')
      )`;
    }

    if (q) {
      paramIdx++;
      where += ` AND (c.name ILIKE $${paramIdx} OR c.sector ILIKE $${paramIdx} OR c.geography ILIKE $${paramIdx} OR c.domain ILIKE $${paramIdx})`;
      params.push(`%${q}%`);
    }
    if (req.query.sector) {
      paramIdx++;
      where += ` AND c.sector ILIKE $${paramIdx}`;
      params.push(`%${req.query.sector}%`);
    }
    if (req.query.geography) {
      paramIdx++;
      where += ` AND c.geography ILIKE $${paramIdx}`;
      params.push(`%${req.query.geography}%`);
    }
    if (req.query.is_client === 'true') {
      where += ` AND c.is_client = true`;
    }
    if (req.query.with_signals === 'true') {
      where += ` AND EXISTS (SELECT 1 FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1))`;
    }
    if (req.query.with_people === 'true') {
      where += ` AND EXISTS (SELECT 1 FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1)`;
    }

    paramIdx++;
    params.push(limit);
    const limitIdx = paramIdx;
    paramIdx++;
    params.push(offset);
    const offsetIdx = paramIdx;

    const [companiesResult, countResult] = await Promise.all([
      db.query(`
        SELECT c.id, c.name, c.sector, c.geography, c.domain, c.is_client,
               c.employee_count_band, c.description,
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1)) AS signal_count,
               (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1)
                AND se.signal_type::text IN ('capital_raising','product_launch','geographic_expansion','partnership','strategic_hiring')
                AND se.detected_at > NOW() - INTERVAL '30 days') AS positive_signal_count,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) AS people_count
        FROM companies c
        ${where}
        ORDER BY
          -- Tier 1: Clients with positive signals (30d)
          CASE WHEN c.is_client = true AND (SELECT COUNT(*) FROM signal_events se
            WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '30 days'
            AND se.signal_type::text IN ('capital_raising','product_launch','geographic_expansion','partnership','strategic_hiring')
          ) > 0 THEN 0
          -- Tier 2: Clients with contacts
          WHEN c.is_client = true THEN 1
          -- Tier 3: Non-clients with signals AND contacts
          WHEN (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '30 days') > 0
            AND (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) > 0 THEN 2
          -- Tier 4: Companies with contacts only
          WHEN (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) > 0 THEN 3
          ELSE 4 END,
          -- Within each tier, sort by signal+contact density
          (SELECT COUNT(*) FROM signal_events se WHERE se.company_id = c.id AND (se.tenant_id IS NULL OR se.tenant_id = $1) AND se.detected_at > NOW() - INTERVAL '30 days') DESC,
          (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id AND p.tenant_id = $1) DESC,
          c.name
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      db.query(`SELECT COUNT(*) AS cnt FROM companies c ${where}`, params.slice(0, -2)),
    ]);

    res.json({
      companies: companiesResult.rows,
      total: parseInt(countResult.rows[0].cnt),
      limit,
      offset,
    });
  } catch (err) {
    console.error('Companies error:', err.message);
    res.status(500).json({ error: 'Failed to fetch companies' });
  }
});

router.get('/api/companies/:id', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    let company = null;
    let companyId = req.params.id;
    let clientRecord = null;

    // Try companies table first
    const { rows: [co] } = await db.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [companyId, req.tenant_id]);
    if (co) {
      company = co;
    } else {
      // Maybe it's a clients table ID — resolve it
      const { rows: [cl] } = await db.query(`
        SELECT cl.*, co.id AS resolved_company_id,
               co.sector, co.geography, co.domain, co.employee_count_band,
               co.description AS company_description, co.is_client AS co_is_client
        FROM accounts cl
        LEFT JOIN companies co ON cl.company_id = co.id
        WHERE cl.id = $1 AND cl.tenant_id = $2
      `, [companyId, req.tenant_id]);
      if (cl && cl.resolved_company_id) {
        // Client has a linked company — use that
        const { rows: [linked] } = await db.query('SELECT * FROM companies WHERE id = $1 AND tenant_id = $2', [cl.resolved_company_id, req.tenant_id]);
        company = linked;
        companyId = cl.resolved_company_id;
        clientRecord = cl;
      } else if (cl) {
        // Client with no linked company — build a synthetic company record
        clientRecord = cl;
        company = {
          id: cl.id,
          name: cl.name,
          sector: cl.sector || null,
          geography: cl.geography || null,
          domain: cl.domain || null,
          is_client: true,
          description: cl.company_description || null,
          employee_count_band: cl.employee_count_band || null,
        };
      }
    }

    if (!company) return res.status(404).json({ error: 'Company not found' });

    // Get client financials if available
    let financials = null;
    try {
      const { rows: [cf] } = await db.query(`
        SELECT cf.* FROM account_financials cf
        JOIN accounts cl ON cf.client_id = cl.id
        WHERE (cl.company_id = $1 OR cl.id = $1) AND cf.tenant_id = $2
      `, [companyId, req.tenant_id]);
      financials = cf || null;
    } catch (e) {}

    // Signals
    const { rows: signals } = await db.query(`
      SELECT se.id, se.signal_type, se.confidence_score, se.evidence_summary,
             se.evidence_snippet, se.detected_at, se.triage_status, se.signal_category,
             se.hiring_implications, se.source_url,
             ed.title AS doc_title, ed.source_name AS doc_source
      FROM signal_events se
      LEFT JOIN external_documents ed ON se.source_document_id = ed.id
      WHERE se.company_id = $1 AND (se.tenant_id IS NULL OR se.tenant_id = $2)
      ORDER BY se.detected_at DESC LIMIT 30
    `, [companyId, req.tenant_id]);

    // People at this company — ordered by engagement level
    const { rows: people } = await db.query(`
      SELECT p.id, p.full_name, p.current_title, p.seniority_level, p.location,
             p.expertise_tags, p.linkedin_url, p.email, p.source,
             COALESCE(ix.cnt, 0) AS interaction_count,
             COALESCE(ix.cnt_90d, 0) AS interactions_90d,
             COALESCE(ix.note_cnt, 0) AS note_count,
             ix.last_at AS last_interaction,
             tp_agg.max_strength AS proximity_strength,
             tp_agg.connected_via,
             tp_agg.connection_types
      FROM people p
      LEFT JOIN LATERAL (
        SELECT COUNT(*) AS cnt,
               COUNT(*) FILTER (WHERE interaction_at > NOW() - INTERVAL '90 days') AS cnt_90d,
               COUNT(*) FILTER (WHERE interaction_type = 'research_note') AS note_cnt,
               MAX(interaction_at) AS last_at
        FROM interactions WHERE person_id = p.id
      ) ix ON true
      LEFT JOIN LATERAL (
        SELECT MAX(relationship_strength) AS max_strength,
               STRING_AGG(DISTINCT u.name, ', ') AS connected_via,
               STRING_AGG(DISTINCT tp.relationship_type, ', ') AS connection_types
        FROM team_proximity tp
        LEFT JOIN users u ON u.id = tp.team_member_id
        WHERE tp.person_id = p.id
      ) tp_agg ON true
      WHERE (p.current_company_id = $1 OR LOWER(TRIM(p.current_company_name)) = LOWER(TRIM($3)))
        AND p.tenant_id = $2
      ORDER BY ix.last_at DESC NULLS LAST,
        COALESCE(ix.cnt, 0) DESC,
        tp_agg.max_strength DESC NULLS LAST,
        CASE WHEN p.seniority_level IN ('c_suite','vp','director') THEN 0 ELSE 1 END,
        p.full_name
      LIMIT 200
    `, [companyId, req.tenant_id, company.name]);

    // Placements at this company — linked via account OR name match in role_title/client_name_raw
    let placements = [];
    try {
      const companyName = company.name || '';
      const { rows } = await db.query(`
        SELECT pl.id, COALESCE(pe.full_name, pl.consultant_name) AS candidate_name,
               pl.role_title, pl.start_date, pl.placement_fee, pl.fee_category,
               pl.invoice_number, pl.payment_status
        FROM conversions pl
        LEFT JOIN people pe ON pl.person_id = pe.id
        LEFT JOIN accounts cl ON pl.client_id = cl.id
        WHERE pl.tenant_id = $2 AND (
          cl.company_id = $1 OR cl.id = $1
          OR (pl.client_id IS NULL AND LENGTH($3) > 3 AND pl.role_title ILIKE '%' || $3 || '%')
          OR (LENGTH($3) > 3 AND pl.client_name_raw ILIKE $3)
        )
        ORDER BY pl.start_date DESC NULLS LAST
      `, [companyId, req.tenant_id, companyName]);
      placements = rows;
    } catch (e) { console.error('Placements query error:', e.message); }

    // Documents mentioning this company — by document_companies link OR title/content match
    let documents = [];
    try {
      // For short names (<=3 chars like "EY"), only use document_companies link — no title search
      // For longer names, use ILIKE title search as well
      let rows;
      if (company.name.length <= 3) {
        ({ rows } = await db.query(`
          SELECT DISTINCT ed.id, ed.title, ed.source_name, ed.source_type, ed.source_url, ed.published_at
          FROM external_documents ed
          JOIN document_companies dc ON dc.document_id = ed.id
          WHERE dc.company_id = $1 AND ed.tenant_id = $2
          ORDER BY ed.published_at DESC NULLS LAST LIMIT 20
        `, [companyId, req.tenant_id]));
      } else {
        const namePattern = company.name.length <= 5
          ? '% ' + company.name + ' %'
          : '%' + company.name + '%';
        ({ rows } = await db.query(`
          SELECT DISTINCT ed.id, ed.title, ed.source_name, ed.source_type, ed.source_url, ed.published_at
          FROM external_documents ed
          LEFT JOIN document_companies dc ON dc.document_id = ed.id
          WHERE ed.tenant_id = $2 AND (dc.company_id = $1 OR ed.title ILIKE $3)
          ORDER BY ed.published_at DESC NULLS LAST LIMIT 20
        `, [companyId, req.tenant_id, namePattern]));
      }
      documents = rows;
    } catch (e) { /* table may not exist */ }

    // Pipeline — opportunities + candidate counts for this company
    let opportunities = [];
    try {
      const { rows } = await db.query(`
        SELECT o.id, o.title, o.status, o.seniority_level,
               (SELECT COUNT(*) FROM pipeline_contacts pc WHERE pc.search_id = o.id) as candidate_count
        FROM opportunities o
        JOIN engagements e ON e.id = o.project_id
        JOIN accounts a ON a.id = e.client_id
        WHERE a.company_id = $1 AND o.tenant_id = $2
        ORDER BY o.created_at DESC
      `, [companyId, req.tenant_id]);
      opportunities = rows;
    } catch (e) {}

    // Total pipeline candidates across all opportunities
    let pipelineTotal = 0;
    try {
      const { rows: [{ cnt }] } = await db.query(`
        SELECT COUNT(DISTINCT pc.person_id) as cnt
        FROM pipeline_contacts pc
        JOIN opportunities o ON o.id = pc.search_id
        JOIN engagements e ON e.id = o.project_id
        JOIN accounts a ON a.id = e.client_id
        WHERE a.company_id = $1 AND pc.tenant_id = $2
      `, [companyId, req.tenant_id]);
      pipelineTotal = parseInt(cnt);
    } catch (e) {}

    // Case studies where this company was the client
    let case_studies = [];
    try {
      const { rows } = await db.query(`
        SELECT id, title, role_title, engagement_type, seniority_level, year,
               challenge, approach, outcome, themes, capabilities, status, visibility
        FROM case_studies
        WHERE (client_id = $1 OR client_name ILIKE $2) AND tenant_id = $3
        ORDER BY year DESC NULLS LAST
      `, [companyId, `%${company.name}%`, req.tenant_id]);
      case_studies = rows;
    } catch (e) { /* table may not exist */ }

    // Interaction summary — relationship activity across people at this company
    let interaction_summary = null;
    try {
      const { rows: [is] } = await db.query(`
        SELECT
          COUNT(i.id) as total_interactions,
          COUNT(DISTINCT i.person_id) as contacts_engaged,
          COUNT(DISTINCT i.user_id) as team_members_involved,
          MAX(i.interaction_at) as last_interaction,
          COUNT(i.id) FILTER (WHERE i.interaction_at > NOW() - INTERVAL '90 days') as interactions_90d,
          COUNT(i.id) FILTER (WHERE i.interaction_at > NOW() - INTERVAL '30 days') as interactions_30d,
          COUNT(i.id) FILTER (WHERE i.interaction_type IN ('email_sent', 'email_received')) as email_count,
          COUNT(i.id) FILTER (WHERE i.interaction_type = 'linkedin_message') as linkedin_count,
          COUNT(i.id) FILTER (WHERE i.direction = 'outbound') as outbound_count,
          COUNT(i.id) FILTER (WHERE i.direction = 'inbound') as inbound_count
        FROM interactions i
        JOIN people p ON p.id = i.person_id
        WHERE (i.company_id = $1 OR p.current_company_id = $1)
          AND i.interaction_at > NOW() - INTERVAL '2 years'
      `, [companyId]);
      if (is && parseInt(is.total_interactions) > 0) interaction_summary = is;
    } catch (e) {}

    // Proximity map — which team members have connections to people at this company (deduplicated)
    let proximity_map = [];
    try {
      const { rows } = await db.query(`
        SELECT u.name AS team_member, u.id AS team_member_id,
               p.full_name AS contact_name, p.id AS person_id,
               p.current_title,
               MAX(tp.relationship_strength) AS relationship_strength,
               string_agg(DISTINCT tp.relationship_type, ', ') AS proximity_type,
               string_agg(DISTINCT tp.source, ', ') AS proximity_source,
               MAX(i.interaction_at) AS last_contact,
               COUNT(DISTINCT i.id) AS interaction_count
        FROM team_proximity tp
        JOIN users u ON u.id = tp.team_member_id
        JOIN people p ON p.id = tp.person_id
        LEFT JOIN interactions i ON i.person_id = p.id AND i.user_id = u.id
        WHERE p.current_company_id = $1
          AND tp.relationship_strength >= 0.15
        GROUP BY u.name, u.id, p.full_name, p.id, p.current_title
        ORDER BY MAX(tp.relationship_strength) DESC
        LIMIT 30
      `, [companyId]);
      proximity_map = rows;
    } catch (e) {}

    res.json({ ...company, signals, people, placements, documents, financials, opportunities, pipeline_total: pipelineTotal, case_studies, interaction_summary, proximity_map });
  } catch (err) {
    console.error('Company detail error:', err.message);
    res.status(500).json({ error: 'Failed to fetch company' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// SEMANTIC SEARCH
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Embedding cache — same query text = same vector, skip OpenAI call ───
const embeddingCache = new Map();
const EMBEDDING_CACHE_TTL = 300000; // 5 min
setInterval(() => {
  var now = Date.now();
  for (var [k, v] of embeddingCache) { if (now - v.at > EMBEDDING_CACHE_TTL) embeddingCache.delete(k); }
}, 60000);

async function generateQueryEmbedding(text) {
  var cacheKey = text.trim().toLowerCase().slice(0, 200);
  var cached = embeddingCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) < EMBEDDING_CACHE_TTL) return cached.vector;

  var vector = await _generateQueryEmbeddingRaw(text);
  embeddingCache.set(cacheKey, { vector, at: Date.now() });
  return vector;
}

function _generateQueryEmbeddingRaw(text) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'text-embedding-3-small',
      input: text,
    });

    const req = https.request({
      hostname: 'api.openai.com',
      path: '/v1/embeddings',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Length': Buffer.byteLength(body),
      },
      timeout: 15000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          if (data.error) return reject(new Error(data.error.message));
          resolve(data.data[0].embedding);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.write(body);
    req.end();
  });
}

async function qdrantSearch(collection, vector, limit = 20, filter = null) {
  return new Promise((resolve, reject) => {
    const body = { vector, limit, with_payload: true };
    if (filter) body.filter = filter;

    const url = new URL(`/collections/${collection}/points/search`, process.env.QDRANT_URL);
    const req = https.request({
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'api-key': process.env.QDRANT_API_KEY,
      },
      timeout: 10000,
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const data = JSON.parse(Buffer.concat(chunks).toString());
          resolve(data.result || []);
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Qdrant timeout')); });
    req.write(JSON.stringify(body));
    req.end();
  });
}

// Company visibility toggle
router.patch('/api/companies/:id/visibility', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { visibility } = req.body;
    if (!visibility || !['company', 'private', 'internal'].includes(visibility)) return res.status(400).json({ error: 'visibility must be "company" or "private"' });

    const { rows: [co] } = await db.query('SELECT id, visibility, owner_user_id FROM companies WHERE id = $1 AND tenant_id = $2', [req.params.id, req.tenant_id]);
    if (!co) return res.status(404).json({ error: 'Company not found' });
    if (co.visibility === 'private' && co.owner_user_id && co.owner_user_id !== req.user.user_id && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Only the owner can change private companies' });
    }

    await db.query(
      `UPDATE companies SET visibility = $1, owner_user_id = CASE WHEN $1 = 'private' THEN $2 ELSE owner_user_id END WHERE id = $3 AND tenant_id = $4`,
      [visibility, req.user.user_id, req.params.id, req.tenant_id]
    );
    res.json({ id: req.params.id, visibility });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Signal visibility toggle

router.get('/api/companies/:id/activities', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT a.*, u.name AS actor_name FROM activities a
      LEFT JOIN users u ON u.id = a.user_id
      WHERE a.tenant_id = $1 AND (
        a.company_id = $2
        OR a.engagement_id IN (
          SELECT e.id FROM engagements e JOIN accounts ac ON ac.id = e.client_id WHERE ac.company_id = $2
        )
      )
      ORDER BY a.activity_at DESC LIMIT 50
    `, [req.tenant_id, req.params.id]);
    res.json({ activities: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// CRM CONNECTIONS (adapter management)
// ═══════════════════════════════════════════════════════════════════════════════


router.get('/api/companies/:id/relationship', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [rel] } = await db.query(`
      SELECT cr.*, c.name AS company_name, c.domain
      FROM company_relationships cr JOIN companies c ON c.id = cr.company_id
      WHERE cr.company_id = $1 AND cr.tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    if (!rel) return res.status(404).json({ error: 'No relationship data' });
    res.json(rel);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/companies/relationships/summary', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { tier, min_score, limit = 50, offset = 0 } = req.query;
    let where = 'cr.tenant_id = $1'; const params = [req.tenant_id]; let idx = 2;
    if (tier) { where += ` AND cr.relationship_tier = $${idx++}`; params.push(tier); }
    if (min_score) { where += ` AND cr.relationship_score >= $${idx++}`; params.push(parseFloat(min_score)); }
    params.push(Math.min(parseInt(limit) || 50, 200)); params.push(parseInt(offset) || 0);
    const { rows } = await db.query(`
      SELECT cr.company_id, c.name AS company_name, c.domain, c.sector, c.geography,
             cr.relationship_tier, cr.relationship_score, cr.active_contact_count,
             cr.total_contact_count, cr.team_member_count,
             cr.last_interaction_at, cr.last_interaction_type, cr.is_stale, cr.computed_at
      FROM company_relationships cr JOIN companies c ON c.id = cr.company_id
      WHERE ${where} ORDER BY cr.relationship_score DESC LIMIT $${idx} OFFSET $${idx + 1}
    `, params);
    const { rows: [cnt] } = await db.query('SELECT COUNT(*) AS cnt FROM company_relationships cr WHERE ' + where, params.slice(0, idx - 1));
    res.json({ companies: rows, total: parseInt(cnt.cnt) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/companies/:id/relationship/history', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT cre.event_type, cre.previous_tier, cre.new_tier, cre.previous_score, cre.new_score,
             cre.stale_reason, cre.detected_at, cre.metadata
      FROM company_relationship_events cre
      WHERE cre.company_id = $1 AND cre.tenant_id = $2
      ORDER BY cre.detected_at DESC LIMIT 20
    `, [req.params.id, req.tenant_id]);
    const { rows: [co] } = await db.query('SELECT name FROM companies WHERE id = $1', [req.params.id]);
    res.json({ company_id: req.params.id, company_name: co?.name, events: rows });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/companies/relationships/stale', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT cr.company_id, c.name AS company_name, c.sector, c.geography,
             cr.stale_reason, cr.stale_since, cr.last_interaction_at,
             cr.total_contact_count, cr.relationship_tier, cr.relationship_score
      FROM company_relationships cr JOIN companies c ON c.id = cr.company_id
      WHERE cr.tenant_id = $1 AND cr.is_stale = true
      ORDER BY cr.last_interaction_at DESC NULLS LAST
    `, [req.tenant_id]);
    res.json({ stale_companies: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/companies/relationships/elevated', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { tier, min_elevated_score = 0.3, exclude_stale = 'true', include_gaps, limit = 25, offset = 0 } = req.query;
    let where = 'cr.tenant_id = $1'; const params = [req.tenant_id]; let idx = 2;
    if (tier) { where += ` AND cr.elevation_tier = $${idx++}`; params.push(tier); }
    where += ` AND cr.elevated_score >= $${idx++}`; params.push(parseFloat(min_elevated_score));
    if (exclude_stale === 'true') where += ' AND cr.is_stale = false';
    if (include_gaps !== 'true') where += ` AND cr.elevation_tier != 'gap'`;
    params.push(Math.min(parseInt(limit) || 25, 100)); params.push(parseInt(offset) || 0);
    const { rows } = await db.query(`
      SELECT cr.company_id, c.name AS company_name, c.domain, cr.relationship_tier, cr.relationship_score,
             cr.signal_score, cr.elevated_score, cr.elevation_tier, cr.signal_count_30d,
             cr.signal_types_active, cr.highest_signal_type, cr.highest_signal_at,
             cr.active_contact_count, cr.last_interaction_at, cr.is_stale
      FROM company_relationships cr JOIN companies c ON c.id = cr.company_id
      WHERE ${where} ORDER BY cr.elevated_score DESC LIMIT $${idx} OFFSET $${idx + 1}
    `, params);
    const { rows: summary } = await db.query(`
      SELECT elevation_tier, COUNT(*) AS cnt FROM company_relationships
      WHERE tenant_id = $1 AND elevation_tier IS NOT NULL GROUP BY elevation_tier
    `, [req.tenant_id]);
    const sumObj = {}; summary.forEach(function(s) { sumObj[s.elevation_tier] = parseInt(s.cnt); });
    res.json({ companies: rows, total: rows.length, summary: sumObj });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/api/companies/relationships/gaps', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT cr.company_id, c.name AS company_name, c.domain, c.sector,
             cr.signal_score, cr.signal_types_active, cr.relationship_tier, cr.relationship_score,
             cr.active_contact_count, cr.signal_count_30d,
             CASE WHEN cr.signal_score >= 0.6 THEN 'high' WHEN cr.signal_score >= 0.3 THEN 'medium' ELSE 'low' END AS gap_severity
      FROM company_relationships cr JOIN companies c ON c.id = cr.company_id
      WHERE cr.tenant_id = $1 AND cr.elevation_tier = 'gap'
      ORDER BY cr.signal_score DESC LIMIT 50
    `, [req.tenant_id]);
    res.json({ gaps: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/companies/:id/jobs — Active job postings for a company
router.get('/api/companies/:id/jobs', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { seniority, function_area, status, limit } = req.query;
    let where = ['jp.tenant_id = $1', 'jp.company_id = $2'];
    const params = [req.tenant_id, req.params.id];
    let idx = 3;
    if (seniority) { where.push(`jp.seniority_level = $${idx++}`); params.push(seniority); }
    if (function_area) { where.push(`jp.function_area = $${idx++}`); params.push(function_area); }
    where.push(`jp.status = $${idx++}`);
    params.push(status || 'active');
    const lim = Math.min(parseInt(limit) || 50, 200);
    const { rows } = await db.query(`
      SELECT jp.id, jp.title, jp.department, jp.location, jp.employment_type,
             jp.seniority_level, jp.function_area, jp.is_leadership, jp.apply_url,
             jp.status, jp.first_seen_at, jp.last_seen_at, jp.removed_at, jp.days_open,
             jp.ats_type, jp.external_id
      FROM job_postings jp
      WHERE ${where.join(' AND ')}
      ORDER BY jp.first_seen_at DESC
      LIMIT ${lim}
    `, params);
    res.json({ jobs: rows, total: rows.length });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /api/companies/:id/ats — ATS detection status for a company
router.get('/api/companies/:id/ats', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const company = await db.queryOne(`
      SELECT id, name, ats_type, ats_feed_url, ats_detected_at, ats_error, careers_url
      FROM companies WHERE id = $1 AND tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    if (!company) return res.status(404).json({ error: 'Company not found' });
    const postingStats = await db.queryOne(`
      SELECT COUNT(*) FILTER (WHERE status = 'active') AS active_postings,
             COUNT(*) FILTER (WHERE status = 'removed') AS removed_postings,
             MAX(last_seen_at) AS last_harvested
      FROM job_postings WHERE company_id = $1 AND tenant_id = $2
    `, [req.params.id, req.tenant_id]);
    res.json({
      ...company,
      posting_count: parseInt(postingStats?.active_postings || 0),
      removed_count: parseInt(postingStats?.removed_postings || 0),
      last_harvested: postingStats?.last_harvested,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

  return router;
};
