// ═══════════════════════════════════════════════════════════════════════════════
// routes/artifacts.js — Work artifact CRUD, entity linking, semantic search
// ═══════════════════════════════════════════════════════════════════════════════
//
// DATA SOVEREIGNTY: Artifacts are tenant IP. They embed ONLY into the
// work_artifacts Qdrant collection with mandatory tenant_id filtering.
// They NEVER feed into people/companies/searches collections or any
// cross-tenant pipeline. This is permanent, not deferred.
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const https = require('https');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding, auditLog }) {

  const { extractEntities } = require('../lib/entity_extraction');

  // ── Helper: embed artifact into work_artifacts Qdrant collection ──────────

  async function embedArtifact(artifact, entityLinks) {
    if (!process.env.OPENAI_API_KEY || !process.env.QDRANT_URL) return;

    // Compose embedding text — double-weight title
    const embeddingText = [
      artifact.title,
      artifact.title,
      artifact.summary || '',
      (artifact.content_markdown || '').substring(0, 6000),
    ].filter(Boolean).join('\n\n');

    if (embeddingText.length < 10) return;

    try {
      const vector = await generateQueryEmbedding(embeddingText);
      const pointId = artifact.id;

      const payload = {
        tenant_id: artifact.tenant_id,
        artifact_id: artifact.id,
        artifact_type: artifact.artifact_type,
        title: artifact.title,
        status: artifact.status,
        created_by_name: artifact.created_by_name || null,
        created_at: artifact.created_at instanceof Date ? artifact.created_at.toISOString() : artifact.created_at,
        person_ids: (entityLinks || []).filter(l => l.entity_type === 'person' && l.person_id).map(l => l.person_id),
        company_ids: (entityLinks || []).filter(l => l.entity_type === 'company' && l.company_id).map(l => l.company_id),
        search_ids: (entityLinks || []).filter(l => l.entity_type === 'search' && l.search_id).map(l => l.search_id),
        link_types: [...new Set((entityLinks || []).map(l => l.link_type))],
      };

      // Upsert to Qdrant — work_artifacts collection ONLY (never people/companies)
      const url = new URL('/collections/work_artifacts/points', process.env.QDRANT_URL);
      const body = JSON.stringify({ points: [{ id: pointId, vector, payload }] });

      await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: url.hostname,
          port: url.port || 443,
          path: url.pathname + '?wait=true',
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY },
          timeout: 15000,
        }, (res) => {
          const chunks = [];
          res.on('data', d => chunks.push(d));
          res.on('end', () => resolve());
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(body);
        req.end();
      });

      // Update PostgreSQL
      await platformPool.query(
        `UPDATE work_artifacts SET qdrant_point_id = $1, embedded_at = NOW() WHERE id = $2`,
        [pointId, artifact.id]
      );
    } catch (e) {
      console.warn('[artifacts] Embed failed:', e.message);
    }
  }

  // ── Helper: insert entity links ───────────────────────────────────────────

  async function insertEntityLinks(db, artifactId, tenantId, links) {
    const inserted = [];
    for (const link of links) {
      try {
        const { rows: [row] } = await db.query(
          `INSERT INTO artifact_entity_links
           (tenant_id, artifact_id, entity_type, person_id, company_id, search_id, link_type, confidence, auto_detected)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT DO NOTHING
           RETURNING id`,
          [
            tenantId, artifactId,
            link.entity_type,
            link.person_id || null,
            link.company_id || null,
            link.search_id || null,
            link.link_type || 'related',
            link.confidence || 1.0,
            link.auto_detected || false,
          ]
        );
        if (row) inserted.push({ ...link, id: row.id });
      } catch (e) { /* duplicate or FK violation — skip */ }
    }
    return inserted;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // POST /api/artifacts — Create artifact
  // ═══════════════════════════════════════════════════════════════════════════

  router.post('/api/artifacts', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const {
        artifact_type, custom_type_label, title, content_markdown,
        summary, key_findings, structured_data, status,
        entity_links, person_ids, person_link_types,
        company_ids, company_link_types, search_ids,
        auto_extract_entities, source_context,
      } = req.body;

      if (!artifact_type || !title || !content_markdown) {
        return res.status(400).json({ error: 'artifact_type, title, and content_markdown are required' });
      }

      // Insert artifact
      const { rows: [artifact] } = await db.query(
        `INSERT INTO work_artifacts
         (tenant_id, artifact_type, custom_type_label, title, status,
          content_markdown, summary, key_findings, structured_data,
          created_by, created_by_name, source_context)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING *`,
        [
          req.tenant_id,
          artifact_type,
          custom_type_label || null,
          title,
          status || 'final',
          content_markdown,
          summary || null,
          JSON.stringify(key_findings || []),
          JSON.stringify(structured_data || {}),
          req.user.user_id,
          req.user.name || req.user.email,
          source_context || 'web_ui',
        ]
      );

      // Build entity links from explicit IDs
      const links = [];

      if (entity_links && Array.isArray(entity_links)) {
        entity_links.forEach(l => links.push({ ...l, confidence: 1.0, auto_detected: false }));
      }

      // Shorthand: person_ids + person_link_types arrays
      if (person_ids && Array.isArray(person_ids)) {
        person_ids.forEach((pid, i) => {
          links.push({
            entity_type: 'person',
            person_id: pid,
            link_type: (person_link_types && person_link_types[i]) || 'subject',
            confidence: 1.0,
            auto_detected: false,
          });
        });
      }

      if (company_ids && Array.isArray(company_ids)) {
        company_ids.forEach((cid, i) => {
          links.push({
            entity_type: 'company',
            company_id: cid,
            link_type: (company_link_types && company_link_types[i]) || 'related',
            confidence: 1.0,
            auto_detected: false,
          });
        });
      }

      if (search_ids && Array.isArray(search_ids)) {
        search_ids.forEach(sid => {
          links.push({
            entity_type: 'search',
            search_id: sid,
            link_type: 'related',
            confidence: 1.0,
            auto_detected: false,
          });
        });
      }

      // Auto-extract entities from content
      if (auto_extract_entities !== false) {
        try {
          const extracted = await extractEntities(content_markdown, req.tenant_id, platformPool);
          const existingPersonIds = new Set(links.filter(l => l.person_id).map(l => l.person_id));
          const existingCompanyIds = new Set(links.filter(l => l.company_id).map(l => l.company_id));

          extracted.people.forEach(p => {
            if (!existingPersonIds.has(p.person_id)) {
              links.push({
                entity_type: 'person', person_id: p.person_id,
                link_type: 'mentioned', confidence: p.confidence, auto_detected: true,
              });
            }
          });
          extracted.companies.forEach(c => {
            if (!existingCompanyIds.has(c.company_id)) {
              links.push({
                entity_type: 'company', company_id: c.company_id,
                link_type: 'mentioned', confidence: c.confidence, auto_detected: true,
              });
            }
          });
        } catch (e) { /* extraction failure should not block save */ }
      }

      // Insert links
      const insertedLinks = await insertEntityLinks(db, artifact.id, req.tenant_id, links);

      // Embed asynchronously — don't block response
      setImmediate(() => embedArtifact(artifact, insertedLinks));

      // Audit
      auditLog(req.user.user_id, 'artifact_created', 'work_artifact', artifact.id,
        { type: artifact_type, title, links: insertedLinks.length }, req.ip);

      res.status(201).json({
        artifact,
        entity_links: insertedLinks,
        message: `Artifact saved. ${insertedLinks.length} entities linked.`,
      });
    } catch (err) {
      console.error('[artifacts] Create error:', err.message);
      res.status(500).json({ error: 'Failed to create artifact: ' + err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GET /api/artifacts/:id — Get artifact with entity links
  // ═══════════════════════════════════════════════════════════════════════════

  router.get('/api/artifacts/:id', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows: [artifact] } = await db.query(
        `SELECT * FROM work_artifacts WHERE id = $1 AND tenant_id = $2`,
        [req.params.id, req.tenant_id]
      );
      if (!artifact) return res.status(404).json({ error: 'Artifact not found' });

      const { rows: links } = await db.query(
        `SELECT ael.*, p.full_name AS person_name, c.name AS company_name
         FROM artifact_entity_links ael
         LEFT JOIN people p ON p.id = ael.person_id
         LEFT JOIN companies c ON c.id = ael.company_id
         WHERE ael.artifact_id = $1 AND ael.tenant_id = $2`,
        [req.params.id, req.tenant_id]
      );

      res.json({ ...artifact, entity_links: links });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // PATCH /api/artifacts/:id — Update artifact
  // ═══════════════════════════════════════════════════════════════════════════

  router.patch('/api/artifacts/:id', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const allowed = ['title', 'content_markdown', 'summary', 'key_findings', 'structured_data', 'status', 'custom_type_label'];
      const updates = [];
      const params = [req.params.id, req.tenant_id];
      let idx = 2;

      for (const key of allowed) {
        if (req.body[key] !== undefined) {
          idx++;
          const val = (key === 'key_findings' || key === 'structured_data')
            ? JSON.stringify(req.body[key]) : req.body[key];
          updates.push(`${key} = $${idx}`);
          params.push(val);
        }
      }

      if (updates.length === 0) return res.json({ ok: true, message: 'Nothing to update' });

      const { rows: [updated] } = await db.query(
        `UPDATE work_artifacts SET ${updates.join(', ')}, updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 RETURNING *`,
        params
      );

      if (!updated) return res.status(404).json({ error: 'Artifact not found' });

      // Re-embed if content changed
      if (req.body.content_markdown || req.body.title || req.body.summary) {
        const { rows: links } = await db.query(
          `SELECT * FROM artifact_entity_links WHERE artifact_id = $1`,
          [req.params.id]
        );
        setImmediate(() => embedArtifact(updated, links));
      }

      auditLog(req.user.user_id, 'artifact_updated', 'work_artifact', updated.id,
        { fields: Object.keys(req.body).filter(k => allowed.includes(k)) }, req.ip);

      res.json(updated);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // DELETE /api/artifacts/:id — Soft delete (archive)
  // ═══════════════════════════════════════════════════════════════════════════

  router.delete('/api/artifacts/:id', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows: [archived] } = await db.query(
        `UPDATE work_artifacts SET status = 'archived', updated_at = NOW()
         WHERE id = $1 AND tenant_id = $2 RETURNING id`,
        [req.params.id, req.tenant_id]
      );
      if (!archived) return res.status(404).json({ error: 'Artifact not found' });

      auditLog(req.user.user_id, 'artifact_archived', 'work_artifact', archived.id, {}, req.ip);
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GET /api/artifacts — Search/list artifacts
  // ═══════════════════════════════════════════════════════════════════════════

  router.get('/api/artifacts', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const limit = Math.min(parseInt(req.query.limit) || 20, 100);
      const offset = parseInt(req.query.offset) || 0;

      let where = 'WHERE wa.tenant_id = $1';
      const params = [req.tenant_id];
      let idx = 1;

      if (req.query.type) { idx++; where += ` AND wa.artifact_type = $${idx}::artifact_type`; params.push(req.query.type); }
      if (req.query.status) { idx++; where += ` AND wa.status = $${idx}::artifact_status`; params.push(req.query.status); }
      else { where += ` AND wa.status != 'archived'`; }

      if (req.query.person_id) {
        idx++; where += ` AND EXISTS (SELECT 1 FROM artifact_entity_links ael WHERE ael.artifact_id = wa.id AND ael.person_id = $${idx})`;
        params.push(req.query.person_id);
      }
      if (req.query.company_id) {
        idx++; where += ` AND EXISTS (SELECT 1 FROM artifact_entity_links ael WHERE ael.artifact_id = wa.id AND ael.company_id = $${idx})`;
        params.push(req.query.company_id);
      }
      if (req.query.search_id) {
        idx++; where += ` AND EXISTS (SELECT 1 FROM artifact_entity_links ael WHERE ael.artifact_id = wa.id AND ael.search_id = $${idx})`;
        params.push(req.query.search_id);
      }
      if (req.query.q) {
        idx++; where += ` AND (wa.title ILIKE $${idx} OR wa.content_markdown ILIKE $${idx} OR wa.summary ILIKE $${idx})`;
        params.push(`%${req.query.q}%`);
      }

      idx++; params.push(limit);
      idx++; params.push(offset);

      const [result, countResult] = await Promise.all([
        db.query(`
          SELECT wa.id, wa.artifact_type, wa.title, wa.summary, wa.key_findings,
                 wa.status, wa.created_by_name, wa.source_context,
                 wa.created_at, wa.updated_at
          FROM work_artifacts wa ${where}
          ORDER BY wa.updated_at DESC
          LIMIT $${idx - 1} OFFSET $${idx}
        `, params),
        db.query(`SELECT COUNT(*) AS cnt FROM work_artifacts wa ${where}`, params.slice(0, -2)),
      ]);

      res.json({
        artifacts: result.rows,
        total: parseInt(countResult.rows[0].cnt),
        limit, offset,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GET /api/people/:id/artifacts — Artifacts linked to a person
  // ═══════════════════════════════════════════════════════════════════════════

  router.get('/api/people/:id/artifacts', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(`
        SELECT wa.id, wa.artifact_type, wa.title, wa.summary, wa.key_findings,
               wa.status, wa.created_by_name, wa.created_at,
               ael.link_type
        FROM work_artifacts wa
        JOIN artifact_entity_links ael ON ael.artifact_id = wa.id AND ael.person_id = $1
        WHERE wa.tenant_id = $2 AND wa.status != 'archived'
        ORDER BY wa.created_at DESC
        LIMIT 20
      `, [req.params.id, req.tenant_id]);
      res.json({ artifacts: rows, total: rows.length });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GET /api/companies/:id/artifacts — Artifacts linked to a company
  // ═══════════════════════════════════════════════════════════════════════════

  router.get('/api/companies/:id/artifacts', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(`
        SELECT wa.id, wa.artifact_type, wa.title, wa.summary, wa.key_findings,
               wa.status, wa.created_by_name, wa.created_at,
               ael.link_type
        FROM work_artifacts wa
        JOIN artifact_entity_links ael ON ael.artifact_id = wa.id AND ael.company_id = $1
        WHERE wa.tenant_id = $2 AND wa.status != 'archived'
        ORDER BY wa.created_at DESC
        LIMIT 20
      `, [req.params.id, req.tenant_id]);
      res.json({ artifacts: rows, total: rows.length });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // GET /api/searches/:id/artifacts — Artifacts linked to a search
  // ═══════════════════════════════════════════════════════════════════════════

  router.get('/api/searches/:id/artifacts', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(`
        SELECT wa.id, wa.artifact_type, wa.title, wa.summary, wa.key_findings,
               wa.status, wa.created_by_name, wa.created_at,
               ael.link_type
        FROM work_artifacts wa
        JOIN artifact_entity_links ael ON ael.artifact_id = wa.id AND ael.search_id = $1
        WHERE wa.tenant_id = $2 AND wa.status != 'archived'
        ORDER BY wa.created_at DESC
        LIMIT 20
      `, [req.params.id, req.tenant_id]);
      res.json({ artifacts: rows, total: rows.length });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // POST /api/artifacts/:id/links — Add entity link
  // ═══════════════════════════════════════════════════════════════════════════

  router.post('/api/artifacts/:id/links', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { entity_type, person_id, company_id, search_id, link_type } = req.body;
      if (!entity_type) return res.status(400).json({ error: 'entity_type required' });

      const links = await insertEntityLinks(db, req.params.id, req.tenant_id, [{
        entity_type, person_id, company_id, search_id,
        link_type: link_type || 'related',
        confidence: 1.0, auto_detected: false,
      }]);

      res.status(201).json(links[0] || { error: 'Link already exists or entity not found' });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // DELETE /api/artifacts/:id/links/:linkId — Remove entity link
  // ═══════════════════════════════════════════════════════════════════════════

  router.delete('/api/artifacts/:id/links/:linkId', authenticateToken, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rowCount } = await db.query(
        `DELETE FROM artifact_entity_links WHERE id = $1 AND artifact_id = $2 AND tenant_id = $3`,
        [req.params.linkId, req.params.id, req.tenant_id]
      );
      if (!rowCount) return res.status(404).json({ error: 'Link not found' });
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // POST /api/artifacts/search-semantic — Vector search across artifacts
  // MANDATORY: tenant_id filter on every query — no exception
  // ═══════════════════════════════════════════════════════════════════════════

  router.post('/api/artifacts/search-semantic', authenticateToken, async (req, res) => {
    try {
      const { query, artifact_type, limit } = req.body;
      if (!query || query.length < 3) return res.status(400).json({ error: 'query required (min 3 chars)' });

      const vector = await generateQueryEmbedding(query);
      const searchLimit = Math.min(parseInt(limit) || 10, 30);

      // MANDATORY tenant_id filter — artifacts NEVER surface cross-tenant
      const filter = {
        must: [{ key: 'tenant_id', match: { value: req.tenant_id } }],
      };
      if (artifact_type) {
        filter.must.push({ key: 'artifact_type', match: { value: artifact_type } });
      }

      // Search Qdrant work_artifacts collection
      const url = new URL('/collections/work_artifacts/points/search', process.env.QDRANT_URL);
      const body = JSON.stringify({ vector, limit: searchLimit, with_payload: true, filter });

      const results = await new Promise((resolve, reject) => {
        const req = https.request({
          hostname: url.hostname, port: url.port || 443, path: url.pathname,
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'api-key': process.env.QDRANT_API_KEY },
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
        req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
        req.write(body);
        req.end();
      });

      res.json({
        results: results.map(r => ({
          score: r.score,
          artifact_id: r.payload?.artifact_id,
          title: r.payload?.title,
          artifact_type: r.payload?.artifact_type,
          created_by_name: r.payload?.created_by_name,
          created_at: r.payload?.created_at,
          person_ids: r.payload?.person_ids || [],
          company_ids: r.payload?.company_ids || [],
        })),
        total: results.length,
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
};
