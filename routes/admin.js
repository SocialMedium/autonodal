// ═══════════════════════════════════════════════════════════════════════════════
// routes/admin.js — Admin-only API routes
// 15 routes: /api/admin/*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const path = require('path');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, requireAdmin, auditLog, rootDir }) {

  // ─── Multer for LinkedIn CSV uploads ──────────────────────────────────────

  const adminUpload = require('multer')({ dest: '/tmp/ml-admin-uploads/', limits: { fileSize: 20 * 1024 * 1024 } });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 1. GET /api/admin/waitlist
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/waitlist', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(
        `SELECT id, name, email, company, status, notes, created_at, updated_at
         FROM waitlist ORDER BY created_at DESC`
      );
      const { rows: [counts] } = await db.query(
        `SELECT
           COUNT(*) as total,
           COUNT(*) FILTER (WHERE status = 'pending') as pending,
           COUNT(*) FILTER (WHERE status = 'approved') as approved,
           COUNT(*) FILTER (WHERE status = 'declined') as declined,
           COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as last_24h,
           COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as last_7d
         FROM waitlist`
      );
      res.json({ entries: rows, counts });
    } catch (err) {
      console.error('[waitlist admin] list error:', err.message);
      res.status(500).json({ error: 'Failed to load waitlist' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 2. PATCH /api/admin/waitlist/:id
  // ═══════════════════════════════════════════════════════════════════════════════

  router.patch('/api/admin/waitlist/:id', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { status, notes } = req.body;
      const allowed = ['pending', 'approved', 'declined', 'contacted'];
      if (status && !allowed.includes(status)) return res.status(400).json({ error: 'Invalid status' });

      const sets = [];
      const vals = [];
      let idx = 1;
      if (status) { sets.push(`status = $${idx++}`); vals.push(status); }
      if (notes !== undefined) { sets.push(`notes = $${idx++}`); vals.push(notes); }
      sets.push(`updated_at = NOW()`);
      vals.push(req.params.id);

      const { rows } = await db.query(
        `UPDATE waitlist SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
        vals
      );
      if (!rows.length) return res.status(404).json({ error: 'Not found' });

      await auditLog(req.user.user_id, 'waitlist_update', 'waitlist', req.params.id,
        { status, notes }, req.ip);
      res.json(rows[0]);
    } catch (err) {
      console.error('[waitlist admin] update error:', err.message);
      res.status(500).json({ error: 'Failed to update' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 3. DELETE /api/admin/waitlist/:id
  // ═══════════════════════════════════════════════════════════════════════════════

  router.delete('/api/admin/waitlist/:id', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rowCount } = await db.query('DELETE FROM waitlist WHERE id = $1', [req.params.id]);
      if (!rowCount) return res.status(404).json({ error: 'Not found' });
      res.json({ ok: true });
    } catch (err) {
      res.status(500).json({ error: 'Failed to delete' });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 4. GET /api/admin/users
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows } = await db.query(`
        SELECT u.id, u.name, u.email, u.role, u.region, u.onboarded, u.created_at, u.updated_at,
          (SELECT COUNT(*) FROM sessions s WHERE s.user_id = u.id AND s.expires_at > NOW()) AS active_sessions,
          (SELECT MAX(s.created_at) FROM sessions s WHERE s.user_id = u.id) AS last_login,
          (SELECT COUNT(*) FROM interactions i WHERE i.user_id = u.id AND i.tenant_id = $1) AS interactions_created,
          (SELECT COUNT(*) FROM interactions i WHERE i.user_id = u.id AND i.tenant_id = $1 AND i.interaction_at > NOW() - INTERVAL '30 days') AS interactions_30d,
          (SELECT COUNT(*) FROM team_proximity tp WHERE tp.team_member_id = u.id AND tp.tenant_id = $1) AS proximity_connections,
          (SELECT COUNT(*) FROM signal_dispatches sd WHERE sd.claimed_by = u.id AND sd.tenant_id = $1) AS dispatches_claimed,
          (SELECT COUNT(*) FROM signal_dispatches sd WHERE sd.claimed_by = u.id AND sd.status = 'sent' AND sd.tenant_id = $1) AS dispatches_sent
        FROM users u
        WHERE u.tenant_id = $1
        ORDER BY u.created_at ASC
      `, [req.tenant_id]);

      try {
        const { rows: googleRows } = await db.query(`
          SELECT user_id,
            COUNT(*) AS google_accounts,
            bool_or(sync_enabled) AS google_sync_active,
            MAX(last_sync_at) AS google_last_sync
          FROM user_google_accounts GROUP BY user_id
        `);
        const googleMap = new Map(googleRows.map(g => [g.user_id, g]));
        rows.forEach(u => {
          const g = googleMap.get(u.id);
          u.google_accounts = g?.google_accounts || 0;
          u.google_sync_active = g?.google_sync_active || false;
          u.google_last_sync = g?.google_last_sync || null;
        });
      } catch (e) {
        rows.forEach(u => { u.google_accounts = 0; u.google_sync_active = false; u.google_last_sync = null; });
      }

      res.json({ users: rows });
    } catch (err) {
      console.error('Admin users error:', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 5. PATCH /api/admin/users/:id/role
  // ═══════════════════════════════════════════════════════════════════════════════

  router.patch('/api/admin/users/:id/role', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { role } = req.body;
      if (!['admin', 'consultant', 'researcher', 'viewer'].includes(role)) {
        return res.status(400).json({ error: 'Invalid role' });
      }
      const { rows: [updated] } = await db.query(
        'UPDATE users SET role = $1, updated_at = NOW() WHERE id = $2 AND tenant_id = $3 RETURNING id, name, email, role',
        [role, req.params.id, req.tenant_id]
      );
      if (!updated) return res.status(404).json({ error: 'User not found' });
      auditLog(req.user.user_id, 'change_role', 'user', updated.id, { name: updated.name, new_role: role });
      res.json({ user: updated });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 6. GET /api/admin/health
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/health', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const stats = await db.query(`
        SELECT
          (SELECT COUNT(*) FROM users WHERE tenant_id = $1) AS total_users,
          (SELECT COUNT(*) FROM sessions WHERE expires_at > NOW()) AS active_sessions,
          (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS total_people,
          (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS total_companies,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1)) AS total_signals,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '24 hours') AS signals_24h,
          (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '7 days') AS signals_7d,
          (SELECT COUNT(*) FROM external_documents WHERE tenant_id = $1) AS total_documents,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) AS total_interactions,
          (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND interaction_at > NOW() - INTERVAL '7 days') AS interactions_7d,
          (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1) AS total_placements,
          (SELECT COALESCE(SUM(placement_fee), 0) FROM conversions WHERE tenant_id = $1 AND source IN ('xero_export', 'xero', 'manual', 'myob_import') AND placement_fee IS NOT NULL) AS total_revenue,
          (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1) AS total_dispatches,
          (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1 AND status = 'draft') AS dispatches_draft,
          (SELECT COUNT(*) FROM signal_dispatches WHERE tenant_id = $1 AND status = 'sent') AS dispatches_sent
      `, [req.tenant_id]).catch(e => { console.error('Admin stats error:', e.message); return { rows: [{}] }; });

      let googleCount = 0;
      try { const r = await db.query('SELECT COUNT(*) AS cnt FROM user_google_accounts WHERE sync_enabled = true AND tenant_id = $1', [req.tenant_id]); googleCount = r.rows[0]?.cnt || 0; } catch (e) {}
      let grabsCount = 0;
      try { const r = await db.query('SELECT COUNT(*) AS cnt FROM signal_grabs WHERE tenant_id = $1', [req.tenant_id]); grabsCount = r.rows[0]?.cnt || 0; } catch (e) {}

      let gmailStats = {};
      try {
        const { rows: [gs] } = await db.query(`
          SELECT
            (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync') AS gmail_interactions,
            (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync' AND interaction_at > NOW() - INTERVAL '7 days') AS gmail_7d,
            (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync' AND interaction_at > NOW() - INTERVAL '24 hours') AS gmail_24h,
            (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) AS total_interactions,
            (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1 AND interaction_at > NOW() - INTERVAL '7 days') AS interactions_7d,
            (SELECT COUNT(DISTINCT person_id) FROM interactions WHERE tenant_id = $1 AND source = 'gmail_sync') AS gmail_people_matched,
            (SELECT COUNT(*) FROM team_proximity WHERE tenant_id = $1 AND source = 'gmail') AS gmail_proximity_links,
            (SELECT MAX(last_sync_at) FROM user_google_accounts WHERE sync_enabled = true AND tenant_id = $1) AS last_gmail_sync,
            (SELECT COUNT(*) FROM case_studies WHERE tenant_id = $1 AND status != 'deleted') AS total_case_studies,
            (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND source = 'wip_workbook') AS wip_records,
            (SELECT COUNT(*) FROM conversions WHERE tenant_id = $1 AND source = 'xero_export') AS xero_records,
            (SELECT COUNT(*) FROM receivables WHERE tenant_id = $1) AS total_receivables,
            (SELECT COUNT(*) FROM feed_proposals) AS user_feeds
        `, [req.tenant_id]);
        gmailStats = gs || {};
      } catch (e) { /* some tables may not exist */ }

      const sources = await db.query(`
        SELECT rs.name, rs.source_type, rs.url, rs.enabled,
               rs.last_fetched_at, rs.last_error, rs.consecutive_errors,
               (SELECT COUNT(*) FROM external_documents ed WHERE ed.source_name = rs.name AND (ed.tenant_id IS NULL OR ed.tenant_id = $1)) AS doc_count
        FROM rss_sources rs
        ORDER BY rs.enabled DESC, rs.last_fetched_at DESC NULLS LAST
      `, [req.tenant_id]).catch(() => ({ rows: [] }));

      const pipelines = await db.query(`
        SELECT pipeline_key, pipeline_name, status, started_at, completed_at, duration_ms,
               items_processed, error_message
        FROM pipeline_runs
        ORDER BY started_at DESC LIMIT 30
      `).catch(() => ({ rows: [] }));

      const storage = await db.query(`
        SELECT
          (SELECT COUNT(*) FROM people WHERE embedded_at IS NOT NULL) AS person_embeddings,
          (SELECT COUNT(*) FROM companies WHERE embedded_at IS NOT NULL) AS company_embeddings,
          (SELECT COUNT(*) FROM external_documents WHERE embedded_at IS NOT NULL) AS document_embeddings,
          (SELECT COUNT(*) FROM signal_events WHERE embedded_at IS NOT NULL) AS signal_embeddings,
          (SELECT COUNT(*) FROM case_studies WHERE embedded_at IS NOT NULL) AS case_study_embeddings,
          (SELECT COUNT(*) FROM people) AS total_people,
          (SELECT COUNT(*) FROM companies) AS total_companies,
          (SELECT COUNT(*) FROM external_documents) AS total_documents,
          (SELECT COUNT(*) FROM signal_events) AS total_signals,
          (SELECT COUNT(*) FROM case_studies WHERE status != 'deleted') AS total_case_studies
      `).catch(() => ({ rows: [{}] }));

      const emb = storage.rows[0] || {};
      res.json({
        stats: { ...stats.rows[0], google_syncs_active: googleCount, total_grabs: grabsCount, ...gmailStats },
        sources: sources.rows,
        pipeline_runs: pipelines.rows,
        embeddings: {
          total_embeddings: Number(emb.person_embeddings || 0) + Number(emb.company_embeddings || 0) + Number(emb.document_embeddings || 0) + Number(emb.signal_embeddings || 0) + Number(emb.case_study_embeddings || 0),
          person_embeddings: `${emb.person_embeddings || 0} / ${emb.total_people || 0}`,
          company_embeddings: `${emb.company_embeddings || 0} / ${emb.total_companies || 0}`,
          document_embeddings: `${emb.document_embeddings || 0} / ${emb.total_documents || 0}`,
          signal_embeddings: `${emb.signal_embeddings || 0} / ${emb.total_signals || 0}`,
          case_study_embeddings: `${emb.case_study_embeddings || 0} / ${emb.total_case_studies || 0}`
        }
      });
    } catch (err) {
      console.error('Admin health error:', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 7. GET /api/admin/ingestion
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/ingestion', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const tenantId = req.tenant_id;

      let peopleBySource = [];
      try {
        const { rows } = await db.query(`
          SELECT
            COALESCE(u1.id, u2.id) AS user_id,
            COALESCE(u1.name, u2.name) AS name,
            COALESCE(u1.email, u2.email) AS email,
            p.source,
            COUNT(*) AS count,
            MIN(p.created_at) AS earliest,
            MAX(p.created_at) AS latest
          FROM people p
          LEFT JOIN users u1 ON u1.id = p.created_by
          LEFT JOIN LATERAL (
            SELECT tp.team_member_id FROM team_proximity tp
            WHERE tp.person_id = p.id AND tp.tenant_id = $1 LIMIT 1
          ) tp_link ON p.created_by IS NULL
          LEFT JOIN users u2 ON u2.id = tp_link.team_member_id AND p.created_by IS NULL
          WHERE p.tenant_id = $1
            AND p.source IN ('csv_import', 'linkedin_import', 'chat_concierge', 'chat_intel', 'ezekia', 'gmail_discovery')
          GROUP BY COALESCE(u1.id, u2.id), COALESCE(u1.name, u2.name), COALESCE(u1.email, u2.email), p.source
          ORDER BY MAX(p.created_at) DESC
        `, [tenantId]);
        peopleBySource = rows;
      } catch (e) { /* created_by column may not exist yet on older deployments */ }

      let linkedinConnections = [];
      try {
        const { rows } = await db.query(`
          SELECT
            lc.team_member_id, u.name, u.email,
            COUNT(*) AS total_connections,
            COUNT(lc.matched_person_id) AS matched,
            COUNT(*) - COUNT(lc.matched_person_id) AS unmatched,
            ROUND(AVG(lc.match_confidence)::numeric, 2) AS avg_confidence,
            MIN(lc.imported_at) AS first_import,
            MAX(lc.imported_at) AS last_import,
            COUNT(DISTINCT lc.company) AS unique_companies
          FROM linkedin_connections lc
          LEFT JOIN users u ON u.id = lc.team_member_id
          GROUP BY lc.team_member_id, u.name, u.email
          ORDER BY COUNT(*) DESC
        `);
        linkedinConnections = rows;
      } catch (e) { /* table may not exist */ }

      let googleAccounts = [];
      try {
        const { rows } = await db.query(`
          SELECT
            ug.user_id, u.name, u.email AS user_email,
            ug.google_email, ug.sync_enabled, ug.last_sync_at, ug.scopes,
            ug.created_at AS connected_at,
            (SELECT COUNT(*) FROM interactions i WHERE i.source = 'gmail' AND i.user_id = ug.user_id AND i.tenant_id = $1) AS emails_synced,
            (SELECT COUNT(*) FROM interactions i WHERE i.source = 'gmail' AND i.user_id = ug.user_id AND i.tenant_id = $1 AND i.interaction_at > NOW() - INTERVAL '7 days') AS emails_7d,
            (SELECT COUNT(*) FROM email_signals es WHERE es.user_id = ug.user_id) AS email_signals
          FROM user_google_accounts ug
          JOIN users u ON u.id = ug.user_id
          WHERE ug.tenant_id = $1
          ORDER BY ug.last_sync_at DESC NULLS LAST
        `, [tenantId]);
        googleAccounts = rows;
      } catch (e) { /* table may not exist */ }

      let xeroSync = [];
      try {
        const { rows } = await db.query(`
          SELECT xt.tenant_name, xt.expires_at, xt.updated_at AS token_updated,
                 xs.last_sync_at, xs.invoices_synced, xs.last_error
          FROM xero_tokens xt
          LEFT JOIN xero_sync_state xs ON xs.tenant_id = xt.tenant_id
        `);
        xeroSync = rows;
      } catch (e) { /* tables may not exist */ }

      let ezekiaStats = null;
      try {
        const { rows: [stats] } = await db.query(`
          SELECT
            COUNT(*) FILTER (WHERE source = 'ezekia') AS ezekia_people,
            COUNT(*) FILTER (WHERE enriched_at IS NOT NULL) AS enriched_people,
            MAX(synced_at) FILTER (WHERE source = 'ezekia') AS last_ezekia_sync,
            MAX(enriched_at) AS last_enrichment
          FROM people WHERE tenant_id = $1
        `, [tenantId]);
        ezekiaStats = stats;
      } catch (e) { /* columns may not exist */ }

      let docUploads = [];
      try {
        const { rows } = await db.query(`
          SELECT u.id AS user_id, u.name, u.email,
                 COUNT(*) AS docs_uploaded,
                 MIN(ed.published_at) AS earliest,
                 MAX(ed.published_at) AS latest
          FROM external_documents ed
          JOIN users u ON u.id = ed.uploaded_by_user_id
          WHERE (ed.tenant_id IS NULL OR ed.tenant_id = $1) AND ed.uploaded_by_user_id IS NOT NULL
          GROUP BY u.id, u.name, u.email
          ORDER BY COUNT(*) DESC
        `, [tenantId]);
        docUploads = rows;
      } catch (e) { /* column may not exist */ }

      let proxBySrc = [];
      try {
        const { rows } = await db.query(`
          SELECT
            u.name, u.email,
            tp.source AS proximity_source,
            COUNT(*) AS connections,
            ROUND(AVG(tp.strength)::numeric, 2) AS avg_strength
          FROM team_proximity tp
          JOIN users u ON u.id = tp.team_member_id
          WHERE tp.tenant_id = $1
          GROUP BY u.name, u.email, tp.source
          ORDER BY u.name, COUNT(*) DESC
        `, [tenantId]);
        proxBySrc = rows;
      } catch (e) { /* table may not exist */ }

      res.json({
        people_by_source: peopleBySource,
        linkedin_connections: linkedinConnections,
        google_accounts: googleAccounts,
        xero_sync: xeroSync,
        ezekia_stats: ezekiaStats,
        doc_uploads: docUploads,
        proximity_by_source: proxBySrc
      });
    } catch (err) {
      console.error('Admin ingestion error:', err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 8. GET /api/admin/tenant
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/tenant', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { rows: [tenant] } = await db.query(
        'SELECT * FROM tenants WHERE id = $1', [req.tenant_id]
      );
      res.json({ tenant });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 9. GET /api/admin/audit
  // ═══════════════════════════════════════════════════════════════════════════════

  router.get('/api/admin/audit', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const limit = Math.min(parseInt(req.query.limit) || 50, 200);
      const { rows } = await db.query(`
        SELECT al.action, al.target_type, al.target_id, al.details, al.ip_address, al.created_at,
               u.name AS user_name, u.email AS user_email
        FROM audit_logs al
        LEFT JOIN users u ON u.id = al.user_id
        ORDER BY al.created_at DESC
        LIMIT $1
      `, [limit]);
      res.json({ logs: rows });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 10. POST /api/admin/import-sales
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/import-sales', authenticateToken, requireAdmin, async (req, res) => {
    try {
      console.log(`📊 Sales import: ${req.body?.rows?.length || 0} rows from ${req.user?.email} (tenant: ${req.tenant_id})`);
      // Use platformPool for speed — TenantDB wraps every query in BEGIN/SET/COMMIT (4x overhead)
      const db = { query: (text, params) => platformPool.query(text, params) };
      const { headers, rows } = req.body;
      if (!rows || !rows.length) return res.status(400).json({ error: 'No rows to import' });

      const colMap = {};
      const aliases = {
        client_name: ['client_name', 'client', 'company', 'company_name', 'account_name', 'contactname', 'customer_name', 'customer'],
        role_title: ['role_title', 'role', 'title', 'position', 'job_title', 'reference', 'role_ref'],
        fee: ['fee', 'placement_fee', 'amount', 'amount_local', 'revenue', 'value', 'invoice_amount', 'lineamount', 'line_amount'],
        fee_total: ['total', 'invoiceamount', 'invoice_total', 'invoice_total_local'],
        entity_name: ['entity', 'entity_name', 'business_unit'],
        account_code: ['account', 'account_code', 'account_name'],
        source_type: ['source', 'source_type', 'document_type', 'type'],
        year: ['year', 'fiscal_year'],
        date: ['date', 'start_date', 'invoice_date', 'invoicedate', 'placement_date', 'close_date'],
        invoice_number: ['invoice_number', 'invoice', 'inv_no', 'invoicenumber', 'invoice_no'],
        description: ['description', 'memo', 'line_description', 'item_description'],
        candidate_name: ['candidate_name', 'candidate', 'placed_candidate'],
        payment_status: ['payment_status', 'status', 'payment'],
        fee_stage: ['fee_stage', 'stage', 'phase', 'invoice_type'],
        consultant: ['consultant', 'consultant_name', 'owner', 'recruiter', 'trackingoption2', 'tracking_option_2'],
        line_quantity: ['quantity'],
        currency: ['currency', 'currency_code'],
      };

      const headersLower = headers.map(h => h.toLowerCase().trim().replace(/[^a-z0-9_]/g, '_'));
      for (const [canonical, alts] of Object.entries(aliases)) {
        const idx = headersLower.findIndex(h => alts.includes(h));
        if (idx !== -1) colMap[canonical] = headers[idx];
      }
      if (!colMap.role_title) {
        const roleIdx = headersLower.findIndex(h => h.includes('role') || h.includes('ref'));
        if (roleIdx !== -1) colMap.role_title = headers[roleIdx];
      }
      if (!colMap.fee) {
        const feeIdx = headersLower.findIndex(h => h.includes('fee') || h.includes('amount') || h.includes('value'));
        if (feeIdx !== -1 && !colMap.fee_total) colMap.fee = headers[feeIdx];
      }
      console.log('📊 Column mapping:', JSON.stringify(colMap));

      if (!colMap.client_name && !colMap.role_title) {
        return res.status(400).json({ error: 'CSV must have at least client_name or role_title column. Detected columns: ' + headers.join(', ') });
      }

      const isXeroFormat = colMap.client_name && headers.includes('contactname') && headers.includes('invoicenumber');

      let created = 0, updated = 0, skipped = 0, errors = 0;

      const invoiceMap = new Map();

      function parseDate(dateRaw) {
        if (!dateRaw) return null;
        const raw = String(dateRaw).trim();
        const dmyMatch = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
        if (dmyMatch) {
          const [, dd, mm, yyyy] = dmyMatch;
          const d = new Date(parseInt(yyyy), parseInt(mm) - 1, parseInt(dd));
          if (!isNaN(d.getTime()) && d.getFullYear() > 2000 && d.getFullYear() < 2100) return d.toISOString().split('T')[0];
        }
        const d = new Date(raw);
        if (!isNaN(d.getTime()) && d.getFullYear() > 2000 && d.getFullYear() < 2100) return d.toISOString().split('T')[0];
        return null;
      }

      function mapStatus(rawStatus) {
        const s = (rawStatus || '').toLowerCase().trim();
        if (s === 'paid' || s === 'closed') return 'paid';
        if (s === 'awaiting payment' || s === 'sent' || s === 'authorised' || s === 'open') return 'pending';
        if (s === 'overdue' || s === 'past due') return 'overdue';
        if (s === 'voided' || s === 'deleted' || s === 'void') return 'written_off';
        return 'pending';
      }

      for (const row of rows) {
        try {
          const clientName = row[colMap.client_name] || null;
          const roleRaw = row[colMap.role_title] || null;
          const invoiceNumRaw = row[colMap.invoice_number] || null;
          const roleRefStr = roleRaw ? String(roleRaw).trim() : '';
          const isNumericRef = /^\d+$/.test(roleRefStr);
          const isMYOBRef = roleRefStr.startsWith('MYOB');
          const invoiceNum = invoiceNumRaw || (isNumericRef ? roleRefStr : null);
          const roleTitle = (isNumericRef || isMYOBRef)
            ? (row[colMap.description] || roleRaw)
            : roleRaw;

          let feeRaw = row[colMap.fee] || row[colMap.fee_total];
          let fee = feeRaw ? parseFloat(String(feeRaw).replace(/[$,£€\s]/g, '')) : null;

          const quantity = row[colMap.line_quantity];
          if (isXeroFormat && quantity !== undefined && parseFloat(quantity) === 0) { skipped++; continue; }
          if (fee !== null && fee === 0) { skipped++; continue; }
          if (fee !== null && fee < 0) { skipped++; continue; }
          if (!clientName && !roleTitle) { skipped++; continue; }
          if (fee !== null && isNaN(fee)) { skipped++; continue; }
          const sourceType = row[colMap.source_type] || '';
          if (sourceType.toLowerCase().includes('credit note')) { skipped++; continue; }

          const description = row[colMap.description] || null;
          const feeStage = row[colMap.fee_stage] || null;
          const currency = row[colMap.currency] ? row[colMap.currency].toUpperCase().trim() : 'AUD';
          const entityName = row[colMap.entity_name] || null;
          const yearRaw = row[colMap.year] || null;
          const startDate = parseDate(row[colMap.date]);
          const paymentStatus = mapStatus(row[colMap.payment_status]);
          const consultant = row[colMap.consultant] || null;

          const key = invoiceNum || `_noref_${invoiceMap.size}`;
          if (invoiceMap.has(key) && invoiceNum) {
            const existing = invoiceMap.get(key);
            existing.fee = (existing.fee || 0) + (fee || 0);
            if (description && !existing.descriptions.includes(description)) {
              existing.descriptions.push(description);
            }
            const stageRank = { 'Placement': 5, 'Third Stage': 4, 'Second Stage': 3, 'First Stage': 2 };
            if ((stageRank[feeStage] || 0) > (stageRank[existing.feeStage] || 0)) {
              existing.feeStage = feeStage;
            }
          } else {
            invoiceMap.set(key, {
              invoiceNum, clientName, roleTitle, fee, startDate, paymentStatus,
              feeStage, consultant, currency, entityName, yearRaw,
              descriptions: description ? [description] : []
            });
          }
        } catch (rowErr) {
          console.error('[sales-import] row parse error:', rowErr.message);
          errors++;
        }
      }

      console.log(`📊 Consolidated ${rows.length} rows → ${invoiceMap.size} records`);

      const importSource = 'xero_export';

      for (const [key, rec] of invoiceMap) {
        try {
          const { invoiceNum, clientName, roleTitle, fee, startDate, paymentStatus,
                  feeStage, consultant, currency, entityName, yearRaw, descriptions } = rec;
          const description = descriptions.join(' | ') || null;

          let clientId = null;
          if (clientName) {
            const { rows: [existing] } = await db.query(
              `SELECT id FROM accounts WHERE LOWER(name) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
              [clientName.trim(), req.tenant_id]
            );
            if (existing) {
              clientId = existing.id;
            } else {
              const { rows: [newClient] } = await db.query(
                `INSERT INTO accounts (name, tenant_id, created_at) VALUES ($1, $2, NOW()) RETURNING id`,
                [clientName.trim(), req.tenant_id]
              );
              clientId = newClient.id;
            }
          }

          let existingId = null;
          if (invoiceNum) {
            const { rows: [dup] } = await db.query(
              `SELECT id FROM conversions WHERE invoice_number = $1 AND tenant_id = $2 LIMIT 1`,
              [invoiceNum, req.tenant_id]
            );
            if (dup) existingId = dup.id;
          }

          const meta = {};
          if (entityName) meta.entity = entityName;
          if (yearRaw) meta.fiscal_year = yearRaw;
          const metaJson = Object.keys(meta).length ? JSON.stringify(meta) : '{}';

          if (existingId) {
            await db.query(`
              UPDATE conversions SET
                role_title = COALESCE($1, role_title),
                placement_fee = COALESCE($2, placement_fee),
                start_date = COALESCE($3, start_date),
                client_id = COALESCE($4, client_id),
                client_name_raw = COALESCE($5, client_name_raw),
                consultant_name = COALESCE($6, consultant_name),
                payment_status = $7,
                fee_stage = COALESCE($8, fee_stage),
                notes = COALESCE($9, notes),
                currency = COALESCE($10, currency),
                metadata = COALESCE($11::jsonb, metadata),
                updated_at = NOW()
              WHERE id = $12`,
              [roleTitle, fee, startDate, clientId, clientName, consultant, paymentStatus, feeStage,
               description, currency, metaJson, existingId]
            );
            updated++;
          } else {
            await db.query(`
              INSERT INTO conversions (
                role_title, placement_fee, start_date, client_id, client_name_raw,
                invoice_number, consultant_name, payment_status, fee_stage,
                currency, source, tenant_id, notes, metadata, created_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())`,
              [roleTitle, fee, startDate, clientId, clientName,
               invoiceNum, consultant, paymentStatus, feeStage, currency, importSource, req.tenant_id,
               description, metaJson]
            );
            created++;
          }
        } catch (rowErr) {
          console.error('[sales-import] row error:', rowErr.message);
          errors++;
        }
      }

      await auditLog(req.user.user_id, 'sales_csv_import', 'conversions', null,
        { rows: rows.length, created, updated, skipped, errors }, req.ip);

      console.log(`[sales-import] ${created} created, ${updated} updated, ${skipped} skipped, ${errors} errors`);
      res.json({ created, updated, skipped, errors, total: rows.length });
    } catch (err) {
      console.error('[sales-import] error:', err.message);
      res.status(500).json({ error: 'Import failed: ' + err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 11. POST /api/admin/clear-sales
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/clear-sales', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = { query: (text, params) => platformPool.query(text, params) };
      const { source } = req.body || {};
      let result;
      if (source) {
        result = await db.query(`DELETE FROM conversions WHERE tenant_id = $1 AND source = $2`, [req.tenant_id, source]);
      } else {
        result = await db.query(`DELETE FROM conversions WHERE tenant_id = $1`, [req.tenant_id]);
      }
      await db.query(`
        DELETE FROM accounts a WHERE a.tenant_id = $1
          AND NOT EXISTS (SELECT 1 FROM conversions c WHERE c.client_id = a.id)
          AND NOT EXISTS (SELECT 1 FROM searches s WHERE s.client_id = a.id)
      `, [req.tenant_id]);
      await auditLog(req.user.user_id, 'sales_data_cleared', 'conversions', null,
        { deleted: result.rowCount, source: source || 'all' }, req.ip);
      console.log(`[clear-sales] Deleted ${result.rowCount} conversions for tenant ${req.tenant_id}`);
      res.json({ deleted: result.rowCount });
    } catch (err) {
      console.error('[clear-sales] error:', err.message);
      res.status(500).json({ error: 'Clear failed: ' + err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 12. POST /api/admin/import-sales-myob
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/import-sales-myob', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { invoices } = req.body;
      if (!invoices || !invoices.length) return res.status(400).json({ error: 'No invoices to import' });

      let created = 0, updated = 0, skipped = 0, errors = 0;

      for (const inv of invoices) {
        try {
          const clientName = inv.client_name ? inv.client_name.trim() : null;
          const invoiceNum = inv.invoice_number ? inv.invoice_number.trim() : null;
          const roleTitle = inv.role_title || null;
          const candidateName = inv.candidate_name || null;
          const description = inv.description || null;
          const fee = (inv.fee !== null && inv.fee !== undefined && !isNaN(inv.fee)) ? inv.fee : null;
          const currency = inv.currency || 'AUD';
          const paymentStatus = inv.payment_status || 'pending';

          let startDate = null;
          if (inv.date) {
            const d = new Date(inv.date);
            if (!isNaN(d.getTime()) && d.getFullYear() > 2000 && d.getFullYear() < 2100) {
              startDate = d.toISOString().split('T')[0];
            }
          }

          if (!clientName && !roleTitle) { skipped++; continue; }
          if (fee === null || fee === 0) { skipped++; continue; }

          let clientId = null;
          if (clientName) {
            const { rows: [existing] } = await db.query(
              `SELECT id FROM accounts WHERE LOWER(name) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
              [clientName, req.tenant_id]
            );
            if (existing) {
              clientId = existing.id;
            } else {
              const { rows: [newClient] } = await db.query(
                `INSERT INTO accounts (name, tenant_id, created_at) VALUES ($1, $2, NOW()) RETURNING id`,
                [clientName, req.tenant_id]
              );
              clientId = newClient.id;
            }
          }

          let existingId = null;
          if (invoiceNum) {
            const { rows: [dup] } = await db.query(
              `SELECT id FROM conversions WHERE invoice_number = $1 AND tenant_id = $2 LIMIT 1`,
              [invoiceNum, req.tenant_id]
            );
            if (dup) existingId = dup.id;
          }

          if (existingId) {
            await db.query(`
              UPDATE conversions SET
                role_title = COALESCE($1, role_title),
                placement_fee = COALESCE($2, placement_fee),
                start_date = COALESCE($3, start_date),
                client_id = COALESCE($4, client_id),
                client_name_raw = COALESCE($5, client_name_raw),
                payment_status = $6,
                currency = COALESCE($7, currency),
                notes = COALESCE($8, notes),
                updated_at = NOW()
              WHERE id = $9`,
              [roleTitle, fee, startDate, clientId, clientName, paymentStatus, currency,
               description ? ('Candidate: ' + (candidateName || '') + ' | ' + description).substring(0, 500) : null,
               existingId]
            );
            updated++;
          } else {
            await db.query(`
              INSERT INTO conversions (
                role_title, placement_fee, start_date, client_id, client_name_raw,
                invoice_number, payment_status, currency, source, notes, tenant_id, created_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())`,
              [roleTitle, fee, startDate, clientId, clientName,
               invoiceNum, paymentStatus, currency, 'myob_import',
               description ? ('Candidate: ' + (candidateName || '') + ' | ' + description).substring(0, 500) : null,
               req.tenant_id]
            );
            created++;
          }
        } catch (rowErr) {
          console.error('[myob-import] row error:', rowErr.message);
          errors++;
        }
      }

      await auditLog(req.user.user_id, 'myob_sales_import', 'conversions', null,
        { invoices: invoices.length, created, updated, skipped, errors }, req.ip);

      console.log(`[myob-import] ${created} created, ${updated} updated, ${skipped} skipped, ${errors} errors`);
      res.json({ created, updated, skipped, errors, total: invoices.length });
    } catch (err) {
      console.error('[myob-import] error:', err.message);
      res.status(500).json({ error: 'MYOB import failed: ' + err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 13. POST /api/admin/upload-linkedin
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/upload-linkedin', authenticateToken, requireAdmin, adminUpload.single('file'), async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const file = req.file;
      const targetUserId = req.body.target_user_id;
      if (!file || !targetUserId) return res.status(400).json({ error: 'File and target_user_id required' });

      const raw = require('fs').readFileSync(file.path, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
      const allLines = raw.split('\n');
      let headerIdx = 0;
      for (let i = 0; i < Math.min(allLines.length, 20); i++) {
        const line = allLines[i].toLowerCase().replace(/[^\x20-\x7E]/g, '');
        if (line.includes('first name') || line.includes('firstname') || (line.includes('name') && line.includes('company'))) {
          headerIdx = i; break;
        }
      }
      if (headerIdx === 0) {
        for (let i = 0; i < Math.min(allLines.length, 20); i++) {
          const parts = allLines[i].split(',');
          if (parts.length >= 3 && parts[0].trim().length > 0 && parts[0].trim().length < 30) { headerIdx = i; break; }
        }
      }
      const lines = allLines.slice(headerIdx).filter(l => l.trim());
      if (!lines.length) return res.json({ error: 'No data found in CSV' });

      function parseCSV(line) { const r=[]; let c='',q=false; for(let i=0;i<line.length;i++){const ch=line[i];if(ch==='"')q=!q;else if(ch===','&&!q){r.push(c.trim());c='';}else c+=ch;} r.push(c.trim()); return r; }
      const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const vals = parseCSV(lines[i]);
        const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
      }

      console.log(`Admin LinkedIn upload: ${rows.length} rows, headers: [${headers.slice(0, 8).join('], [')}]`);

      const sampleRow = rows[0] || {};
      const aColKeys = Object.keys(sampleRow);
      const aFindCol = (...patterns) => aColKeys.find(k => patterns.some(p => k.toLowerCase().trim().replace(/[^a-z\s]/g, '').includes(p))) || '';
      const aFirstNameCol = aFindCol('first name', 'firstname');
      const aLastNameCol = aFindCol('last name', 'lastname');
      const aUrlCol = aFindCol('url', 'profile');
      const aCompanyCol = aFindCol('company', 'organisation', 'organization');
      const aPositionCol = aFindCol('position', 'title', 'role');
      const aEmailCol = aFindCol('email');

      console.log(`Admin LinkedIn upload: detected cols — firstName: "${aFirstNameCol}", lastName: "${aLastNameCol}", url: "${aUrlCol}", company: "${aCompanyCol}"`);

      if (!aFirstNameCol && !aLastNameCol) {
        try { require('fs').unlinkSync(file.path); } catch (e) {}
        return res.json({ error: 'Could not detect name columns. Headers found: ' + headers.slice(0, 8).join(', '), total: rows.length, headers: headers.slice(0, 10) });
      }

      const tenantId = req.tenant_id;
      const adminUserId = req.user.user_id;
      res.json({ total: rows.length, message: `Processing ${rows.length} connections in background. Check admin dashboard for progress.`, headers: headers.slice(0, 8), detected: { firstName: aFirstNameCol, lastName: aLastNameCol, url: aUrlCol, company: aCompanyCol } });

      (async () => {
        try {
          const { rows: dbPeople } = await db.query(
            `SELECT id, full_name, linkedin_url, current_company_name, email FROM people WHERE tenant_id = $1`, [tenantId]
          );
          const linkedinIndex = new Map(), nameIndex = new Map();
          for (const p of dbPeople) {
            if (p.linkedin_url) {
              const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1];
              if (slug) linkedinIndex.set(slug, p);
            }
            const norm = (p.full_name || '').toLowerCase().trim();
            if (norm) { if (!nameIndex.has(norm)) nameIndex.set(norm, []); nameIndex.get(norm).push(p); }
          }

          const stats = { total: rows.length, matched: 0, created: 0, proximity_created: 0, skipped: 0 };

          for (const row of rows) {
            const firstName = (aFirstNameCol ? row[aFirstNameCol] : '') || '';
            const lastName = (aLastNameCol ? row[aLastNameCol] : '') || '';
            const fullName = `${firstName} ${lastName}`.trim();
            const linkedinUrl = (aUrlCol ? row[aUrlCol] : '') || '';
            const company = (aCompanyCol ? row[aCompanyCol] : '') || '';
            const position = (aPositionCol ? row[aPositionCol] : '') || '';
            const email = (aEmailCol ? row[aEmailCol] : '') || '';
            if (!fullName || fullName.length < 2) { stats.skipped++; continue; }

            let personId = null;
            const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
            if (slug && linkedinIndex.has(slug)) personId = linkedinIndex.get(slug).id;
            if (!personId) {
              const cands = nameIndex.get(fullName.toLowerCase().trim()) || [];
              if (cands.length === 1) personId = cands[0].id;
            }

            if (personId) {
              stats.matched++;
            } else {
              try {
                const { rows: [newP] } = await db.query(
                  `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, linkedin_url, email, source, created_by, tenant_id)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,'linkedin_import',$8,$9) RETURNING id`,
                  [fullName, firstName, lastName, position || null, company || null, linkedinUrl || null, email || null, targetUserId, tenantId]
                );
                personId = newP.id;
                stats.created++;
              } catch (e) { stats.skipped++; continue; }
            }

            if (personId) {
              try {
                await db.query(
                  `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
                   VALUES ($1, $2, 'linkedin_connection', 0.5, 'linkedin_import', $3)
                   ON CONFLICT (person_id, team_member_id) DO UPDATE SET relationship_strength = GREATEST(team_proximity.relationship_strength, 0.5)`,
                  [personId, targetUserId, tenantId]
                );
                stats.proximity_created++;
              } catch (e) {}
            }

            if (stats.created % 500 === 0 && stats.created > 0) console.log(`  LinkedIn import: ${stats.created} created, ${stats.matched} matched so far...`);
          }

          try { require('fs').unlinkSync(file.path); } catch (e) {}
          auditLog(adminUserId, 'admin_linkedin_import', 'people', targetUserId, { ...stats, filename: file.originalname });
          console.log(`✅ LinkedIn import complete: ${stats.total} total, ${stats.matched} matched, ${stats.created} created, ${stats.proximity_created} links`);
        } catch (e) {
          console.error('LinkedIn background import error:', e.message);
        }
      })();
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 14. POST /api/admin/trigger-drive-sync
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/trigger-drive-sync', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      const { user_id } = req.body;
      if (!user_id) return res.status(400).json({ error: 'user_id required' });

      const { rows } = await db.query(
        `SELECT id, google_email, access_token, refresh_token FROM user_google_accounts WHERE user_id = $1 AND sync_enabled = true`,
        [user_id]
      );
      if (!rows.length) return res.json({ message: 'No Google account connected for this user' });

      const { rows: [user] } = await db.query('SELECT name FROM users WHERE id = $1', [user_id]);
      auditLog(req.user.user_id, 'admin_trigger_drive_sync', 'user', user_id, { google_email: rows[0].google_email });
      res.json({ message: `Drive sync triggered for ${user?.name || user_id} (${rows[0].google_email}). Will process on next sync cycle.` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ═══════════════════════════════════════════════════════════════════════════════
  // 15. POST /api/admin/trigger-crm-sync
  // ═══════════════════════════════════════════════════════════════════════════════

  router.post('/api/admin/trigger-crm-sync', authenticateToken, requireAdmin, async (req, res) => {
    try {
      const db = new TenantDB(req.tenant_id);
      if (!process.env.EZEKIA_API_TOKEN) return res.json({ message: 'Ezekia API not configured. Set EZEKIA_API_TOKEN in environment.' });

      const { rows: [ezCount] } = await db.query(
        `SELECT COUNT(*) AS cnt FROM people WHERE source = 'ezekia' AND tenant_id = $1`, [req.tenant_id]
      ).catch(() => ({ rows: [{ cnt: 0 }] }));

      const { exec } = require('child_process');
      exec(`node ${path.join(rootDir, 'scripts', 'sync_ezekia.js')}`, { timeout: 600000 }, (err, stdout, stderr) => {
        if (stdout) console.log(stdout.slice(-500));
        if (stderr) console.error('CRM sync stderr:', stderr.slice(-200));
        if (err) console.error('CRM sync error:', err.message?.slice(0, 200));
        else console.log('✅ Ezekia CRM sync complete');
      });

      auditLog(req.user.user_id, 'trigger_crm_sync', 'system', null, { current_ezekia_count: ezCount.cnt });
      res.json({ message: `Ezekia CRM sync triggered. Currently ${parseInt(ezCount.cnt).toLocaleString()} people from Ezekia. Sync running in background.` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
};
