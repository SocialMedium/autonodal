#!/usr/bin/env node
/**
 * MitchelLake — Xero API Invoice Sync
 *
 * Connects to Xero via OAuth 2.0, fetches invoices incrementally,
 * and upserts placements + client financials.
 *
 * Usage:
 *   node scripts/sync_xero.js              # Run sync
 *   node scripts/sync_xero.js --status     # Check connection status
 *
 * OAuth setup:
 *   1. Create app at https://developer.xero.com/app/manage
 *   2. Set XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REDIRECT_URI in .env
 *   3. Visit /api/xero/connect in browser to authorize
 *   4. Sync runs automatically via scheduler or manually here
 */

require('dotenv').config();
const { Pool } = require('pg');
const {
  extractCandidateName,
  extractRoleTitle,
  determineRoleLevel,
  findPersonByName,
  findOrCreateClient,
  updateClientFinancials
} = require('./ingest_xero_invoices');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
  max: 5
});

// ═══════════════════════════════════════════════════════════════════════════════
// TOKEN MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════

async function getValidToken() {
  const { rows } = await pool.query(
    'SELECT * FROM xero_tokens ORDER BY updated_at DESC LIMIT 1'
  );
  if (!rows.length) {
    throw new Error('No Xero tokens found. Visit /api/xero/connect to authorize.');
  }

  const token = rows[0];

  // Refresh if expiring within 2 minutes
  if (new Date(token.expires_at) < new Date(Date.now() + 120_000)) {
    console.log('   🔄 Refreshing Xero access token...');
    return await refreshToken(token);
  }

  return token;
}

async function refreshToken(token) {
  const credentials = Buffer.from(
    `${process.env.XERO_CLIENT_ID}:${process.env.XERO_CLIENT_SECRET}`
  ).toString('base64');

  const res = await fetch('https://identity.xero.com/connect/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${credentials}`
    },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: token.refresh_token
    })
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Xero token refresh failed (${res.status}): ${err}`);
  }

  const data = await res.json();

  await pool.query(`
    UPDATE xero_tokens SET
      access_token = $1,
      refresh_token = $2,
      expires_at = $3,
      updated_at = NOW()
    WHERE tenant_id = $4
  `, [
    data.access_token,
    data.refresh_token,
    new Date(Date.now() + data.expires_in * 1000),
    token.tenant_id
  ]);

  return { ...token, access_token: data.access_token, refresh_token: data.refresh_token };
}

async function saveTokens(tokenData, tenantId, tenantName) {
  await pool.query(`
    INSERT INTO xero_tokens (tenant_id, tenant_name, access_token, refresh_token, expires_at, scope)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (tenant_id) DO UPDATE SET
      tenant_name = $2, access_token = $3, refresh_token = $4,
      expires_at = $5, scope = $6, updated_at = NOW()
  `, [
    tenantId, tenantName,
    tokenData.access_token, tokenData.refresh_token,
    new Date(Date.now() + tokenData.expires_in * 1000),
    tokenData.scope || ''
  ]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// XERO API HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

async function xeroGet(endpoint, token, params = {}) {
  const url = new URL(`https://api.xero.com/api.xro/2.0${endpoint}`);
  Object.entries(params).forEach(([k, v]) => {
    if (v != null) url.searchParams.set(k, v);
  });

  const headers = {
    Authorization: `Bearer ${token.access_token}`,
    'Xero-Tenant-Id': token.tenant_id,
    Accept: 'application/json'
  };

  const res = await fetch(url.toString(), { headers });

  if (res.status === 429) {
    // Rate limited — wait and retry
    const retryAfter = parseInt(res.headers.get('Retry-After') || '5', 10);
    console.log(`   ⏳ Rate limited, waiting ${retryAfter}s...`);
    await new Promise(r => setTimeout(r, retryAfter * 1000));
    return xeroGet(endpoint, token, params);
  }

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Xero API ${res.status}: ${err.slice(0, 300)}`);
  }

  return res.json();
}

// ═══════════════════════════════════════════════════════════════════════════════
// INVOICE SYNC
// ═══════════════════════════════════════════════════════════════════════════════

function mapPaymentStatus(xeroStatus, amountDue, dueDate) {
  const status = (xeroStatus || '').toUpperCase();
  if (status === 'PAID') return 'paid';
  if (status === 'VOIDED' || status === 'DELETED') return 'written_off';
  if (amountDue > 0 && dueDate && new Date(dueDate) < new Date()) return 'overdue';
  if (status === 'AUTHORISED' || status === 'SUBMITTED') return 'pending';
  return 'pending';
}

async function processXeroInvoice(invoice) {
  // Skip credit notes, drafts, deleted
  if (invoice.Type !== 'ACCREC') return 'skipped';
  if (invoice.Status === 'DELETED' || invoice.Status === 'DRAFT') return 'skipped';
  if (!invoice.Total || invoice.Total <= 0) return 'skipped';

  // Extract candidate from first line item description
  const description = invoice.LineItems?.[0]?.Description || invoice.Reference || '';
  if (!description) return 'skipped';

  const candidateName = extractCandidateName(description);
  if (!candidateName) return 'no_candidate';

  const person = await findPersonByName(candidateName);
  if (!person) return 'candidate_not_found';

  const clientName = invoice.Contact?.Name;
  if (!clientName) return 'skipped';

  const client = await findOrCreateClient(clientName);
  const roleTitle = extractRoleTitle(description);
  const roleLevel = determineRoleLevel(roleTitle);
  const paymentStatus = mapPaymentStatus(invoice.Status, invoice.AmountDue, invoice.DueDate);

  const result = await pool.query(`
    INSERT INTO placements (
      person_id, client_id, role_title, role_level,
      placement_fee, currency, invoice_number, invoice_date,
      payment_status, payment_date, payment_amount, outstanding_amount,
      start_date, placed_by_user_id, source, xero_invoice_id, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
      (SELECT id FROM users WHERE role IN ('admin','consultant') ORDER BY created_at LIMIT 1),
      'xero_api', $14, $15)
    ON CONFLICT (xero_invoice_id) DO UPDATE SET
      payment_status = EXCLUDED.payment_status,
      payment_date = EXCLUDED.payment_date,
      payment_amount = EXCLUDED.payment_amount,
      outstanding_amount = EXCLUDED.outstanding_amount,
      placement_fee = EXCLUDED.placement_fee,
      updated_at = NOW()
    RETURNING id, (xmax = 0) as is_new
  `, [
    person.id,
    client.id,
    roleTitle,
    roleLevel,
    invoice.Total,
    invoice.CurrencyCode || 'AUD',
    invoice.InvoiceNumber,
    invoice.Date || null,
    paymentStatus,
    invoice.FullyPaidOnDate || null,
    invoice.AmountPaid || 0,
    invoice.AmountDue || 0,
    invoice.Date || null,
    invoice.InvoiceID,
    JSON.stringify({
      xero_status: invoice.Status,
      xero_reference: invoice.Reference,
      line_item_count: invoice.LineItems?.length || 0,
      synced_at: new Date().toISOString()
    })
  ]);

  return result.rows[0]?.is_new ? 'created' : 'updated';
}

async function syncInvoices() {
  const stats = { fetched: 0, created: 0, updated: 0, skipped: 0, no_candidate: 0, not_found: 0, errors: 0 };

  const token = await getValidToken();
  console.log(`   🔗 Connected to Xero tenant: ${token.tenant_name || token.tenant_id}`);

  // Get last sync time for incremental fetch
  const { rows: syncRows } = await pool.query(
    'SELECT last_modified_since FROM xero_sync_state WHERE tenant_id = $1',
    [token.tenant_id]
  );
  const ifModifiedSince = syncRows[0]?.last_modified_since || null;

  if (ifModifiedSince) {
    console.log(`   📅 Incremental sync since ${new Date(ifModifiedSince).toLocaleDateString()}`);
  } else {
    console.log('   📅 Full initial sync');
  }

  // Paginate through invoices (Xero returns max 100 per page)
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    // Refresh token if needed between pages
    const currentToken = await getValidToken();

    const params = {
      page: page.toString(),
      where: 'Type=="ACCREC"',
      order: 'UpdatedDateUTC DESC'
    };

    // Xero uses If-Modified-Since header, not query param
    const headers = {};
    if (ifModifiedSince) {
      headers['If-Modified-Since'] = new Date(ifModifiedSince).toUTCString();
    }

    const data = await xeroGet('/Invoices', currentToken, params);
    const invoices = data.Invoices || [];

    console.log(`   📄 Page ${page}: ${invoices.length} invoices`);

    for (const inv of invoices) {
      try {
        const result = await processXeroInvoice(inv);
        switch (result) {
          case 'created': stats.created++; console.log(`     ✅ New: ${inv.Contact?.Name} — $${inv.Total?.toLocaleString()}`); break;
          case 'updated': stats.updated++; break;
          case 'skipped': stats.skipped++; break;
          case 'no_candidate': stats.no_candidate++; break;
          case 'candidate_not_found': stats.not_found++; break;
        }
      } catch (e) {
        stats.errors++;
        console.warn(`     ❌ Error on ${inv.InvoiceNumber}: ${e.message}`);
      }
    }

    stats.fetched += invoices.length;
    hasMore = invoices.length === 100;
    page++;

    if (hasMore) await new Promise(r => setTimeout(r, 200)); // Rate limit courtesy
  }

  // Update sync state
  await pool.query(`
    INSERT INTO xero_sync_state (tenant_id, last_sync_at, last_modified_since, invoices_synced)
    VALUES ($1, NOW(), NOW(), $2)
    ON CONFLICT (tenant_id) DO UPDATE SET
      last_sync_at = NOW(), last_modified_since = NOW(),
      invoices_synced = $2, last_error = NULL, updated_at = NOW()
  `, [token.tenant_id, stats.fetched]);

  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE FUNCTION (called by scheduler)
// ═══════════════════════════════════════════════════════════════════════════════

async function pipelineSyncXero() {
  if (!process.env.XERO_CLIENT_ID) {
    console.log('     ⚠️  XERO_CLIENT_ID not set, skipping');
    return { skipped: true };
  }

  console.log('   💰 Syncing invoices from Xero...');
  const stats = await syncInvoices();

  if (stats.created > 0 || stats.updated > 0) {
    console.log('   📊 Updating client financials...');
    await updateClientFinancials();
  }

  console.log(`   📋 Fetched: ${stats.fetched} | New: ${stats.created} | Updated: ${stats.updated} | Skipped: ${stats.skipped}`);
  if (stats.no_candidate > 0) console.log(`   ⚠️  No candidate name: ${stats.no_candidate}`);
  if (stats.not_found > 0) console.log(`   ⚠️  Candidate not found: ${stats.not_found}`);

  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// CLI
// ═══════════════════════════════════════════════════════════════════════════════

if (require.main === module) {
  const args = process.argv.slice(2);

  if (args.includes('--status')) {
    (async () => {
      const tokens = await pool.query('SELECT tenant_id, tenant_name, expires_at FROM xero_tokens');
      const sync = await pool.query('SELECT * FROM xero_sync_state');
      console.log('Xero connection:', tokens.rows.length ? tokens.rows : 'Not connected');
      console.log('Sync state:', sync.rows.length ? sync.rows : 'Never synced');
      await pool.end();
    })();
  } else {
    pipelineSyncXero()
      .then(stats => { console.log('\n✅ Xero sync complete:', stats); process.exit(0); })
      .catch(err => { console.error('❌ Xero sync failed:', err.message); process.exit(1); });
  }
}

module.exports = { pipelineSyncXero, saveTokens, getValidToken };
