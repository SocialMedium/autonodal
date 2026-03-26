#!/usr/bin/env node
// ============================================================================
// Ingest Invoice Ledger Sheets from Global Billings & WIP workbook
// Sheets: "Invoices - ML UK" and "Invoices - ML AU"
// ============================================================================

require('dotenv').config();
const XLSX = require('xlsx');
const path = require('path');
const { Pool } = require('pg');

const DRY_RUN = process.argv.includes('--dry-run');
const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const FILE_PATH = path.join(__dirname, '..', 'data', 'Global_Billings_and_WIP.xlsx');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function ensureSchema() {
  // NOTE: "placements" is a VIEW over "conversions" — ALTER the real table
  const alters = [
    `ALTER TABLE conversions ALTER COLUMN person_id DROP NOT NULL`,
    `ALTER TABLE conversions ALTER COLUMN client_id DROP NOT NULL`,
    `ALTER TABLE conversions ALTER COLUMN placed_by_user_id DROP NOT NULL`,
    `ALTER TABLE conversions ALTER COLUMN start_date DROP NOT NULL`,
    `ALTER TABLE conversions ALTER COLUMN placement_fee DROP NOT NULL`,
    `ALTER TABLE conversions ALTER COLUMN role_title DROP NOT NULL`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS company_id UUID`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS fee_stage VARCHAR(30)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS fee_estimate DECIMAL(12,2)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS opportunity_type VARCHAR(50)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS source_sheet VARCHAR(100)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS raw_monthly_data JSONB`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS consultant_name VARCHAR(100)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS client_name_raw VARCHAR(255)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS candidate_salary_raw VARCHAR(50)`,
    `ALTER TABLE conversions ADD COLUMN IF NOT EXISTS tenant_id UUID DEFAULT '00000000-0000-0000-0000-000000000001'`,
    `CREATE TABLE IF NOT EXISTS receivables (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID DEFAULT '00000000-0000-0000-0000-000000000001', invoice_number VARCHAR(50), client_name VARCHAR(255), company_id UUID, invoice_date DATE, due_date DATE, invoice_total DECIMAL(12,2), currency VARCHAR(3) DEFAULT 'GBP', status VARCHAR(50), days_overdue INTEGER, notes TEXT, action VARCHAR(100), source VARCHAR(50) DEFAULT 'workbook', created_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE OR REPLACE VIEW placements AS SELECT * FROM conversions`
  ];
  let applied = 0;
  for (const sql of alters) {
    try { await pool.query(sql); applied++; } catch (e) {
      console.log(`    ⚠️ ${sql.slice(0, 50)}... → ${e.message.slice(0, 60)}`);
    }
  }
  console.log(`  ✅ Schema migration: ${applied}/${alters.length}`);
}

const FEE_TYPE_MAP = {
  'client fees - first stage': 'retainer_stage1',
  'client fees - second stage': 'retainer_stage2',
  'client fees - placement': 'placement',
  'client fees - project': 'project',
};

function extractCandidateName(description) {
  if (!description) return null;
  const m = description.match(/Candidate:\s*([^\n\\,]+)/i);
  if (m) return m[1].trim();
  // Fallback: "Role - Name" pattern at end
  const m2 = description.match(/ - ([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})$/);
  return m2 ? m2[1].trim() : null;
}

async function matchCompany(name) {
  if (!name || name === 'NaN') return null;
  const clean = name.replace(/\s+(Pty|Ltd|Limited|Inc)\b\.?/gi, '').trim();
  const { rows } = await pool.query(
    `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
    [`%${clean}%`, TENANT_ID]
  );
  return rows[0]?.id || null;
}

async function matchUser(name) {
  if (!name || name === 'NaN') return null;
  const { rows } = await pool.query(
    `SELECT id FROM users WHERE name ILIKE $1 LIMIT 1`,
    [`%${name.trim()}%`]
  );
  return rows[0]?.id || null;
}

async function matchPerson(name) {
  if (!name) return null;
  const { rows } = await pool.query(
    `SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
    [name.trim(), TENANT_ID]
  );
  return rows[0]?.id || null;
}

async function processSheet(workbook, sheetName, defaultCurrency) {
  const sheet = workbook.Sheets[sheetName];
  if (!sheet) { console.log(`  ⚠️  Sheet "${sheetName}" not found, skipping`); return { inserted: 0, skipped: 0 }; }

  const allRows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
  // Header at row 6 (0-indexed), data from row 7
  const headers = allRows[6] || [];
  const dataRows = allRows.slice(7);

  console.log(`\n  📄 ${sheetName}: ${dataRows.length} rows, ${headers.length} columns`);
  console.log(`     Headers: ${headers.slice(0, 10).join(' | ')}`);

  const stats = { inserted: 0, skipped: 0, noInvoice: 0, noCredit: 0, errors: 0,
    unmatchedClients: new Set(), unmatchedCandidates: new Set(), matchedClients: 0, matchedCandidates: 0 };

  for (const row of dataRows) {
    const invoiceNumber = String(row[0] || '').trim();
    const dateRaw = row[1];
    const clientName = String(row[2] || '').trim();
    const partner = String(row[3] || '').trim();
    const description = String(row[4] || '').trim();
    const reference = String(row[5] || '').trim();
    const currency = String(row[6] || defaultCurrency).trim() || defaultCurrency;
    const creditSource = parseFloat(row[8]) || 0;
    const feeTypeRaw = String(row[11] || '').trim().toLowerCase();
    const deliveryLead = String(row[12] || '').trim();
    const endToEnd = String(row[13] || '').trim();
    const deliverySupport = String(row[14] || '').trim();

    if (!invoiceNumber || invoiceNumber === 'NaN' || invoiceNumber === 'Invoice Number') { stats.noInvoice++; continue; }
    if (creditSource <= 0) { stats.noCredit++; continue; }

    const feeStage = FEE_TYPE_MAP[feeTypeRaw] || null;
    const candidateName = extractCandidateName(description);
    const invoiceDate = dateRaw ? (typeof dateRaw === 'number' ? excelDateToISO(dateRaw) : new Date(dateRaw).toISOString().slice(0, 10)) : null;

    // Resolve entities
    const companyId = await matchCompany(clientName);
    const userId = await matchUser(partner);
    const personId = candidateName ? await matchPerson(candidateName) : null;

    if (companyId) stats.matchedClients++;
    else if (clientName && clientName !== 'NaN') stats.unmatchedClients.add(clientName);

    if (candidateName && personId) stats.matchedCandidates++;
    else if (candidateName) stats.unmatchedCandidates.add(candidateName);

    if (DRY_RUN) {
      stats.inserted++;
      continue;
    }

    try {
      // Check for existing by invoice number
      const { rows: existing } = await pool.query(
        `SELECT id FROM conversions WHERE invoice_number = $1 AND tenant_id = $2 LIMIT 1`,
        [invoiceNumber, TENANT_ID]
      );
      if (existing.length) { stats.skipped++; continue; }

      await pool.query(`
        INSERT INTO conversions (
          person_id, client_id, company_id, placed_by_user_id,
          role_title, start_date, placement_fee, currency, fee_stage,
          invoice_number, invoice_date, payment_status,
          consultant_name, client_name_raw, source, source_sheet,
          notes, tenant_id
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      `, [
        personId, null, companyId, userId,
        description.slice(0, 255) || 'Invoice', invoiceDate, creditSource, currency, feeStage,
        invoiceNumber, invoiceDate, 'invoiced',
        partner || deliveryLead || endToEnd, clientName, 'xero_export', sheetName,
        [reference, deliverySupport ? `Support: ${deliverySupport}` : ''].filter(Boolean).join(' | '),
        TENANT_ID
      ]);
      stats.inserted++;
    } catch (e) {
      stats.errors++;
      if (stats.errors <= 3) console.error(`     ❌ ${invoiceNumber}: ${e.message}`);
    }
  }

  return stats;
}

function excelDateToISO(serial) {
  const d = new Date((serial - 25569) * 86400 * 1000);
  return d.toISOString().slice(0, 10);
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log(' Invoice Ledger Ingestion — Global Billings & WIP');
  console.log(DRY_RUN ? ' 🔍 DRY RUN — no data will be written' : ' 💾 LIVE RUN');
  console.log('═══════════════════════════════════════════════════════════');

  await ensureSchema();

  const workbook = XLSX.readFile(FILE_PATH);
  console.log(`Sheets found: ${workbook.SheetNames.join(', ')}`);

  const sheets = [
    { name: 'Invoices - ML UK', currency: 'GBP' },
    { name: 'Invoices - ML AU', currency: 'AUD' },
  ];

  let totalInserted = 0, totalSkipped = 0;
  const allUnmatchedClients = new Set();
  const allUnmatchedCandidates = new Set();

  for (const s of sheets) {
    const stats = await processSheet(workbook, s.name, s.currency);
    totalInserted += stats.inserted;
    totalSkipped += stats.skipped;
    stats.unmatchedClients?.forEach(c => allUnmatchedClients.add(c));
    stats.unmatchedCandidates?.forEach(c => allUnmatchedCandidates.add(c));

    console.log(`\n  ✅ ${s.name}:`);
    console.log(`     Inserted: ${stats.inserted} | Skipped: ${stats.skipped} | Errors: ${stats.errors}`);
    console.log(`     Clients matched: ${stats.matchedClients} | Candidates matched: ${stats.matchedCandidates}`);
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(`Total inserted: ${totalInserted} | Total skipped: ${totalSkipped}`);

  if (allUnmatchedClients.size > 0) {
    console.log(`\n⚠️  Unmatched clients (${allUnmatchedClients.size}):`);
    [...allUnmatchedClients].slice(0, 30).forEach(c => console.log(`   - ${c}`));
  }
  if (allUnmatchedCandidates.size > 0) {
    console.log(`\n⚠️  Unmatched candidates (${allUnmatchedCandidates.size}):`);
    [...allUnmatchedCandidates].slice(0, 30).forEach(c => console.log(`   - ${c}`));
  }

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
