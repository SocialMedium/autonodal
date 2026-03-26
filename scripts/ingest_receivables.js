#!/usr/bin/env node
// ============================================================================
// Ingest Receivables Sheet from Global Billings & WIP workbook
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
  const fs = require('fs');
  const sqlPath = path.join(__dirname, '..', 'sql', 'migration_wip_workbook.sql');
  if (!fs.existsSync(sqlPath)) return;
  const sql = fs.readFileSync(sqlPath, 'utf8');
  const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 5 && !s.startsWith('--'));
  for (const stmt of statements) {
    try { await pool.query(stmt); } catch (e) { /* already applied */ }
  }
  console.log('  ✅ Schema migration applied');
}

function parseExcelDate(val) {
  if (!val || val === 'NaN' || val === '') return null;
  if (typeof val === 'number') {
    const d = new Date((val - 25569) * 86400 * 1000);
    return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
  }
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
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

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log(' Receivables Ingestion — Global Billings & WIP');
  console.log(DRY_RUN ? ' 🔍 DRY RUN — no data will be written' : ' 💾 LIVE RUN');
  console.log('═══════════════════════════════════════════════════════════');

  await ensureSchema();

  const workbook = XLSX.readFile(FILE_PATH);

  // Try common sheet names for receivables
  const sheetName = workbook.SheetNames.find(s =>
    /receivable|debtor|outstanding|aged/i.test(s)
  );
  if (!sheetName) {
    console.log('⚠️  No receivables sheet found. Available sheets:');
    workbook.SheetNames.forEach(s => console.log(`   - ${s}`));
    await pool.end();
    return;
  }

  const sheet = workbook.Sheets[sheetName];
  const allRows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

  // Header at row 3 (0-indexed), data from row 4
  const headers = allRows[3] || allRows[2] || allRows[1] || [];
  const dataRows = allRows.slice(4);

  console.log(`\n📄 Sheet: ${sheetName} (${dataRows.length} rows)`);
  console.log(`   Headers: ${headers.join(' | ')}`);

  const stats = { inserted: 0, skipped: 0, overdue90: 0, errors: 0 };

  for (const row of dataRows) {
    const invoiceNumber = String(row[0] || '').trim();
    if (!invoiceNumber || invoiceNumber === 'NaN' || invoiceNumber === '') continue;

    const clientName = String(row[1] || '').trim();
    const invoiceDate = parseExcelDate(row[2]);
    const dueDate = parseExcelDate(row[3]);
    const invoiceTotal = parseFloat(row[4]) || 0;
    const status = String(row[5] || '').trim();
    const daysOverdue = parseInt(row[6]) || 0;
    const notes = String(row[7] || '').trim();
    const action = String(row[8] || '').trim();

    if (daysOverdue > 90) {
      stats.overdue90++;
      console.log(`   ⚠️  90+ days overdue: ${clientName} — ${invoiceNumber} — $${invoiceTotal} (${daysOverdue} days)`);
    }

    if (DRY_RUN) { stats.inserted++; continue; }

    try {
      const companyId = await matchCompany(clientName);

      // Check for existing
      const { rows: existing } = await pool.query(
        `SELECT id FROM receivables WHERE invoice_number = $1 LIMIT 1`,
        [invoiceNumber]
      );
      if (existing.length) { stats.skipped++; continue; }

      await pool.query(`
        INSERT INTO receivables (
          invoice_number, client_name, company_id, invoice_date, due_date,
          invoice_total, status, days_overdue, notes, action, tenant_id
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      `, [
        invoiceNumber, clientName, companyId, invoiceDate, dueDate,
        invoiceTotal, status || null, daysOverdue, notes || null, action || null,
        TENANT_ID
      ]);
      stats.inserted++;
    } catch (e) {
      stats.errors++;
      if (stats.errors <= 5) console.error(`   ❌ ${invoiceNumber}: ${e.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(` Inserted: ${stats.inserted} | Skipped: ${stats.skipped} | Errors: ${stats.errors}`);
  console.log(` Invoices 90+ days overdue: ${stats.overdue90}`);

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
