#!/usr/bin/env node
// ============================================================================
// Ingest Consultant WIP Sheets from Global Billings & WIP workbook
// 25 sheets, one per consultant — ~900 placement/opportunity records
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

const WIP_SHEETS = [
  'Matt', 'JT', 'Illona', 'Rob', 'SYD-JV', 'Andrew', 'Sam', 'Mark',
  'Jamie G', 'Michael D', 'Solly', 'James', 'Claire', 'Jimmy', 'Rachel',
  'Priyanka', 'David', 'Conny', 'Lexi', 'Richard', 'Ananya', 'Megan',
  'Yoko', 'Timo', 'MLG-UK'
];

// Skip rows that are summary/total rows
const SKIP_PATTERNS = /^(invoiced|committed|forecast|total pipe|grand total|subtotal|\s*$|nan)/i;

// Company match cache
const companyCache = new Map();
const userCache = new Map();

async function matchCompany(name) {
  if (!name || SKIP_PATTERNS.test(name)) return null;
  const key = name.toLowerCase().trim();
  if (companyCache.has(key)) return companyCache.get(key);
  const clean = name.replace(/\s+(Pty|Ltd|Limited|Inc|Corp|plc|AG)\b\.?/gi, '').trim();
  const { rows } = await pool.query(
    `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
    [`%${clean}%`, TENANT_ID]
  );
  const id = rows[0]?.id || null;
  companyCache.set(key, id);
  return id;
}

async function matchUser(name) {
  if (!name || name === 'NaN') return null;
  const key = name.toLowerCase().trim();
  if (userCache.has(key)) return userCache.get(key);
  const { rows } = await pool.query(
    `SELECT id FROM users WHERE name ILIKE $1 LIMIT 1`,
    [`%${name.trim()}%`]
  );
  const id = rows[0]?.id || null;
  userCache.set(key, id);
  return id;
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

function isDateHeader(val) {
  if (!val) return false;
  if (typeof val === 'number' && val > 30000 && val < 60000) return true; // Excel serial date
  if (typeof val === 'string') {
    // Match "Jul-19", "Aug-20", "Jan 2024", etc.
    return /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/i.test(val.trim());
  }
  return false;
}

function parseMonthlyValue(val) {
  if (!val || val === '' || val === 'NaN' || String(val).includes('#REF')) return 0;
  const n = parseFloat(val);
  return isNaN(n) ? 0 : n;
}

async function processSheet(workbook, sheetName) {
  const sheet = workbook.Sheets[sheetName];
  if (!sheet) { console.log(`  ⚠️  Sheet "${sheetName}" not found`); return null; }

  const allRows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
  if (allRows.length < 6) { console.log(`  ⚠️  Sheet "${sheetName}" too short (${allRows.length} rows)`); return null; }

  // Auto-detect header row: find row containing "Opportunity" or "Client" in col 0
  let headerIdx = -1;
  for (let i = 0; i < Math.min(allRows.length, 20); i++) {
    const first = String(allRows[i][0] || '').toLowerCase().trim();
    if (first.includes('opportunity') || (first.includes('client') && !first.includes('committed'))) {
      headerIdx = i;
      break;
    }
  }

  let dataStartIdx;
  let headers;
  if (headerIdx >= 0) {
    headers = allRows[headerIdx];
    dataStartIdx = headerIdx + 1;
  } else {
    // No header row found — data likely starts after summary rows
    // Find first row where col 0 looks like a company name (not a summary keyword)
    for (let i = 4; i < Math.min(allRows.length, 20); i++) {
      const first = String(allRows[i][0] || '').trim();
      if (first && !SKIP_PATTERNS.test(first) && first !== 'NaN' && !first.toLowerCase().includes('invoiced') &&
          !first.toLowerCase().includes('committed') && !first.toLowerCase().includes('forecast') &&
          !first.toLowerCase().includes('total pipe') && !first.toLowerCase().includes('mitchellake') &&
          !first.toLowerCase().includes('work-in-progress') && first.length > 2) {
        dataStartIdx = i;
        break;
      }
    }
    if (!dataStartIdx) { console.log(`  ⚠️  Sheet "${sheetName}" — cannot find data start`); return null; }
    // Use standard column layout
    headers = ['Opportunity / Client', 'Role', 'Consultant', 'Role Salary', 'Total Fee Estimate', 'Currency',
               'Open Date', 'Anticipated Close Date', 'Opportunity Type', 'Current Stage', 'Comments'];
  }

  const dataRows = allRows.slice(dataStartIdx);

  // Find where monthly columns start: first column after index 10 where the header is a date
  let monthStartIdx = -1;
  const headerRow = allRows[headerIdx >= 0 ? headerIdx : (dataStartIdx > 0 ? dataStartIdx - 1 : 0)] || [];
  for (let i = 11; i < headerRow.length; i++) {
    if (isDateHeader(headerRow[i])) { monthStartIdx = i; break; }
  }
  if (monthStartIdx === -1) {
    for (let i = 8; i < headerRow.length; i++) {
      if (isDateHeader(headerRow[i])) { monthStartIdx = i; break; }
    }
  }

  const monthHeaders = monthStartIdx >= 0 ? headerRow.slice(monthStartIdx) : [];

  console.log(`\n  📋 ${sheetName}: ${dataRows.length} data rows, monthly cols from idx ${monthStartIdx} (${monthHeaders.length} months)`);

  const stats = { total: 0, inserted: 0, skipped: 0, dupes: 0, errors: 0,
    unmatchedClients: new Set(), matchedClients: 0 };

  for (const row of dataRows) {
    const clientName = String(row[0] || '').trim();
    if (!clientName || clientName === 'NaN' || SKIP_PATTERNS.test(clientName)) continue;

    const roleTitle = String(row[1] || '').trim();
    const consultantName = String(row[2] || sheetName).trim();
    const roleSalary = String(row[3] || '').trim();
    const feeEstimate = parseFloat(row[4]) || null;
    const currency = String(row[5] || 'AUD').trim().toUpperCase();
    const openDate = parseExcelDate(row[6]);
    const closeDate = parseExcelDate(row[7]);
    const opportunityType = String(row[8] || '').trim();
    const currentStage = String(row[9] || '').trim();
    const comments = String(row[10] || '').trim();

    stats.total++;

    // Parse monthly data
    const monthlyMap = {};
    let totalInvoiced = 0;
    let firstInvoiceDate = null;

    if (monthStartIdx >= 0) {
      for (let i = 0; i < monthHeaders.length; i++) {
        const val = parseMonthlyValue(row[monthStartIdx + i]);
        if (val > 0) {
          const monthKey = typeof monthHeaders[i] === 'number'
            ? new Date((monthHeaders[i] - 25569) * 86400 * 1000).toISOString().slice(0, 7)
            : String(monthHeaders[i]);
          monthlyMap[monthKey] = val;
          totalInvoiced += val;
          if (!firstInvoiceDate) {
            firstInvoiceDate = typeof monthHeaders[i] === 'number'
              ? new Date((monthHeaders[i] - 25569) * 86400 * 1000).toISOString().slice(0, 10)
              : null;
          }
        }
      }
    }

    const placementFee = totalInvoiced > 0 ? totalInvoiced : null;
    const startDate = firstInvoiceDate || closeDate || openDate;

    // Match entities
    const companyId = await matchCompany(clientName);
    const userId = await matchUser(consultantName !== 'NaN' ? consultantName : sheetName);

    if (companyId) stats.matchedClients++;
    else stats.unmatchedClients.add(clientName);

    if (DRY_RUN) {
      stats.inserted++;
      continue;
    }

    try {
      // Dedup check
      const { rows: existing } = await pool.query(
        `SELECT id FROM placements WHERE client_name_raw = $1 AND role_title = $2 AND source_sheet = $3 AND tenant_id = $4 LIMIT 1`,
        [clientName, roleTitle || 'Unknown', sheetName, TENANT_ID]
      );
      if (existing.length) { stats.dupes++; continue; }

      await pool.query(`
        INSERT INTO placements (
          company_id, client_id, placed_by_user_id,
          role_title, start_date, placement_fee, fee_estimate,
          currency, opportunity_type, candidate_salary_raw,
          consultant_name, client_name_raw,
          source, source_sheet, notes, raw_monthly_data,
          payment_status, tenant_id
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
      `, [
        companyId, companyId, userId,
        roleTitle || 'Unknown', startDate, placementFee, feeEstimate,
        currency === 'NAN' ? 'AUD' : (currency || 'AUD'),
        opportunityType || null, roleSalary || null,
        consultantName !== 'NaN' ? consultantName : sheetName,
        clientName, 'wip_workbook', sheetName,
        [currentStage, comments].filter(Boolean).join(' | ') || null,
        Object.keys(monthlyMap).length > 0 ? JSON.stringify(monthlyMap) : null,
        totalInvoiced > 0 ? 'invoiced' : 'pending',
        TENANT_ID
      ]);
      stats.inserted++;
    } catch (e) {
      stats.errors++;
      if (stats.errors <= 5) console.error(`     ❌ ${clientName} / ${roleTitle}: ${e.message}`);
    }
  }

  return stats;
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════');
  console.log(' Consultant WIP Ingestion — Global Billings & WIP');
  console.log(DRY_RUN ? ' 🔍 DRY RUN — no data will be written' : ' 💾 LIVE RUN');
  console.log('═══════════════════════════════════════════════════════════');

  await ensureSchema();

  const workbook = XLSX.readFile(FILE_PATH);
  console.log(`Workbook sheets: ${workbook.SheetNames.join(', ')}\n`);

  // Filter to sheets that actually exist
  const availableSheets = WIP_SHEETS.filter(s => workbook.SheetNames.includes(s));
  const missingSheets = WIP_SHEETS.filter(s => !workbook.SheetNames.includes(s));
  if (missingSheets.length) console.log(`⚠️  Missing sheets: ${missingSheets.join(', ')}\n`);

  let grandTotal = 0, grandInserted = 0, grandSkipped = 0, grandDupes = 0, grandErrors = 0;
  const allUnmatched = new Set();

  for (const sheetName of availableSheets) {
    const stats = await processSheet(workbook, sheetName);
    if (!stats) continue;

    grandTotal += stats.total;
    grandInserted += stats.inserted;
    grandSkipped += stats.skipped;
    grandDupes += stats.dupes;
    grandErrors += stats.errors;
    stats.unmatchedClients.forEach(c => allUnmatched.add(c));

    console.log(`  ✅ ${sheetName}: ${stats.total} rows → ${stats.inserted} inserted, ${stats.dupes} dupes, ${stats.matchedClients} clients matched`);
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(` Total: ${grandTotal} rows processed`);
  console.log(` Inserted: ${grandInserted} | Dupes: ${grandDupes} | Errors: ${grandErrors}`);
  console.log(` Unmatched clients: ${allUnmatched.size}`);

  if (allUnmatched.size > 0) {
    console.log(`\n⚠️  Top unmatched clients (create these in companies table):`);
    [...allUnmatched].sort().slice(0, 40).forEach(c => console.log(`   - ${c}`));
  }

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
