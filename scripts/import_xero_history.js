#!/usr/bin/env node

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

// ============================================================================
// ROLE EXTRACTION FROM REFERENCE FIELD
// ============================================================================

function extractRoleFromReference(reference) {
  if (!reference) return null;
  
  // Remove common prefixes
  let role = reference
    .replace(/^(Placement:|Completion Fee -|Final Stage:|Support -)/i, '')
    .trim();
  
  // If it's just a fee description with no role, skip it
  if (/^(First Stage Fee|Second Stage Fee|#\d+ search fee|Cancellation Fee)/i.test(role)) {
    return null;
  }
  
  return role || null;
}

function determineRoleLevel(roleTitle) {
  if (!roleTitle) return 'Mid';
  const title = roleTitle.toLowerCase();
  
  if (/\b(ceo|cfo|coo|cto|cmo|cpo|chief|president)\b/.test(title)) return 'C-level';
  if (/\b(vp|vice president|svp|evp)\b/.test(title)) return 'VP';
  if (/\b(director|head|gm|general manager)\b/.test(title)) return 'Director';
  if (/\b(manager|lead)\b/.test(title)) return 'Manager';
  if (/\b(senior|sr)\b/.test(title)) return 'Senior';
  
  return 'Mid';
}

function convertDate(dateStr) {
  if (!dateStr) return null;
  
  // Convert DD/MM/YYYY to YYYY-MM-DD
  const parts = dateStr.split('/');
  if (parts.length === 3) {
    const [day, month, year] = parts;
    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
  }
  
  return dateStr;
}

// ============================================================================
// INVOICE FILTERING
// ============================================================================

function isPlacementInvoice(reference, description) {
  if (!reference) return false;
  
  // Include these patterns
  const includePatterns = [
    /placement:/i,
    /completion fee/i,
    /final stage/i,
  ];
  
  // Exclude these patterns (retainers, project fees, credits)
  const excludePatterns = [
    /first stage/i,
    /second stage/i,
    /retainer/i,
    /credit/i,
    /cancellation/i,
    /mapping/i,
    /research project/i,
    /^#\d+ search fee/i,
  ];
  
  const text = (reference + ' ' + (description || '')).toLowerCase();
  
  // Must match at least one include pattern
  const hasInclude = includePatterns.some(p => p.test(text));
  
  // Must not match any exclude pattern
  const hasExclude = excludePatterns.some(p => p.test(text));
  
  return hasInclude && !hasExclude;
}

// ============================================================================
// DATABASE HELPERS
// ============================================================================

async function findOrCreateClient(clientName) {
  if (!clientName) throw new Error('Client name is required');
  
  let result = await pool.query(
    `SELECT id, name FROM accounts WHERE LOWER(name) = LOWER($1) LIMIT 1`,
    [clientName.trim()]
  );
  
  if (result.rows.length > 0) {
    return result.rows[0];
  }
  
  result = await pool.query(
    `INSERT INTO accounts (name, relationship_status) VALUES ($1, 'active') RETURNING id, name`,
    [clientName.trim()]
  );
  
  return result.rows[0];
}

async function findConsultantByName(consultantName) {
  if (!consultantName) return null;
  
  const result = await pool.query(
    `SELECT id, name FROM users 
     WHERE LOWER(name) LIKE LOWER($1)
     LIMIT 1`,
    [`%${consultantName}%`]
  );
  
  return result.rows.length > 0 ? result.rows[0] : null;
}

async function getDefaultConsultant() {
  const result = await pool.query(
    `SELECT id, name FROM users ORDER BY created_at ASC LIMIT 1`
  );
  
  if (result.rows.length === 0) {
    throw new Error('No consultant users found');
  }
  
  return result.rows[0];
}

// ============================================================================
// CSV PROCESSING
// ============================================================================

async function processCSV(csvFilePath) {
  console.log(`📄 Processing: ${path.basename(csvFilePath)}`);
  
  const invoices = new Map(); // Group by invoice number
  
  await new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const invoiceNumber = row['InvoiceNumber'];
        
        if (!invoices.has(invoiceNumber)) {
          invoices.set(invoiceNumber, {
            invoice_number: invoiceNumber,
            reference: row['Reference'],
            contact: row['ContactName'],
            date: convertDate(row['InvoiceDate']),
            total: parseFloat((row['Total'] || '0').replace(/[^0-9.-]/g, '')),
            status: row['Status'],
            consultant: row['TrackingOption1'],
            description: row['Description'],
            currency: row['Currency'] || 'AUD'
          });
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });
  
  return Array.from(invoices.values());
}

// ============================================================================
// IMPORT LOGIC
// ============================================================================

async function importXeroHistory(csvFiles) {
  console.log('💰 XERO HISTORICAL IMPORT');
  console.log('═'.repeat(70));
  console.log(`Files to process: ${csvFiles.length}`);
  console.log('');
  
  const stats = {
    totalInvoices: 0,
    placements: 0,
    skipped: 0,
    errors: 0,
    clients: new Set(),
    consultants: new Set()
  };
  
  const defaultConsultant = await getDefaultConsultant();
  console.log(`Default consultant: ${defaultConsultant.name}\n`);
  
  for (const csvFile of csvFiles) {
    const invoices = await processCSV(csvFile);
    stats.totalInvoices += invoices.length;
    
    for (const invoice of invoices) {
      try {
        // Filter for placement invoices only
        if (!isPlacementInvoice(invoice.reference, invoice.description)) {
          stats.skipped++;
          continue;
        }
        
        // Extract role
        const roleTitle = extractRoleFromReference(invoice.reference);
        if (!roleTitle) {
          stats.skipped++;
          console.log(`⏭️  Skipped (no role): ${invoice.reference}`);
          continue;
        }
        
        // Find or create client
        const client = await findOrCreateClient(invoice.contact);
        stats.clients.add(client.name);
        
        // Find consultant
        let consultant = null;
        if (invoice.consultant) {
          consultant = await findConsultantByName(invoice.consultant);
          if (consultant) {
            stats.consultants.add(consultant.name);
          }
        }
        consultant = consultant || defaultConsultant;
        
        // Determine role level
        const roleLevel = determineRoleLevel(roleTitle);
        
        // Payment status
        const paymentStatus = invoice.status?.toLowerCase().includes('paid') ? 'paid' : 'pending';
        
        // Create placement
        await pool.query(`
          INSERT INTO conversions (
            client_id,
            placed_by_user_id,
            role_title,
            role_level,
            placement_fee,
            currency,
            invoice_number,
            invoice_date,
            start_date,
            payment_status,
            payment_date,
            source,
            xero_invoice_id,
            metadata
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10, 'xero', $11, $12)
          ON CONFLICT (xero_invoice_id) DO NOTHING
        `, [
          client.id,
          consultant.id,
          roleTitle,
          roleLevel,
          invoice.total,
          invoice.currency,
          invoice.invoice_number,
          invoice.date,
          paymentStatus,
          paymentStatus === 'paid' ? invoice.date : null,
          invoice.invoice_number,
          JSON.stringify({
            original_reference: invoice.reference,
            consultant_name: invoice.consultant,
            imported_at: new Date().toISOString()
          })
        ]);
        
        stats.placements++;
        console.log(`✅ ${client.name} - ${roleTitle} ($${invoice.total.toLocaleString()})`);
        
      } catch (error) {
        stats.errors++;
        console.error(`❌ Error: ${invoice.invoice_number} - ${error.message}`);
      }
    }
  }
  
  console.log('\n' + '═'.repeat(70));
  console.log('📊 IMPORT SUMMARY');
  console.log('═'.repeat(70));
  console.log(`Total invoices processed: ${stats.totalInvoices}`);
  console.log(`✅ Placements imported:   ${stats.placements}`);
  console.log(`⏭️  Skipped (non-placement): ${stats.skipped}`);
  console.log(`❌ Errors:                ${stats.errors}`);
  console.log(`🏢 Unique clients:        ${stats.clients.size}`);
  console.log(`👥 Consultants found:     ${stats.consultants.size}`);
  console.log('═'.repeat(70));
  
  // Update client financials
  console.log('\n🔄 Updating client financials...');
  await updateClientFinancials();
  console.log('✅ Client financials updated\n');
}

async function updateClientFinancials() {
  await pool.query(`
    INSERT INTO account_financials (
      client_id, total_invoiced, total_paid, total_outstanding,
      total_placements, active_placements,
      average_placement_fee, highest_placement_fee, lowest_placement_fee,
      first_placement_date, last_placement_date,
      payment_reliability, computed_at
    )
    SELECT 
      pl.client_id,
      SUM(pl.placement_fee),
      SUM(CASE WHEN pl.payment_status = 'paid' THEN pl.placement_fee ELSE 0 END),
      SUM(CASE WHEN pl.payment_status IN ('pending', 'overdue') THEN pl.placement_fee ELSE 0 END),
      COUNT(*),
      SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END),
      AVG(pl.placement_fee),
      MAX(pl.placement_fee),
      MIN(pl.placement_fee),
      MIN(pl.start_date),
      MAX(pl.start_date),
      CASE WHEN COUNT(*) = 0 THEN 0 ELSE 
        ROUND(1.0 * SUM(CASE WHEN pl.payment_status = 'paid' THEN 1 ELSE 0 END) / COUNT(*), 2)
      END,
      NOW()
    FROM conversions pl
    GROUP BY pl.client_id
    ON CONFLICT (client_id) DO UPDATE SET
      total_invoiced = EXCLUDED.total_invoiced,
      total_paid = EXCLUDED.total_paid,
      total_outstanding = EXCLUDED.total_outstanding,
      total_placements = EXCLUDED.total_placements,
      active_placements = EXCLUDED.active_placements,
      average_placement_fee = EXCLUDED.average_placement_fee,
      highest_placement_fee = EXCLUDED.highest_placement_fee,
      lowest_placement_fee = EXCLUDED.lowest_placement_fee,
      first_placement_date = EXCLUDED.first_placement_date,
      last_placement_date = EXCLUDED.last_placement_date,
      payment_reliability = EXCLUDED.payment_reliability,
      computed_at = EXCLUDED.computed_at
  `);
}

// ============================================================================
// CLI
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.error('Usage: node import_xero_history.js <file1.csv> [file2.csv] ...');
    console.error('Example: node import_xero_history.js ~/Downloads/Xero*.csv');
    process.exit(1);
  }
  
  const csvFiles = args.map(f => path.resolve(f));
  
  // Verify all files exist
  for (const file of csvFiles) {
    if (!fs.existsSync(file)) {
      console.error(`❌ File not found: ${file}`);
      process.exit(1);
    }
  }
  
  try {
    await importXeroHistory(csvFiles);
    console.log('✅ Import complete!');
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main();
}

module.exports = { importXeroHistory };