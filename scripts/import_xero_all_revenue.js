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
// DATE CONVERSION
// ============================================================================

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
// CATEGORIZATION
// ============================================================================

function categorizeInvoice(reference, description) {
  if (!reference) return { category: 'unknown', role: null };
  
  const text = (reference + ' ' + (description || '')).toLowerCase();
  
  // PLACEMENTS
  if (/placement:/i.test(reference) || 
      /completion fee/i.test(reference) ||
      /final stage/i.test(text) ||
      /final payment/i.test(text)) {
    return {
      category: 'placement',
      role: extractRole(reference)
    };
  }
  
  // RETAINERS
  if (/first stage|second stage|#\d+ search fee|stage \d+|retainer #\d+/i.test(reference)) {
    return {
      category: 'retainer',
      role: extractRole(reference)
    };
  }
  
  // PROJECTS
  if (/mapping|research project|technology scan|project fee|support -/i.test(text)) {
    return {
      category: 'project',
      role: extractRole(reference) || 'Project Work'
    };
  }
  
  // CREDITS (skip these)
  if (/credit|cancellation/i.test(text) || reference.startsWith('CN-')) {
    return {
      category: 'skip',
      role: null
    };
  }
  
  // If it looks like a role title, assume placement
  const rolePatterns = [
    /\b(ceo|cfo|coo|cto|cmo|cpo|chief)\b/i,
    /\b(vp|vice president)\b/i,
    /\bhead of\b/i,
    /\bdirector\b/i,
    /\b(general )?manager\b/i,
    /\bpartner\b/i,
    /\b(i)?ned\b/i,
  ];
  
  if (rolePatterns.some(p => p.test(reference))) {
    return {
      category: 'placement',
      role: extractRole(reference)
    };
  }
  
  return { category: 'unknown', role: null };
}

function extractRole(reference) {
  if (!reference) return null;
  
  // Remove common prefixes
  let role = reference
    .replace(/^(Placement:|Completion Fee -|Final Stage:|Support -|First Stage Fee -|Second Stage Fee -|#\d+ search fee -)/i, '')
    .trim();
  
  return role || null;
}

function determineRoleLevel(roleTitle) {
  if (!roleTitle) return null;
  const title = roleTitle.toLowerCase();
  
  if (/\b(ceo|cfo|coo|cto|cmo|cpo|chief|president)\b/.test(title)) return 'C-level';
  if (/\b(vp|vice president|svp|evp)\b/.test(title)) return 'VP';
  if (/\b(director|head|gm|general manager)\b/.test(title)) return 'Director';
  if (/\b(manager|lead)\b/.test(title)) return 'Manager';
  if (/\b(senior|sr)\b/.test(title)) return 'Senior';
  
  return 'Mid';
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
  
  const invoices = new Map();
  
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

async function importAllRevenue(csvFiles) {
  console.log('💰 XERO COMPREHENSIVE REVENUE IMPORT');
  console.log('═'.repeat(70));
  console.log(`Files to process: ${csvFiles.length}`);
  console.log('');
  
  const stats = {
    totalInvoices: 0,
    placements: 0,
    retainers: 0,
    projects: 0,
    skipped: 0,
    errors: 0,
    clients: new Set(),
    consultants: new Set(),
    totalRevenue: 0,
    placementRevenue: 0,
    retainerRevenue: 0,
    projectRevenue: 0
  };
  
  const defaultConsultant = await getDefaultConsultant();
  console.log(`Default consultant: ${defaultConsultant.name}\n`);
  
  for (const csvFile of csvFiles) {
    const invoices = await processCSV(csvFile);
    stats.totalInvoices += invoices.length;
    
    for (const invoice of invoices) {
      try {
        // Skip zero or negative amounts (credits handled separately)
        if (invoice.total <= 0) {
          stats.skipped++;
          continue;
        }
        
        // Categorize
        const { category, role } = categorizeInvoice(invoice.reference, invoice.description);
        
        if (category === 'skip' || category === 'unknown') {
          stats.skipped++;
          continue;
        }
        
        if (!role) {
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
        const roleLevel = determineRoleLevel(role);
        
        // Payment status
        const paymentStatus = invoice.status?.toLowerCase().includes('paid') ? 'paid' : 'pending';
        
        // Create entry
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
            fee_category,
            metadata
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10, 'xero', $11, $12, $13)
          ON CONFLICT (xero_invoice_id) DO NOTHING
        `, [
          client.id,
          consultant.id,
          role,
          roleLevel,
          invoice.total,
          invoice.currency,
          invoice.invoice_number,
          invoice.date,
          paymentStatus,
          paymentStatus === 'paid' ? invoice.date : null,
          invoice.invoice_number,
          category,
          JSON.stringify({
            original_reference: invoice.reference,
            consultant_name: invoice.consultant,
            category: category,
            imported_at: new Date().toISOString()
          })
        ]);
        
        // Update stats
        stats.totalRevenue += invoice.total;
        if (category === 'placement') {
          stats.placements++;
          stats.placementRevenue += invoice.total;
          console.log(`✅ ${client.name} - ${role} ($${invoice.total.toLocaleString()}) [PLACEMENT]`);
        } else if (category === 'retainer') {
          stats.retainers++;
          stats.retainerRevenue += invoice.total;
          console.log(`💼 ${client.name} - ${role} ($${invoice.total.toLocaleString()}) [RETAINER]`);
        } else if (category === 'project') {
          stats.projects++;
          stats.projectRevenue += invoice.total;
          console.log(`📋 ${client.name} - ${role} ($${invoice.total.toLocaleString()}) [PROJECT]`);
        }
        
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
  console.log(`✅ Placements imported:   ${stats.placements} ($${stats.placementRevenue.toLocaleString()})`);
  console.log(`💼 Retainers imported:    ${stats.retainers} ($${stats.retainerRevenue.toLocaleString()})`);
  console.log(`📋 Projects imported:     ${stats.projects} ($${stats.projectRevenue.toLocaleString()})`);
  console.log(`⏭️  Skipped:               ${stats.skipped}`);
  console.log(`❌ Errors:                ${stats.errors}`);
  console.log(`💰 TOTAL REVENUE:         $${stats.totalRevenue.toLocaleString()}`);
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
      SUM(pl.placement_fee) as total_invoiced,
      SUM(CASE WHEN pl.payment_status = 'paid' THEN pl.placement_fee ELSE 0 END) as total_paid,
      SUM(CASE WHEN pl.payment_status IN ('pending', 'overdue') THEN pl.placement_fee ELSE 0 END) as total_outstanding,
      COUNT(*) FILTER (WHERE pl.fee_category = 'placement') as total_placements,
      SUM(CASE WHEN pl.still_employed AND pl.fee_category = 'placement' THEN 1 ELSE 0 END) as active_placements,
      AVG(pl.placement_fee) FILTER (WHERE pl.fee_category = 'placement') as average_placement_fee,
      MAX(pl.placement_fee) FILTER (WHERE pl.fee_category = 'placement') as highest_placement_fee,
      MIN(pl.placement_fee) FILTER (WHERE pl.fee_category = 'placement') as lowest_placement_fee,
      MIN(pl.start_date) FILTER (WHERE pl.fee_category = 'placement') as first_placement_date,
      MAX(pl.start_date) FILTER (WHERE pl.fee_category = 'placement') as last_placement_date,
      CASE WHEN COUNT(*) = 0 THEN 0 ELSE 
        ROUND(1.0 * SUM(CASE WHEN pl.payment_status = 'paid' THEN 1 ELSE 0 END) / COUNT(*), 2)
      END as payment_reliability,
      NOW() as computed_at
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
    console.error('Usage: node import_xero_all_revenue.js <file1.csv> [file2.csv] ...');
    console.error('Example: node import_xero_all_revenue.js ~/Downloads/Xero*.csv');
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
    await importAllRevenue(csvFiles);
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

module.exports = { importAllRevenue };