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
// CANDIDATE NAME EXTRACTION
// ============================================================================

function extractCandidateName(description) {
  if (!description) return null;
  
  const patterns = [
    / - ([A-Z][a-z]+(?:-[A-Z][a-z]+)? [A-Z][a-z]+(?:-[A-Z][a-z]+)?)$/,
    /^([A-Z][a-z]+(?:-[A-Z][a-z]+)? [A-Z][a-z]+(?:-[A-Z][a-z]+)?) - /,
    /(?:Placement|Search|Fee).*?([A-Z][a-z]+(?:-[A-Z][a-z]+)? [A-Z][a-z]+(?:-[A-Z][a-z]+)?)/,
    /(?:For|Candidate):\s*([A-Z][a-z]+(?:-[A-Z][a-z]+)? [A-Z][a-z]+(?:-[A-Z][a-z]+)?)/i,
  ];
  
  for (const pattern of patterns) {
    const match = description.match(pattern);
    if (match && match[1]) {
      const name = match[1].trim();
      const words = name.split(/\s+/);
      if (words.length >= 2 && words.length <= 3) {
        return name;
      }
    }
  }
  
  return null;
}

function extractRoleTitle(description) {
  if (!description) return 'Unknown Role';
  
  const titlePatterns = [
    /(?:VP|Vice President) (?:of\s+)?([A-Za-z\s]+?)(?:\s+-|\s+Search|\s+Placement|$)/i,
    /(?:Chief|Head|Director) (?:of\s+)?([A-Za-z\s]+?)(?:\s+-|\s+Search|\s+Placement|$)/i,
    /(CEO|CFO|COO|CTO|CMO|CPO)(?:\s+-|\s+Search|\s+Placement|$)/i,
  ];
  
  for (const pattern of titlePatterns) {
    const match = description.match(pattern);
    if (match) {
      return match[0].replace(/\s+-.*$/, '').replace(/\s+(Search|Placement|Fee)$/i, '').trim();
    }
  }
  
  const fallback = description.split(/\s+-/)[0].split(/Search|Placement|Fee/i)[0].trim();
  return fallback || 'Unknown Role';
}

function determineRoleLevel(roleTitle) {
  const title = roleTitle.toLowerCase();
  
  if (/\b(ceo|cfo|coo|cto|cmo|cpo|chief|president)\b/.test(title)) {
    return 'C-level';
  }
  if (/\b(vp|vice president|svp|evp)\b/.test(title)) {
    return 'VP';
  }
  if (/\b(director|head|lead)\b/.test(title)) {
    return 'Director';
  }
  if (/\b(manager|principal)\b/.test(title)) {
    return 'Manager';
  }
  if (/\b(senior|sr)\b/.test(title)) {
    return 'Senior';
  }
  
  return 'Mid';
}

// ============================================================================
// DATABASE HELPERS
// ============================================================================

async function findPersonByName(name) {
  let result = await pool.query(
    `SELECT id, full_name FROM people WHERE LOWER(full_name) = LOWER($1) LIMIT 1`,
    [name]
  );
  
  if (result.rows.length > 0) {
    return result.rows[0];
  }
  
  const parts = name.split(/\s+/);
  if (parts.length >= 2) {
    const firstName = parts[0];
    const lastName = parts[parts.length - 1];
    
    result = await pool.query(
      `SELECT id, full_name FROM people 
       WHERE LOWER(full_name) LIKE LOWER($1) 
       OR LOWER(full_name) LIKE LOWER($2)
       LIMIT 1`,
      [`%${firstName}%${lastName}%`, `%${lastName}%${firstName}%`]
    );
    
    if (result.rows.length > 0) {
      return result.rows[0];
    }
  }
  
  return null;
}

async function findOrCreateClient(clientName) {
  if (!clientName) {
    throw new Error('Client name is required');
  }
  
  let result = await pool.query(
    `SELECT id, name FROM clients WHERE LOWER(name) = LOWER($1) LIMIT 1`,
    [clientName.trim()]
  );
  
  if (result.rows.length > 0) {
    return result.rows[0];
  }
  
  result = await pool.query(
    `INSERT INTO clients (name, type, status)
     VALUES ($1, 'direct', 'active')
     RETURNING id, name`,
    [clientName.trim()]
  );
  
  console.log(`  🆕 Created new client: ${clientName}`);
  return result.rows[0];
}

async function getDefaultConsultant() {
  const result = await pool.query(
    `SELECT id, full_name FROM users 
     WHERE role IN ('admin', 'consultant')
     ORDER BY created_at ASC
     LIMIT 1`
  );
  
  if (result.rows.length === 0) {
    throw new Error('No consultant users found in database');
  }
  
  return result.rows[0];
}

// ============================================================================
// XERO INVOICE INGESTION
// ============================================================================

async function ingestXeroInvoices(csvFilePath) {
  console.log('💰 XERO INVOICE INGESTION');
  console.log('═'.repeat(60));
  console.log(`📄 File: ${csvFilePath}\n`);
  
  if (!fs.existsSync(csvFilePath)) {
    throw new Error(`File not found: ${csvFilePath}`);
  }
  
  const invoices = [];
  
  await new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        invoices.push({
          invoice_number: row['Invoice Number'] || row['InvoiceNumber'] || row['Number'],
          date: row['Date'] || row['InvoiceDate'],
          contact: row['Contact'] || row['ContactName'] || row['Client'],
          description: row['Description'] || row['Subject'] || row['Reference'],
          amount: parseFloat((row['Amount'] || row['Total'] || '0').replace(/[^0-9.-]/g, '')),
          status: row['Status'] || row['InvoiceStatus'] || 'UNKNOWN',
          project: row['Project'] || row['ProjectName'] || '',
          currency: row['Currency'] || 'USD'
        });
      })
      .on('end', resolve)
      .on('error', reject);
  });
  
  console.log(`📊 Found ${invoices.length} invoices\n`);
  
  const stats = {
    processed: 0,
    created: 0,
    updated: 0,
    skipped: 0,
    errors: 0,
    noCandidateName: 0,
    candidateNotFound: 0
  };
  
  const defaultConsultant = await getDefaultConsultant();
  console.log(`👤 Default consultant: ${defaultConsultant.full_name}\n`);
  
  for (const invoice of invoices) {
    stats.processed++;
    
    try {
      if (!invoice.amount || invoice.amount <= 0) {
        stats.skipped++;
        console.log(`⏭️  Skipped (no amount): ${invoice.invoice_number}`);
        continue;
      }
      
      if (!invoice.description) {
        stats.skipped++;
        console.log(`⏭️  Skipped (no description): ${invoice.invoice_number}`);
        continue;
      }
      
      const candidateName = extractCandidateName(invoice.description);
      if (!candidateName) {
        stats.noCandidateName++;
        console.log(`⚠️  No candidate name found: "${invoice.description}"`);
        continue;
      }
      
      const person = await findPersonByName(candidateName);
      if (!person) {
        stats.candidateNotFound++;
        console.log(`⚠️  Candidate not found: ${candidateName}`);
        continue;
      }
      
      const client = await findOrCreateClient(invoice.contact);
      const roleTitle = extractRoleTitle(invoice.description);
      const roleLevel = determineRoleLevel(roleTitle);
      
      const paymentStatus = invoice.status.toLowerCase().includes('paid') ? 'paid' :
                           invoice.status.toLowerCase().includes('partial') ? 'partial' :
                           invoice.status.toLowerCase().includes('void') ? 'written_off' :
                           invoice.status.toLowerCase().includes('overdue') ? 'overdue' : 'pending';
      
      const result = await pool.query(`
        INSERT INTO placements (
          person_id, client_id, role_title, role_level,
          placement_fee, currency, invoice_number, invoice_date,
          payment_status, payment_date, start_date,
          placed_by_user_id, source, xero_invoice_id,
          metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'xero', $13, $14)
        ON CONFLICT (xero_invoice_id) DO UPDATE SET
          payment_status = EXCLUDED.payment_status,
          payment_date = EXCLUDED.payment_date,
          placement_fee = EXCLUDED.placement_fee,
          updated_at = NOW()
        RETURNING id, (xmax = 0) as is_new
      `, [
        person.id,
        client.id,
        roleTitle,
        roleLevel,
        invoice.amount,
        invoice.currency,
        invoice.invoice_number,
        invoice.date,
        paymentStatus,
        paymentStatus === 'paid' ? invoice.date : null,
        invoice.date,
        defaultConsultant.id,
        invoice.invoice_number,
        JSON.stringify({
          original_description: invoice.description,
          project: invoice.project,
          imported_from: 'xero_csv',
          imported_at: new Date().toISOString()
        })
      ]);
      
      const isNew = result.rows[0].is_new;
      if (isNew) {
        stats.created++;
        console.log(`✅ ${person.full_name} → ${client.name} ($${invoice.amount.toLocaleString()}) [${roleTitle}]`);
      } else {
        stats.updated++;
        console.log(`🔄 Updated: ${person.full_name} → ${client.name}`);
      }
      
    } catch (error) {
      stats.errors++;
      console.error(`❌ Error processing invoice ${invoice.invoice_number}:`, error.message);
    }
  }
  
  console.log('\n' + '═'.repeat(60));
  console.log('📊 INGESTION SUMMARY');
  console.log('═'.repeat(60));
  console.log(`Total invoices:          ${stats.processed}`);
  console.log(`✅ Created placements:   ${stats.created}`);
  console.log(`🔄 Updated placements:   ${stats.updated}`);
  console.log(`⏭️  Skipped:              ${stats.skipped}`);
  console.log(`⚠️  No candidate name:   ${stats.noCandidateName}`);
  console.log(`⚠️  Candidate not found: ${stats.candidateNotFound}`);
  console.log(`❌ Errors:               ${stats.errors}`);
  console.log('═'.repeat(60));
  
  console.log('\n🔄 Updating client financials...');
  await updateClientFinancials();
  console.log('✅ Client financials updated\n');
}

async function updateClientFinancials() {
  await pool.query(`
    INSERT INTO client_financials (
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
      SUM(CASE WHEN pl.payment_status IN ('pending', 'partial', 'overdue') THEN pl.placement_fee ELSE 0 END) as total_outstanding,
      COUNT(*) as total_placements,
      SUM(CASE WHEN pl.still_employed THEN 1 ELSE 0 END) as active_placements,
      AVG(pl.placement_fee) as average_placement_fee,
      MAX(pl.placement_fee) as highest_placement_fee,
      MIN(pl.placement_fee) as lowest_placement_fee,
      MIN(pl.start_date) as first_placement_date,
      MAX(pl.start_date) as last_placement_date,
      CASE 
        WHEN COUNT(*) = 0 THEN 0
        ELSE ROUND(1.0 * SUM(CASE WHEN pl.payment_status = 'paid' THEN 1 ELSE 0 END) / COUNT(*), 2)
      END as payment_reliability,
      NOW() as computed_at
    FROM placements pl
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
    console.error('Usage: node ingest_xero_invoices.js <path-to-invoices.csv>');
    console.error('Example: node ingest_xero_invoices.js ~/Downloads/Xero_Invoices_2024.csv');
    process.exit(1);
  }
  
  const csvPath = path.resolve(args[0]);
  
  try {
    await ingestXeroInvoices(csvPath);
    console.log('✅ Done!');
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

module.exports = {
  ingestXeroInvoices,
  extractCandidateName,
  extractRoleTitle,
  determineRoleLevel,
  findPersonByName,
  findOrCreateClient,
  updateClientFinancials
};