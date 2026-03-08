#!/usr/bin/env node
/**
 * Embed all companies with placement intelligence for semantic search
 * Usage: node scripts/embed_companies.js [--limit N] [--force]
 */

require('dotenv').config();
const db = require('../lib/db');
const qdrant = require('../lib/qdrant');
const { generateEmbeddings } = require('../lib/embeddings');

const BATCH_SIZE = 50;
const COLLECTION_NAME = 'companies';

// ============================================================================
// COMPANY TEXT PREPARATION
// ============================================================================

function prepareCompanyText(company) {
  const parts = [];
  
  // Company name and basic info
  parts.push(`Company: ${company.name}`);
  
  // Placement statistics
  if (company.total_placements) {
    parts.push(`Total placements: ${company.total_placements}`);
  }
  
  if (company.total_invoiced) {
    parts.push(`Total revenue: $${parseFloat(company.total_invoiced).toLocaleString()}`);
  }
  
  if (company.average_placement_fee) {
    parts.push(`Average placement fee: $${parseFloat(company.average_placement_fee).toLocaleString()}`);
  }
  
  // Dates
  if (company.first_placement_date) {
    parts.push(`First placement: ${company.first_placement_date}`);
  }
  
  if (company.last_placement_date) {
    parts.push(`Last placement: ${company.last_placement_date}`);
  }
  
  // Roles placed
  if (company.roles_placed && company.roles_placed.length > 0) {
    const roles = company.roles_placed.filter(r => r).slice(0, 20); // Limit to 20 roles
    parts.push(`Roles placed: ${roles.join(', ')}`);
  }
  
  // Role levels
  if (company.role_levels && company.role_levels.length > 0) {
    const levels = [...new Set(company.role_levels.filter(l => l))];
    parts.push(`Role levels: ${levels.join(', ')}`);
  }
  
  // Relationship tier
  if (company.relationship_tier) {
    parts.push(`Client tier: ${company.relationship_tier}`);
  }
  
  // Fee categories
  if (company.placement_count) {
    parts.push(`Placements: ${company.placement_count}`);
  }
  if (company.retainer_count) {
    parts.push(`Retainers: ${company.retainer_count}`);
  }
  if (company.project_count) {
    parts.push(`Projects: ${company.project_count}`);
  }
  
  return parts.join('\n');
}

// ============================================================================
// MAIN EMBEDDING FUNCTION
// ============================================================================

async function embedCompanies(options = {}) {
  const { limit = null, force = false } = options;
  
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  EMBED COMPANIES - MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Batch size: ${BATCH_SIZE}`);
  if (limit) console.log(`  Limit: ${limit}`);
  if (force) console.log(`  Force: re-embedding all`);
  console.log('');
  
  try {
    // Ensure Qdrant collection exists
    console.log('Checking Qdrant collection...');
    await qdrant.ensureCollection(COLLECTION_NAME, 1536);
    console.log('✓ Collection ready\n');
    
    // Get companies with placement intelligence
    console.log('Fetching companies from database...');
    
    let whereClause = '';
    if (!force) {
      whereClause = `AND (c.id NOT IN (
        SELECT id FROM embeddings 
        WHERE entity_type = 'company' AND entity_id = c.id
      ))`;
    }
    
    const query = `
      SELECT 
        c.id,
        c.name,
        c.relationship_tier,
        cf.total_placements,
        cf.total_invoiced,
        cf.average_placement_fee,
        cf.first_placement_date,
        cf.last_placement_date,
        cf.payment_reliability,
        -- Aggregate roles
        ARRAY_AGG(DISTINCT p.role_title) FILTER (WHERE p.role_title IS NOT NULL) as roles_placed,
        ARRAY_AGG(DISTINCT p.role_level) FILTER (WHERE p.role_level IS NOT NULL) as role_levels,
        -- Fee category counts
        COUNT(*) FILTER (WHERE p.fee_category = 'placement') as placement_count,
        COUNT(*) FILTER (WHERE p.fee_category = 'retainer') as retainer_count,
        COUNT(*) FILTER (WHERE p.fee_category = 'project') as project_count
      FROM clients c
      LEFT JOIN client_financials cf ON c.id = cf.client_id
      LEFT JOIN placements p ON c.id = p.client_id
      WHERE cf.total_placements > 0 ${whereClause}
      GROUP BY c.id, c.name, c.relationship_tier, cf.total_placements, 
               cf.total_invoiced, cf.average_placement_fee, cf.first_placement_date,
               cf.last_placement_date, cf.payment_reliability
      ORDER BY cf.total_invoiced DESC
      ${limit ? `LIMIT ${limit}` : ''}
    `;
    
    const result = await db.query(query);
    const companies = result.rows;
    
    if (companies.length === 0) {
      console.log('No companies to embed.');
      return;
    }
    
    console.log(`Found ${companies.length} companies to embed\n`);
    
    // Process in batches
    let embedded = 0;
    let errors = 0;
    
    for (let i = 0; i < companies.length; i += BATCH_SIZE) {
      const batch = companies.slice(i, i + BATCH_SIZE);
      
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(companies.length / BATCH_SIZE)}...`);
      
      try {
        // Prepare texts
        const texts = batch.map(company => prepareCompanyText(company));
        
        // Generate embeddings
        const embeddings = await generateEmbeddings(texts);
        
        // Upsert to Qdrant
        const points = batch.map((company, idx) => ({
          id: company.id,
          vector: embeddings[idx],
          payload: {
            name: company.name,
            total_placements: company.total_placements || 0,
            total_invoiced: parseFloat(company.total_invoiced || 0),
            average_fee: parseFloat(company.average_placement_fee || 0),
            first_placement_date: company.first_placement_date,
            last_placement_date: company.last_placement_date,
            roles_placed: company.roles_placed?.filter(r => r) || [],
            role_levels: company.role_levels?.filter(l => l) || [],
            tier: company.relationship_tier,
            placement_count: company.placement_count || 0,
            retainer_count: company.retainer_count || 0,
            project_count: company.project_count || 0
          }
        }));
        
        await qdrant.upsertPoints(COLLECTION_NAME, points);
        
        // Record in embeddings table
        for (const company of batch) {
          await db.query(`
            INSERT INTO embeddings (entity_type, entity_id, embedding_type, qdrant_collection, qdrant_point_id)
            VALUES ('company', $1, 'composite', $2, $3)
          `, [company.id, COLLECTION_NAME, company.id]);
        }
        
        embedded += batch.length;
        console.log(`  ✓ Embedded ${batch.length} companies (${embedded}/${companies.length})\n`);
        
        // Rate limiting
        if (i + BATCH_SIZE < companies.length) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
        
      } catch (error) {
        console.error(`  ✗ Error processing batch: ${error.message}\n`);
        errors += batch.length;
      }
    }
    
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`✅ EMBEDDING COMPLETE`);
    console.log(`   Embedded: ${embedded}`);
    console.log(`   Errors: ${errors}`);
    console.log('═══════════════════════════════════════════════════════════════\n');
    
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  } finally {
    await db.pool.end();
  }
}

// ============================================================================
// CLI
// ============================================================================

async function main() {
  const args = process.argv.slice(2);
  const limit = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : null;
  const force = args.includes('--force');
  
  await embedCompanies({ limit, force });
}

if (require.main === module) {
  main();
}

module.exports = { embedCompanies, prepareCompanyText };