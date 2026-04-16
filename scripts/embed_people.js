#!/usr/bin/env node
/**
 * Embed all people for vector search
 * Usage: node scripts/embed_people.js [--limit N] [--force]
 */

require('dotenv').config();

const db = require('../lib/db');
const qdrant = require('../lib/qdrant');
const { generateEmbeddings, preparePersonText } = require('../lib/embeddings');

const BATCH_SIZE = 50; // Process 50 at a time

async function main() {
  const args = process.argv.slice(2);
  const limit = args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : null;
  const force = args.includes('--force');
  const tenantId = args.includes('--tenant') ? args[args.indexOf('--tenant') + 1] : null;

  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  EMBED PEOPLE - MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Batch size: ${BATCH_SIZE}`);
  if (limit) console.log(`  Limit: ${limit}`);
  if (force) console.log(`  Force: re-embedding all`);
  if (tenantId) console.log(`  Tenant: ${tenantId}`);
  console.log('');

  try {
    // Ensure Qdrant collection exists
    console.log('Checking Qdrant collection...');
    await qdrant.ensureCollection('people');
    
    // Get total count
    const totalResult = await db.queryOne('SELECT COUNT(*) as count FROM people');
    const totalPeople = parseInt(totalResult.count);
    console.log(`Total people in database: ${totalPeople.toLocaleString()}`);
    
    // Get Qdrant count
    const qdrantInfo = await qdrant.getCollectionInfo('people');
    const qdrantCount = qdrantInfo?.points_count || 0;
    console.log(`Already embedded in Qdrant: ${qdrantCount.toLocaleString()}`);
    
    // Get people to embed
    let query = `
      SELECT id, full_name, first_name, last_name, headline, bio,
             current_title, current_company_name, location,
             expertise_tags, career_history, tenant_id,
             is_investor, investor_type, investor_stage_focus, investor_sector_focus
      FROM people
      WHERE full_name IS NOT NULL AND full_name != ''
    `;
    const params = [];

    if (tenantId) {
      params.push(tenantId);
      query += ` AND tenant_id = $${params.length}`;
    }

    if (!force) {
      query += ` AND (embedded_at IS NULL OR embedded_at < NOW() - INTERVAL '7 days')`;
    }

    if (limit) {
      query += ` LIMIT ${limit}`;
    }

    const people = await db.queryAll(query, params);
    console.log(`People to process: ${people.length.toLocaleString()}\n`);

    if (people.length === 0) {
      console.log('No people to embed.');
      process.exit(0);
    }

    let processed = 0;
    let embedded = 0;
    let errors = 0;
    const startTime = Date.now();

    // Process in batches
    for (let i = 0; i < people.length; i += BATCH_SIZE) {
      const batch = people.slice(i, i + BATCH_SIZE);
      
      try {
        // Prepare texts for embedding
        const texts = batch.map(person => {
          // Parse career_history if it's a string
          let careerHistory = person.career_history;
          if (typeof careerHistory === 'string') {
            try {
              careerHistory = JSON.parse(careerHistory);
            } catch (e) {
              careerHistory = null;
            }
          }
          
          return preparePersonText({
            ...person,
            career_history: careerHistory
          });
        });
        
        // Generate embeddings
        const embeddings = await generateEmbeddings(texts);
        
        // Prepare points for Qdrant (include tenant_id for filtered search)
        const points = batch.map((person, idx) => ({
          id: person.id,
          vector: embeddings[idx],
          payload: {
            type: 'person',
            person_id: person.id,
            name: person.full_name,
            full_name: person.full_name,
            title: person.current_title,
            current_title: person.current_title,
            company: person.current_company_name,
            location: person.location,
            seniority: null,
            tenant_id: person.tenant_id,
            content_preview: [person.current_title, person.current_company_name, person.location].filter(Boolean).join(' · '),
            is_investor: person.is_investor || false,
            investor_type: person.investor_type || null,
          }
        }));
        
        // Upsert to Qdrant
        await qdrant.upsertPoints('people', points);
        
        embedded += batch.length;
        processed += batch.length;
        
        // Progress update
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = processed / elapsed;
        const remaining = (people.length - processed) / rate;
        
        console.log(`  ✓ Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} embedded (${processed.toLocaleString()}/${people.length.toLocaleString()}) - ${rate.toFixed(1)}/sec - ETA: ${formatTime(remaining)}`);
        
      } catch (err) {
        console.error(`  ✗ Batch error:`, err.message);
        errors += batch.length;
        processed += batch.length;
      }
      
      // Rate limiting - avoid OpenAI limits
      await sleep(200);
    }

    const totalTime = (Date.now() - startTime) / 1000;
    
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  EMBEDDING COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`  Processed: ${processed.toLocaleString()}`);
    console.log(`  Embedded: ${embedded.toLocaleString()}`);
    console.log(`  Errors: ${errors}`);
    console.log(`  Time: ${formatTime(totalTime)}`);
    console.log(`  Rate: ${(embedded / totalTime).toFixed(1)} people/sec`);
    console.log('═══════════════════════════════════════════════════════════════');

  } catch (error) {
    console.error('Embedding failed:', error);
    process.exit(1);
  }

  process.exit(0);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function formatTime(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

main();
