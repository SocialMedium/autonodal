#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/embed_documents.js - Document Embedding Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const db = require('../lib/db');
const qdrant = require('../lib/qdrant');
const { generateEmbedding, prepareDocumentText } = require('../lib/embeddings');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const BATCH_SIZE = 50;
const RATE_LIMIT_DELAY = 200; // ms between API calls

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN EMBEDDING FUNCTION
// ═══════════════════════════════════════════════════════════════════════════════

async function embedDocuments() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - DOCUMENT EMBEDDING');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  
  const startTime = Date.now();
  
  // Ensure Qdrant collection exists
  await qdrant.ensureCollection('documents');
  
  // Get pending documents
  const pendingDocs = await db.queryAll(`
    SELECT id, source_type, source_name, title, content, summary, published_at
    FROM external_documents
    WHERE embedded_at IS NULL
    AND (title IS NOT NULL OR content IS NOT NULL)
    ORDER BY published_at DESC NULLS LAST
    LIMIT $1
  `, [BATCH_SIZE * 10]); // Get more than batch size for multiple batches
  
  console.log(`📄 Found ${pendingDocs.length} documents to embed`);
  console.log();
  
  let embedded = 0;
  let errors = 0;
  
  // Process in batches
  for (let i = 0; i < pendingDocs.length; i += BATCH_SIZE) {
    const batch = pendingDocs.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(pendingDocs.length / BATCH_SIZE);
    
    console.log(`🔄 Processing batch ${batchNum}/${totalBatches} (${batch.length} documents)`);
    
    for (const doc of batch) {
      try {
        // Prepare text for embedding
        const text = prepareDocumentText(doc);
        
        if (!text || text.trim().length < 20) {
          console.log(`   ⏭️  Skipping doc ${doc.id.substring(0, 8)}... (no content)`);
          continue;
        }
        
        // Generate embedding
        const embedding = await generateEmbedding(text);
        
        // Store in Qdrant
        await qdrant.upsertPoint('documents', doc.id, embedding, {
          source_type: doc.source_type,
          source_name: doc.source_name,
          title: doc.title,
          published_at: doc.published_at
        });
        
        // Update PostgreSQL
        await db.query(
          'UPDATE external_documents SET embedded_at = NOW() WHERE id = $1',
          [doc.id]
        );
        
        embedded++;
        
        // Rate limiting
        await sleep(RATE_LIMIT_DELAY);
        
      } catch (error) {
        console.log(`   ❌ Error embedding doc ${doc.id.substring(0, 8)}...: ${error.message}`);
        errors++;
      }
    }
    
    console.log(`   ✅ Batch complete: ${embedded} embedded, ${errors} errors`);
    console.log();
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  
  // Get collection stats
  const collectionInfo = await qdrant.getCollectionInfo('documents');
  
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  DOCUMENT EMBEDDING COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  console.log(`   📊 Summary:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   Documents processed: ${pendingDocs.length}`);
  console.log(`   Successfully embedded: ${embedded}`);
  console.log(`   Errors: ${errors}`);
  console.log(`   Duration: ${duration}s`);
  console.log();
  console.log(`   📦 Qdrant Collection:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   Total points: ${collectionInfo?.points_count || 'N/A'}`);
  console.log(`   Status: ${collectionInfo?.status || 'N/A'}`);
  console.log();
}

// Run if called directly
if (require.main === module) {
  embedDocuments()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { embedDocuments };
