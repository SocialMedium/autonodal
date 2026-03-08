#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/compute_signals.js - Signal Detection Pipeline
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const db = require('../lib/db');
const { 
  detectSignals, 
  extractCompanyNames, 
  extractAmounts,
  scoreDocument 
} = require('../lib/signal_keywords');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const BATCH_SIZE = 100;
const MIN_CONFIDENCE = 0.45;

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN SIGNAL COMPUTATION
// ═══════════════════════════════════════════════════════════════════════════════

async function computeSignals() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - SIGNAL DETECTION');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  
  const startTime = Date.now();
  
  // Get unprocessed documents
  const docs = await db.queryAll(`
    SELECT id, source_type, source_name, source_url, title, content, published_at
    FROM external_documents
    WHERE signals_computed_at IS NULL
    AND (title IS NOT NULL OR content IS NOT NULL)
    ORDER BY published_at DESC NULLS LAST
    LIMIT $1
  `, [BATCH_SIZE]);
  
  console.log(`📄 Found ${docs.length} documents to analyze`);
  console.log();
  
  let processed = 0;
  let signalsCreated = 0;
  let companiesLinked = 0;
  
  for (const doc of docs) {
    const text = `${doc.title || ''} ${doc.content || ''}`;
    
    console.log(`🔍 Analyzing: ${(doc.title || 'Untitled').substring(0, 60)}...`);
    
    // Detect signals
    const signals = detectSignals(text);
    const companies = extractCompanyNames(text);
    const amounts = extractAmounts(text);
    
    // Extract entities for storage
    const extractedEntities = {
      companies,
      amounts,
      detected_at: new Date().toISOString()
    };
    
    // Extract signals for storage
    const extractedSignals = signals.map(s => ({
      type: s.signal_type,
      confidence: s.confidence,
      matches: s.matches,
      evidence: s.evidence
    }));
    
    // Update document with extracted data
    await db.query(`
      UPDATE external_documents 
      SET 
        extracted_entities = $1,
        extracted_signals = $2,
        signals_computed_at = NOW(),
        processing_status = 'processed'
      WHERE id = $3
    `, [JSON.stringify(extractedEntities), JSON.stringify(extractedSignals), doc.id]);
    
    // Create signal events for high-confidence signals
    const highConfidenceSignals = signals.filter(s => s.confidence >= MIN_CONFIDENCE);
    
    if (highConfidenceSignals.length > 0) {
      console.log(`   📊 Found ${highConfidenceSignals.length} signals above ${MIN_CONFIDENCE} threshold`);
      
      for (const signal of highConfidenceSignals) {
        // Try to find or create company
        let companyId = null;
        let companyName = null;
        
        if (companies.length > 0) {
          // Use first extracted company name
          companyName = companies[0];
          
          // Try to find existing company
          const existingCompany = await db.queryOne(
            `SELECT id, name FROM companies WHERE LOWER(name) = LOWER($1)`,
            [companyName]
          );
          
          if (existingCompany) {
            companyId = existingCompany.id;
            companyName = existingCompany.name;
          } else {
            // Create new company
            const newCompany = await db.insert('companies', {
              name: companyName
            });
            companyId = newCompany.id;
            console.log(`   🏢 Created company: ${companyName}`);
          }
        }
        
        // Create signal event
        const signalEvent = await db.insert('signal_events', {
          signal_type: signal.signal_type,
          company_id: companyId,
          company_name: companyName,
          confidence_score: signal.confidence,
          scoring_breakdown: JSON.stringify({
            matches: signal.matches,
            evidence: signal.evidence
          }),
          evidence_doc_ids: [doc.id],
          evidence_summary: signal.snippet,
          evidence_snippets: JSON.stringify([{
            doc_id: doc.id,
            snippet: signal.snippet,
            source: doc.source_name
          }]),
          signal_date: doc.published_at
        });
        
        signalsCreated++;
        
        console.log(`   🎯 Signal: ${signal.signal_type} (${(signal.confidence * 100).toFixed(0)}%)`);
        
        // Link document to company
        if (companyId) {
          try {
            await db.query(`
              INSERT INTO document_companies (document_id, company_id, mention_context, confidence)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (document_id, company_id) DO NOTHING
            `, [doc.id, companyId, signal.snippet?.substring(0, 500), signal.confidence]);
            companiesLinked++;
          } catch (e) {
            // Ignore duplicate errors
          }
        }
      }
    } else {
      console.log(`   ⏭️  No high-confidence signals detected`);
    }
    
    processed++;
    console.log();
  }
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  
  // Get signal stats
  const signalStats = await db.queryOne(`
    SELECT 
      COUNT(*) FILTER (WHERE triage_status = 'new') as new_signals,
      COUNT(*) FILTER (WHERE detected_at > NOW() - INTERVAL '24 hours') as signals_24h,
      COUNT(*) as total_signals
    FROM signal_events
  `);
  
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  SIGNAL DETECTION COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  console.log(`   📊 This Run:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   Documents processed: ${processed}`);
  console.log(`   Signals created: ${signalsCreated}`);
  console.log(`   Companies linked: ${companiesLinked}`);
  console.log(`   Duration: ${duration}s`);
  console.log();
  console.log(`   📈 Total Signals:`);
  console.log(`   ─────────────────────────────────────────`);
  console.log(`   New (untriaged): ${signalStats?.new_signals || 0}`);
  console.log(`   Last 24 hours: ${signalStats?.signals_24h || 0}`);
  console.log(`   Total: ${signalStats?.total_signals || 0}`);
  console.log();
}

// Run if called directly
if (require.main === module) {
  computeSignals()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { computeSignals };
