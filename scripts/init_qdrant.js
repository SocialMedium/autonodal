#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/init_qdrant.js - Initialize Qdrant Collections
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const VECTOR_SIZE = 1536;

const COLLECTIONS = [
  {
    name: 'documents',
    description: 'External documents (news, press releases)',
    payload_schema: {
      source_type: 'keyword',
      source_name: 'keyword',
      title: 'text',
      published_at: 'datetime',
      signal_types: 'keyword[]'
    }
  },
  {
    name: 'people',
    description: 'Person composite embeddings',
    payload_schema: {
      full_name: 'text',
      current_title: 'text',
      current_company_name: 'keyword',
      seniority_level: 'keyword',
      industries: 'keyword[]',
      location: 'keyword',
      expertise_tags: 'keyword[]'
    }
  },
  {
    name: 'person_content',
    description: 'Content created by people (blogs, newsletters, podcasts)',
    payload_schema: {
      person_id: 'uuid',
      content_type: 'keyword',
      title: 'text',
      key_topics: 'keyword[]',
      published_at: 'datetime'
    }
  },
  {
    name: 'companies',
    description: 'Company profiles',
    payload_schema: {
      name: 'text',
      sector: 'keyword',
      sub_sector: 'keyword',
      geography: 'keyword',
      employee_count_band: 'keyword'
    }
  },
  {
    name: 'searches',
    description: 'Search requirements and briefs',
    payload_schema: {
      title: 'text',
      seniority_level: 'keyword',
      location: 'keyword',
      target_industries: 'keyword[]',
      status: 'keyword'
    }
  },
  {
    name: 'interests',
    description: 'MitchelLake focus areas and interests',
    payload_schema: {
      interest_type: 'keyword',
      name: 'text',
      keywords: 'keyword[]',
      priority: 'integer'
    }
  }
];

async function initQdrant() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - QDRANT INITIALIZATION');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  
  // Check environment
  if (!process.env.QDRANT_URL) {
    console.error('❌ QDRANT_URL not set in environment');
    process.exit(1);
  }
  
  console.log('🔗 Connecting to Qdrant...');
  console.log(`   URL: ${process.env.QDRANT_URL}`);
  console.log();
  
  const client = new QdrantClient({
    url: process.env.QDRANT_URL,
    apiKey: process.env.QDRANT_API_KEY
  });
  
  try {
    // Get existing collections
    const existing = await client.getCollections();
    const existingNames = existing.collections.map(c => c.name);
    
    console.log('📊 Existing collections:', existingNames.length > 0 ? existingNames.join(', ') : 'None');
    console.log();
    
    // Create each collection
    console.log('🔄 Creating collections...');
    console.log('─────────────────────────────────────────');
    
    for (const collection of COLLECTIONS) {
      const exists = existingNames.includes(collection.name);
      
      if (exists) {
        // Get info
        const info = await client.getCollection(collection.name);
        console.log(`   ⏭️  ${collection.name} (exists, ${info.points_count} points)`);
      } else {
        // Create collection
        await client.createCollection(collection.name, {
          vectors: {
            size: VECTOR_SIZE,
            distance: 'Cosine'
          },
          optimizers_config: {
            default_segment_number: 2
          },
          replication_factor: 1
        });
        console.log(`   ✅ ${collection.name} (created)`);
      }
      
      console.log(`      └─ ${collection.description}`);
    }
    
    console.log();
    
    // Verify all collections
    console.log('📋 Collection Status:');
    console.log('─────────────────────────────────────────');
    
    for (const collection of COLLECTIONS) {
      try {
        const info = await client.getCollection(collection.name);
        console.log(`   ✓ ${collection.name}`);
        console.log(`     Points: ${info.points_count}, Vectors: ${info.vectors_count}`);
        console.log(`     Status: ${info.status}`);
      } catch (error) {
        console.log(`   ✗ ${collection.name}: ${error.message}`);
      }
    }
    
    console.log();
    console.log('═══════════════════════════════════════════════════════════════════');
    console.log('  ✅ QDRANT INITIALIZATION COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════════');
    
  } catch (error) {
    console.error('❌ Error initializing Qdrant:', error.message);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  initQdrant();
}

module.exports = { initQdrant, COLLECTIONS };
