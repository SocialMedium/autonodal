require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY
});

async function recreateCollections() {
  console.log('🔧 Fixing Qdrant collections...\n');
  
  // Delete and recreate signals collection
  try {
    await qdrant.deleteCollection('signals');
    console.log('✓ Deleted old signals collection');
  } catch (err) {
    console.log('  (signals collection did not exist)');
  }
  
  await qdrant.createCollection('signals', {
    vectors: {
      size: 1536,
      distance: 'Cosine'
    }
  });
  console.log('✨ Created signals collection');
  
  console.log('\n✅ Collections fixed!');
}

recreateCollections().catch(console.error);
