require('dotenv').config();
const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY
});

async function setupCollections() {
  console.log('🎯 Setting up Qdrant collections...\n');
  
  const collections = [
    {
      name: 'people',
      description: 'Person composite embeddings',
      vectors: { size: 1536, distance: 'Cosine' }
    },
    {
      name: 'signals',
      description: 'Market signal embeddings',
      vectors: { size: 1536, distance: 'Cosine' }
    },
    {
      name: 'companies',
      description: 'Company profile embeddings',
      vectors: { size: 1536, distance: 'Cosine' }
    }
  ];
  
  for (const collection of collections) {
    try {
      const existing = await qdrant.getCollection(collection.name);
      console.log(`✓ ${collection.name} exists (${existing.points_count || 0} points)`);
    } catch (err) {
      await qdrant.createCollection(collection.name, collection.vectors);
      console.log(`✨ Created ${collection.name}`);
    }
  }
  
  console.log('\n✅ Qdrant setup complete!');
}

setupCollections().catch(console.error);
