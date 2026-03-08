require('dotenv').config();
const { Pool } = require('pg');
const { QdrantClient } = require('@qdrant/js-client-rest');
const OpenAI = require('openai');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY
});

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

async function generateEmbedding(text) {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text
  });
  return response.data[0].embedding;
}

async function embedSignal(signal) {
  const text = `
${signal.signal_type} signal at ${signal.company_name || 'Unknown Company'}.
${signal.evidence_summary || signal.evidence_snippet || ''}
Sector: ${signal.sector || 'Unknown'}
Geography: ${signal.geography || 'Unknown'}
Confidence: ${Math.round((signal.confidence_score || 0) * 100)}%
${signal.hiring_implications ? `Hiring: ${JSON.stringify(signal.hiring_implications)}` : ''}
  `.trim();
  
  const embedding = await generateEmbedding(text);
  
  await qdrant.upsert('signals', {
    points: [{
      id: signal.id,
      vector: embedding,
      payload: {
        signal_type: signal.signal_type,
        company_id: signal.company_id,
        company_name: signal.company_name,
        confidence_score: signal.confidence_score,
        detected_at: signal.detected_at
      }
    }]
  });
  
  await pool.query(`UPDATE signal_events SET embedded_at = NOW() WHERE id = $1`, [signal.id]);
}

async function main() {
  console.log('🎯 Embedding signals...\n');
  
  const signals = await pool.query(`
    SELECT se.*, c.name as company_name, c.sector, c.geography
    FROM signal_events se
    LEFT JOIN companies c ON se.company_id = c.id
    WHERE se.embedded_at IS NULL
  `);
  
  console.log(`Found ${signals.rows.length} signals to embed\n`);
  
  for (let i = 0; i < signals.rows.length; i++) {
    await embedSignal(signals.rows[i]);
    if ((i + 1) % 10 === 0) {
      console.log(`✓ ${i + 1}/${signals.rows.length}`);
    }
  }
  
  console.log(`\n✅ Embedded ${signals.rows.length} signals!`);
  console.log(`💰 Cost: ~$${(signals.rows.length * 0.00001).toFixed(4)}`);
  pool.end();
}

main().catch(console.error);
