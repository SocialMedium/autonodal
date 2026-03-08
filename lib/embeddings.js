// ═══════════════════════════════════════════════════════════════════════════════
// lib/embeddings.js - OpenAI Embeddings Helper
// ═══════════════════════════════════════════════════════════════════════════════

const OpenAI = require('openai');

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBEDDING_DIMENSIONS = 1536;
const MAX_TOKENS = 8191; // Model limit
const CHARS_PER_TOKEN = 4; // Approximate

/**
 * Generate embedding for text
 * @param {string} text - Text to embed
 * @returns {Promise<number[]>} Embedding vector
 */
async function generateEmbedding(text) {
  if (!text || text.trim().length === 0) {
    throw new Error('Text is required for embedding');
  }
  
  // Truncate if too long
  const truncatedText = truncateText(text);
  
  const response = await openai.embeddings.create({
    model: EMBEDDING_MODEL,
    input: truncatedText,
  });
  
  return response.data[0].embedding;
}

/**
 * Generate embeddings for multiple texts in batch
 * @param {string[]} texts - Array of texts to embed
 * @returns {Promise<number[][]>} Array of embedding vectors
 */
async function generateEmbeddings(texts) {
  if (!texts || texts.length === 0) {
    return [];
  }
  
  // Truncate each text
  const truncatedTexts = texts.map(truncateText);
  
  // OpenAI allows up to 2048 inputs per request
  const batchSize = 100;
  const results = [];
  
  for (let i = 0; i < truncatedTexts.length; i += batchSize) {
    const batch = truncatedTexts.slice(i, i + batchSize);
    
    const response = await openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: batch,
    });
    
    results.push(...response.data.map(d => d.embedding));
    
    // Rate limiting pause
    if (i + batchSize < truncatedTexts.length) {
      await sleep(100);
    }
  }
  
  return results;
}

/**
 * Truncate text to fit within token limit
 */
function truncateText(text, maxChars = MAX_TOKENS * CHARS_PER_TOKEN) {
  if (!text) return '';
  
  // Clean the text
  let cleaned = text
    .replace(/\s+/g, ' ')  // Normalize whitespace
    .replace(/[^\x00-\x7F]/g, '') // Remove non-ASCII
    .trim();
  
  // Truncate if needed
  if (cleaned.length > maxChars) {
    cleaned = cleaned.substring(0, maxChars - 3) + '...';
  }
  
  return cleaned;
}

/**
 * Compute cosine similarity between two vectors
 */
function cosineSimilarity(a, b) {
  if (a.length !== b.length) {
    throw new Error('Vectors must have same length');
  }
  
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  
  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  
  return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
}

/**
 * Prepare document text for embedding
 */
function prepareDocumentText(document) {
  const parts = [];
  
  if (document.title) {
    parts.push(`Title: ${document.title}`);
  }
  
  if (document.source_name) {
    parts.push(`Source: ${document.source_name}`);
  }
  
  if (document.summary) {
    parts.push(`Summary: ${document.summary}`);
  } else if (document.content) {
    // Use first 2000 chars of content if no summary
    parts.push(document.content.substring(0, 2000));
  }
  
  return parts.join('\n\n');
}

/**
 * Prepare person profile for embedding
 */
function preparePersonText(person) {
  const parts = [];
  
  parts.push(`Name: ${person.full_name}`);
  
  if (person.current_title) {
    parts.push(`Title: ${person.current_title}`);
  }
  
  if (person.current_company_name) {
    parts.push(`Company: ${person.current_company_name}`);
  }
  
  if (person.headline) {
    parts.push(`Headline: ${person.headline}`);
  }
  
  if (person.bio) {
    parts.push(`Bio: ${person.bio}`);
  }
  
  if (person.expertise_tags && person.expertise_tags.length > 0) {
    parts.push(`Expertise: ${person.expertise_tags.join(', ')}`);
  }
  
  if (person.industries && person.industries.length > 0) {
    parts.push(`Industries: ${person.industries.join(', ')}`);
  }
  
  // Add career history if available
  if (person.career_history && Array.isArray(person.career_history)) {
    const careerText = person.career_history
      .slice(0, 5)
      .map(job => `${job.title} at ${job.company}`)
      .join('; ');
    parts.push(`Career: ${careerText}`);
  }
  
  return parts.join('\n');
}

/**
 * Prepare search brief for embedding
 */
function prepareSearchText(search) {
  const parts = [];
  
  parts.push(`Role: ${search.title}`);
  
  if (search.location) {
    parts.push(`Location: ${search.location}`);
  }
  
  if (search.seniority_level) {
    parts.push(`Seniority: ${search.seniority_level}`);
  }
  
  if (search.role_overview) {
    parts.push(`Overview: ${search.role_overview}`);
  }
  
  if (search.required_experience) {
    parts.push(`Required Experience: ${search.required_experience}`);
  }
  
  if (search.ideal_background) {
    parts.push(`Ideal Background: ${search.ideal_background}`);
  }
  
  if (search.must_have_keywords && search.must_have_keywords.length > 0) {
    parts.push(`Must Have: ${search.must_have_keywords.join(', ')}`);
  }
  
  if (search.target_industries && search.target_industries.length > 0) {
    parts.push(`Industries: ${search.target_industries.join(', ')}`);
  }
  
  return parts.join('\n');
}

/**
 * Prepare company profile for embedding
 */
function prepareCompanyText(company) {
  const parts = [];
  
  parts.push(`Company: ${company.name}`);
  
  if (company.sector) {
    parts.push(`Sector: ${company.sector}`);
  }
  
  if (company.sub_sector) {
    parts.push(`Sub-sector: ${company.sub_sector}`);
  }
  
  if (company.geography) {
    parts.push(`Geography: ${company.geography}`);
  }
  
  if (company.description) {
    parts.push(`Description: ${company.description}`);
  }
  
  if (company.employee_count_band) {
    parts.push(`Size: ${company.employee_count_band}`);
  }
  
  return parts.join('\n');
}

/**
 * Sleep helper for rate limiting
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  generateEmbedding,
  generateEmbeddings,
  truncateText,
  cosineSimilarity,
  prepareDocumentText,
  preparePersonText,
  prepareSearchText,
  prepareCompanyText,
  EMBEDDING_MODEL,
  EMBEDDING_DIMENSIONS
};
