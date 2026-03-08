// ═══════════════════════════════════════════════════════════════════════════════
// lib/qdrant.js - Qdrant Vector Search Client
// ═══════════════════════════════════════════════════════════════════════════════

const { QdrantClient } = require('@qdrant/js-client-rest');

// Initialize client
const client = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
});

const VECTOR_SIZE = 1536;

// Collection names
const COLLECTIONS = {
  DOCUMENTS: 'documents',
  PEOPLE: 'people',
  PERSON_CONTENT: 'person_content',
  COMPANIES: 'companies',
  SEARCHES: 'searches',
  INTERESTS: 'interests'
};

// ═══════════════════════════════════════════════════════════════════════════════
// COLLECTION MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Create a collection if it doesn't exist
 */
async function ensureCollection(collectionName) {
  try {
    const exists = await client.collectionExists(collectionName);
    
    if (!exists.exists) {
      await client.createCollection(collectionName, {
        vectors: {
          size: VECTOR_SIZE,
          distance: 'Cosine'
        },
        optimizers_config: {
          default_segment_number: 2
        },
        replication_factor: 1
      });
      console.log(`✅ Created collection: ${collectionName}`);
    }
    
    return true;
  } catch (error) {
    console.error(`❌ Error creating collection ${collectionName}:`, error);
    throw error;
  }
}

/**
 * Initialize all collections
 */
async function initializeCollections() {
  console.log('🔄 Initializing Qdrant collections...');
  
  for (const [name, collection] of Object.entries(COLLECTIONS)) {
    await ensureCollection(collection);
  }
  
  console.log('✅ All collections initialized');
  return true;
}

/**
 * Get collection info
 */
async function getCollectionInfo(collectionName) {
  try {
    return await client.getCollection(collectionName);
  } catch (error) {
    console.error(`Error getting collection info:`, error);
    return null;
  }
}

/**
 * Get all collections status
 */
async function getCollectionsStatus() {
  const status = {};
  
  for (const [name, collection] of Object.entries(COLLECTIONS)) {
    try {
      const info = await client.getCollection(collection);
      status[collection] = {
        exists: true,
        points_count: info.points_count,
        vectors_count: info.vectors_count,
        status: info.status
      };
    } catch (error) {
      status[collection] = {
        exists: false,
        error: error.message
      };
    }
  }
  
  return status;
}

// ═══════════════════════════════════════════════════════════════════════════════
// VECTOR OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Upsert a vector point
 * @param {string} collection - Collection name
 * @param {string} id - Point ID (UUID)
 * @param {number[]} vector - Embedding vector
 * @param {object} payload - Metadata payload
 */
async function upsertPoint(collection, id, vector, payload = {}) {
  await client.upsert(collection, {
    wait: true,
    points: [
      {
        id,
        vector,
        payload
      }
    ]
  });
  
  return id;
}

/**
 * Upsert multiple points in batch
 */
async function upsertPoints(collection, points) {
  // Process in batches of 100
  const batchSize = 100;
  let upserted = 0;
  
  for (let i = 0; i < points.length; i += batchSize) {
    const batch = points.slice(i, i + batchSize);
    
    await client.upsert(collection, {
      wait: true,
      points: batch
    });
    
    upserted += batch.length;
  }
  
  return upserted;
}

/**
 * Delete a point by ID
 */
async function deletePoint(collection, id) {
  await client.delete(collection, {
    wait: true,
    points: [id]
  });
  
  return true;
}

/**
 * Delete points by filter
 */
async function deleteByFilter(collection, filter) {
  await client.delete(collection, {
    wait: true,
    filter
  });
  
  return true;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEARCH OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Search for similar vectors
 * @param {string} collection - Collection name
 * @param {number[]} vector - Query vector
 * @param {object} options - Search options
 */
async function search(collection, vector, options = {}) {
  const {
    limit = 10,
    filter = null,
    scoreThreshold = 0.0,
    withPayload = true,
    withVector = false
  } = options;
  
  const results = await client.search(collection, {
    vector,
    limit,
    filter,
    score_threshold: scoreThreshold,
    with_payload: withPayload,
    with_vector: withVector
  });
  
  return results;
}

/**
 * Search with payload filter
 */
async function searchWithFilter(collection, vector, filterConditions, limit = 10) {
  const filter = {
    must: filterConditions.map(condition => ({
      key: condition.field,
      match: { value: condition.value }
    }))
  };
  
  return search(collection, vector, { limit, filter });
}

/**
 * Hybrid search combining vector similarity with payload filters
 */
async function hybridSearch(collection, vector, options = {}) {
  const {
    limit = 20,
    mustMatch = [],
    mustNotMatch = [],
    shouldMatch = [],
    scoreThreshold = 0.3
  } = options;
  
  const filter = {};
  
  if (mustMatch.length > 0) {
    filter.must = mustMatch.map(m => ({
      key: m.field,
      match: { value: m.value }
    }));
  }
  
  if (mustNotMatch.length > 0) {
    filter.must_not = mustNotMatch.map(m => ({
      key: m.field,
      match: { value: m.value }
    }));
  }
  
  if (shouldMatch.length > 0) {
    filter.should = shouldMatch.map(m => ({
      key: m.field,
      match: { value: m.value }
    }));
  }
  
  return search(collection, vector, {
    limit,
    filter: Object.keys(filter).length > 0 ? filter : null,
    scoreThreshold
  });
}

/**
 * Get point by ID
 */
async function getPoint(collection, id) {
  try {
    const result = await client.retrieve(collection, {
      ids: [id],
      with_payload: true,
      with_vector: false
    });
    return result[0] || null;
  } catch (error) {
    return null;
  }
}

/**
 * Get multiple points by IDs
 */
async function getPoints(collection, ids) {
  try {
    return await client.retrieve(collection, {
      ids,
      with_payload: true,
      with_vector: false
    });
  } catch (error) {
    return [];
  }
}

/**
 * Scroll through all points in a collection
 */
async function scrollPoints(collection, options = {}) {
  const {
    limit = 100,
    filter = null,
    offset = null,
    withPayload = true,
    withVector = false
  } = options;
  
  return await client.scroll(collection, {
    limit,
    filter,
    offset,
    with_payload: withPayload,
    with_vector: withVector
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// DOMAIN-SPECIFIC SEARCH FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Search for similar documents
 */
async function searchDocuments(queryVector, options = {}) {
  const {
    limit = 10,
    sourceTypes = null,
    minDate = null,
    maxDate = null
  } = options;
  
  const mustConditions = [];
  
  if (sourceTypes && sourceTypes.length > 0) {
    mustConditions.push({
      key: 'source_type',
      match: { any: sourceTypes }
    });
  }
  
  // Note: Date filtering requires range filter
  // This is a simplified version
  
  return search(COLLECTIONS.DOCUMENTS, queryVector, {
    limit,
    filter: mustConditions.length > 0 ? { must: mustConditions } : null,
    scoreThreshold: 0.3
  });
}

/**
 * Search for similar people
 */
async function searchPeople(queryVector, options = {}) {
  const {
    limit = 20,
    seniorityLevels = null,
    industries = null,
    excludeIds = []
  } = options;
  
  const filter = {};
  const mustConditions = [];
  const mustNotConditions = [];
  
  if (seniorityLevels && seniorityLevels.length > 0) {
    mustConditions.push({
      key: 'seniority_level',
      match: { any: seniorityLevels }
    });
  }
  
  if (excludeIds.length > 0) {
    mustNotConditions.push({
      has_id: excludeIds
    });
  }
  
  if (mustConditions.length > 0) filter.must = mustConditions;
  if (mustNotConditions.length > 0) filter.must_not = mustNotConditions;
  
  return search(COLLECTIONS.PEOPLE, queryVector, {
    limit,
    filter: Object.keys(filter).length > 0 ? filter : null,
    scoreThreshold: 0.4
  });
}

/**
 * Find candidates matching a search brief
 */
async function findMatchingCandidates(searchVector, searchCriteria = {}) {
  const {
    limit = 50,
    targetIndustries = [],
    targetCompanies = [],
    offLimitsCompanies = [],
    seniorityLevel = null
  } = searchCriteria;
  
  const mustConditions = [];
  const mustNotConditions = [];
  const shouldConditions = [];
  
  if (seniorityLevel) {
    mustConditions.push({
      key: 'seniority_level',
      match: { value: seniorityLevel }
    });
  }
  
  if (targetIndustries.length > 0) {
    shouldConditions.push({
      key: 'industries',
      match: { any: targetIndustries }
    });
  }
  
  if (offLimitsCompanies.length > 0) {
    mustNotConditions.push({
      key: 'current_company_name',
      match: { any: offLimitsCompanies }
    });
  }
  
  const filter = {};
  if (mustConditions.length > 0) filter.must = mustConditions;
  if (mustNotConditions.length > 0) filter.must_not = mustNotConditions;
  if (shouldConditions.length > 0) filter.should = shouldConditions;
  
  return search(COLLECTIONS.PEOPLE, searchVector, {
    limit,
    filter: Object.keys(filter).length > 0 ? filter : null,
    scoreThreshold: 0.35
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  client,
  COLLECTIONS,
  VECTOR_SIZE,
  
  // Collection management
  ensureCollection,
  initializeCollections,
  getCollectionInfo,
  getCollectionsStatus,
  
  // Vector operations
  upsertPoint,
  upsertPoints,
  deletePoint,
  deleteByFilter,
  
  // Search operations
  search,
  searchWithFilter,
  hybridSearch,
  getPoint,
  getPoints,
  scrollPoints,
  
  // Domain-specific
  searchDocuments,
  searchPeople,
  findMatchingCandidates
};
