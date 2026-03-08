// ═══════════════════════════════════════════════════════════════════════════════
// lib/db.js - PostgreSQL Database Helpers
// ═══════════════════════════════════════════════════════════════════════════════

const { Pool } = require('pg');

// Create connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Test connection
pool.on('connect', () => {
  console.log('📦 Database connected');
});

pool.on('error', (err) => {
  console.error('❌ Database pool error:', err);
});

// ═══════════════════════════════════════════════════════════════════════════════
// Query Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Execute a query with parameters
 */
async function query(text, params) {
  const start = Date.now();
  const result = await pool.query(text, params);
  const duration = Date.now() - start;
  
  if (process.env.NODE_ENV === 'development' && duration > 100) {
    console.log(`⚠️ Slow query (${duration}ms):`, text.substring(0, 100));
  }
  
  return result;
}

/**
 * Get a single row
 */
async function queryOne(text, params) {
  const result = await query(text, params);
  return result.rows[0] || null;
}

/**
 * Get all rows
 */
async function queryAll(text, params) {
  const result = await query(text, params);
  return result.rows;
}

/**
 * Insert and return the created row
 */
async function insert(table, data) {
  const keys = Object.keys(data);
  const values = Object.values(data);
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
  const columns = keys.join(', ');
  
  const text = `INSERT INTO ${table} (${columns}) VALUES (${placeholders}) RETURNING *`;
  const result = await query(text, values);
  return result.rows[0];
}

/**
 * Update and return the updated row
 */
async function update(table, id, data) {
  const keys = Object.keys(data);
  const values = Object.values(data);
  const setClause = keys.map((key, i) => `${key} = $${i + 1}`).join(', ');
  
  const text = `UPDATE ${table} SET ${setClause} WHERE id = $${keys.length + 1} RETURNING *`;
  const result = await query(text, [...values, id]);
  return result.rows[0];
}

/**
 * Upsert (insert or update on conflict)
 */
async function upsert(table, data, conflictColumn, updateColumns) {
  const keys = Object.keys(data);
  const values = Object.values(data);
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
  const columns = keys.join(', ');
  
  const updateClause = updateColumns
    .map(col => `${col} = EXCLUDED.${col}`)
    .join(', ');
  
  const text = `
    INSERT INTO ${table} (${columns}) 
    VALUES (${placeholders}) 
    ON CONFLICT (${conflictColumn}) 
    DO UPDATE SET ${updateClause}, updated_at = NOW()
    RETURNING *
  `;
  
  const result = await query(text, values);
  return result.rows[0];
}

/**
 * Delete by ID
 */
async function deleteById(table, id) {
  const text = `DELETE FROM ${table} WHERE id = $1 RETURNING *`;
  const result = await query(text, [id]);
  return result.rows[0];
}

/**
 * Transaction helper
 */
async function transaction(callback) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Domain-Specific Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Find or create a company by name
 */
async function findOrCreateCompany(name, additionalData = {}) {
  // First try to find by exact name
  let company = await queryOne(
    `SELECT * FROM companies WHERE LOWER(name) = LOWER($1)`,
    [name]
  );
  
  if (!company) {
    // Try fuzzy match
    company = await queryOne(
      `SELECT * FROM companies WHERE name ILIKE $1 OR $2 = ANY(aliases)`,
      [`%${name}%`, name.toLowerCase()]
    );
  }
  
  if (!company) {
    // Create new company
    company = await insert('companies', {
      name,
      ...additionalData
    });
  }
  
  return company;
}

/**
 * Get paginated results
 */
async function paginate(table, options = {}) {
  const {
    page = 1,
    limit = 20,
    orderBy = 'created_at',
    orderDir = 'DESC',
    where = '',
    params = []
  } = options;
  
  const offset = (page - 1) * limit;
  
  // Get total count
  const countQuery = `SELECT COUNT(*) FROM ${table} ${where ? `WHERE ${where}` : ''}`;
  const countResult = await query(countQuery, params);
  const total = parseInt(countResult.rows[0].count);
  
  // Get paginated data
  const dataQuery = `
    SELECT * FROM ${table} 
    ${where ? `WHERE ${where}` : ''} 
    ORDER BY ${orderBy} ${orderDir}
    LIMIT $${params.length + 1} OFFSET $${params.length + 2}
  `;
  const dataResult = await query(dataQuery, [...params, limit, offset]);
  
  return {
    data: dataResult.rows,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
      hasMore: page * limit < total
    }
  };
}

/**
 * Full-text search helper
 */
async function search(table, searchColumn, searchTerm, options = {}) {
  const { limit = 20, additionalWhere = '', additionalParams = [] } = options;
  
  const text = `
    SELECT *, 
           similarity(${searchColumn}, $1) as match_score
    FROM ${table}
    WHERE ${searchColumn} ILIKE $2
    ${additionalWhere ? `AND ${additionalWhere}` : ''}
    ORDER BY match_score DESC
    LIMIT $${additionalParams.length + 3}
  `;
  
  const result = await query(text, [
    searchTerm,
    `%${searchTerm}%`,
    ...additionalParams,
    limit
  ]);
  
  return result.rows;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Stats Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Get dashboard stats
 */
async function getDashboardStats() {
  const stats = await queryOne(`
    SELECT
      (SELECT COUNT(*) FROM signal_events WHERE triage_status = 'new' AND detected_at > NOW() - INTERVAL '7 days') as new_signals,
      (SELECT COUNT(*) FROM signal_events WHERE triage_status = 'qualified' AND detected_at > NOW() - INTERVAL '7 days') as qualified_signals,
      (SELECT COUNT(*) FROM companies) as total_companies,
      (SELECT COUNT(*) FROM people) as total_people,
      (SELECT COUNT(*) FROM searches WHERE status NOT IN ('placed', 'cancelled')) as active_searches,
      (SELECT COUNT(*) FROM search_candidates WHERE status IN ('shortlisted', 'presented', 'client_interview')) as shortlisted_candidates,
      (SELECT COUNT(*) FROM external_documents WHERE fetched_at > NOW() - INTERVAL '24 hours') as docs_today
  `);
  
  return stats;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Exports
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  pool,
  query,
  queryOne,
  queryAll,
  insert,
  update,
  upsert,
  deleteById,
  transaction,
  findOrCreateCompany,
  paginate,
  search,
  getDashboardStats
};
