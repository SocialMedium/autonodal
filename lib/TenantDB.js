// ═══════════════════════════════════════════════════════════════════════════════
// lib/TenantDB.js — Context-Gated PostgreSQL Client
// ═══════════════════════════════════════════════════════════════════════════════
//
// Every query runs within a transaction that sets app.current_tenant.
// This ensures Postgres RLS policies enforce tenant isolation at the DB level.
// No query reaches the database without a tenant context.
//
// Usage:
//   const db = new TenantDB(tenantId);
//   const { rows } = await db.query('SELECT * FROM people WHERE name ILIKE $1', ['%john%']);
//   // RLS automatically filters to this tenant only
//
// For platform-level operations (tenant listing, health checks, migrations):
//   const { platformPool } = require('./TenantDB');
//   await platformPool.query('SELECT * FROM tenants');

const { Pool } = require('pg');

// Application pool — non-superuser (autonodal_app role), RLS enforced
// Falls back to DATABASE_URL if DATABASE_URL_APP not set
const pool = new Pool({
  connectionString: process.env.DATABASE_URL_APP || process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

class TenantDB {
  constructor(tenantId) {
    if (!tenantId) {
      throw new Error(
        'TenantDB requires a tenantId. ' +
        'Never instantiate without a valid tenant context.'
      );
    }
    this.tenantId = tenantId;
  }

  /**
   * Execute a query within tenant context.
   * Wraps in BEGIN/COMMIT to scope SET LOCAL to this operation.
   */
  async query(text, params = []) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL app.current_tenant = '${this.tenantId}'`);
      const result = await client.query(text, params);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Get a single row within tenant context.
   */
  async queryOne(text, params = []) {
    const result = await this.query(text, params);
    return result.rows[0] || null;
  }

  /**
   * Get all rows within tenant context.
   */
  async queryAll(text, params = []) {
    const result = await this.query(text, params);
    return result.rows;
  }

  /**
   * Execute multiple queries in a single transaction with tenant context.
   * Callback receives the pg client — all queries within are tenant-scoped.
   */
  async transaction(callback) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL app.current_tenant = '${this.tenantId}'`);
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Insert a row, returning the result.
   * Convenience wrapper matching the existing db.js pattern.
   */
  async insert(table, data) {
    const keys = Object.keys(data);
    const values = Object.values(data);
    const placeholders = keys.map((_, i) => `$${i + 1}`);
    const text = `INSERT INTO ${table} (${keys.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`;
    return this.query(text, values);
  }

  /**
   * Upsert a row on conflict.
   */
  async upsert(table, data, conflictColumns, updateColumns) {
    const keys = Object.keys(data);
    const values = Object.values(data);
    const placeholders = keys.map((_, i) => `$${i + 1}`);
    const updateSet = (updateColumns || keys.filter(k => !conflictColumns.includes(k)))
      .map(k => `${k} = EXCLUDED.${k}`).join(', ');
    const text = `INSERT INTO ${table} (${keys.join(', ')}) VALUES (${placeholders.join(', ')})
      ON CONFLICT (${conflictColumns.join(', ')}) DO UPDATE SET ${updateSet} RETURNING *`;
    return this.query(text, values);
  }
}

// Platform-level pool — superuser for cross-tenant operations ONLY:
// User login, tenant provisioning, health checks, migrations.
// NEVER use for tenant data queries — use TenantDB instead.
const _platformPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});
const platformPool = {
  query: (text, params) => _platformPool.query(text, params),
  connect: () => _platformPool.connect(),
};

module.exports = { TenantDB, platformPool, pool };
