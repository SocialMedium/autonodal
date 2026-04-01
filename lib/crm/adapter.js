// ═══════════════════════════════════════════════════════════════════════════════
// lib/crm/adapter.js — Base CRM Adapter (abstract interface)
//
// All CRM adapters extend this class. It provides:
//   - Standard interface: testConnection, syncInbound, syncOutbound, handleWebhook
//   - Shared helpers: logSync, logActivity, resolveFieldMapping
//   - Connection and pool references
// ═══════════════════════════════════════════════════════════════════════════════

class CrmAdapter {
  constructor(connection, pool) {
    this.connection = connection;
    this.pool = pool;
    this.tenant_id = connection.tenant_id;
    this.provider = connection.provider;
    this.credentials = connection.credentials_encrypted || {};
    this.fieldMappings = connection.field_mappings || {};
  }

  // ── Must implement ──────────────────────────────────────────────────────

  async testConnection() { throw new Error(`${this.provider}: testConnection not implemented`); }

  /**
   * Pull data from CRM into MLX.
   * @param {Object} options - { since: Date, entities: ['person','company','opportunity'] }
   * @returns {{ created: number, updated: number, skipped: number, errors: number }}
   */
  async syncInbound(options = {}) { throw new Error(`${this.provider}: syncInbound not implemented`); }

  /**
   * Push a local entity change to the CRM.
   * @param {string} entityType - 'person', 'company', 'opportunity', 'placement'
   * @param {string} action - 'create', 'update', 'add_note'
   * @param {Object} data - Entity data to push
   * @returns {{ success: boolean, external_id?: string }}
   */
  async syncOutbound(entityType, action, data) { throw new Error(`${this.provider}: syncOutbound not implemented`); }

  /**
   * Handle an inbound webhook from the CRM.
   * @param {Object} payload - Webhook body
   * @param {Object} headers - Request headers (for HMAC validation)
   * @returns {{ processed: boolean }}
   */
  async handleWebhook(payload, headers) { throw new Error(`${this.provider}: handleWebhook not implemented`); }

  // ── Shared helpers ──────────────────────────────────────────────────────

  async logSync(direction, entityType, entityId, externalId, action, changes = {}, errorMessage = null) {
    await this.pool.query(
      `INSERT INTO crm_sync_log (connection_id, tenant_id, direction, entity_type, entity_id, external_id, action, changes, error_message)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [this.connection.id, this.tenant_id, direction, entityType, entityId, externalId, action,
       JSON.stringify(changes), errorMessage]
    );
  }

  async logActivity(activityType, subject, entityLinks = {}) {
    await this.pool.query(
      `INSERT INTO activities (tenant_id, activity_type, subject, source,
         opportunity_id, engagement_id, person_id, company_id, metadata)
       VALUES ($1, $2, $3, 'crm_sync', $4, $5, $6, $7, $8)`,
      [this.tenant_id, activityType, subject,
       entityLinks.opportunity_id || null,
       entityLinks.engagement_id || null,
       entityLinks.person_id || null,
       entityLinks.company_id || null,
       JSON.stringify({ provider: this.provider })]
    );
  }

  async updateLastSync(status, stats = {}) {
    await this.pool.query(
      `UPDATE crm_connections SET last_sync_at = NOW(), last_sync_status = $1,
         last_sync_stats = $2, last_error = $3, updated_at = NOW()
       WHERE id = $4`,
      [status, JSON.stringify(stats), status === 'failed' ? stats.error : null, this.connection.id]
    );
  }

  resolveFieldMapping(entityType, localField) {
    const key = `${entityType}.${localField}`;
    return this.fieldMappings[key] || null;
  }
}

module.exports = CrmAdapter;
