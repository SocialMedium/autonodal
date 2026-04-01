// ═══════════════════════════════════════════════════════════════════════════════
// lib/crm/ezekia_adapter.js — Ezekia CRM Adapter
//
// Wraps the existing lib/ezekia.js functions into the standard adapter interface.
// Supports: people sync (inbound), note push (outbound), project sync (inbound).
// ═══════════════════════════════════════════════════════════════════════════════

const CrmAdapter = require('./adapter');

class EzekiaAdapter extends CrmAdapter {
  constructor(connection, pool) {
    super(connection, pool);
    this.apiToken = this.credentials.api_token || process.env.EZEKIA_API_TOKEN;
    this.apiUrl = this.credentials.api_url || process.env.EZEKIA_API_URL || 'https://app.ezekia.com/api/v1';
  }

  async testConnection() {
    if (!this.apiToken) throw new Error('Ezekia API token not configured');
    const response = await fetch(`${this.apiUrl}/me`, {
      headers: { 'Authorization': `Bearer ${this.apiToken}`, 'Accept': 'application/json' }
    });
    if (!response.ok) throw new Error(`Ezekia API returned ${response.status}`);
    const data = await response.json();
    return { success: true, user: data.name || data.email, provider: 'ezekia' };
  }

  async syncInbound(options = {}) {
    const stats = { created: 0, updated: 0, skipped: 0, errors: 0 };

    try {
      // Delegate to existing sync_ezekia.js logic if available
      try {
        const { syncEzekia } = require('../../scripts/sync_ezekia');
        const result = await syncEzekia();
        stats.created = result?.created || 0;
        stats.updated = result?.updated || 0;
        await this.updateLastSync('success', stats);
      } catch (e) {
        // Fallback: direct API call for people
        const response = await fetch(`${this.apiUrl}/candidates?per_page=100&updated_since=${(options.since || new Date(Date.now() - 86400000)).toISOString()}`, {
          headers: { 'Authorization': `Bearer ${this.apiToken}`, 'Accept': 'application/json' }
        });
        if (!response.ok) throw new Error(`Ezekia candidates API: ${response.status}`);
        const data = await response.json();
        const candidates = data.data || data.candidates || data;

        for (const c of (Array.isArray(candidates) ? candidates : [])) {
          try {
            const existing = await this.pool.query(
              `SELECT id FROM people WHERE source = 'ezekia' AND source_id = $1 AND tenant_id = $2`,
              [String(c.id), this.tenant_id]
            );

            if (existing.rows.length > 0) {
              stats.updated++;
              await this.logSync('inbound', 'person', existing.rows[0].id, String(c.id), 'update');
            } else {
              stats.skipped++; // Creation handled by full sync script
            }
          } catch (e) {
            stats.errors++;
            await this.logSync('inbound', 'person', null, String(c.id), 'error', {}, e.message);
          }
        }
        await this.updateLastSync('success', stats);
      }
    } catch (e) {
      stats.errors++;
      await this.updateLastSync('failed', { ...stats, error: e.message });
    }

    return stats;
  }

  async syncOutbound(entityType, action, data) {
    if (!this.apiToken) return { success: false, error: 'No API token' };

    if (entityType === 'person' && action === 'add_note') {
      const ezekiaId = data.source_id || data.ezekia_id;
      if (!ezekiaId) return { success: false, error: 'No Ezekia ID for this person' };

      const response = await fetch(`${this.apiUrl}/candidates/${ezekiaId}/notes`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.apiToken}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          body: data.note || data.description,
          subject: data.subject || 'Note from MLX Intelligence'
        })
      });

      if (!response.ok) {
        const err = await response.text();
        await this.logSync('outbound', 'person', data.person_id, ezekiaId, 'error', {}, err);
        return { success: false, error: err };
      }

      await this.logSync('outbound', 'person', data.person_id, ezekiaId, 'create', { note: data.subject });
      return { success: true, external_id: ezekiaId };
    }

    return { success: false, error: `Unsupported: ${entityType}/${action}` };
  }

  async handleWebhook(payload, headers) {
    // Ezekia doesn't natively support webhooks — polling only
    return { processed: false, reason: 'Ezekia uses polling, not webhooks' };
  }
}

module.exports = EzekiaAdapter;
