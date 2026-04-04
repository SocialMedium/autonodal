// ═══════════════════════════════════════════════════════════════════════════════
// lib/pipelineLogger.js — Structured pipeline run logging
// ═══════════════════════════════════════════════════════════════════════════════

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3,
});

class PipelineLogger {
  constructor(pipelineName, tenantId) {
    this.pipelineName = pipelineName;
    this.tenantId = tenantId || null;
    this.runId = null;
    this.startedAt = Date.now();
    this.counts = { input: 0, processed: 0, failed: 0, skipped: 0 };
    this.firstError = null;
  }

  async start(metadata) {
    try {
      const { rows: [run] } = await pool.query(
        `INSERT INTO pipeline_runs (pipeline_key, pipeline_name, tenant_id, started_at, status, metadata)
         VALUES ($1, $1, $2, NOW(), 'running', $3::jsonb) RETURNING id`,
        [this.pipelineName, this.tenantId, JSON.stringify(metadata || {})]
      );
      this.runId = run.id;
    } catch (e) { /* table may not exist — non-fatal */ }
    return this;
  }

  input(n) { this.counts.input += (n || 1); return this; }
  processed(n) { this.counts.processed += (n || 1); return this; }
  skipped(n) { this.counts.skipped += (n || 1); return this; }
  failed(n, err) {
    this.counts.failed += (n || 1);
    if (err && !this.firstError) this.firstError = err instanceof Error ? err.message : String(err);
    return this;
  }

  async complete(metadata) {
    if (!this.runId) return this.counts;
    var status = this.counts.failed > 0 ? (this.counts.processed > 0 ? 'partial' : 'failed') : 'completed';
    var ms = Date.now() - this.startedAt;
    try {
      await pool.query(
        `UPDATE pipeline_runs SET completed_at = NOW(), duration_ms = $1, status = $2,
         records_input = $3, records_processed = $4, records_failed = $5, records_skipped = $6,
         error_summary = $7, metadata = COALESCE(metadata, '{}') || $8::jsonb WHERE id = $9`,
        [ms, status, this.counts.input, this.counts.processed, this.counts.failed,
         this.counts.skipped, this.firstError, JSON.stringify(metadata || {}), this.runId]
      );
    } catch (e) { /* non-fatal */ }
    return { status, ...this.counts, durationMs: ms };
  }

  async fail(error) {
    if (!this.runId) return;
    this.firstError = error instanceof Error ? error.message : String(error);
    try {
      await pool.query(
        `UPDATE pipeline_runs SET completed_at = NOW(), duration_ms = $1, status = 'failed', error_summary = $2 WHERE id = $3`,
        [Date.now() - this.startedAt, this.firstError, this.runId]
      );
    } catch (e) { /* non-fatal */ }
  }
}

module.exports = { PipelineLogger };
