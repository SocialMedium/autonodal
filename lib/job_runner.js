// ═══════════════════════════════════════════════════════════════════════════
// lib/job_runner.js
// PIPELINE-CONTEXT: Uses pool.query intentionally — job runner manages
// pipeline execution state in job_runs table. Not tenant-scoped.
// Standardised wrapper for all MitchelLake cron/batch scripts.
//
// Usage:
//   const { runJob, logProgress, withRetry, getLastSuccessfulRun } = require('../lib/job_runner');
//
//   await runJob(pool, 'gmail_sync', async () => {
//     // ... your job logic
//     return { records_in: threadsScanned, records_out: interactionsCreated };
//   });
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Run a named job, tracking its health in the job_runs table.
 *
 * @param {import('pg').Pool} pool - PostgreSQL pool
 * @param {string} jobName - Unique job identifier (e.g. 'gmail_sync')
 * @param {Function} fn - Async function containing job logic.
 *   Must return: { records_in, records_out, metadata? }
 * @returns {Promise<{ records_in, records_out, metadata }>}
 */
async function runJob(pool, jobName, fn) {
  const client = await pool.connect();
  let runId;

  try {
    const { rows } = await client.query(
      `INSERT INTO job_runs (job_name, status) VALUES ($1, 'running') RETURNING id`,
      [jobName]
    );
    runId = rows[0].id;
  } finally {
    client.release();
  }

  const startedAt = Date.now();

  try {
    const result = await fn();

    const records_in  = result?.records_in  ?? 0;
    const records_out = result?.records_out ?? 0;
    const metadata    = result?.metadata    ?? {};

    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
    metadata.elapsed_seconds = parseFloat(elapsed);

    await pool.query(
      `UPDATE job_runs
       SET completed_at = NOW(),
           status       = 'success',
           records_in   = $2,
           records_out  = $3,
           metadata     = $4
       WHERE id = $1`,
      [runId, records_in, records_out, JSON.stringify(metadata)]
    );

    console.log(`\n[${jobName}] ✅ Completed in ${elapsed}s — in: ${records_in}, out: ${records_out}`);
    return result;

  } catch (err) {
    await pool.query(
      `UPDATE job_runs
       SET completed_at  = NOW(),
           status        = 'failed',
           error_message = $2
       WHERE id = $1`,
      [runId, err.message]
    );

    console.error(`\n[${jobName}] ✗ Failed: ${err.message}`);
    throw err;
  }
}

/**
 * Get metadata from the most recent successful run of a job.
 * Used to determine resume points (e.g. gmail_history_id, last sync cursor).
 *
 * @param {import('pg').Pool} pool
 * @param {string} jobName
 * @returns {Promise<{ id, started_at, records_in, records_out, metadata } | null>}
 */
async function getLastSuccessfulRun(pool, jobName) {
  const { rows } = await pool.query(
    `SELECT id, started_at, records_in, records_out, metadata
     FROM job_runs
     WHERE job_name = $1 AND status = 'success'
     ORDER BY started_at DESC
     LIMIT 1`,
    [jobName]
  );
  return rows[0] ?? null;
}

/**
 * Log progress at consistent intervals (every 50 records by default).
 * Format: [job_name] 450/1200 (37.5%) — message
 *
 * @param {string} jobName
 * @param {number} current
 * @param {number} total
 * @param {string} [message]
 * @param {number} [interval] - Log every N records (default: 50)
 */
function logProgress(jobName, current, total, message = '', interval = 50) {
  if (current % interval !== 0 && current !== total) return;
  const pct = total > 0 ? ((current / total) * 100).toFixed(1) : '?';
  const suffix = message ? ` — ${message}` : '';
  console.log(`[${jobName}] ${current}/${total} (${pct}%)${suffix}`);
}

/**
 * Retry an async function with exponential backoff + jitter.
 *
 * @param {Function} fn - Async function to retry
 * @param {number} [maxAttempts=3]
 * @param {number} [baseDelayMs=1000]
 * @returns {Promise<any>}
 */
async function withRetry(fn, maxAttempts = 3, baseDelayMs = 1000) {
  let lastErr;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;

      if (attempt < maxAttempts - 1) {
        const jitter = Math.floor(Math.random() * 500);
        const delay  = baseDelayMs * Math.pow(2, attempt) + jitter;
        console.warn(`[withRetry] Attempt ${attempt + 1} failed: ${err.message}. Retrying in ${delay}ms...`);
        await sleep(delay);
      }
    }
  }

  throw lastErr;
}

/**
 * Simple sleep helper.
 * @param {number} ms
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  runJob,
  getLastSuccessfulRun,
  logProgress,
  withRetry,
  sleep,
};
