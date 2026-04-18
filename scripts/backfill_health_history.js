#!/usr/bin/env node
// ============================================================================
// Backfill market_health_history — compute daily scores retroactively
// Uses signal_events detected_at to reconstruct what the score would have been
// Run once: node scripts/backfill_health_history.js
// ============================================================================

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

const SIGNAL_STOCKS = {
  capital_raising:      { sentiment: 'bullish',  weight: 3.0 },
  ma_activity:          { sentiment: 'neutral',  weight: 2.5 },
  product_launch:       { sentiment: 'bullish',  weight: 2.0 },
  leadership_change:    { sentiment: 'neutral',  weight: 1.5 },
  strategic_hiring:     { sentiment: 'bullish',  weight: 1.0 },
  geographic_expansion: { sentiment: 'bullish',  weight: 1.5 },
  partnership:          { sentiment: 'bullish',  weight: 1.2 },
  layoffs:              { sentiment: 'bearish',  weight: 2.0 },
  restructuring:        { sentiment: 'bearish',  weight: 2.5 },
};

const HORIZONS = [
  { key: '7d',  days: 7,   priorDays: 14  },
  { key: '30d', days: 30,  priorDays: 60  },
  { key: '90d', days: 90,  priorDays: 180 },
];

function computeDelta(current, prior) {
  const smoothedPrior = Math.max(prior, 5);
  const raw = ((current - prior) / smoothedPrior) * 100;
  return Math.min(99, Math.max(-99, raw));
}

function deltaToScore(delta, sentiment) {
  const dir = sentiment === 'bullish' ? 1 : sentiment === 'bearish' ? -1 : 0.3;
  const raw = 50 + (dir * delta * 0.65);
  return Math.min(100, Math.max(0, Math.round(raw * 10) / 10));
}

async function computeScoreAtDate(asOfDate, horizon) {
  const currentEnd = asOfDate;
  const currentStart = new Date(asOfDate);
  currentStart.setDate(currentStart.getDate() - horizon.days);
  const priorEnd = new Date(currentStart);
  const priorStart = new Date(priorEnd);
  priorStart.setDate(priorStart.getDate() - horizon.days);

  let weightedSum = 0, totalWeight = 0;
  let bullishVolume = 0, bearishVolume = 0;
  let rawBullish = 0, rawBearish = 0;

  for (const [signalType, cfg] of Object.entries(SIGNAL_STOCKS)) {
    const { rows: [counts] } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE detected_at BETWEEN $3 AND $4)::int AS current_count,
        COUNT(*) FILTER (WHERE detected_at BETWEEN $5 AND $6)::int AS prior_count
      FROM signal_events
      WHERE signal_type = $1 AND (tenant_id IS NULL OR tenant_id = $2)
    `, [signalType, TENANT_ID, currentStart, currentEnd, priorStart, priorEnd]);

    const current = counts.current_count;
    const prior = counts.prior_count;
    const delta = computeDelta(current, prior);
    const score = deltaToScore(delta, cfg.sentiment);

    weightedSum += score * cfg.weight;
    totalWeight += cfg.weight;

    if (cfg.sentiment === 'bullish') { bullishVolume += current * cfg.weight; rawBullish += current; }
    if (cfg.sentiment === 'bearish') { bearishVolume += current * cfg.weight; rawBearish += current; }
  }

  // Match live pipeline: 30% trend + 40% weighted balance + 30% raw ratio
  const trendScore = totalWeight > 0 ? weightedSum / totalWeight : 50;
  const totalVolume = bullishVolume + bearishVolume;
  const balanceScore = totalVolume > 0 ? (bullishVolume / totalVolume) * 100 : 50;
  const rawTotal = rawBullish + rawBearish;
  const rawRatio = rawTotal > 0 ? (rawBullish / rawTotal) * 100 : 50;
  const compositeScore = Math.round((trendScore * 0.3 + balanceScore * 0.4 + rawRatio * 0.3) * 10) / 10;

  return compositeScore;
}

async function backfill() {
  console.log('Backfilling market_health_history...');

  // Find earliest signal
  const { rows: [earliest] } = await pool.query(
    `SELECT MIN(detected_at) AS min_date FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1)`,
    [TENANT_ID]
  );
  if (!earliest?.min_date) { console.log('No signals found'); return; }

  const startDate = new Date(earliest.min_date);
  startDate.setHours(0, 0, 0, 0);
  const today = new Date();
  today.setHours(0, 0, 0, 0);

  // Delete existing history so we can recompute with new formula
  const { rowCount: deleted } = await pool.query(
    `DELETE FROM market_health_history WHERE tenant_id = $1`,
    [TENANT_ID]
  );
  if (deleted) console.log(`  Cleared ${deleted} existing rows`);

  let inserted = 0;
  const cursor = new Date(startDate);

  while (cursor <= today) {
    const dateStr = cursor.toISOString().substring(0, 10);

    for (const horizon of HORIZONS) {
      // Only backfill if we have enough history for this horizon
      const daysSinceStart = Math.round((cursor - startDate) / 86400000);
      if (daysSinceStart < horizon.days) continue;

      const score = await computeScoreAtDate(cursor, horizon);

      // Compute delta vs previous day
      const prevDate = new Date(cursor);
      prevDate.setDate(prevDate.getDate() - 1);
      const { rows: [prev] } = await pool.query(
        `SELECT score FROM market_health_history WHERE tenant_id = $1 AND horizon = $2 AND snapshot_at::date = $3`,
        [TENANT_ID, horizon.key, prevDate.toISOString().substring(0, 10)]
      );
      const delta = prev ? Math.round(computeDelta(score, prev.score) * 10) / 10 : 0;

      await pool.query(
        `INSERT INTO market_health_history (tenant_id, horizon, score, delta, snapshot_at) VALUES ($1, $2, $3, $4, $5)`,
        [TENANT_ID, horizon.key, score, delta, cursor.toISOString()]
      );
      inserted++;
    }

    cursor.setDate(cursor.getDate() + 1);
    if (inserted % 30 === 0 && inserted > 0) process.stdout.write(`  ${dateStr} (${inserted} rows)...\r`);
  }

  console.log(`\nDone. Recomputed ${inserted} history rows from ${startDate.toISOString().substring(0, 10)} to ${today.toISOString().substring(0, 10)}`);
  await pool.end();
}

backfill().catch(e => { console.error('Backfill error:', e.message); pool.end(); process.exit(1); });
