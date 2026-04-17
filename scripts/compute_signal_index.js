#!/usr/bin/env node
// ============================================================================
// Signal Index — Market Health Computation
// Computes signal stocks, composite index, sector indices, history
// Runs every hour via scheduler or manually
// ============================================================================

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

// ── Signal stock configuration ──────────────────────────────────────
// Maps our 9 actual signal_types to stocks with sentiment and weight
const SIGNAL_STOCKS = {
  capital_raising:      { sentiment: 'bullish',  weight: 3.0, label: 'Capital Raise' },
  ma_activity:          { sentiment: 'neutral',  weight: 2.5, label: 'M&A' },
  product_launch:       { sentiment: 'bullish',  weight: 2.0, label: 'Product Launch' },
  leadership_change:    { sentiment: 'neutral',  weight: 1.5, label: 'Leadership' },
  strategic_hiring:     { sentiment: 'bullish',  weight: 1.0, label: 'Hiring' },
  geographic_expansion: { sentiment: 'bullish',  weight: 1.5, label: 'Expansion' },
  partnership:          { sentiment: 'bullish',  weight: 1.2, label: 'Partnership' },
  layoffs:              { sentiment: 'bearish',  weight: 2.0, label: 'Layoffs' },
  restructuring:        { sentiment: 'bearish',  weight: 2.5, label: 'Restructuring' },
};
// media_sentiment is computed separately from document_sentiment table, not signal_events
const MEDIA_SENTIMENT_CFG = { sentiment: 'bullish', weight: 1.5, label: 'Media' };

const HORIZONS = [
  { key: '7d',  days: 7,   priorDays: 14  },
  { key: '30d', days: 30,  priorDays: 60  },
  { key: '90d', days: 90,  priorDays: 180 },
];

function computeDelta(current, prior) {
  // Use smoothed comparison to avoid extreme swings from small bases
  // Add a floor of 5 to the denominator so 0→50 doesn't show as +∞
  const smoothedPrior = Math.max(prior, 5);
  const raw = ((current - prior) / smoothedPrior) * 100;
  // Cap at ±99% — prevents absurd numbers from young data
  return Math.min(99, Math.max(-99, raw));
}

function deltaToScore(delta, sentiment) {
  // Map delta to a 0-100 score:
  // Bullish rising → high score. Bearish rising → low score.
  // Scale: ±30% delta moves score by ±20 points from 50
  const dir = sentiment === 'bullish' ? 1 : sentiment === 'bearish' ? -1 : 0.3;
  const raw = 50 + (dir * delta * 0.65);
  return Math.min(100, Math.max(0, Math.round(raw * 10) / 10));
}

function getDirection(delta) {
  return delta > 5 ? 'up' : delta < -5 ? 'down' : 'flat';
}

async function ensureSchema() {
  const tables = [
    `CREATE TABLE IF NOT EXISTS signal_stocks (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, stock_name VARCHAR(100) NOT NULL, sentiment VARCHAR(10) NOT NULL, weight FLOAT NOT NULL DEFAULT 1.0, horizon VARCHAR(10) NOT NULL, current_count INT DEFAULT 0, prior_count INT DEFAULT 0, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', score FLOAT NOT NULL DEFAULT 50, computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, stock_name, horizon))`,
    `CREATE TABLE IF NOT EXISTS market_health_index (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', bullish_count INT DEFAULT 0, bearish_count INT DEFAULT 0, dominant_signal VARCHAR(100), computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, horizon))`,
    `CREATE TABLE IF NOT EXISTS sector_indices (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, sector VARCHAR(100) NOT NULL, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL DEFAULT 50, delta FLOAT NOT NULL DEFAULT 0, direction VARCHAR(10) NOT NULL DEFAULT 'flat', signal_count INT DEFAULT 0, company_count INT DEFAULT 0, computed_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(tenant_id, sector, horizon))`,
    `CREATE TABLE IF NOT EXISTS market_health_history (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID, horizon VARCHAR(10) NOT NULL, score FLOAT NOT NULL, delta FLOAT NOT NULL DEFAULT 0, snapshot_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS signal_index_stats (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id UUID UNIQUE, people_tracked INT DEFAULT 0, companies_tracked INT DEFAULT 0, signals_7d INT DEFAULT 0, signals_30d INT DEFAULT 0, computed_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE INDEX IF NOT EXISTS idx_signal_stocks_tenant ON signal_stocks(tenant_id, horizon)`,
    `CREATE INDEX IF NOT EXISTS idx_market_health_tenant ON market_health_index(tenant_id, horizon)`,
    `CREATE INDEX IF NOT EXISTS idx_sector_indices_tenant ON sector_indices(tenant_id, sector, horizon)`,
    `CREATE INDEX IF NOT EXISTS idx_market_health_history ON market_health_history(tenant_id, horizon, snapshot_at)`,
  ];
  for (const sql of tables) {
    try { await pool.query(sql); } catch (e) { /* already exists */ }
  }
}

async function computeSignalIndex() {
  await ensureSchema();
  console.log('  📈 Computing signal index...');

  for (const horizon of HORIZONS) {
    const stockResults = {};

    // 1. Compute each signal stock using multi-bucket trend analysis
    //    Split the lookback into equal buckets and fit a linear trend
    const BUCKETS = 6; // 6 buckets for trend calculation
    for (const [signalType, cfg] of Object.entries(SIGNAL_STOCKS)) {
      const bucketDays = Math.ceil(horizon.priorDays / BUCKETS);

      // Get counts per bucket (most recent bucket = bucket 0)
      const { rows: buckets } = await pool.query(`
        SELECT
          FLOOR(EXTRACT(EPOCH FROM (NOW() - detected_at)) / (86400 * $1))::int AS bucket,
          COUNT(*)::int AS cnt
        FROM signal_events
        WHERE signal_type = $2 AND (tenant_id IS NULL OR tenant_id = $3)
          AND detected_at > NOW() - ($4 || ' days')::INTERVAL
        GROUP BY bucket
        ORDER BY bucket
      `, [bucketDays, signalType, TENANT_ID, horizon.priorDays]);

      // Build array of counts per bucket (index 0 = oldest, N-1 = most recent)
      const counts = new Array(BUCKETS).fill(0);
      for (const b of buckets) {
        const idx = BUCKETS - 1 - Math.min(b.bucket, BUCKETS - 1);
        counts[idx] += parseInt(b.cnt);
      }

      const current = counts[BUCKETS - 1]; // most recent bucket
      const total = counts.reduce((a, b) => a + b, 0);
      const avg = total / BUCKETS;

      // Linear regression slope across buckets (normalised)
      let sumXY = 0, sumX = 0, sumY = 0, sumX2 = 0;
      for (let i = 0; i < BUCKETS; i++) {
        sumX += i; sumY += counts[i]; sumXY += i * counts[i]; sumX2 += i * i;
      }
      const slope = (BUCKETS * sumXY - sumX * sumY) / (BUCKETS * sumX2 - sumX * sumX);
      // Normalise slope as % of average (how fast is it changing relative to baseline)
      const delta = avg > 0 ? Math.min(99, Math.max(-99, (slope / avg) * 100)) : (current > 0 ? 10 : 0);
      const score = deltaToScore(delta, cfg.sentiment);
      const dir = getDirection(delta);

      const prior = counts[BUCKETS - 2] || 0; // second-most-recent bucket
      stockResults[signalType] = { delta, score, direction: dir, current_count: current, prior_count: prior };

      await pool.query(`
        INSERT INTO signal_stocks (tenant_id, stock_name, sentiment, weight, horizon, current_count, prior_count, delta, direction, score, computed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
        ON CONFLICT (tenant_id, stock_name, horizon) DO UPDATE SET
          current_count = EXCLUDED.current_count, prior_count = EXCLUDED.prior_count,
          delta = EXCLUDED.delta, direction = EXCLUDED.direction, score = EXCLUDED.score, computed_at = NOW()
      `, [TENANT_ID, signalType, cfg.sentiment, cfg.weight, horizon.key, current, prior, Math.round(delta * 10) / 10, dir, score]);
    }

    // 1b. Media Sentiment stock — derived from document_sentiment table
    try {
      const { rows: [sentCounts] } = await pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE ds.sentiment = 'bullish')::int AS bullish,
          COUNT(*) FILTER (WHERE ds.sentiment = 'bearish')::int AS bearish,
          COUNT(*) FILTER (WHERE ds.sentiment = 'neutral')::int AS neutral,
          COUNT(*)::int AS total
        FROM document_sentiment ds
        JOIN external_documents ed ON ed.id = ds.document_id
        WHERE ds.computed_at > NOW() - ($1 || ' days')::INTERVAL
      `, [horizon.days]);

      const total = sentCounts?.total || 0;
      const bullish = sentCounts?.bullish || 0;
      const bearish = sentCounts?.bearish || 0;
      // Sentiment score: 100 = all bullish, 0 = all bearish, 50 = balanced
      const sentScore = total > 0 ? Math.round(((bullish - bearish) / total * 50 + 50) * 10) / 10 : 50;
      const sentDelta = total > 2 ? Math.round(((bullish - bearish) / Math.max(total, 1)) * 100) : 0;

      stockResults['media_sentiment'] = {
        delta: sentDelta, score: sentScore, direction: getDirection(sentDelta),
        current_count: total, prior_count: 0
      };

      await pool.query(`
        INSERT INTO signal_stocks (tenant_id, stock_name, sentiment, weight, horizon, current_count, prior_count, delta, direction, score, computed_at)
        VALUES ($1, 'media_sentiment', 'bullish', 1.5, $2, $3, 0, $4, $5, $6, NOW())
        ON CONFLICT (tenant_id, stock_name, horizon) DO UPDATE SET
          current_count = EXCLUDED.current_count, delta = EXCLUDED.delta, direction = EXCLUDED.direction, score = EXCLUDED.score, computed_at = NOW()
      `, [TENANT_ID, horizon.key, total, sentDelta, getDirection(sentDelta), sentScore]);
    } catch (e) { /* document_sentiment table may not exist yet */ }

    // 2. Compute composite Market Health Index
    // Three components:
    //   (a) Delta-based trend scores (is each signal type rising or falling?)
    //   (b) Absolute sentiment balance (how many bullish vs bearish signals exist right now?)
    //   (c) Volume-weighted sentiment ratio
    let weightedSum = 0, totalWeight = 0, bullishCount = 0, bearishCount = 0;
    let bullishVolume = 0, bearishVolume = 0;
    let dominantStock = null, dominantContrib = 0;

    for (const [name, data] of Object.entries(stockResults)) {
      const cfg = SIGNAL_STOCKS[name] || (name === 'media_sentiment' ? MEDIA_SENTIMENT_CFG : null);
      if (!cfg) continue;
      const contrib = data.score * cfg.weight;
      weightedSum += contrib;
      totalWeight += cfg.weight;

      // Track volume-weighted sentiment
      if (cfg.sentiment === 'bullish') bullishVolume += (data.current_count || 0) * cfg.weight;
      else if (cfg.sentiment === 'bearish') bearishVolume += (data.current_count || 0) * cfg.weight;

      const absContrib = Math.abs(data.score - 50) * cfg.weight;
      if (absContrib > dominantContrib) { dominantContrib = absContrib; dominantStock = name; }

      if (cfg.sentiment === 'bullish' && data.direction === 'up') bullishCount++;
      if (cfg.sentiment === 'bearish' && data.direction === 'up') bearishCount++;
    }

    // (a) Trend score — are signal types accelerating or decelerating?
    const trendScore = totalWeight > 0 ? weightedSum / totalWeight : 50;

    // (b) Absolute balance — right now, what's the ratio of bullish to bearish?
    //     100 expansion + 10 layoffs = 91% bullish → score ~82
    //     50 expansion + 50 layoffs = 50% bullish → score 50
    const totalVolume = bullishVolume + bearishVolume;
    const balanceScore = totalVolume > 0 ? (bullishVolume / totalVolume) * 100 : 50;

    // (c) Volume ratio (same as balance but unweighted by signal weight)
    const rawBullish = Object.entries(stockResults).reduce((s, [n, d]) => {
      const cfg = SIGNAL_STOCKS[n] || (n === 'media_sentiment' ? MEDIA_SENTIMENT_CFG : null);
      return s + (cfg?.sentiment === 'bullish' ? (d.current_count || 0) : 0);
    }, 0);
    const rawBearish = Object.entries(stockResults).reduce((s, [n, d]) => {
      const cfg = SIGNAL_STOCKS[n] || (n === 'media_sentiment' ? MEDIA_SENTIMENT_CFG : null);
      return s + (cfg?.sentiment === 'bearish' ? (d.current_count || 0) : 0);
    }, 0);
    const rawTotal = rawBullish + rawBearish;
    const rawRatio = rawTotal > 0 ? (rawBullish / rawTotal) * 100 : 50;

    // Blend: 30% trend (rate of change) + 40% weighted balance + 30% raw ratio
    // This ensures a consistently healthy market (many expansions, few layoffs)
    // scores high even when volumes are stable week-over-week
    const compositeScore = Math.round((trendScore * 0.3 + balanceScore * 0.4 + rawRatio * 0.3) * 10) / 10;

    // Prior composite for delta
    const { rows: priorRows } = await pool.query(`
      SELECT score FROM market_health_history
      WHERE tenant_id = $1 AND horizon = $2
      ORDER BY snapshot_at DESC OFFSET 1 LIMIT 1
    `, [TENANT_ID, horizon.key]);
    const compositeDelta = priorRows.length > 0 ? computeDelta(compositeScore, priorRows[0].score) : 0;

    await pool.query(`
      INSERT INTO market_health_index (tenant_id, horizon, score, delta, direction, bullish_count, bearish_count, dominant_signal, computed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
      ON CONFLICT (tenant_id, horizon) DO UPDATE SET
        score = EXCLUDED.score, delta = EXCLUDED.delta, direction = EXCLUDED.direction,
        bullish_count = EXCLUDED.bullish_count, bearish_count = EXCLUDED.bearish_count,
        dominant_signal = EXCLUDED.dominant_signal, computed_at = NOW()
    `, [TENANT_ID, horizon.key, compositeScore, Math.round(compositeDelta * 10) / 10, getDirection(compositeDelta), bullishCount, bearishCount, dominantStock]);

    // 3. Append to history
    await pool.query(`INSERT INTO market_health_history (tenant_id, horizon, score, delta, snapshot_at) VALUES ($1,$2,$3,$4,NOW())`,
      [TENANT_ID, horizon.key, compositeScore, Math.round(compositeDelta * 10) / 10]);

    // 4. Sector indices (by company sector)
    const { rows: sectors } = await pool.query(`
      SELECT COALESCE(c.sector, 'Unknown') AS sector,
        COUNT(*) FILTER (WHERE se.detected_at > NOW() - ($1 || ' days')::INTERVAL)::int AS current_count,
        COUNT(*) FILTER (WHERE se.detected_at BETWEEN NOW() - ($2 || ' days')::INTERVAL AND NOW() - ($1 || ' days')::INTERVAL)::int AS prior_count,
        COUNT(DISTINCT se.company_id) AS company_count
      FROM signal_events se
      LEFT JOIN companies c ON c.id = se.company_id
      WHERE (se.tenant_id = $3 OR se.tenant_id IS NULL) AND se.detected_at > NOW() - ($2 || ' days')::INTERVAL
      GROUP BY COALESCE(c.sector, 'Unknown')
      HAVING COUNT(*) >= 3
      ORDER BY current_count DESC LIMIT 20
    `, [horizon.days, horizon.priorDays, TENANT_ID]);

    for (const sec of sectors) {
      const sd = computeDelta(sec.current_count, sec.prior_count);
      const ss = Math.min(100, Math.max(0, Math.round((50 + sd / 3) * 10) / 10));
      await pool.query(`
        INSERT INTO sector_indices (tenant_id, sector, horizon, score, delta, direction, signal_count, company_count, computed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT (tenant_id, sector, horizon) DO UPDATE SET
          score = EXCLUDED.score, delta = EXCLUDED.delta, direction = EXCLUDED.direction,
          signal_count = EXCLUDED.signal_count, company_count = EXCLUDED.company_count, computed_at = NOW()
      `, [TENANT_ID, sec.sector, horizon.key, ss, Math.round(sd * 10) / 10, getDirection(sd), sec.current_count, sec.company_count]);
    }

    console.log(`     ${horizon.key}: index=${compositeScore} (${getDirection(compositeDelta)}), ${Object.keys(stockResults).length} stocks, ${sectors.length} sectors`);
  }

  // 5. Platform stats
  const { rows: [stats] } = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS people,
      (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS companies,
      (SELECT COUNT(*) FROM signal_events WHERE (tenant_id = $1 OR tenant_id IS NULL) AND detected_at > NOW() - INTERVAL '7 days') AS s7d,
      (SELECT COUNT(*) FROM signal_events WHERE (tenant_id = $1 OR tenant_id IS NULL) AND detected_at > NOW() - INTERVAL '30 days') AS s30d
  `, [TENANT_ID]);

  await pool.query(`
    INSERT INTO signal_index_stats (tenant_id, people_tracked, companies_tracked, signals_7d, signals_30d, computed_at)
    VALUES ($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (tenant_id) DO UPDATE SET
      people_tracked = EXCLUDED.people_tracked, companies_tracked = EXCLUDED.companies_tracked,
      signals_7d = EXCLUDED.signals_7d, signals_30d = EXCLUDED.signals_30d, computed_at = NOW()
  `, [TENANT_ID, parseInt(stats.people) || 0, parseInt(stats.companies) || 0, parseInt(stats.s7d) || 0, parseInt(stats.s30d) || 0]);

  console.log(`  ✅ Signal index complete — ${stats.people} people, ${stats.companies} companies, ${stats.s7d} signals/7d`);
  return { people: stats.people, companies: stats.companies, signals_7d: stats.s7d };
}

if (require.main === module) {
  computeSignalIndex().then(() => pool.end()).catch(e => { console.error('Fatal:', e); process.exit(1); });
}

module.exports = { computeSignalIndex };
