#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — score people exposed to recent negative signals.
// Writes to person_scores with category = 'talent_refresh'.
// Usage: node scripts/score_talent_refresh.js [--dry-run]
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { talentRefreshScore } = require('../lib/talent_refresh_scoring');

const DRY_RUN = process.argv.includes('--dry-run');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

const ML_TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

async function run() {
  console.log(`Scoring talent refresh${DRY_RUN ? ' [DRY RUN]' : ''}...`);

  // Negative signals in last 180 days, with a linked company
  const { rows: signals } = await pool.query(`
    SELECT se.id, se.signal_type, se.polarity, se.company_id,
           COALESCE(se.first_detected_at, se.detected_at) AS first_detected_at
    FROM signal_events se
    WHERE (se.tenant_id IS NULL OR se.tenant_id = $1)
      AND se.polarity = 'negative'
      AND se.company_id IS NOT NULL
      AND se.detected_at > NOW() - INTERVAL '180 days'
  `, [ML_TENANT_ID]);

  console.log(`  ${signals.length} negative signals in window`);
  if (!signals.length) { await pool.end(); return; }

  const companyIds = [...new Set(signals.map(s => s.company_id))];

  // People at those companies, with proximity
  const { rows: people } = await pool.query(`
    SELECT p.id AS person_id, p.seniority_level, p.current_company_id,
           tp.user_id, tp.proximity_strength
    FROM people p
    JOIN team_proximity tp ON tp.person_id = p.id AND tp.tenant_id = $1
    WHERE p.current_company_id = ANY($2::uuid[])
      AND p.tenant_id = $1
      AND tp.proximity_strength >= 0.15
  `, [ML_TENANT_ID, companyIds]);

  console.log(`  ${people.length} person-consultant proximity edges`);

  // For each person × signal pair, compute score
  let written = 0;
  const signalsByCompany = {};
  for (const s of signals) {
    if (!signalsByCompany[s.company_id]) signalsByCompany[s.company_id] = [];
    signalsByCompany[s.company_id].push(s);
  }

  for (const edge of people) {
    const companySignals = signalsByCompany[edge.current_company_id] || [];
    // Take the most recent negative signal per company-person pair
    const best = companySignals[0];
    if (!best) continue;

    const score = talentRefreshScore(
      { seniority_level: edge.seniority_level },
      best,
      parseFloat(edge.proximity_strength)
    );
    if (score === null || score <= 0) continue;

    if (!DRY_RUN) {
      // Store in person_scores — table may vary across deployments; upsert best-effort
      try {
        await pool.query(`
          INSERT INTO person_scores (person_id, user_id, category, score, tenant_id, computed_at)
          VALUES ($1, $2, 'talent_refresh', $3, $4, NOW())
          ON CONFLICT (person_id, user_id, category) DO UPDATE SET
            score = EXCLUDED.score, computed_at = NOW()
        `, [edge.person_id, edge.user_id, score, ML_TENANT_ID]);
        written++;
      } catch (e) {
        // Table may lack unique constraint or category column — log once and continue
        if (written === 0) console.warn('  person_scores upsert failed (table shape?):', e.message);
      }
    } else {
      written++;
    }
  }

  console.log(`\n  ${written} scores ${DRY_RUN ? 'would be written' : 'written'}`);
  await pool.end();
}

run().catch(e => { console.error('Error:', e.message); pool.end(); process.exit(1); });
