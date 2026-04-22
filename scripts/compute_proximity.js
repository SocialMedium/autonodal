#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// Nightly — recompute team_proximity composite (4-factor) edges for every tenant.
// Writes a dedicated relationship_type='composite' row per (person, member, tenant)
// alongside the existing per-channel edges. Bulk CTE + UNNEST upserts — no per-row
// round trips.
//
// Usage:
//   node scripts/compute_proximity.js                    # all tenants
//   node scripts/compute_proximity.js --tenant=<uuid>    # single tenant
//   node scripts/compute_proximity.js --dry-run          # no writes
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');
const { composeProximity } = require('../lib/scoring/proximity');

const DRY_RUN = process.argv.includes('--dry-run');
const tenantIdx = process.argv.indexOf('--tenant');
const SINGLE_TENANT = tenantIdx !== -1 ? process.argv[tenantIdx + 1] : null;
const BATCH_SIZE = 2000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false },
});

// Map raw interaction rows into channel bucket used by the scoring module
function channelOf(row) {
  const t = row.interaction_type || '';
  const c = row.channel || '';
  const dur = row.duration_minutes || 0;
  if (t === 'meeting') return dur > 0 ? 'in_person_meeting' : 'video_call';
  if (t === 'phone_call' || c === 'phone') return 'phone_call';
  if (t === 'email' || t === 'email_sent' || t === 'email_received' || c === 'email') {
    return row._reciprocal ? 'email_reciprocal' : 'email_one_way';
  }
  if (t === 'linkedin_message' || c === 'linkedin') return 'linkedin_message';
  if (t === 'linkedin_reaction') return 'linkedin_reaction';
  if (t === 'research_note' || t === 'note') return 'research_note';
  return 'unknown';
}

async function recomputeTenant(tenantId) {
  // Pull pre-aggregated per-(person, user, channel) stats in one shot.
  // We aggregate at the channel level so Node only receives ~O(edges × channels) rows.
  // Email reciprocity is computed here too: a (person,user) pair is treated as reciprocal
  // if it has both inbound and outbound emails in the last 18 months.
  const { rows: emailRecip } = await pool.query(`
    SELECT person_id, user_id
    FROM interactions
    WHERE tenant_id = $1
      AND person_id IS NOT NULL AND user_id IS NOT NULL
      AND (interaction_type IN ('email','email_sent','email_received') OR channel = 'email')
      AND interaction_at > NOW() - INTERVAL '18 months'
    GROUP BY person_id, user_id
    HAVING COUNT(*) FILTER (WHERE direction = 'inbound' OR interaction_type = 'email_received') > 0
       AND COUNT(*) FILTER (WHERE direction = 'outbound' OR interaction_type = 'email_sent') > 0
  `, [tenantId]);
  const recipSet = new Set(emailRecip.map(r => r.person_id + ':' + r.user_id));

  // Per-channel counts in last 12 months + overall first/last + in/out totals
  const { rows } = await pool.query(`
    WITH pair_agg AS (
      SELECT
        i.person_id,
        i.user_id,
        MIN(i.interaction_at) AS first_at,
        MAX(i.interaction_at) AS last_at,
        (ARRAY_AGG(i.interaction_type ORDER BY i.interaction_at DESC))[1] AS last_type,
        (ARRAY_AGG(i.channel           ORDER BY i.interaction_at DESC))[1] AS last_channel,
        (ARRAY_AGG(i.duration_minutes  ORDER BY i.interaction_at DESC))[1] AS last_duration,
        COUNT(*) FILTER (WHERE i.direction = 'inbound'  OR i.interaction_type = 'email_received') AS inbound_12mo,
        COUNT(*) FILTER (WHERE i.direction = 'outbound' OR i.interaction_type = 'email_sent')     AS outbound_12mo,
        -- channel counts in last 12 months
        COUNT(*) FILTER (WHERE i.interaction_type = 'meeting' AND i.duration_minutes > 0 AND i.interaction_at > NOW() - INTERVAL '12 months') AS meeting_in_person_12mo,
        COUNT(*) FILTER (WHERE i.interaction_type = 'meeting' AND (i.duration_minutes IS NULL OR i.duration_minutes = 0) AND i.interaction_at > NOW() - INTERVAL '12 months') AS meeting_video_12mo,
        COUNT(*) FILTER (WHERE (i.interaction_type IN ('email','email_sent','email_received') OR i.channel = 'email') AND i.interaction_at > NOW() - INTERVAL '12 months') AS email_12mo,
        COUNT(*) FILTER (WHERE (i.interaction_type = 'linkedin_message' OR i.channel = 'linkedin') AND i.interaction_at > NOW() - INTERVAL '12 months') AS linkedin_12mo,
        COUNT(*) FILTER (WHERE i.interaction_type = 'research_note' AND i.interaction_at > NOW() - INTERVAL '12 months') AS research_note_12mo,
        COUNT(*) FILTER (WHERE i.interaction_type = 'phone_call' AND i.interaction_at > NOW() - INTERVAL '12 months') AS phone_12mo
      FROM interactions i
      WHERE i.tenant_id = $1
        AND i.person_id IS NOT NULL
        AND i.user_id IS NOT NULL
      GROUP BY i.person_id, i.user_id
    )
    SELECT * FROM pair_agg
  `, [tenantId]);

  if (!rows.length) return { tenant_id: tenantId, edges: 0 };

  const now = new Date();
  const updates = [];
  for (const r of rows) {
    const key = r.person_id + ':' + r.user_id;
    const emailReciprocal = recipSet.has(key);

    const counts12mo = {
      in_person_meeting: parseInt(r.meeting_in_person_12mo) || 0,
      video_call:        parseInt(r.meeting_video_12mo)     || 0,
      phone_call:        parseInt(r.phone_12mo)             || 0,
      research_note:     parseInt(r.research_note_12mo)     || 0,
      linkedin_message:  parseInt(r.linkedin_12mo)          || 0,
    };
    const emailN = parseInt(r.email_12mo) || 0;
    if (emailReciprocal) counts12mo.email_reciprocal = emailN;
    else counts12mo.email_one_way = emailN;

    const lastChannel = channelOf({
      interaction_type: r.last_type,
      channel:          r.last_channel,
      duration_minutes: r.last_duration,
      _reciprocal:      emailReciprocal,
    });

    const { score, factors } = composeProximity({
      lastContactAt:  r.last_at,
      firstContactAt: r.first_at,
      lastChannel,
      counts12mo,
      inbound12mo:    parseInt(r.inbound_12mo)  || 0,
      outbound12mo:   parseInt(r.outbound_12mo) || 0,
      now,
    });

    updates.push({
      person_id:  r.person_id,
      team_member_id: r.user_id,
      tenant_id:  tenantId,
      strength:   score,
      factors:    JSON.stringify(factors),
      currency:   factors.currency.score,
      history:    factors.history.score,
      weight:     factors.weight.score,
      reciprocity:factors.reciprocity.score,
      first_at:   r.first_at,
      last_at:    r.last_at,
      last_channel: lastChannel,
      inbound:    parseInt(r.inbound_12mo)  || 0,
      outbound:   parseInt(r.outbound_12mo) || 0,
    });
  }

  if (DRY_RUN) return { tenant_id: tenantId, edges: updates.length, written: 0 };

  let written = 0;
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    const res = await pool.query(
      `INSERT INTO team_proximity (
         person_id, team_member_id, tenant_id, relationship_type, relationship_strength,
         score_factors, currency_score, history_score, weight_score, reciprocity_score,
         first_interaction_at, last_interaction_at, last_interaction_channel,
         interaction_count_inbound, interaction_count_outbound,
         source, last_computed_at, updated_at
       )
       SELECT person_id, team_member_id, tenant_id, 'composite', strength,
              factors::jsonb, currency, history, weight, reciprocity,
              first_at, last_at, last_channel, inbound, outbound,
              'compute_proximity', NOW(), NOW()
       FROM UNNEST(
         $1::uuid[], $2::uuid[], $3::uuid[],
         $4::float8[], $5::text[], $6::numeric[], $7::numeric[], $8::numeric[], $9::numeric[],
         $10::timestamptz[], $11::timestamptz[], $12::text[], $13::int[], $14::int[]
       ) AS t(
         person_id, team_member_id, tenant_id,
         strength, factors, currency, history, weight, reciprocity,
         first_at, last_at, last_channel, inbound, outbound
       )
       ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
         relationship_strength = EXCLUDED.relationship_strength,
         score_factors = EXCLUDED.score_factors,
         currency_score = EXCLUDED.currency_score,
         history_score = EXCLUDED.history_score,
         weight_score = EXCLUDED.weight_score,
         reciprocity_score = EXCLUDED.reciprocity_score,
         first_interaction_at = EXCLUDED.first_interaction_at,
         last_interaction_at = EXCLUDED.last_interaction_at,
         last_interaction_channel = EXCLUDED.last_interaction_channel,
         interaction_count_inbound = EXCLUDED.interaction_count_inbound,
         interaction_count_outbound = EXCLUDED.interaction_count_outbound,
         last_computed_at = NOW(),
         updated_at = NOW()`,
      [
        batch.map(b => b.person_id),
        batch.map(b => b.team_member_id),
        batch.map(b => b.tenant_id),
        batch.map(b => b.strength),
        batch.map(b => b.factors),
        batch.map(b => b.currency),
        batch.map(b => b.history),
        batch.map(b => b.weight),
        batch.map(b => b.reciprocity),
        batch.map(b => b.first_at),
        batch.map(b => b.last_at),
        batch.map(b => b.last_channel),
        batch.map(b => b.inbound),
        batch.map(b => b.outbound),
      ]
    );
    written += res.rowCount || batch.length;
    process.stdout.write(`    composite upserts: ${Math.min(i + BATCH_SIZE, updates.length).toLocaleString()}/${updates.length.toLocaleString()}\r`);
  }
  if (updates.length) process.stdout.write('\n');
  return { tenant_id: tenantId, edges: updates.length, written };
}

async function run() {
  console.log(`Computing 4-factor proximity${DRY_RUN ? ' [DRY RUN]' : ''}...`);
  const start = Date.now();

  let tenants;
  if (SINGLE_TENANT) {
    tenants = [{ id: SINGLE_TENANT }];
  } else {
    const { rows } = await pool.query('SELECT id, name FROM tenants WHERE id IS NOT NULL');
    tenants = rows;
  }
  console.log(`  Tenants: ${tenants.length}`);

  const results = [];
  for (const t of tenants) {
    const started = Date.now();
    try {
      const r = await recomputeTenant(t.id);
      const ms = Date.now() - started;
      console.log(`  [${t.name || t.id.slice(0, 8)}] ${r.edges.toLocaleString()} edges, ${r.written?.toLocaleString() ?? 0} written in ${(ms/1000).toFixed(1)}s`);
      results.push(r);
    } catch (e) {
      console.error(`  [${t.name || t.id.slice(0, 8)}] ERROR:`, e.message);
    }
  }

  const total = results.reduce((s, r) => s + r.edges, 0);
  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n  Total edges: ${total.toLocaleString()}, elapsed ${elapsed}s`);

  await pool.end();
}

if (require.main === module) {
  run().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
}

module.exports = { recomputeTenant };
