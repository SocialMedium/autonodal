#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// orchestrator.js — Multi-Tenant Pipeline Orchestrator
// ═══════════════════════════════════════════════════════════════════════════════
//
// Runs all platform services for all active tenants on schedule.
// Single entry point for all background processing.
//
// Usage:
//   node orchestrator.js                    # Start scheduler daemon
//   node orchestrator.js --run-now          # Run all pipelines once
//   node orchestrator.js --run <service>    # Run single service for all tenants
//   node orchestrator.js --tenant <id> --run <service>  # Run for one tenant

require('dotenv').config();
const cron = require('node-cron');
const { platformPool } = require('./lib/TenantDB');

// Lazy-load services to avoid startup overhead
function loadService(name) {
  switch (name) {
    case 'harvest': return require('./lib/platform/HarvestService').HarvestService;
    case 'embed': return require('./lib/platform/EmbedService').EmbedService;
    case 'signals': return require('./lib/platform/SignalEngine').SignalEngine;
    case 'scores': return require('./lib/platform/ScoreEngine').ScoreEngine;
    case 'proximity': return require('./lib/platform/ProximityEngine').ProximityEngine;
    case 'triangulate': return require('./lib/platform/TriangulateEngine').TriangulateEngine;
    case 'match': return require('./lib/platform/MatchEngine').MatchEngine;
    default: throw new Error(`Unknown service: ${name}`);
  }
}

// ── Tenant Resolution ─────────────────────────────────────────────────────────

async function getActiveTenants() {
  const { rows } = await platformPool.query(`
    SELECT id, slug, name, vertical, status FROM tenants
    WHERE status IN ('active', 'pilot', 'demo')
    ORDER BY created_at ASC
  `);
  return rows;
}

// ── Service Runner ────────────────────────────────────────────────────────────

async function runService(serviceName, tenants, opts = {}) {
  const ServiceClass = loadService(serviceName);
  const label = ServiceClass.name || serviceName;
  const parallel = opts.parallel || false;

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  ${label} — ${tenants.length} tenant(s)`);
  console.log(`${'═'.repeat(60)}`);

  const results = {};

  if (parallel) {
    const settled = await Promise.allSettled(
      tenants.map(async (tenant) => {
        const service = new ServiceClass(tenant.id);
        const result = await service.run();
        return { tenant: tenant.slug, result };
      })
    );
    settled.forEach((s, i) => {
      if (s.status === 'fulfilled') {
        results[tenants[i].slug] = s.value.result;
      } else {
        console.error(`  [${label}][${tenants[i].slug}] FAILED: ${s.reason?.message || s.reason}`);
        results[tenants[i].slug] = { error: s.reason?.message };
      }
    });
  } else {
    for (const tenant of tenants) {
      try {
        const service = new ServiceClass(tenant.id);
        const result = await service.run();
        results[tenant.slug] = result;
      } catch (err) {
        console.error(`  [${label}][${tenant.slug}] FAILED: ${err.message}`);
        results[tenant.slug] = { error: err.message };
      }
    }
  }

  console.log(`  ${label} complete.`);
  return results;
}

// ── Pipeline Definitions ──────────────────────────────────────────────────────

const PIPELINES = {
  harvest:     { schedule: '*/30 * * * *', parallel: false, description: 'RSS + source ingestion' },
  embed:       { schedule: '5 * * * *',    parallel: true,  description: 'Document embedding' },
  signals:     { schedule: '15 * * * *',   parallel: true,  description: 'Signal detection' },
  triangulate: { schedule: '25 * * * *',   parallel: true,  description: 'Cross-signal triangulation' },
  scores:      { schedule: '0 */6 * * *',  parallel: true,  description: 'People scoring' },
  proximity:   { schedule: '30 */6 * * *', parallel: false, description: 'Proximity graph rebuild' },
  match:       { schedule: '0 2 * * *',    parallel: true,  description: 'Opportunity matching' },
};

// ── NIGHTLY JOBS (run outside tenant loop) ────────────────────────────────

async function runNightlyHuddleRecompute() {
  console.log('[Orchestrator] Nightly huddle entry point recompute');
  try {
    const { HuddleEngine } = require('./lib/platform/HuddleEngine');
    const { rows: activeHuddles } = await platformPool.query(
      "SELECT id FROM huddles WHERE status = 'active'"
    );
    const engine = new HuddleEngine();
    for (const h of activeHuddles) {
      try {
        await engine.recomputeBestEntryPoints(h.id);
      } catch (err) {
        console.error('[Huddle] Recompute failed for ' + h.id + ':', err.message);
      }
    }
    console.log('[Orchestrator] Huddle recompute done (' + activeHuddles.length + ' huddles)');
  } catch (err) {
    console.error('[Orchestrator] Huddle recompute error:', err.message);
  }
}

async function runNightlyInfluence() {
  console.log('[Orchestrator] Nightly influence dashboard recompute');
  try {
    const { rows: individuals } = await platformPool.query(
      "SELECT id, slug FROM tenants WHERE tenant_type = 'individual'"
    );
    for (const t of individuals) {
      try {
        await platformPool.query(`
          INSERT INTO individual_influence (
            tenant_id, total_people, active_huddle_count,
            lent_people_unique, computed_at
          )
          SELECT
            $1,
            (SELECT COUNT(*) FROM people WHERE tenant_id = $1),
            (SELECT COUNT(*) FROM huddle_members WHERE tenant_id = $1 AND status = 'active'),
            (SELECT COALESCE(SUM(net_new_people_count), 0) FROM huddle_members WHERE tenant_id = $1 AND status = 'active'),
            NOW()
          ON CONFLICT (tenant_id) DO UPDATE SET
            total_people = EXCLUDED.total_people,
            active_huddle_count = EXCLUDED.active_huddle_count,
            lent_people_unique = EXCLUDED.lent_people_unique,
            computed_at = NOW()
        `, [t.id]);
      } catch (err) {
        console.error('[Influence] Failed for ' + t.slug + ':', err.message);
      }
    }
    console.log('[Orchestrator] Influence done (' + individuals.length + ' individuals)');
  } catch (err) {
    console.error('[Orchestrator] Influence error:', err.message);
  }
}

// ── CLI Handling ──────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--run-now')) {
    // Run all pipelines once, sequentially
    const tenants = await getActiveTenants();
    console.log(`Running all pipelines for ${tenants.length} tenants...\n`);

    for (const [name] of Object.entries(PIPELINES)) {
      try {
        await runService(name, tenants, { parallel: PIPELINES[name].parallel });
      } catch (err) {
        console.error(`Pipeline ${name} failed:`, err.message);
      }
    }

    console.log('\nAll pipelines complete.');
    process.exit(0);
  }

  const runIdx = args.indexOf('--run');
  if (runIdx >= 0) {
    const serviceName = args[runIdx + 1];
    if (!serviceName || !PIPELINES[serviceName]) {
      console.error('Available services:', Object.keys(PIPELINES).join(', '));
      process.exit(1);
    }

    const tenantIdx = args.indexOf('--tenant');
    let tenants;
    if (tenantIdx >= 0 && args[tenantIdx + 1]) {
      const tid = args[tenantIdx + 1];
      tenants = [{ id: tid, slug: tid.substring(0, 8) }];
    } else {
      tenants = await getActiveTenants();
    }

    await runService(serviceName, tenants, { parallel: PIPELINES[serviceName].parallel });
    process.exit(0);
  }

  // ── Scheduler Daemon ──────────────────────────────────────────────────────

  console.log('\n' + '═'.repeat(60));
  console.log('  AUTONODAL ORCHESTRATOR');
  console.log('  Multi-tenant pipeline scheduler');
  console.log('═'.repeat(60));
  console.log('\nPipelines:');
  for (const [name, config] of Object.entries(PIPELINES)) {
    console.log(`  ${name.padEnd(15)} ${config.schedule.padEnd(18)} ${config.description}`);
  }
  console.log('\nWaiting for next tick...\n');

  for (const [name, config] of Object.entries(PIPELINES)) {
    cron.schedule(config.schedule, async () => {
      try {
        const tenants = await getActiveTenants();
        await runService(name, tenants, { parallel: config.parallel });
      } catch (err) {
        console.error(`[Orchestrator] ${name} cron error:`, err.message);
      }
    });
  }

  // Nightly: huddle entry point recompute (1am UTC)
  cron.schedule('0 1 * * *', runNightlyHuddleRecompute);

  // Nightly: individual influence dashboard (3am UTC)
  cron.schedule('0 3 * * *', runNightlyInfluence);
}

main().catch(err => {
  console.error('Orchestrator fatal error:', err);
  process.exit(1);
});
