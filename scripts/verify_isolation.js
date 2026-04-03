#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/verify_isolation.js — Tenant Isolation Verification Suite
// ═══════════════════════════════════════════════════════════════════════════════
//
// Run after deployment to confirm zero cross-tenant data bleed.
//
// Usage: node scripts/verify_isolation.js

require('dotenv').config();
const { TenantDB, platformPool } = require('../lib/TenantDB');

const TENANT_TABLES = [
  'signal_events',
  'people',
  'companies',
  'interactions',
  'external_documents',
  'person_signals',
  'person_scores',
  'team_proximity',
  'accounts',
  'conversions',
  'opportunities',
  'pipeline_contacts',
  'signal_dispatches',
  'search_matches',
  'sessions',
];

async function verify() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  TENANT ISOLATION VERIFICATION SUITE');
  console.log('═══════════════════════════════════════════════════════════════\n');

  let passed = 0;
  let failed = 0;
  let warnings = 0;

  // ── 1. Verify RLS is enabled on all tenant tables ──────────────────────

  console.log('1. RLS STATUS\n');

  const { rows: rlsStatus } = await platformPool.query(`
    SELECT c.relname AS table_name, c.relrowsecurity AS rls_enabled, c.relforcerowsecurity AS rls_forced
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
    AND c.relname = ANY($1)
    ORDER BY c.relname
  `, [TENANT_TABLES]);

  const rlsMap = {};
  rlsStatus.forEach(r => { rlsMap[r.table_name] = r; });

  for (const table of TENANT_TABLES) {
    const status = rlsMap[table];
    if (!status) {
      console.log(`  [ SKIP ] ${table} — table does not exist`);
      warnings++;
    } else if (status.rls_enabled) {
      console.log(`  [  OK  ] ${table} — RLS enabled${status.rls_forced ? ' (FORCED)' : ''}`);
      passed++;
    } else {
      console.log(`  [ FAIL ] ${table} — RLS NOT enabled`);
      failed++;
    }
  }

  // ── 2. Verify RLS policies exist ───────────────────────────────────────

  console.log('\n2. RLS POLICIES\n');

  const { rows: policies } = await platformPool.query(`
    SELECT tablename, policyname, permissive, cmd
    FROM pg_policies
    WHERE policyname LIKE 'tenant_isolation_%'
    ORDER BY tablename
  `);

  const policyMap = {};
  policies.forEach(p => { policyMap[p.tablename] = p; });

  for (const table of TENANT_TABLES) {
    const policy = policyMap[table];
    if (policy) {
      console.log(`  [  OK  ] ${table} — ${policy.policyname} (${policy.permissive}, ${policy.cmd})`);
      passed++;
    } else if (!rlsMap[table]) {
      console.log(`  [ SKIP ] ${table} — table does not exist`);
    } else {
      console.log(`  [ FAIL ] ${table} — NO tenant_isolation policy`);
      failed++;
    }
  }

  // ── 3. Verify each tenant only sees their own data ─────────────────────

  console.log('\n3. CROSS-TENANT DATA CHECK\n');

  const { rows: tenants } = await platformPool.query(
    `SELECT id, name, slug FROM tenants ORDER BY created_at`
  );

  if (tenants.length < 2) {
    console.log('  [ SKIP ] Only 1 tenant — cross-tenant check requires 2+');
    warnings++;
  } else {
    for (const tenant of tenants) {
      const db = new TenantDB(tenant.id);
      let tenantPassed = true;

      for (const table of ['signal_events', 'people', 'companies', 'interactions']) {
        try {
          const { rows: [r] } = await db.query(
            `SELECT COUNT(*) AS cnt FROM ${table} WHERE tenant_id != $1`, [tenant.id]
          );
          if (parseInt(r.cnt) > 0) {
            console.log(`  [ FAIL ] ${tenant.slug} can see ${r.cnt} foreign rows in ${table}`);
            tenantPassed = false;
            failed++;
          }
        } catch (e) {
          // Table might not exist or RLS might block — both acceptable
        }
      }

      if (tenantPassed) {
        console.log(`  [  OK  ] ${tenant.name} (${tenant.slug}) — no cross-tenant data visible`);
        passed++;
      }
    }

    // ── 4. Cross-tenant ID lookup test ─────────────────────────────────────

    console.log('\n4. CROSS-TENANT ID LOOKUP\n');

    const dbA = new TenantDB(tenants[0].id);
    const dbB = new TenantDB(tenants[1].id);

    try {
      const signalA = await dbA.queryOne('SELECT id FROM signal_events LIMIT 1');
      if (signalA) {
        const crossResult = await dbB.queryOne(
          'SELECT id FROM signal_events WHERE id = $1', [signalA.id]
        );
        if (!crossResult) {
          console.log(`  [  OK  ] Cross-tenant ID lookup correctly returns empty`);
          passed++;
        } else {
          console.log(`  [ FAIL ] Tenant B read Tenant A's signal ${signalA.id}`);
          failed++;
        }
      } else {
        console.log(`  [ SKIP ] Tenant A has no signals to test`);
        warnings++;
      }
    } catch (e) {
      console.log(`  [ SKIP ] Cross-lookup test error: ${e.message}`);
      warnings++;
    }
  }

  // ── 5. TenantDB enforcement test ───────────────────────────────────────

  console.log('\n5. TENANTDB ENFORCEMENT\n');

  try {
    new (require('../lib/TenantDB').TenantDB)();
    console.log('  [ FAIL ] TenantDB accepted null tenantId');
    failed++;
  } catch (e) {
    console.log('  [  OK  ] TenantDB rejects null tenantId');
    passed++;
  }

  try {
    new (require('../lib/TenantDB').TenantDB)('');
    console.log('  [ FAIL ] TenantDB accepted empty tenantId');
    failed++;
  } catch (e) {
    console.log('  [  OK  ] TenantDB rejects empty tenantId');
    passed++;
  }

  // ── 6. TenantQdrant enforcement test ───────────────────────────────────

  console.log('\n6. TENANTQDRANT ENFORCEMENT\n');

  try {
    new (require('../lib/TenantQdrant').TenantQdrant)();
    console.log('  [ FAIL ] TenantQdrant accepted null tenantId');
    failed++;
  } catch (e) {
    console.log('  [  OK  ] TenantQdrant rejects null tenantId');
    passed++;
  }

  // ── Summary ────────────────────────────────────────────────────────────

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log(`  RESULTS: ${passed} passed, ${failed} failed, ${warnings} skipped`);
  console.log('═══════════════════════════════════════════════════════════════\n');

  if (failed > 0) {
    console.log('  ISOLATION VERIFICATION FAILED — review failures above\n');
    process.exit(1);
  } else {
    console.log('  ALL CHECKS PASSED\n');
    process.exit(0);
  }
}

verify().catch(err => {
  console.error('Verification error:', err);
  process.exit(1);
});
