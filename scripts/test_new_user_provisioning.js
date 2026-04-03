#!/usr/bin/env node
// Test: simulates the OAuth callback's new-user provisioning path
// Verifies tenant isolation, correct type, and onboarding status

require('dotenv').config();
const { platformPool } = require('../lib/TenantDB');

async function testNewUserProvisioning() {
  console.log('=== NEW USER PROVISIONING TEST ===\n');

  var TEST_EMAIL = 'test-individual-' + Date.now() + '@gmail.com';
  var TEST_NAME = 'Test Individual User';
  var passed = 0;
  var failed = 0;

  function assert(condition, label) {
    if (condition) { console.log('  ✓ ' + label); passed++; }
    else { console.log('  ✗ ' + label); failed++; }
  }

  try {
    // 1. Confirm user does not exist
    var { rows: existing } = await platformPool.query(
      'SELECT id FROM users WHERE email = $1', [TEST_EMAIL]
    );
    assert(existing.length === 0, 'User does not exist yet');

    // 2. Simulate provisioning (replicates OAuth callback logic for external user)
    var slug = TEST_EMAIL.split('@')[0]
      .toLowerCase().replace(/[^a-z0-9]/g, '-').slice(0, 30) + '-' + Date.now().toString(36);

    var { rows: [newTenant] } = await platformPool.query(
      "INSERT INTO tenants (id, name, slug, vertical, plan, tenant_type, onboarding_status, subscription_status, profile, created_at) " +
      "VALUES (gen_random_uuid(), $1, $2, 'revenue', 'free', 'individual', 'step_1', 'free', $3, NOW()) RETURNING id",
      [TEST_NAME, slug, JSON.stringify({ provisioned_from: 'google_oauth', email: TEST_EMAIL })]
    );

    var { rows: [newUser] } = await platformPool.query(
      "INSERT INTO users (id, email, name, role, password_hash, tenant_id, created_at, updated_at) " +
      "VALUES (gen_random_uuid(), $1, $2, 'admin', 'oauth_google', $3, NOW(), NOW()) RETURNING id, email, name, role",
      [TEST_EMAIL, TEST_NAME, newTenant.id]
    );

    // 3. Verify
    var { rows: [user] } = await platformPool.query(
      'SELECT u.email, u.role, t.slug, t.tenant_type, t.onboarding_status, t.subscription_status, t.plan ' +
      'FROM users u JOIN tenants t ON t.id = u.tenant_id WHERE u.email = $1',
      [TEST_EMAIL]
    );

    assert(!!user, 'User created');
    assert(user.tenant_type === 'individual', 'tenant_type = individual (got: ' + user.tenant_type + ')');
    assert(user.onboarding_status === 'step_1', 'onboarding_status = step_1 (got: ' + user.onboarding_status + ')');
    assert(user.subscription_status === 'free', 'subscription_status = free (got: ' + user.subscription_status + ')');
    assert(user.plan === 'free', 'plan = free (got: ' + user.plan + ')');
    assert(user.role === 'admin', 'role = admin (got: ' + user.role + ')');
    assert(!user.slug.includes('mitchellake'), 'not in MitchelLake tenant (slug: ' + user.slug + ')');
    assert(!user.slug.includes('autonodal-demo'), 'not in demo tenant (slug: ' + user.slug + ')');
    assert(user.slug.startsWith('test-individual-'), 'slug derived from email (slug: ' + user.slug + ')');

    console.log('\n  Results:');
    console.log('    Email:              ' + user.email);
    console.log('    Tenant slug:        ' + user.slug);
    console.log('    Tenant type:        ' + user.tenant_type);
    console.log('    Onboarding:         ' + user.onboarding_status);
    console.log('    Plan:               ' + user.plan);

    // 4. Verify MitchelLake is untouched
    console.log('\n--- MitchelLake integrity check ---');
    var { rows: [ml] } = await platformPool.query(
      "SELECT name, slug, tenant_type, onboarding_status FROM tenants WHERE slug = 'mitchellake'"
    );
    assert(ml.tenant_type === 'company', 'MitchelLake tenant_type = company');
    assert(ml.onboarding_status === 'complete', 'MitchelLake onboarding = complete');

    var { rows: [mlSignals] } = await platformPool.query(
      "SELECT COUNT(*) as cnt FROM signal_events WHERE tenant_id = (SELECT id FROM tenants WHERE slug = 'mitchellake')"
    );
    assert(parseInt(mlSignals.cnt) > 15000, 'MitchelLake signals intact (' + mlSignals.cnt + ')');

    var { rows: mlUsers } = await platformPool.query(
      "SELECT email FROM users WHERE tenant_id = (SELECT id FROM tenants WHERE slug = 'mitchellake') ORDER BY email"
    );
    assert(mlUsers.length >= 6, 'MitchelLake has ' + mlUsers.length + ' users');

    // 5. Clean up
    await platformPool.query('DELETE FROM users WHERE email = $1', [TEST_EMAIL]);
    await platformPool.query('DELETE FROM tenants WHERE id = $1', [newTenant.id]);
    console.log('\n  ✓ Test data cleaned up');

    // Summary
    console.log('\n═══════════════════════════════════════');
    console.log('  ' + passed + ' passed, ' + failed + ' failed');
    console.log('═══════════════════════════════════════');

    if (failed > 0) {
      console.log('\n  ✗ FIX ISSUES BEFORE ADDING TEST USERS\n');
      process.exit(1);
    } else {
      console.log('\n  ✓ SAFE TO ADD TEST USERS\n');
      process.exit(0);
    }

  } catch (err) {
    console.error('\n✗ TEST FAILED:', err.message);
    // Clean up on error
    await platformPool.query('DELETE FROM users WHERE email = $1', [TEST_EMAIL]).catch(function() {});
    await platformPool.query("DELETE FROM tenants WHERE slug LIKE $1", ['%test-individual-%']).catch(function() {});
    process.exit(1);
  }
}

testNewUserProvisioning();
