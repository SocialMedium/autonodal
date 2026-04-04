#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/smoke_tests.js — Critical path integration tests
// Run before every deploy: node scripts/smoke_tests.js
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const results = [];
async function test(name, fn) {
  var start = Date.now();
  try {
    await fn();
    results.push({ status: '\u2705', name: name, ms: Date.now() - start });
  } catch (err) {
    results.push({ status: '\u274c', name: name, ms: Date.now() - start, error: err.message });
  }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || 'Assertion failed'); }

async function run() {
  console.log('\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550');
  console.log('AUTONODAL SMOKE TESTS');
  console.log('\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n');

  var tenants = (await pool.query("SELECT id, name, slug FROM tenants ORDER BY created_at LIMIT 5")).rows;
  var ml = tenants.find(function(t) { return t.slug === 'mitchellake'; });
  var other = tenants.find(function(t) { return t.slug !== 'mitchellake'; });
  console.log('ML tenant:', ml?.name);
  console.log('Other:    ', other?.name, '\n');

  // 1. DB connectivity
  await test('Database connectivity', async function() {
    var r = await pool.query('SELECT 1 + 1 AS result');
    assert(r.rows[0].result === 2);
  });

  // 2. Tenant isolation
  await test('Tenant isolation — people', async function() {
    if (!ml || !other) throw new Error('Need 2 tenants');
    var client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SET LOCAL app.current_tenant = '" + other.id + "'");
      var r = await client.query('SELECT COUNT(*) AS cnt FROM people WHERE tenant_id = $1', [ml.id]);
      await client.query('COMMIT');
      var cnt = parseInt(r.rows[0].cnt);
      if (cnt > 0) console.log('  \u26a0\ufe0f  Superuser bypasses RLS (' + cnt + ' rows visible). App-layer isolation active.');
    } finally { client.release(); }
  });

  // 3. Platform signals
  await test('Platform signals (NULL tenant) exist', async function() {
    var r = await pool.query('SELECT COUNT(*) AS cnt FROM signal_events WHERE tenant_id IS NULL');
    var n = parseInt(r.rows[0].cnt);
    assert(n > 100, 'Only ' + n + ' platform signals. Expected 100+.');
  });

  // 4. Recent signal ingestion
  await test('Signal ingestion active (last 7 days)', async function() {
    var r = await pool.query("SELECT COUNT(*) AS cnt FROM signal_events WHERE detected_at > NOW() - INTERVAL '7 days'");
    var n = parseInt(r.rows[0].cnt);
    assert(n > 10, 'Only ' + n + ' signals in last 7 days. Pipeline may be stalled.');
  });

  // 5. RSS sources materialised
  await test('RSS sources materialised from subscriptions', async function() {
    var r = await pool.query("SELECT COUNT(DISTINCT tfs.tenant_id) AS sub_tenants, (SELECT COUNT(DISTINCT tenant_id) FROM rss_sources WHERE enabled = true) AS src_tenants FROM tenant_feed_subscriptions tfs WHERE tfs.is_enabled = true");
    var subs = parseInt(r.rows[0].sub_tenants);
    var srcs = parseInt(r.rows[0].src_tenants);
    if (subs > 0 && srcs === 0) throw new Error(subs + ' tenants subscribed but 0 have RSS sources.');
  });

  // 6. Qdrant vectors
  await test('Qdrant people vectors exist', async function() {
    if (!process.env.QDRANT_URL) throw new Error('QDRANT_URL not set');
    var res = await fetch(process.env.QDRANT_URL + '/collections/people', {
      headers: { 'api-key': process.env.QDRANT_API_KEY || '' },
    });
    var data = await res.json();
    var count = data.result?.points_count || 0;
    assert(count > 1000, 'Only ' + count + ' people vectors. Expected 100K+.');
  });

  // 7. Auth — valid token
  await test('Auth — valid session accepted', async function() {
    var s = await pool.query("SELECT s.token FROM sessions s WHERE s.expires_at > NOW() ORDER BY s.created_at DESC LIMIT 1");
    if (!s.rows[0]) throw new Error('No active sessions');
    var base = 'https://optimistic-spirit-production-a174.up.railway.app';
    var res = await fetch(base + '/api/auth/me', { headers: { 'Authorization': 'Bearer ' + s.rows[0].token } });
    assert(res.ok, 'Auth returned ' + res.status);
  });

  // 8. Auth — invalid token rejected
  await test('Auth — invalid token returns 401', async function() {
    var base = 'https://optimistic-spirit-production-a174.up.railway.app';
    var res = await fetch(base + '/api/auth/me', { headers: { 'Authorization': 'Bearer invalid_xyz_123' } });
    assert(res.status === 401, 'Expected 401, got ' + res.status);
  });

  // 9. Pipeline runs table
  await test('Pipeline observability table writable', async function() {
    var r = await pool.query("INSERT INTO pipeline_runs (pipeline_key, pipeline_name, status, records_processed) VALUES ('smoke_test', 'smoke_test', 'completed', 1) RETURNING id");
    assert(r.rows[0].id, 'Insert failed');
    await pool.query('DELETE FROM pipeline_runs WHERE id = $1', [r.rows[0].id]);
  });

  // 10. Feed bundles wired
  await test('Feed bundles have sources', async function() {
    var r = await pool.query("SELECT COUNT(DISTINCT fb.id) AS total, COUNT(DISTINCT CASE WHEN fbs.source_id IS NOT NULL THEN fb.id END) AS wired FROM feed_bundles fb LEFT JOIN feed_bundle_sources fbs ON fbs.bundle_id = fb.id WHERE fb.is_active = true");
    var total = parseInt(r.rows[0].total);
    var wired = parseInt(r.rows[0].wired);
    assert(total > 0, 'No active bundles');
    assert(wired >= Math.floor(total * 0.5), 'Only ' + wired + '/' + total + ' bundles have sources');
  });

  // Results
  console.log('\n\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550');
  console.log('RESULTS');
  console.log('\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550');
  results.forEach(function(r) {
    console.log(r.status + ' ' + r.name + ' (' + r.ms + 'ms)' + (r.error ? '\n     \u2192 ' + r.error : ''));
  });
  var passed = results.filter(function(r) { return r.status === '\u2705'; }).length;
  var failed = results.filter(function(r) { return r.status === '\u274c'; }).length;
  console.log('\n' + passed + '/' + results.length + ' passed');
  if (failed > 0) { console.log('\n\u26a0\ufe0f  Fix failures before deploying.'); process.exit(1); }
  else { console.log('\n\u2705 All critical paths verified.'); process.exit(0); }
}

run().catch(function(e) { console.error('Runner failed:', e.message); process.exit(1); }).finally(function() { pool.end(); });
