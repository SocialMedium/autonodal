#!/usr/bin/env node
/**
 * Ezekia Batching Test — uses lib/ezekia.js directly
 * Run from mitchellake-signals/: node test_ezekia_batching.js
 */

require('dotenv').config();
const ezekia = require('./lib/ezekia');

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function run() {
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║   Ezekia Batching Test — using lib/ezekia.js         ║');
  console.log('╚══════════════════════════════════════════════════════╝\n');

  // ── TEST 1: Baseline ──────────────────────────────────────────────────────
  console.log('TEST 1: Baseline — total count and pagination');
  console.log('─────────────────────────────────────────────');
  try {
    const r = await ezekia.getPeople({ page: 1, per_page: 100 });
    const meta = r.meta || {};
    console.log(`  Total:       ${meta.total ?? 'not returned'}`);
    console.log(`  Last page:   ${meta.lastPage ?? meta.last_page ?? 'not returned'}`);
    console.log(`  Per page:    ${meta.perPage ?? meta.per_page ?? 100}`);
    console.log(`  This page:   ${(r.data || []).length} records`);

    const records = r.data || [];
    if (records.length > 0) {
      const dates = records.map(p => p.updatedAt || p.updated_at).filter(Boolean).sort();
      console.log(`  Oldest updated_at on p1: ${dates[0]}`);
      console.log(`  Newest updated_at on p1: ${dates[dates.length - 1]}`);
      console.log(`  Sample fields: ${Object.keys(records[0]).join(', ')}`);
    }
  } catch (e) {
    console.log(`  ❌ ${e.message}`);
    process.exit(1);
  }

  console.log('');
  await sleep(300);

  // ── TEST 2: updated_since filter ──────────────────────────────────────────
  console.log('TEST 2: updated_since — does it narrow results?');
  console.log('─────────────────────────────────────────────');

  for (const since of ['2025-01-01', '2024-01-01', '2023-01-01', '2022-01-01', '2020-01-01']) {
    try {
      const r = await ezekia.getPeople({ page: 1, per_page: 10, updated_since: since });
      const meta = r.meta || {};
      console.log(`  updated_since=${since} → total: ${meta.total ?? '?'}, lastPage: ${meta.lastPage ?? '?'}, records: ${(r.data||[]).length}`);
    } catch (e) {
      console.log(`  updated_since=${since} → ❌ ${e.message}`);
    }
    await sleep(300);
  }

  console.log('');

  // ── TEST 3: Pagination depth ──────────────────────────────────────────────
  console.log('TEST 3: Pagination depth — pages at per_page=100');
  console.log('─────────────────────────────────────────────');
  try {
    const r = await ezekia.getPeople({ page: 1, per_page: 100 });
    const meta = r.meta || {};
    const lastPage = meta.lastPage ?? meta.last_page;
    const total = meta.total;
    console.log(`  lastPage: ${lastPage ?? 'unknown'}, total: ${total ?? 'unknown'}`);
    if (lastPage && !total) {
      console.log(`  ⚠️  No total returned — Ezekia may be hiding true count`);
      console.log(`  Implied records: ~${(lastPage * 100).toLocaleString()}`);
    }
    if (lastPage) {
      await sleep(300);
      const last = await ezekia.getPeople({ page: lastPage, per_page: 100 });
      console.log(`  Records on final page ${lastPage}: ${(last.data||[]).length}`);
      await sleep(300);
      const beyond = await ezekia.getPeople({ page: lastPage + 1, per_page: 100 });
      console.log(`  Records on page ${lastPage+1} (beyond): ${(beyond.data||[]).length}`);
    }
  } catch (e) {
    console.log(`  ❌ ${e.message}`);
  }

  console.log('');
  await sleep(300);

  // ── TEST 4: Alphabetical name batching ────────────────────────────────────
  console.log('TEST 4: Alphabetical batching — name search');
  console.log('─────────────────────────────────────────────');
  for (const letter of ['A', 'B', 'M', 'S']) {
    try {
      const r = await ezekia.searchPeople({ name: letter, page: 1, per_page: 10 });
      const meta = r.meta || {};
      console.log(`  name="${letter}" → total: ${meta.total ?? '?'}, lastPage: ${meta.lastPage ?? '?'}, records: ${(r.data||[]).length}`);
    } catch (e) {
      console.log(`  name="${letter}" → ❌ ${e.message}`);
    }
    await sleep(300);
  }

  console.log('');
  console.log('════════════════════════════════════════════════════════');
  console.log('ANALYSIS — what to look for:');
  console.log('════════════════════════════════════════════════════════');
  console.log('TEST 2: different totals per date = updated_since batching works');
  console.log('TEST 3: lastPage=100 no total = hard 10K cap, need a workaround');
  console.log('TEST 4: totals per letter = alphabetical batching is viable');
  console.log('');
}

run().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
