#!/usr/bin/env node

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function check() {
  console.log('\n=== MITCHELLAKE DIAGNOSTIC ===\n');
  
  try {
    const people = await pool.query('SELECT COUNT(*) FROM people');
    console.log('✓ People in database:', parseInt(people.rows[0].count).toLocaleString());
    
    const signals = await pool.query('SELECT COUNT(*) FROM signal_events');
    const signalCount = parseInt(signals.rows[0].count);
    console.log('✓ Total signals:', signalCount.toLocaleString());
    
    const recent = await pool.query("SELECT COUNT(*) FROM signal_events WHERE detected_at > NOW() - INTERVAL '24 hours'");
    console.log('✓ Signals (24h):', parseInt(recent.rows[0].count).toLocaleString());
    
    if (signalCount > 0) {
      const sample = await pool.query('SELECT signal_type, detected_at FROM signal_events ORDER BY detected_at DESC LIMIT 1');
      console.log('\nMost recent signal:', sample.rows[0].signal_type, 'at', sample.rows[0].detected_at);
    }
    
    console.log('\n=== RESULT ===');
    if (signalCount === 0) {
      console.log('⚠️  No signals yet - need to run harvester');
      console.log('Fix: node scripts/harvest_signals.js --limit 20');
    } else {
      console.log('✅ Database has signals - dashboard should work!');
    }
    
  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    pool.end();
  }
}

check();
