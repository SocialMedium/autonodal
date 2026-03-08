#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/init_db.js - Initialize PostgreSQL Database
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

async function initDatabase() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - DATABASE INITIALIZATION');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();
  
  // Check environment
  if (!process.env.DATABASE_URL) {
    console.error('❌ DATABASE_URL not set in environment');
    process.exit(1);
  }
  
  console.log('📦 Connecting to PostgreSQL...');
  
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
  });
  
  try {
    // Test connection
    const client = await pool.connect();
    console.log('✅ Connected to database');
    
    // Read schema file
    const schemaPath = path.join(__dirname, '..', 'sql', 'schema.sql');
    
    if (!fs.existsSync(schemaPath)) {
      console.error('❌ Schema file not found:', schemaPath);
      process.exit(1);
    }
    
    console.log('📄 Reading schema file...');
    const schema = fs.readFileSync(schemaPath, 'utf8');
    
    // Execute schema
    console.log('🔄 Executing schema...');
    console.log();
    
    await client.query(schema);
    
    console.log('✅ Schema executed successfully');
    console.log();
    
    // Verify tables
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    
    console.log('📊 Tables created:');
    console.log('─────────────────────────────────────────');
    tablesResult.rows.forEach(row => {
      console.log(`   ✓ ${row.table_name}`);
    });
    console.log(`   Total: ${tablesResult.rows.length} tables`);
    console.log();
    
    // Verify seed data
    const rssCount = await client.query('SELECT COUNT(*) FROM rss_sources');
    const stagesCount = await client.query('SELECT COUNT(*) FROM pipeline_stages');
    const interestsCount = await client.query('SELECT COUNT(*) FROM ml_interests');
    
    console.log('🌱 Seed data:');
    console.log('─────────────────────────────────────────');
    console.log(`   ✓ RSS Sources: ${rssCount.rows[0].count}`);
    console.log(`   ✓ Pipeline Stages: ${stagesCount.rows[0].count}`);
    console.log(`   ✓ ML Interests: ${interestsCount.rows[0].count}`);
    console.log();
    
    client.release();
    
    console.log('═══════════════════════════════════════════════════════════════════');
    console.log('  ✅ DATABASE INITIALIZATION COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════════');
    
  } catch (error) {
    console.error('❌ Error initializing database:', error.message);
    
    if (error.position) {
      console.error('   Position:', error.position);
    }
    
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run if called directly
if (require.main === module) {
  initDatabase();
}

module.exports = { initDatabase };
