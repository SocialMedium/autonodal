#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// MitchelLake — Contact Enrichment Migration
// Run with: node scripts/migrate_contact_enrichment.js
// No psql required — uses your existing DATABASE_URL from .env
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const MIGRATION_SQL = `
-- ─────────────────────────────────────────────────────────────────────────
-- 1. interactions — add linkedin_message_id + UNIQUE index on external_id
-- ─────────────────────────────────────────────────────────────────────────
ALTER TABLE interactions
  ADD COLUMN IF NOT EXISTS linkedin_message_id TEXT;

ALTER TABLE interactions
  ADD COLUMN IF NOT EXISTS external_id VARCHAR(255);

ALTER TABLE interactions
  ADD COLUMN IF NOT EXISTS metadata JSONB;

-- ─────────────────────────────────────────────────────────────────────────
-- 2. people — add secondary email + contacts tracking
-- ─────────────────────────────────────────────────────────────────────────
ALTER TABLE people
  ADD COLUMN IF NOT EXISTS email_alt VARCHAR(255);

ALTER TABLE people
  ADD COLUMN IF NOT EXISTS contacts_last_updated_at TIMESTAMPTZ;

-- ─────────────────────────────────────────────────────────────────────────
-- 3. user_google_accounts — Gmail + Contacts sync tracking columns
-- ─────────────────────────────────────────────────────────────────────────
ALTER TABLE user_google_accounts
  ADD COLUMN IF NOT EXISTS gmail_history_id TEXT;

ALTER TABLE user_google_accounts
  ADD COLUMN IF NOT EXISTS gmail_last_sync_at TIMESTAMPTZ;

ALTER TABLE user_google_accounts
  ADD COLUMN IF NOT EXISTS contacts_last_sync_at TIMESTAMPTZ;

ALTER TABLE user_google_accounts
  ADD COLUMN IF NOT EXISTS contacts_sync_token TEXT;

-- ─────────────────────────────────────────────────────────────────────────
-- 4. new_contacts_review — unknown emails discovered via Gmail
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS new_contacts_review (
  id                    SERIAL PRIMARY KEY,
  email                 VARCHAR(255) NOT NULL UNIQUE,
  name                  VARCHAR(255),
  thread_count          INTEGER DEFAULT 1,
  last_thread_date      TIMESTAMPTZ,
  discovered_by_user_id UUID,
  status                VARCHAR(20) DEFAULT 'pending'
                          CHECK (status IN ('pending', 'added', 'ignored')),
  created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────
-- 5. job_runs — health tracking for all cron/batch scripts
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_runs (
  id            SERIAL PRIMARY KEY,
  job_name      TEXT NOT NULL,
  started_at    TIMESTAMPTZ DEFAULT NOW(),
  completed_at  TIMESTAMPTZ,
  status        TEXT CHECK (status IN ('running', 'success', 'failed', 'partial')),
  records_in    INTEGER DEFAULT 0,
  records_out   INTEGER DEFAULT 0,
  error_message TEXT,
  metadata      JSONB
);
`;

// Indexes must run separately (CREATE INDEX can't run inside a multi-statement string in some drivers)
const INDEXES = [
  {
    name: 'idx_interactions_external_id',
    sql: `CREATE UNIQUE INDEX IF NOT EXISTS idx_interactions_external_id
          ON interactions(external_id) WHERE external_id IS NOT NULL`
  },
  {
    name: 'idx_people_email_alt',
    sql: `CREATE INDEX IF NOT EXISTS idx_people_email_alt
          ON people(email_alt) WHERE email_alt IS NOT NULL`
  },
  {
    name: 'idx_job_runs_name_started',
    sql: `CREATE INDEX IF NOT EXISTS idx_job_runs_name_started
          ON job_runs(job_name, started_at DESC)`
  },
  {
    name: 'idx_job_runs_status',
    sql: `CREATE INDEX IF NOT EXISTS idx_job_runs_status
          ON job_runs(status) WHERE status IN ('running', 'failed')`
  }
];

// Verification checks
const CHECKS = [
  { label: 'interactions.external_id',              sql: `SELECT 1 FROM information_schema.columns WHERE table_name='interactions' AND column_name='external_id'` },
  { label: 'interactions.metadata',                 sql: `SELECT 1 FROM information_schema.columns WHERE table_name='interactions' AND column_name='metadata'` },
  { label: 'people.email_alt',                      sql: `SELECT 1 FROM information_schema.columns WHERE table_name='people' AND column_name='email_alt'` },
  { label: 'user_google_accounts.gmail_history_id', sql: `SELECT 1 FROM information_schema.columns WHERE table_name='user_google_accounts' AND column_name='gmail_history_id'` },
  { label: 'user_google_accounts.contacts_sync_token', sql: `SELECT 1 FROM information_schema.columns WHERE table_name='user_google_accounts' AND column_name='contacts_sync_token'` },
  { label: 'table: new_contacts_review',            sql: `SELECT 1 FROM information_schema.tables WHERE table_name='new_contacts_review'` },
  { label: 'table: job_runs',                       sql: `SELECT 1 FROM information_schema.tables WHERE table_name='job_runs'` },
];

async function run() {
  const green  = (s) => `\x1b[32m${s}\x1b[0m`;
  const red    = (s) => `\x1b[31m${s}\x1b[0m`;
  const yellow = (s) => `\x1b[33m${s}\x1b[0m`;
  const blue   = (s) => `\x1b[34m${s}\x1b[0m`;

  console.log('');
  console.log(blue('═══════════════════════════════════════════════════════'));
  console.log(blue('  MitchelLake — Contact Enrichment Migration          '));
  console.log(blue('═══════════════════════════════════════════════════════'));
  console.log('');

  if (!process.env.DATABASE_URL) {
    console.log(red('✗ DATABASE_URL not found in .env'));
    process.exit(1);
  }

  const client = await pool.connect();

  try {
    // ── Run main migration statements ──────────────────────────────────────
    console.log(yellow('▶ Running schema changes...'));
    await client.query(MIGRATION_SQL);
    console.log(green('✓ Schema changes applied'));

    // ── Run indexes one at a time ──────────────────────────────────────────
    console.log(yellow('▶ Creating indexes...'));
    for (const idx of INDEXES) {
      try {
        await client.query(idx.sql);
        console.log(green(`  ✓ ${idx.name}`));
      } catch (err) {
        // Index may already exist with slight variation — log but don't fail
        console.log(yellow(`  ⚠ ${idx.name}: ${err.message}`));
      }
    }

    // ── Verify everything landed ───────────────────────────────────────────
    console.log('');
    console.log(yellow('▶ Verifying migration...'));
    let allPassed = true;

    for (const check of CHECKS) {
      const { rows } = await client.query(check.sql);
      if (rows.length > 0) {
        console.log(green(`  ✅ ${check.label}`));
      } else {
        console.log(red(`  ✗  ${check.label} — NOT FOUND`));
        allPassed = false;
      }
    }

    console.log('');
    if (allPassed) {
      console.log(green('═══════════════════════════════════════════════════════'));
      console.log(green('  ✅ Migration complete — all checks passed           '));
      console.log(green('═══════════════════════════════════════════════════════'));
      console.log('');
      console.log('  Next step:');
      console.log('  node scripts/migrate_contact_enrichment.js  ← you just ran this');
      console.log('  node lib/job_runner.js                      ← build next');
      console.log('  node scripts/ingest_linkedin_messages.js --dry-run');
      console.log('');
    } else {
      console.log(red('✗ Some checks failed — review errors above'));
      process.exit(1);
    }

  } catch (err) {
    console.log('');
    console.log(red(`✗ Migration failed: ${err.message}`));
    console.log('');
    console.log('  Full error:');
    console.log(err);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

run();
