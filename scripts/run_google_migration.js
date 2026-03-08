require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL, 
  ssl: { rejectUnauthorized: false } 
});

async function run() {
  const client = await pool.connect();
  
  try {
    // User Google Accounts table
    await client.query(`
      CREATE TABLE IF NOT EXISTS user_google_accounts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        google_email VARCHAR(255) NOT NULL,
        access_token TEXT,
        refresh_token TEXT,
        token_expires_at TIMESTAMP,
        scopes TEXT[],
        sync_enabled BOOLEAN DEFAULT true,
        last_sync_at TIMESTAMP,
        last_history_id VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(user_id, google_email)
      )
    `);
    console.log('✓ user_google_accounts table');

    // Indexes for user_google_accounts
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_google_accounts_user ON user_google_accounts(user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_google_accounts_email ON user_google_accounts(google_email)`);
    console.log('✓ user_google_accounts indexes');

    // Add columns to interactions
    const interactionColumns = [
      ['visibility', "VARCHAR(20) DEFAULT 'company'"],
      ['owner_user_id', 'INTEGER REFERENCES users(id)'],
      ['marked_private_at', 'TIMESTAMP'],
      ['marked_private_by', 'INTEGER REFERENCES users(id)'],
      ['email_message_id', 'VARCHAR(255)'],
      ['email_thread_id', 'VARCHAR(255)'],
      ['email_subject', 'TEXT'],
      ['email_snippet', 'TEXT'],
      ['email_from', 'VARCHAR(255)'],
      ['email_to', 'TEXT[]'],
      ['email_cc', 'TEXT[]'],
      ['email_labels', 'TEXT[]'],
      ['email_has_attachments', 'BOOLEAN DEFAULT false'],
      ['email_attachment_names', 'TEXT[]'],
      ['calendar_event_id', 'VARCHAR(255)'],
      ['calendar_title', 'TEXT'],
      ['calendar_start_time', 'TIMESTAMP'],
      ['calendar_end_time', 'TIMESTAMP'],
      ['calendar_attendees', 'TEXT[]']
    ];

    for (const [col, type] of interactionColumns) {
      try {
        await client.query(`ALTER TABLE interactions ADD COLUMN IF NOT EXISTS ${col} ${type}`);
      } catch (e) {
        // Column might already exist
      }
    }
    console.log('✓ interactions columns added');

    // Indexes for interactions
    await client.query(`CREATE INDEX IF NOT EXISTS idx_interactions_visibility ON interactions(visibility)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_interactions_owner ON interactions(owner_user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_interactions_email_thread ON interactions(email_thread_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_interactions_email_message ON interactions(email_message_id)`);
    console.log('✓ interactions indexes');

    // Email signals table
    await client.query(`
      CREATE TABLE IF NOT EXISTS email_signals (
        id SERIAL PRIMARY KEY,
        person_id INTEGER REFERENCES people(id) ON DELETE CASCADE,
        user_id INTEGER REFERENCES users(id),
        direction VARCHAR(10) NOT NULL,
        email_date TIMESTAMP NOT NULL,
        response_time_minutes INTEGER,
        thread_id VARCHAR(255),
        thread_position INTEGER,
        has_attachment BOOLEAN DEFAULT false,
        email_domain VARCHAR(255),
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(person_id, user_id, email_date, direction)
      )
    `);
    console.log('✓ email_signals table');

    // Indexes for email_signals
    await client.query(`CREATE INDEX IF NOT EXISTS idx_email_signals_person ON email_signals(person_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_email_signals_user ON email_signals(user_id)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_email_signals_date ON email_signals(email_date DESC)`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_email_signals_thread ON email_signals(thread_id)`);
    console.log('✓ email_signals indexes');

    // Add email engagement columns to person_scores
    const scoreColumns = [
      ['email_response_rate', 'DECIMAL(3,2)'],
      ['email_avg_response_hours', 'DECIMAL(6,2)'],
      ['email_total_inbound', 'INTEGER DEFAULT 0'],
      ['email_total_outbound', 'INTEGER DEFAULT 0'],
      ['email_last_inbound_at', 'TIMESTAMP'],
      ['email_last_outbound_at', 'TIMESTAMP'],
      ['email_thread_count', 'INTEGER DEFAULT 0'],
      ['email_active_users', 'INTEGER DEFAULT 0'],
      ['meeting_count', 'INTEGER DEFAULT 0'],
      ['meeting_last_at', 'TIMESTAMP']
    ];

    for (const [col, type] of scoreColumns) {
      try {
        await client.query(`ALTER TABLE person_scores ADD COLUMN IF NOT EXISTS ${col} ${type}`);
      } catch (e) {
        // Column might already exist or table might not exist
      }
    }
    console.log('✓ person_scores columns added');

    console.log('\n✅ Google integration migration complete!');

  } catch (err) {
    console.error('Migration error:', err.message);
  } finally {
    client.release();
    pool.end();
  }
}

run();
