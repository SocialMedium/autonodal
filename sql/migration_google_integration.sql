-- Migration: Google Integration & Interaction Visibility
-- Run with: psql $DATABASE_URL -f sql/migration_google_integration.sql

-- =====================================================
-- USER GOOGLE CONNECTIONS
-- =====================================================

CREATE TABLE IF NOT EXISTS user_google_accounts (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  google_email VARCHAR(255) NOT NULL,
  access_token TEXT,
  refresh_token TEXT,
  token_expires_at TIMESTAMP,
  scopes TEXT[], -- Array of granted scopes
  sync_enabled BOOLEAN DEFAULT true,
  last_sync_at TIMESTAMP,
  last_history_id VARCHAR(100), -- Gmail incremental sync cursor
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(user_id, google_email)
);

CREATE INDEX idx_user_google_accounts_user ON user_google_accounts(user_id);
CREATE INDEX idx_user_google_accounts_email ON user_google_accounts(google_email);

-- =====================================================
-- INTERACTIONS TABLE UPDATES
-- =====================================================

-- Add visibility and ownership columns
ALTER TABLE interactions 
  ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) DEFAULT 'company',
  ADD COLUMN IF NOT EXISTS owner_user_id INTEGER REFERENCES users(id),
  ADD COLUMN IF NOT EXISTS marked_private_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS marked_private_by INTEGER REFERENCES users(id);

-- Add email-specific metadata
ALTER TABLE interactions
  ADD COLUMN IF NOT EXISTS email_message_id VARCHAR(255),
  ADD COLUMN IF NOT EXISTS email_thread_id VARCHAR(255),
  ADD COLUMN IF NOT EXISTS email_subject TEXT,
  ADD COLUMN IF NOT EXISTS email_snippet TEXT,
  ADD COLUMN IF NOT EXISTS email_from VARCHAR(255),
  ADD COLUMN IF NOT EXISTS email_to TEXT[], -- Array of recipients
  ADD COLUMN IF NOT EXISTS email_cc TEXT[],
  ADD COLUMN IF NOT EXISTS email_labels TEXT[],
  ADD COLUMN IF NOT EXISTS email_has_attachments BOOLEAN DEFAULT false,
  ADD COLUMN IF NOT EXISTS email_attachment_names TEXT[];

-- Add calendar-specific metadata  
ALTER TABLE interactions
  ADD COLUMN IF NOT EXISTS calendar_event_id VARCHAR(255),
  ADD COLUMN IF NOT EXISTS calendar_title TEXT,
  ADD COLUMN IF NOT EXISTS calendar_start_time TIMESTAMP,
  ADD COLUMN IF NOT EXISTS calendar_end_time TIMESTAMP,
  ADD COLUMN IF NOT EXISTS calendar_attendees TEXT[];

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_interactions_visibility ON interactions(visibility);
CREATE INDEX IF NOT EXISTS idx_interactions_owner ON interactions(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_email_thread ON interactions(email_thread_id);
CREATE INDEX IF NOT EXISTS idx_interactions_email_message ON interactions(email_message_id);
CREATE INDEX IF NOT EXISTS idx_interactions_calendar_event ON interactions(calendar_event_id);

-- =====================================================
-- EMAIL SIGNALS (Always extracted, never private)
-- =====================================================

CREATE TABLE IF NOT EXISTS email_signals (
  id SERIAL PRIMARY KEY,
  person_id INTEGER REFERENCES people(id) ON DELETE CASCADE,
  user_id INTEGER REFERENCES users(id), -- MitchelLake user who sent/received
  
  -- Signal data (aggregated, no content)
  direction VARCHAR(10) NOT NULL, -- 'inbound' or 'outbound'
  email_date TIMESTAMP NOT NULL,
  response_time_minutes INTEGER, -- How fast they replied (if reply)
  thread_id VARCHAR(255),
  thread_position INTEGER, -- 1st email, 2nd reply, etc.
  
  -- Extracted metadata only
  has_attachment BOOLEAN DEFAULT false,
  email_domain VARCHAR(255), -- Company domain they emailed from
  
  created_at TIMESTAMP DEFAULT NOW(),
  
  -- Prevent duplicates
  UNIQUE(person_id, user_id, email_date, direction)
);

CREATE INDEX idx_email_signals_person ON email_signals(person_id);
CREATE INDEX idx_email_signals_user ON email_signals(user_id);
CREATE INDEX idx_email_signals_date ON email_signals(email_date DESC);
CREATE INDEX idx_email_signals_thread ON email_signals(thread_id);

-- =====================================================
-- PERSON ENGAGEMENT SCORES (Computed from signals)
-- =====================================================

-- Add email engagement columns to person_scores
ALTER TABLE person_scores
  ADD COLUMN IF NOT EXISTS email_response_rate DECIMAL(3,2), -- 0.00 to 1.00
  ADD COLUMN IF NOT EXISTS email_avg_response_hours DECIMAL(6,2),
  ADD COLUMN IF NOT EXISTS email_total_inbound INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS email_total_outbound INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS email_last_inbound_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS email_last_outbound_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS email_thread_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS email_active_users INTEGER DEFAULT 0, -- How many ML people in contact
  ADD COLUMN IF NOT EXISTS meeting_count INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS meeting_last_at TIMESTAMP;

-- =====================================================
-- VIEWS
-- =====================================================

-- View: Person interactions with visibility filtering
CREATE OR REPLACE VIEW person_interactions_visible AS
SELECT 
  i.*,
  u.full_name as owner_name,
  CASE 
    WHEN i.visibility = 'private' THEN '[Private]'
    ELSE i.email_subject 
  END as display_subject,
  CASE 
    WHEN i.visibility = 'private' THEN NULL
    ELSE i.content 
  END as display_content
FROM interactions i
LEFT JOIN users u ON i.owner_user_id = u.id;

-- View: Person engagement summary (always visible)
CREATE OR REPLACE VIEW person_engagement_summary AS
SELECT 
  p.id as person_id,
  p.full_name,
  COUNT(DISTINCT es.user_id) as ml_contacts,
  COUNT(CASE WHEN es.direction = 'inbound' THEN 1 END) as emails_received,
  COUNT(CASE WHEN es.direction = 'outbound' THEN 1 END) as emails_sent,
  MAX(CASE WHEN es.direction = 'inbound' THEN es.email_date END) as last_email_from,
  MAX(CASE WHEN es.direction = 'outbound' THEN es.email_date END) as last_email_to,
  AVG(es.response_time_minutes) as avg_response_minutes,
  COUNT(DISTINCT es.thread_id) as thread_count
FROM people p
LEFT JOIN email_signals es ON p.id = es.person_id
GROUP BY p.id, p.full_name;

-- =====================================================
-- FUNCTIONS
-- =====================================================

-- Function: Compute engagement score for a person
CREATE OR REPLACE FUNCTION compute_email_engagement(p_person_id INTEGER)
RETURNS TABLE(
  response_rate DECIMAL,
  avg_response_hours DECIMAL,
  engagement_score DECIMAL
) AS $$
DECLARE
  total_threads INTEGER;
  replied_threads INTEGER;
  avg_response DECIMAL;
BEGIN
  -- Count threads where we sent outbound
  SELECT COUNT(DISTINCT thread_id) INTO total_threads
  FROM email_signals
  WHERE person_id = p_person_id AND direction = 'outbound';
  
  -- Count threads where they replied (have inbound after outbound)
  SELECT COUNT(DISTINCT es1.thread_id) INTO replied_threads
  FROM email_signals es1
  JOIN email_signals es2 ON es1.thread_id = es2.thread_id 
    AND es1.person_id = es2.person_id
  WHERE es1.person_id = p_person_id 
    AND es1.direction = 'outbound'
    AND es2.direction = 'inbound'
    AND es2.email_date > es1.email_date;
  
  -- Calculate average response time
  SELECT AVG(response_time_minutes) / 60.0 INTO avg_response
  FROM email_signals
  WHERE person_id = p_person_id 
    AND direction = 'inbound'
    AND response_time_minutes IS NOT NULL;
  
  RETURN QUERY SELECT
    CASE WHEN total_threads > 0 
      THEN (replied_threads::DECIMAL / total_threads) 
      ELSE NULL 
    END as response_rate,
    COALESCE(avg_response, NULL) as avg_response_hours,
    CASE 
      WHEN total_threads = 0 THEN 0
      WHEN replied_threads::DECIMAL / total_threads > 0.7 AND COALESCE(avg_response, 999) < 24 THEN 0.9
      WHEN replied_threads::DECIMAL / total_threads > 0.5 THEN 0.7
      WHEN replied_threads::DECIMAL / total_threads > 0.3 THEN 0.5
      ELSE 0.3
    END as engagement_score;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- TRIGGERS
-- =====================================================

-- Update timestamp trigger for user_google_accounts
CREATE OR REPLACE FUNCTION update_google_account_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_user_google_accounts_updated
  BEFORE UPDATE ON user_google_accounts
  FOR EACH ROW
  EXECUTE FUNCTION update_google_account_timestamp();

-- =====================================================
-- SAMPLE DATA
-- =====================================================

-- Add visibility types comment
COMMENT ON COLUMN interactions.visibility IS 'private = only owner sees content, team = search team sees, company = all MitchelLake (default)';
