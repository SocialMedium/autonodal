-- Waitlist table for autonodal.com beta registration
CREATE TABLE IF NOT EXISTS waitlist (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(255),
  email VARCHAR(255) NOT NULL UNIQUE,
  company VARCHAR(255),
  status VARCHAR(20) DEFAULT 'pending',
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_waitlist_email ON waitlist (email);
CREATE INDEX IF NOT EXISTS idx_waitlist_status ON waitlist (status);
CREATE INDEX IF NOT EXISTS idx_waitlist_created ON waitlist (created_at DESC);

-- Add columns if table already exists (idempotent)
DO $$ BEGIN
  ALTER TABLE waitlist ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pending';
  ALTER TABLE waitlist ADD COLUMN IF NOT EXISTS notes TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
