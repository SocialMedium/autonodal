-- Signal Dispatches — automated intelligence briefs generated from signal events
-- Run against production database to add the signal_dispatches table

CREATE TABLE IF NOT EXISTS signal_dispatches (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  signal_event_id UUID REFERENCES signal_events(id) ON DELETE SET NULL,
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  company_name TEXT,
  signal_type TEXT,
  signal_summary TEXT,

  -- Proximity map: ranked connections at/near the target company
  proximity_map JSONB DEFAULT '[]'::jsonb,
  -- [{person_id, name, title, company, relationship_type:
  --   'current_employee'|'past_employee'|'placed'|'interacted'|'network_connection',
  --   strength: 'direct'|'warm'|'cold',
  --   score: 0-100,
  --   last_contact, team_member, team_member_id}]

  best_entry_point JSONB,
  -- {person_id, name, title, approach_via, reason}

  -- Approach strategy
  opportunity_angle TEXT,
  approach_rationale TEXT,

  -- Generated content
  blog_theme TEXT,
  blog_title TEXT,
  blog_body TEXT,
  blog_keywords TEXT[],

  -- Distribution plan
  send_to JSONB DEFAULT '[]'::jsonb,
  -- [{person_id, name, title, channel: 'email'|'linkedin',
  --   personal_note TEXT}]

  -- Lifecycle
  status TEXT NOT NULL DEFAULT 'draft',
  -- draft | reviewed | sent | archived

  generated_at TIMESTAMPTZ DEFAULT NOW(),
  reviewed_at TIMESTAMPTZ,
  reviewed_by UUID REFERENCES users(id),
  sent_at TIMESTAMPTZ,
  created_by UUID REFERENCES users(id),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dispatches_signal_event ON signal_dispatches(signal_event_id);
CREATE INDEX IF NOT EXISTS idx_dispatches_company ON signal_dispatches(company_id);
CREATE INDEX IF NOT EXISTS idx_dispatches_status ON signal_dispatches(status);
CREATE INDEX IF NOT EXISTS idx_dispatches_generated ON signal_dispatches(generated_at DESC);

DO $$ BEGIN RAISE NOTICE 'signal_dispatches table created successfully'; END $$;
