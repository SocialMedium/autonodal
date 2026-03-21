-- ════════════════════════════════════════════════════════════════
-- User attribution migration
-- Adds created_by to people and companies so every record
-- traces back to the user or system process that created it
-- ════════════════════════════════════════════════════════════════

-- People: who created this record
ALTER TABLE people ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);

-- Companies: who created this record
ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);

-- Indexes for admin queries
CREATE INDEX IF NOT EXISTS idx_people_created_by ON people(created_by) WHERE created_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_companies_created_by ON companies(created_by) WHERE created_by IS NOT NULL;

-- Interactions: ensure created_by exists (separate from user_id which means "the team member involved")
ALTER TABLE interactions ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id);
CREATE INDEX IF NOT EXISTS idx_interactions_created_by ON interactions(created_by) WHERE created_by IS NOT NULL;
