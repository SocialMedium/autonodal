-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Language V1 — Neutral terminology, vertical overrides
-- Run: psql $DATABASE_URL -f sql/migration_language_v1.sql
-- Safe to run multiple times (fully idempotent).
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. TABLE RENAMES
-- ═══════════════════════════════════════════════════════════════════════════════
-- Prior migration_multi_tenant.sql already renamed:
--   searches → opportunities  (with backward view)
--   search_candidates → pipeline_contacts  (with backward view)
--   search_activities → pipeline_activities  (with backward view)
-- But search_matches was never renamed, and search_id columns remain.
-- This migration completes the job.

-- ─────────────────────────────────────────────────────────────────────────────
-- 1a. search_matches → opportunity_matches
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='search_matches')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_matches') THEN
    ALTER TABLE search_matches RENAME TO opportunity_matches;
  END IF;
END $$;

-- Backward-compatible view
CREATE OR REPLACE VIEW search_matches AS SELECT * FROM opportunity_matches;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1b. pipeline_activities → opportunity_activities
--     (multi_tenant renamed search_activities → pipeline_activities;
--      we now move to the final name)
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='pipeline_activities')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_activities') THEN
    ALTER TABLE pipeline_activities RENAME TO opportunity_activities;
  END IF;
  -- Edge case: original search_activities still exists (multi_tenant never ran)
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='search_activities')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_activities') THEN
    ALTER TABLE search_activities RENAME TO opportunity_activities;
  END IF;
END $$;

-- Backward-compatible views
CREATE OR REPLACE VIEW search_activities AS SELECT * FROM opportunity_activities;
CREATE OR REPLACE VIEW pipeline_activities AS SELECT * FROM opportunity_activities;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1c. pipeline_contacts → opportunity_contacts
--     (multi_tenant renamed search_candidates → pipeline_contacts;
--      we now move to the final name)
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='pipeline_contacts')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_contacts') THEN
    ALTER TABLE pipeline_contacts RENAME TO opportunity_contacts;
  END IF;
  -- Edge case: original search_candidates still exists
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='search_candidates')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunity_contacts') THEN
    ALTER TABLE search_candidates RENAME TO opportunity_contacts;
  END IF;
END $$;

-- Backward-compatible views
CREATE OR REPLACE VIEW search_candidates AS SELECT * FROM opportunity_contacts;
CREATE OR REPLACE VIEW pipeline_contacts AS SELECT * FROM opportunity_contacts;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1d. Ensure searches view exists (may already from multi_tenant)
-- ─────────────────────────────────────────────────────────────────────────────
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='opportunities') THEN
    -- Recreate view in case it was dropped
    EXECUTE 'CREATE OR REPLACE VIEW searches AS SELECT * FROM opportunities';
  END IF;
END $$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1e. Rename search_id → opportunity_id in child tables
-- ─────────────────────────────────────────────────────────────────────────────

-- opportunity_contacts (was pipeline_contacts / search_candidates)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_contacts' AND column_name='search_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_contacts' AND column_name='opportunity_id') THEN
    ALTER TABLE opportunity_contacts RENAME COLUMN search_id TO opportunity_id;
  END IF;
END $$;

-- opportunity_matches (was search_matches)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_matches' AND column_name='search_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_matches' AND column_name='opportunity_id') THEN
    ALTER TABLE opportunity_matches RENAME COLUMN search_id TO opportunity_id;
  END IF;
END $$;

-- opportunity_activities (was search_activities / pipeline_activities)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_activities' AND column_name='search_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_activities' AND column_name='opportunity_id') THEN
    ALTER TABLE opportunity_activities RENAME COLUMN search_id TO opportunity_id;
  END IF;
END $$;

-- opportunity_activities: also rename search_candidate_id → opportunity_contact_id
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_activities' AND column_name='search_candidate_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='opportunity_activities' AND column_name='opportunity_contact_id') THEN
    ALTER TABLE opportunity_activities RENAME COLUMN search_candidate_id TO opportunity_contact_id;
  END IF;
END $$;

-- conversions (was placements) — search_id → opportunity_id
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='conversions' AND column_name='search_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='conversions' AND column_name='opportunity_id') THEN
    ALTER TABLE conversions RENAME COLUMN search_id TO opportunity_id;
  END IF;
END $$;

-- conversions: replacement_search_id → replacement_opportunity_id
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='conversions' AND column_name='replacement_search_id')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='conversions' AND column_name='replacement_opportunity_id') THEN
    ALTER TABLE conversions RENAME COLUMN replacement_search_id TO replacement_opportunity_id;
  END IF;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1f. Recreate backward-compatible views WITH column aliases
--     so old code referencing search_id still works via views
-- ─────────────────────────────────────────────────────────────────────────────
-- NOTE: We recreate these views after the column renames so they expose
-- both the new column name (opportunity_id) and the old (search_id) alias.

CREATE OR REPLACE VIEW search_candidates AS
  SELECT *, opportunity_id AS search_id FROM opportunity_contacts;

CREATE OR REPLACE VIEW pipeline_contacts AS
  SELECT *, opportunity_id AS search_id FROM opportunity_contacts;

CREATE OR REPLACE VIEW search_matches AS
  SELECT *, opportunity_id AS search_id FROM opportunity_matches;

CREATE OR REPLACE VIEW search_activities AS
  SELECT *, opportunity_id AS search_id FROM opportunity_activities;

CREATE OR REPLACE VIEW pipeline_activities AS
  SELECT *, opportunity_id AS search_id FROM opportunity_activities;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1g. Drop old indexes and recreate with new names
-- ─────────────────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS idx_search_cand_search;
DROP INDEX IF EXISTS idx_search_cand_person;
DROP INDEX IF EXISTS idx_search_cand_status;
DROP INDEX IF EXISTS idx_search_act_search;
DROP INDEX IF EXISTS idx_search_matches_score;
DROP INDEX IF EXISTS idx_searches_project;
DROP INDEX IF EXISTS idx_searches_status;

CREATE INDEX IF NOT EXISTS idx_opportunity_contacts_opportunity ON opportunity_contacts (opportunity_id);
CREATE INDEX IF NOT EXISTS idx_opportunity_contacts_person ON opportunity_contacts (person_id);
CREATE INDEX IF NOT EXISTS idx_opportunity_contacts_status ON opportunity_contacts (status);
CREATE INDEX IF NOT EXISTS idx_opportunity_activities_opportunity ON opportunity_activities (opportunity_id);
CREATE INDEX IF NOT EXISTS idx_opportunity_matches_score ON opportunity_matches (overall_match_score DESC);
CREATE INDEX IF NOT EXISTS idx_opportunities_project ON opportunities (project_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_status ON opportunities (status);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. COLUMN RENAMES IN person_scores
-- ═══════════════════════════════════════════════════════════════════════════════

-- flight_risk_score → transition_likelihood
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='person_scores' AND column_name='flight_risk_score')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='person_scores' AND column_name='transition_likelihood') THEN
    ALTER TABLE person_scores RENAME COLUMN flight_risk_score TO transition_likelihood;
  END IF;
END $$;

-- No search_match column exists in person_scores (it was never created),
-- but handle it defensively for any environment that may have added it.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='person_scores' AND column_name='search_match')
     AND NOT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='public' AND table_name='person_scores' AND column_name='opportunity_alignment') THEN
    ALTER TABLE person_scores RENAME COLUMN search_match TO opportunity_alignment;
  END IF;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. PIPELINE STAGES — neutral language
-- ═══════════════════════════════════════════════════════════════════════════════

-- Clear old talent-specific stages and insert neutral ones
DELETE FROM pipeline_stages;

INSERT INTO pipeline_stages (name, sort_order, color) VALUES
  ('identified',  1, 'gray'),
  ('engaged',     2, 'blue'),
  ('qualified',   3, 'yellow'),
  ('priority',    4, 'orange'),
  ('in_play',     5, 'purple'),
  ('resolved',    6, 'green')
ON CONFLICT DO NOTHING;

-- Vertical-specific display labels
CREATE TABLE IF NOT EXISTS pipeline_stage_labels (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stage_name VARCHAR(100) NOT NULL,
  vertical VARCHAR(50) NOT NULL,  -- talent, revenue, mandate, all
  display_label VARCHAR(255) NOT NULL,
  description TEXT,
  UNIQUE(stage_name, vertical)
);

INSERT INTO pipeline_stage_labels (stage_name, vertical, display_label, description) VALUES
  -- Talent vertical
  ('identified',  'talent', 'Identified',       'Name sourced, not yet contacted'),
  ('engaged',     'talent', 'In Conversation',   'Initial outreach or screening call done'),
  ('qualified',   'talent', 'Qualified',         'Meets brief, advancing to shortlist'),
  ('priority',    'talent', 'Shortlisted',       'Presented or about to present to client'),
  ('in_play',     'talent', 'Interviewing',      'Client interviews in progress'),
  ('resolved',    'talent', 'Placed / Closed',   'Offer accepted or candidate withdrawn'),

  -- Revenue vertical
  ('identified',  'revenue', 'Lead',             'Potential opportunity identified'),
  ('engaged',     'revenue', 'Contacted',        'Intro call or meeting held'),
  ('qualified',   'revenue', 'Qualified',        'Budget, authority, need confirmed'),
  ('priority',    'revenue', 'Proposal Sent',    'Commercial terms shared'),
  ('in_play',     'revenue', 'Negotiating',      'Terms under discussion'),
  ('resolved',    'revenue', 'Won / Lost',       'Deal closed or lost'),

  -- Mandate vertical
  ('identified',  'mandate', 'Prospect',         'Potential mandate identified'),
  ('engaged',     'mandate', 'Briefing',         'Scope discussion underway'),
  ('qualified',   'mandate', 'Scoped',           'Requirements locked, ready to kick off'),
  ('priority',    'mandate', 'Active',           'Search/project actively running'),
  ('in_play',     'mandate', 'Shortlist',        'Candidates or deliverables in review'),
  ('resolved',    'mandate', 'Completed',        'Mandate fulfilled or cancelled')
ON CONFLICT (stage_name, vertical) DO NOTHING;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. TENANT TERMINOLOGY TABLE
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tenant_terminology (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vertical VARCHAR(50) NOT NULL,          -- talent, revenue, mandate, all
  entity VARCHAR(100) NOT NULL,           -- the code-level concept
  display_singular VARCHAR(255) NOT NULL, -- what the UI shows (singular)
  display_plural VARCHAR(255) NOT NULL,   -- what the UI shows (plural)
  icon VARCHAR(50),                       -- optional icon identifier
  description TEXT,
  UNIQUE(vertical, entity)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 'all' defaults — neutral baseline every tenant sees
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO tenant_terminology (vertical, entity, display_singular, display_plural, icon, description) VALUES
  ('all', 'opportunity',         'Opportunity',        'Opportunities',        'briefcase',    'A trackable initiative or deal'),
  ('all', 'opportunity_contact', 'Contact',            'Contacts',             'user',         'A person linked to an opportunity'),
  ('all', 'engagement',          'Engagement',         'Engagements',          'folder',       'A project or client engagement'),
  ('all', 'conversion',          'Conversion',         'Conversions',          'check-circle', 'A successful outcome'),
  ('all', 'signal',              'Signal',             'Signals',              'zap',          'A market or relationship signal'),
  ('all', 'dispatch',            'Dispatch',           'Dispatches',           'send',         'A signal sent to a user or team'),
  ('all', 'pipeline_stage',      'Stage',              'Stages',               'layers',       'A step in the pipeline'),
  ('all', 'person',              'Person',             'People',               'user',         'An individual in the network'),
  ('all', 'company',             'Company',            'Companies',            'building',     'An organisation'),
  ('all', 'account',             'Account',            'Accounts',             'briefcase',    'A client account')
ON CONFLICT (vertical, entity) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────────────
-- Talent vertical overrides
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO tenant_terminology (vertical, entity, display_singular, display_plural, icon, description) VALUES
  ('talent', 'opportunity',         'Search',             'Searches',             'search',       'An executive search mandate'),
  ('talent', 'opportunity_contact', 'Candidate',          'Candidates',           'user-check',   'A person being considered for a search'),
  ('talent', 'engagement',          'Mandate',            'Mandates',             'folder',       'A retained or exclusive search engagement'),
  ('talent', 'conversion',          'Placement',          'Placements',           'award',        'A successful hire'),
  ('talent', 'pipeline_stage',      'Stage',              'Stages',               'layers',       'A step in the talent pipeline'),
  ('talent', 'dispatch',            'Talent Alert',       'Talent Alerts',        'bell',         'A talent movement signal dispatched')
ON CONFLICT (vertical, entity) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────────────
-- Revenue vertical overrides
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO tenant_terminology (vertical, entity, display_singular, display_plural, icon, description) VALUES
  ('revenue', 'opportunity',         'Deal',               'Deals',                'dollar-sign',  'A revenue opportunity in the pipeline'),
  ('revenue', 'opportunity_contact', 'Stakeholder',        'Stakeholders',         'users',        'A key contact in a deal'),
  ('revenue', 'engagement',          'Account Plan',       'Account Plans',        'target',       'A structured account growth plan'),
  ('revenue', 'conversion',          'Closed Deal',        'Closed Deals',         'check-circle', 'A deal that reached won/lost'),
  ('revenue', 'pipeline_stage',      'Deal Stage',         'Deal Stages',          'bar-chart',    'A step in the revenue pipeline'),
  ('revenue', 'dispatch',            'Revenue Signal',     'Revenue Signals',      'trending-up',  'A revenue-relevant market signal')
ON CONFLICT (vertical, entity) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────────────
-- Mandate vertical overrides
-- ─────────────────────────────────────────────────────────────────────────────
INSERT INTO tenant_terminology (vertical, entity, display_singular, display_plural, icon, description) VALUES
  ('mandate', 'opportunity',         'Mandate',            'Mandates',             'clipboard',    'A client mandate or project'),
  ('mandate', 'opportunity_contact', 'Participant',        'Participants',         'user-plus',    'A person involved in a mandate'),
  ('mandate', 'engagement',          'Project',            'Projects',             'folder-open',  'A delivery project under a mandate'),
  ('mandate', 'conversion',          'Completion',         'Completions',          'flag',         'A mandate brought to completion'),
  ('mandate', 'pipeline_stage',      'Phase',              'Phases',               'git-branch',   'A phase of mandate delivery'),
  ('mandate', 'dispatch',            'Mandate Signal',     'Mandate Signals',      'radio',        'A signal relevant to active mandates')
ON CONFLICT (vertical, entity) DO NOTHING;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 5. RLS on new tables (idempotent)
-- ═══════════════════════════════════════════════════════════════════════════════

-- pipeline_stage_labels: read-only platform table, no tenant scoping needed
-- tenant_terminology: read-only platform table, no tenant scoping needed

-- Ensure RLS still active on renamed base tables
DO $$ BEGIN
  EXECUTE 'ALTER TABLE opportunity_contacts ENABLE ROW LEVEL SECURITY';
EXCEPTION WHEN undefined_table THEN NULL;
END $$;

DO $$ BEGIN
  EXECUTE 'ALTER TABLE opportunity_matches ENABLE ROW LEVEL SECURITY';
EXCEPTION WHEN undefined_table THEN NULL;
END $$;

DO $$ BEGIN
  EXECUTE 'ALTER TABLE opportunity_activities ENABLE ROW LEVEL SECURITY';
EXCEPTION WHEN undefined_table THEN NULL;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- Done. Backward-compatible views ensure old queries still work.
-- New code should use: opportunities, opportunity_contacts,
--   opportunity_matches, opportunity_activities, opportunity_id columns.
-- ═══════════════════════════════════════════════════════════════════════════════
