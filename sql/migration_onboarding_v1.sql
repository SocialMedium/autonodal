-- migration_onboarding_v1.sql
-- Onboarding tables: user_intent, watched_people, onboarding_invites
-- Plus new columns on users table
-- Idempotent — safe to re-run

BEGIN;

-- 1. user_intent
CREATE TABLE IF NOT EXISTS user_intent (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       uuid REFERENCES tenants(id),
  user_id         uuid REFERENCES users(id),
  intent_types    text[] NOT NULL,
  horizon_text    text,
  target_outcome  text,
  vertical        text,
  onboarding_step int DEFAULT 1,
  completed_at    timestamptz,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_user_intent_user ON user_intent(user_id);
CREATE INDEX IF NOT EXISTS idx_user_intent_tenant ON user_intent(tenant_id);

-- 2. watched_people
CREATE TABLE IF NOT EXISTS watched_people (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id           uuid REFERENCES tenants(id),
  person_id           uuid REFERENCES people(id),
  canonical_name      text NOT NULL,
  current_title       text,
  current_company     text,
  current_company_id  uuid REFERENCES companies(id),
  linkedin_url        text,
  watch_reason        text,
  watch_context       text CHECK (watch_context IN ('capital_raising', 'hiring', 'bd_prospecting', 'market_tracking', 'portfolio')),
  confidence_floor    float DEFAULT 0.75,
  signal_types        text[],
  huddle_id           uuid,
  opportunity_id      uuid,
  added_by            uuid REFERENCES users(id),
  added_at_onboarding boolean DEFAULT false,
  created_at          timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_watched_people_tenant ON watched_people(tenant_id);
CREATE INDEX IF NOT EXISTS idx_watched_people_person ON watched_people(person_id);
CREATE INDEX IF NOT EXISTS idx_watched_people_opportunity ON watched_people(opportunity_id);

-- 3. onboarding_invites
CREATE TABLE IF NOT EXISTS onboarding_invites (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     uuid REFERENCES tenants(id),
  invited_by    uuid REFERENCES users(id),
  email         text NOT NULL,
  name          text,
  role_context  text,
  huddle_id     uuid,
  token         text UNIQUE DEFAULT encode(gen_random_bytes(24), 'hex'),
  status        text DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'expired')),
  expires_at    timestamptz DEFAULT now() + interval '7 days',
  created_at    timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_onboarding_invites_token ON onboarding_invites(token);
CREATE INDEX IF NOT EXISTS idx_onboarding_invites_email ON onboarding_invites(email);

-- 4. Add onboarding columns to users
ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_complete boolean DEFAULT false;
ALTER TABLE users ADD COLUMN IF NOT EXISTS vertical text;

COMMIT;
