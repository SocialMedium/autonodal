-- ═══════════════════════════════════════════════════════════════════════════════
-- Google user-data privacy hardening (for Google OAuth verification).
-- Triggered by the privacy disclosure audit (docs/audits/proximity_audit_2026-04-22.md
-- plus the Google user data discovery report on the same date).
--
-- This migration:
--   1. Purges email body content from interactions (never should have been stored)
--   2. Purges email body content from external_documents rows sourced from Gmail
--   3. Enables RLS on user_google_accounts (Google OAuth token table)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── 1. Purge any legacy email body content from interactions ───────────────
-- Production schema has no `content` column on `interactions` today, but some
-- deploys had it earlier. Defensive — only runs if the column exists.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'interactions' AND column_name = 'content'
  ) THEN
    EXECUTE $sql$
      UPDATE interactions
      SET content = NULL
      WHERE interaction_type IN ('email','email_sent','email_received')
        AND content IS NOT NULL
    $sql$;
  END IF;
END$$;

-- ─── 2. Purge newsletter body content from external_documents ────────────────
-- Rows sourced from Gmail are identified by source_type='newsletter' +
-- source_url LIKE 'https://mail.google.com/%'.
UPDATE external_documents
SET content = NULL
WHERE source_type = 'newsletter'
  AND source_url LIKE 'https://mail.google.com/%'
  AND content IS NOT NULL;

-- ─── 3. Enable RLS on user_google_accounts ───────────────────────────────────
-- Tokens for user A must never be readable by user B's tenant context.
ALTER TABLE user_google_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_google_accounts FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policy
    WHERE polrelid = 'user_google_accounts'::regclass
      AND polname = 'tenant_isolation_user_google_accounts'
  ) THEN
    CREATE POLICY tenant_isolation_user_google_accounts ON user_google_accounts
      USING (
        current_tenant_id() IS NULL
        OR user_id IN (SELECT id FROM users WHERE tenant_id = current_tenant_id())
      );
  END IF;
END$$;

-- Grant app role read/write (platform pool bypasses this, but app role must be
-- able to operate within tenant scope via the policy).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'autonodal_app') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON user_google_accounts TO autonodal_app;
  END IF;
END$$;

-- ─── 4. Disconnect queue — deleted accounts scheduled for data purge ────────
CREATE TABLE IF NOT EXISTS google_disconnect_queue (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  google_email TEXT NOT NULL,
  disconnected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  purge_after TIMESTAMPTZ NOT NULL,  -- disconnect + 30d
  purged_at TIMESTAMPTZ,
  purge_outcome TEXT
);

CREATE INDEX IF NOT EXISTS idx_google_disconnect_queue_pending
  ON google_disconnect_queue (purge_after) WHERE purged_at IS NULL;
