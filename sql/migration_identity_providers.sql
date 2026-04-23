-- ═══════════════════════════════════════════════════════════════════════════
-- Migration: user_identity_providers
-- Shared identity layer for multiple OIDC providers per user.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS user_identity_providers (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  provider       TEXT NOT NULL,           -- 'linkedin' | 'google' | 'microsoft'
  provider_sub   TEXT NOT NULL,           -- stable provider subject ID
  email_at_link  TEXT NOT NULL,           -- email reported by provider at link time
  linked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_used_at   TIMESTAMPTZ,
  UNIQUE (provider, provider_sub)
);

CREATE INDEX IF NOT EXISTS idx_uip_user ON user_identity_providers(user_id);
CREATE INDEX IF NOT EXISTS idx_uip_email ON user_identity_providers(email_at_link);
CREATE INDEX IF NOT EXISTS idx_uip_provider ON user_identity_providers(provider);

-- Backfill existing Google-authenticated users.
-- Autonodal currently identifies Google sign-in only via:
--   users.password_hash = 'oauth_google'
-- and (if Gmail/Drive connected) user_google_accounts row.
--
-- There is no stored Google subject ID, so we use a synthetic sub of 'google-email:{email}'.
-- On next sign-in the reconciliation will match by email and replace the sub with the real one.
INSERT INTO user_identity_providers (user_id, provider, provider_sub, email_at_link, linked_at, last_used_at)
SELECT u.id, 'google',
       'google-email:' || LOWER(u.email),
       LOWER(u.email),
       u.created_at,
       u.updated_at
FROM users u
WHERE u.password_hash = 'oauth_google'
  AND NOT EXISTS (
    SELECT 1 FROM user_identity_providers uip
    WHERE uip.user_id = u.id AND uip.provider = 'google'
  );
