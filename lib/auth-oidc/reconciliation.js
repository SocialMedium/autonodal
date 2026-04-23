// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/reconciliation.js — Identity → local user matching
// ═══════════════════════════════════════════════════════════════════════════
//
// PLATFORM-CONTEXT: uses platformPool — reconciliation runs pre-tenant-context
// and must be able to match users across all tenants by email.
//
// Matching order (first hit wins):
//   1. Exact provider + sub match in user_identity_providers → existing user
//   2. Email match in users → existing user, add provider link
//   3. No match → create new user, provision tenant, add provider link
//
// Tenant provisioning logic mirrors routes/auth.js Google flow:
//   - @mitchellake.com → ML tenant
//   - Pending invite → join invited tenant
//   - Otherwise → new individual tenant

const { OidcError, CODES } = require('./errors');

async function reconcileIdentity(platformPool, identity, opts = {}) {
  if (!identity || !identity.provider || !identity.sub || !identity.email) {
    throw new OidcError(CODES.RECONCILIATION_FAILED, 'Identity missing required fields');
  }

  const { provider, sub, email, name, picture } = identity;

  // Step 1: Existing link via provider + sub
  const { rows: linkRows } = await platformPool.query(
    `SELECT uip.user_id, u.id, u.email, u.name, u.role, u.tenant_id
     FROM user_identity_providers uip
     JOIN users u ON u.id = uip.user_id
     WHERE uip.provider = $1 AND uip.provider_sub = $2
     LIMIT 1`,
    [provider, sub]
  );
  if (linkRows.length > 0) {
    await platformPool.query(
      `UPDATE user_identity_providers SET last_used_at = NOW() WHERE provider = $1 AND provider_sub = $2`,
      [provider, sub]
    );
    return { user: linkRows[0], isNew: false, linkCreated: false };
  }

  // Step 2: Email match → add provider link
  const { rows: emailRows } = await platformPool.query(
    'SELECT id, email, name, role, tenant_id FROM users WHERE LOWER(email) = $1 LIMIT 1',
    [email]
  );
  if (emailRows.length > 0) {
    const user = emailRows[0];
    await platformPool.query(
      `INSERT INTO user_identity_providers (user_id, provider, provider_sub, email_at_link, last_used_at)
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT (provider, provider_sub) DO UPDATE SET last_used_at = NOW()`,
      [user.id, provider, sub, email]
    );
    return { user, isNew: false, linkCreated: true };
  }

  // Step 3: No match — create new user + tenant
  const createUser = opts.createUser;
  if (!createUser) {
    throw new OidcError(CODES.RECONCILIATION_FAILED, 'No matching user and no createUser handler supplied');
  }

  const newUser = await createUser({ email, name, picture, provider });
  if (!newUser || !newUser.id) {
    throw new OidcError(CODES.RECONCILIATION_FAILED, 'createUser handler did not return a user');
  }

  // Link the provider
  await platformPool.query(
    `INSERT INTO user_identity_providers (user_id, provider, provider_sub, email_at_link, last_used_at)
     VALUES ($1, $2, $3, $4, NOW())
     ON CONFLICT (provider, provider_sub) DO NOTHING`,
    [newUser.id, provider, sub, email]
  );

  return { user: newUser, isNew: true, linkCreated: true };
}

/**
 * Get all identity providers linked to a user.
 */
async function getLinkedProviders(platformPool, userId) {
  const { rows } = await platformPool.query(
    `SELECT provider, email_at_link, linked_at, last_used_at
     FROM user_identity_providers WHERE user_id = $1
     ORDER BY linked_at`,
    [userId]
  );
  return rows;
}

/**
 * Unlink a provider. Refuses to unlink the last remaining provider.
 */
async function unlinkProvider(platformPool, userId, providerId) {
  const { rows: existing } = await platformPool.query(
    'SELECT provider FROM user_identity_providers WHERE user_id = $1',
    [userId]
  );
  if (existing.length <= 1) {
    throw new OidcError(CODES.RECONCILIATION_FAILED, 'Cannot unlink the last remaining identity provider');
  }
  const { rowCount } = await platformPool.query(
    'DELETE FROM user_identity_providers WHERE user_id = $1 AND provider = $2',
    [userId, providerId]
  );
  return { unlinked: rowCount > 0 };
}

module.exports = { reconcileIdentity, getLinkedProviders, unlinkProvider };
