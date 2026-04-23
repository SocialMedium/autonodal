// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/providers/google.js — Google OIDC provider config
// ═══════════════════════════════════════════════════════════════════════════
//
// Skeleton for future migration of the existing Google flow onto this shared
// module. The existing /api/auth/google flow in routes/auth.js remains the
// production path — it has extensive tenant provisioning / invite matching
// logic that would need to be threaded through the generic flow.
//
// This config is already accurate and can be used by the shared module once
// the Google flow is ported over in a later sprint.

module.exports = {
  id: 'google',
  displayName: 'Google',

  authorizationEndpoint: 'https://accounts.google.com/o/oauth2/v2/auth',
  tokenEndpoint: 'https://oauth2.googleapis.com/token',
  userinfoEndpoint: 'https://www.googleapis.com/oauth2/v2/userinfo',

  // Identity-only scopes. Data-access scopes (Gmail, Drive) are requested
  // separately via the existing Google Connect flow in routes/auth.js.
  defaultScopes: ['openid', 'email', 'profile'],

  clientIdEnv: 'GOOGLE_CLIENT_ID',
  clientSecretEnv: 'GOOGLE_CLIENT_SECRET',
  redirectUriEnv: 'GOOGLE_REDIRECT_URI',

  supportsPkce: true,

  mapUserinfo(payload) {
    return {
      provider: 'google',
      sub: payload.id || payload.sub,
      email: payload.email ? payload.email.toLowerCase() : null,
      emailVerified: !!(payload.verified_email || payload.email_verified),
      name: payload.name,
      givenName: payload.given_name,
      familyName: payload.family_name,
      picture: payload.picture || null,
      locale: payload.locale || null,
    };
  },
};
