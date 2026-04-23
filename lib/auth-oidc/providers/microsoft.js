// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/providers/microsoft.js — Microsoft OIDC provider config
// ═══════════════════════════════════════════════════════════════════════════
//
// SKELETON — not wired this sprint. Included so the shared module's provider
// registry is complete and the sign-in UI can render a "coming soon" button.
//
// To activate: register an Azure AD app, set MICROSOFT_CLIENT_ID +
// MICROSOFT_CLIENT_SECRET in env, add /api/auth/microsoft and
// /api/auth/microsoft/callback routes wired to the shared flow.

module.exports = {
  id: 'microsoft',
  displayName: 'Microsoft',

  // Common endpoint supports both personal and work/school accounts
  authorizationEndpoint: 'https://login.microsoftonline.com/common/oauth2/v2.0/authorize',
  tokenEndpoint: 'https://login.microsoftonline.com/common/oauth2/v2.0/token',
  userinfoEndpoint: 'https://graph.microsoft.com/oidc/userinfo',

  defaultScopes: ['openid', 'profile', 'email'],

  clientIdEnv: 'MICROSOFT_CLIENT_ID',
  clientSecretEnv: 'MICROSOFT_CLIENT_SECRET',
  redirectUriEnv: 'MICROSOFT_REDIRECT_URI',

  supportsPkce: true,
  enabled: false, // set to true when wired

  mapUserinfo(payload) {
    return {
      provider: 'microsoft',
      sub: payload.sub,
      email: (payload.email || payload.preferred_username || '').toLowerCase() || null,
      emailVerified: true, // Microsoft verifies emails before account creation
      name: payload.name,
      givenName: payload.given_name,
      familyName: payload.family_name,
      picture: payload.picture || null,
      locale: payload.locale || null,
    };
  },
};
