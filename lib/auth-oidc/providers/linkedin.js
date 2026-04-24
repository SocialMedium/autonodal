// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/providers/linkedin.js — LinkedIn OIDC provider config
// ═══════════════════════════════════════════════════════════════════════════

module.exports = {
  id: 'linkedin',
  displayName: 'LinkedIn',

  // OIDC endpoints (LinkedIn's OpenID Connect migration completed 2024)
  authorizationEndpoint: 'https://www.linkedin.com/oauth/v2/authorization',
  tokenEndpoint: 'https://www.linkedin.com/oauth/v2/accessToken',
  userinfoEndpoint: 'https://api.linkedin.com/v2/userinfo',

  // Standard OIDC scopes
  defaultScopes: ['openid', 'profile', 'email'],

  // Env var conventions
  clientIdEnv: 'LINKEDIN_CLIENT_ID',
  clientSecretEnv: 'LINKEDIN_CLIENT_SECRET',
  redirectUriEnv: 'LINKEDIN_REDIRECT_URI',

  // LinkedIn's OIDC flow claims PKCE support but observed invalid_client rejections
  // when code_verifier is present on tokens endpoint. Disabled until reproducible.
  supportsPkce: false,

  // Map LinkedIn userinfo response to normalised identity
  mapUserinfo(payload) {
    return {
      provider: 'linkedin',
      sub: payload.sub,
      email: payload.email ? payload.email.toLowerCase() : null,
      emailVerified: !!payload.email_verified,
      name: payload.name,
      givenName: payload.given_name,
      familyName: payload.family_name,
      picture: payload.picture || null,
      locale: payload.locale && typeof payload.locale === 'object' ? payload.locale.country : payload.locale || null,
    };
  },
};
