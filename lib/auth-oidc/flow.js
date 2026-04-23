// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/flow.js — Generic OIDC Authorization Code + PKCE flow
// ═══════════════════════════════════════════════════════════════════════════

const pkce = require('./pkce');
const stateStore = require('./state');
const { OidcError, CODES } = require('./errors');

/**
 * Start an OIDC authorization code flow.
 *
 * @param {object} provider - provider config (from providers/linkedin.js etc)
 * @param {object} opts - { returnTo?: string, scopes?: string[] }
 * @returns {{ authorizeUrl: string, state: string }}
 */
function startFlow(provider, opts = {}) {
  if (!provider) throw new OidcError(CODES.UNKNOWN_PROVIDER, 'No provider supplied');
  const clientId = process.env[provider.clientIdEnv];
  if (!clientId) throw new OidcError(CODES.NOT_CONFIGURED, `${provider.displayName} client ID not configured (${provider.clientIdEnv})`);

  const redirectUri = process.env[provider.redirectUriEnv] || opts.redirectUri;
  if (!redirectUri) throw new OidcError(CODES.NOT_CONFIGURED, `${provider.displayName} redirect URI not configured`);

  const scopes = (opts.scopes || provider.defaultScopes).join(' ');
  const state = stateStore.generate();
  const pkcePair = provider.supportsPkce ? pkce.generatePair() : null;

  // Store state → verifier + return_to, single-use, 10min TTL
  stateStore.store(state, {
    provider: provider.id,
    codeVerifier: pkcePair ? pkcePair.verifier : null,
    returnTo: opts.returnTo || '/',
  });

  const params = new URLSearchParams({
    response_type: 'code',
    client_id: clientId,
    redirect_uri: redirectUri,
    scope: scopes,
    state,
  });
  if (pkcePair) {
    params.set('code_challenge', pkcePair.challenge);
    params.set('code_challenge_method', pkcePair.method);
  }

  return {
    authorizeUrl: provider.authorizationEndpoint + '?' + params.toString(),
    state,
  };
}

/**
 * Validate state on callback and return the stored flow context.
 * @returns {{ provider, codeVerifier, returnTo }}
 */
function validateCallback(state) {
  if (!state) throw new OidcError(CODES.STATE_MISMATCH, 'Missing state parameter');
  const entry = stateStore.consume(state);
  if (!entry) throw new OidcError(CODES.STATE_MISMATCH, 'State parameter invalid, expired, or replayed');
  return entry;
}

/**
 * Exchange authorization code for tokens.
 *
 * @returns {{ accessToken, idToken?, expiresIn, refreshToken? }}
 */
async function exchangeCode(provider, code, codeVerifier, redirectUri) {
  const clientId = process.env[provider.clientIdEnv];
  const clientSecret = process.env[provider.clientSecretEnv];
  const uri = redirectUri || process.env[provider.redirectUriEnv];

  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: uri,
    client_id: clientId,
    client_secret: clientSecret,
  });
  if (codeVerifier) body.set('code_verifier', codeVerifier);

  const res = await fetch(provider.tokenEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  let data;
  try {
    data = await res.json();
  } catch (e) {
    throw new OidcError(CODES.TOKEN_EXCHANGE_FAILED, `${provider.displayName} token endpoint returned non-JSON`);
  }

  if (!res.ok || !data.access_token) {
    // Don't leak error details from provider — log structured info only
    const providerError = data.error || 'unknown';
    throw new OidcError(CODES.TOKEN_EXCHANGE_FAILED, `${provider.displayName} token exchange failed: ${providerError}`, data);
  }

  return {
    accessToken: data.access_token,
    idToken: data.id_token || null,
    expiresIn: data.expires_in || null,
    refreshToken: data.refresh_token || null,
    tokenType: data.token_type || 'Bearer',
  };
}

/**
 * Fetch userinfo using the access token. Returns normalised identity.
 */
async function fetchUserinfo(provider, accessToken) {
  const res = await fetch(provider.userinfoEndpoint, {
    headers: { Authorization: 'Bearer ' + accessToken },
  });
  if (!res.ok) {
    throw new OidcError(CODES.USERINFO_FAILED, `${provider.displayName} userinfo returned ${res.status}`);
  }
  let payload;
  try { payload = await res.json(); } catch (e) {
    throw new OidcError(CODES.USERINFO_FAILED, `${provider.displayName} userinfo returned non-JSON`);
  }
  const identity = provider.mapUserinfo(payload);
  if (!identity.sub) throw new OidcError(CODES.USERINFO_FAILED, `${provider.displayName} userinfo missing sub`);
  if (!identity.email) throw new OidcError(CODES.NO_EMAIL, `${provider.displayName} userinfo missing email`);
  return identity;
}

module.exports = { startFlow, validateCallback, exchangeCode, fetchUserinfo };
