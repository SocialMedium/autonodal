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
  // Trim to defend against trailing whitespace/newlines on env var paste
  const clientId = (process.env[provider.clientIdEnv] || '').trim();
  const clientSecret = (process.env[provider.clientSecretEnv] || '').trim();
  const uri = redirectUri || process.env[provider.redirectUriEnv];

  console.log(`[OIDC ${provider.id}] exchangeCode — client_id: ${clientId ? clientId.slice(0, 6) + '...' + clientId.slice(-4) : 'MISSING'} (${clientId.length} chars), redirect_uri: ${uri}, secret: ${clientSecret ? clientSecret.slice(0, 8) + '... (' + clientSecret.length + ' chars)' : 'MISSING'}, pkce: ${codeVerifier ? 'yes' : 'no'}`);

  async function tryExchange(authMethod) {
    const body = new URLSearchParams({
      grant_type: 'authorization_code',
      code,
      redirect_uri: uri,
      client_id: clientId, // LinkedIn requires client_id in body even when Basic auth is used
    });
    if (codeVerifier) body.set('code_verifier', codeVerifier);
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    if (authMethod === 'basic') {
      // client_secret_basic: secret in Authorization header, client_id still in body
      headers['Authorization'] = 'Basic ' + Buffer.from(`${clientId}:${clientSecret}`).toString('base64');
    } else {
      // client_secret_post: client_secret in form body
      body.set('client_secret', clientSecret);
    }
    const res = await fetch(provider.tokenEndpoint, { method: 'POST', headers, body: body.toString() });
    let data = {};
    try { data = await res.json(); } catch (e) { /* handled below */ }
    console.log(`[OIDC ${provider.id}] token endpoint (${authMethod}) status ${res.status}, error: ${data.error || 'none'}, description: ${data.error_description || 'none'}`);
    return { res, data };
  }

  // Try form-body credentials first (LinkedIn's documented method)
  let { res, data } = await tryExchange('post');

  // Retry with HTTP Basic if LinkedIn rejects the form-body auth with invalid_client
  if (!res.ok && data.error === 'invalid_client') {
    console.log(`[OIDC ${provider.id}] retrying with client_secret_basic (HTTP Basic auth)`);
    ({ res, data } = await tryExchange('basic'));
  }

  if (!res.ok || !data.access_token) {
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
