# `lib/auth-oidc/` — Shared OIDC Module

Provider-agnostic OpenID Connect authorization code flow for Autonodal.

## Architecture

```
lib/auth-oidc/
├── index.js              # Public API
├── flow.js               # Generic OIDC flow (start, exchange, userinfo)
├── pkce.js               # RFC 7636 PKCE helpers
├── state.js              # In-memory CSRF state tracker (10min TTL)
├── reconciliation.js     # Identity → local user matching
├── errors.js             # Structured OIDC error types
└── providers/
    ├── linkedin.js       # ✅ Wired
    ├── google.js         # Skeleton (existing Google flow not yet migrated)
    └── microsoft.js      # Skeleton (not wired — coming sprint)
```

## Usage

```javascript
const oidc = require('../lib/auth-oidc');

// 1. Start flow — build authorize URL
const provider = oidc.getProvider('linkedin');
const { authorizeUrl, state } = oidc.startFlow(provider, {
  returnTo: '/dashboard',
});
res.redirect(authorizeUrl);

// 2. On callback
const entry = oidc.validateCallback(req.query.state);  // throws on replay/expiry
const tokens = await oidc.exchangeCode(provider, req.query.code, entry.codeVerifier);
const identity = await oidc.fetchUserinfo(provider, tokens.accessToken);

// 3. Reconcile with local user DB
const { user, isNew } = await oidc.reconcileIdentity(platformPool, identity, {
  createUser: async ({ email, name, picture, provider }) => {
    // Provision tenant, create user, return { id, email, ... }
  },
});

// 4. Your app's session issuance
const sessionToken = await createSession(user.id);
```

## Required environment variables

Per provider:
```
LINKEDIN_CLIENT_ID=...
LINKEDIN_CLIENT_SECRET=...
LINKEDIN_REDIRECT_URI=https://www.autonodal.com/api/auth/linkedin/callback
```

## Database

Requires `user_identity_providers` table (see `sql/migration_identity_providers.sql`).

## Security properties

- **PKCE (S256)**: every flow generates a random verifier, sends challenge, verifies on exchange
- **State parameter**: CSRF-safe random 256-bit state, single-use, 10min TTL
- **State expiry**: stale state rejected
- **State replay prevention**: state consumed on first use
- **Scopes minimised**: identity-only (`openid profile email`) — no data scopes
- **Token disposal**: access tokens discarded after userinfo fetch (identity-only flow)

## Not in scope

- Data-access OAuth (Gmail sync, LinkedIn connections export) — separate flows
- JWT issuance — reuses Autonodal's existing `sessions` table mechanism
- Multi-process state storage — single-process Map is sufficient for current scale
