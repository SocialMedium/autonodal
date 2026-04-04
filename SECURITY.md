# Security Architecture — Autonodal

## Data Sovereignty Model

Each user's data lives in an isolated tenant sandbox.
Data never merges between tenants. Users lend proximity
to collaborative contexts — data never moves.

## Isolation Layers

### Layer 1: Application
All database queries include `WHERE tenant_id = :tenantId`
derived from the validated session token. Tenant ID cannot be
supplied by user input — it is resolved from the authenticated
session in middleware before any query runs.

### Layer 2: Database RLS
PostgreSQL Row Level Security enforces isolation at the
database layer. All sensitive tables have RLS enabled with
`FORCE ROW LEVEL SECURITY`. Policies use `current_tenant_id()`
helper function to restrict row visibility.

### Layer 3: Cron Job Isolation
Background pipelines (harvest, scoring, matching) operate
through TenantDB which sets `SET LOCAL app.current_tenant`
per-query within a transaction scope.

## Platform Signals

Signals from global macro sources (SEC, PR Newswire, Business Wire,
Reuters, FT) and curated catalog sources have `tenant_id = NULL`
and are visible to all tenants. These are filtered per-tenant by
the user's declared signal dial (sectors, geographies, intents).

Private signals (from user-connected sources like Gmail, LinkedIn
imports) retain their `tenant_id` and are invisible to other tenants.

## Huddle Privacy

Huddle members see: relationship strength score, recency category,
reciprocity indicator.

Huddle members never see: message content, platform source, exact
timing, contact email addresses, phone numbers, or any raw
interaction data.

On huddle exit: all contributed data visibility is removed
immediately. Not gradually — immediately. The exiting member's
sandbox is completely unchanged.

## OAuth Token Security

Google access and refresh tokens should be encrypted at rest using
AES-256-GCM with a per-deployment encryption key stored in the
`ENCRYPTION_KEY` environment variable. The `lib/crypto.js` module
provides `encryptToken()` and `decryptToken()` functions.

## Security Headers

All responses include:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-XSS-Protection: 1; mode=block`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `Permissions-Policy: camera=(), microphone=(), geolocation=()`
- `Strict-Transport-Security` (on HTTPS)

## Rate Limiting

- Global: 200 requests/minute per IP+tenant
- Auth endpoints: 20 requests/minute per IP
- Waitlist: 5 requests/minute per IP

## Recommended Hardening (TODO)

1. **Non-superuser DB role**: Create `autonodal_app` role for
   application connections. The current `postgres` superuser
   bypasses FORCE ROW LEVEL SECURITY.

2. **OAuth token encryption**: Deploy `ENCRYPTION_KEY` env var
   and run `scripts/encrypt_existing_tokens.js` to backfill.

3. **Google OAuth verification**: Submit consent screen for
   production verification (2-6 week process).

## Vulnerability Disclosure

Report security issues to: security@autonodal.com
We aim to acknowledge within 24 hours and patch within 7 days.
