# CLAUDE.md — MLX Intelligence / Autonodal Signal Platform

## Route Architecture

All 279 API routes are extracted from server.js into 8 route modules under `routes/`.
server.js retains only initialisation, middleware, shared helpers, static page routes, and module mounts.

| Module | Routes | Scope |
|--------|--------|-------|
| `routes/public.js` | 9 | Unauthenticated: `/api/public/*`, `/api/health*`, `/api/waitlist` |
| `routes/auth.js` | 8 | Google OAuth, session: `/api/auth/*` |
| `routes/admin.js` | 15 | Admin panel: `/api/admin/waitlist*`, `/api/admin/users*`, `/api/admin/health`, `/api/admin/ingestion`, `/api/admin/tenant`, `/api/admin/audit`, sales import, LinkedIn upload, sync triggers |
| `routes/people.js` | 12 | People CRUD + enrich: `/api/people/*` |
| `routes/companies.js` | 14 | Companies CRUD + enrich + relationships + jobs/ATS: `/api/companies/*` |
| `routes/signals.js` | 17 | Signal intelligence: `/api/signals/*`, `/api/signal-index/*`, `/api/market-temperature`, `/api/talent-in-motion`, `/api/converging-themes`, `/api/top-podcasts`, `/api/reengage-windows` |
| `routes/onboarding.js` | 29 | 4-phase onboarding: `/api/onboarding/*` (wizard, field-mapping, AI, feed-config, diagnostic, health) |
| `routes/platform.js` | 168 | Everything else: feeds, searches/opportunities, huddles, dispatches, placements, network, documents, grabs, chat/AI, case studies, profile, messaging, billing, xero, CRM, events, pipeline, delivery, activities, audit, jobs, enrichment, ecosystem |

## Shared Helpers (server.js)

These are defined in server.js and passed as dependencies to route modules:

- `authenticateToken`, `optionalAuth`, `requireAdmin` — auth middleware
- `auditLog` — structured audit logging
- `generateQueryEmbedding`, `qdrantSearch` — vector embedding + search
- `cachedResponse`, `setCachedResponse` — response cache (2min TTL)
- `endpointLimit`, `safeError` — rate limiting + error sanitisation
- `getGoogleToken`, `sendEmail`, `verifyHuddleMember` — integration helpers
- `REGION_MAP`, `REGION_CODES`, `NICKNAMES` — shared constants

## Key Conventions

- Route modules export a factory function: `module.exports = function(deps) { ... return router; }`
- Dependencies are explicitly passed, never captured from server.js closure scope
- Inline `require()` inside route handlers use `../lib/` or `../scripts/` paths (relative to `routes/`)
- `TenantDB` is used for tenant-scoped queries; `platformPool` for cross-tenant or platform-level queries
- RLS policies must include `tenant_id IS NULL` for platform content visibility

## Database

- Application pool uses `DATABASE_URL_APP` (non-superuser, RLS enforced)
- `platformPool` is the cross-tenant pool from `lib/TenantDB`
- Xero is SOR for revenue; WIP is pipeline only, never revenue
- Ingestion must be deterministic; AI only for ambiguity resolution

## Deployment

- Railway (production), env vars in Railway dashboard
- Domain: autonodal.com (public), app served from same origin
