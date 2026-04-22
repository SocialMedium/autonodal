# Proximity Intelligence — Audit Findings

**Date:** 2026-04-22
**Auditor:** Claude Code
**Repo commit:** 7aaa8d9c89a2e69ef23d91a77861757893d0fadb
**Phase:** 1 (read-only discovery)

---

## Summary — TL;DR

- The 4-factor score (currency · history · weight · reciprocity) is **absent or partial** across the board. Current `relationship_strength` is a **source-bucketed maximum**, not a composite.
- The **populator is not `compute_network_topology.js`** (which writes `company_adjacency_scores`, not `team_proximity`). Edges are written inline by sync scripts (`sync_gmail`, `sync_calendar`, LinkedIn importers) and one ad-hoc script `scripts/calculate_team_proximity.js` that is **not scheduled**.
- The bulk-fetch pattern from the Apr 13 optimisation applies to `compute_network_topology.js` only. The proximity-write paths are mostly already bulk (INSERT … SELECT … FROM CTE) but sync_gmail.js does per-row UPSERTs (one per thread).
- **UI surfacing is uneven.** Dossier, company page, index dashboard feed, and pipeline show some proximity. **People list, search-match cards, signal cards, watchlists show none.**
- **API consistency is poor.** Six different shapes across endpoints. No single `ProximityHint` contract.
- **MCP is partially parity.** `ml_get_person_dossier` returns proximity when `include_proximity=true`; `ml_search_people` does not return any proximity hint.
- **Security isolation is clean.** RLS enforced + forced; no caller-supplied tenant_id in proximity queries; huddle membership validated before cross-tenant pooling.

---

## 1. Schema state

### 1.1 `team_proximity` columns (from `information_schema`, live DB)

| Column | Type | Nullable | Default |
|---|---|---|---|
| `id` | uuid | NO | gen_random_uuid() |
| `person_id` | uuid | NO | — |
| `team_member_id` | uuid | NO | — |
| `relationship_type` | varchar | NO | — |
| `relationship_strength` | double precision | NO | — |
| `connected_date` | date | YES | — |
| `source` | varchar | NO | — |
| `notes` | text | YES | — |
| `metadata` | jsonb | YES | `'{}'::jsonb` |
| `last_interaction_date` | date | YES | — |
| `interaction_count` | integer | YES | 0 |
| `recency_score` | double precision | YES | — (all NULL in samples) |
| `warmth_score` | double precision | YES | — (all NULL in samples) |
| `created_at` | timestamptz | YES | now() |
| `updated_at` | timestamptz | YES | now() |
| `tenant_id` | uuid | YES | `'00000000-…-001'::uuid` |

### 1.2 Columns absent vs. Build Block 1 requirements

- `score_factors` JSONB — **ABSENT**
- `currency_score`, `history_score`, `weight_score`, `reciprocity_score` — **ABSENT** (`recency_score` / `warmth_score` exist but NULL)
- `last_interaction_channel` — **ABSENT** (nearest proxy is `source`: `gmail_sync`, `gcal_sync`, `linkedin_import`, `ezekia`, `pipeline`, `xero_export`)
- `last_interaction_at` (timestamptz) — **ABSENT** (`last_interaction_date` is date-only)
- `first_interaction_at` — **ABSENT**
- `interaction_count_inbound` / `interaction_count_outbound` — **ABSENT**

### 1.3 Indexes

| Index | Columns |
|---|---|
| team_proximity_pkey | (id) UNIQUE |
| team_proximity_person_id_team_member_id_relationship_type_key | (person_id, team_member_id, relationship_type) UNIQUE |
| idx_team_proximity_person | (person_id) |
| idx_team_proximity_team_member | (team_member_id) |
| idx_team_proximity_strength | (relationship_strength DESC) |
| idx_team_prox_person | (person_id) — duplicate of above |
| idx_team_prox_user | (team_member_id) — duplicate |
| idx_team_proximity_tenant | (tenant_id) |
| idx_team_proximity_tenant_person | (tenant_id, person_id) |

**Missing vs. spec:**
- `(tenant_id, person_id, relationship_strength DESC)` — **ABSENT** (critical for "best path per person" JOIN)
- `(tenant_id, member_user_id, last_computed_at)` — **ABSENT** (no `last_computed_at` column either)

### 1.4 Foreign keys + cascade

- `person_id → people.id` ON DELETE CASCADE
- `team_member_id → users.id` ON DELETE CASCADE
- `tenant_id → tenants.id` ON DELETE NO ACTION

### 1.5 RLS

- `relrowsecurity = true`, `relforcerowsecurity = true`.
- Policy `tenant_isolation_team_proximity` (command `*`, qual): `((current_tenant_id() IS NULL) OR (tenant_id IS NULL) OR (tenant_id = current_tenant_id()))` — correct pattern for platform content visibility.

### 1.6 Materialised view / best-entry-point cache

- **None exist.** `pg_matviews` returns empty. Best-entry-point is computed per query.

### 1.7 Live data snapshot

| tenant_id | edges | people | members |
|---|---:|---:|---:|
| `00000000-…-001` (ML) | 51,555 | 45,144 | 6 |
| `a0000000-…-002` | 66 | 55 | 1 |
| `f285f8d5-…` | 12,670 | 12,670 | 1 |

**Score distribution (ML tenant):** strong (≥0.7) 543 · warm (0.4–0.7) 41,991 · cool (0.2–0.4) 9,021 · cold (<0.2) 0. The heavy "warm" skew with no "cold" is a tell — current scoring quantises to a few preset values (0.30, 0.45, 0.55, 0.60, 0.65, 0.70, 0.80, 0.85, 0.90, 1.00).

**Freshness:** `MAX(updated_at)` ≈ 3 hours ago; `MIN` = 2026-03-07. Edges are refreshed by sync scripts as threads/events land, so staleness varies per edge.

---

## 2. Scoring formula state — 4-factor matrix

Populator: [scripts/calculate_team_proximity.js](scripts/calculate_team_proximity.js) (ad-hoc; **not scheduled** — only on-demand or manual). Live edges are written inline by [scripts/sync_gmail.js:497](scripts/sync_gmail.js#L497), [scripts/sync_calendar.js](scripts/sync_calendar.js), [scripts/sync_telegram.js](scripts/sync_telegram.js), LinkedIn importers. All paths use the same UPSERT-with-`GREATEST()` pattern.

| Factor | State | Evidence |
|---|---|---|
| **Currency** (recency decay, per-channel half-lives) | **ABSENT** | `calculate_team_proximity.js:49-51`: `CASE WHEN c.cnt >= 10 THEN 0.85 WHEN c.cnt >= 3 THEN 0.60 ELSE 0.30 END` — bucketed by count only. No decay on `last_interaction_date`. `recency_score` column exists but is all NULL. |
| **History** (tenure floor) | **ABSENT** | No `first_interaction_at` tracked. No logistic curve. A 5-year relationship with one email in the last 6 months scores identically to a 2-week-old relationship with the same count. |
| **Weight** (channel-depth) | **PARTIAL** | Each source has a different max ceiling (meeting 0.90, email 0.85, research note 0.90, LinkedIn 0.80, project 0.85). But edges are UPSERTed per (person, member, relationship_type) with `GREATEST()`, so the strongest single-channel signal wins — **not** a weighted rolling average across channels. |
| **Reciprocity** (inbound/outbound) | **ABSENT** | `interactions` table distinguishes `email_sent`/`email_received` but the proximity CTE groups them together (`interaction_type IN ('email', 'email_sent', 'email_received')`). No inbound/outbound counts persisted. |

### Time window
- **All time.** No rolling 12-month cutoff. A dormant 5-year email series counts as much as a live one.

### Decay semantics
- **No daily decay.** `updated_at` bumps only when a new interaction is written. Edges never soften over time.

### Multi-tenant scoping
- `calculate_team_proximity.js:24`: `const TENANT_ID = process.env.ML_TENANT_ID || '00000000-…-001'`. **Hardcoded to the ML tenant.** Not multi-tenant aware as-written. Sync scripts pick up `tenant_id` from the user being synced, so live writes are correctly scoped; the batch recalc script would need to loop tenants.

### Huddle pooling
- No pooling inside the score computation itself. Pooling happens at query time in specific routes (see §4).

**Verdict:** Path B (score upgrade) per the decision tree.

---

## 3. Pipeline state

### 3.1 What actually populates `team_proximity`

| Writer | Pattern | Scheduled? |
|---|---|---|
| [scripts/calculate_team_proximity.js](scripts/calculate_team_proximity.js) | Bulk: `WITH counts AS (…) INSERT … SELECT` per user × channel | **No** — not in `scripts/scheduler.js` |
| [scripts/sync_gmail.js:497](scripts/sync_gmail.js#L497) | Per-row UPSERT inside the per-thread loop | Yes (sync cron) |
| [scripts/sync_calendar.js](scripts/sync_calendar.js) | Per-row UPSERT | Yes (sync cron) |
| [scripts/sync_telegram.js](scripts/sync_telegram.js) | Per-row UPSERT | Yes (MTProto cron) |
| LinkedIn importers | Per-row UPSERT | Event-driven (user upload) |

`scripts/compute_network_topology.js` — **does not write `team_proximity`**. Despite the prompt's framing, this script populates `company_adjacency_scores` and `network_density_scores`. The Apr 13 optimisation (per memory) applies to that script's bulk-fetch pattern, not to proximity.

### 3.2 Concurrency

`scripts/scheduler.js:2215` — "Concurrency semaphore — max 3 pipelines at once". In effect, applies to all named pipelines including the sync jobs that produce proximity edges.

### 3.3 Runtime

No `compute_network_topology`-equivalent nightly proximity recalc exists. The per-row UPSERT inside `sync_gmail.js` is O(threads × involved-people) per sync run; threads are processed in batches by the caller.

**Any Build Block 1 rewrite must extract the logic into a dedicated `scripts/compute_proximity.js` (or similar) that runs nightly, does a single bulk recalc per tenant, and preserves the bulk-fetch pattern.** It must iterate tenants, not hardcode ML.

---

## 4. API surface matrix

| Endpoint | Returns proximity? | Shape | Query pattern | Huddle-aware? |
|---|---|---|---|---|
| `GET /api/people` ([routes/people.js](routes/people.js)) | **No** | — | — | No |
| `GET /api/people/:id` ([routes/people.js:197-355](routes/people.js#L197-L355)) | Yes | nested `team_proximity[].{name, strength, types, source, last_contact, count}` | Single query; if huddle_id, loops over huddle tenants on `platformPool` | Yes (cross-tenant UNION) |
| `GET /api/people/:id/proximity` | **Does not exist** | — | — | — |
| `GET /api/searches/:id/matches` ([routes/platform.js:423-452](routes/platform.js#L423-L452)) | **No** | `overall_match_score` only | Simple JOIN on people | No |
| `GET /api/signals/brief` ([routes/signals.js:702-1014](routes/signals.js#L702-L1014)) | Yes, aggregated | `prox_connection_count`, `best_connector_name` | LEFT LATERAL per signal; `p.tenant_id = ANY($n)` when huddle | **Yes** |
| `GET /api/signals/:id` ([routes/signals.js:1123-1222](routes/signals.js#L1123-L1222)) | No | — | — | No |
| `GET /api/signals/:id/proximity-graph` ([routes/signals.js:1223-1360](routes/signals.js#L1223-L1360)) | Yes, full graph | `proximity_by_user` json_object_agg per contact | JOIN; hardcoded `tp.tenant_id = $1` | No |
| `GET /api/companies/:id` ([routes/companies.js:550-819](routes/companies.js#L550-L819)) | Yes | `proximity_map[].{team_member, contact, strength, types, source, last_contact, count}` | JOIN with threshold ≥ 0.15 | No |

**Verdict:** Six different shapes across endpoints. No `ProximityHint` contract. Three endpoints (people list, search matches, signal detail) return no proximity at all. Huddle-awareness only on two endpoints.

---

## 5. Huddle context state

### 5.1 Client-side injection — working

[public/huddle-context.js:165-174](public/huddle-context.js#L165-L174): globally wraps `window.api()`. If `activeHuddleId` is set in `sessionStorage`, appends `huddle_id=…` to every outbound request.

### 5.2 Server-side consumption — partial

- `GET /api/people/:id` — reads `huddle_id`, resolves huddle members via `platformPool`, UNIONs proximity from each member tenant. **Works.**
- `GET /api/signals/brief` — reads `huddle_id`, switches `p.tenant_id = ANY($n)` for the lateral subquery. **Works.**
- `GET /api/companies/:id` — **does not read `huddle_id`.** Company dossier proximity is still single-tenant even when a huddle is active.
- `GET /api/signals/:id/proximity-graph` — **does not read `huddle_id`.** Hardcoded to caller's tenant only.
- `GET /api/people` — **does not read `huddle_id`** (and returns no proximity anyway).

### 5.3 Membership validation

[server.js:462-469](server.js#L462-L469) `verifyHuddleMember()`: `SELECT role, status FROM huddle_members WHERE huddle_id=$1 AND tenant_id=$2 AND status='active'`. Called from [routes/signals.js:722-735](routes/signals.js#L722-L735) before expanding scope. Correct.

---

## 6. UI surface matrix

Parity baseline: [public/company.html:278-313](public/company.html#L278-L313) renders "Proximity Map" — dedicated sidebar card, grouped by team member, with per-contact strength % + label + color (emerald / amber / ink-4) + last-contact + count.

| Page | Proximity shown? | Field | Form | Mobile |
|---|---|---|---|---|
| [public/people.html:291-346](public/people.html#L291-L346) list cards | **No** | — | — | — |
| [public/person.html:273-295](public/person.html#L273-L295) dossier | **Partial** | `relationship_strength` as % + Strong/Warm/Cool label | Flat list at top of dossier (no grouping by company, no factor breakdown) | Yes |
| public/search.html / searches.html | **No** (no person cards on this surface) | — | — | — |
| [public/signals.html:420-446](public/signals.html#L420-L446) signal cards | **Hidden in modal** (card shows nothing; modal opens lead_score_breakdown.client_proximity) | points value, not 0–1 | Text in modal only | Yes (modal) |
| [public/index.html](public/index.html) dashboard feed | **Yes, rich** (network-signal cards only) | `prox_connection_count` + `best_connector_name` | Badge + D3 popup via [components/proximity-popup.js](public/components/proximity-popup.js) | Yes |
| [public/pipeline.html:228-252](public/pipeline.html#L228-L252) pipeline cards | **Minimal** | `prox_count` only — no strength | Avatar stack + "N connected" | Yes |
| public/my.html / public/dashboard.html | **Files do not exist** — dashboard panel lives on index.html | — | — | — |

**Gap vs. parity target:** person list (high-traffic triage surface), signal cards (where the person is often the subject), and search-match cards show no inline proximity at all.

---

## 7. Security / isolation state

| Check | Result | Evidence |
|---|---|---|
| RLS enabled + forced on `team_proximity` | ✅ | `relrowsecurity=true`, `relforcerowsecurity=true` |
| RLS policy scopes by `current_tenant_id()` | ✅ | `tenant_isolation_team_proximity`: `((current_tenant_id() IS NULL) OR (tenant_id IS NULL) OR (tenant_id = current_tenant_id()))` — per [sql/migration_rls.sql:82-83](sql/migration_rls.sql#L82-L83) |
| Tenant context set per-connection | ✅ | [lib/TenantDB.js:49](lib/TenantDB.js#L49): `SET LOCAL app.current_tenant = '${this.tenantId}'` |
| No route accepts caller-supplied `tenant_id` for proximity queries | ✅ | grep confirms every proximity query uses `req.tenant_id` from session, not params/body |
| Huddle membership validated before scope expansion | ✅ | `verifyHuddleMember()` is called first in all huddle-aware endpoints |
| `platformPool` bypass usage | ⚠️ Noted but justified | [routes/platform.js:6174-6182](routes/platform.js#L6174-L6182) explicitly documents the user-scoped filter is sufficient when proximity is filtered by `user_id` |

**Verdict:** Security state is clean. No remediation needed in this workstream.

---

## 8. MCP parity state

| Tool | Returns proximity? | Shape |
|---|---|---|
| `ml_search_people` ([scripts/mcp_server.js:117-216](scripts/mcp_server.js#L117-L216)) | **No** | — |
| `ml_get_person_dossier` ([scripts/mcp_server.js:219-374](scripts/mcp_server.js#L219-L374), `include_proximity=true` default) | **Yes (partial)** | `relationship_type`, `relationship_strength`, `connected_date`, team member name — no factor breakdown |

Both tools hardcode `ML_TENANT_ID` (lines 166, 184, 252, 255, 273). Not multi-tenant; not huddle-aware.

Markdown output for dossier (lines 345-351): `## Team Proximity (Who Knows Them)` section with `- [member] — [type] ([strength])` bullets.

---

## 9. Recommended build path

Decision tree → **Path B** (score is partial, needs upgrade).

Execute in this order:

1. **Build Block 1 — 4-factor score.** Add `score_factors` JSONB + per-factor columns + `first_interaction_at` + inbound/outbound counts + `last_interaction_channel`. Extract scoring into `lib/scoring/proximity.js`. Create a scheduled `scripts/compute_proximity.js` that iterates tenants (not hardcoded ML), uses bulk CTE pre-fetches, and writes `score_factors` with every row. Add index `(tenant_id, person_id, relationship_strength DESC)`.
2. **Build Block 2 — `ProximityBadge` component.** Vanilla JS, three variants. Wire onto: `people.html` (compact), `person.html` (expanded — replaces the flat list with grouped/factor-annotated view), `search.html` AI match cards (inline), `signals.html` cards (inline, showing on the card itself — move the breakdown from modal-only), `index.html` dashboard (already has rich feed — extend to use the unified component), `pipeline.html` (upgrade from count-only to strength-aware).
3. **Build Block 3 — API consistency.** Define `ProximityHint` contract. Add it to every person-returning endpoint, computed via CTE JOIN not N+1. Retrofit huddle awareness onto `GET /api/companies/:id` and `GET /api/signals/:id/proximity-graph`. Create `GET /api/people/:id/proximity` as the single canonical endpoint for the expanded-view sheet.
4. **Build Block 4 — MCP parity.** Add `proximity` hint to `ml_search_people` results; expand `ml_get_person_dossier` proximity section to include the 4-factor breakdown. Remove `ML_TENANT_ID` hardcode and thread tenant through from the MCP session context.

### Out-of-scope to flag

- `scripts/calculate_team_proximity.js` is not wired into the scheduler. Fix in Build Block 1 by retiring it in favour of the new `compute_proximity.js`.
- Sync-script per-row UPSERTs are tolerable because they ride the sync batching cadence, but they use `GREATEST()` on `relationship_strength` — once the composite score is in place, this pattern will clobber fresh scores with stale-but-higher ones. Build Block 1 must replace `GREATEST()` with "always recompute on next nightly" semantics.
- Duplicate indexes (`idx_team_proximity_person` vs `idx_team_prox_person`; `idx_team_proximity_team_member` vs `idx_team_prox_user`) — safe to drop but out of this scope.

### What NOT to touch

- RLS policies (already correct — do not weaken to accommodate huddle pooling; use `platformPool` + validated member list like signals.js does).
- `compute_network_topology.js` bulk-fetch pattern (it's a different script; do not refactor as part of this workstream).
- Huddle creation / membership UI (out-of-scope per prompt).

---

*End of findings report. Phase 2 gate: this file is committed. Do not proceed to Build Blocks until the commit lands.*
