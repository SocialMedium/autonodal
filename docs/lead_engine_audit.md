# Positive-Signal Lead Engine — Phase 1 Audit

**Date:** 2026-04-20
**Scope:** Audit codebase state before Phase 2 build. No modifications.

---

## 1.1 Calibration Artefacts

### File inventory

| File | Size | Purpose |
|---|---|---|
| `reports/newsapi_signal_calibration.json` | 26.8 MB | **Primary calibration source.** 40,519 signals / 424 clients / 456 client_detail records. Contains per-signal raw observations with `days_before` lead times. |
| `reports/newsapi_sleep_timers.json` | 9.5 KB | Per-signal-type aggregates with P10/P25/P50/P75/P90 percentiles already computed. |
| `reports/newsapi_compound_patterns.json` | 4.7 KB | Compound signal co-occurrence counts. |
| `reports/newsapi_source_ranking.json` | 961 KB | 2,034 sources ranked by signal yield. |
| `reports/sleep_timers.json` | 3.3 KB | Older aggregate (superseded by newsapi version). |
| `reports/gdelt_signal_calibration.json` | 429 KB | Earlier GDELT-based calibration. |
| `reports/signal_patterns.json` | 541 KB | Pattern analysis output. |

**`signal_scoring_model.json` — NOT PRESENT.** The spec references it but the file doesn't exist.

### What's usable

- **Percentiles**: Already in `newsapi_sleep_timers.json` as `p10/p25/p50/p75/p90_lead_days` per signal type. No reconstruction needed.
- **Raw observations**: `newsapi_signal_calibration.json > client_details[].signals[].days_before` provides per-signal lead times (sample sizes 2,031 for layoffs up to much larger for common types). Can recompute P25/P50/P75/P90 exactly if desired, or use the pre-aggregated values.
- **All 9 signal types populated** with distributions and timer values: `layoffs, capital_raising, ma_activity, strategic_hiring, partnership, geographic_expansion, product_launch, restructuring, leadership_change`.
- **Overall median lead**: 68 days across all types and clients.

### What's missing

- **Polarity markers in the scoring model**: Not explicitly present in calibration files. Already defined as a lookup in `scripts/backfill_health_history.js` and `scripts/compute_signal_index.js` as `sentiment: 'bullish'|'bearish'|'neutral'` — but not persisted to signal_events.
- **Thematic clusters**: Not in calibration output. `compound_patterns.json` has raw co-occurrences (e.g. "capital_raising + strategic_hiring: 19") but no named clusters like "Growth Acceleration".
- **Conversion rates per signal type**: Not computed. The calibration shows signals-preceding-engagement correlation (60.6% overall) but not per-type conversion probabilities.

### Polarity mapping (from existing code)

Found at `scripts/backfill_health_history.js:19-29`:
- **positive**: `capital_raising, product_launch, strategic_hiring, geographic_expansion, partnership` (5)
- **neutral**: `ma_activity, leadership_change` (2)
- **negative**: `layoffs, restructuring` (2)

Matches spec exactly. Can lift this into `lib/signal_polarity.js`.

---

## 1.2 Schema State

### signal_events (current columns)

From `sql/schema.sql:115` and ALTER statements in `server.js`:

| Column | Status |
|---|---|
| `id, signal_type, company_id, company_name, confidence_score` | ✅ Base schema |
| `scoring_breakdown, evidence_doc_ids, evidence_summary, evidence_snippets` | ✅ Base schema |
| `triage_status, triaged_by, triaged_at, triage_notes` | ✅ Base schema |
| `detected_at, signal_date, expires_at, created_at, updated_at` | ✅ Base schema |
| `tenant_id` | ✅ Added by multi-tenant migration |
| `visibility, owner_user_id` | ✅ Added by server.js startup |
| `embedded_at` | ✅ Added by server.js startup |
| `is_megacap, company_tier, source_url, image_url, hiring_implications, source_document_id` | ✅ Referenced in queries, assumed present from migrations |
| **`polarity`** | ❌ **NOT PRESENT** |
| **`first_detected_at`** | ❌ **NOT PRESENT** (only `detected_at` exists) |
| **`phase`** | ❌ **NOT PRESENT** |
| **`critical_at, closing_at, closed_at`** | ❌ **NOT PRESENT** |

### Existing claim/lifecycle infrastructure

**`signal_dispatches` table** (`sql/migration_signal_dispatches.sql` + server.js startup):
- Has `claimed_by UUID` and `claimed_at TIMESTAMPTZ`
- Has `pipeline_stage TEXT` (referenced in `routes/platform.js:7888-7925`, added via ALTER)
- Has `status` enum: `draft, reviewed, sent, archived`
- One-to-one-ish with `signal_events` via `signal_event_id`

**This is the existing "claim" mechanism.** The spec wants a separate `lead_claims` table keyed on `signal_id` directly. Two options:
- **Option A**: Build new `lead_claims` table, deprecate signal_dispatches for claim tracking (keep it for generated content like blog themes).
- **Option B**: Extend `signal_dispatches` with polarity-aware filtering, reuse existing claim columns, add outcome tracking there.

**Recommendation**: Option A. `signal_dispatches` was designed for outbound content generation, not lead lifecycle. Mixing concerns will create confusion. Build new `lead_claims` as spec'd.

### Missing tables per spec

| Table | Status |
|---|---|
| `lead_claims` | ❌ Not present (signal_dispatches is the closest existing) |
| `signal_outcomes` | ❌ Not present |
| `signal_phase_transitions` | ❌ Not present |
| `company_relationships` (for state) | ✅ **PRESENT** — but uses different vocabulary |

### companies.relationship_state

**NOT PRESENT as a column.**

However, there IS a `company_relationships` table (created by `scripts/compute_company_relationships.js`) with column `relationship_tier` using values: `critical, active, monitor, gap, quiet` — these are **engagement levels**, not commercial states. They blend proximity + signals, which doesn't map cleanly to the spec's `active_client / ex_client / warm_non_client / cold_non_client`.

The spec's desired states require:
- Knowledge of last mandate date (available via `conversions` table with `start_date` + `placement_fee`)
- Open mandate detection (available via `opportunities` table with `status` IN active values)
- Proximity threshold derivation (available via `team_proximity.proximity_strength`)

All data exists — the classification function needs to be built.

### Indexes

Existing on signal_events:
- `idx_signals_company, idx_signals_type, idx_signals_status, idx_signals_detected`
- `idx_signal_events_detected, idx_signal_events_company_type_date`
- `idx_signal_events_megacap` (partial)

Missing: `idx_signal_events_tenant_polarity_phase` — needs to be added after polarity/phase columns land.

---

## 1.3 Scoring Logic

### Existing libraries

| File | Purpose |
|---|---|
| `lib/signal_timing.js` | `computeSignalTimingScore(signalType, ageDays, opts)` + `checkForReboot()`. Reads `reports/sleep_timers.json` (old) — needs updating to read `newsapi_sleep_timers.json` or percentile file. |
| `lib/signal_weighter.js` | Source authority tier weighting (official_primary 1.3x through community 0.6x). Applied at ingest time. |
| `lib/proximity-graph.js` | Network topology computation. |
| `lib/team_proximity_queries.js` | Helpers for proximity lookups. |

### Current lead ranking

Located in `routes/signals.js` (the `/api/signals/brief` endpoint, ~line 960+):

```sql
ORDER BY
  -- 0. Exclude megacaps and tenant's own company
  CASE WHEN c.company_tier = 'tenant_company' THEN 2 WHEN se.is_megacap = true THEN 1 ELSE 0 END,
  -- 1. CLIENT PRIORITY
  CASE WHEN EXISTS (... is_client ...) THEN 0 ELSE 1 END,
  -- 2. NETWORK DENSITY (3 tiers)
  CASE WHEN net_density.cnt >= 5 THEN 0 WHEN cnt >= 2 THEN 1 WHEN cnt >= 1 THEN 2 ELSE 3 END,
  -- 3. GEOGRAPHIC RELEVANCE
  -- 4. SIGNAL TYPE HIERARCHY (hiring-intent first)
  -- 5. confidence DESC, detected_at DESC
```

**No polarity filtering**. `restructuring` and `layoffs` signals compete in the same ranking as `capital_raising` and `strategic_hiring`. The spec's matrix (polarity × relationship × proximity × phase) is not implemented.

**Hero signal scoring** (routes/signals.js:~100):
- Client: +100, user proximity: +80 max, team proximity: +40 max, warmth: +60 max, contacts: +25, confidence: +30, image: +20
- Also no polarity filter.

**Market Health Index** (`scripts/compute_signal_index.js`):
- DOES use sentiment (bullish/bearish) to compute blended score
- But this is a single tenant-wide index, not per-lead ranking

### Proximity storage

- `team_proximity` table: `person_id, user_id, proximity_strength DECIMAL(3,2), proximity_type, last_contact_date`
- Per-person, per-team-member edge. Company-level proximity is derived by aggregating across people at that company.
- Used in signals.js via `LATERAL JOIN` with `MAX(tp.relationship_strength) AS strength`.

Column naming inconsistency: schema says `proximity_strength`, most queries use `relationship_strength`. Need to verify actual column name in production DB before building.

---

## 1.4 Email Infrastructure

### Resend integration

- `lib/email.js` exports `sendEmail({to, subject, html, text}), sendWelcome, sendDailyDigest`
- Uses Resend API directly (`https://api.resend.com/emails`)
- `FROM_EMAIL` env var, falls back to `'Autonodal <signals@autonodal.com>'`
- Plain string HTML templates (no MJML, no React-email). `baseLayout()` helper wraps content.

### Existing scheduled emails

| Pipeline | Schedule | Script |
|---|---|---|
| `daily_digest_email` | Mondays 06:45 UTC | `pipelineDailyDigestEmail()` in scheduler.js — actually **weekly** despite the name |
| `waitlist_digest` | Daily 07:22 UTC | `pipelineWaitlistDigest()` |
| `daily_sales_brief` | Weekdays 21:00 UTC (7am AEST) | `scripts/daily_sales_brief.js` (built earlier this session) |

**The `daily_sales_brief.js` script already exists** and covers significant overlap with this spec's Section 2.6. It:
- Pulls 48h signals + older signals in peak/rising window
- Uses `computeSignalTimingScore` from `lib/signal_timing.js`
- Computes composite score (timing 35% + confidence 25% + client 15% + compound 10% + reboot 30%)
- Groups by company, maps best proximity path per team member
- Sends team-wide brief + personalised briefs
- Archives to `reports/daily_briefs/brief_YYYY-MM-DD.json`

**Gaps versus the new spec:**
- No polarity filtering (shows both growth and distress signals in the same ranking)
- No lifecycle phase labels (fresh/warming/hot/critical/closing) — uses older phase names (too_early/rising/approaching_peak/peak_window/declining/dormant)
- No matrix-based relationship state weighting (spec: active_client 1.0, warm_non 0.85, ex_client 0.55, cold 0.20)
- No "critical window — act this week" section with time-perishable items at top regardless of category
- No "claimed leads with pipeline status" section
- No per-section ordering by perishability rather than category

### Consultant ↔ user mapping

`users` table has `id, email, name, role, tenant_id`. All MitchelLake users have `tenant_id = '00000000-0000-0000-0000-000000000001'` and `email LIKE '%mitchellake.com'`. Email delivery iterates users within a tenant.

No user-level timezone column — all scheduling is in UTC with team-wide convention (AEST).

---

## 1.5 Dashboard / UI Surface

### Pages showing leads/signals

| Page | Purpose | Has claim UI? |
|---|---|---|
| `index.html` | Main dashboard with hero signals, network signals, region cards, full feed | ✅ Claim button via dispatch |
| `dispatches.html` | Kanban-ish dispatch feed with claim/review lifecycle | ✅ Primary claim UI |
| `pipeline.html` | Pipeline board with `pipeline_stage` kanban | ✅ Stage transitions |
| `delivery.html` | Post-placement workflow | ✅ Status + members |
| `company.html` | Per-company dossier with signals, artifacts, actions | ✅ Claim from signal card |
| `signals.html` | Full signal feed (detail) | Limited |
| `ecosystem.html` | Company-level ecosystem view | ❌ |

### Huddle context

Already wired correctly:
- `sessionStorage.huddleId` on client
- `huddleParam()` helper in index.html builds `&huddle_id=xxx` query param
- API endpoints (`/api/signals/brief`) read `req.query.huddle_id` and verify membership via `verifyHuddleMember()`
- Cross-tenant signal pooling happens server-side only after membership verification

**Any new endpoints for the lead engine must respect this pattern.** The spec's verification requirement ("huddle context lens continues to filter correctly on all new endpoints") is critical.

### Existing kanban to extend

`pipeline.html` already renders a `pipeline_stage` kanban board with columns: claimed → contacted → meeting → proposal → mandate → lost (inferred from spec, actual values in `signal_dispatches.pipeline_stage`). This is the natural place to surface claimed leads.

---

## 1.6 Ezekia + Relationship Data

### Active client detection

Multi-layered:
- **`companies.is_client BOOLEAN`** — primary flag, backfilled from revenue data (`conversions.placement_fee > 0` → is_client = true). Set at ingest time, re-run nightly via startup migration at `server.js:~630`.
- **`accounts` table** — per-client detailed record with `relationship_status`, `relationship_tier`, `first_engagement_date`, `total_placements`, `annual_value`.
- **Fuzzy matching** exists in server.js startup: `accounts.name ILIKE companies.name` prefix/contains match, plus direct `conversions.client_name_raw = companies.name`.

### Last mandate date

- `conversions.start_date` — populated from Xero import (primary source of truth per CLAUDE.md).
- `conversions.placement_fee > 0 AND source IN ('xero_export', 'xero', 'manual', 'myob_import')` — filters to actual invoiced revenue.
- Can compute `MAX(start_date)` per `company_id` to get last engagement date.

### Ex-client derivation

Not currently a first-class state. Can be computed as:
```sql
is_client = true
AND NOT EXISTS (open mandate for this company)
AND last_mandate_date < NOW() - INTERVAL '18 months'
```

### Warm non-client

- `team_proximity.proximity_strength` per person → aggregate `MAX(...)` per company → threshold at 0.6 (spec: > 0.6 = warm, > 0.3 = cool).
- No cached flag — computed per query today.

### Existing `company_relationships` table

Already has per-company relationship scoring (`relationship_tier`, `relationship_score`, `elevation_tier`) but uses engagement/signal blending vocabulary (critical/active/monitor/gap/quiet). **Keep this table** — it serves a different purpose (relationship health) from the spec's `relationship_state` (commercial lifecycle state).

Add the new `relationship_state` either:
- **As a column on companies**: `ALTER TABLE companies ADD COLUMN relationship_state TEXT`
- **As a column on company_relationships**: `ALTER TABLE company_relationships ADD COLUMN relationship_state TEXT`

**Recommendation**: Column on `companies`. It's a property of the company's commercial relationship with the tenant, not an engagement score. Keeps `company_relationships` focused on engagement health.

---

## Build Plan — Phase 2 Mapping

Each task mapped to a concrete file action.

### 2.1 Signal polarity as first-class

| Task | Action | File |
|---|---|---|
| Add polarity column | **New migration** | `sql/migration_signal_polarity.sql` (new) |
| Polarity lookup | **New build** | `lib/signal_polarity.js` (new) — lift from `backfill_health_history.js` |
| Backfill script | **New build** | `scripts/backfill_signal_polarity.js` (new) |
| Startup migration hook | **Extend existing** | `server.js` — add to migration block at ~line 1015 |

### 2.2 Percentile-based countdown

| Task | Action | File |
|---|---|---|
| Percentile computation | **New build** | `scripts/compute_signal_percentiles.js` (new) — reads `newsapi_signal_calibration.json > client_details[].signals[].days_before`, outputs `reports/signal_timing_percentiles.json` |
| Percentiles JSON | **New output** | `reports/signal_timing_percentiles.json` |
| Lifecycle phase logic | **New build** | `lib/signal_lifecycle.js` (new) with `currentPhase()` and `compoundCompression()` |
| Extend timing helpers | **Modify existing** | `lib/signal_timing.js` — add new phase names (fresh/warming/hot/critical/closing/closed), update to read percentile file |
| Nightly phase advancement | **New build** | `scripts/advance_signal_phases.js` (new). Writes to `signal_phase_transitions` log. |
| Cron registration | **Extend existing** | `scripts/scheduler.js` — add `advance_signal_phases` pipeline, schedule `0 3 * * *` |

### 2.3 Relationship state

| Task | Action | File |
|---|---|---|
| Add column | **New migration** | `sql/migration_companies_relationship_state.sql` — `ALTER TABLE companies ADD COLUMN relationship_state TEXT` |
| Compute function | **New build** | `lib/relationship_state.js` (new) with `computeRelationshipState(companyId, tenantId, db)` |
| Nightly compute | **Extend existing** | `scripts/compute_company_relationships.js` — add relationship_state computation to the existing per-company loop |

### 2.4 Matrix ranking (commercial leads)

| Task | Action | File |
|---|---|---|
| Pure ranking function | **New build** | `lib/lead_scoring.js` (new) with `leadScore()` returning null for non-positive polarity |
| Relationship + phase weights | **New build** | Tables defined in `lib/lead_scoring.js` |
| Replace brief ranking | **Modify existing** | `routes/signals.js` — update `/api/signals/brief` to filter `polarity = 'positive'` and rank by matrix score (or add opt-in query param first for safe rollout) |
| Update hero ranking | **Modify existing** | `routes/signals.js` — `/api/signals/hero` to respect polarity |

### 2.5 Claim lifecycle

| Task | Action | File |
|---|---|---|
| Create `lead_claims` table | **New migration** | `sql/migration_lead_claims.sql` |
| Create `signal_outcomes` table | **New migration** | `sql/migration_signal_outcomes.sql` (combine with 2.5 — one migration file) |
| Startup migration hook | **Extend existing** | `server.js` — add to migration block |
| API routes | **Extend existing** | `routes/signals.js` — add `POST /api/signals/:id/claim`, `POST /api/signals/:id/release`, `PATCH /api/signals/:id/pipeline`, `POST /api/signals/:id/outcome` |
| RLS policies | **Included in migration** | `sql/migration_lead_claims.sql` |

### 2.6 Daily brief (v2 — matrix-aware)

| Task | Action | File |
|---|---|---|
| Rewrite brief builder | **Modify existing** | `scripts/daily_sales_brief.js` — rewrite to use matrix scoring, four-section structure (critical/claimed/warm-paths/hot-net-new), respect polarity |
| Personal brief template | **Modify existing** | Same file — section reordering per perishability |
| Scheduler entry | **Already present** | `scripts/scheduler.js` — `daily_sales_brief` at `0 21 * * 0-4` — keep |
| Dry-run flag | **Modify existing** | `scripts/daily_sales_brief.js` — add `--dry-run` |
| Countdown SVG renderer | **New build** | Inline helper in daily_sales_brief.js or `lib/brief_widgets.js` |

### 2.7 Talent engine (negative signals)

| Task | Action | File |
|---|---|---|
| Talent receptivity curve | **New build** | `lib/talent_refresh_scoring.js` (new) |
| Score pass for negative signals | **New build** | `scripts/score_talent_refresh.js` (new) — runs nightly, writes to `person_scores` (existing table) with a new category |
| Weekly digest | **Out of scope** | Deferred per spec |

### 2.8 Forward calibration capture

| Task | Action | File |
|---|---|---|
| Already covered in 2.5 | `signal_outcomes` table in `sql/migration_lead_claims.sql` | |
| Outcome hook on claim transitions | **Extend existing** | `routes/signals.js` `/api/signals/:id/outcome` handler writes the row |
| Monthly aggregation | **New build** | `scripts/aggregate_forward_calibration.js` (new) — outputs `reports/forward_calibration_YYYY_MM.json` |
| Monthly cron | **Extend existing** | `scripts/scheduler.js` — add monthly pipeline `1 0 1 * *` |

---

## Sequence Confirmation

Per spec:
1. ✅ Phase 1 audit — **this document**
2. ⏸ User reviews and approves before Phase 2 begins
3. Schema migrations (2.1, 2.3, 2.5, 2.8) — all new tables/columns in one deploy
4. Calibration data (2.2) — `compute_signal_percentiles.js` against historical data (no deps)
5. Ranking + lifecycle (2.2, 2.4) — pure functions, unit-testable
6. API routes (2.5) — claim/release/pipeline/outcome
7. Daily brief rewrite (2.6) — dry-run first
8. Talent scoring (2.7) — surface deferred
9. Forward capture wiring (2.8) — must be live before team starts claiming

## Key Decisions Required From User

Before Phase 2:

1. **lead_claims vs signal_dispatches**: Build separate `lead_claims` table as spec'd (recommended) or extend `signal_dispatches`?
2. **relationship_state location**: On `companies` table (recommended) or on `company_relationships`?
3. **Existing daily_sales_brief**: Rewrite in place or preserve as v1 and build v2 alongside for comparison?
4. **Polarity column backfill**: Backfill all existing signal_events immediately or only mark new signals going forward?
5. **Brief rollout**: Switch the brief to matrix-ranked on day one, or run both in parallel with a feature flag for comparison?

## Blocking Issues

None. All prerequisites (calibration data, schema extensibility, email infrastructure, huddle context wiring) are in place.

The build is clean to proceed after the decisions above are confirmed.
