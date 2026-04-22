# Proximity Intelligence — API + Scoring Model

## The contract

Every endpoint that returns a person attaches a `proximity` field (or `proximity_hint` on the dossier) in this canonical shape:

```typescript
interface ProximityHint {
  best_path: {
    member_user_id: string;
    member_name: string;
    score: number;              // 0.0 – 1.0
    band: "strong" | "warm" | "cool" | "cold";
    last_contact_at: string | null;       // ISO
    last_contact_channel: string | null;  // see channel vocabulary below
    factors?: {                            // only on expanded endpoints
      currency:    { score: number; last_contact_days: number | null; channel: string | null };
      history:     { score: number; tenure_years: number; first_contact_at: string | null };
      weight:      { score: number; weighted_interactions_12mo: number; dominant_channel: string | null };
      reciprocity: { score: number; inbound: number; outbound: number; ratio: number | null };
    };
  } | null;
  backup_paths_count: number;   // paths with score >= 0.2, excluding best_path
  pooled: boolean;              // true when resolved via huddle lens
  paths?: Array<...>;           // only on /api/people/:id/proximity
}
```

Bands:
- `strong` — score ≥ 0.70
- `warm`   — 0.40 ≤ score < 0.70
- `cool`   — 0.20 ≤ score < 0.40
- `cold`   — score < 0.20 (rarely surfaced — default threshold is 0.20)

## Endpoints that return `ProximityHint`

| Endpoint | Field | Shape |
|---|---|---|
| `GET /api/people` | `proximity` on each row | best_path + backup_count |
| `GET /api/people/:id` | `proximity_hint` (+ legacy `proximity`, `huddle_proximity`) | full `paths[]` with factors |
| `GET /api/people/:id/proximity` | body | `{ person_id, paths[], best_path, backup_paths_count, pooled }` |
| `GET /api/searches/:id/matches` | `proximity` on each match | best_path + backup_count |
| `GET /api/companies/:id` | `proximity_map[]` (company's contacts) + `proximity_pooled` | composite-score rows with factors |
| MCP `ml_search_people` | `proximity` in result payload | best_path |
| MCP `ml_get_person_dossier` (`include_proximity=true`) | "Team Proximity" markdown section | 4-factor per-path breakdown |

## Huddle lens

When the client is in huddle context, the browser-side `api()` wrapper appends `huddle_id=<uuid>` to every request. Server-side, endpoints marked huddle-aware:
- validate the caller is an active `huddle_members` row for that huddle (`verifyHuddleMember`)
- resolve the set of member tenant_ids
- query team_proximity across those tenants via `platformPool` (bypasses per-tenant RLS intentionally, since pooling is authorized by membership)
- set `pooled: true` on resolved hints so the UI can render a coalition indicator

Endpoints currently huddle-aware: `/api/people`, `/api/people/:id`, `/api/people/:id/proximity`, `/api/companies/:id`, `/api/signals/brief`.

## Scoring model — 4 factors

`relationship_strength` is the clamped weighted sum of four independent signals, each in `[0, 1]`:

```
relationship_strength = clamp(
    0.35 * currency       # channel-aware recency decay
  + 0.20 * history        # tenure floor
  + 0.25 * weight         # channel-depth weighted volume (12mo)
  + 0.20 * reciprocity    # inbound / (inbound + outbound)
)
```

### Currency — exponential decay with channel-aware half-life

```
currency = 0.5 ^ (days_since_last_contact / half_life_days[channel])
```

Half-lives:

| Channel | Half-life (days) |
|---|---:|
| `in_person_meeting` | 180 |
| `video_call`        | 120 |
| `phone_call`        | 120 |
| `email_reciprocal`  |  90 |
| `email_one_way`     |  45 |
| `linkedin_message`  |  30 |
| `linkedin_reaction` |  14 |
| `research_note`     |  60 |
| unknown             |  45 |

### History — logistic with floor

```
raw = 1 / (1 + exp(-0.55 * (tenure_years - 1)))
history = max(0.15, raw)
```

At 5 years ≈ 0.85. At 10 years ≈ 0.96. Never falls below 0.15 once a relationship exists.

### Weight — saturating weighted sum over 12 months

```
weight = clamp(
  Σ (count_12mo[channel] * channel_weight[channel])
  / 10
)
```

Channel weights:

| Channel | Weight |
|---|---:|
| `in_person_meeting` | 1.00 |
| `video_call`        | 0.80 |
| `phone_call`        | 0.80 |
| `email_reciprocal`  | 0.60 |
| `email_one_way`     | 0.30 |
| `linkedin_message`  | 0.20 |
| `linkedin_reaction` | 0.15 |
| `research_note`     | 0.35 |

Saturates at weighted_interactions = 10.

### Reciprocity — directed ratio with min-n floor

```
total = inbound + outbound
if total < 3: reciprocity = 0.5      # neutral — avoid divide-by-small-n
else:         reciprocity = inbound / total
```

High reciprocity = they engage back. Low = one-way outreach.

### Email reciprocal vs one-way

A (person, user) pair is classified `email_reciprocal` if it has BOTH inbound and outbound email in the last 18 months. Otherwise `email_one_way`. This routes the pair through different currency half-lives and weight buckets.

## Schema

`team_proximity` rows (post-migration):

```sql
relationship_type = 'composite'          -- one composite row per (person, member, tenant)
relationship_strength NUMERIC             -- 0-1 composite score
currency_score / history_score / weight_score / reciprocity_score NUMERIC(4,3)
score_factors JSONB                       -- full breakdown (see interface above)
first_interaction_at / last_interaction_at TIMESTAMPTZ
last_interaction_channel TEXT             -- channel name at time of last contact
interaction_count_inbound / _outbound INTEGER
last_computed_at TIMESTAMPTZ
```

Indexes:
- `(tenant_id, person_id, relationship_strength DESC)` — best-path-per-person lookups
- `(tenant_id, team_member_id, last_computed_at)` — freshness checks per member

Per-channel edges (written by sync scripts with source-bucketed scoring) remain in the table alongside the composite row. Queries for "best path" filter on `relationship_type = 'composite'`.

## Pipeline

`scripts/compute_proximity.js` runs nightly at 02:45 per scheduler entry. It:
1. Iterates every tenant from the `tenants` table
2. Pulls pre-aggregated per-(person, user) stats in a single CTE query with per-channel 12-month counts, first/last timestamps, in/out totals
3. Classifies each email pair as reciprocal or one-way
4. Composes the 4-factor score via `lib/scoring/proximity.js`
5. Bulk UPSERTs composite rows with `relationship_type='composite'` in 2000-row UNNEST batches

Current runtime (production data, 3 tenants): ~10 seconds end-to-end. Target: ≤ 10 minutes. Writes roll through pooled connections — no per-row round trips.

## UI component

`public/components/proximity-badge.js` — vanilla JS. Exposes `window.ProximityBadge` with:

- `render(hint, 'compact')` — one-line list-card badge (handshake icon + band label + initials)
- `render(hint, 'inline')` — signal-card / match-card badge with member name + band + pct + backup count
- `renderExpanded(hint, opts)` — dossier sidebar block with factor breakdown, medal ordering (1st/2nd/3rd), Request-intro / Log-interaction actions, coalition indicator when `pooled`

Colour band:
- strong → emerald `#10B981`
- warm   → blue `#2563EB`
- cool   → amber `#F59E0B`
- cold   → slate `#94A3B8`

## Security

- `team_proximity` has RLS enabled + forced with policy `tenant_isolation_team_proximity`.
- No route accepts `tenant_id` from request params; all use `req.tenant_id` from the authenticated session.
- Huddle pooling uses `verifyHuddleMember` + `huddle_members` table as authorization source — the caller must be an active member of the named huddle before pooling widens tenant scope.
- Work artifacts are never involved in proximity scoring or queries — proximity lives in `team_proximity` only; artifact queries go through their own collection with mandatory `tenant_id` filter.
