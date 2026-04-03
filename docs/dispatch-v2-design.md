# Dispatch v2: Intelligence Primitive Architecture

## 1. Product Evolution Overview

The current dispatch system works as a signal-to-outreach pipeline: detect signal → map network proximity → generate blog content → assign distribution. The `signal_dispatches` table stores proximity maps, approach angles, blog posts, and send lists. The `signal_grabs` table stores short editorial observations clustered by theme/geography.

Dispatch v2 evolves both of these into a single structured intelligence primitive that can power:
- public-facing content pages on mitchellake.com
- SEO/LLM-discoverable observation pages
- pattern/theme aggregation pages
- internal briefings and BD notes
- downstream automation (newsletter, social, escalation)

The shift: dispatches stop being "generated blog posts attached to signals" and become **canonical intelligence objects** where the structured data is the primary asset and rendered copy is a derived output.

**What stays:** signal ingestion, proximity mapping, confidence scoring, the signal_events pipeline, entity models, the scheduling framework, the grabs editorial voice.

**What changes:** the dispatch object model expands dramatically; generation produces structured fields not prose; provenance becomes explicit; rendering becomes a separate layer; patterns become a first-class entity.

---

## 2. Trust and Anti-Hallucination Architecture

### 2.1 Claim Provenance Model

Every interpretive statement in a dispatch is decomposed into typed claims:

| claim_type | rule | publication gate |
|---|---|---|
| `fact` | Must trace to ≥1 source with URL | auto-allowed |
| `interpretation` | Must trace to ≥2 converging sources OR be marked low-confidence | requires confidence ≥ 0.6 |
| `implication` | Must trace to interpretation + structured context | requires confidence ≥ 0.7 |
| `forecast` | Must trace to pattern + evidence convergence | blocked from public unless reviewed |

### 2.2 Evidence Density Scoring

```
evidence_density = (source_count * 0.3) + (avg_source_quality * 0.3) + (claim_coverage_ratio * 0.4)

where:
  source_count = min(1.0, supporting_sources.length / 5)
  avg_source_quality = mean(source.quality_score) across supporting_sources
  claim_coverage_ratio = claims_with_2plus_sources / total_claims
```

Dispatches with `evidence_density < 0.3` are suppressed from public rendering.

### 2.3 Suppression Logic

A dispatch enters `suppression_state = 'suppressed'` when:
- `evidence_density_score < 0.3`
- `confidence_score < 0.4`
- All claims are single-sourced AND claim_type is not `fact`
- Sources do not converge (no 2 sources support the same thesis)

Suppressed dispatches are still stored — they can contribute to future patterns — but they cannot be published or rendered publicly.

### 2.4 Provenance Score

```
provenance_score = (
  (claims_with_source_refs / total_claims) * 0.5 +
  (sources_with_urls / total_sources) * 0.3 +
  (entities_from_structured_data / total_entities) * 0.2
)
```

### 2.5 Editorial Gating

| visibility_level | gate |
|---|---|
| `internal_only` | No gate — all dispatches visible internally |
| `draft_public` | evidence_density ≥ 0.3, provenance ≥ 0.5, no forecast claims |
| `published` | editorial_score ≥ 0.6, reviewed_by is set, no suppression flags |
| `featured` | published + editorial review + pattern attachment |

### 2.6 Generation-Time Anti-Hallucination

The LLM generation prompt must:
1. Receive only verified source material (no hallucinated context)
2. Output structured JSON with explicit `source_refs` per claim
3. Be instructed to return `null` for fields where evidence is insufficient
4. Never generate funding amounts, timelines, or causal links without source backing
5. Include a `restraint_note` field: what the dispatch is deliberately NOT claiming

The generation pipeline validates post-generation:
- Every `source_ref` in claims must match a provided source ID
- Any claim referencing a source not in the input set is stripped
- Claims without source refs are downgraded to `support_level: 'unsupported'` and blocked from publication

---

## 3. Dispatch v2 Domain Model

### 3.1 Relationship to Existing Tables

```
signal_events (exists) ──→ dispatch_v2 (new, replaces signal_dispatches for new flow)
                              │
                              ├── dispatch_sources (new)
                              ├── dispatch_claims (new)
                              ├── dispatch_entities (new)
                              ├── dispatch_related_assets (new)
                              └── patterns (new) ←── pattern_dispatches (new, join table)

signal_dispatches (exists) ── remains for legacy, migrated incrementally
signal_grabs (exists) ── grabs become a lightweight dispatch_v2 variant
```

### 3.2 Core Object Shape

```
DispatchV2 {
  // Identity
  id, slug, tenant_id, status, created_at, updated_at, published_at
  source_cluster_id    // signal_grabs cluster or signal grouping that spawned this
  pattern_id           // nullable, set when dispatch joins a pattern
  generation_version   // tracks which prompt/pipeline version produced this

  // Scores
  confidence_score     // overall signal confidence (inherited + computed)
  editorial_score      // quality of the editorial output
  seo_score            // structured metadata completeness
  shareability_score   // public-readiness
  provenance_score     // source traceability
  evidence_density_score
  suppression_state    // active | suppressed | deferred | withdrawn

  // Editorial
  title, thesis, summary, observation, implication
  non_obvious_angle, watch_next, restraint_note
  why_it_matters_for_clients
  possible_search_implications
  timing_window, market_phase
  strategic_relevance_notes

  // Rendering (pre-computed)
  render_public_snippet, render_public_full, render_internal_brief
  render_seo_excerpt, render_social_excerpt, render_llm_summary

  // Governance
  reviewed_by, approved_at, editorial_flags[]
  suppression_reason, visibility_level

  // Linked arrays (separate tables)
  supporting_sources[]
  claims[]
  entities (companies, people, geographies, sectors, themes, signal_types, change_vectors)
  related_assets[]
  likely_roles_impacted[]
}
```

---

## 4. Database Schema Changes

### 4.1 Migration DDL

```sql
-- ════════════════════════════════════════════════════════════════
-- Dispatch v2 Migration
-- ════════════════════════════════════════════════════════════════

-- Enums
CREATE TYPE dispatch_status AS ENUM (
  'generating', 'draft', 'review', 'published', 'featured', 'archived', 'suppressed'
);

CREATE TYPE suppression_state AS ENUM (
  'active', 'suppressed', 'deferred', 'withdrawn'
);

CREATE TYPE visibility_level AS ENUM (
  'internal_only', 'draft_public', 'published', 'featured'
);

CREATE TYPE claim_type AS ENUM (
  'fact', 'interpretation', 'implication', 'forecast'
);

CREATE TYPE claim_support_level AS ENUM (
  'strong', 'moderate', 'weak', 'unsupported'
);

CREATE TYPE related_asset_type AS ENUM (
  'case_study', 'podcast', 'insight', 'pattern_page', 'sector_page', 'dispatch'
);

-- ── Core dispatch_v2 table ──────────────────────────────────────

CREATE TABLE dispatch_v2 (
  id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  slug                    VARCHAR(300) UNIQUE,
  tenant_id               UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'
                            REFERENCES tenants(id),
  status                  dispatch_status NOT NULL DEFAULT 'generating',

  -- Origin references
  signal_event_ids        UUID[] DEFAULT '{}',
  source_cluster_id       UUID,          -- signal_grabs id if derived from grab cluster
  legacy_dispatch_id      UUID,          -- signal_dispatches id if migrated
  pattern_id              UUID,          -- set when attached to a pattern
  generation_version      VARCHAR(50) NOT NULL DEFAULT 'v2.0',

  -- Scores (0.00 - 1.00)
  confidence_score        DECIMAL(4,3) DEFAULT 0,
  editorial_score         DECIMAL(4,3) DEFAULT 0,
  seo_score               DECIMAL(4,3) DEFAULT 0,
  shareability_score      DECIMAL(4,3) DEFAULT 0,
  provenance_score        DECIMAL(4,3) DEFAULT 0,
  evidence_density_score  DECIMAL(4,3) DEFAULT 0,
  suppression_state       suppression_state NOT NULL DEFAULT 'active',

  -- Core editorial fields
  title                   TEXT,
  thesis                  TEXT,
  summary                 TEXT,
  observation             TEXT,
  implication             TEXT,
  non_obvious_angle       TEXT,
  watch_next              TEXT,
  restraint_note          TEXT,

  -- Commercial translation
  why_it_matters_for_clients  TEXT,
  possible_search_implications TEXT,
  likely_roles_impacted   TEXT[],
  timing_window           VARCHAR(100),
  market_phase            VARCHAR(100),
  strategic_relevance_notes TEXT,

  -- Pre-rendered variants (generated, cached)
  render_public_snippet   JSONB,
  render_public_full      JSONB,
  render_internal_brief   JSONB,
  render_seo_excerpt      TEXT,
  render_social_excerpt   TEXT,
  render_llm_summary      TEXT,

  -- Governance
  reviewed_by             UUID REFERENCES users(id),
  approved_at             TIMESTAMPTZ,
  editorial_flags         TEXT[] DEFAULT '{}',
  suppression_reason      TEXT,
  visibility_level        visibility_level NOT NULL DEFAULT 'internal_only',

  -- Timestamps
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at            TIMESTAMPTZ
);

CREATE INDEX idx_dv2_tenant ON dispatch_v2(tenant_id);
CREATE INDEX idx_dv2_status ON dispatch_v2(status);
CREATE INDEX idx_dv2_slug ON dispatch_v2(slug);
CREATE INDEX idx_dv2_published ON dispatch_v2(published_at DESC) WHERE published_at IS NOT NULL;
CREATE INDEX idx_dv2_pattern ON dispatch_v2(pattern_id) WHERE pattern_id IS NOT NULL;
CREATE INDEX idx_dv2_confidence ON dispatch_v2(confidence_score DESC);
CREATE INDEX idx_dv2_suppression ON dispatch_v2(suppression_state);
CREATE INDEX idx_dv2_visibility ON dispatch_v2(visibility_level);

CREATE TRIGGER update_dispatch_v2_timestamp
  BEFORE UPDATE ON dispatch_v2
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── Supporting sources ──────────────────────────────────────────

CREATE TABLE dispatch_sources (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  dispatch_id           UUID NOT NULL REFERENCES dispatch_v2(id) ON DELETE CASCADE,
  source_document_id    UUID REFERENCES external_documents(id),
  source_name           TEXT NOT NULL,
  source_title          TEXT,
  source_url            TEXT,
  published_at          TIMESTAMPTZ,
  evidence_snippet      TEXT,
  source_quality_score  DECIMAL(4,3) DEFAULT 0,
  why_it_matters_note   TEXT,
  claim_support_tags    TEXT[] DEFAULT '{}',
  sort_order            INT DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ds_dispatch ON dispatch_sources(dispatch_id);
CREATE INDEX idx_ds_document ON dispatch_sources(source_document_id) WHERE source_document_id IS NOT NULL;

-- ── Claims with provenance ──────────────────────────────────────

CREATE TABLE dispatch_claims (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  dispatch_id           UUID NOT NULL REFERENCES dispatch_v2(id) ON DELETE CASCADE,
  claim_text            TEXT NOT NULL,
  claim_type            claim_type NOT NULL DEFAULT 'fact',
  support_level         claim_support_level NOT NULL DEFAULT 'unsupported',
  source_refs           UUID[] DEFAULT '{}',   -- dispatch_sources ids
  structured_data_refs  JSONB DEFAULT '[]',    -- [{entity_type, entity_id, field}]
  confidence            DECIMAL(4,3) DEFAULT 0,
  allowed_for_publication BOOLEAN NOT NULL DEFAULT false,
  sort_order            INT DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dc_dispatch ON dispatch_claims(dispatch_id);
CREATE INDEX idx_dc_type ON dispatch_claims(claim_type);
CREATE INDEX idx_dc_publication ON dispatch_claims(allowed_for_publication);

-- ── Entity associations ─────────────────────────────────────────

CREATE TABLE dispatch_entities (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  dispatch_id           UUID NOT NULL REFERENCES dispatch_v2(id) ON DELETE CASCADE,
  entity_type           VARCHAR(50) NOT NULL,  -- company, person, product, geography, sector, theme, signal_type, change_vector
  entity_id             UUID,                  -- FK to companies/people if applicable
  entity_value          TEXT NOT NULL,          -- display name / value
  relevance_score       DECIMAL(4,3) DEFAULT 0,
  is_primary            BOOLEAN DEFAULT false,
  metadata              JSONB DEFAULT '{}',
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_de_dispatch ON dispatch_entities(dispatch_id);
CREATE INDEX idx_de_type ON dispatch_entities(entity_type);
CREATE INDEX idx_de_entity ON dispatch_entities(entity_id) WHERE entity_id IS NOT NULL;
CREATE INDEX idx_de_value ON dispatch_entities(entity_value);

-- ── Related assets (case studies, podcasts, insights) ───────────

CREATE TABLE dispatch_related_assets (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  dispatch_id           UUID NOT NULL REFERENCES dispatch_v2(id) ON DELETE CASCADE,
  asset_type            related_asset_type NOT NULL,
  asset_id              UUID,                  -- internal id if stored locally
  title                 TEXT NOT NULL,
  slug_or_url           TEXT,
  relevance_reason      TEXT,
  relevance_scores      JSONB DEFAULT '{}',    -- {thematic, longitudinal, sector, geography, pattern_alignment, editorial}
  overall_relevance     DECIMAL(4,3) DEFAULT 0,
  sort_order            INT DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dra_dispatch ON dispatch_related_assets(dispatch_id);
CREATE INDEX idx_dra_type ON dispatch_related_assets(asset_type);
CREATE INDEX idx_dra_relevance ON dispatch_related_assets(overall_relevance DESC);

-- ── Patterns (storylines) ───────────────────────────────────────

CREATE TABLE patterns (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  slug                  VARCHAR(300) UNIQUE,
  tenant_id             UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001'
                          REFERENCES tenants(id),
  title                 TEXT NOT NULL,
  thesis                TEXT,
  summary               TEXT,
  status                VARCHAR(50) NOT NULL DEFAULT 'emerging',
    -- emerging | active | mature | archived
  convergence_score     DECIMAL(4,3) DEFAULT 0,
  dispatch_count        INT DEFAULT 0,
  first_signal_at       TIMESTAMPTZ,
  latest_signal_at      TIMESTAMPTZ,

  -- Clustering dimensions
  primary_themes        TEXT[] DEFAULT '{}',
  primary_geographies   TEXT[] DEFAULT '{}',
  primary_sectors       TEXT[] DEFAULT '{}',
  primary_change_vectors TEXT[] DEFAULT '{}',
  primary_signal_types  TEXT[] DEFAULT '{}',

  -- Rendered
  render_pattern_page   JSONB,
  render_overview       TEXT,

  -- Governance
  visibility_level      visibility_level NOT NULL DEFAULT 'internal_only',
  reviewed_by           UUID REFERENCES users(id),
  approved_at           TIMESTAMPTZ,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at          TIMESTAMPTZ
);

CREATE INDEX idx_patterns_tenant ON patterns(tenant_id);
CREATE INDEX idx_patterns_status ON patterns(status);
CREATE INDEX idx_patterns_slug ON patterns(slug);
CREATE INDEX idx_patterns_convergence ON patterns(convergence_score DESC);

CREATE TRIGGER update_patterns_timestamp
  BEFORE UPDATE ON patterns
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── Pattern-dispatch join table ─────────────────────────────────

CREATE TABLE pattern_dispatches (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  pattern_id            UUID NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
  dispatch_id           UUID NOT NULL REFERENCES dispatch_v2(id) ON DELETE CASCADE,
  contribution_score    DECIMAL(4,3) DEFAULT 0,
  added_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(pattern_id, dispatch_id)
);

CREATE INDEX idx_pd_pattern ON pattern_dispatches(pattern_id);
CREATE INDEX idx_pd_dispatch ON pattern_dispatches(dispatch_id);

-- ── Pattern related assets ──────────────────────────────────────

CREATE TABLE pattern_related_assets (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  pattern_id            UUID NOT NULL REFERENCES patterns(id) ON DELETE CASCADE,
  asset_type            related_asset_type NOT NULL,
  asset_id              UUID,
  title                 TEXT NOT NULL,
  slug_or_url           TEXT,
  relevance_reason      TEXT,
  overall_relevance     DECIMAL(4,3) DEFAULT 0,
  sort_order            INT DEFAULT 0,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pra_pattern ON pattern_related_assets(pattern_id);
```

### 4.2 Legacy Compatibility

The existing `signal_dispatches` and `signal_grabs` tables remain untouched. New dispatches flow into `dispatch_v2`. A migration script (section 12) backfills existing dispatches into v2 format where evidence permits.

---

## 5. TypeScript Interfaces

```typescript
// ═══════════════════════════════════════════════════════════════
// Dispatch v2 — Canonical Interfaces
// ═══════════════════════════════════════════════════════════════

type DispatchStatus = 'generating' | 'draft' | 'review' | 'published' | 'featured' | 'archived' | 'suppressed';
type SuppressionState = 'active' | 'suppressed' | 'deferred' | 'withdrawn';
type VisibilityLevel = 'internal_only' | 'draft_public' | 'published' | 'featured';
type ClaimType = 'fact' | 'interpretation' | 'implication' | 'forecast';
type ClaimSupportLevel = 'strong' | 'moderate' | 'weak' | 'unsupported';
type RelatedAssetType = 'case_study' | 'podcast' | 'insight' | 'pattern_page' | 'sector_page' | 'dispatch';
type PatternStatus = 'emerging' | 'active' | 'mature' | 'archived';

// ── Core Dispatch ───────────────────────────────────────────────

interface DispatchV2 {
  id: string;
  slug: string | null;
  tenant_id: string;
  status: DispatchStatus;

  // Origin
  signal_event_ids: string[];
  source_cluster_id: string | null;
  legacy_dispatch_id: string | null;
  pattern_id: string | null;
  generation_version: string;

  // Scores
  confidence_score: number;
  editorial_score: number;
  seo_score: number;
  shareability_score: number;
  provenance_score: number;
  evidence_density_score: number;
  suppression_state: SuppressionState;

  // Editorial
  title: string | null;
  thesis: string | null;
  summary: string | null;
  observation: string | null;
  implication: string | null;
  non_obvious_angle: string | null;
  watch_next: string | null;
  restraint_note: string | null;

  // Commercial
  why_it_matters_for_clients: string | null;
  possible_search_implications: string | null;
  likely_roles_impacted: string[];
  timing_window: string | null;
  market_phase: string | null;
  strategic_relevance_notes: string | null;

  // Rendered variants
  render_public_snippet: PublicSnippetRender | null;
  render_public_full: PublicFullRender | null;
  render_internal_brief: InternalBriefRender | null;
  render_seo_excerpt: string | null;
  render_social_excerpt: string | null;
  render_llm_summary: string | null;

  // Governance
  reviewed_by: string | null;
  approved_at: string | null;
  editorial_flags: string[];
  suppression_reason: string | null;
  visibility_level: VisibilityLevel;

  // Timestamps
  created_at: string;
  updated_at: string;
  published_at: string | null;

  // Joined/computed (not stored, resolved at query time)
  supporting_sources?: DispatchSourceEvidence[];
  claims?: DispatchClaim[];
  entities?: DispatchEntitySet;
  related_assets?: RelatedAssetLink[];
}

// ── Source Evidence ─────────────────────────────────────────────

interface DispatchSourceEvidence {
  id: string;
  dispatch_id: string;
  source_document_id: string | null;
  source_name: string;
  source_title: string | null;
  source_url: string | null;
  published_at: string | null;
  evidence_snippet: string | null;
  source_quality_score: number;
  why_it_matters_note: string | null;
  claim_support_tags: string[];
  sort_order: number;
}

// ── Claims ──────────────────────────────────────────────────────

interface DispatchClaim {
  id: string;
  dispatch_id: string;
  claim_text: string;
  claim_type: ClaimType;
  support_level: ClaimSupportLevel;
  source_refs: string[];           // dispatch_sources.id[]
  structured_data_refs: StructuredDataRef[];
  confidence: number;
  allowed_for_publication: boolean;
  sort_order: number;
}

interface StructuredDataRef {
  entity_type: string;
  entity_id: string;
  field: string;
}

// ── Entities ────────────────────────────────────────────────────

interface DispatchEntity {
  id: string;
  dispatch_id: string;
  entity_type: 'company' | 'person' | 'product' | 'geography' | 'sector' | 'theme' | 'signal_type' | 'change_vector';
  entity_id: string | null;
  entity_value: string;
  relevance_score: number;
  is_primary: boolean;
  metadata: Record<string, unknown>;
}

interface DispatchEntitySet {
  companies: DispatchEntity[];
  people: DispatchEntity[];
  products: DispatchEntity[];
  geographies: DispatchEntity[];
  sectors: DispatchEntity[];
  themes: DispatchEntity[];
  signal_types: DispatchEntity[];
  change_vectors: DispatchEntity[];
}

// ── Commercial Layer ────────────────────────────────────────────

interface DispatchCommercialLayer {
  why_it_matters_for_clients: string | null;
  possible_search_implications: string | null;
  likely_roles_impacted: string[];
  timing_window: string | null;
  market_phase: string | null;
  strategic_relevance_notes: string | null;
}

// ── Related Assets ──────────────────────────────────────────────

interface RelatedAssetLink {
  id: string;
  dispatch_id: string;
  asset_type: RelatedAssetType;
  asset_id: string | null;
  title: string;
  slug_or_url: string | null;
  relevance_reason: string | null;
  relevance_scores: RelevanceScores;
  overall_relevance: number;
  sort_order: number;
}

interface RelevanceScores {
  thematic?: number;
  longitudinal?: number;
  sector?: number;
  geography?: number;
  pattern_alignment?: number;
  editorial?: number;
}

// ── Pattern / Storyline ─────────────────────────────────────────

interface Pattern {
  id: string;
  slug: string | null;
  tenant_id: string;
  title: string;
  thesis: string | null;
  summary: string | null;
  status: PatternStatus;
  convergence_score: number;
  dispatch_count: number;
  first_signal_at: string | null;
  latest_signal_at: string | null;

  primary_themes: string[];
  primary_geographies: string[];
  primary_sectors: string[];
  primary_change_vectors: string[];
  primary_signal_types: string[];

  render_pattern_page: PatternPageRender | null;
  render_overview: string | null;

  visibility_level: VisibilityLevel;
  reviewed_by: string | null;
  approved_at: string | null;

  created_at: string;
  updated_at: string;
  published_at: string | null;

  // Joined
  dispatches?: DispatchV2[];
  related_assets?: RelatedAssetLink[];
}

// ── Render Shapes ───────────────────────────────────────────────

interface PublicSnippetRender {
  title: string;
  observation: string;
  source_count: number;
  source_names: string[];
  tags: string[];
  linked_case_study: { title: string; url: string } | null;
  linked_podcast: { title: string; url: string } | null;
  cta_url: string;
}

interface PublicFullRender {
  title: string;
  summary: string;
  evidence_section: {
    sources: Array<{ name: string; title: string; url: string; snippet: string }>;
  };
  observation: string;
  implication: string;
  non_obvious_angle: string;
  watch_next: string;
  restraint_note: string | null;
  related_case_studies: Array<{ title: string; url: string; reason: string }>;
  related_podcasts: Array<{ title: string; url: string; reason: string }>;
  related_patterns: Array<{ title: string; url: string }>;
  tags: string[];
  schema_org: Record<string, unknown>;
}

interface InternalBriefRender {
  title: string;
  thesis: string;
  observation: string;
  implication: string;
  commercial_read: string;
  roles_impacted: string[];
  timing: string;
  market_phase: string;
  proximity_notes: string | null;
  watch_next: string;
}

interface PatternPageRender {
  title: string;
  thesis: string;
  overview: string;
  timeline: Array<{ date: string; dispatch_title: string; dispatch_slug: string }>;
  contributing_signals_count: number;
  geographies: string[];
  sectors: string[];
  related_case_studies: Array<{ title: string; url: string; reason: string }>;
  related_podcasts: Array<{ title: string; url: string; reason: string }>;
  tags: string[];
}
```

---

## 6. Generation Pipeline Evolution

### 6.1 Current Flow

```
signal_events (confidence ≥ 0.65, age ≤ 72h)
  → proximity mapping (5 queries across people/interactions/placements)
  → approach angle selection (lookup table by signal_type)
  → Claude call → blog post JSON (linkedin_post, email_brief, data_points, trend_summary)
  → distribution plan
  → INSERT signal_dispatches
```

### 6.2 Evolved Flow

```
signal_events (same filtering)
  │
  ├── [STEP 1] Source Assembly
  │   Gather: signal evidence_snippets, linked external_documents, related signals (30 days),
  │   converging themes data, entity context from companies/people tables
  │   Output: SourceBundle { sources[], entity_context, historical_signals[] }
  │
  ├── [STEP 2] Structured Generation (Claude)
  │   System prompt: "You are an intelligence analyst. Output ONLY structured JSON."
  │   Input: SourceBundle as context
  │   Output: DispatchDraft (see 6.3)
  │   Key constraint: Every field must reference source IDs from the input bundle.
  │                   Return null for any field you cannot support with evidence.
  │
  ├── [STEP 3] Provenance Validation
  │   - Verify every source_ref in claims exists in provided sources
  │   - Strip claims with invalid refs
  │   - Compute support_level per claim
  │   - Compute evidence_density_score, provenance_score, confidence_score
  │   - Determine suppression_state
  │
  ├── [STEP 4] Entity Extraction & Linking
  │   - Extract companies, people, geographies, sectors, themes, change_vectors
  │   - Resolve entity_ids against companies/people tables
  │   - Compute relevance_scores
  │
  ├── [STEP 5] Asset Linking
  │   - Match to case studies via embedding similarity + entity overlap
  │   - Match to podcasts via embedding similarity + theme overlap
  │   - Score and rank related assets
  │
  ├── [STEP 6] Scoring
  │   - editorial_score (thesis quality, non-obvious angle present, compression)
  │   - seo_score (slug quality, entity richness, structured metadata completeness)
  │   - shareability_score (public-readiness: no internal refs, evidence density, voice)
  │
  ├── [STEP 7] Rendering
  │   - Generate render_public_snippet, render_public_full, render_internal_brief
  │   - Generate render_seo_excerpt, render_social_excerpt, render_llm_summary
  │   - These are deterministic transforms from structured fields, NOT additional LLM calls
  │
  └── [STEP 8] Persist
      - INSERT dispatch_v2 + dispatch_sources + dispatch_claims + dispatch_entities + dispatch_related_assets
      - If legacy proximity mapping still needed: also populate signal_dispatches for backwards compat
```

### 6.3 Claude Generation Prompt (Structured)

The generation prompt replaces the current blog-generation call. Input:

```
System: You are a market intelligence analyst for {tenant_name}, a firm with 25 years
in growth and innovation ventures. Produce structured JSON only. Every interpretive
statement must reference source_ids from the provided sources. If evidence is
insufficient for any field, return null. Prefer silence over speculation.

User: Given these sources and signal context, produce a structured dispatch:

SIGNAL: {signal_type} at {company_name}
CONFIDENCE: {confidence_score}
SOURCES: [
  { id: "src_1", name: "...", title: "...", url: "...", snippet: "..." },
  { id: "src_2", ... }
]
RELATED SIGNALS (last 30 days): [...]
ENTITY CONTEXT: { company_sector, company_geography, is_client, ... }

Output JSON with these fields:
{
  "title": "short, specific, no hype (max 12 words)",
  "thesis": "one sentence: the core claim this dispatch makes",
  "summary": "2-3 sentences: what happened and why it matters",
  "observation": "what the pattern looks like from the evidence",
  "implication": "what this might mean commercially (only if ≥2 sources support it, else null)",
  "non_obvious_angle": "the insight not visible from headlines alone (required)",
  "watch_next": "what would confirm or negate this thesis",
  "restraint_note": "what we are deliberately not claiming",
  "claims": [
    {
      "claim_text": "...",
      "claim_type": "fact|interpretation|implication|forecast",
      "source_refs": ["src_1", "src_2"],
      "confidence": 0.0-1.0
    }
  ],
  "change_vectors": ["directional shifts, not events"],
  "themes": ["..."],
  "why_it_matters_for_clients": "commercial read (null if speculative)",
  "possible_search_implications": "talent market read (null if speculative)",
  "likely_roles_impacted": ["CTO", "VP Engineering"],
  "timing_window": "near-term | medium-term | long-term | null",
  "market_phase": "early signal | building momentum | inflection | established | null"
}
```

### 6.4 Post-Generation Validation (Pseudocode)

```javascript
function validateDispatchDraft(draft, sourceBundle) {
  const validSourceIds = new Set(sourceBundle.sources.map(s => s.id));

  // Validate claim source refs
  for (const claim of draft.claims) {
    claim.source_refs = claim.source_refs.filter(ref => validSourceIds.has(ref));

    if (claim.source_refs.length === 0) {
      claim.support_level = 'unsupported';
      claim.allowed_for_publication = false;
    } else if (claim.source_refs.length >= 2) {
      claim.support_level = 'strong';
      claim.allowed_for_publication = claim.claim_type !== 'forecast';
    } else {
      claim.support_level = claim.claim_type === 'fact' ? 'moderate' : 'weak';
      claim.allowed_for_publication = claim.claim_type === 'fact';
    }
  }

  // Compute scores
  const totalClaims = draft.claims.length || 1;
  const sourcedClaims = draft.claims.filter(c => c.source_refs.length > 0).length;
  const multiSourcedClaims = draft.claims.filter(c => c.source_refs.length >= 2).length;

  const evidence_density = (
    Math.min(1, sourceBundle.sources.length / 5) * 0.3 +
    mean(sourceBundle.sources.map(s => s.quality_score || 0.5)) * 0.3 +
    (multiSourcedClaims / totalClaims) * 0.4
  );

  const provenance = (
    (sourcedClaims / totalClaims) * 0.5 +
    (sourceBundle.sources.filter(s => s.url).length / Math.max(sourceBundle.sources.length, 1)) * 0.3 +
    0.2 // entity grounding placeholder
  );

  // Suppression check
  let suppression_state = 'active';
  if (evidence_density < 0.3) suppression_state = 'suppressed';
  if (draft.claims.every(c => c.support_level === 'unsupported')) suppression_state = 'suppressed';

  return {
    ...draft,
    evidence_density_score: round3(evidence_density),
    provenance_score: round3(provenance),
    confidence_score: round3(mean(draft.claims.map(c => c.confidence))),
    suppression_state,
  };
}
```

---

## 7. Clustering / Pattern Architecture

### 7.1 Pattern Detection

Patterns emerge from dispatch similarity. Detection runs as a scheduled job (daily or after each dispatch batch).

```
[Scheduled: pattern_detection]

1. Fetch all dispatch_v2 where status IN ('draft','review','published') AND created_at > NOW() - 90 days

2. For each dispatch, extract clustering dimensions:
   - themes[]
   - change_vectors[]
   - geographies[]
   - sectors[]
   - signal_types[]
   - entity overlap (company_ids, people_ids)

3. Compute pairwise similarity:
   similarity(d1, d2) = (
     jaccard(d1.themes, d2.themes) * 0.25 +
     jaccard(d1.change_vectors, d2.change_vectors) * 0.25 +
     jaccard(d1.geographies, d2.geographies) * 0.15 +
     jaccard(d1.sectors, d2.sectors) * 0.15 +
     entity_overlap(d1, d2) * 0.10 +
     temporal_proximity(d1, d2) * 0.10
   )

4. Cluster dispatches where similarity > 0.4 (agglomerative, single-link)

5. For each cluster with ≥ 3 dispatches:
   - Compute convergence_score = mean(pairwise similarities within cluster)
   - If convergence_score ≥ 0.5: create or update pattern
   - If convergence_score < 0.5: skip (evidence not converging)

6. For existing patterns: check if new dispatches should merge in (similarity to pattern centroid > 0.4)
```

### 7.2 Pattern Lifecycle

```
emerging  →  active  →  mature  →  archived
(3-4 dispatches)  (5+ dispatches, convergence ≥ 0.6)  (10+ or stable for 30 days)
```

### 7.3 Pattern Page Generation

Only when `status = 'active'` and `visibility_level >= 'draft_public'`:

```javascript
async function renderPatternPage(pattern, dispatches) {
  return {
    title: pattern.title,
    thesis: pattern.thesis,
    overview: pattern.summary,
    timeline: dispatches
      .sort((a, b) => new Date(a.created_at) - new Date(b.created_at))
      .map(d => ({
        date: d.published_at || d.created_at,
        dispatch_title: d.title,
        dispatch_slug: d.slug,
      })),
    contributing_signals_count: dispatches.reduce((n, d) => n + d.signal_event_ids.length, 0),
    geographies: pattern.primary_geographies,
    sectors: pattern.primary_sectors,
    related_case_studies: await getPatternAssets(pattern.id, 'case_study'),
    related_podcasts: await getPatternAssets(pattern.id, 'podcast'),
    tags: [...pattern.primary_themes, ...pattern.primary_change_vectors],
  };
}
```

---

## 8. Related Asset Linking System

### 8.1 Asset Sources

| Asset Type | Source Table | Identifier |
|---|---|---|
| case_study | To be added (or external CMS reference) | slug/url |
| podcast | external_documents WHERE source_type = 'podcast' | id |
| insight | dispatch_v2 WHERE visibility_level >= 'published' | id |
| pattern_page | patterns WHERE visibility_level >= 'draft_public' | id |

### 8.2 Matching Pipeline

For each dispatch, after entity extraction:

```javascript
async function linkRelatedAssets(dispatch, entities) {
  const results = [];

  // 1. Podcast matching via Qdrant embedding similarity
  const dispatchEmbedding = await embedText(dispatch.thesis + ' ' + dispatch.observation);
  const podcastHits = await qdrant.search('external_documents', dispatchEmbedding, {
    filter: { source_type: 'podcast' },
    limit: 10,
  });

  for (const hit of podcastHits) {
    const scores = {
      thematic: computeThematicScore(dispatch, hit),
      sector: computeSectorOverlap(entities.sectors, hit.payload),
      geography: computeGeoOverlap(entities.geographies, hit.payload),
      editorial: hit.score, // embedding similarity
    };
    const overall = (
      scores.thematic * 0.35 +
      scores.sector * 0.20 +
      scores.geography * 0.15 +
      scores.editorial * 0.30
    );

    if (overall >= 0.4) {
      results.push({
        asset_type: 'podcast',
        asset_id: hit.id,
        title: hit.payload.title,
        slug_or_url: hit.payload.source_url,
        relevance_reason: generateRelevanceReason(dispatch, hit, scores),
        relevance_scores: scores,
        overall_relevance: overall,
      });
    }
  }

  // 2. Case study matching (same pattern, against case study embeddings)
  // When case_studies table exists, query Qdrant 'case_studies' collection
  // For now, match against external_documents with source_type in ('blog','newsletter')
  // where source_name matches known MitchelLake case study sources

  // 3. Related dispatches (other published dispatches with entity/theme overlap)
  const relatedDispatches = await findRelatedDispatches(dispatch.id, entities);
  for (const rd of relatedDispatches) {
    results.push({
      asset_type: 'dispatch',
      asset_id: rd.id,
      title: rd.title,
      slug_or_url: `/observations/${rd.slug}`,
      relevance_reason: rd.overlap_reason,
      relevance_scores: rd.scores,
      overall_relevance: rd.overall,
    });
  }

  return results.sort((a, b) => b.overall_relevance - a.overall_relevance);
}
```

### 8.3 Relevance Reason Generation

Not an LLM call — a template function:

```javascript
function generateRelevanceReason(dispatch, asset, scores) {
  const parts = [];
  if (scores.thematic > 0.6) {
    parts.push(`explores the same shift in ${dispatch.entities?.change_vectors?.[0]?.entity_value || 'this market'}`);
  }
  if (scores.sector > 0.5) {
    parts.push(`covers the ${dispatch.entities?.sectors?.[0]?.entity_value || 'same'} sector`);
  }
  if (scores.geography > 0.5) {
    parts.push(`focuses on the ${dispatch.entities?.geographies?.[0]?.entity_value || 'same'} market`);
  }
  if (parts.length === 0) {
    parts.push('thematically aligned with this observation');
  }
  return `Related because this ${asset.asset_type === 'podcast' ? 'episode' : 'piece'} ${parts.join(' and ')}.`;
}
```

---

## 9. Rendering Architecture

Rendering is a pure function layer: structured dispatch data in → HTML/JSON out. No additional LLM calls.

### 9.1 Public Snippet

```javascript
function renderPublicSnippet(dispatch) {
  if (dispatch.suppression_state !== 'active') return null;
  if (dispatch.visibility_level === 'internal_only') return null;

  const casStudy = dispatch.related_assets?.find(a => a.asset_type === 'case_study');
  const podcast = dispatch.related_assets?.find(a => a.asset_type === 'podcast');

  return {
    title: dispatch.title,
    observation: dispatch.observation || dispatch.summary,
    source_count: dispatch.supporting_sources?.length || 0,
    source_names: (dispatch.supporting_sources || []).map(s => s.source_name),
    tags: [
      ...(dispatch.entities?.themes || []).map(e => e.entity_value),
      ...(dispatch.entities?.geographies || []).map(e => e.entity_value),
    ].slice(0, 5),
    linked_case_study: casStudy ? { title: casStudy.title, url: casStudy.slug_or_url } : null,
    linked_podcast: podcast ? { title: podcast.title, url: podcast.slug_or_url } : null,
    cta_url: `/observations/${dispatch.slug}`,
  };
}
```

### 9.2 Public Full Page

```javascript
function renderPublicFull(dispatch) {
  if (dispatch.visibility_level === 'internal_only') return null;

  const publishableClaims = (dispatch.claims || []).filter(c => c.allowed_for_publication);

  return {
    title: dispatch.title,
    summary: dispatch.summary,
    evidence_section: {
      sources: (dispatch.supporting_sources || []).map(s => ({
        name: s.source_name,
        title: s.source_title,
        url: s.source_url,
        snippet: s.evidence_snippet,
      })),
    },
    observation: dispatch.observation,
    implication: dispatch.implication,
    non_obvious_angle: dispatch.non_obvious_angle,
    watch_next: dispatch.watch_next,
    restraint_note: dispatch.restraint_note,
    related_case_studies: (dispatch.related_assets || [])
      .filter(a => a.asset_type === 'case_study')
      .map(a => ({ title: a.title, url: a.slug_or_url, reason: a.relevance_reason })),
    related_podcasts: (dispatch.related_assets || [])
      .filter(a => a.asset_type === 'podcast')
      .map(a => ({ title: a.title, url: a.slug_or_url, reason: a.relevance_reason })),
    related_patterns: (dispatch.related_assets || [])
      .filter(a => a.asset_type === 'pattern_page')
      .map(a => ({ title: a.title, url: a.slug_or_url })),
    tags: [
      ...(dispatch.entities?.themes || []).map(e => e.entity_value),
      ...(dispatch.entities?.sectors || []).map(e => e.entity_value),
      ...(dispatch.entities?.geographies || []).map(e => e.entity_value),
    ],
    schema_org: generateSchemaOrg(dispatch),
  };
}
```

### 9.3 Internal Brief

```javascript
function renderInternalBrief(dispatch) {
  return {
    title: dispatch.title,
    thesis: dispatch.thesis,
    observation: dispatch.observation,
    implication: dispatch.implication,
    commercial_read: dispatch.why_it_matters_for_clients,
    roles_impacted: dispatch.likely_roles_impacted,
    timing: dispatch.timing_window,
    market_phase: dispatch.market_phase,
    proximity_notes: dispatch.strategic_relevance_notes,
    watch_next: dispatch.watch_next,
  };
}
```

### 9.4 SEO Excerpt

```javascript
function renderSeoExcerpt(dispatch) {
  // max 160 chars for meta description
  const base = dispatch.thesis || dispatch.summary || dispatch.observation || '';
  return base.length > 155 ? base.slice(0, 155) + '...' : base;
}
```

### 9.5 Social Excerpt

```javascript
function renderSocialExcerpt(dispatch) {
  // LinkedIn/Twitter card: title + one-liner + link
  const angle = dispatch.non_obvious_angle || dispatch.observation || '';
  return `${dispatch.title}\n\n${angle}`.slice(0, 280);
}
```

### 9.6 LLM Summary

```javascript
function renderLlmSummary(dispatch) {
  // Structured text for LLM retrieval (RAG)
  const parts = [
    `Title: ${dispatch.title}`,
    `Thesis: ${dispatch.thesis}`,
    `Observation: ${dispatch.observation}`,
    dispatch.implication ? `Implication: ${dispatch.implication}` : null,
    dispatch.non_obvious_angle ? `Non-obvious angle: ${dispatch.non_obvious_angle}` : null,
    dispatch.watch_next ? `Watch next: ${dispatch.watch_next}` : null,
    dispatch.entities?.themes?.length ? `Themes: ${dispatch.entities.themes.map(t => t.entity_value).join(', ')}` : null,
    dispatch.entities?.geographies?.length ? `Geographies: ${dispatch.entities.geographies.map(g => g.entity_value).join(', ')}` : null,
    dispatch.entities?.sectors?.length ? `Sectors: ${dispatch.entities.sectors.map(s => s.entity_value).join(', ')}` : null,
  ].filter(Boolean);
  return parts.join('\n');
}
```

---

## 10. SEO / Metadata Model

### 10.1 Schema.org Generation

```javascript
function generateSchemaOrg(dispatch) {
  return {
    '@context': 'https://schema.org',
    '@type': 'Article',
    headline: dispatch.title,
    description: dispatch.render_seo_excerpt || dispatch.summary,
    datePublished: dispatch.published_at,
    dateModified: dispatch.updated_at,
    author: {
      '@type': 'Organization',
      name: 'MitchelLake',
      url: 'https://mitchellake.com',
    },
    publisher: {
      '@type': 'Organization',
      name: 'MitchelLake',
    },
    about: (dispatch.entities?.themes || []).map(t => ({
      '@type': 'Thing',
      name: t.entity_value,
    })),
    mentions: (dispatch.entities?.companies || []).map(c => ({
      '@type': 'Organization',
      name: c.entity_value,
    })),
    keywords: [
      ...(dispatch.entities?.themes || []).map(t => t.entity_value),
      ...(dispatch.entities?.sectors || []).map(s => s.entity_value),
      ...(dispatch.entities?.change_vectors || []).map(v => v.entity_value),
    ].join(', '),
  };
}
```

### 10.2 Slug Generation

```javascript
function generateSlug(dispatch) {
  const base = dispatch.title
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .slice(0, 80);
  const datePart = new Date(dispatch.created_at).toISOString().slice(0, 7); // YYYY-MM
  return `${datePart}/${base}`;
}
// Example: "2026-03/southeast-asia-infrastructure-buildout-accelerates"
```

### 10.3 Page Metadata

```javascript
function generatePageMeta(dispatch) {
  return {
    title: `${dispatch.title} | MitchelLake Intelligence`,
    description: dispatch.render_seo_excerpt,
    canonical: `https://mitchellake.com/observations/${dispatch.slug}`,
    og: {
      title: dispatch.title,
      description: dispatch.render_seo_excerpt,
      type: 'article',
      url: `https://mitchellake.com/observations/${dispatch.slug}`,
    },
    twitter: {
      card: 'summary',
      title: dispatch.title,
      description: dispatch.render_seo_excerpt,
    },
    structured_data: generateSchemaOrg(dispatch),
  };
}
```

---

## 11. API Design

### 11.1 New Endpoints

Add to [server.js](server.js) alongside existing dispatch routes:

```
# Dispatch v2
GET    /api/v2/dispatches                    # List with filters (status, visibility, theme, geo, pattern)
GET    /api/v2/dispatches/:slug              # Full dispatch by slug (includes sources, claims, entities, assets)
POST   /api/v2/dispatches/generate           # Trigger v2 generation pipeline
PATCH  /api/v2/dispatches/:id                # Update status, visibility, editorial fields
POST   /api/v2/dispatches/:id/review         # Mark reviewed, compute shareability
POST   /api/v2/dispatches/:id/publish        # Promote to published visibility

# Patterns
GET    /api/v2/patterns                      # List patterns (status, theme filters)
GET    /api/v2/patterns/:slug                # Full pattern with dispatches and assets
POST   /api/v2/patterns/detect               # Trigger pattern detection job
PATCH  /api/v2/patterns/:id                  # Update status, thesis, visibility
POST   /api/v2/patterns/:id/publish          # Promote to published

# Public (unauthenticated, for public website)
GET    /api/public/observations              # Published dispatch snippets (paginated)
GET    /api/public/observations/:slug        # Full public page data
GET    /api/public/patterns                  # Published pattern list
GET    /api/public/patterns/:slug            # Full pattern page data
GET    /api/public/feed.json                 # JSON feed of recent observations
GET    /api/public/sitemap.json              # Slugs for sitemap generation

# Related assets
GET    /api/v2/dispatches/:id/related        # Related assets for a dispatch
GET    /api/v2/patterns/:id/related          # Related assets for a pattern
```

### 11.2 Example: GET /api/v2/dispatches

```javascript
app.get('/api/v2/dispatches', authenticateToken, async (req, res) => {
  const { status, visibility, theme, geography, pattern_id, page = 1, per_page = 20 } = req.query;
  const offset = (page - 1) * per_page;
  let idx = 1;
  const params = [req.tenant_id];
  let where = `WHERE d.tenant_id = $${idx}`;

  if (status) { idx++; where += ` AND d.status = $${idx}`; params.push(status); }
  if (visibility) { idx++; where += ` AND d.visibility_level = $${idx}`; params.push(visibility); }
  if (pattern_id) { idx++; where += ` AND d.pattern_id = $${idx}`; params.push(pattern_id); }
  if (theme) {
    idx++;
    where += ` AND EXISTS (SELECT 1 FROM dispatch_entities de WHERE de.dispatch_id = d.id AND de.entity_type = 'theme' AND de.entity_value ILIKE $${idx})`;
    params.push(`%${theme}%`);
  }
  if (geography) {
    idx++;
    where += ` AND EXISTS (SELECT 1 FROM dispatch_entities de WHERE de.dispatch_id = d.id AND de.entity_type = 'geography' AND de.entity_value ILIKE $${idx})`;
    params.push(`%${geography}%`);
  }

  idx++;
  params.push(per_page);
  idx++;
  params.push(offset);

  const { rows } = await pool.query(`
    SELECT d.*,
      (SELECT json_agg(de.*) FROM dispatch_entities de WHERE de.dispatch_id = d.id) AS entities,
      (SELECT COUNT(*) FROM dispatch_sources ds WHERE ds.dispatch_id = d.id) AS source_count
    FROM dispatch_v2 d
    ${where}
    ORDER BY d.created_at DESC
    LIMIT $${idx - 1} OFFSET $${idx}
  `, params);

  const { rows: [{ count }] } = await pool.query(
    `SELECT COUNT(*) FROM dispatch_v2 d ${where}`, params.slice(0, -2)
  );

  res.json({ dispatches: rows, total: parseInt(count), page: parseInt(page), per_page: parseInt(per_page) });
});
```

### 11.3 Example: GET /api/public/observations/:slug

```javascript
app.get('/api/public/observations/:slug', async (req, res) => {
  const { rows: [dispatch] } = await pool.query(`
    SELECT d.* FROM dispatch_v2 d
    WHERE d.slug = $1
      AND d.visibility_level IN ('published', 'featured')
      AND d.suppression_state = 'active'
  `, [req.params.slug]);

  if (!dispatch) return res.status(404).json({ error: 'Not found' });

  // Fetch related data
  const [sources, claims, entities, assets] = await Promise.all([
    pool.query('SELECT * FROM dispatch_sources WHERE dispatch_id = $1 ORDER BY sort_order', [dispatch.id]),
    pool.query('SELECT * FROM dispatch_claims WHERE dispatch_id = $1 AND allowed_for_publication = true ORDER BY sort_order', [dispatch.id]),
    pool.query('SELECT * FROM dispatch_entities WHERE dispatch_id = $1', [dispatch.id]),
    pool.query('SELECT * FROM dispatch_related_assets WHERE dispatch_id = $1 ORDER BY overall_relevance DESC', [dispatch.id]),
  ]);

  const fullRender = dispatch.render_public_full || renderPublicFull({
    ...dispatch,
    supporting_sources: sources.rows,
    claims: claims.rows,
    entities: groupEntities(entities.rows),
    related_assets: assets.rows,
  });

  res.json({
    dispatch: fullRender,
    meta: generatePageMeta(dispatch),
  });
});
```

---

## 12. Migration Plan

### Phase 0: Schema (Week 1)

1. Run the migration DDL from section 4 against production
2. No existing tables are modified — purely additive
3. Add `legacy_dispatch_id` column to `dispatch_v2` for backfill tracking

### Phase 1: Dual-Write (Week 2-3)

1. Modify [generate_dispatches.js](scripts/generate_dispatches.js) to produce BOTH:
   - Existing `signal_dispatches` row (unchanged, for current UI)
   - New `dispatch_v2` row with structured fields
2. The v2 generation is additive — uses the same signal_events query but runs the structured prompt (section 6.3) instead of the blog prompt
3. Current dispatches UI continues to work against `signal_dispatches`

### Phase 2: Backfill (Week 3-4)

```javascript
// scripts/migrate_dispatches_to_v2.js
async function backfillDispatch(legacyDispatch) {
  // Parse blog_body JSON to extract what we can
  const blogData = JSON.parse(legacyDispatch.blog_body || '{}');
  const signal = await getSignalEvent(legacyDispatch.signal_event_id);

  // Create v2 dispatch with available fields
  const v2 = {
    tenant_id: legacyDispatch.tenant_id || MITCHELLAKE_TENANT,
    legacy_dispatch_id: legacyDispatch.id,
    signal_event_ids: [legacyDispatch.signal_event_id].filter(Boolean),
    status: mapStatus(legacyDispatch.status), // draft→draft, reviewed→review, sent→published
    title: legacyDispatch.blog_title,
    summary: blogData.trend_summary || null,
    observation: legacyDispatch.opportunity_angle,

    // Legacy dispatches have weak provenance — mark accordingly
    confidence_score: signal?.confidence_score || 0.5,
    evidence_density_score: 0.2,  // weak by default for legacy
    provenance_score: 0.3,
    suppression_state: 'active',
    visibility_level: 'internal_only', // legacy dispatches stay internal
    generation_version: 'v1_migrated',
  };

  // Insert and create entity associations from signal data
  const id = await insertDispatchV2(v2);
  if (signal) {
    await insertDispatchEntity(id, 'company', legacyDispatch.company_id, legacyDispatch.company_name);
    await insertDispatchEntity(id, 'signal_type', null, legacyDispatch.signal_type);
  }
}
```

### Phase 3: V2 UI (Week 4-6)

1. Build new dispatches view reading from `dispatch_v2`
2. Add pattern detection job to [scheduler.js](scripts/scheduler.js)
3. Add public API endpoints (unauthenticated)
4. Build public observation pages on mitchellake.com consuming public API

### Phase 4: Deprecate Legacy (Week 8+)

1. Stop writing to `signal_dispatches` for new dispatches
2. Route all UI to `dispatch_v2`
3. Keep `signal_dispatches` table for reference but mark as deprecated
4. `signal_grabs` continues as-is; new grabs can optionally create lightweight `dispatch_v2` entries

---

## 13. MVP Implementation Plan

**MVP = the smallest slice that proves the model works end-to-end.**

### MVP Scope

1. **Schema migration** — deploy all new tables (section 4)
2. **Structured generation for one signal type** — pick `capital_raising` (most common, best evidence)
   - New prompt (section 6.3) replacing blog prompt for this signal type only
   - Post-generation validation (section 6.4)
   - Persist to `dispatch_v2` + `dispatch_sources` + `dispatch_claims` + `dispatch_entities`
3. **Basic scoring** — evidence_density, provenance, confidence, suppression logic
4. **One rendering surface** — `render_public_snippet` and `render_internal_brief`
5. **One public API endpoint** — `GET /api/public/observations` returning published snippets
6. **Manual publish flow** — PATCH to set visibility_level, reviewed_by
7. **Podcast linking** — match dispatches to podcasts via existing Qdrant embeddings

### MVP Does NOT Include

- Pattern detection (phase 2)
- Case study linking (needs case study data)
- Full public page rendering
- Social excerpts
- SEO/sitemap generation
- Automation triggers

### MVP Validation Criteria

- Dispatches contain typed claims with source refs
- Suppression logic correctly blocks weak dispatches
- Public snippets contain no unsupported claims
- At least one podcast linked per dispatch with >0.4 relevance
- Internal brief is usable for BD preparation

---

## 14. Phase 2 Enhancements

Once MVP is live and validated:

1. **Expand to all signal types** — extend structured generation prompt for all 9 signal types
2. **Pattern detection** — implement clustering job (section 7), pattern table, pattern pages
3. **Case study ingestion** — create case_studies table or CMS integration, build embedding index, enable linking
4. **Full public pages** — `render_public_full`, schema.org, sitemap, canonical URLs
5. **Social excerpts** — LinkedIn/Twitter card generation
6. **Newsletter integration** — weekly brief composed from top dispatches + patterns
7. **Automation triggers** — publish-to-site, escalate-for-review, create-follow-on triggers in scheduler
8. **Grabs → Dispatch V2 bridge** — signal_grabs optionally create lightweight dispatch_v2 entries with `generation_version: 'grab_derived'`
9. **Change vector taxonomy** — build structured vocabulary of change vectors, enable cross-dispatch vector tracking
10. **Embedding-based search** — embed dispatch_v2 thesis+observation, enable semantic search across dispatches
11. **Editorial dashboard** — UI for reviewing suppressed dispatches, promoting to published, managing patterns

---

## 15. Risks / Failure Modes

### 15.1 Thin Content Risk

**Risk:** Structured generation produces dispatches that meet all provenance rules but contain no real insight — technically grounded but editorially empty.

**Mitigation:** The `non_obvious_angle` field is required. If generation returns null for it, the dispatch receives `editorial_score = 0` and stays internal. The `editorial_score` should include a check for specificity: a thesis that could apply to any company in any market scores low.

### 15.2 Fake Confidence

**Risk:** The model learns to game the structured output by producing plausible-sounding claims with correct source_ref formatting but tenuous actual grounding.

**Mitigation:** Post-generation validation must check that `evidence_snippet` in each source actually supports the claim. This is hard to automate perfectly. For MVP, require editorial review before publication. Long-term, add an NLI (natural language inference) check between claim_text and source evidence_snippets.

### 15.3 Bad Linkage

**Risk:** Related podcasts/case studies are technically similar (high embedding score) but contextually irrelevant — creating a feeling of algorithmic randomness rather than editorial judgment.

**Mitigation:** The `relevance_reason` field forces explicit justification. If the system cannot generate a specific reason (not just "thematically aligned"), the link should be suppressed. Require `overall_relevance ≥ 0.5` for public display.

### 15.4 Pattern Overfitting

**Risk:** Pattern detection creates too many patterns from weak similarity, producing thin aggregation pages.

**Mitigation:** Convergence threshold of 0.5 minimum + 3 dispatch minimum is conservative. Pattern pages should not be published unless `convergence_score ≥ 0.6` and `dispatch_count ≥ 5`. The system should prefer fewer stronger patterns.

### 15.5 Generic AI Voice

**Risk:** Despite structured generation, the rendered output still sounds like Claude writing about markets.

**Mitigation:** The rendering layer is deterministic (no LLM call). It assembles fields that were generated under voice constraints. The generation prompt explicitly forbids consultant clichés and requires Australian English and the MitchelLake voice profile. The `restraint_note` field forces the model to practice editorial restraint explicitly.

### 15.6 Schema Drift

**Risk:** The dispatch_v2 schema grows complex and fields become inconsistently populated across generation versions.

**Mitigation:** `generation_version` tracks which pipeline version produced each dispatch. Migration scripts must handle schema evolution. New fields should be nullable and backward-compatible.

### 15.7 Performance

**Risk:** The v2 generation pipeline is slower (more steps, more queries, post-validation) and could bottleneck the existing 72-hour signal processing window.

**Mitigation:** The structured prompt replaces (not supplements) the blog prompt — net token usage is similar. Post-validation is CPU-cheap. Entity extraction and asset linking can run asynchronously after initial persist. Batch size remains 20 per run.

---

## Appendix: Example Generated Dispatch Object

```json
{
  "id": "a1b2c3d4-...",
  "slug": "2026-03/canva-infrastructure-leadership-hire-signals-enterprise-scaling",
  "tenant_id": "00000000-0000-0000-0000-000000000001",
  "status": "review",
  "signal_event_ids": ["sig-001", "sig-002"],
  "generation_version": "v2.0",

  "confidence_score": 0.78,
  "editorial_score": 0.72,
  "evidence_density_score": 0.65,
  "provenance_score": 0.80,
  "suppression_state": "active",

  "title": "Canva infrastructure hires point to enterprise architecture shift",
  "thesis": "Canva's recent infrastructure leadership appointments suggest a pivot from consumer-scale reliability to enterprise-grade multi-tenancy.",
  "summary": "Two senior infrastructure hires in 8 weeks, both from enterprise SaaS backgrounds. Combined with Canva's enterprise revenue targets disclosed in their Series F materials, this looks like deliberate org redesign.",
  "observation": "The hire pattern — VP Infrastructure from Atlassian, Principal Engineer from Salesforce — suggests Canva is importing enterprise operational DNA rather than promoting from within.",
  "implication": "Enterprise SaaS infrastructure leadership is becoming a competitive hiring category in APAC. Firms scaling from consumer to enterprise need experienced operators who have built multi-tenant platforms at scale.",
  "non_obvious_angle": "The more interesting signal is not the hiring itself but the sourcing geography. Both hires relocated from US-based roles to Sydney, suggesting Canva is willing to pay relocation premiums to build this capability locally rather than distribute it.",
  "watch_next": "Whether Canva's next infrastructure hire comes from cloud provider background (AWS/GCP) — that would confirm they are building, not buying, enterprise infrastructure.",
  "restraint_note": "We are not claiming Canva has publicly committed to an enterprise pivot. The evidence is hiring-pattern based, not strategic disclosure.",

  "why_it_matters_for_clients": "Any growth-stage company scaling into enterprise in APAC will face the same infrastructure leadership gap. The candidate pool is thin and getting thinner as incumbents like Canva compete aggressively.",
  "likely_roles_impacted": ["VP Infrastructure", "Principal Engineer", "Head of Platform"],
  "timing_window": "near-term",
  "market_phase": "building momentum",

  "supporting_sources": [
    {
      "id": "src-1",
      "source_name": "LinkedIn",
      "source_title": "VP Infrastructure appointment at Canva",
      "source_url": "https://linkedin.com/...",
      "evidence_snippet": "Previously VP Platform Engineering at Atlassian (2019-2025)...",
      "source_quality_score": 0.85
    },
    {
      "id": "src-2",
      "source_name": "AFR",
      "source_title": "Canva enterprise push",
      "source_url": "https://afr.com/...",
      "evidence_snippet": "Canva targeting 50% enterprise revenue by 2027...",
      "source_quality_score": 0.90
    }
  ],

  "claims": [
    {
      "claim_text": "Two senior infrastructure hires in 8 weeks",
      "claim_type": "fact",
      "source_refs": ["src-1"],
      "support_level": "moderate",
      "confidence": 0.90,
      "allowed_for_publication": true
    },
    {
      "claim_text": "This suggests a pivot from consumer-scale to enterprise-grade multi-tenancy",
      "claim_type": "interpretation",
      "source_refs": ["src-1", "src-2"],
      "support_level": "strong",
      "confidence": 0.70,
      "allowed_for_publication": true
    }
  ],

  "entities": {
    "companies": [{ "entity_value": "Canva", "entity_id": "co-123", "is_primary": true }],
    "geographies": [{ "entity_value": "Australia" }, { "entity_value": "APAC" }],
    "sectors": [{ "entity_value": "Enterprise SaaS" }],
    "themes": [{ "entity_value": "enterprise scaling" }, { "entity_value": "infrastructure buildout" }],
    "change_vectors": [{ "entity_value": "consumer-to-enterprise org redesign" }],
    "signal_types": [{ "entity_value": "strategic_hiring" }]
  },

  "related_assets": [
    {
      "asset_type": "podcast",
      "title": "Scaling Engineering Orgs in APAC",
      "slug_or_url": "/podcasts/scaling-eng-orgs-apac",
      "relevance_reason": "Related because this episode explores the same shift from product growth to operational scaling in the APAC market.",
      "overall_relevance": 0.72
    }
  ]
}
```
