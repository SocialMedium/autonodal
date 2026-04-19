-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: Work Artifacts — Intelligence write-back system
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Captures judgement work products (debriefs, assessments, interview guides)
-- from Claude MCP conversations back into the Autonodal knowledge graph.
--
-- DATA SOVEREIGNTY: Artifacts are tenant IP. They NEVER feed into platform-level
-- pipelines, cross-tenant matching, composite embeddings, or huddle intelligence.
-- The work_artifacts Qdrant collection enforces mandatory tenant_id filtering.
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── Types ────────────────────────────────────────────────────────────────────

DO $$ BEGIN
  CREATE TYPE artifact_type AS ENUM (
    'debrief_360',
    'executive_summary',
    'interview_guide',
    'assessment_framework',
    'calibration_note',
    'search_update',
    'candidate_note',
    'company_note',
    'market_analysis',
    'reference_check',
    'offer_brief',
    'custom'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE artifact_status AS ENUM ('draft', 'final', 'archived');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE entity_link_type AS ENUM (
    'subject', 'mentioned', 'assessed', 'interviewer',
    'client', 'target', 'related'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE artifact_entity_type AS ENUM ('person', 'company', 'search');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ── Work Artifacts ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS work_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),

  -- Classification
  artifact_type artifact_type NOT NULL,
  custom_type_label VARCHAR(100),
  title VARCHAR(500) NOT NULL,
  status artifact_status NOT NULL DEFAULT 'final',

  -- Content
  content_markdown TEXT NOT NULL,
  summary TEXT,
  key_findings JSONB DEFAULT '[]'::jsonb,
  structured_data JSONB DEFAULT '{}'::jsonb,

  -- Authorship
  created_by UUID REFERENCES users(id),
  created_by_name VARCHAR(200),
  source_context VARCHAR(50) DEFAULT 'claude_mcp',

  -- Vector reference
  qdrant_point_id VARCHAR(100),
  embedded_at TIMESTAMPTZ,

  -- Timestamps
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Entity Links ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS artifact_entity_links (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  artifact_id UUID NOT NULL REFERENCES work_artifacts(id) ON DELETE CASCADE,

  -- Entity reference
  entity_type artifact_entity_type NOT NULL,
  person_id UUID REFERENCES people(id) ON DELETE SET NULL,
  company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
  search_id UUID,  -- No FK constraint — searches table may use 'opportunities' alias

  -- Link classification
  link_type entity_link_type NOT NULL DEFAULT 'related',

  -- Context
  context_note VARCHAR(500),
  confidence DECIMAL(3,2) DEFAULT 1.0,
  auto_detected BOOLEAN DEFAULT FALSE,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT valid_artifact_entity CHECK (
    (entity_type = 'person' AND person_id IS NOT NULL) OR
    (entity_type = 'company' AND company_id IS NOT NULL) OR
    (entity_type = 'search' AND search_id IS NOT NULL)
  )
);

-- ── Indexes ─────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_work_artifacts_tenant ON work_artifacts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_work_artifacts_type ON work_artifacts(tenant_id, artifact_type);
CREATE INDEX IF NOT EXISTS idx_work_artifacts_status ON work_artifacts(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_work_artifacts_created ON work_artifacts(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_work_artifacts_created_by ON work_artifacts(tenant_id, created_by);

CREATE INDEX IF NOT EXISTS idx_artifact_links_artifact ON artifact_entity_links(artifact_id);
CREATE INDEX IF NOT EXISTS idx_artifact_links_person ON artifact_entity_links(tenant_id, person_id) WHERE person_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artifact_links_company ON artifact_entity_links(tenant_id, company_id) WHERE company_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artifact_links_search ON artifact_entity_links(tenant_id, search_id) WHERE search_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_artifact_links_type ON artifact_entity_links(artifact_id, link_type);

-- ── Unique constraint for dedup (no duplicate link of same type to same entity) ──

CREATE UNIQUE INDEX IF NOT EXISTS idx_artifact_links_unique
  ON artifact_entity_links(artifact_id, entity_type, COALESCE(person_id, '00000000-0000-0000-0000-000000000000'), COALESCE(company_id, '00000000-0000-0000-0000-000000000000'), COALESCE(search_id, '00000000-0000-0000-0000-000000000000'), link_type);

-- ── Row Level Security ──────────────────────────────────────────────────────

ALTER TABLE work_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE work_artifacts FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_work_artifacts ON work_artifacts
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

ALTER TABLE artifact_entity_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_entity_links FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_artifact_entity_links ON artifact_entity_links
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- ── Grant access to app role ────────────────────────────────────────────────

DO $$ BEGIN
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON work_artifacts TO autonodal_app';
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON artifact_entity_links TO autonodal_app';
EXCEPTION WHEN undefined_object THEN NULL;
END $$;
