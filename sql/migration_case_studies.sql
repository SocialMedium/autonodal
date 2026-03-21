-- ════════════════════════════════════════════════════════════════
-- Case study library + document classification
-- Companion intelligence layer — links TO signals, never creates them
-- ════════════════════════════════════════════════════════════════

-- Document classification on external_documents
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS document_type VARCHAR(50);
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS content_summary TEXT;
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS relevance_tags TEXT[] DEFAULT '{}';
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS classified_at TIMESTAMPTZ;
ALTER TABLE external_documents ADD COLUMN IF NOT EXISTS classification_version VARCHAR(20);

CREATE INDEX IF NOT EXISTS idx_docs_document_type ON external_documents(document_type) WHERE document_type IS NOT NULL;

-- People mentioned in documents (shortlisted candidates in decks, etc.)
CREATE TABLE IF NOT EXISTS document_people (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id     UUID NOT NULL REFERENCES external_documents(id) ON DELETE CASCADE,
  person_id       UUID REFERENCES people(id) ON DELETE SET NULL,
  person_name     TEXT NOT NULL,
  person_title    TEXT,
  person_company  TEXT,
  mention_role    VARCHAR(50) NOT NULL DEFAULT 'mentioned',
    -- shortlisted, longlisted, placed, referenced, authored, interviewed, target
  confidence      DECIMAL(3,2) DEFAULT 0.5,
  context_note    TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(document_id, person_name, mention_role)
);

CREATE INDEX IF NOT EXISTS idx_docpeople_document ON document_people(document_id);
CREATE INDEX IF NOT EXISTS idx_docpeople_person ON document_people(person_id) WHERE person_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_docpeople_role ON document_people(mention_role);

-- Case studies — structured extraction from classified documents
CREATE TABLE IF NOT EXISTS case_studies (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id         UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  document_id       UUID REFERENCES external_documents(id) ON DELETE SET NULL,
  slug              VARCHAR(300),

  -- Core fields
  title             TEXT NOT NULL,
  client_name       TEXT,
  client_id         UUID REFERENCES companies(id),
  engagement_type   VARCHAR(100),
    -- executive_search, board_advisory, leadership_assessment, team_build, succession, market_mapping
  role_title        TEXT,
  seniority_level   VARCHAR(50),

  -- Context
  sector            TEXT,
  geography         TEXT,
  year              INT,
  challenge         TEXT,
  approach          TEXT,
  outcome           TEXT,
  impact_note       TEXT,

  -- Linking dimensions
  themes            TEXT[] DEFAULT '{}',
  change_vectors    TEXT[] DEFAULT '{}',
  capabilities      TEXT[] DEFAULT '{}',
    -- e.g., cross-border, founder-transition, post-acquisition, high-growth, turnaround

  -- Scores
  relevance_score   DECIMAL(4,3) DEFAULT 0,
  completeness      DECIMAL(4,3) DEFAULT 0,

  -- ═══ External-safe fields ═══
  -- These are the ONLY fields that may appear in dispatches, public pages, or external content.
  -- They must NEVER contain: candidate names, fees, contact details, or identifiable client info
  -- unless the client has approved public use.
  public_title      TEXT,
    -- e.g., "CTO Search — High-Growth Fintech, Singapore"
    -- NOT "CTO Search — Acme Corp" unless client has approved
  public_summary    TEXT,
    -- 2-3 sentence anonymised summary safe for external use
  public_sector     TEXT,
    -- can match internal sector or be more general
  public_geography  TEXT,
  public_capability TEXT,
    -- e.g., "Cross-border executive search in high-growth fintech"
  public_approved   BOOLEAN NOT NULL DEFAULT false,
    -- HARD GATE: case study cannot appear in dispatches/public unless true

  -- Governance
  visibility        VARCHAR(20) NOT NULL DEFAULT 'internal_only',
    -- internal_only: visible to team only
    -- dispatch_ready: approved for bundling with dispatches (requires public_approved = true)
    -- published: live on public site
  status            VARCHAR(20) NOT NULL DEFAULT 'draft',
    -- draft, reviewed, sanitised, published
  extracted_by      VARCHAR(50) DEFAULT 'system',
  reviewed_by       UUID REFERENCES users(id),
  sanitised_by      UUID REFERENCES users(id),
  sanitised_at      TIMESTAMPTZ,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_casestudies_tenant ON case_studies(tenant_id);
CREATE INDEX IF NOT EXISTS idx_casestudies_sector ON case_studies(sector);
CREATE INDEX IF NOT EXISTS idx_casestudies_geography ON case_studies(geography);
CREATE INDEX IF NOT EXISTS idx_casestudies_themes ON case_studies USING gin(themes);
CREATE INDEX IF NOT EXISTS idx_casestudies_status ON case_studies(status);
CREATE INDEX IF NOT EXISTS idx_casestudies_client ON case_studies(client_id) WHERE client_id IS NOT NULL;
