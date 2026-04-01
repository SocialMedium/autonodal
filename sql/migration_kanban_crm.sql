-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Pipeline Kanban, Project Delivery, Activities, CRM Architecture
-- Run: psql $DATABASE_URL -f sql/migration_kanban_crm.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- Link dispatches to opportunities (claim → opportunity flow)
ALTER TABLE signal_dispatches ADD COLUMN IF NOT EXISTS opportunity_id UUID REFERENCES opportunities(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_dispatches_opportunity ON signal_dispatches(opportunity_id);

-- Project members (multi-party delivery teams — replaces team_member_ids array)
CREATE TABLE IF NOT EXISTS project_members (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  engagement_id UUID NOT NULL REFERENCES engagements(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role VARCHAR(50) NOT NULL DEFAULT 'member',
  joined_at TIMESTAMPTZ DEFAULT NOW(),
  tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  UNIQUE(engagement_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_project_members_engagement ON project_members(engagement_id);
CREATE INDEX IF NOT EXISTS idx_project_members_user ON project_members(user_id);

-- Unified activity log (cascades to entity dossiers)
CREATE TABLE IF NOT EXISTS activities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  user_id UUID REFERENCES users(id),
  user_name TEXT,
  activity_type VARCHAR(50) NOT NULL,
  subject VARCHAR(500),
  description TEXT,
  opportunity_id UUID REFERENCES opportunities(id) ON DELETE CASCADE,
  engagement_id UUID REFERENCES engagements(id) ON DELETE CASCADE,
  person_id UUID REFERENCES people(id) ON DELETE CASCADE,
  company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
  dispatch_id UUID REFERENCES signal_dispatches(id) ON DELETE SET NULL,
  pipeline_contact_id UUID REFERENCES pipeline_contacts(id) ON DELETE SET NULL,
  metadata JSONB DEFAULT '{}',
  source VARCHAR(50) DEFAULT 'manual',
  external_id VARCHAR(255),
  activity_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_activities_tenant ON activities(tenant_id);
CREATE INDEX IF NOT EXISTS idx_activities_opportunity ON activities(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_activities_engagement ON activities(engagement_id);
CREATE INDEX IF NOT EXISTS idx_activities_person ON activities(person_id);
CREATE INDEX IF NOT EXISTS idx_activities_company ON activities(company_id);
CREATE INDEX IF NOT EXISTS idx_activities_at ON activities(activity_at DESC);

-- CRM connections (per-tenant, multi-provider)
CREATE TABLE IF NOT EXISTS crm_connections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  provider VARCHAR(50) NOT NULL,
  display_name TEXT,
  auth_type VARCHAR(20) NOT NULL DEFAULT 'api_key',
  credentials_encrypted JSONB NOT NULL DEFAULT '{}',
  sync_enabled BOOLEAN DEFAULT true,
  sync_direction VARCHAR(20) DEFAULT 'bidirectional',
  sync_interval_minutes INTEGER DEFAULT 30,
  last_sync_at TIMESTAMPTZ,
  last_sync_status VARCHAR(20),
  last_sync_stats JSONB DEFAULT '{}',
  last_error TEXT,
  field_mappings JSONB DEFAULT '{}',
  webhook_secret TEXT,
  webhook_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id, provider)
);

-- CRM sync audit log
CREATE TABLE IF NOT EXISTS crm_sync_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  connection_id UUID NOT NULL REFERENCES crm_connections(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  direction VARCHAR(10) NOT NULL,
  entity_type VARCHAR(50) NOT NULL,
  entity_id UUID,
  external_id VARCHAR(255),
  action VARCHAR(20) NOT NULL,
  changes JSONB DEFAULT '{}',
  error_message TEXT,
  synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crm_sync_log_connection ON crm_sync_log(connection_id);
CREATE INDEX IF NOT EXISTS idx_crm_sync_log_synced ON crm_sync_log(synced_at DESC);

-- RLS
ALTER TABLE activities ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_activities ON activities
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

ALTER TABLE project_members ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_project_members ON project_members
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

ALTER TABLE crm_connections ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_crm_connections ON crm_connections
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- Backfill project_members from existing team_member_ids arrays
INSERT INTO project_members (engagement_id, user_id, role, tenant_id)
SELECT e.id, unnest(e.team_member_ids), 'member', e.tenant_id
FROM engagements e
WHERE e.team_member_ids IS NOT NULL AND array_length(e.team_member_ids, 1) > 0
ON CONFLICT (engagement_id, user_id) DO NOTHING;

-- Set lead_partner as 'lead' role
INSERT INTO project_members (engagement_id, user_id, role, tenant_id)
SELECT e.id, e.lead_partner_id, 'lead', e.tenant_id
FROM engagements e
WHERE e.lead_partner_id IS NOT NULL
ON CONFLICT (engagement_id, user_id) DO UPDATE SET role = 'lead';
