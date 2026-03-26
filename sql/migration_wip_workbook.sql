-- ============================================================================
-- WIP Workbook Ingestion — Schema Additions
-- Run this BEFORE the ingestion scripts
-- ============================================================================

-- The placements table has NOT NULL on person_id, client_id, placed_by_user_id,
-- start_date, placement_fee — but WIP data includes proposals, lost work, and
-- active searches that don't have all of these. We need to relax constraints.

-- 1. Relax NOT NULL constraints for WIP/proposal data
ALTER TABLE placements ALTER COLUMN person_id DROP NOT NULL;
ALTER TABLE placements ALTER COLUMN client_id DROP NOT NULL;
ALTER TABLE placements ALTER COLUMN placed_by_user_id DROP NOT NULL;
ALTER TABLE placements ALTER COLUMN start_date DROP NOT NULL;
ALTER TABLE placements ALTER COLUMN placement_fee DROP NOT NULL;
ALTER TABLE placements ALTER COLUMN role_title DROP NOT NULL;

-- 2. Change client_id FK from clients to companies (if referencing clients table)
-- The codebase uses 'companies' table, not 'clients' — but the original DDL
-- references clients(id). We add a companies reference if needed.
ALTER TABLE placements ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES companies(id);

-- 3. New columns for WIP workbook data
ALTER TABLE placements ADD COLUMN IF NOT EXISTS fee_stage VARCHAR(30);
  -- retainer_stage1, retainer_stage2, placement, project

ALTER TABLE placements ADD COLUMN IF NOT EXISTS fee_estimate DECIMAL(12,2);

ALTER TABLE placements ADD COLUMN IF NOT EXISTS opportunity_type VARCHAR(50);
  -- WIP - Placed, WIP - Active, Proposal - Won, Proposal - Lost, etc.

ALTER TABLE placements ADD COLUMN IF NOT EXISTS source_sheet VARCHAR(100);

ALTER TABLE placements ADD COLUMN IF NOT EXISTS raw_monthly_data JSONB;

ALTER TABLE placements ADD COLUMN IF NOT EXISTS consultant_name VARCHAR(100);

ALTER TABLE placements ADD COLUMN IF NOT EXISTS client_name_raw VARCHAR(255);

ALTER TABLE placements ADD COLUMN IF NOT EXISTS candidate_salary_raw VARCHAR(50);

-- 4. Tenant ID (if missing)
ALTER TABLE placements ADD COLUMN IF NOT EXISTS tenant_id UUID DEFAULT '00000000-0000-0000-0000-000000000001';

-- 5. Dedup index for re-runnable imports
CREATE UNIQUE INDEX IF NOT EXISTS idx_placements_wip_dedup
  ON placements (client_name_raw, role_title, source_sheet)
  WHERE source IN ('wip_workbook', 'xero_export');

-- 6. Receivables table
CREATE TABLE IF NOT EXISTS receivables (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID DEFAULT '00000000-0000-0000-0000-000000000001',
  invoice_number VARCHAR(50),
  client_name VARCHAR(255),
  company_id UUID REFERENCES companies(id),
  invoice_date DATE,
  due_date DATE,
  invoice_total DECIMAL(12,2),
  currency VARCHAR(3) DEFAULT 'GBP',
  status VARCHAR(50),
  days_overdue INTEGER,
  notes TEXT,
  action VARCHAR(100),
  source VARCHAR(50) DEFAULT 'workbook',
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_receivables_client ON receivables(company_id);
CREATE INDEX IF NOT EXISTS idx_receivables_status ON receivables(status);
CREATE INDEX IF NOT EXISTS idx_receivables_overdue ON receivables(days_overdue DESC);
