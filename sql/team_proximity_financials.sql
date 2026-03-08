-- ============================================================================
-- MITCHELLAKE TEAM PROXIMITY + FINANCIAL INTELLIGENCE
-- ============================================================================
-- Tracks relationships between candidates and consultants
-- Tracks placement fees, revenue, and client financial health
-- ============================================================================

-- ============================================================================
-- TEAM PROXIMITY: WHO KNOWS WHOM
-- ============================================================================

CREATE TABLE IF NOT EXISTS team_proximity (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Core relationship
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE CASCADE,
  team_member_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  
  -- Connection details
  relationship_type VARCHAR(50) NOT NULL,
  relationship_strength FLOAT NOT NULL CHECK (relationship_strength >= 0 AND relationship_strength <= 1),
  
  -- Context
  connected_date DATE,
  source VARCHAR(50) NOT NULL,
  notes TEXT,
  metadata JSONB DEFAULT '{}',
  
  -- Interaction tracking
  last_interaction_date DATE,
  interaction_count INTEGER DEFAULT 0,
  
  -- Computed scores
  recency_score FLOAT,
  warmth_score FLOAT,
  
  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  
  UNIQUE(person_id, team_member_id, relationship_type)
);

CREATE INDEX idx_team_proximity_person ON team_proximity(person_id);
CREATE INDEX idx_team_proximity_team_member ON team_proximity(team_member_id);
CREATE INDEX idx_team_proximity_strength ON team_proximity(relationship_strength DESC);

-- ============================================================================
-- PLACEMENTS: SUCCESSFUL CANDIDATE PLACEMENTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS placements (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  
  -- Core placement data
  person_id UUID NOT NULL REFERENCES people(id) ON DELETE RESTRICT,
  client_id UUID NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  search_id UUID REFERENCES searches(id) ON DELETE SET NULL,
  
  -- Consultant who made the placement
  placed_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  assisted_by_user_ids UUID[] DEFAULT '{}',
  
  -- Position details
  role_title VARCHAR(255) NOT NULL,
  role_level VARCHAR(50),
  department VARCHAR(100),
  start_date DATE NOT NULL,
  
  -- Financial data
  placement_fee DECIMAL(12,2) NOT NULL,
  currency VARCHAR(3) DEFAULT 'USD',
  fee_type VARCHAR(50) DEFAULT 'percentage',
  fee_percentage DECIMAL(5,2),
  candidate_salary DECIMAL(12,2),
  
  -- Payment tracking
  invoice_number VARCHAR(50),
  invoice_date DATE,
  payment_status VARCHAR(50) NOT NULL DEFAULT 'pending',
  payment_date DATE,
  payment_amount DECIMAL(12,2),
  outstanding_amount DECIMAL(12,2),
  
  -- Performance tracking
  guarantee_period_months INTEGER DEFAULT 6,
  still_employed BOOLEAN DEFAULT TRUE,
  left_date DATE,
  left_reason VARCHAR(100),
  replacement_search_id UUID REFERENCES searches(id) ON DELETE SET NULL,
  replacement_completed BOOLEAN DEFAULT FALSE,
  
  -- Client satisfaction
  client_satisfaction_score INTEGER CHECK (client_satisfaction_score BETWEEN 1 AND 5),
  client_feedback TEXT,
  would_recommend BOOLEAN,
  
  -- Source tracking
  source VARCHAR(50) NOT NULL DEFAULT 'manual',
  xero_invoice_id VARCHAR(100),
  xero_project_id VARCHAR(100),
  ezekia_placement_id INTEGER,
  
  notes TEXT,
  metadata JSONB DEFAULT '{}',
  
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  
  CONSTRAINT unique_xero_invoice UNIQUE(xero_invoice_id)
);

CREATE INDEX idx_placements_person ON placements(person_id);
CREATE INDEX idx_placements_client ON placements(client_id);
CREATE INDEX idx_placements_placed_by ON placements(placed_by_user_id);
CREATE INDEX idx_placements_start_date ON placements(start_date DESC);

-- ============================================================================
-- CLIENT FINANCIALS: REVENUE & RELATIONSHIP HEALTH
-- ============================================================================

CREATE TABLE IF NOT EXISTS client_financials (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  
  -- Revenue metrics
  total_invoiced DECIMAL(12,2) DEFAULT 0,
  total_paid DECIMAL(12,2) DEFAULT 0,
  total_outstanding DECIMAL(12,2) DEFAULT 0,
  
  -- Placement metrics
  total_placements INTEGER DEFAULT 0,
  active_placements INTEGER DEFAULT 0,
  replacements_needed INTEGER DEFAULT 0,
  
  average_placement_fee DECIMAL(12,2),
  highest_placement_fee DECIMAL(12,2),
  lowest_placement_fee DECIMAL(12,2),
  
  first_placement_date DATE,
  last_placement_date DATE,
  
  -- Payment behavior
  average_days_to_payment DECIMAL(6,2),
  payment_reliability FLOAT,
  overdue_invoices_count INTEGER DEFAULT 0,
  
  -- Relationship health
  client_lifetime_value DECIMAL(12,2),
  relationship_status VARCHAR(50),
  last_contact_date DATE,
  
  -- Performance
  average_client_satisfaction DECIMAL(3,2),
  placement_success_rate DECIMAL(5,2),
  
  computed_at TIMESTAMPTZ DEFAULT NOW(),
  
  UNIQUE(client_id)
);

CREATE INDEX idx_client_financials_client ON client_financials(client_id);

-- ============================================================================
-- TRIGGER: Auto-create team_proximity on placement
-- ============================================================================

CREATE OR REPLACE FUNCTION create_placement_team_proximity()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO team_proximity (
    person_id,
    team_member_id,
    relationship_type,
    relationship_strength,
    connected_date,
    source,
    last_interaction_date,
    metadata
  ) VALUES (
    NEW.person_id,
    NEW.placed_by_user_id,
    'past_placement',
    1.0,
    NEW.start_date,
    NEW.source,
    NEW.start_date,
    jsonb_build_object(
      'placement_id', NEW.id,
      'role_title', NEW.role_title,
      'placement_fee', NEW.placement_fee
    )
  )
  ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
    last_interaction_date = GREATEST(team_proximity.last_interaction_date, NEW.start_date),
    metadata = team_proximity.metadata || jsonb_build_object(
      'latest_placement_id', NEW.id,
      'latest_role_title', NEW.role_title,
      'latest_placement_fee', NEW.placement_fee
    );
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER placements_create_proximity
  AFTER INSERT ON placements
  FOR EACH ROW
  EXECUTE FUNCTION create_placement_team_proximity();

-- ============================================================================
-- SUCCESS!
-- ============================================================================

DO $$
BEGIN
  RAISE NOTICE '✅ Team Proximity & Financial Intelligence schema created successfully';
  RAISE NOTICE '📊 Tables: team_proximity, placements, client_financials';
  RAISE NOTICE '⚡ Triggers: Auto-create team_proximity on placements';
END $$;