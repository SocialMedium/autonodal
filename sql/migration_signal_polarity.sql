-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: Signal polarity + lifecycle columns on signal_events
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS polarity TEXT
  CHECK (polarity IN ('positive', 'neutral', 'negative'));

ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS phase TEXT
  CHECK (phase IN ('fresh', 'warming', 'hot', 'critical', 'closing', 'closed'));

-- first_detected_at — countdown start point.
-- Defaults to detected_at. Preserved across phase transitions even if signal_events
-- gets re-created (e.g. retroactive detection).
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS first_detected_at TIMESTAMPTZ;

-- Precomputed phase transition timestamps — set by advance_signal_phases
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS critical_at TIMESTAMPTZ;
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS closing_at TIMESTAMPTZ;
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;

-- Hot-path index: lead ranking queries filter by polarity + phase, scope by tenant
CREATE INDEX IF NOT EXISTS idx_signal_events_tenant_polarity_phase
  ON signal_events (tenant_id, polarity, phase)
  WHERE phase != 'closed';

CREATE INDEX IF NOT EXISTS idx_signal_events_polarity_detected
  ON signal_events (polarity, detected_at DESC)
  WHERE polarity IS NOT NULL;

-- Phase transition log
CREATE TABLE IF NOT EXISTS signal_phase_transitions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID,
  signal_id UUID NOT NULL REFERENCES signal_events(id) ON DELETE CASCADE,
  from_phase TEXT,
  to_phase TEXT NOT NULL,
  age_days INTEGER NOT NULL,
  transitioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_phase_transitions_signal ON signal_phase_transitions(signal_id, transitioned_at DESC);
CREATE INDEX IF NOT EXISTS idx_phase_transitions_tenant ON signal_phase_transitions(tenant_id, transitioned_at DESC);

-- Grants
DO $$ BEGIN
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON signal_phase_transitions TO autonodal_app';
EXCEPTION WHEN undefined_object THEN NULL;
END $$;
