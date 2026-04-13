-- Speed up enrichment matching queries
-- Currently taking ~300ms per row without these

-- LinkedIn URL lookup (exact + slug pattern)
CREATE INDEX IF NOT EXISTS idx_people_linkedin_url_lower
  ON people (LOWER(linkedin_url))
  WHERE linkedin_url IS NOT NULL;

-- Case-insensitive full name lookup
CREATE INDEX IF NOT EXISTS idx_people_name_lower
  ON people (LOWER(full_name), tenant_id);

-- Email lookup
CREATE INDEX IF NOT EXISTS idx_people_email_lower
  ON people (LOWER(email))
  WHERE email IS NOT NULL;

-- Investor flag (for investor queries)
CREATE INDEX IF NOT EXISTS idx_people_is_investor
  ON people (tenant_id, investor_fit_score DESC)
  WHERE is_investor = true;
