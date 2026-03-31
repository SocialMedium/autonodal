-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: EventMedium Event Intelligence Tables
-- Run: psql $DATABASE_URL -f sql/migration_events.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- Enum for event formats
DO $$ BEGIN
  CREATE TYPE event_format AS ENUM (
    'conference', 'meetup', 'summit', 'workshop', 'webinar', 'roundtable',
    'demo_day', 'pitch_event', 'awards', 'networking', 'panel', 'other'
  );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Event Sources
CREATE TABLE IF NOT EXISTS event_sources (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  tenant_id UUID NOT NULL,
  name TEXT NOT NULL,
  feed_url TEXT NOT NULL UNIQUE,
  theme TEXT,
  region TEXT,
  is_active BOOLEAN DEFAULT true,
  last_fetched_at TIMESTAMPTZ,
  last_error TEXT,
  fetch_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Events
CREATE TABLE IF NOT EXISTS events (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  tenant_id UUID NOT NULL,
  external_id TEXT,
  source_id UUID REFERENCES event_sources(id),
  title TEXT NOT NULL,
  description TEXT,
  event_url TEXT,
  source_url TEXT,
  url_hash TEXT UNIQUE,
  theme TEXT,
  region TEXT,
  city TEXT,
  country TEXT,
  format event_format DEFAULT 'other',
  event_date DATE,
  event_end_date DATE,
  event_time TEXT,
  is_virtual BOOLEAN DEFAULT false,
  organiser TEXT,
  speaker_names TEXT[],
  company_mentions TEXT[],
  signal_relevance TEXT[],
  relevance_score FLOAT DEFAULT 0,
  is_featured BOOLEAN DEFAULT false,
  raw_feed_data JSONB,
  published_at TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ DEFAULT NOW(),
  embedded_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_events_theme ON events(theme);
CREATE INDEX IF NOT EXISTS idx_events_region ON events(region);
CREATE INDEX IF NOT EXISTS idx_events_event_date ON events(event_date);
CREATE INDEX IF NOT EXISTS idx_events_relevance ON events(relevance_score DESC);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id);

-- Event-Company links
CREATE TABLE IF NOT EXISTS event_company_links (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_id UUID REFERENCES events(id) ON DELETE CASCADE,
  company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
  link_type TEXT DEFAULT 'mentioned',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(event_id, company_id)
);

-- Event-Person links
CREATE TABLE IF NOT EXISTS event_person_links (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_id UUID REFERENCES events(id) ON DELETE CASCADE,
  person_id UUID REFERENCES people(id) ON DELETE CASCADE,
  role TEXT DEFAULT 'mentioned',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(event_id, person_id)
);

-- Triggers
CREATE TRIGGER events_updated_at BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER event_sources_updated_at BEFORE UPDATE ON event_sources FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- RLS
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_events ON events
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

ALTER TABLE event_sources ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_event_sources ON event_sources
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- Seed EventMedium sources
INSERT INTO event_sources (tenant_id, name, feed_url, theme, region) VALUES
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — All Events',              'https://www.eventmedium.ai/api/events/rss',                                     NULL,            NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — AI',                      'https://www.eventmedium.ai/api/events/rss?theme=AI',                            'AI',            NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — FinTech',                 'https://www.eventmedium.ai/api/events/rss?theme=FinTech',                       'FinTech',       NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Climate Tech',            'https://www.eventmedium.ai/api/events/rss?theme=Climate%20Tech',                'Climate Tech',  NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Cybersecurity',           'https://www.eventmedium.ai/api/events/rss?theme=Cybersecurity',                 'Cybersecurity', NULL),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — UK',                      'https://www.eventmedium.ai/api/events/rss?region=UK',                           NULL,            'UK'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Australia',               'https://www.eventmedium.ai/api/events/rss?region=Australia',                    NULL,            'Australia'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Singapore',               'https://www.eventmedium.ai/api/events/rss?region=Singapore',                    NULL,            'Singapore'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — US',                      'https://www.eventmedium.ai/api/events/rss?region=US',                           NULL,            'US'),
  ('00000000-0000-0000-0000-000000000001', 'EventMedium — Cybersecurity Singapore', 'https://www.eventmedium.ai/api/events/rss?theme=Cybersecurity&region=Singapore','Cybersecurity', 'Singapore')
ON CONFLICT (feed_url) DO NOTHING;
