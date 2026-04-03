-- ═══════════════════════════════════════════════════════════════════════════════
-- Platform Catalog — Onboarding pick-and-mix
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS platform_groups (
  slug          TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  icon          TEXT NOT NULL,
  description   TEXT,
  display_order INTEGER NOT NULL DEFAULT 100
);

CREATE TABLE IF NOT EXISTS platform_catalog (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name            TEXT NOT NULL,
  slug            TEXT NOT NULL UNIQUE,
  description     TEXT,
  icon            TEXT,
  group_slug      TEXT NOT NULL REFERENCES platform_groups(slug),
  status          TEXT NOT NULL DEFAULT 'available',
  auth_type       TEXT NOT NULL DEFAULT 'oauth',
  oauth_provider  TEXT,
  import_entity   TEXT,
  typical_records TEXT,
  value_prop      TEXT,
  display_order   INTEGER NOT NULL DEFAULT 100,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS onboarding_connections (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  platform_slug    TEXT NOT NULL REFERENCES platform_catalog(slug),
  status           TEXT NOT NULL DEFAULT 'selected',
  connected_at     TIMESTAMPTZ,
  records_imported INTEGER,
  last_error       TEXT,
  metadata         JSONB DEFAULT '{}',
  UNIQUE(tenant_id, platform_slug)
);

ALTER TABLE onboarding_connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE onboarding_connections FORCE ROW LEVEL SECURITY;

DO $$ BEGIN
  CREATE POLICY tenant_isolation_onboarding_connections
    ON onboarding_connections
    USING (tenant_id = current_tenant_id())
    WITH CHECK (tenant_id = current_tenant_id());
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Platform groups
INSERT INTO platform_groups (slug, name, icon, description, display_order) VALUES
('communication', 'Email',              'mail',      'Maps your real relationship graph from email history',  10),
('network',       'Network & Contacts', 'users',     'Imports your professional contacts and connections',    20),
('crm',           'CRM',                'briefcase', 'Syncs your client, deal, and contact database',         30),
('documents',     'Documents',          'file-text', 'Makes your files and notes searchable intelligence',    40),
('profile',       'Profile & CV',       'user',      'Extracts career history and expertise',                  50),
('messaging',     'Messaging',          'message-square', 'Captures relationship signals from conversations', 60),
('spreadsheets',  'Spreadsheets',       'bar-chart', 'Imports contact and company data from tables',           70)
ON CONFLICT (slug) DO NOTHING;

-- Platform catalog
INSERT INTO platform_catalog
  (name, slug, group_slug, icon, status, auth_type, oauth_provider,
   import_entity, typical_records, value_prop, display_order)
VALUES
-- Email
('Gmail',               'gmail',             'communication','mail',          'available',   'oauth',     'google',    'emails',  '1,000-50,000 interactions','Highest value — maps who you talk to, how often, how recently', 10),
('Outlook / Office 365','outlook',           'communication','mail',          'available',   'oauth',     'microsoft', 'emails',  '1,000-50,000 interactions','Same relationship mapping as Gmail for Microsoft users',        20),
-- Network
('LinkedIn Connections','linkedin_csv',      'network',      'linkedin',      'available',   'csv_upload',NULL,        'contacts','200-5,000 connections',    'Adds company, role, and seniority context to everyone you know', 10),
('Google Contacts',     'google_contacts',   'network',      'users',         'available',   'oauth',     'google',    'contacts','100-2,000 contacts',       'Your full Google address book as a starting network',            20),
('Microsoft Contacts',  'microsoft_contacts','network',      'users',         'available',   'oauth',     'microsoft', 'contacts','100-2,000 contacts',       'Your Outlook address book and People contacts',                  30),
-- CRM
('HubSpot',             'hubspot',           'crm',          'circle-dot',    'available',   'oauth',     'hubspot',   'contacts','500-50,000 records',       'Syncs contacts, companies, and deal history',                    10),
('Salesforce',          'salesforce',        'crm',          'cloud',         'available',   'oauth',     'salesforce','contacts','500-100,000 records',      'Syncs contacts, accounts, and opportunity pipeline',             20),
('Pipedrive',           'pipedrive',         'crm',          'target',        'available',   'oauth',     'pipedrive', 'contacts','200-20,000 records',       'Syncs your Pipedrive contacts and deal pipeline',                30),
('Copper CRM',          'copper',            'crm',          'shield',        'available',   'oauth',     'copper',    'contacts','200-10,000 records',       'Google-native CRM — syncs directly with your workspace',         40),
('Ezekia',              'ezekia',            'crm',          'zap',           'available',   'csv_upload',NULL,        'contacts','500-50,000 records',       'Upload your Ezekia export to sync candidate and client data',    50),
('Contacts CSV',        'contacts_csv',      'crm',          'file-text',     'available',   'csv_upload',NULL,        'contacts','Any size',                 'Export from any CRM and upload directly',                        60),
-- Documents
('Google Drive',        'google_drive',      'documents',    'file-text',     'available',   'oauth',     'google',    'files',   'Selected folders',         'Pitch decks, briefs, and notes become searchable intelligence',  10),
('OneDrive',            'onedrive',          'documents',    'cloud',         'available',   'oauth',     'microsoft', 'files',   'Selected folders',         'SharePoint and OneDrive files as intelligence context',          20),
('Notion',              'notion',            'documents',    'clipboard',     'coming_soon', 'oauth',     NULL,        'pages',   'Selected pages',           'Notion pages and databases as intelligence context',             30),
-- Profile
('CV / Resume',         'cv_upload',         'profile',      'user',          'available',   'csv_upload',NULL,        'profile', '1 document',               'Extracts career history, skills, and sector expertise',          10),
('LinkedIn Profile Export','linkedin_profile','profile',      'linkedin',      'available',   'csv_upload',NULL,        'profile', '1 export',                 'Full LinkedIn data export — positions, education, skills',       20),
-- Messaging
('Telegram',            'telegram',          'messaging',    'message-square','coming_soon', 'oauth',     NULL,        'messages','Chat history',             'Relationship signals from your Telegram conversations',          10),
('WhatsApp',            'whatsapp',          'messaging',    'message-square','coming_soon', 'oauth',     NULL,        'messages','Chat history',             'WhatsApp Business or personal relationship mapping',             20),
('Slack',               'slack',             'messaging',    'message-square','coming_soon', 'oauth',     NULL,        'messages','Workspace messages',       'External conversation relationship intelligence',                30),
-- Spreadsheets
('Airtable',            'airtable',          'spreadsheets', 'bar-chart',     'available',   'api_key',   NULL,        'records', 'Any base size',            'Import any Airtable base containing contacts or companies',      10),
('Google Sheets',       'google_sheets',     'spreadsheets', 'bar-chart',     'available',   'oauth',     'google',    'rows',    'Any sheet size',           'Import contact and company lists from Google Sheets',            20)
ON CONFLICT (slug) DO NOTHING;
