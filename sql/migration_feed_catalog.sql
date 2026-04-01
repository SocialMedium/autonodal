-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: Feed Catalog, Bundles, Subscriptions, Quality Metrics
-- The curated signal feed library — pick-and-mix RSS curation system
-- Run: node scripts/run_migration.js sql/migration_feed_catalog.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── ENUMS ────────────────────────────────────────────────────────────────────
DO $$ BEGIN CREATE TYPE feed_tier AS ENUM ('global_macro','curated');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN CREATE TYPE catalog_feed_format AS ENUM ('rss','atom','json_feed','api','scrape');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ── FEED CATALOG (Platform-Level) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feed_catalog (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name              TEXT NOT NULL,
  slug              TEXT NOT NULL UNIQUE,
  description       TEXT,
  url               TEXT NOT NULL,
  feed_format       catalog_feed_format NOT NULL DEFAULT 'rss',
  homepage_url      TEXT,
  tier              feed_tier NOT NULL DEFAULT 'curated',
  primary_category  TEXT NOT NULL,
  tags              TEXT[] NOT NULL DEFAULT '{}',
  sectors           TEXT[] NOT NULL DEFAULT '{}',
  geographies       TEXT[] NOT NULL DEFAULT '{}',
  verticals         TEXT[] NOT NULL DEFAULT '{}',
  is_active         BOOLEAN NOT NULL DEFAULT true,
  is_deprecated     BOOLEAN NOT NULL DEFAULT false,
  deprecation_note  TEXT,
  quality_score     NUMERIC(3,2),
  avg_articles_day  INTEGER,
  language          TEXT NOT NULL DEFAULT 'en',
  requires_auth     BOOLEAN NOT NULL DEFAULT false,
  fetch_interval_min INTEGER NOT NULL DEFAULT 60,
  last_tested_at    TIMESTAMPTZ,
  last_test_status  TEXT,
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feed_catalog_tier ON feed_catalog(tier);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_category ON feed_catalog(primary_category);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_tags ON feed_catalog USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_sectors ON feed_catalog USING GIN(sectors);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_geographies ON feed_catalog USING GIN(geographies);
CREATE INDEX IF NOT EXISTS idx_feed_catalog_active ON feed_catalog(is_active, is_deprecated);

-- ── FEED BUNDLES ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feed_bundles (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name            TEXT NOT NULL,
  slug            TEXT NOT NULL UNIQUE,
  description     TEXT NOT NULL,
  icon            TEXT,
  tier            feed_tier NOT NULL DEFAULT 'curated',
  bundle_type     TEXT NOT NULL,
  tags            TEXT[] NOT NULL DEFAULT '{}',
  sectors         TEXT[] NOT NULL DEFAULT '{}',
  geographies     TEXT[] NOT NULL DEFAULT '{}',
  verticals       TEXT[] NOT NULL DEFAULT '{}',
  display_order   INTEGER NOT NULL DEFAULT 100,
  is_featured     BOOLEAN NOT NULL DEFAULT false,
  is_active       BOOLEAN NOT NULL DEFAULT true,
  source_count    INTEGER NOT NULL DEFAULT 0,
  subscriber_count INTEGER NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feed_bundles_type ON feed_bundles(bundle_type);
CREATE INDEX IF NOT EXISTS idx_feed_bundles_featured ON feed_bundles(is_featured, is_active);

-- ── BUNDLE ↔ SOURCE MAPPING ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feed_bundle_sources (
  bundle_id       UUID NOT NULL REFERENCES feed_bundles(id) ON DELETE CASCADE,
  source_id       UUID NOT NULL REFERENCES feed_catalog(id) ON DELETE CASCADE,
  added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  added_by        TEXT,
  PRIMARY KEY (bundle_id, source_id)
);

-- ── TENANT FEED SUBSCRIPTIONS ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tenant_feed_subscriptions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       UUID NOT NULL,
  bundle_id       UUID REFERENCES feed_bundles(id) ON DELETE SET NULL,
  source_id       UUID REFERENCES feed_catalog(id) ON DELETE SET NULL,
  is_enabled      BOOLEAN NOT NULL DEFAULT true,
  subscribed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  disabled_at     TIMESTAMPTZ,
  disabled_reason TEXT,
  CONSTRAINT chk_bundle_or_source CHECK (
    (bundle_id IS NOT NULL AND source_id IS NULL) OR
    (bundle_id IS NULL AND source_id IS NOT NULL)
  ),
  UNIQUE (tenant_id, bundle_id),
  UNIQUE (tenant_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_tenant_feed_subs_tenant ON tenant_feed_subscriptions(tenant_id, is_enabled);

ALTER TABLE tenant_feed_subscriptions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_feed_subscriptions ON tenant_feed_subscriptions
  USING (current_tenant_id() IS NULL OR tenant_id = current_tenant_id());

-- ── FEED QUALITY METRICS ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS feed_quality_metrics (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id       UUID NOT NULL REFERENCES feed_catalog(id) ON DELETE CASCADE,
  measured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  articles_fetched  INTEGER NOT NULL DEFAULT 0,
  articles_period   INTERVAL NOT NULL DEFAULT '7 days',
  signals_detected  INTEGER NOT NULL DEFAULT 0,
  signal_yield      NUMERIC(5,4),
  high_conf_signals INTEGER NOT NULL DEFAULT 0,
  duplicate_rate    NUMERIC(4,3),
  avg_article_age   INTERVAL,
  noise_rate        NUMERIC(4,3),
  signal_breakdown  JSONB DEFAULT '{}',
  active_subscribers INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_feed_quality_source ON feed_quality_metrics(source_id, measured_at DESC);

-- ── LINK rss_sources TO CATALOG ──────────────────────────────────────────────
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS catalog_source_id UUID REFERENCES feed_catalog(id);
CREATE INDEX IF NOT EXISTS idx_rss_sources_catalog ON rss_sources(catalog_source_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED: GLOBAL MACRO SOURCES (Always-on, platform-wide)
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO feed_catalog (name, slug, tier, primary_category, url, tags, geographies, description, fetch_interval_min) VALUES
('SEC EDGAR 8-K Feed', 'sec-edgar-8k', 'global_macro', 'regulatory',
  'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&output=atom',
  ARRAY['regulatory','filings','us','public_markets'], ARRAY['USA'],
  'Real-time 8-K filings — the gold standard for US public company events', 15),
('PR Newswire', 'pr-newswire', 'global_macro', 'wire',
  'https://www.prnewswire.com/rss/news-releases-list.rss',
  ARRAY['wire','press_release','global'], ARRAY[]::TEXT[],
  'Global press release distribution — broadest corporate announcement coverage', 30),
('Business Wire', 'business-wire', 'global_macro', 'wire',
  'https://feed.businesswire.com/rss/home/?rss=G1',
  ARRAY['wire','press_release','global'], ARRAY[]::TEXT[],
  'Major corporate press releases — strong for US and European companies', 30),
('GlobeNewswire M&A', 'globenewswire', 'global_macro', 'wire',
  'https://www.globenewswire.com/RssFeed/subjectcode/23-Mergers%20Acquisitions',
  ARRAY['wire','press_release','ma','global'], ARRAY[]::TEXT[],
  'M&A and corporate actions focus', 30),
('Accesswire', 'accesswire', 'global_macro', 'wire',
  'https://www.accesswire.com/rss',
  ARRAY['wire','press_release','global'], ARRAY[]::TEXT[],
  'Broad press release distribution — good for smaller cap and international', 60),
('Reuters Business', 'reuters-business', 'global_macro', 'news',
  'https://feeds.reuters.com/reuters/businessNews',
  ARRAY['news','global','financial'], ARRAY[]::TEXT[],
  'Reuters global business and markets news', 30),
('Financial Times Companies', 'ft-companies', 'global_macro', 'news',
  'https://www.ft.com/companies?format=rss',
  ARRAY['news','global','financial','companies'], ARRAY[]::TEXT[],
  'FT companies coverage — authoritative on large cap and European markets', 60)
ON CONFLICT (slug) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED: CURATED BUNDLES
-- ═══════════════════════════════════════════════════════════════════════════════

-- Investor Type
INSERT INTO feed_bundles (name, slug, description, icon, bundle_type, tags, display_order, is_featured) VALUES
('Venture Capital', 'vc-global', 'Global VC deal flow, fund news, and portfolio company activity', '🚀', 'investor_type', ARRAY['vc','venture','startup','funding'], 10, true),
('Private Equity', 'pe-global', 'PE buyouts, add-ons, portfolio operations, and exits', '💼', 'investor_type', ARRAY['pe','private_equity','buyout','lbo'], 11, true),
('Growth Equity', 'growth-equity', 'Growth-stage rounds, scale-up funding, Series B-D', '📈', 'investor_type', ARRAY['growth','scale','series_b','series_c'], 12, false),
('Hedge Funds & Alt Assets', 'hedge-funds', 'Activist moves, short positions, macro funds, and alternatives', '⚡', 'investor_type', ARRAY['hedge_fund','activist','macro'], 13, false),
('Family Office & UHNW', 'family-office', 'Family office deals, direct investments, UHNW wealth events', '🏛', 'investor_type', ARRAY['family_office','uhnw'], 14, false),
('Sovereign Wealth', 'sovereign-wealth', 'SWF investment activity and strategic asset moves', '🌐', 'investor_type', ARRAY['swf','sovereign'], 15, false)
ON CONFLICT (slug) DO NOTHING;

-- Sector
INSERT INTO feed_bundles (name, slug, description, icon, bundle_type, tags, sectors, display_order, is_featured) VALUES
('FinTech & Payments', 'sector-fintech', 'Payments, lending, banking infrastructure, insurtech', '💳', 'sector', ARRAY['fintech','payments','banking'], ARRAY['fintech'], 20, true),
('HealthTech & BioTech', 'sector-healthtech', 'Digital health, medtech, biotech, pharma', '🧬', 'sector', ARRAY['healthtech','biotech','pharma'], ARRAY['healthtech','biotech'], 21, true),
('Web3 & DeFi', 'sector-web3', 'Crypto, DeFi protocols, NFT infrastructure, blockchain', '⛓', 'sector', ARRAY['web3','crypto','defi'], ARRAY['web3'], 22, true),
('CleanTech & Climate', 'sector-cleantech', 'Clean energy, climate tech, sustainability, ESG', '🌱', 'sector', ARRAY['cleantech','climate','energy','esg'], ARRAY['cleantech'], 23, true),
('Enterprise SaaS & AI', 'sector-saas-ai', 'B2B SaaS, AI/ML platforms, developer tools, infrastructure', '🤖', 'sector', ARRAY['saas','ai','ml','devtools'], ARRAY['saas','ai'], 24, true),
('AdTech & MarTech', 'sector-adtech', 'Programmatic advertising, marketing technology', '📣', 'sector', ARRAY['adtech','martech'], ARRAY['adtech'], 25, false),
('DeepTech & Defence', 'sector-deeptech', 'Semiconductors, quantum, robotics, defence technology', '🔬', 'sector', ARRAY['deeptech','semiconductor','quantum'], ARRAY['deeptech'], 26, false),
('Consumer & Retail', 'sector-consumer', 'Consumer brands, e-commerce, retail technology', '🛍', 'sector', ARRAY['consumer','retail','ecommerce'], ARRAY['consumer'], 27, false),
('Real Estate & PropTech', 'sector-proptech', 'Commercial real estate, PropTech, REIT activity', '🏗', 'sector', ARRAY['proptech','realestate'], ARRAY['proptech'], 28, false),
('Industrial & Supply Chain', 'sector-industrial', 'Manufacturing, logistics, supply chain, industrial tech', '🏭', 'sector', ARRAY['industrial','manufacturing','logistics'], ARRAY['industrial'], 29, false),
('Professional Services', 'sector-prof-services', 'Consulting, legal, accounting, advisory firm news', '⚖', 'sector', ARRAY['consulting','legal','accounting'], ARRAY['professional_services'], 30, false)
ON CONFLICT (slug) DO NOTHING;

-- Region
INSERT INTO feed_bundles (name, slug, description, icon, bundle_type, tags, geographies, display_order, is_featured) VALUES
('North America', 'region-north-america', 'US and Canadian business news, deals, and company activity', '🇺🇸', 'region', ARRAY['north_america','usa','canada'], ARRAY['USA','CAN'], 40, true),
('Europe (EMEA)', 'region-emea', 'Pan-European deals, company news, regulatory developments', '🇪🇺', 'region', ARRAY['emea','europe','eu'], ARRAY['EUR','GBR','DEU','FRA'], 41, true),
('Asia Pacific (APAC)', 'region-apac', 'APAC deal flow, company events, and market signals', '🌏', 'region', ARRAY['apac','asia'], ARRAY['APAC','SGP','HKG','JPN','AUS','IND'], 42, true),
('Southeast Asia', 'region-sea', 'Singapore, Indonesia, Malaysia, Thailand, Vietnam', '🌴', 'region', ARRAY['sea','southeast_asia','asean'], ARRAY['SGP','IDN','MYS','THA','VNM'], 43, true),
('UK & Ireland', 'region-uk', 'British and Irish company news, deals, and financial markets', '🇬🇧', 'region', ARRAY['uk','ireland','london'], ARRAY['GBR','IRL'], 44, false),
('DACH', 'region-dach', 'Germany, Austria, Switzerland — Mittelstand and enterprise', '🇩🇪', 'region', ARRAY['dach','germany','austria'], ARRAY['DEU','AUT','CHE'], 45, false),
('Australia & New Zealand', 'region-anz', 'ANZ business, ASX-listed companies, and private markets', '🦘', 'region', ARRAY['anz','australia','new_zealand'], ARRAY['AUS','NZL'], 46, false),
('India', 'region-india', 'Indian startup ecosystem, enterprise, and public markets', '🇮🇳', 'region', ARRAY['india'], ARRAY['IND'], 47, false),
('Middle East & Africa', 'region-mea', 'Gulf states, Africa tech hubs, and regional deal flow', '🌍', 'region', ARRAY['mea','gulf','africa'], ARRAY['ARE','SAU','NGA','KEN'], 48, false),
('Latin America', 'region-latam', 'Brazil, Mexico, Colombia, and pan-LATAM company activity', '🌎', 'region', ARRAY['latam','brazil','mexico'], ARRAY['BRA','MEX','COL'], 49, false)
ON CONFLICT (slug) DO NOTHING;

-- Signal Type
INSERT INTO feed_bundles (name, slug, description, icon, bundle_type, tags, display_order, is_featured) VALUES
('M&A & Deal Flow', 'signal-ma', 'Mergers, acquisitions, divestitures, and strategic transactions', '🤝', 'signal_type', ARRAY['ma','mergers','acquisitions'], 50, true),
('Funding & Capital Events', 'signal-funding', 'Venture rounds, PE closes, debt raises, and IPOs', '💰', 'signal_type', ARRAY['funding','capital','rounds'], 51, true),
('Restructuring & Distress', 'signal-distress', 'Layoffs, restructuring, insolvency, and turnaround', '⚠', 'signal_type', ARRAY['restructuring','distress','layoffs'], 52, true),
('Executive Movements', 'signal-exec-moves', 'C-suite appointments, departures, board changes', '👤', 'signal_type', ARRAY['leadership','appointments','ceo','cfo'], 53, true),
('Regulatory & Compliance', 'signal-regulatory', 'Regulatory actions, fines, policy changes', '⚖', 'signal_type', ARRAY['regulatory','compliance','legal'], 54, false),
('Product & Market Launch', 'signal-launches', 'Product launches, market entries, commercial milestones', '🚀', 'signal_type', ARRAY['product','launch','expansion'], 55, false),
('Events & Conferences', 'signal-events', 'Industry conferences, investor days, earnings calls', '📅', 'signal_type', ARRAY['events','conferences','earnings'], 56, false)
ON CONFLICT (slug) DO NOTHING;

-- Company Stage
INSERT INTO feed_bundles (name, slug, description, icon, bundle_type, tags, display_order) VALUES
('Startup & Early Stage', 'stage-early', 'Pre-seed through Series A, founder moves', '🌱', 'stage', ARRAY['startup','early_stage','seed'], 60),
('Growth & Scale-up', 'stage-growth', 'Series B through pre-IPO, hypergrowth companies', '📈', 'stage', ARRAY['growth','scale','series_b'], 61),
('Public Markets', 'stage-public', 'Listed company earnings, SEC filings, activist positions', '🏛', 'stage', ARRAY['public','listed','nyse'], 62),
('Private Markets', 'stage-private', 'PE-backed, family-owned, and unlisted company news', '🔒', 'stage', ARRAY['private','pe_backed'], 63)
ON CONFLICT (slug) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SEED: CURATED SOURCES
-- ═══════════════════════════════════════════════════════════════════════════════

INSERT INTO feed_catalog (name, slug, tier, primary_category, url, tags, sectors, geographies, description, fetch_interval_min, quality_score) VALUES
-- VC / Funding
('TechCrunch', 'techcrunch', 'curated', 'funding', 'https://techcrunch.com/feed/', ARRAY['vc','startup','funding','tech'], ARRAY['saas','ai','fintech'], ARRAY['USA','GBR'], 'Broad US tech startup coverage', 30, 0.72),
('TechCrunch Fundings', 'techcrunch-fundings', 'curated', 'funding', 'https://techcrunch.com/category/fundings-exits/feed/', ARRAY['vc','funding','rounds'], ARRAY['saas','ai'], ARRAY['USA'], 'TC fundings only — higher signal-to-noise', 30, 0.88),
('Crunchbase News', 'crunchbase-news', 'curated', 'funding', 'https://news.crunchbase.com/feed/', ARRAY['vc','funding','startup'], ARRAY[]::TEXT[], ARRAY[]::TEXT[], 'Crunchbase editorial — funding and startup ecosystem', 30, 0.79),
('The Information', 'the-information', 'curated', 'news', 'https://www.theinformation.com/feed', ARRAY['tech','vc','enterprise'], ARRAY['saas','ai'], ARRAY['USA'], 'High-quality tech business journalism', 60, 0.85),
('Axios Pro Deals', 'axios-pro-deals', 'curated', 'funding', 'https://www.axios.com/feeds/feed.rss', ARRAY['vc','pe','deals'], ARRAY[]::TEXT[], ARRAY['USA'], 'Axios deals and finance coverage', 30, 0.76),
-- PE
('PE Hub', 'pe-hub', 'curated', 'ma', 'https://www.pehub.com/feed/', ARRAY['pe','buyout','ma','lbo'], ARRAY[]::TEXT[], ARRAY['USA'], 'PE Hub — buyout news and deal flow', 60, 0.80),
('Pitchbook News', 'pitchbook-news', 'curated', 'funding', 'https://pitchbook.com/news/rss', ARRAY['pe','vc','deals','ma'], ARRAY[]::TEXT[], ARRAY[]::TEXT[], 'PitchBook editorial — PE and VC news', 60, 0.82),
('Buyouts Insider', 'buyouts-insider', 'curated', 'ma', 'https://www.buyoutsinsider.com/feed/', ARRAY['pe','buyout','lbo'], ARRAY[]::TEXT[], ARRAY['USA'], 'PE buyout focus', 60, 0.78),
-- M&A
('Mergermarket', 'mergermarket', 'curated', 'ma', 'https://www.mergermarket.com/rss', ARRAY['ma','deals','global'], ARRAY[]::TEXT[], ARRAY[]::TEXT[], 'Mergermarket global M&A intelligence', 30, 0.90),
-- Distress
('Debtwire', 'debtwire', 'curated', 'distress', 'https://www.debtwire.com/rss', ARRAY['distress','debt','restructuring'], ARRAY[]::TEXT[], ARRAY[]::TEXT[], 'Distressed debt and restructuring intelligence', 60, 0.88),
('Reorg Research', 'reorg', 'curated', 'distress', 'https://reorg.com/feed/', ARRAY['restructuring','bankruptcy','distress'], ARRAY[]::TEXT[], ARRAY['USA'], 'Bankruptcy and restructuring coverage', 60, 0.87),
-- Europe
('Sifted', 'sifted', 'curated', 'news', 'https://sifted.eu/feed', ARRAY['eu','europe','startup','vc'], ARRAY['saas','fintech','cleantech'], ARRAY['EUR'], 'European startup and tech ecosystem', 60, 0.82),
('EU-Startups', 'eu-startups', 'curated', 'funding', 'https://www.eu-startups.com/feed/', ARRAY['eu','europe','startup','funding'], ARRAY[]::TEXT[], ARRAY['EUR'], 'EU startup funding and news', 60, 0.74),
('Tech.eu', 'tech-eu', 'curated', 'news', 'https://tech.eu/feed/', ARRAY['eu','europe','startup','tech'], ARRAY[]::TEXT[], ARRAY['EUR'], 'European tech news and deals', 60, 0.71),
('Dealroom News', 'dealroom', 'curated', 'funding', 'https://dealroom.co/blog/feed', ARRAY['eu','vc','funding','data'], ARRAY[]::TEXT[], ARRAY['EUR'], 'Dealroom European VC data and news', 120, 0.76),
-- APAC
('DealStreetAsia', 'dealstreetasia', 'curated', 'ma', 'https://www.dealstreetasia.com/feed/', ARRAY['apac','sea','ma','vc','pe'], ARRAY[]::TEXT[], ARRAY['APAC','SGP'], 'Best APAC deal flow source', 30, 0.88),
('Tech in Asia', 'tech-in-asia', 'curated', 'news', 'https://www.techinasia.com/feed', ARRAY['sea','asia','startup','tech'], ARRAY[]::TEXT[], ARRAY['APAC','SEA'], 'Southeast and South Asia tech news', 60, 0.76),
('e27', 'e27', 'curated', 'funding', 'https://e27.co/feed/', ARRAY['sea','singapore','startup','funding'], ARRAY[]::TEXT[], ARRAY['SGP','SEA'], 'Singapore-anchored SEA startup news', 60, 0.71),
('Business Times SG', 'business-times-sg', 'curated', 'news', 'https://www.businesstimes.com.sg/rss/all', ARRAY['singapore','apac','business'], ARRAY[]::TEXT[], ARRAY['SGP'], 'Singapore business news', 60, 0.73),
('Nikkei Asia', 'nikkei-asia', 'curated', 'news', 'https://asia.nikkei.com/rss/feed/nar', ARRAY['japan','apac','business'], ARRAY[]::TEXT[], ARRAY['JPN','APAC'], 'Authoritative Japan and broader APAC coverage', 60, 0.84),
('AVCJ', 'avcj', 'curated', 'funding', 'https://www.avcj.com/rss', ARRAY['apac','vc','pe','ma'], ARRAY[]::TEXT[], ARRAY['APAC'], 'Asian Venture Capital Journal — APAC PE/VC deals', 60, 0.85),
('Australian Financial Review', 'afr', 'curated', 'news', 'https://www.afr.com/rss', ARRAY['australia','anz','business','ma'], ARRAY[]::TEXT[], ARRAY['AUS'], 'AFR — authoritative Australian business', 60, 0.83),
-- Web3
('CoinDesk', 'coindesk', 'curated', 'sector', 'https://www.coindesk.com/arc/outboundfeeds/rss/', ARRAY['web3','crypto','defi'], ARRAY['web3'], ARRAY[]::TEXT[], 'CoinDesk — broad crypto and web3 coverage', 30, 0.74),
('The Block', 'the-block', 'curated', 'sector', 'https://www.theblock.co/rss.xml', ARRAY['web3','crypto','institutional'], ARRAY['web3'], ARRAY[]::TEXT[], 'Institutional crypto and web3 intelligence', 30, 0.82),
('CoinTelegraph', 'cointelegraph', 'curated', 'sector', 'https://cointelegraph.com/rss', ARRAY['web3','crypto'], ARRAY['web3'], ARRAY[]::TEXT[], 'Broad crypto news', 30, 0.68),
-- FinTech
('Finextra', 'finextra', 'curated', 'sector', 'https://www.finextra.com/rss/finextra-news.xml', ARRAY['fintech','banking','payments'], ARRAY['fintech'], ARRAY['EUR','GBR'], 'Financial technology news', 60, 0.80),
('Finovate Blog', 'finovate', 'curated', 'sector', 'https://finovate.com/feed/', ARRAY['fintech','banking','innovation'], ARRAY['fintech'], ARRAY[]::TEXT[], 'Fintech innovation and product launch', 120, 0.72),
('The Paypers', 'the-paypers', 'curated', 'sector', 'https://thepaypers.com/rss/', ARRAY['payments','fintech'], ARRAY['fintech'], ARRAY['EUR'], 'Payments industry news', 60, 0.75),
-- AdTech
('AdExchanger', 'adexchanger', 'curated', 'sector', 'https://www.adexchanger.com/feed/', ARRAY['adtech','programmatic','data'], ARRAY['adtech'], ARRAY['USA'], 'Programmatic and data-driven advertising', 60, 0.83),
('Digiday', 'digiday', 'curated', 'sector', 'https://digiday.com/feed/', ARRAY['adtech','media','digital'], ARRAY['adtech'], ARRAY['USA','GBR'], 'Digital media and advertising', 60, 0.76),
('The Drum', 'the-drum', 'curated', 'sector', 'https://www.thedrum.com/rss', ARRAY['adtech','martech','creative'], ARRAY['adtech'], ARRAY['GBR'], 'UK marketing and advertising', 60, 0.70),
-- HealthTech
('STAT News', 'stat-news', 'curated', 'sector', 'https://www.statnews.com/feed/', ARRAY['healthtech','biotech','pharma'], ARRAY['healthtech','biotech'], ARRAY['USA'], 'Health and life sciences journalism', 60, 0.84),
('MedCity News', 'medcity', 'curated', 'sector', 'https://medcitynews.com/feed/', ARRAY['healthtech','medtech','digital_health'], ARRAY['healthtech'], ARRAY['USA'], 'Healthcare innovation and startup news', 60, 0.76),
-- Exec Moves
('WSJ CFO Journal', 'wsj-cfo', 'curated', 'exec_moves', 'https://feeds.content.dowjones.io/public/rss/RSSWSJD', ARRAY['cfo','finance','exec_moves'], ARRAY[]::TEXT[], ARRAY['USA'], 'Finance leader appointments and trends', 120, 0.81),
-- India
('Economic Times India', 'economic-times', 'curated', 'news', 'https://economictimes.indiatimes.com/rssfeedsdefault.cms', ARRAY['india','business','corporate'], ARRAY[]::TEXT[], ARRAY['IND'], 'India business and corporate news', 30, 0.72),
('VCCircle India', 'vccircle', 'curated', 'funding', 'https://www.vccircle.com/rss', ARRAY['india','vc','pe','funding'], ARRAY[]::TEXT[], ARRAY['IND'], 'India PE and VC deal flow', 60, 0.80)
ON CONFLICT (slug) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- WIRE BUNDLES TO SOURCES
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION wire_bundle(p_bundle_slug TEXT, p_source_slugs TEXT[])
RETURNS void AS $$
DECLARE v_bundle_id UUID; v_source_id UUID; v_slug TEXT;
BEGIN
  SELECT id INTO v_bundle_id FROM feed_bundles WHERE slug = p_bundle_slug;
  IF v_bundle_id IS NULL THEN RETURN; END IF;
  FOREACH v_slug IN ARRAY p_source_slugs LOOP
    SELECT id INTO v_source_id FROM feed_catalog WHERE slug = v_slug;
    IF v_source_id IS NOT NULL THEN
      INSERT INTO feed_bundle_sources (bundle_id, source_id) VALUES (v_bundle_id, v_source_id) ON CONFLICT DO NOTHING;
    END IF;
  END LOOP;
END; $$ LANGUAGE plpgsql;

SELECT wire_bundle('vc-global', ARRAY['techcrunch-fundings','crunchbase-news','the-information','axios-pro-deals','pitchbook-news']);
SELECT wire_bundle('pe-global', ARRAY['pe-hub','pitchbook-news','buyouts-insider','mergermarket']);
SELECT wire_bundle('signal-ma', ARRAY['mergermarket','globenewswire','pe-hub','dealstreetasia','avcj']);
SELECT wire_bundle('signal-funding', ARRAY['techcrunch-fundings','crunchbase-news','pitchbook-news','dealstreetasia','vccircle']);
SELECT wire_bundle('signal-distress', ARRAY['debtwire','reorg']);
SELECT wire_bundle('signal-exec-moves', ARRAY['wsj-cfo','pr-newswire','business-wire']);
SELECT wire_bundle('region-emea', ARRAY['sifted','eu-startups','tech-eu','dealroom','finextra','the-drum']);
SELECT wire_bundle('region-apac', ARRAY['dealstreetasia','tech-in-asia','business-times-sg','nikkei-asia','avcj','afr','economic-times']);
SELECT wire_bundle('region-sea', ARRAY['dealstreetasia','tech-in-asia','e27','business-times-sg']);
SELECT wire_bundle('region-anz', ARRAY['afr']);
SELECT wire_bundle('region-india', ARRAY['economic-times','vccircle']);
SELECT wire_bundle('region-uk', ARRAY['the-drum','digiday','finextra']);
SELECT wire_bundle('sector-web3', ARRAY['coindesk','the-block','cointelegraph']);
SELECT wire_bundle('sector-fintech', ARRAY['finextra','finovate','the-paypers','techcrunch']);
SELECT wire_bundle('sector-adtech', ARRAY['adexchanger','digiday','the-drum']);
SELECT wire_bundle('sector-healthtech', ARRAY['stat-news','medcity']);
SELECT wire_bundle('sector-saas-ai', ARRAY['techcrunch','the-information','crunchbase-news']);
SELECT wire_bundle('growth-equity', ARRAY['pitchbook-news','crunchbase-news','techcrunch-fundings']);
SELECT wire_bundle('stage-public', ARRAY['sec-edgar-8k','wsj-cfo']);
SELECT wire_bundle('stage-early', ARRAY['techcrunch-fundings','crunchbase-news','e27','eu-startups','vccircle']);
SELECT wire_bundle('region-north-america', ARRAY['techcrunch','axios-pro-deals','pe-hub','buyouts-insider']);

-- Update source counts on bundles
UPDATE feed_bundles fb SET source_count = (SELECT COUNT(*) FROM feed_bundle_sources WHERE bundle_id = fb.id);

DROP FUNCTION IF EXISTS wire_bundle(TEXT, TEXT[]);
