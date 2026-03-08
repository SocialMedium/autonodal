# MitchelLake Harvester Consolidation — Setup Guide

## What Was Built

Three consolidated harvesters adapted from ResearchMedium's 16 files → your PostgreSQL schema:

| Script | Source Files Consolidated | Writes To | Schedule |
|--------|--------------------------|-----------|----------|
| `harvest_news_pr.js` | harvest_news.js, harvest.js | `external_documents`, `signal_events`, `document_companies` | Every 30 min |
| `harvest_podcasts.js` | harvest_podcasts.js, backfill_podcasts.js, extract_podcast_meta.js, add_*_podcasts.js, fix_*_podcasts.js | `person_content_sources`, `person_content`, `signal_events`, `person_signals` | Daily |
| `harvest_corporate.js` | harvest_corporate.js, harvest_bulk.js, harvest_corporate.js | `signal_events`, `companies` | Weekly |

### What was NOT ported (and why)
- `harvest.js` (OAI-PMH) — Academic papers, not relevant to exec search
- `cluster_signals.js` — Concept is good but needs full rewrite for PG + your scoring engine
- `harvest_events.js` — Seed data useful later, events aren't core to candidate intelligence yet
- `harvest_overnight.sh` — Was a batch runner, replaced by scheduler integration

## Setup

### 1. Copy scripts to your project
```bash
cp harvest_news_pr.js   ~/Downloads/mitchellake-signals/scripts/
cp harvest_podcasts.js   ~/Downloads/mitchellake-signals/scripts/
cp harvest_corporate.js  ~/Downloads/mitchellake-signals/scripts/
```

### 2. Install dependencies (if not already)
```bash
cd ~/Downloads/mitchellake-signals
npm install xml2js rss-parser  # Only if not already installed
```

### 3. Ensure schema columns exist

The scripts use existing tables but may need these columns if missing:

```sql
-- rss_sources: may need source_type, region, error_count, last_error
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS source_type VARCHAR(30) DEFAULT 'rss';
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS region VARCHAR(30) DEFAULT 'global';
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS error_count INTEGER DEFAULT 0;
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS last_error TEXT;

-- person_content_sources: may need source_url unique constraint
-- (check first: SELECT indexname FROM pg_indexes WHERE tablename = 'person_content_sources';)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pcs_source_url ON person_content_sources(source_url);

-- person_content: content_hash for dedup
ALTER TABLE person_content ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pc_content_hash ON person_content(content_hash);

-- signal_events: may need signal_category, metadata
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS signal_category VARCHAR(30);
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS evidence TEXT;
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS source_type VARCHAR(30);
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS confidence NUMERIC(3,2);
ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ;

-- companies: unique on name for upsert
CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_name ON companies(name);
```

### 4. Seed sources
```bash
# Seed news/PR feeds into rss_sources
node scripts/harvest_news_pr.js --seed

# Seed podcast feeds into person_content_sources
node scripts/harvest_podcasts.js --seed
```

### 5. First run
```bash
# News/PR — should pick up items immediately
node scripts/harvest_news_pr.js

# Podcasts — backfill 24 months of episodes
node scripts/harvest_podcasts.js --backfill

# After backfill, run person detection
node scripts/harvest_podcasts.js --detect

# Corporate — start with a few key companies
node scripts/harvest_corporate.js --watchlist tech_majors
```

### 6. Scheduler integration

Add to your existing `scheduler-production.js` pipelines:

```javascript
// ── In your pipeline definitions, add: ──

// News/PR harvesting (enhanced) — runs with existing Ingest Signals
// Your existing RSS harvester already covers some sources.
// The new one adds 13+ PR newswire and deal-specific feeds.
// Option A: Replace existing harvest call with new one
// Option B: Run both (new one only inserts on unique url_hash)

{
  name: 'Harvest News/PR',
  schedule: '*/30 * * * *',  // Every 30 min (same as existing)
  script: 'scripts/harvest_news_pr.js',
  args: [],
},

{
  name: 'Harvest Podcasts',
  schedule: '0 6 * * *',  // Daily at 6am
  script: 'scripts/harvest_podcasts.js',
  args: ['--poll'],
},

{
  name: 'Podcast Person Detection',
  schedule: '0 7 * * *',  // Daily at 7am (after podcast poll)
  script: 'scripts/harvest_podcasts.js',
  args: ['--detect'],
},

{
  name: 'Corporate SEC Harvest',
  schedule: '0 2 * * 1',  // Weekly Monday 2am
  script: 'scripts/harvest_corporate.js',
  args: ['--all'],
},
```

## Signal Flow

```
News/PR RSS → external_documents → signal_events → score computation
                                                  ↓
Podcasts RSS → person_content → signal_events → person_signals (mentions)
                                                  ↓
SEC Filings → signal_events → companies (enrichment)
                                                  ↓
                                    ← All feed into scoring engine →
                                    timing_score, flight_risk, engagement
```

## Coverage

### News/PR Sources (13 feeds)
- GlobeNewswire: M&A, Contracts, Public Companies
- PR Newswire: Technology, Financial Services
- Business Wire: Technology
- TechCrunch
- Regional: Startup Daily AU, SmartCompany AU, Tech in Asia, e27, Sifted EU, UKTN

### Podcast Sources (33 feeds)
- VC: Equity, TWIST, BG2Pod, Invest Like the Best, How I Built This
- Tech/AI: Techmeme, No Priors, Eye on AI, Practical AI
- Fintech: Fintech Takes, Bankless
- Climate: My Climate Journey, Catalyst
- UK/EU: Riding Unicorns, Sifted, EU Startups, Seedcamp
- ANZ: Wild Hearts (Blackbird), AirTree, Startup Daily, Cut Through Venture
- Asia: BRAVE SEA, Analyse Asia, Hard Truths (Vertex)
- Web3: Unchained, Empire

### Corporate SEC (70+ tickers across 8 watchlists)
- tech_majors, ai_leaders, fintech, cybersecurity
- healthcare, cleantech, semiconductor, anz_listed

## Signal Types Detected

### From News/PR
capital_raising, geographic_expansion, strategic_hiring, ma_activity,
partnership, product_launch, layoffs, leadership_departure,
board_appointment, ipo_listing, pe_buyout

### From Podcasts
capital_raising, ma_activity, leadership, expansion, layoffs
+ podcast_mention (person-level visibility signal)

### From SEC Filings
ma_intent, growth, investment, partnership, product,
expansion, restructuring, exec_change

## Estimated Volume

| Source | Items/Day | Signals/Day | Notes |
|--------|-----------|-------------|-------|
| News/PR | 100-300 | 30-80 | ~30% have hiring-relevant signals |
| Podcasts | 15-40 | 5-15 | Daily poll, weekly backfill |
| SEC | 5-20 (weekly) | 10-50 | 70 companies × 6 filings |
| **Total** | **~150-350** | **~45-145** | |

## Cost

- News/PR: Free (RSS)
- Podcasts: Free (RSS)
- SEC: Free (EDGAR API)
- Total incremental cost: **$0/month**
- (Embedding these docs uses your existing OpenAI budget: ~$2-5/month additional)
