-- Signal Index — Market Health Ticker
-- Tables for signal stocks, composite index, sector indices, history

CREATE TABLE IF NOT EXISTS signal_stocks (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     UUID,
  stock_name    VARCHAR(100) NOT NULL,
  sentiment     VARCHAR(10) NOT NULL CHECK (sentiment IN ('bullish', 'bearish', 'neutral')),
  weight        FLOAT NOT NULL DEFAULT 1.0,
  horizon       VARCHAR(10) NOT NULL CHECK (horizon IN ('7d', '30d', '90d')),
  current_count INT DEFAULT 0,
  prior_count   INT DEFAULT 0,
  delta         FLOAT NOT NULL DEFAULT 0,
  direction     VARCHAR(10) NOT NULL CHECK (direction IN ('up', 'down', 'flat')),
  score         FLOAT NOT NULL DEFAULT 50,
  computed_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id, stock_name, horizon)
);

CREATE TABLE IF NOT EXISTS market_health_index (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id           UUID,
  horizon             VARCHAR(10) NOT NULL CHECK (horizon IN ('7d', '30d', '90d')),
  score               FLOAT NOT NULL,
  delta               FLOAT NOT NULL DEFAULT 0,
  direction           VARCHAR(10) NOT NULL CHECK (direction IN ('up', 'down', 'flat')),
  bullish_count       INT DEFAULT 0,
  bearish_count       INT DEFAULT 0,
  dominant_signal     VARCHAR(100),
  computed_at         TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id, horizon)
);

CREATE TABLE IF NOT EXISTS sector_indices (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id       UUID,
  sector          VARCHAR(100) NOT NULL,
  horizon         VARCHAR(10) NOT NULL CHECK (horizon IN ('7d', '30d', '90d')),
  score           FLOAT NOT NULL DEFAULT 50,
  delta           FLOAT NOT NULL DEFAULT 0,
  direction       VARCHAR(10) NOT NULL CHECK (direction IN ('up', 'down', 'flat')),
  signal_count    INT DEFAULT 0,
  company_count   INT DEFAULT 0,
  computed_at     TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(tenant_id, sector, horizon)
);

CREATE TABLE IF NOT EXISTS market_health_history (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   UUID,
  horizon     VARCHAR(10) NOT NULL,
  score       FLOAT NOT NULL,
  delta       FLOAT NOT NULL DEFAULT 0,
  snapshot_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_index_stats (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id             UUID UNIQUE,
  people_tracked        INT DEFAULT 0,
  companies_tracked     INT DEFAULT 0,
  signals_7d            INT DEFAULT 0,
  signals_30d           INT DEFAULT 0,
  computed_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signal_stocks_tenant ON signal_stocks(tenant_id, horizon);
CREATE INDEX IF NOT EXISTS idx_market_health_tenant ON market_health_index(tenant_id, horizon);
CREATE INDEX IF NOT EXISTS idx_sector_indices_tenant ON sector_indices(tenant_id, sector, horizon);
CREATE INDEX IF NOT EXISTS idx_market_health_history ON market_health_history(tenant_id, horizon, snapshot_at);
