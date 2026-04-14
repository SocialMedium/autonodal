-- ═══════════════════════════════════════════════════════════════════════════════
-- MIGRATION: RSS Sources — geographic metadata + expansion seed
-- Adds category, regions, signal_priority, notes to rss_sources
-- Then inserts 44 new feeds covering LatAm, Canada, Nordics, Europe, Asia,
-- Cleantech, PropTech, LegalTech, AdTech, GovTech
-- Run: node scripts/run_migration.js sql/migration_rss_geo_expansion.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── SCHEMA ADDITIONS ────────────────────────────────────────────────────────

ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS category       TEXT;
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS regions        TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS signal_priority TEXT DEFAULT 'medium';
ALTER TABLE rss_sources ADD COLUMN IF NOT EXISTS notes          TEXT;

-- ── SEED: Geographic + Thematic Expansion ───────────────────────────────────

INSERT INTO rss_sources (
  name, url, source_type, category,
  regions, enabled, poll_interval_minutes,
  signal_priority, notes
)
SELECT name, url, source_type, category,
       regions, enabled, poll_interval_minutes,
       signal_priority, notes
FROM (VALUES

  -- ═══════════════════════════════════════════════════════════════════
  -- SOUTH AMERICA
  -- ═══════════════════════════════════════════════════════════════════

  ( 'Contxto',
    'https://contxto.com/en/feed/',
    'news', 'news_latam',
    ARRAY['latam','brazil','mexico','colombia','chile'],
    true, 480, 'high',
    'Primary English-language LatAm startup/VC source. Funding rounds, founding signals, expansion announcements.' ),

  ( 'LatamList',
    'https://latamlist.com/feed/',
    'news', 'news_latam',
    ARRAY['latam','brazil','mexico','argentina','colombia'],
    true, 480, 'high',
    'Startup funding rounds, M&A, company launches across LatAm. Clean structured data per article.' ),

  ( 'The LatAm Investor',
    'https://www.thelataminvestor.com/feed',
    'news', 'news_latam',
    ARRAY['latam','global'],
    true, 720, 'high',
    'Fund activity, LP/GP signals, cross-border deal intelligence. VC/PE focused.' ),

  ( 'Valor Econômico (BR)',
    'https://valor.globo.com/rss/home/index.ghtml',
    'news', 'news_latam',
    ARRAY['brazil'],
    true, 240, 'medium',
    'Brazilian business intelligence. Largest LatAm economy. M&A, leadership, capital markets signals.' ),

  ( 'Startups.com.br',
    'https://startups.com.br/feed/',
    'news', 'news_latam',
    ARRAY['brazil'],
    true, 480, 'medium',
    'Brazilian startup ecosystem. Funding rounds, product launches, founder signals.' ),

  ( 'NotiLatam',
    'https://notilatam.com/feed/',
    'news', 'news_latam',
    ARRAY['latam'],
    true, 720, 'low',
    'Regional tech and business news. Broad LatAm coverage for secondary signal enrichment.' ),

  ( 'Contxto Podcast',
    'https://feeds.buzzsprout.com/1574596.rss',
    'podcast', 'news_latam',
    ARRAY['latam'],
    true, 720, 'medium',
    'LatAm tech founder and investor interviews. Buzzsprout hosted — strong episode metadata.' ),

  -- ═══════════════════════════════════════════════════════════════════
  -- CANADA
  -- ═══════════════════════════════════════════════════════════════════

  ( 'BetaKit',
    'https://betakit.com/feed/',
    'news', 'news_canada',
    ARRAY['canada'],
    true, 240, 'high',
    'Primary Canadian startup/VC source. Funding, leadership moves, M&A. Toronto/Vancouver/Montreal ecosystems.' ),

  ( 'The Logic',
    'https://thelogic.co/feed/',
    'news', 'news_canada',
    ARRAY['canada'],
    true, 480, 'medium',
    'Investigative Canadian tech journalism. Corporate and strategic signals. Partially paywalled — headlines still signal.' ),

  ( 'Globe and Mail Business',
    'https://www.theglobeandmail.com/rss/article/section/business/',
    'news', 'news_canada',
    ARRAY['canada'],
    true, 240, 'high',
    'National business intelligence. M&A, leadership, capital markets. Primary Canadian broadsheet.' ),

  ( 'MaRS Discovery District',
    'https://www.marsdd.com/feed/',
    'news', 'news_canada',
    ARRAY['canada'],
    true, 720, 'medium',
    'Toronto innovation ecosystem. Company launches, funding, government tech programmes.' ),

  ( 'Vancouver Tech Journal',
    'https://www.vantechjournal.com/feed',
    'news', 'news_canada',
    ARRAY['canada'],
    true, 720, 'medium',
    'BC tech ecosystem. Vancouver/Victoria startup signals. Clean energy, AI, biotech.' ),

  ( 'BetaKit Podcast',
    'https://feeds.buzzsprout.com/1562786.rss',
    'podcast', 'news_canada',
    ARRAY['canada'],
    true, 480, 'high',
    'Canadian founder and VC interviews. Buzzsprout hosted. Complements BetaKit news feed with deeper signals.' ),

  -- ═══════════════════════════════════════════════════════════════════
  -- NORDICS
  -- ═══════════════════════════════════════════════════════════════════

  ( 'Breakit (Sweden)',
    'https://www.breakit.se/feed/articles',
    'news', 'news_nordics',
    ARRAY['sweden','nordics','europe'],
    true, 240, 'high',
    'Primary Swedish tech/startup source. Funding, leadership, M&A. Klarna, Spotify, iZettle ecosystem signals.' ),

  ( 'Shifter (Norway)',
    'https://shifter.no/feed/',
    'news', 'news_nordics',
    ARRAY['norway','nordics'],
    true, 480, 'medium',
    'Norwegian startup and VC signals. Schibsted, Autostore, NEL ecosystem. Energy tech strong.' ),

  ( 'The Nordic Web',
    'https://thenordicweb.com/feed/',
    'news', 'news_nordics',
    ARRAY['nordics','europe'],
    true, 480, 'high',
    'Pan-Nordic startup funding, M&A tracking. Strong signal density. Northzone, EQT Ventures, Creandum portfolio.' ),

  ( 'Silicon Canals',
    'https://siliconcanals.com/feed/',
    'news', 'news_nordics',
    ARRAY['netherlands','nordics','europe'],
    true, 360, 'high',
    'Benelux + Nordic tech intelligence. Amsterdam fintech and SaaS hub. Biweekly round-ups of funding.' ),

  ( 'Di Digital (Sweden)',
    'https://digital.di.se/rss',
    'news', 'news_nordics',
    ARRAY['sweden','nordics'],
    true, 480, 'medium',
    'Dagens Industri digital section. Swedish business/tech signals. Financial market intelligence.' ),

  ( 'Slush Blog',
    'https://www.slush.org/feed/',
    'news', 'news_nordics',
    ARRAY['finland','nordics','europe'],
    true, 1440, 'low',
    'Helsinki ecosystem. Event-linked signals. Slush conference is primary signal window for Nordic/EU VC activity.' ),

  -- ═══════════════════════════════════════════════════════════════════
  -- EUROPE BROADER
  -- ═══════════════════════════════════════════════════════════════════

  ( 'EU-Startups',
    'https://www.eu-startups.com/feed/',
    'news', 'news_europe',
    ARRAY['europe'],
    true, 240, 'high',
    'Pan-European funding rounds, M&A, leadership. Best single feed for continental EU startup signal coverage.' ),

  ( 'Dealroom News',
    'https://news.dealroom.co/feed',
    'news', 'news_europe',
    ARRAY['europe','global'],
    true, 240, 'high',
    'European deal flow and fund announcements. Strong signal density. Structured funding data. Must-have for EU.' ),

  ( 'Gründerszene (Germany)',
    'https://www.gruenderszene.de/feed',
    'news', 'news_europe',
    ARRAY['germany','europe'],
    true, 360, 'high',
    'German startup ecosystem. Rocket Internet heritage. Strong deal and leadership signals for DE market.' ),

  ( 'The Recursive (CEE)',
    'https://therecursive.com/feed/',
    'news', 'news_europe',
    ARRAY['poland','romania','czechia','bulgaria','europe'],
    true, 480, 'high',
    'CEE tech ecosystem. Fastest-growing EU VC market. Poland, Romania, Czechia — underreported but active.' ),

  ( 'Finsider Italy',
    'https://finsider.it/feed/',
    'news', 'news_europe',
    ARRAY['italy','europe'],
    true, 720, 'medium',
    'Italian fintech and startup signals. Milan-Torino tech corridor. Scaling fintech activity.' ),

  ( 'EU-Startups Podcast',
    'https://feeds.buzzsprout.com/1532044.rss',
    'podcast', 'news_europe',
    ARRAY['europe'],
    true, 480, 'high',
    'Founder interviews, funding rounds, European market signals. Complements EU-Startups news feed.' ),

  ( 'Maddyness UK',
    'https://www.maddyness.com/uk/feed/',
    'news', 'news_europe',
    ARRAY['uk','france','europe'],
    true, 480, 'medium',
    'French and UK startup signals. Cross-Channel deals and founder movement.' ),

  ( 'Charge VC (DE)',
    'https://www.charge.vc/feed',
    'news', 'news_europe',
    ARRAY['germany','europe'],
    true, 1440, 'low',
    'German VC perspectives. Portfolio signals, investment thesis signals. Complements Gründerszene.' ),

  -- ═══════════════════════════════════════════════════════════════════
  -- ASIA BROADER
  -- ═══════════════════════════════════════════════════════════════════

  ( 'Pandaily (China/Asia tech)',
    'https://pandaily.com/feed/',
    'news', 'news_asia',
    ARRAY['china','asia','global'],
    true, 360, 'high',
    'English-language China tech signals. Funding, expansion, M&A. Fills gap left by SCMP being paywalled.' ),

  ( '36Kr Global',
    'https://36kr.com/feed',
    'news', 'news_asia',
    ARRAY['china','asia'],
    true, 480, 'high',
    'Chinese startup intelligence. International expansion signals. Best coverage of Chinese company moves.' ),

  ( 'Bridge (Japan)',
    'https://thebridge.jp/en/feed',
    'news', 'news_asia',
    ARRAY['japan','asia'],
    true, 480, 'high',
    'Japanese startup English coverage. SoftBank, Recruit Holdings, Sony Ventures ecosystem signals.' ),

  ( 'KoreaTechDesk',
    'https://www.koreatech.io/feed',
    'news', 'news_asia',
    ARRAY['korea','asia'],
    true, 720, 'medium',
    'Korean startup/VC. Kakao, Coupang, Krafton ecosystem signals. K-beauty and K-content tech.' ),

  ( 'Platum (Korea)',
    'https://platum.kr/feed',
    'news', 'news_asia',
    ARRAY['korea','asia'],
    true, 720, 'medium',
    'Korean startup funding and leadership moves. Complements KoreaTechDesk for market breadth.' ),

  ( 'HKEX News (Hong Kong Exchange)',
    'https://www.hkexnews.hk/listedco/listconews/rss/hkex_rss.xml',
    'filing', 'filings_hk',
    ARRAY['hong_kong','china','asia'],
    true, 60, 'high',
    'HKEX listed company announcements. Director changes, capital raises, M&A — same value as ASX feed for HK listed companies.' ),

  ( 'SGX Company Announcements (Singapore Exchange)',
    'https://links.sgx.com/1.0.0/corporate-announcements/rss',
    'filing', 'filings_sg',
    ARRAY['singapore','sea'],
    true, 60, 'high',
    'SGX-listed company material announcements. Mandatory disclosure. Director appointments, capital actions, M&A completions.' ),

  ( 'Asia Tech Podcast',
    'https://feeds.buzzsprout.com/1082850.rss',
    'podcast', 'news_asia',
    ARRAY['sea','asia','singapore'],
    true, 480, 'high',
    'Southeast and broader Asia founder/investor interviews. Weekly. Good for SEA startup ecosystem signals.' ),

  ( 'The Ken (India/SEA)',
    'https://the-ken.com/feed/',
    'news', 'news_asia',
    ARRAY['india','sea'],
    true, 480, 'high',
    'Strong India/SEA signals. Investigative tech journalism.' ),

  -- ═══════════════════════════════════════════════════════════════════
  -- THEMATIC VERTICALS
  -- ═══════════════════════════════════════════════════════════════════

  -- Cleantech / Climate
  ( 'GreenBiz',
    'https://www.greenbiz.com/feeds/news',
    'news', 'cleantech',
    ARRAY['global','north_america'],
    true, 360, 'high',
    'Climate/sustainability business intelligence. Corporate sustainability leadership, investment signals, policy.' ),

  ( 'CleanTechnica',
    'https://cleantechnica.com/feed/',
    'news', 'cleantech',
    ARRAY['global'],
    true, 360, 'high',
    'Daily. Clean energy, EV, solar investment signals. Company launches, funding, expansion. High volume.' ),

  ( 'CTVC (Climate Tech VC)',
    'https://www.ctvc.co/feed',
    'news', 'cleantech',
    ARRAY['global','north_america'],
    true, 720, 'high',
    'Climate tech VC deal intelligence. Weekly. Fund launches, portfolio signals, investor movement.' ),

  -- PropTech
  ( 'CRETech',
    'https://www.cretech.com/feed/',
    'news', 'proptech',
    ARRAY['global','north_america'],
    true, 720, 'medium',
    'Commercial real estate tech. Proptech funding, M&A, product launches. JLL, CBRE ecosystem signals.' ),

  ( 'PropTech Insider (EU)',
    'https://proptechinsider.eu/feed/',
    'news', 'proptech',
    ARRAY['europe'],
    true, 720, 'medium',
    'European proptech signals. Venture-backed property tech companies, expansion signals.' ),

  -- Legal Tech
  ( 'Artificial Lawyer',
    'https://www.artificiallawyer.com/feed/',
    'news', 'legaltech',
    ARRAY['global','uk','europe'],
    true, 720, 'medium',
    'AI in legal practice. Law firm tech adoption, vendor landscape, M&A. Relevant for consulting vertical.' ),

  -- Brand/Advertising
  ( 'WARC',
    'https://www.warc.com/NewsAndOpinion/Feed',
    'news', 'adtech',
    ARRAY['global'],
    true, 480, 'high',
    'Advertising effectiveness intelligence. Brand investment signals, campaign intelligence, media market moves.' ),

  ( 'The Drum',
    'https://www.thedrum.com/rss',
    'news', 'adtech',
    ARRAY['global','uk'],
    true, 360, 'high',
    'Daily. Brand/agency news, CMO leadership moves, adtech deals. UK-anchored but global coverage.' ),

  ( 'Campaign (UK)',
    'https://www.campaignlive.co.uk/rss',
    'news', 'adtech',
    ARRAY['uk','europe'],
    true, 360, 'high',
    'Agency and brand intelligence. Pitch wins/losses (= BD signals), leadership moves, network restructuring.' ),

  -- Defense / GovTech
  ( 'GovTech',
    'https://www.govtech.com/rss/news',
    'news', 'govtech',
    ARRAY['north_america'],
    true, 720, 'medium',
    'Government technology procurement, digital transformation. Contract awards = BD signals for GovTech vendors.' ),

  ( 'Apolitical',
    'https://apolitical.co/feed',
    'news', 'govtech',
    ARRAY['global'],
    true, 1440, 'low',
    'Global public sector innovation. Policy, digital government, smart cities. Low frequency but quality signals.' )

) AS src(name, url, source_type, category, regions, enabled,
         poll_interval_minutes, signal_priority, notes)
WHERE NOT EXISTS (
  SELECT 1 FROM rss_sources r WHERE r.url = src.url
);

-- ── VERIFY ──────────────────────────────────────────────────────────────────

-- SELECT category, COUNT(*) AS new_feeds,
--        STRING_AGG(name, ', ' ORDER BY name) AS feeds
-- FROM rss_sources
-- WHERE created_at > NOW() - INTERVAL '5 minutes'
-- GROUP BY category ORDER BY new_feeds DESC;
