-- Add Finextra topic-specific RSS feeds to rss_sources (active harvester)
-- Finextra covers fintech, banking, payments, capital markets across EUR/AMER/ASIA

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Finextra Headlines', 'https://www.finextra.com/rss/finextra-news.xml', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra AI', 'https://www.finextra.com/rss/channel.aspx?m=ai', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Cloud', 'https://www.finextra.com/rss/channel.aspx?m=cloud', 'news', true, ARRAY['EUR','AMER']),
  ('Finextra Payments', 'https://www.finextra.com/rss/channel.aspx?m=payments', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Markets', 'https://www.finextra.com/rss/channel.aspx?m=markets', 'news', true, ARRAY['EUR','AMER']),
  ('Finextra Retail Banking', 'https://www.finextra.com/rss/channel.aspx?m=retail', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Wholesale', 'https://www.finextra.com/rss/channel.aspx?m=wholesale', 'news', true, ARRAY['EUR']),
  ('Finextra Wealth', 'https://www.finextra.com/rss/channel.aspx?m=wealth', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Regulation', 'https://www.finextra.com/rss/channel.aspx?m=regulation', 'news', true, ARRAY['EUR','AMER']),
  ('Finextra Crime', 'https://www.finextra.com/rss/channel.aspx?m=crime', 'news', true, ARRAY['EUR','AMER']),
  ('Finextra Crypto', 'https://www.finextra.com/rss/channel.aspx?m=crypto', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Sustainable', 'https://www.finextra.com/rss/channel.aspx?m=sustainable', 'news', true, ARRAY['EUR']),
  ('Finextra Startups', 'https://www.finextra.com/rss/channel.aspx?m=startups', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Finextra Identity', 'https://www.finextra.com/rss/channel.aspx?m=identity', 'news', true, ARRAY['EUR','AMER']),
  ('Finextra Security', 'https://www.finextra.com/rss/channel.aspx?m=security', 'news', true, ARRAY['EUR','AMER'])
ON CONFLICT (url) DO NOTHING;

-- Also add to feed_inventory catalog so tenants can discover/subscribe
INSERT INTO feed_inventory (slug, name, source_type, feed_type, url, sectors, verticals, geographies, description, quality_score, is_active)
VALUES
  ('finextra-ai', 'Finextra AI', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=ai', ARRAY['fintech','ai'], ARRAY['fintech'], ARRAY['EUR','AMER','ASIA'], 'AI in financial services', 60, 0.80),
  ('finextra-payments', 'Finextra Payments', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=payments', ARRAY['fintech','payments'], ARRAY['fintech'], ARRAY['EUR','AMER','ASIA'], 'Global payments innovation', 60, 0.80),
  ('finextra-markets', 'Finextra Markets', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=markets', ARRAY['fintech','capital_markets'], ARRAY['fintech'], ARRAY['EUR','AMER'], 'Capital markets and trading tech', 60, 0.75),
  ('finextra-crypto', 'Finextra Crypto', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=crypto', ARRAY['fintech','crypto'], ARRAY['fintech'], ARRAY['EUR','AMER','ASIA'], 'Digital currencies and crypto assets', 50, 0.70),
  ('finextra-startups', 'Finextra Startups', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=startups', ARRAY['fintech','startups','vc'], ARRAY['fintech'], ARRAY['EUR','AMER','ASIA'], 'Fintech startups, VCs, and funding', 60, 0.85),
  ('finextra-regulation', 'Finextra Regulation', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=regulation', ARRAY['fintech','regulation'], ARRAY['fintech'], ARRAY['EUR','AMER'], 'Financial regulation and compliance', 50, 0.70),
  ('finextra-wealth', 'Finextra Wealth', 'curated', 'sector', 'https://www.finextra.com/rss/channel.aspx?m=wealth', ARRAY['fintech','wealth'], ARRAY['fintech'], ARRAY['EUR','AMER','ASIA'], 'Wealth management and robo-advisory', 50, 0.75)
ON CONFLICT (slug) DO NOTHING;
