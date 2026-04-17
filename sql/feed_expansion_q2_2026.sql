-- ═══════════════════════════════════════════════════════════════════════════════
-- Feed Expansion Q2 2026 — Broaden coverage for multi-tenant SaaS
-- Fills gaps: MENA, consumer, industrial, deep tech, gov tech, exec moves
-- Run: psql $DATABASE_URL -f sql/feed_expansion_q2_2026.sql
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── MIDDLE EAST & AFRICA (currently ~0 feeds) ──────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Wamda', 'https://www.wamda.com/feed', 'news', true, ARRAY['MENA']),
  ('Disrupt Africa', 'https://disrupt-africa.com/feed/', 'news', true, ARRAY['MENA']),
  ('Magnitt', 'https://magnitt.com/feed', 'news', true, ARRAY['MENA']),
  ('TechCabal', 'https://techcabal.com/feed/', 'news', true, ARRAY['MENA']),
  ('Zawya', 'https://www.zawya.com/en/rss.xml', 'news', true, ARRAY['MENA']),
  ('Arabian Business', 'https://www.arabianbusiness.com/rss', 'news', true, ARRAY['MENA']),
  ('Gulf Business', 'https://gulfbusiness.com/feed/', 'news', true, ARRAY['MENA']),
  ('The National UAE', 'https://www.thenationalnews.com/rss/business', 'news', true, ARRAY['MENA']),
  ('Africa Business Insider', 'https://africa.businessinsider.com/rss', 'news', true, ARRAY['MENA']),
  ('Ventureburn', 'https://ventureburn.com/feed/', 'news', true, ARRAY['MENA']),
  ('IT News Africa', 'https://www.itnewsafrica.com/feed/', 'news', true, ARRAY['MENA'])
ON CONFLICT (url) DO NOTHING;

-- ─── CONSUMER, RETAIL & CPG ─────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Retail Dive', 'https://www.retaildive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Modern Retail', 'https://www.modernretail.co/feed/', 'news', true, ARRAY['AMER','EUR']),
  ('Retail Week', 'https://www.retailweek.com/rss', 'news', true, ARRAY['EUR']),
  ('Grocery Dive', 'https://www.grocerydive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('eMarketer Retail', 'https://www.insiderintelligence.com/rss/retail', 'news', true, ARRAY['AMER','EUR']),
  ('Vogue Business', 'https://www.voguebusiness.com/rss', 'news', true, ARRAY['EUR','AMER']),
  ('Business of Fashion', 'https://www.businessoffashion.com/rss', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('Food Navigator', 'https://www.foodnavigator.com/rss/news', 'news', true, ARRAY['EUR','AMER']),
  ('Cosmetics Design', 'https://www.cosmeticsdesign.com/rss/news', 'news', true, ARRAY['EUR','AMER','ASIA']),
  ('RetailTechNews', 'https://retailtechnews.com/feed/', 'news', true, ARRAY['EUR','AMER'])
ON CONFLICT (url) DO NOTHING;

-- ─── INDUSTRIAL, SUPPLY CHAIN & LOGISTICS ───────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Supply Chain Dive', 'https://www.supplychaindive.com/feeds/news/', 'news', true, ARRAY['AMER','EUR']),
  ('Logistics Manager', 'https://www.logisticsmanager.com/feed/', 'news', true, ARRAY['EUR']),
  ('FreightWaves', 'https://www.freightwaves.com/feed', 'news', true, ARRAY['AMER']),
  ('Manufacturing Dive', 'https://www.manufacturingdive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Plant Engineering', 'https://www.plantengineering.com/feed/', 'news', true, ARRAY['AMER']),
  ('The Manufacturer', 'https://www.themanufacturer.com/feed/', 'news', true, ARRAY['EUR']),
  ('Robotics & Automation News', 'https://roboticsandautomationnews.com/feed/', 'news', true, ARRAY['AMER','EUR','ASIA']),
  ('Automation World', 'https://www.automationworld.com/rss.xml', 'news', true, ARRAY['AMER'])
ON CONFLICT (url) DO NOTHING;

-- ─── DEEP TECH, SEMICONDUCTORS & HARDWARE ───────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('MIT Technology Review', 'https://www.technologyreview.com/feed/', 'news', true, ARRAY['AMER','EUR','ASIA']),
  ('IEEE Spectrum', 'https://spectrum.ieee.org/feeds/feed.rss', 'news', true, ARRAY['AMER','EUR','ASIA']),
  ('Semiconductor Engineering', 'https://semiengineering.com/feed/', 'news', true, ARRAY['AMER','ASIA']),
  ('EE Times', 'https://www.eetimes.com/feed/', 'news', true, ARRAY['AMER','EUR','ASIA']),
  ('The Robot Report', 'https://www.therobotreport.com/feed/', 'news', true, ARRAY['AMER','EUR']),
  ('SpaceNews', 'https://spacenews.com/feed/', 'news', true, ARRAY['AMER','EUR']),
  ('Defense One', 'https://www.defenseone.com/rss/', 'news', true, ARRAY['AMER']),
  ('Quantum Computing Report', 'https://quantumcomputingreport.com/feed/', 'news', true, ARRAY['AMER','EUR']),
  ('Biotech Daily', 'https://www.biotechdaily.com.au/rss.xml', 'news', true, ARRAY['OCE'])
ON CONFLICT (url) DO NOTHING;

-- ─── GOVERNMENT, PUBLIC SECTOR & POLICY ─────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('GovTech', 'https://www.govtech.com/rss/', 'news', true, ARRAY['AMER']),
  ('FedScoop', 'https://fedscoop.com/feed/', 'news', true, ARRAY['AMER']),
  ('StateScoop', 'https://statescoop.com/feed/', 'news', true, ARRAY['AMER']),
  ('PublicTechnology', 'https://www.publictechnology.net/feed', 'news', true, ARRAY['EUR']),
  ('GovInsider Asia', 'https://govinsider.asia/feed/', 'news', true, ARRAY['ASIA']),
  ('InnovationAus', 'https://www.innovationaus.com/feed/', 'news', true, ARRAY['OCE']),
  ('The Mandarin', 'https://www.themandarin.com.au/feed/', 'news', true, ARRAY['OCE']),
  ('Apolitical', 'https://apolitical.co/feed/', 'news', true, ARRAY['EUR','AMER','ASIA'])
ON CONFLICT (url) DO NOTHING;

-- ─── ENERGY, MINING & RESOURCES ─────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Utility Dive', 'https://www.utilitydive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Energy Voice', 'https://www.energyvoice.com/feed/', 'news', true, ARRAY['EUR','MENA']),
  ('Renewables Now', 'https://renewablesnow.com/rss/', 'news', true, ARRAY['EUR','AMER']),
  ('Mining.com', 'https://www.mining.com/feed/', 'news', true, ARRAY['OCE','AMER','MENA']),
  ('RenewEconomy', 'https://reneweconomy.com.au/feed/', 'news', true, ARRAY['OCE']),
  ('Hydrogen Insight', 'https://www.hydrogeninsight.com/rss', 'news', true, ARRAY['EUR','OCE','ASIA']),
  ('Recharge News', 'https://www.rechargenews.com/rss', 'news', true, ARRAY['EUR','AMER'])
ON CONFLICT (url) DO NOTHING;

-- ─── HEALTHCARE & LIFE SCIENCES (expand beyond existing) ────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Healthcare Dive', 'https://www.healthcaredive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('BioPharma Dive', 'https://www.biopharmadive.com/feeds/news/', 'news', true, ARRAY['AMER','EUR']),
  ('Pharma Times', 'https://www.pharmatimes.com/rss', 'news', true, ARRAY['EUR']),
  ('Digital Health', 'https://www.digitalhealth.net/feed/', 'news', true, ARRAY['EUR']),
  ('Healthcare IT News', 'https://www.healthcareitnews.com/feed', 'news', true, ARRAY['AMER','EUR']),
  ('Pulse+IT', 'https://www.pulseit.news/feed/', 'news', true, ARRAY['OCE']),
  ('Asian Scientist', 'https://www.asianscientist.com/feed/', 'news', true, ARRAY['ASIA'])
ON CONFLICT (url) DO NOTHING;

-- ─── EDUCATION & EDTECH ─────────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('EdSurge', 'https://www.edsurge.com/feed', 'news', true, ARRAY['AMER']),
  ('Education Dive', 'https://www.highereddive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Times Higher Education', 'https://www.timeshighereducation.com/rss', 'news', true, ARRAY['EUR','ASIA','OCE']),
  ('EdTech Magazine', 'https://edtechmagazine.com/higher/rss.xml', 'news', true, ARRAY['AMER']),
  ('The Educator', 'https://www.theeducatoronline.com/au/rss', 'news', true, ARRAY['OCE'])
ON CONFLICT (url) DO NOTHING;

-- ─── REAL ESTATE & PROPTECH (expand) ────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Commercial Real Estate Dive', 'https://www.commercialrealestate.com.au/feed/', 'news', true, ARRAY['OCE']),
  ('Propmodo', 'https://www.propmodo.com/feed/', 'news', true, ARRAY['AMER']),
  ('Property Week', 'https://www.propertyweek.com/rss', 'news', true, ARRAY['EUR']),
  ('Mingtiandi', 'https://www.mingtiandi.com/feed/', 'news', true, ARRAY['ASIA']),
  ('The Urban Developer', 'https://www.theurbandeveloper.com/feed', 'news', true, ARRAY['OCE'])
ON CONFLICT (url) DO NOTHING;

-- ─── INSURANCE & INSURTECH ──────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Insurance Journal', 'https://www.insurancejournal.com/feed/', 'news', true, ARRAY['AMER']),
  ('Coverager', 'https://coverager.com/feed/', 'news', true, ARRAY['AMER']),
  ('Insurance Times', 'https://www.insurancetimes.co.uk/rss', 'news', true, ARRAY['EUR']),
  ('InsuranceAsia News', 'https://insuranceasianews.com/feed/', 'news', true, ARRAY['ASIA'])
ON CONFLICT (url) DO NOTHING;

-- ─── LEGAL & PROFESSIONAL SERVICES ──────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Law.com', 'https://www.law.com/rss/', 'news', true, ARRAY['AMER']),
  ('Legal Cheek', 'https://www.legalcheek.com/feed/', 'news', true, ARRAY['EUR']),
  ('Australasian Lawyer', 'https://www.australasianlawyer.com.au/rss', 'news', true, ARRAY['OCE']),
  ('Consulting.us', 'https://www.consulting.us/feed/rss', 'news', true, ARRAY['AMER']),
  ('Management Consulted', 'https://managementconsulted.com/feed/', 'news', true, ARRAY['AMER','EUR'])
ON CONFLICT (url) DO NOTHING;

-- ─── EXECUTIVE MOVES & LEADERSHIP ───────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('Chief Executive', 'https://chiefexecutive.net/feed/', 'news', true, ARRAY['AMER']),
  ('Board Agenda', 'https://boardagenda.com/feed/', 'news', true, ARRAY['EUR']),
  ('HR Dive', 'https://www.hrdive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('SHRM', 'https://www.shrm.org/rss/feeds', 'news', true, ARRAY['AMER']),
  ('People Management', 'https://www.peoplemanagement.co.uk/rss', 'news', true, ARRAY['EUR']),
  ('HRD Asia', 'https://www.hcamag.com/asia/rss', 'news', true, ARRAY['ASIA']),
  ('HRD Australia', 'https://www.hcamag.com/au/rss', 'news', true, ARRAY['OCE'])
ON CONFLICT (url) DO NOTHING;

-- ─── ADDITIONAL ASIA-PACIFIC ────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('KrASIA', 'https://kr-asia.com/feed', 'news', true, ARRAY['ASIA']),
  ('Tech in Asia', 'https://www.techinasia.com/feed', 'news', true, ARRAY['ASIA']),
  ('Japan Times Business', 'https://www.japantimes.co.jp/feed/business/', 'news', true, ARRAY['ASIA']),
  ('Straits Times Business', 'https://www.straitstimes.com/rss/business.xml', 'news', true, ARRAY['ASIA']),
  ('Livemint', 'https://www.livemint.com/rss/companies', 'news', true, ARRAY['ASIA']),
  ('YourStory', 'https://yourstory.com/feed', 'news', true, ARRAY['ASIA']),
  ('Business Standard', 'https://www.business-standard.com/rss/companies-101.rss', 'news', true, ARRAY['ASIA'])
ON CONFLICT (url) DO NOTHING;

-- ─── ADDITIONAL AMERICAS ────────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('CFO Dive', 'https://www.cfodive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('CIO Dive', 'https://www.ciodive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Construction Dive', 'https://www.constructiondive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Transport Dive', 'https://www.transportdive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Restaurant Dive', 'https://www.restaurantdive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('Banking Dive', 'https://www.bankingdive.com/feeds/news/', 'news', true, ARRAY['AMER']),
  ('MarketWatch', 'https://feeds.marketwatch.com/marketwatch/topstories/', 'news', true, ARRAY['AMER']),
  ('Inc.com', 'https://www.inc.com/rss', 'news', true, ARRAY['AMER']),
  ('Fast Company', 'https://www.fastcompany.com/latest/rss', 'news', true, ARRAY['AMER','EUR'])
ON CONFLICT (url) DO NOTHING;

-- ─── ADDITIONAL EUROPE ──────────────────────────────────────────────────────

INSERT INTO rss_sources (name, url, source_type, enabled, regions) VALUES
  ('City AM', 'https://www.cityam.com/feed/', 'news', true, ARRAY['EUR']),
  ('AltFi', 'https://www.altfi.com/rss', 'news', true, ARRAY['EUR']),
  ('EU-Startups', 'https://www.eu-startups.com/feed/', 'news', true, ARRAY['EUR']),
  ('UKTN', 'https://www.uktech.news/feed', 'news', true, ARRAY['EUR']),
  ('Business Insider DE', 'https://www.businessinsider.de/feed/', 'news', true, ARRAY['EUR']),
  ('Les Echos Tech', 'https://www.lesechos.fr/rss/tech-medias', 'news', true, ARRAY['EUR']),
  ('Maddyness UK', 'https://www.maddyness.com/uk/feed/', 'news', true, ARRAY['EUR'])
ON CONFLICT (url) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════════
-- SUMMARY: ~120 new feeds across 12 verticals
-- MENA: 11 feeds (from 0)
-- Consumer/Retail: 10 feeds (from 0)
-- Industrial/Supply Chain: 8 feeds (from 0)
-- Deep Tech: 9 feeds (from ~2)
-- Gov/Public Sector: 8 feeds (from ~1)
-- Energy/Mining: 7 feeds (from ~2)
-- Healthcare: 7 feeds (expanded)
-- Education: 5 feeds (from 0)
-- Real Estate: 5 feeds (expanded)
-- Insurance: 4 feeds (from 0)
-- Legal/Professional Services: 5 feeds (from ~2)
-- Executive/Leadership: 7 feeds (from ~1)
-- Asia-Pacific: 7 feeds (supplemental)
-- Americas: 9 feeds (supplemental)
-- Europe: 7 feeds (supplemental)
-- ═══════════════════════════════════════════════════════════════════════════════
