#!/usr/bin/env node
/**
 * RSS Feed Expansion — Phase 2 (March 2026)
 * ~80 new feeds: UK, ANZ, Singapore/SEA, Global Sector
 * Derived from MitchelLake Network Fingerprint analysis
 *
 * Run: node scripts/expand_rss_sources.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const NEW_FEEDS = [
  // ═══════════════════════════════════════════════════════════════════════════
  // AUSTRALIA — Startup & Growth Signal
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'StartupDaily', url: 'https://www.startupdaily.net/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','partnership'], credibility_score: 0.85, poll_interval_minutes: 30 },
  { name: 'SmartCompany', url: 'https://www.smartcompany.com.au/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change','restructuring'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'InnovationAus', url: 'https://www.innovationaus.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','partnership'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'AFR Business', url: 'https://www.afr.com/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity','leadership_change'], credibility_score: 0.92, poll_interval_minutes: 60 },
  { name: 'Dynamic Business', url: 'https://dynamicbusiness.com.au/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change','product_launch'], credibility_score: 0.75, poll_interval_minutes: 30 },
  { name: 'Business News Australia', url: 'https://www.businessnewsaustralia.com/articles.rss', source_type: 'rss', signal_types: ['capital_raising','leadership_change','ma_activity'], credibility_score: 0.78, poll_interval_minutes: 30 },
  { name: 'TechGuide AU', url: 'https://www.techguide.com.au/feed', source_type: 'rss', signal_types: ['product_launch'], credibility_score: 0.70, poll_interval_minutes: 30 },

  // ═══════════════════════════════════════════════════════════════════════════
  // AUSTRALIA — VC & Investment Signal
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Blackbird Ventures Blog', url: 'https://blog.blackbird.vc/feed', source_type: 'vc_blog', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Square Peg Capital Blog', url: 'https://blog.squarepeg.vc/feed', source_type: 'vc_blog', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'AirTree Ventures Blog', url: 'https://www.airtree.vc/blog/rss', source_type: 'vc_blog', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Cut Through Venture', url: 'https://www.cutthroughventure.com/feed', source_type: 'vc_blog', signal_types: ['capital_raising'], credibility_score: 0.80, poll_interval_minutes: 120 },
  { name: 'Crunchbase News', url: 'https://news.crunchbase.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 30 },

  // ═══════════════════════════════════════════════════════════════════════════
  // AUSTRALIA — Corporate & Financial Markets
  // ═══════════════════════════════════════════════════════════════════════════
  // NOTE: ASX feed URL is indicative — may require company-specific queries.
  // TODO: Build proper ASX announcement polling script as Phase 2.
  { name: 'ASX Announcements', url: 'https://www.asx.com.au/asx/1/company/announcements/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity','leadership_change'], credibility_score: 0.95, poll_interval_minutes: 60 },
  { name: 'AFR Street Talk', url: 'https://www.afr.com/street-talk/rss', source_type: 'rss', signal_types: ['ma_activity','capital_raising'], credibility_score: 0.92, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // AUSTRALIA — Sector-Specific
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Fintech Australia', url: 'https://fintechaustralia.org.au/news/feed', source_type: 'rss', signal_types: ['capital_raising','partnership','product_launch'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'AICD News', url: 'https://www.aicd.com.au/news-media/rss.html', source_type: 'rss', signal_types: ['leadership_change'], credibility_score: 0.88, poll_interval_minutes: 60 },
  { name: 'B Lab Australia & NZ', url: 'https://bcorporation.com.au/news/feed', source_type: 'rss', signal_types: ['partnership'], credibility_score: 0.75, poll_interval_minutes: 120 },
  { name: 'Climate Salad', url: 'https://www.climatesalad.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.75, poll_interval_minutes: 120 },
  { name: 'Stone & Chalk News', url: 'https://stoneandchalk.com.au/news/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'Defence Connect', url: 'https://www.defenceconnect.com.au/feed', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.70, poll_interval_minutes: 120 },
  { name: 'Open Gov Asia', url: 'https://opengovasia.com/feed', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.70, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // NEW ZEALAND
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'NZ Herald Business', url: 'https://www.nzherald.co.nz/business/rss', source_type: 'rss', signal_types: ['capital_raising','leadership_change','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 30 },
  { name: 'Idealog NZ', url: 'https://idealog.co.nz/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Stuff Business NZ', url: 'https://www.stuff.co.nz/business/rss', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.78, poll_interval_minutes: 60 },
  { name: 'NZ Tech Alliance', url: 'https://nztech.org.nz/news/feed', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'Scoop Business NZ', url: 'https://www.scoop.co.nz/stories/business.rss', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.75, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // UNITED KINGDOM — Growth Company & Startup Signal
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'UKTN (UK Tech News)', url: 'https://uktech.news/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change','product_launch','partnership'], credibility_score: 0.85, poll_interval_minutes: 30 },
  { name: 'BusinessCloud UK', url: 'https://businesscloud.co.uk/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'GrowthBusiness UK', url: 'https://www.growthbusiness.co.uk/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'Beauhurst Blog', url: 'https://www.beauhurst.com/blog/feed', source_type: 'rss', signal_types: ['capital_raising'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Prolific North', url: 'https://www.prolificnorth.co.uk/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'Real Business UK', url: 'https://realbusiness.co.uk/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'Maddyness UK', url: 'https://www.maddyness.com/uk/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.78, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // UNITED KINGDOM — Financial & Corporate Press
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'City A.M.', url: 'https://www.cityam.com/feed', source_type: 'rss', signal_types: ['capital_raising','ma_activity','leadership_change'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'The Guardian Business', url: 'https://www.theguardian.com/uk/business/rss', source_type: 'rss', signal_types: ['leadership_change','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Evening Standard Business', url: 'https://www.standard.co.uk/business/rss', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.72, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // UNITED KINGDOM — Private Equity & Venture
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Real Deals (UK PE)', url: 'https://realdeals.eu.com/feed', source_type: 'rss', signal_types: ['ma_activity','capital_raising'], credibility_score: 0.88, poll_interval_minutes: 60 },
  { name: 'PE Hub Europe', url: 'https://pehub.com/europe/feed', source_type: 'rss', signal_types: ['ma_activity','capital_raising'], credibility_score: 0.88, poll_interval_minutes: 60 },
  { name: 'Atomico Blog', url: 'https://www.atomico.com/blog/feed', source_type: 'vc_blog', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Balderton Blog', url: 'https://www.balderton.com/blog/feed', source_type: 'vc_blog', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // UNITED KINGDOM — Sector-Specific
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'AltFi UK', url: 'https://www.altfi.com/rss', source_type: 'rss', signal_types: ['capital_raising','partnership'], credibility_score: 0.85, poll_interval_minutes: 30 },
  { name: 'The Fintech Times', url: 'https://thefintechtimes.com/feed', source_type: 'rss', signal_types: ['capital_raising','partnership','product_launch'], credibility_score: 0.80, poll_interval_minutes: 30 },
  { name: 'Total Telecom', url: 'https://www.totaltele.com/rss', source_type: 'rss', signal_types: ['partnership','product_launch','ma_activity'], credibility_score: 0.82, poll_interval_minutes: 60 },
  { name: 'Telecoms.com', url: 'https://telecoms.com/feed', source_type: 'rss', signal_types: ['partnership','product_launch','ma_activity'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Verdict Tech', url: 'https://www.verdict.co.uk/feed', source_type: 'rss', signal_types: ['product_launch','leadership_change'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'Design Week UK', url: 'https://www.designweek.co.uk/feed', source_type: 'rss', signal_types: ['product_launch'], credibility_score: 0.72, poll_interval_minutes: 120 },
  { name: 'Pioneers Post', url: 'https://www.pioneerspost.com/feed', source_type: 'rss', signal_types: ['capital_raising','partnership'], credibility_score: 0.75, poll_interval_minutes: 120 },
  { name: 'Tech Nation', url: 'https://technation.io/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.78, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // SINGAPORE / SEA — Core Tech & Startup Signal
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'TechInAsia', url: 'https://www.techinasia.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','partnership','geographic_expansion'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'e27', url: 'https://e27.co/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change','product_launch','partnership'], credibility_score: 0.85, poll_interval_minutes: 30 },
  { name: 'DealStreetAsia', url: 'https://www.dealstreetasia.com/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.90, poll_interval_minutes: 60 },
  { name: 'KrASIA', url: 'https://kr-asia.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','geographic_expansion'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Vulcan Post', url: 'https://vulcanpost.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.75, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // SINGAPORE — Business & Financial Press
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'The Business Times SG', url: 'https://www.businesstimes.com.sg/rss/home', source_type: 'rss', signal_types: ['capital_raising','ma_activity','leadership_change'], credibility_score: 0.90, poll_interval_minutes: 60 },
  { name: 'The Edge Singapore', url: 'https://www.theedgesingapore.com/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.88, poll_interval_minutes: 60 },
  { name: 'Singapore Business Review', url: 'https://sbr.com.sg/feed', source_type: 'rss', signal_types: ['capital_raising','leadership_change'], credibility_score: 0.78, poll_interval_minutes: 60 },
  { name: 'Today Business SG', url: 'https://www.todayonline.com/business/rss', source_type: 'rss', signal_types: ['leadership_change'], credibility_score: 0.72, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // SINGAPORE — Regulatory & Ecosystem
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'MAS Press Releases', url: 'https://www.mas.gov.sg/news/press-releases/rss', source_type: 'rss', signal_types: ['partnership','restructuring'], credibility_score: 0.95, poll_interval_minutes: 120 },
  { name: 'Enterprise Singapore', url: 'https://www.enterprisesg.gov.sg/media-centre/news/rss', source_type: 'rss', signal_types: ['partnership','geographic_expansion'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'SGInnovate News', url: 'https://www.sginnovate.com/news/rss', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.80, poll_interval_minutes: 120 },
  { name: 'IMDA News', url: 'https://www.imda.gov.sg/news-and-events/rss', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.80, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // SINGAPORE / SEA — Sector-Specific
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Fintechnews Singapore', url: 'https://fintechnews.sg/feed', source_type: 'rss', signal_types: ['capital_raising','partnership','product_launch'], credibility_score: 0.82, poll_interval_minutes: 30 },
  { name: 'Asian Venture Capital Journal', url: 'https://www.avcj.com/feed', source_type: 'rss', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.90, poll_interval_minutes: 60 },
  { name: 'FinanceAsia', url: 'https://www.financeasia.com/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Fintechnews.ch (SEA)', url: 'https://www.fintechnews.ch/feed', source_type: 'rss', signal_types: ['capital_raising','partnership'], credibility_score: 0.78, poll_interval_minutes: 120 },
  { name: 'Eco-Business', url: 'https://www.eco-business.com/feed', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.78, poll_interval_minutes: 120 },
  { name: 'Capacity Asia', url: 'https://www.capacitymedia.com/asia/feed', source_type: 'rss', signal_types: ['partnership','product_launch','ma_activity'], credibility_score: 0.78, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — Telco & Network Infrastructure
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'SDxCentral', url: 'https://www.sdxcentral.com/feed', source_type: 'rss', signal_types: ['product_launch','partnership','ma_activity'], credibility_score: 0.82, poll_interval_minutes: 60 },
  { name: 'Light Reading', url: 'https://www.lightreading.com/rss.asp', source_type: 'rss', signal_types: ['product_launch','partnership','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Fierce Telecom', url: 'https://www.fiercetelecom.com/feed', source_type: 'rss', signal_types: ['partnership','product_launch','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Capacity Media', url: 'https://www.capacitymedia.com/feed', source_type: 'rss', signal_types: ['partnership','product_launch','ma_activity'], credibility_score: 0.80, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — PE, M&A & Capital Flow
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'PitchBook News', url: 'https://pitchbook.com/news/rss', source_type: 'rss', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.90, poll_interval_minutes: 30 },
  { name: 'Axios Pro Deals', url: 'https://www.axios.com/pro/deals/rss', source_type: 'rss', signal_types: ['ma_activity','capital_raising'], credibility_score: 0.88, poll_interval_minutes: 30 },
  { name: 'PE Hub', url: 'https://pehub.com/feed', source_type: 'rss', signal_types: ['ma_activity','capital_raising','leadership_change'], credibility_score: 0.90, poll_interval_minutes: 60 },
  { name: 'The Deal', url: 'https://www.thedeal.com/rss', source_type: 'rss', signal_types: ['ma_activity','capital_raising'], credibility_score: 0.85, poll_interval_minutes: 60 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — Impact, B-Corp & Sustainability
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'B Lab Global', url: 'https://bcorporation.net/news/rss', source_type: 'rss', signal_types: ['partnership'], credibility_score: 0.80, poll_interval_minutes: 120 },
  { name: 'GreenBiz', url: 'https://www.greenbiz.com/feeds/news', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.78, poll_interval_minutes: 120 },
  { name: 'Impact Alpha', url: 'https://impactalpha.com/feed', source_type: 'rss', signal_types: ['capital_raising','partnership'], credibility_score: 0.80, poll_interval_minutes: 120 },
  { name: 'Sustainable Brands', url: 'https://sustainablebrands.com/rss', source_type: 'rss', signal_types: ['partnership','product_launch'], credibility_score: 0.72, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — AI Product & Enterprise SaaS
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'AI Business', url: 'https://aibusiness.com/rss.xml', source_type: 'rss', signal_types: ['product_launch','partnership','capital_raising'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'VentureBeat AI', url: 'https://venturebeat.com/ai/feed', source_type: 'rss', signal_types: ['product_launch','capital_raising','partnership'], credibility_score: 0.82, poll_interval_minutes: 30 },
  { name: 'SaaStr', url: 'https://www.saastr.com/feed', source_type: 'rss', signal_types: ['product_launch','capital_raising'], credibility_score: 0.78, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — HR Tech
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'HR Dive', url: 'https://www.hrdive.com/feeds/news', source_type: 'rss', signal_types: ['leadership_change','strategic_hiring','restructuring'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'UNLEASH', url: 'https://unleash.ai/news/rss', source_type: 'rss', signal_types: ['product_launch','leadership_change'], credibility_score: 0.75, poll_interval_minutes: 120 },
  { name: 'People Matters', url: 'https://www.peoplematters.in/rss', source_type: 'rss', signal_types: ['leadership_change','strategic_hiring'], credibility_score: 0.72, poll_interval_minutes: 120 },

  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL — Design & Innovation Consulting
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Fast Company', url: 'https://www.fastcompany.com/latest/rss', source_type: 'rss', signal_types: ['product_launch','leadership_change'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Core77', url: 'https://www.core77.com/rss', source_type: 'rss', signal_types: ['product_launch'], credibility_score: 0.70, poll_interval_minutes: 120 },
];

async function run() {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  RSS FEED EXPANSION — Phase 2 (March 2026)');
  console.log('  ~80 new feeds: UK, ANZ, Singapore/SEA, Global Sector');
  console.log('═══════════════════════════════════════════════════════════════\n');

  const client = await pool.connect();
  let added = 0, skipped = 0, errors = 0;

  try {
    // Get all existing URLs for fast lookup
    const existing = await client.query('SELECT url FROM rss_sources');
    const existingUrls = new Set(existing.rows.map(r => r.url));

    for (const feed of NEW_FEEDS) {
      try {
        // Normalize URL comparison (strip trailing slashes)
        const normalizedUrl = feed.url.replace(/\/+$/, '');
        const alreadyExists = [...existingUrls].some(u =>
          u.replace(/\/+$/, '') === normalizedUrl
        );

        if (alreadyExists) {
          console.log(`  ⏭️  ${feed.name} (already exists)`);
          skipped++;
          continue;
        }

        await client.query(
          `INSERT INTO rss_sources (name, url, source_type, signal_types, credibility_score, poll_interval_minutes, enabled, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, true, NOW())`,
          [feed.name, feed.url, feed.source_type, feed.signal_types, feed.credibility_score, feed.poll_interval_minutes]
        );

        existingUrls.add(feed.url);
        console.log(`  ✅ ${feed.name}`);
        added++;

      } catch (err) {
        console.error(`  ❌ ${feed.name}: ${err.message}`);
        errors++;
      }
    }

    // Summary
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  COMPLETE');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`  Added:   ${added}`);
    console.log(`  Skipped: ${skipped} (already existed)`);
    console.log(`  Errors:  ${errors}`);
    console.log(`  Total in list: ${NEW_FEEDS.length}`);

    // Report by region
    const counts = await client.query(`
      SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE enabled = true) as enabled,
        COUNT(*) FILTER (WHERE last_error IS NULL AND enabled = true) as healthy
      FROM rss_sources
    `);
    const s = counts.rows[0];
    console.log(`\n  Platform totals: ${s.total} total | ${s.enabled} enabled | ${s.healthy} healthy`);

    // Check for duplicate URLs
    const dupes = await client.query(
      "SELECT url, COUNT(*) as cnt FROM rss_sources GROUP BY url HAVING COUNT(*) > 1"
    );
    if (dupes.rows.length > 0) {
      console.log(`\n  ⚠️  ${dupes.rows.length} duplicate URL(s) found:`);
      for (const d of dupes.rows) {
        console.log(`     ${d.url} (${d.cnt}x)`);
      }
    } else {
      console.log('  ✅ No duplicate URLs');
    }

  } finally {
    client.release();
  }

  console.log('\n  Next: node scripts/harvest_rss.js');
  console.log('═══════════════════════════════════════════════════════════════\n');
  await pool.end();
}

run().catch(e => { console.error(e.message); process.exit(1); });
