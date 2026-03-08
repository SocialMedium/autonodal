#!/usr/bin/env node
/**
 * Add comprehensive RSS sources for MitchelLake Signal Intelligence
 * Covers: Australia, UK, Singapore/SEA, Global VC/Tech, PE/M&A, Executive Moves
 */

require('dotenv').config();
const db = require('../lib/db');

const sources = [
  // ═══════════════════════════════════════════════════════════════════════════
  // GLOBAL / US SOURCES
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'The Information', url: 'https://www.theinformation.com/feed', credibility: 0.90, signals: ['capital_raising','leadership_change','ma_activity'] },
  { name: 'Axios Pro Rata', url: 'https://www.axios.com/pro-rata/feed', credibility: 0.85, signals: ['capital_raising','ma_activity'] },
  { name: 'PitchBook News', url: 'https://pitchbook.com/news/feed', credibility: 0.90, signals: ['capital_raising','ma_activity','partnership'] },
  { name: 'Fortune Term Sheet', url: 'https://fortune.com/section/term-sheet/feed', credibility: 0.85, signals: ['capital_raising','ma_activity','leadership_change'] },
  { name: 'StrictlyVC', url: 'https://www.strictlyvc.com/feed/', credibility: 0.85, signals: ['capital_raising','ma_activity'] },
  { name: 'Crunchbase News', url: 'https://news.crunchbase.com/feed/', credibility: 0.85, signals: ['capital_raising','product_launch','ma_activity'] },
  { name: 'VentureBeat', url: 'https://venturebeat.com/feed/', credibility: 0.80, signals: ['capital_raising','product_launch','partnership'] },
  { name: 'Wired', url: 'https://www.wired.com/feed/rss', credibility: 0.80, signals: ['product_launch','leadership_change'] },
  { name: 'Ars Technica', url: 'https://feeds.arstechnica.com/arstechnica/technology-lab', credibility: 0.80, signals: ['product_launch'] },
  { name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml', credibility: 0.80, signals: ['product_launch','leadership_change','ma_activity'] },
  
  // ═══════════════════════════════════════════════════════════════════════════
  // AUSTRALIA SOURCES
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Startup Daily (AU)', url: 'https://www.startupdaily.net/feed/', credibility: 0.85, signals: ['capital_raising','leadership_change','product_launch','partnership'], region: 'AU' },
  { name: 'SmartCompany Startups', url: 'https://www.smartcompany.com.au/startupsmart/feed/', credibility: 0.85, signals: ['capital_raising','leadership_change','restructuring'], region: 'AU' },
  { name: 'SmartCompany', url: 'https://www.smartcompany.com.au/feed/', credibility: 0.80, signals: ['capital_raising','leadership_change','restructuring'], region: 'AU' },
  { name: 'AFR Technology', url: 'https://www.afr.com/technology/rss', credibility: 0.90, signals: ['capital_raising','ma_activity','leadership_change'], region: 'AU' },
  { name: 'iTnews Australia', url: 'https://www.itnews.com.au/RSS/rss.ashx', credibility: 0.80, signals: ['product_launch','partnership','leadership_change'], region: 'AU' },
  { name: 'InnovationAus', url: 'https://www.innovationaus.com/feed/', credibility: 0.80, signals: ['capital_raising','product_launch','partnership'], region: 'AU' },
  { name: 'Business News Australia', url: 'https://www.businessnewsaustralia.com/rss.xml', credibility: 0.80, signals: ['capital_raising','leadership_change','ma_activity'], region: 'AU' },
  { name: 'Dynamic Business AU', url: 'https://dynamicbusiness.com/feed', credibility: 0.75, signals: ['capital_raising','leadership_change','product_launch'], region: 'AU' },
  { name: 'Anthill Magazine', url: 'https://anthillonline.com/feed/', credibility: 0.75, signals: ['capital_raising','product_launch'], region: 'AU' },
  
  // ═══════════════════════════════════════════════════════════════════════════
  // UK SOURCES
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'UKTN - UK Tech News', url: 'https://www.uktech.news/feed', credibility: 0.85, signals: ['capital_raising','leadership_change','product_launch','partnership'], region: 'UK' },
  { name: 'TechRound UK', url: 'https://techround.co.uk/feed/', credibility: 0.80, signals: ['capital_raising','leadership_change','product_launch'], region: 'UK' },
  { name: 'City AM Tech', url: 'https://www.cityam.com/topic/technology/feed/', credibility: 0.80, signals: ['capital_raising','ma_activity','leadership_change'], region: 'UK' },
  { name: 'Wired UK', url: 'https://www.wired.co.uk/feed/rss', credibility: 0.80, signals: ['product_launch','leadership_change'], region: 'UK' },
  { name: 'The Register', url: 'https://www.theregister.com/headlines.atom', credibility: 0.75, signals: ['product_launch','leadership_change','restructuring'], region: 'UK' },
  { name: 'ComputerWeekly', url: 'https://www.computerweekly.com/rss/IT-management.xml', credibility: 0.80, signals: ['leadership_change','product_launch'], region: 'UK' },
  { name: 'Tech.eu', url: 'https://tech.eu/feed/', credibility: 0.85, signals: ['capital_raising','ma_activity','leadership_change','geographic_expansion'], region: 'EU' },
  { name: 'Business Leader UK', url: 'https://www.businessleader.co.uk/feed/', credibility: 0.80, signals: ['capital_raising','leadership_change'], region: 'UK' },
  { name: 'Fintech Futures', url: 'https://www.fintechfutures.com/feed/', credibility: 0.85, signals: ['capital_raising','partnership','product_launch'], region: 'UK' },
  { name: 'AltFi', url: 'https://www.altfi.com/feed', credibility: 0.85, signals: ['capital_raising','partnership'], region: 'UK' },
  { name: 'FinTech Global', url: 'https://fintech.global/feed/', credibility: 0.80, signals: ['capital_raising','partnership','ma_activity'], region: 'UK' },
  
  // ═══════════════════════════════════════════════════════════════════════════
  // SINGAPORE / SOUTHEAST ASIA SOURCES
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'Tech in Asia', url: 'https://www.techinasia.com/feed', credibility: 0.90, signals: ['capital_raising','ma_activity','leadership_change','geographic_expansion'], region: 'SEA' },
  { name: 'e27', url: 'https://e27.co/feed/', credibility: 0.85, signals: ['capital_raising','leadership_change','product_launch','partnership'], region: 'SEA' },
  { name: 'DealStreetAsia', url: 'https://www.dealstreetasia.com/feed/', credibility: 0.90, signals: ['capital_raising','ma_activity','leadership_change'], region: 'SEA' },
  { name: 'KrASIA', url: 'https://kr-asia.com/feed', credibility: 0.80, signals: ['capital_raising','product_launch','geographic_expansion'], region: 'SEA' },
  { name: 'Digital News Asia', url: 'https://www.digitalnewsasia.com/feed', credibility: 0.80, signals: ['capital_raising','product_launch','partnership'], region: 'SEA' },
  { name: 'Vulcan Post', url: 'https://vulcanpost.com/feed/', credibility: 0.75, signals: ['capital_raising','product_launch'], region: 'SEA' },
  { name: 'Business Times SG', url: 'https://www.businesstimes.com.sg/rss/companies-markets', credibility: 0.90, signals: ['capital_raising','ma_activity','leadership_change'], region: 'SG' },
  { name: 'Channel News Asia Biz', url: 'https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6511', credibility: 0.85, signals: ['capital_raising','leadership_change','ma_activity'], region: 'SG' },
  { name: 'iTnews Asia', url: 'https://www.itnews.asia/rss', credibility: 0.80, signals: ['product_launch','partnership','leadership_change'], region: 'SEA' },
  
  // ═══════════════════════════════════════════════════════════════════════════
  // PE / M&A SPECIFIC
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'PE Hub', url: 'https://www.pehub.com/feed/', credibility: 0.90, signals: ['ma_activity','capital_raising','leadership_change'] },
  { name: 'Private Equity Intl', url: 'https://www.privateequityinternational.com/feed/', credibility: 0.90, signals: ['capital_raising','ma_activity'] },
  { name: 'Buyouts Insider', url: 'https://www.buyoutsinsider.com/feed/', credibility: 0.85, signals: ['ma_activity','capital_raising'] },
  
  // ═══════════════════════════════════════════════════════════════════════════
  // EXECUTIVE MOVES
  // ═══════════════════════════════════════════════════════════════════════════
  { name: 'HR Dive', url: 'https://www.hrdive.com/feeds/news/', credibility: 0.80, signals: ['leadership_change','restructuring'] },
];

async function addSources() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  ADD RSS SOURCES - MitchelLake Signal Intelligence');
  console.log('═══════════════════════════════════════════════════════════════\n');

  let added = 0;
  let skipped = 0;
  let errors = 0;

  for (const source of sources) {
    try {
      // Check if already exists
      const existing = await db.queryOne(
        'SELECT id FROM rss_sources WHERE url = $1 OR name = $2',
        [source.url, source.name]
      );

      if (existing) {
        console.log(`  ⏭️  ${source.name} (already exists)`);
        skipped++;
        continue;
      }

      // Insert new source
      await db.query(`
        INSERT INTO rss_sources (name, source_type, url, poll_interval_minutes, enabled, credibility_score, signal_types)
        VALUES ($1, 'rss', $2, 60, true, $3, $4)
      `, [
        source.name,
        source.url,
        source.credibility,
        `{${source.signals.join(',')}}`
      ]);

      console.log(`  ✅ ${source.name} ${source.region ? `(${source.region})` : ''}`);
      added++;

    } catch (err) {
      console.error(`  ❌ ${source.name}: ${err.message}`);
      errors++;
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('  COMPLETE');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`  Added: ${added}`);
  console.log(`  Skipped (existing): ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Total sources: ${added + skipped}`);
  console.log('═══════════════════════════════════════════════════════════════\n');

  // Show summary by region
  const counts = await db.queryAll(`
    SELECT 
      CASE 
        WHEN url LIKE '%australia%' OR url LIKE '%smartcompany%' OR url LIKE '%startupdaily%' OR url LIKE '%itnews.com.au%' OR url LIKE '%afr.com%' OR name LIKE '%(AU)%' THEN 'Australia'
        WHEN url LIKE '%uktech%' OR url LIKE '%wired.co.uk%' OR url LIKE '%cityam%' OR url LIKE '%tech.eu%' OR url LIKE '%fintech%' OR name LIKE '%UK%' THEN 'UK/Europe'
        WHEN url LIKE '%techinasia%' OR url LIKE '%e27%' OR url LIKE '%dealstreet%' OR url LIKE '%asia%' OR url LIKE '%channelnews%' OR url LIKE '%businesstimes.com.sg%' THEN 'Singapore/SEA'
        ELSE 'Global'
      END as region,
      COUNT(*) as count
    FROM rss_sources
    WHERE enabled = true
    GROUP BY region
    ORDER BY count DESC
  `);
  
  console.log('  Sources by Region:');
  for (const row of counts) {
    console.log(`    ${row.region}: ${row.count}`);
  }

  process.exit(0);
}

addSources().catch(err => {
  console.error('Failed:', err);
  process.exit(1);
});
