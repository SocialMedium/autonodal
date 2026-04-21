#!/usr/bin/env node
// Add verified industry feeds to rss_sources
// Usage: DATABASE_URL="postgresql://..." node scripts/add_feeds.js

require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const feeds = [
  // Finextra (channel.aspx pattern — verified)
  ['Finextra — Payments', 'https://www.finextra.com/rss/channel.aspx?m=payments'],
  ['Finextra — Startups', 'https://www.finextra.com/rss/channel.aspx?m=startups'],
  ['Finextra — Regulation', 'https://www.finextra.com/rss/channel.aspx?m=regulation'],
  ['Finextra — AI', 'https://www.finextra.com/rss/channel.aspx?m=ai'],
  ['Finextra — Crypto', 'https://www.finextra.com/rss/channel.aspx?m=crypto'],
  ['Finextra — Security', 'https://www.finextra.com/rss/channel.aspx?m=security'],
  ['Finextra — Wealth', 'https://www.finextra.com/rss/channel.aspx?m=wealth'],
  ['Finextra — Retail Banking', 'https://www.finextra.com/rss/channel.aspx?m=retail'],
  ['Finextra — Wholesale', 'https://www.finextra.com/rss/channel.aspx?m=wholesale'],
  ['Finextra — Markets', 'https://www.finextra.com/rss/channel.aspx?m=markets'],
  ['Finextra — Cloud', 'https://www.finextra.com/rss/channel.aspx?m=cloud'],
  ['Finextra — Identity', 'https://www.finextra.com/rss/channel.aspx?m=identity'],
  ['Finextra — Sustainable', 'https://www.finextra.com/rss/channel.aspx?m=sustainable'],
  ['Finextra — Crime', 'https://www.finextra.com/rss/channel.aspx?m=crime'],
  ['Finextra — DevOps', 'https://www.finextra.com/rss/channel.aspx?m=devops'],
  // Healthcare
  ['Healthcare Dive', 'https://www.healthcaredive.com/feeds/news/'],
  ['BioPharma Dive', 'https://www.biopharmadive.com/feeds/news/'],
  ['Fierce Healthcare', 'https://www.fiercehealthcare.com/rss/xml'],
  ['Fierce Pharma', 'https://www.fiercepharma.com/rss/xml'],
  ['Fierce Biotech', 'https://www.fiercebiotech.com/rss/xml'],
  ['STAT News', 'https://www.statnews.com/feed/'],
  ['Endpoints News', 'https://endpts.com/feed/'],
  ['Healthcare IT News', 'https://www.healthcareitnews.com/feed'],
  ['Digital Health', 'https://www.digitalhealth.net/feed/'],
  ['Pharma Times', 'https://www.pharmatimes.com/rss'],
  ['Pulse+IT', 'https://www.pulseit.news/feed/'],
  // Education
  ['EdSurge', 'https://www.edsurge.com/feed'],
  ['Higher Ed Dive', 'https://www.highereddive.com/feeds/news/'],
  ['K-12 Dive', 'https://www.k12dive.com/feeds/news/'],
  ['EdTech Magazine', 'https://edtechmagazine.com/higher/rss.xml'],
  ['Campus Morning Mail', 'https://campusmorningmail.com.au/feed/'],
  ['The PIE News', 'https://thepienews.com/feed/'],
  // Agriculture & Food
  ['AgFunder News', 'https://agfundernews.com/feed'],
  ['Food Dive', 'https://www.fooddive.com/feeds/news/'],
  ['Grocery Dive', 'https://www.grocerydive.com/feeds/news/'],
  ['The Spoon', 'https://thespoon.tech/feed/'],
  // Consumer & FMCG
  ['Retail Dive', 'https://www.retaildive.com/feeds/news/'],
  ['Modern Retail', 'https://www.modernretail.co/feed/'],
  ['Marketing Week', 'https://www.marketingweek.com/feed/'],
  ['Consumer Goods Technology', 'https://consumergoods.com/rss.xml'],
  ['RetailBiz AU', 'https://www.retailbiz.com.au/feed/'],
  ['Glossy', 'https://www.glossy.co/feed/'],
  // Advertising & Media
  ['Ad Age', 'https://adage.com/arc/outboundfeeds/rss/'],
  ['AdExchanger', 'https://www.adexchanger.com/feed/'],
  ['Digiday', 'https://digiday.com/feed/'],
  ['Marketing Dive', 'https://www.marketingdive.com/feeds/news/'],
  ['B&T Magazine', 'https://www.bandt.com.au/feed/'],
];

async function run() {
  let added = 0, skipped = 0;
  for (const [name, url] of feeds) {
    // Check if URL already exists
    const { rows } = await pool.query('SELECT id FROM rss_sources WHERE url = $1', [url]);
    if (rows.length > 0) {
      skipped++;
      continue;
    }
    await pool.query(
      'INSERT INTO rss_sources (name, url, source_type, enabled) VALUES ($1, $2, $3, true)',
      [name, url, 'news']
    );
    console.log(' + ' + name);
    added++;
  }
  console.log(`\nDone: ${added} added, ${skipped} already existed`);
  await pool.end();
}

run().catch(e => { console.error(e.message); pool.end(); process.exit(1); });
