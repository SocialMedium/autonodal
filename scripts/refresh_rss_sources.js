require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

const DISABLE = [
  'https://www.afr.com/technology/rss','https://www.businessleader.co.uk/feed/','https://www.cityam.com/topic/technology/feed/','https://fortune.com/section/term-sheet/feed','https://www.wired.co.uk/feed/rss','https://a16z.com/feed/','https://www.airtree.vc/open-source-vc/rss.xml','https://www.blackbird.vc/blog/rss.xml','https://www.bvp.com/atlas/rss.xml','https://www.indexventures.com/feed/','https://www.investible.com/blog/rss.xml','https://localglobe.vc/feed/','https://www.moltenventures.com/insights/rss','https://newsletter.overnightsuccess.vc/feed','https://medium.com/feed/playfair-capital','https://speedinvest.com/feed/','https://www.squarepegcap.com/feed','https://eqtventures.com/feed/','https://www.giantleap.com.au/blog/rss.xml','https://www.cutthrough.com/insights/rss.xml','https://www.cherry.vc/feed/','https://www.atomico.com/feed/','https://avcj.com/feeds/rss','https://www.jungleventures.com/feed','https://www.goldengatevcs.com/blog-feed.xml','https://www.digitalnewsasia.com/feed','https://www.axios.com/pro-rata/feed','https://www.fintechfutures.com/feed/','https://pitchbook.com/news/feed','https://www.theinformation.com/feed','https://www.innovationaus.com/feed/','https://www.techinasia.com/feed','https://feeds.simplecast.com/K2vy7R0B','https://feeds.buzzsprout.com/1127040.rss','https://anchor.fm/s/3afcfcf8/podcast/rss','https://feeds.buzzsprout.com/1926839.rss','https://feeds.simplecast.com/MjBMnJMR','https://feeds.acast.com/public/shows/sifted-talks','https://feeds.simplecast.com/dMT4hKVX','https://feeds.megaphone.fm/strictlyvc','https://feeds.acast.com/public/shows/the-overnight-success','https://feeds.megaphone.fm/20vceurope','https://feeds.redcircle.com/c04ab51a-3a0c-4e38-aef1-658a1eb6c606','https://feeds.simplecast.com/hnGaXQ2g','https://feeds.megaphone.fm/village-global','https://www.axios.com/pro/deals/feed','https://www.ben-evans.com/feed','https://review.firstround.com/feed/','https://hbr.org/feed','https://www.intrepidrecruitment.com/feed','https://www.meritechcapital.com/blog/feed','https://www.profgmedia.com/feed','https://sourcery.substack.com/feed','https://fortune.com/tag/term-sheet/feed/','https://aussietechroundup.substack.com/feed','https://www.deeplearning.ai/the-batch/feed/','https://www.readthegeneralist.com/feed','https://www.theneurondaily.com/feed','https://aussiefintech.substack.com/feed','https://www.altfi.com/feed','https://www.itnews.asia/rss','https://kr-asia.com/feed','https://bothsidesofthetable.com/feed','https://christophjanz.blogspot.com/feeds/posts/default/-/SaaS?alt=rss','https://review.firstround.com/feed.xml','https://www.generalist.com/feed',
];

const URL_FIXES = [
  { old: 'https://www.startupdaily.net/topic/venture-capital/feed/', new: 'https://www.startupdaily.net/feed/' },
  { old: 'https://www.strictlyvc.com/feed/', new: 'https://www.strictlyvc.com/feed' },
  { old: 'https://www.itnews.asia/rss', new: 'https://www.itnews.asia/RSS/rss.ashx' },
  { old: 'https://kr-asia.com/feed', new: 'https://kr-asia.com/feed/' },
];

const NEW_FEEDS = [
  { name: 'AFR Street Talk (M&A)', url: 'https://www.afr.com/rss/street-talk', source_type: 'rss', signal_types: ['ma_activity','capital_raising','ipo'], credibility_score: 0.92, poll_interval_minutes: 60 },
  { name: 'AFR Technology', url: 'https://www.afr.com/rss/technology', source_type: 'rss', signal_types: ['capital_raising','leadership_change','ma_activity'], credibility_score: 0.90, poll_interval_minutes: 60 },
  { name: 'Startup Daily', url: 'https://www.startupdaily.net/feed/', source_type: 'rss', signal_types: ['capital_raising','product_launch','partnership'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Overnight Success (AU VC Newsletter)', url: 'https://overnightsuccess.substack.com/feed', source_type: 'newsletter', signal_types: ['capital_raising','geographic_expansion'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Blackbird Ventures Blog', url: 'https://blackbird.vc/feed/', source_type: 'vc_blog', signal_types: ['capital_raising','portfolio_news'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Square Peg Capital Blog', url: 'https://www.squarepeg.vc/blog/rss.xml', source_type: 'vc_blog', signal_types: ['capital_raising','portfolio_news'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'DealStreetAsia', url: 'https://www.dealstreetasia.com/feed/', source_type: 'rss', signal_types: ['capital_raising','ma_activity','ipo'], credibility_score: 0.88, poll_interval_minutes: 60 },
  { name: 'KrASIA', url: 'https://kr-asia.com/feed/', source_type: 'rss', signal_types: ['capital_raising','product_launch','geographic_expansion'], credibility_score: 0.82, poll_interval_minutes: 60 },
  { name: 'Tech in Asia', url: 'https://www.techinasia.com/feed', source_type: 'rss', signal_types: ['capital_raising','product_launch','partnership'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'Vulcan Post (SEA Startups)', url: 'https://vulcanpost.com/feed/', source_type: 'rss', signal_types: ['capital_raising','product_launch'], credibility_score: 0.75, poll_interval_minutes: 60 },
  { name: 'FinanceAsia', url: 'https://financeasia.com/rss/latest', source_type: 'rss', signal_types: ['capital_raising','ma_activity','ipo'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'HRM Asia', url: 'https://hrmasia.com/feed/', source_type: 'rss', signal_types: ['leadership_change','strategic_hiring'], credibility_score: 0.75, poll_interval_minutes: 120 },
  { name: 'South China Morning Post - Business', url: 'https://www.scmp.com/rss/91/feed', source_type: 'rss', signal_types: ['capital_raising','ma_activity','geographic_expansion'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'City AM', url: 'https://www.cityam.com/feed/', source_type: 'rss', signal_types: ['capital_raising','ma_activity','leadership_change'], credibility_score: 0.78, poll_interval_minutes: 60 },
  { name: 'The Guardian - Technology', url: 'https://www.theguardian.com/uk/technology/rss', source_type: 'rss', signal_types: ['product_launch','leadership_change'], credibility_score: 0.85, poll_interval_minutes: 60 },
  { name: 'EU-Startups', url: 'https://www.eu-startups.com/feed/', source_type: 'rss', signal_types: ['capital_raising','product_launch','geographic_expansion'], credibility_score: 0.80, poll_interval_minutes: 60 },
  { name: 'Beauhurst (UK Startups Data)', url: 'https://www.beauhurst.com/blog/feed/', source_type: 'vc_blog', signal_types: ['capital_raising'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'a16z Blog', url: 'https://a16z.com/feed/', source_type: 'vc_blog', signal_types: ['capital_raising','portfolio_news'], credibility_score: 0.90, poll_interval_minutes: 120 },
  { name: 'The Generalist', url: 'https://thegeneralist.substack.com/feed', source_type: 'newsletter', signal_types: ['capital_raising','ma_activity'], credibility_score: 0.85, poll_interval_minutes: 120 },
  { name: 'Benedict Evans', url: 'https://newsletter.ben-evans.com/feed', source_type: 'newsletter', signal_types: ['product_launch','geographic_expansion'], credibility_score: 0.88, poll_interval_minutes: 120 },
  { name: 'Matt Turck (AI/Data VC)', url: 'https://mattturck.com/feed', source_type: 'vc_blog', signal_types: ['capital_raising','product_launch'], credibility_score: 0.85, poll_interval_minutes: 240 },
  { name: 'Sam Altman Blog', url: 'https://blog.samaltman.com/posts.atom', source_type: 'vc_blog', signal_types: ['product_launch'], credibility_score: 0.85, poll_interval_minutes: 240 },
  { name: 'For Entrepreneurs (David Skok)', url: 'https://feeds.feedburner.com/forentrepreneurs', source_type: 'vc_blog', signal_types: ['product_launch'], credibility_score: 0.82, poll_interval_minutes: 240 },
];

async function run() {
  console.log('\n🔄 RSS Sources Refresh\n');
  const client = await pool.connect();
  let disabled=0, fixed=0, added=0, skipped=0;
  try {
    console.log('Step 1: Disabling dead feeds...');
    for (const url of DISABLE) {
      const r = await client.query('UPDATE rss_sources SET enabled=false WHERE url=$1 RETURNING name',[url]);
      if (r.rows.length) { console.log('  ⛔ '+r.rows[0].name); disabled++; }
    }
    console.log('  → '+disabled+' disabled\n');

    console.log('Step 2: Fixing broken URLs...');
    for (const fix of URL_FIXES) {
      const exists = await client.query('SELECT id FROM rss_sources WHERE url=$1',[fix.new]);
      if (exists.rows.length) { await client.query('UPDATE rss_sources SET enabled=false WHERE url=$1',[fix.old]); skipped++; continue; }
      const r = await client.query('UPDATE rss_sources SET url=$1, last_error=NULL, enabled=true WHERE url=$2 RETURNING name',[fix.new,fix.old]);
      if (r.rows.length) { console.log('  🔧 Fixed: '+r.rows[0].name); fixed++; }
    }
    console.log('  → '+fixed+' fixed\n');

    console.log('Step 3: Adding new feeds...');
    for (const feed of NEW_FEEDS) {
      const exists = await client.query('SELECT id FROM rss_sources WHERE url=$1',[feed.url]);
      if (exists.rows.length) { skipped++; continue; }
      await client.query('INSERT INTO rss_sources (name,url,source_type,signal_types,credibility_score,poll_interval_minutes,enabled,created_at) VALUES ($1,$2,$3,$4,$5,$6,true,NOW())',[feed.name,feed.url,feed.source_type,feed.signal_types,feed.credibility_score,feed.poll_interval_minutes]);
      console.log('  ✅ '+feed.name); added++;
    }
    console.log('  → '+added+' added\n');

    const cleared = await client.query("UPDATE rss_sources SET last_error=NULL, consecutive_errors=0 WHERE enabled=true AND (last_error LIKE '%source_type%' OR last_error LIKE '%url_hash%') RETURNING name");
    console.log('Step 4: Cleared error flags on '+cleared.rowCount+' feeds\n');

  } finally { client.release(); }

  const s = (await pool.query("SELECT COUNT(*) FILTER (WHERE enabled) as on, COUNT(*) FILTER (WHERE NOT enabled) as off, COUNT(*) FILTER (WHERE enabled AND last_error IS NULL) as healthy FROM rss_sources")).rows[0];
  console.log('═══════════════════════════');
  console.log('Disabled: '+disabled+' | Fixed: '+fixed+' | Added: '+added);
  console.log('Enabled: '+s.on+' | Disabled: '+s.off+' | Healthy: '+s.healthy);
  console.log('\nDone. Run: railway up');
  await pool.end();
}
run().catch(e => { console.error(e.message); process.exit(1); });
