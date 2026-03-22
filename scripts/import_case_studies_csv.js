#!/usr/bin/env node
/**
 * Direct case study import from structured CSV/PDF data.
 * No LLM interpretation — stores exactly what's provided.
 *
 * Expected format: Client, Role, Company Description
 *
 * Usage:
 *   node scripts/import_case_studies_csv.js --data inline
 *   (data is embedded below from the PDF extract)
 */

require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const TENANT_ID = '00000000-0000-0000-0000-000000000001';

// Raw data extracted from the PDF — Client | Role | Description
const CASE_STUDIES = [
  ['28 By Sam Wood', 'Chief Product Officer', 'Technology-driven health and fitness company, Melbourne, Australia'],
  ['Academy+', 'Fractional CTO', 'Tech-focused education platform, training and upskilling for frontline workers'],
  ['Adventus', 'Chief People Officer', 'AI-driven education marketplace connecting students with global institutions'],
  ['Adventus', 'Chief Technology Officer', 'AI-driven education marketplace connecting students with global institutions'],
  ['Agricultural Victoria Services', 'CEO of New Venture', 'Government-backed initiative in agriculture and biotechnology innovation'],
  ['Applied AI Lab', 'Head of Product', 'Tailored AI solutions for banking, finance, insurance, e-commerce, and retail'],
  ['Applied AI Lab', 'Sales Leader', 'Tailored AI solutions for banking, finance, insurance, e-commerce, and retail'],
  ['Airlock Digital Pty Ltd', 'Chief Revenue Officer', 'Cybersecurity and digital identity company'],
  ['Alcidion', 'Director of Strategy and Business Development', 'Health informatics and intelligent software solutions'],
  ['Alternative Investment REIT plc', 'NED', 'UK-based real estate investment trust, London Stock Exchange'],
  ['Antler', 'Partner', 'Global early-stage venture capital firm and startup accelerator, Singapore'],
  ['Appellon', 'CEO', 'Workplace culture transformation through psychology-driven methodologies, Australia'],
  ['Arts Centre Melbourne', 'Director of Data and Insights', 'Australia\'s largest performing arts venue'],
  ['Asia Market Entry Pte Ltd', 'Marketing Director APAC', 'Singapore-based consultancy for tech expansion into APAC'],
  ['Atlas Carbon', 'Customer Success Director', 'AgTech — carbon projects for graziers, Australia'],
  ['Atlas Carbon', 'Head of Product', 'AgTech — carbon projects for graziers, Australia'],
  ['Atlas Carbon', 'VP Revenue', 'AgTech — carbon projects for graziers, Australia'],
  ['Atlas Carbon', 'Chief Executive Officer', 'AgTech — carbon projects for graziers, Australia'],
  ['Atlas Carbon', 'Commercial leader', 'AgTech — carbon projects for graziers, Australia'],
  ['Aussie Life Tech', 'Chief Operating Officer', 'Technology-driven health and fitness company, Melbourne'],
  ['Avarni', 'CCO', 'Carbon management software, AI-driven emissions tracking, Australia'],
  ['Backpocket', 'Head of Product Marketing', 'Australian fintech startup for group payments'],
  ['Banxa', 'Chief Financial Controller', 'Fiat-to-crypto gateway platform, Australia'],
  ['Canva', 'Group Lead/GM', 'Global online design and visual communication platform, Sydney'],
  ['Capability Co', 'Chief Technology Officer', 'Workforce acceleration platform, Australia'],
  ['Capgemini Singapore Pte Ltd', 'Capability Director - Salesforce CoE', 'Global consulting and technology, Singapore'],
  ['CG Spectrum', 'Chief Executive Officer', 'Global online school for animation, VFX, game development'],
  ['Computershare', 'Head of Engineering', 'Financial and governance services, global, Melbourne'],
  ['CoStar UK Limited', 'Head of Sales - Germany', 'Commercial real estate information and analytics, UK'],
  ['Culture Amp', 'VP Data & AI', 'Employee experience platform, Melbourne'],
  ['Culture Amp', 'VP Product', 'Employee experience platform, Melbourne'],
  ['Culture Amp', 'VP People Science', 'Employee experience platform, Melbourne'],
  ['Culture Amp', 'VP of Design and Chief Designer', 'Employee experience platform, Melbourne'],
  ['Culture Amp', 'VP Engineering', 'Employee experience platform, Melbourne'],
  ['Decidr', 'Head of Product', 'AI-first business transformation platform, Australia'],
  ['DoorDash', 'Senior Product Designer', 'Food delivery technology company, global'],
  ['DoorDash', 'Director of Engineering', 'Food delivery technology company, global'],
  ['Eloque', 'VP Engineering', 'Fiber-optic sensing technology, joint venture Xerox PARC / Victorian Government'],
  ['Emesent', 'COO/President (now CEO)', 'Drone autonomy, LiDAR mapping, and data analytics, Australia'],
  ['Emesent', 'CCO', 'Drone autonomy, LiDAR mapping, and data analytics, Australia'],
  ['Emesent', 'Chief Commercial Officer', 'Drone autonomy, LiDAR mapping, and data analytics, Australia'],
  ['Emesent', 'Director of Software and Analytics', 'Drone autonomy, LiDAR mapping, and data analytics, Australia'],
  ['Emesent', 'CFO', 'Drone autonomy, LiDAR mapping, and data analytics, Australia'],
  ['Endava (UK) Limited', 'Senior UX Leader', 'Digital transformation consulting, agile software development, UK/global'],
  ['Endava (UK) Limited', 'Senior Industry Consultant - Payments Specialist', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Senior Infrastructure Architect', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Senior Principal Consultant', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Vice President - Business Development - South East Asia', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Vice President - Sales Australia', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Director of Sales (VP) Supply Chain UK', 'Digital transformation consulting, UK/global'],
  ['Endava (UK) Limited', 'Director New Business, Retail USA', 'Digital transformation consulting, UK/global'],
  ['enviroDNA', 'CEO', 'Environmental DNA technology for biodiversity monitoring, Australia'],
  ['Ernst & Young (EMEIA) Services Limited', 'EGP Cyber Sector Cloud Partner', 'Cybersecurity and cloud services, EMEIA'],
  ['Ernst & Young (ASEAN)', 'Senior Director, Financial Transformation', 'Finance transformation services, Asia'],
  ['EYGS LLP', 'Global Digital Engineering lead', 'Cloud and digital engineering services, global'],
  ['EYGS LLP', 'Luminary Board Advisor Blockchain/Web3', 'Blockchain and Web3 services, global'],
  ['EYGS LLP', 'Luminary Board Advisor Technology', 'Advanced technology, cloud, and data analytics, global'],
  ['EYGS LLP', 'Luminary Board Advisor Product', 'Future of consulting and generative AI, global'],
  ['EY Skills Foundry', 'Product Director', 'AI-powered workforce skills platform'],
  ['EY Skills Foundry', 'Chief Product Officer', 'AI-powered workforce skills platform'],
  ['Farmbot', 'CTO', 'AgTech IoT company, remote monitoring for farm water systems, Australia'],
  ['Fearless Talent', 'Global Talent Director', 'Full-service design agency, London'],
  ['Fearless Talent', 'Head of Content, Community, Social and Growth', 'Full-service design agency, London'],
  ['Fearless Talent', 'Product Design Director', 'Full-service design agency, London'],
  ['Foresight VCT', '2 Non-Executive appointments', 'UK-based venture capital trust'],
  ['Global Message Services AG', 'Legal Counsel', 'AI-driven communication solutions, Switzerland/global'],
  ['Global Message Services AG', 'Chief Human Resources and Administration Officer', 'AI-driven communication solutions, Switzerland/global'],
  ['Global Message Services AG', 'Chief Marketing Officer', 'AI-driven communication solutions, Switzerland/global'],
  ['Henderson Diversified Income Trust', 'Board Appointment', 'UK-based investment trust, Janus Henderson'],
  ['Henderson High Income Trust', 'Board Appointment', 'UK-based investment trust, Janus Henderson'],
  ['Household Capital', 'Chief Experience Officer', 'Australian financial services, reverse mortgages for retirees'],
  ['Immutable', 'Chief Growth Officer', 'Blockchain company, NFT scaling solutions, Australia'],
  ['Immutable', 'Chief People Officer', 'Blockchain company, NFT scaling solutions, Australia'],
  ['Immutable', 'VP Product Partnerships', 'Blockchain company, NFT scaling solutions, Australia'],
  ['Immutable', 'GM of Studio', 'Blockchain company, NFT scaling solutions, Australia'],
  ['Immutable', 'Chief Commercial Officer', 'Blockchain company, NFT scaling solutions, Australia'],
  ['IMPAX Environmental Markets', 'Board Appointment', 'UK investment trust, environmental solutions'],
  ['IntegraFin Services Limited', 'Board Appointment', 'UK financial services, investment platforms'],
  ['ixDF', 'Head of Sales', 'Non-profit design education organisation, global'],
  ['IxDF FZE', 'Board Appointment', 'Non-profit design education organisation, global'],
  ['Jamieson Corporate Finance LLP', 'Prof Services leader', 'Management advisory, private equity transactions, London/NY/SG'],
  ['JPMorgan European Discovery Trust plc', 'Board Appointment', 'UK-based investment trust, European equities'],
  ['JPMorgan European Growth and Income PLC', 'Board Appointment', 'UK-based investment trust, European equities'],
  ['Linius', 'Advisory board', 'Video virtualization technology, Australia'],
  ['Linius', 'CEO', 'Video virtualization technology, Australia'],
  ['Livewire Group', 'Global Head of Media Monetisation', 'Gaming marketing and gametech, global'],
  ['Livewire Group', 'Head of Europe/UK', 'Gaming marketing and gametech, global'],
  ['Livewire Group', 'Head of North America', 'Gaming marketing and gametech, global'],
  ['Livewire Group', 'Head of Japan', 'Gaming marketing and gametech, global'],
  ['Livework', 'Service Design Director', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Livework', 'Head of Partners', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Livework', 'Service Design Director', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Livework', 'Head of Key Accounts', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Livework', 'Head of Partners', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Livework', 'Finance Director', 'Global service design consultancy, London/Rotterdam/Sao Paulo'],
  ['Longtail UX Pty Ltd', 'Head of Finance', 'eCommerce SEO automation, Australia'],
  ['Longtail UX Pty Ltd', 'Head of Technology', 'eCommerce SEO automation, Australia'],
  ['Longtail UX Pty Ltd', 'Head of Product', 'eCommerce SEO automation, Australia'],
  ['Longtail UX Pty Ltd', 'Client Growth Director', 'eCommerce SEO automation, Australia'],
  ['Longtail UX Pty Ltd', 'Non Executive Director', 'eCommerce SEO automation, Australia'],
  ['Lyka', 'Head of Product', 'Fresh pet food company, Australia'],
  ['Lyka', 'Head of Growth', 'Fresh pet food company, Australia'],
  ['Macdoch Ventures', 'CFO compensation market scan', 'Venture capital firm, early-stage, Australia'],
  ['Marex Financial', 'Head of OTC', 'Commodities and financial markets, global'],
  ['Marex Financial', 'Head of Energy - APAC', 'Commodities and financial markets, global'],
  ['Marmalade', 'Customer Success', 'Invoice payment platform, Australia'],
  ['Marmalade', 'Product Hiring', 'Invoice payment platform, Australia'],
  ['Marmalade', 'Growth Marketing', 'Invoice payment platform, Australia'],
  ['me&u', 'Head of Sales', 'Hospitality tech, QR ordering platform, Australia/global'],
  ['MECCA Beauty', 'GM Delivery', 'Beauty retailer, 100+ stores, Australia and New Zealand'],
  ['Mutual Mobile', 'European Sales Leader', 'Technology consultancy and design studio, USA/global'],
  ['MYOB', 'Group Manager Sales Ops and Enablement', 'Business management software, accounting/payroll, Australia'],
  ['Myriota', 'VP of Engineering', 'Satellite IoT connectivity, Australia/UK'],
  ['Op Central', 'Head of Growth', 'AI-powered operations and compliance platform, Australia'],
  ['Opus2', 'Head of Product', 'Legal technology, cloud-based case management, London/global'],
  ['PAM', 'Head of Product', 'Smart navigation solutions for stadiums/airports, Sydney'],
  ['PAM', 'Head of Engineering', 'Smart navigation solutions for stadiums/airports, Sydney'],
  ['Pilot44', 'GTM Director', 'Brand innovation and venture building studio, San Francisco'],
  ['Pilot44', 'Ventures Director', 'Brand innovation and venture building studio, San Francisco'],
  ['Pilot44', 'VP/CEO for venture - SF based', 'Brand innovation and venture building studio, San Francisco'],
  ['Plotlogic', 'Head of / VP People', 'Real-time ore characterization for mining, Queensland, Australia'],
  ['Pulsant', 'Product Director', 'Digital edge infrastructure, cloud and colocation, UK'],
  ['PWC', 'Director - Sustainability Services', 'ESG and sustainability consulting, global'],
  ['PWC', 'Senior Manager - Transformation Assurance', 'Digital and business transformation, global'],
  ['Quantium', 'Executive Manager Engineering', 'Data science and AI company, Australia'],
  ['Rainmaking Innovation (SG) Pte. Ltd.', 'Venture CEO', 'Corporate innovation and venture development, Singapore/global'],
  ['Redbubble', 'Lead Product Designer', 'Online marketplace for independent artists, Melbourne'],
  ['Redbubble', 'Product Designer', 'Online marketplace for independent artists, Melbourne'],
  ['Redbubble', 'Senior Product Designer', 'Online marketplace for independent artists, Melbourne'],
  ['REST Superannuation', 'Advisory Role', 'Australian industry superannuation fund, $80B+ assets'],
  ['Rome2rio', 'Head of Product', 'Online travel platform, multimodal travel planning, Melbourne'],
  ['Rome2rio', 'Head of Commercial', 'Online travel platform, multimodal travel planning, Melbourne'],
  ['Rostro Management Limited', 'Board Appointment', 'Fintech and financial services, London'],
  ['Seek', 'CTO Asia', 'Online employment marketplace, Southeast Asia'],
  ['Selfwealth', 'CTO', 'Flat-fee online brokerage, Australian fintech'],
  ['Selfwealth', 'Chief Experience Officer', 'Flat-fee online brokerage, Australian fintech'],
  ['Sensis', 'GM Product', 'Marketing services and directories, Australia'],
  ['Sharesight', 'Director of Customer Success', 'Investment portfolio tracking platform, NZ/Australia'],
  ['Sharesight', 'Director of Ecosystems', 'Investment portfolio tracking platform, NZ/Australia'],
  ['Sidekicker', 'Commercial Director', 'On-demand staffing platform, Australia/NZ'],
  ['Sidekicker', 'Chief Operating Officer', 'On-demand staffing platform, Australia/NZ'],
  ['SJ Mobile Labs (Habitto)', 'Chief Technology Officer', 'Japanese fintech startup, digital banking, Tokyo'],
  ['SmartDev Holding Pte Ltd', 'Business Development & Sales Leader', 'Software development, Vietnam/Singapore'],
  ['SmilingMind', 'COO', 'Not-for-profit mindfulness and meditation app, Australia'],
  ['SmilingMind', 'Non Executive Director', 'Not-for-profit mindfulness and meditation app, Australia'],
  ['SolChicks Limited', 'Chief Marketing Officer', 'Blockchain gaming, play-to-earn MMORPG'],
  ['SolChicks Limited', 'Regional Head of Marketing', 'Blockchain gaming, play-to-earn MMORPG'],
  ['SolChicks Limited', 'Head of Japan', 'Blockchain gaming, play-to-earn MMORPG'],
  ['Spenmo', 'Chief Product Officer', 'Spend management platform, Singapore'],
  ['Splitit', 'Head of Implementation', 'Buy Now Pay Later using existing credit cards, global'],
  ['SPS Global', 'Banking Sales Lead', 'Business transformation and data management, Switzerland/global'],
  ['SPS Global', 'Business Development & Sales Leader', 'Business transformation and data management, Switzerland/global'],
  ['STRAAND', 'Head of eCommerce', 'Hair and scalp care brand, Melbourne'],
  ['Sven Jobs', 'CEO', 'Casual worker platform, Australia'],
  ['Sypht', 'VP Sales', 'AI-driven document intelligence platform, Australia'],
  ['Tally', 'Chief Product Officer', 'Utility industry software, cloud-native billing, Australia/global'],
  ['The Beauty Chef', 'e-Commerce Director', 'Wellness brand, bio-fermented probiotic supplements, Bondi Beach'],
  ['The Creature Technology Company Pty Ltd', 'General Manager', 'Advanced animatronics for entertainment, Melbourne'],
  ['The Littleoak Company Pty Ltd', 'Head of eCommerce & CX', 'Goat milk infant formula, New Zealand'],
  ['Triple point', 'NED', 'UK investment management firm, venture capital'],
  ['UK Commercial Property', 'NED', 'UK commercial property REIT'],
  ['Upflowy Pty Ltd', 'Chief Product Officer', 'No-code web experience platform, Sydney'],
  ['VA Media Pty Ltd', 'Production Manager', 'Digital media, video content monetization, Australia'],
  ['Verypay', 'CFO', 'Mobile payment solutions, financial inclusion, Switzerland/Africa'],
  ['VeryPay', 'Chair, Non executive Director', 'Mobile payment solutions, financial inclusion, Switzerland/Africa'],
  ['Verysell Technologies S.A.', 'Head of Fund Raising', 'International technology consortium, Switzerland/global'],
  ['WINR Corporation Pty Ltd', 'VP Sales - USA', 'Media technology, identity resolution, Australia/USA'],
  ['X15', 'Portfolio GM', 'Venture-building entity, Commonwealth Bank of Australia'],
  ['X15', 'Fractional COO', 'Venture-building entity, Commonwealth Bank of Australia'],
  ['Xero', 'Design Ops Lead', 'Cloud accounting software, New Zealand/global'],
  ['Xero', 'Director of Research Operations', 'Cloud accounting software, New Zealand/global'],
  ['Xero', 'Product Design Director', 'Cloud accounting software, New Zealand/global'],
  ['Xero', 'GM Design Auckland or MEL', 'Cloud accounting software, New Zealand/global'],
  ['Xero', 'GM Data', 'Cloud accounting software, New Zealand/global'],
  ['Xero', 'GM Design Operations', 'Cloud accounting software, New Zealand/global'],
  ['Xero Australia Pty Ltd', 'Executive General Manager - Design', 'Cloud accounting software, New Zealand/global'],
];

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Case Study Bulk Import — 177 engagements');
  console.log('═══════════════════════════════════════════════════\n');

  // Ensure table exists
  try {
    const fs = require('fs');
    const migPath = require('path').join(__dirname, '..', 'sql', 'migration_case_studies.sql');
    if (fs.existsSync(migPath)) await pool.query(fs.readFileSync(migPath, 'utf8'));
  } catch (e) {}

  // Clear ALL existing case studies — clean slate before bulk import
  const { rowCount: deleted } = await pool.query(
    `DELETE FROM case_studies WHERE tenant_id = $1`,
    [TENANT_ID]
  );
  if (deleted > 0) console.log(`  Cleared ${deleted} existing case studies (clean slate)\n`);

  let imported = 0, skipped = 0;

  for (const [client, role, description] of CASE_STUDIES) {
    // Resolve client company
    let clientId = null;
    try {
      const { rows } = await pool.query(
        `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
        [`%${client.trim()}%`, TENANT_ID]
      );
      if (rows.length) clientId = rows[0].id;
    } catch (e) {}

    const title = `${role} — ${client}`;

    // Dedup
    const { rows: dupes } = await pool.query(
      `SELECT id FROM case_studies WHERE client_name = $1 AND role_title = $2 AND tenant_id = $3 LIMIT 1`,
      [client.trim(), role.trim(), TENANT_ID]
    );
    if (dupes.length) { skipped++; continue; }

    try {
      await pool.query(`
        INSERT INTO case_studies (
          tenant_id, title, client_name, client_id, role_title,
          engagement_type, challenge,
          status, visibility, extracted_by
        ) VALUES ($1, $2, $3, $4, $5, 'executive_search', $6, 'draft', 'internal_only', 'bulk_import')
      `, [TENANT_ID, title, client.trim(), clientId, role.trim(), description.trim()]);
      imported++;
    } catch (e) {
      console.error(`  Error: ${client} / ${role}: ${e.message}`);
    }
  }

  console.log(`  Imported: ${imported}`);
  console.log(`  Skipped (duplicates): ${skipped}`);
  console.log(`  Total in PDF: ${CASE_STUDIES.length}`);
  console.log('\n═══════════════════════════════════════════════════');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
