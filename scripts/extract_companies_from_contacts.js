#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════
// scripts/extract_companies_from_contacts.js
//
// Creates company records from contact data:
//   1. People with current_company_name but no current_company_id
//   2. Email domains from people + interactions (domain → company name)
//
// Safe to run repeatedly — uses ON CONFLICT to avoid duplicates.
//
// Usage:
//   node scripts/extract_companies_from_contacts.js
//   node scripts/extract_companies_from_contacts.js --tenant-id <uuid>
//   node scripts/extract_companies_from_contacts.js --dry-run
// ═══════════════════════════════════════════════════════════════════════════

require('dotenv').config();

const { Pool } = require('pg');
const { runJob } = require('../lib/job_runner');

const DRY_RUN = process.argv.includes('--dry-run');
const TENANT_ID = (() => {
  const idx = process.argv.indexOf('--tenant-id');
  return idx !== -1 ? process.argv[idx + 1] : null;
})();

const c = {
  green:  (s) => `\x1b[32m${s}\x1b[0m`,
  red:    (s) => `\x1b[31m${s}\x1b[0m`,
  yellow: (s) => `\x1b[33m${s}\x1b[0m`,
  dim:    (s) => `\x1b[2m${s}\x1b[0m`,
};

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Common freemail domains to skip
const FREEMAIL = new Set([
  'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'live.com',
  'icloud.com', 'me.com', 'mac.com', 'aol.com', 'msn.com',
  'protonmail.com', 'proton.me', 'fastmail.com', 'zoho.com',
  'ymail.com', 'googlemail.com', 'mail.com', 'gmx.com',
  'tutanota.com', 'hey.com', 'pm.me',
]);

// Skip these "company" names — noise from LinkedIn imports
const SKIP_NAMES = new Set([
  '', 'n/a', 'none', 'self', 'self-employed', 'selfemployed',
  'freelance', 'freelancer', 'independent', 'consultant',
  'retired', 'student', 'unemployed', 'looking', 'seeking',
  '-', '--', '—', '.', '..', 'tbd', 'tbc',
]);

function shouldSkipCompany(name) {
  if (!name) return true;
  const norm = name.toLowerCase().trim();
  if (norm.length < 2) return true;
  if (SKIP_NAMES.has(norm)) return true;
  if (/^(self|freelanc|independent|retired|student)/i.test(norm)) return true;
  return false;
}

// Well-known domain → proper company name
const KNOWN_DOMAINS = {
  'google.com': 'Google', 'microsoft.com': 'Microsoft', 'amazon.com': 'Amazon',
  'apple.com': 'Apple', 'meta.com': 'Meta', 'facebook.com': 'Meta',
  'netflix.com': 'Netflix', 'salesforce.com': 'Salesforce', 'oracle.com': 'Oracle',
  'ibm.com': 'IBM', 'deloitte.com': 'Deloitte', 'pwc.com': 'PwC',
  'ey.com': 'EY', 'kpmg.com': 'KPMG', 'mckinsey.com': 'McKinsey',
  'bcg.com': 'BCG', 'bain.com': 'Bain & Company', 'accenture.com': 'Accenture',
  'jpmorgan.com': 'JPMorgan', 'goldmansachs.com': 'Goldman Sachs',
  'morganstanley.com': 'Morgan Stanley', 'citi.com': 'Citi',
  'hsbc.com': 'HSBC', 'anz.com': 'ANZ', 'commbank.com.au': 'Commonwealth Bank',
  'westpac.com.au': 'Westpac', 'nab.com.au': 'NAB', 'macquarie.com': 'Macquarie',
  'atlassian.com': 'Atlassian', 'canva.com': 'Canva', 'stripe.com': 'Stripe',
  'uber.com': 'Uber', 'airbnb.com': 'Airbnb', 'spotify.com': 'Spotify',
  'twitter.com': 'X (Twitter)', 'linkedin.com': 'LinkedIn', 'github.com': 'GitHub',
  'slack.com': 'Slack', 'zoom.us': 'Zoom', 'shopify.com': 'Shopify',
  'twilio.com': 'Twilio', 'datadog.com': 'Datadog', 'snowflake.com': 'Snowflake',
  'palantir.com': 'Palantir', 'databricks.com': 'Databricks',
};

function domainToCompanyName(domain) {
  if (KNOWN_DOMAINS[domain]) return KNOWN_DOMAINS[domain];
  const parts = domain.split('.');
  if (parts.length < 2) return null;
  const name = parts[0];
  if (name.length < 2) return null;
  // Capitalize first letter, preserve rest (handles acronyms better)
  return name.charAt(0).toUpperCase() + name.slice(1);
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  COMPANY EXTRACTION FROM CONTACTS');
  console.log('═══════════════════════════════════════════════════════════════');
  if (DRY_RUN) console.log(c.yellow('  ⚠ DRY RUN — no writes'));

  await runJob(pool, 'extract_companies', async () => {
    // Ensure required columns exist
    try { await pool.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS source VARCHAR(50)`); } catch (e) {}
    try { await pool.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS domain VARCHAR(255)`); } catch (e) {}
    try { await pool.query(`ALTER TABLE companies ADD COLUMN IF NOT EXISTS employee_count_estimate INTEGER`); } catch (e) {}

    // Get tenants to process
    let tenantIds;
    if (TENANT_ID) {
      tenantIds = [TENANT_ID];
    } else {
      const { rows } = await pool.query(`SELECT id FROM tenants`);
      tenantIds = rows.map(r => r.id);
    }

    let totalCompaniesCreated = 0;
    let totalPeopleLinked = 0;
    let totalDomainCompanies = 0;

    for (const tenantId of tenantIds) {
      console.log(c.yellow(`\n  ▶ Tenant: ${tenantId}`));

      // ═══════════════════════════════════════════════════════════════
      // STEP 1: Extract companies from people.current_company_name
      // ═══════════════════════════════════════════════════════════════

      const { rows: unlinked } = await pool.query(`
        SELECT TRIM(current_company_name) AS name, COUNT(*) AS people_count
        FROM people
        WHERE tenant_id = $1
          AND current_company_name IS NOT NULL
          AND TRIM(current_company_name) != ''
          AND current_company_id IS NULL
        GROUP BY TRIM(current_company_name)
        ORDER BY COUNT(*) DESC
      `, [tenantId]);

      console.log(`    📋 ${unlinked.length} unique company names from contacts without company records`);

      let created = 0, linked = 0, skipped = 0;

      for (const row of unlinked) {
        if (shouldSkipCompany(row.name)) { skipped++; continue; }

        // Check if company already exists (case-insensitive)
        const { rows: existing } = await pool.query(
          `SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
          [row.name.trim(), tenantId]
        );

        let companyId;
        if (existing.length) {
          companyId = existing[0].id;
        } else if (!DRY_RUN) {
          // Create company
          try {
            const { rows: [newCo] } = await pool.query(
              `INSERT INTO companies (name, source, tenant_id, created_at, updated_at)
               VALUES ($1, 'contact_extraction', $2, NOW(), NOW())
               RETURNING id`,
              [row.name.trim(), tenantId]
            );
            companyId = newCo.id;
            created++;
          } catch (insertErr) {
            // Duplicate — look it up
            const { rows: r } = await pool.query(
              `SELECT id FROM companies WHERE LOWER(TRIM(name)) = LOWER($1) AND tenant_id = $2 LIMIT 1`,
              [row.name.trim(), tenantId]
            );
            companyId = r[0]?.id;
          }
        } else {
          created++; // Count for dry run
          continue;
        }

        // Link people to company
        if (companyId && !DRY_RUN) {
          const { rowCount } = await pool.query(
            `UPDATE people SET current_company_id = $1, updated_at = NOW()
             WHERE tenant_id = $2
               AND LOWER(TRIM(current_company_name)) = LOWER($3)
               AND (current_company_id IS NULL OR current_company_id != $1)`,
            [companyId, tenantId, row.name.trim()]
          );
          linked += rowCount;
        } else {
          linked += parseInt(row.people_count);
        }
      }

      console.log(c.green(`    ✓ ${created} companies created, ${linked} people linked, ${skipped} skipped`));

      // ═══════════════════════════════════════════════════════════════
      // STEP 2: Extract companies from email domains
      // ═══════════════════════════════════════════════════════════════

      const { rows: domains } = await pool.query(`
        SELECT
          LOWER(SPLIT_PART(email, '@', 2)) AS domain,
          COUNT(*) AS people_count,
          MODE() WITHIN GROUP (ORDER BY current_company_name) AS likely_name
        FROM people
        WHERE tenant_id = $1
          AND email IS NOT NULL
          AND email LIKE '%@%'
          AND LOWER(SPLIT_PART(email, '@', 2)) NOT IN (${[...FREEMAIL].map((_, i) => `$${i + 2}`).join(',')})
        GROUP BY LOWER(SPLIT_PART(email, '@', 2))
        HAVING COUNT(*) >= 2
        ORDER BY COUNT(*) DESC
      `, [tenantId, ...FREEMAIL]);

      console.log(`    📧 ${domains.length} corporate email domains with 2+ contacts`);

      let domainCreated = 0;

      for (const d of domains) {
        if (!d.domain || d.domain.length < 4) continue;

        // Use the most common company name from people at this domain, or derive from domain
        const companyName = d.likely_name && !shouldSkipCompany(d.likely_name)
          ? d.likely_name.trim()
          : domainToCompanyName(d.domain);

        if (!companyName) continue;

        // Check if company exists
        const { rows: existing } = await pool.query(
          `SELECT id FROM companies
           WHERE tenant_id = $1 AND (LOWER(TRIM(name)) = LOWER($2) OR LOWER(domain) = LOWER($3))
           LIMIT 1`,
          [tenantId, companyName, d.domain]
        );

        if (existing.length) {
          // Update domain if not set
          if (!DRY_RUN) {
            await pool.query(
              `UPDATE companies SET domain = COALESCE(domain, $1), updated_at = NOW()
               WHERE id = $2 AND domain IS NULL`,
              [d.domain, existing[0].id]
            );
          }
          continue;
        }

        if (!DRY_RUN) {
          const { rows: [newCo] } = await pool.query(
            `INSERT INTO companies (name, domain, source, tenant_id, created_at, updated_at)
             VALUES ($1, $2, 'email_domain_extraction', $3, NOW(), NOW())
             ON CONFLICT DO NOTHING
             RETURNING id`,
            [companyName, d.domain, tenantId]
          );
          if (newCo) {
            domainCreated++;
            // Link people with this domain
            await pool.query(
              `UPDATE people SET current_company_id = $1, updated_at = NOW()
               WHERE tenant_id = $2
                 AND LOWER(SPLIT_PART(email, '@', 2)) = LOWER($3)
                 AND current_company_id IS NULL`,
              [newCo.id, tenantId, d.domain]
            );
          }
        } else {
          domainCreated++;
        }
      }

      console.log(c.green(`    ✓ ${domainCreated} companies from email domains`));

      // ═══════════════════════════════════════════════════════════════
      // STEP 2b: Extract from interaction email domains (email_from)
      // ═══════════════════════════════════════════════════════════════

      let interactionDomains = 0;
      try {
        const { rows: iDomains } = await pool.query(`
          SELECT
            LOWER(SPLIT_PART(i.email_from, '@', 2)) AS domain,
            COUNT(DISTINCT i.email_from) AS sender_count,
            COUNT(*) AS thread_count
          FROM interactions i
          JOIN users u ON u.id = i.user_id AND u.tenant_id = $1
          WHERE i.email_from IS NOT NULL AND i.email_from LIKE '%@%'
            AND i.channel = 'email'
            AND LOWER(SPLIT_PART(i.email_from, '@', 2)) NOT IN (${[...FREEMAIL].map((_, i) => `$${i + 2}`).join(',')})
          GROUP BY LOWER(SPLIT_PART(i.email_from, '@', 2))
          HAVING COUNT(*) >= 3
          ORDER BY COUNT(*) DESC
          LIMIT 500
        `, [tenantId, ...FREEMAIL]);

        for (const d of iDomains) {
          if (!d.domain || d.domain.length < 4) continue;
          const companyName = domainToCompanyName(d.domain);
          if (!companyName) continue;

          const { rows: existing } = await pool.query(
            `SELECT id FROM companies WHERE tenant_id = $1 AND (LOWER(TRIM(name)) = LOWER($2) OR LOWER(domain) = LOWER($3)) LIMIT 1`,
            [tenantId, companyName, d.domain]
          );
          if (existing.length) continue;

          if (!DRY_RUN) {
            const { rows: [newCo] } = await pool.query(
              `INSERT INTO companies (name, domain, source, tenant_id, created_at, updated_at)
               VALUES ($1, $2, 'email_interaction_domain', $3, NOW(), NOW())
               ON CONFLICT DO NOTHING RETURNING id`,
              [companyName, d.domain, tenantId]
            );
            if (newCo) interactionDomains++;
          } else {
            interactionDomains++;
          }
        }
        if (interactionDomains) console.log(c.green(`    ✓ ${interactionDomains} companies from email interaction domains`));
      } catch (e) { console.log(c.dim(`    ⚠ Interaction domain extraction: ${e.message}`)); }

      // ═══════════════════════════════════════════════════════════════
      // STEP 2c: Extract from new_contacts_review (unmatched Gmail contacts)
      // ═══════════════════════════════════════════════════════════════

      let reviewCompanies = 0;
      try {
        const { rows: reviewDomains } = await pool.query(`
          SELECT
            LOWER(SPLIT_PART(email, '@', 2)) AS domain,
            COUNT(*) AS contact_count
          FROM new_contacts_review
          WHERE email IS NOT NULL AND email LIKE '%@%'
            AND LOWER(SPLIT_PART(email, '@', 2)) NOT IN (${[...FREEMAIL].map((_, i) => `$${i + 1}`).join(',')})
          GROUP BY LOWER(SPLIT_PART(email, '@', 2))
          HAVING COUNT(*) >= 2
          ORDER BY COUNT(*) DESC
          LIMIT 300
        `, [...FREEMAIL]);

        for (const d of reviewDomains) {
          if (!d.domain || d.domain.length < 4) continue;
          const companyName = domainToCompanyName(d.domain);
          if (!companyName) continue;

          const { rows: existing } = await pool.query(
            `SELECT id FROM companies WHERE tenant_id = $1 AND (LOWER(TRIM(name)) = LOWER($2) OR LOWER(domain) = LOWER($3)) LIMIT 1`,
            [tenantId, companyName, d.domain]
          );
          if (existing.length) continue;

          if (!DRY_RUN) {
            const { rows: [newCo] } = await pool.query(
              `INSERT INTO companies (name, domain, source, tenant_id, created_at, updated_at)
               VALUES ($1, $2, 'email_review_domain', $3, NOW(), NOW())
               ON CONFLICT DO NOTHING RETURNING id`,
              [companyName, d.domain, tenantId]
            );
            if (newCo) reviewCompanies++;
          } else {
            reviewCompanies++;
          }
        }
        if (reviewCompanies) console.log(c.green(`    ✓ ${reviewCompanies} companies from unmatched email contacts`));
      } catch (e) { console.log(c.dim(`    ⚠ Review contacts extraction: ${e.message}`)); }

      totalDomainCompanies += domainCreated + interactionDomains + reviewCompanies;

      // ═══════════════════════════════════════════════════════════════
      // STEP 3: Enrich companies with sector from people titles
      // ═══════════════════════════════════════════════════════════════

      if (!DRY_RUN) {
        // Set headcount from people count
        const { rowCount: hcSet } = await pool.query(`
          UPDATE companies c SET
            employee_count_estimate = sub.cnt,
            updated_at = NOW()
          FROM (
            SELECT current_company_id AS cid, COUNT(*) AS cnt
            FROM people
            WHERE tenant_id = $1 AND current_company_id IS NOT NULL
            GROUP BY current_company_id
          ) sub
          WHERE c.id = sub.cid AND c.tenant_id = $1
            AND (c.employee_count_estimate IS NULL OR c.employee_count_estimate = 0)
        `, [tenantId]);
        if (hcSet) console.log(`    📊 Set headcount estimate on ${hcSet} companies`);

        // Derive sector from most common title keywords
        const SECTOR_KEYWORDS = {
          'Technology': ['engineer', 'developer', 'software', 'devops', 'data scientist', 'cto', 'tech lead', 'architect'],
          'Banking/Financial Services': ['banker', 'finance', 'cfo', 'investment', 'trading', 'portfolio', 'wealth', 'fund'],
          'Consulting': ['consultant', 'advisory', 'partner', 'strategy', 'managing director'],
          'Healthcare': ['doctor', 'medical', 'health', 'clinical', 'pharma', 'nurse'],
          'Legal': ['lawyer', 'solicitor', 'legal', 'counsel', 'attorney', 'barrister'],
          'Marketing': ['marketing', 'brand', 'creative', 'advertising', 'cmo', 'content'],
          'Sales': ['sales', 'business development', 'account executive', 'revenue'],
          'Human Resources': ['recruiter', 'hr ', 'people', 'talent', 'human resource'],
        };

        const { rows: noSector } = await pool.query(
          `SELECT c.id, array_agg(LOWER(p.current_title)) AS titles
           FROM companies c
           JOIN people p ON p.current_company_id = c.id
           WHERE c.tenant_id = $1 AND (c.sector IS NULL OR c.sector = '') AND p.current_title IS NOT NULL
           GROUP BY c.id`,
          [tenantId]
        );

        let sectorSet = 0;
        for (const co of noSector) {
          const allTitles = co.titles.join(' ');
          let bestSector = null, bestScore = 0;
          for (const [sector, keywords] of Object.entries(SECTOR_KEYWORDS)) {
            const score = keywords.filter(k => allTitles.includes(k)).length;
            if (score > bestScore) { bestScore = score; bestSector = sector; }
          }
          if (bestSector && bestScore >= 1) {
            await pool.query(`UPDATE companies SET sector = $1, updated_at = NOW() WHERE id = $2`, [bestSector, co.id]);
            sectorSet++;
          }
        }
        if (sectorSet) console.log(`    🏷️  Derived sector on ${sectorSet} companies`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: Link ALL unlinked people to companies (final sweep)
        // ═══════════════════════════════════════════════════════════════

        // 4a. By exact company name match (case-insensitive, trimmed)
        const { rowCount: nameLinked } = await pool.query(`
          UPDATE people p SET current_company_id = c.id, updated_at = NOW()
          FROM companies c
          WHERE p.tenant_id = $1 AND c.tenant_id = $1
            AND p.current_company_id IS NULL
            AND p.current_company_name IS NOT NULL
            AND TRIM(p.current_company_name) != ''
            AND LOWER(TRIM(c.name)) = LOWER(TRIM(p.current_company_name))
        `, [tenantId]);
        if (nameLinked) console.log(`    🔗 Linked ${nameLinked} people by company name`);

        // 4b. By email domain → company domain match
        const { rowCount: domainLinked } = await pool.query(`
          UPDATE people p SET current_company_id = c.id,
            current_company_name = COALESCE(NULLIF(p.current_company_name, ''), c.name),
            updated_at = NOW()
          FROM companies c
          WHERE p.tenant_id = $1 AND c.tenant_id = $1
            AND p.current_company_id IS NULL
            AND p.email IS NOT NULL AND p.email LIKE '%@%'
            AND c.domain IS NOT NULL
            AND LOWER(SPLIT_PART(p.email, '@', 2)) = LOWER(c.domain)
        `, [tenantId]);
        if (domainLinked) console.log(`    🔗 Linked ${domainLinked} people by email domain`);

        // 4c. Fuzzy: company name contains or is contained in people's company name
        // (handles "Google Inc." vs "Google", "JPMorgan Chase" vs "JPMorgan Chase & Co.")
        const { rowCount: fuzzyLinked } = await pool.query(`
          UPDATE people p SET current_company_id = sub.company_id, updated_at = NOW()
          FROM (
            SELECT DISTINCT ON (p2.id) p2.id AS person_id, c2.id AS company_id
            FROM people p2
            JOIN companies c2 ON c2.tenant_id = $1
            WHERE p2.tenant_id = $1
              AND p2.current_company_id IS NULL
              AND p2.current_company_name IS NOT NULL
              AND LENGTH(TRIM(p2.current_company_name)) >= 3
              AND (
                LOWER(TRIM(c2.name)) LIKE '%' || LOWER(TRIM(p2.current_company_name)) || '%'
                OR LOWER(TRIM(p2.current_company_name)) LIKE '%' || LOWER(TRIM(c2.name)) || '%'
              )
              AND LENGTH(TRIM(c2.name)) >= 3
            ORDER BY p2.id, LENGTH(c2.name) DESC
          ) sub
          WHERE p.id = sub.person_id
        `, [tenantId]);
        if (fuzzyLinked) console.log(`    🔗 Linked ${fuzzyLinked} people by fuzzy company name`);

        totalPeopleLinked += (nameLinked || 0) + (domainLinked || 0) + (fuzzyLinked || 0);

        // Report remaining unlinked
        const { rows: [remaining] } = await pool.query(`
          SELECT COUNT(*) AS cnt FROM people
          WHERE tenant_id = $1 AND current_company_id IS NULL
            AND current_company_name IS NOT NULL AND TRIM(current_company_name) != ''
        `, [tenantId]);
        if (parseInt(remaining.cnt) > 0) {
          console.log(c.dim(`    ℹ ${remaining.cnt} people still unlinked (company name not in companies table)`));
        }
      }

      totalCompaniesCreated += created + domainCreated;
      totalDomainCompanies += domainCreated;
    }

    console.log(`\n  ════════════════════════════════════`);
    console.log(`  🏢 Companies created:   ${totalCompaniesCreated}`);
    console.log(`  👥 People linked:        ${totalPeopleLinked}`);
    console.log(`  📧 From email domains:   ${totalDomainCompanies}`);

    return {
      records_in: totalPeopleLinked,
      records_out: totalCompaniesCreated,
      metadata: { companies_created: totalCompaniesCreated, people_linked: totalPeopleLinked, domain_companies: totalDomainCompanies }
    };
  });

  await pool.end();
}

main().catch(err => {
  console.error(c.red('FATAL: ' + err.message));
  process.exit(1);
});
