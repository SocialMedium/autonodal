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

function domainToCompanyName(domain) {
  // Strip common TLDs and format
  const parts = domain.split('.');
  if (parts.length < 2) return null;
  const name = parts[0];
  if (name.length < 2) return null;
  // Capitalize
  return name.charAt(0).toUpperCase() + name.slice(1);
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  COMPANY EXTRACTION FROM CONTACTS');
  console.log('═══════════════════════════════════════════════════════════════');
  if (DRY_RUN) console.log(c.yellow('  ⚠ DRY RUN — no writes'));

  await runJob(pool, 'extract_companies', async () => {
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
          const { rows: [newCo] } = await pool.query(
            `INSERT INTO companies (name, source, tenant_id, created_at, updated_at)
             VALUES ($1, 'contact_extraction', $2, NOW(), NOW())
             ON CONFLICT DO NOTHING
             RETURNING id`,
            [row.name.trim(), tenantId]
          );
          if (newCo) {
            companyId = newCo.id;
            created++;
          } else {
            // Race condition — try to get it
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
      }

      totalCompaniesCreated += created + domainCreated;
      totalPeopleLinked += linked;
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
