#!/usr/bin/env node
/**
 * Fuzzy Dedup — Find and merge duplicate people records
 *
 * Matching strategies (in order of confidence):
 * 1. Same email (exact match) — highest confidence
 * 2. Same LinkedIn URL — high confidence
 * 3. Same Ezekia source_id — high confidence
 * 4. Same full_name + same company — medium confidence (auto-merge)
 * 5. Same full_name, different company — low confidence (report only)
 *
 * Usage:
 *   node scripts/dedup_people.js              # Find dupes, report only
 *   node scripts/dedup_people.js --merge      # Auto-merge high-confidence dupes
 *   node scripts/dedup_people.js --company=X  # Only check people at company X
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const ML_TENANT = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const LOG = (icon, msg) => console.log(`${icon}  ${msg}`);

async function mergePeople(keepId, removeId) {
  // Move all relationships from removeId to keepId
  await pool.query('UPDATE interactions SET person_id = $1 WHERE person_id = $2', [keepId, removeId]);
  await pool.query('UPDATE person_signals SET person_id = $1 WHERE person_id = $2', [keepId, removeId]);

  // team_proximity — try to move, delete on conflict
  const { rows: tps } = await pool.query('SELECT id, team_member_id, relationship_type FROM team_proximity WHERE person_id = $1', [removeId]);
  for (const tp of tps) {
    try {
      await pool.query('UPDATE team_proximity SET person_id = $1 WHERE id = $2', [keepId, tp.id]);
    } catch (e) {
      await pool.query('DELETE FROM team_proximity WHERE id = $1', [tp.id]);
    }
  }

  // pipeline_contacts — try to move, delete on conflict
  const { rows: pcs } = await pool.query('SELECT id, search_id FROM pipeline_contacts WHERE person_id = $1', [removeId]);
  for (const pc of pcs) {
    try {
      await pool.query('UPDATE pipeline_contacts SET person_id = $1 WHERE id = $2', [keepId, pc.id]);
    } catch (e) {
      await pool.query('DELETE FROM pipeline_contacts WHERE id = $1', [pc.id]);
    }
  }

  // Copy missing fields from removeId to keepId
  const { rows: [keep] } = await pool.query('SELECT * FROM people WHERE id = $1', [keepId]);
  const { rows: [remove] } = await pool.query('SELECT * FROM people WHERE id = $1', [removeId]);
  if (keep && remove) {
    const updates = [];
    const params = [keepId];
    let idx = 1;
    const fields = ['email', 'phone', 'linkedin_url', 'current_title', 'current_company_name',
      'current_company_id', 'location', 'country', 'seniority_level', 'bio', 'headline',
      'career_history', 'education', 'source_id'];
    for (const f of fields) {
      if (!keep[f] && remove[f]) {
        idx++;
        updates.push(`${f} = $${idx}`);
        params.push(remove[f]);
      }
    }
    if (updates.length) {
      await pool.query(`UPDATE people SET ${updates.join(', ')}, updated_at = NOW() WHERE id = $1`, params);
    }
  }

  // Delete the duplicate
  await pool.query('DELETE FROM person_scores WHERE person_id = $1', [removeId]);
  await pool.query('DELETE FROM team_proximity WHERE person_id = $1', [removeId]);
  await pool.query('DELETE FROM interactions WHERE person_id = $1', [removeId]);
  await pool.query('DELETE FROM pipeline_contacts WHERE person_id = $1', [removeId]);
  await pool.query('DELETE FROM person_signals WHERE person_id = $1', [removeId]);
  await pool.query('DELETE FROM people WHERE id = $1', [removeId]);
}

function pickPrimary(a, b) {
  // Keep the one with more data
  const scoreA = (a.email ? 2 : 0) + (a.linkedin_url ? 2 : 0) + (a.current_company_id ? 1 : 0)
    + (a.current_title ? 1 : 0) + (a.interaction_count || 0) + (a.proximity_count || 0);
  const scoreB = (b.email ? 2 : 0) + (b.linkedin_url ? 2 : 0) + (b.current_company_id ? 1 : 0)
    + (b.current_title ? 1 : 0) + (b.interaction_count || 0) + (b.proximity_count || 0);
  return scoreA >= scoreB ? { keep: a, remove: b } : { keep: b, remove: a };
}

async function main() {
  const args = process.argv.slice(2);
  const doMerge = args.includes('--merge');
  const companyFilter = args.find(a => a.startsWith('--company='))?.split('=')[1];

  LOG('🔍', '═══ Fuzzy Dedup — Finding duplicate people ═══');
  if (doMerge) LOG('⚠️', 'MERGE MODE — will auto-merge high-confidence duplicates');
  else LOG('ℹ️', 'REPORT MODE — add --merge to auto-merge');

  let totalFound = 0, totalMerged = 0;

  // 1. Email duplicates (highest confidence)
  LOG('📧', 'Finding email duplicates...');
  const { rows: emailDupes } = await pool.query(`
    SELECT email, array_agg(id) as ids, array_agg(full_name) as names, COUNT(*) as cnt
    FROM people
    WHERE email IS NOT NULL AND email != '' AND tenant_id = $1
    ${companyFilter ? "AND current_company_name ILIKE '%" + companyFilter.replace(/'/g, "''") + "%'" : ""}
    GROUP BY email HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
  `, [ML_TENANT]);
  LOG('📧', `  ${emailDupes.length} email duplicate groups`);
  for (const d of emailDupes) {
    totalFound++;
    LOG('  ', `${d.email}: ${d.names.join(' / ')} (${d.cnt} records)`);
    if (doMerge) {
      const ids = d.ids;
      // Get full records to pick primary
      const { rows } = await pool.query(`
        SELECT p.*, (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) as interaction_count,
               (SELECT COUNT(*) FROM team_proximity tp WHERE tp.person_id = p.id) as proximity_count
        FROM people p WHERE p.id = ANY($1)
      `, [ids]);
      let primary = rows[0];
      for (let i = 1; i < rows.length; i++) {
        const { keep, remove } = pickPrimary(primary, rows[i]);
        primary = keep;
        await mergePeople(keep.id, remove.id);
        totalMerged++;
        LOG('✅', `    Merged ${remove.full_name} (${remove.id.slice(0, 8)}) → ${keep.full_name} (${keep.id.slice(0, 8)})`);
      }
    }
  }

  // 2. LinkedIn URL duplicates
  LOG('🔗', 'Finding LinkedIn URL duplicates...');
  const { rows: liDupes } = await pool.query(`
    SELECT LOWER(REGEXP_REPLACE(linkedin_url, '/+$', '')) as li_url,
           array_agg(id) as ids, array_agg(full_name) as names, COUNT(*) as cnt
    FROM people
    WHERE linkedin_url IS NOT NULL AND linkedin_url != '' AND tenant_id = $1
    ${companyFilter ? "AND current_company_name ILIKE '%" + companyFilter.replace(/'/g, "''") + "%'" : ""}
    GROUP BY LOWER(REGEXP_REPLACE(linkedin_url, '/+$', '')) HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
  `, [ML_TENANT]);
  LOG('🔗', `  ${liDupes.length} LinkedIn duplicate groups`);
  for (const d of liDupes) {
    totalFound++;
    LOG('  ', `${d.li_url?.slice(0, 50)}: ${d.names.join(' / ')}`);
    if (doMerge) {
      const { rows } = await pool.query(`
        SELECT p.*, (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) as interaction_count,
               (SELECT COUNT(*) FROM team_proximity tp WHERE tp.person_id = p.id) as proximity_count
        FROM people p WHERE p.id = ANY($1)
      `, [d.ids]);
      let primary = rows[0];
      for (let i = 1; i < rows.length; i++) {
        const { keep, remove } = pickPrimary(primary, rows[i]);
        primary = keep;
        await mergePeople(keep.id, remove.id);
        totalMerged++;
        LOG('✅', `    Merged ${remove.full_name} → ${keep.full_name}`);
      }
    }
  }

  // 3. Exact name + exact company duplicates (medium confidence)
  LOG('👥', 'Finding name + company duplicates...');
  const { rows: nameDupes } = await pool.query(`
    SELECT LOWER(TRIM(full_name)) as name_key, LOWER(TRIM(current_company_name)) as co_key,
           array_agg(id) as ids, array_agg(full_name) as names, COUNT(*) as cnt
    FROM people
    WHERE full_name IS NOT NULL AND current_company_name IS NOT NULL AND tenant_id = $1
    ${companyFilter ? "AND current_company_name ILIKE '%" + companyFilter.replace(/'/g, "''") + "%'" : ""}
    GROUP BY LOWER(TRIM(full_name)), LOWER(TRIM(current_company_name))
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT 200
  `, [ML_TENANT]);
  LOG('👥', `  ${nameDupes.length} name+company duplicate groups`);
  for (const d of nameDupes.slice(0, 50)) {
    totalFound++;
    LOG('  ', `${d.names[0]} @ ${d.co_key} (${d.cnt} records)`);
    if (doMerge) {
      const { rows } = await pool.query(`
        SELECT p.*, (SELECT COUNT(*) FROM interactions i WHERE i.person_id = p.id) as interaction_count,
               (SELECT COUNT(*) FROM team_proximity tp WHERE tp.person_id = p.id) as proximity_count
        FROM people p WHERE p.id = ANY($1)
      `, [d.ids]);
      let primary = rows[0];
      for (let i = 1; i < rows.length; i++) {
        const { keep, remove } = pickPrimary(primary, rows[i]);
        primary = keep;
        await mergePeople(keep.id, remove.id);
        totalMerged++;
        LOG('✅', `    Merged → ${keep.full_name}`);
      }
    }
  }

  // 4. Company dedup
  LOG('🏢', 'Finding duplicate companies...');
  const { rows: coDupes } = await pool.query(`
    SELECT LOWER(TRIM(name)) as name_key, array_agg(id) as ids, array_agg(name) as names, COUNT(*) as cnt
    FROM companies WHERE tenant_id = $1
    GROUP BY LOWER(TRIM(name)) HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC LIMIT 100
  `, [ML_TENANT]);
  LOG('🏢', `  ${coDupes.length} duplicate company groups`);
  for (const d of coDupes.slice(0, 30)) {
    LOG('  ', `${d.names[0]} (${d.cnt} records)`);
    if (doMerge) {
      // Keep the one with the most people linked
      const { rows } = await pool.query(`
        SELECT c.id, c.name, c.is_client, c.sector, c.domain,
               (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) as people_count,
               (SELECT COUNT(*) FROM accounts a WHERE a.company_id = c.id) as account_count
        FROM companies c WHERE c.id = ANY($1) ORDER BY
          (SELECT COUNT(*) FROM people p WHERE p.current_company_id = c.id) DESC,
          CASE WHEN c.is_client THEN 0 ELSE 1 END
      `, [d.ids]);
      const primary = rows[0];
      for (let i = 1; i < rows.length; i++) {
        const dupe = rows[i];
        // Relink everything
        await pool.query('UPDATE people SET current_company_id = $1 WHERE current_company_id = $2', [primary.id, dupe.id]);
        await pool.query('UPDATE accounts SET company_id = $1 WHERE company_id = $2', [primary.id, dupe.id]);
        await pool.query('UPDATE signal_events SET company_id = $1 WHERE company_id = $2', [primary.id, dupe.id]);
        // Copy missing fields
        if (!primary.sector && dupe.sector) await pool.query('UPDATE companies SET sector = $1 WHERE id = $2', [dupe.sector, primary.id]);
        if (!primary.domain && dupe.domain) await pool.query('UPDATE companies SET domain = $1 WHERE id = $2', [dupe.domain, primary.id]);
        if (!primary.is_client && dupe.is_client) await pool.query('UPDATE companies SET is_client = true WHERE id = $1', [primary.id]);
        await pool.query('DELETE FROM companies WHERE id = $1', [dupe.id]);
        totalMerged++;
        LOG('✅', `    Merged company ${dupe.name} → ${primary.name}`);
      }
    }
  }

  LOG('🔍', '═══ Dedup Complete ═══');
  LOG('📊', `Duplicate groups found: ${totalFound}`);
  LOG('✅', `Records merged: ${totalMerged}`);
  if (!doMerge && totalFound > 0) LOG('💡', 'Run with --merge to auto-merge high-confidence duplicates');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
