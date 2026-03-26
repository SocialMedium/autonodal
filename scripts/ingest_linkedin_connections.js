#!/usr/bin/env node
// ============================================================================
// Ingest LinkedIn Connections CSV — batch INSERT for speed
// Usage: node scripts/ingest_linkedin_connections.js <csv_path> <user_email>
// ============================================================================

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const BATCH_SIZE = 50;

function parseCSV(line) {
  const r = []; let c = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') q = !q;
    else if (ch === ',' && !q) { r.push(c.trim()); c = ''; }
    else c += ch;
  }
  r.push(c.trim());
  return r;
}

async function main() {
  const csvPath = process.argv[2] || path.join(__dirname, '..', 'data', 'sophie_linkedin_connections.csv');
  const userEmail = process.argv[3] || 'sophiec@mitchellake.com';

  console.log('═══════════════════════════════════════════════════════════');
  console.log(` LinkedIn Connections Import`);
  console.log(` File: ${csvPath}`);
  console.log(` User: ${userEmail}`);
  console.log('═══════════════════════════════════════════════════════════');

  // Find user
  const { rows: [user] } = await pool.query('SELECT id FROM users WHERE email = $1', [userEmail]);
  if (!user) { console.error('User not found:', userEmail); process.exit(1); }
  const userId = user.id;
  console.log(`  User ID: ${userId}`);

  // Read CSV
  const raw = fs.readFileSync(csvPath, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const allLines = raw.split('\n');

  // Find header row
  let headerIdx = 0;
  for (let i = 0; i < Math.min(allLines.length, 20); i++) {
    if (allLines[i].toLowerCase().includes('first name')) { headerIdx = i; break; }
  }
  const lines = allLines.slice(headerIdx).filter(l => l.trim());
  const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
  console.log(`  Headers: ${headers.join(', ')}`);

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSV(lines[i]);
    const row = {};
    headers.forEach((h, idx) => { row[h] = vals[idx] || ''; });
    rows.push(row);
  }
  console.log(`  Total rows: ${rows.length}\n`);

  // Load existing people for matching — build indexes
  console.log('  Loading existing people...');
  const { rows: dbPeople } = await pool.query(
    `SELECT id, full_name, linkedin_url FROM people WHERE tenant_id = $1`, [TENANT_ID]
  );
  const linkedinIndex = new Map();
  const nameIndex = new Map();
  for (const p of dbPeople) {
    if (p.linkedin_url) {
      const slug = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1];
      if (slug) linkedinIndex.set(slug, p.id);
    }
    const norm = (p.full_name || '').toLowerCase().trim();
    if (norm) {
      if (!nameIndex.has(norm)) nameIndex.set(norm, []);
      nameIndex.get(norm).push(p.id);
    }
  }
  console.log(`  Loaded ${dbPeople.length} people (${linkedinIndex.size} with LinkedIn URLs)\n`);

  const stats = { total: 0, matched: 0, created: 0, proximity: 0, skipped: 0 };

  // Process in batches
  for (let batch = 0; batch < rows.length; batch += BATCH_SIZE) {
    const chunk = rows.slice(batch, batch + BATCH_SIZE);
    const toCreate = [];
    const toProximity = [];

    for (const row of chunk) {
      const firstName = row['First Name'] || '';
      const lastName = row['Last Name'] || '';
      const fullName = `${firstName} ${lastName}`.trim();
      const linkedinUrl = row['URL'] || '';
      const company = row['Company'] || '';
      const position = row['Position'] || '';
      const email = row['Email Address'] || '';

      if (!fullName || fullName.length < 2) { stats.skipped++; continue; }
      stats.total++;

      // Match by LinkedIn URL
      let personId = null;
      const slug = linkedinUrl ? (linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1] : null;
      if (slug && linkedinIndex.has(slug)) {
        personId = linkedinIndex.get(slug);
        stats.matched++;
      }

      // Match by exact name (only if unique)
      if (!personId) {
        const cands = nameIndex.get(fullName.toLowerCase().trim()) || [];
        if (cands.length === 1) {
          personId = cands[0];
          stats.matched++;
        }
      }

      if (!personId) {
        toCreate.push({ fullName, firstName, lastName, position, company, linkedinUrl, email });
      } else {
        toProximity.push(personId);
      }
    }

    // Batch create new people
    for (const p of toCreate) {
      try {
        const { rows: [newP] } = await pool.query(
          `INSERT INTO people (full_name, first_name, last_name, current_title, current_company_name, linkedin_url, email, source, created_by, tenant_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,'linkedin_import',$8,$9)
           ON CONFLICT DO NOTHING RETURNING id`,
          [p.fullName, p.firstName, p.lastName, p.position || null, p.company || null, p.linkedinUrl || null, p.email || null, userId, TENANT_ID]
        );
        if (newP) {
          toProximity.push(newP.id);
          stats.created++;
          // Add to indexes for future matching within this run
          const norm = p.fullName.toLowerCase().trim();
          if (!nameIndex.has(norm)) nameIndex.set(norm, []);
          nameIndex.get(norm).push(newP.id);
          if (p.linkedinUrl) {
            const s = (p.linkedinUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1];
            if (s) linkedinIndex.set(s, newP.id);
          }
        }
      } catch (e) { stats.skipped++; }
    }

    // Batch create proximity
    for (const pid of toProximity) {
      try {
        await pool.query(
          `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
           VALUES ($1, $2, 'linkedin_connection', 0.5, 'linkedin_import', $3)
           ON CONFLICT (person_id, team_member_id) DO UPDATE SET relationship_strength = GREATEST(team_proximity.relationship_strength, 0.5)`,
          [pid, userId, TENANT_ID]
        );
        stats.proximity++;
      } catch (e) {}
    }

    if ((batch + BATCH_SIZE) % 1000 < BATCH_SIZE) {
      console.log(`  Progress: ${Math.min(batch + BATCH_SIZE, rows.length)}/${rows.length} — ${stats.created} created, ${stats.matched} matched`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════════════');
  console.log(` Total: ${stats.total}`);
  console.log(` Matched: ${stats.matched}`);
  console.log(` Created: ${stats.created}`);
  console.log(` Proximity links: ${stats.proximity}`);
  console.log(` Skipped: ${stats.skipped}`);
  console.log('═══════════════════════════════════════════════════════════');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
