#!/usr/bin/env node
/**
 * MitchelLake Signal Intelligence Platform
 * LinkedIn Connections Import Script
 * 
 * Imports LinkedIn connections and matches against Ezekia candidates
 * to populate team_proximity and boost lead scores.
 * 
 * Usage: node scripts/import_linkedin_connections.js <path-to-Connections.csv> [--user-id=<uuid>]
 */

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ═══════════════════════════════════════════════════════════════════
// CSV PARSER (lightweight, no dependency)
// ═══════════════════════════════════════════════════════════════════

function parseCSV(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n');
  
  // Find header line (skip LinkedIn notes at top)
  let headerIndex = -1;
  for (let i = 0; i < Math.min(lines.length, 10); i++) {
    if (lines[i].startsWith('First Name')) {
      headerIndex = i;
      break;
    }
  }
  
  if (headerIndex === -1) {
    throw new Error('Could not find CSV header row');
  }
  
  const headers = parseCSVLine(lines[headerIndex]);
  const rows = [];
  
  for (let i = headerIndex + 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h.trim()] = (values[idx] || '').trim();
    });
    rows.push(row);
  }
  
  return rows;
}

function parseCSVLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  values.push(current);
  return values;
}

// ═══════════════════════════════════════════════════════════════════
// MATCHING ENGINE
// ═══════════════════════════════════════════════════════════════════

function normalizeLinkedInUrl(url) {
  if (!url) return null;
  url = url.trim().toLowerCase();
  // Remove trailing slashes, query params, fragments
  url = url.replace(/\/+$/, '').split('?')[0].split('#')[0];
  // Extract the profile slug
  const match = url.match(/linkedin\.com\/in\/([^\/]+)/);
  return match ? match[1] : null;
}

function normalizeName(firstName, lastName) {
  return `${(firstName || '').toLowerCase().trim()} ${(lastName || '').toLowerCase().trim()}`.trim();
}

function parseConnectedDate(dateStr) {
  if (!dateStr) return null;
  // Format: "18 Feb 2026"
  try {
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d.toISOString();
  } catch {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════
// MAIN IMPORT
// ═══════════════════════════════════════════════════════════════════

async function importConnections(csvPath, userId) {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   MitchelLake LinkedIn Connections Import                     ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log();

  // ─── Parse CSV ───
  console.log(`Reading ${csvPath}...`);
  const connections = parseCSV(csvPath);
  console.log(`  Parsed ${connections.length} connections`);
  console.log();

  // ─── Get or create user ───
  let importUserId = userId;
  if (!importUserId) {
    const userResult = await pool.query('SELECT id FROM users LIMIT 1');
    if (userResult.rows.length > 0) {
      importUserId = userResult.rows[0].id;
      console.log(`  Using user: ${importUserId}`);
    } else {
      console.log('  WARNING: No users found, team_proximity team_member_id will be null');
    }
  }

  // ─── Load existing people for matching ───
  console.log('Loading existing people from database...');
  const peopleResult = await pool.query(`
    SELECT id, full_name, first_name, last_name, linkedin_url, 
           current_company_name, email, source_id
    FROM people 
    WHERE full_name IS NOT NULL AND full_name != ''
  `);
  const dbPeople = peopleResult.rows;
  console.log(`  Loaded ${dbPeople.length} people`);

  // Build lookup indexes
  const linkedinIndex = new Map(); // slug -> person
  const nameIndex = new Map();     // normalized name -> [persons]
  const emailIndex = new Map();    // email -> person

  for (const p of dbPeople) {
    // LinkedIn URL index
    if (p.linkedin_url) {
      const slug = normalizeLinkedInUrl(p.linkedin_url);
      if (slug) linkedinIndex.set(slug, p);
    }
    
    // Name index (can have multiple people with same name)
    const normName = normalizeName(p.first_name || p.full_name?.split(' ')[0], 
                                    p.last_name || p.full_name?.split(' ').slice(1).join(' '));
    if (normName.length > 1) {
      if (!nameIndex.has(normName)) nameIndex.set(normName, []);
      nameIndex.get(normName).push(p);
    }
    
    // Email index
    if (p.email) {
      emailIndex.set(p.email.toLowerCase(), p);
    }
  }
  
  console.log(`  LinkedIn URL index: ${linkedinIndex.size} entries`);
  console.log(`  Name index: ${nameIndex.size} unique names`);
  console.log(`  Email index: ${emailIndex.size} entries`);
  console.log();

  // ─── Ensure team_proximity table exists ───
  await pool.query(`
    CREATE TABLE IF NOT EXISTS team_proximity (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      person_id UUID REFERENCES people(id) ON DELETE CASCADE,
      team_member_id UUID REFERENCES users(id),
      proximity_type VARCHAR(50) NOT NULL,
      source VARCHAR(50) NOT NULL,
      strength NUMERIC(3,2) DEFAULT 0.5,
      context TEXT,
      connected_at TIMESTAMPTZ,
      metadata JSONB DEFAULT '{}',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(person_id, team_member_id, proximity_type, source)
    )
  `);
  
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_team_prox_person ON team_proximity(person_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_team_prox_user ON team_proximity(team_member_id)`);

  // ─── Ensure linkedin_connections table for unmatched ───
  await pool.query(`
    CREATE TABLE IF NOT EXISTS linkedin_connections (
      id SERIAL PRIMARY KEY,
      team_member_id UUID REFERENCES users(id),
      first_name VARCHAR(255),
      last_name VARCHAR(255),
      full_name VARCHAR(255),
      linkedin_url TEXT,
      linkedin_slug VARCHAR(255),
      email VARCHAR(255),
      company VARCHAR(255),
      position VARCHAR(255),
      connected_at TIMESTAMPTZ,
      matched_person_id UUID REFERENCES people(id),
      match_method VARCHAR(50),
      match_confidence NUMERIC(3,2),
      imported_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(linkedin_slug)
    )
  `);
  
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_li_conn_slug ON linkedin_connections(linkedin_slug)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_li_conn_matched ON linkedin_connections(matched_person_id)`);

  // ─── Process connections ───
  console.log('Processing connections...');
  
  const stats = {
    total: connections.length,
    matched_linkedin: 0,
    matched_email: 0,
    matched_name_company: 0,
    matched_name_only: 0,
    unmatched: 0,
    proximity_created: 0,
    proximity_updated: 0,
    connections_stored: 0,
    errors: 0
  };

  let processed = 0;

  for (const conn of connections) {
    try {
      processed++;
      
      const firstName = conn['First Name'] || '';
      const lastName = conn['Last Name'] || '';
      const fullName = `${firstName} ${lastName}`.trim();
      const linkedinUrl = conn['URL'] || '';
      const email = conn['Email Address'] || '';
      const company = conn['Company'] || '';
      const position = conn['Position'] || '';
      const connectedOn = parseConnectedDate(conn['Connected On']);
      const slug = normalizeLinkedInUrl(linkedinUrl);
      
      if (!fullName || fullName.length < 2) continue;

      // ─── Match against existing people ───
      let matchedPerson = null;
      let matchMethod = null;
      let matchConfidence = 0;

      // Priority 1: LinkedIn URL match (highest confidence)
      if (slug && linkedinIndex.has(slug)) {
        matchedPerson = linkedinIndex.get(slug);
        matchMethod = 'linkedin_url';
        matchConfidence = 0.99;
        stats.matched_linkedin++;
      }
      
      // Priority 2: Email match
      if (!matchedPerson && email) {
        const emailMatch = emailIndex.get(email.toLowerCase());
        if (emailMatch) {
          matchedPerson = emailMatch;
          matchMethod = 'email';
          matchConfidence = 0.95;
          stats.matched_email++;
        }
      }
      
      // Priority 3: Name + Company match
      if (!matchedPerson) {
        const normName = normalizeName(firstName, lastName);
        const candidates = nameIndex.get(normName) || [];
        
        if (candidates.length === 1) {
          // Unique name match
          matchedPerson = candidates[0];
          matchMethod = 'name_unique';
          matchConfidence = 0.80;
          stats.matched_name_only++;
        } else if (candidates.length > 1 && company) {
          // Multiple matches - try company disambiguation
          const companyLower = company.toLowerCase();
          const companyMatch = candidates.find(p => 
            p.current_company_name && 
            p.current_company_name.toLowerCase().includes(companyLower)
          );
          if (companyMatch) {
            matchedPerson = companyMatch;
            matchMethod = 'name_company';
            matchConfidence = 0.90;
            stats.matched_name_company++;
          }
        }
      }

      if (!matchedPerson) {
        stats.unmatched++;
      }

      // ─── Store in linkedin_connections table ───
      try {
        await pool.query(`
          INSERT INTO linkedin_connections 
            (team_member_id, first_name, last_name, full_name, linkedin_url, linkedin_slug,
             email, company, position, connected_at, matched_person_id, match_method, match_confidence)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          ON CONFLICT (linkedin_slug) DO UPDATE SET
            company = EXCLUDED.company,
            position = EXCLUDED.position,
            matched_person_id = EXCLUDED.matched_person_id,
            match_method = EXCLUDED.match_method,
            match_confidence = EXCLUDED.match_confidence,
            imported_at = NOW()
        `, [
          importUserId, firstName, lastName, fullName, linkedinUrl, slug,
          email || null, company || null, position || null, connectedOn,
          matchedPerson?.id || null, matchMethod, matchConfidence || null
        ]);
        stats.connections_stored++;
      } catch (e) {
        // Skip duplicate slug errors silently
        if (!e.message.includes('duplicate')) {
          stats.errors++;
        }
      }

      // ─── Create team_proximity record if matched ───
      if (matchedPerson && importUserId) {
        try {
          // Calculate strength based on connection age
          let strength = 0.5; // base
          if (connectedOn) {
            const yearsConnected = (Date.now() - new Date(connectedOn).getTime()) / (365.25 * 24 * 60 * 60 * 1000);
            if (yearsConnected > 5) strength = 0.8;
            else if (yearsConnected > 2) strength = 0.7;
            else if (yearsConnected > 1) strength = 0.6;
          }
          // Boost for higher match confidence
          strength = Math.min(1.0, strength + (matchConfidence - 0.5) * 0.2);

          const result = await pool.query(`
            INSERT INTO team_proximity 
              (person_id, team_member_id, proximity_type, source, strength, context, connected_at, metadata)
            VALUES ($1, $2, 'linkedin_connection', 'linkedin_import', $3, $4, $5, $6)
            ON CONFLICT (person_id, team_member_id, proximity_type, source) DO UPDATE SET
              strength = GREATEST(team_proximity.strength, EXCLUDED.strength),
              context = EXCLUDED.context,
              metadata = EXCLUDED.metadata,
              updated_at = NOW()
            RETURNING (xmax = 0) as is_insert
          `, [
            matchedPerson.id,
            importUserId,
            strength.toFixed(2),
            `${position} @ ${company}`,
            connectedOn,
            JSON.stringify({
              linkedin_url: linkedinUrl,
              match_method: matchMethod,
              match_confidence: matchConfidence,
              connected_on: conn['Connected On']
            })
          ]);
          
          if (result.rows[0]?.is_insert) {
            stats.proximity_created++;
          } else {
            stats.proximity_updated++;
          }
        } catch (e) {
          if (!e.message.includes('duplicate')) {
            stats.errors++;
          }
        }
      }

      // Also update linkedin_url on matched person if they don't have one
      if (matchedPerson && linkedinUrl && !matchedPerson.linkedin_url) {
        try {
          await pool.query(
            'UPDATE people SET linkedin_url = $1, updated_at = NOW() WHERE id = $2 AND linkedin_url IS NULL',
            [linkedinUrl, matchedPerson.id]
          );
        } catch (e) { /* ignore */ }
      }

      // Progress logging
      if (processed % 1000 === 0) {
        const matchRate = ((stats.matched_linkedin + stats.matched_email + stats.matched_name_company + stats.matched_name_only) / processed * 100).toFixed(1);
        console.log(`  ${processed}/${stats.total} processed | Match rate: ${matchRate}% | Proximity: ${stats.proximity_created} created`);
      }
      
    } catch (error) {
      stats.errors++;
      if (stats.errors <= 5) {
        console.error(`  Error processing ${conn['First Name']} ${conn['Last Name']}: ${error.message}`);
      }
    }
  }

  // ─── Final Report ───
  console.log();
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   IMPORT COMPLETE                                            ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  console.log();
  console.log('=== MATCHING RESULTS ===');
  console.log(`  Total connections:     ${stats.total.toLocaleString()}`);
  console.log(`  ─────────────────────────────`);
  console.log(`  LinkedIn URL match:    ${stats.matched_linkedin.toLocaleString()} (highest confidence)`);
  console.log(`  Email match:           ${stats.matched_email.toLocaleString()}`);
  console.log(`  Name + Company match:  ${stats.matched_name_company.toLocaleString()}`);
  console.log(`  Name only match:       ${stats.matched_name_only.toLocaleString()}`);
  console.log(`  Unmatched:             ${stats.unmatched.toLocaleString()}`);
  const totalMatched = stats.matched_linkedin + stats.matched_email + stats.matched_name_company + stats.matched_name_only;
  console.log(`  ─────────────────────────────`);
  console.log(`  MATCH RATE:            ${(totalMatched / stats.total * 100).toFixed(1)}%`);
  console.log();
  console.log('=== DATABASE UPDATES ===');
  console.log(`  Connections stored:    ${stats.connections_stored.toLocaleString()}`);
  console.log(`  Proximity created:     ${stats.proximity_created.toLocaleString()}`);
  console.log(`  Proximity updated:     ${stats.proximity_updated.toLocaleString()}`);
  console.log(`  Errors:                ${stats.errors}`);
  console.log();

  // Quick stats on match quality
  const matchStats = await pool.query(`
    SELECT match_method, COUNT(*) as count, 
           ROUND(AVG(match_confidence)::numeric, 2) as avg_confidence
    FROM linkedin_connections 
    WHERE matched_person_id IS NOT NULL
    GROUP BY match_method
    ORDER BY count DESC
  `);
  
  if (matchStats.rows.length > 0) {
    console.log('=== MATCH QUALITY ===');
    for (const row of matchStats.rows) {
      console.log(`  ${row.match_method}: ${row.count} matches (avg confidence: ${row.avg_confidence})`);
    }
    console.log();
  }

  // Top matched companies
  const topMatched = await pool.query(`
    SELECT company, COUNT(*) as count 
    FROM linkedin_connections 
    WHERE matched_person_id IS NOT NULL AND company IS NOT NULL AND company != ''
    GROUP BY company 
    ORDER BY count DESC 
    LIMIT 10
  `);
  
  if (topMatched.rows.length > 0) {
    console.log('=== TOP MATCHED COMPANIES ===');
    for (const row of topMatched.rows) {
      console.log(`  ${row.count.toString().padStart(3)}  ${row.company}`);
    }
  }

  await pool.end();
}

// ═══════════════════════════════════════════════════════════════════
// CLI
// ═══════════════════════════════════════════════════════════════════

const args = process.argv.slice(2);
let csvPath = args.find(a => !a.startsWith('--'));
const userIdArg = args.find(a => a.startsWith('--user-id='));
const userId = userIdArg ? userIdArg.split('=')[1] : null;

if (!csvPath) {
  // Default to common locations
  const defaults = [
    'Connections.csv',
    'Connections_A.csv',
    '../Connections.csv',
    path.join(process.env.HOME || '', 'Downloads', 'Connections.csv')
  ];
  csvPath = defaults.find(p => fs.existsSync(p));
}

if (!csvPath || !fs.existsSync(csvPath)) {
  console.error('Usage: node scripts/import_linkedin_connections.js <path-to-Connections.csv>');
  console.error('');
  console.error('No connections file found. Please provide the path to your LinkedIn Connections.csv export.');
  process.exit(1);
}

importConnections(csvPath, userId).catch(err => {
  console.error('Fatal error:', err);
  pool.end();
  process.exit(1);
});
