#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/link_events.js - Link events to known companies and people
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const db = require('../lib/db');

// ═══════════════════════════════════════════════════════════════════════════════
// COMPANY LINKING
// ═══════════════════════════════════════════════════════════════════════════════

const COMPANY_SUFFIXES = /\b(Ltd|Inc|Pty|GmbH|PLC|AG|SE|Corp|Co|LLC|Limited|Corporation)\b/i;

/**
 * Extract company-like noun phrases from text
 */
function extractCompanyCandidates(text) {
  if (!text) return [];
  const candidates = new Set();

  // Multi-word capitalised phrases (2-4 words)
  const capPattern = /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\b/g;
  let match;
  while ((match = capPattern.exec(text)) !== null) {
    const phrase = match[1].trim();
    if (phrase.length > 3 && phrase.length < 60) {
      candidates.add(phrase);
    }
  }

  // Phrases with company suffixes
  const suffixPattern = new RegExp(`([A-Z][\\w\\s]{2,40})\\s+(?:${COMPANY_SUFFIXES.source})`, 'gi');
  while ((match = suffixPattern.exec(text)) !== null) {
    candidates.add(match[0].trim());
  }

  // Filter out common false positives
  const stopWords = new Set([
    'The', 'This', 'That', 'These', 'Those', 'Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday', 'Sunday', 'January', 'February', 'March',
    'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November',
    'December', 'New York', 'San Francisco', 'Los Angeles'
  ]);

  return [...candidates].filter(c => !stopWords.has(c));
}

async function linkCompanies() {
  console.log('🔗 Linking events to companies...');

  const events = await db.queryAll(`
    SELECT e.id, e.title, e.description, e.tenant_id
    FROM events e
    LEFT JOIN event_company_links ecl ON ecl.event_id = e.id
    WHERE e.fetched_at > NOW() - INTERVAL '3 hours'
      AND ecl.id IS NULL
    ORDER BY e.created_at DESC
    LIMIT 200
  `);

  console.log(`   Found ${events.length} events to process`);

  let totalLinks = 0;

  for (const event of events) {
    const combined = `${event.title || ''} ${event.description || ''}`;
    const candidates = extractCompanyCandidates(combined);

    for (const name of candidates) {
      const company = await db.queryOne(
        `SELECT id FROM companies WHERE name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
        [`%${name}%`, event.tenant_id]
      );

      if (company) {
        try {
          await db.query(
            `INSERT INTO event_company_links (event_id, company_id, link_type)
             VALUES ($1, $2, 'mentioned')
             ON CONFLICT (event_id, company_id) DO NOTHING`,
            [event.id, company.id]
          );
          totalLinks++;
        } catch (err) {
          // ignore duplicate key errors
        }
      }
    }
  }

  console.log(`   ✅ Created ${totalLinks} company links`);
  return totalLinks;
}

// ═══════════════════════════════════════════════════════════════════════════════
// PERSON LINKING
// ═══════════════════════════════════════════════════════════════════════════════

async function linkPeople() {
  console.log('👤 Linking events to people...');

  const events = await db.queryAll(`
    SELECT e.id, e.speaker_names, e.tenant_id
    FROM events e
    LEFT JOIN event_person_links epl ON epl.event_id = e.id
    WHERE e.fetched_at > NOW() - INTERVAL '3 hours'
      AND e.speaker_names IS NOT NULL
      AND array_length(e.speaker_names, 1) > 0
      AND epl.id IS NULL
    ORDER BY e.created_at DESC
    LIMIT 200
  `);

  console.log(`   Found ${events.length} events with speakers`);

  let totalLinks = 0;

  for (const event of events) {
    for (const name of event.speaker_names) {
      const person = await db.queryOne(
        `SELECT id FROM people WHERE full_name ILIKE $1 AND tenant_id = $2 LIMIT 1`,
        [`%${name}%`, event.tenant_id]
      );

      if (person) {
        try {
          await db.query(
            `INSERT INTO event_person_links (event_id, person_id, role)
             VALUES ($1, $2, 'speaker')
             ON CONFLICT (event_id, person_id) DO NOTHING`,
            [event.id, person.id]
          );
          totalLinks++;
        } catch (err) {
          // ignore duplicate key errors
        }
      }
    }
  }

  console.log(`   ✅ Created ${totalLinks} person links`);
  return totalLinks;
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════

async function linkEvents() {
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log('  MITCHELLAKE SIGNAL INTELLIGENCE - EVENT ENTITY LINKING');
  console.log('═══════════════════════════════════════════════════════════════════');
  console.log();

  const companyLinks = await linkCompanies();
  const personLinks = await linkPeople();

  console.log();
  console.log(`   📊 Total: ${companyLinks} company links, ${personLinks} person links`);
  console.log();

  return { companyLinks, personLinks };
}

if (require.main === module) {
  linkEvents()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

module.exports = { linkEvents };
