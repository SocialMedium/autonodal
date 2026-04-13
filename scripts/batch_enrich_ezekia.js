#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/batch_enrich_ezekia.js — Batch Ezekia enrichment for signal matching
//
// Pulls career history, skills, seniority, location, profile picture from Ezekia
// for all people with source_id. Designed to run in sections (offset/limit).
//
// What it extracts (for signal matching, not full CRM replication):
//   - career_history (full positions with skills, summary, company IDs)
//   - expertise_tags (aggregated skills from all positions)
//   - seniority_level (from Ezekia career tag, more reliable than title parsing)
//   - profile_photo_url
//   - location, city, country (from address)
//   - linkedin_url (from links)
//   - education
//
// Usage:
//   node scripts/batch_enrich_ezekia.js                    # All unprocessed
//   node scripts/batch_enrich_ezekia.js --limit=500        # First 500
//   node scripts/batch_enrich_ezekia.js --offset=500       # Skip first 500
//   node scripts/batch_enrich_ezekia.js --force            # Re-enrich already enriched
//   node scripts/batch_enrich_ezekia.js --dry-run          # Preview only
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5,
});

const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';
const RATE_LIMIT_MS = 300; // Ezekia rate limit — ~3 req/s

// OpenAI + Qdrant for re-embedding
let openai, qdrantClient;
try {
  const OpenAI = require('openai');
  openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
} catch (e) {}
try {
  const { QdrantClient } = require('@qdrant/js-client-rest');
  qdrantClient = new QdrantClient({ url: process.env.QDRANT_URL, apiKey: process.env.QDRANT_API_KEY });
} catch (e) {}

const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBED_BATCH_SIZE = 50; // OpenAI batch limit

const args = process.argv.slice(2);
const LIMIT = parseInt((args.find(a => a.startsWith('--limit=')) || '').split('=')[1]) || 0;
const OFFSET = parseInt((args.find(a => a.startsWith('--offset=')) || '').split('=')[1]) || 0;
const DRY_RUN = args.includes('--dry-run');
const FORCE = args.includes('--force');

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function deriveSeniority(careerName) {
  if (!careerName) return null;
  var c = careerName.toLowerCase();
  if (/\b(ceo|cfo|cto|coo|cmo|cio|chief|founder|co-founder|managing director|president|partner)\b/.test(c)) return 'c_suite';
  if (/\b(vp|vice president|svp|evp)\b/.test(c)) return 'vp';
  if (/\b(director|head of|general manager)\b/.test(c)) return 'director';
  if (/\b(senior|lead|principal|staff)\b/.test(c)) return 'senior';
  if (/\b(manager|supervisor|controller)\b/.test(c)) return 'manager';
  return null;
}

// Batch embed + upsert to Qdrant
// embedQueue and stats are set inside run() but accessed by flushEmbeddings
var embedQueue = [], stats = {};
async function flushEmbeddings() {
  if (!embedQueue || !embedQueue.length || !openai || !qdrantClient) return;
  try {
    var texts = embedQueue.map(function(e) { return e.text; });
    var response = await openai.embeddings.create({ model: EMBEDDING_MODEL, input: texts });
    var points = embedQueue.map(function(e, i) {
      return {
        id: Date.now() * 1000 + i,
        vector: response.data[i].embedding,
        payload: { person_id: e.id, type: 'person' }
      };
    });
    // Ensure collection exists
    try { await qdrantClient.getCollection('people'); } catch (ce) {
      await qdrantClient.createCollection('people', { vectors: { size: 1536, distance: 'Cosine' } });
    }
    await qdrantClient.upsert('people', { points: points });
    // Mark embedded in DB
    var ids = embedQueue.map(function(e) { return e.id; });
    await pool.query('UPDATE people SET embedded_at = NOW() WHERE id = ANY($1)', [ids]);
    stats.embedded += embedQueue.length;
  } catch (e) {
    console.log('  Embed batch error: ' + e.message.substring(0, 60));
  }
  embedQueue.length = 0;
}

async function run() {
  console.log('═══════════════════════════════════════════════════════');
  console.log('  Batch Ezekia Enrichment — Signal Matching Data');
  console.log('  Mode:', DRY_RUN ? 'DRY RUN' : 'LIVE');
  if (LIMIT) console.log('  Limit:', LIMIT);
  if (OFFSET) console.log('  Offset:', OFFSET);
  if (FORCE) console.log('  Force re-enrich: yes');
  console.log('═══════════════════════════════════════════════════════\n');

  if (!process.env.EZEKIA_API_TOKEN) {
    console.error('EZEKIA_API_TOKEN not set');
    process.exit(1);
  }

  const ezekia = require('../lib/ezekia');

  // Get people with Ezekia IDs
  var enrichedFilter = FORCE ? '' : "AND (p.enriched_at IS NULL OR p.career_history IS NULL OR p.career_history = 'null'::jsonb OR p.career_history = '[]'::jsonb)";
  var { rows: candidates } = await pool.query(`
    SELECT p.id, p.full_name, p.source_id, p.source, p.enriched_at,
           p.career_history IS NOT NULL AND p.career_history != 'null'::jsonb AS has_career,
           p.expertise_tags, p.seniority_level, p.profile_photo_url
    FROM people p
    WHERE p.tenant_id = $1
      AND p.source_id IS NOT NULL
      AND p.source IN ('ezekia', 'ezekia_enrich')
      ${enrichedFilter}
    ORDER BY p.enriched_at ASC NULLS FIRST
    ${LIMIT ? 'LIMIT ' + LIMIT : ''}
    ${OFFSET ? 'OFFSET ' + OFFSET : ''}
  `, [TENANT_ID]);

  console.log('Candidates:', candidates.length);
  if (!candidates.length) { console.log('Nothing to do.'); await pool.end(); return; }
  if (DRY_RUN) {
    console.log('DRY RUN — would enrich ' + candidates.length + ' people');
    candidates.slice(0, 10).forEach(c => console.log('  ' + c.full_name + ' (ezekia:' + c.source_id + ') has_career=' + c.has_career));
    await pool.end();
    return;
  }

  stats = { enriched: 0, skipped: 0, errors: 0, skills_added: 0, seniority_set: 0, photos_set: 0, embedded: 0 };
  var startTime = Date.now();
  embedQueue = []; // { id, text } — batched for OpenAI efficiency

  for (var i = 0; i < candidates.length; i++) {
    var person = candidates[i];

    try {
      var res = await ezekia.getPersonFull(parseInt(person.source_id));
      var d = res?.data;
      if (!d) { stats.skipped++; continue; }

      // Verify name match
      var ezName = (d.fullName || (d.firstName || '') + ' ' + (d.lastName || '')).trim().toLowerCase();
      var ourName = (person.full_name || '').toLowerCase();
      if (ezName && ourName && !ezName.includes(ourName.split(' ')[0]) && !ourName.includes(ezName.split(' ')[0])) {
        stats.skipped++;
        continue;
      }

      var updates = [];
      var params = [person.id, TENANT_ID];
      var idx = 2;

      // Career history — full extraction for signal matching
      var positions = (d.profile?.positions || []).sort((a, b) => (b.startDate || '0000').localeCompare(a.startDate || '0000'));
      var currentPos = positions.find(function(p) { return p.endDate === '9999-12-31' || !p.endDate; }) || positions[0];
      if (positions.length > 0) {
        var career = positions.map(function(p) {
          return {
            title: p.title,
            company: p.company?.name || p.company,
            company_id: p.company?.id || null,
            location: p.location?.name || null,
            start_date: p.startDate,
            end_date: p.endDate,
            current: p.tense || p.primary || !p.endDate || p.endDate === '9999-12-31',
            skills: p.skills || [],
            summary: p.summary || null,
            achievements: (p.achievements || []).filter(Boolean),
            seniority_tag: p.career?.name || null,
            department: p.department || null,
            industry: p.industry || null,
          };
        });
        idx++; updates.push('career_history = $' + idx); params.push(JSON.stringify(career));

        // Skills aggregated
        var allSkills = [...new Set(positions.flatMap(function(p) { return p.skills || []; }))].filter(Boolean);
        if (allSkills.length > 0) {
          idx++; updates.push('expertise_tags = $' + idx); params.push(allSkills);
          stats.skills_added++;
        }

        // Seniority from career tag
        var sen = deriveSeniority(currentPos?.career?.name);
        if (!sen) sen = deriveSeniority(currentPos?.title);
        if (sen) {
          idx++; updates.push('seniority_level = COALESCE(seniority_level, $' + idx + ')'); params.push(sen);
          stats.seniority_set++;
        }

        // Current title/company
        if (currentPos?.title) { idx++; updates.push('current_title = $' + idx); params.push(currentPos.title); }
        if (currentPos?.company?.name) { idx++; updates.push('current_company_name = $' + idx); params.push(currentPos.company.name); }
      }

      // Profile picture
      if (d.profilePicture) {
        idx++; updates.push('profile_photo_url = COALESCE(profile_photo_url, $' + idx + ')'); params.push(d.profilePicture);
        stats.photos_set++;
      }

      // Contact data
      var defaultEmail = d.emails?.find(function(e) { return e.isDefault; })?.address || d.emails?.[0]?.address;
      var defaultPhone = d.phones?.find(function(p) { return p.isDefault; })?.number || d.phones?.[0]?.number;
      var linkedinLink = (d.links || []).find(function(l) { return l.type === 'linkedin' || (l.url || '').includes('linkedin'); })?.url;
      var defaultAddr = d.addresses?.find(function(a) { return a.isDefault; }) || d.addresses?.[0];

      if (defaultEmail) { idx++; updates.push('email = COALESCE(email, $' + idx + ')'); params.push(defaultEmail); }
      if (defaultPhone) { idx++; updates.push('phone = COALESCE(phone, $' + idx + ')'); params.push(defaultPhone); }
      if (linkedinLink) { idx++; updates.push('linkedin_url = COALESCE(linkedin_url, $' + idx + ')'); params.push(linkedinLink); }
      if (defaultAddr) {
        var loc = [defaultAddr.city, defaultAddr.state, defaultAddr.country].filter(Boolean).join(', ');
        if (loc) { idx++; updates.push('location = COALESCE(location, $' + idx + ')'); params.push(loc); }
        if (defaultAddr.city) { idx++; updates.push('city = COALESCE(city, $' + idx + ')'); params.push(defaultAddr.city); }
        if (defaultAddr.country) { idx++; updates.push('country = COALESCE(country, $' + idx + ')'); params.push(defaultAddr.country); }
      }

      // Headline
      if (d.profile?.headline) { idx++; updates.push('headline = COALESCE(headline, $' + idx + ')'); params.push(d.profile.headline); }

      // Education
      if (d.profile?.education?.length > 0) {
        idx++; updates.push('education = $' + idx); params.push(JSON.stringify(d.profile.education));
      }

      if (updates.length === 0) { stats.skipped++; continue; }

      await pool.query(
        'UPDATE people SET ' + updates.join(', ') + ', enriched_at = NOW(), updated_at = NOW() WHERE id = $1 AND tenant_id = $2',
        params
      );
      stats.enriched++;

      // Build embedding text from enriched data — same format as person enrich endpoint
      if (openai && qdrantClient) {
        var embParts = [d.fullName || person.full_name];
        if (positions.length > 0) {
          var cp = positions.find(function(p) { return p.tense || p.primary; }) || positions[0];
          if (cp.title) embParts.push(cp.title);
          if (cp.company?.name) embParts.push(cp.company.name);
        }
        if (d.profile?.headline) embParts.push(d.profile.headline);
        var addr = d.addresses?.[0];
        if (addr) embParts.push([addr.city, addr.country].filter(Boolean).join(', '));
        var allSkills = [...new Set(positions.flatMap(function(p) { return p.skills || []; }))].filter(Boolean);
        if (allSkills.length) embParts.push('Skills: ' + allSkills.join(', '));
        // Add career summary from top 3 positions
        positions.slice(0, 3).forEach(function(p) {
          if (p.summary) embParts.push(p.summary.slice(0, 300));
        });
        // Add recent research notes for context
        var notesRes = await pool.query(
          "SELECT summary FROM interactions WHERE person_id = $1 AND interaction_type = 'research_note' AND tenant_id = $2 ORDER BY interaction_at DESC NULLS LAST LIMIT 3",
          [person.id, TENANT_ID]
        ).catch(function() { return { rows: [] }; });
        notesRes.rows.forEach(function(n) { if (n.summary) embParts.push(n.summary.slice(0, 300)); });

        embedQueue.push({ id: person.id, text: embParts.filter(Boolean).join('\n').slice(0, 8000) });

        // Flush embed batch when full
        if (embedQueue.length >= EMBED_BATCH_SIZE) {
          await flushEmbeddings();
        }
      }

      if ((i + 1) % 50 === 0) {
        var elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        var rate = (stats.enriched / (elapsed / 60)).toFixed(0);
        console.log('  ' + (i + 1) + '/' + candidates.length + ' | enriched=' + stats.enriched + ' | embedded=' + stats.embedded + ' | ' + rate + '/min | ' + elapsed + 's');
      }

      await sleep(RATE_LIMIT_MS);

    } catch (e) {
      stats.errors++;
      if (stats.errors <= 5) console.log('  ERR ' + person.full_name + ': ' + e.message.substring(0, 60));
    }
  }

  // Flush remaining embeddings
  if (embedQueue.length > 0) await flushEmbeddings();

  var duration = ((Date.now() - startTime) / 1000).toFixed(0);
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('  RESULTS (' + duration + 's)');
  console.log('  Enriched:      ' + stats.enriched);
  console.log('  Embedded:      ' + stats.embedded);
  console.log('  Skipped:       ' + stats.skipped);
  console.log('  Errors:        ' + stats.errors);
  console.log('  Skills added:  ' + stats.skills_added);
  console.log('  Seniority set: ' + stats.seniority_set);
  console.log('  Photos set:    ' + stats.photos_set);
  console.log('═══════════════════════════════════════════════════════');

  await pool.end();
}

module.exports = { run };

if (require.main === module) {
  run().catch(function(e) { console.error('Fatal:', e); process.exit(1); });
}
