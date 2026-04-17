#!/usr/bin/env node
/**
 * Co-Author Proximity Pipeline
 *
 * PIPELINE-CONTEXT: Runs as system cron, uses platformPool directly.
 * Checks if any people in each tenant's sandbox have publications in the
 * ResearchMedium dataset, then creates proximity edges between co-authors.
 *
 * Schedule: daily at 3:30am (after network topology at 2:28am)
 * Usage: node scripts/compute_coauthor_proximity.js
 */

require('dotenv').config();
const { Pool } = require('pg');
const { searchPublications } = require('../lib/research_search');
const { generateEmbedding } = require('../lib/embeddings');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const BATCH_SIZE = 50;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  Co-Author Proximity Pipeline');
  console.log('═══════════════════════════════════════════════════\n');

  const startTime = Date.now();
  let totalEdges = 0;
  let totalChecked = 0;

  // Get all active tenants
  const { rows: tenants } = await pool.query('SELECT id, name FROM tenants');
  console.log(`Processing ${tenants.length} tenants\n`);

  for (const tenant of tenants) {
    try {
      // Get people with names — limit to a reasonable batch per tenant per day
      const { rows: people } = await pool.query(`
        SELECT id, full_name FROM people
        WHERE tenant_id = $1 AND full_name IS NOT NULL AND full_name != ''
        ORDER BY RANDOM() LIMIT 200
      `, [tenant.id]);

      if (people.length === 0) continue;
      console.log(`  [${tenant.name}] Checking ${people.length} people...`);

      let tenantEdges = 0;

      for (let i = 0; i < people.length; i += BATCH_SIZE) {
        const batch = people.slice(i, i + BATCH_SIZE);

        for (const person of batch) {
          try {
            // Generate embedding for this person as an author
            const embedding = await generateEmbedding('author researcher: ' + person.full_name);
            if (!embedding) continue;

            // Search publications
            const pubs = await searchPublications(embedding, { limit: 5, scoreThreshold: 0.45 });
            totalChecked++;

            // Filter to publications where person's last name appears
            const lastName = person.full_name.split(' ').pop().toLowerCase();
            const matches = pubs.filter(p => {
              const authorStr = (p.authors_full || p.authors || '').toLowerCase();
              return authorStr.includes(lastName);
            });

            if (matches.length === 0) continue;

            // Extract co-author names from matching publications
            for (const pub of matches) {
              const coAuthors = (pub.authors_full || pub.authors || '')
                .split(/[,;]\s*/)
                .map(a => a.trim())
                .filter(a => a.length > 2 && !a.toLowerCase().includes(lastName));

              // Check if any co-authors exist in this tenant's people table
              for (const coAuthor of coAuthors) {
                const coLastName = coAuthor.split(' ').pop().toLowerCase();
                if (coLastName.length < 3) continue;

                const { rows: coAuthorPeople } = await pool.query(`
                  SELECT id, full_name FROM people
                  WHERE tenant_id = $1
                    AND LOWER(SPLIT_PART(full_name, ' ', -1)) = $2
                    AND id != $3
                  LIMIT 1
                `, [tenant.id, coLastName, person.id]);

                if (coAuthorPeople.length > 0) {
                  // Create proximity edge
                  const coAuthorPerson = coAuthorPeople[0];
                  const weight = Math.min(0.6, 0.3 + (pub.match_score || 0) * 0.3);

                  await pool.query(`
                    INSERT INTO team_proximity
                      (person_id, team_member_id, relationship_type, relationship_strength,
                       source, notes, tenant_id)
                    VALUES ($1, $2, 'publication_coauthor', $3, 'coauthor_pipeline',
                            $4, $5)
                    ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
                      relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength),
                      notes = EXCLUDED.notes,
                      updated_at = NOW()
                  `, [
                    person.id, coAuthorPerson.id, weight,
                    `Co-authored: "${pub.title}" (${pub.year || 'unknown'})`,
                    tenant.id,
                  ]);
                  tenantEdges++;
                }
              }
            }
          } catch (e) { /* skip individual person errors */ }
        }

        await sleep(500); // Rate limit between batches
      }

      totalEdges += tenantEdges;
      if (tenantEdges > 0) {
        console.log(`  [${tenant.name}] Created ${tenantEdges} co-author edges`);
      }
    } catch (e) {
      console.error(`  [${tenant.name}] Error: ${e.message}`);
    }
  }

  const duration = Date.now() - startTime;

  // Log to pipeline_runs
  try {
    await pool.query(`
      INSERT INTO pipeline_runs (pipeline_key, pipeline_name, status, started_at, completed_at,
        duration_ms, items_processed, triggered_by)
      VALUES ('coauthor_proximity', 'Co-Author Proximity', 'completed', $1, NOW(), $2, $3, 'cron')
    `, [new Date(startTime), duration, totalEdges]);
  } catch (e) { /* non-fatal */ }

  console.log('\n═══════════════════════════════════════════════════');
  console.log(`  People checked: ${totalChecked}`);
  console.log(`  Edges created: ${totalEdges}`);
  console.log(`  Duration: ${Math.round(duration / 1000)}s`);
  console.log('═══════════════════════════════════════════════════');

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
