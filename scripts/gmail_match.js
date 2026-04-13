#!/usr/bin/env node
/**
 * Gmail Match & Signal Extraction — Phase 2
 * 
 * Runs AFTER gmail_import.js has bulk-loaded emails.
 * 1. Builds email → person index
 * 2. Batch matches all unmatched emails to people
 * 3. Computes engagement signals (response times, frequency, recency)
 * 4. Updates person_scores with email engagement data
 * 
 * Usage:
 *   node scripts/gmail_match.js
 */

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 5
});

async function main() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║   Gmail Match & Signal Extraction — Phase 2               ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  // ─── Step 1: Build email index ───
  console.log('  Step 1: Building email → person index...');
  
  const people = await pool.query(`
    SELECT id, email, full_name FROM people WHERE email IS NOT NULL AND email != ''
  `);

  const emailIndex = new Map();
  for (const p of people.rows) {
    const emails = p.email.toLowerCase().split(/[,;\s]+/).filter(e => e.includes('@'));
    for (const email of emails) {
      emailIndex.set(email, p.id);
    }
  }
  console.log(`  ✅ ${emailIndex.size} email addresses from ${people.rows.length} people\n`);

  // ─── Step 2: Get user's email ───
  const acct = await pool.query('SELECT google_email, user_id FROM user_google_accounts LIMIT 1');
  if (acct.rows.length === 0) { console.log('No Gmail account'); await pool.end(); return; }
  const userEmail = acct.rows[0].google_email.toLowerCase();
  const userId = acct.rows[0].user_id;
  console.log(`  User: ${userEmail}\n`);

  // ─── Step 3: Batch match unmatched emails ───
  console.log('  Step 2: Matching emails to people...');

  // Get all unmatched gmail emails  
  const unmatched = await pool.query(`
    SELECT id, email_from, email_to, direction
    FROM interactions 
    WHERE source = 'gmail_sync' 
    AND person_id IS NULL
  `);

  console.log(`  ${unmatched.rows.length} unmatched emails to process`);

  let matched = 0;
  let batch = [];

  for (const row of unmatched.rows) {
    // The "other party" depends on direction
    const otherEmails = [];
    
    if (row.direction === 'outbound') {
      // We sent it — match recipients
      const recipients = Array.isArray(row.email_to) ? row.email_to : [];
      otherEmails.push(...recipients);
    } else {
      // We received it — match sender
      if (row.email_from) otherEmails.push(row.email_from);
    }

    // Find first matching person
    let personId = null;
    for (const email of otherEmails) {
      personId = emailIndex.get(email.toLowerCase());
      if (personId) break;
    }

    if (personId) {
      batch.push({ id: row.id, personId });
      matched++;

      // Flush in batches of 500
      if (batch.length >= 500) {
        await flushBatch(batch);
        console.log(`    ${matched} matched so far...`);
        batch = [];
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) await flushBatch(batch);

  console.log(`  ✅ ${matched} emails matched to people\n`);

  // ─── Step 4: Create email_signals for matched emails ───
  console.log('  Step 3: Creating email signals...');

  const matchedEmails = await pool.query(`
    SELECT id, person_id, user_id, direction, interaction_at,
           email_thread_id, email_has_attachments, email_from
    FROM interactions
    WHERE source = 'gmail_sync'
    AND person_id IS NOT NULL
    AND interaction_at > NOW() - INTERVAL '90 days'
    AND NOT EXISTS (
      SELECT 1 FROM email_signals es
      WHERE es.user_id = interactions.user_id
      AND es.email_date = interactions.interaction_at
      AND es.thread_id = interactions.email_thread_id
    )
  `);

  let signals = 0;
  for (const em of matchedEmails.rows) {
    const domain = em.email_from ? em.email_from.split('@')[1] : null;
    
    await pool.query(`
      INSERT INTO email_signals (
        person_id, user_id, direction, email_date,
        thread_id, has_attachment, email_domain
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT DO NOTHING
    `, [
      em.person_id,
      em.user_id || userId,
      em.direction,
      em.interaction_at,
      em.email_thread_id,
      em.email_has_attachments || false,
      domain
    ]).catch(() => {});
    signals++;
  }

  console.log(`  ✅ ${signals} email signals created\n`);

  // ─── Step 5: Compute response times from threads ───
  console.log('  Step 4: Computing response times...');

  const threads = await pool.query(`
    SELECT 
      person_id,
      email_thread_id,
      direction,
      interaction_at
    FROM interactions
    WHERE source = 'gmail_sync'
    AND person_id IS NOT NULL
    AND email_thread_id IS NOT NULL
    ORDER BY email_thread_id, interaction_at
  `);

  // Group by thread
  const threadMap = new Map();
  for (const row of threads.rows) {
    if (!threadMap.has(row.email_thread_id)) threadMap.set(row.email_thread_id, []);
    threadMap.get(row.email_thread_id).push(row);
  }

  let responseTimesComputed = 0;
  for (const [threadId, messages] of threadMap) {
    // Look for send → receive pairs (response times)
    for (let i = 1; i < messages.length; i++) {
      const prev = messages[i - 1];
      const curr = messages[i];
      
      // Different directions = a response
      if (prev.direction !== curr.direction && prev.person_id === curr.person_id) {
        const responseMinutes = Math.round(
          (new Date(curr.interaction_at) - new Date(prev.interaction_at)) / 60000
        );
        
        // Reasonable response time (1 min to 30 days)
        if (responseMinutes > 0 && responseMinutes < 43200) {
          await pool.query(`
            UPDATE email_signals SET response_time_minutes = $1
            WHERE person_id = $2 AND thread_id = $3 AND direction = $4
            AND response_time_minutes IS NULL
          `, [responseMinutes, curr.person_id, threadId, curr.direction]).catch(() => {});
          responseTimesComputed++;
        }
      }
    }
  }

  console.log(`  ✅ ${responseTimesComputed} response times computed\n`);

  // ─── Step 6: Aggregate per-person engagement ───
  console.log('  Step 5: Computing engagement scores...');

  const personStats = await pool.query(`
    SELECT 
      person_id,
      COUNT(*) as total_emails,
      COUNT(*) FILTER (WHERE direction = 'inbound') as received,
      COUNT(*) FILTER (WHERE direction = 'outbound') as sent,
      AVG(response_time_minutes) FILTER (WHERE direction = 'inbound' AND response_time_minutes IS NOT NULL) as avg_response_min,
      MAX(email_date) as last_email,
      COUNT(DISTINCT thread_id) as thread_count
    FROM email_signals
    GROUP BY person_id
  `);

  let scoresUpdated = 0;
  let signalsCreated = 0;

  for (const ps of personStats.rows) {
    const responseRate = ps.sent > 0 ? Math.min(ps.received / ps.sent, 1.0) : 0;
    const daysSinceLast = ps.last_email
      ? Math.floor((Date.now() - new Date(ps.last_email)) / 86400000)
      : 999;

    // Compute engagement from email patterns
    const emailEngagement = Math.min(1.0,
      (responseRate > 0.5 ? 0.3 : responseRate > 0.2 ? 0.15 : 0) +
      (daysSinceLast < 30 ? 0.3 : daysSinceLast < 90 ? 0.15 : 0) +
      (ps.thread_count > 5 ? 0.2 : ps.thread_count > 2 ? 0.1 : 0) +
      (ps.avg_response_min && ps.avg_response_min < 1440 ? 0.2 : 0.05)
    );

    // Update person_scores
    await pool.query(`
      INSERT INTO person_scores (person_id, engagement_score, last_interaction_at, interaction_count_30d, score_factors, computed_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (person_id) DO UPDATE SET
        engagement_score = GREATEST(person_scores.engagement_score, $2),
        last_interaction_at = GREATEST(person_scores.last_interaction_at, $3),
        interaction_count_30d = GREATEST(person_scores.interaction_count_30d, $4),
        score_factors = COALESCE(person_scores.score_factors, '{}'::jsonb) || $5::jsonb,
        computed_at = NOW()
    `, [
      ps.person_id,
      emailEngagement,
      ps.last_email,
      parseInt(ps.total_emails) || 0,
      JSON.stringify({
        email_total: parseInt(ps.total_emails),
        email_sent: parseInt(ps.sent),
        email_received: parseInt(ps.received),
        email_threads: parseInt(ps.thread_count),
        avg_response_min: ps.avg_response_min ? Math.round(ps.avg_response_min) : null,
        response_rate: Math.round(responseRate * 100) / 100,
        days_since_last: daysSinceLast
      })
    ]).catch(() => {});
    scoresUpdated++;

    // Generate notable signals
    if (daysSinceLast > 180 && ps.total_emails > 5) {
      await pool.query(`
        INSERT INTO person_signals (person_id, signal_type, signal_category, confidence, detail, source, detected_at)
        VALUES ($1, 'going_cold', 'engagement', 0.7, $2, 'gmail_analysis', NOW())
        ON CONFLICT DO NOTHING
      `, [ps.person_id, JSON.stringify({ days_silent: daysSinceLast, prev_emails: ps.total_emails })]).catch(() => {});
      signalsCreated++;
    }

    if (ps.avg_response_min && ps.avg_response_min < 60 && ps.received > 3) {
      await pool.query(`
        INSERT INTO person_signals (person_id, signal_type, signal_category, confidence, detail, source, detected_at)
        VALUES ($1, 'highly_responsive', 'engagement', 0.8, $2, 'gmail_analysis', NOW())
        ON CONFLICT DO NOTHING
      `, [ps.person_id, JSON.stringify({ avg_min: Math.round(ps.avg_response_min), responses: ps.received })]).catch(() => {});
      signalsCreated++;
    }
  }

  console.log(`  ✅ ${scoresUpdated} person scores updated, ${signalsCreated} signals created\n`);

  // ─── Summary ───
  console.log('═'.repeat(50));
  const final = await pool.query(`
    SELECT 
      (SELECT COUNT(*) FROM interactions WHERE source='gmail_sync') as emails,
      (SELECT COUNT(*) FROM interactions WHERE source='gmail_sync' AND person_id IS NOT NULL) as matched,
      (SELECT COUNT(DISTINCT person_id) FROM email_signals) as people_with_signals,
      (SELECT COUNT(*) FROM email_signals WHERE response_time_minutes IS NOT NULL) as with_response_time,
      (SELECT ROUND(AVG(response_time_minutes)) FROM email_signals WHERE response_time_minutes IS NOT NULL AND direction='inbound') as avg_response
  `);
  const f = final.rows[0];
  console.log(`  Total emails:          ${f.emails}`);
  console.log(`  Matched to people:     ${f.matched} (${f.emails > 0 ? Math.round(f.matched/f.emails*100) : 0}%)`);
  console.log(`  People with signals:   ${f.people_with_signals}`);
  console.log(`  With response times:   ${f.with_response_time}`);
  console.log(`  Avg inbound response:  ${f.avg_response ? f.avg_response + ' min' : 'n/a'}`);
  console.log('═'.repeat(50));

  // Top engaged people
  const topEngaged = await pool.query(`
    SELECT p.full_name, p.current_company_name, 
           COUNT(*) as emails, 
           ROUND(AVG(es.response_time_minutes) FILTER (WHERE es.direction='inbound')) as avg_resp
    FROM email_signals es
    JOIN people p ON es.person_id = p.id
    GROUP BY p.id, p.full_name, p.current_company_name
    ORDER BY COUNT(*) DESC
    LIMIT 15
  `);
  
  if (topEngaged.rows.length > 0) {
    console.log('\n  Top 15 by email volume:');
    topEngaged.rows.forEach((r, i) => {
      console.log(`    ${i+1}. ${r.full_name} (${r.current_company_name || '?'}) — ${r.emails} emails${r.avg_resp ? ', avg reply ' + r.avg_resp + 'min' : ''}`);
    });
  }

  console.log('\n✅ Done');
  await pool.end();
}

async function flushBatch(batch) {
  // Use a single UPDATE with CASE for efficiency
  for (const item of batch) {
    await pool.query(
      'UPDATE interactions SET person_id = $1 WHERE id = $2',
      [item.personId, item.id]
    ).catch(() => {});
  }
}

main().catch(e => { console.error('Fatal:', e); pool.end(); process.exit(1); });
