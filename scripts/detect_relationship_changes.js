#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════════════════════
// scripts/detect_relationship_changes.js
// Lightweight change detector — runs every 6h, only re-evaluates companies
// where something recently changed. Logs events for audit trail.
// ═══════════════════════════════════════════════════════════════════════════════

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false }, max: 5 });
const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

// Import scoring functions from the main script
const { compute } = require('./compute_company_relationships');

async function detect() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  DETECT RELATIONSHIP CHANGES');
  console.log('═══════════════════════════════════════════════════════════════\n');

  var stats = { checked: 0, changed: 0, stale_new: 0, recovered: 0 };

  // Get companies that need re-evaluation
  var { rows: candidates } = await pool.query(`
    SELECT DISTINCT c.id, c.name FROM companies c WHERE c.tenant_id = $1 AND (
      -- 1. Companies with recent signals (leadership change, layoffs, etc.)
      EXISTS (SELECT 1 FROM signal_events se WHERE se.company_id = c.id
        AND se.signal_type IN ('leadership_change','restructuring','layoffs')
        AND se.detected_at > NOW() - INTERVAL '6 hours')
      -- 2. Companies with recent interactions
      OR EXISTS (SELECT 1 FROM interactions i JOIN people p ON p.id = i.person_id AND p.current_company_id = c.id
        WHERE i.tenant_id = $1 AND i.created_at > NOW() - INTERVAL '6 hours')
      -- 3. Currently stale (re-check if recoverable)
      OR EXISTS (SELECT 1 FROM company_relationships cr WHERE cr.company_id = c.id AND cr.tenant_id = $1 AND cr.is_stale = true)
    )
  `, [TENANT_ID]);

  console.log('Companies to re-evaluate: ' + candidates.length + '\n');

  for (var co of candidates) {
    stats.checked++;

    // Get current state
    var { rows: [current] } = await pool.query(
      'SELECT relationship_tier, relationship_score, is_stale, stale_reason FROM company_relationships WHERE company_id = $1 AND tenant_id = $2',
      [co.id, TENANT_ID]
    );

    // Compute staleness
    var staleness = await computeStaleness(co.id);

    // Get fresh interaction data for scoring
    var { rows: ixData } = await pool.query(`
      SELECT MAX(i.interaction_at) AS last_at,
        COUNT(DISTINCT CASE WHEN i.interaction_at > NOW() - INTERVAL '12 months' THEN i.person_id END) AS active_contacts,
        COUNT(DISTINCT i.user_id) AS team_members
      FROM interactions i JOIN people p ON p.id = i.person_id AND p.current_company_id = $1
      WHERE i.tenant_id = $2 AND i.interaction_type NOT IN ('system_note','research_note','note')
    `, [co.id, TENANT_ID]);

    if (!current) continue; // No existing record — will be caught by daily full compute

    var changed = false;
    var events = [];

    // Check staleness change
    if (staleness.isStale && !current.is_stale) {
      changed = true;
      events.push({ type: 'became_stale', staleReason: staleness.reason });
      console.log('[STALE]     ' + co.name + ': ' + staleness.reason);
      stats.stale_new++;
    } else if (!staleness.isStale && current.is_stale) {
      changed = true;
      events.push({ type: 'recovered' });
      console.log('[RECOVERED] ' + co.name + ': relationship active again');
      stats.recovered++;
    }

    // Update staleness fields
    if (staleness.isStale !== current.is_stale || staleness.reason !== current.stale_reason) {
      await pool.query(`
        UPDATE company_relationships SET
          is_stale = $1, stale_reason = $2,
          stale_since = CASE WHEN $1 AND NOT is_stale THEN NOW() WHEN NOT $1 THEN NULL ELSE stale_since END,
          updated_at = NOW()
        WHERE company_id = $3 AND tenant_id = $4
      `, [staleness.isStale, staleness.reason, co.id, TENANT_ID]);
      changed = true;
    }

    // Log events
    for (var ev of events) {
      await pool.query(`
        INSERT INTO company_relationship_events (tenant_id, company_id, event_type, previous_tier, new_tier,
          previous_score, new_score, stale_reason, metadata)
        VALUES ($1, $2, $3, $4, $4, $5, $5, $6, $7)
      `, [TENANT_ID, co.id, ev.type, current.relationship_tier,
          current.relationship_score, ev.staleReason || null,
          JSON.stringify({ company_name: co.name })]);
    }

    if (changed) stats.changed++;
  }

  console.log('\n  Checked: ' + stats.checked + ' | Changed: ' + stats.changed +
    ' | New stale: ' + stats.stale_new + ' | Recovered: ' + stats.recovered);

  await pool.end();
  return stats;
}

async function computeStaleness(companyId) {
  // Rule 1: all_contacts_departed
  var { rows: people } = await pool.query(
    'SELECT id FROM people WHERE current_company_id = $1 AND tenant_id = $2',
    [companyId, TENANT_ID]
  );

  if (people.length > 0) {
    var personIds = people.map(function(p) { return p.id; });
    var { rows: [departed] } = await pool.query(`
      SELECT COUNT(DISTINCT ps.person_id) AS departed_count
      FROM person_signals ps
      WHERE ps.person_id = ANY($1)
        AND ps.signal_type IN ('new_role','company_exit')
        AND ps.detected_at > NOW() - INTERVAL '180 days'
        AND ps.tenant_id = $2
    `, [personIds, TENANT_ID]);

    if (parseInt(departed.departed_count) >= people.length) {
      // Check if any new contact added recently
      var { rows: [newContact] } = await pool.query(
        "SELECT COUNT(*) AS cnt FROM people WHERE current_company_id = $1 AND tenant_id = $2 AND created_at > NOW() - INTERVAL '90 days'",
        [companyId, TENANT_ID]
      );
      if (parseInt(newContact.cnt) === 0) {
        return { isStale: true, reason: 'all_contacts_departed' };
      }
    }
  }

  // Rule 2: no_recent_contact
  var { rows: [lastIx] } = await pool.query(`
    SELECT MAX(i.interaction_at) AS last_at,
      COUNT(DISTINCT CASE WHEN i.interaction_at > NOW() - INTERVAL '12 months' THEN i.person_id END) AS active
    FROM interactions i JOIN people p ON p.id = i.person_id AND p.current_company_id = $1
    WHERE i.tenant_id = $2 AND i.interaction_type NOT IN ('system_note','research_note','note')
  `, [companyId, TENANT_ID]);

  if (lastIx.last_at && new Date(lastIx.last_at) < new Date(Date.now() - 365 * 86400000) && parseInt(lastIx.active) === 0) {
    return { isStale: true, reason: 'no_recent_contact' };
  }

  // Rule 3: contact_data_old
  if (people.length > 0) {
    var { rows: [freshness] } = await pool.query(
      "SELECT COUNT(*) AS fresh FROM people WHERE current_company_id = $1 AND tenant_id = $2 AND updated_at > NOW() - INTERVAL '18 months'",
      [companyId, TENANT_ID]
    );
    if (parseInt(freshness.fresh) === 0) {
      return { isStale: true, reason: 'contact_data_old' };
    }
  }

  return { isStale: false, reason: null };
}

if (require.main === module) {
  detect().then(function() { process.exit(0); }).catch(function(e) { console.error(e); process.exit(1); });
}

module.exports = { detect };
