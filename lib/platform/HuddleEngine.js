// =============================================================================
// lib/platform/HuddleEngine.js — Huddle Join / Exit / Recompute Logic
// =============================================================================
//
// Orchestrates atomic join/exit for huddle members and maintains the merged
// huddle_people + huddle_proximity graph. Uses platformPool for cross-tenant
// huddle tables and TenantDB for reading member sandbox data.

var { TenantDB, platformPool } = require('../TenantDB');
var { rankEntryPoints, buildEntryRecommendation } = require('./ProximityScorer');

// ─────────────────────────────────────────────────────────────────────────────
// HuddleEngine
// ─────────────────────────────────────────────────────────────────────────────

function HuddleEngine() {}

/**
 * Preview what happens if a tenant joins a huddle.
 * Read-only — no mutations.
 *
 * @param {string} huddleId
 * @param {string} joiningTenantId
 * @returns {Promise<Object>} { your_network, huddle_current, overlap, net_new, huddle_after }
 */
HuddleEngine.prototype.previewJoin = async function previewJoin(huddleId, joiningTenantId) {
  var memberDb = new TenantDB(joiningTenantId);

  // Count people in the joining member's sandbox (person_proximity or people table)
  var myPeople = await memberDb.queryAll(
    'SELECT person_id FROM person_proximity'
  );
  var myPersonIds = myPeople.map(function(r) { return r.person_id; });
  var yourNetwork = myPersonIds.length;

  // Current huddle people
  var { rows: huddleRows } = await platformPool.query(
    'SELECT person_id FROM huddle_people WHERE huddle_id = $1',
    [huddleId]
  );
  var huddleCurrent = huddleRows.length;
  var huddlePersonSet = {};
  for (var i = 0; i < huddleRows.length; i++) {
    huddlePersonSet[huddleRows[i].person_id] = true;
  }

  // Overlap and net-new
  var overlap = 0;
  var netNew = 0;
  for (var j = 0; j < myPersonIds.length; j++) {
    if (huddlePersonSet[myPersonIds[j]]) {
      overlap++;
    } else {
      netNew++;
    }
  }

  return {
    your_network: yourNetwork,
    huddle_current: huddleCurrent,
    overlap: overlap,
    net_new: netNew,
    huddle_after: huddleCurrent + netNew,
  };
};

/**
 * Atomic join: activate membership, contribute people + proximity edges,
 * update stats, then trigger async recompute.
 *
 * @param {string} huddleId
 * @param {string} joiningTenantId
 * @param {string} role  'member' | 'admin' | 'observer'
 */
HuddleEngine.prototype.join = async function join(huddleId, joiningTenantId, role) {
  var memberDb = new TenantDB(joiningTenantId);
  var client = await platformPool.connect();

  try {
    await client.query('BEGIN');

    // 1. Mark member as active
    await client.query(
      `UPDATE huddle_members
         SET status = 'active', role = $1, joined_at = NOW()
       WHERE huddle_id = $2 AND tenant_id = $3`,
      [role || 'member', huddleId, joiningTenantId]
    );

    // 2. Get member's people from their sandbox
    var myPeople = await memberDb.queryAll(
      `SELECT person_id, strength_score, currency_score, history_score,
              depth_score, reciprocity_score, depth_type, currency_label,
              entry_recommendation, entry_action, primary_platform,
              last_contact_date, interaction_count
       FROM person_proximity`
    );

    var contributedCount = 0;
    var netNewCount = 0;

    for (var i = 0; i < myPeople.length; i++) {
      var p = myPeople[i];

      // Check if person already in huddle
      var { rows: existing } = await client.query(
        'SELECT id, contributor_count FROM huddle_people WHERE huddle_id = $1 AND person_id = $2',
        [huddleId, p.person_id]
      );

      if (existing.length > 0) {
        // Already present — increment contributor_count
        await client.query(
          `UPDATE huddle_people
             SET contributor_count = contributor_count + 1,
                 member_connection_count = member_connection_count + 1,
                 updated_at = NOW()
           WHERE id = $1`,
          [existing[0].id]
        );
      } else {
        // Net new person
        await client.query(
          `INSERT INTO huddle_people
             (huddle_id, person_id, first_contributed_by, contributor_count, member_connection_count)
           VALUES ($1, $2, $3, 1, 1)`,
          [huddleId, p.person_id, joiningTenantId]
        );
        netNewCount++;
      }
      contributedCount++;

      // 3. Write proximity edge (source_platform stored but never returned to API)
      await client.query(
        `INSERT INTO huddle_proximity
           (huddle_id, member_tenant_id, person_id, strength_score, currency_score,
            history_score, depth_score, reciprocity_score, depth_type, currency_label,
            entry_recommendation, entry_action, source_platform, last_contact,
            interaction_count, score_computed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
         ON CONFLICT (huddle_id, member_tenant_id, person_id) DO UPDATE SET
           strength_score = EXCLUDED.strength_score,
           currency_score = EXCLUDED.currency_score,
           history_score = EXCLUDED.history_score,
           depth_score = EXCLUDED.depth_score,
           reciprocity_score = EXCLUDED.reciprocity_score,
           depth_type = EXCLUDED.depth_type,
           currency_label = EXCLUDED.currency_label,
           entry_recommendation = EXCLUDED.entry_recommendation,
           entry_action = EXCLUDED.entry_action,
           source_platform = EXCLUDED.source_platform,
           last_contact = EXCLUDED.last_contact,
           interaction_count = EXCLUDED.interaction_count,
           score_computed_at = NOW()`,
        [
          huddleId, joiningTenantId, p.person_id,
          p.strength_score || 0, p.currency_score, p.history_score,
          p.depth_score, p.reciprocity_score, p.depth_type, p.currency_label,
          p.entry_recommendation, p.entry_action, p.primary_platform,
          p.last_contact_date, p.interaction_count || 0,
        ]
      );
    }

    // 4. Update member contribution stats
    await client.query(
      `UPDATE huddle_members
         SET contributed_people_count = $1, net_new_people_count = $2
       WHERE huddle_id = $3 AND tenant_id = $4`,
      [contributedCount, netNewCount, huddleId, joiningTenantId]
    );

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(function() {});
    throw err;
  } finally {
    client.release();
  }

  // 5. Async recompute best entry points
  var self = this;
  setImmediate(function() {
    self.recomputeBestEntryPoints(huddleId).catch(function(err) {
      console.error('[HuddleEngine] recomputeBestEntryPoints error:', err.message);
    });
  });

  return { contributed: contributedCount, net_new: netNewCount };
};

/**
 * Atomic clean exit: remove member's exclusive contributions, decrement shared,
 * delete all proximity edges, remove exclusive signals, mark detached.
 *
 * @param {string} huddleId
 * @param {string} exitingTenantId
 */
HuddleEngine.prototype.exit = async function exit(huddleId, exitingTenantId) {
  var client = await platformPool.connect();

  try {
    await client.query('BEGIN');

    // 1. Find all people this member contributed proximity edges for
    var { rows: memberEdges } = await client.query(
      'SELECT person_id FROM huddle_proximity WHERE huddle_id = $1 AND member_tenant_id = $2',
      [huddleId, exitingTenantId]
    );

    for (var i = 0; i < memberEdges.length; i++) {
      var personId = memberEdges[i].person_id;

      var { rows: hp } = await client.query(
        'SELECT id, contributor_count FROM huddle_people WHERE huddle_id = $1 AND person_id = $2',
        [huddleId, personId]
      );

      if (hp.length > 0) {
        if (hp[0].contributor_count <= 1) {
          // Sole contributor — delete from huddle_people
          await client.query('DELETE FROM huddle_people WHERE id = $1', [hp[0].id]);
        } else {
          // Shared — decrement
          await client.query(
            `UPDATE huddle_people
               SET contributor_count = contributor_count - 1,
                   member_connection_count = GREATEST(0, member_connection_count - 1),
                   updated_at = NOW()
             WHERE id = $1`,
            [hp[0].id]
          );
        }
      }
    }

    // 2. Delete ALL member's huddle_proximity edges
    await client.query(
      'DELETE FROM huddle_proximity WHERE huddle_id = $1 AND member_tenant_id = $2',
      [huddleId, exitingTenantId]
    );

    // 3. Remove exclusive signal contributions
    await client.query(
      `DELETE FROM huddle_signal_pool
       WHERE huddle_id = $1 AND contributed_by = $2 AND contributor_count <= 1`,
      [huddleId, exitingTenantId]
    );
    await client.query(
      `UPDATE huddle_signal_pool
         SET contributor_count = contributor_count - 1
       WHERE huddle_id = $1 AND contributed_by = $2 AND contributor_count > 1`,
      [huddleId, exitingTenantId]
    );

    // 4. Mark member as detached
    await client.query(
      `UPDATE huddle_members
         SET status = 'detached', detached_at = NOW(),
             contributed_people_count = 0, net_new_people_count = 0
       WHERE huddle_id = $1 AND tenant_id = $2`,
      [huddleId, exitingTenantId]
    );

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(function() {});
    throw err;
  } finally {
    client.release();
  }

  // 5. Async recompute best entry points
  var self = this;
  setImmediate(function() {
    self.recomputeBestEntryPoints(huddleId).catch(function(err) {
      console.error('[HuddleEngine] recomputeBestEntryPoints error:', err.message);
    });
  });
};

/**
 * Recompute best entry points for every person in a huddle.
 * Uses ProximityScorer.rankEntryPoints() and buildEntryRecommendation().
 *
 * @param {string} huddleId
 */
HuddleEngine.prototype.recomputeBestEntryPoints = async function recomputeBestEntryPoints(huddleId) {
  // Get all people in this huddle
  var { rows: people } = await platformPool.query(
    'SELECT id, person_id FROM huddle_people WHERE huddle_id = $1',
    [huddleId]
  );

  if (!people.length) return;

  // Build member name lookup
  var { rows: members } = await platformPool.query(
    `SELECT hm.tenant_id, t.name
     FROM huddle_members hm
     JOIN tenants t ON t.id = hm.tenant_id
     WHERE hm.huddle_id = $1 AND hm.status = 'active'`,
    [huddleId]
  );
  var memberNames = {};
  for (var m = 0; m < members.length; m++) {
    memberNames[members[m].tenant_id] = members[m].name;
  }

  var client = await platformPool.connect();
  try {
    await client.query('BEGIN');

    for (var i = 0; i < people.length; i++) {
      var person = people[i];

      // Get all proximity edges for this person in the huddle
      var { rows: edges } = await client.query(
        `SELECT member_tenant_id, strength_score, depth_type, currency_label,
                entry_recommendation, entry_action, last_contact
         FROM huddle_proximity
         WHERE huddle_id = $1 AND person_id = $2`,
        [huddleId, person.person_id]
      );

      var ranked = rankEntryPoints(edges);
      var recommendation = buildEntryRecommendation(ranked, memberNames);

      // Count total interactions across all members
      var { rows: interactionRows } = await client.query(
        `SELECT COALESCE(SUM(interaction_count), 0) as total
         FROM huddle_proximity
         WHERE huddle_id = $1 AND person_id = $2`,
        [huddleId, person.person_id]
      );

      await client.query(
        `UPDATE huddle_people SET
           best_member_tenant_id = $1,
           best_strength_score = $2,
           best_depth_type = $3,
           best_entry_label = $4,
           best_entry_reason = $5,
           member_connection_count = $6,
           total_team_interactions = $7,
           updated_at = NOW()
         WHERE id = $8`,
        [
          recommendation.best_member_tenant_id,
          recommendation.best_strength_score,
          recommendation.best_depth_type,
          recommendation.best_entry_label,
          recommendation.best_entry_reason,
          edges.length,
          parseInt(interactionRows[0].total) || 0,
          person.id,
        ]
      );
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(function() {});
    console.error('[HuddleEngine] recompute failed:', err.message);
    throw err;
  } finally {
    client.release();
  }
};

module.exports = { HuddleEngine };
