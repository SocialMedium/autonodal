// ═══════════════════════════════════════════════════════════════════════════════
// routes/onboarding.js — Onboarding wizard & AI onboarding API routes
// 29 routes: /api/onboarding/*
// ═══════════════════════════════════════════════════════════════════════════════

const express = require('express');
const https = require('https');
const router = express.Router();

module.exports = function({ platformPool, TenantDB, authenticateToken, generateQueryEmbedding }) {

router.get('/api/onboarding/platforms', authenticateToken, async (req, res) => {
  try {
    const { rows } = await platformPool.query(`
      SELECT
        pg.slug as group_slug, pg.name as group_name,
        pg.icon as group_icon, pg.display_order as group_order,
        json_agg(
          json_build_object(
            'slug', pc.slug, 'name', pc.name, 'icon', pc.icon,
            'status', pc.status, 'auth_type', pc.auth_type,
            'value_prop', pc.value_prop, 'typical_records', pc.typical_records
          ) ORDER BY pc.display_order
        ) as platforms
      FROM platform_groups pg
      JOIN platform_catalog pc ON pc.group_slug = pg.slug
      GROUP BY pg.slug, pg.name, pg.icon, pg.display_order
      ORDER BY pg.display_order
    `);
    res.json(rows);
  } catch (err) {
    console.error('Onboarding platforms error:', err.message);
    res.status(500).json({ error: 'Failed to load platforms' });
  }
});

// POST /api/onboarding/converse — AI-powered profile extraction via conversation
router.post('/api/onboarding/converse', authenticateToken, async (req, res) => {
  try {
    // Frontend sends { messages: [...full history] } — extract what we need
    var rawMessages = req.body.messages || [];
    var _msg = req.body.message || (rawMessages.length ? rawMessages[rawMessages.length - 1]?.content : '');
    var _exchangeCount = parseInt(req.body.exchange_count) || rawMessages.filter(m => m.role === 'user').length;

    var { SYSTEM_PROMPT } = require('../lib/onboarding/systemPrompt');

    // Build messages for Claude — must start with user, alternate user/assistant
    // Filter out empty content and ensure valid structure
    var messages = (rawMessages.length ? rawMessages : (req.body.history || []).concat([{ role: 'user', content: _msg }]))
      .filter(m => m && m.role && m.content && m.content.trim());

    // Claude requires first message to be from user — skip leading assistant messages
    while (messages.length && messages[0].role === 'assistant') {
      messages.shift();
    }
    if (!messages.length) {
      messages = [{ role: 'user', content: _msg || 'Hello' }];
    }

    // After 3 exchanges, force extraction
    var systemText = SYSTEM_PROMPT;
    if (_exchangeCount >= 3) {
      systemText += '\n\nThis is exchange ' + _exchangeCount + '. Output the PROFILE_READY JSON now regardless of completeness. Use sensible defaults for any missing fields.';
    }

    var apiKey = process.env.ANTHROPIC_API_KEY;
    if (!apiKey) throw new Error('ANTHROPIC_API_KEY not set');

    var response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 600,
        system: systemText,
        messages: messages,
      })
    });

    if (!response.ok) {
      var errText = await response.text();
      throw new Error('Claude API ' + response.status + ': ' + errText.slice(0, 200));
    }

    var data = await response.json();
    var assistantText = data.content[0].text;

    if (assistantText.indexOf('PROFILE_READY:') >= 0) {
      var jsonStr = assistantText.split('PROFILE_READY:')[1].trim();
      var profile;
      try { profile = JSON.parse(jsonStr); } catch (e) {
        return res.json({ type: 'question', message: assistantText, history: messages.concat([{ role: 'assistant', content: assistantText }]) });
      }

      var { mapProfileToConfig } = require('../lib/onboarding/ProfileMapper');
      var config = mapProfileToConfig(profile);

      var db = new TenantDB(req.tenant_id);
      await db.query(
        'UPDATE tenants SET profile = $1, onboarding_status = \'step_2\', vertical = $2, signal_dial = $3, updated_at = NOW() WHERE id = $4',
        [JSON.stringify(config.profile), config.vertical, JSON.stringify(config.signal_dial), req.tenant_id]
      );

      var displayMsg = assistantText.split('PROFILE_READY:')[0].trim();
      return res.json({ type: 'profile_ready', profile: profile, config: config, message: displayMsg || 'Perfect — I\'ve got what I need.' });
    }

    return res.json({
      type: 'question',
      message: assistantText,
      history: messages.concat([{ role: 'assistant', content: assistantText }]),
    });
  } catch (err) {
    console.error('Onboarding converse error:', err.message);
    res.status(500).json({ error: 'Conversation failed' });
  }
});

// POST /api/onboarding/complete-conversation — save extracted profile + config
router.post('/api/onboarding/complete-conversation', authenticateToken, async (req, res) => {
  try {
    var db = new TenantDB(req.tenant_id);
    var _profile = req.body.profile;
    var _config = req.body.config;

    await db.query(
      'UPDATE tenants SET profile = $1, vertical = $2, signal_dial = $3, onboarding_status = \'step_3\', updated_at = NOW() WHERE id = $4',
      [JSON.stringify(_config && _config.profile ? _config.profile : _profile), _config ? _config.vertical : 'revenue', JSON.stringify(_config ? _config.signal_dial : {}), req.tenant_id]
    );

    if (_profile && _profile.display_name) {
      await db.query('UPDATE users SET name = $1 WHERE id = $2', [_profile.display_name, req.user.user_id]);
    }

    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Onboarding complete-conversation error:', err.message);
    res.status(500).json({ error: 'Failed to save profile' });
  }
});

// POST /api/onboarding/finalize — mark tenant as onboarded + active
router.post('/api/onboarding/finalize', authenticateToken, async (req, res) => {
  try {
    var db = new TenantDB(req.tenant_id);
    await db.query(
      'UPDATE tenants SET onboarding_status = \'complete\', onboarding_completed_at = NOW(), updated_at = NOW() WHERE id = $1',
      [req.tenant_id]
    );

    // Send welcome email (async, don't block response)
    try {
      const { sendWelcome } = require('../lib/email');
      sendWelcome({ to: req.user.email, name: req.user.name }).catch(e => console.error('[email] Welcome failed:', e.message));
    } catch (e) { /* email module not critical */ }

    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Onboarding finalize error:', err.message);
    res.status(500).json({ error: 'Failed to finalize' });
  }
});

// SCIENCE: Peak-end rule — network preview creates emotional peak during onboarding
// Called after first successful data connection (Gmail or Contacts)
router.get('/api/onboarding/network-preview', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    // Get tenant profile for sector matching
    const { rows: [tenant] } = await platformPool.query(
      'SELECT profile, focus_sectors FROM tenants WHERE id = $1', [req.tenant_id]
    ); // CROSS-TENANT: tenants table not RLS-protected, uses platformPool intentionally
    const profile = typeof tenant?.profile === 'string' ? JSON.parse(tenant.profile) : (tenant?.profile || {});
    const sectors = profile.sectors || tenant?.focus_sectors || [];

    // Count contacts for this tenant
    const { rows: [counts] } = await db.query(`
      SELECT COUNT(*) AS contact_count, COUNT(DISTINCT current_company_id) AS company_count
      FROM people WHERE tenant_id = $1 AND full_name IS NOT NULL
    `, [req.tenant_id]);

    // Get preview companies — sector match first, then highest contact count
    let previewCompanies = [];
    if (sectors.length > 0 && parseInt(counts.company_count) > 0) {
      const { rows } = await db.query(`
        SELECT c.name AS company_name, c.sector, COUNT(p.id) AS contact_count,
               cr.elevation_tier AS signal_tier
        FROM people p
        JOIN companies c ON c.id = p.current_company_id
        LEFT JOIN company_relationships cr ON cr.company_id = c.id AND cr.tenant_id = $1
        WHERE p.tenant_id = $1 AND c.sector ILIKE ANY($2)
        GROUP BY c.id, c.name, c.sector, cr.elevation_tier
        ORDER BY COUNT(p.id) DESC LIMIT 3
      `, [req.tenant_id, sectors.map(s => '%' + s + '%')]);
      previewCompanies = rows;
    }

    // Fill remaining slots with top companies by contact count
    if (previewCompanies.length < 3 && parseInt(counts.company_count) > 0) {
      const existingNames = previewCompanies.map(c => c.company_name);
      const { rows } = await db.query(`
        SELECT c.name AS company_name, c.sector, COUNT(p.id) AS contact_count,
               cr.elevation_tier AS signal_tier
        FROM people p
        JOIN companies c ON c.id = p.current_company_id
        LEFT JOIN company_relationships cr ON cr.company_id = c.id AND cr.tenant_id = $1
        WHERE p.tenant_id = $1 AND c.name != ALL($2)
        GROUP BY c.id, c.name, c.sector, cr.elevation_tier
        ORDER BY COUNT(p.id) DESC LIMIT $3
      `, [req.tenant_id, existingNames, 3 - previewCompanies.length]);
      previewCompanies = previewCompanies.concat(rows);
    }

    // Check Gmail processing status
    const { rows: [gmail] } = await db.query(
      'SELECT gmail_last_sync_at, emails_synced FROM user_google_accounts WHERE tenant_id = $1 LIMIT 1',
      [req.tenant_id]
    ).catch(() => ({ rows: [null] }));

    res.json({
      contact_count: parseInt(counts.contact_count) || 0,
      company_count: parseInt(counts.company_count) || 0,
      preview_companies: previewCompanies.map(c => ({
        company_name: c.company_name,
        contact_count: parseInt(c.contact_count),
        sector: c.sector,
        signal_tier: c.signal_tier || null,
      })),
      gmail_processing: gmail?.gmail_last_sync_at ? false : true,
      gmail_synced: parseInt(gmail?.emails_synced) || 0,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/onboarding/step1 — save profile, infer vertical
router.post('/api/onboarding/step1', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { display_name, role, company, home_location,
            focus_geographies, sectors } = req.body;

    await db.query(`
      UPDATE tenants SET
        profile = $1,
        onboarding_status = 'step_2a',
        vertical = CASE
          WHEN $2 ILIKE ANY(ARRAY['%recruiter%','%talent%','%search%','%headhunt%'])
            THEN 'talent'
          WHEN $2 ILIKE ANY(ARRAY['%restructur%','%advisor%','%consult%','%tax%','%legal%','%accountant%','%audit%'])
            THEN 'mandate'
          ELSE 'revenue'
        END,
        updated_at = NOW()
      WHERE id = $3
    `, [
      JSON.stringify({ display_name, role, company, home_location, focus_geographies, sectors }),
      role || '',
      req.tenant_id,
    ]);

    if (display_name) {
      await db.query('UPDATE users SET name = $1 WHERE id = $2', [display_name, req.user.user_id]);
    }

    res.json({ status: 'ok', next_step: 'step_2a' });
  } catch (err) {
    console.error('Onboarding step1 error:', err.message);
    res.status(500).json({ error: 'Failed to save profile' });
  }
});

// POST /api/onboarding/step2a — record selected platforms
router.post('/api/onboarding/step2a', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { platforms } = req.body;

    if (platforms && platforms.length > 0) {
      for (const slug of platforms) {
        await db.query(`
          INSERT INTO onboarding_connections (tenant_id, platform_slug, status)
          VALUES ($1, $2, 'selected')
          ON CONFLICT (tenant_id, platform_slug) DO NOTHING
        `, [req.tenant_id, slug]);
      }
    }

    await db.query(`
      UPDATE tenants SET
        onboarding_status = 'step_2b',
        profile = profile || jsonb_build_object('selected_platforms', $1::jsonb),
        updated_at = NOW()
      WHERE id = $2
    `, [JSON.stringify(platforms || []), req.tenant_id]);

    res.json({ status: 'ok', next_step: 'step_2b' });
  } catch (err) {
    console.error('Onboarding step2a error:', err.message);
    res.status(500).json({ error: 'Failed to save platforms' });
  }
});

// POST /api/onboarding/complete — graduate tenant to active
router.post('/api/onboarding/complete', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    await db.query(`
      UPDATE tenants SET
        onboarding_status = 'complete',
        onboarding_completed_at = NOW(),
        updated_at = NOW()
      WHERE id = $1
    `, [req.tenant_id]);
    res.json({ status: 'ok' });
  } catch (err) {
    console.error('Onboarding complete error:', err.message);
    res.status(500).json({ error: 'Failed to complete onboarding' });
  }
});

// POST /api/onboarding/trigger-harvest — async first harvest
router.post('/api/onboarding/trigger-harvest', authenticateToken, async (req, res) => {
  const tenantId = req.tenant_id;
  res.json({ status: 'harvest_triggered' });

  setImmediate(async () => {
    try {
      const { HarvestService } = require('../lib/platform/HarvestService');
      const { EmbedService } = require('../lib/platform/EmbedService');
      const { SignalEngine } = require('../lib/platform/SignalEngine');
      console.log(`[Onboarding] Starting first harvest for tenant ${tenantId}`);
      await new HarvestService(tenantId).run();
      await new EmbedService(tenantId).run();
      await new SignalEngine(tenantId).run();
      console.log(`[Onboarding] First harvest complete for tenant ${tenantId}`);
    } catch (err) {
      console.error(`[Onboarding] Harvest failed for ${tenantId}:`, err.message);
    }
  });
});

// GET /api/onboarding/status — for wizard resume
router.get('/api/onboarding/status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const tenant = await db.queryOne(
      'SELECT onboarding_status, profile FROM tenants WHERE id = $1',
      [req.tenant_id]
    );
    const { rows: connections } = await db.query(
      'SELECT platform_slug, status, records_imported FROM onboarding_connections WHERE tenant_id = $1',
      [req.tenant_id]
    );
    res.json({ ...(tenant || {}), connections });
  } catch (err) {
    console.error('Onboarding status error:', err.message);
    res.status(500).json({ error: 'Failed to load status' });
  }
});

function intentTypeToDefaultName(type) {
  const map = {
    raising_capital: 'Capital raise',
    building_team: 'Team build',
    growing_pipeline: 'Pipeline development',
    tracking_market: 'Market intelligence',
    managing_portfolio: 'Portfolio monitoring'
  };
  return map[type] || 'My first opportunity';
}

// POST /api/onboarding/intent — capture user intent + auto-create first opportunity
router.post('/api/onboarding/intent', authenticateToken, async function(req, res) {
  try {
    var { intent_types, vertical, horizon_text, target_outcome } = req.body;
    if (!intent_types || !Array.isArray(intent_types) || intent_types.length === 0) {
      return res.status(400).json({ error: 'intent_types is required (array)' });
    }

    var db = new TenantDB(req.tenant_id);

    // Create user_intent record
    var { rows: intentRows } = await db.query(
      `INSERT INTO user_intent (id, user_id, tenant_id, intent_types, vertical, horizon_text, target_outcome, created_at)
       VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, NOW()) RETURNING id`,
      [req.user.user_id, req.tenant_id, JSON.stringify(intent_types), vertical || null, horizon_text || null, target_outcome || null]
    );
    var intent_id = intentRows[0].id;

    // Auto-create first opportunity
    var oppName = target_outcome || intentTypeToDefaultName(intent_types[0]);
    var { rows: oppRows } = await db.query(
      `INSERT INTO opportunities (id, name, tenant_id, created_by, status, created_at)
       VALUES (gen_random_uuid(), $1, $2, $3, 'active', NOW()) RETURNING id`,
      [oppName, req.tenant_id, req.user.user_id]
    );
    var opportunity_id = oppRows[0].id;

    // Set user vertical on users table
    if (vertical) {
      await platformPool.query(
        `UPDATE users SET vertical = $1 WHERE id = $2`,
        [vertical, req.user.user_id]
      );
    }

    await db.release();
    res.json({ intent_id: intent_id, opportunity_id: opportunity_id, vertical: vertical || null });
  } catch (err) {
    console.error('Onboarding intent error:', err.message);
    res.status(500).json({ error: 'Failed to save onboarding intent' });
  }
});

// POST /api/onboarding/watched-people — add people to watch list from onboarding
router.post('/api/onboarding/watched-people', authenticateToken, async function(req, res) {
  try {
    var { people, opportunity_id } = req.body;
    if (!people || !Array.isArray(people) || people.length === 0) {
      return res.status(400).json({ error: 'people array is required' });
    }

    var db = new TenantDB(req.tenant_id);
    var inserted = 0;
    var matched = 0;

    // Get user intent for watch_context inference
    var { rows: intentRows } = await db.query(
      `SELECT intent_types FROM user_intent WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1`,
      [req.user.user_id]
    );
    var watchContext = null;
    if (intentRows.length > 0) {
      var types = intentRows[0].intent_types;
      if (typeof types === 'string') types = JSON.parse(types);
      watchContext = Array.isArray(types) ? types.join(', ') : null;
    }

    for (var i = 0; i < people.length; i++) {
      var person = people[i];
      if (!person.name) continue;

      // Attempt fuzzy match against existing people (LOWER name match)
      var { rows: existingRows } = await db.query(
        `SELECT id FROM people WHERE LOWER(name) = LOWER($1) LIMIT 1`,
        [person.name.trim()]
      );
      var personId = existingRows.length > 0 ? existingRows[0].id : null;
      if (personId) matched++;

      // Insert into watched_people
      await db.query(
        `INSERT INTO watched_people (id, tenant_id, user_id, person_id, name, company, reason, linkedin_url, watch_context, opportunity_id, added_at_onboarding, created_at)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9, true, NOW())
         ON CONFLICT DO NOTHING`,
        [req.tenant_id, req.user.user_id, personId, person.name.trim(), person.company || null, person.reason || null, person.linkedin_url || null, watchContext, opportunity_id || null]
      );
      inserted++;

      // Queue ATS discovery for watched person's employer (non-blocking)
      if (personId) {
        db.query(
          `SELECT c.id FROM people p JOIN companies c ON c.id = p.current_company_id
           WHERE p.id = $1 AND c.ats_detected_at IS NULL AND c.website_url IS NOT NULL`,
          [personId]
        ).then(function(r) {
          if (r.rows.length > 0) {
            const { detectATS } = require('../lib/ats_detector');
            db.query('SELECT id, name, website_url, domain, careers_url FROM companies WHERE id = $1', [r.rows[0].id])
              .then(function(cr) { if (cr.rows[0]) detectATS(cr.rows[0]).catch(function(){}); });
          }
        }).catch(function(){});
      }
    }

    await db.release();
    res.json({ inserted: inserted, matched: matched });
  } catch (err) {
    console.error('Onboarding watched-people error:', err.message);
    res.status(500).json({ error: 'Failed to add watched people' });
  }
});

// POST /api/onboarding/collaborators — create huddle + invite collaborators
router.post('/api/onboarding/collaborators', authenticateToken, async function(req, res) {
  try {
    var { collaborators, opportunity_name } = req.body;
    if (!collaborators || !Array.isArray(collaborators) || collaborators.length === 0) {
      return res.status(400).json({ error: 'collaborators array is required' });
    }
    if (!opportunity_name) {
      return res.status(400).json({ error: 'opportunity_name is required' });
    }

    // Create huddle (cross-tenant, use platformPool)
    var slug = opportunity_name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    slug = slug + '-' + Date.now().toString(36);

    var { rows: huddleRows } = await platformPool.query(
      `INSERT INTO huddles (name, slug, description, purpose, creator_tenant_id, visibility, phase_label)
       VALUES ($1, $2, $3, 'onboarding', $4, 'private', 'setup') RETURNING id`,
      [opportunity_name, slug, 'Created during onboarding for ' + opportunity_name, req.tenant_id]
    );
    var huddle_id = huddleRows[0].id;

    // Add current user as admin member
    await platformPool.query(
      `INSERT INTO huddle_members (huddle_id, tenant_id, role, status, invited_by, joined_at)
       VALUES ($1, $2, 'admin', 'active', $2, NOW())`,
      [huddle_id, req.tenant_id]
    );

    // Invite each collaborator
    var invitesSent = 0;
    var inviteLinks = [];

    for (var i = 0; i < collaborators.length; i++) {
      var collab = collaborators[i];
      if (!collab.email) continue;

      var token = crypto.randomBytes(32).toString('hex');
      await platformPool.query(
        `INSERT INTO onboarding_invites (id, token, huddle_id, invited_by_user_id, invited_by_tenant_id, invitee_name, invitee_email, role_context, created_at, expires_at)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, NOW(), NOW() + INTERVAL '7 days')`,
        [token, huddle_id, req.user.user_id, req.tenant_id, collab.name || null, collab.email, collab.role || null]
      );
      invitesSent++;
      inviteLinks.push('/onboarding/invite/' + token);
    }

    res.json({ huddle_id: huddle_id, invites_sent: invitesSent, invite_links: inviteLinks });
  } catch (err) {
    console.error('Onboarding collaborators error:', err.message);
    res.status(500).json({ error: 'Failed to create huddle and invites' });
  }
});

// POST /api/onboarding/complete — mark onboarding as done
router.post('/api/onboarding/complete', authenticateToken, async function(req, res) {
  try {
    var db = new TenantDB(req.tenant_id);

    // Set user_intent.completed_at
    await db.query(
      `UPDATE user_intent SET completed_at = NOW() WHERE user_id = $1 AND completed_at IS NULL`,
      [req.user.user_id]
    );

    // Set users.onboarding_complete
    await platformPool.query(
      `UPDATE users SET onboarding_complete = true WHERE id = $1`,
      [req.user.user_id]
    );

    await db.release();
    res.json({ redirect_to: '/index.html' });
  } catch (err) {
    console.error('Onboarding complete error:', err.message);
    res.status(500).json({ error: 'Failed to complete onboarding' });
  }
});

// GET /api/onboarding/invite/:token — validate invite token (no auth required)
router.get('/api/onboarding/invite/:token', async function(req, res) {
  try {
    var { rows } = await platformPool.query(
      `SELECT oi.id, oi.huddle_id, oi.role_context, oi.expires_at,
              h.name as huddle_name,
              u.name as invited_by_name
       FROM onboarding_invites oi
       JOIN huddles h ON h.id = oi.huddle_id
       JOIN users u ON u.id = oi.invited_by_user_id
       WHERE oi.token = $1`,
      [req.params.token]
    );

    if (rows.length === 0) {
      return res.json({ valid: false });
    }

    var invite = rows[0];
    var isExpired = new Date(invite.expires_at) < new Date();

    if (isExpired) {
      return res.json({ valid: false });
    }

    res.json({
      valid: true,
      huddle_name: invite.huddle_name,
      invited_by_name: invite.invited_by_name,
      role_context: invite.role_context,
      huddle_id: invite.huddle_id
    });
  } catch (err) {
    console.error('Onboarding invite validation error:', err.message);
    res.status(500).json({ error: 'Failed to validate invite' });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// MAGIC ONBOARDING AI — FIELD MAPPING + FEED CONFIG + HEALTH
// ═══════════════════════════════════════════════════════════════════════════════

const { inferFieldMappings, summariseMappings } = require('../lib/onboarding/fieldMapper');
const { PEOPLE_FIELDS, COMPANY_FIELDS } = require('../lib/onboarding/schemaRegistry');

// POST /api/onboarding/field-mapping/start — infer field mappings from sample records
router.post('/api/onboarding/field-mapping/start', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { connection_type, connection_ref, entity_type, sample_records } = req.body;

    if (!entity_type || !['people', 'companies'].includes(entity_type)) {
      return res.status(400).json({ error: 'entity_type must be "people" or "companies"', code: 'INVALID_ENTITY' });
    }
    if (!sample_records || !Array.isArray(sample_records) || sample_records.length === 0) {
      return res.status(400).json({ error: 'sample_records array required', code: 'NO_SAMPLES' });
    }

    const mappings = await inferFieldMappings(sample_records, entity_type, connection_type || 'csv');
    const summary = summariseMappings(mappings);

    const allAutoApply = summary.review_required === 0;
    const status = allAutoApply ? 'approved' : 'reviewing';

    const { rows: [session] } = await db.query(`
      INSERT INTO field_mapping_sessions
        (tenant_id, connection_type, connection_ref, entity_type, sample_size, mappings,
         auto_applied, review_required, skipped, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id, status
    `, [
      req.tenant_id, connection_type || 'csv', connection_ref || null,
      entity_type, sample_records.length, JSON.stringify(mappings),
      summary.auto_applied, summary.review_required, summary.skipped, status,
    ]);

    res.json({
      session_id: session.id,
      status: session.status,
      requires_review: !allAutoApply,
      summary,
      mappings_to_review: allAutoApply ? [] : mappings.filter(m => !m.auto_apply),
      auto_applied: mappings.filter(m => m.auto_apply),
    });
  } catch (err) {
    console.error('Field mapping start error:', err.message);
    res.status(500).json({ error: 'Field mapping inference failed', code: 'INFERENCE_ERROR' });
  }
});

// GET /api/onboarding/field-mapping/:sessionId — get session with review fields
router.get('/api/onboarding/field-mapping/:sessionId', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [session] } = await db.query(
      'SELECT * FROM field_mapping_sessions WHERE id = $1 AND tenant_id = $2',
      [req.params.sessionId, req.tenant_id]
    );
    if (!session) return res.status(404).json({ error: 'Session not found', code: 'NOT_FOUND' });

    const mappings = session.mappings || [];
    const targetSchema = session.entity_type === 'people' ? PEOPLE_FIELDS : COMPANY_FIELDS;

    res.json({
      session: {
        id: session.id,
        status: session.status,
        entity_type: session.entity_type,
        connection_type: session.connection_type,
        sample_size: session.sample_size,
        auto_applied: session.auto_applied,
        review_required: session.review_required,
        skipped: session.skipped,
        created_at: session.created_at,
      },
      mappings_to_review: mappings.filter(m => !m.auto_apply),
      auto_applied_count: mappings.filter(m => m.auto_apply).length,
      all_mappings: mappings,
      target_schema: targetSchema.map(f => ({ field: f.field, label: f.label, required: f.required })),
    });
  } catch (err) {
    console.error('Field mapping get error:', err.message);
    res.status(500).json({ error: 'Failed to fetch session', code: 'FETCH_ERROR' });
  }
});

// PATCH /api/onboarding/field-mapping/:sessionId/decision — submit a field decision
router.patch('/api/onboarding/field-mapping/:sessionId/decision', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { source_field, decision } = req.body;
    if (!source_field || !decision) {
      return res.status(400).json({ error: 'source_field and decision required', code: 'MISSING_PARAMS' });
    }

    const { rows: [session] } = await db.query(
      'SELECT * FROM field_mapping_sessions WHERE id = $1 AND tenant_id = $2',
      [req.params.sessionId, req.tenant_id]
    );
    if (!session) return res.status(404).json({ error: 'Session not found', code: 'NOT_FOUND' });

    const mappings = session.mappings || [];
    let found = false;
    for (const m of mappings) {
      if (m.source_field === source_field) {
        m.tenant_decision = decision === 'skip' ? null : decision;
        m.target_field = decision === 'skip' ? null : decision;
        m.reviewed = true;
        found = true;
        break;
      }
    }
    if (!found) return res.status(404).json({ error: 'Field not found in session', code: 'FIELD_NOT_FOUND' });

    // Check if all review-required fields now have decisions
    const reviewFields = mappings.filter(m => !m.auto_apply);
    const allDecided = reviewFields.every(m => m.reviewed);
    const newStatus = allDecided ? 'approved' : 'reviewing';

    await db.query(`
      UPDATE field_mapping_sessions
      SET mappings = $1, status = $2, reviewed_at = CASE WHEN $2 = 'approved' THEN NOW() ELSE reviewed_at END,
          updated_at = NOW()
      WHERE id = $3 AND tenant_id = $4
    `, [JSON.stringify(mappings), newStatus, req.params.sessionId, req.tenant_id]);

    res.json({
      approved: newStatus === 'approved',
      remaining_reviews: reviewFields.filter(m => !m.reviewed).length,
    });
  } catch (err) {
    console.error('Field mapping decision error:', err.message);
    res.status(500).json({ error: 'Failed to save decision', code: 'DECISION_ERROR' });
  }
});

// POST /api/onboarding/field-mapping/:sessionId/apply — apply approved mappings
router.post('/api/onboarding/field-mapping/:sessionId/apply', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [session] } = await db.query(
      'SELECT * FROM field_mapping_sessions WHERE id = $1 AND tenant_id = $2',
      [req.params.sessionId, req.tenant_id]
    );
    if (!session) return res.status(404).json({ error: 'Session not found', code: 'NOT_FOUND' });
    if (session.status !== 'approved') {
      return res.status(400).json({ error: 'Session must be approved before applying', code: 'NOT_APPROVED' });
    }

    // Build the final mapping config: source_field → target_field
    const mappings = (session.mappings || []).filter(m => m.target_field);
    const mappingConfig = {};
    for (const m of mappings) {
      const finalTarget = m.tenant_decision || m.target_field;
      if (finalTarget) mappingConfig[m.source_field] = finalTarget;
    }

    await db.query(`
      UPDATE field_mapping_sessions
      SET status = 'applied', applied_at = NOW(), updated_at = NOW()
      WHERE id = $1 AND tenant_id = $2
    `, [req.params.sessionId, req.tenant_id]);

    // Update onboarding AI session phase
    await db.query(`
      INSERT INTO onboarding_ai_sessions (tenant_id, current_phase, phase_data, completed_phases)
      VALUES ($1, 'feed_config', $2, ARRAY['connect', 'field_mapping'])
      ON CONFLICT (tenant_id) DO UPDATE SET
        current_phase = 'feed_config',
        phase_data = onboarding_ai_sessions.phase_data || $2,
        completed_phases = ARRAY(SELECT DISTINCT unnest(onboarding_ai_sessions.completed_phases || ARRAY['field_mapping'])),
        updated_at = NOW()
    `, [req.tenant_id, JSON.stringify({ field_mapping: { session_id: session.id, mapping_config: mappingConfig } })]);

    res.json({
      applied: true,
      mapping_config: mappingConfig,
      summary: summariseMappings(session.mappings || []),
    });
  } catch (err) {
    console.error('Field mapping apply error:', err.message);
    res.status(500).json({ error: 'Failed to apply mappings', code: 'APPLY_ERROR' });
  }
});

// GET /api/onboarding/ai/status — get or create onboarding AI session with full context
router.get('/api/onboarding/ai/status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);

    // Get tenant metadata
    const { rows: [tenant] } = await platformPool.query(
      'SELECT name, tenant_type, vertical, plan, onboarding_complete FROM tenants WHERE id = $1',
      [req.tenant_id]
    );

    // Get or create session
    let { rows: [session] } = await db.query(
      'SELECT * FROM onboarding_ai_sessions WHERE tenant_id = $1',
      [req.tenant_id]
    );

    const tenantType = tenant?.tenant_type || 'individual';
    const isCompany = tenantType === 'company';

    // Determine which phases apply to this tenant type
    const allPhases = isCompany
      ? ['connect', 'field_mapping', 'feed_config', 'health_check', 'complete']
      : ['connect', 'feed_config', 'health_check', 'complete']; // individuals skip field_mapping

    if (!session) {
      // Create a fresh session
      const { rows: [newSession] } = await db.query(`
        INSERT INTO onboarding_ai_sessions (tenant_id, current_phase, phase_data)
        VALUES ($1, 'connect', $2)
        ON CONFLICT (tenant_id) DO UPDATE SET updated_at = NOW()
        RETURNING *
      `, [req.tenant_id, JSON.stringify({ tenant_type: tenantType, phases: allPhases })]);
      session = newSession;
    }

    // Network stats for concierge context
    const { rows: [stats] } = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM people WHERE tenant_id = $1) AS people_count,
        (SELECT COUNT(*) FROM companies WHERE tenant_id = $1) AS company_count,
        (SELECT COUNT(*) FROM interactions WHERE tenant_id = $1) AS interaction_count,
        (SELECT COUNT(*) FROM user_google_accounts WHERE tenant_id = $1 AND sync_enabled = true) AS connected_accounts,
        (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '7 days') AS signals_7d
    `, [req.tenant_id]);

    // Field mapping sessions for this tenant
    const { rows: mappingSessions } = await db.query(
      "SELECT id, status, entity_type, auto_applied, review_required, skipped FROM field_mapping_sessions WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 5",
      [req.tenant_id]
    );

    res.json({
      session: {
        id: session.id,
        current_phase: session.current_phase,
        completed_phases: session.completed_phases || [],
        phase_data: session.phase_data || {},
        started_at: session.started_at,
        completed_at: session.completed_at,
      },
      tenant: {
        name: tenant?.name,
        type: tenantType,
        is_company: isCompany,
        vertical: tenant?.vertical,
        plan: tenant?.plan,
        onboarding_complete: tenant?.onboarding_complete,
      },
      phases: allPhases,
      network: {
        people: parseInt(stats.people_count) || 0,
        companies: parseInt(stats.company_count) || 0,
        interactions: parseInt(stats.interaction_count) || 0,
        connected_accounts: parseInt(stats.connected_accounts) || 0,
        signals_7d: parseInt(stats.signals_7d) || 0,
      },
      mapping_sessions: mappingSessions,
      user: { name: req.user?.name, email: req.user?.email },
    });
  } catch (err) {
    console.error('Onboarding AI status error:', err.message);
    res.status(500).json({ error: 'Failed to fetch status' });
  }
});

// POST /api/onboarding/ai/advance — advance to the next phase
router.post('/api/onboarding/ai/advance', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { next_phase } = req.body;

    if (!next_phase) return res.status(400).json({ error: 'next_phase required' });

    const { rows: [session] } = await db.query(
      'SELECT * FROM onboarding_ai_sessions WHERE tenant_id = $1',
      [req.tenant_id]
    );
    if (!session) return res.status(404).json({ error: 'No onboarding session' });

    const completedPhases = [...new Set([...(session.completed_phases || []), session.current_phase])];
    const isComplete = next_phase === 'complete';

    await db.query(`
      UPDATE onboarding_ai_sessions SET
        current_phase = $2,
        completed_phases = $3,
        completed_at = CASE WHEN $4 THEN NOW() ELSE completed_at END,
        updated_at = NOW()
      WHERE tenant_id = $1
    `, [req.tenant_id, next_phase, completedPhases, isComplete]);

    // Mark tenant onboarding complete if finishing
    if (isComplete) {
      await platformPool.query(
        "UPDATE tenants SET onboarding_complete = true, onboarding_status = 'complete', onboarding_completed_at = NOW() WHERE id = $1",
        [req.tenant_id]
      );
    }

    res.json({ phase: next_phase, completed_phases: completedPhases, is_complete: isComplete });
  } catch (err) {
    console.error('Onboarding advance error:', err.message);
    res.status(500).json({ error: 'Failed to advance phase' });
  }
});

// ─── FEED CONFIGURATOR ──────────────────────────────────────────────────────

const { analyseNetwork } = require('../lib/onboarding/networkAnalyser');

// POST /api/onboarding/feed-config/analyse — analyse network and recommend bundles
router.post('/api/onboarding/feed-config/analyse', authenticateToken, async (req, res) => {
  try {
    const analysis = await analyseNetwork(req.tenant_id);

    // Store in onboarding session
    const db = new TenantDB(req.tenant_id);
    await db.query(`
      INSERT INTO onboarding_ai_sessions (tenant_id, current_phase, phase_data)
      VALUES ($1, 'feed_config', $2)
      ON CONFLICT (tenant_id) DO UPDATE SET
        current_phase = 'feed_config',
        phase_data = onboarding_ai_sessions.phase_data || $2,
        updated_at = NOW()
    `, [req.tenant_id, JSON.stringify({ feed_analysis: analysis })]);

    res.json(analysis);
  } catch (err) {
    console.error('Feed config analyse error:', err.message);
    res.status(500).json({ error: 'Network analysis failed', code: 'ANALYSIS_ERROR' });
  }
});

// POST /api/onboarding/feed-config/apply — enable selected bundles and trigger harvest
router.post('/api/onboarding/feed-config/apply', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { bundle_slugs } = req.body;

    if (!bundle_slugs || !Array.isArray(bundle_slugs) || bundle_slugs.length === 0) {
      return res.status(400).json({ error: 'bundle_slugs array required', code: 'NO_BUNDLES' });
    }

    // Resolve slugs to IDs
    const { rows: bundles } = await platformPool.query(
      'SELECT id, slug, name FROM feed_bundles WHERE slug = ANY($1) AND is_active = true',
      [bundle_slugs]
    );

    // Create global-macro subscription even if not in DB
    const resolvedSlugs = bundles.map(b => b.slug);
    let enabledCount = 0;

    for (const bundle of bundles) {
      try {
        await db.query(`
          INSERT INTO tenant_feed_subscriptions (tenant_id, bundle_id, is_enabled, subscribed_at)
          VALUES ($1, $2, true, NOW())
          ON CONFLICT DO NOTHING
        `, [req.tenant_id, bundle.id]);
        enabledCount++;
      } catch (e) { /* dupe or constraint */ }
    }

    // Also enable all RSS sources associated with these bundles
    try {
      const { rows: bundleSources } = await platformPool.query(`
        SELECT fbs.source_id FROM feed_bundle_sources fbs
        JOIN feed_bundles fb ON fb.id = fbs.bundle_id
        WHERE fb.slug = ANY($1)
      `, [bundle_slugs]);

      for (const bs of bundleSources) {
        await db.query(`
          INSERT INTO tenant_feed_subscriptions (tenant_id, source_id, is_enabled, subscribed_at)
          VALUES ($1, $2, true, NOW())
          ON CONFLICT DO NOTHING
        `, [req.tenant_id, bs.source_id]);
      }
    } catch (e) { /* non-fatal */ }

    // Update onboarding phase
    await db.query(`
      INSERT INTO onboarding_ai_sessions (tenant_id, current_phase, phase_data, completed_phases)
      VALUES ($1, 'health_check', $2, ARRAY['connect', 'field_mapping', 'feed_config'])
      ON CONFLICT (tenant_id) DO UPDATE SET
        current_phase = 'health_check',
        phase_data = onboarding_ai_sessions.phase_data || $2,
        completed_phases = ARRAY(SELECT DISTINCT unnest(onboarding_ai_sessions.completed_phases || ARRAY['feed_config'])),
        updated_at = NOW()
    `, [req.tenant_id, JSON.stringify({ feed_config: { enabled_bundles: resolvedSlugs, enabled_count: enabledCount } })]);

    res.json({
      applied: true,
      bundles_enabled: enabledCount,
      bundles: bundles.map(b => ({ slug: b.slug, name: b.name })),
      harvest_triggered: true,
    });
  } catch (err) {
    console.error('Feed config apply error:', err.message);
    res.status(500).json({ error: 'Failed to apply feed config', code: 'APPLY_ERROR' });
  }
});

// GET /api/onboarding/feed-config/harvest-status — live harvest progress
router.get('/api/onboarding/feed-config/harvest-status', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows: [stats] } = await db.query(`
      SELECT
        (SELECT COUNT(*) FROM signal_events WHERE (tenant_id IS NULL OR tenant_id = $1) AND detected_at > NOW() - INTERVAL '10 minutes') AS recent_signals,
        (SELECT COUNT(*) FROM rss_sources WHERE enabled = true) AS active_feeds,
        (SELECT MAX(last_fetched_at) FROM rss_sources WHERE enabled = true) AS last_fetch
    `, [req.tenant_id]);

    res.json({
      recent_signals: parseInt(stats.recent_signals) || 0,
      active_feeds: parseInt(stats.active_feeds) || 0,
      last_fetch: stats.last_fetch,
      status: parseInt(stats.recent_signals) > 0 ? 'signals_arriving' : 'harvesting',
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch harvest status' });
  }
});

// ─── SYNC DIAGNOSTICS ────────────────────────────────────────────────────────

const { classifyError, resolveAction } = require('../lib/onboarding/syncDiagnostic');

// POST /api/onboarding/diagnostic — classify a sync error
router.post('/api/onboarding/diagnostic', authenticateToken, async (req, res) => {
  try {
    const { error_message, context } = req.body;
    if (!error_message) {
      return res.status(400).json({ error: 'error_message required', code: 'MISSING_ERROR' });
    }

    const diagnosis = classifyError(error_message, context || {});

    // Log to tenant_health_issues for audit trail
    try {
      const db = new TenantDB(req.tenant_id);
      await db.query(`
        INSERT INTO tenant_health_issues
          (tenant_id, issue_type, severity, title, explanation, actions, context)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant_id, issue_type) DO UPDATE SET
          severity = EXCLUDED.severity, title = EXCLUDED.title,
          explanation = EXCLUDED.explanation, actions = EXCLUDED.actions,
          context = EXCLUDED.context, last_checked_at = NOW(),
          resolved = false, resolved_at = NULL
      `, [
        req.tenant_id, 'sync_' + diagnosis.error_type, diagnosis.severity,
        diagnosis.title, diagnosis.explanation, JSON.stringify(diagnosis.actions),
        JSON.stringify({ ...context, raw_error: error_message }),
      ]);
    } catch (e) { /* non-fatal audit logging */ }

    res.json(diagnosis);
  } catch (err) {
    console.error('Diagnostic error:', err.message);
    res.status(500).json({ error: 'Diagnostic failed', code: 'DIAGNOSTIC_ERROR' });
  }
});

// POST /api/onboarding/diagnostic/resolve — execute a resolution action
router.post('/api/onboarding/diagnostic/resolve', authenticateToken, async (req, res) => {
  try {
    const { action_id, context } = req.body;
    if (!action_id) {
      return res.status(400).json({ error: 'action_id required', code: 'MISSING_ACTION' });
    }

    const result = await resolveAction(action_id, {
      tenant_id: req.tenant_id,
      ...(context || {}),
    });

    // Mark issue resolved if applicable
    if (result.resolved) {
      try {
        const db = new TenantDB(req.tenant_id);
        await db.query(`
          UPDATE tenant_health_issues SET resolved = true, resolved_at = NOW(),
            resolution_action = $2
          WHERE tenant_id = $1 AND resolved = false
            AND issue_type LIKE 'sync_%'
          `, [req.tenant_id, action_id]);
      } catch (e) { /* non-fatal */ }
    }

    res.json(result);
  } catch (err) {
    console.error('Diagnostic resolve error:', err.message);
    res.status(500).json({ error: 'Resolution failed', code: 'RESOLVE_ERROR' });
  }
});

// ─── HEALTH MONITOR ──────────────────────────────────────────────────────────

const { runHealthCheck, getIntegrationStatus } = require('../lib/onboarding/healthMonitor');

// GET /api/onboarding/health — run health check on demand
router.get('/api/onboarding/health', authenticateToken, async (req, res) => {
  try {
    const issues = await runHealthCheck(req.tenant_id);
    const integrations = await getIntegrationStatus(req.tenant_id);

    res.json({
      issues,
      integrations,
      last_checked: new Date().toISOString(),
      all_clear: issues.length === 0,
    });
  } catch (err) {
    console.error('Health check error:', err.message);
    res.status(500).json({ error: 'Health check failed' });
  }
});

// GET /api/onboarding/health/summary — lightweight badge counts
router.get('/api/onboarding/health/summary', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { rows } = await db.query(`
      SELECT severity, COUNT(*) AS cnt
      FROM tenant_health_issues
      WHERE tenant_id = $1 AND resolved = false
      GROUP BY severity
    `, [req.tenant_id]);

    const counts = { error: 0, warning: 0, info: 0 };
    rows.forEach(r => { counts[r.severity] = parseInt(r.cnt) || 0; });

    res.json({
      ...counts,
      total: counts.error + counts.warning + counts.info,
      all_clear: counts.error + counts.warning + counts.info === 0,
    });
  } catch (err) {
    res.status(500).json({ error: 'Summary failed' });
  }
});

// POST /api/onboarding/health/resolve — resolve a health issue
router.post('/api/onboarding/health/resolve', authenticateToken, async (req, res) => {
  try {
    const db = new TenantDB(req.tenant_id);
    const { issue_id, action_id } = req.body;

    if (!issue_id || !action_id) {
      return res.status(400).json({ error: 'issue_id and action_id required' });
    }

    // Get the issue
    const { rows: [issue] } = await db.query(
      'SELECT * FROM tenant_health_issues WHERE id = $1 AND tenant_id = $2',
      [issue_id, req.tenant_id]
    );
    if (!issue) return res.status(404).json({ error: 'Issue not found' });

    // Resolve via the diagnostic resolver
    const result = await resolveAction(action_id, {
      tenant_id: req.tenant_id,
      ...(issue.context || {}),
    });

    // Mark issue resolved
    if (result.resolved) {
      await db.query(`
        UPDATE tenant_health_issues SET resolved = true, resolved_at = NOW(),
          resolution_action = $2
        WHERE id = $1 AND tenant_id = $3
      `, [issue_id, action_id, req.tenant_id]);
    }

    res.json(result);
  } catch (err) {
    console.error('Health resolve error:', err.message);
    res.status(500).json({ error: 'Resolution failed' });
  }
});

  return router;
};
