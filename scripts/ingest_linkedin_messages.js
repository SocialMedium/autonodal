#!/usr/bin/env node
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});
const TENANT_ID = process.env.ML_TENANT_ID || '00000000-0000-0000-0000-000000000001';

function parseCSV(line) {
  const r = []; let c = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') q = !q;
    else if (ch === ',' && !q) { r.push(c.trim()); c = ''; }
    else c += ch;
  }
  r.push(c.trim()); return r;
}

async function main() {
  const csvPath = process.argv[2] || path.join(__dirname, '..', 'data', 'sophie_linkedin_messages.csv');
  const userEmail = process.argv[3] || 'sophiec@mitchellake.com';

  console.log('LinkedIn Messages Import');
  console.log('File:', csvPath, '| User:', userEmail);

  const { rows: [user] } = await pool.query('SELECT id, name FROM users WHERE email = $1', [userEmail]);
  if (!user) { console.error('User not found:', userEmail); process.exit(1); }
  const userId = user.id;
  const userName = (user.name || '').toLowerCase();

  const raw = fs.readFileSync(csvPath, 'utf8').replace(/^\uFEFF/, '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const allLines = raw.split('\n');
  let headerIdx = 0;
  for (let i = 0; i < Math.min(allLines.length, 10); i++) {
    if (allLines[i].toLowerCase().includes('conversation id')) { headerIdx = i; break; }
  }
  const lines = allLines.slice(headerIdx).filter(l => l.trim());
  const headers = parseCSV(lines[0]).map(h => h.replace(/[^\x20-\x7E]/g, '').trim());
  console.log('Headers:', headers.join(', '));

  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = parseCSV(lines[i]);
    if (vals.length < 5) continue;
    const row = {}; headers.forEach((h, idx) => { row[h] = vals[idx] || ''; }); rows.push(row);
  }
  console.log('Messages:', rows.length);

  // Load people
  const { rows: dbPeople } = await pool.query('SELECT id, full_name, linkedin_url FROM people WHERE tenant_id = $1', [TENANT_ID]);
  const linkedinIdx = new Map(), nameIdx = new Map();
  for (const p of dbPeople) {
    if (p.linkedin_url) { const s = (p.linkedin_url.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1]; if (s) linkedinIdx.set(s, p.id); }
    const n = (p.full_name || '').toLowerCase().trim();
    if (n) { if (!nameIdx.has(n)) nameIdx.set(n, []); nameIdx.get(n).push(p.id); }
  }
  console.log('People loaded:', dbPeople.length, '| LinkedIn URLs:', linkedinIdx.size);

  // Group by conversation
  const convos = new Map();
  for (const row of rows) {
    const cid = row['CONVERSATION ID'] || ''; if (!cid) continue;
    if (!convos.has(cid)) convos.set(cid, []);
    convos.get(cid).push(row);
  }
  console.log('Conversations:', convos.size, '\n');

  const stats = { convos: 0, interactions: 0, matched: 0, unmatched: 0, skipped: 0, proximity: 0 };

  for (const [convId, msgs] of convos) {
    stats.convos++;
    let otherName = null, otherUrl = null;
    for (const m of msgs) {
      const fn = (m['FROM'] || '').trim(), fu = (m['SENDER PROFILE URL'] || '').trim();
      const tn = (m['TO'] || '').trim(), tu = (m['RECIPIENT PROFILE URLS'] || '').trim();
      if (fn && fn.toLowerCase() !== userName && fn.toLowerCase() !== 'sophie cohen') { otherName = fn; otherUrl = fu; break; }
      if (tn && tn.toLowerCase() !== userName && tn.toLowerCase() !== 'sophie cohen') { otherName = tn; otherUrl = tu; }
    }
    if (!otherName) { stats.skipped++; continue; }

    let personId = null;
    if (otherUrl) { const s = (otherUrl.toLowerCase().match(/linkedin\.com\/in\/([^\/]+)/) || [])[1]; if (s && linkedinIdx.has(s)) personId = linkedinIdx.get(s); }
    if (!personId) { const c = nameIdx.get(otherName.toLowerCase().trim()) || []; if (c.length === 1) personId = c[0]; }
    if (!personId) { stats.unmatched++; continue; }
    stats.matched++;

    const sorted = msgs.sort((a, b) => new Date(a['DATE'] || 0) - new Date(b['DATE'] || 0));
    const lastDate = sorted[sorted.length - 1]?.['DATE'];
    const summary = sorted.slice(-10).map(m => '[' + (m['DATE'] || '').slice(0, 10) + '] ' + (m['FROM'] || '').split(' ')[0] + ': ' + (m['CONTENT'] || '').slice(0, 200)).join('\n');
    const hasIn = msgs.some(m => { const f = (m['FROM'] || '').toLowerCase(); return f !== userName && f !== 'sophie cohen'; });
    const hasOut = msgs.some(m => { const f = (m['FROM'] || '').toLowerCase(); return f === userName || f === 'sophie cohen'; });

    try {
      await pool.query(
        `INSERT INTO interactions (person_id, user_id, interaction_type, direction, subject, summary, channel, source, external_id, interaction_at, requires_response, response_received, metadata, tenant_id)
         VALUES ($1,$2,'linkedin_message',$3,$4,$5,'linkedin','linkedin_import',$6,$7,$8,$9,$10,$11)
         ON CONFLICT (external_id) WHERE external_id IS NOT NULL DO NOTHING`,
        [personId, userId, hasIn && hasOut ? 'both' : hasOut ? 'outbound' : 'inbound',
         'LinkedIn: ' + otherName + ' (' + msgs.length + ' msgs)', summary.slice(0, 5000),
         'linkedin_msg_' + convId, lastDate ? new Date(lastDate).toISOString() : new Date().toISOString(),
         hasOut && !hasIn, hasIn, JSON.stringify({ message_count: msgs.length, other_name: otherName }), TENANT_ID]);
      stats.interactions++;
    } catch (e) { if (!e.message.includes('duplicate')) stats.skipped++; }

    try {
      await pool.query(
        `INSERT INTO team_proximity (person_id, team_member_id, relationship_type, relationship_strength, source, tenant_id)
         VALUES ($1,$2,'linkedin_message',$3,'linkedin_import',$4)
         ON CONFLICT (person_id, team_member_id) DO UPDATE SET relationship_strength = GREATEST(team_proximity.relationship_strength, EXCLUDED.relationship_strength)`,
        [personId, userId, Math.min(0.3 + msgs.length * 0.05, 0.95), TENANT_ID]);
      stats.proximity++;
    } catch (e) {}

    if (stats.convos % 1000 === 0) console.log('  Progress:', stats.convos + '/' + convos.size, '—', stats.interactions, 'interactions,', stats.matched, 'matched');
  }

  console.log('\nConversations:', stats.convos);
  console.log('Interactions:', stats.interactions);
  console.log('Matched:', stats.matched, '| Unmatched:', stats.unmatched, '| Skipped:', stats.skipped);
  console.log('Proximity links:', stats.proximity);
  await pool.end();
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
