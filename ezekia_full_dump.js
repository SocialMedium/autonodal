require('dotenv').config();
const ezekia = require('./lib/ezekia');

async function dump() {
  // 1. Full candidate from a project (no field filtering)
  console.log('=== PROJECT CANDIDATE (no fields filter) ===');
  const proj = await ezekia.getProjectCandidates(131791, { per_page: 1 });
  const c = (proj.data || [])[0];
  if (c) {
    console.log('\nTOP-LEVEL KEYS:', Object.keys(c));
    for (const [key, val] of Object.entries(c)) {
      const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
      const preview = JSON.stringify(val)?.substring(0, 200);
      console.log(`  ${key} (${type}): ${preview}`);
    }
    
    // Drill into nested objects
    if (c.profile) {
      console.log('\nPROFILE KEYS:', Object.keys(c.profile));
      for (const [key, val] of Object.entries(c.profile)) {
        const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
        const preview = JSON.stringify(val)?.substring(0, 200);
        console.log(`  profile.${key} (${type}): ${preview}`);
      }
    }
    
    if (c.meta) {
      console.log('\nMETA KEYS:', Object.keys(c.meta));
      for (const [key, val] of Object.entries(c.meta)) {
        const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
        const preview = JSON.stringify(val)?.substring(0, 200);
        console.log(`  meta.${key} (${type}): ${preview}`);
      }
    }
  }

  // 2. Full candidate from /api/people (different endpoint)
  console.log('\n\n=== PEOPLE ENDPOINT (direct) ===');
  const people = await ezekia.getPeople({ per_page: 1 });
  const p = (people.data || [])[0];
  if (p) {
    console.log('\nTOP-LEVEL KEYS:', Object.keys(p));
    for (const [key, val] of Object.entries(p)) {
      const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
      const preview = JSON.stringify(val)?.substring(0, 200);
      console.log(`  ${key} (${type}): ${preview}`);
    }
    
    if (p.profile) {
      console.log('\nPROFILE KEYS:', Object.keys(p.profile));
      for (const [key, val] of Object.entries(p.profile)) {
        const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
        const preview = JSON.stringify(val)?.substring(0, 200);
        console.log(`  profile.${key} (${type}): ${preview}`);
      }
    }
  }

  // 3. Try fetching a single person with ALL fields
  console.log('\n\n=== SINGLE PERSON (full detail) ===');
  const personId = c?.id || p?.id;
  if (personId) {
    const full = await ezekia.getPerson(personId);
    console.log('\nTOP-LEVEL KEYS:', Object.keys(full));
    for (const [key, val] of Object.entries(full)) {
      const type = Array.isArray(val) ? `array[${val.length}]` : typeof val;
      const preview = JSON.stringify(val)?.substring(0, 300);
      console.log(`  ${key} (${type}): ${preview}`);
    }
  }

  // 4. Check what fields are requestable
  console.log('\n\n=== TEST FIELD REQUESTS ===');
  const testFields = [
    'manager', 'manager.researchNotes', 'manager.notes',
    'notes', 'researchNotes', 'research_notes',
    'comments', 'activities', 'history'
  ];
  for (const field of testFields) {
    try {
      const r = await ezekia.getProjectCandidates(131791, { per_page: 1, fields: `id,${field}` });
      const item = (r.data || [])[0];
      const hasField = item ? Object.keys(item).filter(k => k !== 'id') : [];
      console.log(`  fields="${field}" => got keys: [${hasField}]`);
      if (hasField.length > 0) {
        const val = item[hasField[0]];
        console.log(`    value: ${JSON.stringify(val)?.substring(0, 200)}`);
      }
    } catch (e) {
      console.log(`  fields="${field}" => ERROR: ${e.message.substring(0, 80)}`);
    }
  }
}

dump().catch(console.error);
