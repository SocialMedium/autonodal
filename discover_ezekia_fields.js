#!/usr/bin/env node
/**
 * Ezekia Field Discovery — Dump EVERYTHING
 * 
 * Fetches raw data from Ezekia with maximum fields and dumps
 * the complete structure so we can see what's available.
 */

require('dotenv').config();

const BASE_URL = process.env.EZEKIA_API_URL || 'https://app.ezekia.com';
const TOKEN = process.env.EZEKIA_API_TOKEN || process.env.EZEKIA_API_KEY;
const TARGET = process.argv.find(a => a.startsWith('--company='))?.replace('--company=', '') || 'Immutable';

async function ezFetch(endpoint) {
  const url = endpoint.startsWith('http') ? endpoint : `${BASE_URL}${endpoint}`;
  const res = await fetch(url, {
    headers: { 'Authorization': `Bearer ${TOKEN}`, 'Accept': 'application/json' }
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${url}`);
  return res.json();
}

function printKeys(obj, indent = 0, maxDepth = 4) {
  if (!obj || typeof obj !== 'object' || indent > maxDepth) return;
  const pad = '  '.repeat(indent);
  for (const [k, v] of Object.entries(obj)) {
    if (Array.isArray(v)) {
      console.log(`${pad}${k}: Array[${v.length}]`);
      if (v.length > 0 && typeof v[0] === 'object') {
        printKeys(v[0], indent + 1, maxDepth);
      } else if (v.length > 0) {
        console.log(`${pad}  [0]: ${JSON.stringify(v[0]).substring(0, 100)}`);
      }
    } else if (v && typeof v === 'object') {
      console.log(`${pad}${k}: {Object}`);
      printKeys(v, indent + 1, maxDepth);
    } else {
      const val = v === null ? 'null' : String(v).substring(0, 120);
      console.log(`${pad}${k}: ${val}`);
    }
  }
}

async function main() {
  console.log(`\n═══ EZEKIA FIELD DISCOVERY — "${TARGET}" ═══\n`);

  // ── 1. Find an Immutable project ──────────────────────────────────────
  console.log('STEP 1: Finding projects...');
  let targetProject = null;
  let page = 1;

  while (!targetProject) {
    const resp = await ezFetch(`/api/projects?page=${page}&per_page=100`);
    const projects = resp.data || [];
    if (projects.length === 0) break;

    targetProject = projects.find(p =>
      (p.relationships?.company?.name || '').toLowerCase().includes(TARGET.toLowerCase())
    );

    if (!targetProject && page < (resp.meta?.lastPage || 1)) {
      page++;
    } else if (!targetProject) {
      break;
    }
  }

  if (!targetProject) {
    console.log(`❌ No project found for "${TARGET}". Try --company="Other Name"`);
    return;
  }

  console.log(`✅ Found project: "${targetProject.name}" (ID: ${targetProject.id})`);
  const ezCompanyId = targetProject.relationships?.company?.id;
  console.log(`   Client company ID: ${ezCompanyId}\n`);

  // ── 2. Dump project with ALL possible fields ─────────────────────────
  console.log('═══════════════════════════════════════════════════════');
  console.log('STEP 2: PROJECT — All fields');
  console.log('═══════════════════════════════════════════════════════\n');

  // Ask for every relationship/manager field we can think of
  const projectFields = [
    'relationships.company', 'relationships.client', 'relationships.contacts',
    'relationships.people', 'relationships.candidates', 'relationships.stakeholders',
    'relationships.billings', 'relationships.invoices', 'relationships.revenues',
    'manager.researchNotes', 'manager.tasks', 'manager.meetings',
    'manager.contacts', 'manager.notes', 'manager.activities',
    'manager.status', 'manager.feedback', 'manager.comments',
    'contacts', 'people', 'notes', 'activities', 'billing',
  ];
  const fieldsParam = projectFields.map(f => `fields[]=${f}`).join('&');

  try {
    const fullProject = await ezFetch(`/api/projects/${targetProject.id}?${fieldsParam}`);
    const data = fullProject.data || fullProject;
    
    console.log('── COMPLETE KEY STRUCTURE ──\n');
    printKeys(data);
    
    console.log('\n── RAW JSON (truncated) ──\n');
    console.log(JSON.stringify(data, null, 2).substring(0, 5000));
    console.log('\n...(truncated)\n');
  } catch (err) {
    console.log(`❌ Project detail error: ${err.message}`);
    // Fallback: just dump what we already have
    console.log('\n── FALLBACK: Project from list ──\n');
    printKeys(targetProject);
  }

  // ── 3. Dump first candidate with ALL fields ──────────────────────────
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('STEP 3: CANDIDATE — All fields');
  console.log('═══════════════════════════════════════════════════════\n');

  const candidateFields = [
    'meta.candidate', 'meta.contact',
    'profile.positions', 'profile.education', 'profile.skills',
    'manager.researchNotes', 'manager.mostRecentResearcherNote',
    'manager.pipelineTags', 'manager.tasks', 'manager.meetings',
    'manager.activities', 'manager.notes', 'manager.feedback',
    'manager.contacts', 'manager.comments', 'manager.status',
    'relationships.assignments', 'relationships.billings',
    'relationships.candidatesLists', 'relationships.companies',
    'relationships.contacts', 'relationships.opportunities',
    'contacts', 'notes', 'activities',
  ];
  const cFieldsParam = candidateFields.map(f => `fields=${f}`).join('&');

  try {
    const candidates = await ezFetch(
      `/api/projects/${targetProject.id}/candidates?per_page=5&${cFieldsParam}`
    );
    const items = candidates.data || [];

    if (items.length > 0) {
      // Find one with notes if possible
      const withNotes = items.find(c => c.manager?.researchNotes?.length > 0) || items[0];
      
      console.log(`Candidate: ${withNotes.fullName || withNotes.firstName + ' ' + withNotes.lastName}`);
      console.log(`Has notes: ${withNotes.manager?.researchNotes?.length || 0}\n`);
      
      console.log('── COMPLETE KEY STRUCTURE ──\n');
      printKeys(withNotes);
      
      console.log('\n── RAW JSON (truncated) ──\n');
      console.log(JSON.stringify(withNotes, null, 2).substring(0, 5000));
      console.log('\n...(truncated)\n');
    } else {
      console.log('No candidates found on this project');
    }
  } catch (err) {
    console.log(`❌ Candidates error: ${err.message}`);
  }

  // ── 4. Fetch a SINGLE PERSON by ID with ALL fields ────────────────────
  // The project candidates endpoint gives a subset — the individual
  // person endpoint should give LinkedIn, resume, full profile etc.
  console.log('\n═══════════════════════════════════════════════════════');
  console.log('STEP 4: SINGLE PERSON DETAIL — Full record');
  console.log('═══════════════════════════════════════════════════════\n');

  // Get a candidate ID from step 3
  let samplePersonId = null;
  try {
    const candidates = await ezFetch(
      `/api/projects/${targetProject.id}/candidates?per_page=3&fields=meta.candidate`
    );
    samplePersonId = (candidates.data || [])[0]?.id;
  } catch (e) {}

  if (samplePersonId) {
    console.log(`Fetching full person record for ID: ${samplePersonId}\n`);

    const personFields = [
      'profile.positions', 'profile.education', 'profile.skills',
      'profile.languages', 'profile.certifications', 'profile.summary',
      'relationships.assignments', 'relationships.billings',
      'relationships.candidatesLists', 'relationships.companies',
      'relationships.contacts', 'relationships.opportunities',
      'relationships.projects', 'relationships.documents',
      'manager.researchNotes', 'manager.tasks', 'manager.notes',
      'manager.meetings', 'manager.activities', 'manager.feedback',
      'manager.mostRecentResearcherNote', 'manager.pipelineTags',
      'contacts', 'notes', 'activities', 'documents', 'resume',
      'links', 'tags', 'industries', 'sources',
    ];
    const pFieldsParam = personFields.map(f => `fields[]=${f}`).join('&');

    // Try multiple API versions for the person endpoint
    const personUrls = [
      `/api/people/${samplePersonId}?${pFieldsParam}`,
      `/api/v2/people/${samplePersonId}?${pFieldsParam}`,
      `/api/v3/people/${samplePersonId}?${pFieldsParam}`,
    ];

    for (const url of personUrls) {
      try {
        console.log(`Trying: GET ${url.split('?')[0]}...`);
        const person = await ezFetch(url);
        const data = person.data || person;

        console.log(`✅ Got response!\n`);
        console.log('── COMPLETE KEY STRUCTURE ──\n');
        printKeys(data);

        // Specifically look for LinkedIn, resume, documents, links
        console.log('\n── HUNTING FOR LINKEDIN / RESUME / CV ──');
        const allText = JSON.stringify(data);
        if (allText.includes('linkedin')) console.log('  ✅ Contains "linkedin" somewhere!');
        if (allText.includes('resume') || allText.includes('Resume')) console.log('  ✅ Contains "resume"!');
        if (allText.includes('cv') || allText.includes('CV')) console.log('  ✅ Contains "cv/CV"!');
        if (allText.includes('document')) console.log('  ✅ Contains "document"!');
        if (data.links) console.log('  links:', JSON.stringify(data.links));
        if (data.linkedinUrl) console.log('  linkedinUrl:', data.linkedinUrl);
        if (data.linkedin) console.log('  linkedin:', data.linkedin);
        if (data.profile?.links) console.log('  profile.links:', JSON.stringify(data.profile.links));
        if (data.urls) console.log('  urls:', JSON.stringify(data.urls));

        console.log('\n── RAW JSON (truncated to 6000 chars) ──\n');
        console.log(JSON.stringify(data, null, 2).substring(0, 6000));
        console.log('\n...(truncated)\n');
        break; // Got it, no need to try other versions
      } catch (err) {
        console.log(`  ❌ ${err.message}`);
      }
    }

    // ── 4b. Try documents/CV endpoints for this person ─────────────────
    console.log('\n── PERSON DOCUMENTS / CV ──\n');
    const docUrls = [
      `/api/people/${samplePersonId}/documents`,
      `/api/v2/people/${samplePersonId}/documents`,
      `/api/people/${samplePersonId}/resume`,
      `/api/people/${samplePersonId}/files`,
      `/api/people/${samplePersonId}/attachments`,
    ];

    for (const url of docUrls) {
      try {
        console.log(`Trying: GET ${url}`);
        const resp = await ezFetch(url);
        const items = resp.data || resp;
        const count = Array.isArray(items) ? items.length : '?';
        console.log(`  ✅ ${count} items`);
        if (Array.isArray(items) && items.length > 0) {
          console.log('  First item:', JSON.stringify(items[0], null, 2).substring(0, 500));
        } else if (typeof items === 'object') {
          console.log('  Response:', JSON.stringify(items, null, 2).substring(0, 500));
        }
      } catch (err) {
        console.log(`  ❌ ${err.message}`);
      }
    }

    // ── 4c. Try notes/interactions for this person ─────────────────────
    console.log('\n── PERSON NOTES / INTERACTIONS ──\n');
    const noteUrls = [
      `/api/people/${samplePersonId}/notes`,
      `/api/people/${samplePersonId}/activities`,
      `/api/people/${samplePersonId}/interactions`,
      `/api/people/${samplePersonId}/history`,
      `/api/people/${samplePersonId}/comments`,
    ];

    for (const url of noteUrls) {
      try {
        console.log(`Trying: GET ${url}`);
        const resp = await ezFetch(url);
        const items = resp.data || resp;
        const count = Array.isArray(items) ? items.length : '?';
        console.log(`  ✅ ${count} items`);
        if (Array.isArray(items) && items.length > 0) {
          console.log('  First item:', JSON.stringify(items[0], null, 2).substring(0, 500));
        }
      } catch (err) {
        console.log(`  ❌ ${err.message}`);
      }
    }
  }

  // ── 5. Full COMPANY detail for target ────────────────────────────────
  if (ezCompanyId) {
    console.log('\n═══════════════════════════════════════════════════════');
    console.log(`STEP 5: COMPANY FULL DETAIL — ${TARGET} (ID: ${ezCompanyId})`);
    console.log('═══════════════════════════════════════════════════════\n');

    const companyFields = [
      'relationships.contacts', 'relationships.people', 'relationships.projects',
      'relationships.assignments', 'relationships.candidates', 'relationships.invoices',
      'relationships.opportunities', 'relationships.notes', 'relationships.documents',
      'contacts', 'people', 'notes', 'activities', 'documents',
      'links', 'tags', 'industries', 'addresses',
    ];
    const coFieldsParam = companyFields.map(f => `fields[]=${f}`).join('&');

    // Try multiple API versions
    const companyUrls = [
      `/api/companies/${ezCompanyId}?${coFieldsParam}`,
      `/api/v2/companies/${ezCompanyId}?${coFieldsParam}`,
      `/api/v3/companies/${ezCompanyId}?${coFieldsParam}`,
    ];

    for (const url of companyUrls) {
      try {
        console.log(`Trying: GET ${url.split('?')[0]}...`);
        const company = await ezFetch(url);
        const data = company.data || company;

        console.log(`✅ Got response!\n`);
        console.log('── COMPLETE KEY STRUCTURE ──\n');
        printKeys(data);

        // Hunt for contacts
        const allText = JSON.stringify(data);
        if (allText.includes('contact')) console.log('\n  ✅ Contains "contact" somewhere!');
        
        console.log('\n── RAW JSON (truncated to 6000 chars) ──\n');
        console.log(JSON.stringify(data, null, 2).substring(0, 6000));
        console.log('\n...(truncated)\n');
        break;
      } catch (err) {
        console.log(`  ❌ ${err.message}`);
      }
    }

    // Try every sub-endpoint on the company
    console.log('\n── COMPANY SUB-ENDPOINTS ──\n');
    for (const sub of ['contacts', 'people', 'projects', 'notes', 'documents', 
                        'activities', 'assignments', 'candidates', 'opportunities']) {
      try {
        console.log(`GET /api/companies/${ezCompanyId}/${sub}`);
        const resp = await ezFetch(`/api/companies/${ezCompanyId}/${sub}?per_page=10`);
        const items = resp.data || resp;
        const count = Array.isArray(items) ? items.length : 
                      (items.meta?.total || Object.keys(items).length);
        console.log(`  ✅ ${count} items`);
        if (Array.isArray(items) && items.length > 0) {
          console.log('  Keys:', Object.keys(items[0]).join(', '));
          console.log('  Sample:', JSON.stringify(items[0], null, 2).substring(0, 600));
        }
      } catch (err) {
        console.log(`  ❌ ${err.message}`);
      }
    }
  }

  // ── 6. People search — find contacts at target company ───────────────
  console.log('\n═══════════════════════════════════════════════════════');
  console.log(`STEP 6: PEOPLE SEARCH — Who works at "${TARGET}"?`);
  console.log('═══════════════════════════════════════════════════════\n');

  // Try search/filter endpoints
  const searchUrls = [
    `/api/people?per_page=20&search=${encodeURIComponent(TARGET)}&fields[]=profile.positions`,
    `/api/people?per_page=20&company=${encodeURIComponent(TARGET)}&fields[]=profile.positions`,
    `/api/people?per_page=20&q=${encodeURIComponent(TARGET)}&fields[]=profile.positions`,
    `/api/people?per_page=20&filter[company]=${encodeURIComponent(TARGET)}&fields[]=profile.positions`,
  ];

  for (const url of searchUrls) {
    try {
      const paramName = url.split('?')[1].split('&')[1].split('=')[0];
      console.log(`Trying: ${paramName}=${TARGET}`);
      const resp = await ezFetch(url);
      const people = resp.data || [];
      console.log(`  Got ${people.length} people (total: ${resp.meta?.total || '?'})`);
      
      if (people.length > 0) {
        // Show who we found
        for (const p of people.slice(0, 10)) {
          const pos = (p.profile?.positions || []).find(pos => pos.primary) || 
                      (p.profile?.positions || [])[0];
          console.log(`  👤 ${p.fullName} — ${pos?.title || '?'} @ ${pos?.company?.name || '?'}`);
        }
        
        // Check if these are actually at the target company
        const atTarget = people.filter(p =>
          (p.profile?.positions || []).some(pos =>
            (pos.company?.name || '').toLowerCase().includes(TARGET.toLowerCase())
          )
        );
        console.log(`  → ${atTarget.length} actually at "${TARGET}"`);
        break; // Found a working search method
      }
    } catch (err) {
      console.log(`  ❌ ${err.message}`);
    }
  }

  console.log('\n═══════════════════════════════════════════════════════');
  console.log('DONE — Send me this full output for field mapping!');
  console.log('═══════════════════════════════════════════════════════\n');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
