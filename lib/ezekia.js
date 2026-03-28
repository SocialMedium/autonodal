// Ezekia CRM API Client - Fixed pagination
// Docs: https://ezekia.com/api/documentation

const EZEKIA_API_URL = process.env.EZEKIA_API_URL || 'https://ezekia.com';
const EZEKIA_API_TOKEN = process.env.EZEKIA_API_TOKEN;

/**
 * Make authenticated request to Ezekia API
 */
async function ezekiaFetch(endpoint, options = {}) {
  if (!EZEKIA_API_TOKEN) {
    throw new Error('EZEKIA_API_TOKEN not configured');
  }

  const url = `${EZEKIA_API_URL}${endpoint}`;
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${EZEKIA_API_TOKEN}`,
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      ...options.headers
    }
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Ezekia API error ${response.status}: ${error}`);
  }

  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

/**
 * Get paginated list of all people (candidates)
 */
async function getPeople(options = {}) {
  const params = new URLSearchParams({
    page: options.page || 1,
    per_page: options.per_page || 100,
    ...(options.updated_since && { updated_since: options.updated_since }),
    ...(options.fields && { fields: options.fields }),
    isCandidate: true
  });
  
  const fields = [
    'profile.positions',
    'profile.education',
    'profile.headline',
    'relationships.billings',
    'relationships.assignments',
    'relationships.candidatesLists'
  ];
  
  fields.forEach(field => params.append('fields[]', field));
  
  return ezekiaFetch(`/api/people?${params}`);
}

/**
 * Get single person by ID
 */
async function getPerson(personId, fields = null) {
  const params = fields ? `?fields=${fields}` : '';
  return ezekiaFetch(`/api/people/${personId}${params}`);
}

/**
 * Search people by criteria
 */
async function searchPeople(criteria = {}) {
  const params = new URLSearchParams();
  
  if (criteria.name) params.append('name', criteria.name);
  if (criteria.email) params.append('email', criteria.email);
  if (criteria.company) params.append('company', criteria.company);
  if (criteria.title) params.append('title', criteria.title);
  if (criteria.location) params.append('location', criteria.location);
  if (criteria.tags) params.append('tags', criteria.tags);
  if (criteria.page) params.append('page', criteria.page);
  if (criteria.per_page) params.append('per_page', criteria.per_page || 100);

  return ezekiaFetch(`/api/people?${params}`);
}

/**
 * Get person's documents
 */
async function getPersonDocuments(personId) {
  return ezekiaFetch(`/api/people/${personId}/documents`);
}

/**
 * Download a specific document
 */
async function downloadDocument(personId, documentId) {
  const url = `${EZEKIA_API_URL}/api/v2/people/${personId}/documents/${documentId}`;
  
  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${EZEKIA_API_TOKEN}`
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to download document: ${response.status}`);
  }

  return {
    buffer: await response.arrayBuffer(),
    contentType: response.headers.get('content-type'),
    filename: response.headers.get('content-disposition')?.match(/filename="(.+)"/)?.[1]
  };
}

/**
 * Get all projects
 */
async function getProjects(options = {}) {
  const params = new URLSearchParams({
    page: options.page || 1,
    per_page: options.per_page || 100
  });

  return ezekiaFetch(`/api/projects?${params}`);
}

/**
 * Get candidates for a specific project
 */
async function getProjectCandidates(projectId, options = {}) {
  const params = new URLSearchParams({
    fields: options.fields || 'meta.candidate',
    ...(options.page && { page: options.page }),
    ...(options.per_page && { per_page: options.per_page })
  });

  return ezekiaFetch(`/api/projects/${projectId}/candidates?${params}`);
}

/**
 * Get all companies
 */
async function getCompanies(options = {}) {
  const params = new URLSearchParams({
    page: options.page || 1,
    per_page: options.per_page || 100
  });

  return ezekiaFetch(`/api/companies?${params}`);
}

/**
 * Transform Ezekia person to MitchelLake schema
 * NOTE: Ezekia uses camelCase and nests positions under profile
 */
async function transformPerson(ezekiaPerson, db) {
  // Positions — sort by startDate DESC, pick most recent active role
  // Don't trust primary/tense flags — Ezekia data often has stale flags on old positions
  const positions = (ezekiaPerson.profile?.positions || ezekiaPerson.positions || [])
    .sort((a, b) => (b.startDate || '0000').localeCompare(a.startDate || '0000'));
  const currentPosition = positions.find(p => p.endDate === '9999-12-31' || !p.endDate || p.endDate > new Date().toISOString().slice(0, 10))
    || positions[0];
  // Look up company ID from companies table
  let currentCompanyId = null;
  if (currentPosition?.company?.name && db) {
    try {
      const companyResult = await db.queryOne(
        'SELECT id FROM companies WHERE name = $1',
        [currentPosition.company.name]
      );
      if (companyResult) {
        currentCompanyId = companyResult.id;
      }
    } catch (err) {
      // Company lookup failed, will just use name
    }
  }

  const education = ezekiaPerson.education || [];
  
  const careerHistory = positions.map(pos => ({
    company: pos.company?.name,
    
    title: pos.title,
    location: pos.location,
    start_date: pos.startDate,
    end_date: pos.endDate === '9999-12-31' ? null : pos.endDate,
    current: pos.primary || pos.tense || false,
    description: pos.summary
  }));

  const educationHistory = education.map(edu => ({
    school: edu.school_name || edu.school,
    degree: edu.degree,
    field_of_study: edu.field_of_study,
    start_date: edu.start_date,
    end_date: edu.end_date,
    description: edu.description
  }));

  // Get location from addresses array
  const primaryAddress = ezekiaPerson.addresses?.find(a => a.isDefault) || ezekiaPerson.addresses?.[0];
  const location = primaryAddress ? [
    primaryAddress.city,
    primaryAddress.state,
    primaryAddress.country
  ].filter(Boolean).join(', ') : null;

  // Get email and phone from arrays
  const primaryEmail = ezekiaPerson.emails?.find(e => e.isDefault)?.email || ezekiaPerson.emails?.[0]?.email;
  const primaryPhone = ezekiaPerson.phones?.find(p => p.isDefault)?.number || ezekiaPerson.phones?.[0]?.number;
  
  // Get LinkedIn from links array
  const linkedInLink = ezekiaPerson.links?.find(l => l.label?.toLowerCase() === 'linkedin' || l.url?.includes('linkedin'));
  const twitterLink = ezekiaPerson.links?.find(l => l.label?.toLowerCase() === 'twitter' || l.url?.includes('twitter'));

  return {
    source: 'ezekia',
    source_id: String(ezekiaPerson.id),
    full_name: ezekiaPerson.fullName || `${ezekiaPerson.firstName || ''} ${ezekiaPerson.lastName || ''}`.trim(),
    first_name: ezekiaPerson.firstName,
    last_name: ezekiaPerson.lastName,
    headline: currentPosition ? `${currentPosition.title}${currentPosition.company?.name ? ' at ' + currentPosition.company.name : ''}` : null,
    bio: ezekiaPerson.summary,
    current_title: currentPosition?.title,
    current_company_name: currentPosition?.company?.name,
    current_company_id: currentCompanyId,
    
    email: primaryEmail,
    phone: primaryPhone,
    location: location,
    city: primaryAddress?.city,
    country: primaryAddress?.country,
    linkedin_url: linkedInLink?.url,
    twitter_url: twitterLink?.url,
    career_history: careerHistory.length ? JSON.stringify(careerHistory) : null,
    education: educationHistory.length ? JSON.stringify(educationHistory) : null,
    expertise_tags: ezekiaPerson.tags?.map(t => t.name || t) || [],
    ezekia_data: {
      id: ezekiaPerson.id,
      created_at: ezekiaPerson.createdAt,
      updated_at: ezekiaPerson.updatedAt,
      owner_id: ezekiaPerson.owner?.id,
      owner_name: ezekiaPerson.owner?.fullName,
      profile_picture: ezekiaPerson.profilePicture
    },
    synced_at: new Date().toISOString()
  };
}

/**
 * Sync all people from Ezekia to local database
 * FIXED: Uses meta.lastPage for pagination (Ezekia returns 15 per page)
 */
async function syncAllPeople(db, options = {}) {
  const stats = {
    total: 0,
    created: 0,
    updated: 0,
    errors: 0,
    startedAt: new Date()
  };

  let page = options.startPage || 1;
  let lastPage = null; // Will be set from first API response
  const updatedSince = options.updatedSince || null;

  console.log(`Starting Ezekia sync${updatedSince ? ` (updated since ${updatedSince})` : ' (full sync)'}...`);

  while (true) {
    try {
      const pageInfo = lastPage ? ` of ${lastPage}` : '';
      console.log(`Fetching page ${page}${pageInfo}...`);
      
      const response = await getPeople({
        page,
        per_page: 100,
        updated_since: updatedSince
      });

      // Get pagination info from meta (Ezekia uses camelCase)
      const meta = response.meta || {};
      if (meta.lastPage) {
        lastPage = meta.lastPage;
      }

      const people = response.data || [];
      
      if (people.length === 0) {
        console.log('  No more records');
        break;
      }

      // Process batch
      for (const ezekiaPerson of people) {
        try {
          stats.total++;
          const transformed = await transformPerson(ezekiaPerson, db);
          
          const existing = await db.queryOne(
            'SELECT id FROM people WHERE source = $1 AND source_id = $2',
            ['ezekia', transformed.source_id]
          );

          if (existing) {
            await db.update('people', existing.id, transformed);
            stats.updated++;
          } else {
            await db.insert('people', transformed);
            stats.created++;
          }
        } catch (err) {
          console.error(`  Error processing person ${ezekiaPerson.id}:`, err.message);
          stats.errors++;
        }
      }

      console.log(`  ✓ ${people.length} people (total: ${stats.total.toLocaleString()}, page ${page}/${lastPage || '?'})`);
      
      // Check if we've reached the last page
      if (lastPage && page >= lastPage) {
        console.log('  Reached last page');
        break;
      }

      page++;

      // Rate limiting - be nice to the API
      await new Promise(r => setTimeout(r, 150));
      
    } catch (err) {
      console.error(`Error on page ${page}:`, err.message);
      stats.errors++;
      
      if (stats.errors > 10) {
        console.error('Too many consecutive errors, stopping');
        break;
      }
      
      // Wait and retry
      console.log('  Retrying in 3 seconds...');
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  stats.completedAt = new Date();
  stats.duration = (stats.completedAt - stats.startedAt) / 1000;
  
  console.log(`\nSync completed in ${(stats.duration / 60).toFixed(1)} minutes`);
  console.log(`  Total processed: ${stats.total.toLocaleString()}`);
  console.log(`  Created: ${stats.created.toLocaleString()}`);
  console.log(`  Updated: ${stats.updated.toLocaleString()}`);
  console.log(`  Errors: ${stats.errors}`);

  return stats;
}

/**
 * Get last sync timestamp
 */
async function getLastSyncTime(db) {
  const result = await db.queryOne(`
    SELECT MAX(synced_at) as last_sync
    FROM people
    WHERE source = 'ezekia'
  `);
  return result?.last_sync;
}

/**
 * Get person's notes (research notes, system notes, GDPR notes)
 */
async function getPersonNotes(personId) {
  return ezekiaFetch(`/api/people/${personId}/notes`);
}

/**
 * Get person with full relationship data (assignments, companies)
 */
async function getPersonFull(personId) {
  // Two calls: base (emails, phones, links, addresses, tags) + fields (positions, relationships)
  // The fields[] param strips base-level data, so we need both
  const params = new URLSearchParams();
  [
    'profile.positions', 'profile.education', 'profile.headline',
    'relationships.assignments', 'relationships.companies',
    'relationships.billings', 'relationships.candidatesLists'
  ].forEach(f => params.append('fields[]', f));

  const [base, extended] = await Promise.all([
    ezekiaFetch(`/api/people/${personId}`),
    ezekiaFetch(`/api/people/${personId}?${params}`)
  ]);

  // Merge: base has emails/phones/links/addresses, extended has profile/relationships
  if (base?.data && extended?.data) {
    return { data: { ...base.data, ...extended.data, profile: extended.data.profile || base.data.profile } };
  }
  return extended || base;
}

/**
 * Get person's aspirations (target roles, preferred companies, comp)
 */
async function getPersonAspirations(personId) {
  return ezekiaFetch(`/api/people/${personId}/aspirations`);
}

/**
 * Get person's confidential details
 */
async function getPersonConfidential(personId) {
  return ezekiaFetch(`/api/people/${personId}/confidential`);
}

/**
 * Get person's current status (availability, looking for role)
 */
async function getPersonStatus(personId) {
  return ezekiaFetch(`/api/people/${personId}/current-status`);
}

/**
 * Write a note to a person in Ezekia
 * @param {number} personId - Ezekia person ID
 * @param {string} content - Note content (HTML or plain text)
 * @param {object} options - { subject, category, isConfidential }
 */
async function addPersonNote(personId, content, options = {}) {
  return ezekiaFetch('/api/people/notes', {
    method: 'POST',
    body: JSON.stringify({
      people: [personId],
      body: content,
      subject: options.subject || 'Signal Intelligence Note',
      ...(options.category && { category: options.category }),
      ...(options.isConfidential && { confidential: true })
    })
  });
}

/**
 * Update a company in Ezekia
 * @param {number} companyId - Ezekia company ID
 * @param {object} data - Fields to update
 */
async function updateCompany(companyId, data) {
  return ezekiaFetch(`/api/companies/${companyId}`, {
    method: 'PUT',
    body: JSON.stringify(data)
  });
}

/**
 * Update a person's current status in Ezekia
 * @param {number} personId - Ezekia person ID
 * @param {object} status - { availableFrom, lookingForNewRole, etc. }
 */
async function updatePersonStatus(personId, status) {
  return ezekiaFetch(`/api/people/${personId}/current-status`, {
    method: 'POST',
    body: JSON.stringify(status)
  });
}

/**
 * Get a company by ID from Ezekia
 */
async function getCompany(companyId) {
  return ezekiaFetch(`/api/companies/${companyId}`);
}

/**
 * Get clients from Ezekia
 */
async function getClients(options = {}) {
  const params = new URLSearchParams({
    page: options.page || 1,
    per_page: options.per_page || 100
  });
  return ezekiaFetch(`/api/clients?${params}`);
}

/**
 * Get meetings from Ezekia
 */
async function getMeetings(options = {}) {
  const params = new URLSearchParams({
    page: options.page || 1,
    per_page: options.per_page || 100,
    ...(options.past !== undefined && { past: options.past }),
  });
  return ezekiaFetch(`/api/meetings?${params}`);
}

module.exports = {
  getPeople,
  getPerson,
  getPersonFull,
  getPersonNotes,
  searchPeople,
  getPersonDocuments,
  downloadDocument,
  getProjects,
  getProjectCandidates,
  getCompanies,
  getCompany,
  getClients,
  getMeetings,
  transformPerson,
  syncAllPeople,
  getLastSyncTime,
  ezekiaFetch,
  // Read - extended
  getPersonAspirations,
  getPersonConfidential,
  getPersonStatus,
  // Write operations
  addPersonNote,
  updateCompany,
  updatePersonStatus
};
