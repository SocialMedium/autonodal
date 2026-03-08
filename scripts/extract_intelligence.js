#!/usr/bin/env node
/**
 * MitchelLake Intelligence Extraction System
 * 
 * Extracts rich intelligence from Ezekia:
 * - All candidates with research notes (regardless of stage)
 * - Constraints, motivations, timing
 * - Creates temporal signals for re-engagement
 * - Links to companies properly
 * - Stores as confidential intelligence
 */

require('dotenv').config();
const { Pool } = require('pg');
const ezekia = require('../lib/ezekia');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

const stats = {
  projects: { total: 0, processed: 0 },
  candidates: { total: 0, withNotes: 0, new: 0, updated: 0 },
  notes: { total: 0, parsed: 0 },
  companies: { linked: 0 },
  signals: { created: 0 }
};

// ═══════════════════════════════════════════════════════════════════════════════
// CONSTRAINT & MOTIVATION PATTERNS
// ═══════════════════════════════════════════════════════════════════════════════

const CONSTRAINT_PATTERNS = {
  timing: {
    patterns: [
      /just started/i,
      /recently (joined|transitioned)/i,
      /timing (not|isn't|wasn't) right/i,
      /too soon/i,
      /new role/i,
      /only been.*months/i
    ],
    typical_duration: 12, // months
    signal_type: 'timing_opportunity'
  },
  
  compensation: {
    patterns: [
      /compensation/i,
      /salary.*too low/i,
      /\$[\d,]+/,
      /expecting.*\d+k/i,
      /budget/i
    ],
    signal_type: 'compensation_expectation'
  },
  
  geography: {
    patterns: [
      /relocat/i,
      /not interested in.*location/i,
      /remote/i,
      /wants to stay in/i,
      /prefer.*\b(city|state|country)/i
    ],
    signal_type: 'geographic_preference'
  },
  
  role_type: {
    patterns: [
      /not interested in (sales|engineering|product)/i,
      /prefer.*role/i,
      /step (back|down)/i,
      /seeking.*position/i
    ],
    signal_type: 'role_preference'
  },
  
  company_stage: {
    patterns: [
      /too (early|late) stage/i,
      /prefer (startup|enterprise)/i,
      /company size/i
    ],
    signal_type: 'company_stage_preference'
  },
  
  happiness: {
    patterns: [
      /very happy/i,
      /thriving/i,
      /loves current/i,
      /not looking/i
    ],
    signal_type: 'high_satisfaction',
    follow_up_months: 12
  }
};

const MOTIVATION_PATTERNS = {
  seeking_growth: [
    /seeking.*growth/i,
    /looking for.*opportunity/i,
    /ready for next/i
  ],
  
  seeking_strategy: [
    /strategy/i,
    /strategic role/i,
    /transition.*strategy/i
  ],
  
  seeking_leadership: [
    /leadership/i,
    /management/i,
    /director|vp|c-level/i
  ],
  
  open_to_opportunities: [
    /open to/i,
    /would consider/i,
    /interested in/i
  ]
};

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 1: EXTRACT CANDIDATES WITH INTELLIGENCE
// ═══════════════════════════════════════════════════════════════════════════════

async function extractCandidatesWithIntel() {
  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log('║ PHASE 1: Extracting Candidates with Research Notes           ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');
  
  // Get all projects
  let projectPage = 1;
  const allProjects = [];
  
  console.log('Fetching all projects...');
  
  while (true) {
    try {
      const response = await ezekia.getProjects({
        page: projectPage,
        per_page: 100
      });
      
      const projects = response.data || [];
      
      if (projects.length === 0) break;
      
      allProjects.push(...projects);
      stats.projects.total += projects.length;
      
      console.log(`  Fetched page ${projectPage} (${projects.length} projects)`);
      
      if (!response.meta?.lastPage || projectPage >= response.meta.lastPage) {
        break;
      }
      
      projectPage++;
      
    } catch (error) {
      console.error(`Error fetching projects page ${projectPage}:`, error.message);
      break;
    }
  }
  
  console.log(`\n✅ Found ${stats.projects.total} total projects`);
  console.log(`\nExtracting candidates with research notes...\n`);
  
  // Process each project to get candidates with notes
  for (const project of allProjects) {
    try {
      stats.projects.processed++;
      
      if (stats.projects.processed % 50 === 0) {
        console.log(`Progress: ${stats.projects.processed}/${stats.projects.total} projects processed...`);
      }
      
      // Get candidates with research notes field
      let candidatePage = 1;
      
      while (true) {
        const candidates = await ezekia.getProjectCandidates(project.id, {
          page: candidatePage,
          per_page: 50,
          fields: 'id,firstName,lastName,fullName,emails,meta.candidate,profile.positions,manager.researchNotes'
        });
        
        const candidateList = candidates.data || [];
        
        if (candidateList.length === 0) break;
        
        for (const candidate of candidateList) {
          stats.candidates.total++;
          
          const hasNotes = candidate.manager?.researchNotes?.length > 0;
          
          if (hasNotes) {
            stats.candidates.withNotes++;
            
            // Process this candidate
            await processCandidateIntelligence(candidate, project);
          }
        }
        
        // Check if more pages
        if (!candidates.meta?.lastPage || candidatePage >= candidates.meta.lastPage) {
          break;
        }
        
        candidatePage++;
      }
      
    } catch (error) {
      console.error(`Error processing project ${project.id}:`, error.message);
    }
  }
  
  console.log(`\n✅ Extraction complete!`);
  console.log(`   Total candidates scanned: ${stats.candidates.total}`);
  console.log(`   Candidates with notes: ${stats.candidates.withNotes}`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// PROCESS INDIVIDUAL CANDIDATE
// ═══════════════════════════════════════════════════════════════════════════════

async function processCandidateIntelligence(candidate, project) {
  try {
    // Skip candidates without an ID
    if (!candidate.id) {
      console.log('Skipping candidate without ID:', candidate.fullName || 'Unknown');
      return;
    }

    // 1. Ensure person exists in database
    const personId = await ensurePersonExists(candidate);
    
    // 2. Store research notes
    await storeResearchNotes(personId, candidate.manager.researchNotes, project);
    
    // 3. Parse notes for constraints & motivations
    await parseIntelligence(personId, candidate.manager.researchNotes);
    
    // 4. Create temporal signals
    await createTemporalSignals(personId, candidate.manager.researchNotes);
    
  } catch (error) {
    console.error(`Error processing candidate ${candidate.fullName}:`, error.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ENSURE PERSON EXISTS
// ═══════════════════════════════════════════════════════════════════════════════

async function ensurePersonExists(candidate) {
  // Check if person exists
  const existing = await pool.query(
   'SELECT id FROM people WHERE source = \'ezekia\' AND source_id = $1',
    [String(candidate.id)]
  );
  
  if (existing.rows.length > 0) {
    return existing.rows[0].id;
  }
  
  // Create new person
  const email = candidate.emails?.[0]?.address;
  const phone = candidate.phones?.[0]?.number;
  const linkedinUrl = candidate.links?.find(l => l.type === 'linkedin')?.url;
  
  // Get current position
  const currentPosition = candidate.profile?.positions?.find(p => p.primary && p.tense) ||
                         candidate.profile?.positions?.[0];
  
  const currentCompanyName = currentPosition?.company?.name;
  const currentTitle = currentPosition?.title;
  
  // Look up company ID
  let currentCompanyId = null;
  if (currentCompanyName) {
    const companyResult = await pool.query(
      'SELECT id FROM companies WHERE name = $1',
      [currentCompanyName]
    );
    if (companyResult.rows.length > 0) {
      currentCompanyId = companyResult.rows[0].id;
      stats.companies.linked++;
    }
  }
  
  const result = await pool.query(`
    INSERT INTO people (
      source,
      source_id,
      full_name,
      first_name,
      last_name,
      email,
      phone,
      linkedin_url,
      current_company_name,
      current_company_id,
      current_title,
      location,
      created_at,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
    RETURNING id
  `, [
    'ezekia',
    String(candidate.id),
    candidate.fullName,
    candidate.firstName,
    candidate.lastName,
    email,
    phone,
    linkedinUrl,
    currentCompanyName,
    currentCompanyId,
    currentTitle,
    candidate.addresses?.[0]?.city
  ]);
  
  stats.candidates.new++;
  return result.rows[0].id;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STORE RESEARCH NOTES
// ═══════════════════════════════════════════════════════════════════════════════

async function storeResearchNotes(personId, notes, project) {
  for (const note of notes) {
    try {
      const noteText = note.text || note.textStripped;
      
      if (!noteText || noteText.trim() === '') {
        continue; // Skip empty notes
      }
      
      // Check if this exact content already exists for this person
      // Use MD5 hash to compare content regardless of external_id
      const existing = await pool.query(`
        SELECT id FROM interactions 
        WHERE person_id = $1 
          AND interaction_type = 'research_note'
          AND MD5(summary) = MD5($2)
      `, [personId, noteText]);
      
      if (existing.rows.length > 0) {
        continue; // Already have this exact note for this person
      }
      
      // Store as interaction with project context
      await pool.query(`
  INSERT INTO interactions (
    person_id,
    user_id,
    interaction_type,
    source,
    external_id,
    summary,
    created_at,
    interaction_at
  ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
`, [
  personId,
  '13ab009a-62b1-4023-80e3-6241cbcda25d',
  'research_note',
  'ezekia',
  String(note.id),
  noteText,
  note.date
]);
      
      stats.notes.total++;
      
    } catch (error) {
      console.error(`Error storing note ${note.id}:`, error.message);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PARSE INTELLIGENCE (Constraints & Motivations)
// ═══════════════════════════════════════════════════════════════════════════════

async function parseIntelligence(personId, notes) {
  const allText = notes.map(n => n.text || n.textStripped || '').join(' ');
  const noteDate = notes[0]?.date || new Date().toISOString();
  
  // Parse constraints
  for (const [constraintType, config] of Object.entries(CONSTRAINT_PATTERNS)) {
    for (const pattern of config.patterns) {
      if (pattern.test(allText)) {
        await storePersonSignal(personId, {
          signal_type: config.signal_type,
          signal_category: 'constraint',
          constraint_type: constraintType,
          detected_at: noteDate,
          evidence: allText.match(pattern)?.[0] || '',
          follow_up_months: config.typical_duration || config.follow_up_months
        });
        break;
      }
    }
  }
  
  // Parse motivations
  for (const [motivationType, patterns] of Object.entries(MOTIVATION_PATTERNS)) {
    for (const pattern of patterns) {
      if (pattern.test(allText)) {
        await storePersonSignal(personId, {
          signal_type: 'motivation',
          signal_category: 'motivation',
          motivation_type: motivationType,
          detected_at: noteDate,
          evidence: allText.match(pattern)?.[0] || ''
        });
        break;
      }
    }
  }
  
  stats.notes.parsed++;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STORE PERSON SIGNAL
// ═══════════════════════════════════════════════════════════════════════════════

async function storePersonSignal(personId, signalData) {
  try {
    await pool.query(`
      INSERT INTO person_signals (
        person_id,
        signal_type,
        signal_category,
        signal_value,
        detected_at,
        evidence,
        metadata,
        created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    `, [
      personId,
      signalData.signal_type,
      signalData.signal_category,
      signalData.constraint_type || signalData.motivation_type,
      signalData.detected_at,
      signalData.evidence,
      JSON.stringify({
        follow_up_months: signalData.follow_up_months
      })
    ]);
  } catch (error) {
    // Ignore duplicates
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CREATE TEMPORAL SIGNALS
// ═══════════════════════════════════════════════════════════════════════════════

async function createTemporalSignals(personId, notes) {
  const now = new Date();
  
  for (const note of notes) {
    const noteDate = new Date(note.date);
    const monthsSince = (now - noteDate) / (1000 * 60 * 60 * 24 * 30);
    
    const text = note.text || note.textStripped || '';
    
    if (CONSTRAINT_PATTERNS.timing.patterns.some(p => p.test(text))) {
      if (monthsSince >= 6) {
        try {
          await pool.query(`
            INSERT INTO person_signals (
              person_id,
              signal_type,
              signal_category,
              title,
              description,
              confidence_score,
              signal_date,
              detected_at,
              metadata,
              created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
          `, [
            personId,
            'timing_opportunity',
            'temporal',
            'Timing Window Open',
            text.substring(0, 200),
            Math.min(monthsSince / 12, 1.0),
            noteDate.toISOString(),
            now.toISOString(),
            JSON.stringify({ months_since: monthsSince })
          ]);
          
          stats.signals.created++;
        } catch (error) {
          console.error('Error creating temporal signal:', error.message);
        }
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   MitchelLake Intelligence Extraction System                 ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  
  const startTime = Date.now();
  
  try {
    await extractCandidatesWithIntel();
    
    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║   EXTRACTION COMPLETE!                                        ║');
    console.log('╚════════════════════════════════════════════════════════════════╝');
    console.log(`\n⏱️  Duration: ${duration} minutes`);
    console.log(`\n📊 Intelligence Extracted:`);
    console.log(`   Projects processed: ${stats.projects.processed}`);
    console.log(`   Candidates scanned: ${stats.candidates.total}`);
    console.log(`   Candidates with intel: ${stats.candidates.withNotes}`);
    console.log(`   New people created: ${stats.candidates.new}`);
    console.log(`   Research notes stored: ${stats.notes.total}`);
    console.log(`   Notes parsed for signals: ${stats.notes.parsed}`);
    console.log(`   Temporal signals created: ${stats.signals.created}`);
    console.log(`   Companies linked: ${stats.companies.linked}`);
    
    console.log(`\n🎯 Intelligence Database Ready:`);
    console.log(`   - Constraints & motivations tagged`);
    console.log(`   - Temporal signals for re-engagement`);
    console.log(`   - Confidential intel stored securely`);
    console.log(`   - Ready for signal-based workflows`);
    
  } catch (error) {
    console.error('\n❌ Extraction failed:', error);
  } finally {
    await pool.end();
  }
}

main();