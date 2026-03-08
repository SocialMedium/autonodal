/**
 * MitchelLake Signal Intelligence Platform
 * Search Matching Engine
 * 
 * Matches candidates to active searches using vector similarity + scoring
 */

require('dotenv').config();
const pool = require('../lib/db');
const { embedText, qdrantClient, COLLECTIONS } = require('../lib/qdrant');

// Match score weights
const MATCH_WEIGHTS = {
  vector_similarity: 0.4,
  experience_match: 0.2,
  skills_match: 0.15,
  location_match: 0.1,
  timing_score: 0.15
};

/**
 * Generate embedding for a search brief
 */
async function embedSearchBrief(search) {
  const textParts = [
    search.title,
    search.brief_text,
    search.requirements,
    search.ideal_background,
    search.target_companies
  ].filter(Boolean);

  const text = textParts.join('\n\n');
  return await embedText(text);
}

/**
 * Find similar candidates using vector search
 */
async function findSimilarCandidates(searchEmbedding, limit = 100) {
  try {
    const results = await qdrantClient.search(COLLECTIONS.people, {
      vector: searchEmbedding,
      limit: limit,
      with_payload: true,
      score_threshold: 0.5
    });

    return results.map(r => ({
      personId: r.payload.person_id,
      vectorScore: r.score,
      payload: r.payload
    }));
  } catch (err) {
    console.error('Vector search error:', err.message);
    return [];
  }
}

/**
 * Score experience match based on career history
 */
function scoreExperienceMatch(person, search) {
  let score = 0;
  const factors = [];

  // Check title relevance
  if (person.current_title && search.title) {
    const searchTerms = search.title.toLowerCase().split(/\s+/);
    const titleTerms = person.current_title.toLowerCase().split(/\s+/);
    const overlap = searchTerms.filter(t => titleTerms.some(pt => pt.includes(t) || t.includes(pt)));
    if (overlap.length > 0) {
      score += 0.4;
      factors.push(`Title alignment: ${overlap.join(', ')}`);
    }
  }

  // Check target companies
  if (search.target_companies && person.current_company) {
    const targets = search.target_companies.toLowerCase().split(/[,;]+/).map(t => t.trim());
    if (targets.some(t => person.current_company.toLowerCase().includes(t))) {
      score += 0.3;
      factors.push(`Works at target company: ${person.current_company}`);
    }
  }

  // Check seniority level
  const seniorityKeywords = {
    'c-suite': ['ceo', 'cfo', 'cto', 'coo', 'chief'],
    'vp': ['vp', 'vice president', 'svp', 'evp'],
    'director': ['director', 'head of'],
    'manager': ['manager', 'lead']
  };

  if (person.current_title) {
    const titleLower = person.current_title.toLowerCase();
    for (const [level, keywords] of Object.entries(seniorityKeywords)) {
      if (keywords.some(k => titleLower.includes(k))) {
        score += 0.15;
        factors.push(`Seniority: ${level}`);
        break;
      }
    }
  }

  // Years of experience bonus
  if (person.years_experience) {
    if (person.years_experience >= 10) {
      score += 0.15;
      factors.push(`${person.years_experience}+ years experience`);
    } else if (person.years_experience >= 5) {
      score += 0.1;
    }
  }

  return { score: Math.min(score, 1), factors };
}

/**
 * Score skills/expertise match
 */
function scoreSkillsMatch(person, search) {
  let score = 0;
  const factors = [];

  // Extract keywords from brief
  const briefText = [search.brief_text, search.requirements, search.ideal_background]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  // Check expertise tags
  if (person.expertise_tags && person.expertise_tags.length > 0) {
    const matchingTags = person.expertise_tags.filter(tag => 
      briefText.includes(tag.toLowerCase())
    );
    if (matchingTags.length > 0) {
      score += Math.min(matchingTags.length * 0.2, 0.6);
      factors.push(`Matching skills: ${matchingTags.slice(0, 3).join(', ')}`);
    }
  }

  // Check sector alignment
  if (person.sector && briefText.includes(person.sector.toLowerCase())) {
    score += 0.3;
    factors.push(`Sector match: ${person.sector}`);
  }

  return { score: Math.min(score, 1), factors };
}

/**
 * Score location match
 */
function scoreLocationMatch(person, search) {
  if (!search.location || !person.location) {
    return { score: 0.5, factors: ['Location not specified'] };
  }

  const searchLoc = search.location.toLowerCase();
  const personLoc = person.location.toLowerCase();

  // Exact or partial match
  if (personLoc.includes(searchLoc) || searchLoc.includes(personLoc)) {
    return { score: 1, factors: [`Location match: ${person.location}`] };
  }

  // Same country check (basic)
  const searchCountry = searchLoc.split(',').pop()?.trim();
  const personCountry = personLoc.split(',').pop()?.trim();
  if (searchCountry && personCountry && searchCountry === personCountry) {
    return { score: 0.7, factors: [`Same country: ${personCountry}`] };
  }

  return { score: 0.3, factors: ['Different location'] };
}

/**
 * Compute overall match score for a candidate
 */
async function computeMatchScore(personId, search, vectorScore) {
  // Get person details
  const personResult = await pool.query(`
    SELECT p.*, ps.timing_score
    FROM people p
    LEFT JOIN person_scores ps ON p.id = ps.person_id
    WHERE p.id = $1
  `, [personId]);

  if (personResult.rows.length === 0) {
    return null;
  }

  const person = personResult.rows[0];

  // Compute component scores
  const experience = scoreExperienceMatch(person, search);
  const skills = scoreSkillsMatch(person, search);
  const location = scoreLocationMatch(person, search);
  const timingScore = person.timing_score || 0.5;

  // Weighted combination
  const overallScore = 
    MATCH_WEIGHTS.vector_similarity * vectorScore +
    MATCH_WEIGHTS.experience_match * experience.score +
    MATCH_WEIGHTS.skills_match * skills.score +
    MATCH_WEIGHTS.location_match * location.score +
    MATCH_WEIGHTS.timing_score * timingScore;

  // Collect all match reasons
  const strengths = [
    ...experience.factors,
    ...skills.factors,
    ...(location.score >= 0.7 ? location.factors : [])
  ];

  const gaps = [];
  if (experience.score < 0.4) gaps.push('Limited title/experience alignment');
  if (skills.score < 0.3) gaps.push('Few matching skills identified');
  if (location.score < 0.5) gaps.push(location.factors[0]);
  if (timingScore < 0.4) gaps.push('May not be receptive to move');

  return {
    personId,
    overallScore,
    vectorScore,
    experienceScore: experience.score,
    skillsScore: skills.score,
    locationScore: location.score,
    timingScore,
    strengths,
    gaps,
    person
  };
}

/**
 * Match a single search to candidates
 */
async function matchSearch(searchId) {
  console.log(`Matching search ${searchId}...`);

  // Get search details
  const searchResult = await pool.query(`
    SELECT * FROM searches WHERE id = $1
  `, [searchId]);

  if (searchResult.rows.length === 0) {
    console.log(`Search ${searchId} not found`);
    return [];
  }

  const search = searchResult.rows[0];

  // Generate embedding for the search brief
  let embedding;
  try {
    embedding = await embedSearchBrief(search);
  } catch (err) {
    console.error(`Failed to embed search ${searchId}:`, err.message);
    return [];
  }

  // Store/update search embedding in Qdrant
  try {
    await qdrantClient.upsert(COLLECTIONS.searches, {
      wait: true,
      points: [{
        id: searchId,
        vector: embedding,
        payload: {
          search_id: searchId,
          title: search.title,
          client_name: search.client_name,
          status: search.status
        }
      }]
    });
  } catch (err) {
    console.error(`Failed to store search embedding:`, err.message);
  }

  // Find similar candidates via vector search
  const candidates = await findSimilarCandidates(embedding, 100);
  console.log(`  Found ${candidates.length} vector matches`);

  // Get people already in pipeline (to exclude)
  const pipelineResult = await pool.query(`
    SELECT person_id FROM search_candidates WHERE search_id = $1
  `, [searchId]);
  const existingIds = new Set(pipelineResult.rows.map(r => r.person_id));

  // Score each candidate
  const matches = [];
  for (const candidate of candidates) {
    // Skip if already in pipeline
    if (existingIds.has(candidate.personId)) continue;

    const match = await computeMatchScore(candidate.personId, search, candidate.vectorScore);
    if (match && match.overallScore >= 0.5) {
      matches.push(match);
    }
  }

  // Sort by overall score
  matches.sort((a, b) => b.overallScore - a.overallScore);

  // Store top matches
  const topMatches = matches.slice(0, 50);
  console.log(`  Storing ${topMatches.length} qualified matches`);

  for (const match of topMatches) {
    await pool.query(`
      INSERT INTO search_matches (search_id, person_id, match_score, vector_score,
        experience_score, skills_score, location_score, timing_score,
        match_reasons, strengths, gaps, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
      ON CONFLICT (search_id, person_id) DO UPDATE SET
        match_score = EXCLUDED.match_score,
        vector_score = EXCLUDED.vector_score,
        experience_score = EXCLUDED.experience_score,
        skills_score = EXCLUDED.skills_score,
        location_score = EXCLUDED.location_score,
        timing_score = EXCLUDED.timing_score,
        match_reasons = EXCLUDED.match_reasons,
        strengths = EXCLUDED.strengths,
        gaps = EXCLUDED.gaps,
        updated_at = NOW()
    `, [
      searchId,
      match.personId,
      match.overallScore,
      match.vectorScore,
      match.experienceScore,
      match.skillsScore,
      match.locationScore,
      match.timingScore,
      JSON.stringify([...match.strengths, ...match.gaps]),
      JSON.stringify(match.strengths),
      JSON.stringify(match.gaps)
    ]);
  }

  return topMatches;
}

/**
 * Main function - match all active searches
 */
async function main() {
  console.log('Starting search matching...');

  try {
    // Get all active searches
    const result = await pool.query(`
      SELECT id, title
      FROM searches
      WHERE status IN ('sourcing', 'outreach', 'research', 'interviewing', 'shortlist')
      ORDER BY updated_at DESC
    `);

    console.log(`Found ${result.rows.length} active searches`);

    let totalMatches = 0;

    for (const search of result.rows) {
      try {
        const matches = await matchSearch(search.id);
        totalMatches += matches.length;
        console.log(`  ✓ ${search.title}: ${matches.length} matches`);
      } catch (err) {
        console.error(`  ✗ ${search.title}: ${err.message}`);
      }
    }

    console.log(`\n✅ Search matching complete`);
    console.log(`   Total matches: ${totalMatches}`);

  } catch (err) {
    console.error('Fatal error:', err);
    process.exit(1);
  } finally {
    // pool managed by lib/db
  }
}

// Export for use in other modules
module.exports = { matchSearch, computeMatchScore };

// Run if called directly
if (require.main === module) {
  main();
}
