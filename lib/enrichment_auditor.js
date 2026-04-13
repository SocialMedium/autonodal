// ═══════════════════════════════════════════════════════════════════════════════
// lib/enrichment_auditor.js — Column audit for enrichment documents
// Maps source columns to platform fields, detects scoring criteria
// ═══════════════════════════════════════════════════════════════════════════════

const KNOWN_MAPPINGS = {
  'full name':              { field: 'name',           type: 'string',  required: true },
  'name':                   { field: 'name',           type: 'string',  required: true },
  'linkedin':               { field: 'linkedin_url',   type: 'url',     required: false },
  'linkedin url':           { field: 'linkedin_url',   type: 'url',     required: false },
  'linkedin_url':           { field: 'linkedin_url',   type: 'url',     required: false },
  'email':                  { field: 'email',          type: 'email',   required: false },
  'user2_last_job_title':   { field: 'current_title',  type: 'string',  required: false },
  'job title':              { field: 'current_title',  type: 'string',  required: false },
  'title':                  { field: 'current_title',  type: 'string',  required: false },
  'user2_last_company':     { field: 'current_company_name', type: 'string', required: false },
  'company':                { field: 'current_company_name', type: 'string', required: false },
  'user2_city':             { field: 'city',           type: 'string',  required: false },
  'city':                   { field: 'city',           type: 'string',  required: false },
  'user2_country':          { field: 'country',        type: 'string',  required: false },
  'country':                { field: 'country',        type: 'string',  required: false },
  'user2_last_institution': { field: 'education_inst', type: 'string',  required: false },
  'user2_last_field_of_study': { field: 'education_field', type: 'string', required: false },
  'overall_score':          { field: 'investor_fit_score',     type: 'float',  required: false },
  'overall_ai_score':       { field: 'investor_fit_score',     type: 'float',  required: false },
  'overall_rationale':      { field: 'investor_fit_rationale', type: 'text',   required: false },
  'jt note':                { field: 'enrichment_classification', type: 'string', required: false },
  'network':                { field: 'enrichment_network_source', type: 'string', required: false },
  '1st approach':           { field: 'approach_1_method', type: 'string', required: false },
  'date ':                  { field: 'approach_1_date',   type: 'date',   required: false },
  'date':                   { field: 'approach_1_date',   type: 'date',   required: false },
  '2nd ':                   { field: 'approach_2_method', type: 'string', required: false },
  '2nd':                    { field: 'approach_2_method', type: 'string', required: false },
  'date .1':                { field: 'approach_2_date',   type: 'date',   required: false },
  'feedback':               { field: 'approach_feedback', type: 'string', required: false },
  'ai_processed_data':      { field: 'DISCARD',           type: null,     required: false },
};

const CRITERIA_PATTERN = /^(.+)_(score|rationale)$/i;

function auditColumns(columns) {
  const result = {
    mapped: [], unmapped: [], scoring_criteria: [], warnings: [],
    has_name: false, has_linkedin: false,
  };

  for (const col of columns) {
    if (!col) continue;
    const norm = col.toLowerCase().trim();

    if (CRITERIA_PATTERN.test(norm)) {
      const m = norm.match(CRITERIA_PATTERN);
      result.scoring_criteria.push({
        source: col, criteria: m[1], type: m[2],
        storage: 'investor_fit_criteria (JSON blob)',
      });
      continue;
    }

    if (KNOWN_MAPPINGS[norm]) {
      const m = KNOWN_MAPPINGS[norm];
      result.mapped.push({ source: col, target: m.field, type: m.type });
      if (m.field === 'name') result.has_name = true;
      if (m.field === 'linkedin_url') result.has_linkedin = true;
    } else {
      result.unmapped.push({ source: col, suggestion: 'store in enrichment_notes' });
    }
  }

  if (!result.has_name && !result.has_linkedin)
    result.warnings.push('CRITICAL: No identifier column found — cannot match');
  if (!result.has_linkedin)
    result.warnings.push('WARNING: No LinkedIn URL — name-only matching (lower confidence)');
  if (result.scoring_criteria.length > 0)
    result.warnings.push(
      `INFO: ${result.scoring_criteria.length} scoring criteria → investor_fit_criteria JSON`
    );

  return result;
}

module.exports = { auditColumns, KNOWN_MAPPINGS };
