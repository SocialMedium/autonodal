// ═══════════════════════════════════════════════════════════════════════════
// lib/onboarding/fieldMapper.js — AI Field Inference Engine
// ═══════════════════════════════════════════════════════════════════════════
//
// Two-pass approach:
//   Pass 1 (heuristic): alias matching + value pattern detection — zero cost
//   Pass 2 (LLM):       Claude inference for uncertain fields only — minimal cost

const { PEOPLE_FIELDS, COMPANY_FIELDS, THRESHOLDS, confidenceLabel } = require('./schemaRegistry');

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const CLAUDE_MODEL = 'claude-haiku-4-5-20251001';

async function callClaude(system, user) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 2048,
      system,
      messages: [{ role: 'user', content: user }],
    }),
  });
  if (!res.ok) throw new Error(`Claude API ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.content.find(c => c.type === 'text')?.text || '';
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════════

async function inferFieldMappings(sampleRecords, entityType, integrationSource) {
  if (!sampleRecords || sampleRecords.length === 0) {
    throw new Error('No sample records provided for field mapping inference');
  }

  const targetSchema = entityType === 'people' ? PEOPLE_FIELDS : COMPANY_FIELDS;
  const sourceFields = extractSourceFields(sampleRecords);
  const fieldSamples = buildFieldSamples(sampleRecords, sourceFields);

  // Pass 1: heuristic
  const heuristicResults = runHeuristicPass(sourceFields, fieldSamples, targetSchema);

  // Pass 2: LLM for uncertain fields
  const uncertainFields = heuristicResults.filter(
    r => r.confidence < THRESHOLDS.AUTO_APPLY && r.confidence >= 0
  );

  let llmResults = [];
  if (uncertainFields.length > 0 && ANTHROPIC_API_KEY) {
    try {
      llmResults = await runLLMPass(uncertainFields, fieldSamples, targetSchema, integrationSource);
    } catch (err) {
      console.error('[FieldMapper] LLM pass failed, using heuristic only:', err.message);
    }
  }

  const mergedResults = mergePassResults(heuristicResults, llmResults);

  return mergedResults.map(mapping => ({
    ...mapping,
    auto_apply: mapping.confidence >= THRESHOLDS.AUTO_APPLY,
    confidence_label: confidenceLabel(mapping.confidence),
    target_label: getTargetLabel(mapping.target_field, targetSchema),
  }));
}

function getTargetLabel(field, schema) {
  if (!field) return null;
  const match = schema.find(s => s.field === field);
  return match ? match.label : field;
}

// ═══════════════════════════════════════════════════════════════════════════
// FIELD EXTRACTION
// ═══════════════════════════════════════════════════════════════════════════

function extractSourceFields(records) {
  const fields = new Set();
  for (const record of records.slice(0, 20)) {
    for (const k of Object.keys(record)) {
      if (record[k] === null || record[k] === undefined) continue;
      if (typeof record[k] === 'object' && !Array.isArray(record[k])) {
        // One level of nesting (HubSpot properties, Airtable fields)
        for (const nested of Object.keys(record[k])) {
          fields.add(`${k}.${nested}`);
        }
      } else {
        fields.add(k);
      }
    }
  }
  return Array.from(fields);
}

function buildFieldSamples(records, fields) {
  const samples = {};
  for (const field of fields) {
    const values = [];
    for (const record of records) {
      const val = getNestedValue(record, field);
      if (val !== null && val !== undefined && String(val).trim() && values.length < 3) {
        values.push(String(val).trim().slice(0, 100));
      }
    }
    samples[field] = values;
  }
  return samples;
}

function getNestedValue(obj, path) {
  return path.split('.').reduce((acc, key) => acc?.[key], obj);
}

// ═══════════════════════════════════════════════════════════════════════════
// PASS 1: HEURISTIC MATCHING
// ═══════════════════════════════════════════════════════════════════════════

function runHeuristicPass(sourceFields, fieldSamples, targetSchema) {
  return sourceFields.map(sourceField => {
    const cleanField = sourceField
      .replace(/^(properties|fields)\./, '')
      .toLowerCase()
      .replace(/[_\-]/g, ' ')
      .replace(/^hs /, '')     // HubSpot prefixes
      .trim();

    const samples = fieldSamples[sourceField] || [];
    let bestMatch = null;
    let bestScore = 0;

    for (const target of targetSchema) {
      let score = 0;

      // Exact match on field name
      if (cleanField === target.field.replace(/_/g, ' ')) score = 0.95;
      // Exact alias match
      else if (target.aliases.includes(cleanField)) score = 0.92;
      // Alias contains source or source contains alias (partial)
      else if (target.aliases.some(a => a.includes(cleanField) && cleanField.length >= 3)) score = 0.75;
      else if (target.aliases.some(a => cleanField.includes(a) && a.length >= 3)) score = 0.70;

      // Value pattern bonus — only boosts existing name matches, never creates new ones
      // Exception: definitive patterns (email, URL) can create matches alone via inferFromValues
      if (score > 0 && target.valuePatterns && samples.length > 0) {
        const patternScore = scoreValuePatterns(samples, target.valuePatterns);
        score = Math.min(1.0, score + patternScore * 0.15);
      }

      if (score > bestScore) {
        bestScore = score;
        bestMatch = target.field;
      }
    }

    // Value-only inference for completely unmatched fields
    if (bestScore === 0 && samples.length > 0) {
      const valueInference = inferFromValues(samples);
      if (valueInference) {
        bestMatch = valueInference.field;
        bestScore = valueInference.score;
      }
    }

    return {
      source_field: sourceField,
      source_sample: samples,
      target_field: bestMatch,
      confidence: Math.round(bestScore * 100) / 100,
      ai_reasoning: null,
      tenant_decision: null,
      reviewed: false,
    };
  });
}

function scoreValuePatterns(samples, patterns) {
  let score = 0;
  let checks = 0;
  for (const sample of samples) {
    for (const pattern of patterns) {
      checks++;
      switch (pattern) {
        case 'email_format':
          if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(sample)) score++;
          break;
        case 'linkedin_url':
          if (/linkedin\.com\/in\//i.test(sample)) score++;
          break;
        case 'linkedin_company_url':
          if (/linkedin\.com\/company\//i.test(sample)) score++;
          break;
        case 'phone_format':
          if (/^\+?[\d\s\-().]{8,}$/.test(sample)) score++;
          break;
        case 'domain_format':
          if (/^[a-z0-9.-]+\.(com|io|co|org|net|ai|vc|xyz)$/i.test(sample)) score++;
          break;
        case 'two_words':
          if (sample.trim().split(/\s+/).length >= 2) score++;
          break;
        case 'mixed_case_name':
          if (/^[A-Z][a-z]+ [A-Z][a-z]/.test(sample)) score++;
          break;
        case 'job_title':
          if (/\b(CEO|CTO|CFO|VP|Director|Manager|Head|Lead|Engineer|Founder|Partner)\b/i.test(sample)) score++;
          break;
      }
    }
  }
  return checks > 0 ? score / checks : 0;
}

function inferFromValues(samples) {
  // Email detection
  if (samples.some(s => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s))) {
    return { field: 'email', score: 0.90 };
  }
  // LinkedIn URL detection
  if (samples.some(s => /linkedin\.com\/in\//i.test(s))) {
    return { field: 'linkedin_url', score: 0.90 };
  }
  // Phone detection
  if (samples.some(s => /^\+?[\d\s\-().]{8,}$/.test(s))) {
    return { field: 'phone', score: 0.75 };
  }
  // Domain detection
  if (samples.some(s => /^[a-z0-9.-]+\.(com|io|co|org|net|ai)$/i.test(s))) {
    return { field: 'domain', score: 0.80 };
  }
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// PASS 2: LLM INFERENCE (UNCERTAIN FIELDS ONLY)
// ═══════════════════════════════════════════════════════════════════════════

async function runLLMPass(uncertainFields, fieldSamples, targetSchema, integrationSource) {
  const targetFieldList = targetSchema
    .map(f => `  - ${f.field}: ${f.label} (e.g. ${f.examples[0]})`)
    .join('\n');

  const fieldsToInfer = uncertainFields.map(f => ({
    source_field: f.source_field,
    samples: f.source_sample,
  }));

  const system = 'You map integration fields to a target schema. Return ONLY a JSON array. No markdown, no preamble.';
  const user = `Map these fields from a ${integrationSource} integration to the Autonodal schema.

Available target fields:
${targetFieldList}
  - SKIP: No useful mapping

For each source field, return: {"source_field": "...", "target_field": "...", "confidence": 0.0-1.0, "reasoning": "one sentence"}

Source fields:
${JSON.stringify(fieldsToInfer, null, 2)}`;

  const raw = await callClaude(system, user);

  try {
    const cleaned = raw.replace(/```json\n?|\n?```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    const results = Array.isArray(parsed) ? parsed : (parsed.mappings || parsed.results || []);

    return results.map(r => ({
      source_field: r.source_field,
      target_field: r.target_field === 'SKIP' ? null : r.target_field,
      confidence: Math.round((r.confidence || 0) * 100) / 100,
      ai_reasoning: r.reasoning || null,
    }));
  } catch (e) {
    console.error('[FieldMapper] Failed to parse LLM response:', e.message);
    return [];
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// RESULT MERGING
// ═══════════════════════════════════════════════════════════════════════════

function mergePassResults(heuristicResults, llmResults) {
  const llmMap = new Map(llmResults.map(r => [r.source_field, r]));

  const merged = heuristicResults.map(h => {
    const llm = llmMap.get(h.source_field);
    if (llm) {
      // If heuristic had zero match, cap LLM confidence at HIGH (never auto-apply)
      // These fields genuinely need human review — the LLM is guessing
      const cappedConfidence = h.confidence === 0
        ? Math.min(llm.confidence, THRESHOLDS.HIGH)
        : llm.confidence;
      if (cappedConfidence > h.confidence) {
        return { ...h, ...llm, confidence: cappedConfidence };
      }
    }
    return h;
  });

  // Deduplicate: if two source fields map to same target, keep highest confidence
  const targetsSeen = new Map();
  return merged.map(mapping => {
    if (!mapping.target_field) return mapping;
    const existing = targetsSeen.get(mapping.target_field);
    if (existing && existing.confidence >= mapping.confidence) {
      return {
        ...mapping,
        target_field: null,
        confidence: 0,
        ai_reasoning: `Duplicate: another field mapped to ${mapping.target_field} with higher confidence`,
      };
    }
    targetsSeen.set(mapping.target_field, mapping);
    return mapping;
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// SUMMARY
// ═══════════════════════════════════════════════════════════════════════════

function summariseMappings(mappings) {
  const autoApply = mappings.filter(m => m.auto_apply && m.target_field).length;
  const reviewRequired = mappings.filter(m => !m.auto_apply && m.target_field).length;
  const skipped = mappings.filter(m => !m.target_field).length;

  return {
    total: mappings.length,
    auto_applied: autoApply,
    review_required: reviewRequired,
    skipped,
  };
}

module.exports = { inferFieldMappings, summariseMappings };
