// ═══════════════════════════════════════════════════════════════════════════
// lib/onboarding/syncDiagnostic.js — Sync Error Classification & Resolution
// ═══════════════════════════════════════════════════════════════════════════
//
// Maps raw sync errors to user-facing diagnoses with ranked resolution actions.
// Used by the onboarding flow, integrations page, and admin health dashboard.

const ERROR_TAXONOMY = {
  AUTH_EXPIRED: {
    patterns: ['401', 'unauthorized', 'token expired', 'invalid_grant',
               'access_denied', 'token_revoked', 'invalid_token', 'unauthenticated'],
    title: 'Connection needs refreshing',
    explanation: 'Your {integration} connection has expired. This happens automatically after {duration}.',
    severity: 'warning',
    actions: [
      { id: 'reconnect', label: 'Reconnect {integration}', recommended: true, auto_resolve: true },
      { id: 'dismiss', label: "I'll do this later", recommended: false },
    ],
  },
  RATE_LIMITED: {
    patterns: ['429', 'rate limit', 'too many requests', 'quota exceeded', 'throttled'],
    title: 'Sync paused — rate limit reached',
    explanation: 'Your {integration} account has a request limit. The sync will resume automatically in {retry_after}.',
    severity: 'info',
    actions: [
      { id: 'auto_retry', label: 'Retry automatically', recommended: true, auto_resolve: true },
      { id: 'schedule', label: 'Schedule for tonight', recommended: false },
    ],
  },
  MISSING_IDENTIFIER: {
    patterns: ['no email', 'missing identifier', 'no linkedin', 'cannot identify',
               'missing required', 'no name', 'empty record'],
    title: "Some records couldn't be imported",
    explanation: '{count} records have no email address or LinkedIn URL. Autonodal needs at least one identifier to build a profile.',
    severity: 'warning',
    actions: [
      { id: 'skip_unidentified', label: 'Skip these {count} records', recommended: true },
      { id: 'import_anyway', label: 'Import as unidentified contacts (limited intelligence)', recommended: false },
      { id: 'enrich_first', label: 'Enrich in {integration} first, then re-sync', recommended: false },
    ],
  },
  DUPLICATE_DETECTED: {
    patterns: ['duplicate', 'already exists', 'conflict', 'unique constraint',
               'duplicate key', 'on conflict'],
    title: 'Duplicates found',
    explanation: '{count} records already exist in your sandbox from a previous import. I can merge them or skip.',
    severity: 'info',
    actions: [
      { id: 'merge', label: 'Merge duplicates (keep best data from each)', recommended: true },
      { id: 'skip_duplicates', label: 'Skip duplicates, import net-new only', recommended: false },
      { id: 'overwrite', label: 'Overwrite existing records', recommended: false },
    ],
  },
  FIELD_MAPPING_INCOMPLETE: {
    patterns: ['field mapping', 'unmapped field', 'schema mismatch', 'column not found',
               'invalid field', 'unknown column'],
    title: 'Field mapping needs updating',
    explanation: 'Some fields in your {integration} have changed since the last sync. {count} fields need remapping.',
    severity: 'warning',
    actions: [
      { id: 'remap', label: 'Review and update field mappings', recommended: true },
      { id: 'skip_unmapped', label: 'Skip unmapped fields for now', recommended: false },
    ],
  },
  PERMISSION_DENIED: {
    patterns: ['403', 'forbidden', 'insufficient permissions', 'scope', 'access denied',
               'not authorized', 'missing scope'],
    title: 'Missing permissions',
    explanation: "Autonodal doesn't have permission to access {resource} in your {integration} account.",
    severity: 'error',
    actions: [
      { id: 'reconnect_full', label: 'Reconnect with full permissions', recommended: true, auto_resolve: true },
      { id: 'contact_support', label: 'Get help', recommended: false },
    ],
  },
  NETWORK_ERROR: {
    patterns: ['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'network', 'fetch failed',
               'socket hang up', 'ECONNRESET', 'DNS', 'timeout'],
    title: 'Connection interrupted',
    explanation: 'The sync was interrupted by a network error. This is usually temporary.',
    severity: 'info',
    actions: [
      { id: 'retry', label: 'Retry now', recommended: true, auto_resolve: true },
      { id: 'schedule_retry', label: 'Retry in 1 hour automatically', recommended: false },
    ],
  },
  DATA_TOO_LARGE: {
    patterns: ['payload too large', 'request entity too large', 'too many records',
               'batch size exceeded', 'memory', 'out of memory'],
    title: 'Import too large for single batch',
    explanation: 'The dataset is larger than can be processed in one go. I can split it into smaller batches.',
    severity: 'warning',
    actions: [
      { id: 'batch_import', label: 'Split into smaller batches', recommended: true, auto_resolve: true },
      { id: 'limit_import', label: 'Import first {count} records only', recommended: false },
    ],
  },
  UNKNOWN: {
    patterns: [],
    title: 'Something went wrong',
    explanation: 'The sync encountered an unexpected error. Our team has been notified.',
    severity: 'error',
    actions: [
      { id: 'retry', label: 'Try again', recommended: true },
      { id: 'contact_support', label: 'Get help', recommended: false },
    ],
  },
};

/**
 * Classify a sync error and return a structured diagnosis.
 *
 * @param {string} rawError - The raw error message/code from the sync pipeline
 * @param {Object} context - { integration, count, resource, retry_after, duration }
 * @returns {Object} Diagnosis with title, explanation, severity, actions
 */
function classifyError(rawError, context = {}) {
  const errorLower = String(rawError).toLowerCase();

  let matched = null;
  let matchedKey = 'UNKNOWN';

  // Check patterns in priority order (AUTH_EXPIRED before PERMISSION_DENIED matters for "401")
  const priorityOrder = [
    'AUTH_EXPIRED', 'RATE_LIMITED', 'PERMISSION_DENIED', 'NETWORK_ERROR',
    'MISSING_IDENTIFIER', 'DUPLICATE_DETECTED', 'FIELD_MAPPING_INCOMPLETE',
    'DATA_TOO_LARGE',
  ];

  for (const key of priorityOrder) {
    const taxonomy = ERROR_TAXONOMY[key];
    if (taxonomy.patterns.some(p => errorLower.includes(p.toLowerCase()))) {
      matched = taxonomy;
      matchedKey = key;
      break;
    }
  }

  if (!matched) {
    matched = ERROR_TAXONOMY.UNKNOWN;
    matchedKey = 'UNKNOWN';
  }

  const interpolate = (str) => str
    .replace(/\{integration\}/g, context.integration || 'your integration')
    .replace(/\{count\}/g, String(context.count || 'some'))
    .replace(/\{resource\}/g, context.resource || 'this resource')
    .replace(/\{retry_after\}/g, context.retry_after || 'a few minutes')
    .replace(/\{duration\}/g, context.duration || '60 minutes');

  return {
    error_type: matchedKey,
    title: interpolate(matched.title),
    explanation: interpolate(matched.explanation),
    severity: matched.severity,
    actions: matched.actions.map(a => ({
      ...a,
      label: interpolate(a.label),
    })),
    raw_error: rawError,
    classified_at: new Date().toISOString(),
  };
}

/**
 * Execute a resolution action.
 *
 * @param {string} actionId - The action to execute
 * @param {Object} context - { tenant_id, connection_type, connection_ref, integration }
 * @returns {Object} { resolved, next_action, message }
 */
async function resolveAction(actionId, context = {}) {
  switch (actionId) {
    case 'reconnect':
    case 'reconnect_full':
      return {
        resolved: false,
        next_action: 'redirect',
        redirect_url: `/api/auth/${context.integration || 'google'}/connect`,
        message: 'Redirecting to reconnect...',
      };

    case 'auto_retry':
    case 'retry':
      return {
        resolved: true,
        next_action: 'retry_sync',
        message: 'Sync job re-queued. Will retry shortly.',
      };

    case 'schedule':
    case 'schedule_retry':
      return {
        resolved: true,
        next_action: 'scheduled',
        message: 'Scheduled for automatic retry in 1 hour.',
      };

    case 'skip_unidentified':
      return {
        resolved: true,
        next_action: 'continue_import',
        message: `Skipping ${context.count || 'unidentified'} records. Importing the rest.`,
        config_update: { skip_missing_identifier: true },
      };

    case 'import_anyway':
      return {
        resolved: true,
        next_action: 'continue_import',
        message: 'Importing all records including unidentified. Limited intelligence for those contacts.',
        config_update: { skip_missing_identifier: false },
      };

    case 'merge':
      return {
        resolved: true,
        next_action: 'merge_duplicates',
        message: 'Merging duplicates — keeping the best data from each record.',
      };

    case 'skip_duplicates':
      return {
        resolved: true,
        next_action: 'continue_import',
        message: 'Skipping duplicates. Only net-new records will be imported.',
        config_update: { skip_duplicates: true },
      };

    case 'overwrite':
      return {
        resolved: true,
        next_action: 'continue_import',
        message: 'Overwriting existing records with fresh data.',
        config_update: { overwrite_existing: true },
      };

    case 'remap':
      return {
        resolved: false,
        next_action: 'redirect',
        redirect_url: '/onboarding-field-mapping.html',
        message: 'Opening field mapping review...',
      };

    case 'skip_unmapped':
      return {
        resolved: true,
        next_action: 'continue_import',
        message: 'Skipping unmapped fields. Known fields will still sync.',
        config_update: { skip_unmapped_fields: true },
      };

    case 'batch_import':
      return {
        resolved: true,
        next_action: 'batch_import',
        message: 'Splitting into smaller batches. This may take longer but will complete reliably.',
        config_update: { batch_size: 100 },
      };

    case 'dismiss':
      return { resolved: true, next_action: 'none', message: 'Dismissed.' };

    case 'contact_support':
      return {
        resolved: false,
        next_action: 'redirect',
        redirect_url: 'mailto:support@autonodal.com',
        message: 'Opening support contact...',
      };

    default:
      return { resolved: false, next_action: 'none', message: 'Unknown action.' };
  }
}

module.exports = { classifyError, resolveAction, ERROR_TAXONOMY };
