// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/index.js — Public API for the shared OIDC module
// ═══════════════════════════════════════════════════════════════════════════

const flow = require('./flow');
const reconciliation = require('./reconciliation');
const { OidcError, CODES } = require('./errors');

const providers = {
  linkedin: require('./providers/linkedin'),
  google: require('./providers/google'),
  microsoft: require('./providers/microsoft'),
};

function getProvider(id) {
  const p = providers[id];
  if (!p) throw new OidcError(CODES.UNKNOWN_PROVIDER, `Unknown OIDC provider: ${id}`);
  return p;
}

module.exports = {
  // Providers
  providers,
  getProvider,

  // Flow orchestration
  startFlow: flow.startFlow,
  validateCallback: flow.validateCallback,
  exchangeCode: flow.exchangeCode,
  fetchUserinfo: flow.fetchUserinfo,

  // Reconciliation
  reconcileIdentity: reconciliation.reconcileIdentity,
  getLinkedProviders: reconciliation.getLinkedProviders,
  unlinkProvider: reconciliation.unlinkProvider,

  // Errors
  OidcError,
  ERROR_CODES: CODES,
};
