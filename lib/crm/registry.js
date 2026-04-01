// ═══════════════════════════════════════════════════════════════════════════════
// lib/crm/registry.js — CRM Adapter Factory
//
// Usage:
//   const { getAdapter } = require('./registry');
//   const adapter = getAdapter(connectionRow, pool);
//   await adapter.syncInbound({ since: lastSync });
// ═══════════════════════════════════════════════════════════════════════════════

const adapters = {};

// Register adapters lazily to avoid missing-module errors
function register(provider, requirePath) {
  Object.defineProperty(adapters, provider, {
    get: () => require(requirePath),
    configurable: true,
    enumerable: true,
  });
}

register('ezekia', './ezekia_adapter');
// register('bullhorn', './bullhorn_adapter');
// register('vincere', './vincere_adapter');
// register('jobadder', './jobadder_adapter');

function getAdapter(connection, pool) {
  const AdapterClass = adapters[connection.provider];
  if (!AdapterClass) {
    throw new Error(`Unknown CRM provider: ${connection.provider}. Available: ${Object.keys(adapters).join(', ')}`);
  }
  return new AdapterClass(connection, pool);
}

function listProviders() {
  return Object.keys(adapters);
}

module.exports = { getAdapter, listProviders };
