// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/state.js — CSRF state parameter + in-memory tracker
// ═══════════════════════════════════════════════════════════════════════════
//
// The state parameter prevents CSRF on the OAuth callback. We generate a
// random state at flow start, store it server-side alongside the PKCE
// verifier + optional return_to URL, and validate it on callback.
//
// Storage: in-memory Map with TTL. Autonodal is single-process, so this is
// sufficient. If/when multi-process, move to Redis/DB.

const crypto = require('crypto');

const TTL_MS = 10 * 60 * 1000; // 10 minutes — generous for slow consent screens
const _store = new Map();

// Periodic cleanup of expired entries
setInterval(() => {
  const now = Date.now();
  for (const [state, entry] of _store.entries()) {
    if (entry.expiresAt < now) _store.delete(state);
  }
}, 60 * 1000);

function generate() {
  return crypto.randomBytes(32).toString('hex');
}

function store(state, data) {
  _store.set(state, { ...data, expiresAt: Date.now() + TTL_MS });
}

function consume(state) {
  if (!state) return null;
  const entry = _store.get(state);
  if (!entry) return null;
  _store.delete(state); // single-use — prevents replay
  if (entry.expiresAt < Date.now()) return null;
  return entry;
}

module.exports = { generate, store, consume };
