// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/errors.js — Structured OIDC error types
// ═══════════════════════════════════════════════════════════════════════════

class OidcError extends Error {
  constructor(code, message, cause) {
    super(message);
    this.name = 'OidcError';
    this.code = code;
    this.cause = cause;
  }
}

const CODES = {
  UNKNOWN_PROVIDER: 'unknown_provider',
  NOT_CONFIGURED: 'not_configured',
  NO_CODE: 'no_code',
  STATE_MISMATCH: 'state_mismatch',
  STATE_EXPIRED: 'state_expired',
  TOKEN_EXCHANGE_FAILED: 'token_exchange_failed',
  USERINFO_FAILED: 'userinfo_failed',
  NO_EMAIL: 'no_email',
  RECONCILIATION_FAILED: 'reconciliation_failed',
  SESSION_CREATE_FAILED: 'session_create_failed',
};

module.exports = { OidcError, CODES };
