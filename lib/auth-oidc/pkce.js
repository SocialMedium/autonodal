// ═══════════════════════════════════════════════════════════════════════════
// lib/auth-oidc/pkce.js — PKCE (Proof Key for Code Exchange) helpers
// RFC 7636 — required for production OAuth
// ═══════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');

/**
 * Generate a cryptographically random code_verifier.
 * Length must be between 43 and 128 characters. 64 bytes base64url → 86 chars.
 */
function generateVerifier() {
  return crypto.randomBytes(64).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

/**
 * Compute the code_challenge for a given code_verifier.
 * Uses S256 method: BASE64URL(SHA256(verifier))
 */
function computeChallenge(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

/**
 * Generate a verifier + matching challenge pair.
 */
function generatePair() {
  const verifier = generateVerifier();
  const challenge = computeChallenge(verifier);
  return { verifier, challenge, method: 'S256' };
}

module.exports = { generateVerifier, computeChallenge, generatePair };
