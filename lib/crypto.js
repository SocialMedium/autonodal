// ═══════════════════════════════════════════════════════════════════════════════
// lib/crypto.js — Token encryption/decryption (AES-256-GCM)
// ═══════════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');

const ALGORITHM = 'aes-256-gcm';
const RAW_KEY = process.env.ENCRYPTION_KEY;

// Key derivation — accept any non-empty string and produce a 32-byte AES key.
//   - 64 hex chars: used directly (recommended shape)
//   - anything else: SHA-256(key) → 32 bytes (base64, passphrase, JWT secret, etc.)
// The derivation is deterministic so ciphertext stays decryptable across restarts.
// Startup logs which mode fired so the invariant is auditable at boot.
let _derivedKey = null;
let _keyMode = 'none';
if (!RAW_KEY) {
  console.error('[CRYPTO] ENCRYPTION_KEY missing — token encryption will FALL BACK TO PLAINTEXT. Set any non-empty value in the environment to enable AES-256-GCM.');
} else if (/^[0-9a-fA-F]{64}$/.test(RAW_KEY)) {
  _derivedKey = Buffer.from(RAW_KEY, 'hex');
  _keyMode = 'hex64';
  console.log('[CRYPTO] ENCRYPTION_KEY present (64-hex) — AES-256-GCM token encryption active.');
} else {
  _derivedKey = crypto.createHash('sha256').update(RAW_KEY, 'utf8').digest();
  _keyMode = 'sha256-derived';
  console.log(`[CRYPTO] ENCRYPTION_KEY present (${RAW_KEY.length} chars, SHA-256 derived) — AES-256-GCM token encryption active.`);
}

// Per-process flag so we only warn once per fallback write rather than on every call
let _fallbackWarnedOnce = false;

function getKey() {
  return _derivedKey;
}

function encryptToken(plaintext) {
  if (!plaintext) return null;
  const key = getKey();
  if (!key) {
    if (!_fallbackWarnedOnce) {
      console.error('[CRYPTO] encryptToken() falling back to plaintext — ENCRYPTION_KEY not configured. This message is logged once per process.');
      _fallbackWarnedOnce = true;
    }
    return plaintext; // Graceful fallback if key not set
  }
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return iv.toString('hex') + ':' + authTag.toString('hex') + ':' + encrypted.toString('hex');
}

function decryptToken(stored) {
  if (!stored) return null;
  const key = getKey();
  if (!key) return stored; // Graceful fallback
  // Detect if already plaintext (Google tokens start with ya29. or look like JWTs)
  if (/^ya29\.|^eyJ/.test(stored)) return stored;
  const parts = stored.split(':');
  if (parts.length !== 3) return stored; // Not encrypted format
  try {
    const iv = Buffer.from(parts[0], 'hex');
    const authTag = Buffer.from(parts[1], 'hex');
    const encrypted = Buffer.from(parts[2], 'hex');
    const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
    decipher.setAuthTag(authTag);
    return decipher.update(encrypted) + decipher.final('utf8');
  } catch {
    return stored; // Decryption failed — return as-is (may be plaintext)
  }
}

module.exports = { encryptToken, decryptToken };
