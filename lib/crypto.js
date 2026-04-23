// ═══════════════════════════════════════════════════════════════════════════════
// lib/crypto.js — Token encryption/decryption (AES-256-GCM)
// ═══════════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');

const ALGORITHM = 'aes-256-gcm';
const KEY_HEX = process.env.ENCRYPTION_KEY;

// Startup assertion — logged once so Railway boot output makes the encryption
// state unambiguous. Guards against silent plaintext-fallback regressions.
if (!KEY_HEX) {
  console.error('[CRYPTO] ENCRYPTION_KEY missing — token encryption will FALL BACK TO PLAINTEXT. Set a 64-char hex key in the environment to enable AES-256-GCM.');
} else if (KEY_HEX.length !== 64) {
  console.error(`[CRYPTO] ENCRYPTION_KEY is ${KEY_HEX.length} chars — must be exactly 64 hex chars for AES-256-GCM. Token encryption will fall back to plaintext.`);
} else {
  console.log('[CRYPTO] ENCRYPTION_KEY present — AES-256-GCM token encryption active.');
}

// Per-process flag so we only warn once per fallback write rather than on every call
let _fallbackWarnedOnce = false;

function getKey() {
  if (!KEY_HEX || KEY_HEX.length !== 64) return null;
  return Buffer.from(KEY_HEX, 'hex');
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
