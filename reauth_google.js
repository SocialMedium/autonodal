require('dotenv').config();
const { google } = require('googleapis');
const http = require('http');
const { Pool } = require('pg');

const oauth2 = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  'http://localhost:3002/callback'
);

const url = oauth2.generateAuthUrl({
  access_type: 'offline',
  prompt: 'consent',
  scope: [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/contacts.readonly',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile',
  ]
});

console.log('\n✅ Open this URL in your browser:\n');
console.log(url);
console.log('\nWaiting for Google to redirect back...\n');

const server = http.createServer(async (req, res) => {
  const code = new URL(req.url, 'http://localhost:3002').searchParams.get('code');

  if (!code) {
    res.end('No code received');
    return;
  }

  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  try {
    const { tokens } = await oauth2.getToken(code);
    oauth2.setCredentials(tokens);

    const oauth2api = google.oauth2({ version: 'v2', auth: oauth2 });
    const { data } = await oauth2api.userinfo.get();

    await pool.query(
      `UPDATE user_google_accounts SET
         access_token     = $1,
         refresh_token    = $2,
         token_expires_at = $3
       WHERE google_email = $4`,
      [tokens.access_token, tokens.refresh_token, new Date(tokens.expiry_date), data.email]
    );

    console.log('✅ Token saved for', data.email);
    console.log('   Expires:', new Date(tokens.expiry_date));
    console.log('\n▶ Now run: node scripts/sync_gmail.js --full-scan --dry-run\n');

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end('<h2 style="font-family:sans-serif;padding:40px">✅ Connected! You can close this tab and return to the terminal.</h2>');

    pool.end();
    server.close();

  } catch (err) {
    console.error('✗ Error:', err.message);
    res.writeHead(500);
    res.end('Error: ' + err.message);
    pool.end();
    server.close();
  }
});

server.listen(3002, () => {
  console.log('Listening on http://localhost:3002/callback');
});
