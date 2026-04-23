// Google OAuth & Gmail Integration
// Handles authentication and email sync

const { google } = require('googleapis');

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URL || process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3001/api/auth/google/callback';

// Scopes we need
const SCOPES = [
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/calendar.readonly',
  'https://www.googleapis.com/auth/userinfo.email',
  'https://www.googleapis.com/auth/userinfo.profile',
  'https://www.googleapis.com/auth/contacts.readonly'
];

/**
 * Create OAuth2 client
 */
function createOAuth2Client() {
  return new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );
}

/**
 * Generate auth URL for user to connect
 */
function getAuthUrl(state = null) {
  const oauth2Client = createOAuth2Client();
  
  return oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
    prompt: 'consent', // Force refresh token
    state: state // Pass user ID or session
  });
}

/**
 * Exchange code for tokens
 */
async function getTokensFromCode(code) {
  const oauth2Client = createOAuth2Client();
  const { tokens } = await oauth2Client.getToken(code);
  return tokens;
}

/**
 * Refresh access token
 */
async function refreshAccessToken(refreshToken) {
  const oauth2Client = createOAuth2Client();
  oauth2Client.setCredentials({ refresh_token: refreshToken });
  
  const { credentials } = await oauth2Client.refreshAccessToken();
  return credentials;
}

/**
 * Get authenticated OAuth client for a user
 */
function getAuthenticatedClient(accessToken, refreshToken) {
  const oauth2Client = createOAuth2Client();
  oauth2Client.setCredentials({
    access_token: accessToken,
    refresh_token: refreshToken
  });
  return oauth2Client;
}

/**
 * Get user's email from Google
 */
async function getUserEmail(accessToken) {
  const oauth2Client = createOAuth2Client();
  oauth2Client.setCredentials({ access_token: accessToken });
  
  const oauth2 = google.oauth2({ version: 'v2', auth: oauth2Client });
  const { data } = await oauth2.userinfo.get();
  return data.email;
}

/**
 * Get Gmail client
 */
function getGmailClient(auth) {
  return google.gmail({ version: 'v1', auth });
}

/**
 * Get Calendar client
 */
function getCalendarClient(auth) {
  return google.calendar({ version: 'v3', auth });
}

// =====================================================
// CONTACTS (PEOPLE API) FUNCTIONS  
// =====================================================

/**
 * Get People API client for Contacts
 */
function getContactsClient(auth) {
  return google.people({ version: 'v1', auth });
}

/**
 * List all contacts from Google
 */
async function listContacts(people, options = {}) {
  const response = await people.people.connections.list({
    resourceName: 'people/me',
    pageSize: options.pageSize || 1000,
    personFields: 'names,emailAddresses,phoneNumbers,organizations,addresses,photos,biographies,urls',
    pageToken: options.pageToken
  });
  
  return {
    connections: response.data.connections || [],
    nextPageToken: response.data.nextPageToken,
    totalItems: response.data.totalItems
  };
}

/**
 * Parse a Google contact into our format
 */
function parseContact(contact) {
  const name = contact.names?.[0];
  const email = contact.emailAddresses?.[0];
  const phone = contact.phoneNumbers?.[0];
  const org = contact.organizations?.[0];
  const address = contact.addresses?.[0];
  const photo = contact.photos?.[0];
  const bio = contact.biographies?.[0];
  const linkedIn = contact.urls?.find(u => u.value?.includes('linkedin'));
  
  return {
    resourceName: contact.resourceName,
    full_name: name?.displayName || null,
    first_name: name?.givenName || null,
    last_name: name?.familyName || null,
    email: email?.value || null,
    phone: phone?.value || null,
    current_title: org?.title || null,
    current_company_name: org?.name || null,
    location: address?.formattedValue || null,
    city: address?.city || null,
    country: address?.country || null,
    linkedin_url: linkedIn?.value || null,
    profile_photo_url: photo?.url || null,
    bio: bio?.value || null
  };
}

/**
 * Sync Google Contacts to people table
 */
async function syncContacts(db, userGoogleAccount) {
  const auth = getAuthenticatedClient(
    userGoogleAccount.access_token,
    userGoogleAccount.refresh_token
  );
  
  const people = getContactsClient(auth);
  const stats = { total: 0, created: 0, updated: 0, skipped: 0, errors: 0 };
  
  let pageToken = null;
  
  do {
    try {
      const { connections, nextPageToken, totalItems } = await listContacts(people, { pageToken });
      
      if (stats.total === 0) {
        console.log(`Found ${totalItems || connections.length} contacts`);
      }
      
      for (const contact of connections) {
        try {
          const parsed = parseContact(contact);
          
          // Skip contacts without email
          if (!parsed.email) {
            stats.skipped++;
            continue;
          }
          
          stats.total++;
          
          // Check if person exists by email
          const existing = await db.queryOne(
            'SELECT id, source FROM people WHERE LOWER(email) = LOWER($1)',
            [parsed.email]
          );
          
          if (existing) {
            // Only update if source is google_contacts (don't overwrite Ezekia data)
            if (existing.source === 'google_contacts') {
              await db.query(`
                UPDATE people SET
                  full_name = COALESCE($1, full_name),
                  first_name = COALESCE($2, first_name),
                  last_name = COALESCE($3, last_name),
                  phone = COALESCE($4, phone),
                  current_title = COALESCE($5, current_title),
                  current_company_name = COALESCE($6, current_company_name),
                  location = COALESCE($7, location),
                  linkedin_url = COALESCE($8, linkedin_url),
                  profile_photo_url = COALESCE($9, profile_photo_url),
                  synced_at = NOW()
                WHERE id = $10
              `, [
                parsed.full_name, parsed.first_name, parsed.last_name,
                parsed.phone, parsed.current_title, parsed.current_company_name,
                parsed.location, parsed.linkedin_url, parsed.profile_photo_url,
                existing.id
              ]);
              stats.updated++;
            } else {
              // Person exists from another source, skip
              stats.skipped++;
            }
          } else {
            // Create new person
            await db.query(`
              INSERT INTO people (
                source, full_name, first_name, last_name, email, phone,
                current_title, current_company_name, location, linkedin_url,
                profile_photo_url, synced_at, status
              ) VALUES (
                'google_contacts', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), 'active'
              )
            `, [
              parsed.full_name, parsed.first_name, parsed.last_name,
              parsed.email, parsed.phone, parsed.current_title,
              parsed.current_company_name, parsed.location, parsed.linkedin_url,
              parsed.profile_photo_url
            ]);
            stats.created++;
          }
        } catch (err) {
          console.error(`Error processing contact:`, err.message);
          stats.errors++;
        }
      }
      
      pageToken = nextPageToken;
      
      // Rate limiting
      await new Promise(r => setTimeout(r, 100));
      
    } catch (err) {
      console.error('Error fetching contacts:', err.message);
      stats.errors++;
      break;
    }
  } while (pageToken);
  
  console.log(`Contacts sync: ${stats.total} processed, ${stats.created} created, ${stats.updated} updated, ${stats.skipped} skipped`);
  
  return stats;
}

// =====================================================
// GMAIL FUNCTIONS
// =====================================================

/**
 * List messages matching query
 */
async function listMessages(gmail, query, maxResults = 100, pageToken = null) {
  const response = await gmail.users.messages.list({
    userId: 'me',
    q: query,
    maxResults,
    pageToken
  });
  
  return {
    messages: response.data.messages || [],
    nextPageToken: response.data.nextPageToken
  };
}

/**
 * Get full message details
 */
async function getMessage(gmail, messageId) {
  const response = await gmail.users.messages.get({
    userId: 'me',
    id: messageId,
    format: 'full'
  });
  
  return parseMessage(response.data);
}

/**
 * Parse Gmail message into usable format
 */
function parseMessage(message) {
  const headers = message.payload?.headers || [];
  const getHeader = (name) => headers.find(h => h.name.toLowerCase() === name.toLowerCase())?.value;
  
  // Get body
  let body = '';
  let snippet = message.snippet || '';
  
  if (message.payload?.body?.data) {
    body = Buffer.from(message.payload.body.data, 'base64').toString('utf-8');
  } else if (message.payload?.parts) {
    const textPart = message.payload.parts.find(p => p.mimeType === 'text/plain');
    if (textPart?.body?.data) {
      body = Buffer.from(textPart.body.data, 'base64').toString('utf-8');
    }
  }
  
  // Check for attachments
  const hasAttachments = message.payload?.parts?.some(p => p.filename && p.filename.length > 0) || false;
  const attachmentNames = message.payload?.parts
    ?.filter(p => p.filename && p.filename.length > 0)
    ?.map(p => p.filename) || [];
  
  // Parse recipients
  const parseRecipients = (header) => {
    if (!header) return [];
    return header.split(',').map(e => e.trim());
  };
  
  return {
    id: message.id,
    threadId: message.threadId,
    labelIds: message.labelIds || [],
    snippet,
    
    // Headers
    subject: getHeader('Subject') || '(No subject)',
    from: getHeader('From'),
    to: parseRecipients(getHeader('To')),
    cc: parseRecipients(getHeader('Cc')),
    date: getHeader('Date'),
    messageId: getHeader('Message-ID'),
    inReplyTo: getHeader('In-Reply-To'),
    
    // Content
    body,
    
    // Attachments
    hasAttachments,
    attachmentNames,
    
    // Timestamps
    internalDate: new Date(parseInt(message.internalDate))
  };
}

/**
 * Extract email address from "Name <email>" format
 */
function extractEmail(fromString) {
  if (!fromString) return null;
  const match = fromString.match(/<(.+?)>/);
  return match ? match[1].toLowerCase() : fromString.toLowerCase().trim();
}

/**
 * Build query for emails to/from specific addresses
 */
function buildCandidateQuery(emails, afterDate = null) {
  if (!emails || emails.length === 0) return null;
  
  const emailClauses = emails.map(e => `from:${e} OR to:${e}`).join(' OR ');
  let query = `(${emailClauses})`;
  
  if (afterDate) {
    const dateStr = afterDate.toISOString().split('T')[0].replace(/-/g, '/');
    query += ` after:${dateStr}`;
  }
  
  return query;
}

/**
 * Get history (incremental sync)
 */
async function getHistory(gmail, startHistoryId) {
  try {
    const response = await gmail.users.history.list({
      userId: 'me',
      startHistoryId,
      historyTypes: ['messageAdded']
    });
    
    return {
      history: response.data.history || [],
      historyId: response.data.historyId
    };
  } catch (error) {
    if (error.code === 404) {
      // History ID too old, need full sync
      return { history: [], historyId: null, needsFullSync: true };
    }
    throw error;
  }
}

/**
 * Get current history ID (for starting point)
 */
async function getCurrentHistoryId(gmail) {
  const response = await gmail.users.getProfile({ userId: 'me' });
  return response.data.historyId;
}

// =====================================================
// CALENDAR FUNCTIONS
// =====================================================

/**
 * List calendar events
 */
async function listEvents(calendar, options = {}) {
  const response = await calendar.events.list({
    calendarId: 'primary',
    timeMin: options.timeMin || new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString(),
    timeMax: options.timeMax || new Date().toISOString(),
    maxResults: options.maxResults || 250,
    singleEvents: true,
    orderBy: 'startTime',
    pageToken: options.pageToken
  });
  
  return {
    events: response.data.items || [],
    nextPageToken: response.data.nextPageToken
  };
}

/**
 * Parse calendar event
 */
function parseEvent(event) {
  return {
    id: event.id,
    title: event.summary || '(No title)',
    description: event.description,
    location: event.location,
    startTime: event.start?.dateTime || event.start?.date,
    endTime: event.end?.dateTime || event.end?.date,
    attendees: (event.attendees || []).map(a => ({
      email: a.email,
      name: a.displayName,
      responseStatus: a.responseStatus,
      organizer: a.organizer || false
    })),
    organizer: event.organizer?.email,
    htmlLink: event.htmlLink,
    status: event.status
  };
}

// =====================================================
// SYNC FUNCTIONS
// =====================================================

/**
 * Sync emails for all known candidates
 * @param {object} db - Database instance
 * @param {object} userGoogleAccount - User's Google account record
 * @param {array} candidateEmails - List of candidate email addresses to match
 */
async function syncCandidateEmails(db, userGoogleAccount, candidateEmails) {
  const auth = getAuthenticatedClient(
    userGoogleAccount.access_token,
    userGoogleAccount.refresh_token
  );
  
  const gmail = getGmailClient(auth);
  const stats = { processed: 0, matched: 0, errors: 0 };
  
  // Build query for all candidate emails
  const query = buildCandidateQuery(candidateEmails, userGoogleAccount.last_sync_at);
  if (!query) return stats;
  
  let pageToken = null;
  
  do {
    const { messages, nextPageToken } = await listMessages(gmail, query, 100, pageToken);
    
    for (const msg of messages) {
      try {
        stats.processed++;
        const email = await getMessage(gmail, msg.id);
        
        // Find which candidate this email is about
        const fromEmail = extractEmail(email.from);
        const toEmails = email.to.map(extractEmail);
        const allEmails = [fromEmail, ...toEmails].filter(Boolean);
        
        const matchedCandidate = allEmails.find(e => 
          candidateEmails.includes(e?.toLowerCase())
        );
        
        if (!matchedCandidate) continue;
        
        // Determine direction
        const isFromCandidate = candidateEmails.includes(fromEmail?.toLowerCase());
        const direction = isFromCandidate ? 'inbound' : 'outbound';
        
        // Get person_id for this email
        const person = await db.queryOne(
          'SELECT id FROM people WHERE LOWER(email) = $1',
          [matchedCandidate.toLowerCase()]
        );
        
        if (!person) continue;
        
        stats.matched++;
        
        // Store email signal (always, even if content marked private)
        await db.query(`
          INSERT INTO email_signals (person_id, user_id, direction, email_date, thread_id, has_attachment, email_domain)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (person_id, user_id, email_date, direction) DO NOTHING
        `, [
          person.id,
          userGoogleAccount.user_id,
          direction,
          email.internalDate,
          email.threadId,
          email.hasAttachments,
          fromEmail?.split('@')[1]
        ]);
        
        // Store interaction with metadata only — NO MESSAGE BODY.
        // Gmail body content is never persisted (Google Limited Use compliance).
        // email_snippet is Gmail's own ~140-char preview and is kept for at-a-glance context.
        await db.query(`
          INSERT INTO interactions (
            person_id, type, direction, occurred_at, content,
            email_message_id, email_thread_id, email_subject, email_snippet,
            email_from, email_to, email_labels, email_has_attachments, email_attachment_names,
            visibility, owner_user_id
          )
          VALUES ($1, 'email', $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'company', $13)
          ON CONFLICT (email_message_id) WHERE email_message_id IS NOT NULL DO NOTHING
        `, [
          person.id,
          direction,
          email.internalDate,
          email.id,
          email.threadId,
          email.subject,
          email.snippet,
          email.from,
          email.to,
          email.labelIds,
          email.hasAttachments,
          email.attachmentNames,
          userGoogleAccount.user_id
        ]);
        
      } catch (err) {
        console.error(`Error processing message ${msg.id}:`, err.message);
        stats.errors++;
      }
    }
    
    pageToken = nextPageToken;
    
    // Rate limiting
    await new Promise(r => setTimeout(r, 100));
    
  } while (pageToken);
  
  // Update last sync time
  await db.query(
    'UPDATE user_google_accounts SET last_sync_at = NOW() WHERE id = $1',
    [userGoogleAccount.id]
  );
  
  return stats;
}

/**
 * Compute response time for a reply
 */
async function computeResponseTimes(db, personId) {
  // Get all email signals for this person, ordered by thread and date
  const signals = await db.query(`
    SELECT * FROM email_signals 
    WHERE person_id = $1 
    ORDER BY thread_id, email_date
  `, [personId]);
  
  let prevByThread = {};
  
  for (const signal of signals) {
    const prev = prevByThread[signal.thread_id];
    
    if (prev && prev.direction !== signal.direction) {
      // This is a reply - compute response time
      const responseMinutes = Math.round(
        (signal.email_date - prev.email_date) / (1000 * 60)
      );
      
      await db.query(
        'UPDATE email_signals SET response_time_minutes = $1 WHERE id = $2',
        [responseMinutes, signal.id]
      );
    }
    
    prevByThread[signal.thread_id] = signal;
  }
}

module.exports = {
  // OAuth
  createOAuth2Client,
  getAuthUrl,
  getTokensFromCode,
  refreshAccessToken,
  getAuthenticatedClient,
  getUserEmail,
  SCOPES,
  
  // Gmail
  getGmailClient,
  listMessages,
  getMessage,
  parseMessage,
  extractEmail,
  buildCandidateQuery,
  getHistory,
  getCurrentHistoryId,
  
  // Calendar
  getCalendarClient,
  listEvents,
  parseEvent,
  
  // Contacts
  getContactsClient,
  listContacts,
  syncContacts,
  
  // Sync
  syncCandidateEmails,
  computeResponseTimes
};
