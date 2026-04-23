# Google OAuth verification — demo video script

**Target length:** 100–120 seconds
**Upload as:** Unlisted YouTube video (not Loom, not Drive)
**Narration:** Voice-over in one take, or on-screen captions — never silent

Record in a single pass at 1080p. Use a clean browser window with no personal tabs/extensions visible. Sign out of Google first so you hit the consent screen fresh.

---

## Scene 1 — Landing page (0:00 – 0:10)

**On screen:** Browser showing `https://www.autonodal.com/home.html`

**Narration:**

> Autonodal is a professional network intelligence platform. It helps consultants and executives understand who in their team has the warmest path to any given contact by building a proximity graph from their own email, calendar, and contacts metadata.

---

## Scene 2 — Sign in with Google (0:10 – 0:30)

**On screen:**
1. Click "Sign in with Google" on the landing page
2. OAuth consent screen appears — **pause for 3 full seconds** so reviewers can read the app name "Autonodal" and the full scope list
3. Click "Allow"
4. App dashboard loads

**Narration:**

> Sign-in uses Google OAuth. Autonodal requests the user's basic profile, Gmail read access, contacts, and calendar. Each of these is explained on the consent screen. The user reviews the scopes and grants access. Authentication lands the user directly in their own private dashboard.

**Critical check:** the consent screen must show "Autonodal" prominently (not the project ID). If it shows anything else, fix the branding under Google Auth Platform → Branding before recording.

---

## Scene 3 — Gmail scope in action (0:30 – 0:55)

**On screen:**
1. Navigate to the proximity graph view (or the person dossier)
2. Show a person record with their interaction timeline visible
3. Highlight with mouse pointer: number of email interactions, last-contact date, reciprocity ratio
4. **Explicitly show that message bodies are NOT shown** — e.g., hover over a row that shows "Email — From: x@co.com, To: y@co.com, Date: 2026-04-15" with no subject or body content

**Narration:**

> Gmail read access is used only to compute relationship proximity. Autonodal reads email headers — sender, recipient, timestamp, subject line, and Gmail's own short snippet — to score how frequently and recently the user has been in contact with each person. Message bodies, attachments, and message parts are never read or stored. On this person record, you can see the email count, last contact date, and reciprocity ratio — all derived from metadata.

---

## Scene 4 — Contacts scope in action (0:55 – 1:10)

**On screen:**
1. Navigate to the people list (`/people.html`)
2. Show the populated list with names, titles, companies
3. Highlight one contact that was sourced from Google Contacts (hover over the card)

**Narration:**

> Google Contacts is used to seed the user's professional network. Contact name, email, title, and company are imported into the user's own private sandbox. Contacts are never shared across tenants or with other Autonodal users.

---

## Scene 5 — Calendar scope in action (1:10 – 1:25)

**On screen:**
1. Navigate to a person dossier (`/person.html?id=<someone>`)
2. Scroll to the interaction timeline
3. Point to a "meeting" interaction — showing title, date, duration
4. Highlight that no event description or meeting notes are visible

**Narration:**

> Google Calendar is read for meeting titles, attendee email addresses, and start and end timestamps. These are stored as meeting interactions and contribute to the user's proximity scoring. Event descriptions, meeting notes, and attached documents are never read.

---

## Scene 6 — Disconnect flow (1:25 – 1:45)

**On screen:**
1. Navigate to `/profile.html` (or wherever the disconnect button lives)
2. Click "Disconnect Google" button
3. Confirmation message: "Google access revoked. Data will be deleted within 30 days."
4. Refresh — demonstrate that Google-derived data is no longer refreshing

**Narration:**

> Users can disconnect their Google account at any time from the Autonodal settings page. Disconnection immediately revokes Autonodal's access tokens at Google and schedules deletion of all Google-derived data within thirty days. Users can also revoke access directly in their Google account settings, and can request full deletion by emailing privacy@autonodal.com.

---

## Scene 7 — Close (1:45 – 1:55)

**On screen:** Return to `/home.html` or a branding shot. Can fade to a title card with:

> Autonodal · Limited Use Compliance
>
> privacy@autonodal.com · autonodal.com/privacy.html#google-user-data

**Narration:**

> Google user data is stored within the user's own tenant sandbox, protected by row-level security, with OAuth tokens encrypted at rest. Google user data is never used to train AI or machine-learning models, and is never transmitted to OpenAI or any other third-party AI service. Full disclosure is at autonodal.com slash privacy.

---

## Recording tips

- **Sign out of Google first.** Clear cookies for `accounts.google.com`. Otherwise the OAuth screen auto-advances and reviewers can't see consent.
- **Use a fresh incognito window** with only Autonodal bookmarks visible.
- **Hide personal tabs, extensions, password managers.** They end up on the reviewer's screen too.
- **Show the full URL bar** on each page — reviewers check the domain (`autonodal.com`) matches the registered Cloud Console URL.
- **Mouse cursor must be visible** and must clearly click each interactive element.
- **Audio quality:** record in a quiet room, use a decent mic, keep levels even. If you can't narrate live, add captions with QuickTime or Descript — but never silent.
- **Don't speed up.** Reviewers need to read on-screen text. Natural pace.
- **Before uploading:** watch it back once end-to-end. Check that (a) consent screen shows "Autonodal", (b) no other user's data accidentally appears, (c) no browser notifications fire mid-record.

## Upload and submit

1. Upload to YouTube as **Unlisted** (not Private, not Public).
2. Title: "Autonodal — Google OAuth Verification Demo"
3. Description: "Demonstration video for Google OAuth verification submission. Autonodal project mitchellake-signals."
4. Copy the YouTube URL.
5. Cloud Console → Verification Center → paste URL in the "Demo video" field of the submission.
6. Include the URL in the reply email to the verification team as well (see [google_verification_reply.md](./google_verification_reply.md)).

## If the scope you demonstrate doesn't match what's registered

Automatic re-rejection. Before recording, open a second tab with Data Access open; the scope list on the consent screen (Scene 2) should match that Data Access list exactly. After recording, do a final sanity check that the video shows the same three restricted/sensitive scopes configured.

---

*End of script.*
