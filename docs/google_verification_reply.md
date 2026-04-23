# Draft reply to Google OAuth verification team

**Send from:** Cloud Console owner Google account (JT)
**To:** The verification thread where Google requested fuller data-usage disclosure
**Subject:** Re: OAuth Verification — Privacy Policy Update Complete — Project mitchellake-signals

---

Hi Google verification team,

Thank you for the feedback. We have updated our privacy policy to fully document how Autonodal interacts with Google user data, in accordance with the Google API Services User Data Policy and the Limited Use requirements.

**New disclosure section:** https://www.autonodal.com/privacy.html#google-user-data

**Summary of changes:**

- Dedicated section (§9 of the policy) titled "How Autonodal Accesses, Uses, Stores, and Shares Google User Data" with the anchor `#google-user-data`.
- Each requested OAuth scope disclosed individually with a specific, literal description of what data it accesses and what we do with it.
- Explicit AI/ML training disclosure: Google user data is never used to develop, improve, or train any AI or machine-learning model, internal or external, and is never transmitted to any third-party AI model provider.
- Data storage disclosure: Railway (US) primary database, Qdrant Cloud (EU, GCP Frankfurt) for derived proximity scores only. OAuth tokens additionally encrypted at the application layer with AES-256-GCM before being written to the database.
- Row-level security enforcement documented for all user-derived data, including OAuth tokens.
- User-initiated disconnect: the user can disconnect the Google integration from the Autonodal settings page at any time; this immediately revokes access at Google, deletes the stored OAuth tokens, and schedules deletion of previously-retrieved Google user data within thirty (30) days.
- Processor list limited to what is strictly necessary and DPA-bound. Google user data is not transmitted to OpenAI or any other AI/ML service.
- Full Limited Use compliance statement mirroring the four bulleted requirements from the Google API Services User Data Policy.

**Scopes currently requested on the consent screen:**

- `openid`
- `https://www.googleapis.com/auth/userinfo.email`
- `https://www.googleapis.com/auth/userinfo.profile`
- `https://www.googleapis.com/auth/gmail.readonly`
- `https://www.googleapis.com/auth/contacts.readonly`
- `https://www.googleapis.com/auth/calendar.readonly`

Please note that Drive, Docs, Sheets, and Slides scopes (`drive.readonly`, `documents.readonly`, `spreadsheets.readonly`, `presentations.readonly`) have been removed from the consent screen and are not currently requested by the application. If we choose to re-introduce a Drive-backed feature in the future, we will submit a scope amendment through the standard verification process.

**Demonstration video:** [PASTE UNLISTED YOUTUBE URL HERE]

The video walks through the sign-in consent flow, the use of Gmail metadata / Contacts / Calendar scopes within the application, and the user-initiated disconnect flow that revokes Google access and schedules 30-day deletion of Google-derived data.

Please let us know if any additional detail is required to complete verification.

Best regards,
JT
Autonodal
