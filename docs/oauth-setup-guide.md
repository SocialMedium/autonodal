# Google OAuth — Switch to External (Public)

## Step 1: OAuth Consent Screen

1. Go to: https://console.cloud.google.com
2. Select your project (the one used for Autonodal/MitchelLake OAuth)
3. Navigate to: **APIs & Services → OAuth consent screen**
4. User Type: Change from **"Internal"** to **"External"**
5. Click Save

## Step 2: Fill in App Information

| Field | Value |
|-------|-------|
| App name | Autonodal |
| User support email | privacy@autonodal.com |
| App logo | Upload when available |
| App domain | https://www.autonodal.com |
| Authorised domains | `autonodal.com` |
| Developer contact | jt@socialmedium.ai |

## Step 3: Privacy Policy + Terms URLs

| Field | URL |
|-------|-----|
| Privacy policy URL | https://www.autonodal.com/privacy |
| Terms of service URL | https://www.autonodal.com/terms |

## Step 4: Scopes

Current scopes (should already be configured):
```
openid
email
profile
https://www.googleapis.com/auth/gmail.readonly
https://www.googleapis.com/auth/drive.readonly
https://www.googleapis.com/auth/documents.readonly
https://www.googleapis.com/auth/spreadsheets.readonly
https://www.googleapis.com/auth/presentations.readonly
```

Note: Gmail and Drive scopes are "sensitive" — requires OAuth verification for Production.

## Step 5: Test Users (Testing mode)

Add test users while in Testing status:
- jt@socialmedium.ai
- kikanoyen@gmail.com
- [investor emails]
- [advisor emails]
- [early tester emails]

Up to 100 test users allowed in Testing mode.

## Step 6: Publishing Status

**For now: Leave as "Testing"**
- You and your listed test users can sign in
- No verification required yet
- Perfect for building and testing with first users

**When ready for broader rollout: Click "Publish App"**
- If sensitive scopes are requested, Google will require verification (1-4 weeks)
- You'll need to submit:
  - Privacy policy at stable URL
  - Homepage at stable URL
  - Data deletion page at stable URL
  - Demo video (~1 min) showing how Gmail data is used (show proximity scoring, NOT email content)

## Step 7: OAuth Verification Checklist (for when ready)

Required for Production with Gmail/Drive scopes:

- [ ] Privacy policy live at https://www.autonodal.com/privacy
- [ ] Homepage live at https://www.autonodal.com/home
- [ ] Data deletion page live at https://www.autonodal.com/data-deletion
- [ ] Terms of service live at https://www.autonodal.com/terms
- [ ] App description accurately describes scope usage
- [ ] Demo video (~1 min) showing how Gmail data is used for proximity scoring
- [ ] All test users confirmed working
- [ ] No policy violations in app behaviour

Submit at: OAuth verification request form in Console

## Env Vars Checklist (Railway)

Confirm these are set:
```
GOOGLE_CLIENT_ID=xxx
GOOGLE_CLIENT_SECRET=xxx
GOOGLE_REDIRECT_URL=https://www.autonodal.com/api/auth/google/callback
GOOGLE_GMAIL_REDIRECT_URL=https://www.autonodal.com/api/auth/gmail/callback
```

## Post-Setup Test

1. Open an incognito window
2. Go to https://www.autonodal.com/autonodal
3. Click "Login"
4. Sign in with a test user (e.g. kikanoyen@gmail.com)
5. Should land on onboarding wizard (not "Access Blocked" error)
6. Complete wizard → land on dashboard with correct initials
