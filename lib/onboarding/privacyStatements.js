// ═══════════════════════════════════════════════════════════════════════════════
// lib/onboarding/privacyStatements.js — Privacy trust copy for every data moment
// ═══════════════════════════════════════════════════════════════════════════════

const PRIVACY_STATEMENTS = {

  opening: {
    headline: 'Your data. Your control. Always.',
    body: 'Your network stays in your private sandbox. ' +
          'It is never merged, sold, or used to train AI models. ' +
          'You lend your influence to collaborations — your data never moves.',
  },

  email_connect: {
    headline: 'We never read your messages.',
    body: 'Connecting Gmail maps your real relationship graph — ' +
          'who you talk to, how often, how recently. ' +
          'The content of every email you have ever written remains yours, always.',
    detail: [
      'We extract: who, when, frequency, reciprocity',
      'We never access: subject lines, message body, attachments',
      'Nothing leaves your session context',
    ],
  },

  messaging_connect: {
    headline: 'Your conversations stay private.',
    body: 'For messaging apps, extraction happens locally. ' +
          'Only mathematical proximity signals reach the platform — ' +
          'who, when, how often. Message content never leaves your device.',
    detail: [
      'Local extraction only — content never transmitted',
      'We extract: frequency, recency, reciprocity',
      'We never store: message content, group names, media',
    ],
  },

  document_upload: {
    headline: 'Your documents stay private.',
    body: 'Uploaded files are used only to build your personal profile. ' +
          'They are stored securely in your private sandbox ' +
          'and never shared or used for AI training.',
  },

  profile_confirm: {
    headline: 'This is your private profile.',
    body: 'Only you see these details. ' +
          'In any collaborative context, others see only ' +
          'your proximity scores — never this profile.',
  },

  huddle_join: {
    headline: 'You\'re lending influence — not data.',
    body: 'Huddle members will see who you know and how strong ' +
          'those relationships are. They will never see what ' +
          'you have said to anyone, or which platform any relationship lives on.',
    detail: [
      'Members see: that you know someone, relationship strength, recency',
      'Members never see: messages, platforms, interaction content',
      'Leave any time: their visibility ends immediately — not gradually',
      'Your sandbox: completely unchanged when you exit',
    ],
  },

  huddle_exit: {
    headline: 'Your relationships are yours again.',
    body: 'The huddle has lost visibility of your network. ' +
          'Not gradually — immediately. Your personal sandbox is unchanged.',
  },
};

module.exports = { PRIVACY_STATEMENTS };
