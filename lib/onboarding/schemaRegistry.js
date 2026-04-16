// ═══════════════════════════════════════════════════════════════════════════
// lib/onboarding/schemaRegistry.js — Canonical Autonodal Target Schema
// ═══════════════════════════════════════════════════════════════════════════
//
// Authoritative reference for field mapping inference. Defines the target
// fields, their aliases, value patterns, and confidence thresholds.

const PEOPLE_FIELDS = [
  {
    field: 'name',
    label: 'Full Name',
    required: true,
    aliases: ['full name', 'contact name', 'person', 'display name', 'contact', 'fullname', 'name'],
    valuePatterns: ['two_words', 'mixed_case_name'],
    examples: ['Alice Chen', "James O'Brien"],
  },
  {
    field: 'first_name',
    label: 'First Name',
    required: false,
    aliases: ['first', 'firstname', 'first name', 'given name', 'forename'],
    examples: ['Alice', 'James'],
  },
  {
    field: 'last_name',
    label: 'Last Name',
    required: false,
    aliases: ['last', 'lastname', 'last name', 'surname', 'family name'],
    examples: ['Chen', "O'Brien"],
  },
  {
    field: 'email',
    label: 'Email Address',
    required: true,
    aliases: ['email', 'email address', 'work email', 'primary email', 'e-mail', 'mail'],
    valuePatterns: ['email_format'],
    examples: ['alice@company.com'],
  },
  {
    field: 'current_employer',
    label: 'Current Employer',
    required: false,
    aliases: ['company', 'organisation', 'organization', 'employer', 'firm',
              'current company', 'current org', 'where they work', 'account',
              'company name', 'associated company', 'companyname'],
    examples: ['Atlassian', 'MitchelLake'],
  },
  {
    field: 'title',
    label: 'Job Title',
    required: false,
    aliases: ['title', 'job title', 'position', 'role', 'designation',
              'current title', 'current role', 'jobtitle', 'job role'],
    valuePatterns: ['job_title'],
    examples: ['CEO', 'VP Engineering', 'Managing Director'],
  },
  {
    field: 'linkedin_url',
    label: 'LinkedIn URL',
    required: false,
    aliases: ['linkedin', 'linkedin url', 'linkedin profile', 'li url',
              'linkedin link', 'li profile', 'hs linkedin url', 'linkedinurl'],
    valuePatterns: ['linkedin_url'],
    examples: ['https://linkedin.com/in/alicechen'],
  },
  {
    field: 'location',
    label: 'Location',
    required: false,
    aliases: ['location', 'city', 'country', 'geography', 'based in',
              'region', 'office', 'address', 'city state'],
    examples: ['Singapore', 'London, UK', 'Sydney, Australia'],
  },
  {
    field: 'phone',
    label: 'Phone',
    required: false,
    aliases: ['phone', 'mobile', 'cell', 'telephone', 'phone number',
              'contact number', 'mobilephone', 'work phone'],
    valuePatterns: ['phone_format'],
    examples: ['+65 9123 4567'],
  },
  {
    field: 'notes',
    label: 'Notes',
    required: false,
    aliases: ['notes', 'comments', 'description', 'bio', 'about',
              'background', 'summary', 'memo', 'internal notes'],
    examples: ['Met at Web Summit. Intro via Craig.'],
  },
  {
    field: 'tags',
    label: 'Tags',
    required: false,
    aliases: ['tags', 'labels', 'categories', 'type', 'segment',
              'contact type', 'lead status', 'lifecycle stage'],
    examples: ['investor, VC, fintech'],
  },
];

const COMPANY_FIELDS = [
  {
    field: 'name',
    label: 'Company Name',
    required: true,
    aliases: ['company', 'company name', 'organisation', 'organization',
              'account name', 'firm name', 'name', 'business name'],
    examples: ['Atlassian', 'Sequoia Capital'],
  },
  {
    field: 'domain',
    label: 'Domain',
    required: false,
    aliases: ['domain', 'website', 'url', 'web', 'site', 'company domain',
              'website url', 'homepage'],
    valuePatterns: ['domain_format'],
    examples: ['atlassian.com'],
  },
  {
    field: 'sector',
    label: 'Sector / Industry',
    required: false,
    aliases: ['industry', 'sector', 'vertical', 'space', 'category',
              'type', 'business type', 'market'],
    examples: ['FinTech', 'SaaS', 'Professional Services'],
  },
  {
    field: 'country',
    label: 'Country',
    required: false,
    aliases: ['country', 'location', 'headquarters', 'hq', 'based in',
              'region', 'geography', 'country region'],
    examples: ['Australia', 'Singapore', 'United Kingdom'],
  },
  {
    field: 'size',
    label: 'Employee Count',
    required: false,
    aliases: ['size', 'employees', 'headcount', 'staff', 'team size',
              'number of employees', 'company size', 'numberofemployees'],
    examples: ['250', '1000-5000', 'Enterprise'],
  },
  {
    field: 'linkedin_url',
    label: 'LinkedIn URL',
    required: false,
    aliases: ['linkedin', 'linkedin url', 'company linkedin', 'li page',
              'linkedin company url'],
    valuePatterns: ['linkedin_company_url'],
    examples: ['https://linkedin.com/company/atlassian'],
  },
];

// Confidence thresholds
const THRESHOLDS = {
  AUTO_APPLY: 0.85,
  HIGH: 0.70,
  MEDIUM: 0.50,
  UNCERTAIN: 0.0,
};

function confidenceLabel(score) {
  if (score >= THRESHOLDS.AUTO_APPLY) return 'certain';
  if (score >= THRESHOLDS.HIGH) return 'high';
  if (score >= THRESHOLDS.MEDIUM) return 'medium';
  return 'uncertain';
}

module.exports = { PEOPLE_FIELDS, COMPANY_FIELDS, THRESHOLDS, confidenceLabel };
