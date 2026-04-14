// ═══════════════════════════════════════════════════════════════════════════════
// lib/official_api_sources.js — Official API source configs + parsers
// Each source: fetch URL builder, response parser, signal mapper
// Deterministic: no LLM, structured fields only
// ═══════════════════════════════════════════════════════════════════════════════

const crypto = require('crypto');

function md5(str) {
  return crypto.createHash('md5').update(str).digest('hex');
}

function sha256(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function truncate(str, len) {
  if (!str) return '';
  return str.length > len ? str.slice(0, len - 1) + '…' : str;
}

function isoDate(d) {
  if (!d) return null;
  try { return new Date(d).toISOString(); } catch { return null; }
}

// ═══════════════════════════════════════════════════════════════════════════════
// UK FIND A TENDER — OCDS procurement notices + awards
// ═══════════════════════════════════════════════════════════════════════════════

const UK_FIND_A_TENDER = {
  sourceKey: 'uk_find_a_tender',
  sourceType: 'procurement_uk',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // OCDS release packages — requires ISO 8601 datetime without milliseconds
    const raw = watermark?.last_published || new Date(Date.now() - 7 * 86400000).toISOString();
    const since = raw.includes('T') ? raw.replace(/\.\d{3}Z$/, 'Z') : raw + 'T00:00:00Z';
    return `https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages?updatedFrom=${since}&limit=100`;
  },

  buildNextUrl(currentUrl, response) {
    return response?.links?.next || null;
  },

  parseResponse(data) {
    const releases = data?.releases || [];
    return releases.map(r => {
      const tender = r.tender || {};
      const awards = r.awards || [];
      const buyer = r.buyer || {};
      const parties = r.parties || [];
      const suppliers = awards.flatMap(a => (a.suppliers || []).map(s => s.name)).filter(Boolean);

      const isAward = awards.length > 0 && awards.some(a => a.status === 'active');
      const value = isAward
        ? awards.find(a => a.value)?.value
        : tender.value;

      return {
        id: r.ocid || r.id,
        title: tender.title || (isAward ? `Contract awarded: ${tender.description || ''}` : ''),
        description: tender.description || '',
        published_at: r.date || r.publishedDate,
        buyer_name: buyer.name || '',
        buyer_id: buyer.id || '',
        suppliers,
        value_amount: value?.amount || null,
        value_currency: value?.currency || 'GBP',
        procurement_method: tender.procurementMethod || '',
        status: isAward ? 'awarded' : (tender.status || 'planning'),
        categories: (tender.items || []).map(i => i.classification?.description).filter(Boolean),
        source_url: `https://www.find-tender.service.gov.uk/Notice/${r.ocid || r.id}`,
      };
    }).filter(r => r.title);
  },

  toDocument(item) {
    const content = [
      `Procurement: ${item.status === 'awarded' ? 'Contract Award' : 'Notice'}`,
      `Buyer: ${item.buyer_name}`,
      item.suppliers.length ? `Supplier(s): ${item.suppliers.join(', ')}` : '',
      item.value_amount ? `Value: ${item.value_currency} ${item.value_amount.toLocaleString()}` : '',
      `Method: ${item.procurement_method}`,
      item.categories.length ? `Categories: ${item.categories.slice(0, 3).join(', ')}` : '',
      item.description ? `Description: ${truncate(item.description, 500)}` : '',
    ].filter(Boolean).join('\n');

    return {
      source_type: 'procurement_uk',
      source_name: 'Find a Tender (UK)',
      source_url: item.source_url,
      source_url_hash: sha256(item.id || item.source_url),
      title: truncate(`${item.status === 'awarded' ? 'Award' : 'Notice'}: ${item.title}`, 500),
      content,
      author: item.buyer_name,
      published_at: isoDate(item.published_at),
    };
  },

  toSignal(item, docId, companyId) {
    // Only create signals for awards (actual contract wins)
    if (item.status !== 'awarded' || !item.suppliers.length) return null;

    const isLarge = item.value_amount && item.value_amount > 1000000;
    return {
      signal_type: 'partnership', // Government contract = commercial partnership signal
      company_name: item.suppliers[0],
      confidence_score: isLarge ? 0.88 : 0.75,
      evidence_summary: `${item.suppliers[0]} awarded UK government contract: "${truncate(item.title, 100)}"` +
        (item.value_amount ? ` (${item.value_currency} ${(item.value_amount / 1000000).toFixed(1)}M)` : '') +
        ` — buyer: ${item.buyer_name}`,
      source_url: item.source_url,
      signal_date: isoDate(item.published_at),
      scoring_breakdown: {
        source: 'uk_find_a_tender',
        buyer: item.buyer_name,
        value: item.value_amount,
        currency: item.value_currency,
        procurement_method: item.procurement_method,
        categories: item.categories.slice(0, 5),
      },
    };
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// AUSTENDER — Australian Government Procurement (OCDS)
// ═══════════════════════════════════════════════════════════════════════════════

const AU_AUSTENDER = {
  sourceKey: 'au_austender',
  sourceType: 'procurement_au',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    const since = watermark?.last_published || new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    const until = new Date().toISOString().split('T')[0];
    return `https://api.tenders.gov.au/ocds/findByDates/contractPublished/${since}T00:00:00Z/${until}T23:59:59Z`;
  },

  buildNextUrl(currentUrl, response) {
    return response?.links?.next || null;
  },

  parseResponse(data) {
    const releases = data?.releases || [];
    return releases.map(r => {
      const contracts = r.contracts || [];
      const awards = r.awards || [];
      const parties = r.parties || [];
      const buyer = parties.find(p => p.roles?.includes('procuringEntity') || p.roles?.includes('buyer'));
      const suppliers = awards.flatMap(a => (a.suppliers || []).map(s => s.name)).filter(Boolean);
      // AusTender puts value/title/description in contracts, not tender
      const contract = contracts[0] || {};
      const value = contract.value;
      const title = contract.description || contract.title || '';
      const valueAmount = value?.amount ? parseFloat(value.amount) : null;

      return {
        id: r.ocid || r.id,
        title,
        description: title,
        published_at: r.date,
        buyer_name: buyer?.name || '',
        suppliers,
        value_amount: valueAmount,
        value_currency: value?.currency || 'AUD',
        status: awards.length > 0 ? 'awarded' : 'planning',
        contract_id: contract.id || '',
        source_url: `https://www.tenders.gov.au/Cn/Show/${contract.id || r.ocid || ''}`,
      };
    }).filter(r => r.title || r.suppliers.length);
  },

  buildNextUrl(currentUrl, response) {
    return response?.links?.next || null;
  },

  toDocument(item) {
    const valueStr = item.value_amount ? `${item.value_currency} ${item.value_amount.toLocaleString()}` : '';
    const content = [
      `Australian Government Contract Award`,
      `Buyer: ${item.buyer_name}`,
      item.suppliers.length ? `Supplier(s): ${item.suppliers.join(', ')}` : '',
      valueStr ? `Value: ${valueStr}` : '',
      item.description ? `Description: ${truncate(item.description, 500)}` : '',
    ].filter(Boolean).join('\n');

    return {
      source_type: 'procurement_au',
      source_name: 'AusTender',
      source_url: item.source_url,
      source_url_hash: sha256(item.id || item.source_url),
      title: truncate(`AU Contract: ${item.title} — ${item.suppliers[0] || item.buyer_name}`, 500),
      content,
      author: item.buyer_name,
      published_at: isoDate(item.published_at),
    };
  },

  toSignal(item, docId, companyId) {
    if (!item.suppliers.length) return null;
    const isLarge = item.value_amount && item.value_amount > 1000000;
    return {
      signal_type: 'partnership',
      company_name: item.suppliers[0],
      confidence_score: isLarge ? 0.88 : 0.72,
      evidence_summary: `${item.suppliers[0]} awarded AU government contract: "${truncate(item.title, 100)}"` +
        (item.value_amount ? ` (AUD ${(item.value_amount / 1000).toFixed(0)}K)` : '') +
        ` — buyer: ${item.buyer_name}`,
      source_url: item.source_url,
      signal_date: isoDate(item.published_at),
      scoring_breakdown: {
        source: 'au_austender',
        buyer: item.buyer_name,
        value: item.value_amount,
        currency: item.value_currency,
        contract_id: item.contract_id,
      },
    };
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// UK COMPANIES HOUSE — Insolvency, director changes, charges
// ═══════════════════════════════════════════════════════════════════════════════

const UK_COMPANIES_HOUSE = {
  sourceKey: 'uk_companies_house',
  sourceType: 'filing_uk',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // Search recent filing events
    const since = watermark?.last_published || new Date(Date.now() - 3 * 86400000).toISOString().split('T')[0];
    // Filing history search — insolvency and officer changes
    return `https://api.company-information.service.gov.uk/search/disqualified-officers?q=*&items_per_page=100`;
  },

  getAuthHeaders() {
    const key = process.env.COMPANIES_HOUSE_KEY;
    if (!key) return null;
    return { 'Authorization': 'Basic ' + Buffer.from(key + ':').toString('base64') };
  },

  parseResponse(data) {
    const items = data?.items || [];
    return items.map(item => ({
      id: item.links?.self || `ch-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      title: `Officer disqualification: ${item.title || 'Unknown'}`,
      description: item.description || '',
      published_at: item.disqualified_from || item.date_of_birth,
      company_name: item.title || '',
      officer_name: item.title || '',
      source_url: item.links?.self ? `https://find-and-update.company-information.service.gov.uk${item.links.self}` : '',
    })).filter(r => r.title);
  },

  toDocument(item) {
    return {
      source_type: 'filing_uk',
      source_name: 'Companies House (UK)',
      source_url: item.source_url,
      source_url_hash: sha256(item.id || item.source_url),
      title: truncate(item.title, 500),
      content: item.description || item.title,
      author: 'Companies House',
      published_at: isoDate(item.published_at),
    };
  },

  toSignal(item) {
    return {
      signal_type: 'restructuring',
      company_name: item.company_name,
      confidence_score: 0.90,
      evidence_summary: item.title,
      source_url: item.source_url,
      signal_date: isoDate(item.published_at),
      scoring_breakdown: { source: 'uk_companies_house' },
    };
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// USPTO PATENTSVIEW — Patent grants (assignee = company signal)
// ═══════════════════════════════════════════════════════════════════════════════

const US_PATENTSVIEW = {
  sourceKey: 'us_patentsview',
  sourceType: 'patent_us',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // PatentsView API — requires X-Api-Key (free registration)
    const since = watermark?.last_published || new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
    return `https://search.patentsview.org/api/v1/patent/?q={"_gte":{"patent_date":"${since}"}}&f=["patent_id","patent_title","patent_date","patent_abstract","assignees.assignee_organization","assignees.assignee_country","inventors.inventor_first_name","inventors.inventor_last_name"]&o={"size":100}&s=[{"patent_date":"desc"}]`;
  },

  getAuthHeaders() {
    const key = process.env.PATENTSVIEW_API_KEY;
    if (!key) return null;
    return { 'X-Api-Key': key };
  },

  buildNextUrl(currentUrl, response) {
    if (!response?.patents || response.patents.length < 100) return null;
    // PatentsView doesn't support cursor pagination easily — stop at first page
    return null;
  },

  parseResponse(data) {
    const patents = data?.patents || [];
    return patents.map(p => {
      const assignees = (p.assignees || []).map(a => a.assignee_organization).filter(Boolean);
      const inventors = (p.inventors || []).map(i => `${i.inventor_first_name} ${i.inventor_last_name}`).filter(Boolean);

      return {
        id: p.patent_id,
        title: p.patent_title || '',
        description: p.patent_abstract || '',
        published_at: p.patent_date,
        assignee_names: assignees,
        inventor_names: inventors,
        source_url: `https://patents.google.com/patent/US${p.patent_id}`,
      };
    }).filter(r => r.title && r.assignee_names.length > 0);
  },

  toDocument(item) {
    const content = [
      `US Patent Grant: ${item.id}`,
      `Assignee(s): ${item.assignee_names.join(', ')}`,
      `Inventor(s): ${item.inventor_names.slice(0, 5).join(', ')}`,
      item.description ? `Abstract: ${truncate(item.description, 800)}` : '',
    ].filter(Boolean).join('\n');

    return {
      source_type: 'patent_us',
      source_name: 'USPTO PatentsView',
      source_url: item.source_url,
      source_url_hash: sha256(`patent-${item.id}`),
      title: truncate(`Patent: ${item.title} — ${item.assignee_names[0]}`, 500),
      content,
      author: item.assignee_names[0],
      published_at: isoDate(item.published_at),
    };
  },

  toSignal(item) {
    // Only signal for patents — product_launch is the closest signal type
    return {
      signal_type: 'product_launch',
      company_name: item.assignee_names[0],
      confidence_score: 0.65,
      evidence_summary: `${item.assignee_names[0]} granted US patent: "${truncate(item.title, 120)}"` +
        (item.inventor_names.length ? ` — inventors: ${item.inventor_names.slice(0, 3).join(', ')}` : ''),
      source_url: item.source_url,
      signal_date: isoDate(item.published_at),
      scoring_breakdown: {
        source: 'us_patentsview',
        patent_id: item.id,
        assignees: item.assignee_names,
        inventor_count: item.inventor_names.length,
      },
    };
  },
};

// ═══════════════════════════════════════════════════════════════════════════════
// UK ONS — Economic releases (GDP, labour market, CPI)
// ═══════════════════════════════════════════════════════════════════════════════

const UK_ONS = {
  sourceKey: 'uk_ons',
  sourceType: 'statistics_uk',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // ONS search API — bulletins = statistical releases
    return 'https://api.beta.ons.gov.uk/v1/search?content_type=bulletin&limit=50&sort=release_date';
  },

  parseResponse(data) {
    const items = data?.items || [];
    return items.map(item => ({
      id: item.uri || item.links?.self,
      title: item.title || item.description?.summary || '',
      description: item.description?.summary || item.meta_description || item.summary || '',
      published_at: item.release_date,
      category: (item.topics || []).join(', ') || 'economics',
      source_url: item.uri
        ? `https://www.ons.gov.uk${item.uri}`
        : `https://www.ons.gov.uk/search?q=${encodeURIComponent(item.title || '')}`,
    })).filter(r => r.title);
  },

  toDocument(item) {
    return {
      source_type: 'statistics_uk',
      source_name: 'UK ONS',
      source_url: item.source_url,
      source_url_hash: sha256(item.id || item.source_url),
      title: truncate(`ONS: ${item.title}`, 500),
      content: item.description || item.title,
      author: 'Office for National Statistics',
      published_at: isoDate(item.published_at),
    };
  },

  // Statistics releases are context documents — no direct company signals
  toSignal() { return null; },
};

// ═══════════════════════════════════════════════════════════════════════════════
// AUSTRALIAN BUREAU OF STATISTICS — Economic indicators
// ═══════════════════════════════════════════════════════════════════════════════

const AU_ABS = {
  sourceKey: 'au_abs',
  sourceType: 'statistics_au',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // ABS — use their indicator API for latest releases
    return 'https://indicator.data.abs.gov.au/dataflows';
  },

  parseResponse(data) {
    // Indicator API returns JSON with dataflows
    const dataflows = data?.data?.dataflows || data?.dataflows || (Array.isArray(data) ? data : []);
    return dataflows.slice(0, 50).map(df => ({
      id: df.id || df.agencyId || '',
      title: typeof df.name === 'string' ? df.name : (df.name?.en || df.name?.default || ''),
      description: typeof df.description === 'string' ? df.description : (df.description?.en || ''),
      published_at: df.validFrom || new Date().toISOString(),
      category: df.id || '',
      source_url: `https://www.abs.gov.au/statistics`,
    })).filter(r => r.title);
  },

  toDocument(item) {
    return {
      source_type: 'statistics_au',
      source_name: 'Australian Bureau of Statistics',
      source_url: item.source_url,
      source_url_hash: sha256(`abs-${item.id}`),
      title: truncate(`ABS: ${item.title}`, 500),
      content: item.description || item.title,
      author: 'Australian Bureau of Statistics',
      published_at: isoDate(item.published_at),
    };
  },

  toSignal() { return null; },
};

// ═══════════════════════════════════════════════════════════════════════════════
// SINGSTAT — Singapore economic indicators
// ═══════════════════════════════════════════════════════════════════════════════

const SG_SINGSTAT = {
  sourceKey: 'sg_singstat',
  sourceType: 'statistics_sg',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // SingStat resource ID list — GDP, employment, trade
    // Key tables: M015811 (GDP), M182701 (Labour Force), M451001 (Trade)
    return 'https://tablebuilder.singstat.gov.sg/api/table/resourceid?keyword=gdp&searchOption=all';
  },

  parseResponse(data) {
    const records = data?.Data?.records || data?.records || [];
    return records.slice(0, 30).map(r => ({
      id: r.resourceId || r.id,
      title: r.title || r.resourceName || '',
      description: r.description || r.title || '',
      published_at: r.lastUpdated || new Date().toISOString(),
      source_url: `https://tablebuilder.singstat.gov.sg/table/${r.resourceId || r.id}`,
    })).filter(r => r.title);
  },

  toDocument(item) {
    return {
      source_type: 'statistics_sg',
      source_name: 'SingStat',
      source_url: item.source_url,
      source_url_hash: sha256(`singstat-${item.id}`),
      title: truncate(`SingStat: ${item.title}`, 500),
      content: item.description || item.title,
      author: 'Department of Statistics Singapore',
      published_at: isoDate(item.published_at),
    };
  },

  toSignal() { return null; },
};

// ═══════════════════════════════════════════════════════════════════════════════
// STATISTICS CANADA — Economic data tables
// ═══════════════════════════════════════════════════════════════════════════════

const CA_STATCAN = {
  sourceKey: 'ca_statcan',
  sourceType: 'statistics_ca',
  userAgent: 'MitchelLake Signal Intelligence/1.0',

  buildUrl(watermark) {
    // StatCan changed cube list — date in path, returns JSON array
    const since = watermark?.last_published || new Date(Date.now() - 14 * 86400000).toISOString().split('T')[0];
    return `https://www150.statcan.gc.ca/t1/wds/rest/getChangedCubeList/${since}`;
  },

  parseResponse(data) {
    // Returns { status: [...], object: [...] } or just array
    const cubes = Array.isArray(data) ? data : (data?.object || []);
    return cubes.slice(0, 30).map(c => ({
      id: String(c.productId || ''),
      title: c.cubeTitleEn || `Data cube ${c.productId}`,
      description: c.cubeTitleEn || '',
      published_at: c.releaseTime || new Date().toISOString(),
      source_url: `https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=${String(c.productId || '').padStart(8, '0')}`,
    })).filter(r => r.title && r.id);
  },

  toDocument(item) {
    return {
      source_type: 'statistics_ca',
      source_name: 'Statistics Canada',
      source_url: item.source_url,
      source_url_hash: sha256(`statcan-${item.id}`),
      title: truncate(`StatCan: ${item.title}`, 500),
      content: item.description || item.title,
      author: 'Statistics Canada',
      published_at: isoDate(item.published_at),
    };
  },

  toSignal() { return null; },
};

// ═══════════════════════════════════════════════════════════════════════════════
// REGISTRY — all sources by key
// ═══════════════════════════════════════════════════════════════════════════════

const SOURCES = {
  uk_find_a_tender: UK_FIND_A_TENDER,
  au_austender: AU_AUSTENDER,
  uk_companies_house: UK_COMPANIES_HOUSE,
  us_patentsview: US_PATENTSVIEW,
  uk_ons: UK_ONS,
  au_abs: AU_ABS,
  sg_singstat: SG_SINGSTAT,
  ca_statcan: CA_STATCAN,
};

module.exports = { SOURCES, md5, sha256 };
