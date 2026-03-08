require('dotenv').config();
const ezekia = require('./lib/ezekia');

// Test WITH the same fields the extraction script uses
ezekia.getProjectCandidates(131791, {
  page: 1,
  per_page: 5,
  fields: 'meta.candidate,profile.positions,manager.researchNotes'
}).then(r => {
  const items = r.data || [];
  console.log('WITH fields param - count:', items.length);
  
  items.slice(0, 3).forEach((c, i) => {
    console.log('\n--- Candidate', i+1, '---');
    console.log('id:', c.id);
    console.log('fullName:', c.fullName);
    console.log('keys:', Object.keys(c));
    console.log('has manager?', !!c.manager);
    console.log('researchNotes?', c.manager?.researchNotes?.length || 0);
  });
}).catch(e => console.error('WITH fields error:', e.message));

// Test WITHOUT fields for comparison
ezekia.getProjectCandidates(131791, {
  page: 1,
  per_page: 5
}).then(r => {
  const items = r.data || [];
  console.log('\n\nWITHOUT fields param - count:', items.length);
  
  items.slice(0, 3).forEach((c, i) => {
    console.log('\n--- Candidate', i+1, '---');
    console.log('id:', c.id);
    console.log('fullName:', c.fullName);
    console.log('keys:', Object.keys(c));
  });
}).catch(e => console.error('WITHOUT fields error:', e.message));
