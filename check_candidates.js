require('dotenv').config();
const ezekia = require('./lib/ezekia');

ezekia.getProjectCandidates(131791, { per_page: 100 }).then(r => {
  const items = r.data || [];
  console.log('Total candidates:', items.length);
  
  const withId = items.filter(c => c.id);
  const withoutId = items.filter(c => c.id === undefined || c.id === null);
  
  console.log('With ID:', withId.length);
  console.log('Without ID:', withoutId.length);
  
  if (withoutId.length > 0) {
    console.log('\n=== CANDIDATES WITHOUT ID ===');
    withoutId.slice(0, 3).forEach((c, i) => {
      console.log('\nCandidate', i+1, '- keys:', Object.keys(c));
      console.log(JSON.stringify(c, null, 2).substring(0, 800));
    });
  }
  
  console.log('\n=== ID VALUES (first 10) ===');
  items.slice(0, 10).forEach((c, i) => {
    console.log(i, '| id:', c.id, '| type:', typeof c.id, '| fullName:', c.fullName);
  });
}).catch(e => console.error('Error:', e.message));
