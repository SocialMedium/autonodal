require('dotenv').config();
const ezekia = require('./lib/ezekia');

ezekia.getProjectCandidates(131791, {
  page: 1,
  per_page: 5,
  fields: 'id,firstName,lastName,fullName,emails,meta.candidate,profile.positions,manager.researchNotes'
}).then(r => {
  const items = r.data || [];
  items.slice(0, 3).forEach((c, i) => {
    console.log('Candidate', i+1, '| id:', c.id, '| name:', c.fullName, '| notes:', c.manager?.researchNotes?.length || 0);
  });
}).catch(e => console.error('Error:', e.message));
