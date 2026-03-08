require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    const projects = await ezekia.getProjects({ page: 1, per_page: 5 });
    
    console.log('Searching for candidates with notes...\n');
    
    for (const project of projects.data) {
      const candidates = await ezekia.getProjectCandidates(project.id, {
        page: 1,
        per_page: 10,
        fields: 'meta.candidate,manager.researchNotes'
      });
      
      for (const candidate of candidates.data || []) {
        if (candidate.manager?.researchNotes?.length > 0) {
          console.log('✅ Found candidate with notes!');
          console.log('Project:', project.name);
          console.log('Candidate:', candidate.fullName);
          console.log('Note count:', candidate.manager.researchNotes.length);
          console.log('\nSample note:');
          console.log(JSON.stringify(candidate.manager.researchNotes[0], null, 2));
          return;
        }
      }
    }
    
    console.log('No candidates with notes found in first 50 candidates');
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
