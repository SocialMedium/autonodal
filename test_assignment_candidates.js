require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    // Get a project
    const projects = await ezekia.getProjects({ page: 1, per_page: 1 });
    const projectId = projects.data[0].id;
    
    console.log('Project:', projects.data[0].name);
    console.log('Project ID:', projectId);
    console.log('\nFetching candidates for this project...\n');
    
    // Get candidates - pass projectId as first arg
    const candidates = await ezekia.getProjectCandidates(projectId, {
      page: 1,
      per_page: 3
    });
    
    console.log('Total candidates:', candidates.meta?.total);
    
    if (candidates.data?.[0]) {
      console.log('\nSample candidate structure:');
      console.log(JSON.stringify(candidates.data[0], null, 2));
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
