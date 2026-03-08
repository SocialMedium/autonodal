require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    console.log('Examining project relationships...\n');
    
    const projects = await ezekia.getProjects({ page: 1, per_page: 1 });
    const project = projects.data[0];
    
    console.log('Project:', project.name);
    console.log('\nRelationships object:');
    console.log(JSON.stringify(project.relationships, null, 2));
    
    // Try getting the full project detail from single endpoint
    console.log('\n\nFetching single project for more detail...\n');
    
    const axios = require('axios');
    const response = await axios.get(`https://ezekia.com/api/v3/projects/${project.id}`, {
      headers: {
        'Authorization': `Bearer ${process.env.EZEKIA_API_TOKEN}`,
        'Accept': 'application/json'
      }
    });
    
    const fullProject = response.data.data;
    console.log('Full project keys:', Object.keys(fullProject));
    
    if (fullProject.relationships) {
      console.log('\nRelationships keys:', Object.keys(fullProject.relationships));
      console.log('\nFull relationships:');
      console.log(JSON.stringify(fullProject.relationships, null, 2));
    }
    
    if (fullProject.manager) {
      console.log('\nManager keys:', Object.keys(fullProject.manager));
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
