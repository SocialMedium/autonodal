require('dotenv').config();
const axios = require('axios');

(async () => {
  try {
    const projectId = 131791;
    
    console.log('Fetching with ALL relationship fields...\n');
    
    const response = await axios.get(`https://ezekia.com/api/v4/projects/${projectId}`, {
      headers: {
        'Authorization': `Bearer ${process.env.EZEKIA_API_TOKEN}`,
        'Accept': 'application/json'
      },
      params: {
        'fields[]': [
          'relationships.contacts',
          'relationships.billing',
          'manager.researchNotes',
          'manager.tasks',
          'manager.meetings'
        ]
      }
    });
    
    const project = response.data.data || response.data;
    
    console.log('Project:', project.name);
    console.log('\nTop-level keys:', Object.keys(project));
    
    if (project.relationships) {
      console.log('\nRelationships keys:', Object.keys(project.relationships));
      console.log('\nRelationships:');
      console.log(JSON.stringify(project.relationships, null, 2));
    }
    
    if (project.manager) {
      console.log('\nManager keys:', Object.keys(project.manager));
      console.log('\nManager:');
      console.log(JSON.stringify(project.manager, null, 2));
    }
    
    if (project.billing) {
      console.log('\n✅ Billing data found!');
      console.log(JSON.stringify(project.billing, null, 2));
    }
    
    if (project.contacts) {
      console.log('\n✅ Contacts found!');
      console.log(JSON.stringify(project.contacts, null, 2));
    }
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
})();
