require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    console.log('Checking assignment/project data for notes...\n');
    
    const projects = await ezekia.getProjects({ 
      page: 1, 
      per_page: 5
    });
    
    for (const project of projects.data) {
      console.log('Project:', project.name);
      console.log('Available keys:', Object.keys(project));
      
      if (project.manager) {
        console.log('Manager keys:', Object.keys(project.manager));
      }
      
      if (project.notes) {
        console.log('✅ HAS NOTES!');
        console.log(JSON.stringify(project.notes, null, 2));
      }
      
      if (project.description) {
        console.log('Description length:', project.description.length, 'chars');
      }
      
      console.log('---\n');
    }
    
    // Also try with fields parameter
    console.log('\nTrying with fields parameter...\n');
    const withFields = await ezekia.getProjects({
      page: 1,
      per_page: 1,
      fields: 'manager.researchNotes,manager.tasks,manager.meetings'
    });
    
    const proj = withFields.data[0];
    console.log('With fields - manager keys:', Object.keys(proj.manager || {}));
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
