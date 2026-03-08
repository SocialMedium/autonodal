require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    console.log('Testing Ezekia Projects/Assignments...\n');
    
    // Test active projects
    const active = await ezekia.getProjects({ page: 1, per_page: 2 });
    console.log('Active projects total:', active.meta?.total);
    
    if (active.data?.[0]) {
      console.log('\nSample active project:');
      console.log(JSON.stringify(active.data[0], null, 2));
    }
    
    // Test if archived parameter exists
    console.log('\n--- Testing archived parameter ---');
    const all = await ezekia.getProjects({ page: 1, per_page: 1, archived: true });
    console.log('With archived=true total:', all.meta?.total);
    
  } catch (error) {
    console.error('Error:', error.message);
    console.error('Full error:', error);
  }
})();
