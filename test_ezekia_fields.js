require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    console.log('Position company data:\n');
    
    const response = await ezekia.getPeople({ page: 1, per_page: 1 });
    const person = response.data[0];
    
    if (person.profile?.positions?.[0]) {
      const pos = person.profile.positions[0];
      console.log('Position 1:');
      console.log('  Title:', pos.title);
      console.log('  Company:', JSON.stringify(pos.company, null, 2));
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
