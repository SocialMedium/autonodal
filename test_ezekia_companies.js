require('dotenv').config();
const ezekia = require('./lib/ezekia');

(async () => {
  try {
    console.log('Testing Ezekia Companies endpoint...\n');
    
    const response = await ezekia.getCompanies({ page: 1, per_page: 5 });
    
    console.log('Total companies:', response.meta?.total);
    console.log('\nSample company structure:');
    
    if (response.data?.[0]) {
      const company = response.data[0];
      console.log(JSON.stringify(company, null, 2));
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
})();
