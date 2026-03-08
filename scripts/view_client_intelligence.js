require('dotenv').config();
const queries = require('../lib/team_proximity_queries');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function viewIntelligence() {
  console.log('📊 CLIENT INTELLIGENCE REPORT');
  console.log('═'.repeat(70));
  console.log();
  
  const topClients = await queries.getTopClients(pool, 10);
  
  console.log('💰 TOP CLIENTS BY REVENUE:');
  console.log('─'.repeat(70));
  topClients.forEach((client, i) => {
    console.log(`${i + 1}. ${client.client_name}`);
    console.log(`   Revenue: $${client.total_revenue?.toLocaleString() || 0}`);
    console.log(`   Placements: ${client.total_placements}`);
    console.log(`   Last Placement: ${client.last_placement_date || 'N/A'}`);
    console.log(`   Tier: ${client.tier}`);
    console.log();
  });
  
  if (topClients.length > 0) {
    const topClient = topClients[0];
    console.log('🔍 DETAILED VIEW: ' + topClient.client_name);
    console.log('─'.repeat(70));
    
    const placements = await queries.getClientPlacements(pool, topClient.client_id);
    
    console.log(`  Found ${placements.length} placements:`);
    console.log();
    
    placements.forEach(pl => {
      console.log(`  ✅ ${pl.role_title}`);
      console.log(`     Fee: $${pl.placement_fee.toLocaleString()}`);
      console.log(`     Date: ${pl.start_date}`);
      console.log(`     Placed by: ${pl.placed_by_name}`);
      console.log();
    });
  }
  
  console.log('═'.repeat(70));
  
  await pool.end();
}

viewIntelligence();