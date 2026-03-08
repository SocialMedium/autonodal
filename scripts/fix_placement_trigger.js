require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('localhost') ? false : { rejectUnauthorized: false }
});

async function fixTrigger() {
  console.log('🔧 Fixing placement trigger to handle optional person_id...\n');
  
  try {
    await pool.query(`
      CREATE OR REPLACE FUNCTION create_placement_team_proximity()
      RETURNS TRIGGER AS $$
      BEGIN
        -- Only create team_proximity if person_id is not null
        IF NEW.person_id IS NOT NULL THEN
          INSERT INTO team_proximity (
            person_id,
            team_member_id,
            relationship_type,
            relationship_strength,
            connected_date,
            source,
            last_interaction_date,
            metadata
          ) VALUES (
            NEW.person_id,
            NEW.placed_by_user_id,
            'past_placement',
            1.0,
            NEW.start_date,
            NEW.source,
            NEW.start_date,
            jsonb_build_object(
              'placement_id', NEW.id,
              'role_title', NEW.role_title,
              'placement_fee', NEW.placement_fee
            )
          )
          ON CONFLICT (person_id, team_member_id, relationship_type) DO UPDATE SET
            last_interaction_date = GREATEST(team_proximity.last_interaction_date, NEW.start_date),
            metadata = team_proximity.metadata || jsonb_build_object(
              'latest_placement_id', NEW.id,
              'latest_role_title', NEW.role_title,
              'latest_placement_fee', NEW.placement_fee
            );
        END IF;
        
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);
    
    console.log('✅ Trigger fixed!');
    console.log('\nNow placements work with or without candidate names.');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pool.end();
  }
}

fixTrigger();
