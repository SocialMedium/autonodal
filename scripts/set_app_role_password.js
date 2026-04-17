#!/usr/bin/env node
/**
 * Set password for autonodal_app database role.
 * Run via: railway run node scripts/set_app_role_password.js
 */
require('dotenv').config();
const { Pool } = require('pg');

const PASSWORD = process.env.APP_ROLE_PASSWORD || 'ALREADY_SET_IN_RAILWAY';

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  try {
    // Verify we're connected as superuser
    const { rows: [me] } = await pool.query('SELECT current_user, session_user');
    console.log('Connected as:', me.current_user);

    // Verify role exists
    const { rows: roles } = await pool.query(
      "SELECT rolname, rolbypassrls, rolsuper FROM pg_roles WHERE rolname = 'autonodal_app'"
    );
    if (roles.length === 0) {
      console.error('ERROR: autonodal_app role does not exist');
      process.exit(1);
    }
    console.log('Role found:', JSON.stringify(roles[0]));

    // Set password
    await pool.query(`ALTER ROLE autonodal_app WITH PASSWORD '${PASSWORD}'`);
    console.log('Password set successfully.');

    // Verify grants
    const { rows: grants } = await pool.query(`
      SELECT table_name, privilege_type
      FROM information_schema.role_table_grants
      WHERE grantee = 'autonodal_app'
      ORDER BY table_name
      LIMIT 5
    `);
    console.log('Table grants:', grants.length > 0 ? grants.length + ' tables' : 'NONE — may need GRANT');

    // Grant if needed
    if (grants.length === 0) {
      console.log('Granting permissions...');
      await pool.query('GRANT USAGE ON SCHEMA public TO autonodal_app');
      await pool.query('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO autonodal_app');
      await pool.query('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO autonodal_app');
      await pool.query('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO autonodal_app');
      await pool.query('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO autonodal_app');
      console.log('Permissions granted.');
    }

    // Output the connection string
    const url = new URL(process.env.DATABASE_URL);
    const appUrl = `postgresql://autonodal_app:${PASSWORD}@${url.hostname}:${url.port}${url.pathname}?sslmode=require`;
    console.log('\n══════════════════════════════════════════════');
    console.log('DATABASE_URL_APP:');
    console.log(appUrl);
    console.log('══════════════════════════════════════════════');
    console.log('\nSet in Railway:');
    console.log(`railway variables set DATABASE_URL_APP="${appUrl}"`);

  } catch (err) {
    console.error('FAILED:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
