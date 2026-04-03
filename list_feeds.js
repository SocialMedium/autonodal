require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
pool.query(`
  SELECT name, url, source_type, enabled, last_error, signal_types
  FROM rss_sources
  ORDER BY enabled DESC, source_type, name
`).then(r => { 
  const healthy = r.rows.filter(x => x.enabled && !x.last_error).length;
  const errored = r.rows.filter(x => x.enabled && x.last_error).length;
  const disabled = r.rows.filter(x => !x.enabled).length;
  console.log('Total: ' + r.rows.length + ' | Healthy: ' + healthy + ' | Erroring: ' + errored + ' | Disabled: ' + disabled);
  let lastType = '';
  r.rows.forEach(row => {
    if (row.source_type !== lastType) { console.log('\n── ' + row.source_type + ' ──'); lastType = row.source_type; }
    const status = !row.enabled ? 'DISABLED' : row.last_error ? 'ERROR   ' : 'OK      ';
    const err = row.last_error ? ' [' + row.last_error.slice(0,50) + ']' : '';
    console.log(status + ' ' + row.name + err);
    console.log('         ' + row.url);
  });
  pool.end(); 
});
