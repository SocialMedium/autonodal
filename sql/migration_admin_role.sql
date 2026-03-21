-- Promote tenant owner to admin role
-- Run once to bootstrap admin access

UPDATE users SET role = 'admin', updated_at = NOW()
WHERE email = 'jonathan.tanner@mitchellake.com'
  AND tenant_id = '00000000-0000-0000-0000-000000000001';

-- Fallback: if the exact email differs, promote the first user created in the tenant
UPDATE users SET role = 'admin', updated_at = NOW()
WHERE id = (
  SELECT id FROM users
  WHERE tenant_id = '00000000-0000-0000-0000-000000000001'
    AND role != 'admin'
  ORDER BY created_at ASC
  LIMIT 1
)
AND NOT EXISTS (
  SELECT 1 FROM users
  WHERE tenant_id = '00000000-0000-0000-0000-000000000001'
    AND role = 'admin'
);
