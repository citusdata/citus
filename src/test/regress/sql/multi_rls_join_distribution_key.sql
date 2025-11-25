--
-- MULTI_RLS_JOIN_DISTRIBUTION_KEY
--
-- Test that RLS policies with volatile functions don't prevent
-- Citus from recognizing joins on distribution columns.
--

SET citus.next_shard_id TO 1900000;
SET citus.shard_replication_factor TO 1;

-- Create test user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'app_user') THEN
        CREATE USER app_user;
    END IF;
END
$$;

-- Create schema and grant privileges
CREATE SCHEMA IF NOT EXISTS rls_join_test;
GRANT ALL PRIVILEGES ON SCHEMA rls_join_test TO app_user;

SET search_path TO rls_join_test;

-- Create and distribute tables
CREATE TABLE table_a (tenant_id uuid, id int);
CREATE TABLE table_b (tenant_id uuid, id int);

SELECT create_distributed_table('table_a', 'tenant_id');
SELECT create_distributed_table('table_b', 'tenant_id', colocate_with => 'table_a');

-- Grant privileges on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA rls_join_test TO app_user;

-- Insert test data
INSERT INTO table_a VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 1),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 2);

INSERT INTO table_b VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 10),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 20);

-- Enable RLS and create policy
ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_0 ON table_a TO app_user
    USING (tenant_id = current_setting('session.current_tenant_id')::UUID);

-- Test scenario that previously failed
-- Switch to app_user and execute the query with RLS
SET ROLE app_user;

SET citus.propagate_set_commands = local;
SET application_name = '0194d116-5dd5-74af-be74-7f5e8468eeb7';

BEGIN;
-- Set session.current_tenant_id from application_name
DO $$
DECLARE
BEGIN
    EXECUTE 'SET LOCAL session.current_tenant_id = ' || quote_literal(current_setting('application_name', true));
END;
$$;

-- This query should work with RLS enabled

SELECT c.id, t.id
FROM table_a AS c
LEFT OUTER JOIN table_b AS t ON c.tenant_id = t.tenant_id
ORDER BY c.id, t.id;

ROLLBACK;

-- Switch back to superuser for cleanup
RESET ROLE;

-- Cleanup: Drop schema and all objects
DROP SCHEMA rls_join_test CASCADE;

DROP USER IF EXISTS app_user;
