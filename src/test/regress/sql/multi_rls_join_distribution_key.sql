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
CREATE TABLE table_c (tenant_id uuid, id int);
CREATE TABLE table_d (tenant_id uuid, id int);

SELECT create_distributed_table('table_a', 'tenant_id');
SELECT create_distributed_table('table_b', 'tenant_id', colocate_with => 'table_a');
SELECT create_distributed_table('table_c', 'tenant_id', colocate_with => 'table_a');
SELECT create_distributed_table('table_d', 'tenant_id', colocate_with => 'table_a');

-- Grant privileges on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA rls_join_test TO app_user;

-- Insert test data
INSERT INTO table_a VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 1),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 2);

INSERT INTO table_b VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 10),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 20);

INSERT INTO table_c VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 100),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 200);

INSERT INTO table_d VALUES
    ('0194d116-5dd5-74af-be74-7f5e8468eeb7', 1000),
    ('0194d116-5dd5-74af-be74-7f5e8468eeb8', 2000);

-- Enable RLS and create policies on multiple tables
ALTER TABLE table_a ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_a ON table_a TO app_user
    USING (tenant_id = current_setting('session.current_tenant_id')::UUID);

ALTER TABLE table_c ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_c ON table_c TO app_user
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

-- Simple 2-way join (original test case)
SELECT a.id, b.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
ORDER BY a.id, b.id;

SELECT a.id, b.id
FROM table_a AS a
RIGHT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
ORDER BY a.id, b.id;

-- 3-way join with RLS on multiple tables
SELECT a.id, b.id, c.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
JOIN table_c AS c ON b.tenant_id = c.tenant_id
ORDER BY a.id, b.id, c.id;

SELECT a.id, b.id, c.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT OUTER JOIN table_c AS c ON b.tenant_id = c.tenant_id
ORDER BY a.id, b.id, c.id;

SELECT a.id, b.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
JOIN table_d AS d ON b.tenant_id = d.tenant_id
ORDER BY a.id, b.id, d.id;

SELECT a.id, b.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT OUTER JOIN table_d AS d ON b.tenant_id = d.tenant_id
ORDER BY a.id, b.id, d.id;

SELECT a.id, b.id, d.id
FROM table_a AS a
RIGHT JOIN table_b AS b ON a.tenant_id = b.tenant_id
RIGHT OUTER JOIN table_d AS d ON b.tenant_id = d.tenant_id
ORDER BY a.id, b.id, d.id;

SELECT a.id, b.id, d.id
FROM table_a AS a
RIGHT JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT OUTER JOIN table_d AS d ON b.tenant_id = d.tenant_id
ORDER BY a.id, b.id, d.id;

SELECT a.id, c.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_c AS c ON a.tenant_id = c.tenant_id
JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, c.id, d.id;

SELECT a.id, c.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_c AS c ON a.tenant_id = c.tenant_id
LEFT OUTER JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, c.id, d.id;

-- 4-way join with different join types
SELECT a.id, b.id, c.id, d.id
FROM table_a AS a
INNER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT JOIN table_c AS c ON b.tenant_id = c.tenant_id
INNER JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, b.id, c.id, d.id;

SELECT a.id, b.id, c.id, d.id
FROM table_a AS a
INNER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT JOIN table_c AS c ON b.tenant_id = c.tenant_id
RIGHT JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, b.id, c.id, d.id;

SELECT a.id, b.id, c.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT OUTER JOIN table_c AS c ON b.tenant_id = c.tenant_id
LEFT OUTER JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, b.id, c.id, d.id;

SELECT a.id, b.id, c.id, d.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
RIGHT OUTER JOIN table_c AS c ON b.tenant_id = c.tenant_id
LEFT OUTER JOIN table_d AS d ON c.tenant_id = d.tenant_id
ORDER BY a.id, b.id, c.id, d.id;

-- IN subquery that can be transformed to semi-join
SELECT a.id
FROM table_a a
WHERE a.tenant_id IN (
    SELECT b.tenant_id
    FROM table_b b
    JOIN table_c c USING (tenant_id)
)
ORDER BY a.id;

SELECT a.id
FROM table_a a
WHERE a.tenant_id IN (
    SELECT b.tenant_id
    FROM table_b b
    LEFT OUTER JOIN table_c c USING (tenant_id)
)
ORDER BY a.id;

-- Another multi-way join variation
SELECT a.id, b.id, c.id
FROM table_a AS a
INNER JOIN table_b AS b ON a.tenant_id = b.tenant_id
INNER JOIN table_c AS c ON a.tenant_id = c.tenant_id
ORDER BY a.id, b.id, c.id;

SELECT a.id, b.id, c.id
FROM table_a AS a
LEFT OUTER JOIN table_b AS b ON a.tenant_id = b.tenant_id
LEFT OUTER JOIN table_c AS c ON a.tenant_id = c.tenant_id
ORDER BY a.id, b.id, c.id;

ROLLBACK;

-- Switch back to superuser for cleanup
RESET ROLE;

-- Cleanup: Drop schema and all objects
DROP SCHEMA rls_join_test CASCADE;

DROP USER IF EXISTS app_user;
