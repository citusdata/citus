--
-- MULTI_METADATA_SYNC_DOMAIN
--
-- Test that metadata sync works correctly with DOMAIN types in non-public schemas
--

-- Create the initial cluster setup
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 9000000;
-- Store current sequence values and calculate restart values
SELECT nextval('pg_catalog.pg_dist_groupid_seq') - 1 AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') - 1 AS last_node_id \gset
-- Remove the second worker node to start with
SELECT citus_remove_node('localhost', :worker_2_port);

-- Test 1: Domain in a non-public schema with special characters
CREATE SCHEMA "prepared statements";
CREATE DOMAIN "prepared statements".test_key AS text CHECK(VALUE ~ '^test-\d$');
CREATE TABLE dist_domain_nonpublic(a "prepared statements".test_key, b int);
SELECT create_distributed_table('dist_domain_nonpublic', 'a');

-- Insert some test data
INSERT INTO dist_domain_nonpublic VALUES ('test-1', 100), ('test-2', 200);

-- Reset sequences to avoid conflicts with other tests
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
-- Now add the worker back - this is should not fail
-- The metadata sync should now work
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- cleanup before next test
DROP TABLE dist_domain_nonpublic CASCADE;
DROP DOMAIN "prepared statements".test_key CASCADE;
DROP SCHEMA "prepared statements" CASCADE;

SELECT citus_remove_node('localhost', :worker_2_port);

-- Test 2: Domain in public schema
CREATE DOMAIN public.positive_int AS int CHECK(VALUE > 0);
CREATE TABLE dist_domain_public(a positive_int, b text);
SELECT create_distributed_table('dist_domain_public', 'a');

-- Insert some test data
INSERT INTO dist_domain_public VALUES (1, 'one'), (2, 'two');

-- Reset sequences to avoid conflicts with other tests
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
-- Now add the worker back - this is should not fail
-- The metadata sync should now work
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- cleanup before next test
DROP TABLE dist_domain_public CASCADE;
DROP DOMAIN public.positive_int CASCADE;

SELECT citus_remove_node('localhost', :worker_2_port);

-- Test 3: Domain in a regular schema
CREATE SCHEMA test_schema;
CREATE DOMAIN test_schema.email_address AS text CHECK(VALUE ~ '^[^@]+@[^@]+\.[^@]+$');
CREATE TABLE dist_domain_regular_schema(id int, email test_schema.email_address);
SELECT create_distributed_table('dist_domain_regular_schema', 'id');

-- Insert some test data
INSERT INTO dist_domain_regular_schema VALUES (1, 'user@example.com'), (2, 'admin@test.org');


-- Reset sequences to avoid conflicts with other tests
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
-- Now add the worker back - this is should not fail
-- The metadata sync should now work
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

CREATE SCHEMA "prepared statements";
CREATE DOMAIN "prepared statements".test_key AS text CHECK(VALUE ~ '^test-\d$');
CREATE TABLE dist_domain_nonpublic(a "prepared statements".test_key, b int);
SELECT create_distributed_table('dist_domain_nonpublic', 'a');

CREATE DOMAIN public.positive_int AS int CHECK(VALUE > 0);
CREATE TABLE dist_domain_public(a positive_int, b text);
SELECT create_distributed_table('dist_domain_public', 'a');

-- Insert some test data
INSERT INTO dist_domain_public VALUES (11, 'eleven'), (12, 'twelve');
INSERT INTO dist_domain_regular_schema VALUES (11, 'user@example.com'), (12, 'admin@test.org');
INSERT INTO dist_domain_nonpublic VALUES ('test-3', 110), ('test-4', 120);

-- Verify the colocation metadata exists
SELECT shardcount, replicationfactor, distributioncolumntype::regtype
FROM pg_dist_colocation
WHERE distributioncolumntype IN (
    '"prepared statements".test_key'::regtype,
    'public.positive_int'::regtype,
    'test_schema.email_address'::regtype
)
ORDER BY distributioncolumntype::regtype::text;

-- Verify metadata was synced correctly by checking colocation on the worker
\c - - - :worker_2_port
SELECT shardcount, replicationfactor, distributioncolumntype::regtype
FROM pg_dist_colocation
WHERE distributioncolumntype IN (
    '"prepared statements".test_key'::regtype,
    'public.positive_int'::regtype,
    'test_schema.email_address'::regtype
)
ORDER BY distributioncolumntype::regtype::text;

-- Verify the domains exist on the worker
SELECT typname, typnamespace::regnamespace
FROM pg_type
WHERE typname IN ('test_key', 'positive_int', 'email_address')
ORDER BY typname;

-- Back to coordinator for cleanup
\c - - - :master_port

-- Test that queries still work after re-adding the worker
SELECT * FROM dist_domain_nonpublic ORDER BY a;
SELECT * FROM dist_domain_public ORDER BY a;
SELECT * FROM dist_domain_regular_schema ORDER BY id;

-- Test inserts still work
INSERT INTO dist_domain_regular_schema VALUES (1, 'test@domain.com');
INSERT INTO dist_domain_regular_schema VALUES (2, 'info@citusdata.com');

SELECT * FROM dist_domain_nonpublic ORDER BY a;
SELECT * FROM dist_domain_public ORDER BY a;
SELECT * FROM dist_domain_regular_schema ORDER BY id;

-- Cleanup

DROP TABLE dist_domain_nonpublic CASCADE;
DROP TABLE dist_domain_public CASCADE;
DROP TABLE dist_domain_regular_schema CASCADE;

DROP DOMAIN "prepared statements".test_key CASCADE;
DROP DOMAIN public.positive_int CASCADE;
DROP DOMAIN test_schema.email_address CASCADE;

DROP SCHEMA "prepared statements" CASCADE;
DROP SCHEMA test_schema CASCADE;
