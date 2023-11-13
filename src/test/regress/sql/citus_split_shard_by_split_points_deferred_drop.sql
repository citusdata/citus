CREATE SCHEMA "citus_split_shard_by_split_points_deferred_schema";

CREATE ROLE test_split_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_split_shard_by_split_points_deferred_schema" TO test_split_role;
SET ROLE test_split_role;

SET search_path TO "citus_split_shard_by_split_points_deferred_schema";

-- Valide user cannot insert directly to pg_dist_cleanup table but can select from it.
CREATE TABLE temp_table (id INT);
INSERT INTO pg_catalog.pg_dist_cleanup (operation_id, object_type, object_name, node_group_id, policy_type)
    VALUES (3134, 1, 'citus_split_shard_by_split_points_deferred_schema.temp_table', 1, 1);

SELECT * from pg_dist_cleanup;

-- Disable Deferred drop auto cleanup to avoid flaky tests.
\c - postgres - :master_port
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

-- Perform a split and validate shard is marked for deferred drop.
SET citus.next_shard_id TO 8981000;
SET citus.next_placement_id TO 8610000;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SET citus.next_operation_id TO 777;
ALTER SEQUENCE pg_catalog.pg_dist_cleanup_recordid_seq RESTART 511;
SET ROLE test_split_role;
SET search_path TO "citus_split_shard_by_split_points_deferred_schema";

CREATE TABLE table_to_split(id int PRIMARY KEY, int_data int, data text);
SELECT create_distributed_table('table_to_split', 'id');

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

SET citus.next_shard_id TO 9999000;
SET citus.next_placement_id TO 5555000;

SELECT pg_catalog.citus_split_shard_by_split_points(
    8981000,
    ARRAY['-100000'],
    ARRAY[:worker_1_node, :worker_2_node],
    'block_writes');

SELECT pg_catalog.citus_split_shard_by_split_points(
    8981001,
    ARRAY['100000'],
    ARRAY[:worker_1_node, :worker_2_node],
    'force_logical');

-- The original shard is marked for deferred drop with policy_type = 2.
-- The previous shard should be dropped at the beginning of the second split call
SELECT * FROM pg_dist_cleanup WHERE policy_type = 2;

-- One of the physical shards should not be deleted, the other one should.
\c - - - :worker_1_port
SELECT relname FROM pg_class where relname LIKE '%table_to_split_%' AND relkind = 'r' ORDER BY relname;

\c - - - :worker_2_port
SELECT relname FROM pg_class where relname LIKE '%table_to_split_%' AND relkind = 'r' ORDER BY relname;

-- Perform deferred drop cleanup.
\c - postgres - :master_port
SELECT public.wait_for_resource_cleanup();

-- Clenaup has been done.
SELECT * from pg_dist_cleanup;

-- Verify that the shard to be dropped is dropped
\c - - - :worker_1_port
SELECT relname FROM pg_class where relname LIKE '%table_to_split_%' AND relkind = 'r' ORDER BY relname;

\c - - - :worker_2_port
SELECT relname FROM pg_class where relname LIKE '%table_to_split_%' AND relkind = 'r' ORDER BY relname;

-- Test Cleanup
\c - postgres - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA "citus_split_shard_by_split_points_deferred_schema" CASCADE;
DROP USER test_split_role;
