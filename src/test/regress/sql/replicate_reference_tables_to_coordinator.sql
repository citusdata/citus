--
-- REPLICATE_REF_TABLES_ON_COORDINATOR
--

CREATE SCHEMA replicate_ref_to_coordinator;
SET search_path TO 'replicate_ref_to_coordinator';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 8000000;
SET citus.next_placement_id TO 8000000;

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;

CREATE TABLE squares(a int, b int);
SELECT create_reference_table('squares');
INSERT INTO squares SELECT i, i * i FROM generate_series(1, 10) i;

SELECT groupid FROM pg_dist_placement 
WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='squares'::regclass::oid)
ORDER BY groupid;

-- should be executed on a worker
SELECT count(*) FROM squares;

-- The colocation group for reference tables should have replication factor 2
SELECT replicationfactor = 2 FROM pg_dist_colocation WHERE distributioncolumntype=0;

SELECT master_add_node('localhost', :master_port, groupid => 0) AS master_nodeid \gset

-- adding the same node again should return the existing nodeid
SELECT master_add_node('localhost', :master_port, groupid => 0) = :master_nodeid;

-- adding another node with groupid=0 should error out
SELECT master_add_node('localhost', 12345, groupid => 0) = :master_nodeid;

-- The colocation group for reference tables should have replication factor 3
SELECT replicationfactor = 3 FROM pg_dist_colocation WHERE distributioncolumntype=0;

SELECT groupid FROM pg_dist_placement 
WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='squares'::regclass::oid)
ORDER BY groupid;

-- should be executed locally
SELECT count(*) FROM squares;

-- create a second reference table
CREATE TABLE numbers(a int);
SELECT create_reference_table('numbers');
INSERT INTO numbers VALUES (20), (21);

-- INSERT ... SELECT between reference tables
BEGIN;
EXPLAIN INSERT INTO squares SELECT a, a*a FROM numbers;
INSERT INTO squares SELECT a, a*a FROM numbers;
SELECT * FROM squares WHERE a >= 20 ORDER BY a;
ROLLBACK;

BEGIN;
EXPLAIN INSERT INTO numbers SELECT a FROM squares WHERE a < 3;
INSERT INTO numbers SELECT a FROM squares WHERE a < 3;
SELECT * FROM numbers ORDER BY a;
ROLLBACK;

-- removing coordinator from pg_dist_node should update pg_dist_colocation
SELECT master_remove_node('localhost', :master_port);
SELECT replicationfactor = 2 FROM pg_dist_colocation WHERE distributioncolumntype=0;

-- clean-up
SET client_min_messages TO ERROR;
DROP SCHEMA replicate_ref_to_coordinator CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
