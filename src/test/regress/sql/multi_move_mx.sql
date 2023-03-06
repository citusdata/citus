--
-- MULTI_MOVE_MX
--
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1550000;

-- Create mx test tables
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE mx_table_1 (a int);
SELECT create_distributed_table('mx_table_1', 'a');

CREATE TABLE mx_table_2 (a int);
SELECT create_distributed_table('mx_table_2', 'a');

CREATE TABLE mx_table_3 (a text);
SELECT create_distributed_table('mx_table_3', 'a');

-- Check that the first two tables are colocated
SELECT
	logicalrelid, repmodel
FROM
	pg_dist_partition
WHERE
	logicalrelid = 'mx_table_1'::regclass
	OR logicalrelid = 'mx_table_2'::regclass
	OR logicalrelid = 'mx_table_3'::regclass
ORDER BY
	logicalrelid;

-- Check the list of shards
SELECT
	logicalrelid, shardid, nodename, nodeport
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	OR logicalrelid = 'mx_table_2'::regclass
	OR logicalrelid = 'mx_table_3'::regclass
ORDER BY
	logicalrelid, shardid;

-- Check the data on the worker
\c - - - :worker_2_port
SELECT
	logicalrelid, shardid, nodename, nodeport
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	OR logicalrelid = 'mx_table_2'::regclass
	OR logicalrelid = 'mx_table_3'::regclass
ORDER BY
	logicalrelid, shardid;

\c - - - :master_port
-- Check that citus_copy_shard_placement cannot be run with MX tables
SELECT
	citus_copy_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical')
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	AND nodeport = :worker_1_port
ORDER BY
	shardid
LIMIT 1;

-- Move a shard from worker 1 to worker 2
SELECT
	master_move_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical')
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	AND nodeport = :worker_1_port
ORDER BY
	shardid
LIMIT 1;

-- Test changing citus.node_conninfo on the target node affects the
-- CREATE SUBSCRIPTION command for shard move

\c - - - :worker_2_port

ALTER SYSTEM SET citus.node_conninfo TO 'sslrootcert=/non/existing/certificate.crt sslmode=verify-full';
SELECT pg_reload_conf();

\c - - - :worker_2_port
-- before reseting citus.node_conninfo, check that CREATE SUBSCRIPTION
-- with citus_use_authinfo takes into account node_conninfo even when
-- one of host, port, or user parameters are not specified.
--
-- We need to specify host and port to not get an hba error, so we test
-- only with ommitting user.

CREATE SUBSCRIPTION subs_01 CONNECTION 'host=''localhost'' port=57637'
PUBLICATION pub_01 WITH (citus_use_authinfo=true);

ALTER SYSTEM RESET citus.node_conninfo;
SELECT pg_reload_conf();

\c - - - :master_port

-- Check that the shard and its colocated shard is moved, but not the other shards
SELECT
	logicalrelid, shardid, nodename, nodeport
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	(logicalrelid = 'mx_table_1'::regclass
	OR logicalrelid = 'mx_table_2'::regclass
	OR logicalrelid = 'mx_table_3'::regclass)
	AND shardstate != 4
ORDER BY
	logicalrelid, shardid;

-- Check that the changes are made in the worker as well
\c - - - :worker_2_port
SELECT
	logicalrelid, shardid, nodename, nodeport
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	OR logicalrelid = 'mx_table_2'::regclass
	OR logicalrelid = 'mx_table_3'::regclass
ORDER BY
	logicalrelid, shardid;

-- Check that the UDFs cannot be called from the workers
SELECT
	citus_copy_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical')
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	AND nodeport = :worker_2_port
ORDER BY
	shardid
LIMIT 1 OFFSET 1;

SELECT
	master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical')
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	AND nodeport = :worker_2_port
ORDER BY
	shardid
LIMIT 1 OFFSET 1;

-- Check that shards of a table with GENERATED columns can be moved.
\c - - - :master_port
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE mx_table_with_generated_column (a int, b int GENERATED ALWAYS AS ( a + 3 ) STORED, c int);
SELECT create_distributed_table('mx_table_with_generated_column', 'a');

-- Check that dropped columns are handled properly in a move.
ALTER TABLE mx_table_with_generated_column DROP COLUMN c;

-- Move a shard from worker 1 to worker 2
SELECT
        citus_move_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical')
FROM
        pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
        logicalrelid = 'mx_table_with_generated_column'::regclass
	AND nodeport = :worker_1_port
ORDER BY
        shardid
LIMIT 1;

-- Cleanup
\c - - - :master_port
SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
DROP TABLE mx_table_with_generated_column;
DROP TABLE mx_table_1;
DROP TABLE mx_table_2;
DROP TABLE mx_table_3;
