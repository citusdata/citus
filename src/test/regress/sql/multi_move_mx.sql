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
-- Check that master_copy_shard_placement cannot be run with MX tables
SELECT
	master_copy_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, false, 'force_logical')
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
	master_move_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port)
FROM
	pg_dist_shard NATURAL JOIN pg_dist_shard_placement
WHERE
	logicalrelid = 'mx_table_1'::regclass
	AND nodeport = :worker_1_port
ORDER BY
	shardid
LIMIT 1;

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
	master_copy_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, false, 'force_logical')
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

-- Cleanup
\c - - - :master_port
DROP TABLE mx_table_1;
DROP TABLE mx_table_2;
DROP TABLE mx_table_3;
