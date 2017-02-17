ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1420000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1420000;

-- run master_create_empty_shard with differing placement policies

CREATE TABLE append_table (a int);
SELECT master_create_distributed_table('append_table', 'a', 'append');

SET citus.shard_placement_policy TO 'round-robin';
SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');

SELECT * FROM pg_dist_shard_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'append_table'::regclass);

UPDATE pg_dist_node SET noderole = 's' WHERE nodeport = :worker_2_port;

-- round robin only considers primary nodes
SELECT master_create_empty_shard('append_table');

SET citus.shard_replication_factor = 1;
SET citus.shard_placement_policy TO 'random';

-- make sure it works when there's only one primary
SELECT master_create_empty_shard('append_table');

SELECT * FROM pg_dist_shard_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'append_table'::regclass);

SELECT setseed(0.5);
UPDATE pg_dist_node SET noderole = 'p' WHERE nodeport = :worker_2_port;

SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');
SELECT master_create_empty_shard('append_table');

SELECT * FROM pg_dist_shard_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'append_table'::regclass);

-- clean up
DROP TABLE append_table;
