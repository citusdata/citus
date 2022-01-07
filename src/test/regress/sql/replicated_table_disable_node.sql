CREATE SCHEMA disable_node_with_replicated_tables;
SET search_path TO disable_node_with_replicated_tables;

SET citus.next_shard_id TO 101500;

SET citus.shard_replication_factor TO 2;
CREATE TABLE replicated(a int, b int);
SELECT create_distributed_table('replicated', 'a', shard_count:=2);

CREATE TABLE ref (a int, b int);
SELECT create_reference_table('ref');

INSERT INTO replicated SELECT i,i FROM generate_series(0,10)i;
INSERT INTO ref SELECT i,i FROM generate_series(0,10)i;


-- should be successfully disable node
SELECT citus_disable_node('localhost', :worker_2_port, true);
SELECT public.wait_until_metadata_sync();

-- the active placement should NOT be removed from the coordinator
-- and from the workers
SELECT count(*) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
	WHERE n.isactive AND n.noderole = 'primary'
		  AND p.shardid IN (101500, 101501, 101502);

\c - - - :worker_1_port
SELECT count(*) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
	WHERE n.isactive AND n.noderole = 'primary'
		  AND p.shardid IN (101500, 101501, 101502);

SET search_path TO disable_node_with_replicated_tables;

-- should be able to ingest data from both the worker and the coordinator
INSERT INTO replicated SELECT i,i FROM generate_series(0,10)i;
INSERT INTO ref SELECT i,i FROM generate_series(0,10)i;

\c - - - :master_port
SET search_path TO disable_node_with_replicated_tables;

-- should be able to ingest data from both the worker and the coordinator
INSERT INTO replicated SELECT i,i FROM generate_series(0,10)i;
INSERT INTO ref SELECT i,i FROM generate_series(0,10)i;

-- now, query with round-robin policy such that
-- each query should hit different replicas
SET citus.task_assignment_policy to "round-robin";
SELECT count(*) FROM ref;
SELECT count(*) FROM ref;

SELECT count(*) FROM replicated;
SELECT count(*) FROM replicated;

-- now, we should be able to replicate the shards back
SET client_min_messages TO ERROR;
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);
SELECT 1 FROM replicate_table_shards('replicated', shard_replication_factor:=2, shard_transfer_mode:='block_writes');
RESET client_min_messages;

-- should be able to ingest data from both the worker and the coordinator
INSERT INTO replicated SELECT i,i FROM generate_series(0,10)i;
INSERT INTO ref SELECT i,i FROM generate_series(0,10)i;

-- now, query with round-robin policy such that
-- each query should hit different replicas
SET citus.task_assignment_policy to "round-robin";
SELECT count(*) FROM ref;
SELECT count(*) FROM ref;

SELECT count(*) FROM replicated;
SELECT count(*) FROM replicated;

SET client_min_messages TO ERROR;
DROP SCHEMA disable_node_with_replicated_tables CASCADE;
