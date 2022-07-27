CREATE SCHEMA worker_copy_table_to_node;
SET search_path TO worker_copy_table_to_node;
SET citus.shard_count TO 1; -- single shard table for ease of testing
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 62629600;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

CREATE TABLE t(a int);
INSERT INTO t SELECT generate_series(1, 100);

CREATE TABLE ref(a int);
INSERT INTO ref SELECT generate_series(1, 100);

select create_distributed_table('t', 'a');
select create_reference_table('ref');

\c - - - :worker_2_port
SET search_path TO worker_copy_table_to_node;

-- Create empty shard on worker 2 too
CREATE TABLE t_62629600(a int);

\c - - - :worker_1_port
SET search_path TO worker_copy_table_to_node;

-- Make sure that the UDF doesn't work on Citus tables
SELECT worker_copy_table_to_node('t', :worker_1_node);
SELECT worker_copy_table_to_node('ref', :worker_1_node);

-- It should work on shards
SELECT worker_copy_table_to_node('t_62629600', :worker_1_node);

SELECT count(*) FROM t;
SELECT count(*) FROM t_62629600;

SELECT worker_copy_table_to_node('t_62629600', :worker_2_node);

\c - - - :worker_2_port
SET search_path TO worker_copy_table_to_node;

SELECT count(*) FROM t_62629600;

\c - - - :master_port
SET search_path TO worker_copy_table_to_node;

SET client_min_messages TO WARNING;
DROP SCHEMA worker_copy_table_to_node CASCADE;
