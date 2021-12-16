CREATE SCHEMA partition_wise_join;
SET search_path to partition_wise_join;

SET citus.next_shard_id TO 360147;

CREATE TABLE partitioning_hash_test(id int, subid int) PARTITION BY HASH(subid);

CREATE TABLE partitioning_hash_test_0 PARTITION OF partitioning_hash_test FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE partitioning_hash_test_1 PARTITION OF partitioning_hash_test FOR VALUES WITH (MODULUS 3, REMAINDER 1);

INSERT INTO partitioning_hash_test VALUES (1, 2);
INSERT INTO partitioning_hash_test VALUES (2, 13);
INSERT INTO partitioning_hash_test VALUES (3, 7);
INSERT INTO partitioning_hash_test VALUES (4, 4);

-- distribute partitioned table
SELECT create_distributed_table('partitioning_hash_test', 'id');

-- test partition-wise join
CREATE TABLE partitioning_hash_join_test(id int, subid int) PARTITION BY HASH(subid);
CREATE TABLE partitioning_hash_join_test_0 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE partitioning_hash_join_test_1 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE partitioning_hash_join_test_2 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 2);

SELECT create_distributed_table('partitioning_hash_join_test', 'id');

SELECT success FROM run_command_on_workers('alter system set enable_mergejoin to off');
SELECT success FROM run_command_on_workers('alter system set enable_nestloop to off');
SELECT success FROM run_command_on_workers('alter system set enable_indexscan to off');
SELECT success FROM run_command_on_workers('alter system set enable_indexonlyscan to off');
SELECT success FROM run_command_on_workers('alter system set enable_partitionwise_join to off');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id, subid);

-- set partition-wise join on and parallel to off
SELECT success FROM run_command_on_workers('alter system set enable_partitionwise_join to on');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

-- SET enable_partitionwise_join TO on;
ANALYZE partitioning_hash_test, partitioning_hash_join_test;

EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id, subid);

-- note that partition-wise joins only work when partition key is in the join
-- following join does not have that, therefore join will not be pushed down to
-- partitions
EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id);

-- reset partition-wise join
SELECT success FROM run_command_on_workers('alter system reset enable_partitionwise_join');
SELECT success FROM run_command_on_workers('alter system reset enable_mergejoin');
SELECT success FROM run_command_on_workers('alter system reset enable_nestloop');
SELECT success FROM run_command_on_workers('alter system reset enable_indexscan');
SELECT success FROM run_command_on_workers('alter system reset enable_indexonlyscan');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

RESET enable_partitionwise_join;

DROP SCHEMA partition_wise_join CASCADE;
