--
-- NON_COLOCATED_JOIN_ORDER
--

-- Tests to check placements of shards must be equal to choose local join logic.

CREATE TABLE test_table_1(id int, value_1 int);
SELECT master_create_distributed_table('test_table_1', 'id', 'append');
SET citus.large_table_shard_count to 1;

\copy test_table_1 FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.

\copy test_table_1 FROM STDIN DELIMITER ','
5,2
6,3
7,4
\.

CREATE TABLE test_table_2(id int, value_1 int);
SELECT master_create_distributed_table('test_table_2', 'id', 'append');

\copy test_table_2 FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.

\copy test_table_2 FROM STDIN DELIMITER ','
5,2
6,3
7,4
\.

SET citus.log_multi_join_order to TRUE;
SET client_min_messages to DEBUG1;

-- Since we both have same amount of shards and they are colocated on the same node
-- local join logic will be triggered.
SELECT count(*) FROM test_table_1, test_table_2 WHERE test_table_1.id = test_table_2.id;

-- Add two shards placement of interval [8,10] to test_table_1
SET citus.shard_replication_factor to 2;

\copy test_table_1 FROM STDIN DELIMITER ','
8,2
9,3
10,4
\.

-- Add two shards placement of interval [8,10] to test_table_2
SET citus.shard_replication_factor to 1;

\copy test_table_2 FROM STDIN DELIMITER ','
8,2
9,3
10,4
\.

-- Although shard interval of relation are same, since they have different amount of placements
-- for interval [8,10] repartition join logic will be triggered.
SET citus.enable_repartition_joins to ON;
SELECT count(*) FROM test_table_1, test_table_2 WHERE test_table_1.id = test_table_2.id;

SET client_min_messages TO default;

DROP TABLE test_table_1;
DROP TABLE test_table_2;
