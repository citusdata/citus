-- Test scenario (parent shard and child shards are located on same machine)
-- 1. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
-- 2. table_to_split_1 is located on worker1.
-- 3. table_to_split_2 and table_to_split_3 are located on worker1
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;

-- Create publication at worker1
CREATE PUBLICATION pub1 for table table_to_split_1, table_to_split_2, table_to_split_3;

-- Worker1 is target for table_to_split_2 and table_to_split_3
SELECT count(*) FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[
    ROW(1, 'id', 2, '-2147483648', '-1', :worker_1_node)::pg_catalog.split_shard_info,
    ROW(1, 'id', 3, '0', '2147483647', :worker_1_node)::pg_catalog.split_shard_info
    ], 0);

-- we create replication slots with a name including the next_operation_id as a suffix
-- if this test file fails, make sure you compare the next_operation_id output to the object name in the next command
SHOW citus.next_operation_id;

SELECT slot_name AS local_slot FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_slot_%s_10_0', :worker_1_node), 'citus') \gset

-- Create subscription at worker1 with copy_data to 'false' a
BEGIN;
CREATE SUBSCRIPTION local_subscription
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub1
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:local_slot,
                   copy_data=false);
COMMIT;

INSERT INTO table_to_split_1 VALUES(100, 'a');
INSERT INTO table_to_split_1 VALUES(400, 'a');
INSERT INTO table_to_split_1 VALUES(500, 'a');

-- expect data to present in  table_to_split_2/3 on worker1
SELECT * FROM table_to_split_1;

SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 1);
SELECT * FROM table_to_split_2;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 2);
SELECT * FROM table_to_split_3;

DELETE FROM table_to_split_1;

SELECT wait_for_expected_rowcount_at_table('table_to_split_1', 0);
SELECT * FROM table_to_split_1;

SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 0);
SELECT * FROM table_to_split_2;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 0);
SELECT * FROM table_to_split_3;

-- clean up
DROP SUBSCRIPTION local_subscription;
DROP PUBLICATION pub1;
