-- Test scenario (Parent and one child on same node. Other child on different node)
-- 1. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
-- 2. table_to_split_1 is located on worker1.
-- 3. table_to_split_2 is located on worker1 and table_to_split_3 is located on worker2
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;

-- Create publication at worker1
CREATE PUBLICATION pub1 FOR TABLE table_to_split_1, table_to_split_2, table_to_split_3;

SELECT count(*) FROM worker_split_shard_replication_setup(ARRAY[
    ROW(1, 'id', 2, '-2147483648', '-1', :worker_1_node)::citus.split_shard_info,
    ROW(1, 'id', 3, '0', '2147483647', :worker_2_node)::citus.split_shard_info
    ]);

SELECT slot_name AS slot_for_worker1 FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_%s_10', :worker_1_node), 'citus') \gset
SELECT slot_name AS slot_for_worker2 FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_%s_10', :worker_2_node), 'citus') \gset

-- Create subscription at worker1 with copy_data to 'false' and 'slot_for_worker1'
CREATE SUBSCRIPTION sub_worker1
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub1
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_for_worker1,
                   copy_data=false);

\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;

-- Create subscription at worker2 with copy_data to 'false' and 'slot_for_worker2'
CREATE SUBSCRIPTION sub_worker2
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub1
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_for_worker2,
                   copy_data=false);

-- No data is present at this moment in all the below tables at worker2
SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;

-- Insert data in table_to_split_1 at worker1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
INSERT INTO table_to_split_1 VALUES(100, 'a');
INSERT INTO table_to_split_1 VALUES(400, 'a');
INSERT INTO table_to_split_1 VALUES(500, 'a');
UPDATE table_to_split_1 SET value='b' WHERE id = 400;
SELECT * FROM table_to_split_1;

-- expect data to present in table_to_split_2 on worker1 as its destination for value '400'
SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 1);
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;

-- Expect data to be present only in table_to_split3 on worker2
\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 2);
SELECT * FROM table_to_split_3;

-- delete all from table_to_split_1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
DELETE FROM table_to_split_1;

-- rows from table_to_split_2 should be deleted
SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 0);
SELECT * FROM table_to_split_2;

-- rows from table_to_split_3 should be deleted
\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 0);
SELECT * FROM table_to_split_3;

\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;
DROP SUBSCRIPTION sub_worker2;

 -- drop publication from worker1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;
DROP SUBSCRIPTION sub_worker1;
DROP PUBLICATION pub1;
