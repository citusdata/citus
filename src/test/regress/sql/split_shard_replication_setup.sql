CREATE SCHEMA split_shard_replication_setup_schema;
SET search_path TO split_shard_replication_setup_schema;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 1;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

CREATE OR REPLACE FUNCTION wait_for_expected_rowcount_at_table(tableName text, expectedCount integer) RETURNS void AS $$
DECLARE
actualCount integer;
BEGIN
    EXECUTE FORMAT('SELECT COUNT(*) FROM %s', tableName) INTO actualCount;
    WHILE  expectedCount != actualCount LOOP
	 EXECUTE FORMAT('SELECT COUNT(*) FROM %s', tableName) INTO actualCount;
    END LOOP;
END$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION wait_for_updated_rowcount_at_table(tableName text, expectedCount integer) RETURNS void AS $$
DECLARE
actualCount integer;
BEGIN
    EXECUTE FORMAT($query$SELECT COUNT(*) FROM %s WHERE value='b'$query$, tableName) INTO actualCount;
    WHILE  expectedCount != actualCount LOOP
    EXECUTE FORMAT($query$SELECT COUNT(*) FROM %s WHERE value='b'$query$, tableName) INTO actualCount;
    END LOOP;
END$$ LANGUAGE plpgsql;

-- Create distributed table (non co-located)
CREATE TABLE table_to_split (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_to_split','id');

-- Test scenario one starts from here
-- 1. table_to_split is a citus distributed table
-- 2. Shard table_to_split_1 is located on worker1.
-- 3. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
--    table_to_split_2/3 are located on worker2
-- 4. execute UDF split_shard_replication_setup on worker1 with below
--    params:
--    worker_split_shard_replication_setup
--        (
--          ARRAY[
--                ROW(1 /*source shardId */, 2 /* new shardId */,-2147483648 /* minHashValue */, -1 /* maxHasValue */ , 18 /* nodeId where new shard is placed */ ),
--                ROW(1, 3 , 0 , 2147483647, 18 )
--               ]
--         );
-- 5. Create Replication slot with 'citus'
-- 6. Setup Pub/Sub
-- 7. Insert into table_to_split_1 at source worker1
-- 8. Expect the results in either table_to_split_2 or table_to_split_3 at worker2

\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
CREATE TABLE table_to_split_1(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_to_split_2(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_to_split_3(id bigserial PRIMARY KEY, value char);

-- Create dummy shard tables(table_to_split_2/3b) at worker1
-- This is needed for Pub/Sub framework to work.
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
CREATE TABLE table_to_split_2(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_to_split_3(id bigserial PRIMARY KEY, value char);

-- Create publication at worker1
CREATE PUBLICATION pub1 FOR TABLE table_to_split_1, table_to_split_2, table_to_split_3;

SELECT count(*) FROM worker_split_shard_replication_setup(ARRAY[
    ROW(1, 2, '-2147483648', '-1', :worker_2_node)::citus.split_shard_info,
    ROW(1, 3, '0', '2147483647', :worker_2_node)::citus.split_shard_info
    ]);

SELECT slot_name FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_%s_10', :worker_2_node), 'citus') \gset

-- Create subscription at worker2 with copy_data to 'false' and derived replication slot name
\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;

CREATE SUBSCRIPTION sub1
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub1
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_name,
                   copy_data=false);

-- No data is present at this moment in all the below tables at worker2
SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;

-- Insert data in table_to_split_1 at worker1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
INSERT INTO table_to_split_1 values(100, 'a');
INSERT INTO table_to_split_1 values(400, 'a');
INSERT INTO table_to_split_1 values(500, 'a');

SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;


-- Expect data to be present in shard 2 and shard 3 based on the hash value.
\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_to_split_1; -- should alwasy have zero rows

SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 1);
SELECT * FROM table_to_split_2;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 2);
SELECT * FROM table_to_split_3;

-- UPDATE data of table_to_split_1 from worker1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
UPDATE table_to_split_1 SET value='b' WHERE id = 100;
UPDATE table_to_split_1 SET value='b' WHERE id = 400;
UPDATE table_to_split_1 SET value='b' WHERE id = 500;

\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_to_split_1;

-- Value should be updated in table_to_split_2;
SELECT wait_for_updated_rowcount_at_table('table_to_split_2', 1);
SELECT * FROM table_to_split_2;

-- Value should be updated in table_to_split_3;
SELECT wait_for_updated_rowcount_at_table('table_to_split_3', 2);
SELECT * FROM table_to_split_3;

\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
DELETE FROM table_to_split_1;

-- Child shard rows should be deleted
\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;

SELECT wait_for_expected_rowcount_at_table('table_to_split_1', 0);
SELECT * FROM table_to_split_1;

SELECT wait_for_expected_rowcount_at_table('table_to_split_2', 0);
SELECT * FROM table_to_split_2;

SELECT wait_for_expected_rowcount_at_table('table_to_split_3', 0);
SELECT * FROM table_to_split_3;

 -- drop publication from worker1
\c - - - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
DROP PUBLICATION pub1;

\c - - - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;
DROP SUBSCRIPTION sub1;

