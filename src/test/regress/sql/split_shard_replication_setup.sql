SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 1;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

-- Create distributed table (non co-located)
CREATE TABLE table_to_split (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_to_split','id');

-- slotName_table is used to persist replication slot name.
-- It is only used for testing as the worker2 needs to create subscription over the same replication slot.
CREATE TABLE slotName_table (name text, nodeId int, id int primary key);
SELECT create_distributed_table('slotName_table','id');

-- Test scenario one starts from here
-- 1. table_to_split is a citus distributed table
-- 2. Shard table_to_split_1 is located on worker1.
-- 3. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
--    table_to_split_2/3 are located on worker2
-- 4. execute UDF split_shard_replication_setup on worker1 with below
--    params:
--    split_shard_replication_setup
--        (
--          ARRAY[
--                ARRAY[1 /*source shardId */, 2 /* new shardId */,-2147483648 /* minHashValue */, -1 /* maxHasValue */ , 18 /* nodeId where new shard is placed */ ], 
--                ARRAY[1, 3 , 0 , 2147483647, 18 ]
--               ]
--         );
-- 5. Create Replication slot with 'decoding_plugin_for_shard_split'
-- 6. Setup Pub/Sub
-- 7. Insert into table_to_split_1 at source worker1
-- 8. Expect the results in either table_to_split_2 or table_to_split_3 at worker2

\c - - - :worker_2_port
CREATE TABLE table_to_split_1(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_to_split_2(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_to_split_3(id bigserial PRIMARY KEY, value char);

-- Create dummy shard tables(table_to_split_2/3) at worker1
-- This is needed for Pub/Sub framework to work.
\c - - - :worker_1_port
BEGIN;
    CREATE TABLE table_to_split_2(id bigserial PRIMARY KEY, value char);
    CREATE TABLE table_to_split_3(id bigserial PRIMARY KEY, value char);
COMMIT;

-- Create publication at worker1
BEGIN;
    CREATE PUBLICATION PUB1 for table table_to_split_1, table_to_split_2, table_to_split_3;
COMMIT;

-- Create replication slot for target node worker2
BEGIN;
select 1 from public.CreateReplicationSlot(:worker_2_node, :worker_2_node);
COMMIT;

\c - - - :worker_2_port
-- Create subscription at worker2 with copy_data to 'false' and derived replication slot name
BEGIN;
SELECT 1 from public.CreateSubscription(:worker_2_node, 'SUB1');
COMMIT;

-- No data is present at this moment in all the below tables at worker2
SELECT * from table_to_split_1;
SELECT * from table_to_split_2;
SELECT * from table_to_split_3;
select pg_sleep(10);

-- Insert data in table_to_split_1 at worker1 
\c - - - :worker_1_port
INSERT into table_to_split_1 values(100, 'a');
INSERT into table_to_split_1 values(400, 'a');
INSERT into table_to_split_1 values(500, 'a');
SELECT * from table_to_split_1;
SELECT * from table_to_split_2;
SELECT * from table_to_split_3;
select pg_sleep(10);

-- Expect data to be present in shard 2 and shard 3 based on the hash value.
\c - - - :worker_2_port
select pg_sleep(10);
SELECT * from table_to_split_1; -- should alwasy have zero rows
SELECT * from table_to_split_2;
SELECT * from table_to_split_3;

-- UPDATE data of table_to_split_1 from worker1
\c - - - :worker_1_port
UPDATE table_to_split_1 SET value='b' where id = 100;
UPDATE table_to_split_1 SET value='b' where id = 400;
UPDATE table_to_split_1 SET value='b' where id = 500;
SELECT pg_sleep(10);

-- Value should be updated in table_to_split_2;
\c - - - :worker_2_port
SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;

\c - - - :worker_1_port
DELETE FROM table_to_split_1;
SELECT pg_sleep(10);

-- Child shard rows should be deleted
\c - - - :worker_2_port
SELECT * FROM table_to_split_1;
SELECT * FROM table_to_split_2;
SELECT * FROM table_to_split_3;

 -- drop publication from worker1
\c - - - :worker_1_port
drop PUBLICATION PUB1;
DELETE FROM slotName_table;

\c - - - :worker_2_port
SET client_min_messages TO WARNING;
DROP SUBSCRIPTION SUB1;
DELETE FROM slotName_table;

-- Test scenario two starts from here
-- 1. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
-- 2. table_to_split_1 is located on worker1.
-- 3. table_to_split_2 is located on worker1 and table_to_split_3 is located on worker2

\c - - - :worker_1_port
-- Create publication at worker1
BEGIN;
    CREATE PUBLICATION PUB1 for table table_to_split_1, table_to_split_2, table_to_split_3;
COMMIT;

-- Create replication slots for two target nodes worker1 and worker2.
-- Worker1 is target for table_to_split_2 and Worker2 is target for table_to_split_3
BEGIN;
select 1 from public.CreateReplicationSlot(:worker_1_node, :worker_2_node);
COMMIT;
SELECT pg_sleep(10);

-- Create subscription at worker1 with copy_data to 'false' and derived replication slot name
BEGIN;
SELECT 1 from public.CreateSubscription(:worker_1_node, 'SUB1');
COMMIT;

\c - - - :worker_2_port
-- Create subscription at worker2 with copy_data to 'false' and derived replication slot name
BEGIN;
SELECT 1 from public.CreateSubscription(:worker_2_node, 'SUB2');
COMMIT;

-- No data is present at this moment in all the below tables at worker2
SELECT * from table_to_split_1;
SELECT * from table_to_split_2;
SELECT * from table_to_split_3;
select pg_sleep(10);

-- Insert data in table_to_split_1 at worker1
\c - - - :worker_1_port
INSERT into table_to_split_1 values(100, 'a');
INSERT into table_to_split_1 values(400, 'a');
INSERT into table_to_split_1 values(500, 'a');
UPDATE table_to_split_1 SET value='b' where id = 400;
select pg_sleep(10);

-- expect data to present in table_to_split_2 on worker1 as its destination for value '400'
SELECT * from table_to_split_1;
SELECT * from table_to_split_2; 
SELECT * from table_to_split_3;
select pg_sleep(10);

-- Expect data to be present only in table_to_split3 on worker2
\c - - - :worker_2_port
select pg_sleep(10);
SELECT * from table_to_split_1;
SELECT * from table_to_split_2; 
SELECT * from table_to_split_3;

-- delete all from table_to_split_1
\c - - - :worker_1_port
DELETE FROM table_to_split_1;
SELECT pg_sleep(5);

-- rows from table_to_split_2 should be deleted
SELECT * from table_to_split_2;

-- rows from table_to_split_3 should be deleted
\c - - - :worker_2_port
SELECT * from table_to_split_3;

 -- drop publication from worker1
\c - - - :worker_1_port
SET client_min_messages TO WARNING;
DROP PUBLICATION PUB1;
DROP SUBSCRIPTION SUB1;
DELETE FROM slotName_table;

\c - - - :worker_2_port

SET client_min_messages TO WARNING;
DROP SUBSCRIPTION SUB2;
DELETE FROM slotName_table;

-- Test scenario three starts from here (parent shard and child shards are located on same machine)
-- 1. table_to_split_1 is split into table_to_split_2 and table_to_split_3.
-- 2. table_to_split_1 is located on worker1.
-- 3. table_to_split_2 and table_to_split_3 are located on worker1

\c - - - :worker_1_port
SET client_min_messages TO WARNING;

-- Create publication at worker1
BEGIN;
    CREATE PUBLICATION PUB1 for table table_to_split_1, table_to_split_2, table_to_split_3;
COMMIT;

-- Worker1 is target for table_to_split_2 and table_to_split_3
BEGIN;
select 1 from public.CreateReplicationSlot(:worker_1_node, :worker_1_node);
COMMIT;
SELECT pg_sleep(10);

-- Create subscription at worker1 with copy_data to 'false' and derived replication slot name
BEGIN;
SELECT 1 from public.CreateSubscription(:worker_1_node, 'SUB1');
COMMIT;
SELECT pg_sleep(10);

INSERT into table_to_split_1 values(100, 'a');
INSERT into table_to_split_1 values(400, 'a');
INSERT into table_to_split_1 values(500, 'a');
select pg_sleep(10);

-- expect data to present in  table_to_split_2/3 on worker1
SELECT * from table_to_split_1;
SELECT * from table_to_split_2; 
SELECT * from table_to_split_3;
select pg_sleep(10);

DELETE FROM table_to_split_1;
SELECT pg_sleep(10);
SELECT * from table_to_split_1;
SELECT * from table_to_split_2; 
SELECT * from table_to_split_3;

-- clean up
DROP PUBLICATION PUB1;
DELETE FROM slotName_table;
DROP SUBSCRIPTION SUB1;