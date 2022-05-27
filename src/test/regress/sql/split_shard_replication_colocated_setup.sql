\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 4;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

-- Create distributed table (co-located)
CREATE TABLE table_first (id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second (id bigserial PRIMARY KEY, value char);

SELECT create_distributed_table('table_first','id');

SET citus.next_shard_id TO 7;
SET citus.shard_count TO 1;
SELECT create_distributed_table('table_second', 'id', colocate_with => 'table_first');

\c - - - :worker_1_port
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

CREATE TABLE table_second_8(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_9(id bigserial PRIMARY KEY, value char);

\c - - - :worker_2_port
CREATE TABLE table_first_4(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

CREATE TABLE table_second_7(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_8(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_9(id bigserial PRIMARY KEY, value char);

-- Test scenario one starts from here
-- 1. table_first and table_second are colocated tables.
-- 2. Shard table_first_4 and table_second_7 are colocated on worker1
-- 3. table_first_4 is split into table_first_5 and table_first_6 with target as worker2
-- 4. table_second_7 is split into table_second_8 and table_second_9 with target as worker2
-- 5. Create Replication slot with 'decoding_plugin_for_shard_split'
-- 6. Setup Pub/Sub
-- 7. Insert into table_first_4 and table_second_7 at source worker1
-- 8. Expect the results in child shards on worker2

\c - - - :worker_1_port
-- Create publication at worker1
BEGIN;
    CREATE PUBLICATION PUB1 for table table_first_4, table_first_5, table_first_6, table_second_7, table_second_8, table_second_9;
COMMIT;

BEGIN;
select 1 from public.CreateReplicationSlotForColocatedShards(:worker_2_node, :worker_2_node);
COMMIT;
SELECT pg_sleep(10);

\c - - - :worker_2_port
-- Create subscription at worker2 with copy_data to 'false' and derived replication slot name
BEGIN;
SELECT 1 from public.CreateSubscription(:worker_2_node, 'SUB1');
COMMIT;

-- No data is present at this moment in all the below tables at worker2
SELECT * from table_first_4;
SELECT * from table_first_5;
SELECT * from table_first_6;
select pg_sleep(10);

-- Insert data in table_to_split_1 at worker1 
\c - - - :worker_1_port
INSERT into table_first_4 values(100, 'a');
INSERT into table_first_4 values(400, 'a');
INSERT into table_first_4 values(500, 'a');

SELECT * from table_first_4;
SELECT * from table_first_5;
SELECT * from table_first_6;

INSERT into table_second_7 values(100, 'a');
INSERT into table_second_7 values(400, 'a');
INSERT into table_second_7 values(500, 'a');

SELECT * FROM table_second_7;
SELECT * FROM table_second_8;
SELECT * FROM table_second_9;
select pg_sleep(10);

-- Expect data to be present in shard 5,6 and 8,9 based on the hash value.
\c - - - :worker_2_port
select pg_sleep(10);
SELECT * from table_first_4; -- should alwasy have zero rows
SELECT * from table_first_5;
SELECT * from table_first_6;

SELECT * FROM table_second_7; --should alwasy have zero rows
SELECT * FROM table_second_8;
SELECT * FROM table_second_9;