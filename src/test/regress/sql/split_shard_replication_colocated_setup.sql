\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 4;

CREATE USER myuser;
CREATE USER admin_user;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

\c - myuser - -
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 4;
CREATE TABLE table_first (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_first','id');

\c - admin_user - -
SET citus.next_shard_id TO 7;
SET citus.shard_count TO 1;
CREATE TABLE table_second (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_second', 'id', colocate_with => 'table_first');

\c - myuser - :worker_1_port
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

\c - myuser - :worker_2_port
CREATE TABLE table_first_4(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

\c - admin_user - :worker_1_port
CREATE TABLE table_second_8(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_9(id bigserial PRIMARY KEY, value char);

\c - admin_user - :worker_2_port
CREATE TABLE table_second_7(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_8(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_9(id bigserial PRIMARY KEY, value char);

--- Test scenario one starts from here
--- 1. table_first and table_second are colocated tables.
--- 2. myuser is the owner table_first and admin_user is the owner of table_second.
--- 3. Shard table_first_4 and table_second_7 are colocated on worker1
--- 4. table_first_4 is split into table_first_5 and table_first_6 with target as worker2
--- 5. table_second_7 is split into table_second_8 and table_second_9 with target as worker2
--- 6. Create two publishers and two subscribers for respective table owners.
--- 7. Insert into table_first_4 and table_second_7 at source worker1
--- 8. Expect the results in child shards on worker2

\c - postgres - :worker_1_port
-- Create publication at worker1
BEGIN;
    CREATE PUBLICATION PUB1 for table table_first_4, table_first_5, table_first_6;
COMMIT;

BEGIN;
    CREATE PUBLICATION PUB2 for table table_second_7, table_second_8, table_second_9;
COMMIT;

BEGIN;
select 1 from public.create_replication_slot_for_colocated_shards(:worker_2_node, :worker_2_node);
COMMIT;
SELECT pg_sleep(5);


-- Create subscription at worker2 with copy_data to 'false' and derived replication slot name
\c - postgres - :worker_2_port
SET client_min_messages TO WARNING;
BEGIN;
SELECT 1 from public.create_subscription_for_owner_one(:worker_2_node, 'SUB1');
COMMIT;

\c - myuser - :worker_1_port
INSERT into table_first_4 values(100, 'a');
INSERT into table_first_4 values(400, 'a');
INSERT into table_first_4 values(500, 'a');
select pg_sleep(2);

\c - admin_user - :worker_1_port
INSERT INTO table_second_7 values(100, 'a');
INSERT INTO table_second_7 values(400, 'a');
SELECT * from table_second_7;
select pg_sleep(2);

\c - myuser - :worker_2_port
SELECT * from table_first_4; -- should alwasy have zero rows
SELECT * from table_first_5;
SELECT * from table_first_6;

\c - admin_user - :worker_2_port
SELECT * from table_second_7;
SELECT * from table_second_8;
SELECT * from table_second_9;

\c - postgres - :worker_2_port
SET client_min_messages TO WARNING;
BEGIN;
SELECT 1 from public.create_subscription_for_owner_two(:worker_2_node, 'SUB2');
COMMIT;
select pg_sleep(5);

\c - admin_user - :worker_2_port
SELECT * from table_second_7;
SELECT * from table_second_8;
SELECT * from table_second_9;