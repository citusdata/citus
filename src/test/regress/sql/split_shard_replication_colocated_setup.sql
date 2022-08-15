\c - - - :master_port
CREATE USER myuser;
CREATE USER admin_user;

GRANT USAGE, CREATE ON SCHEMA split_shard_replication_setup_schema, public to myuser;
GRANT USAGE, CREATE ON SCHEMA split_shard_replication_setup_schema, public to admin_user;

SET search_path TO split_shard_replication_setup_schema;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 4;

SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset

\c - myuser - -
SET search_path TO split_shard_replication_setup_schema;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;
SET citus.next_shard_id TO 4;
CREATE TABLE table_first (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_first','id');

\c - admin_user - -
SET search_path TO split_shard_replication_setup_schema;
SET citus.next_shard_id TO 7;
SET citus.shard_count TO 1;
CREATE TABLE table_second (id bigserial PRIMARY KEY, value char);
SELECT create_distributed_table('table_second', 'id', colocate_with => 'table_first');

\c - myuser - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

\c - myuser - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
CREATE TABLE table_first_4(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_5(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_first_6(id bigserial PRIMARY KEY, value char);

\c - admin_user - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
CREATE TABLE table_second_8(id bigserial PRIMARY KEY, value char);
CREATE TABLE table_second_9(id bigserial PRIMARY KEY, value char);

\c - admin_user - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
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

-- Create publication at worker1
\c - postgres - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
CREATE PUBLICATION pub1 FOR TABLE table_first_4, table_first_5, table_first_6;
CREATE PUBLICATION pub2 FOR TABLE table_second_7, table_second_8, table_second_9;

SELECT count(*) FROM pg_catalog.worker_split_shard_replication_setup(ARRAY[
    ROW(4, 'id', 5, '-2147483648', '-1', :worker_2_node)::pg_catalog.split_shard_info,
    ROW(4, 'id', 6, '0', '2147483647', :worker_2_node)::pg_catalog.split_shard_info,
    ROW(7, 'id', 8, '-2147483648', '-1', :worker_2_node)::pg_catalog.split_shard_info,
    ROW(7, 'id', 9, '0', '2147483647', :worker_2_node)::pg_catalog.split_shard_info
    ]);

SELECT relowner AS table_owner_one FROM pg_class WHERE relname='table_first' \gset
SELECT relowner AS table_owner_two FROM pg_class WHERE relname='table_second' \gset

SELECT slot_name AS slot_for_first_owner FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_slot_%s_%s', :worker_2_node, :table_owner_one), 'citus') \gset

SELECT slot_name AS slot_for_second_owner FROM pg_create_logical_replication_slot(FORMAT('citus_shard_split_slot_%s_%s', :worker_2_node, :table_owner_two), 'citus') \gset

-- Create subscription at worker2 with copy_data to 'false'
\c - postgres - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO WARNING;
CREATE SUBSCRIPTION sub1
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub1
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_for_first_owner,
                   copy_data=false);

\c - myuser - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
INSERT INTO table_first_4 VALUES(100, 'a');
INSERT INTO table_first_4 VALUES(400, 'a');
INSERT INTO table_first_4 VALUES(500, 'a');

SELECT wait_for_expected_rowcount_at_table('table_first_4', 3);
SELECT * FROM table_first_4;

\c - admin_user - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
INSERT INTO table_second_7 VALUES(100, 'a');
INSERT INTO table_second_7 VALUES(400, 'a');

SELECT wait_for_expected_rowcount_at_table('table_second_7', 2);
SELECT * FROM table_second_7;

-- expect data in table_first_5/6
\c - myuser - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_first_4;

SELECT wait_for_expected_rowcount_at_table('table_first_5', 1);
SELECT * FROM table_first_5;

SELECT wait_for_expected_rowcount_at_table('table_first_6', 2);
SELECT * FROM table_first_6;

-- should have zero rows in all the below tables as the subscription is not yet created for admin_user
\c - admin_user - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_second_7;
SELECT * FROM table_second_8;
SELECT * FROM table_second_9;

\c - postgres - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO WARNING;
CREATE SUBSCRIPTION sub2
        CONNECTION 'host=localhost port=57637 user=postgres dbname=regression'
        PUBLICATION pub2
               WITH (
                   create_slot=false,
                   enabled=true,
                   slot_name=:slot_for_second_owner,
                   copy_data=false);

-- expect data
\c - admin_user - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SELECT * FROM table_second_7;

SELECT wait_for_expected_rowcount_at_table('table_second_8', 1);
SELECT * FROM table_second_8;

SELECT wait_for_expected_rowcount_at_table('table_second_9', 1);
SELECT * FROM table_second_9;

\c - postgres - :worker_1_port
SET search_path TO split_shard_replication_setup_schema;
DROP PUBLICATION pub1;
DROP PUBLICATION pub2;

\c - postgres - :worker_2_port
SET search_path TO split_shard_replication_setup_schema;
SET client_min_messages TO ERROR;
DROP SUBSCRIPTION sub1;
DROP SUBSCRIPTION sub2;
