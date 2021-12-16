ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 360005;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 100000;

SET citus.shard_replication_factor TO 1;

-- test that range and append distributed tables have coordinator replication
CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

-- test that deprecated api creates distributed tables with coordinator replication
CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'hash');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'append');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

CREATE TABLE repmodel_test (a int);
SELECT master_create_distributed_table('repmodel_test', 'a', 'range');
SELECT repmodel FROM pg_dist_partition WHERE logicalrelid='repmodel_test'::regclass;
DROP TABLE repmodel_test;

RESET citus.shard_replication_factor;

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 360025;

-- There should be no table on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT relname FROM pg_class WHERE relname LIKE 'data_load_test%';
\c - - :master_host :master_port

-- creating an index after loading data works
BEGIN;
CREATE TABLE data_load_test (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test VALUES (132, 'hello');
SELECT create_distributed_table('data_load_test', 'col1');
CREATE INDEX data_load_test_idx ON data_load_test (col2);
DROP TABLE data_load_test;
END;

-- popping in and out of existence in the same transaction works
BEGIN;
CREATE TABLE data_load_test (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test VALUES (132, 'hello');
SELECT create_distributed_table('data_load_test', 'col1');
DROP TABLE data_load_test;
END;

-- but dropping after a write on the distributed table is currently disallowed
BEGIN;
CREATE TABLE data_load_test (col1 int, col2 text, col3 serial);
INSERT INTO data_load_test VALUES (132, 'hello');
SELECT create_distributed_table('data_load_test', 'col1');
INSERT INTO data_load_test VALUES (243, 'world');
DROP TABLE data_load_test;
END;

-- Test data loading after dropping a column
CREATE TABLE data_load_test (col1 int, col2 text, col3 text, "CoL4"")" int);
INSERT INTO data_load_test VALUES (132, 'hello', 'world');
INSERT INTO data_load_test VALUES (243, 'world', 'hello');
ALTER TABLE data_load_test DROP COLUMN col1;
SELECT create_distributed_table('data_load_test', 'col3');
SELECT * FROM data_load_test ORDER BY col2;
-- make sure the tuple went to the right shard
SELECT * FROM data_load_test WHERE col3 = 'world';
DROP TABLE data_load_test;

SET citus.shard_replication_factor TO default;
SET citus.shard_count to 4;

CREATE TABLE lineitem_hash_part (like lineitem);
SELECT create_distributed_table('lineitem_hash_part', 'l_orderkey');

CREATE TABLE orders_hash_part (like orders);
SELECT create_distributed_table('orders_hash_part', 'o_orderkey');

CREATE UNLOGGED TABLE unlogged_table
(
	key text,
	value text
);
SELECT create_distributed_table('unlogged_table', 'key');
SELECT * FROM master_get_table_ddl_events('unlogged_table');

\c - - :public_worker_1_host :worker_1_port
SELECT relpersistence FROM pg_class WHERE relname LIKE 'unlogged_table_%';
\c - - :master_host :master_port

-- Test rollback of create table
BEGIN;
CREATE TABLE rollback_table(id int, name varchar(20));
SELECT create_distributed_table('rollback_table','id');
ROLLBACK;

-- Table should not exist on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = (SELECT oid FROM pg_class WHERE relname LIKE 'rollback_table%');
\c - - :master_host :master_port

-- Insert 3 rows to make sure that copy after shard creation touches the same
-- worker node twice.
BEGIN;
CREATE TABLE rollback_table(id int, name varchar(20));
INSERT INTO rollback_table VALUES(1, 'Name_1');
INSERT INTO rollback_table VALUES(2, 'Name_2');
INSERT INTO rollback_table VALUES(3, 'Name_3');
SELECT create_distributed_table('rollback_table','id');
ROLLBACK;

-- Table should not exist on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = (SELECT oid FROM pg_class WHERE relname LIKE 'rollback_table%');
\c - - :master_host :master_port

BEGIN;
CREATE TABLE rollback_table(id int, name varchar(20));
SELECT create_distributed_table('rollback_table','id');
\copy rollback_table from stdin delimiter ','
1, 'name_1'
2, 'name_2'
3, 'name_3'
\.
CREATE INDEX rollback_index ON rollback_table(id);
COMMIT;

-- Check the table is created
SELECT count(*) FROM rollback_table;
DROP TABLE rollback_table;

BEGIN;
CREATE TABLE rollback_table(id int, name varchar(20));
SELECT create_distributed_table('rollback_table','id');
\copy rollback_table from stdin delimiter ','
1, 'name_1'
2, 'name_2'
3, 'name_3'
\.
ROLLBACK;

-- Table should not exist on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = (SELECT oid FROM pg_class WHERE relname LIKE 'rollback_table%');
\c - - :master_host :master_port

BEGIN;
CREATE TABLE tt1(id int);
SELECT create_distributed_table('tt1','id');
CREATE TABLE tt2(id int);
SELECT create_distributed_table('tt2','id');
INSERT INTO tt1 VALUES(1);
INSERT INTO tt2 SELECT * FROM tt1 WHERE id = 1;
COMMIT;

-- Table should exist on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = 'public.tt1_360069'::regclass;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = 'public.tt2_360073'::regclass;
\c - - :master_host :master_port

DROP TABLE tt1;
DROP TABLE tt2;

-- It is known that creating a table with master_create_empty_shard is not
-- transactional, so table stay remaining on the worker node after the rollback
BEGIN;
CREATE TABLE append_tt1(id int);
SELECT create_distributed_table('append_tt1','id','append');
SELECT master_create_empty_shard('append_tt1');
ROLLBACK;

-- Table exists on the worker node.
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = 'public.append_tt1_360077'::regclass;
\c - - :master_host :master_port

-- There should be no table on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = (SELECT oid from pg_class WHERE relname LIKE 'public.tt1%');
\c - - :master_host :master_port

-- Queries executing with router executor is allowed in the same transaction
-- with create_distributed_table
BEGIN;
CREATE TABLE tt1(id int);
INSERT INTO tt1 VALUES(1);
SELECT create_distributed_table('tt1','id');
INSERT INTO tt1 VALUES(2);
SELECT * FROM tt1 WHERE id = 1;
COMMIT;

-- Placements should be created on the worker
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = 'public.tt1_360078'::regclass;
\c - - :master_host :master_port

DROP TABLE tt1;

BEGIN;
CREATE TABLE tt1(id int);
SELECT create_distributed_table('tt1','id');
DROP TABLE tt1;
COMMIT;

-- There should be no table on the worker node
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid = (SELECT oid from pg_class WHERE  relname LIKE 'tt1%');
\c - - :master_host :master_port

-- Tests with create_distributed_table & DDL & DML commands

-- Test should pass since GetPlacementListConnection can provide connections
-- in this order of execution
CREATE TABLE sample_table(id int);
SELECT create_distributed_table('sample_table','id');

BEGIN;
CREATE TABLE stage_table (LIKE sample_table);
\COPY stage_table FROM stdin; -- Note that this operation is a local copy
1
2
3
4
\.
SELECT create_distributed_table('stage_table', 'id');
INSERT INTO sample_table SELECT * FROM stage_table;
DROP TABLE stage_table;
SELECT * FROM sample_table WHERE id = 3;
COMMIT;

-- Show that rows of sample_table are updated
SELECT count(*) FROM sample_table;
DROP table sample_table;

-- Test as create_distributed_table - copy - create_distributed_table - copy
-- This combination is used by tests written by some ORMs.
BEGIN;
CREATE TABLE tt1(id int);
SELECT create_distributed_table('tt1','id');
\COPY tt1 from stdin;
1
2
3
\.
CREATE TABLE tt2(like tt1);
SELECT create_distributed_table('tt2','id');
\COPY tt2 from stdin;
4
5
6
\.
INSERT INTO tt1 SELECT * FROM tt2;
SELECT * FROM tt1 WHERE id = 3;
SELECT * FROM tt2 WHERE id = 6;
END;

SELECT count(*) FROM tt1;


-- the goal of the following test is to make sure that
-- both create_reference_table and create_distributed_table
-- calls creates the schemas without leading to any deadlocks

-- first create reference table, then hash distributed table
BEGIN;

CREATE SCHEMA sc;
CREATE TABLE sc.ref(a int);
insert into sc.ref SELECT s FROM generate_series(0, 100) s;
SELECT create_reference_table('sc.ref');

CREATE TABLE sc.hash(a int);
insert into sc.hash SELECT s FROM generate_series(0, 100) s;
SELECT create_distributed_table('sc.hash', 'a');

COMMIT;


-- first create hash distributed table, then reference table
BEGIN;

CREATE SCHEMA sc2;
CREATE TABLE sc2.hash(a int);
insert into sc2.hash SELECT s FROM generate_series(0, 100) s;
SELECT create_distributed_table('sc2.hash', 'a');

CREATE TABLE sc2.ref(a int);
insert into sc2.ref SELECT s FROM generate_series(0, 100) s;
SELECT create_reference_table('sc2.ref');

COMMIT;

SET citus.shard_count TO 4;

BEGIN;
CREATE SCHEMA sc3;
CREATE TABLE sc3.alter_replica_table
(
	name text,
	id int PRIMARY KEY
);
ALTER TABLE sc3.alter_replica_table REPLICA IDENTITY USING INDEX alter_replica_table_pkey;
SELECT create_distributed_table('sc3.alter_replica_table', 'id');
COMMIT;
SELECT run_command_on_workers($$SELECT relreplident FROM pg_class join information_schema.tables AS tables ON (pg_class.relname=tables.table_name) WHERE relname LIKE 'alter_replica_table_%' AND table_schema='sc3' LIMIT 1$$);

BEGIN;
CREATE SCHEMA sc4;
CREATE TABLE sc4.alter_replica_table
(
	name text,
	id int PRIMARY KEY
);
INSERT INTO sc4.alter_replica_table(id) SELECT generate_series(1,100);
SET search_path = 'sc4';
ALTER TABLE alter_replica_table REPLICA IDENTITY USING INDEX alter_replica_table_pkey;
SELECT create_distributed_table('alter_replica_table', 'id');
COMMIT;
SELECT run_command_on_workers($$SELECT relreplident FROM pg_class join information_schema.tables AS tables ON (pg_class.relname=tables.table_name) WHERE relname LIKE 'alter_replica_table_%' AND table_schema='sc4' LIMIT 1$$);
SET search_path = 'public';

BEGIN;
CREATE SCHEMA sc5;
CREATE TABLE sc5.alter_replica_table
(
	name text,
	id int NOT NULL
);
INSERT INTO sc5.alter_replica_table(id) SELECT generate_series(1,100);
ALTER TABLE sc5.alter_replica_table REPLICA IDENTITY FULL;
SELECT create_distributed_table('sc5.alter_replica_table', 'id');
COMMIT;
SELECT run_command_on_workers($$SELECT relreplident FROM pg_class join information_schema.tables AS tables ON (pg_class.relname=tables.table_name) WHERE relname LIKE 'alter_replica_table_%' AND table_schema='sc5' LIMIT 1$$);

BEGIN;
CREATE SCHEMA sc6;
CREATE TABLE sc6.alter_replica_table
(
	name text,
	id int NOT NULL
);
INSERT INTO sc6.alter_replica_table(id) SELECT generate_series(1,100);
CREATE UNIQUE INDEX unique_idx ON sc6.alter_replica_table(id);
ALTER TABLE sc6.alter_replica_table REPLICA IDENTITY USING INDEX unique_idx;
SELECT create_distributed_table('sc6.alter_replica_table', 'id');
COMMIT;
SELECT run_command_on_workers($$SELECT relreplident FROM pg_class join information_schema.tables AS tables ON (pg_class.relname=tables.table_name) WHERE relname LIKE 'alter_replica_table_%' AND table_schema='sc6' LIMIT 1$$);

BEGIN;
CREATE TABLE alter_replica_table
(
	name text,
	id int NOT NULL
);
INSERT INTO alter_replica_table(id) SELECT generate_series(1,100);
CREATE UNIQUE INDEX unique_idx ON alter_replica_table(id);
ALTER TABLE alter_replica_table REPLICA IDENTITY USING INDEX unique_idx;
SELECT create_distributed_table('alter_replica_table', 'id');
COMMIT;
SELECT run_command_on_workers($$SELECT relreplident FROM pg_class join information_schema.tables AS tables ON (pg_class.relname=tables.table_name) WHERE relname LIKE 'alter_replica_table_%' AND table_schema='public' LIMIT 1$$);

DROP TABLE tt1;
DROP TABLE tt2;
DROP TABLE alter_replica_table;
DROP SCHEMA sc CASCADE;
DROP SCHEMA sc2 CASCADE;
DROP SCHEMA sc3 CASCADE;
DROP SCHEMA sc4 CASCADE;
DROP SCHEMA sc5 CASCADE;
DROP SCHEMA sc6 CASCADE;

CREATE TABLE shard_col_table (a INT, b INT);
CREATE TABLE shard_col_table_2 (a INT, b INT);

SELECT create_distributed_table('shard_col_table', 'a');

-- ensure there are no colocation group with 11 shards
SELECT count(*) FROM pg_dist_colocation WHERE shardcount = 11;
UPDATE pg_dist_colocation SET shardcount = 11 WHERE colocationid IN
(
	SELECT colocation_id FROM citus_tables WHERE table_name = 'shard_col_table'::regclass
);

SELECT create_distributed_table('shard_col_table_2', 'a', shard_count:=11);

-- ensure shard_col_table and shard_col_table_2 are not colocated
SELECT a.colocation_id = b.colocation_id FROM citus_tables a, citus_tables b
	WHERE a.table_name = 'shard_col_table'::regclass AND b.table_name = 'shard_col_table_2'::regclass;

UPDATE pg_dist_colocation SET shardcount = 12 WHERE colocationid IN
(
	SELECT colocation_id FROM citus_tables WHERE table_name = 'shard_col_table'::regclass
);

DROP TABLE shard_col_table, shard_col_table_2;
