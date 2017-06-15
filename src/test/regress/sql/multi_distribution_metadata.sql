-- ===================================================================
-- create test functions
-- ===================================================================


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 540000;


CREATE FUNCTION load_shard_id_array(regclass)
	RETURNS bigint[]
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_interval_array(bigint, anyelement)
	RETURNS anyarray
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_placement_array(bigint, bool)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_column_id(regclass)
	RETURNS smallint
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_type(regclass)
	RETURNS "char"
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION is_distributed_table(regclass)
	RETURNS boolean
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION column_name_to_column_id(regclass, cstring)
	RETURNS smallint
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION create_monolithic_shard_row(regclass)
	RETURNS bigint
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION acquire_shared_shard_lock(bigint)
	RETURNS void
	AS 'citus'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================

-- create hash distributed table
CREATE TABLE events_hash (
	id bigint,
	name text
);
SELECT master_create_distributed_table('events_hash', 'name', 'hash');

-- create worker shards
SELECT master_create_worker_shards('events_hash', 4, 2);

-- set shardstate of one replication from each shard to 0 (invalid value)
UPDATE pg_dist_shard_placement SET shardstate = 0 WHERE nodeport = 57638 AND shardid BETWEEN 540000 AND 540003;

-- should see above shard identifiers
SELECT load_shard_id_array('events_hash');

-- should see array with first shard range
SELECT load_shard_interval_array(540000, 0);

-- should even work for range-partitioned shards
-- create range distributed table
CREATE TABLE events_range (
	id bigint,
	name text
);
SELECT master_create_distributed_table('events_range', 'name', 'range');

-- create empty shard
SELECT master_create_empty_shard('events_range');

UPDATE pg_dist_shard SET
	shardminvalue = 'Aardvark',
	shardmaxvalue = 'Zebra'
WHERE shardid = 540004;

SELECT load_shard_interval_array(540004, ''::text);

-- should see error for non-existent shard
SELECT load_shard_interval_array(540005, 0);

-- should see two placements
SELECT load_shard_placement_array(540001, false);

-- only one of which is finalized
SELECT load_shard_placement_array(540001, true);

-- should see error for non-existent shard
SELECT load_shard_placement_array(540001, false);

-- should see column id of 'name'
SELECT partition_column_id('events_hash');

-- should see hash partition type and fail for non-distributed tables
SELECT partition_type('events_hash');
SELECT partition_type('pg_type');

-- should see true for events_hash, false for others
SELECT is_distributed_table('events_hash');
SELECT is_distributed_table('pg_type');
SELECT is_distributed_table('pg_dist_shard');

-- test underlying column name-id translation
SELECT column_name_to_column_id('events_hash', 'name');
SELECT column_name_to_column_id('events_hash', 'ctid');
SELECT column_name_to_column_id('events_hash', 'non_existent');

-- drop shard rows (must drop placements first)
DELETE FROM pg_dist_shard_placement
	WHERE shardid BETWEEN 540000 AND 540004;
DELETE FROM pg_dist_shard
	WHERE logicalrelid = 'events_hash'::regclass;
DELETE FROM pg_dist_shard
	WHERE logicalrelid = 'events_range'::regclass;

-- verify that an eager load shows them missing
SELECT load_shard_id_array('events_hash');

-- create second table to distribute
CREATE TABLE customers (
	id bigint,
	name text
);

-- now we'll distribute using function calls but verify metadata manually...

-- partition on id and manually inspect partition row
INSERT INTO pg_dist_partition (logicalrelid, partmethod, partkey)
VALUES
	('customers'::regclass, 'h', column_name_to_column('customers'::regclass, 'id'));
SELECT partmethod, column_to_column_name(logicalrelid, partkey) FROM pg_dist_partition
	WHERE logicalrelid = 'customers'::regclass;

-- make one huge shard and manually inspect shard row
SELECT create_monolithic_shard_row('customers') AS new_shard_id
\gset
SELECT shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
WHERE shardid = :new_shard_id;

-- now we'll even test our lock methods...

-- use transaction to bound how long we hold the lock
BEGIN;

-- pick up a shard lock and look for it in pg_locks
SELECT acquire_shared_shard_lock(5);
SELECT objid, mode FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- commit should drop the lock
COMMIT;

-- lock should be gone now
SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- test get_shard_id_for_distribution_column
SET citus.shard_count TO 4;
CREATE TABLE get_shardid_test_table1(column1 int, column2 int);
SELECT create_distributed_table('get_shardid_test_table1', 'column1');
\COPY get_shardid_test_table1 FROM STDIN with delimiter '|';
1|1
2|2
3|3
\.
SELECT get_shard_id_for_distribution_column('get_shardid_test_table1', 1);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table1', 2);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table1', 3);

-- verify result of the get_shard_id_for_distribution_column
\c - - - :worker_1_port
SELECT * FROM get_shardid_test_table1_540006;
SELECT * FROM get_shardid_test_table1_540009;
SELECT * FROM get_shardid_test_table1_540007;
\c - - - :master_port

-- test non-existing value
SELECT get_shard_id_for_distribution_column('get_shardid_test_table1', 4);

-- test array type
SET citus.shard_count TO 4;
CREATE TABLE get_shardid_test_table2(column1 text[], column2 int);
SELECT create_distributed_table('get_shardid_test_table2', 'column1');
\COPY get_shardid_test_table2 FROM STDIN with delimiter '|';
{a, b, c}|1
{d, e, f}|2
\.
SELECT get_shard_id_for_distribution_column('get_shardid_test_table2', '{a, b, c}');
SELECT get_shard_id_for_distribution_column('get_shardid_test_table2', '{d, e, f}');

-- verify result of the get_shard_id_for_distribution_column
\c - - - :worker_1_port
SELECT * FROM get_shardid_test_table2_540013;
SELECT * FROM get_shardid_test_table2_540011;
\c - - - :master_port

-- test mismatching data type
SELECT get_shard_id_for_distribution_column('get_shardid_test_table2', 'a');

-- test NULL distribution column value for hash distributed table
SELECT get_shard_id_for_distribution_column('get_shardid_test_table2');
SELECT get_shard_id_for_distribution_column('get_shardid_test_table2', NULL);

-- test non-distributed table
CREATE TABLE get_shardid_test_table3(column1 int, column2 int);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table3', 1);

-- test append distributed table
SELECT create_distributed_table('get_shardid_test_table3', 'column1', 'append');
SELECT get_shard_id_for_distribution_column('get_shardid_test_table3', 1);

-- test reference table;
CREATE TABLE get_shardid_test_table4(column1 int, column2 int);
SELECT create_reference_table('get_shardid_test_table4');

-- test NULL distribution column value for reference table
SELECT get_shard_id_for_distribution_column('get_shardid_test_table4');
SELECT get_shard_id_for_distribution_column('get_shardid_test_table4', NULL);

-- test different data types for reference table
SELECT get_shard_id_for_distribution_column('get_shardid_test_table4', 1);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table4', 'a');
SELECT get_shard_id_for_distribution_column('get_shardid_test_table4', '{a, b, c}');

-- test range distributed table
CREATE TABLE get_shardid_test_table5(column1 int, column2 int);
SELECT create_distributed_table('get_shardid_test_table5', 'column1', 'range');

-- create worker shards
SELECT master_create_empty_shard('get_shardid_test_table5');
SELECT master_create_empty_shard('get_shardid_test_table5');
SELECT master_create_empty_shard('get_shardid_test_table5');
SELECT master_create_empty_shard('get_shardid_test_table5');

-- now the comparison is done via the partition column type, which is text
UPDATE pg_dist_shard SET shardminvalue = 1, shardmaxvalue = 1000 WHERE shardid = 540015;
UPDATE pg_dist_shard SET shardminvalue = 1001, shardmaxvalue = 2000 WHERE shardid = 540016;
UPDATE pg_dist_shard SET shardminvalue = 2001, shardmaxvalue = 3000 WHERE shardid = 540017;
UPDATE pg_dist_shard SET shardminvalue = 3001, shardmaxvalue = 4000 WHERE shardid = 540018;

SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', 5);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', 1111);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', 2689);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', 3248);

-- test non-existing value for range distributed tables
SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', 4001);
SELECT get_shard_id_for_distribution_column('get_shardid_test_table5', -999);

-- clear unnecessary tables;
DROP TABLE get_shardid_test_table1, get_shardid_test_table2, get_shardid_test_table3, get_shardid_test_table4, get_shardid_test_table5;
