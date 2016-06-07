
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 370000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 370000;


-- ===================================================================
-- create test functions and types needed for tests
-- ===================================================================

CREATE FUNCTION sort_names(cstring, cstring, cstring)
	RETURNS cstring
	AS 'citus'
	LANGUAGE C STRICT;

-- create a custom type...
CREATE TYPE dummy_type AS (
    i integer
);

-- ... as well as a function to use as its comparator...
CREATE FUNCTION dummy_type_function(dummy_type, dummy_type) RETURNS boolean
AS 'SELECT TRUE;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- ... use that function to create a custom operator...
CREATE OPERATOR = (
    LEFTARG = dummy_type,
    RIGHTARG = dummy_type,
    PROCEDURE = dummy_type_function
);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY dummy_op_family USING hash;

-- ... finally, build an operator class, designate it as the default operator
-- class for the type, but only specify an equality operator. So the type will
-- have a default op class but no hash operator in that class.
CREATE OPERATOR CLASS dummy_op_family_class
DEFAULT FOR TYPE dummy_type USING hash FAMILY dummy_op_family AS
OPERATOR 1 =;

-- ===================================================================
-- test shard creation functionality
-- ===================================================================

CREATE TABLE table_to_distribute (
	name text PRIMARY KEY,
	id bigint,
	json_data json,
	test_type_data dummy_type
);

-- use an index instead of table name
SELECT master_create_distributed_table('table_to_distribute_pkey', 'id', 'hash');

-- use a bad column name
SELECT master_create_distributed_table('table_to_distribute', 'bad_column', 'hash');

-- use unrecognized partition type
SELECT master_create_distributed_table('table_to_distribute', 'name', 'unrecognized');

-- use a partition column of a type lacking any default operator class
SELECT master_create_distributed_table('table_to_distribute', 'json_data', 'hash');

-- use a partition column of type lacking the required support function (hash)
SELECT master_create_distributed_table('table_to_distribute', 'test_type_data', 'hash');

-- distribute table and inspect side effects
SELECT master_create_distributed_table('table_to_distribute', 'name', 'hash');
SELECT partmethod, partkey FROM pg_dist_partition
	WHERE logicalrelid = 'table_to_distribute'::regclass;

-- use a bad shard count
SELECT master_create_worker_shards('table_to_distribute', 0, 1);

-- use a bad replication factor
SELECT master_create_worker_shards('table_to_distribute', 16, 0);

-- use a replication factor higher than shard count
SELECT master_create_worker_shards('table_to_distribute', 16, 3);

-- finally, create shards and inspect metadata
SELECT master_create_worker_shards('table_to_distribute', 16, 1);

SELECT shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
	WHERE logicalrelid = 'table_to_distribute'::regclass
	ORDER BY (shardminvalue::integer) ASC;

-- all shards should have the same size (16 divides evenly into the hash space)
SELECT count(*) AS shard_count,
	shardmaxvalue::integer - shardminvalue::integer AS shard_size
	FROM pg_dist_shard
	WHERE logicalrelid='table_to_distribute'::regclass
	GROUP BY shard_size;

SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'table_to_distribute%' AND relkind = 'r';

-- try to create them again
SELECT master_create_worker_shards('table_to_distribute', 16, 1);

-- test list sorting
SELECT sort_names('sumedh', 'jason', 'ozgun');

SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'throwaway%' AND relkind = 'r';

-- test foreign table creation
CREATE FOREIGN TABLE foreign_table_to_distribute
(
	name text,
	id bigint
)
SERVER fake_fdw_server;

SELECT master_create_distributed_table('foreign_table_to_distribute', 'id', 'hash');
SELECT master_create_worker_shards('foreign_table_to_distribute', 16, 1);

SELECT shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
	WHERE logicalrelid = 'foreign_table_to_distribute'::regclass
	ORDER BY (shardminvalue::integer) ASC;

-- test shard creation using weird shard count
CREATE TABLE weird_shard_count
(
	name text,
	id bigint
);

SELECT master_create_distributed_table('weird_shard_count', 'id', 'hash');
SELECT master_create_worker_shards('weird_shard_count', 7, 1);

-- Citus ensures all shards are roughly the same size
SELECT shardmaxvalue::integer - shardminvalue::integer AS shard_size
	FROM pg_dist_shard
	WHERE logicalrelid = 'weird_shard_count'::regclass
	ORDER BY shardminvalue::integer ASC;

-- cleanup foreign table, related shards and shard placements
DELETE FROM pg_dist_shard_placement
	WHERE shardid IN (SELECT shardid FROM pg_dist_shard
					   WHERE logicalrelid = 'foreign_table_to_distribute'::regclass);
	
DELETE FROM pg_dist_shard
	WHERE logicalrelid = 'foreign_table_to_distribute'::regclass;
	
DELETE FROM pg_dist_partition
	WHERE logicalrelid = 'foreign_table_to_distribute'::regclass;
