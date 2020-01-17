CREATE SCHEMA prune_shard_list;
SET search_path TO prune_shard_list;

SET citus.next_shard_id TO 800000;


-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION prune_using_no_values(regclass)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION prune_using_single_value(regclass, text)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C;

CREATE FUNCTION prune_using_either_value(regclass, text, text)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION prune_using_both_values(regclass, text, text)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION debug_equality_expression(regclass)
	RETURNS cstring
	AS 'citus'
	LANGUAGE C STRICT;

CREATE FUNCTION print_sorted_shard_intervals(regclass)
	RETURNS text[]
	AS 'citus'
	LANGUAGE C STRICT;

-- ===================================================================
-- test shard pruning functionality
-- ===================================================================

-- create distributed table observe shard pruning
CREATE TABLE pruning ( species text, last_pruned date, plant_id integer );
SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('pruning', 'species', 'hash');

-- with no values, expect all shards
SELECT prune_using_no_values('pruning');

-- with a single value, expect a single shard
SELECT prune_using_single_value('pruning', 'tomato');

-- null values should result in no pruning
SELECT prune_using_single_value('pruning', NULL);

-- build an OR clause and expect more than one sahrd
SELECT prune_using_either_value('pruning', 'tomato', 'petunia');

-- an AND clause with values on different shards returns no shards
SELECT prune_using_both_values('pruning', 'tomato', 'petunia');

-- even if both values are on the same shard, a value can't be equal to two others
SELECT prune_using_both_values('pruning', 'tomato', 'rose');

-- unit test of the equality expression generation code
SELECT debug_equality_expression('pruning');

-- print the initial ordering of shard intervals
SELECT print_sorted_shard_intervals('pruning');

-- update only min value for one shard
UPDATE pg_dist_shard set shardminvalue = NULL WHERE shardid = 800001;
SELECT print_sorted_shard_intervals('pruning');

-- repair shard
UPDATE pg_dist_shard set shardminvalue = -1073741824 WHERE shardid = 800001;

-- create range distributed table observe shard pruning
CREATE TABLE pruning_range ( species text, last_pruned date, plant_id integer );
SELECT create_distributed_table('pruning_range', 'species', 'range');

-- create worker shards
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');

-- now the comparison is done via the partition column type, which is text
UPDATE pg_dist_shard SET shardminvalue = 'a', shardmaxvalue = 'b' WHERE shardid = 800004;
UPDATE pg_dist_shard SET shardminvalue = 'c', shardmaxvalue = 'd' WHERE shardid = 800005;
UPDATE pg_dist_shard SET shardminvalue = 'e', shardmaxvalue = 'f' WHERE shardid = 800006;
UPDATE pg_dist_shard SET shardminvalue = 'g', shardmaxvalue = 'h' WHERE shardid = 800007;

-- print the ordering of shard intervals with range partitioning as well
SELECT print_sorted_shard_intervals('pruning_range');

-- update only min value for one shard
UPDATE pg_dist_shard set shardminvalue = NULL WHERE shardid = 800005;
SELECT print_sorted_shard_intervals('pruning_range');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 800006;
SELECT print_sorted_shard_intervals('pruning_range');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 800004;
SELECT print_sorted_shard_intervals('pruning_range');

-- all shard placements are uninitialized
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 800007;
SELECT print_sorted_shard_intervals('pruning_range');

-- ===================================================================
-- test pruning using values whose types are coerced
-- ===================================================================

CREATE TABLE coerce_hash (
	id bigint NOT NULL,
	value text NOT NULL
);
SELECT create_distributed_table('coerce_hash', 'id');

INSERT INTO coerce_hash VALUES (1, 'test value');

-- All three of the following should return the same results...

-- SELECT with same type as partition column
SELECT * FROM coerce_hash WHERE id = 1::bigint;

-- SELECT with similar type to partition column
SELECT * FROM coerce_hash WHERE id = 1;

-- SELECT with numeric type (original impetus for this change)

-- PostgreSQL will cast id to numeric rather than 1.0 to bigint...
-- We used to blindly pass the RHS' Datum to our comparison func.,
-- resulting in inaccurate pruning. An Assert now verifies type-
-- compatibility; the following would crash the server in an Assert-
-- before the underlying issue was addressed. It looks like a boring
-- test now, but if the old behavior is restored, it should crash again.
SELECT * FROM coerce_hash WHERE id = 1.0;

SELECT * FROM coerce_hash WHERE id = 1.0::numeric;

-- same queries with PREPARE
PREPARE coerce_bigint(bigint) AS SELECT * FROM coerce_hash WHERE id=$1::bigint;
PREPARE coerce_numeric(bigint) AS SELECT * FROM coerce_hash WHERE id=$1::numeric;
PREPARE coerce_numeric_2(numeric) AS SELECT * FROM coerce_hash WHERE id=$1;

EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);
EXECUTE coerce_bigint(1);

EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);
EXECUTE coerce_numeric(1);


EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);
EXECUTE coerce_numeric_2(1);


SET search_path TO public;
DROP SCHEMA prune_shard_list CASCADE;
