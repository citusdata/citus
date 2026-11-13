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


-- Test that we can insert an integer literal into a numeric column as well
CREATE TABLE numeric_test (id numeric(6, 1), val int);
SELECT create_distributed_table('numeric_test', 'id');

INSERT INTO numeric_test VALUES (21, 87) RETURNING *;
SELECT * FROM numeric_test WHERE id = 21;
SELECT * FROM numeric_test WHERE id = 21::int;
SELECT * FROM numeric_test WHERE id = 21::bigint;
SELECT * FROM numeric_test WHERE id = 21.0;
SELECT * FROM numeric_test WHERE id = 21.0::numeric;

PREPARE insert_p(int) AS INSERT INTO numeric_test VALUES ($1, 87) RETURNING *;
EXECUTE insert_p(1);
EXECUTE insert_p(2);
EXECUTE insert_p(3);
EXECUTE insert_p(4);
EXECUTE insert_p(5);
EXECUTE insert_p(6);

PREPARE select_p(int) AS SELECT * FROM numeric_test WHERE id=$1;
EXECUTE select_p(1);
EXECUTE select_p(2);
EXECUTE select_p(3);
EXECUTE select_p(4);
EXECUTE select_p(5);
EXECUTE select_p(6);

SET citus.enable_fast_path_router_planner TO false;
EXECUTE select_p(1);
EXECUTE select_p(2);
EXECUTE select_p(3);
EXECUTE select_p(4);
EXECUTE select_p(5);
EXECUTE select_p(6);

-- make sure that we don't return wrong resuls
INSERT INTO numeric_test VALUES (21.1, 87) RETURNING *;
SELECT * FROM numeric_test WHERE id = 21;
SELECT * FROM numeric_test WHERE id = 21::numeric;
SELECT * FROM numeric_test WHERE id = 21.1::numeric;

CREATE TABLE range_dist_table_1 (dist_col BIGINT);
SELECT create_distributed_table('range_dist_table_1', 'dist_col', 'range');

CALL public.create_range_partitioned_shards('range_dist_table_1', '{1000,3000,6000}', '{2000,4000,7000}');

INSERT INTO range_dist_table_1 VALUES (1001);
INSERT INTO range_dist_table_1 VALUES (3800);
INSERT INTO range_dist_table_1 VALUES (6500);

-- all were returning false before fixing #5077
SELECT SUM(dist_col)=3800+6500 FROM range_dist_table_1 WHERE dist_col >= 2999;
SELECT SUM(dist_col)=3800+6500 FROM range_dist_table_1 WHERE dist_col > 2999;
SELECT SUM(dist_col)=3800+6500 FROM range_dist_table_1 WHERE dist_col >= 2500;
SELECT SUM(dist_col)=3800+6500 FROM range_dist_table_1 WHERE dist_col > 2000;

SELECT SUM(dist_col)=3800+6500 FROM range_dist_table_1 WHERE dist_col > 1001;
SELECT SUM(dist_col)=1001+3800+6500 FROM range_dist_table_1 WHERE dist_col >= 1001;
SELECT SUM(dist_col)=1001+3800+6500 FROM range_dist_table_1 WHERE dist_col > 1000;
SELECT SUM(dist_col)=1001+3800+6500 FROM range_dist_table_1 WHERE dist_col >= 1000;

-- we didn't have such an off-by-one error in upper bound
-- calculation, but let's test such cases too
SELECT SUM(dist_col)=1001+3800 FROM range_dist_table_1 WHERE dist_col <= 4001;
SELECT SUM(dist_col)=1001+3800 FROM range_dist_table_1 WHERE dist_col < 4001;
SELECT SUM(dist_col)=1001+3800 FROM range_dist_table_1 WHERE dist_col <= 4500;
SELECT SUM(dist_col)=1001+3800 FROM range_dist_table_1 WHERE dist_col < 6000;

-- now test with composite type and more shards
CREATE TYPE comp_type AS (
    int_field_1 BIGINT,
    int_field_2 BIGINT
);

CREATE TYPE comp_type_range AS RANGE (
    subtype = comp_type);

CREATE TABLE range_dist_table_2 (dist_col comp_type);
SELECT create_distributed_table('range_dist_table_2', 'dist_col', 'range');

CALL public.create_range_partitioned_shards(
	'range_dist_table_2',
    '{"(10,24)","(10,58)",
	  "(10,90)","(20,100)"}',
	'{"(10,25)","(10,65)",
	  "(10,99)","(20,100)"}');

INSERT INTO range_dist_table_2 VALUES ((10, 24));
INSERT INTO range_dist_table_2 VALUES ((10, 60));
INSERT INTO range_dist_table_2 VALUES ((10, 91));
INSERT INTO range_dist_table_2 VALUES ((20, 100));

SELECT dist_col='(10, 60)'::comp_type FROM range_dist_table_2
WHERE dist_col >= '(10,26)'::comp_type AND
      dist_col <= '(10,75)'::comp_type;

SELECT * FROM range_dist_table_2
WHERE dist_col >= '(10,57)'::comp_type AND
      dist_col <= '(10,95)'::comp_type
ORDER BY dist_col;

SELECT * FROM range_dist_table_2
WHERE dist_col >= '(10,57)'::comp_type
ORDER BY dist_col;

SELECT dist_col='(20,100)'::comp_type FROM range_dist_table_2
WHERE dist_col > '(20,99)'::comp_type;

DROP TABLE range_dist_table_1, range_dist_table_2;
DROP TYPE comp_type CASCADE;

SET search_path TO public;
DROP SCHEMA prune_shard_list CASCADE;
