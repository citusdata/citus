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

-- create distributed table metadata to observe shard pruning
CREATE TABLE pruning ( species text, last_pruned date, plant_id integer );

INSERT INTO pg_dist_partition (logicalrelid, partmethod, partkey)
VALUES
	('pruning'::regclass, 'h', column_name_to_column('pruning'::regclass, 'species'));

INSERT INTO pg_dist_shard
	(shardid, logicalrelid, shardstorage, shardminvalue, shardmaxvalue)
VALUES
	(10, 'pruning'::regclass, 't', '-2147483648', '-1073741826'),
	(11, 'pruning'::regclass, 't', '-1073741825', '-3'),
	(12, 'pruning'::regclass, 't', '-2', '1073741820'),
	(13, 'pruning'::regclass, 't', '1073741821', '2147483647');

-- with no values, expect all shards
SELECT prune_using_no_values('pruning');

-- with a single value, expect a single shard
SELECT prune_using_single_value('pruning', 'tomato');

-- the above is true even if that value is null
SELECT prune_using_single_value('pruning', NULL);

-- build an OR clause and expect more than one sahrd
SELECT prune_using_either_value('pruning', 'tomato', 'petunia');

-- an AND clause with incompatible values returns no shards
SELECT prune_using_both_values('pruning', 'tomato', 'petunia');

-- but if both values are on the same shard, should get back that shard
SELECT prune_using_both_values('pruning', 'tomato', 'rose');

-- unit test of the equality expression generation code
SELECT debug_equality_expression('pruning');

-- print the initial ordering of shard intervals
SELECT print_sorted_shard_intervals('pruning');

-- update only min value for one shard
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 11;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 12;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 10;
SELECT print_sorted_shard_intervals('pruning');

-- all shard placements are uninitialized
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 13;
SELECT print_sorted_shard_intervals('pruning');

-- now update the metadata so that the table is a range distributed table
UPDATE pg_dist_partition SET partmethod = 'r' WHERE logicalrelid = 'pruning'::regclass;

-- now the comparison is done via the partition column type, which is text
UPDATE pg_dist_shard SET shardminvalue = 'a', shardmaxvalue = 'b' WHERE shardid = 10;
UPDATE pg_dist_shard SET shardminvalue = 'c', shardmaxvalue = 'd' WHERE shardid = 11;
UPDATE pg_dist_shard SET shardminvalue = 'e', shardmaxvalue = 'f' WHERE shardid = 12;
UPDATE pg_dist_shard SET shardminvalue = 'g', shardmaxvalue = 'h' WHERE shardid = 13;

-- print the ordering of shard intervals with range partitioning as well
SELECT print_sorted_shard_intervals('pruning');

-- update only min value for one shard
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 11;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 12;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 10;
SELECT print_sorted_shard_intervals('pruning');

-- all shard placements are uninitialized
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 13;
SELECT print_sorted_shard_intervals('pruning');
