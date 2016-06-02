
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 800000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 800000;


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
SELECT master_create_distributed_table('pruning', 'species', 'hash');

-- create worker shards
SELECT master_create_worker_shards('pruning', 4, 1);

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
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103071;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103072;
SELECT print_sorted_shard_intervals('pruning');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103070;
SELECT print_sorted_shard_intervals('pruning');

-- all shard placements are uninitialized
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103073;
SELECT print_sorted_shard_intervals('pruning');

-- create range distributed table observe shard pruning
CREATE TABLE pruning_range ( species text, last_pruned date, plant_id integer );
SELECT master_create_distributed_table('pruning_range', 'species', 'range');

-- create worker shards
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');
SELECT master_create_empty_shard('pruning_range');

-- now the comparison is done via the partition column type, which is text
UPDATE pg_dist_shard SET shardminvalue = 'a', shardmaxvalue = 'b' WHERE shardid = 103074;
UPDATE pg_dist_shard SET shardminvalue = 'c', shardmaxvalue = 'd' WHERE shardid = 103075;
UPDATE pg_dist_shard SET shardminvalue = 'e', shardmaxvalue = 'f' WHERE shardid = 103076;
UPDATE pg_dist_shard SET shardminvalue = 'g', shardmaxvalue = 'h' WHERE shardid = 103077;

-- print the ordering of shard intervals with range partitioning as well
SELECT print_sorted_shard_intervals('pruning_range');

-- update only min value for one shard
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103075;
SELECT print_sorted_shard_intervals('pruning_range');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103076;
SELECT print_sorted_shard_intervals('pruning_range');

-- now lets have one more shard without min/max values
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103074;
SELECT print_sorted_shard_intervals('pruning_range');

-- all shard placements are uninitialized
UPDATE pg_dist_shard set shardminvalue = NULL, shardmaxvalue = NULL WHERE shardid = 103077;
SELECT print_sorted_shard_intervals('pruning_range');
