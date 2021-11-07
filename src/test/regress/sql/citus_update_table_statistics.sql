--
-- citus_update_table_statistics.sql
--
-- Test citus_update_table_statistics function on both
-- hash and append distributed tables
-- This function updates shardlength, shardminvalue and shardmaxvalue
--
SET citus.next_shard_id TO 981000;
SET citus.next_placement_id TO 982000;
SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 2;

-- test with a hash-distributed table
-- here we update only shardlength, not shardminvalue and shardmaxvalue
CREATE TABLE test_table_statistics_hash (id int);
SELECT create_distributed_table('test_table_statistics_hash', 'id');

-- populate table
INSERT INTO test_table_statistics_hash SELECT i FROM generate_series(0, 10000)i;

-- originally shardlength (size of the shard) is zero
SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    ds.shardminvalue AS shardminvalue,
    ds.shardmaxvalue AS shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_hash') AND dsp.shardlength = 0
ORDER BY 2, 3;

-- setting this to on in order to verify that we use a distributed transaction id
-- to run the size queries from different connections
-- this is going to help detect deadlocks
SET citus.log_remote_commands TO ON;

-- setting this to sequential in order to have a deterministic order
-- in the output of citus.log_remote_commands
SET citus.multi_shard_modify_mode TO sequential;

-- update table statistics and then check that shardlength has changed
-- but shardminvalue and shardmaxvalue stay the same because this is
-- a hash distributed table

SELECT citus_update_table_statistics('test_table_statistics_hash');

RESET citus.log_remote_commands;
RESET citus.multi_shard_modify_mode;

SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_hash') AND dsp.shardlength > 0
ORDER BY 2, 3;

-- check with an append-distributed table
-- here we update shardlength, shardminvalue and shardmaxvalue
CREATE TABLE test_table_statistics_append (id int);
SELECT create_distributed_table('test_table_statistics_append', 'id', 'append');
SELECT master_create_empty_shard('test_table_statistics_append') AS shardid1 \gset
SELECT master_create_empty_shard('test_table_statistics_append') AS shardid2 \gset
COPY test_table_statistics_append FROM PROGRAM 'echo 0 && echo 1 && echo 2 && echo 3' WITH (format 'csv', append_to_shard :shardid1);
COPY test_table_statistics_append FROM PROGRAM 'echo 4 && echo 5 && echo 6 && echo 7' WITH (format 'csv', append_to_shard :shardid2);

-- shardminvalue and shardmaxvalue are NULL
SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_append')
ORDER BY 2, 3;

-- delete some data to change shardminvalues of a shards
DELETE FROM test_table_statistics_append WHERE id = 0 OR id = 4;

SET citus.log_remote_commands TO ON;
SET citus.multi_shard_modify_mode TO sequential;

-- update table statistics and then check that shardminvalue has changed
-- shardlength (shardsize) is still 8192 since there is very few data
SELECT citus_update_table_statistics('test_table_statistics_append');

RESET citus.log_remote_commands;
RESET citus.multi_shard_modify_mode;

SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_append')
ORDER BY 2, 3;

DROP TABLE test_table_statistics_hash, test_table_statistics_append;
ALTER SYSTEM RESET citus.shard_count;
ALTER SYSTEM RESET citus.shard_replication_factor;
