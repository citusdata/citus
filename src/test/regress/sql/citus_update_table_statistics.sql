--
-- citus_update_table_statistics.sql
--
-- Test citus_update_table_statistics function on both
-- hash and append distributed tables
-- This function updates shardlength, shardminvalue and shardmaxvalue
--
SET citus.next_shard_id TO 981000;
SET client_min_messages TO WARNING;
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
    dsp.shardlength AS shardsize,
    ds.shardminvalue AS shardminvalue,
    ds.shardmaxvalue AS shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_hash')
ORDER BY 2, 3;

-- update table statistics and then check that shardlength has changed
-- but shardminvalue and shardmaxvalue stay the same because this is
-- a hash distributed table

SELECT citus_update_table_statistics('test_table_statistics_hash');
SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    dsp.shardlength as shardsize,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_hash')
ORDER BY 2, 3;

-- check with an append-distributed table
-- here we update shardlength, shardminvalue and shardmaxvalue
CREATE TABLE test_table_statistics_append (id int);
SELECT create_distributed_table('test_table_statistics_append', 'id', 'append');
COPY test_table_statistics_append FROM PROGRAM 'echo 0 && echo 1 && echo 2 && echo 3' WITH CSV;
COPY test_table_statistics_append FROM PROGRAM 'echo 4 && echo 5 && echo 6 && echo 7' WITH CSV;

-- originally shardminvalue and shardmaxvalue will be 0,3 and 4, 7
SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    dsp.shardlength as shardsize,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_append')
ORDER BY 2, 3;

-- delete some data to change shardminvalues of a shards
DELETE FROM test_table_statistics_append WHERE id = 0 OR id = 4;

-- update table statistics and then check that shardminvalue has changed
-- shardlength (shardsize) is still 8192 since there is very few data

SELECT citus_update_table_statistics('test_table_statistics_append');
SELECT
	ds.logicalrelid::regclass::text AS tablename,
	ds.shardid AS shardid,
    dsp.placementid AS placementid,
	shard_name(ds.logicalrelid, ds.shardid) AS shardname,
    dsp.shardlength as shardsize,
    ds.shardminvalue as shardminvalue,
    ds.shardmaxvalue as shardmaxvalue
FROM pg_dist_shard ds JOIN pg_dist_shard_placement dsp USING (shardid)
WHERE ds.logicalrelid::regclass::text in ('test_table_statistics_append')
ORDER BY 2, 3;

DROP TABLE test_table_statistics_hash, test_table_statistics_append;
ALTER SYSTEM RESET citus.shard_count;
ALTER SYSTEM RESET citus.shard_replication_factor;
