CREATE SCHEMA "partitioned indexes";
SET search_path TO "partitioned indexes";
GRANT ALL ON SCHEMA "partitioned indexes" TO regularuser;

-- test with proper table
CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('dist_partitioned_table', 'dist_col');

-- create a partition with a long name and another with a short name
CREATE TABLE partition_table_with_very_long_name PARTITION OF dist_partitioned_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE p PARTITION OF dist_partitioned_table FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

-- create an index on parent table
-- we will see that it doesn't matter whether we name the index on parent or not
-- indexes auto-generated on partitions will not use this name
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE UNIQUE INDEX short ON dist_partitioned_table USING btree (dist_col, partition_col);

-- if we explicitly create index on partition-to-be table, Citus handles the naming
-- hence we would have no broken index names
CREATE TABLE another_partition_table_with_very_long_name (dist_col int, another_col int, partition_col timestamp);
SELECT create_distributed_table('another_partition_table_with_very_long_name', 'dist_col');
CREATE INDEX ON another_partition_table_with_very_long_name USING btree (another_col, partition_col);

-- normally, in arbitrary tests, we DO NOT any of the paramaters, they are managed by the test suite
-- however, due to the issue https://github.com/citusdata/citus/issues/4845 we have to switch to
-- sequential execution on this test. Because this test covers an important case, and the pool size
-- here does not really matter, so we set it to 1 for this transaction block
BEGIN;
	SET LOCAL citus.max_adaptive_executor_pool_size TO 1;
	ALTER TABLE dist_partitioned_table ATTACH PARTITION another_partition_table_with_very_long_name FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
COMMIT;

-- check it works even if we give a weird index name
CREATE TABLE yet_another_partition_table (dist_col int, another_col int, partition_col timestamp);
SELECT create_distributed_table('yet_another_partition_table', 'dist_col');
CREATE INDEX "really weird index name !!" ON yet_another_partition_table USING btree (another_col, partition_col);
ALTER TABLE dist_partitioned_table ATTACH PARTITION yet_another_partition_table FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

-- rename & create partition to trigger
-- fix_partition_shard_index_names on an index that a foreign key relies
ALTER INDEX short RENAME TO little_long;
CREATE TABLE p2 PARTITION OF dist_partitioned_table FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');


-- try creating index to foreign key
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX ON dist_partitioned_table USING btree (dist_col, partition_col);

-- try with index on only parent
-- this is also an invalid index
-- also try with hash method, not btree
CREATE INDEX short_parent ON ONLY dist_partitioned_table USING hash (dist_col);
-- only another_partition will have the index on dist_col inherited from short_parent
-- hence short_parent will still be invalid
CREATE TABLE another_partition (dist_col int, another_col int, partition_col timestamp);
-- SELECT fix_partition_shard_index_names('another_partition') will be executed
-- automatically at the end of the ATTACH PARTITION command
ALTER TABLE dist_partitioned_table ATTACH PARTITION another_partition FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');

-- try with expression indexes
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX expression_index ON dist_partitioned_table ((dist_col || ' ' || another_col)) INCLUDE(another_col) WITH(fillfactor 80) WHERE (dist_col > 10);
-- try with statistics on index
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX statistics_on_index on dist_partitioned_table ((dist_col+another_col), (dist_col-another_col));
ALTER INDEX statistics_on_index ALTER COLUMN 1 SET STATISTICS 3737;
ALTER INDEX statistics_on_index ALTER COLUMN 2 SET STATISTICS 3737;

-- we can drop index on detached partition
-- https://github.com/citusdata/citus/issues/5138
ALTER TABLE dist_partitioned_table DETACH PARTITION p;
DROP INDEX p_dist_col_partition_col_idx;
