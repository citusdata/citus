----------------------------------------------------
-- multi_fix_partition_shard_index_names
-- check the following two issues
-- https://github.com/citusdata/citus/issues/4962
-- https://github.com/citusdata/citus/issues/5138
----------------------------------------------------
SET citus.next_shard_id TO 910000;
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA fix_idx_names;
SET search_path TO fix_idx_names, public;

-- fix_partition_shard_index_names cannot be called for distributed
-- and not partitioned tables
CREATE TABLE not_partitioned(id int);
SELECT create_distributed_table('not_partitioned', 'id');
SELECT fix_partition_shard_index_names('not_partitioned'::regclass);

-- fix_partition_shard_index_names cannot be called for partitioned
-- and not distributed tables
CREATE TABLE not_distributed(created_at timestamptz) PARTITION BY RANGE (created_at);
SELECT fix_partition_shard_index_names('not_distributed'::regclass);

-- test with proper table
CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('dist_partitioned_table', 'dist_col');

-- create a partition with a long name and another with a short name
CREATE TABLE partition_table_with_very_long_name PARTITION OF dist_partitioned_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE p PARTITION OF dist_partitioned_table FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

-- create an index on parent table
-- we will see that it doesn't matter whether we name the index on parent or not
-- indexes auto-generated on partitions will not use this name
CREATE INDEX short ON dist_partitioned_table USING btree (another_col, partition_col);

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_1_port
-- Note that, the shell table from above partition_table_with_very_long_name
-- and its shard partition_table_with_very_long_name_910008
-- have the same index name: partition_table_with_very_long_na_another_col_partition_col_idx
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
-- this should fail because of the name clash explained above
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- let's fix the problematic table
SET search_path TO fix_idx_names, public;
SELECT fix_partition_shard_index_names('dist_partitioned_table'::regclass);

\c - - - :worker_1_port
-- shard id has been appended to all index names which didn't end in shard id
-- this goes in line with Citus's way of naming indexes of shards: always append shardid to the end
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

-- this should now work
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- if we run this command again, the names will not change anymore since shardid is appended to them
SELECT fix_partition_shard_index_names('dist_partitioned_table'::regclass);
SELECT fix_all_partition_shard_index_names();

\c - - - :worker_1_port
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 910020;

-- if we explicitly create index on partition-to-be table, Citus handles the naming
-- hence we would have no broken index names
CREATE TABLE another_partition_table_with_very_long_name (dist_col int, another_col int, partition_col timestamp);
SELECT create_distributed_table('another_partition_table_with_very_long_name', 'dist_col');
CREATE INDEX ON another_partition_table_with_very_long_name USING btree (another_col, partition_col);
ALTER TABLE dist_partitioned_table ATTACH PARTITION another_partition_table_with_very_long_name FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

-- check it works even if we give a weird index name
CREATE TABLE yet_another_partition_table (dist_col int, another_col int, partition_col timestamp);
SELECT create_distributed_table('yet_another_partition_table', 'dist_col');
CREATE INDEX "really weird index name !!" ON yet_another_partition_table USING btree (another_col, partition_col);
ALTER TABLE dist_partitioned_table ATTACH PARTITION yet_another_partition_table FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_1_port
-- notice indexes of shards of another_partition_table_with_very_long_name already have shardid appended to the end
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
-- this command would not do anything
SELECT fix_all_partition_shard_index_names();

\c - - - :worker_1_port
-- names are the same as before
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

DROP TABLE dist_partitioned_table;

-- also, we cannot do any further operations (e.g. rename) on the indexes of partitions because
-- the index names on shards of partitions have been generated by Postgres, not Citus
-- it doesn't matter here whether the partition name is long or short

-- replicate scenario from above but this time with one shard so that this test isn't flaky
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 910030;

CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('dist_partitioned_table', 'dist_col');
CREATE TABLE partition_table_with_very_long_name PARTITION OF dist_partitioned_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE p PARTITION OF dist_partitioned_table FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
CREATE INDEX short ON dist_partitioned_table USING btree (another_col, partition_col);

-- rename shouldn't work
ALTER INDEX partition_table_with_very_long_na_another_col_partition_col_idx RENAME TO partition_table_with_very_long_name_idx;

-- we currently can't drop index on detached partition
-- https://github.com/citusdata/citus/issues/5138
ALTER TABLE dist_partitioned_table DETACH PARTITION p;
DROP INDEX p_another_col_partition_col_idx;

-- let's reattach and retry after fixing index names
ALTER TABLE dist_partitioned_table ATTACH PARTITION p FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');

\c - - - :worker_1_port
-- check the current broken index names
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
-- fix index names
SELECT fix_all_partition_shard_index_names();

\c - - - :worker_1_port
-- check the fixed index names
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
-- should now work
ALTER INDEX partition_table_with_very_long_na_another_col_partition_col_idx RENAME TO partition_table_with_very_long_name_idx;

-- now we can drop index on detached partition
ALTER TABLE dist_partitioned_table DETACH PARTITION p;
DROP INDEX p_another_col_partition_col_idx;

\c - - - :worker_1_port
-- check that indexes have been renamed
-- and that index on p has been dropped (it won't appear)
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

DROP SCHEMA fix_idx_names CASCADE;
SELECT run_command_on_workers($$ DROP SCHEMA IF EXISTS fix_idx_names CASCADE $$);
