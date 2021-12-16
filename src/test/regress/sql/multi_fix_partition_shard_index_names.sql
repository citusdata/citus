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

-- stop metadata sync for one of the worker nodes so we test both cases
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

-- NULL input should automatically return NULL since
-- fix_partition_shard_index_names is strict
-- same for worker_fix_partition_shard_index_names
SELECT fix_partition_shard_index_names(NULL);
SELECT worker_fix_partition_shard_index_names(NULL, NULL, NULL);

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
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX short ON dist_partitioned_table USING btree (another_col, partition_col);

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_1_port
-- the names are generated correctly
-- shard id has been appended to all index names which didn't end in shard id
-- this goes in line with Citus's way of naming indexes of shards: always append shardid to the end
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
-- this should work properly
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
-- we have no clashes
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

-- if we run this command again, the names will not change since shardid is appended to them
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

DROP INDEX short;
DROP TABLE yet_another_partition_table, another_partition_table_with_very_long_name;
-- this will create constraint1 index on parent
SET citus.max_adaptive_executor_pool_size TO 1;
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the ADD CONSTRAINT command
ALTER TABLE dist_partitioned_table ADD CONSTRAINT constraint1 UNIQUE (dist_col, partition_col);
RESET citus.max_adaptive_executor_pool_size;
CREATE TABLE fk_table (id int, fk_column timestamp, FOREIGN KEY (id, fk_column) REFERENCES dist_partitioned_table (dist_col, partition_col));

-- try creating index to foreign key
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX ON dist_partitioned_table USING btree (dist_col, partition_col);

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;
\c - - - :worker_1_port
-- index names end in shardid for partitions
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
SET citus.next_shard_id TO 361176;

ALTER TABLE dist_partitioned_table DROP CONSTRAINT constraint1 CASCADE;
DROP INDEX dist_partitioned_table_dist_col_partition_col_idx;

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

SELECT c.relname AS indexname
FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n, pg_catalog.pg_index i
WHERE  (i.indisvalid = false) AND i.indexrelid = c.oid AND c.relnamespace = n.oid AND n.nspname = 'fix_idx_names';

-- try with index on only partition
CREATE INDEX short_child ON ONLY p USING hash (dist_col);
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_1_port
-- index names are already correct, including inherited index for another_partition
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

DROP INDEX short_parent;
DROP INDEX short_child;
DROP TABLE another_partition;

-- try with expression indexes
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX expression_index ON dist_partitioned_table ((dist_col || ' ' || another_col));
-- try with statistics on index
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX statistics_on_index on dist_partitioned_table ((dist_col+another_col), (dist_col-another_col));
ALTER INDEX statistics_on_index ALTER COLUMN 1 SET STATISTICS 3737;
ALTER INDEX statistics_on_index ALTER COLUMN 2 SET STATISTICS 3737;

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_1_port
-- we have correct names
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

-- try with a table with no partitions
ALTER TABLE dist_partitioned_table DETACH PARTITION p;
ALTER TABLE dist_partitioned_table DETACH PARTITION partition_table_with_very_long_name;
DROP TABLE p;
DROP TABLE partition_table_with_very_long_name;

-- still dist_partitioned_table has indexes
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

-- this does nothing
SELECT fix_partition_shard_index_names('dist_partitioned_table'::regclass);

\c - - - :worker_1_port
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
DROP TABLE dist_partitioned_table;

-- add test with replication factor = 2
SET citus.shard_replication_factor TO 2;
SET citus.next_shard_id TO 910050;

CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('dist_partitioned_table', 'dist_col');

-- create a partition with a long name
CREATE TABLE partition_table_with_very_long_name PARTITION OF dist_partitioned_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

-- create an index on parent table
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX index_rep_factor_2 ON dist_partitioned_table USING btree (another_col, partition_col);

SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;

\c - - - :worker_2_port
-- index names are correct
-- shard id has been appended to all index names which didn't end in shard id
-- this goes in line with Citus's way of naming indexes of shards: always append shardid to the end
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;

-- test with role that is not superuser
SET client_min_messages TO warning;
SET citus.enable_ddl_propagation TO off;
CREATE USER user1;
RESET client_min_messages;
RESET citus.enable_ddl_propagation;

SET ROLE user1;
SELECT fix_partition_shard_index_names('fix_idx_names.dist_partitioned_table'::regclass);

RESET ROLE;
SET search_path TO fix_idx_names, public;
DROP TABLE dist_partitioned_table;

-- We can do any further operations (e.g. rename) on the indexes of partitions because
-- the index names on shards of partitions have Citus naming, hence are reachable
-- replicate scenario from above but this time with one shard so that this test isn't flaky

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 910030;

CREATE TABLE dist_partitioned_table (dist_col int, another_col int, partition_col timestamp) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('dist_partitioned_table', 'dist_col');
CREATE TABLE partition_table_with_very_long_name PARTITION OF dist_partitioned_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
CREATE TABLE p PARTITION OF dist_partitioned_table FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
-- SELECT fix_partition_shard_index_names('dist_partitioned_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX short ON dist_partitioned_table USING btree (another_col, partition_col);

-- rename works!
ALTER INDEX partition_table_with_very_long_na_another_col_partition_col_idx RENAME TO partition_table_with_very_long_name_idx;

-- we can drop index on detached partition
-- https://github.com/citusdata/citus/issues/5138
ALTER TABLE dist_partitioned_table DETACH PARTITION p;
DROP INDEX p_another_col_partition_col_idx;

\c - - - :worker_1_port
-- check that indexes have been renamed
-- and that index on p has been dropped (it won't appear)
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' AND tablename SIMILAR TO '%\_\d*' ORDER BY 1, 2;

\c - - - :master_port
SET search_path TO fix_idx_names, public;
DROP TABLE dist_partitioned_table;

-- test with citus local table
SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid=>0);
RESET client_min_messages;

CREATE TABLE date_partitioned_citus_local_table(
 measureid integer,
 eventdate date,
 measure_data jsonb) PARTITION BY RANGE(eventdate);
SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table');

CREATE TABLE partition_local_table PARTITION OF date_partitioned_citus_local_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

-- SELECT fix_partition_shard_index_names('date_partitioned_citus_local_table') will be executed
-- automatically at the end of the CREATE INDEX command
CREATE INDEX ON date_partitioned_citus_local_table USING btree(measureid);

-- check that index names are correct
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 'fix_idx_names' ORDER BY 1, 2;


-- creating a single object should only need to trigger fixing the single object
-- for example, if a partitioned table has already many indexes and we create a new
-- index, only the new index should be fixed

-- create only one shard & one partition so that the output easier to check
SET citus.next_shard_id TO 915000;

SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
CREATE TABLE parent_table (dist_col int, another_col int, partition_col timestamp, name text) PARTITION BY RANGE (partition_col);
SELECT create_distributed_table('parent_table', 'dist_col');
CREATE TABLE p1 PARTITION OF parent_table FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

CREATE INDEX i1 ON parent_table(dist_col);
CREATE INDEX i2 ON parent_table(dist_col);
CREATE INDEX i3 ON parent_table(dist_col);

SET citus.log_remote_commands TO ON;

-- only fix i4
CREATE INDEX i4 ON parent_table(dist_col);

-- only fix the index backing the pkey
ALTER TABLE parent_table ADD CONSTRAINT pkey_cst PRIMARY KEY (dist_col, partition_col);
ALTER TABLE parent_table ADD CONSTRAINT unique_cst UNIQUE (dist_col, partition_col);

RESET citus.log_remote_commands;

-- we should also be able to alter/drop these indexes
ALTER INDEX i4 RENAME TO i4_renamed;
ALTER INDEX p1_dist_col_idx3 RENAME TO p1_dist_col_idx3_renamed;
ALTER INDEX p1_pkey RENAME TO p1_pkey_renamed;
ALTER INDEX p1_dist_col_partition_col_key RENAME TO p1_dist_col_partition_col_key_renamed;
ALTER INDEX p1_dist_col_idx RENAME TO p1_dist_col_idx_renamed;

-- should be able to create a new partition that is columnar
SET citus.log_remote_commands TO ON;
CREATE TABLE p2(dist_col int NOT NULL, another_col int, partition_col timestamp NOT NULL, name text) USING columnar;
ALTER TABLE parent_table ATTACH PARTITION p2 FOR VALUES FROM ('2019-01-01') TO ('2020-01-01');
RESET citus.log_remote_commands;

DROP INDEX i4_renamed CASCADE;
ALTER TABLE parent_table DROP CONSTRAINT pkey_cst CASCADE;
ALTER TABLE parent_table DROP CONSTRAINT unique_cst CASCADE;

DROP SCHEMA fix_idx_names CASCADE;
SELECT citus_remove_node('localhost', :master_port);
SELECT run_command_on_workers($$ DROP SCHEMA IF EXISTS fix_idx_names CASCADE $$);
