-- Test creation of mx tables and metadata syncing

-- Temporarily disable automatic 2PC recovery
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

-- pg 10 and pg 11 has different error messages for acl checks
-- so catch them and print only one type of error message to prevent
-- multiple output test files
CREATE OR REPLACE FUNCTION raise_failed_aclcheck(query text) RETURNS void AS $$
BEGIN
    EXECUTE query;
    EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE 'must be owner of%' THEN
        RAISE 'must be owner of the object';
    END IF;
END;
$$LANGUAGE plpgsql;

-- get rid of the previously created entries in pg_dist_transaction
-- for the sake of getting consistent results in this test file
TRUNCATE pg_dist_transaction;

CREATE TABLE distributed_mx_table (
    key text primary key,
    value jsonb,
    some_val bigserial
);
CREATE INDEX ON distributed_mx_table USING GIN (value);

SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

SET citus.shard_count TO 4;

SELECT create_distributed_table('distributed_mx_table', 'key');

-- Verify that we've logged commit records
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that the metadata transactions have been committed
SELECT recover_prepared_transactions();

-- Verify that the commit records have been removed
SELECT count(*) FROM pg_dist_transaction;

\c - - :public_worker_1_host :worker_1_port

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='distributed_mx_table'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'distributed_mx_table_pkey'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'distributed_mx_table_value_idx'::regclass;

SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'distributed_mx_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'distributed_mx_table'::regclass;

\c - - :public_worker_2_host :worker_2_port

SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='distributed_mx_table'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'distributed_mx_table_pkey'::regclass;
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'distributed_mx_table_value_idx'::regclass;

SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'distributed_mx_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'distributed_mx_table'::regclass;

-- Create a table and then roll back the transaction
\c - - :master_host :master_port
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

BEGIN;
CREATE TABLE should_not_exist (
    key text primary key,
    value jsonb
);
SELECT create_distributed_table('should_not_exist', 'key');
ABORT;

-- Verify that the table does not exist on the worker
\c - - :public_worker_1_host :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_not_exist';

-- Ensure that we don't allow prepare on a metadata transaction
\c - - :master_host :master_port
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

BEGIN;
CREATE TABLE should_not_exist (
    key text primary key,
    value jsonb
);
SELECT create_distributed_table('should_not_exist', 'key');
PREPARE TRANSACTION 'this_should_fail';

-- now show that we can create tables and schemas withing a single transaction
BEGIN;
CREATE SCHEMA IF NOT EXISTS citus_mx_schema_for_xacts;
SET search_path TO citus_mx_schema_for_xacts;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 1;

CREATE TABLE objects_for_xacts (
    id bigint PRIMARY KEY,
    name text NOT NULL
);
SELECT create_distributed_table('objects_for_xacts', 'id');

COMMIT;

-- see that the table actually created and distributed
\c - - :public_worker_1_host :worker_1_port
SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'citus_mx_schema_for_xacts.objects_for_xacts'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'citus_mx_schema_for_xacts.objects_for_xacts'::regclass;

\c - - :master_host :master_port
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

-- now show that we can rollback on creating mx table, but shards remain....
BEGIN;
CREATE SCHEMA IF NOT EXISTS citus_mx_schema_for_xacts;
SET search_path TO citus_mx_schema_for_xacts;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 2;

CREATE TABLE objects_for_xacts2 (
    id bigint PRIMARY KEY,
    name text NOT NULL
);
SELECT create_distributed_table('objects_for_xacts2', 'id');

ROLLBACK;

-- show that the table not exists on the coordinator
SELECT count(*) FROM pg_tables WHERE tablename = 'objects_for_xacts2' and schemaname = 'citus_mx_schema_for_xacts';

\c - - :public_worker_1_host :worker_1_port

-- the distributed table not exists on the worker node
SELECT count(*) FROM pg_tables WHERE tablename = 'objects_for_xacts2' and schemaname = 'citus_mx_schema_for_xacts';
-- shard also does not exist since we create shards in a transaction
SELECT count(*) FROM pg_tables WHERE tablename LIKE 'objects_for_xacts2_%' and schemaname = 'citus_mx_schema_for_xacts';

-- make sure that master_drop_all_shards does not work from the worker nodes
SELECT master_drop_all_shards('citus_mx_schema_for_xacts.objects_for_xacts'::regclass, 'citus_mx_schema_for_xacts', 'objects_for_xacts');

-- Ensure pg_dist_transaction is empty for test
SELECT recover_prepared_transactions();

-- Create some "fake" prepared transactions to recover
\c - - :public_worker_1_host :worker_1_port

BEGIN;
CREATE TABLE should_abort (value int);
PREPARE TRANSACTION 'citus_0_should_abort';

BEGIN;
CREATE TABLE should_commit (value int);
PREPARE TRANSACTION 'citus_0_should_commit';

BEGIN;
CREATE TABLE should_be_sorted_into_middle (value int);
PREPARE TRANSACTION 'citus_0_should_be_sorted_into_middle';

\c - - :master_host :master_port
-- Add "fake" pg_dist_transaction records and run recovery
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport = :worker_1_port \gset
INSERT INTO pg_dist_transaction VALUES (:worker_1_group, 'citus_0_should_commit');
INSERT INTO pg_dist_transaction VALUES (:worker_1_group, 'citus_0_should_be_forgotten');

SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that transactions were correctly rolled forward
\c - - :public_worker_1_host :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_abort';
SELECT count(*) FROM pg_tables WHERE tablename = 'should_commit';

\c - - :master_host :master_port

CREATE USER no_access_mx;
SELECT run_command_on_workers($$CREATE USER no_access_mx;$$);

SET ROLE no_access_mx;

SELECT raise_failed_aclcheck($$
    DROP TABLE distributed_mx_table;
$$);

SELECT raise_failed_aclcheck($$
    SELECT master_remove_distributed_table_metadata_from_workers('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');
 $$);

SELECT raise_failed_aclcheck($$
    SELECT master_drop_all_shards('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');
$$);
SELECT raise_failed_aclcheck($$
    SELECT master_remove_partition_metadata('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');
$$);
SELECT raise_failed_aclcheck($$
    SELECT master_drop_sequences(ARRAY['public.distributed_mx_table_some_val_seq']);
$$);
SELECT raise_failed_aclcheck($$
    SELECT master_drop_sequences(ARRAY['distributed_mx_table_some_val_seq']);
$$);

SELECT master_drop_sequences(ARRAY['non_existing_schema.distributed_mx_table_some_val_seq']);
SELECT master_drop_sequences(ARRAY['']);
SELECT master_drop_sequences(ARRAY['public.']);
SELECT master_drop_sequences(ARRAY['public.distributed_mx_table_some_val_seq_not_existing']);

-- make sure that we can drop unrelated tables/sequences
CREATE TABLE unrelated_table(key serial);
DROP TABLE unrelated_table;

-- doesn't error out but it has no effect, so no need to error out
SELECT master_drop_sequences(NULL);

\c - postgres - :master_port

-- finally make sure that the sequence remains
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='distributed_mx_table'::regclass;

\c - no_access_mx - :worker_1_port

-- see the comment in the top of the file
CREATE OR REPLACE FUNCTION raise_failed_aclcheck(query text) RETURNS void AS $$
BEGIN
    EXECUTE query;
    EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE 'must be owner of%' THEN
        RAISE 'must be owner of the object';
    END IF;
END;
$$LANGUAGE plpgsql;

SELECT raise_failed_aclcheck($$
    DROP TABLE distributed_mx_table;
$$);

SELECT raise_failed_aclcheck($$
    SELECT master_remove_distributed_table_metadata_from_workers('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');
 $$);

SELECT raise_failed_aclcheck($$
    SELECT master_drop_sequences(ARRAY['public.distributed_mx_table_some_val_seq']);
$$);

SELECT master_drop_all_shards('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');
SELECT master_remove_partition_metadata('distributed_mx_table'::regclass, 'public', 'distributed_mx_table');

-- make sure that we can drop unrelated tables/sequences
CREATE TABLE unrelated_table(key serial);
DROP TABLE unrelated_table;

\c - postgres - :worker_1_port

-- finally make sure that the sequence remains
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='distributed_mx_table'::regclass;

-- Resume ordinary recovery
\c - postgres - :master_port
ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();
