-- Test creation of mx tables and metadata syncing

-- get rid of the previously created entries in pg_dist_transaction
-- for the sake of getting consistent results in this test file
SELECT recover_prepared_transactions();

CREATE TABLE distributed_mx_table (
    key text primary key,
    value jsonb
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

\c - - - :worker_1_port

\d distributed_mx_table

SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'distributed_mx_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'distributed_mx_table'::regclass;

\c - - - :worker_2_port

\d distributed_mx_table

SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'distributed_mx_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'distributed_mx_table'::regclass;

-- Create a table and then roll back the transaction
\c - - - :master_port
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
\c - - - :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_not_exist';

-- Ensure that we don't allow prepare on a metadata transaction
\c - - - :master_port
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
\c - - - :worker_1_port
SELECT repmodel FROM pg_dist_partition
WHERE logicalrelid = 'citus_mx_schema_for_xacts.objects_for_xacts'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'citus_mx_schema_for_xacts.objects_for_xacts'::regclass;

\c - - - :master_port
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

\c - - - :worker_1_port

-- the distributed table not exists on the worker node
SELECT count(*) FROM pg_tables WHERE tablename = 'objects_for_xacts2' and schemaname = 'citus_mx_schema_for_xacts';
-- but the shard exists since we do not create shards in a transaction
SELECT count(*) FROM pg_tables WHERE tablename LIKE 'objects_for_xacts2_%' and schemaname = 'citus_mx_schema_for_xacts';

-- make sure that master_drop_all_shards does not work from the worker nodes
SELECT master_drop_all_shards('citus_mx_schema_for_xacts.objects_for_xacts'::regclass, 'citus_mx_schema_for_xacts', 'objects_for_xacts');

-- Ensure pg_dist_transaction is empty for test
SELECT recover_prepared_transactions();

-- Create some "fake" prepared transactions to recover
\c - - - :worker_1_port

BEGIN;
CREATE TABLE should_abort (value int);
PREPARE TRANSACTION 'citus_0_should_abort';

BEGIN;
CREATE TABLE should_commit (value int);
PREPARE TRANSACTION 'citus_0_should_commit';

BEGIN;
CREATE TABLE should_be_sorted_into_middle (value int);
PREPARE TRANSACTION 'citus_0_should_be_sorted_into_middle';

\c - - - :master_port
-- Add "fake" pg_dist_transaction records and run recovery
INSERT INTO pg_dist_transaction VALUES (12, 'citus_0_should_commit');
INSERT INTO pg_dist_transaction VALUES (12, 'citus_0_should_be_forgotten');

SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that transactions were correctly rolled forward
\c - - - :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_abort';
SELECT count(*) FROM pg_tables WHERE tablename = 'should_commit';
