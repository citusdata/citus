CREATE SCHEMA metadata_sync_helpers;
SET search_path TO metadata_sync_helpers;
SET citus.next_shard_id TO 1420000;
SET citus.next_placement_id TO 1500000;

-- supress notice messages to make sure that the tests
-- do not diverge with enterprise
SET client_min_messages TO WARNING;
CREATE ROLE metadata_sync_helper_role WITH LOGIN;
GRANT ALL ON SCHEMA metadata_sync_helpers TO metadata_sync_helper_role;
RESET client_min_messages;

\c - metadata_sync_helper_role -
SET search_path TO metadata_sync_helpers;
CREATE TABLE test(col_1 int);

-- not in a distributed transaction
SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
SELECT citus_internal_update_relation_colocation ('test'::regclass, 1);

-- in a distributed transaction, but the application name is not Citus
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- in a distributed transaction and the application name is Citus
-- but we are on the coordinator, so still not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- connect back as super user, and then connect to the worker
-- with the superuser to make sure we can ingest metadata with
-- a regular user under the certain conditions
\c - postgres -

-- we don't need the table/schema anymore
SET client_min_messages TO ERROR;
DROP SCHEMA metadata_sync_helpers CASCADE;
DROP ROLE metadata_sync_helper_role;

\c - - - :worker_1_port

CREATE SCHEMA metadata_sync_helpers;
SET search_path TO metadata_sync_helpers;

CREATE TABLE test(col_1 int, col_2 int);

-- supress notice messages to make sure that the tests
-- do not diverge with enterprise
SET client_min_messages TO WARNING;
SET citus.enable_ddl_propagation TO OFF;
CREATE ROLE metadata_sync_helper_role WITH LOGIN;
GRANT ALL ON SCHEMA metadata_sync_helpers TO metadata_sync_helper_role;
RESET client_min_messages;
RESET citus.enable_ddl_propagation;

-- connect back with the regular user
\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- in a distributed transaction and the application name is Citus
-- and we are on the worker, still not allowed because the user is not
-- owner of the table test
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- we do not own the relation
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_update_relation_colocation ('test'::regclass, 10);
ROLLBACK;

-- finally, a user can only add its own tables to the metadata
CREATE TABLE test_2(col_1 int, col_2 int);
CREATE TABLE test_3(col_1 int, col_2 int);
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'metadata_sync_helpers.test_2'::regclass;
ROLLBACK;

-- fails because there is no X distribution method
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 's');
ROLLBACK;

-- fails because there is the column does not exist
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'non_existing_col', 0, 's');
ROLLBACK;

--- fails because we do not allow NULL parameters
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
		SET application_name to 'citus';
		SELECT citus_internal_add_partition_metadata (NULL, 'h', 'non_existing_col', 0, 's');
ROLLBACK;

-- fails because colocationId cannot be negative
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', -1, 's');
ROLLBACK;

-- fails because there is no X replication model
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 'X');
ROLLBACK;

-- the same table cannot be added twice, that is enforced by a primary key
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- the same table cannot be added twice, that is enforced by a primary key even if distribution key changes
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_2', 0, 's');
ROLLBACK;

-- hash distributed table cannot have NULL distribution key
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', NULL, 0, 's');
ROLLBACK;

-- even if metadata_sync_helper_role is not owner of the table test
-- the operator allowed metadata_sync_helper_role user to skip
-- checks, such as wrong partition method can be set
-- connect back with the regular user
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;

ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'metadata_sync_helper_role';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 's');
ROLLBACK;

-- should throw error even if we skip the checks, there are no such nodes
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420007, 10000, 11111);
ROLLBACK;

-- non-existing users should fail to pass the checks
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'non_existing_user';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 's');
ROLLBACK;

\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- we also enforce some checks on the reference tables
-- cannot add because reference tables cannot have distribution column
CREATE TABLE test_ref(col_1 int, col_2 int);
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', 'col_1', 0, 's');
ROLLBACK;

-- non-valid replication model
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 'A');
ROLLBACK;

-- not-matching replication model for reference table
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 'c');
ROLLBACK;

-- add entry for super user table
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
CREATE TABLE super_user_table(col_1 int);
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('super_user_table'::regclass, 'h', 'col_1', 0, 's');
COMMIT;

-- now, lets check shard metadata

-- the user is only allowed to add a shard for add a table which they do not own

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('super_user_table'::regclass, 1420000::bigint, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- the user is only allowed to add a shard for add a table which is in pg_dist_partition
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- ok, now add the table to the pg_dist_partition
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 250, 's');
	SELECT citus_internal_add_partition_metadata ('test_3'::regclass, 'h', 'col_1', 251, 's');
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 't');
COMMIT;

-- we can update to a non-existing colocation group (e.g., colocate_with:=none)
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_update_relation_colocation ('test_2'::regclass, 1231231232);
ROLLBACK;

-- invalid shard ids are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, -1, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- invalid storage types are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000, 'X'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- NULL shard ranges are not allowed for hash distributed tables
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000, 't'::"char", NULL, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- non-integer shard ranges are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", 'non-int'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- shardMinValue should be smaller than shardMaxValue
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '-1610612737'::text, '-2147483648'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- we do not allow overlapping shards for the same table
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '10'::text, '20'::text),
				   ('test_2'::regclass, 1420001::bigint, 't'::"char", '20'::text, '30'::text),
				   ('test_2'::regclass, 1420002::bigint, 't'::"char", '10'::text, '50'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

\c - postgres - :worker_1_port

-- we do not allow wrong partmethod
-- so manually insert wrong partmethod for the sake of the test
SET search_path TO metadata_sync_helpers;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	UPDATE pg_dist_partition SET partmethod = 'X';
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '10'::text, '20'::text),
				   ('test_2'::regclass, 1420001::bigint, 't'::"char", '20'::text, '30'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- we do not allow NULL shardMinMax values
-- so manually insert wrong partmethod for the sake of the test
SET search_path TO metadata_sync_helpers;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '10'::text, '20'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
	-- manually ingest NULL values, otherwise not likely unless metadata is corrupted
	UPDATE pg_dist_shard SET shardminvalue = NULL WHERE shardid = 1420000;
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420001::bigint, 't'::"char", '20'::text, '30'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- now, add few shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '11'::text, '20'::text),
				   ('test_2'::regclass, 1420001::bigint, 't'::"char", '21'::text, '30'::text),
				   ('test_2'::regclass, 1420002::bigint, 't'::"char", '31'::text, '40'::text),
				   ('test_2'::regclass, 1420003::bigint, 't'::"char", '41'::text, '50'::text),
				   ('test_2'::regclass, 1420004::bigint, 't'::"char", '51'::text, '60'::text),
				   ('test_2'::regclass, 1420005::bigint, 't'::"char", '61'::text, '70'::text),
				   ('test_3'::regclass, 1420008::bigint, 't'::"char", '11'::text, '20'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
COMMIT;

-- we cannot mark these two tables colocated because they are not colocated
BEGIN;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

-- now, add few more shards for test_3 to make it colocated with test_2
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_3'::regclass, 1420009::bigint, 't'::"char", '21'::text, '30'::text),
				   ('test_3'::regclass, 1420010::bigint, 't'::"char", '31'::text, '40'::text),
				   ('test_3'::regclass, 1420011::bigint, 't'::"char", '41'::text, '50'::text),
				   ('test_3'::regclass, 1420012::bigint, 't'::"char", '51'::text, '60'::text),
				   ('test_3'::regclass, 1420013::bigint, 't'::"char", '61'::text, '70'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
COMMIT;

-- shardMin/MaxValues should be NULL for reference tables
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_ref'::regclass, 1420003::bigint, 't'::"char", '-1610612737'::text, NULL))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- reference tables cannot have multiple shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_ref'::regclass, 1420006::bigint, 't'::"char", NULL, NULL),
				   ('test_ref'::regclass, 1420007::bigint, 't'::"char", NULL, NULL))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- finally, add a shard for reference tables
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_ref'::regclass, 1420006::bigint, 't'::"char", NULL, NULL))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
COMMIT;

\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- and a shard for the superuser table
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('super_user_table'::regclass, 1420007::bigint, 't'::"char", '11'::text, '20'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
COMMIT;

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- now, check placement metadata

-- shard does not exist
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (-10, 1, 0::bigint, 1::int, 1500000::bigint))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- invalid placementid
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1420000, 1, 0::bigint, 1::int, -10))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- non-existing shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1430100, 1, 0::bigint, 1::int, 10))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- invalid shard state
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1420000, 10, 0::bigint, 1::int, 1500000))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- non-existing node with non-existing node-id 123123123
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES ( 1420000, 1, 0::bigint, 123123123::int, 1500000))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- create a volatile function that returns the local node id
CREATE OR REPLACE FUNCTION get_node_id()
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
	SELECT
		groupid into localGroupId
	FROM
		pg_dist_node
	WHERE
		nodeport = 57637 AND nodename = 'localhost' AND isactive AND nodecluster = 'default';
  RETURN localGroupId;
END; $$ language plpgsql;

-- fails because we ingest more placements for the same shards to the same worker node
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1420000, 1, 0::bigint, get_node_id(), 1500000),
				(1420000, 1, 0::bigint, get_node_id(), 1500001))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- shard is not owned by us
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1420007, 1, 0::bigint, get_node_id(), 1500000))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- sucessfully add placements
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (1420000, 1, 0::bigint, get_node_id(), 1500000),
				(1420001, 1, 0::bigint, get_node_id(), 1500001),
				(1420002, 1, 0::bigint, get_node_id(), 1500002),
				(1420003, 1, 0::bigint, get_node_id(), 1500003),
				(1420004, 1, 0::bigint, get_node_id(), 1500004),
				(1420005, 1, 0::bigint, get_node_id(), 1500005),
				(1420008, 1, 0::bigint, get_node_id(), 1500006),
				(1420009, 1, 0::bigint, get_node_id(), 1500007),
				(1420010, 1, 0::bigint, get_node_id(), 1500008),
				(1420011, 1, 0::bigint, get_node_id(), 1500009),
				(1420012, 1, 0::bigint, get_node_id(), 1500010),
				(1420013, 1, 0::bigint, get_node_id(), 1500011))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
COMMIT;

-- we should be able to colocate both tables now
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

-- try to update placements

-- fails because we are trying to update it to non-existing node
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420000, get_node_id(), get_node_id()+1000);
COMMIT;

-- fails because the source node doesn't contain the shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420000, get_node_id()+10000, get_node_id());
COMMIT;

-- fails because shard does not exist
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(0, get_node_id(), get_node_id()+1);
COMMIT;

-- fails because none-existing shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(213123123123, get_node_id(), get_node_id()+1);
COMMIT;

-- fails because we do not own the shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420007, get_node_id(), get_node_id()+1);
COMMIT;

-- the user only allowed to delete their own shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(shardid)
		AS (VALUES (1420007))
	SELECT citus_internal_delete_shard_metadata(shardid) FROM shard_data;
ROLLBACK;

-- the user only allowed to delete shards in a distributed transaction
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(shardid)
		AS (VALUES (1420007))
	SELECT citus_internal_delete_shard_metadata(shardid) FROM shard_data;
ROLLBACK;

-- the user cannot delete non-existing shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(shardid)
		AS (VALUES (1420100))
	SELECT citus_internal_delete_shard_metadata(shardid) FROM shard_data;
ROLLBACK;


-- sucessfully delete shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;

	SELECT count(*) FROM pg_dist_shard WHERE shardid = 1420000;
	SELECT count(*) FROM pg_dist_placement WHERE shardid = 1420000;

	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	WITH shard_data(shardid)
		AS (VALUES (1420000))
	SELECT citus_internal_delete_shard_metadata(shardid) FROM shard_data;

	SELECT count(*) FROM pg_dist_shard WHERE shardid = 1420000;
	SELECT count(*) FROM pg_dist_placement WHERE shardid = 1420000;
ROLLBACK;


-- we'll do some metadata changes to trigger some error cases
-- so connect as superuser
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	-- with an ugly trick, update the repmodel
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET repmodel = 't'
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;


BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	-- with an ugly trick, update the vartype of table from int to bigint
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET partkey = '{VAR :varno 1 :varattno 1 :vartype 20 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 1 :location -1}'
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	-- with an ugly trick, update the partmethod of the table to not-valid
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET partmethod = ''
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	-- with an ugly trick, update the partmethod of the table to not-valid
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET partmethod = 'a'
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

-- colocated hash distributed table should have the same dist key columns
CREATE TABLE test_5(int_col int, text_col text);
CREATE TABLE test_6(int_col int, text_col text);

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_5'::regclass, 'h', 'int_col', 500, 's');
	SELECT citus_internal_add_partition_metadata ('test_6'::regclass, 'h', 'text_col', 500, 's');
ROLLBACK;


BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	CREATE COLLATION collation_t1 (provider = icu, locale = 'de-u-co-phonebk');
	CREATE COLLATION caseinsensitive (provider = icu, locale = 'und-u-ks-level2');

	-- colocated hash distributed table should have the same dist key collations
	CREATE TABLE test_7(int_col int, text_col text COLLATE "collation_t1");
	CREATE TABLE test_8(int_col int, text_col text COLLATE "caseinsensitive");

	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_7'::regclass, 'h', 'text_col', 500, 's');
	SELECT citus_internal_add_partition_metadata ('test_8'::regclass, 'h', 'text_col', 500, 's');
ROLLBACK;

-- we don't need the table/schema anymore
-- connect back as super user to drop everything
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
-- remove the manually generated metadata
DELETE FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass));
DELETE FROM pg_dist_shard WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass);
DELETE FROM pg_dist_partition WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass);

SET client_min_messages TO ERROR;
SET citus.enable_ddl_propagation TO OFF;
DROP OWNED BY metadata_sync_helper_role;
DROP ROLE metadata_sync_helper_role;
DROP SCHEMA metadata_sync_helpers CASCADE;
