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
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;
\c - postgres -
\c - - - :worker_1_port

SET search_path TO metadata_sync_helpers;

CREATE TABLE test(col_1 int, col_2 int);

-- connect back with the regular user
\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

-- in a distributed transaction and the application name is Citus
-- and we are on the worker, still not allowed because the user is not
-- owner of the table test
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- we do not own the relation
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_update_relation_colocation ('test'::regclass, 10);
ROLLBACK;

-- finally, a user can only add its own tables to the metadata
CREATE TABLE test_2(col_1 int, col_2 int);
CREATE TABLE test_3(col_1 int, col_2 int);
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'metadata_sync_helpers.test_2'::regclass;
ROLLBACK;

-- also works if done by the rebalancer
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_rebalancer gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- application_name with incorrect gpid
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=not a correct gpid';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- also faills if done by the rebalancer
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_rebalancer gpid=not a correct gpid';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- application_name with suffix is ok (e.g. pgbouncer might add this)
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001 - from 10.12.14.16:10370';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- application_name with empty gpid
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- empty application_name
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to '';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- application_name with incorrect prefix
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- fails because there is no X distribution method
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 's');
ROLLBACK;

-- fails because there is the column does not exist
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'non_existing_col', 0, 's');
ROLLBACK;

--- fails because we do not allow NULL parameters
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
		SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
		SET application_name to 'citus_internal gpid=10000000001';
		SELECT citus_internal_add_partition_metadata (NULL, 'h', 'non_existing_col', 0, 's');
ROLLBACK;

-- fails because colocationId cannot be negative
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', -1, 's');
ROLLBACK;

-- fails because there is no X replication model
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 'X');
ROLLBACK;

-- the same table cannot be added twice, that is enforced by a primary key
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
ROLLBACK;

-- the same table cannot be added twice, that is enforced by a primary key even if distribution key changes
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 0, 's');
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_2', 0, 's');
ROLLBACK;

-- hash distributed table cannot have NULL distribution key
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'X', 'col_1', 0, 's');
ROLLBACK;

-- should throw error even if we skip the checks, there are no such nodes
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', 'col_1', 0, 's');
ROLLBACK;

-- non-valid replication model
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 'A');
ROLLBACK;

-- not-matching replication model for reference table
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 'c');
ROLLBACK;

-- add entry for super user table
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
CREATE TABLE super_user_table(col_1 int);
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('super_user_table'::regclass, 'h', 'col_1', 0, 's');
COMMIT;

-- now, lets check shard metadata

-- the user is only allowed to add a shard for add a table which they do not own

\c - metadata_sync_helper_role - :worker_1_port
SET search_path TO metadata_sync_helpers;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('super_user_table'::regclass, 1420000::bigint, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- the user is only allowed to add a shard for add a table which is in pg_dist_partition
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- ok, now add the table to the pg_dist_partition
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_add_partition_metadata ('test_2'::regclass, 'h', 'col_1', 250, 's');
	SELECT citus_internal_add_partition_metadata ('test_3'::regclass, 'h', 'col_1', 251, 's');
	SELECT citus_internal_add_partition_metadata ('test_ref'::regclass, 'n', NULL, 0, 't');
COMMIT;

-- we can update to a non-existing colocation group (e.g., colocate_with:=none)
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_update_relation_colocation ('test_2'::regclass, 1231231232);
ROLLBACK;

-- invalid shard ids are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, -1, 't'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- invalid storage types are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000, 'X'::"char", '-2147483648'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- NULL shard ranges are not allowed for hash distributed tables
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000, 't'::"char", NULL, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- non-integer shard ranges are not allowed
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", 'non-int'::text, '-1610612737'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- shardMinValue should be smaller than shardMaxValue
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '-1610612737'::text, '-2147483648'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- we do not allow overlapping shards for the same table
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_2'::regclass, 1420000::bigint, 't'::"char", '10'::text, '20'::text),
				   ('test_2'::regclass, 1420001::bigint, 't'::"char", '20'::text, '30'::text),
				   ('test_2'::regclass, 1420002::bigint, 't'::"char", '10'::text, '50'::text))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- Now let's check valid pg_dist_object updates

-- check with non-existing object type
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('non_existing_type', ARRAY['non_existing_user']::text[], ARRAY[]::text[], -1, 0, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

-- check the sanity of distributionArgumentIndex and colocationId
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('role', ARRAY['metadata_sync_helper_role']::text[], ARRAY[]::text[], -100, 0, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('role', ARRAY['metadata_sync_helper_role']::text[], ARRAY[]::text[], -1, -1, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

-- check with non-existing object
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('role', ARRAY['non_existing_user']::text[], ARRAY[]::text[], -1, 0, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

-- since citus_internal_add_object_metadata is strict function returns NULL
-- if any parameter is NULL
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('role', ARRAY['metadata_sync_helper_role']::text[], ARRAY[]::text[], 0, NULL::int, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

\c - postgres - :worker_1_port

-- Show that citus_internal_add_object_metadata only works for object types
-- which is known how to distribute
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse

    CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    FUNCTION = int4eq
    );

	SET ROLE metadata_sync_helper_role;
	WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
		AS (VALUES ('operator', ARRAY['===']::text[], ARRAY['int','int']::text[], -1, 0, false))
	SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

-- Show that citus_internal_add_object_metadata checks the priviliges
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
    SET application_name to 'citus_internal gpid=10000000001';
    \set VERBOSITY terse

	SET citus.enable_ddl_propagation TO OFF;
    CREATE FUNCTION distribution_test_function(int) RETURNS int
    AS $$ SELECT $1 $$
    LANGUAGE SQL;

    SET ROLE metadata_sync_helper_role;
    WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
        AS (VALUES ('function', ARRAY['distribution_test_function']::text[], ARRAY['integer']::text[], -1, 0, false))
    SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
    SET application_name to 'citus_internal gpid=10000000001';
    SET citus.enable_ddl_propagation TO OFF;
    \set VERBOSITY terse

    CREATE TYPE distributed_test_type AS (a int, b int);

    SET ROLE metadata_sync_helper_role;
    WITH distributed_object_data(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation)
        AS (VALUES ('type', ARRAY['distributed_test_type']::text[], ARRAY[]::text[], -1, 0, false))
    SELECT citus_internal_add_object_metadata(typetext, objnames, objargs, distargumentindex, colocationid, force_delegation) FROM distributed_object_data;
ROLLBACK;

-- we do not allow wrong partmethod
-- so manually insert wrong partmethod for the sake of the test
SET search_path TO metadata_sync_helpers;
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

-- now, add few more shards for test_3 to make it colocated with test_2
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_ref'::regclass, 1420003::bigint, 't'::"char", '-1610612737'::text, NULL))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- reference tables cannot have multiple shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(relationname, shardid, storagetype, shardminvalue, shardmaxvalue)
		AS (VALUES ('test_ref'::regclass, 1420006::bigint, 't'::"char", NULL, NULL),
				   ('test_ref'::regclass, 1420007::bigint, 't'::"char", NULL, NULL))
	SELECT citus_internal_add_shard_metadata(relationname, shardid, storagetype, shardminvalue, shardmaxvalue) FROM shard_data;
ROLLBACK;

-- finally, add a shard for reference tables
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardstate, shardlength, groupid, placementid) AS
		(VALUES (-10, 1, 0::bigint, 1::int, 1500000::bigint))
	SELECT citus_internal_add_placement_metadata(shardid, shardstate, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- invalid placementid
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES (1420000, 0::bigint, 1::int, -10))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- non-existing shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES (1430100, 0::bigint, 1::int, 10))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- non-existing node with non-existing node-id 123123123
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES ( 1420000, 0::bigint, 123123123::int, 1500000))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- create a volatile function that returns the local node id
SET citus.enable_metadata_sync TO OFF;
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
RESET citus.enable_metadata_sync;

-- fails because we ingest more placements for the same shards to the same worker node
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES (1420000, 0::bigint, get_node_id(), 1500000),
				(1420000, 0::bigint, get_node_id(), 1500001))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- shard is not owned by us
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES (1420007, 0::bigint, get_node_id(), 1500000))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
ROLLBACK;

-- sucessfully add placements
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH placement_data(shardid, shardlength, groupid, placementid) AS
		(VALUES (1420000, 0::bigint, get_node_id(), 1500000),
				(1420001, 0::bigint, get_node_id(), 1500001),
				(1420002, 0::bigint, get_node_id(), 1500002),
				(1420003, 0::bigint, get_node_id(), 1500003),
				(1420004, 0::bigint, get_node_id(), 1500004),
				(1420005, 0::bigint, get_node_id(), 1500005),
				(1420008, 0::bigint, get_node_id(), 1500006),
				(1420009, 0::bigint, get_node_id(), 1500007),
				(1420010, 0::bigint, get_node_id(), 1500008),
				(1420011, 0::bigint, get_node_id(), 1500009),
				(1420012, 0::bigint, get_node_id(), 1500010),
				(1420013, 0::bigint, get_node_id(), 1500011))
	SELECT citus_internal_add_placement_metadata(shardid, shardlength, groupid, placementid) FROM placement_data;
COMMIT;

-- we should be able to colocate both tables now
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

-- try to update placements

-- fails because we are trying to update it to non-existing node
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420000, get_node_id(), get_node_id()+1000);
COMMIT;

-- fails because the source node doesn't contain the shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420000, get_node_id()+10000, get_node_id());
COMMIT;

-- fails because shard does not exist
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(0, get_node_id(), get_node_id()+1);
COMMIT;

-- fails because none-existing shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(213123123123, get_node_id(), get_node_id()+1);
COMMIT;

-- fails because we do not own the shard
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_update_placement_metadata(1420007, get_node_id(), get_node_id()+1);
COMMIT;

-- the user only allowed to delete their own shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	WITH shard_data(shardid)
		AS (VALUES (1420007))
	SELECT citus_internal_delete_shard_metadata(shardid) FROM shard_data;
ROLLBACK;

-- the user cannot delete non-existing shards
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	-- with an ugly trick, update the repmodel
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET repmodel = 't'
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;


BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	-- with an ugly trick, update the vartype of table from int to bigint
	-- so that making two tables colocated fails

    -- include varnullingrels for PG16
    SHOW server_version \gset
    SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
    \gset
    \if :server_version_ge_16
    UPDATE pg_dist_partition SET partkey = '{VAR :varno 1 :varattno 1 :vartype 20 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 1 :varnoold 1 :varoattno 1 :location -1}'
	WHERE logicalrelid = 'test_2'::regclass;
    \else
	UPDATE pg_dist_partition SET partkey = '{VAR :varno 1 :varattno 1 :vartype 20 :vartypmod -1 :varcollid 0 :varlevelsup 1 :varnoold 1 :varoattno 1 :location -1}'
	WHERE logicalrelid = 'test_2'::regclass;
    \endif

	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	-- with an ugly trick, update the partmethod of the table to not-valid
	-- so that making two tables colocated fails
	UPDATE pg_dist_partition SET partmethod = ''
	WHERE logicalrelid = 'test_2'::regclass;
	SELECT citus_internal_update_relation_colocation('test_2'::regclass, 251);
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
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
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_5'::regclass, 'h', 'int_col', 500, 's');
	SELECT citus_internal_add_partition_metadata ('test_6'::regclass, 'h', 'text_col', 500, 's');
ROLLBACK;


BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SET citus.enable_ddl_propagation TO OFF;
	CREATE COLLATION collation_t1 (provider = icu, locale = 'de-u-co-phonebk');
	CREATE COLLATION caseinsensitive (provider = icu, locale = 'und-u-ks-level2');

	-- colocated hash distributed table should have the same dist key collations
	CREATE TABLE test_7(int_col int, text_col text COLLATE "collation_t1");
	CREATE TABLE test_8(int_col int, text_col text COLLATE "caseinsensitive");

	SELECT assign_distributed_transaction_id(0, 8, '2021-07-09 15:41:55.542377+02');
	SET application_name to 'citus_internal gpid=10000000001';
	\set VERBOSITY terse
	SELECT citus_internal_add_partition_metadata ('test_7'::regclass, 'h', 'text_col', 500, 's');
	SELECT citus_internal_add_partition_metadata ('test_8'::regclass, 'h', 'text_col', 500, 's');
ROLLBACK;

-- we don't need the table/schema anymore
-- connect back as super user to drop everything
\c - postgres - :worker_1_port
SET search_path TO metadata_sync_helpers;
-- remove the manually generated metadata
DELETE FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass, 'test_3'::regclass, 'super_user_table'::regclass));
DELETE FROM pg_dist_shard WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass, 'test_3'::regclass, 'super_user_table'::regclass);
DELETE FROM pg_dist_partition WHERE logicalrelid IN ('test_ref'::regclass, 'test_2'::regclass, 'test_3'::regclass, 'super_user_table'::regclass);

\c - - - :master_port
-- cleanup
SET client_min_messages TO ERROR;
DROP OWNED BY metadata_sync_helper_role;
DROP ROLE metadata_sync_helper_role;
DROP SCHEMA metadata_sync_helpers CASCADE;
