--
-- MULTI_EXTENSION
--
-- Tests around extension creation / upgrades
--
-- It'd be nice to script generation of this file, but alas, that's
-- not done yet.

-- differentiate the output file for pg11 and versions above, with regards to objects
-- created per citus version depending on the postgres version. Upgrade tests verify the
-- objects are added in citus_finish_pg_upgrade()
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS version_above_eleven;

SET citus.next_shard_id TO 580000;
CREATE SCHEMA multi_extension;

SELECT $definition$
CREATE OR REPLACE FUNCTION test.maintenance_worker()
    RETURNS pg_stat_activity
    LANGUAGE plpgsql
AS $$
DECLARE
   activity record;
BEGIN
    DO 'BEGIN END'; -- Force maintenance daemon to start
    -- we don't want to wait forever; loop will exit after 20 seconds
    FOR i IN 1 .. 200 LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT * INTO activity FROM pg_stat_activity
        WHERE application_name = 'Citus Maintenance Daemon' AND datname = current_database();
        IF activity.pid IS NOT NULL THEN
            RETURN activity;
        ELSE
            PERFORM pg_sleep(0.1);
        END IF ;
    END LOOP;
    -- fail if we reach the end of this loop
    raise 'Waited too long for maintenance daemon to start';
END;
$$;
$definition$ create_function_test_maintenance_worker
\gset

CREATE TABLE multi_extension.prev_objects(description text);
CREATE TABLE multi_extension.extension_diff(previous_object text COLLATE "C",
                            current_object text COLLATE "C");

CREATE FUNCTION multi_extension.print_extension_changes()
RETURNS TABLE(previous_object text, current_object text)
AS $func$
BEGIN
    SET LOCAL search_path TO multi_extension;
	TRUNCATE TABLE extension_diff;

	CREATE TABLE current_objects AS
	SELECT pg_catalog.pg_describe_object(classid, objid, 0)
           || ' ' ||
           coalesce(pg_catalog.pg_get_function_result(objid), '') AS description
	FROM pg_catalog.pg_depend, pg_catalog.pg_extension e
	WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
		AND refobjid = e.oid
		AND deptype = 'e'
		AND e.extname='citus';

	INSERT INTO extension_diff
	SELECT p.description previous_object, c.description current_object
	FROM current_objects c FULL JOIN prev_objects p
	ON p.description = c.description
	WHERE p.description is null OR c.description is null;

	DROP TABLE prev_objects;
	ALTER TABLE current_objects RENAME TO prev_objects;

	RETURN QUERY SELECT * FROM extension_diff ORDER BY 1, 2;
END
$func$ LANGUAGE plpgsql;

CREATE SCHEMA test;
:create_function_test_maintenance_worker

-- check maintenance daemon is started
SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- ensure no unexpected objects were created outside pg_catalog
SELECT pgio.type, pgio.identity
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus', 'citus_internal', 'test', 'columnar')
ORDER BY 1, 2;


-- DROP EXTENSION pre-created by the regression suite
DROP EXTENSION citus;
\c

-- these tests switch between citus versions and call ddl's that require pg_dist_object to be created
SET citus.enable_metadata_sync TO 'false';

SET citus.enable_version_checks TO 'false';

SET columnar.enable_version_checks TO 'false';

CREATE EXTENSION citus VERSION '8.0-1';
ALTER EXTENSION citus UPDATE TO '8.0-2';
ALTER EXTENSION citus UPDATE TO '8.0-3';
ALTER EXTENSION citus UPDATE TO '8.0-4';
ALTER EXTENSION citus UPDATE TO '8.0-5';
ALTER EXTENSION citus UPDATE TO '8.0-6';
ALTER EXTENSION citus UPDATE TO '8.0-7';
ALTER EXTENSION citus UPDATE TO '8.0-8';
ALTER EXTENSION citus UPDATE TO '8.0-9';
ALTER EXTENSION citus UPDATE TO '8.0-10';
ALTER EXTENSION citus UPDATE TO '8.0-11';
ALTER EXTENSION citus UPDATE TO '8.0-12';
ALTER EXTENSION citus UPDATE TO '8.0-13';
ALTER EXTENSION citus UPDATE TO '8.1-1';
ALTER EXTENSION citus UPDATE TO '8.2-1';
ALTER EXTENSION citus UPDATE TO '8.2-2';
ALTER EXTENSION citus UPDATE TO '8.2-3';
ALTER EXTENSION citus UPDATE TO '8.2-4';
ALTER EXTENSION citus UPDATE TO '8.3-1';
ALTER EXTENSION citus UPDATE TO '9.0-1';
ALTER EXTENSION citus UPDATE TO '9.0-2';
ALTER EXTENSION citus UPDATE TO '9.1-1';
ALTER EXTENSION citus UPDATE TO '9.2-1';
ALTER EXTENSION citus UPDATE TO '9.2-2';
-- Snapshot of state at 9.2-2
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 9.2-2 from 9.2-4
ALTER EXTENSION citus UPDATE TO '9.2-4';
ALTER EXTENSION citus UPDATE TO '9.2-2';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

/*
 * As we mistakenly bumped schema version to 9.3-1 in a bad release, we support
 * updating citus schema from 9.3-1 to 9.2-4, but we do not support updates to 9.3-1.
 *
 * Hence the query below should fail.
 */
ALTER EXTENSION citus UPDATE TO '9.3-1';

ALTER EXTENSION citus UPDATE TO '9.2-4';
-- Snapshot of state at 9.2-4
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 9.2-4 from 9.3-2
ALTER EXTENSION citus UPDATE TO '9.3-2';
ALTER EXTENSION citus UPDATE TO '9.2-4';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.3-2
ALTER EXTENSION citus UPDATE TO '9.3-2';
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 9.3-2 from 9.4-1
ALTER EXTENSION citus UPDATE TO '9.4-1';
ALTER EXTENSION citus UPDATE TO '9.3-2';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.4-1
ALTER EXTENSION citus UPDATE TO '9.4-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test upgrade paths for backported citus_pg_upgrade functions
ALTER EXTENSION citus UPDATE TO '9.4-2';
ALTER EXTENSION citus UPDATE TO '9.4-1';
-- Should be empty result, even though the downgrade doesn't undo the upgrade, the
-- function signature doesn't change, which is reflected here.
SELECT * FROM multi_extension.print_extension_changes();

ALTER EXTENSION citus UPDATE TO '9.4-2';
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.4-1
ALTER EXTENSION citus UPDATE TO '9.4-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test upgrade paths for backported improvement of master_update_table_statistics function
ALTER EXTENSION citus UPDATE TO '9.4-3';
-- should see the new source code with internal function citus_update_table_statistics
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
ALTER EXTENSION citus UPDATE TO '9.4-2';

-- should see the old source code
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

ALTER EXTENSION citus UPDATE TO '9.4-3';
-- should see the new source code with internal function citus_update_table_statistics
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.4-1
ALTER EXTENSION citus UPDATE TO '9.4-1';
-- should see the old source code
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 9.4-1 from 9.5-1
ALTER EXTENSION citus UPDATE TO '9.5-1';

-- TODO: This test should be moved to a valid downgrade testing suite where the downgrade is done, both on the schema and the binaries. Later changes in Citus made a C vs Schema discrepancy error here
-- BEGIN;
--   SET citus.enable_metadata_sync TO on;
--   SELECT master_add_node('localhost', :master_port, groupId=>0);
--   CREATE TABLE citus_local_table (a int);
--   SELECT create_citus_local_table('citus_local_table');
--   RESET citus.enable_metadata_sync;
--
--   -- downgrade from 9.5-1 to 9.4-1 should fail as we have a citus local table
--   ALTER EXTENSION citus UPDATE TO '9.4-1';
-- ROLLBACK;

-- now we can downgrade as there is no citus local table
ALTER EXTENSION citus UPDATE TO '9.4-1';

-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.5-1
ALTER EXTENSION citus UPDATE TO '9.5-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test upgrade paths for backported citus_pg_upgrade functions
ALTER EXTENSION citus UPDATE TO '9.5-2';
ALTER EXTENSION citus UPDATE TO '9.5-1';
-- Should be empty result, even though the downgrade doesn't undo the upgrade, the
-- function signature doesn't change, which is reflected here.
SELECT * FROM multi_extension.print_extension_changes();

ALTER EXTENSION citus UPDATE TO '9.5-2';
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.5-1
ALTER EXTENSION citus UPDATE TO '9.5-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test upgrade paths for backported improvement of master_update_table_statistics function
ALTER EXTENSION citus UPDATE TO '9.5-3';
-- should see the new source code with internal function citus_update_table_statistics
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
ALTER EXTENSION citus UPDATE TO '9.5-2';

-- should see the old source code
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

ALTER EXTENSION citus UPDATE TO '9.5-3';
-- should see the new source code with internal function citus_update_table_statistics
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 9.5-1
ALTER EXTENSION citus UPDATE TO '9.5-1';
-- should see the old source code
SELECT prosrc FROM pg_proc WHERE proname = 'master_update_table_statistics' ORDER BY 1;
-- Should be empty result
SELECT * FROM multi_extension.print_extension_changes();

-- We removed the upgrade paths to 10.0-1, 10.0-2 and 10.0-3 due to a bug that blocked
-- upgrades to 10.0, Therefore we test upgrades to 10.0-4 instead

-- Test downgrade to 9.5-1 from 10.0-4
ALTER EXTENSION citus UPDATE TO '10.0-4';
ALTER EXTENSION citus UPDATE TO '9.5-1';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.0-4
ALTER EXTENSION citus UPDATE TO '10.0-4';
SELECT * FROM multi_extension.print_extension_changes();

-- check that we depend on the existence of public schema, and we can not drop it now
DROP SCHEMA public;

-- verify that citus_tables view is on pg_catalog if public schema is absent.
ALTER EXTENSION citus UPDATE TO '9.5-1';
DROP SCHEMA public;
ALTER EXTENSION citus UPDATE TO '10.0-4';
SELECT * FROM multi_extension.print_extension_changes();

-- recreate public schema, and recreate citus_tables in the public schema by default
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO public;
ALTER EXTENSION citus UPDATE TO '9.5-1';
ALTER EXTENSION citus UPDATE TO '10.0-4';
SELECT * FROM multi_extension.print_extension_changes();

-- not print "HINT: " to hide current lib version
\set VERBOSITY terse
CREATE TABLE columnar_table(a INT, b INT) USING columnar;
SET citus.enable_version_checks TO ON;
SET columnar.enable_version_checks TO ON;

-- all should throw an error due to version mismatch
VACUUM FULL columnar_table;
INSERT INTO columnar_table SELECT i FROM generate_series(1, 10) i;
VACUUM columnar_table;
TRUNCATE columnar_table;
DROP TABLE columnar_table;
CREATE INDEX ON columnar_table (a);
SELECT alter_columnar_table_set('columnar_table', compression => 'pglz');
SELECT alter_columnar_table_reset('columnar_table');
INSERT INTO columnar_table SELECT * FROM columnar_table;

SELECT 1 FROM columnar_table; -- columnar custom scan

SET columnar.enable_custom_scan TO OFF;
SELECT 1 FROM columnar_table; -- seq scan

CREATE TABLE new_columnar_table (a int) USING columnar;

-- do cleanup for the rest of the tests
SET citus.enable_version_checks TO OFF;
SET columnar.enable_version_checks TO OFF;
DROP TABLE columnar_table;
RESET columnar.enable_custom_scan;
\set VERBOSITY default

-- Test downgrade to 10.0-4 from 10.1-1
ALTER EXTENSION citus UPDATE TO '10.1-1';
ALTER EXTENSION citus UPDATE TO '10.0-4';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.1-1
ALTER EXTENSION citus UPDATE TO '10.1-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 10.1-1 from 10.2-1
ALTER EXTENSION citus UPDATE TO '10.2-1';
ALTER EXTENSION citus UPDATE TO '10.1-1';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.2-1
ALTER EXTENSION citus UPDATE TO '10.2-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 10.2-1 from 10.2-2
ALTER EXTENSION citus UPDATE TO '10.2-2';
ALTER EXTENSION citus UPDATE TO '10.2-1';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.2-2
ALTER EXTENSION citus UPDATE TO '10.2-2';
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 10.2-2 from 10.2-3
ALTER EXTENSION citus UPDATE TO '10.2-3';
ALTER EXTENSION citus UPDATE TO '10.2-2';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.2-3
ALTER EXTENSION citus UPDATE TO '10.2-3';
SELECT * FROM multi_extension.print_extension_changes();

-- Test downgrade to 10.2-3 from 10.2-4
ALTER EXTENSION citus UPDATE TO '10.2-4';
ALTER EXTENSION citus UPDATE TO '10.2-3';

-- Make sure that we don't delete pg_depend entries added in
-- columnar--10.2-3--10.2-4.sql when downgrading to 10.2-3.
SELECT COUNT(*)=10
FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid AND
      objid = (select oid from pg_am where amname = 'columnar') AND
      objsubid = 0 AND
      refclassid = 'pg_class'::regclass::oid AND
      refobjsubid = 0 AND
      deptype = 'n';

-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 10.2-4
ALTER EXTENSION citus UPDATE TO '10.2-4';
SELECT * FROM multi_extension.print_extension_changes();

-- Make sure that we defined dependencies from all rel objects (tables,
-- indexes, sequences ..) to columnar table access method ...
SELECT pg_class.oid INTO columnar_schema_members
FROM pg_class, pg_namespace
WHERE pg_namespace.oid=pg_class.relnamespace AND
      pg_namespace.nspname='columnar';
SELECT refobjid INTO columnar_schema_members_pg_depend
FROM pg_depend
WHERE classid = 'pg_am'::regclass::oid AND
      objid = (select oid from pg_am where amname = 'columnar') AND
      objsubid = 0 AND
      refclassid = 'pg_class'::regclass::oid AND
      refobjsubid = 0 AND
      deptype = 'n';

-- ... , so this should be empty,
(TABLE columnar_schema_members EXCEPT TABLE columnar_schema_members_pg_depend)
UNION
(TABLE columnar_schema_members_pg_depend EXCEPT TABLE columnar_schema_members);

-- ... , and both columnar_schema_members_pg_depend & columnar_schema_members
-- should have 10 entries.
SELECT COUNT(*)=10 FROM columnar_schema_members_pg_depend;

DROP TABLE columnar_schema_members, columnar_schema_members_pg_depend;

-- Use a synthetic pg_dist_shard record to show that upgrade fails
-- when there are cstore_fdw tables
INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage) VALUES ('pg_dist_shard', 1, 'c');
ALTER EXTENSION citus UPDATE TO '11.0-1';
DELETE FROM pg_dist_shard WHERE shardid = 1;

-- partitioned table count is tracked on Citus 11 upgrade
CREATE TABLE e_transactions(order_id varchar(255) NULL, transaction_id int) PARTITION BY LIST(transaction_id);
CREATE TABLE orders_2020_07_01
PARTITION OF e_transactions FOR VALUES IN (1,2,3);
INSERT INTO pg_dist_partition VALUES ('e_transactions'::regclass,'h', '{VAR :varno 1 :varattno 1 :vartype 1043 :vartypmod 259 :varcollid 100 :varlevelsup 0 :varnosyn 1 :varattnosyn 1 :location -1}', 7, 's');

SELECT
	(metadata->>'partitioned_citus_table_exists_pre_11')::boolean as partitioned_citus_table_exists_pre_11,
	(metadata->>'partitioned_citus_table_exists_pre_11') IS NULL as is_null
FROM
	pg_dist_node_metadata;

-- Test downgrade to 10.2-4 from 11.0-1
ALTER EXTENSION citus UPDATE TO '11.0-1';

SELECT
	(metadata->>'partitioned_citus_table_exists_pre_11')::boolean as partitioned_citus_table_exists_pre_11,
	(metadata->>'partitioned_citus_table_exists_pre_11') IS NULL as is_null
FROM
	pg_dist_node_metadata;

DELETE FROM pg_dist_partition WHERE logicalrelid = 'e_transactions'::regclass;
DROP TABLE e_transactions;

ALTER EXTENSION citus UPDATE TO '10.2-4';
-- Should be empty result since upgrade+downgrade should be a no-op
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 11.0-1
ALTER EXTENSION citus UPDATE TO '11.0-1';
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 11.0-2
ALTER EXTENSION citus UPDATE TO '11.0-2';
SELECT * FROM multi_extension.print_extension_changes();

-- Snapshot of state at 11.1-1
ALTER EXTENSION citus UPDATE TO '11.1-1';
SELECT * FROM multi_extension.print_extension_changes();

DROP TABLE multi_extension.prev_objects, multi_extension.extension_diff;

-- show running version
SHOW citus.version;

-- ensure no unexpected objects were created outside pg_catalog
SELECT pgio.type, pgio.identity
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus', 'citus_internal', 'test', 'columnar')
ORDER BY 1, 2;

-- see incompatible version errors out
RESET citus.enable_version_checks;
RESET columnar.enable_version_checks;
DROP EXTENSION citus;
CREATE EXTENSION citus VERSION '8.0-1';

-- Test non-distributed queries work even in version mismatch
SET citus.enable_version_checks TO 'false';
SET columnar.enable_version_checks TO 'false';
CREATE EXTENSION citus VERSION '8.1-1';
SET citus.enable_version_checks TO 'true';
SET columnar.enable_version_checks TO 'true';

-- Test CREATE TABLE
CREATE TABLE version_mismatch_table(column1 int);

-- Test COPY
\copy version_mismatch_table FROM STDIN;
0
1
2
3
4
\.

-- Test INSERT
INSERT INTO version_mismatch_table(column1) VALUES(5);

-- Test SELECT
SELECT * FROM version_mismatch_table ORDER BY column1;

-- Test SELECT from pg_catalog
SELECT d.datname as "Name",
       pg_catalog.pg_get_userbyid(d.datdba) as "Owner",
       pg_catalog.array_to_string(d.datacl, E'\n') AS "Access privileges"
FROM pg_catalog.pg_database d
ORDER BY 1;

-- We should not distribute table in version mistmatch
SELECT create_distributed_table('version_mismatch_table', 'column1');

-- This function will cause fail in next ALTER EXTENSION
CREATE OR REPLACE FUNCTION pg_catalog.relation_is_a_known_shard(regclass)
RETURNS void LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

SET citus.enable_version_checks TO 'false';
SET columnar.enable_version_checks TO 'false';
-- This will fail because of previous function declaration
ALTER EXTENSION citus UPDATE TO '9.1-1';

-- We can DROP problematic function and continue ALTER EXTENSION even when version checks are on
SET citus.enable_version_checks TO 'true';
SET columnar.enable_version_checks TO 'true';
DROP FUNCTION pg_catalog.relation_is_a_known_shard(regclass);

SET citus.enable_version_checks TO 'false';
SET columnar.enable_version_checks TO 'false';
ALTER EXTENSION citus UPDATE TO '9.1-1';

-- Test updating to the latest version without specifying the version number
ALTER EXTENSION citus UPDATE;

-- re-create in newest version
DROP EXTENSION citus;
\c
CREATE EXTENSION citus;

-- test cache invalidation in workers
\c - - - :worker_1_port

DROP EXTENSION citus;
SET citus.enable_version_checks TO 'false';
SET columnar.enable_version_checks TO 'false';
CREATE EXTENSION citus VERSION '8.0-1';
SET citus.enable_version_checks TO 'true';
SET columnar.enable_version_checks TO 'true';
-- during ALTER EXTENSION, we should invalidate the cache
ALTER EXTENSION citus UPDATE;

-- if cache is invalidated succesfull, this \d should work without any problem
\d

\c - - - :master_port

-- test https://github.com/citusdata/citus/issues/3409
CREATE USER testuser2 SUPERUSER;
SET ROLE testuser2;
DROP EXTENSION Citus;
-- Loop until we see there's no maintenance daemon running
DO $$begin
    for i in 0 .. 100 loop
        if i = 100 then raise 'Waited too long'; end if;
        PERFORM pg_stat_clear_snapshot();
        perform * from pg_stat_activity where application_name = 'Citus Maintenance Daemon';
        if not found then exit; end if;
        perform pg_sleep(0.1);
    end loop;
end$$;
SELECT datid, datname, usename FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';
CREATE EXTENSION Citus;
-- Loop until we there's a maintenance daemon running
DO $$begin
    for i in 0 .. 100 loop
        if i = 100 then raise 'Waited too long'; end if;
        PERFORM pg_stat_clear_snapshot();
        perform * from pg_stat_activity where application_name = 'Citus Maintenance Daemon';
        if found then exit; end if;
        perform pg_sleep(0.1);
    end loop;
end$$;
SELECT datid, datname, usename FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';
RESET ROLE;

-- check that maintenance daemon gets (re-)started for the right user
DROP EXTENSION citus;
CREATE USER testuser SUPERUSER;
SET ROLE testuser;
CREATE EXTENSION citus;

SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- and recreate as the right owner
RESET ROLE;
DROP EXTENSION citus;
CREATE EXTENSION citus;


-- Check that maintenance daemon can also be started in another database
CREATE DATABASE another;
\c another
CREATE EXTENSION citus;

CREATE SCHEMA test;
:create_function_test_maintenance_worker

-- see that the daemon started
SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- Test that database with active worker can be dropped.
\c regression

CREATE SCHEMA test_daemon;

-- we create a similar function on the regression database
-- note that this function checks for the existence of the daemon
-- when not found, returns true else tries for 5 times and
-- returns false
CREATE OR REPLACE FUNCTION test_daemon.maintenance_daemon_died(p_dbname text)
    RETURNS boolean
    LANGUAGE plpgsql
AS $$
DECLARE
   activity record;
BEGIN
    PERFORM pg_stat_clear_snapshot();
    SELECT * INTO activity FROM pg_stat_activity
    WHERE application_name = 'Citus Maintenance Daemon' AND datname = p_dbname;
    IF activity.pid IS NULL THEN
        RETURN true;
    ELSE
        RETURN false;
    END IF;
END;
$$;

-- drop the database and see that the daemon is dead
DROP DATABASE another;
SELECT
    *
FROM
    test_daemon.maintenance_daemon_died('another');

-- we don't need the schema and the function anymore
DROP SCHEMA test_daemon CASCADE;


-- verify citus does not crash while creating a table when run against an older worker
-- create_distributed_table piggybacks multiple commands into single one, if one worker
-- did not have the required UDF it should fail instead of crash.

-- create a test database, configure citus with single node
CREATE DATABASE another;
\c - - - :worker_1_port
CREATE DATABASE another;
\c - - - :master_port

\c another
CREATE EXTENSION citus;
\c - - - :worker_1_port
CREATE EXTENSION citus;
\c - - - :master_port

SELECT FROM master_add_node('localhost', :worker_1_port);

\c - - - :worker_1_port
ALTER FUNCTION assign_distributed_transaction_id(initiator_node_identifier integer, transaction_number bigint, transaction_stamp timestamp with time zone)
RENAME TO dummy_assign_function;

\c - - - :master_port
SET citus.shard_replication_factor to 1;
-- create_distributed_table command should fail
CREATE TABLE t1(a int, b int);
SET client_min_messages TO ERROR;
DO $$
BEGIN
        BEGIN
                SELECT create_distributed_table('t1', 'a');
        EXCEPTION WHEN OTHERS THEN
                RAISE 'create distributed table failed';
        END;
END;
$$;

\c regression
\c - - - :master_port
DROP DATABASE another;

\c - - - :worker_1_port
DROP DATABASE another;

\c - - - :master_port
-- only the regression database should have a maintenance daemon
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';

-- recreate the extension immediately after the maintenancae daemon errors
SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';
DROP EXTENSION citus;
CREATE EXTENSION citus;

-- wait for maintenance daemon restart
SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- confirm that there is only one maintenance daemon
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';

-- kill the maintenance daemon
SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';

-- reconnect
\c - - - :master_port
-- run something that goes through planner hook and therefore kicks of maintenance daemon
SELECT 1;

-- wait for maintenance daemon restart
SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- confirm that there is only one maintenance daemon
SELECT count(*) FROM pg_stat_activity WHERE application_name = 'Citus Maintenance Daemon';

DROP TABLE version_mismatch_table;
DROP SCHEMA multi_extension;
