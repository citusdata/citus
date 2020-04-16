--
-- MULTI_EXTENSION
--
-- Tests around extension creation / upgrades
--
-- It'd be nice to script generation of this file, but alas, that's
-- not done yet.


SET citus.next_shard_id TO 580000;

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

CREATE SCHEMA test;
:create_function_test_maintenance_worker

-- check maintenance daemon is started
SELECT datname, current_database(),
    usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

-- ensure no objects were created outside pg_catalog
SELECT COUNT(*)
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus', 'citus_internal', 'test');


-- DROP EXTENSION pre-created by the regression suite
DROP EXTENSION citus;
\c

-- these tests switch between citus versions and call ddl's that require pg_dist_object to be created
SET citus.enable_object_propagation TO 'false';

SET citus.enable_version_checks TO 'false';

CREATE EXTENSION citus VERSION '7.0-1';
ALTER EXTENSION citus UPDATE TO '7.0-2';
ALTER EXTENSION citus UPDATE TO '7.0-3';
ALTER EXTENSION citus UPDATE TO '7.0-4';
ALTER EXTENSION citus UPDATE TO '7.0-5';
ALTER EXTENSION citus UPDATE TO '7.0-6';
ALTER EXTENSION citus UPDATE TO '7.0-7';
ALTER EXTENSION citus UPDATE TO '7.0-8';
ALTER EXTENSION citus UPDATE TO '7.0-9';
ALTER EXTENSION citus UPDATE TO '7.0-10';
ALTER EXTENSION citus UPDATE TO '7.0-11';
ALTER EXTENSION citus UPDATE TO '7.0-12';
ALTER EXTENSION citus UPDATE TO '7.0-13';
ALTER EXTENSION citus UPDATE TO '7.0-14';
ALTER EXTENSION citus UPDATE TO '7.0-15';
ALTER EXTENSION citus UPDATE TO '7.1-1';
ALTER EXTENSION citus UPDATE TO '7.1-2';
ALTER EXTENSION citus UPDATE TO '7.1-3';
ALTER EXTENSION citus UPDATE TO '7.1-4';
ALTER EXTENSION citus UPDATE TO '7.2-1';
ALTER EXTENSION citus UPDATE TO '7.2-2';
ALTER EXTENSION citus UPDATE TO '7.2-3';
ALTER EXTENSION citus UPDATE TO '7.3-3';
ALTER EXTENSION citus UPDATE TO '7.4-1';
ALTER EXTENSION citus UPDATE TO '7.4-2';
ALTER EXTENSION citus UPDATE TO '7.4-3';
ALTER EXTENSION citus UPDATE TO '7.5-1';
ALTER EXTENSION citus UPDATE TO '7.5-2';
ALTER EXTENSION citus UPDATE TO '7.5-3';
ALTER EXTENSION citus UPDATE TO '7.5-4';
ALTER EXTENSION citus UPDATE TO '7.5-5';
ALTER EXTENSION citus UPDATE TO '7.5-6';
ALTER EXTENSION citus UPDATE TO '7.5-7';
ALTER EXTENSION citus UPDATE TO '8.0-1';
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
ALTER EXTENSION citus UPDATE TO '9.2-4';
/*
 * As we mistakenly bumped schema version to 9.3-1 (in previous
 * release), we support updating citus schema from 9.3-1 to 9.2-4,
 * but we do not support explicitly updating it to to 9.3-1.
 * Hence below update (to 9.3-1) command should fail.
 */
ALTER EXTENSION citus UPDATE TO '9.3-1';
ALTER EXTENSION citus UPDATE TO '9.3-2';

-- show running version
SHOW citus.version;

-- ensure no objects were created outside pg_catalog
SELECT COUNT(*)
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus', 'citus_internal', 'test');

-- see incompatible version errors out
RESET citus.enable_version_checks;
DROP EXTENSION citus;
CREATE EXTENSION citus VERSION '7.0-1';

-- Test non-distributed queries work even in version mismatch
SET citus.enable_version_checks TO 'false';
CREATE EXTENSION citus VERSION '7.1-1';
SET citus.enable_version_checks TO 'true';

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
CREATE OR REPLACE FUNCTION pg_catalog.master_dist_authinfo_cache_invalidate()
RETURNS void LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

SET citus.enable_version_checks TO 'false';
-- This will fail because of previous function declaration
ALTER EXTENSION citus UPDATE TO '8.1-1';

-- We can DROP problematic function and continue ALTER EXTENSION even when version checks are on
SET citus.enable_version_checks TO 'true';
DROP FUNCTION pg_catalog.master_dist_authinfo_cache_invalidate();

SET citus.enable_version_checks TO 'false';
ALTER EXTENSION citus UPDATE TO '8.1-1';

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
CREATE EXTENSION citus VERSION '7.0-1';
SET citus.enable_version_checks TO 'true';
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
SET citus.enable_object_propagation TO off; -- prevent distributed transactions during add node
SELECT FROM master_add_node('localhost', :worker_1_port);

\c - - - :worker_1_port
CREATE EXTENSION citus;
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

