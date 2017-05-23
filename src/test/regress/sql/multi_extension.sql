--
-- MULTI_EXTENSION
--
-- Tests around extension creation / upgrades
--
-- It'd be nice to script generation of this file, but alas, that's
-- not done yet.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 580000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 580000;

-- ensure no objects were created outside pg_catalog
SELECT COUNT(*)
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus');

-- DROP EXTENSION pre-created by the regression suite
DROP EXTENSION citus;
\c

SET citus.enable_version_checks TO 'false';

-- Create extension in oldest version
CREATE EXTENSION citus VERSION '5.0';
ALTER EXTENSION citus UPDATE TO '5.0-1';
ALTER EXTENSION citus UPDATE TO '5.0-2';
ALTER EXTENSION citus UPDATE TO '5.1-1';
ALTER EXTENSION citus UPDATE TO '5.1-2';
ALTER EXTENSION citus UPDATE TO '5.1-3';
ALTER EXTENSION citus UPDATE TO '5.1-4';
ALTER EXTENSION citus UPDATE TO '5.1-5';
ALTER EXTENSION citus UPDATE TO '5.1-6';
ALTER EXTENSION citus UPDATE TO '5.1-7';
ALTER EXTENSION citus UPDATE TO '5.1-8';
ALTER EXTENSION citus UPDATE TO '5.2-1';
ALTER EXTENSION citus UPDATE TO '5.2-2';
ALTER EXTENSION citus UPDATE TO '5.2-3';
ALTER EXTENSION citus UPDATE TO '5.2-4';
ALTER EXTENSION citus UPDATE TO '6.0-1';
ALTER EXTENSION citus UPDATE TO '6.0-2';
ALTER EXTENSION citus UPDATE TO '6.0-3';
ALTER EXTENSION citus UPDATE TO '6.0-4';
ALTER EXTENSION citus UPDATE TO '6.0-5';
ALTER EXTENSION citus UPDATE TO '6.0-6';
ALTER EXTENSION citus UPDATE TO '6.0-7';
ALTER EXTENSION citus UPDATE TO '6.0-8';
ALTER EXTENSION citus UPDATE TO '6.0-9';
ALTER EXTENSION citus UPDATE TO '6.0-10';
ALTER EXTENSION citus UPDATE TO '6.0-11';
ALTER EXTENSION citus UPDATE TO '6.0-12';
ALTER EXTENSION citus UPDATE TO '6.0-13';
ALTER EXTENSION citus UPDATE TO '6.0-14';
ALTER EXTENSION citus UPDATE TO '6.0-15';
ALTER EXTENSION citus UPDATE TO '6.0-16';
ALTER EXTENSION citus UPDATE TO '6.0-17';
ALTER EXTENSION citus UPDATE TO '6.0-18';
ALTER EXTENSION citus UPDATE TO '6.1-1';
ALTER EXTENSION citus UPDATE TO '6.1-2';
ALTER EXTENSION citus UPDATE TO '6.1-3';
ALTER EXTENSION citus UPDATE TO '6.1-4';
ALTER EXTENSION citus UPDATE TO '6.1-5';
ALTER EXTENSION citus UPDATE TO '6.1-6';
ALTER EXTENSION citus UPDATE TO '6.1-7';
ALTER EXTENSION citus UPDATE TO '6.1-8';
ALTER EXTENSION citus UPDATE TO '6.1-9';
ALTER EXTENSION citus UPDATE TO '6.1-10';
ALTER EXTENSION citus UPDATE TO '6.1-11';
ALTER EXTENSION citus UPDATE TO '6.1-12';
ALTER EXTENSION citus UPDATE TO '6.1-13';
ALTER EXTENSION citus UPDATE TO '6.1-14';
ALTER EXTENSION citus UPDATE TO '6.1-15';
ALTER EXTENSION citus UPDATE TO '6.1-16';
ALTER EXTENSION citus UPDATE TO '6.1-17';
ALTER EXTENSION citus UPDATE TO '6.2-1';
ALTER EXTENSION citus UPDATE TO '6.2-2';
ALTER EXTENSION citus UPDATE TO '6.2-3';
ALTER EXTENSION citus UPDATE TO '6.2-4';

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
	  pgio.schema    NOT IN ('pg_catalog', 'citus');

-- see incompatible version errors out
RESET citus.enable_version_checks;
DROP EXTENSION citus;
CREATE EXTENSION citus VERSION '5.0';

-- Test non-distributed queries work even in version mismatch
SET citus.enable_version_checks TO 'false';
CREATE EXTENSION citus VERSION '6.1-17';
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
CREATE OR REPLACE FUNCTION pg_catalog.citus_table_size(table_name regclass)
RETURNS bigint LANGUAGE plpgsql
AS $function$
BEGIN
END;
$function$;

SET citus.enable_version_checks TO 'false';
-- This will fail because of previous function declaration
ALTER EXTENSION citus UPDATE TO '6.2-2';

-- We can DROP problematic function and continue ALTER EXTENSION even when version checks are on
SET citus.enable_version_checks TO 'true';
DROP FUNCTION citus_table_size(regclass);

SET citus.enable_version_checks TO 'false';
ALTER EXTENSION citus UPDATE TO '6.2-2';

-- Test updating to the latest version without specifying the version number
ALTER EXTENSION citus UPDATE;

-- re-create in newest version
DROP EXTENSION citus;
\c
CREATE EXTENSION citus;

-- test cache invalidation in workers
\c - - - :worker_1_port

-- this will initialize the cache
\d
DROP EXTENSION citus;
SET citus.enable_version_checks TO 'false';
CREATE EXTENSION citus VERSION '5.2-4';
SET citus.enable_version_checks TO 'true';
-- during ALTER EXTENSION, we should invalidate the cache
ALTER EXTENSION citus UPDATE;

-- if cache is invalidated succesfull, this \d should work without any problem
\d
