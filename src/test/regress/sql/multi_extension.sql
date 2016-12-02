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

-- Create extension in oldest version, test every upgrade step
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

-- ensure no objects were created outside pg_catalog
SELECT COUNT(*)
FROM pg_depend AS pgd,
	 pg_extension AS pge,
	 LATERAL pg_identify_object(pgd.classid, pgd.objid, pgd.objsubid) AS pgio
WHERE pgd.refclassid = 'pg_extension'::regclass AND
	  pgd.refobjid   = pge.oid AND
	  pge.extname    = 'citus' AND
	  pgio.schema    NOT IN ('pg_catalog', 'citus');

-- drop extension an re-create in newest version
DROP EXTENSION citus;
\c
CREATE EXTENSION citus;
