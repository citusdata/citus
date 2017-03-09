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

-- Create extension in oldest version, this is expected to fail
CREATE EXTENSION citus VERSION '5.0';

-- Create extension in newest major version, test every upgrade step
CREATE EXTENSION citus VERSION '6.2-1';
ALTER EXTENSION citus UPDATE TO '6.2-2';

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

-- drop extension an re-create in newest version
DROP EXTENSION citus;
\c
CREATE EXTENSION citus;
