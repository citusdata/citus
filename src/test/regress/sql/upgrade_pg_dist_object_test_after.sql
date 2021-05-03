-- run this test only when old citus version is 9.0
\set upgrade_test_old_citus_version `echo "$upgrade_test_old_citus_version"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int = 9 AND
       substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int = 0
AS upgrade_test_old_citus_version_e_9_0;
\gset
\if :upgrade_test_old_citus_version_e_9_0
\else
\q
\endif

-- drop objects from previous test (uprade_basic_after.sql) for a clean test
-- drop upgrade_basic schema and switch back to public schema
SET search_path to public;
DROP SCHEMA upgrade_basic CASCADE;

-- as we updated citus to available version,
--   "isn" extension
--   "new_schema" schema
--   "public" schema
--   "fooschema" schema
--   "footype" type (under schema 'fooschema')
-- will now be marked as distributed
-- but,
--   "seg" extension
-- will not be marked as distributed

-- see underlying objects
SELECT i.* FROM citus.pg_dist_object, pg_identify_object_as_address(classid, objid, objsubid) i ORDER BY 1, 2, 3;
