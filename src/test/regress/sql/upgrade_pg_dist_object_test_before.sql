-- run this test only when old citus version is earlier than 10.0
\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 10
AS upgrade_test_old_citus_version_lt_10_0;
\gset
\if :upgrade_test_old_citus_version_lt_10_0
\else
\q
\endif

-- schema propagation --

-- public schema
CREATE TABLE dist_table (a int);
SELECT create_reference_table('dist_table');

-- custom schema
CREATE SCHEMA new_schema;

SET search_path to new_schema;

CREATE TABLE another_dist_table (a int);
SELECT create_reference_table('another_dist_table');

-- another custom schema and a type

-- create table that depends both on a type & schema here (actually type depends on the schema)
-- here we test if schema is marked as distributed successfully.
-- This is because tracking the dependencies will hit to the schema for two times

CREATE SCHEMA fooschema;
CREATE TYPE fooschema.footype AS (x int, y int);

CREATE TABLE fooschema.footable (f fooschema.footype);
SELECT create_reference_table('fooschema.footable');
