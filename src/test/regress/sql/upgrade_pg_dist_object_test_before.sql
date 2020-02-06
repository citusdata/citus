-- create some objects that we just included into distributed object
-- infrastructure in 9.1 versions but not included in 9.0.2

-- extension propagation --

-- create an extension on all nodes and a table that depends on it
CREATE EXTENSION isn;
SELECT run_command_on_workers($$CREATE EXTENSION isn;$$);

CREATE TABLE isn_dist_table (key int, value issn);
SELECT create_reference_table('isn_dist_table');

-- create an extension on all nodes, but do not create a table depending on it
CREATE EXTENSION seg;
SELECT run_command_on_workers($$CREATE EXTENSION seg;$$);

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

SELECT run_command_on_workers($$CREATE SCHEMA fooschema;$$);
SELECT run_command_on_workers($$CREATE TYPE fooschema.footype AS (x int, y int);$$);

CREATE TABLE fooschema.footable (f fooschema.footype);
SELECT create_reference_table('fooschema.footable');
