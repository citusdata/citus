CREATE SCHEMA text_search;
CREATE SCHEMA text_search2;
SET search_path TO text_search;

-- create a new configruation from scratch
CREATE TEXT SEARCH CONFIGURATION my_text_search_config ( parser = default );
CREATE TABLE t1(id int, name text);
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t1', 'name');

DROP TABLE t1;
DROP TEXT SEARCH CONFIGURATION my_text_search_config;

-- try to create table and index in 1 transaction
BEGIN;
CREATE TEXT SEARCH CONFIGURATION my_text_search_config ( parser = default );
CREATE TABLE t1(id int, name text);
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t1', 'name');
ABORT;

-- try again, should not fail with my_text_search_config being retained on the worker
BEGIN;
CREATE TEXT SEARCH CONFIGURATION my_text_search_config ( parser = default );
COMMENT ON TEXT SEARCH CONFIGURATION my_text_search_config IS 'on demand propagation of text search object with a comment';
CREATE TABLE t1(id int, name text);
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t1', 'name');
SELECT run_command_on_workers($$
    SELECT obj_description('text_search.my_text_search_config'::regconfig);
$$);

-- verify that changing anything on a managed TEXT SEARCH CONFIGURATION fails after parallel execution
COMMENT ON TEXT SEARCH CONFIGURATION my_text_search_config  IS 'this comment can''t be set right now';
ABORT;

-- create an index on an already distributed table
BEGIN;
CREATE TEXT SEARCH CONFIGURATION my_text_search_config2 ( parser = default );
COMMENT ON TEXT SEARCH CONFIGURATION my_text_search_config2 IS 'on demand propagation of text search object with a comment 2';
CREATE TABLE t1(id int, name text);
SELECT create_distributed_table('t1', 'name');
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config2'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT run_command_on_workers($$
    SELECT obj_description('text_search.my_text_search_config2'::regconfig);
$$);
ABORT;

-- should be able to create a configuration based on a copy of an existing configuration
CREATE TEXT SEARCH CONFIGURATION french_noaccent ( COPY = french );
CREATE TABLE t2(id int, name text);
CREATE INDEX t2_search_name ON t2 USING gin (to_tsvector('text_search.french_noaccent'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t2', 'id');

-- spot check that french_noaccent copied settings from french
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'comment tu t''appelle') WHERE alias = 'asciiword' LIMIT 1;
$$);
-- makes no sense, however we expect that the dictionary for the first token changes accordingly
ALTER TEXT SEARCH CONFIGURATION french_noaccent ALTER MAPPING FOR asciiword WITH dutch_stem;
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'comment tu t''appelle') WHERE alias = 'asciiword' LIMIT 1;
$$);
-- do the same but we will replace all french dictionaries
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciihword' LIMIT 1;
$$);
ALTER TEXT SEARCH CONFIGURATION french_noaccent ALTER MAPPING REPLACE french_stem WITH dutch_stem;
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciihword' LIMIT 1;
$$);
-- once more but now back via yet a different DDL command
ALTER TEXT SEARCH CONFIGURATION french_noaccent ALTER MAPPING FOR asciihword REPLACE dutch_stem WITH french_stem;
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciihword' LIMIT 1;
$$);
-- drop a mapping
ALTER TEXT SEARCH CONFIGURATION french_noaccent DROP MAPPING FOR asciihword;
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionary) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciihword' LIMIT 1;
$$);
-- also with exists, doesn't change anything, but should not error
ALTER TEXT SEARCH CONFIGURATION french_noaccent DROP MAPPING IF EXISTS FOR asciihword;

-- Comment on a text search configuration
COMMENT ON TEXT SEARCH CONFIGURATION french_noaccent IS 'a text configuration that is butcherd to test all edge cases';
SELECT run_command_on_workers($$
    SELECT obj_description('text_search.french_noaccent'::regconfig);
$$);

-- Remove a comment
COMMENT ON TEXT SEARCH CONFIGURATION french_noaccent IS NULL;
SELECT run_command_on_workers($$
    SELECT obj_description('text_search.french_noaccent'::regconfig);
$$);

SET client_min_messages TO 'warning';
SELECT run_command_on_workers($$CREATE ROLE text_search_owner;$$);
CREATE ROLE text_search_owner;
RESET client_min_messages;

CREATE TEXT SEARCH CONFIGURATION changed_owner ( PARSER = default );
SELECT run_command_on_workers($$
    SELECT cfgowner::regrole
      FROM pg_ts_config
     WHERE oid = 'text_search.changed_owner'::regconfig;
$$);
ALTER TEXT SEARCH CONFIGURATION changed_owner OWNER TO text_search_owner;
SELECT run_command_on_workers($$
    SELECT cfgowner::regrole
      FROM pg_ts_config
     WHERE oid = 'text_search.changed_owner'::regconfig;
$$);

-- rename tests
CREATE TEXT SEARCH CONFIGURATION change_name ( PARSER = default );
SELECT run_command_on_workers($$ -- verify the name exists on the worker
    SELECT 'text_search.change_name'::regconfig;
$$);
ALTER TEXT SEARCH CONFIGURATION change_name RENAME TO changed_name;
SELECT run_command_on_workers($$ -- verify the name exists on the worker
    SELECT 'text_search.changed_name'::regconfig;
$$);

-- test move of schema
CREATE TEXT SEARCH CONFIGURATION change_schema ( PARSER = default );
SELECT run_command_on_workers($$ -- verify the name exists on the worker
    SELECT 'text_search.change_schema'::regconfig;
$$);
ALTER TEXT SEARCH CONFIGURATION change_schema SET SCHEMA text_search2;
SELECT run_command_on_workers($$ -- verify the name exists on the worker
    SELECT 'text_search2.change_schema'::regconfig;
$$);


SET client_min_messages TO 'warning';
DROP SCHEMA text_search, text_search2 CASCADE;
DROP ROLE text_search_owner;
