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

-- verify adding 2 dictionaries for two tokes at once
ALTER TEXT SEARCH CONFIGURATION french_noaccent DROP MAPPING IF EXISTS FOR asciiword, asciihword;
ALTER TEXT SEARCH CONFIGURATION french_noaccent ADD MAPPING FOR asciiword, asciihword WITH french_stem, dutch_stem;
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionaries) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciiword' LIMIT 1;
$$);
SELECT run_command_on_workers($$
    SELECT ROW(alias,dictionaries) FROM ts_debug('text_search.french_noaccent', 'un chou-fleur') WHERE alias = 'asciihword' LIMIT 1;
$$);

--verify we can drop cascade a configuration that is in use
-- verify it is in use
DROP TEXT SEARCH CONFIGURATION text_search.french_noaccent;
-- drop cascade
DROP TEXT SEARCH CONFIGURATION text_search.french_noaccent CASCADE;
-- verify the configuration is dropped from the workers
SELECT run_command_on_workers($$ SELECT 'text_search.french_noaccent'::regconfig; $$);

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

-- redo test with propagating object after it was created and changed of owner
SET citus.enable_ddl_propagation TO off;
CREATE TEXT SEARCH CONFIGURATION changed_owner2 ( PARSER = default );
ALTER TEXT SEARCH CONFIGURATION changed_owner2 OWNER TO text_search_owner;
RESET citus.enable_ddl_propagation;
-- verify object doesn't exist before propagating
SELECT run_command_on_workers($$ SELECT 'text_search.changed_owner2'::regconfig; $$);

-- distribute configuration
CREATE TABLE t2(id int, name text);
CREATE INDEX t2_search_name ON t2 USING gin (to_tsvector('text_search.changed_owner2'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t2', 'name');

-- verify config owner
SELECT run_command_on_workers($$
    SELECT cfgowner::regrole
      FROM pg_ts_config
     WHERE oid = 'text_search.changed_owner2'::regconfig;
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

-- verify we get an error that the configuration change_schema is not found, even though the object address will be
-- found in its new schema, and is distributed
ALTER TEXT SEARCH CONFIGURATION change_schema SET SCHEMA text_search2;
-- should tell us that text_search.does_not_exist does not exist, covers a complex edgecase
-- in resolving the object address
ALTER TEXT SEARCH CONFIGURATION text_search.does_not_exist SET SCHEMA text_search2;


-- verify edgecases in deparsers
CREATE TEXT SEARCH CONFIGURATION config1 ( PARSER = default );
CREATE TEXT SEARCH CONFIGURATION config2 ( PARSER = default );
SET citus.enable_ddl_propagation TO off;
CREATE TEXT SEARCH CONFIGURATION config3 ( PARSER = default );
RESET citus.enable_ddl_propagation;

-- verify config1, config2 exist on workers, config3 not
SELECT run_command_on_workers($$ SELECT 'text_search.config1'::regconfig; $$);
SELECT run_command_on_workers($$ SELECT 'text_search.config2'::regconfig; $$);
SELECT run_command_on_workers($$ SELECT 'text_search.config3'::regconfig; $$);

-- DROP all config's, only 1&2 are distributed, they should propagate well to remotes
DROP TEXT SEARCH CONFIGURATION config1, config2, config3;

-- verify all existing ones have been removed (checking config3 for consistency)
SELECT run_command_on_workers($$ SELECT 'text_search.config1'::regconfig; $$);
SELECT run_command_on_workers($$ SELECT 'text_search.config2'::regconfig; $$);
SELECT run_command_on_workers($$ SELECT 'text_search.config3'::regconfig; $$);
-- verify they are all removed locally
SELECT 'text_search.config1'::regconfig;
SELECT 'text_search.config2'::regconfig;
SELECT 'text_search.config3'::regconfig;

SET client_min_messages TO 'warning';
DROP SCHEMA text_search, text_search2 CASCADE;
DROP ROLE text_search_owner;
