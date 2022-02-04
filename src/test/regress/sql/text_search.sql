CREATE SCHEMA text_search;
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
CREATE TABLE t1(id int, name text);
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t1', 'name');
ABORT;

-- create an index on an already distributed table
BEGIN;
CREATE TEXT SEARCH CONFIGURATION my_text_search_config ( parser = default );
CREATE TABLE t1(id int, name text);
SELECT create_distributed_table('t1', 'name');
CREATE INDEX t1_search_name ON t1 USING gin (to_tsvector('text_search.my_text_search_config'::regconfig, (COALESCE(name, ''::character varying))::text));
ABORT;

-- should be able to create a configuration based on a copy of an existing configuration
CREATE TEXT SEARCH CONFIGURATION french_noaccent ( COPY = french );
CREATE TABLE t2(id int, name text);
CREATE INDEX t2_search_name ON t2 USING gin (to_tsvector('text_search.french_noaccent'::regconfig, (COALESCE(name, ''::character varying))::text));
SELECT create_distributed_table('t2', 'id');

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

SET client_min_messages TO 'warning';
DROP SCHEMA text_search CASCADE;
DROP ROLE text_search_owner;
