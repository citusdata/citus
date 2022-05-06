--
-- MULTI_DROP_EXTENSION
--
-- Tests around dropping and recreating the extension

BEGIN;
CREATE EXTENSION citus;
ROLLBACK;
CREATE EXTENSION citus;
DROP EXTENSION citus;

BEGIN;
BEGIN;
CREATE EXTENSION citus;
ROLLBACK;
CREATE EXTENSION citus;

SET citus.next_shard_id TO 550000;


CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');

-- this emits a NOTICE message for every table we are dropping with our CASCADE. It would
-- be nice to check that we get those NOTICE messages, but it's nicer to not have to
-- change this test every time the previous tests change the set of tables they leave
-- around.
SET client_min_messages TO 'WARNING';
DROP FUNCTION pg_catalog.master_create_worker_shards(text, integer, integer);
DROP EXTENSION citus CASCADE;
RESET client_min_messages;

BEGIN;
  SET client_min_messages TO ERROR;
  SET search_path TO public;
  CREATE EXTENSION citus;

  -- not wait for replicating reference tables from other test files
  SET citus.replicate_reference_tables_on_activate TO OFF;
  SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

  create table l1 (a int unique);
  SELECT create_reference_table('l1');

  create schema other_schema;
  create table other_schema.l3 (a int);
  select create_reference_table ('other_schema.l3');

  alter table other_schema.l3 add constraint fkey foreign key (a) references l1(a);

  -- show that works fine
  drop schema public cascade;
ROLLBACK;

CREATE EXTENSION citus;

CREATE SCHEMA test_schema;
SET search_path TO test_schema;

CREATE TABLE ref(x int, y int);
SELECT create_reference_table('ref');

CREATE INDEX CONCURRENTLY ref_concurrent_idx_x ON ref(x);
CREATE INDEX CONCURRENTLY ref_concurrent_idx_y ON ref(x);

SELECT substring(current_Setting('server_version'), '\d+')::int > 11 AS server_version_above_eleven
\gset
\if :server_version_above_eleven
REINDEX INDEX CONCURRENTLY ref_concurrent_idx_x;
REINDEX INDEX CONCURRENTLY ref_concurrent_idx_y;
REINDEX TABLE CONCURRENTLY ref;
REINDEX SCHEMA CONCURRENTLY test_schema;
\endif

SET search_path TO public;
\set VERBOSITY TERSE
DROP SCHEMA test_schema CASCADE;
DROP EXTENSION citus CASCADE;
\set VERBOSITY DEFAULT

CREATE EXTENSION citus;

-- this function is dropped in Citus10, added here for tests
CREATE OR REPLACE FUNCTION pg_catalog.master_create_worker_shards(table_name text, shard_count integer,
                                                                  replication_factor integer DEFAULT 2)
    RETURNS void
    AS 'citus', $$master_create_worker_shards$$
    LANGUAGE C STRICT;
-- re-add the nodes to the cluster
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- verify that a table can be created after the extension has been dropped and recreated
CREATE TABLE testtableddl(somecol int, distributecol text NOT NULL);
SELECT create_distributed_table('testtableddl', 'distributecol', 'append');
SELECT 1 FROM master_create_empty_shard('testtableddl');
SELECT * FROM testtableddl;
DROP TABLE testtableddl;
