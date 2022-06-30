create schema test_pg13;
set search_path to test_pg13;

SET citus.shard_replication_factor to 1;
SET citus.shard_count to 2;
SET citus.next_shard_id TO 65000;

-- Ensure tuple data in explain analyze output is the same on all PG versions
SET citus.enable_binary_protocol = TRUE;

CREATE TABLE dist_table (name char, age int);
CREATE INDEX name_index on dist_table(name);

SELECT create_distributed_table('dist_table', 'name');

SET client_min_messages to DEBUG1;
SET citus.log_remote_commands to ON;
-- make sure vacuum parallel doesn't error out
VACUUM (PARALLEL 2) dist_table;
VACUUM (PARALLEL 0) dist_table;
-- This should error out since -5 is not valid.
VACUUM (PARALLEL -5) dist_table;
-- This should error out since no number is given
VACUUM (PARALLEL) dist_table;

RESET client_min_messages;
RESET citus.log_remote_commands;

-- test alter table alter column drop expression
CREATE TABLE generated_col_table(a int, b int GENERATED ALWAYS AS (a * 10) STORED);
SELECT create_distributed_table('generated_col_table', 'a');
INSERT INTO generated_col_table VALUES (1);
-- Make sure that we currently error out
ALTER TABLE generated_col_table ALTER COLUMN b DROP EXPRESSION;

-- alter view rename column works fine
CREATE VIEW v AS SELECT * FROM dist_table;
ALTER VIEW v RENAME age to new_age;
SELECT * FROM v;

-- row suffix notation works fine
CREATE TABLE ab (a int, b int);
SELECT create_distributed_table('ab','a');
INSERT INTO ab SELECT i, 2 * i FROM generate_series(1,20)i;
SELECT * FROM ab WHERE (ROW(a,b)).f1 > (ROW(10,30)).f1 ORDER BY 1,2;
SELECT * FROM ab WHERE (ROW(a,b)).f2 > (ROW(0,38)).f2 ORDER BY 1,2;

-- test normalized
CREATE TABLE text_table (name text);
SELECT create_distributed_table('text_table', 'name');
INSERT INTO text_table VALUES ('abc');
-- not normalized
INSERT INTO text_table VALUES (U&'\0061\0308bc');
SELECT name IS NORMALIZED FROM text_table ORDER BY 1;
SELECT is_normalized(name) FROM text_table ORDER BY 1;
SELECT normalize(name) FROM text_table ORDER BY 1;
INSERT INTO text_table VALUES (normalize(U&'\0061\0308bc', NFC));

-- test unicode escape
-- insert the word 'data' with unicode escapes
INSERT INTO text_table VALUES(U&'d\0061t\+000061');
-- insert the word слон
INSERT INTO text_table VALUES(U&'\0441\043B\043E\043D');
SELECT * FROM text_table ORDER BY 1;

-- Test that we don't propagate base types
CREATE TYPE myvarchar;
CREATE FUNCTION myvarcharin(cstring, oid, integer) RETURNS myvarchar
LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharin';
CREATE FUNCTION myvarcharout(myvarchar) RETURNS cstring
LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharout';
CREATE TYPE myvarchar (
    input = myvarcharin,
    output = myvarcharout,
    alignment = integer,
    storage = main
);

CREATE TABLE my_table (a int, b myvarchar);
-- this will error because it seems that we don't propagate the "BASE TYPES"
-- Alter table also errors out so this doesn't seem to apply to use:
-- """Add ALTER TYPE options useful for extensions,
-- like TOAST and I/O functions control (Tomas Vondra, Tom Lane)"""
SELECT create_distributed_table('my_table', 'a');

CREATE TABLE test_table(a int, b tsvector);
SELECT create_distributed_table('test_table', 'a');
-- operator class options are supported
CREATE INDEX test_table_index ON test_table USING gist (b tsvector_ops(siglen = 100));

-- testing WAL
CREATE TABLE test_wal(a int, b int);
-- test WAL without ANALYZE, this should raise an error
EXPLAIN (WAL) INSERT INTO test_wal VALUES(1,11);
-- test WAL working properly for router queries
EXPLAIN (ANALYZE TRUE, WAL TRUE, COSTS FALSE, SUMMARY FALSE, BUFFERS FALSE, TIMING FALSE)
INSERT INTO test_wal VALUES(1,11);
SELECT create_distributed_table('test_wal', 'a');
EXPLAIN (ANALYZE TRUE, WAL TRUE, COSTS FALSE, SUMMARY FALSE, BUFFERS FALSE, TIMING FALSE)
INSERT INTO test_wal VALUES(2,22);

-- Test WAL working for multi-shard query
SET citus.explain_all_tasks TO on;
EXPLAIN (ANALYZE TRUE, WAL TRUE, COSTS FALSE, SUMMARY FALSE, BUFFERS FALSE, TIMING FALSE)
INSERT INTO test_wal VALUES(3,33),(4,44),(5,55) RETURNING *;

-- make sure WAL works in distributed subplans
-- this test has different output for pg14 and here we mostly test that
-- we don't get an error, hence we use explain_has_distributed_subplan.
SELECT public.explain_has_distributed_subplan(
$$
EXPLAIN (ANALYZE TRUE, WAL TRUE, COSTS FALSE, SUMMARY FALSE, BUFFERS FALSE, TIMING FALSE)
WITH cte_1 AS (INSERT INTO test_wal VALUES(6,66),(7,77),(8,88) RETURNING *)
SELECT * FROM cte_1;
$$
);

SET client_min_messages TO WARNING;
drop schema test_pg13 cascade;
