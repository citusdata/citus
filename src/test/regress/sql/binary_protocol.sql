SET citus.shard_count = 2;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 4754000;
CREATE SCHEMA binary_protocol;
SET search_path TO binary_protocol, public;
SET citus.enable_binary_protocol = TRUE;

CREATE TABLE t(id int);
SELECT create_distributed_table('t', 'id');

INSERT INTO t (SELECT i FROM generate_series(1, 10) i);

SELECT * FROM t ORDER BY id;
-- Select more than 16 columns to trigger growing of columns
SELECT id, id, id, id, id,
       id, id, id, id, id,
       id, id, id, id, id,
       id, id, id, id, id,
       id, id, id, id, id,
       id, id, id, id, id
    FROM t ORDER BY id;

EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, SUMMARY FALSE) SELECT id FROM t ORDER BY 1;
SET citus.explain_all_tasks TO ON;
EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, SUMMARY FALSE) SELECT id FROM t ORDER BY 1;

INSERT INTO t SELECT count(*) from t;

INSERT INTO t (SELECT id+1 from t);

SELECT * FROM t ORDER BY id;

CREATE TYPE composite_type AS (
    i integer,
    i2 integer
);

CREATE TABLE composite_type_table
(
    id bigserial,
    col composite_type
);

SELECT create_distributed_table('composite_type_table', 'id');

CREATE TYPE nested_composite_type AS (
    a composite_type,
    b composite_type
);
CREATE DOMAIN binary_protocol.composite_type_domain AS binary_protocol.composite_type;
CREATE DOMAIN binary_protocol.nested_composite_type_domain AS binary_protocol.nested_composite_type;


INSERT INTO composite_type_table(col) VALUES  ((1, 2)::composite_type);
SELECT col FROM composite_type_table;
SELECT col::composite_type_domain FROM composite_type_table;
SELECT (col, col) FROM composite_type_table;
SELECT (col, col)::nested_composite_type FROM composite_type_table;
SELECT (col, col)::nested_composite_type_domain FROM composite_type_table;
SELECT ARRAY[col] FROM composite_type_table;
SELECT ARRAY[col::composite_type_domain] FROM composite_type_table;
SELECT ARRAY[(col, col)] FROM composite_type_table;
SELECT ARRAY[(col, col)::nested_composite_type] FROM composite_type_table;
SELECT ARRAY[(col, col)::nested_composite_type_domain] FROM composite_type_table;


-- Confirm that aclitem doesn't have receive and send functions
SELECT typreceive, typsend FROM pg_type WHERE typname = 'aclitem';
CREATE TABLE binaryless_builtin (
col1 aclitem NOT NULL,
col2 character varying(255) NOT NULL
);
SELECT create_reference_table('binaryless_builtin');

CREATE TYPE binaryless_composite_type AS (
    a aclitem,
    b aclitem
);

CREATE DOMAIN binary_protocol.binaryless_domain AS aclitem;
CREATE DOMAIN binary_protocol.binaryless_composite_domain AS binary_protocol.binaryless_composite_type;

INSERT INTO binaryless_builtin VALUES ('user postgres=r/postgres', 'test');
SELECT col1 FROM binaryless_builtin;
SELECT col1::binaryless_domain FROM binaryless_builtin;
SELECT (col1, col1) FROM binaryless_builtin;
SELECT (col1, col1)::binaryless_composite_type FROM binaryless_builtin;
SELECT (col1, col1)::binaryless_composite_domain FROM binaryless_builtin;
SELECT ARRAY[col1] FROM binaryless_builtin;
SELECT ARRAY[col1::binaryless_domain] FROM binaryless_builtin;
SELECT ARRAY[(col1, col1)] FROM binaryless_builtin;
SELECT ARRAY[(col1, col1)::binaryless_composite_type] FROM binaryless_builtin;
SELECT ARRAY[(col1, col1)::binaryless_composite_domain] FROM binaryless_builtin;

CREATE TABLE test_table_1(id int, val1 int);
CREATE TABLE test_table_2(id int, val1 bigint);
SELECT create_distributed_table('test_table_1', 'id');
SELECT create_distributed_table('test_table_2', 'id');
INSERT INTO test_table_1 VALUES(1,1),(2,4),(3,3);
INSERT INTO test_table_2 VALUES(1,1),(3,3),(4,5);

SELECT id, val1
FROM test_table_1 LEFT JOIN test_table_2 USING(id, val1)
ORDER BY 1, 2;

SET client_min_messages TO WARNING;
DROP SCHEMA binary_protocol CASCADE;
