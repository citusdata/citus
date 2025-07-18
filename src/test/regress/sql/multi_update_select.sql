CREATE SCHEMA multi_update_select;
SET search_path TO multi_update_select;

SET citus.next_shard_id TO 751000;

-- specific tests related to get_update_query_targetlist_def
-- we test only queries with sublinks, like:
-- ( ... SET (...) = (SELECT ...))

-- Reference tables
CREATE TABLE test_ref_indirection (
    id bigint primary key
  , col_bool bool , col_date date , col_int integer , col_text text
  );
SELECT create_reference_table('test_ref_indirection');

CREATE TABLE test_ref_indirection_new (
    id bigint primary key
  , col_bool bool , col_date date , col_int integer , col_text text
  );
SELECT create_reference_table('test_ref_indirection_new');

-- Distributed tables
CREATE TABLE test_dist_indirection (
    id bigint primary key
  , col_bool bool , col_date date , col_int integer , col_text text
  );
SELECT create_distributed_table('test_dist_indirection', 'id');

CREATE TABLE test_dist_indirection_new (
    id bigint primary key
  , col_bool bool , col_date date , col_int integer , col_text text
  );
SELECT create_distributed_table('test_dist_indirection_new', 'id');

-- those should work:
INSERT INTO test_ref_indirection (id, col_bool, col_date, col_int, col_text)
    SELECT 1, true, '1970-01-01'::date, 1, 'one';
INSERT INTO test_dist_indirection (id, col_bool, col_date, col_int, col_text)
    SELECT 1, true, '1970-01-01'::date, 1, 'one';

INSERT INTO test_ref_indirection (id, col_text, col_bool, col_date, col_int)
    SELECT 2, 'two', false, '1970-01-01'::date, 2;
INSERT INTO test_dist_indirection (id, col_text, col_bool, col_date, col_int)
    SELECT 2, 'two', false, '1970-01-01'::date, 2;

INSERT INTO test_ref_indirection SELECT 3, false, '1970-01-01'::date, 0, 'empty';
INSERT INTO test_dist_indirection SELECT 3, false, '1970-01-01'::date, 0, 'empty';
INSERT INTO test_ref_indirection SELECT 4, false, '1970-01-01'::date, 0, 'empty';
INSERT INTO test_dist_indirection SELECT 4, false, '1970-01-01'::date, 0, 'empty';

INSERT INTO test_ref_indirection_new SELECT * FROM test_ref_indirection;
INSERT INTO test_dist_indirection_new SELECT * FROM test_dist_indirection;

SELECT * FROM test_ref_indirection ORDER BY id;
SELECT * FROM test_dist_indirection ORDER BY id;

SELECT * FROM test_ref_indirection_new ORDER BY id;
SELECT * FROM test_dist_indirection_new ORDER BY id;

-- now UPDATEs
UPDATE test_ref_indirection
    SET (col_bool, col_date, col_int, col_text)
        = (SELECT true, '1970-01-01'::date, 1, 'ok')
RETURNING *;
UPDATE test_dist_indirection
    SET (col_bool, col_date, col_int, col_text)
        = (SELECT true, '1970-01-01'::date, 1, 'ok')
RETURNING *;

UPDATE test_ref_indirection
    SET (col_bool, col_date)  = (select false, '1971-01-01'::date)
      , (col_int, col_text) = (select 2, '2 ok')
RETURNING *;
UPDATE test_dist_indirection
    SET (col_bool, col_date)  = (select false, '1971-01-01'::date)
      , (col_int, col_text) = (select 2, '2 ok')
RETURNING *;

UPDATE test_ref_indirection
    SET (col_bool, col_int)  = (select true, 3)
      , (col_text) = (select '3 ok')
RETURNING *;
UPDATE test_dist_indirection
    SET (col_bool, col_int)  = (select true, 3)
      , (col_text) = (select '3 ok')
RETURNING *;

-- but those should work since 13.X
UPDATE test_ref_indirection
    SET (col_date, col_text, col_int, col_bool)
        = (SELECT '1972-01-01'::date, '4 ok', 4, false)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_date, col_text, col_int, col_bool)
        = (SELECT '1972-01-01'::date, '4 ok', 4, false)
RETURNING *;

UPDATE test_ref_indirection
    SET (col_int, col_text)  = (select 5, '5 ok')
      , (col_bool) = (select true)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_int, col_text)  = (select 5, '5 ok')
      , (col_bool) = (select true)
RETURNING *;

UPDATE test_ref_indirection
    SET (col_int, col_date)  = (select 6, '1973-01-01'::date)
      , (col_text, col_bool) = (select '6 ok', false)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_int, col_date)  = (select 6, '1973-01-01'::date)
      , (col_text, col_bool) = (select '6 ok', false)
RETURNING *;

UPDATE test_ref_indirection
    SET (col_int, col_date, col_text)  = (select 7, '1974-01-01'::date, '7 ok')
      , (col_bool) = (select true)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_int, col_date, col_text)  = (select 7, '1974-01-01'::date, '7 ok')
      , (col_bool) = (select true)
RETURNING *;

UPDATE test_ref_indirection
    SET (col_date, col_text)  = (select '1975-01-01'::date, '8 ok')
      , (col_int) = (select 8)
      , (col_bool) = (select false)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_date, col_text)  = (select '1975-01-01'::date, '8 ok')
      , (col_int) = (select 8)
      , (col_bool) = (select false)
RETURNING *;

--
-- more restrictive ones, just in case we miss a wrong value
--
-- those should work
UPDATE test_ref_indirection
    SET (col_bool, col_text) = (SELECT true, '9 ok')
RETURNING *;
UPDATE test_dist_indirection
    SET (col_bool, col_text) = (SELECT true, '9 ok')
RETURNING *;

UPDATE test_ref_indirection
    SET (col_bool, col_text) = (SELECT false, '10 ok')
WHERE id = 1
RETURNING *;
UPDATE test_dist_indirection
    SET (col_bool, col_text) = (SELECT false, '10 ok')
WHERE id = 1
RETURNING *;

UPDATE test_ref_indirection
    SET (col_text, col_bool) = (SELECT '11 ok', true)
RETURNING *;
UPDATE test_dist_indirection
    SET (col_text, col_bool) = (SELECT '11 ok', true)
RETURNING *;

UPDATE test_ref_indirection
    SET (col_text, col_bool) = (SELECT '12 ok', false)
WHERE id = 2
RETURNING *;
UPDATE test_dist_indirection
    SET (col_text, col_bool) = (SELECT '12 ok', false)
WHERE id = 2
RETURNING *;

-- several updates in CTE shoult not work
with qq3 as (
    update test_ref_indirection
    SET (col_text, col_bool)
      = (SELECT '13', true)
    where id = 3
    returning *
),
qq4 as (
    update test_ref_indirection
    SET (col_text, col_bool)
      = (SELECT '14', false)
    where id = 4
    returning *
)
select * from qq3 union all select * from qq4;
with qq3 as (
    update test_dist_indirection
    SET (col_text, col_bool)
      = (SELECT '13', true)
    where id = 3
    returning *
),
qq4 as (
    update test_dist_indirection
    SET (col_text, col_bool)
      = (SELECT '14', false)
    where id = 4
    returning *
)
select * from qq3 union all select * from qq4;

DROP TABLE test_dist_indirection;
DROP TABLE test_dist_indirection_new;
DROP TABLE test_ref_indirection;
DROP TABLE test_ref_indirection_new;

-- https://github.com/citusdata/citus/issues/4092
CREATE TABLE update_test (
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

SELECT create_reference_table('update_test');
INSERT INTO update_test VALUES (11, 41, 'car');
INSERT INTO update_test VALUES (100, 20, 'bike');
INSERT INTO update_test VALUES (100, 20, 'tractor');
SELECT * FROM update_test;

UPDATE update_test
SET (b,a) = (select a,b from update_test where b = 41 and c = 'car')
WHERE a = 100 AND b = 20
RETURNING *;

-- Test that multiple out of order columns and multiple sublinks are handled correctly.
CREATE TABLE upd2_test (a1 int, b1 int, c1 int, d1 int, e1 int, f1 int, g1 int);
SELECT create_reference_table('upd2_test');

INSERT INTO upd2_test SELECT 1, 1, 1, 1, 1, 1, 1 FROM generate_series(1,5) c(i);

UPDATE upd2_test set (b1, a1) = (SELECT 200, 100), (g1, f1, e1) = (SELECT 700, 600, 500), (d1, c1) = (SELECT 400, 300);
SELECT * FROM upd2_test;

UPDATE upd2_test set (g1, a1) = (SELECT 77, 11), (f1, b1) = (SELECT 66, 22), (e1, c1) = (SELECT 55, 33), (d1) = (SELECT 44);
SELECT * FROM upd2_test;

UPDATE upd2_test set (g1, a1) = (SELECT 7, 1), (f1) = (SELECT 6), (c1, e1) = (SELECT 3, 5), (b1) = (SELECT 2), (d1) = (SELECT 4);
SELECT * FROM upd2_test;

-- Test out of order updates on distributed table
CREATE TABLE dist_test(a1 int, b1 numeric, c1 text, d1 int);
SELECT create_distributed_table('dist_test', 'a1');
INSERT INTO dist_test VALUES (1, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (1, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (2, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (2, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (3, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (3, 10.0, 'xxx', 4);
INSERT INTO dist_test VALUES (3, 10.0, 'xxx', 4);

-- Router plan:
UPDATE dist_test
SET (d1, c1, b1) = (SELECT 7, 'tractor', 4.2)
WHERE a1=1
RETURNING *;

-- Pushdown plan:
UPDATE dist_test
SET (d1, c1, b1) = (SELECT X, 'car', Y)
FROM (SELECT r.a * d1 as X, r.b * b1 as Y FROM update_test r, dist_test WHERE r.c=c1) upd
WHERE dist_test.a1 > 2
RETURNING *;

-- suppress cascade messages
SET client_min_messages to ERROR;
DROP SCHEMA multi_update_select CASCADE;
RESET client_min_messages;

