\set VERBOSITY terse

SET citus.next_shard_id TO 1520000;

CREATE SCHEMA subscripting_op;
SET search_path TO subscripting_op;

CREATE TABLE arr_subs_update(id INT, arr INT[], text_col TEXT, int_col_1 INT, int_col_2 INT);
SELECT create_distributed_table('arr_subs_update', 'id');
INSERT INTO arr_subs_update VALUES (1, '{1,2,3}', 'foo', 50, 60), (2, '{4,5,6}', 'bar', 60, 70);

-- test if we can properly expand target list entries when there are dropped columns
ALTER TABLE arr_subs_update DROP COLUMN int_col_1;

UPDATE arr_subs_update
SET arr[1] = updated_vals.b,
    arr[3] = updated_vals.d,
    int_col_2 = 400,
    arr[2] = updated_vals.c
FROM (
  SELECT id,
         arr[0] AS a,
         arr[1]::NUMERIC + 1 AS b,
         arr[2]::NUMERIC + 2 AS c,
         arr[3]::NUMERIC + 3 AS d
  FROM arr_subs_update
) updated_vals
WHERE arr_subs_update.id = updated_vals.id;

SELECT * FROM arr_subs_update ORDER BY 1,2,3,4;

TRUNCATE arr_subs_update;
INSERT INTO arr_subs_update VALUES (1, '{1,2,3}', 'foo', 60), (2, '{4,5,6}', 'bar', 70);

ALTER TABLE arr_subs_update ADD CONSTRAINT pkey PRIMARY KEY (id, arr);

INSERT INTO arr_subs_update VALUES (1, '{1,2,3}')
ON CONFLICT (id, arr)
DO UPDATE SET arr[0]=100, arr[1]=200, arr[5]=500;

SELECT * FROM arr_subs_update ORDER BY 1,2,3,4;

CREATE DOMAIN single_int_dom AS int[] CHECK (VALUE[1] != 0);
CREATE DOMAIN dummy_dom AS single_int_dom CHECK (VALUE[2] != 5);

-- Citus doesn't propagate DOMAIN objects
SELECT run_command_on_workers(
$$
CREATE DOMAIN subscripting_op.single_int_dom AS INT[] CHECK (VALUE[1] != 0);
CREATE DOMAIN subscripting_op.dummy_dom AS subscripting_op.single_int_dom CHECK (VALUE[2] != 5);
$$);

CREATE TABLE dummy_dom_test (id int, dummy_dom_col dummy_dom);
SELECT create_distributed_table('dummy_dom_test', 'id');

INSERT INTO dummy_dom_test VALUES (1, '{1,2,3}'), (2, '{6,7,8}');

UPDATE dummy_dom_test
SET dummy_dom_col[2] = 50,
    dummy_dom_col[1] = 60;

SELECT * FROM dummy_dom_test ORDER BY 1,2;

CREATE TYPE two_ints as (if1 int, if2 int[]);
CREATE DOMAIN two_ints_dom AS two_ints CHECK ((VALUE).if1 > 0);

-- Citus doesn't propagate DOMAIN objects
SELECT run_command_on_workers(
$$
CREATE DOMAIN subscripting_op.two_ints_dom AS subscripting_op.two_ints CHECK ((VALUE).if1 > 0);
$$);

CREATE TABLE two_ints_dom_indirection_test (id int, two_ints_dom_col two_ints_dom);
SELECT create_distributed_table('two_ints_dom_indirection_test', 'id');

INSERT INTO two_ints_dom_indirection_test VALUES (1, '(5, "{1,2,3}")'), (2, '(50, "{10,20,30}")');

-- Citus planner already doesn't allow doing field indirection (e.g.:
-- insert/update <composite type>.<field>) and we have an extra guard against
-- that in deparser for future implementations; so here we test that by using
-- deparse_shard_query_test() as well.

-- i) planner would throw an error
UPDATE two_ints_dom_indirection_test
SET two_ints_dom_col.if2[1] = 50,
    two_ints_dom_col.if2[3] = 60;

-- ii) deparser would throw an error
SELECT public.deparse_shard_query_test(
$$
UPDATE two_ints_dom_indirection_test
SET two_ints_dom_col.if2[1] = 50,
    two_ints_dom_col.if2[3] = 60;
$$);

SET client_min_messages TO WARNING;
DROP SCHEMA subscripting_op CASCADE;
