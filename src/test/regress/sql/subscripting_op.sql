\set VERBOSITY terse

SET citus.next_shard_id TO 1520000;

CREATE SCHEMA subscripting_op;
SET search_path TO subscripting_op;

CREATE TABLE arr_subs_update(id INT, arr INT[], text_col TEXT, int_col_1 INT, int_col_2 INT);
SELECT create_distributed_table('arr_subs_update', 'id');
INSERT INTO arr_subs_update
  VALUES (1, '{1,2,3}', 'foo', 50, 60),
         (2, '{4,5,6}', 'bar', 60, 70),
         (3, '{7,8,9}', 'baz', 70, 80);

BEGIN;
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

ROLLBACK;

BEGIN;
-- Test fast path router plan for subscripting update
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
WHERE arr_subs_update.id = updated_vals.id
  AND arr_subs_update.id = 1;

SELECT * FROM arr_subs_update
WHERE id=1 ORDER BY 1,2,3,4;

ROLLBACK;

-- test if we can properly expand target list entries when there are dropped columns
ALTER TABLE arr_subs_update DROP COLUMN int_col_1;

BEGIN;
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

ROLLBACK;

TRUNCATE arr_subs_update;
INSERT INTO arr_subs_update VALUES (1, '{1,2,3}', 'foo', 60), (2, '{4,5,6}', 'bar', 70);

ALTER TABLE arr_subs_update ADD CONSTRAINT pkey PRIMARY KEY (id, arr);

INSERT INTO arr_subs_update VALUES (1, '{1,2,3}')
ON CONFLICT (id, arr)
DO UPDATE SET arr[0]=100, arr[1]=200, arr[5]=500;

SELECT * FROM arr_subs_update ORDER BY 1,2,3,4;

SET client_min_messages TO WARNING;
DROP SCHEMA subscripting_op CASCADE;
