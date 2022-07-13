CREATE SCHEMA insert_select_into_local_table;
SET search_path TO insert_select_into_local_table;

SET citus.shard_count = 4;
SET citus.next_shard_id TO 11235800;


CREATE TABLE dist_table (a INT, b INT, c TEXT);
SELECT create_distributed_table('dist_table', 'a');

INSERT INTO dist_table VALUES (1, 6, 'txt1'), (2, 7, 'txt2'), (3, 8, 'txt3');

CREATE TABLE non_dist_1 (a INT, b INT, c TEXT);
CREATE TABLE non_dist_2 (a INT, c TEXT);
CREATE TABLE non_dist_3 (a INT);


-- test non-router queries
INSERT INTO non_dist_1 SELECT * FROM dist_table;
INSERT INTO non_dist_2 SELECT a, c FROM dist_table;
INSERT INTO non_dist_3 SELECT a FROM dist_table;

SELECT * FROM non_dist_1 ORDER BY 1, 2, 3;
SELECT * FROM non_dist_2 ORDER BY 1, 2;
SELECT * FROM non_dist_3 ORDER BY 1;

TRUNCATE non_dist_1, non_dist_2, non_dist_3;


-- test router queries
INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1;
INSERT INTO non_dist_2 SELECT a, c FROM dist_table WHERE a = 1;
INSERT INTO non_dist_3 SELECT a FROM dist_table WHERE a = 1;

SELECT * FROM non_dist_1 ORDER BY 1, 2, 3;
SELECT * FROM non_dist_2 ORDER BY 1, 2;
SELECT * FROM non_dist_3 ORDER BY 1;

TRUNCATE non_dist_1, non_dist_2, non_dist_3;


-- test columns in different order
INSERT INTO non_dist_1(b, a, c) SELECT a, b, c FROM dist_table;
SELECT * FROM non_dist_1 ORDER BY 1, 2, 3;

TRUNCATE non_dist_1;


-- test EXPLAIN
EXPLAIN (COSTS FALSE) INSERT INTO non_dist_1 SELECT * FROM dist_table;
EXPLAIN (COSTS FALSE) INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1;


-- test RETURNING
INSERT INTO non_dist_1 SELECT * FROM dist_table ORDER BY 1, 2, 3 RETURNING *;
INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1 ORDER BY 1, 2, 3 RETURNING *;


-- test INSERT INTO a table with UNIQUE
CREATE TABLE non_dist_unique (a INT UNIQUE, b INT);
INSERT INTO non_dist_unique SELECT a, b FROM dist_table;
SELECT * FROM non_dist_unique ORDER BY 1;
INSERT INTO non_dist_unique SELECT a+1, b FROM dist_table ON CONFLICT (a) DO NOTHING;
SELECT * FROM non_dist_unique ORDER BY 1;
INSERT INTO non_dist_unique SELECT a+2, b FROM dist_table ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b + non_dist_unique.b;
SELECT * FROM non_dist_unique ORDER BY 1;
DROP TABLE non_dist_unique;


-- test INSERT INTO a table with DEFAULT
CREATE TABLE non_dist_default (a INT, c TEXT DEFAULT 'def');
INSERT INTO non_dist_default SELECT a FROM dist_table WHERE a = 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
INSERT INTO non_dist_default SELECT a FROM dist_table WHERE a > 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
INSERT INTO non_dist_default SELECT a, c FROM dist_table WHERE a = 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
INSERT INTO non_dist_default SELECT a, c FROM dist_table WHERE a > 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
DROP TABLE non_dist_default;


-- test CTEs
WITH with_table AS (SELECT a, c FROM dist_table ORDER BY a LIMIT 2) INSERT INTO non_dist_2 SELECT * FROM with_table;
SELECT * FROM non_dist_2 ORDER BY 1, 2;

INSERT INTO non_dist_2 WITH with_table AS (SELECT a, c FROM dist_table ORDER BY a LIMIT 2) SELECT * FROM with_table;
SELECT * FROM non_dist_2 ORDER BY 1, 2;

TRUNCATE non_dist_2;

WITH deleted_rows AS (DELETE FROM dist_table WHERE a < 3 RETURNING a, c) INSERT INTO non_dist_2 SELECT * FROM deleted_rows;
SELECT * FROM dist_table ORDER BY 1, 2, 3;
SELECT * FROM non_dist_2 ORDER BY 1, 2;

TRUNCATE non_dist_2;
INSERT INTO dist_table VALUES (1, 6, 'txt1'), (2, 7, 'txt2');

WITH insert_table AS (INSERT INTO non_dist_2 SELECT a, c FROM dist_table RETURNING *) SELECT * FROM insert_table ORDER BY 1, 2;
SELECT * FROM non_dist_2 ORDER BY 1, 2;

TRUNCATE non_dist_2;


-- test PREPARE
PREPARE insert_select_into_local AS INSERT INTO non_dist_2 SELECT a, c FROM dist_table WHERE a = 1;
EXECUTE insert_select_into_local;
EXECUTE insert_select_into_local;
EXECUTE insert_select_into_local;
EXECUTE insert_select_into_local;
EXECUTE insert_select_into_local;
SELECT * FROM non_dist_2 ORDER BY 1, 2;
EXECUTE insert_select_into_local;
SELECT * FROM non_dist_2 ORDER BY 1, 2;
TRUNCATE non_dist_2;
DEALLOCATE insert_select_into_local;

PREPARE insert_select_into_local(int) AS INSERT INTO non_dist_2 SELECT a, c FROM dist_table WHERE a = $1;
EXECUTE insert_select_into_local(2);
EXECUTE insert_select_into_local(2);
EXECUTE insert_select_into_local(2);
EXECUTE insert_select_into_local(2);
EXECUTE insert_select_into_local(2);
SELECT * FROM non_dist_2 ORDER BY 1, 2;
EXECUTE insert_select_into_local(2);
SELECT * FROM non_dist_2 ORDER BY 1, 2;
TRUNCATE non_dist_2;
DEALLOCATE insert_select_into_local;


PREPARE insert_select_into_local(int) AS INSERT INTO non_dist_2 SELECT a, c FROM dist_table WHERE b = $1;
EXECUTE insert_select_into_local(8);
EXECUTE insert_select_into_local(8);
EXECUTE insert_select_into_local(8);
EXECUTE insert_select_into_local(8);
EXECUTE insert_select_into_local(8);
SELECT * FROM non_dist_2 ORDER BY 1, 2;
EXECUTE insert_select_into_local(8);
SELECT * FROM non_dist_2 ORDER BY 1, 2;
TRUNCATE non_dist_2;
DEALLOCATE insert_select_into_local;


-- test reference table
CREATE TABLE ref_table (a INT, b INT, c TEXT);
SELECT create_reference_table('ref_table');
INSERT INTO ref_table VALUES (1, 6, 'txt1'), (2, 7, 'txt2'), (3, 8, 'txt3');
INSERT INTO non_dist_2 SELECT a, c FROM ref_table;
SELECT * FROM non_dist_2 ORDER BY 1, 2;
TRUNCATE non_dist_2;

-- check issue https://github.com/citusdata/citus/issues/5858
CREATE TABLE local_table(
  col_1 integer,
  col_2 integer,
  col_3 text,
  col_4 text,
  col_5 int,
  col_6 text,
  col_7 text,
  col_8 text
);

CREATE TABLE dist_table_1(
  dist_col integer,
  int_col integer,
  text_col_1 text,
  text_col_2 text
);
SELECT create_distributed_table('dist_table_1', 'dist_col');

INSERT INTO dist_table_1 VALUES (1, 1, 'string', 'string');

CREATE TABLE dist_table_2(
  dist_col integer,
  int_col integer
);
SELECT create_distributed_table('dist_table_2', 'dist_col');

INSERT INTO dist_table_2 VALUES (1, 1);

INSERT INTO local_table
SELECT
  t1.dist_col,
  1,
  'string',
  'string',
  1,
  'string',
  t1.text_col_1,
  t1.text_col_2
FROM dist_table_1 t1
WHERE t1.int_col IN (SELECT int_col FROM dist_table_2);

INSERT INTO local_table
SELECT
  t1.dist_col,
  1,
  'string',
  'string',
  1,
  'string',
  t1.text_col_1,
  t1.text_col_2
FROM dist_table_1 t1
returning *;
INSERT INTO local_table (col_3, col_4) SELECT
  'string',
  'string'::text
FROM dist_table_1 t1
returning *;

EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF) INSERT INTO local_table
  SELECT
    t1.dist_col,
    1,
    'string',
    'string',
    1,
    'string',
    t1.text_col_1,
    t1.text_col_2
  FROM dist_table_1 t1
  RETURNING *;

\set VERBOSITY terse
DROP SCHEMA insert_select_into_local_table CASCADE;
