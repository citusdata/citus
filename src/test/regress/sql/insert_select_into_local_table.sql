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

INSERT INTO non_dist_unique
SELECT a+1, b FROM dist_table
UNION ALL
SELECT a+100, b FROM dist_table
ON CONFLICT (a) DO NOTHING;
SELECT * FROM non_dist_unique ORDER BY 1;

INSERT INTO non_dist_unique
SELECT a+1, b FROM dist_table
UNION ALL
SELECT a+100, b FROM dist_table
ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b + 1;
SELECT * FROM non_dist_unique ORDER BY 1;

WITH cte1 AS (SELECT s FROM generate_series(1,10) s)
INSERT INTO non_dist_unique
WITH cte2 AS (SELECT s FROM generate_series(1,10) s)
SELECT a+1, b FROM dist_table WHERE b IN (SELECT s FROM cte1)
UNION ALL
SELECT s, s FROM cte1
ON CONFLICT (a) DO NOTHING;
SELECT * FROM non_dist_unique ORDER BY 1;

DROP TABLE non_dist_unique;


-- test INSERT INTO a table with DEFAULT
CREATE TABLE non_dist_default (a INT, c TEXT DEFAULT 'def');
INSERT INTO non_dist_default SELECT a FROM dist_table WHERE a = 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
INSERT INTO non_dist_default SELECT a FROM dist_table WHERE a > 1;
SELECT * FROM non_dist_default ORDER BY 1, 2;
SELECT alter_table_set_access_method('non_dist_default', 'columnar');
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
CREATE TABLE local_dest_table(
  col_1 integer,
  col_2 integer,
  col_3 text,
  col_4 text,
  drop_col text,
  col_5 bigint,
  col_6 text,
  col_7 text default 'col_7',
  col_8 varchar
);

ALTER TABLE local_dest_table DROP COLUMN drop_col;

CREATE TABLE dist_source_table_1(
  int_col integer,
  drop_col text,
  text_col_1 text,
  dist_col integer,
  text_col_2 text
);
SELECT create_distributed_table('dist_source_table_1', 'dist_col');

ALTER TABLE dist_source_table_1 DROP COLUMN drop_col;

INSERT INTO dist_source_table_1 VALUES (1, 'value', 1, 'value');
INSERT INTO dist_source_table_1 VALUES (2, 'value2', 1, 'value');
INSERT INTO dist_source_table_1 VALUES (3, 'value', 3, 'value3');

CREATE TABLE dist_source_table_2(
  dist_col integer,
  int_col integer
);
SELECT create_distributed_table('dist_source_table_2', 'dist_col');

INSERT INTO dist_source_table_2 VALUES (1, 1);
INSERT INTO dist_source_table_2 VALUES (2, 2);
INSERT INTO dist_source_table_2 VALUES (4, 4);

CREATE TABLE local_source_table_1 AS SELECT * FROM dist_source_table_1;
CREATE TABLE local_source_table_2 AS SELECT * FROM dist_source_table_2;

/*
 * query_results_equal compares the effect of two queries on local_dest_table.
 * We use this to ensure that INSERT INTO local_dest_table SELECT behaves
 * the same when selecting from a regular table (postgres handles it) and
 * a distributed table (Citus handles it).
 *
 * The queries are generated by calling format() on query_table twice,
 * once for each source_table argument.
 */
CREATE OR REPLACE FUNCTION query_results_equal(query_template text, source_table_1 text, source_table_2 text)
RETURNS bool
AS $$
DECLARE
	l1 local_dest_table[];
	l2 local_dest_table[];
BEGIN
	/* get the results using source_table_1 as source */
    TRUNCATE local_dest_table;
	EXECUTE format(query_template, source_table_1);
	SELECT array_agg(l) INTO l1
	FROM (SELECT * FROM local_dest_table ORDER BY 1, 2, 3, 4, 5, 6, 7, 8) l;

	/* get the results using source_table_2 as source */
    TRUNCATE local_dest_table;
	EXECUTE format(query_template, source_table_2);
	SELECT array_agg(l) INTO l2
	FROM (SELECT * FROM local_dest_table ORDER BY 1, 2, 3, 4, 5, 6, 7, 8) l;

	RAISE NOTICE 'l2=%', l1;
	RAISE NOTICE 'l2=%', l2;
	RETURN l1 = l2;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table
  SELECT
    t1.dist_col,
    1,
    'string1',
    'string2',
    2,
    'string3',
    t1.text_col_1,
    t1.text_col_2
  FROM %1$s_1 t1
  WHERE t1.int_col IN (SELECT int_col FROM %1$s_2)
$$, 'local_source_table', 'dist_source_table');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table
  SELECT
    t1.dist_col,
    1,
    'string1',
    'string2',
    2,
    'string3',
    t1.text_col_1,
    t1.text_col_2
  FROM %1$s t1
  returning *
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_3, col_4) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_7, col_4) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_4, col_3) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  WHERE dist_col = 1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s
  UNION ALL
  SELECT
    'string',
     int_col
  FROM %1$s;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  WITH cte1 AS (SELECT s FROM generate_series(1,10) s)
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s WHERE int_col IN (SELECT s FROM cte1)
  UNION ALL
  SELECT
    'string',
     int_col
  FROM %1$s WHERE int_col IN (SELECT s + 1 FROM cte1)
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  WITH cte1 AS (SELECT 'stringcte', s FROM generate_series(1,10) s)
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s WHERE int_col IN (SELECT s FROM cte1)
  UNION ALL
  SELECT
    *
  FROM cte1
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_3)
  SELECT t1.text_col_1
  FROM %1$s t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_1, col_2, col_3, col_5, col_6, col_7, col_8)
  SELECT
    max(t1.dist_col),
    3,
    'string_3',
    4,
    44,
    t1.text_col_1,
    'string_1000'
  FROM %1$s t1
  GROUP BY t1.text_col_2, t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_7, col_8)
  SELECT
    t1.text_col_1,
    'string_1000'
  FROM dist_source_table_1 t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_6, col_7, col_8)
  SELECT
    'string_4',
    t1.text_col_1,
    'string_1000'
  FROM %1$s t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_5, col_3)
  SELECT 12, 'string_11' FROM %1$s t1
  UNION
  SELECT int_col, 'string' FROM %1$s;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table(col_3, col_2)
  SELECT text_col_1, count(*) FROM %1$s GROUP BY 1
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table(col_3, col_5)
  SELECT text_col_1, count(*)::int FROM %1$s GROUP BY 1
$$, 'local_source_table_1', 'dist_source_table_1');

-- repeat above tests with Citus local table
SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table
  SELECT
    t1.dist_col,
    1,
    'string1',
    'string2',
    2,
    'string3',
    t1.text_col_1,
    t1.text_col_2
  FROM %1$s_1 t1
  WHERE t1.int_col IN (SELECT int_col FROM %1$s_2)
$$, 'local_source_table', 'dist_source_table');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table
  SELECT
    t1.dist_col,
    1,
    'string1',
    'string2',
    2,
    'string3',
    t1.text_col_1,
    t1.text_col_2
  FROM %1$s t1
  returning *
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_3, col_4) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_7, col_4) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_4, col_3) SELECT
    'string1',
    'string2'::text
  FROM %1$s t1
  WHERE dist_col = 1
  returning *;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s
  UNION ALL
  SELECT
    'string',
     int_col
  FROM %1$s;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  WITH cte1 AS (SELECT s FROM generate_series(1,10) s)
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s WHERE int_col IN (SELECT s FROM cte1)
  UNION ALL
  SELECT
    'string',
     int_col
  FROM %1$s WHERE int_col IN (SELECT s + 1 FROM cte1)
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  WITH cte1 AS (SELECT 'stringcte', s FROM generate_series(1,10) s)
  INSERT INTO local_dest_table (col_4, col_1)
  SELECT
    'string1',
     dist_col
  FROM %1$s WHERE int_col IN (SELECT s FROM cte1)
  UNION ALL
  SELECT
    *
  FROM cte1
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_3)
  SELECT t1.text_col_1
  FROM %1$s t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_1, col_2, col_3, col_5, col_6, col_7, col_8)
  SELECT
    max(t1.dist_col),
    3,
    'string_3',
    4,
    44,
    t1.text_col_1,
    'string_1000'
  FROM %1$s t1
  GROUP BY t1.text_col_2, t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_7, col_8)
  SELECT
    t1.text_col_1,
    'string_1000'
  FROM dist_source_table_1 t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_6, col_7, col_8)
  SELECT
    'string_4',
    t1.text_col_1,
    'string_1000'
  FROM %1$s t1
  GROUP BY t1.text_col_1;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table (col_5, col_3)
  SELECT 12, 'string_11' FROM %1$s t1
  UNION
  SELECT int_col, 'string' FROM %1$s;
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table(col_3, col_2)
  SELECT text_col_1, count(*) FROM %1$s GROUP BY 1
$$, 'local_source_table_1', 'dist_source_table_1');

SELECT * FROM query_results_equal($$
  INSERT INTO local_dest_table(col_3, col_5)
  SELECT text_col_1, count(*)::int FROM %1$s GROUP BY 1
$$, 'local_source_table_1', 'dist_source_table_1');

-- go back to proper local table for remaining tests
TRUNCATE local_dest_table;
SELECT undistribute_table('local_source_table_1');

-- use a sequence (cannot use query_results_equal, since sequence values would not match)
CREATE SEQUENCE seq;

BEGIN;
INSERT INTO local_dest_table (col_5, col_3)
SELECT 12, 'string_11' FROM dist_source_table_1
UNION
SELECT nextval('seq'), 'string' FROM dist_source_table_1;
SELECT * FROM local_dest_table ORDER BY 1,2,3,4,5,6,7,8;
ROLLBACK;

-- add a bigserial column
ALTER TABLE local_dest_table ADD COLUMN col_9 bigserial;

-- not supported due to limitations in nextval handling
INSERT INTO local_dest_table (col_5, col_3)
SELECT 12, 'string_11' FROM dist_source_table_1
UNION
SELECT 11, 'string' FROM dist_source_table_1;
SELECT * FROM local_dest_table ORDER BY 1,2,3,4,5,6,7,8;

BEGIN;
INSERT INTO local_dest_table(col_3, col_2)
SELECT text_col_1, count(*) FROM dist_source_table_1 GROUP BY 1;
SELECT * FROM local_dest_table ORDER BY 1,2,3,4,5,6,7,8;
ROLLBACK;

BEGIN;
INSERT INTO local_dest_table (col_4, col_3) SELECT
  'string1',
  'string2'::text
FROM dist_source_table_1 t1
WHERE dist_col = 1
RETURNING *;
ROLLBACK;

\set VERBOSITY terse
DROP SCHEMA insert_select_into_local_table CASCADE;
