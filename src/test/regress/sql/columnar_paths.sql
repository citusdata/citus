CREATE SCHEMA columnar_paths;
SET search_path TO columnar_paths;

-- columnar_paths has an alternative test output file because PG17 improved
-- the optimizer's ability to use statistics to estimate the size of a CTE
-- scan.
-- The relevant PG commit is:
-- https://github.com/postgres/postgres/commit/f7816aec23eed1dc1da5f9a53cb6507d30b7f0a2

CREATE TABLE full_correlated (a int, b text, c int, d int) USING columnar;
INSERT INTO full_correlated SELECT i, i::text FROM generate_series(1, 1000000) i;
CREATE INDEX full_correlated_btree ON full_correlated (a);
ANALYZE full_correlated;

-- Prevent qual pushdown from competing with index scans.
SET columnar.enable_qual_pushdown = false;

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a=200;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a<0;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a>10 AND a<20;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a>1000000;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a>900000;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a FROM full_correlated WHERE a>900000;
  $$
  );
ROLLBACK;

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a<1000;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a,b FROM full_correlated WHERE a<3000;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a<9000;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_index_scan (
  $$
  SELECT a FROM full_correlated WHERE a<9000;
  $$
  );
ROLLBACK;

BEGIN;
  TRUNCATE full_correlated;
  INSERT INTO full_correlated SELECT i, i::text FROM generate_series(1, 1000) i;

  -- Since we have much smaller number of rows, selectivity of below
  -- query should be much higher. So we would choose columnar custom scan.
  SELECT columnar_test_helpers.uses_custom_scan (
  $$
  SELECT a FROM full_correlated WHERE a=200;
  $$
  );

  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a FROM full_correlated WHERE a=200;
  $$
  );
ROLLBACK;

-- same filter used in above, but choosing multiple columns would increase
-- custom scan cost, so we would prefer index scan this time
SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a,b,c,d FROM full_correlated WHERE a<9000;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_index_scan (
  $$
  SELECT a,b,c,d FROM full_correlated WHERE a<9000;
  $$
  );
ROLLBACK;

-- again same filter used in above, but we would choose custom scan this
-- time since it would read three less columns from disk
SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT c FROM full_correlated WHERE a<10000;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_index_scan (
  $$
  SELECT c FROM full_correlated WHERE a<10000;
  $$
  );
ROLLBACK;

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a>200;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a=0 OR a=5;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a FROM full_correlated WHERE a=0 OR a=5;
  $$
  );
ROLLBACK;

--
-- some tests with joins / subqueries etc.
--
CREATE TABLE heap_table (a int, b text, c int, d int);
INSERT INTO heap_table SELECT i, i::text, (i+1000)*7, (i+900)*5 FROM generate_series(1, 1000000) i;
CREATE INDEX heap_table_btree ON heap_table (a);
ANALYZE heap_table;

EXPLAIN (COSTS OFF)
WITH cte AS MATERIALIZED (SELECT d FROM full_correlated WHERE a > 1)
SELECT SUM(ht_1.a), MIN(ct_1.c)
FROM heap_table AS ht_1
LEFT JOIN full_correlated AS ct_1 ON ht_1.a=ct_1.d
LEFT JOIN heap_table AS ht_2 ON ht_2.a=ct_1.c
JOIN cte ON cte.d=ht_1.a
WHERE ct_1.a < 3000;

-- same query but columnar custom scan is disabled
BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';

  EXPLAIN (COSTS OFF)
  WITH cte AS MATERIALIZED (SELECT d FROM full_correlated WHERE a > 1)
  SELECT SUM(ht_1.a), MIN(ct_1.c)
  FROM heap_table AS ht_1
  LEFT JOIN full_correlated AS ct_1 ON ht_1.a=ct_1.d
  LEFT JOIN heap_table AS ht_2 ON ht_2.a=ct_1.c
  JOIN cte ON cte.d=ht_1.a
  WHERE ct_1.a < 3000;
ROLLBACK;

-- use custom scan
EXPLAIN (COSTS OFF) WITH w AS (SELECT * FROM full_correlated)
SELECT * FROM w AS w1 JOIN w AS w2 ON w1.a = w2.d
WHERE w2.a = 123;

-- use index
EXPLAIN (COSTS OFF) WITH w AS NOT MATERIALIZED (SELECT * FROM full_correlated)
SELECT * FROM w AS w1 JOIN w AS w2 ON w1.a = w2.d
WHERE w2.a = 123;

EXPLAIN (COSTS OFF) SELECT sub_1.b, sub_2.a, sub_3.avg
FROM
  (SELECT b FROM full_correlated WHERE (a > 2) GROUP BY b ORDER BY 1 DESC LIMIT 5) AS sub_1,
  (SELECT a FROM full_correlated WHERE (a > 10) GROUP BY a HAVING count(DISTINCT a) >= 1 ORDER BY 1 DESC LIMIT 3) AS sub_2,
  (SELECT avg(a) AS AVG FROM full_correlated WHERE (a > 2) GROUP BY a HAVING sum(a) > 10 ORDER BY (sum(d) - avg(a) - COALESCE(array_upper(ARRAY[max(a)],1) * 5, 0)) DESC LIMIT 3) AS sub_3
WHERE sub_2.a < sub_1.b::integer
ORDER BY 3 DESC, 2 DESC, 1 DESC
LIMIT 100;

DROP INDEX full_correlated_btree;

CREATE INDEX full_correlated_hash ON full_correlated USING hash(a);
ANALYZE full_correlated;

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a<10;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a>1 AND a<10;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a=0 OR a=5;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_correlated WHERE a=1000;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a,c FROM full_correlated WHERE a=1000;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_index_scan (
  $$
  SELECT a,c FROM full_correlated WHERE a=1000;
  $$
  );
ROLLBACK;

CREATE TABLE full_anti_correlated (a int, b text) USING columnar;
INSERT INTO full_anti_correlated SELECT i, i::text FROM generate_series(1, 500000) i;
CREATE INDEX full_anti_correlated_hash ON full_anti_correlated USING hash(b);
ANALYZE full_anti_correlated;

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_anti_correlated WHERE b='600';
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a,b FROM full_anti_correlated WHERE b='600';
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a,b FROM full_anti_correlated WHERE b='600' OR b='10';
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a,b FROM full_anti_correlated WHERE b='600' OR b='10';
  $$
  );
ROLLBACK;

DROP INDEX full_anti_correlated_hash;

CREATE INDEX full_anti_correlated_btree ON full_anti_correlated (a,b);
ANALYZE full_anti_correlated;

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_anti_correlated WHERE a>6500 AND a<7000 AND b<'10000';
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_anti_correlated WHERE a>2000 AND a<7000;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_anti_correlated WHERE a<7000 AND b<'10000';
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a FROM full_anti_correlated WHERE a<7000 AND b<'10000';
  $$
  );
ROLLBACK;

CREATE TABLE no_correlation (a int, b text) USING columnar;
INSERT INTO no_correlation SELECT random()*5000, (random()*5000)::int::text FROM generate_series(1, 500000) i;
CREATE INDEX no_correlation_btree ON no_correlation (a);
ANALYZE no_correlation;

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM no_correlation WHERE a < 2;
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM no_correlation WHERE a = 200;
$$
);

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';
  SELECT columnar_test_helpers.uses_seq_scan (
  $$
  SELECT a FROM no_correlation WHERE a = 200;
  $$
  );
ROLLBACK;

SET columnar.enable_qual_pushdown TO DEFAULT;

BEGIN;
SET LOCAL columnar.stripe_row_limit = 2000;
SET LOCAL columnar.chunk_group_row_limit = 1000;

CREATE TABLE correlated(x int) using columnar;
INSERT INTO correlated
  SELECT g FROM generate_series(1,100000) g;

CREATE TABLE uncorrelated(x int) using columnar;
INSERT INTO uncorrelated
  SELECT (g * 19) % 100000 FROM generate_series(1,100000) g;

COMMIT;

CREATE INDEX correlated_idx ON correlated(x);
CREATE INDEX uncorrelated_idx ON uncorrelated(x);
ANALYZE correlated, uncorrelated;

-- should choose chunk group filtering; selective and correlated
EXPLAIN (analyze on, costs off, timing off, summary off)
SELECT * FROM correlated WHERE x = 78910;
SELECT * FROM correlated WHERE x = 78910;

-- should choose index scan; selective but uncorrelated
EXPLAIN (analyze on, costs off, timing off, summary off)
SELECT * FROM uncorrelated WHERE x = 78910;
SELECT * FROM uncorrelated WHERE x = 78910;

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_paths CASCADE;
