CREATE SCHEMA columnar_paths;
SET search_path TO columnar_paths;

CREATE TABLE full_correlated (a int, b text, c int, d int) USING columnar;
INSERT INTO full_correlated SELECT i, i::text FROM generate_series(1, 1000000) i;
CREATE INDEX full_correlated_btree ON full_correlated (a);
ANALYZE full_correlated;

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
  TRUNCATE full_correlated;
  INSERT INTO full_correlated SELECT i, i::text FROM generate_series(1, 1000) i;

  -- Since we have much smaller number of rows, selectivity of below
  -- query should be much higher. So we would choose columnar custom scan.
  SELECT columnar_test_helpers.uses_custom_scan (
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

-- again same filter used in above, but we would choose custom scan this
-- time since it would read three less columns from disk
SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT c FROM full_correlated WHERE a<10000;
$$
);

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

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_correlated WHERE a=1000;
$$
);

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a,c FROM full_correlated WHERE a=1000;
$$
);

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

SELECT columnar_test_helpers.uses_index_scan (
$$
SELECT a FROM full_anti_correlated WHERE a>2000 AND a<7000 AND b='24';
$$
);

SELECT columnar_test_helpers.uses_custom_scan (
$$
SELECT a FROM full_anti_correlated WHERE a<7000 AND b<'10000';
$$
);

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

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_paths CASCADE;
