--
-- COLUMNAR_PARTITIONING
--

CREATE TABLE parent(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

-- row partitions
CREATE TABLE p0 PARTITION OF parent
  FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE p1 PARTITION OF parent
  FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
CREATE TABLE p2 PARTITION OF parent
  FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');
CREATE TABLE p3 PARTITION OF parent
  FOR VALUES FROM ('2020-04-01') TO ('2020-05-01');

INSERT INTO parent SELECT '2020-01-15', 10, 100, 'one thousand'
  FROM generate_series(1,100000);
INSERT INTO parent SELECT '2020-02-15', 20, 200, 'two thousand'
  FROM generate_series(1,100000);
INSERT INTO parent SELECT '2020-03-15', 30, 300, 'three thousand'
  FROM generate_series(1,100000);
INSERT INTO parent SELECT '2020-04-15', 30, 300, 'three thousand'
  FROM generate_series(1,100000);

SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16
\gset

-- run parallel plans
\if :server_version_ge_16
SET debug_parallel_query = regress;
\else
SET force_parallel_mode = regress;
\endif
SET min_parallel_table_scan_size = 1;
SET parallel_tuple_cost = 0;
SET max_parallel_workers = 4;
SET max_parallel_workers_per_gather = 4;

EXPLAIN (costs off) SELECT count(*), sum(i), min(i), max(i) FROM parent;
SELECT count(*), sum(i), min(i), max(i) FROM parent;

-- set older partitions as columnar
SELECT alter_table_set_access_method('p0','columnar');
SELECT alter_table_set_access_method('p1','columnar');
SELECT alter_table_set_access_method('p3','columnar');

-- should also be parallel plan
EXPLAIN (costs off) SELECT count(*), sum(i), min(i), max(i) FROM parent;
SELECT count(*), sum(i), min(i), max(i) FROM parent;

-- and also parallel without custom scan
SET columnar.enable_custom_scan = FALSE;
EXPLAIN (costs off) SELECT count(*), sum(i), min(i), max(i) FROM parent;
SELECT count(*), sum(i), min(i), max(i) FROM parent;
SET columnar.enable_custom_scan TO DEFAULT;

\if :server_version_ge_16
SET debug_parallel_query TO DEFAULT;
\else
SET force_parallel_mode TO DEFAULT;
\endif
SET min_parallel_table_scan_size TO DEFAULT;
SET parallel_tuple_cost TO DEFAULT;
SET max_parallel_workers TO DEFAULT;
SET max_parallel_workers_per_gather TO DEFAULT;

CREATE INDEX parent_btree ON parent (n);
ANALYZE parent;

-- will use columnar custom scan on columnar partitions but index
-- scan on heap partition
EXPLAIN (costs off) SELECT count(*), sum(i), min(i), max(i) FROM parent
WHERE ts > '2020-02-20' AND n < 5;

BEGIN;
  SET LOCAL columnar.enable_custom_scan TO 'OFF';

  -- now that we disabled columnar custom scan, will use seq scan on columnar
  -- partitions since index scan is more expensive than seq scan too
  EXPLAIN (costs off) SELECT count(*), sum(i), min(i), max(i) FROM parent
  WHERE ts > '2020-02-20' AND n < 5;
ROLLBACK;

DROP TABLE parent;

--
-- Test inheritance
--

CREATE TABLE i_row(i int);
INSERT INTO i_row VALUES(100);
CREATE TABLE i_col(i int) USING columnar;
INSERT INTO i_col VALUES(200);
CREATE TABLE ij_row_row(j int) INHERITS(i_row);
INSERT INTO ij_row_row VALUES(300, 1000);
CREATE TABLE ij_row_col(j int) INHERITS(i_row) USING columnar;
INSERT INTO ij_row_col VALUES(400, 2000);
CREATE TABLE ij_col_row(j int) INHERITS(i_col);
INSERT INTO ij_col_row VALUES(500, 3000);
CREATE TABLE ij_col_col(j int) INHERITS(i_col) USING columnar;
INSERT INTO ij_col_col VALUES(600, 4000);

EXPLAIN (costs off) SELECT * FROM i_row;
SELECT * FROM i_row;

EXPLAIN (costs off) SELECT * FROM ONLY i_row;
SELECT * FROM ONLY i_row;

EXPLAIN (costs off) SELECT * FROM i_col;
SELECT * FROM i_col;

EXPLAIN (costs off) SELECT * FROM ONLY i_col;
SELECT * FROM ONLY i_col;

EXPLAIN (costs off) SELECT * FROM ij_row_row;
SELECT * FROM ij_row_row;

EXPLAIN (costs off) SELECT * FROM ij_row_col;
SELECT * FROM ij_row_col;

EXPLAIN (costs off) SELECT * FROM ij_col_row;
SELECT * FROM ij_col_row;

EXPLAIN (costs off) SELECT * FROM ij_col_col;
SELECT * FROM ij_col_col;

SET columnar.enable_custom_scan = FALSE;

EXPLAIN (costs off) SELECT * FROM i_row;
SELECT * FROM i_row;

EXPLAIN (costs off) SELECT * FROM ONLY i_row;
SELECT * FROM ONLY i_row;

EXPLAIN (costs off) SELECT * FROM i_col;
SELECT * FROM i_col;

EXPLAIN (costs off) SELECT * FROM ONLY i_col;
SELECT * FROM ONLY i_col;

EXPLAIN (costs off) SELECT * FROM ij_row_row;
SELECT * FROM ij_row_row;

EXPLAIN (costs off) SELECT * FROM ij_row_col;
SELECT * FROM ij_row_col;

EXPLAIN (costs off) SELECT * FROM ij_col_row;
SELECT * FROM ij_col_row;

EXPLAIN (costs off) SELECT * FROM ij_col_col;
SELECT * FROM ij_col_col;

SET columnar.enable_custom_scan TO DEFAULT;

-- remove the child table from the inheritance hierarchy table
ALTER TABLE ij_row_row NO INHERIT i_row;
DROP TABLE ij_row_row;

DROP TABLE i_row CASCADE;
DROP TABLE i_col CASCADE;

--
-- https://github.com/citusdata/citus/issues/5257
--

set default_table_access_method to columnar;
CREATE TABLE prt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE prt1_p1 PARTITION OF prt1 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt1_p3 PARTITION OF prt1 FOR VALUES FROM (500) TO (600);
CREATE TABLE prt1_p2 PARTITION OF prt1 FOR VALUES FROM (250) TO (500);
INSERT INTO prt1 SELECT i, i % 25, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 2 = 0;

CREATE TABLE prt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE prt2_p1 PARTITION OF prt2 FOR VALUES FROM (0) TO (250);
CREATE TABLE prt2_p2 PARTITION OF prt2 FOR VALUES FROM (250) TO (500);
CREATE TABLE prt2_p3 PARTITION OF prt2 FOR VALUES FROM (500) TO (600);
INSERT INTO prt2 SELECT i % 25, i, to_char(i, 'FM0000') FROM generate_series(0, 599) i WHERE i % 3 = 0;

SET enable_partitionwise_join to true;

EXPLAIN (costs off, timing off, summary off)
SELECT * FROM
  prt1 t1 LEFT JOIN LATERAL
  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b)
    FROM prt1 t2
    JOIN prt2 t3 ON (t2.a = t3.b)
  ) ss
  ON t1.a = ss.t2a WHERE t1.b = 0
  ORDER BY t1.a;

SELECT * FROM
  prt1 t1 LEFT JOIN LATERAL
  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b)
    FROM prt1 t2
    JOIN prt2 t3 ON (t2.a = t3.b)
  ) ss
  ON t1.a = ss.t2a WHERE t1.b = 0
  ORDER BY t1.a;

set default_table_access_method to default;
SET enable_partitionwise_join to default;
DROP TABLE prt1;
DROP TABLE prt2;
