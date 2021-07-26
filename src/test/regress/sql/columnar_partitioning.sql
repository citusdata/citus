
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

-- run parallel plans
SET force_parallel_mode = regress;
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

SET force_parallel_mode TO DEFAULT;
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
