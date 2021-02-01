
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

DROP TABLE parent;
