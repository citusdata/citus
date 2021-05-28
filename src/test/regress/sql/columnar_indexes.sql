--
-- Testing indexes on on columnar tables.
--

CREATE SCHEMA columnar_indexes;
SET search_path tO columnar_indexes, public;

--
-- create index with the concurrent option. We should
-- error out during index creation.
-- https://github.com/citusdata/citus/issues/4599
--
create table t(a int, b int) using columnar;
create index CONCURRENTLY t_idx on t(a, b);
REINDEX INDEX CONCURRENTLY t_idx;
\d t
explain insert into t values (1, 2);
insert into t values (1, 2);
SELECT * FROM t;

explain insert into t values (1, 2);
insert into t values (3, 4);
SELECT * FROM t;

-- make sure that we test index scan
set columnar.enable_custom_scan to 'off';
set enable_seqscan to off;

CREATE table columnar_table (a INT, b int) USING columnar;

INSERT INTO columnar_table (a) VALUES (1), (1);
CREATE UNIQUE INDEX CONCURRENTLY ON columnar_table (a);

-- CONCURRENTLY should leave an invalid index behind
SELECT COUNT(*)=1 FROM pg_index WHERE indrelid = 'columnar_table'::regclass AND indisvalid = 'false';

INSERT INTO columnar_table (a) VALUES (1), (1);

REINDEX TABLE columnar_table;
-- index is still invalid since REINDEX error'ed out
SELECT COUNT(*)=1 FROM pg_index WHERE indrelid = 'columnar_table'::regclass AND indisvalid = 'false';

TRUNCATE columnar_table;
REINDEX TABLE columnar_table;

-- now it should be valid
SELECT COUNT(*)=0 FROM pg_index WHERE indrelid = 'columnar_table'::regclass AND indisvalid = 'false';

DROP INDEX columnar_table_a_idx;

INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(0, 16000) i;

-- unique --
BEGIN;
  INSERT INTO columnar_table VALUES (100000000);
  SAVEPOINT s1;
  -- errors out due to unflushed data in upper transaction
  CREATE UNIQUE INDEX ON columnar_table (a);
ROLLBACK;

CREATE UNIQUE INDEX CONCURRENTLY ON columnar_table (a);

BEGIN;
  INSERT INTO columnar_table VALUES (16050);
  SAVEPOINT s1;
  -- index scan errors out due to unflushed data in upper transaction
  SELECT a FROM columnar_table WHERE a = 16050;
ROLLBACK;

EXPLAIN (COSTS OFF) SELECT * FROM columnar_table WHERE a=6456;
EXPLAIN (COSTS OFF) SELECT a FROM columnar_table WHERE a=6456;
SELECT (SELECT a FROM columnar_table WHERE a=6456 limit 1)=6456;
SELECT (SELECT b FROM columnar_table WHERE a=6456 limit 1)=6456*2;

-- even if a=16050 doesn't exist, we try to insert it twice so this should error out
INSERT INTO columnar_table VALUES (16050), (16050);

-- should work
INSERT INTO columnar_table VALUES (16050);

-- check edge cases around stripe boundaries, error out
INSERT INTO columnar_table VALUES (16050);
INSERT INTO columnar_table VALUES (15999);

DROP INDEX columnar_table_a_idx;

CREATE TABLE partial_unique_idx_test (a INT, b INT) USING columnar;
CREATE UNIQUE INDEX ON partial_unique_idx_test (a)
WHERE b > 500;

-- should work since b =< 500 and our partial index doesn't check this interval
INSERT INTO partial_unique_idx_test VALUES (1, 2), (1, 2);

-- should work since our partial index wouldn't cover the tuples that we inserted above
INSERT INTO partial_unique_idx_test VALUES (1, 800);

INSERT INTO partial_unique_idx_test VALUES (4, 600);

-- should error out due to (4, 600)
INSERT INTO partial_unique_idx_test VALUES (4, 700);

-- btree --
CREATE INDEX CONCURRENTLY ON columnar_table (a);
SELECT (SELECT SUM(b) FROM columnar_table WHERE a>700 and a<965)=439560;

CREATE INDEX ON columnar_table (b)
WHERE (b > 30000 AND b < 33000);

-- partial index should be way smaller than the non-partial index
SELECT pg_total_relation_size('columnar_table_b_idx') * 5 <
       pg_total_relation_size('columnar_table_a_idx');

-- can't use index scan due to partial index boundaries
EXPLAIN (COSTS OFF) SELECT b FROM columnar_table WHERE b = 30000;
-- can use index scan
EXPLAIN (COSTS OFF) SELECT b FROM columnar_table WHERE b = 30001;

-- some more rows
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16000, 17000) i;

DROP INDEX CONCURRENTLY columnar_table_a_idx;
TRUNCATE columnar_table;

-- pkey --
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16000, 16499) i;
ALTER TABLE columnar_table ADD PRIMARY KEY (a);
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16500, 17000) i;

BEGIN;
  INSERT INTO columnar_table (a) SELECT 1;
ROLLBACK;

-- should work
INSERT INTO columnar_table (a) SELECT 1;

-- error out
INSERT INTO columnar_table VALUES (16100), (16101);
INSERT INTO columnar_table VALUES (16999);

BEGIN;
  REINDEX INDEX columnar_table_pkey;
  -- should error even after reindex
  INSERT INTO columnar_table VALUES (16999);
ROLLBACK;

VACUUM FULL columnar_table;

-- show that we don't support clustering columnar tables using indexes
CLUSTER columnar_table USING columnar_table_pkey;

ALTER TABLE columnar_table CLUSTER ON columnar_table_pkey;
CLUSTER columnar_table;

-- should error even after vacuum
INSERT INTO columnar_table VALUES (16999);

TRUNCATE columnar_table;
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(1, 160000) i;
SELECT (SELECT b FROM columnar_table WHERE a = 150000)=300000;

TRUNCATE columnar_table;
ALTER TABLE columnar_table DROP CONSTRAINT columnar_table_pkey;

-- hash --
INSERT INTO columnar_table (a, b) SELECT i*2,i FROM generate_series(1, 8000) i;
CREATE INDEX hash_idx ON columnar_table USING HASH (b);

BEGIN;
  CREATE INDEX hash_idx_fill_factor ON columnar_table USING HASH (b) WITH (fillfactor=10);
  -- same hash index with lower fillfactor should be way bigger
  SELECT pg_total_relation_size ('hash_idx_fill_factor') >
         pg_total_relation_size ('hash_idx') * 5;
ROLLBACK;

BEGIN;
  INSERT INTO columnar_table (a, b) SELECT i*3,i FROM generate_series(1, 8000) i;
ROLLBACK;

INSERT INTO columnar_table (a, b) SELECT i*4,i FROM generate_series(1, 8000) i;

SELECT SUM(a)=42000 FROM columnar_table WHERE b = 7000;

BEGIN;
  REINDEX TABLE columnar_table;
  SELECT SUM(a)=42000 FROM columnar_table WHERE b = 7000;
ROLLBACK;

VACUUM FULL columnar_table;
SELECT SUM(a)=42000 FROM columnar_table WHERE b = 7000;

-- exclusion contraints --
CREATE TABLE exclusion_test (c1 INT,c2 INT, c3 INT, c4 BOX,
EXCLUDE USING btree (c1 WITH =) INCLUDE(c3,c4) WHERE (c1 < 10)) USING columnar;

-- error out since "c1" is "1" for all rows to be inserted
INSERT INTO exclusion_test SELECT 1, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;

BEGIN;
  INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;
ROLLBACK;

-- should work
INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;

INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(10,15) AS x;

BEGIN;
  -- should work thanks to "where" clause in exclusion constraint
  INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(10,15) AS x;
ROLLBACK;

REINDEX TABLE exclusion_test;
-- should still work after reindex
INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(10,15) AS x;

-- make sure that we respect INCLUDE syntax --

CREATE TABLE include_test (a INT, b BIGINT, c BIGINT, d BIGINT) USING columnar;

INSERT INTO include_test SELECT i, i, i, i FROM generate_series (1, 1000) i;

CREATE UNIQUE INDEX CONCURRENTLY unique_a ON include_test (a);

-- cannot use index only scan
EXPLAIN (COSTS OFF) SELECT b FROM include_test WHERE a = 500;

CREATE UNIQUE INDEX unique_a_include_b_c_d ON include_test (a) INCLUDE(b, c, d);

-- same unique index that includes other columns should be way bigger
SELECT pg_total_relation_size ('unique_a') * 1.5 <
       pg_total_relation_size ('unique_a_include_b_c_d');

DROP INDEX unique_a;

-- should use index only scan since unique_a_include_b_c_d includes column "b" too
EXPLAIN (COSTS OFF) SELECT b FROM include_test WHERE a = 500;

BEGIN;
  SET enable_indexonlyscan = OFF;
  -- show that we respect enable_indexonlyscan GUC
  EXPLAIN (COSTS OFF) SELECT b FROM include_test WHERE a = 500;
ROLLBACK;

-- make sure that we read the correct value for "b" when doing index only scan
SELECT b=980 FROM include_test WHERE a = 980;

-- some tests with distributed & partitioned tables --

CREATE TABLE dist_part_table(
  dist_col INT,
  part_col TIMESTAMPTZ,
  col1 TEXT
) PARTITION BY RANGE (part_col);

-- create an index before creating a columnar partition
CREATE INDEX dist_part_table_btree ON dist_part_table (col1);

-- columnar partition
CREATE TABLE p0 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
USING columnar;

SELECT create_distributed_table('dist_part_table', 'dist_col');

-- columnar partition
CREATE TABLE p1 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
USING columnar;

-- row partition
CREATE TABLE p2 PARTITION OF dist_part_table
FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO dist_part_table VALUES (1, '2020-03-15', 'str1', POINT(1, 1));

-- insert into columnar partitions
INSERT INTO dist_part_table VALUES (1, '2020-01-15', 'str2', POINT(2, 2));
INSERT INTO dist_part_table VALUES (1, '2020-02-15', 'str3', POINT(3, 3));

-- create another index after creating a columnar partition
CREATE UNIQUE INDEX dist_part_table_unique ON dist_part_table (dist_col, part_col);

-- verify that indexes are created on columnar partitions
SELECT COUNT(*)=2 FROM pg_indexes WHERE tablename = 'p0';
SELECT COUNT(*)=2 FROM pg_indexes WHERE tablename = 'p1';

-- unsupported index types --

-- gin --
CREATE TABLE testjsonb (j JSONB) USING columnar;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,10) i;
CREATE INDEX jidx ON testjsonb USING GIN (j);
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(15,20) i;

-- gist --
CREATE TABLE gist_point_tbl(id INT4, p POINT) USING columnar;
INSERT INTO gist_point_tbl (id, p) SELECT g, point(g*10, g*10) FROM generate_series(1, 10) g;
CREATE INDEX gist_pointidx ON gist_point_tbl USING gist(p);
INSERT INTO gist_point_tbl (id, p) SELECT g, point(g*10, g*10) FROM generate_series(10, 20) g;

-- sp gist --
CREATE TABLE box_temp (f1 box) USING columnar;
INSERT INTO box_temp SELECT box(point(i, i), point(i * 2, i * 2)) FROM generate_series(1, 10) AS i;
CREATE INDEX CONCURRENTLY box_spgist ON box_temp USING spgist (f1);

-- CONCURRENTLY should not leave an invalid index behind
SELECT COUNT(*)=0 FROM pg_index WHERE indrelid = 'box_temp'::regclass AND indisvalid = 'false';

INSERT INTO box_temp SELECT box(point(i, i), point(i * 2, i * 2)) FROM generate_series(1, 10) AS i;

-- brin --
CREATE TABLE brin_summarize (value int) USING columnar;
CREATE INDEX brin_summarize_idx ON brin_summarize USING brin (value) WITH (pages_per_range=2);

-- Show that we safely fallback to serial index build.
CREATE TABLE parallel_scan_test(a int) USING columnar WITH ( parallel_workers = 2 );
INSERT INTO parallel_scan_test SELECT i FROM generate_series(1,10) i;
CREATE INDEX ON parallel_scan_test (a);
VACUUM FULL parallel_scan_test;
REINDEX TABLE parallel_scan_test;
CREATE INDEX CONCURRENTLY ON parallel_scan_test (a);
REINDEX TABLE CONCURRENTLY parallel_scan_test;

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_indexes CASCADE;
