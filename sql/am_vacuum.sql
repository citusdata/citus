SELECT count(*) AS columnar_table_count FROM cstore.cstore_data_files \gset

CREATE TABLE t(a int, b int) USING cstore_tableam;

SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

INSERT INTO t SELECT i, i * i FROM generate_series(1, 10) i;
INSERT INTO t SELECT i, i * i FROM generate_series(11, 20) i;
INSERT INTO t SELECT i, i * i FROM generate_series(21, 30) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- vacuum full should merge stripes together
VACUUM FULL t;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- test the case when all data cannot fit into a single stripe
SET cstore.stripe_row_count TO 1000;
INSERT INTO t SELECT i, 2 * i FROM generate_series(1,2500) i;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

VACUUM FULL t;

SELECT sum(a), sum(b) FROM t;
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t';

-- VACUUM FULL doesn't reclaim dropped columns, but converts them to NULLs
ALTER TABLE t DROP COLUMN a;

SELECT stripe, attr, block, minimum_value IS NULL, maximum_value IS NULL FROM cstore.cstore_skipnodes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t' ORDER BY 1, 2, 3;

VACUUM FULL t;

SELECT stripe, attr, block, minimum_value IS NULL, maximum_value IS NULL FROM cstore.cstore_skipnodes a, pg_class b WHERE a.relfilenode=b.relfilenode AND b.relname='t' ORDER BY 1, 2, 3;

-- Make sure we cleaned-up the transient table metadata after VACUUM FULL commands
SELECT count(*) - :columnar_table_count FROM cstore.cstore_data_files;

DROP TABLE t;

-- Make sure we cleaned the metadata for t too
SELECT count(*) - :columnar_table_count FROM cstore.cstore_data_files;
