--
-- Test the ANALYZE command for cstore_fdw tables.
--

-- ANALYZE uncompressed table
ANALYZE contestant;
SELECT count(*) FROM pg_stats WHERE tablename='contestant';

-- ANALYZE compressed table
ANALYZE contestant_compressed;
SELECT count(*) FROM pg_stats WHERE tablename='contestant_compressed';

-- ANALYZE a table with lots of data to trigget qsort in analyze.c
CREATE TABLE test_analyze(a int, b text, c char) USING columnar;
INSERT INTO test_analyze SELECT floor(i / 1000), floor(i / 10)::text, 4 FROM generate_series(1, 100000) i;
INSERT INTO test_analyze SELECT floor(i / 2), floor(i / 10)::text, 5 FROM generate_series(1000, 110000) i;

ANALYZE test_analyze;
DROP TABLE test_analyze;
