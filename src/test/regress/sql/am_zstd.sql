SELECT compression_type_supported('zstd') AS zstd_supported \gset
\if :zstd_supported
\else
\q
\endif

CREATE SCHEMA am_zstd;
SET search_path TO am_zstd;

SET columnar.compression TO 'zstd';
CREATE TABLE test_zstd (a int, b text, c int) USING columnar;

INSERT INTO test_zstd SELECT floor(i / 1000), floor(i / 10)::text, 4 FROM generate_series(1, 10000) i;
SELECT count(*) FROM test_zstd;

INSERT INTO test_zstd SELECT floor(i / 2), floor(i / 10)::text, 5 FROM generate_series(1000, 11000) i;
SELECT count(*) FROM test_zstd;

VACUUM VERBOSE test_zstd;

SELECT DISTINCT * FROM test_zstd ORDER BY a, b, c LIMIT 5;

-- change compression level
-- for this particular usecase, higher compression levels
-- don't improve compression ratio
SELECT alter_columnar_table_set('test_zstd', compression_level => 19);
VACUUM FULL test_zstd;
VACUUM VERBOSE test_zstd;

-- compare compression rate to pglz
SET columnar.compression TO 'pglz';
CREATE TABLE test_pglz (LIKE test_zstd) USING columnar;
INSERT INTO test_pglz SELECT * FROM test_zstd;

VACUUM VERBOSE test_pglz;

-- Other operations
VACUUM FULL test_zstd;
ANALYZE test_zstd;

SELECT count(DISTINCT test_zstd.*) FROM test_zstd;

TRUNCATE test_zstd;

SELECT count(DISTINCT test_zstd.*) FROM test_zstd;

SET client_min_messages TO WARNING;
DROP SCHEMA am_zstd CASCADE;
