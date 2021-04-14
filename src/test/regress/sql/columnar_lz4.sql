SELECT columnar_test_helpers.compression_type_supported('lz4') AS lz4_supported \gset
\if :lz4_supported
\else
\q
\endif

CREATE SCHEMA am_alz4;
SET search_path TO am_alz4;

SET columnar.compression TO 'lz4';
CREATE TABLE test_lz4 (a int, b text, c int) USING columnar;

INSERT INTO test_lz4 SELECT floor(i / 1000), floor(i / 10)::text, 4 FROM generate_series(1, 10000) i;
SELECT count(*) FROM test_lz4;

INSERT INTO test_lz4 SELECT floor(i / 2), floor(i / 10)::text, 5 FROM generate_series(1000, 11000) i;
SELECT count(*) FROM test_lz4;

SELECT pg_relation_size('test_lz4') AS size_lz4 \gset

SELECT DISTINCT * FROM test_lz4 ORDER BY a, b, c LIMIT 5;

-- compare compression rate to pglz
SET columnar.compression TO 'pglz';
CREATE TABLE test_pglz (LIKE test_lz4) USING columnar;
INSERT INTO test_pglz SELECT * FROM test_lz4;

SELECT pg_relation_size('test_pglz') AS size_pglz \gset

-- verify that pglz & lz4 resulted in different compression ratios
SELECT :size_pglz <> :size_lz4;

-- Other operations
VACUUM FULL test_lz4;
ANALYZE test_lz4;

SELECT count(DISTINCT test_lz4.*) FROM test_lz4;

TRUNCATE test_lz4;

SELECT count(DISTINCT test_lz4.*) FROM test_lz4;

SET client_min_messages TO WARNING;
DROP SCHEMA am_alz4 CASCADE;
