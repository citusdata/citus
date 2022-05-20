SELECT columnar_test_helpers.compression_type_supported('zstd') AS zstd_supported \gset
\if :zstd_supported
\else
\q
\endif

CREATE SCHEMA am_zstd;
SET search_path TO am_zstd;

SET columnar.compression TO 'zstd';
CREATE TABLE test_zstd (a int, b text, c int) USING columnar;

INSERT INTO test_zstd SELECT i % 1000, (i % 10)::text, 4 FROM generate_series(1, 10000) i;
SELECT count(*) FROM test_zstd;

INSERT INTO test_zstd SELECT floor(i / 2), floor(i / 10)::text, 5 FROM generate_series(1000, 11000) i;
SELECT count(*) FROM test_zstd;

CREATE TABLE test_none (LIKE test_zstd) USING columnar;
INSERT INTO test_none SELECT * FROM test_zstd;

SELECT DISTINCT * FROM test_zstd ORDER BY a, b, c LIMIT 5;

VACUUM FULL test_zstd;

SELECT pg_relation_size('test_zstd') AS size_comp_level_default \gset

-- change compression level
ALTER TABLE test_zstd SET (columnar.compression_level = 19);
VACUUM FULL test_zstd;

SELECT pg_relation_size('test_zstd') AS size_comp_level_19 \gset

-- verify that higher compression level compressed better
SELECT :size_comp_level_default > :size_comp_level_19 AS size_changed;

-- compare compression rate to pglz
SET columnar.compression TO 'pglz';
CREATE TABLE test_pglz (LIKE test_zstd) USING columnar;
INSERT INTO test_pglz SELECT * FROM test_zstd;

SELECT pg_relation_size('test_pglz') AS size_pglz \gset

-- verify that zstd compressed better than pglz
SELECT :size_pglz > :size_comp_level_default;

-- Other operations

ANALYZE test_zstd;
SELECT count(DISTINCT test_zstd.*) FROM test_zstd;

TRUNCATE test_zstd;

SELECT count(DISTINCT test_zstd.*) FROM test_zstd;

SET client_min_messages TO WARNING;
DROP SCHEMA am_zstd CASCADE;
