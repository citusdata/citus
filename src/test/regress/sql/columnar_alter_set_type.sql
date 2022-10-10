--
-- Testing ALTER TABLE on columnar tables.
--

CREATE TABLE test_alter_table (a int, b int, c text) USING columnar;

WITH sample_data AS (VALUES
    (1, 2, '3'),
    (4, 5, '6')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

WITH sample_data AS (VALUES
    (5, 9, '11'),
    (12, 83, '93')
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

ALTER TABLE test_alter_table ALTER COLUMN a TYPE jsonb USING row_to_json(row(a));
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN c TYPE int USING c::integer;
SELECT sum(c) FROM test_alter_table;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE bigint;
SELECT * FROM test_alter_table ORDER BY a;

ALTER TABLE test_alter_table ALTER COLUMN b TYPE float USING (b::float + 0.5);
SELECT * FROM test_alter_table ORDER BY a;

DROP TABLE test_alter_table;

-- Make sure that the correct table options are used when rewriting the table.
-- This is reflected by the VACUUM VERBOSE output right after a rewrite showing
-- that all chunks are compressed with the configured compression algorithm
-- https://github.com/citusdata/citus/issues/5927
CREATE TABLE test(i int) USING columnar;
ALTER TABLE test SET (columnar.compression = lz4);
INSERT INTO test VALUES(1);
VACUUM VERBOSE test;

ALTER TABLE test ALTER COLUMN i TYPE int8;
VACUUM VERBOSE test;

DROP TABLE test;
